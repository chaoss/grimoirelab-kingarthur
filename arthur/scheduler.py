# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2019 Bitergia
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Santiago Dueñas <sduenas@bitergia.com>
#     Alvaro del Castillo San Felix <acs@bitergia.com>
#     Miguel Ángel Fernández <mafesan@bitergia.com>
#

import copy
import logging
import threading
import time
import traceback
import sched
import uuid

import rq
import rq.job

from .common import (CH_PUBSUB,
                     Q_ARCHIVE_JOBS,
                     Q_CREATION_JOBS,
                     Q_RETRYING_JOBS,
                     Q_UPDATING_JOBS,
                     Q_STORAGE_ITEMS,
                     WAIT_FOR_QUEUING,
                     TIMEOUT)
from .errors import NotFoundError, TaskRegistryError
from .events import JobEventType, JobEventsListener
from .jobs import execute_perceval_job
from .tasks import TaskStatus
from .utils import RWLock


logger = logging.getLogger(__name__)


# Poll scheduler after this seconds
SCHED_POLLING = 0.5

# Do not remove jobs and results from the database
INFINITE_TTL = -1


class _TaskScheduler(threading.Thread):
    """Private class to schedule tasks.

    Tasks added to the scheduler go through different stages.
    When a task is added, the scheduler will set it to
    `TaskStatus.SCHEDULED`. It will remain on this stage if a
    waiting time was given. This is useful to delay the execution
    of certain tasks. For example, those recurring tasks that
    overhead and stress some services.

    The scheduler will poll tasks to check their waiting times.
    Polling time can be configured using the attribute `polling`.

    Once waiting time is expired, the task will advance to the
    next stage. The scheduler generates one job for each task and
    enqueues it in some predefined worker queues. At this point,
    the task is set as `TaskStatus.ENQUEUED`.

    Task jobs will execute as soon as possible but it will depend
    on the overhead of the queues. These queues are created during
    the initialization of the class, giving a list of queues
    identifiers in the parameter `queues`.

    Task jobs execution is delegated to external workers. It is
    possible to bypass this behaviour, running tasks in the same
    thread. To do it, set `async_mode` to `False`.

    :param registry: tasks registry
    :param conn: connection to the Redis database
    :param queues: list of queues created during the initialization
    :param polling: sleep time to poll tasks
    :param async_mode: run in async mode (with workers); set to `False`
        for debugging purposes
    """
    def __init__(self, registry, conn, queues, polling=SCHED_POLLING, async_mode=True):
        super().__init__()
        self.registry = registry
        self.conn = conn
        self.polling = polling
        self.async_mode = async_mode

        self._rwlock = RWLock()
        self._delayer = sched.scheduler()
        self._queues = {
            queue_id: rq.Queue(queue_id,
                               connection=self.conn,
                               is_async=self.async_mode)  # noqa: W606
            for queue_id in queues
        }
        self._tasks_events = {}
        self._tasks_jobs = {}

    def run(self):
        """Run thread to schedule tasks."""

        try:
            self.schedule()
        except Exception as e:
            logger.critical("TaskScheduler instance crashed. Error: %s", str(e))
            logger.critical(traceback.format_exc())

    def schedule(self):
        """Start scheduling tasks in loop."""

        while True:
            self._delayer.run(blocking=False)

            if not self.async_mode:
                break

            # Let other threads run
            time.sleep(self.polling)

    def schedule_task(self, task_id, delay=0, reset=False):
        """Schedule the task in the given queue.

        :param task_id: id of the task
        :param delay: delay in seconds to schedule the task
        :param reset: initialize task counters such `age` and
            the number of failures

        :raises NotFoundError: raised when the requested task is not
           found in the registry
        :raises TaskRegistryError: raised when the requested task is not
            retrieved from the registry
        """
        task = self.registry.get(task_id)

        self._rwlock.writer_acquire()

        event = self._delayer.enter(delay, 1, self._enqueue_job_task,
                                    argument=(task_id, ))

        self._tasks_events[task_id] = event
        task.status = TaskStatus.SCHEDULED

        if reset:
            task.num_failures = 0
            task.age = 0

        self.registry.update(task_id, task)

        self._rwlock.writer_release()

        logger.debug("Task: %s scheduled (wait: %s)",
                     task_id, delay)

    def cancel_task(self, task_id):
        """Cancel the given task."""

        self._rwlock.writer_acquire()

        is_scheduled = task_id in self._tasks_events or task_id in self._tasks_jobs

        if is_scheduled:
            self._cancel_task(task_id)
        else:
            logger.warning("Task %s set to be removed was not found",
                           task_id)

        self._rwlock.writer_release()

    def _enqueue_job_task(self, task_id):
        self._rwlock.writer_acquire()

        try:
            task = self.registry.get(task_id)
        except NotFoundError:
            logger.warning("Task %s was canceled; ignore job", task_id)
            del self._tasks_events[task_id]
            return

        job_id = self._generate_job_id()
        job_number = len(task.jobs) + 1
        job_args = _build_job_arguments(task)

        queue_id = self._determine_queue(task)

        if queue_id not in self._queues:
            self._queues[queue_id] = rq.Queue(queue_id,
                                              connection=self.conn,
                                              is_async=self.async_mode)

        job_enqueued = self._queues[queue_id].enqueue(execute_perceval_job,
                                                      job_id=job_id,
                                                      job_number=job_number,
                                                      job_timeout=TIMEOUT,
                                                      ttl=INFINITE_TTL,
                                                      result_ttl=INFINITE_TTL,
                                                      **job_args)
        job_enqueued.meta['job_number'] = job_number
        job_enqueued.save_meta()
        del self._tasks_events[task_id]

        task.status = TaskStatus.ENQUEUED
        task.age += 1
        task.set_job(job_id, job_number)
        self.registry.update(task_id, task)

        self._rwlock.writer_release()

        logger.debug("Job #%s (task: %s) (%s) enqueued in '%s'",
                     job_id, job_args['task_id'],
                     job_args['backend'], queue_id)

    def _cancel_task(self, task_id):
        event = self._tasks_events.get(task_id, None)

        # The job is in the scheduler
        if event:
            try:
                self._delayer.cancel(event)
                del self._tasks_events[task_id]
                logger.debug("Event found for task %s; canceling it", task_id)
                return
            except ValueError:
                logger.debug("Event not found for task %s; it should be on the queue",
                             task_id)

        # The job is running on a queue
        job_id = self._tasks_jobs[task_id]
        rq.cancel_job(job_id, connection=self.conn)
        logger.debug("Job #%s canceled", job_id)
        del self._tasks_jobs[task_id]

    @staticmethod
    def _determine_queue(task):
        scheduling_cfg = task.scheduling_cfg
        archiving_cfg = task.archiving_cfg

        if scheduling_cfg and scheduling_cfg.queue:
            queue_id = scheduling_cfg.queue
        elif archiving_cfg and archiving_cfg.fetch_from_archive:
            queue_id = Q_ARCHIVE_JOBS
        elif task.num_failures > 0:
            queue_id = Q_RETRYING_JOBS
        elif task.age > 0:
            queue_id = Q_UPDATING_JOBS
        else:
            queue_id = Q_CREATION_JOBS
        return queue_id

    @staticmethod
    def _generate_job_id():
        job_id = str(uuid.uuid4())
        return job_id


class StartedJobHandler:
    """Handle started job events.

    This callable will handle the given `JobEventType.STARTED`
    event, changing the status of the task to `TaskStatus.RUNNING`.

    Take into account that while an event is received, the task
    related to it could have been deleted but the notification
    to cancel the job could have not reached on time. On that
    case, the event will be considered as orphan and ignored
    by the handler.

    :param task_scheduler: TaskScheduler instance to manage tasks

    :returns: `True` when the event was handled; `False` when
        it was ignored.
    """
    def __init__(self, task_scheduler):
        self.task_scheduler = task_scheduler

    def __call__(self, event):
        job_id = event.job_id
        task_id = event.task_id

        try:
            task = self.task_scheduler.registry.get(task_id)
        except NotFoundError:
            logger.debug("Task %s not found; orphan event %s for job #%s ignored",
                         task_id, event.uuid, job_id)
            return False
        except TaskRegistryError:
            logger.debug("Task %s wasn't retrieved; job #%s ignored",
                         task_id, job_id)
            return False

        task.status = TaskStatus.RUNNING

        try:
            self.task_scheduler.registry.update(task_id, task)
        except TaskRegistryError:
            logger.debug("Task %s wasn't updated; job #%s ignored",
                         task_id, job_id)
            return False

        return True


class CompletedJobHandler:
    """Handle completed job events.

    This callable will handle the given `JobEventType.COMPLETED`
    event re-scheduling the task related to the successful job.

    Those tasks reaching their maximum age (maximum number of
    times the task was re-scheduled) will be set as
    `TaskStatus.COMPLETED`. Archive tasks will not be re-scheduled,
    either.

    Depending on whether new items where retrieved during the
    last execution, either backend parameters `next_from_date`
    or `next_offset` will be updated accordingly to continue
    with the incremental retrieval for that task.

    Take into account that while an event is received, the task
    related to it could have been deleted but the notification
    to cancel the job could have not reached on time. On that
    case, the event will be considered as orphan and ignored
    by the handler.

    :param task_scheduler: TaskScheduler instance to manage tasks

    :returns: `True` when the event was handled; `False` when
        it was ignored.
    """
    def __init__(self, task_scheduler):
        self.task_scheduler = task_scheduler

    def __call__(self, event):
        result = event.payload
        job_id = event.job_id
        task_id = event.task_id

        try:
            task = self.task_scheduler.registry.get(task_id)
        except NotFoundError:
            logger.debug("Task %s not found; orphan event %s for job #%s ignored",
                         task_id, event.uuid, job_id)
            return False
        except TaskRegistryError:
            logger.debug("Task %s wasn't retrieved; job #%s ignored",
                         task_id, job_id)
            return False

        task.num_failures = 0

        if task.archiving_cfg and task.archiving_cfg.fetch_from_archive:
            task.status = TaskStatus.COMPLETED
            self.task_scheduler.registry.update(task_id, task)
            logger.info("Job #%s (task: %s - archiving) finished successfully",
                        job_id, task_id)
            return True

        if task.scheduling_cfg:
            task_max_age = task.scheduling_cfg.max_age

            if task_max_age and task.age >= task_max_age:
                task.status = TaskStatus.COMPLETED
                self.task_scheduler.registry.update(task_id, task)
                logger.info("Job #%s (task: %s) finished successfully",
                            job_id, task_id)
                return True

        if result.summary.fetched > 0:
            task.backend_args['next_from_date'] = result.summary.max_updated_on

            if result.summary.max_offset:
                task.backend_args['next_offset'] = result.summary.max_offset

        self.task_scheduler.registry.update(task_id, task)
        delay = task.scheduling_cfg.delay if task.scheduling_cfg else WAIT_FOR_QUEUING

        self.task_scheduler.schedule_task(task_id, delay=delay)

        logger.info("Task: %s re-scheduled", task_id)

        return True


class FailedJobHandler:
    """Handle failed job events.

    This callable will handle `JobEventType.FAILURE` events.
    If the task can be resumed, it will be re-scheduled again,
    otherwise, it will be set as `TaskStatus.FAILED`.

    Take into account that while an event is received, the task
    related to it could have been deleted but the notification
    to cancel the job could have not reached on time. On that
    case, the event will be considered as orphan and ignored
    by the handler.

    :param task_scheduler: TaskScheduler instance to manage tasks

    :returns: `True` when the event was handled; `False` when
        it was ignored.
    """
    def __init__(self, task_scheduler):
        self.task_scheduler = task_scheduler

    def __call__(self, event):
        error = event.payload['error']
        result = event.payload['result']
        job_id = event.job_id
        task_id = event.task_id

        try:
            task = self.task_scheduler.registry.get(task_id)
        except NotFoundError:
            logger.debug("Task %s not found; orphan event %s for job %s ignored",
                         task_id, event.uuid, job_id)
            return False
        except TaskRegistryError:
            logger.debug("Task %s wasn't retrieved; job %s ignored",
                         task_id, job_id)
            return False

        task.num_failures += 1

        logger.error("Job #%s (task: %s) failed; error: %s",
                     job_id, task_id, error)

        if task.scheduling_cfg:
            task_max_retries = task.scheduling_cfg.max_retries
        else:
            task_max_retries = 0

        if not task.has_resuming():
            task.status = TaskStatus.FAILED
            self.task_scheduler.registry.update(task_id, task)
            logger.error("Job #%s (task: %s) unable to resume; cancelled",
                         job_id, task_id)
        elif task.num_failures >= task_max_retries:
            task.status = TaskStatus.FAILED
            self.task_scheduler.registry.update(task_id, task)
            logger.error("Job #%s (task: %s) max retries reached; cancelled",
                         job_id, task_id)
        else:
            logger.error("Job #%s (task: %s) failed but will be resumed",
                         job_id, task_id)

            if result.summary.fetched > 0:
                task.backend_args['next_from_date'] = result.summary.max_updated_on

                if result.summary.max_offset:
                    task.backend_args['next_offset'] = result.summary.max_offset

            self.task_scheduler.registry.update(task_id, task)
            delay = task.scheduling_cfg.delay if task.scheduling_cfg else WAIT_FOR_QUEUING

            self.task_scheduler.schedule_task(task_id, delay=delay)

        return True


class Scheduler:
    """Scheduler of jobs.

    This class is able to schedule Perceval jobs. Jobs are added to
    two predefined queues: one for creation of repositories and
    one for updating those repositories. Successful jobs will be
    rescheduled once they finished.

    :param conn: connection to the Redis database
    :param registry: registry of tasks
    :param async_mode: run in async mode (with workers); set to `False`
        for debugging purposes
    """
    def __init__(self, conn, registry, pubsub_channel=CH_PUBSUB, async_mode=True):
        self.conn = conn
        self.registry = registry
        self.async_mode = async_mode
        self._scheduler = _TaskScheduler(self.registry,
                                         self.conn,
                                         [Q_ARCHIVE_JOBS, Q_CREATION_JOBS, Q_RETRYING_JOBS, Q_UPDATING_JOBS],
                                         polling=SCHED_POLLING,
                                         async_mode=self.async_mode)

        self._listener = JobEventsListener(self.conn,
                                           events_channel=pubsub_channel)
        self._listener.subscribe(JobEventType.STARTED,
                                 StartedJobHandler(self._scheduler))
        self._listener.subscribe(JobEventType.COMPLETED,
                                 CompletedJobHandler(self._scheduler))
        self._listener.subscribe(JobEventType.FAILURE,
                                 FailedJobHandler(self._scheduler))

    def schedule(self):
        """Start scheduling jobs."""

        if self.async_mode:
            self._scheduler.start()
            self._listener.start()
        else:
            self._scheduler.schedule()

    def schedule_task(self, task_id, reset=False):
        """Schedule a task.

        :param task_id: identifier of the task to schedule
        :param reset: initialize task counters such `age` and
            the number of failures

        :raises NotFoundError: raised when the requested task is not
            found in the registry
        :raises TaskRegistryError: raised when a RedisError occurs
            when retrieving the task
        """
        self._scheduler.schedule_task(task_id,
                                      delay=0, reset=reset)
        logger.info("Task: %s scheduled", task_id)

    def cancel_task(self, task_id):
        """Cancel or 'un-schedule' a task.

        :param task_id: identifier of the task to cancel

        :raises NotFoundError: raised when the requested task is not
            found in the registry
        :raises TaskRegistryError: raised when a RedisError occurs
            when removing the task
        """
        self.registry.remove(task_id)
        self._scheduler.cancel_task(task_id)

        logger.info("Task %s canceled", task_id)


def _build_job_arguments(task):
    """Build the set of arguments required for running a job"""

    job_args = {}
    job_args['qitems'] = Q_STORAGE_ITEMS
    job_args['task_id'] = task.task_id

    # Backend parameters
    job_args['backend'] = task.backend
    backend_args = copy.deepcopy(task.backend_args)

    if 'next_from_date' in backend_args:
        backend_args['from_date'] = backend_args.pop('next_from_date')

    if 'next_offset' in backend_args:
        backend_args['offset'] = backend_args.pop('next_offset')

    job_args['backend_args'] = backend_args

    # Category
    job_args['category'] = task.category

    # Archiving parameters
    archiving_cfg = task.archiving_cfg
    job_args['archive_args'] = archiving_cfg.to_dict() if archiving_cfg else None

    return job_args
