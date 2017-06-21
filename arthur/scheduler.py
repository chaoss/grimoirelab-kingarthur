# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2016 Bitergia
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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#     Santiago Dueñas <sduenas@bitergia.com>
#     Alvaro del Castillo San Felix <acs@bitergia.com>
#

import copy
import logging
import pickle
import threading
import time
import traceback
import sched
import uuid

import rq
import rq.job

from grimoirelab.toolkit.datetime import unixtime_to_datetime

from .common import (CH_PUBSUB,
                     Q_CREATION_JOBS,
                     Q_UPDATING_JOBS,
                     Q_STORAGE_ITEMS,
                     TIMEOUT)
from .errors import NotFoundError
from .jobs import execute_perceval_job
from .utils import RWLock


logger = logging.getLogger(__name__)


# Poll scheduler after this seconds
SCHED_POLLING = 0.5


class _JobScheduler(threading.Thread):
    """Class handler for scheduling jobs.

    Private class needed to schedule jobs. The scheduler does not really
    runs the job. It queues jobs in some predefined worker queues.
    Depending on the overhead of these queue, the jobs will execute sooner.
    These queues are created during the initialization of the class,
    giving a list of queues identifiers in the paramater `queues`.

    Take into account that the enqueuing of a job can be delayed using a
    waiting time. The scheduler will poll jobs to check their waiting
    times. Once it is expired, the tasks will be enqueued. Polling time
    can be configured using the attribute `polling`. By default, it is
    set to `SCHED_POLLING` value.

    :param conn: connection to the Redis database
    :param queues: list of queues created during the initialization
    :param polling: sleep time to poll jobs
    :param async_mode: run in async mode (with workers); set to `False`
        for debugging purposes
    """
    def __init__(self, conn, queues, polling=SCHED_POLLING, async_mode=True):
        super().__init__()
        self.conn = conn
        self.polling = polling
        self.async_mode = async_mode

        self._rwlock = RWLock()
        self._scheduler = sched.scheduler()
        self._queues = {
            queue_id: rq.Queue(queue_id,
                               connection=self.conn,
                               async=self.async_mode)
            for queue_id in queues
        }
        self._jobs = {}
        self._tasks = {}

    def run(self):
        """Run thread to schedule jobs."""

        try:
            self.schedule()
        except Exception as e:
            logger.critical("JobScheduler instance crashed. Error: %s", str(e))
            logger.critical(traceback.format_exc())

    def schedule(self):
        """Schedule jobs in loop."""""

        while True:
            self._scheduler.run(blocking=False)

            if not self.async_mode:
                break

            # Let other threads run
            time.sleep(self.polling)

    def schedule_job_task(self, queue_id, task_id, job_args, delay=0):
        """Schedule a job in the given queue."""

        self._rwlock.writer_acquire()

        job_id = self._generate_job_id(task_id)

        event = self._scheduler.enter(delay, 1, self._enqueue_job,
                                      argument=(queue_id, job_id, job_args,))
        self._jobs[job_id] = event
        self._tasks[task_id] = job_id

        self._rwlock.writer_release()

        logging.debug("Job #%s (task: %s) scheduled on %s (wait: %s)",
                      job_id, task_id, queue_id, delay)

        return job_id

    def cancel_job_task(self, task_id):
        """Cancel the job related to the given task."""

        try:
            self._rwlock.writer_acquire()

            job_id = self._tasks.get(task_id, None)

            if job_id:
                self._cancel_job(job_id)
            else:
                logger.warning("Task %s set to be removed was not found",
                               task_id)
        finally:
            self._rwlock.writer_release()

    def _enqueue_job(self, queue_id, job_id, job_args):
        self._rwlock.writer_acquire()

        self._queues[queue_id].enqueue(execute_perceval_job,
                                       job_id=job_id,
                                       timeout=TIMEOUT,
                                       **job_args)
        del self._jobs[job_id]

        self._rwlock.writer_release()

        logging.debug("Job #%s (task: %s) (%s) queued in '%s'",
                      job_id, job_args['task_id'],
                      job_args['backend'], queue_id)

    def _cancel_job(self, job_id):
        event = self._jobs.get(job_id, None)

        # The job is in the scheduler
        if event:
            try:
                self._scheduler.cancel(event)
                del self._jobs[job_id]
                logger.debug("Event found for #%s; canceling it", job_id)
                return
            except ValueError:
                logger.debug("Event not found for #%s; it should be on the queue",
                             job_id)

        # The job is running on a queue
        rq.cancel_job(job_id, connection=self.conn)
        logger.debug("Job #%s canceled", job_id)

    @staticmethod
    def _generate_job_id(task_id):
        job_id = '-'.join(['arthur', str(task_id), str(uuid.uuid4())])
        return job_id


class _JobListener(threading.Thread):
    """Listen for finished jobs.

    Private class that listens for the result of a job. To manage
    the result two callables may be provided: `result_handler` for
    managing successful jobs and `result_handler_err` for managing
    failed ones.

    :param conn: connection to the Redis database
    :param result_handler: callable object to handle successful jobs
    :param result_handler_err: callabe object to handle failed jobs
    """
    def __init__(self, conn, result_handler=None, result_handler_err=None):
        super().__init__()
        self.conn = conn
        self.result_handler = result_handler
        self.result_handler_err = result_handler_err

    def run(self):
        """Run thread to listen for jobs and reschedule successful ones."""

        try:
            self.listen()
        except Exception as e:
            logger.critical("JobListener instence crashed. Error: %s", str(e))
            logger.critical(traceback.format_exc())

    def listen(self):
        """Listen for completed jobs and reschedule successful ones."""

        pubsub = self.conn.pubsub()
        pubsub.subscribe(CH_PUBSUB)

        for msg in pubsub.listen():
            logger.debug("New message received of type %s", str(msg['type']))

            if msg['type'] != 'message':
                logger.debug("Ignoring job message")
                continue

            data = pickle.loads(msg['data'])
            job_id = data['job_id']

            job = rq.job.Job.fetch(job_id, connection=self.conn)

            if data['status'] == 'finished':
                logging.debug("Job #%s completed", job_id)
                handler = self.result_handler
            elif data['status'] == 'failed':
                logging.debug("Job #%s failed", job_id)
                handler = self.result_handler_err
            else:
                continue

            if handler:
                logging.debug("Calling handler for job #%s", job_id)
                handler(job)


class Scheduler:
    """Scheduler of jobs.

    This class is able to schedule Perceval jobs. Jobs are added to
    two predefined queues: one for creation of repositories and
    one for updating those repositories. Successful jobs will be
    rescheduled once they finished.

    :param conn: connection to the Redis database
    :param registry: regsitry of tasks
    :param async_mode: run in async mode (with workers); set to `False`
        for debugging purposes
    """
    def __init__(self, conn, registry, async_mode=True):
        self.conn = conn
        self.registry = registry
        self.async_mode = async_mode
        self._scheduler = _JobScheduler(self.conn,
                                        [Q_CREATION_JOBS, Q_UPDATING_JOBS],
                                        polling=SCHED_POLLING,
                                        async_mode=self.async_mode)
        self._listener = _JobListener(self.conn,
                                      result_handler=self._handle_successful_job,
                                      result_handler_err=self._handle_failed_job)

    def schedule(self):
        """Start scheduling jobs."""

        if self.async_mode:
            self._scheduler.start()
            self._listener.start()
        else:
            self._scheduler.schedule()

    def schedule_task(self, task_id):
        """Schedule a task.

        :param task_id: identifier of the task to schedule

        :raises NotFoundError: raised when the requested task is not
            found in the registry
        """
        task = self.registry.get(task_id)

        job_args = self._build_job_arguments(task)

        # Schedule the job as soon as possible
        job_id = self._scheduler.schedule_job_task(Q_CREATION_JOBS,
                                                   task.task_id, job_args,
                                                   delay=0)

        logger.info("Job #%s (task: %s) scheduled", job_id, task.task_id)

        return job_id

    def cancel_task(self, task_id):
        """Cancel or 'un-schedule' a task.

        :param task_id: identifier of the task to cancel

        :raises NotFoundError: raised when the requested task is not
            found in the registry
        """
        self.registry.remove(task_id)
        self._scheduler.cancel_job_task(task_id)

        logger.info("Task %s canceled", task_id)

    def _handle_successful_job(self, job):
        """Handle successufl jobs"""

        result = job.result
        task_id = job.kwargs['task_id']

        try:
            task = self.registry.get(task_id)
        except NotFoundError:
            logger.warning("Task %s not found; related job #%s will not be rescheduled",
                           task_id, job.id)
            return

        job_args = self._build_job_arguments(task)

        job_args['fetch_from_cache'] = False

        if result.nitems > 0:
            from_date = unixtime_to_datetime(result.max_date)
            job_args['backend_args']['from_date'] = from_date

            if result.offset:
                job_args['backend_args']['offset'] = result.offset

        delay = task.sched_args['delay']

        job_id = self._scheduler.schedule_job_task(Q_UPDATING_JOBS,
                                                   task_id, job_args,
                                                   delay=delay)

        logger.info("Job #%s (task: %s, old job: %s) re-scheduled",
                    job_id, task_id, job.id)

    def _handle_failed_job(self, job):
        """Handle failed jobs"""

        task_id = job.kwargs['task_id']
        logger.error("Job #%s (task: %s) failed; cancelled",
                     job.id, task_id)

    @staticmethod
    def _build_job_arguments(task):
        """Build the set of arguments required for running a job"""

        job_args = {}
        job_args['qitems'] = Q_STORAGE_ITEMS
        job_args['task_id'] = task.task_id

        # Backend parameters
        job_args['backend'] = task.backend
        job_args['backend_args'] = copy.deepcopy(task.backend_args)

        # Cache parameters
        job_args['cache_path'] = task.cache_args['cache_path']
        job_args['fetch_from_cache'] = task.cache_args['fetch_from_cache']

        # Other parameters
        job_args['max_retries'] = task.sched_args['max_retries_job']

        return job_args
