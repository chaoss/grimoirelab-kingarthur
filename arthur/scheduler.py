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
import datetime
import logging
import pickle
import time
import sched
import uuid

from threading import Thread

import dateutil

from rq import Queue
from rq.job import Job

from .common import (CH_PUBSUB,
                     Q_CREATION_JOBS,
                     Q_UPDATING_JOBS,
                     Q_STORAGE_ITEMS,
                     TIMEOUT,
                     WAIT_FOR_QUEUING)
from .errors import NotFoundError, InvalidDateError
from .jobs import execute_perceval_job


logger = logging.getLogger(__name__)


class Scheduler(Thread):
    """Scheduler of jobs.

    This class is able to schedule Perceval jobs. Jobs are added to
    two predefined queues: one for creation of repositories and
    one for updating those repositories. Successful jobs will be
    rescheduled once they finished.

    :param asyc_mode: set to run in asynchronous mode
    """
    def __init__(self, conn, async_mode=True):
        super().__init__()
        self.daemon = True

        self.conn = conn
        self.queues = {
                       Q_CREATION_JOBS : Queue(Q_CREATION_JOBS, async=async_mode),
                       Q_UPDATING_JOBS : Queue(Q_UPDATING_JOBS, async=async_mode)
                      }

        self._scheduler = sched.scheduler()

    def add_job(self, queue_id, repository):
        """Add a Perceval job to a queue.

        :param queue_id: identifier of the queue where the job will be added
        :param repository: input repository for the job

        :returns: the job identifier of the scheduled job

        :raises NotFoundError: when the queue set to run the job does not
            exist.
        """
        if queue_id not in self.queues:
            raise NotFoundError(element=queue_id)

        cache_fetch = repository.kwargs.get('cache_fetch', False)

        if 'cache_fetch' in repository.kwargs:
            del repository.kwargs['cache_fetch']

        job_args = copy.deepcopy(repository.kwargs)
        job_args['job_id'] =  'arthur-' + str(uuid.uuid4())
        job_args['timeout'] = TIMEOUT
        job_args['qitems'] = Q_STORAGE_ITEMS
        job_args['origin'] = repository.origin
        job_args['backend'] = repository.backend
        job_args['cache_path'] = repository.cache_path
        job_args['cache_fetch'] = cache_fetch

        # Schedule the job as soon as possible
        self._schedule_job(0, Q_CREATION_JOBS, job_args)

        return job_args['job_id']

    def run(self):
        """Run thread to schedule jobs."""

        import traceback

        try:
            self._schedule()
        except Exception:
            logger.error("Scheduler crashed")
            logger.error(traceback.format_exc())

    def run_sync(self):
        """Run scheduler in sync mode"""

        self._scheduler.run()

    def _schedule(self):
        """Main method that runs scheduling threads"""

        # Create a thread to listen and re-schedule
        # completed jobs
        t = Thread(target=self._reschedule)
        t.start()

        while True:
            self._scheduler.run(blocking=False)
            time.sleep(0) # Let other threads run

        t.join()

    def _reschedule(self):
        """Listen for completed jobs and reschedule successful ones"""

        pubsub = self.conn.pubsub()
        pubsub.subscribe(CH_PUBSUB)

        for msg in pubsub.listen():
            logger.debug("New message received of type %s", str(msg['type']))

            if msg['type'] != 'message':
                continue

            data = pickle.loads(msg['data'])

            if data['status'] == 'finished':
                job = Job.fetch(data['job_id'], connection=self.conn)
                self._reschedule_job(job)
            elif data['status'] == 'failed':
                logging.debug("Job #%s failed", data['job_id'])

    def _schedule_job(self, delay, queue_id, job_args):
        self._scheduler.enter(delay, 1, self._enque_job,
                              argument=(queue_id, job_args,))

    def _reschedule_job(self, job):
        result = job.result
        job_args = job.kwargs

        job_args['job_id'] = job.get_id()

        if result.nitems > 0:
            from_date = unixtime_to_datetime(result.max_date)
            job_args['from_date'] = from_date

            if result.offset:
                job_args['offset'] = result.offset

        job_args['cache_path'] = None
        job_args['cache_fetch'] = False

        delay = job_args.get('scheduler_delay', WAIT_FOR_QUEUING)

        logging.debug("Job #%s finished. Rescheduling to fetch data on %s",
                      job.get_id(), delay)

        self._schedule_job(delay, Q_UPDATING_JOBS, job_args)

    def _enque_job(self, queue_id, job_args):
        job = self.queues[queue_id].enqueue(execute_perceval_job,
                                            **job_args)

        logging.debug("Job #%s %s (%s) enqueued in '%s' queue",
                      job.get_id(), job_args['origin'],
                      job_args['backend'], queue_id)


def unixtime_to_datetime(ut):
    """Convert a unixtime timestamp to a datetime object.

    The function converts a timestamp in Unix format to a
    datetime object. UTC timezone will also be set.

    :param ut: Unix timestamp to convert

    :returns: a datetime object

    :raises InvalidDateError: when the given timestamp cannot be
        converted into a valid date
    """
    try:
        dt = datetime.datetime.utcfromtimestamp(ut)
        dt = dt.replace(tzinfo=dateutil.tz.tzutc())
        return dt
    except Exception:
        raise InvalidDateError(date=str(ut))
