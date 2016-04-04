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

import datetime
import logging
import pickle
import time

from threading import Thread

import dateutil

from rq import Queue
from rq.job import Job

from .common import (CH_PUBSUB,
                     Q_CREATION_JOBS,
                     Q_UPDATING_JOBS,
                     Q_STORAGE_ITEMS,
                     TIMEOUT)
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
        self.pubsub = self.conn.pubsub()
        self.pubsub.subscribe(CH_PUBSUB)

        self.queues = {
                       Q_CREATION_JOBS : Queue(Q_CREATION_JOBS, async=async_mode),
                       Q_UPDATING_JOBS : Queue(Q_UPDATING_JOBS, async=async_mode)
                      }

    def add_job(self, queue_id, repository):
        """Add a Perceval job to a queue.

        :param queue_id: identifier of the queue where the job will be added
        :param repository: input repository for the job

        :returns: the scheduled job

        :raises NotFoundError: when the queue set to run the job does not
            exist.
        """
        if queue_id not in self.queues:
            raise NotFoundError(element=queue_id)

        job = self.queues[queue_id].enqueue(execute_perceval_job,
                                            timeout=TIMEOUT,
                                            qitems=Q_STORAGE_ITEMS,
                                            origin=repository.origin,
                                            backend=repository.backend,
                                            **repository.kwargs)

        logging.debug("Job #%s %s (%s) enqueued in '%s' queue",
                      job.get_id(), repository.origin,
                      repository.backend, queue_id)

        return job

    def run(self):
        """Run thread to reschedule finished jobs"""

        for msg in self.pubsub.listen():
            if msg['type'] != 'message':
                continue

            data = pickle.loads(msg['data'])

            if data['status'] == 'finished':
                job = Job.fetch(data['job_id'], connection=self.conn)

                from_date = unixtime_to_datetime(job.result)
                kwargs = job.kwargs
                kwargs['from_date'] = from_date

                logging.debug("Job #%s finished. Rescheduling to fetch data since %s",
                              data['job_id'], str(from_date))
                time.sleep(10)

                job = self.queues[Q_UPDATING_JOBS].enqueue(execute_perceval_job,
                                                           **kwargs)

                logging.debug("Job #%s %s (%s) enqueued in '%s' queue",
                              job.get_id(), kwargs['origin'],
                              kwargs['backend'], Q_UPDATING_JOBS)
            elif data['status'] == 'failed':
                logging.debug("Job #%s failed", data['job_id'])


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
