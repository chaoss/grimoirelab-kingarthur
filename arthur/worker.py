# -*- coding: utf-8 -*-
#
# Copyright (C) 2014-2019 Bitergia
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
#

import logging

import rq
import rq.job

from .common import CH_PUBSUB
from .events import JobEventType, JobEvent


logger = logging.getLogger(__name__)


class ArthurWorker(rq.Worker):
    """Worker class for Arthur"""

    def __init__(self, queues, **kwargs):
        super().__init__(queues, **kwargs)
        self.__pubsub_channel = CH_PUBSUB

    @property
    def pubsub_channel(self):
        return self.__pubsub_channel

    @pubsub_channel.setter
    def pubsub_channel(self, value):
        self.__pubsub_channel = value

    def perform_job(self, job, queue):
        """Custom method to execute a job and notify of its result

        :param job: Job object
        :param queue: the queue containing the object
        """
        result = super().perform_job(job, queue)

        job_status = job.get_status()

        if job_status == rq.job.JobStatus.FINISHED:
            event_type = JobEventType.COMPLETED
            payload = job.return_value
        elif job_status == rq.job.JobStatus.FAILED:
            event_type = JobEventType.FAILURE
            payload = job.exc_info
        else:
            logger.warning("Unexpected job status %s for finished job %s",
                           job_status, job.id)
            event_type = JobEventType.UNDEFINED
            payload = job_status

        event = JobEvent(event_type, job.id, payload)

        msg = event.serialize()
        self.connection.publish(self.pubsub_channel, msg)

        return result
