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
#

import logging

import rq
import rq.job

from .common import CH_PUBSUB
from .events import JobEventType, JobEvent

JOB_LOGGERS = ['arthur', 'perceval', 'rq']
logger = logging.getLogger(__name__)


class JobLogHandler(logging.StreamHandler):
    """Handler class for the job logs"""

    def __init__(self, job):
        logging.StreamHandler.__init__(self)
        self.job = job
        self.job.meta['log'] = []
        self.job.save_meta()

    def emit(self, record):
        log = {
            'created': record.created,
            'msg': self.format(record),
            'module': record.module,
            'level': self.level
        }
        self.job.meta['log'].append(log)
        self.job.save_meta()


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

    def perform_job(self, job, queue, heartbeat_ttl=None):
        """Custom method to execute a job and notify of its result

        :param job: Job object
        :param queue: the queue containing the object
        :param heartbeat_ttl: time to live heartbeat
        """
        self.setup_job_loghandlers(job)
        self._publish_job_event_when_started(job)
        result = super().perform_job(job, queue, heartbeat_ttl=heartbeat_ttl)
        self._publish_job_event_when_finished(job)

        return result

    def _publish_job_event_when_started(self, job):
        """Send event notifying the job started"""

        task_id = job.kwargs['task_id']

        event = JobEvent(JobEventType.STARTED, job.id, task_id,
                         None)

        msg = event.serialize()
        self.connection.publish(self.pubsub_channel, msg)

    def _publish_job_event_when_finished(self, job):
        """Send event notifying the result of a finished job"""

        job_status = job.get_status()

        if job_status == rq.job.JobStatus.FINISHED:
            event_type = JobEventType.COMPLETED
            payload = job.return_value
        elif job_status == rq.job.JobStatus.FAILED:
            event_type = JobEventType.FAILURE
            payload = {
                'error': job.exc_info,
                'result': job.meta.get('result', None)
            }
        else:
            logger.warning("Unexpected job status %s for finished job %s",
                           job_status, job.id)
            event_type = JobEventType.UNDEFINED
            payload = job_status

        task_id = job.kwargs['task_id']

        event = JobEvent(event_type, job.id, task_id, payload)

        msg = event.serialize()
        self.connection.publish(self.pubsub_channel, msg)

    def setup_job_loghandlers(self, job):
        meta_handler = JobLogHandler(job)
        for logger_name in JOB_LOGGERS:
            logger_job = logging.getLogger(logger_name)
            logger_job.addHandler(meta_handler)
