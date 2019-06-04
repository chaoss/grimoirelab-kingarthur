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
#

import logging
import pickle
import traceback
import threading

import rq

from .common import CH_PUBSUB


logger = logging.getLogger(__name__)


class JobEventsListener(threading.Thread):
    """Listen jobs events.

    This class listens in a separate thread for those events produced
    by the running jobs. A set of handlers can be given to process
    those events.

    :param conn: connection to the Redis database
    :param events_channel: name of the events channel where jobs write events
    :param result_handler: callable object to handle successful jobs
    :param result_handler_err: callable object to handle failed jobs
    """
    def __init__(self, conn, events_channel=CH_PUBSUB,
                 result_handler=None, result_handler_err=None):
        super().__init__()
        self.conn = conn
        self.events_channel = events_channel
        self.result_handler = result_handler
        self.result_handler_err = result_handler_err

    def run(self):
        """Start a thread to listen for jobs events."""

        try:
            self.listen()
        except Exception as e:
            logger.critical("JobEventsListener instance crashed. Error: %s", str(e))
            logger.critical(traceback.format_exc())

    def listen(self):
        """Listen for completed jobs and reschedule successful ones."""

        pubsub = self.conn.pubsub()
        pubsub.subscribe(self.events_channel)

        logger.debug("Listening on channel %s", self.events_channel)

        for msg in pubsub.listen():
            logger.debug("New message received of type %s", str(msg['type']))

            if msg['type'] != 'message':
                logger.debug("Ignoring job message")
                continue

            data = pickle.loads(msg['data'])

            self._dispatch_event(data)

    def _dispatch_event(self, data):
        """Dispatches the job event to the right handler."""

        job_id = data['job_id']
        status = data['status']

        if status == 'finished':
            logging.debug("Job #%s completed", job_id)
            handler = self.result_handler
        elif status == 'failed':
            logging.debug("Job #%s failed", job_id)
            handler = self.result_handler_err
        else:
            logging.debug("No handler defined for %s", status)
            return

        if handler:
            logging.debug("Calling handler for job #%s", job_id)
            job = rq.job.Job.fetch(job_id, connection=self.conn)
            handler(job)
