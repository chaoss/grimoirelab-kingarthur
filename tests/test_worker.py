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
#

import logging
import unittest

import rq.worker

from arthur.common import CH_PUBSUB
from arthur.events import JobEventType, JobEvent
from arthur.worker import ArthurWorker, JobLogHandler

from base import TestBaseRQ, mock_sum, mock_failure


class MockArthurWorker(rq.worker.SimpleWorker, ArthurWorker):
    """Unit tests for ArthurWorker class, avoids rq inheritance problems"""
    pass


class TestArthurWorker(TestBaseRQ):
    """Unit tests for ArthurWorker class"""

    def test_publish_job_events(self):
        """Tests whether job events are correctly sent"""

        self._publish_job_events(CH_PUBSUB)

    def test_publish_job_events_channel_default(self):
        """Tests whether job events are correctly sent to the default channel"""

        self._publish_job_events(CH_PUBSUB)

    def test_publish_job_events_channel_override(self):
        """"Tests whether job events are correctly sent in a defined channel"""

        self._publish_job_events('test_channel', pubsub_override=True)

    def test_job_logs(self):
        """Tests whether job has logs in their meta field"""

        q = rq.Queue('job_logs_queue')
        w = MockArthurWorker([q])

        job_a = q.enqueue(mock_sum, task_id=0, a=2, b=3)
        job_b = q.enqueue(mock_failure, task_id=1)

        status = w.work(burst=True)
        self.assertEqual(status, True)

        job_a_rq = rq.job.Job.fetch(job_a.id, connection=self.conn)
        job_b_rq = rq.job.Job.fetch(job_b.id, connection=self.conn)

        # Check log list of job 'a'
        self.assertEqual(len(job_a_rq.meta['log']), 5)

        # Check first log message that shows the job was initialized
        self.assertEqual(job_a_rq.meta['log'][0]['module'], 'worker')
        self.assertRegex(job_a_rq.meta['log'][0]['msg'], 'Job OK')

        # Check the last log message that shows when the job finishes
        self.assertEqual(job_a_rq.meta['log'][-1]['module'], 'worker')
        self.assertRegex(job_a_rq.meta['log'][-1]['msg'], 'done, quitting')

        # Check log list of job 'b'
        self.assertEqual(len(job_b_rq.meta['log']), 2)

        # Check first log message that shows the failure of the job
        self.assertEqual(job_b_rq.meta['log'][0]['module'], 'worker')
        self.assertRegex(job_b_rq.meta['log'][0]['msg'], 'Exception')

        # Check the last log message that shows when the job finishes
        self.assertEqual(job_b_rq.meta['log'][-1]['module'], 'worker')
        self.assertRegex(job_b_rq.meta['log'][-1]['msg'], 'done, quitting')

    def _publish_job_events(self, pubsub_channel, pubsub_override=False):
        """Generic job events tests"""

        pubsub = self.conn.pubsub()
        pubsub.subscribe(pubsub_channel)

        q = rq.Queue('foo')
        w = MockArthurWorker([q])

        if pubsub_override:
            w.pubsub_channel = pubsub_channel

        job_a = q.enqueue(mock_sum, task_id=0, a=2, b=3)
        job_b = q.enqueue(mock_failure, task_id=1)

        status = w.work(burst=True)
        self.assertEqual(status, True)

        # Ignore the first messages because it is a
        # subscription notification
        _ = pubsub.get_message()

        # STARTED event for job 'a'
        msg_a = pubsub.get_message()
        event = JobEvent.deserialize(msg_a['data'])
        self.assertEqual(event.job_id, job_a.id)
        self.assertEqual(event.task_id, 0)
        self.assertEqual(event.type, JobEventType.STARTED)
        self.assertEqual(event.payload, None)

        # COMPLETED event for job 'a'
        msg_a = pubsub.get_message()
        event = JobEvent.deserialize(msg_a['data'])
        self.assertEqual(job_a.result, 5)
        self.assertEqual(event.job_id, job_a.id)
        self.assertEqual(event.task_id, 0)
        self.assertEqual(event.type, JobEventType.COMPLETED)
        self.assertEqual(event.payload, 5)

        # STARTED event for job 'b'
        msg_b = pubsub.get_message()
        event = JobEvent.deserialize(msg_b['data'])
        self.assertEqual(event.job_id, job_b.id)
        self.assertEqual(event.task_id, 1)
        self.assertEqual(event.type, JobEventType.STARTED)
        self.assertEqual(event.payload, None)

        # FAILURE event for job 'b'
        msg_b = pubsub.get_message()
        event = JobEvent.deserialize(msg_b['data'])
        self.assertEqual(job_b.result, None)
        self.assertEqual(event.job_id, job_b.id)
        self.assertEqual(event.task_id, 1)
        self.assertEqual(event.type, JobEventType.FAILURE)
        self.assertRegex(event.payload['error'], "Traceback")
        self.assertEqual(event.payload['result'], None)


class TestJobLogHandler(TestBaseRQ):
    """Unit tests for JobLogHandler class"""

    def test_job_log_handler_init(self):
        """Tests whether the handler has initialized well"""

        job_a = rq.job.Job()
        meta_handler = JobLogHandler(job_a)
        self.assertEqual(meta_handler.job, job_a)
        self.assertListEqual(meta_handler.job.meta['log'], [])

    def test_job_log_handler_emit(self):
        """Tests whether the handler catches the messages from the logger that handles"""

        job_a = rq.job.Job()

        # Create handler
        meta_handler = JobLogHandler(job_a)

        # Get logger of this current context and add set level to INFO in order to save info and upper
        logger = logging.getLogger(__name__)
        logger.addHandler(meta_handler)
        logger.setLevel(logging.INFO)

        # Write in the logger
        logger.error("Error log to the handler")
        logger.warning("Warning log to the handler")
        logger.info("Info log to the handler")

        # Check if the logs are saved in the job meta field
        self.assertEqual(len(job_a.meta['log']), 3)
        self.assertEqual(sorted(list(job_a.meta['log'][0].keys())), ['created', 'level', 'module', 'msg'])
        self.assertRegex(job_a.meta['log'][0]['msg'], 'Error')
        self.assertRegex(job_a.meta['log'][-1]['msg'], 'Info')


if __name__ == "__main__":
    unittest.main()
