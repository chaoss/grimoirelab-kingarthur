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

import unittest

import rq.worker

from arthur.common import CH_PUBSUB
from arthur.events import JobEventType, JobEvent
from arthur.worker import ArthurWorker

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


if __name__ == "__main__":
    unittest.main()
