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

    def test_publish_finished_job_status_channel_override(self):
        self._publish_finished_job_status('test_channel')

    def test_publish_finished_job_status_channel_default(self):
        self._publish_finished_job_status(CH_PUBSUB, skip_pubsub_override=True)

    def test_publish_finished_job_status(self):
        self._publish_finished_job_status(CH_PUBSUB)

    def _publish_finished_job_status(self, pubsub_channel, skip_pubsub_override=False):
        """Test whether the worker publishes the status of a finished job"""

        pubsub = self.conn.pubsub()
        pubsub.subscribe(pubsub_channel)

        q = rq.Queue('foo')
        w = MockArthurWorker([q])

        if not skip_pubsub_override:
            w.pubsub_channel = pubsub_channel

        job_a = q.enqueue(mock_sum, task_id=0, a=2, b=3)
        job_b = q.enqueue(mock_failure, task_id=0)

        status = w.work(burst=True)
        self.assertEqual(status, True)

        # Ignore the first message because it is a
        # subscription notification
        _ = pubsub.get_message()
        msg_a = pubsub.get_message()
        msg_b = pubsub.get_message()

        event = JobEvent.deserialize(msg_a['data'])
        self.assertEqual(job_a.result, 5)
        self.assertEqual(event.job_id, job_a.id)
        self.assertEqual(event.type, JobEventType.COMPLETED)
        self.assertEqual(event.payload, 5)

        event = JobEvent.deserialize(msg_b['data'])
        self.assertEqual(job_b.result, None)
        self.assertEqual(event.job_id, job_b.id)
        self.assertEqual(event.type, JobEventType.FAILURE)
        self.assertEqual(event.payload['task_id'], 0)
        self.assertRegex(event.payload['error'], "Traceback")


if __name__ == "__main__":
    unittest.main()
