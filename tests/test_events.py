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

import unittest

from grimoirelab_toolkit.datetime import datetime_utcnow

from arthur.common import CH_PUBSUB
from arthur.events import (JobEventType,
                           JobEvent,
                           JobEventsListener)

from base import TestBaseRQ


class MockJobResult:
    """Class to test/mock job results"""

    def __init__(self, result, category):
        self.result = result
        self.category = category
        self.timestamp = datetime_utcnow()


class TestJobEvent(unittest.TestCase):
    """Unit tests for JobEvent class"""

    def test_initialization(self):
        """Test if the instance is correctly initialized"""

        dt_before = datetime_utcnow()
        event = JobEvent(JobEventType.COMPLETED, '1', None)
        dt_after = datetime_utcnow()

        self.assertEqual(event.type, JobEventType.COMPLETED)
        self.assertEqual(event.job_id, '1')
        self.assertEqual(event.payload, None)
        self.assertGreater(event.timestamp, dt_before)
        self.assertLess(event.timestamp, dt_after)

        dt_before = datetime_utcnow()
        event = JobEvent(JobEventType.FAILURE, '2', "Error")
        dt_after = datetime_utcnow()

        self.assertEqual(event.type, JobEventType.FAILURE)
        self.assertEqual(event.job_id, '2')
        self.assertEqual(event.payload, "Error")
        self.assertGreater(event.timestamp, dt_before)
        self.assertLess(event.timestamp, dt_after)

    def test_unique_identifier(self):
        """Test if different identifiers create unique identifiers"""

        event_a = JobEvent(JobEventType.COMPLETED, '1', None)
        event_b = JobEvent(JobEventType.COMPLETED, '2', None)
        event_c = JobEvent(JobEventType.FAILURE, '3', None)

        self.assertNotEqual(event_a.uuid, None)
        self.assertNotEqual(event_a.uuid, event_b.uuid)
        self.assertNotEqual(event_b.uuid, event_c.uuid)
        self.assertNotEqual(event_c.uuid, event_a.uuid)

    def test_serializer(self):
        """Test if an event is properly serialized and deserialized"""

        result = MockJobResult(10, 'mockbackend')
        event_a = JobEvent(JobEventType.COMPLETED, '1', result)

        data = event_a.serialize()
        event = JobEvent.deserialize(data)

        self.assertIsInstance(event, JobEvent)
        self.assertEqual(event.uuid, event_a.uuid)
        self.assertEqual(event.timestamp, event_a.timestamp)
        self.assertEqual(event.type, event_a.type)
        self.assertEqual(event.job_id, event_a.job_id)

        payload = event.payload
        self.assertIsInstance(payload, MockJobResult)
        self.assertEqual(payload.result, result.result)
        self.assertEqual(payload.category, result.category)
        self.assertEqual(payload.timestamp, result.timestamp)


class TestJobEventsListener(TestBaseRQ):
    """Unit tests for JobEventsListener class"""

    def test_initialization(self):
        """Test if the instance is correctly initialized"""

        listener = JobEventsListener(self.conn)
        self.assertEqual(listener.conn, self.conn)
        self.assertEqual(listener.events_channel, CH_PUBSUB)
        self.assertEqual(listener.result_handler, None)
        self.assertEqual(listener.result_handler_err, None)

        def handle_successful_job(job):
            pass

        def handle_failed_job(job):
            pass

        listener = JobEventsListener(self.conn,
                                     events_channel='events',
                                     result_handler=handle_successful_job,
                                     result_handler_err=handle_failed_job)
        self.assertEqual(listener.conn, self.conn)
        self.assertEqual(listener.events_channel, 'events')
        self.assertEqual(listener.result_handler, handle_successful_job)
        self.assertEqual(listener.result_handler_err, handle_failed_job)


if __name__ == "__main__":
    unittest.main()
