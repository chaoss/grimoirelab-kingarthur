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

import fakeredis

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
        event = JobEvent(JobEventType.COMPLETED, '1', 'mytask', None)
        dt_after = datetime_utcnow()

        self.assertEqual(event.type, JobEventType.COMPLETED)
        self.assertEqual(event.job_id, '1')
        self.assertEqual(event.task_id, 'mytask')
        self.assertEqual(event.payload, None)
        self.assertGreater(event.timestamp, dt_before)
        self.assertLess(event.timestamp, dt_after)

        dt_before = datetime_utcnow()
        event = JobEvent(JobEventType.FAILURE, '2', 'myothertask', "Error")
        dt_after = datetime_utcnow()

        self.assertEqual(event.type, JobEventType.FAILURE)
        self.assertEqual(event.job_id, '2')
        self.assertEqual(event.task_id, 'myothertask')
        self.assertEqual(event.payload, "Error")
        self.assertGreater(event.timestamp, dt_before)
        self.assertLess(event.timestamp, dt_after)

    def test_unique_identifier(self):
        """Test if different identifiers create unique identifiers"""

        event_a = JobEvent(JobEventType.COMPLETED, '1', 'A', None)
        event_b = JobEvent(JobEventType.COMPLETED, '2', 'B', None)
        event_c = JobEvent(JobEventType.FAILURE, '3', 'C', None)

        self.assertNotEqual(event_a.uuid, None)
        self.assertNotEqual(event_a.uuid, event_b.uuid)
        self.assertNotEqual(event_b.uuid, event_c.uuid)
        self.assertNotEqual(event_c.uuid, event_a.uuid)

    def test_serializer(self):
        """Test if an event is properly serialized and deserialized"""

        result = MockJobResult(10, 'mockbackend')
        event_a = JobEvent(JobEventType.COMPLETED, '1', 'A', result)

        data = event_a.serialize()
        event = JobEvent.deserialize(data)

        self.assertIsInstance(event, JobEvent)
        self.assertEqual(event.uuid, event_a.uuid)
        self.assertEqual(event.timestamp, event_a.timestamp)
        self.assertEqual(event.type, event_a.type)
        self.assertEqual(event.job_id, event_a.job_id)
        self.assertEqual(event.task_id, event_a.task_id)

        payload = event.payload
        self.assertIsInstance(payload, MockJobResult)
        self.assertEqual(payload.result, result.result)
        self.assertEqual(payload.category, result.category)
        self.assertEqual(payload.timestamp, result.timestamp)


class MockPubSub:
    """Mock class to have a non-blocking job events listener"""

    def __init__(self, fake_events):
        self.fake_events = fake_events

    def subscribe(self, *args, **kwargs):
        pass

    def listen(self):
        for event in self.fake_events:
            yield {
                'type': 'message',
                'data': event.serialize()
            }


class MockRedisPubSubConnection(fakeredis.FakeStrictRedis):
    """Class to produce mock pubsub objects"""

    def __init__(self, fake_events, *args, **kwargs):
        self.fake_events = fake_events
        super().__init__(*args, **kwargs)

    def pubsub(self):
        return MockPubSub(self.fake_events)


class TestJobEventsListener(TestBaseRQ):
    """Unit tests for JobEventsListener class"""

    def test_initialization(self):
        """Test if the instance is correctly initialized"""

        listener = JobEventsListener(self.conn)
        self.assertEqual(listener.conn, self.conn)
        self.assertEqual(listener.events_channel, CH_PUBSUB)
        self.assertDictEqual(listener.handlers, {})

        listener = JobEventsListener(self.conn,
                                     events_channel='events')

        self.assertEqual(listener.conn, self.conn)
        self.assertEqual(listener.events_channel, 'events')
        self.assertDictEqual(listener.handlers, {})

    def test_subscribe(self):
        """Test if a listener is subscribed to a set of events"""

        def handle_successful_job(job):
            pass

        def handle_failed_job(job):
            pass

        listener = JobEventsListener(self.conn)

        listener.subscribe(JobEventType.COMPLETED, handle_successful_job)
        listener.subscribe(JobEventType.FAILURE, handle_failed_job)

        self.assertEqual(listener.handlers[JobEventType.COMPLETED],
                         handle_successful_job)
        self.assertEqual(listener.handlers[JobEventType.FAILURE],
                         handle_failed_job)

    def test_replace_subscription(self):
        """Test if a handler is replaced when subscription is called multiple times"""

        def handle_generic_event(job):
            pass

        listener = JobEventsListener(self.conn)

        listener.subscribe(JobEventType.COMPLETED, handle_generic_event)
        self.assertEqual(listener.handlers[JobEventType.COMPLETED],
                         handle_generic_event)

        def handle_specific_event(job):
            pass

        # This should replace the handler by the newer one
        listener.subscribe(JobEventType.COMPLETED, handle_specific_event)
        self.assertEqual(listener.handlers[JobEventType.COMPLETED],
                         handle_specific_event)

    def test_subscribe_invalid_type(self):
        """Check if an exception is raised when the event type is invalid"""

        def handle_generic_event(job):
            pass

        listener = JobEventsListener(self.conn)

        with self.assertRaisesRegex(TypeError, "'str' object is not a JobEventType"):
            listener.subscribe('COMPLETED', handle_generic_event)

    def test_unsubscribe(self):
        """Check if a listener removes its subscription from a set of events"""

        def handle_generic_event(job):
            pass

        listener = JobEventsListener(self.conn)

        listener.subscribe(JobEventType.COMPLETED, handle_generic_event)
        self.assertEqual(listener.handlers[JobEventType.COMPLETED],
                         handle_generic_event)

        listener.unsubscribe(JobEventType.COMPLETED)
        self.assertEqual(listener.handlers[JobEventType.COMPLETED], None)

    def test_unsubscribe_invalid_type(self):
        """Check if an exception is raised when the event type is invalid"""

        listener = JobEventsListener(self.conn)

        with self.assertRaisesRegex(TypeError, "'str' object is not a JobEventType"):
            listener.unsubscribe('COMPLETED')

    def test_listen(self):
        """Check if if listens and handles event messages.

        Due to listening on a channel is a blocking call (because
        Redis lib implementation), this test mocks the connection
        with it to simulate events reception.
        """

        class TrackedEvents:
            def __init__(self):
                self.handled = 0
                self.ok = 0
                self.failures = 0

        global tracked_events
        tracked_events = TrackedEvents()

        def handle_successful_job(job):
            tracked_events.handled += 1
            tracked_events.ok += 1

        def handle_failed_job(job):
            tracked_events.handled += 1
            tracked_events.failures += 1

        events = [
            JobEvent(JobEventType.COMPLETED, 1, 'A', MockJobResult(20, 'A')),
            JobEvent(JobEventType.FAILURE, 2, 'B', 'ERROR'),
            JobEvent(JobEventType.FAILURE, 3, 'C', 'ERROR'),
            JobEvent(JobEventType.FAILURE, 4, 'D', 'ERROR'),
            JobEvent(JobEventType.COMPLETED, 5, 'E', MockJobResult(22, 'B')),
            JobEvent(JobEventType.FAILURE, 6, 'F', 'ERROR'),
            JobEvent(JobEventType.UNDEFINED, 7, 'G', 'OK'),
        ]

        conn = MockRedisPubSubConnection(events)
        listener = JobEventsListener(conn)
        listener.subscribe(JobEventType.COMPLETED, handle_successful_job)
        listener.subscribe(JobEventType.FAILURE, handle_failed_job)

        listener.run()

        # UNDEFINED event was not handled
        self.assertEqual(tracked_events.handled, 6)
        self.assertEqual(tracked_events.ok, 2)
        self.assertEqual(tracked_events.failures, 4)

    def test_listen_after_unsubscribing(self):
        """Check if only handles subscribed messages after unsubscribing.

        Due to listening on a channel is a blocking call (because
        Redis lib implementation), this test mocks the connection
        with it to simulate events reception.
        """

        class TrackedEvents:
            def __init__(self):
                self.handled = 0
                self.ok = 0
                self.failures = 0

        global tracked_events
        tracked_events = TrackedEvents()

        def handle_successful_job(job):
            tracked_events.handled += 1
            tracked_events.ok += 1

        def handle_failed_job(job):
            tracked_events.handled += 1
            tracked_events.failures += 1

        events = [
            JobEvent(JobEventType.COMPLETED, 1, 'A', MockJobResult(20, 'A')),
            JobEvent(JobEventType.FAILURE, 2, 'B', 'ERROR'),
            JobEvent(JobEventType.FAILURE, 3, 'C', 'ERROR'),
            JobEvent(JobEventType.FAILURE, 4, 'D', 'ERROR'),
            JobEvent(JobEventType.COMPLETED, 5, 'E', MockJobResult(22, 'B')),
            JobEvent(JobEventType.FAILURE, 6, 'F', 'ERROR'),
            JobEvent(JobEventType.UNDEFINED, 7, 'G', 'OK'),
        ]

        conn = MockRedisPubSubConnection(events)
        listener = JobEventsListener(conn)

        # COMPLETED and FAILURE events are handled on the first call
        # to run(); on the next call only FAILURE events are handled,
        # tracked_events keeps track of both calls
        listener.subscribe(JobEventType.COMPLETED, handle_successful_job)
        listener.subscribe(JobEventType.FAILURE, handle_failed_job)
        listener.run()
        listener.unsubscribe(JobEventType.COMPLETED)
        listener.run()

        self.assertEqual(tracked_events.handled, 10)
        self.assertEqual(tracked_events.ok, 2)
        self.assertEqual(tracked_events.failures, 8)


if __name__ == "__main__":
    unittest.main()
