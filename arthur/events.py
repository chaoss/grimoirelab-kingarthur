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

import enum
import logging
import pickle
import traceback
import threading
import uuid

from grimoirelab_toolkit.datetime import datetime_utcnow

from .common import CH_PUBSUB


logger = logging.getLogger(__name__)


@enum.unique
class JobEventType(enum.Enum):
    STARTED = 1
    COMPLETED = 2
    FAILURE = 3
    UNDEFINED = 999


class JobEvent:
    """Job activity notification.

    Job events can be used to notify the activity of a job. Usually,
    they will inform whether the job was completed or finished with
    a failure.

    Each event has a type, a unique identifier and the time when it
    was generated (in UTC), the identifier of the job and task that
    produced it and a payload. Depending on the type of event, the
    payload might contain different data.

    :param type: event type
    :param job_id: identifier of the job
    :param task_id: identifier of the task
    :param payload: data of the event
    """
    def __init__(self, type, job_id, task_id, payload):
        self.uuid = str(uuid.uuid4())
        self.timestamp = datetime_utcnow()
        self.type = type
        self.job_id = job_id
        self.task_id = task_id
        self.payload = payload

    def serialize(self):
        return pickle.dumps(self)

    @classmethod
    def deserialize(cls, data):
        return pickle.loads(data)


class JobEventsListener(threading.Thread):
    """Listen jobs events.

    This class listens in a separate thread for those events produced
    by the running jobs. Events can be processed using a set of
    handlers.

    Handlers can be registered for a certain type of events using the
    method `subscribe`. To stop handling them call the method
    `unsubscribe`.

    :param conn: connection to the Redis database
    :param events_channel: name of the events channel where jobs write events
    """
    def __init__(self, conn, events_channel=CH_PUBSUB):
        super().__init__()
        self.conn = conn
        self.events_channel = events_channel
        self.handlers = {}

    def run(self):
        """Start a thread to listen for jobs events."""

        try:
            self.listen()
        except Exception as e:
            logger.critical("JobEventsListener instance crashed. Error: %s", str(e))
            logger.critical(traceback.format_exc())

    def subscribe(self, event_type, handler):
        """Activate the listener to handle events of a type.

        The listener will start processing events of the given type.
        When an event of that type is caught, the listener will call
        to `handler` passing it that event.

        Next calls to this method will replace the handler that was
        processing those events.

        :param event_type: type of event to subscribe
        :param handler: callable object to process the events

        :raises TypeError: when `even_type` is not a `JobEventType` object.
        """
        if not isinstance(event_type, JobEventType):
            msg = "'%s' object is not a JobEventType" % event_type.__class__.__name__
            raise TypeError(msg)

        self.handlers[event_type] = handler

    def unsubscribe(self, event_type):
        """Deactivate the listener to handle events of a type.

        When this method is called, the listener will stop processing
        events of the given type.

        :param event_type: type of the event to unsubscribe

        :raises TypeError: when `even_type` is not a `JobEventType` object.
        """
        if not isinstance(event_type, JobEventType):
            msg = "'%s' object is not a JobEventType" % event_type.__class__.__name__
            raise TypeError(msg)

        self.handlers[event_type] = None

    def listen(self):
        """Listen for events.

        The object will start listening for those events produced in
        the system. When an event arrives, the event will be handled
        if the listener was previously subscribed to it.

        Take into account this is a blocking call. The listener will
        be waiting for new events and it will not return the control
        to the caller.
        """
        pubsub = self.conn.pubsub()
        pubsub.subscribe(self.events_channel)

        logger.debug("Listening on channel %s", self.events_channel)

        # Redis 'listen' method is a blocking call
        for msg in pubsub.listen():
            logger.debug("New message received of type %s", str(msg['type']))

            if msg['type'] != 'message':
                logger.debug("Ignoring job message")
                continue

            event = JobEvent.deserialize(msg['data'])

            self._dispatch_event(event)

        logger.debug("End listening on channel %s", self.events_channel)

    def _dispatch_event(self, event):
        """Dispatches the job event to the right handler."""

        handler = self.handlers.get(event.type, None)

        if handler:
            logger.debug("Calling handler for job #%s", event.job_id)
            handler(event)
        else:
            logger.debug("No handler defined for %s", event.type.name)
