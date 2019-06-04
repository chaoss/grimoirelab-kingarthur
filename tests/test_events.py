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

from arthur.common import CH_PUBSUB
from arthur.events import JobEventsListener

from base import TestBaseRQ


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
