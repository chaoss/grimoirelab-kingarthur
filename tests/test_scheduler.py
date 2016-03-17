#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2016 Bitergia
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
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
# Authors:
#     Santiago Dueñas <sduenas@bitergia.com>
#

import sys
import unittest

if not '..' in sys.path:
    sys.path.insert(0, '..')

from arthur.common import Q_CREATION_JOBS, Q_UPDATING_JOBS
from arthur.errors import NotFoundError
from arthur.repositories import Repository
from arthur.scheduler import Scheduler

from tests import TestBaseRQ


class TestScheduler(TestBaseRQ):
    """Unit tests for Scheduler class"""

    def test_queues_initialization(self):
        """Queues must be created"""

        schlr = Scheduler(async_mode=False)

        self.assertIn(Q_CREATION_JOBS, schlr.queues)
        self.assertEqual(schlr.queues[Q_CREATION_JOBS]._async, False)

        self.assertIn(Q_UPDATING_JOBS, schlr.queues)
        self.assertEqual(schlr.queues[Q_UPDATING_JOBS]._async, False)

    def test_add_job(self):
        """Jobs should be added and executed"""

        repo = Repository('test', 'git',
                          uri='http://example.com/',
                          gitpath='data/git_log.txt')

        schlr = Scheduler(async_mode=False)
        job = schlr.add_job(Q_CREATION_JOBS, repo)

        self.assertEqual(job.return_value, 1344965413.0)

    def test_not_found_queue(self):
        """Raises an error when a queue does not exist"""

        repo = Repository('test', 'git',
                          uri='http://example.com',
                          gitpath='data/git_log.txt')
        schlr = Scheduler(async_mode=False)

        self.assertRaises(NotFoundError, schlr.add_job, 'myqueue', repo)


if __name__ == "__main__":
    unittest.main()
