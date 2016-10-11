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

from datetime import datetime

if not '..' in sys.path:
    sys.path.insert(0, '..')

import unittest

from arthur.tasks import Task


class TestTask(unittest.TestCase):
    """Unit tests for Task"""

    def test_init(self):
        """Check arguments initialization"""

        args = {
            'from_date' : '1970-01-01',
            'component' : 'test'
        }
        before = datetime.now().timestamp()

        task = Task('mytask', 'mock_backend', args)

        self.assertEqual(task.task_id, 'mytask')
        self.assertEqual(task.backend, 'mock_backend')
        self.assertDictEqual(task.backend_args, args)
        self.assertDictEqual(task.cache_args, {})
        self.assertDictEqual(task.sched_args, {})
        self.assertGreater(task.created_on, before)

        # Test when cache and scheduler arguments are given
        cache = {
            'cache_path' : '/tmp/cache',
            'fetch' : True
        }
        sched = {
            'stime' : 10
        }
        before = datetime.now().timestamp()

        task = Task('mytask', 'mock_backend', args,
                    cache_args=cache, sched_args=sched)

        self.assertEqual(task.task_id, 'mytask')
        self.assertEqual(task.backend, 'mock_backend')
        self.assertDictEqual(task.backend_args, args)
        self.assertDictEqual(task.cache_args, cache)
        self.assertDictEqual(task.sched_args, sched)
        self.assertGreater(task.created_on, before)

    def test_to_dict(self):
        """Check whether the object is converted into a dict"""

        args = {
            'from_date' : '1970-01-01',
            'component' : 'test'
        }
        cache = {
            'cache_path' : '/tmp/cache',
            'fetch' : True
        }
        sched = {
            'stime' : 10
        }
        before = datetime.now().timestamp()

        task = Task('mytask', 'mock_backend', args,
                    cache_args=cache, sched_args=sched)
        d = task.to_dict()

        expected = {
            'task_id' : 'mytask',
            'backend' : 'mock_backend',
            'backend_args' : args,
            'cache' : cache,
            'scheduler' : sched
        }

        created_on = d.pop('created_on')
        self.assertGreater(created_on, before)
        self.assertDictEqual(d, expected)


if __name__ == "__main__":
    unittest.main()
