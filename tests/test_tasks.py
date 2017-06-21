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

from datetime import datetime

if '..' not in sys.path:
    sys.path.insert(0, '..')

from arthur.errors import AlreadyExistsError, NotFoundError
from arthur.tasks import Task, TaskRegistry


class TestTask(unittest.TestCase):
    """Unit tests for Task"""

    def test_init(self):
        """Check arguments initialization"""

        args = {
            'from_date': '1970-01-01',
            'component': 'test'
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
            'cache_path': '/tmp/cache',
            'fetch': True
        }
        sched = {
            'stime': 10
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
            'from_date': '1970-01-01',
            'component': 'test'
        }
        cache = {
            'cache_path': '/tmp/cache',
            'fetch': True
        }
        sched = {
            'stime': 10
        }
        before = datetime.now().timestamp()

        task = Task('mytask', 'mock_backend', args,
                    cache_args=cache, sched_args=sched)
        d = task.to_dict()

        expected = {
            'task_id': 'mytask',
            'backend': 'mock_backend',
            'backend_args': args,
            'cache': cache,
            'scheduler': sched
        }

        created_on = d.pop('created_on')
        self.assertGreater(created_on, before)
        self.assertDictEqual(d, expected)


class TestTaskRegistry(unittest.TestCase):
    """Unit tests for TaskRegistry"""

    def test_empty_registry(self):
        """Check empty registry on init"""

        registry = TaskRegistry()
        tasks = registry.tasks

        self.assertListEqual(tasks, [])

    def test_add_task(self):
        """Test to add tasks to the registry"""

        args = {
            'from_date': '1970-01-01',
            'component': 'test'
        }
        cache = {
            'cache_path': '/tmp/cache',
            'fetch': True
        }
        sched = {
            'stime': 10
        }

        registry = TaskRegistry()
        before = datetime.now().timestamp()
        new_task = registry.add('mytask', 'mock_backend', args)

        tasks = registry.tasks
        self.assertEqual(len(tasks), 1)

        task = tasks[0]
        self.assertIsInstance(task, Task)
        self.assertEqual(task, new_task)
        self.assertEqual(task.task_id, 'mytask')
        self.assertEqual(task.backend, 'mock_backend')
        self.assertDictEqual(task.backend_args, args)
        self.assertDictEqual(task.cache_args, {})
        self.assertDictEqual(task.sched_args, {})
        self.assertGreater(task.created_on, before)

        before = datetime.now().timestamp()
        _ = registry.add('atask', 'mock_backend', args,
                         cache_args=cache, sched_args=sched)

        tasks = registry.tasks
        self.assertEqual(len(tasks), 2)

        task0 = tasks[0]
        self.assertIsInstance(task0, Task)
        self.assertEqual(task0.task_id, 'atask')
        self.assertEqual(task0.backend, 'mock_backend')
        self.assertDictEqual(task0.backend_args, args)
        self.assertDictEqual(task0.cache_args, cache)
        self.assertDictEqual(task0.sched_args, sched)
        self.assertGreater(task0.created_on, before)

        task1 = tasks[1]
        self.assertEqual(task1.task_id, 'mytask')
        self.assertGreater(task0.created_on, task1.created_on)

    def test_add_existing_task(self):
        """Check if it raises an exception when an exisiting task is added again"""

        registry = TaskRegistry()
        registry.add('mytask', 'mock_backend', {})

        with self.assertRaises(AlreadyExistsError):
            registry.add('mytask', 'new_backend', {})

        # Only one tasks is on the registry
        tasks = registry.tasks
        self.assertEqual(len(tasks), 1)

        task = tasks[0]
        self.assertEqual(task.task_id, 'mytask')
        self.assertEqual(task.backend, 'mock_backend')
        self.assertDictEqual(task.backend_args, {})

    def test_remove_task(self):
        """Test to remove a task from the registry"""

        args = {
            'from_date': '1970-01-01',
            'component': 'test'
        }

        registry = TaskRegistry()
        registry.add('mytask', 'mock_backend', args)
        registry.add('newtask', 'to_remove', None)
        registry.add('atask', 'test_backend', None)

        tasks = registry.tasks
        self.assertEqual(len(tasks), 3)

        registry.remove('newtask')

        tasks = registry.tasks
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0].task_id, 'atask')
        self.assertEqual(tasks[1].task_id, 'mytask')

    def test_remove_not_found(self):
        """Check whether it raises an exception when a task is not found"""

        registry = TaskRegistry()

        with self.assertRaises(NotFoundError):
            registry.remove('mytask')

        registry.add('task', 'mock_backend', None, '/tmp/example')

        with self.assertRaises(NotFoundError):
            registry.remove('mytask')

        self.assertEqual(len(registry.tasks), 1)

    def test_get_task(self):
        """Test to get a task from the registry"""

        args = {
            'from_date': '1970-01-01',
            'component': 'test'
        }

        registry = TaskRegistry()
        registry.add('mytask', 'mock_backend', args)
        registry.add('newtask', 'to_remove', None)
        registry.add('atask', 'test_backend', None)

        task = registry.get('atask')
        self.assertIsInstance(task, Task)
        self.assertEqual(task.task_id, 'atask')
        self.assertEqual(task.backend, 'test_backend')
        self.assertEqual(task.backend_args, None)

    def test_get_task_not_found(self):
        """Check whether it raises an exception when a task is not found"""

        registry = TaskRegistry()

        with self.assertRaises(NotFoundError):
            registry.get('mytask')

        registry.add('newtask', 'mock_backend', {})

        with self.assertRaises(NotFoundError):
            registry.get('mytask')


if __name__ == "__main__":
    unittest.main()
