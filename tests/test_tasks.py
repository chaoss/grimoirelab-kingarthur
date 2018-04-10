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

import unittest

from datetime import datetime

from arthur.errors import AlreadyExistsError, NotFoundError
from arthur.tasks import SchedulingTaskConfig, Task, TaskRegistry


class TestTask(unittest.TestCase):
    """Unit tests for Task"""

    def test_init(self):
        """Check arguments initialization"""

        args = {
            'from_date': '1970-01-01',
            'component': 'test'
        }
        before = datetime.now().timestamp()

        task = Task('mytask', 'mock_backend', 'category', args)

        self.assertEqual(task.task_id, 'mytask')
        self.assertEqual(task.backend, 'mock_backend')
        self.assertEqual(task.category, 'category')
        self.assertDictEqual(task.backend_args, args)
        self.assertDictEqual(task.archive_args, {})
        self.assertEqual(task.scheduling_cfg, None)
        self.assertGreater(task.created_on, before)

        # Test when archive and scheduler arguments are given
        archive = {
            'archive_path': '/tmp/archive',
            'fetch_from_archive': False,
            'archived_after': None
        }
        sched = SchedulingTaskConfig(delay=10, max_retries=2)

        before = datetime.now().timestamp()

        task = Task('mytask', 'mock_backend', 'category', args,
                    archive_args=archive, scheduling_cfg=sched)

        self.assertEqual(task.task_id, 'mytask')
        self.assertEqual(task.backend, 'mock_backend')
        self.assertEqual(task.category, 'category')
        self.assertDictEqual(task.backend_args, args)
        self.assertDictEqual(task.archive_args, archive)
        self.assertEqual(task.scheduling_cfg, sched)
        self.assertGreater(task.created_on, before)

    def test_to_dict(self):
        """Check whether the object is converted into a dict"""

        args = {
            'from_date': '1970-01-01',
            'component': 'test'
        }
        category = 'mocked_category'
        archive = {
            'archive_path': '/tmp/archive',
            'fetch_from_archive': False,
            'archived_after': None,
        }
        sched = SchedulingTaskConfig(delay=10, max_retries=2)
        before = datetime.now().timestamp()

        task = Task('mytask', 'mock_backend', category, args,
                    archive_args=archive, scheduling_cfg=sched)
        d = task.to_dict()

        expected = {
            'task_id': 'mytask',
            'backend': 'mock_backend',
            'backend_args': args,
            'category': category,
            'archive_args': archive,
            'scheduling_cfg': {
                'delay': 10,
                'max_retries': 2
            }
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
        archive = {
            'archive_path': '/tmp/archive',
            'fetch_from_archive': False,
            'archived_after': None,
        }
        sched = SchedulingTaskConfig(delay=10, max_retries=2)

        registry = TaskRegistry()
        before = datetime.now().timestamp()
        new_task = registry.add('mytask', 'mock_backend', 'category', args)

        tasks = registry.tasks
        self.assertEqual(len(tasks), 1)

        task = tasks[0]
        self.assertIsInstance(task, Task)
        self.assertEqual(task, new_task)
        self.assertEqual(task.task_id, 'mytask')
        self.assertEqual(task.category, 'category')
        self.assertEqual(task.backend, 'mock_backend')
        self.assertDictEqual(task.backend_args, args)
        self.assertDictEqual(task.archive_args, {})
        self.assertEqual(task.scheduling_cfg, None)
        self.assertGreater(task.created_on, before)

        before = datetime.now().timestamp()
        _ = registry.add('atask', 'mock_backend', 'category', args,
                         archive_args=archive, scheduling_cfg=sched)

        tasks = registry.tasks
        self.assertEqual(len(tasks), 2)

        task0 = tasks[0]
        self.assertIsInstance(task0, Task)
        self.assertEqual(task0.task_id, 'atask')
        self.assertEqual(task0.backend, 'mock_backend')
        self.assertEqual(task0.category, 'category')
        self.assertDictEqual(task0.backend_args, args)
        self.assertDictEqual(task0.archive_args, archive)
        self.assertEqual(task0.scheduling_cfg, sched)
        self.assertGreater(task0.created_on, before)

        task1 = tasks[1]
        self.assertEqual(task1.task_id, 'mytask')
        self.assertGreater(task0.created_on, task1.created_on)

    def test_add_existing_task(self):
        """Check if it raises an exception when an exisiting task is added again"""

        registry = TaskRegistry()
        registry.add('mytask', 'mock_backend', 'category', {})

        with self.assertRaises(AlreadyExistsError):
            registry.add('mytask', 'new_backend', 'category', {})

        # Only one tasks is on the registry
        tasks = registry.tasks
        self.assertEqual(len(tasks), 1)

        task = tasks[0]
        self.assertEqual(task.task_id, 'mytask')
        self.assertEqual(task.backend, 'mock_backend')
        self.assertEqual(task.category, 'category')
        self.assertDictEqual(task.backend_args, {})

    def test_remove_task(self):
        """Test to remove a task from the registry"""

        args = {
            'from_date': '1970-01-01',
            'component': 'test'
        }

        registry = TaskRegistry()
        registry.add('mytask', 'mock_backend', 'mocked_category', args)
        registry.add('newtask', 'to_remove', 'mocked_category', None)
        registry.add('atask', 'test_backend', 'mocked_category', None)

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
        registry.add('mytask', 'mock_backend', 'category', args)
        registry.add('newtask', 'to_remove', 'category', None)
        registry.add('atask', 'test_backend', 'category', None)

        task = registry.get('atask')
        self.assertIsInstance(task, Task)
        self.assertEqual(task.task_id, 'atask')
        self.assertEqual(task.category, 'category')
        self.assertEqual(task.backend, 'test_backend')
        self.assertEqual(task.backend_args, None)

    def test_get_task_not_found(self):
        """Check whether it raises an exception when a task is not found"""

        registry = TaskRegistry()

        with self.assertRaises(NotFoundError):
            registry.get('mytask')

        registry.add('newtask', 'mock_backend', 'mocked_category', {})

        with self.assertRaises(NotFoundError):
            registry.get('mytask')


class TestSchedulingTaskConfig(unittest.TestCase):
    """Unit tests for TaskRegistry"""

    def test_init(self):
        """Test whether object properties are initialized"""

        scheduling_cfg = SchedulingTaskConfig(delay=5, max_retries=1)
        self.assertEqual(scheduling_cfg.delay, 5)
        self.assertEqual(scheduling_cfg.max_retries, 1)

    def test_set_delay(self):
        """Test if delay property can be set"""

        scheduling_cfg = SchedulingTaskConfig(delay=5)
        self.assertEqual(scheduling_cfg.delay, 5)

        scheduling_cfg.delay = 1
        self.assertEqual(scheduling_cfg.delay, 1)

    def test_set_invalid_delay(self):
        """Check if an exception is raised for invalid delay values"""

        with self.assertRaises(ValueError):
            scheduling_cfg = SchedulingTaskConfig(delay=5.0)

        scheduling_cfg = SchedulingTaskConfig(delay=5)

        with self.assertRaises(ValueError):
            scheduling_cfg.delay = 1.0

        with self.assertRaises(ValueError):
            scheduling_cfg.delay = '1'

        self.assertEqual(scheduling_cfg.delay, 5)

    def test_set_max_retries(self):
        """Test if max_retries property can be set"""

        scheduling_cfg = SchedulingTaskConfig(max_retries=3)
        self.assertEqual(scheduling_cfg.max_retries, 3)

        scheduling_cfg.max_retries = 1
        self.assertEqual(scheduling_cfg.max_retries, 1)

    def test_set_invalid_max_retries(self):
        """Check if an exception is raised for invalid max_retries values"""

        with self.assertRaises(ValueError):
            scheduling_cfg = SchedulingTaskConfig(max_retries=2.0)

        scheduling_cfg = SchedulingTaskConfig(max_retries=3)

        with self.assertRaises(ValueError):
            scheduling_cfg.max_retries = 5.0

        with self.assertRaises(ValueError):
            scheduling_cfg.max_retries = '5'

        self.assertEqual(scheduling_cfg.max_retries, 3)

    def test_from_dict(self):
        """Check if an object is created when its properties are given from a dict"""

        opts = {
            'delay': 1,
            'max_retries': 3
        }

        scheduling_cfg = SchedulingTaskConfig.from_dict(opts)
        self.assertIsInstance(scheduling_cfg, SchedulingTaskConfig)
        self.assertEqual(scheduling_cfg.delay, 1)
        self.assertEqual(scheduling_cfg.max_retries, 3)

    def test_from_dict_invalid_option(self):
        """Check if an exception is raised when an invalid option is given"""

        opts = {
            'delay': 1,
            'max_retries': 3,
            'custom_param': 'myparam'
        }

        with self.assertRaisesRegex(ValueError,
                                    "unknown 'custom_param' task config parameter"):
            _ = SchedulingTaskConfig.from_dict(opts)

    def test_to_dict(self):
        """Check whether the object is converted into a dictionary"""

        scheduling_cfg = SchedulingTaskConfig(delay=5, max_retries=1)
        d = scheduling_cfg.to_dict()

        expected = {
            'delay': 5,
            'max_retries': 1
        }

        self.assertDictEqual(d, expected)


if __name__ == "__main__":
    unittest.main()
