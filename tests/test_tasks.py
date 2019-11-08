#!/usr/bin/env python3
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

import datetime
import unittest
import unittest.mock

import dateutil
from redis.exceptions import RedisError

from arthur.common import MAX_JOB_RETRIES, WAIT_FOR_QUEUING
from arthur.errors import (AlreadyExistsError,
                           NotFoundError,
                           TaskRegistryError)
from arthur.tasks import (ArchivingTaskConfig,
                          SchedulingTaskConfig,
                          Task,
                          JobData,
                          TaskRegistry,
                          TaskStatus,
                          logger)

from base import TestBaseRQ

INVALID_ARCHIVE_PATH_ERROR = "'archive_path' must be a str"
INVALID_ARCHIVED_AFTER_ERROR = "'archived_after' must be either a str or a datetime"
INVALID_ARCHIVED_AFTER_INVALID_DATE_ERROR = "is not a valid date"
INVALID_FETCH_FROM_ARCHIVE_ERROR = "'fetch_from_archive' must be a bool;"


class TestTask(unittest.TestCase):
    """Unit tests for Task"""

    @unittest.mock.patch('arthur.tasks.datetime_utcnow')
    def test_init(self, mock_utcnow):
        """Check arguments initialization"""

        mock_utcnow.return_value = datetime.datetime(2017, 1, 1,
                                                     tzinfo=dateutil.tz.tzutc())

        args = {
            'from_date': '1970-01-01',
            'component': 'test'
        }

        task = Task('mytask', 'git', 'commit', args)

        self.assertEqual(task.task_id, 'mytask')
        self.assertEqual(task.status, TaskStatus.NEW)
        self.assertEqual(task.age, 0)
        self.assertListEqual(task.jobs, [])
        self.assertEqual(task.backend, 'git')
        self.assertEqual(task.category, 'commit')
        self.assertDictEqual(task.backend_args, args)
        self.assertEqual(task.archiving_cfg, None)
        self.assertEqual(task.scheduling_cfg, None)
        self.assertEqual(task.created_on, 1483228800.0)

        # Test when archive and scheduler arguments are given
        archive = ArchivingTaskConfig('/tmp/archive', False,
                                      archived_after=None)
        sched = SchedulingTaskConfig(delay=10, max_retries=2,
                                     max_age=5)

        task = Task('mytask', 'git', 'commit', args,
                    archiving_cfg=archive, scheduling_cfg=sched)

        self.assertEqual(task.task_id, 'mytask')
        self.assertEqual(task.status, TaskStatus.NEW)
        self.assertEqual(task.age, 0)
        self.assertListEqual(task.jobs, [])
        self.assertEqual(task.backend, 'git')
        self.assertEqual(task.category, 'commit')
        self.assertDictEqual(task.backend_args, args)
        self.assertEqual(task.archiving_cfg, archive)
        self.assertEqual(task.scheduling_cfg, sched)
        self.assertEqual(task.created_on, 1483228800.0)

    def test_has_resuming(self):
        """Test if a task property returns it might be resumed or not"""

        task = Task('git_task', 'git', 'commit', {},
                    archiving_cfg=None, scheduling_cfg=None)
        self.assertEqual(task.has_resuming(), True)

        task = Task('gerrit_task', 'gerrit', 'review', {},
                    archiving_cfg=None, scheduling_cfg=None)
        self.assertEqual(task.has_resuming(), False)

    def test_backend_not_found(self):
        """Test if it raises an exception when a backend is not found"""

        archive = ArchivingTaskConfig('/tmp/archive', False,
                                      archived_after=None)
        sched = SchedulingTaskConfig(delay=10, max_retries=2,
                                     max_age=5)

        with self.assertRaises(NotFoundError) as e:
            _ = Task('mytask', 'mock_backend', 'mock_item', {},
                     archiving_cfg=archive, scheduling_cfg=sched)
            self.assertEqual(e.exception.element, 'mock_backend')

    def test_set_job(self):
        """Check whether the job is added to the task"""

        args = {
            'from_date': '1970-01-01',
            'component': 'test'
        }
        category = 'commit'

        task = Task('mytask', 'git', category, args)

        task.set_job('job-0', 0)
        task.set_job('job-1', 1)

        expected_job_0 = JobData('job-0', 0)
        expected_job_1 = JobData('job-1', 1)

        self.assertEqual(len(task.jobs), 2)
        self.assertListEqual(task.jobs, [expected_job_0, expected_job_1])

    def test_to_dict(self):
        """Check whether the object is converted into a dict"""

        args = {
            'from_date': '1970-01-01',
            'component': 'test'
        }
        category = 'commit'
        archive = ArchivingTaskConfig('/tmp/archive', False,
                                      archived_after=None)
        sched = SchedulingTaskConfig(delay=10, max_retries=2,
                                     max_age=5, queue='myqueue')
        before = datetime.datetime.now().timestamp()

        task = Task('mytask', 'git', category, args,
                    archiving_cfg=archive, scheduling_cfg=sched)
        task.set_job('job-0', 0)
        task.set_job('job-1', 1)

        d = task.to_dict()

        expected_job_0 = {
            'job_id': 'job-0',
            'job_number': 0
        }
        expected_job_1 = {
            'job_id': 'job-1',
            'job_number': 1
        }
        expected = {
            'task_id': 'mytask',
            'status': 'NEW',
            'age': 0,
            'num_failures': 0,
            'jobs': [expected_job_0, expected_job_1],
            'backend': 'git',
            'backend_args': args,
            'category': category,
            'archiving_cfg': {
                'archive_path': '/tmp/archive',
                'archived_after': None,
                'fetch_from_archive': False
            },
            'scheduling_cfg': {
                'delay': 10,
                'max_retries': 2,
                'max_age': 5,
                'queue': 'myqueue'
            }
        }

        created_on = d.pop('created_on')
        self.assertGreater(created_on, before)
        self.assertDictEqual(d, expected)


class TestTaskRegistry(TestBaseRQ):
    """Unit tests for TaskRegistry"""

    def test_empty_registry(self):
        """Check empty registry on init"""

        registry = TaskRegistry(self.conn)
        tasks = registry.tasks

        self.assertListEqual(tasks, [])

    def test_tasks(self):
        """Test to list tasks in the registry"""

        registry = TaskRegistry(self.conn)
        expected = []
        for i in range(0, 20):
            task_id = 'mytask{}'.format(i)
            expected.append(task_id)
            registry.add(task_id, 'git', 'commit', {})

        tasks = registry.tasks
        self.assertEqual(len(tasks), 20)

        for t in tasks:
            self.assertIn(t.task_id, expected)

    @unittest.mock.patch('redis.StrictRedis.scan')
    def test_tasks_error(self, mock_redis_set):
        """Test whether the lock is released in case of a Redis error and a TaskRegistryError is thrown"""

        mock_redis_set.side_effect = RedisError

        registry = TaskRegistry(self.conn)
        registry.add('mytask', 'git', 'commit', {})
        self.assertGreater(registry._rwlock._readers_mutex._value, 0)
        with self.assertRaises(TaskRegistryError):
            _ = registry.tasks
            self.assertGreater(registry._rwlock._readers_mutex._value, 0)

    def test_matched_tasks(self):
        """Test to list tasks in the registry when other data is stored in Redis"""

        registry = TaskRegistry(self.conn)

        self.conn.set("key1", 'value')
        self.conn.set("key2", 1)
        self.conn.set("key3", b'x')

        expected = []
        for i in range(0, 20):
            task_id = 'mytask{}'.format(i)
            expected.append(task_id)
            registry.add(task_id, 'git', 'commit', {})

        tasks = registry.tasks
        self.assertEqual(len(tasks), 20)

        for t in tasks:
            self.assertIn(t.task_id, expected)

    def test_add_task(self):
        """Test to add tasks to the registry"""

        args = {
            'from_date': '1970-01-01',
            'component': 'test'
        }
        archive = ArchivingTaskConfig('/tmp/archive', False,
                                      archived_after=None)
        sched = SchedulingTaskConfig(delay=10, max_retries=2)

        registry = TaskRegistry(self.conn)
        before = datetime.datetime.now().timestamp()
        _ = registry.add('mytask', 'git', 'commit', args)

        tasks = registry.tasks
        self.assertEqual(len(tasks), 1)

        task = tasks[0]
        self.assertIsInstance(task, Task)
        self.assertEqual(task.task_id, 'mytask')
        self.assertEqual(task.status, TaskStatus.NEW)
        self.assertEqual(task.age, 0)
        self.assertListEqual(task.jobs, [])
        self.assertEqual(task.category, 'commit')
        self.assertEqual(task.backend, 'git')
        self.assertDictEqual(task.backend_args, args)
        self.assertEqual(task.archiving_cfg, None)
        self.assertEqual(task.scheduling_cfg, None)
        self.assertGreater(task.created_on, before)

        before = datetime.datetime.now().timestamp()
        _ = registry.add('atask', 'git', 'commit', args,
                         archiving_cfg=archive, scheduling_cfg=sched)

        tasks = registry.tasks
        self.assertEqual(len(tasks), 2)

        task0 = tasks[0]
        self.assertIsInstance(task0, Task)
        self.assertEqual(task0.task_id, 'atask')
        self.assertEqual(task0.status, TaskStatus.NEW)
        self.assertEqual(task.age, 0)
        self.assertListEqual(task.jobs, [])
        self.assertEqual(task0.backend, 'git')
        self.assertEqual(task0.category, 'commit')
        self.assertDictEqual(task0.backend_args, args)
        self.assertEqual(task0.archiving_cfg.archive_path, archive.archive_path)
        self.assertEqual(task0.archiving_cfg.archived_after, archive.archived_after)
        self.assertEqual(task0.archiving_cfg.fetch_from_archive, archive.fetch_from_archive)
        self.assertEqual(task0.scheduling_cfg.delay, sched.delay)
        self.assertEqual(task0.scheduling_cfg.max_retries, sched.max_retries)
        self.assertGreater(task0.created_on, before)

        task1 = tasks[1]
        self.assertEqual(task1.task_id, 'mytask')
        self.assertGreater(task0.created_on, task1.created_on)

    def test_add_existing_task(self):
        """Check if it raises an exception when an existing task is added again"""

        registry = TaskRegistry(self.conn)
        registry.add('mytask', 'git', 'commit', {})

        with self.assertRaises(AlreadyExistsError):
            registry.add('mytask', 'new_backend', 'commit', {})

        # Only one tasks is on the registry
        tasks = registry.tasks
        self.assertEqual(len(tasks), 1)

        task = tasks[0]
        self.assertEqual(task.task_id, 'mytask')
        self.assertEqual(task.backend, 'git')
        self.assertEqual(task.category, 'commit')
        self.assertDictEqual(task.backend_args, {})

    @unittest.mock.patch('redis.StrictRedis.set')
    def test_add_task_error(self, mock_redis_cmd):
        """Test whether the lock is released in case of a Redis error and a TaskRegistryError is thrown"""

        mock_redis_cmd.side_effect = RedisError

        registry = TaskRegistry(self.conn)
        self.assertGreater(registry._rwlock._access_mutex._value, 0)
        with self.assertRaises(TaskRegistryError):
            registry.add('mytask', 'git', 'commit', {})
            self.assertGreater(registry._rwlock._access_mutex._value, 0)

    def test_remove_task(self):
        """Test to remove a task from the registry"""

        args = {
            'from_date': '1970-01-01',
            'component': 'test'
        }

        registry = TaskRegistry(self.conn)
        registry.add('mytask', 'git', 'commit', args)
        registry.add('newtask', 'bugzilla', 'issue', None)
        registry.add('atask', 'git', 'commit', None)

        tasks = registry.tasks
        self.assertEqual(len(tasks), 3)

        registry.remove('newtask')

        tasks = registry.tasks
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0].task_id, 'atask')
        self.assertEqual(tasks[1].task_id, 'mytask')

    def test_remove_not_found(self):
        """Check whether it raises an exception when a task is not found"""

        registry = TaskRegistry(self.conn)

        with self.assertRaises(NotFoundError):
            registry.remove('mytask')

        registry.add('task', 'git', "mock_category", {})

        with self.assertRaises(NotFoundError):
            registry.remove('mytask')

        self.assertEqual(len(registry.tasks), 1)

    @unittest.mock.patch('redis.StrictRedis.delete')
    def test_remove_task_error(self, mock_redis_cmd):
        """Test whether the lock is released in case of a Redis error and an error TaskRegistryError is thrown"""

        mock_redis_cmd.side_effect = RedisError

        registry = TaskRegistry(self.conn)
        registry.add('mytask', 'git', 'commit', {})
        self.assertGreater(registry._rwlock._access_mutex._value, 0)
        with self.assertRaises(TaskRegistryError):
            registry.remove('mytask')
            self.assertGreater(registry._rwlock._access_mutex._value, 0)

    def test_get_task(self):
        """Test to get a task from the registry"""

        args = {
            'from_date': '1970-01-01',
            'component': 'test'
        }

        registry = TaskRegistry(self.conn)
        registry.add('mytask', 'git', 'commit', args)
        registry.add('newtask', 'bugzilla', 'issue', None)
        registry.add('atask', 'git', 'commit', None)

        task = registry.get('atask')
        self.assertIsInstance(task, Task)
        self.assertEqual(task.task_id, 'atask')
        self.assertEqual(task.status, TaskStatus.NEW)
        self.assertEqual(task.category, 'commit')
        self.assertEqual(task.backend, 'git')
        self.assertEqual(task.backend_args, None)

    def test_get_task_not_found(self):
        """Check whether it raises an exception when a task is not found"""

        registry = TaskRegistry(self.conn)

        with self.assertRaises(NotFoundError):
            registry.get('mytask')

        registry.add('newtask', 'git', 'mocked_category', {})

        with self.assertRaises(NotFoundError):
            registry.get('mytask')

    @unittest.mock.patch('redis.StrictRedis.get')
    def test_get_task_error(self, mock_redis_cmd):
        """Test whether the lock is released in case of a Redis error and a TaskRegistryError is thrown"""

        mock_redis_cmd.side_effect = RedisError

        registry = TaskRegistry(self.conn)
        registry.add('mytask', 'git', 'commit', {})
        self.assertGreater(registry._rwlock._access_mutex._value, 0)
        with self.assertRaises(TaskRegistryError):
            registry.get('mytask')
            self.assertGreater(registry._rwlock._access_mutex._value, 0)

    def test_update_task(self):
        """Test to update tasks in the registry"""

        args = {
            'from_date': '1970-01-01',
            'component': 'test'
        }

        registry = TaskRegistry(self.conn)
        before = datetime.datetime.now().timestamp()
        _ = registry.add('mytask', 'git', 'commit', args)

        tasks = registry.tasks
        self.assertEqual(len(tasks), 1)

        task = tasks[0]
        self.assertIsInstance(task, Task)
        self.assertEqual(task.task_id, 'mytask')
        self.assertEqual(task.status, TaskStatus.NEW)
        self.assertEqual(task.age, 0)
        self.assertListEqual(task.jobs, [])
        self.assertEqual(task.category, 'commit')
        self.assertEqual(task.backend, 'git')
        self.assertDictEqual(task.backend_args, args)
        self.assertEqual(task.archiving_cfg, None)
        self.assertEqual(task.scheduling_cfg, None)
        self.assertGreater(task.created_on, before)

        task.status = TaskStatus.COMPLETED
        task.age = 1
        registry.update('mytask', task)

        tasks = registry.tasks
        self.assertEqual(len(tasks), 1)

        task = tasks[0]
        self.assertIsInstance(task, Task)
        self.assertEqual(task.task_id, 'mytask')
        self.assertEqual(task.status, TaskStatus.COMPLETED)
        self.assertEqual(task.age, 1)
        self.assertListEqual(task.jobs, [])
        self.assertEqual(task.category, 'commit')
        self.assertEqual(task.backend, 'git')
        self.assertDictEqual(task.backend_args, args)
        self.assertEqual(task.archiving_cfg, None)
        self.assertEqual(task.scheduling_cfg, None)
        self.assertGreater(task.created_on, before)

    def test_update_task_not_found(self):
        """Check whether it raises an exception when a task is not found"""

        registry = TaskRegistry(self.conn)

        with self.assertLogs(logger, level='WARNING') as cm:
            registry.update('mytask', None)
            self.assertEqual(cm.output[0], 'WARNING:arthur.tasks:Task mytask not found, adding it')

    @unittest.mock.patch('redis.StrictRedis.exists')
    def test_update_task_error(self, mock_redis_cmd):
        """Test whether the lock is released in case of a Redis error and a TaskRegistryError is thrown"""

        mock_redis_cmd.side_effect = RedisError

        registry = TaskRegistry(self.conn)
        self.assertGreater(registry._rwlock._access_mutex._value, 0)
        with self.assertRaises(TaskRegistryError):
            registry.update('mytask', None)
            self.assertGreater(registry._rwlock._access_mutex._value, 0)


class TestArchivingTaskConfig(unittest.TestCase):
    """Unit tests for ArchivingTaskConfig"""

    def test_init(self):
        """Test whether object properties are initialized"""

        dt = datetime.datetime(2001, 12, 1, 23, 15, 32,
                               tzinfo=dateutil.tz.tzutc())

        archiving_cfg = ArchivingTaskConfig('/tmp/archive', True,
                                            archived_after=dt)
        self.assertEqual(archiving_cfg.archive_path, '/tmp/archive')
        self.assertEqual(archiving_cfg.fetch_from_archive, True)
        self.assertEqual(archiving_cfg.archived_after, dt)

    def test_set_archive_path(self):
        """Test if archive_path property can be set"""

        archiving_cfg = ArchivingTaskConfig('/tmp/archive', False)
        self.assertEqual(archiving_cfg.archive_path, '/tmp/archive')

        archiving_cfg.archive_path = '/tmp/oldarchive'
        self.assertEqual(archiving_cfg.archive_path, '/tmp/oldarchive')

    def test_set_invalid_archive_path(self):
        """Check if an exception is raised for invalid archive_path values"""

        with self.assertRaisesRegex(ValueError, INVALID_ARCHIVE_PATH_ERROR):
            archiving_cfg = ArchivingTaskConfig(5.0, False)

        archiving_cfg = ArchivingTaskConfig('/tmp/archive', False)

        with self.assertRaisesRegex(ValueError, INVALID_ARCHIVE_PATH_ERROR):
            archiving_cfg.archive_path = 1.0

        with self.assertRaisesRegex(ValueError, INVALID_ARCHIVE_PATH_ERROR):
            archiving_cfg.archive_path = True

        self.assertEqual(archiving_cfg.archive_path, '/tmp/archive')

    def test_set_fetch_from_archive(self):
        """Test if fetch_from_archive property can be set"""

        archiving_cfg = ArchivingTaskConfig('/tmp/archive', True)
        self.assertEqual(archiving_cfg.fetch_from_archive, True)

        archiving_cfg.fetch_from_archive = False
        self.assertEqual(archiving_cfg.fetch_from_archive, False)

    def test_set_invalid_fetch_from_archive(self):
        """Check if an exception is raised for invalid fetch_from_archive values"""

        with self.assertRaisesRegex(ValueError, INVALID_FETCH_FROM_ARCHIVE_ERROR):
            archiving_cfg = ArchivingTaskConfig('/tmp/archive', 'False')

        archiving_cfg = ArchivingTaskConfig('/tmp/archive', True)

        with self.assertRaisesRegex(ValueError, INVALID_FETCH_FROM_ARCHIVE_ERROR):
            archiving_cfg.fetch_from_archive = 1.0

        with self.assertRaisesRegex(ValueError, INVALID_FETCH_FROM_ARCHIVE_ERROR):
            archiving_cfg.fetch_from_archive = 'False'

        self.assertEqual(archiving_cfg.fetch_from_archive, True)

    def test_set_archived_after(self):
        """Test if archived_after property can be set"""

        archiving_cfg = ArchivingTaskConfig('/tmp/archive', True)
        self.assertEqual(archiving_cfg.archived_after, None)

        dt = datetime.datetime(2001, 12, 1, 23, 15, 32,
                               tzinfo=dateutil.tz.tzutc())

        archiving_cfg = ArchivingTaskConfig('/tmp/archive', True,
                                            archived_after=dt)
        self.assertEqual(archiving_cfg.archived_after, dt)

        dt = datetime.datetime(2018, 1, 1,
                               tzinfo=dateutil.tz.tzutc())

        archiving_cfg.archived_after = dt
        self.assertEqual(archiving_cfg.archived_after, dt)

    def test_set_archived_after_to_utc(self):
        """Check whether dates are converted to UTC"""

        dt = datetime.datetime(2001, 12, 1, 23, 15, 32,
                               tzinfo=dateutil.tz.tzoffset(None, -21600))
        expected = datetime.datetime(2001, 12, 2, 5, 15, 32,
                                     tzinfo=dateutil.tz.tzutc())

        archiving_cfg = ArchivingTaskConfig('/tmp/archive', True,
                                            archived_after=dt)
        # Date should have been converted to UTC
        self.assertEqual(archiving_cfg.archived_after, expected)

        archiving_cfg.archived_after = datetime.datetime(2001, 12, 2, 5, 15, 32)
        self.assertEqual(archiving_cfg.archived_after, expected)

    def test_set_archived_after_from_date_string(self):
        """Test if archived_after property can be set from a date string"""

        date_str = '2001-12-01 23:15:32 -0600'

        expected = datetime.datetime(2001, 12, 2, 5, 15, 32,
                                     tzinfo=dateutil.tz.tzutc())

        archiving_cfg = ArchivingTaskConfig('/tmp/archive', True,
                                            archived_after=date_str)
        self.assertEqual(archiving_cfg.archived_after, expected)

    def test_set_invalid_archived_after(self):
        """Check if an exception is raised for invalid archived_after values"""

        with self.assertRaisesRegex(ValueError, INVALID_ARCHIVED_AFTER_ERROR):
            archiving_cfg = ArchivingTaskConfig('/tmp/archive', False,
                                                archived_after=1.0)
        with self.assertRaisesRegex(ValueError, INVALID_ARCHIVED_AFTER_INVALID_DATE_ERROR):
            archiving_cfg = ArchivingTaskConfig('/tmp/archive', False,
                                                archived_after='this date')

        archiving_cfg = ArchivingTaskConfig('/tmp/archive', True)

        with self.assertRaisesRegex(ValueError, INVALID_ARCHIVED_AFTER_ERROR):
            archiving_cfg.archived_after = 1.0

        with self.assertRaisesRegex(ValueError, INVALID_ARCHIVED_AFTER_INVALID_DATE_ERROR):
            archiving_cfg.archived_after = ''

        self.assertEqual(archiving_cfg.archived_after, None)


class TestSchedulingTaskConfig(unittest.TestCase):
    """Unit tests for TaskRegistry"""

    def test_init(self):
        """Test whether object properties are initialized"""

        scheduling_cfg = SchedulingTaskConfig()
        self.assertEqual(scheduling_cfg.delay, WAIT_FOR_QUEUING)
        self.assertEqual(scheduling_cfg.max_retries, MAX_JOB_RETRIES)
        self.assertEqual(scheduling_cfg.max_age, None)
        self.assertEqual(scheduling_cfg.queue, None)

        scheduling_cfg = SchedulingTaskConfig(delay=5, max_retries=1,
                                              max_age=10, queue='myqueue')
        self.assertEqual(scheduling_cfg.delay, 5)
        self.assertEqual(scheduling_cfg.max_retries, 1)
        self.assertEqual(scheduling_cfg.max_age, 10)
        self.assertEqual(scheduling_cfg.queue, 'myqueue')

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

    def test_set_max_age(self):
        """Test if max_age property can be set"""

        scheduling_cfg = SchedulingTaskConfig(max_age=3)
        self.assertEqual(scheduling_cfg.max_age, 3)

        scheduling_cfg.max_age = None
        self.assertEqual(scheduling_cfg.max_age, None)

    def test_set_invalid_max_age(self):
        """Check if an exception is raised for invalid max_age values"""

        with self.assertRaises(ValueError):
            _ = SchedulingTaskConfig(max_age=2.0)

        scheduling_cfg = SchedulingTaskConfig(max_age=3)

        with self.assertRaises(ValueError):
            scheduling_cfg.max_age = 0

        with self.assertRaises(ValueError):
            scheduling_cfg.max_age = -1

        with self.assertRaises(ValueError):
            scheduling_cfg.max_age = '5'

        self.assertEqual(scheduling_cfg.max_age, 3)

    def test_set_queue(self):
        """Test if queue property can be set"""

        scheduling_cfg = SchedulingTaskConfig(queue='myqueue')
        self.assertEqual(scheduling_cfg.queue, 'myqueue')

        scheduling_cfg.queue = None
        self.assertEqual(scheduling_cfg.queue, None)

    def test_set_invalid_queue(self):
        """Check if an exception is raised for invalid queue values"""

        with self.assertRaises(ValueError):
            _ = SchedulingTaskConfig(queue=1.0)

        scheduling_cfg = SchedulingTaskConfig(queue='myqueue')

        with self.assertRaises(ValueError):
            scheduling_cfg.queue = 0

        with self.assertRaises(ValueError):
            scheduling_cfg.queue = -1

        self.assertEqual(scheduling_cfg.queue, 'myqueue')

    def test_from_dict(self):
        """Check if an object is created when its properties are given from a dict"""

        opts = {
            'delay': 1,
            'max_retries': 3,
            'max_age': 5,
            'queue': 'myqueue'
        }

        scheduling_cfg = SchedulingTaskConfig.from_dict(opts)
        self.assertIsInstance(scheduling_cfg, SchedulingTaskConfig)
        self.assertEqual(scheduling_cfg.delay, 1)
        self.assertEqual(scheduling_cfg.max_retries, 3)
        self.assertEqual(scheduling_cfg.max_age, 5)
        self.assertEqual(scheduling_cfg.queue, 'myqueue')

    def test_from_dict_max_age_none(self):
        """Check if an object is created from a dict when max_age is None"""

        # Test None options
        opts = {
            'max_age': None
        }

        scheduling_cfg = SchedulingTaskConfig.from_dict(opts)
        self.assertIsInstance(scheduling_cfg, SchedulingTaskConfig)
        self.assertEqual(scheduling_cfg.max_age, None)

    def test_from_dict_queue_none(self):
        """Check if an object is created from a dict when queue is None"""

        # Test None options
        opts = {
            'queue': None
        }

        scheduling_cfg = SchedulingTaskConfig.from_dict(opts)
        self.assertIsInstance(scheduling_cfg, SchedulingTaskConfig)
        self.assertEqual(scheduling_cfg.queue, None)

    def test_from_dict_invalid_option(self):
        """Check if an exception is raised when an invalid option is given"""

        opts = {
            'delay': 1,
            'max_retries': 3,
            'max_age': None,
            'custom_param': 'myparam'
        }

        with self.assertRaisesRegex(ValueError,
                                    "unknown 'custom_param' task config parameter"):
            _ = SchedulingTaskConfig.from_dict(opts)

    def test_to_dict(self):
        """Check whether the object is converted into a dictionary"""

        scheduling_cfg = SchedulingTaskConfig(delay=5, max_retries=1,
                                              max_age=5, queue='myqueue')
        d = scheduling_cfg.to_dict()

        expected = {
            'delay': 5,
            'max_retries': 1,
            'max_age': 5,
            'queue': 'myqueue'
        }

        self.assertDictEqual(d, expected)


if __name__ == "__main__":
    unittest.main()
