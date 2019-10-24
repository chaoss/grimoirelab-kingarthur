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
import os
import os.path
import re
import shutil
import subprocess
import tempfile
import unittest
import unittest.mock

import dateutil
from redis.exceptions import RedisError

from arthur.arthur import Arthur
from arthur.common import ARCHIVES_DEFAULT_PATH
from arthur.errors import AlreadyExistsError, TaskRegistryError
from arthur.tasks import (ArchivingTaskConfig,
                          SchedulingTaskConfig,
                          TaskStatus)

from base import find_empty_redis_database


INVALID_BACKEND_ARGS_ERROR = "Backend_args is not a dict, task .*"
INVALID_FETCH_FROM_ARCHIVE = "'fetch_from_archive' must be a bool; <class 'int'> given"
INVALID_SCHEDULER_PARAM = "unknown .+ task config parameter"
INVALID_SCHEDULER_DELAY = "'delay' must be an int; <class 'str'> given"
INVALID_SCHEDULER_MAX_RETRIES = "'max_retries' must be an int; <class 'str'> given"
INVALID_ARCHIVED_AFTER_INVALID_DATE_ERROR = "'archived_after' is invalid; X is not a valid date"
MISSING_TASK_ID_ERROR = "Missing task_id for task"
MISSING_BACKEND_ERROR = "Missing backend for task .*"
MISSING_CATEGORY_ERROR = "Missing category for task .*"
MISSING_FETCH_FROM_ARCHIVE_PARAM = ".* missing 1 required positional argument: 'fetch_from_archive'"
UNKNOWN_ARCHIVE_PARAMETER = "unknown .* task config parameter"


class TestArthur(unittest.TestCase):
    """Unit tests for Scheduler class"""

    @classmethod
    def setUpClass(cls):
        cls.tmp_path = tempfile.mkdtemp(prefix='arthur_')
        cls.git_path = os.path.join(cls.tmp_path, 'gittest')

        dir = os.path.dirname(os.path.realpath(__file__))
        subprocess.check_call(['tar', '-xzf',
                               os.path.join(dir, 'data/gittest.tar.gz'),
                               '-C', cls.tmp_path])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp_path)

    def setUp(self):
        self.conn = find_empty_redis_database()
        self.conn.flushdb()

    def tearDown(self):
        self.conn.flushdb()

    def test_add_task_no_archive(self):
        """Check when a task has no archive params"""

        task_id = "arthur.task"
        backend = "git"
        category = "commit"
        backend_params = {"a": "a", "b": "b"}

        app = Arthur(self.conn, async_mode=False)

        initial_tasks = len(app._tasks.tasks)
        app.add_task(task_id, backend, category, backend_params)
        after_tasks = len(app._tasks.tasks)

        t = app._tasks.tasks[0]

        self.assertEqual(t.task_id, task_id)
        self.assertEqual(t.backend, backend)
        self.assertDictEqual(t.backend_args, backend_params)
        self.assertEqual(t.archiving_cfg, None)
        self.assertEqual(t.scheduling_cfg, None)

        self.assertEqual(initial_tasks, 0)
        self.assertEqual(after_tasks, 1)

    def test_add_task_empty_archive(self):
        """Check when a task has an empty archive"""

        task_id = "arthur.task"
        backend = "git"
        category = "commit"
        backend_params = {"a": "a", "b": "b"}
        archive_params = None

        app = Arthur(self.conn, async_mode=False)

        initial_tasks = len(app._tasks.tasks)
        app.add_task(task_id, backend, category, backend_params, archive_params)
        after_tasks = len(app._tasks.tasks)

        t = app._tasks.tasks[0]

        self.assertEqual(t.task_id, task_id)
        self.assertEqual(t.backend, backend)
        self.assertDictEqual(t.backend_args, backend_params)
        self.assertEqual(t.archiving_cfg, None)
        self.assertEqual(t.scheduling_cfg, None)

        self.assertEqual(initial_tasks, 0)
        self.assertEqual(after_tasks, 1)

    def test_task_unknown_archive_parameter(self):
        """Check whether an exception is thrown when an unknown parameter is in the archive params"""

        task_id = "arthur.task"
        backend = "git"
        category = None
        backend_params = {"a": "a", "b": "b"}
        archive_params = {"unknown": 1, "fetch_from_archive": True, "archived_after": "2010-10-10"}

        app = Arthur(self.conn, async_mode=False)

        with self.assertRaisesRegex(ValueError, re.compile(UNKNOWN_ARCHIVE_PARAMETER)):
            app.add_task(task_id, backend, category, backend_params, archive_params)

    def test_task_default_archive_path(self):
        """Check whether a default archive path is added when not defined in the archive params"""

        task_id = "arthur.task"
        backend = "git"
        category = "commit"
        backend_params = {"a": "a", "b": "b"}
        archive_params = {
            'fetch_from_archive': True,
            'archived_after': "2010-10-10"
        }

        app = Arthur(self.conn, async_mode=False)
        app.add_task(task_id, backend, category, backend_params, archive_params)

        t = app._tasks.tasks[0]

        archiving_opts = t.archiving_cfg
        expected_dt = datetime.datetime(2010, 10, 10, 0, 0, 0, tzinfo=dateutil.tz.tzutc())
        self.assertIsInstance(archiving_opts, ArchivingTaskConfig)
        self.assertEqual(archiving_opts.archive_path, os.path.expanduser(ARCHIVES_DEFAULT_PATH))
        self.assertEqual(archiving_opts.fetch_from_archive, True)
        self.assertEqual(archiving_opts.archived_after, expected_dt)

    def test_task_wrong_fetch_from_archive(self):
        """Check whether an exception is thrown when fetch_from_archive parameter is not properly set"""

        task_id = "arthur.task"
        backend = "git"
        category = None
        backend_params = {"a": "a", "b": "b"}
        archive_params = {"fetch_from_archive": 100, "archived_after": "2010-10-10"}

        app = Arthur(self.conn, async_mode=False)

        with self.assertRaisesRegex(ValueError, INVALID_FETCH_FROM_ARCHIVE):
            app.add_task(task_id, backend, category, backend_params, archive_params)

        archive_params = {"archived_after": "2010-10-10"}
        with self.assertRaisesRegex(TypeError, re.compile(MISSING_FETCH_FROM_ARCHIVE_PARAM)):
            app.add_task(task_id, backend, category, backend_params, archive_params)

    def test_task_ignore_archive_after(self):
        """Check whether the archived_after parameter is not set when fetch_from_archive is false"""

        task_id = "arthur.task"
        backend = "git"
        category = None
        backend_params = {"a": "a", "b": "b"}
        archive_params = {"fetch_from_archive": False, "archived_after": "X"}

        app = Arthur(self.conn, async_mode=False)

        with self.assertRaisesRegex(ValueError, INVALID_ARCHIVED_AFTER_INVALID_DATE_ERROR):
            app.add_task(task_id, backend, category,
                         backend_params, archive_params)

        self.assertEqual(len(app._tasks.tasks), 0)

    def test_task_wrong_archive_after(self):
        """Check whether an exception is thrown when archived_after parameter is not properly set"""

        task_id = "arthur.task"
        backend = "git"
        category = None
        backend_params = {"a": "a", "b": "b"}
        archive_params = {"fetch_from_archive": True, "archived_after": "X"}

        app = Arthur(self.conn, async_mode=False)

        with self.assertRaisesRegex(ValueError, INVALID_ARCHIVED_AFTER_INVALID_DATE_ERROR):
            app.add_task(task_id, backend, category, backend_params, archive_params)

    def test_add_task_archive(self):
        """Check whether tasks are added"""

        task_id = "arthur.task"
        backend = "git"
        category = 'acme-product'
        backend_params = {"a": "a", "b": "b"}
        archive_args = {
            'fetch_from_archive': True,
            'archived_after': '2100-01-01'
        }
        sched_args = {
            'delay': 10,
            'max_retries': 5
        }

        app = Arthur(self.conn, base_archive_path=self.tmp_path, async_mode=False)

        initial_tasks = len(app._tasks.tasks)
        app.add_task(task_id, backend, category, backend_params,
                     archive_args, sched_args)
        after_tasks = len(app._tasks.tasks)

        t = app._tasks.tasks[0]

        self.assertEqual(t.task_id, task_id)
        self.assertEqual(t.backend, backend)
        self.assertEqual(t.category, category)
        self.assertDictEqual(t.backend_args, backend_params)

        archiving_opts = t.archiving_cfg
        expected_dt = datetime.datetime(2100, 1, 1, 0, 0, 0, tzinfo=dateutil.tz.tzutc())
        self.assertIsInstance(archiving_opts, ArchivingTaskConfig)
        self.assertEqual(archiving_opts.archive_path, self.tmp_path)
        self.assertEqual(archiving_opts.fetch_from_archive, True)
        self.assertEqual(archiving_opts.archived_after, expected_dt)

        sched_opts = t.scheduling_cfg
        self.assertIsInstance(sched_opts, SchedulingTaskConfig)
        self.assertEqual(sched_opts.delay, 10)
        self.assertEqual(sched_opts.max_retries, 5)

        self.assertEqual(initial_tasks, 0)
        self.assertEqual(after_tasks, 1)

    def test_task_unknown_scheduler_parameter(self):
        """Check whether an exception is thrown when an unknown parameter is in the sched params"""

        task_id = "arthur.task"
        backend = "git"
        category = None
        backend_params = {"a": "a", "b": "b"}
        sched_params = {
            "not_valid_param": 1,
            "delay": 10,
            "max_retries": 10
        }

        app = Arthur(self.conn, async_mode=False)

        with self.assertRaisesRegex(ValueError, re.compile(INVALID_SCHEDULER_PARAM)):
            app.add_task(task_id, backend, category, backend_params, sched_args=sched_params)

    def test_task_wrong_delay(self):
        """Check whether an exception is thrown when delay parameter is not properly set"""

        task_id = "arthur.task"
        backend = "git"
        category = None
        backend_params = {"a": "a", "b": "b"}
        sched_params = {"delay": "1", "max_retries": 10}

        app = Arthur(self.conn, async_mode=False)

        with self.assertRaisesRegex(ValueError, INVALID_SCHEDULER_DELAY) as ex:
            app.add_task(task_id, backend, category, backend_params, sched_args=sched_params)

    def test_task_wrong_max_retries(self):
        """Check whether an exception is thrown when max_retries parameter is not properly set"""

        task_id = "arthur.task"
        backend = "git"
        category = None
        backend_params = {"a": "a", "b": "b"}
        sched_params = {"delay": 1, "max_retries": "c"}

        app = Arthur(self.conn, async_mode=False)

        with self.assertRaisesRegex(ValueError, INVALID_SCHEDULER_MAX_RETRIES) as ex:
            app.add_task(task_id, backend, category, backend_params, sched_args=sched_params)

    def test_add_duplicated_task(self):
        """Check whether an exception is thrown when a duplicated task is added"""

        app = Arthur(self.conn, async_mode=False)

        app.add_task("arthur.task", "git", "commit", {"a": "a", "b": "b"})

        with self.assertRaises(AlreadyExistsError):
            app.add_task("arthur.task", "backend", "category", {"a": "a", "b": "b"})

    @unittest.mock.patch('redis.StrictRedis.set')
    def test_add_task_registry_error(self, mock_redis_set):
        """Check whether a TaskRegistryError exception is thrown when a task cannot be added"""

        mock_redis_set.side_effect = RedisError
        app = Arthur(self.conn, async_mode=False)

        with self.assertRaises(TaskRegistryError):
            app.add_task("arthur.task", "git", "commit", {"a": "a", "b": "b"})

    def test_add_task_no_task_id(self):
        """Check whether an exception is thrown when the task id is missing"""

        task_id = None
        backend = "git"
        category = "commit"
        backend_params = {"a": "a", "b": "b"}

        app = Arthur(self.conn, async_mode=False)

        with self.assertRaisesRegex(ValueError, MISSING_TASK_ID_ERROR):
            app.add_task(task_id, backend, category, backend_params)

        task_id = "     "
        with self.assertRaisesRegex(ValueError, MISSING_TASK_ID_ERROR):
            app.add_task(task_id, backend, category, backend_params)

    def test_add_task_no_backend(self):
        """Check whether an exception is thrown when the backend is missing"""

        task_id = "arthur.task"
        backend = None
        category = "commit"
        backend_params = {"a": "a", "b": "b"}

        app = Arthur(self.conn, async_mode=False)

        with self.assertRaisesRegex(ValueError, re.compile(MISSING_BACKEND_ERROR)):
            app.add_task(task_id, backend, category, backend_params)

        backend = "     "
        with self.assertRaisesRegex(ValueError, re.compile(MISSING_BACKEND_ERROR)):
            app.add_task(task_id, backend, category, backend_params)

    def test_add_task_no_category(self):
        """Check whether an exception is thrown when the backend category is missing"""

        task_id = "arthur.task"
        backend = "git"
        category = None
        backend_params = {"a": "a", "b": "b"}

        app = Arthur(self.conn, async_mode=False)

        with self.assertRaisesRegex(ValueError, re.compile(MISSING_CATEGORY_ERROR)):
            app.add_task(task_id, backend, category, backend_params)

        category = "     "
        with self.assertRaisesRegex(ValueError, re.compile(MISSING_CATEGORY_ERROR)):
            app.add_task(task_id, backend, category, backend_params)

    def test_add_task_invalid_format_backend_args(self):
        """Check whether an exception is thrown when the backend args is not a dict"""

        task_id = "arthur.task"
        backend = "git"
        category = "commit"
        backend_params = "wrong_params"

        app = Arthur(self.conn, async_mode=False)

        with self.assertRaisesRegex(ValueError, re.compile(INVALID_BACKEND_ARGS_ERROR)):
            app.add_task(task_id, backend, category, backend_params)

    def test_remove_task(self):
        """Check whether the removal of tasks is properly handled"""

        task_1 = "arthur.task-1"
        task_2 = "arthur.task-2"

        task_params = {"backend": "git",
                       "category": "commit",
                       "backend_params": {"a": "a", "b": "b"}}

        app = Arthur(self.conn, async_mode=False)

        app.add_task(task_1, task_params['backend'], task_params['category'], task_params['backend_params'])
        app.add_task(task_2, task_params['backend'], task_params['category'], task_params['backend_params'])
        tasks = len(app._tasks.tasks)

        self.assertEqual(tasks, 2)

        self.assertTrue(app.remove_task(task_1))
        self.assertTrue(app.remove_task(task_2))

        tasks = len(app._tasks.tasks)
        self.assertEqual(tasks, 0)

    def test_remove_non_existing_task(self):
        """Check whether the removal of non existing tasks is properly handled"""

        app = Arthur(self.conn, async_mode=False)
        self.assertFalse(app.remove_task("task-x"))

    def test_items(self):
        """Check whether items are properly retrieved"""

        new_path = os.path.join(self.tmp_path, 'newgit')

        app = Arthur(self.conn, async_mode=False)
        app.add_task('test', 'git', 'commit',
                     {'uri': self.git_path,
                      'gitpath': new_path})
        app.start()

        commits = [item['data']['commit'] for item in app.items()]

        expected = ['bc57a9209f096a130dcc5ba7089a8663f758a703',
                    '87783129c3f00d2c81a3a8e585eb86a47e39891a',
                    '7debcf8a2f57f86663809c58b5c07a398be7674c',
                    'c0d66f92a95e31c77be08dc9d0f11a16715d1885',
                    'c6ba8f7a1058db3e6b4bc6f1090e932b107605fb',
                    '589bb080f059834829a2a5955bebfd7c2baa110a',
                    'ce8e0b86a1e9877f42fe9453ede418519115f367',
                    '51a3b654f252210572297f47597b31527c475fb8',
                    '456a68ee1407a77f3e804a30dff245bb6c6b872f']

        self.assertListEqual(commits, expected)

        commits = [item for item in app.items()]

        self.assertListEqual(commits, [])

        shutil.rmtree(new_path)

    def test_reschedule(self):
        """Check whether it reschedules a failed task"""

        task_id = "arthur.task"
        backend = "git"
        category = "commit"
        backend_params = {"a": "a", "b": "b"}
        sched_params = {"max_retries": 5}

        app = Arthur(self.conn, async_mode=False)
        app.add_task(task_id, backend, category,
                     backend_params,
                     sched_args=sched_params)

        # Set task as failed
        t = app._tasks.tasks[0]
        t.status = TaskStatus.FAILED
        t.age = 100
        t.num_failures = 5
        app._tasks.update(task_id, t)

        # Task will be scheduled again after calling the method
        result = app.reschedule_task(task_id)

        # Update task values
        t = app._tasks.get(task_id)

        self.assertEqual(result, True)
        self.assertEqual(t.status, TaskStatus.SCHEDULED)
        self.assertEqual(t.age, 0)
        self.assertEqual(t.num_failures, 0)

    def test_reschedule_non_existing_task(self):
        """Check whether re-scheduling does nothing when the task does not exist"""

        app = Arthur(self.conn, async_mode=False)
        result = app.reschedule_task('no-id')

        self.assertEqual(result, False)
        self.assertEqual(len(app._tasks.tasks), 0)

    def test_reschedule_non_failed_task(self):
        """Check whether the method does nothing when re-scheduling a non failed task"""

        task_id = "arthur.task"
        backend = "git"
        category = "commit"
        backend_params = {"a": "a", "b": "b"}

        app = Arthur(self.conn, async_mode=False)
        app.add_task(task_id, backend, category, backend_params)

        # Force a different status
        t = app._tasks.tasks[0]
        t.status = TaskStatus.RUNNING
        app._tasks.update(task_id, t)

        result = app.reschedule_task('arthur.task')

        # The status should not have changed because
        # the task is a non failed one
        t = app._tasks.get(task_id)
        self.assertEqual(result, False)
        self.assertEqual(t.status, TaskStatus.RUNNING)


if __name__ == "__main__":
    unittest.main()
