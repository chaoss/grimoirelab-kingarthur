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

import os
import os.path
import shutil
import subprocess
import tempfile
import unittest

from arthur.arthur import Arthur
from arthur.common import ARCHIVES_DEFAULT_PATH
from arthur.errors import AlreadyExistsError

from base import find_empty_redis_database


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
        """Check when a task a no archive params"""

        task_id = "arthur.task"
        backend = "backend"
        category = None
        backend_params = {"a": "a", "b": "b"}

        app = Arthur(self.conn, async_mode=False)

        initial_tasks = len(app._tasks.tasks)
        app.add_task(task_id, backend, category, backend_params)
        after_tasks = len(app._tasks.tasks)

        t = app._tasks.tasks[0]

        self.assertEqual(t.task_id, task_id)
        self.assertEqual(t.backend, backend)
        self.assertDictEqual(t.backend_args, backend_params)
        self.assertDictEqual(t.archive_args, {})
        self.assertDictEqual(t.sched_args, {'max_retries': 3, 'delay': 10})

        self.assertEqual(initial_tasks, 0)
        self.assertEqual(after_tasks, 1)

    def test_add_task_empty_archive(self):
        """Check when a task has an empty archive"""

        task_id = "arthur.task"
        backend = "backend"
        category = None
        backend_params = {"a": "a", "b": "b"}
        archive_params = {}

        app = Arthur(self.conn, async_mode=False)

        initial_tasks = len(app._tasks.tasks)
        app.add_task(task_id, backend, category, backend_params, archive_params)
        after_tasks = len(app._tasks.tasks)

        t = app._tasks.tasks[0]

        self.assertEqual(t.task_id, task_id)
        self.assertEqual(t.backend, backend)
        self.assertDictEqual(t.backend_args, backend_params)
        self.assertDictEqual(t.archive_args, archive_params)
        self.assertDictEqual(t.sched_args, {'max_retries': 3, 'delay': 10})

        self.assertEqual(initial_tasks, 0)
        self.assertEqual(after_tasks, 1)

    def test_task_unknown_archive_parameter(self):
        """Check whether an exception is thrown when an unknown parameter is in the archive params"""

        task_id = "arthur.task"
        backend = "backend"
        category = None
        backend_params = {"a": "a", "b": "b"}
        archive_params = {"unknown": 1, "fetch_from_archive": True, "archived_after": "2010-10-10"}

        app = Arthur(self.conn, async_mode=False)

        with self.assertRaises(ValueError) as ex:
            app.add_task(task_id, backend, category, backend_params, archive_params)

        self.assertEqual(ex.exception.args[0], "unknown not accepted in archive_args")

    def test_task_default_archive_path(self):
        """Check whether a default archive path is added when not defined in the archive params"""

        task_id = "arthur.task"
        backend = "backend"
        category = None
        backend_params = {"a": "a", "b": "b"}
        archive_params = {"fetch_from_archive": True, "archived_after": "2010-10-10"}

        app = Arthur(self.conn, async_mode=False)
        app.add_task(task_id, backend, category, backend_params, archive_params)

        t = app._tasks.tasks[0]
        self.assertEqual(t.archive_args['fetch_from_archive'], archive_params["fetch_from_archive"])
        self.assertEqual(t.archive_args['archived_after'], archive_params["archived_after"])
        self.assertEqual(t.archive_args['archive_path'], os.path.expanduser(ARCHIVES_DEFAULT_PATH))

    def test_task_wrong_fetch_from_archive(self):
        """Check whether an exception is thrown when fetch_from_archive parameter is not properly set"""

        task_id = "arthur.task"
        backend = "backend"
        category = None
        backend_params = {"a": "a", "b": "b"}
        archive_params = {"fetch_from_archive": 100, "archived_after": "2010-10-10"}

        app = Arthur(self.conn, async_mode=False)

        with self.assertRaises(ValueError) as ex:
            app.add_task(task_id, backend, category, backend_params, archive_params)

        self.assertEqual(ex.exception.args[0], "archive_args.fetch_from_archive not boolean")

        archive_params = {"archived_after": "2010-10-10"}
        with self.assertRaises(ValueError) as ex:
            app.add_task(task_id, backend, category, backend_params, archive_params)

        self.assertEqual(ex.exception.args[0], "archive_args.fetch_from_archive not defined")

    def test_task_ignore_archive_after(self):
        """Check whether the archived_after parameter is not set when fetch_from_archive is false"""

        task_id = "arthur.task"
        backend = "backend"
        category = None
        backend_params = {"a": "a", "b": "b"}
        archive_params = {"fetch_from_archive": False, "archived_after": "X"}

        app = Arthur(self.conn, async_mode=False)
        app.add_task(task_id, backend, category, backend_params, archive_params)

        t = app._tasks.tasks[0]
        self.assertEqual(t.archive_args['fetch_from_archive'], False)
        self.assertIsNone(t.archive_args['archived_after'])

    def test_task_wrong_archive_after(self):
        """Check whether an exception is thrown when archived_after parameter is not properly set"""

        task_id = "arthur.task"
        backend = "backend"
        category = None
        backend_params = {"a": "a", "b": "b"}
        archive_params = {"fetch_from_archive": True, "archived_after": "X"}

        app = Arthur(self.conn, async_mode=False)

        with self.assertRaises(ValueError) as ex:
            app.add_task(task_id, backend, category, backend_params, archive_params)

        self.assertEqual(ex.exception.args[0], "archive_args.archived_after datetime format not valid")

        archive_params = {"fetch_from_archive": True}
        with self.assertRaises(ValueError) as ex:
            app.add_task(task_id, backend, category, backend_params, archive_params)

        self.assertEqual(ex.exception.args[0], "archive_args.archived_after not defined")

    def test_add_task_archive(self):
        """Check whether tasks are added"""

        task_id = "arthur.task"
        backend = "backend"
        category = 'acme-product'
        backend_params = {"a": "a", "b": "b"}
        archive_params = {'fetch_from_archive': True,
                          'archived_after': '2100-01-01',
                          'archive_path': './acme-plan'}
        sched_params = {"delay": 10, "max_retries": 10}

        app = Arthur(self.conn, base_archive_path=self.tmp_path, async_mode=False)

        initial_tasks = len(app._tasks.tasks)
        app.add_task(task_id, backend, category, backend_params, archive_params, sched_params)
        after_tasks = len(app._tasks.tasks)

        t = app._tasks.tasks[0]

        self.assertEqual(t.task_id, task_id)
        self.assertEqual(t.backend, backend)
        self.assertEqual(t.category, category)
        self.assertDictEqual(t.backend_args, backend_params)
        self.assertDictEqual(t.archive_args, archive_params)
        self.assertDictEqual(t.sched_args, sched_params)

        self.assertEqual(initial_tasks, 0)
        self.assertEqual(after_tasks, 1)

    def test_task_unknown_scheduler_parameter(self):
        """Check whether an exception is thrown when an unknown parameter is in the sched params"""

        task_id = "arthur.task"
        backend = "backend"
        category = None
        backend_params = {"a": "a", "b": "b"}
        sched_params = {"unknown": 1, "delay": 10, "max_retries": 10}

        app = Arthur(self.conn, async_mode=False)

        with self.assertRaises(ValueError) as ex:
            app.add_task(task_id, backend, category, backend_params, sched_args=sched_params)

        self.assertEqual(ex.exception.args[0], "unknown not accepted in schedule_args")

    def test_task_wrong_delay(self):
        """Check whether an exception is thrown when delay parameter is not properly set"""

        task_id = "arthur.task"
        backend = "backend"
        category = None
        backend_params = {"a": "a", "b": "b"}
        sched_params = {"delay": "1", "max_retries": 10}

        app = Arthur(self.conn, async_mode=False)

        with self.assertRaises(ValueError) as ex:
            app.add_task(task_id, backend, category, backend_params, sched_args=sched_params)

        self.assertEqual(ex.exception.args[0], "sched_args.delay not int")

    def test_task_wrong_max_retries(self):
        """Check whether an exception is thrown when max_retries parameter is not properly set"""

        task_id = "arthur.task"
        backend = "backend"
        category = None
        backend_params = {"a": "a", "b": "b"}
        sched_params = {"delay": 1, "max_retries": "c"}

        app = Arthur(self.conn, async_mode=False)

        with self.assertRaises(ValueError) as ex:
            app.add_task(task_id, backend, category, backend_params, sched_args=sched_params)

        self.assertEqual(ex.exception.args[0], "sched_args.max_retries not int")

    def test_add_duplicated_task(self):
        """Check whether an exception is thrown when a duplicated task is added"""

        app = Arthur(self.conn, async_mode=False)

        app.add_task("arthur.task", "backend", "category", {"a": "a", "b": "b"})

        with self.assertRaises(AlreadyExistsError):
            app.add_task("arthur.task", "backend", "category", {"a": "a", "b": "b"})

    def test_remove_task(self):
        """Check whether the removal of tasks is properly handled"""

        task_1 = "arthur.task-1"
        task_2 = "arthur.task-2"

        task_params = {"backend": "backend",
                       "category": "category",
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


if __name__ == "__main__":
    unittest.main()
