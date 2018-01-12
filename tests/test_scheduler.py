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

import copy
import logging
import os.path
import sys
import unittest

if '..' not in sys.path:
    sys.path.insert(0, '..')

from arthur.common import Q_CREATION_JOBS, CH_PUBSUB
from arthur.errors import NotFoundError
from arthur.tasks import TaskRegistry
from arthur.scheduler import Scheduler, logger
from arthur.worker import ArthurWorker

from tests import TestBaseRQ


def setup_scheduler(conn, dir, async_mode=False, add_task=0):
    args = {
        'uri': 'http://example.com/',
        'gitpath': os.path.join(dir, 'data/git_log.txt')
    }

    cache_args = {
        'cache_path': None,
        'fetch_from_cache': False
    }

    sched_args = {
        'delay': 1,
        'max_retries_job': 0
    }

    registry = TaskRegistry()

    if add_task > 0:
        for i in range(add_task):
            _ = registry.add("mytask" + str(i), 'git', args,
                             cache_args=cache_args,
                             sched_args=sched_args)

    return Scheduler(conn, registry, async_mode=async_mode)


class TestScheduler(TestBaseRQ):
    """Unit tests for Scheduler class"""

    def test_init(self):
        """Test whether attributes are initializated"""

        registry = TaskRegistry()
        schlr = Scheduler(self.conn, registry, async_mode=False)
        self.assertEqual(schlr.conn, self.conn)
        self.assertIsNotNone(schlr.registry)
        self.assertFalse(schlr.async_mode)
        self.assertIsNotNone(schlr._scheduler)
        self.assertIsNotNone(schlr._listener)

        schlr = Scheduler(self.conn, registry, async_mode=True)
        self.assertTrue(schlr.async_mode)

    def test_schedule(self):
        """Check that the schedule is started"""

        args = {
            'uri': 'http://example.com/',
            'gitpath': os.path.join(self.dir, 'data/git_log.txt')
        }

        cache_args = {
            'cache_path': None,
            'fetch_from_cache': False
        }

        sched_args = {
            'delay': 1,
            'max_retries_job': 0
        }

        registry = TaskRegistry()

        registry.add("mytask0", 'git', args,
                     cache_args=cache_args,
                     sched_args=sched_args)

        schlr = Scheduler(self.conn, registry, async_mode=True)
        schlr.schedule()

        self.assertTrue(schlr._listener.is_alive())
        self.assertTrue(schlr._scheduler.is_alive())
        self.assertTrue(schlr._scheduler.do_run)

        schlr.stop_listener()
        schlr.stop_scheduler()

        self.assertFalse(schlr._scheduler.do_run)
        self.assertFalse(schlr._listener.is_alive())
        self.assertFalse(schlr._scheduler.is_alive())

    def test_schedule_task(self):
        """Jobs should be added and executed"""

        args = {
            'uri': 'http://example.com/',
            'gitpath': os.path.join(self.dir, 'data/git_log.txt')
        }

        cache_args = {
            'cache_path': None,
            'fetch_from_cache': False
        }

        sched_args = {
            'delay': 1,
            'max_retries_job': 0
        }

        registry = TaskRegistry()

        registry.add("mytask0", 'git', args,
                     cache_args=cache_args,
                     sched_args=sched_args)

        schlr = Scheduler(self.conn, registry, async_mode=False)

        job_id = schlr.schedule_task("mytask0")

        schlr.schedule()

        job = schlr._scheduler._queues[Q_CREATION_JOBS].fetch_job(job_id)
        result = job.return_value

        self.assertEqual(result.last_uuid, '1375b60d3c23ac9b81da92523e4144abc4489d4c')
        self.assertEqual(result.max_date, 1392185439.0)
        self.assertEqual(result.nitems, 9)

    def test_not_found_task(self):
        """Raise an error when the task to schedule does not exist"""

        registry = TaskRegistry()
        schlr = Scheduler(self.conn, registry, async_mode=False)

        self.assertRaises(NotFoundError, schlr.schedule_task, 'mytask-not-found')

    def test_cancel_task(self):
        """Check whether tasks are deleted"""

        args = {
            'uri': 'http://example.com/',
            'gitpath': os.path.join(self.dir, 'data/git_log.txt')
        }

        cache_args = {
            'cache_path': None,
            'fetch_from_cache': False
        }

        sched_args = {
            'delay': 1,
            'max_retries_job': 0
        }

        registry = TaskRegistry()

        registry.add("mytask0", 'git', args,
                     cache_args=cache_args,
                     sched_args=sched_args)

        schlr = Scheduler(self.conn, registry, async_mode=False)

        self.assertEqual(len(schlr.registry.tasks), 1)

        schlr.cancel_task("mytask0")
        self.assertEqual(len(schlr.registry.tasks), 0)

    def test_process_jobs_async(self):
        """Check whether successuful jobs are propertly handled"""

        tasks = 5

        args = {
            'uri': 'http://example.com/',
            'gitpath': os.path.join(self.dir, 'data/git_log.txt')
        }

        cache_args = {
            'cache_path': None,
            'fetch_from_cache': False
        }

        sched_args = {
            'delay': 1,
            'max_retries_job': 0
        }

        registry = TaskRegistry()

        for i in range(tasks):
            registry.add("mytask" + str(i), 'git', args, cache_args=cache_args, sched_args=sched_args)

        schlr = Scheduler(self.conn, registry, async_mode=True)
        w = ArthurWorker(schlr._scheduler._queues[Q_CREATION_JOBS])

        job_ids = []
        for i in range(tasks):
            job_ids.append(schlr.schedule_task("mytask" + str(i)))

        schlr.schedule()

        while schlr._scheduler._jobs:
            self.assertNotEqual(len(schlr._scheduler._jobs), 0)

        self.assertEqual(len(schlr._scheduler._jobs.keys()), 0)

        schlr.stop_listener()
        schlr.stop_scheduler()

    def test_handle_successful_job(self):
        """Check whether an exception is thrown when a task id is not found"""

        logger.level = logging.INFO
        tmp_path = '/tmp/tmp-log.txt'
        f = open(tmp_path, 'w')

        stream_handler = logging.StreamHandler(f)
        logger.addHandler(stream_handler)

        args = {
            'uri': 'http://example.com/',
            'gitpath': os.path.join(self.dir, 'data/git_log.txt')
        }

        cache_args = {
            'cache_path': None,
            'fetch_from_cache': False
        }

        sched_args = {
            'delay': 1,
            'max_retries_job': 0
        }

        registry = TaskRegistry()

        registry.add("mytask0", 'git', args,
                     cache_args=cache_args,
                     sched_args=sched_args)

        schlr = Scheduler(self.conn, registry, async_mode=False)
        job_id = schlr.schedule_task("mytask0")

        schlr.schedule()

        job = schlr._scheduler._queues[Q_CREATION_JOBS].fetch_job(job_id)

        schlr._handle_successful_job(job)

        f.close()
        logger.removeHandler(stream_handler)

        with open(tmp_path, 'r') as f:
            content = f.read()
            self.assertTrue("(task: mytask0, old job: " + str(job_id) + ") re-scheduled" in content.lower())

    def test_handle_successful_job_not_found(self):
        """Check whether an exception is thrown when a task id is not found"""

        logger.level = logging.WARNING
        tmp_path = '/tmp/tmp-log.txt'
        f = open(tmp_path, 'w')

        stream_handler = logging.StreamHandler(f)
        logger.addHandler(stream_handler)

        args = {
            'uri': 'http://example.com/',
            'gitpath': os.path.join(self.dir, 'data/git_log.txt')
        }

        cache_args = {
            'cache_path': None,
            'fetch_from_cache': False
        }

        sched_args = {
            'delay': 1,
            'max_retries_job': 0
        }

        registry = TaskRegistry()

        registry.add("mytask0", 'git', args,
                     cache_args=cache_args,
                     sched_args=sched_args)

        schlr = Scheduler(self.conn, registry, async_mode=False)
        job_id = schlr.schedule_task("mytask0")

        schlr.schedule()

        job = schlr._scheduler._queues[Q_CREATION_JOBS].fetch_job(job_id)

        wrong_job = copy.deepcopy(job)
        wrong_job.kwargs = dict(job.kwargs)
        wrong_job.kwargs['task_id'] = "wrong_task"

        schlr._handle_successful_job(wrong_job)

        f.close()
        logger.removeHandler(stream_handler)

        with open(tmp_path, 'r') as f:
            content = f.read()
            self.assertTrue("task wrong_task not found; related job #" + job_id + " will not be rescheduled"
                            in content.lower())


if __name__ == "__main__":
    unittest.main()