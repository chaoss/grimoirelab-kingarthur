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
import os.path
import unittest

from dateutil.tz import UTC

from arthur.common import Q_CREATION_JOBS
from arthur.errors import NotFoundError
from arthur.events import JobEventType, JobEvent
from arthur.jobs import JobResult
from arthur.scheduler import (_TaskScheduler,
                              CompletedJobHandler,
                              FailedJobHandler,
                              StartedJobHandler,
                              Scheduler)
from arthur.tasks import (ArchivingTaskConfig,
                          SchedulingTaskConfig,
                          TaskRegistry,
                          TaskStatus)

from base import TestBaseRQ


class TestScheduler(TestBaseRQ):
    """Unit tests for Scheduler class"""

    def test_schedule_task(self):
        """Jobs should be added and executed"""

        args = {
            'uri': 'http://example.com/',
            'gitpath': os.path.join(self.dir, 'data/git_log.txt')
        }
        category = 'commit'
        archiving_opts = None
        scheduler_opts = SchedulingTaskConfig(delay=0, max_retries=0)

        registry = TaskRegistry()
        task = registry.add('mytask', 'git', category, args,
                            archiving_cfg=archiving_opts,
                            scheduling_cfg=scheduler_opts)

        schlr = Scheduler(self.conn, registry, async_mode=False)
        schlr.schedule_task(task.task_id)
        self.assertEqual(task.status, TaskStatus.SCHEDULED)
        self.assertEqual(task.age, 0)

        schlr.schedule()

        self.assertEqual(task.age, 1)

        job = schlr._scheduler._queues[Q_CREATION_JOBS].fetch_job(task.jobs[0])
        result = job.return_value

        self.assertEqual(result.task_id, task.task_id)
        self.assertEqual(result.last_uuid, '1375b60d3c23ac9b81da92523e4144abc4489d4c')
        self.assertEqual(result.max_date, 1392185439.0)
        self.assertEqual(result.nitems, 9)

    def test_schedule_task_user_queue(self):
        """Task should be added and executed in the user's queue"""

        args = {
            'uri': 'http://example.com/',
            'gitpath': os.path.join(self.dir, 'data/git_log.txt')
        }
        category = 'commit'
        archiving_opts = None
        scheduler_opts = SchedulingTaskConfig(delay=0, max_retries=0,
                                              queue='myqueue')

        registry = TaskRegistry()
        task = registry.add('mytask', 'git', category, args,
                            archiving_cfg=archiving_opts,
                            scheduling_cfg=scheduler_opts)

        schlr = Scheduler(self.conn, registry, async_mode=False)
        schlr.schedule_task(task.task_id)
        self.assertEqual(task.status, TaskStatus.SCHEDULED)
        self.assertEqual(task.age, 0)

        schlr.schedule()

        self.assertEqual(task.age, 1)

        job = schlr._scheduler._queues['myqueue'].fetch_job(task.jobs[0])
        result = job.return_value

        self.assertEqual(result.task_id, task.task_id)
        self.assertEqual(result.last_uuid, '1375b60d3c23ac9b81da92523e4144abc4489d4c')
        self.assertEqual(result.max_date, 1392185439.0)
        self.assertEqual(result.nitems, 9)

    def test_not_found_task(self):
        """Raises an error when the task to schedule does not exist"""

        registry = TaskRegistry()

        schlr = Scheduler(self.conn, registry, async_mode=False)
        self.assertRaises(NotFoundError, schlr.schedule_task, 'mytask')

    def test_task_jobs_is_updated(self):
        """Test if the list of job ids for a task is updated"""

        args = {
            'uri': 'http://example.com/',
            'gitpath': os.path.join(self.dir, 'data/git_log.txt')
        }
        category = 'commit'
        archiving_opts = None
        scheduler_opts = SchedulingTaskConfig(delay=0, max_retries=0)

        registry = TaskRegistry()
        task = registry.add('mytask', 'git', category, args,
                            archiving_cfg=archiving_opts,
                            scheduling_cfg=scheduler_opts)

        schlr = Scheduler(self.conn, registry, async_mode=False)
        schlr.schedule_task(task.task_id)
        self.assertEqual(task.status, TaskStatus.SCHEDULED)
        self.assertEqual(task.age, 0)
        self.assertListEqual(task.jobs, [])

        schlr.schedule()

        self.assertEqual(task.age, 1)
        self.assertEqual(len(task.jobs), 1)


class TestStartedJobHandler(TestBaseRQ):
    """Unit tests for StartedJobHandler"""

    def setUp(self):
        super().setUp()
        self.registry = TaskRegistry()
        self.task_scheduler = _TaskScheduler(self.registry, self.conn, [])

    def test_initialization(self):
        """Check if the handler is correctly initialized"""

        handler = StartedJobHandler(self.task_scheduler)
        self.assertEqual(handler.task_scheduler, self.task_scheduler)

    def test_task_status(self):
        """Check if the handler changes the task status to running"""

        handler = StartedJobHandler(self.task_scheduler)

        task = self.registry.add('mytask', 'git', 'commit', {})

        event = JobEvent(JobEventType.STARTED, 0, 'mytask', None)

        handled = handler(event)
        self.assertEqual(handled, True)
        self.assertEqual(task.status, TaskStatus.RUNNING)

    def test_ignore_orphan_event(self):
        """Check if an orphan event is ignored"""

        handler = StartedJobHandler(self.task_scheduler)

        event = JobEvent(JobEventType.STARTED, 0, 'mytask', None)

        handled = handler(event)
        self.assertEqual(handled, False)


class TestCompletedJobHandler(TestBaseRQ):
    """Unit tests for CompletedJobHandler"""

    def setUp(self):
        super().setUp()
        self.registry = TaskRegistry()
        self.task_scheduler = _TaskScheduler(self.registry, self.conn, [])

    def test_initialization(self):
        """Check if the handler is correctly initialized"""

        handler = CompletedJobHandler(self.task_scheduler)
        self.assertEqual(handler.task_scheduler, self.task_scheduler)

    def test_task_rescheduling(self):
        """Check if the task related to the event is re-scheduled"""

        handler = CompletedJobHandler(self.task_scheduler)

        task = self.registry.add('mytask', 'git', 'commit', {})
        result = JobResult(0, 'mytask', 'git', 'commit',
                           'FFFFFFFF', 1392185439.0, 9)
        event = JobEvent(JobEventType.COMPLETED, 0, 'mytask', result)

        handled = handler(event)
        self.assertEqual(handled, True)
        self.assertEqual(task.status, TaskStatus.SCHEDULED)

    def test_task_rescheduling_age(self):
        """Check if the task is re-scheduled when maximum age is set"""

        handler = CompletedJobHandler(self.task_scheduler)

        scheduler_opts = SchedulingTaskConfig(delay=0, max_retries=0, max_age=3)
        task = self.registry.add('mytask', 'git', 'commit', {},
                                 scheduling_cfg=scheduler_opts)
        # Force the age to be lower than its limit
        task.age = 2

        result = JobResult(0, 'mytask', 'git', 'commit',
                           'FFFFFFFF', 1392185439.0, 9)
        event = JobEvent(JobEventType.COMPLETED, 0, 'mytask', result)

        handled = handler(event)
        self.assertEqual(handled, True)
        self.assertEqual(task.status, TaskStatus.SCHEDULED)

    def test_task_rescheduling_unlimited_age(self):
        """Check if the task is re-scheduled when unlimited age is set"""

        handler = CompletedJobHandler(self.task_scheduler)

        scheduler_opts = SchedulingTaskConfig(delay=0, max_retries=0, max_age=None)
        task = self.registry.add('mytask', 'git', 'commit', {},
                                 scheduling_cfg=scheduler_opts)
        # Force the age to be large value
        task.age = 1000000

        result = JobResult(0, 'mytask', 'git', 'commit',
                           'FFFFFFFF', 1392185439.0, 9)
        event = JobEvent(JobEventType.COMPLETED, 0, 'mytask', result)

        handled = handler(event)
        self.assertEqual(handled, True)
        self.assertEqual(task.status, TaskStatus.SCHEDULED)

    def test_task_not_rescheduled_age_limit(self):
        """Check if the task is not re-scheduled when age reaches its limit"""

        handler = CompletedJobHandler(self.task_scheduler)

        scheduler_opts = SchedulingTaskConfig(delay=0, max_retries=0, max_age=3)
        task = self.registry.add('mytask', 'git', 'commit', {},
                                 scheduling_cfg=scheduler_opts)
        # Force the age to its pre-defined limit
        task.age = 3

        result = JobResult(0, 'mytask', 'git', 'commit',
                           'FFFFFFFF', 1392185439.0, 9)
        event = JobEvent(JobEventType.COMPLETED, 0, 'mytask', result)

        handled = handler(event)
        self.assertEqual(handled, True)
        self.assertEqual(task.status, TaskStatus.COMPLETED)

    def test_task_not_rescheduled_archive_task(self):
        """Check if archive tasks are not rescheduled"""

        handler = CompletedJobHandler(self.task_scheduler)

        archiving_cfg = ArchivingTaskConfig('/tmp/archive', True)
        task = self.registry.add('mytask', 'git', 'commit', {},
                                 archiving_cfg=archiving_cfg)
        result = JobResult(0, 'mytask', 'git', 'commit',
                           'FFFFFFFF', 1392185439.0, 9)
        event = JobEvent(JobEventType.COMPLETED, 0, 'mytask', result)

        handled = handler(event)
        self.assertEqual(handled, True)
        self.assertEqual(task.status, TaskStatus.COMPLETED)

    def test_task_rescheduled_with_next_from_date(self):
        """Check if tasks are rescheduled updating next_from_date"""

        handler = CompletedJobHandler(self.task_scheduler)

        task = self.registry.add('mytask', 'git', 'commit', {})
        result = JobResult(0, 'mytask', 'git', 'commit',
                           'FFFFFFFF', 1392185439.0, 9)
        event = JobEvent(JobEventType.COMPLETED, 0, 'mytask', result)

        handled = handler(event)
        self.assertEqual(handled, True)
        self.assertEqual(task.status, TaskStatus.SCHEDULED)

        # The field is updated to the last date received
        # within the result
        self.assertEqual(task.backend_args['next_from_date'],
                         datetime.datetime(2014, 2, 12, 6, 10, 39, tzinfo=UTC))

    def test_task_rescheduled_with_next_offset(self):
        """Check if tasks are rescheduled updating next_offset"""

        handler = CompletedJobHandler(self.task_scheduler)

        task = self.registry.add('mytask', 'git', 'commit', {})
        result = JobResult(0, 'mytask', 'git', 'commit',
                           'FFFFFFFF', 1392185439.0, 9,
                           offset=1000)
        event = JobEvent(JobEventType.COMPLETED, 0, 'mytask', result)

        handled = handler(event)
        self.assertEqual(handled, True)
        self.assertEqual(task.status, TaskStatus.SCHEDULED)

        # Both fields are updated to the last value received
        # within the result
        self.assertEqual(task.backend_args['next_from_date'],
                         datetime.datetime(2014, 2, 12, 6, 10, 39, tzinfo=UTC))
        self.assertEqual(task.backend_args['next_offset'], 1000)

    def test_ignore_orphan_event(self):
        """Check if an orphan event is ignored"""

        handler = CompletedJobHandler(self.task_scheduler)

        result = JobResult(0, 'mytask', 'git', 'commit',
                           'FFFFFFFF', 1392185439.0, 9)
        event = JobEvent(JobEventType.COMPLETED, 0, 'mytask', result)

        handled = handler(event)
        self.assertEqual(handled, False)


class TestFailedJobHandler(TestBaseRQ):
    """Unit tests for CompletedJobHandler"""

    def setUp(self):
        super().setUp()
        self.registry = TaskRegistry()
        self.task_scheduler = _TaskScheduler(self.registry, self.conn, [])

    def test_initialization(self):
        """Check if the handler is correctly initialized"""

        handler = FailedJobHandler(self.task_scheduler)
        self.assertEqual(handler.task_scheduler, self.task_scheduler)

    def test_handle_event(self):
        """Check if the event is handled correctly"""

        handler = FailedJobHandler(self.task_scheduler)

        task = self.registry.add('mytask', 'git', 'commit', {})

        payload = {
            'error': "Error"
        }
        event = JobEvent(JobEventType.FAILURE, 0, 'mytask', payload)

        handled = handler(event)
        self.assertEqual(handled, True)
        self.assertEqual(task.status, TaskStatus.FAILED)

    def test_ignore_orphan_event(self):
        """Check if an orphan event is ignored"""

        handler = FailedJobHandler(self.task_scheduler)

        payload = {
            'error': "Error"
        }
        event = JobEvent(JobEventType.FAILURE, 0, 'mytask', payload)

        handled = handler(event)
        self.assertEqual(handled, False)


if __name__ == "__main__":
    unittest.main()
