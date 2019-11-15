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
#     Miguel Ángel Fernández <mafesan@bitergia.com>
#

import datetime
import os.path
import unittest
import unittest.mock

from dateutil.tz import UTC
from redis.exceptions import RedisError

from perceval.backend import Summary

from arthur.common import Q_CREATION_JOBS, Q_RETRYING_JOBS
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

        registry = TaskRegistry(self.conn)
        task = registry.add('mytask', 'git', category, args,
                            archiving_cfg=archiving_opts,
                            scheduling_cfg=scheduler_opts)

        schlr = Scheduler(self.conn, registry, async_mode=False)
        schlr.schedule_task(task.task_id)

        task = registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.SCHEDULED)
        self.assertEqual(task.age, 0)

        schlr.schedule()

        task = registry.get('mytask')
        self.assertEqual(task.age, 1)

        job = schlr._scheduler._queues[Q_CREATION_JOBS].fetch_job(task.jobs[0].id)
        result = job.return_value

        self.assertEqual(result.task_id, task.task_id)
        self.assertEqual(result.job_number, 1)
        self.assertEqual(result.summary.last_uuid, '1375b60d3c23ac9b81da92523e4144abc4489d4c')
        self.assertEqual(result.summary.max_updated_on,
                         datetime.datetime(2014, 2, 12, 6, 10, 39, tzinfo=UTC))
        self.assertEqual(result.summary.total, 9)

    def test_schedule_task_with_reset(self):
        """Task should be added and executing reseting some of its counters"""

        args = {
            'uri': 'http://example.com/',
            'gitpath': os.path.join(self.dir, 'data/git_log.txt')
        }
        category = 'commit'
        archiving_opts = None
        scheduler_opts = SchedulingTaskConfig(delay=0, max_retries=0)

        registry = TaskRegistry(self.conn)
        task = registry.add('mytask', 'git', category, args,
                            archiving_cfg=archiving_opts,
                            scheduling_cfg=scheduler_opts)

        # Force values to some of the counters
        task.age = 100
        task.num_failures = 5
        registry.update('mytask', task)

        schlr = Scheduler(self.conn, registry, async_mode=False)
        schlr.schedule_task(task.task_id, reset=True)

        # Counters were reset
        task = registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.SCHEDULED)
        self.assertEqual(task.age, 0)
        self.assertEqual(task.num_failures, 0)

        schlr.schedule()

        task = registry.get('mytask')
        self.assertEqual(task.age, 1)

        job = schlr._scheduler._queues[Q_CREATION_JOBS].fetch_job(task.jobs[0].id)
        result = job.return_value

        self.assertEqual(result.task_id, task.task_id)
        self.assertEqual(result.job_number, 1)
        self.assertEqual(result.summary.last_uuid, '1375b60d3c23ac9b81da92523e4144abc4489d4c')
        self.assertEqual(result.summary.max_updated_on,
                         datetime.datetime(2014, 2, 12, 6, 10, 39, tzinfo=UTC))
        self.assertEqual(result.summary.total, 9)

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

        registry = TaskRegistry(self.conn)
        task = registry.add('mytask', 'git', category, args,
                            archiving_cfg=archiving_opts,
                            scheduling_cfg=scheduler_opts)

        schlr = Scheduler(self.conn, registry, async_mode=False)
        schlr.schedule_task(task.task_id)

        task = registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.SCHEDULED)
        self.assertEqual(task.age, 0)

        schlr.schedule()

        task = registry.get('mytask')
        self.assertEqual(task.age, 1)

        job = schlr._scheduler._queues['myqueue'].fetch_job(task.jobs[0].id)
        result = job.return_value

        self.assertEqual(result.task_id, task.task_id)
        self.assertEqual(result.job_number, 1)
        self.assertEqual(result.summary.last_uuid, '1375b60d3c23ac9b81da92523e4144abc4489d4c')
        self.assertEqual(result.summary.max_updated_on,
                         datetime.datetime(2014, 2, 12, 6, 10, 39, tzinfo=UTC))
        self.assertEqual(result.summary.total, 9)

    def test_schudule_task_retries_queue(self):
        """Failed task should be rescheduled to the retries queue"""

        args = {
            'uri': 'http://example.com/',
            'gitpath': os.path.join(self.dir, 'data/git_log.txt')
        }
        category = 'commit'
        archiving_opts = None
        scheduler_opts = SchedulingTaskConfig(delay=0, max_retries=0)

        registry = TaskRegistry(self.conn)
        task = registry.add('mytask', 'git', category, args,
                            archiving_cfg=archiving_opts,
                            scheduling_cfg=scheduler_opts)

        task.num_failures = 2  # Force number of failures before scheduling the task
        registry.update(task.task_id, task)

        schlr = Scheduler(self.conn, registry, async_mode=False)
        schlr.schedule_task(task.task_id)

        task = registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.SCHEDULED)
        self.assertEqual(task.age, 0)

        schlr.schedule()

        task = registry.get('mytask')
        self.assertEqual(task.age, 1)

        job = schlr._scheduler._queues[Q_RETRYING_JOBS].fetch_job(task.jobs[0].id)
        result = job.return_value

        self.assertEqual(result.task_id, task.task_id)
        self.assertEqual(result.job_number, 1)
        self.assertEqual(result.summary.last_uuid, '1375b60d3c23ac9b81da92523e4144abc4489d4c')
        self.assertEqual(result.summary.max_updated_on,
                         datetime.datetime(2014, 2, 12, 6, 10, 39, tzinfo=UTC))
        self.assertEqual(result.summary.total, 9)

    def test_set_job_number(self):
        """Check if the job number is set correctly when a new job is enqueued"""

        args = {
            'uri': 'http://example.com/',
            'gitpath': os.path.join(self.dir, 'data/git_log.txt')
        }
        category = 'commit'
        archiving_opts = None
        scheduler_opts = SchedulingTaskConfig(delay=0, max_retries=0,
                                              queue='myqueue')

        registry = TaskRegistry(self.conn)
        task = registry.add('mytask', 'git', category, args,
                            archiving_cfg=archiving_opts,
                            scheduling_cfg=scheduler_opts)

        schlr = Scheduler(self.conn, registry, async_mode=False)
        schlr.schedule_task(task.task_id)

        task = registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.SCHEDULED)
        self.assertEqual(task.age, 0)

        # Modify the list of jobs to pretend this is not the first
        # time running this task. Job number will be calculated
        # adding one to the length of jobs.
        task.jobs = ['A', 'B', 'C']
        registry.update('mytask', task)

        schlr = Scheduler(self.conn, registry, async_mode=False)
        schlr.schedule_task(task.task_id)

        task = registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.SCHEDULED)
        self.assertEqual(task.age, 0)

        schlr.schedule()

        task = registry.get('mytask')
        self.assertEqual(task.age, 1)

        # Get the last job run and check the result
        job = schlr._scheduler._queues['myqueue'].fetch_job(task.jobs[3].id)
        result = job.return_value

        self.assertEqual(result.task_id, task.task_id)
        self.assertEqual(result.job_number, 4)

    def test_not_found_task(self):
        """Raises an error when the task to schedule does not exist"""

        registry = TaskRegistry(self.conn)

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

        registry = TaskRegistry(self.conn)
        task = registry.add('mytask', 'git', category, args,
                            archiving_cfg=archiving_opts,
                            scheduling_cfg=scheduler_opts)

        schlr = Scheduler(self.conn, registry, async_mode=False)
        schlr.schedule_task(task.task_id)

        task = registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.SCHEDULED)
        self.assertEqual(task.age, 0)
        self.assertListEqual(task.jobs, [])

        schlr.schedule()

        task = registry.get('mytask')
        self.assertEqual(task.age, 1)
        self.assertEqual(len(task.jobs), 1)


class TestStartedJobHandler(TestBaseRQ):
    """Unit tests for StartedJobHandler"""

    def setUp(self):
        super().setUp()
        self.registry = TaskRegistry(self.conn)
        self.task_scheduler = _TaskScheduler(self.registry, self.conn, [])

    def test_initialization(self):
        """Check if the handler is correctly initialized"""

        handler = StartedJobHandler(self.task_scheduler)
        self.assertEqual(handler.task_scheduler, self.task_scheduler)

    def test_task_status(self):
        """Check if the handler changes the task status to running"""

        handler = StartedJobHandler(self.task_scheduler)

        _ = self.registry.add('mytask', 'git', 'commit', {})

        event = JobEvent(JobEventType.STARTED, 0, 'mytask', None)

        handled = handler(event)
        self.assertEqual(handled, True)

        task = self.registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.RUNNING)

    def test_ignore_orphan_event(self):
        """Check if an orphan event is ignored"""

        handler = StartedJobHandler(self.task_scheduler)

        event = JobEvent(JobEventType.STARTED, 0, 'mytask', None)

        handled = handler(event)
        self.assertEqual(handled, False)

    @unittest.mock.patch('redis.StrictRedis.get')
    def test_ignore_event_on_get_task_registry_error(self, mock_redis_get):
        """Check if an event is ignored when a TaskRegistryError is thrown"""

        mock_redis_get.side_effect = RedisError

        self.task_scheduler.registry.add('mytask', 'git', 'commit', {})

        handler = StartedJobHandler(self.task_scheduler)
        result = JobResult(0, 1, 'mytask', 'git', 'commit')
        event = JobEvent(JobEventType.STARTED, 0, 'mytask', result)

        handled = handler(event)
        self.assertEqual(handled, False)

    @unittest.mock.patch('redis.StrictRedis.exists')
    def test_ignore_event_on_update_task_registry_error(self, mock_redis_exists):
        """Check if an event is ignored when a TaskRegistryError is thrown"""

        mock_redis_exists.side_effect = [False, True, RedisError]

        self.task_scheduler.registry.add('mytask', 'git', 'commit', {})

        handler = StartedJobHandler(self.task_scheduler)
        result = JobResult(0, 1, 'mytask', 'git', 'commit')
        event = JobEvent(JobEventType.STARTED, 0, 'mytask', result)

        handled = handler(event)
        self.assertEqual(handled, False)


class TestCompletedJobHandler(TestBaseRQ):
    """Unit tests for CompletedJobHandler"""

    def setUp(self):
        super().setUp()
        self.registry = TaskRegistry(self.conn)
        self.task_scheduler = _TaskScheduler(self.registry, self.conn, [])

    def test_initialization(self):
        """Check if the handler is correctly initialized"""

        handler = CompletedJobHandler(self.task_scheduler)
        self.assertEqual(handler.task_scheduler, self.task_scheduler)

    def test_task_rescheduling(self):
        """Check if the task related to the event is re-scheduled"""

        handler = CompletedJobHandler(self.task_scheduler)

        _ = self.registry.add('mytask', 'git', 'commit', {})

        result = JobResult(0, 1, 'mytask', 'git', 'commit')

        summary = Summary()
        summary.fetched = 10
        summary.last_updated_on = datetime.datetime(2010, 1, 1, 1, 0, 0, tzinfo=UTC)
        summary.max_updated_on = datetime.datetime(2014, 2, 12, 6, 10, 39, tzinfo=UTC)
        result.summary = summary

        event = JobEvent(JobEventType.COMPLETED, 0, 'mytask', result)

        handled = handler(event)
        self.assertEqual(handled, True)

        task = self.registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.SCHEDULED)

    def test_task_rescheduling_age(self):
        """Check if the task is re-scheduled when maximum age is set"""

        handler = CompletedJobHandler(self.task_scheduler)

        scheduler_opts = SchedulingTaskConfig(delay=0, max_retries=0, max_age=3)
        task = self.registry.add('mytask', 'git', 'commit', {},
                                 scheduling_cfg=scheduler_opts)
        # Force the age to be lower than its limit
        task.age = 2

        result = JobResult(0, 1, 'mytask', 'git', 'commit')

        summary = Summary()
        summary.fetched = 10
        summary.last_updated_on = datetime.datetime(2010, 1, 1, 1, 0, 0, tzinfo=UTC)
        summary.max_updated_on = datetime.datetime(2014, 2, 12, 6, 10, 39, tzinfo=UTC)
        result.summary = summary

        event = JobEvent(JobEventType.COMPLETED, 0, 'mytask', result)

        handled = handler(event)
        self.assertEqual(handled, True)

        task = self.registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.SCHEDULED)

    def test_task_rescheduling_unlimited_age(self):
        """Check if the task is re-scheduled when unlimited age is set"""

        handler = CompletedJobHandler(self.task_scheduler)

        scheduler_opts = SchedulingTaskConfig(delay=0, max_retries=0, max_age=None)
        task = self.registry.add('mytask', 'git', 'commit', {},
                                 scheduling_cfg=scheduler_opts)
        # Force the age to be large value
        task.age = 1000000

        result = JobResult(0, 1, 'mytask', 'git', 'commit')

        summary = Summary()
        summary.fetched = 10
        summary.last_updated_on = datetime.datetime(2010, 1, 1, 1, 0, 0, tzinfo=UTC)
        summary.max_updated_on = datetime.datetime(2014, 2, 12, 6, 10, 39, tzinfo=UTC)
        result.summary = summary

        event = JobEvent(JobEventType.COMPLETED, 0, 'mytask', result)

        handled = handler(event)
        self.assertEqual(handled, True)

        task = self.registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.SCHEDULED)

    def test_task_not_rescheduled_age_limit(self):
        """Check if the task is not re-scheduled when age reaches its limit"""

        handler = CompletedJobHandler(self.task_scheduler)

        scheduler_opts = SchedulingTaskConfig(delay=0, max_retries=0, max_age=3)
        task = self.task_scheduler.registry.add('mytask', 'git', 'commit', {},
                                                scheduling_cfg=scheduler_opts)
        # Force the age to its pre-defined limit
        task.age = 3
        self.task_scheduler.registry.update('mytask', task)

        result = JobResult(0, 1, 'mytask', 'git', 'commit')

        summary = Summary()
        summary.fetched = 10
        summary.last_updated_on = datetime.datetime(2010, 1, 1, 1, 0, 0, tzinfo=UTC)
        summary.max_updated_on = datetime.datetime(2014, 2, 12, 6, 10, 39, tzinfo=UTC)
        result.summary = summary

        event = JobEvent(JobEventType.COMPLETED, 0, 'mytask', result)

        handled = handler(event)
        self.assertEqual(handled, True)

        task = self.task_scheduler.registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.COMPLETED)

    def test_task_not_rescheduled_archive_task(self):
        """Check if archive tasks are not rescheduled"""

        handler = CompletedJobHandler(self.task_scheduler)

        archiving_cfg = ArchivingTaskConfig('/tmp/archive', True)
        _ = self.task_scheduler.registry.add('mytask', 'git', 'commit', {}, archiving_cfg=archiving_cfg)

        result = JobResult(0, 1, 'mytask', 'git', 'commit')

        summary = Summary()
        summary.fetched = 10
        summary.last_updated_on = datetime.datetime(2010, 1, 1, 1, 0, 0, tzinfo=UTC)
        summary.max_updated_on = datetime.datetime(2014, 2, 12, 6, 10, 39, tzinfo=UTC)
        result.summary = summary

        event = JobEvent(JobEventType.COMPLETED, 0, 'mytask', result)

        handled = handler(event)
        self.assertEqual(handled, True)

        task = self.task_scheduler.registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.COMPLETED)

    def test_task_rescheduled_with_next_from_date(self):
        """Check if tasks are rescheduled updating next_from_date"""

        handler = CompletedJobHandler(self.task_scheduler)

        _ = self.task_scheduler.registry.add('mytask', 'git', 'commit', {})
        result = JobResult(0, 1, 'mytask', 'git', 'commit')

        summary = Summary()
        summary.fetched = 10
        summary.last_updated_on = datetime.datetime(2010, 1, 1, 1, 0, 0, tzinfo=UTC)
        summary.max_updated_on = datetime.datetime(2014, 2, 12, 6, 10, 39, tzinfo=UTC)
        result.summary = summary

        event = JobEvent(JobEventType.COMPLETED, 0, 'mytask', result)

        handled = handler(event)
        self.assertEqual(handled, True)

        task = self.task_scheduler.registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.SCHEDULED)

        # The field is updated to the max date received
        # within the result
        self.assertEqual(task.backend_args['next_from_date'],
                         datetime.datetime(2014, 2, 12, 6, 10, 39, tzinfo=UTC))

    def test_task_rescheduled_with_next_offset(self):
        """Check if tasks are rescheduled updating next_offset"""

        handler = CompletedJobHandler(self.task_scheduler)

        _ = self.task_scheduler.registry.add('mytask', 'git', 'commit', {})
        result = JobResult(0, 1, 'mytask', 'git', 'commit')

        summary = Summary()
        summary.fetched = 10
        summary.last_updated_on = datetime.datetime(2010, 1, 1, 1, 0, 0, tzinfo=UTC)
        summary.max_updated_on = datetime.datetime(2014, 2, 12, 6, 10, 39, tzinfo=UTC)
        summary.last_offset = 800
        summary.max_offset = 1000
        result.summary = summary

        event = JobEvent(JobEventType.COMPLETED, 0, 'mytask', result)

        handled = handler(event)
        self.assertEqual(handled, True)

        task = self.task_scheduler.registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.SCHEDULED)

        # Both fields are updated to the max value received
        # within the result
        self.assertEqual(task.backend_args['next_from_date'],
                         datetime.datetime(2014, 2, 12, 6, 10, 39, tzinfo=UTC))
        self.assertEqual(task.backend_args['next_offset'], 1000)

    def test_task_rescheduled_no_new_items(self):
        """Check if tasks are rescheduled when no items where generated before"""

        handler = CompletedJobHandler(self.task_scheduler)

        _ = self.task_scheduler.registry.add('mytask', 'git', 'commit', {})
        result = JobResult(0, 1, 'mytask', 'git', 'commit')

        summary = Summary()
        summary.fetched = 0
        summary.max_updated_on = None
        summary.max_offset = None
        result.summary = summary

        event = JobEvent(JobEventType.COMPLETED, 0, 'mytask', result)

        handled = handler(event)
        self.assertEqual(handled, True)

        task = self.task_scheduler.registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.SCHEDULED)

        # Both fields are not updated
        self.assertNotIn('next_from_date', task.backend_args)
        self.assertNotIn('next_offset', task.backend_args)

    def test_task_reset_num_failures(self):
        """Check if the number of failures is reset if the task is successful"""

        handler = CompletedJobHandler(self.task_scheduler)

        task = self.task_scheduler.registry.add('mytask', 'git', 'commit', {})

        # Force to a pre-defined number of failures
        task.num_failures = 2

        result = JobResult(0, 1, 'mytask', 'git', 'commit')

        summary = Summary()
        summary.fetched = 10
        summary.last_updated_on = datetime.datetime(2010, 1, 1, 1, 0, 0, tzinfo=UTC)
        summary.max_updated_on = datetime.datetime(2014, 2, 12, 6, 10, 39, tzinfo=UTC)
        result.summary = summary

        event = JobEvent(JobEventType.COMPLETED, 0, 'mytask', result)

        handled = handler(event)
        self.assertEqual(handled, True)

        task = self.task_scheduler.registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.SCHEDULED)
        self.assertEqual(task.num_failures, 0)

    def test_ignore_orphan_event(self):
        """Check if an orphan event is ignored"""

        handler = CompletedJobHandler(self.task_scheduler)

        result = JobResult(0, 1, 'mytask', 'git', 'commit')
        event = JobEvent(JobEventType.COMPLETED, 0, 'mytask', result)

        handled = handler(event)
        self.assertEqual(handled, False)

    @unittest.mock.patch('redis.StrictRedis.get')
    def test_ignore_event_on_task_registry_error(self, mock_redis_get):
        """Check if an event is ignored when a TaskRegistryError is thrown"""

        mock_redis_get.side_effect = RedisError

        self.task_scheduler.registry.add('mytask', 'git', 'commit', {})

        handler = CompletedJobHandler(self.task_scheduler)
        result = JobResult(0, 1, 'mytask', 'git', 'commit')
        event = JobEvent(JobEventType.COMPLETED, 0, 'mytask', result)

        handled = handler(event)
        self.assertEqual(handled, False)


class TestFailedJobHandler(TestBaseRQ):
    """Unit tests for CompletedJobHandler"""

    def setUp(self):
        super().setUp()
        self.registry = TaskRegistry(self.conn)
        self.task_scheduler = _TaskScheduler(self.registry, self.conn, [])

    def test_initialization(self):
        """Check if the handler is correctly initialized"""

        handler = FailedJobHandler(self.task_scheduler)
        self.assertEqual(handler.task_scheduler, self.task_scheduler)

    def test_handle_event(self):
        """Check if the event is handled correctly"""

        handler = FailedJobHandler(self.task_scheduler)

        _ = self.registry.add('mytask', 'git', 'commit', {})

        result = JobResult(0, 1, 'mytask', 'git', 'commit')

        summary = Summary()
        summary.fetched = 2
        summary.last_updated_on = datetime.datetime(2010, 1, 1, 1, 0, 0, tzinfo=UTC)
        summary.max_updated_on = datetime.datetime(2010, 1, 1, 1, 0, 0, tzinfo=UTC)
        result.summary = summary

        payload = {
            'error': "Error",
            'result': result
        }
        event = JobEvent(JobEventType.FAILURE, 0, 'mytask', payload)

        # It won't be scheduled because max_retries is not set
        handled = handler(event)
        self.assertEqual(handled, True)

        task = self.registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.FAILED)

    def test_task_not_rescheduled_not_resume(self):
        """Check if tasks unable to resume are not rescheduled"""

        handler = FailedJobHandler(self.task_scheduler)

        scheduler_opts = SchedulingTaskConfig(delay=0, max_retries=3)
        task = self.registry.add('mytask', 'gerrit', 'review', {},
                                 scheduling_cfg=scheduler_opts)

        result = JobResult(0, 1, 'mytask', 'gerrit', 'review')

        summary = Summary()
        summary.fetched = 2
        summary.last_updated_on = datetime.datetime(2010, 1, 1, 1, 0, 0, tzinfo=UTC)
        summary.max_updated_on = datetime.datetime(2010, 1, 1, 1, 0, 0, tzinfo=UTC)
        result.summary = summary

        payload = {
            'error': "Error",
            'result': result
        }
        event = JobEvent(JobEventType.FAILURE, 0, 'mytask', payload)

        handled = handler(event)
        self.assertEqual(handled, True)

        task = self.registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertEqual(task.num_failures, 1)

    def test_failed_task_not_rescheduled_max_retries(self):
        """Check if the task is not re-scheduled when num failures reaches its limit"""

        handler = FailedJobHandler(self.task_scheduler)

        scheduler_opts = SchedulingTaskConfig(delay=0, max_retries=3)
        task = self.task_scheduler.registry.add('mytask', 'git', 'commit', {}, scheduling_cfg=scheduler_opts)

        # Force to a pre-defined number of failures
        task.num_failures = 2
        self.task_scheduler.registry.update('mytask', task)

        result = JobResult(0, 1, 'mytask', 'git', 'commit')

        summary = Summary()
        summary.fetched = 2
        summary.last_updated_on = datetime.datetime(2010, 1, 1, 1, 0, 0, tzinfo=UTC)
        summary.max_updated_on = datetime.datetime(2010, 1, 1, 1, 0, 0, tzinfo=UTC)
        result.summary = summary

        payload = {
            'error': "Error",
            'result': result
        }
        event = JobEvent(JobEventType.FAILURE, 0, 'mytask', payload)

        handled = handler(event)
        self.assertEqual(handled, True)

        task = self.task_scheduler.registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.FAILED)
        self.assertEqual(task.num_failures, 3)

    def test_failed_task_rescheduled_with_next_from_date(self):
        """Check if failed tasks are rescheduled updating next_from_date"""

        handler = FailedJobHandler(self.task_scheduler)

        scheduler_opts = SchedulingTaskConfig(delay=0, max_retries=3)
        _ = self.task_scheduler.registry.add('mytask', 'git', 'commit', {}, scheduling_cfg=scheduler_opts)

        result = JobResult(0, 1, 'mytask', 'git', 'commit')

        summary = Summary()
        summary.fetched = 2
        summary.last_updated_on = datetime.datetime(2010, 1, 1, 1, 0, 0, tzinfo=UTC)
        summary.max_updated_on = datetime.datetime(2014, 2, 12, 6, 10, 39, tzinfo=UTC)
        result.summary = summary

        payload = {
            'error': "Error",
            'result': result
        }
        event = JobEvent(JobEventType.FAILURE, 0, 'mytask', payload)

        handled = handler(event)
        self.assertEqual(handled, True)

        task = self.task_scheduler.registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.SCHEDULED)

        # The field is updated to the max date received
        # within the result
        self.assertEqual(task.backend_args['next_from_date'],
                         datetime.datetime(2014, 2, 12, 6, 10, 39, tzinfo=UTC))
        self.assertEqual(task.num_failures, 1)

    def test_failed_task_rescheduled_with_next_offset(self):
        """Check if failed tasks are rescheduled updating next_offset"""

        handler = FailedJobHandler(self.task_scheduler)

        scheduler_opts = SchedulingTaskConfig(delay=0, max_retries=3)
        _ = self.registry.add('mytask', 'git', 'commit', {}, scheduling_cfg=scheduler_opts)

        result = JobResult(0, 1, 'mytask', 'git', 'commit')

        summary = Summary()
        summary.fetched = 2
        summary.last_updated_on = datetime.datetime(2010, 1, 1, 1, 0, 0, tzinfo=UTC)
        summary.max_updated_on = datetime.datetime(2014, 2, 12, 6, 10, 39, tzinfo=UTC)
        summary.last_offset = 800
        summary.max_offset = 1000
        result.summary = summary

        payload = {
            'error': "Error",
            'result': result
        }
        event = JobEvent(JobEventType.FAILURE, 0, 'mytask', payload)

        handled = handler(event)
        self.assertEqual(handled, True)

        task = self.registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.SCHEDULED)

        # Both fields are updated to the max value received
        # within the result
        self.assertEqual(task.backend_args['next_from_date'],
                         datetime.datetime(2014, 2, 12, 6, 10, 39, tzinfo=UTC))
        self.assertEqual(task.backend_args['next_offset'], 1000)
        self.assertEqual(task.num_failures, 1)

    def test_failed_task_rescheduled_no_new_items(self):
        """Check if tasks are rescheduled when no items where generated before"""

        handler = FailedJobHandler(self.task_scheduler)

        scheduler_opts = SchedulingTaskConfig(delay=0, max_retries=3)
        _ = self.task_scheduler.registry.add('mytask', 'git', 'commit', {}, scheduling_cfg=scheduler_opts)

        result = JobResult(0, 1, 'mytask', 'git', 'commit')

        summary = Summary()
        summary.fetched = 0
        summary.max_updated_on = None
        summary.max_offset = None
        result.summary = summary

        payload = {
            'error': "Error",
            'result': result
        }
        event = JobEvent(JobEventType.FAILURE, 0, 'mytask', payload)

        handled = handler(event)
        self.assertEqual(handled, True)

        task = self.task_scheduler.registry.get('mytask')
        self.assertEqual(task.status, TaskStatus.SCHEDULED)

        # Both fields are not updated
        self.assertNotIn('next_from_date', task.backend_args)
        self.assertNotIn('next_offset', task.backend_args)

    def test_ignore_orphan_event(self):
        """Check if an orphan event is ignored"""

        handler = FailedJobHandler(self.task_scheduler)

        result = JobResult(0, 1, 'mytask', 'git', 'commit')

        payload = {
            'error': "Error",
            'result': result
        }
        event = JobEvent(JobEventType.FAILURE, 0, 'mytask', payload)

        handled = handler(event)
        self.assertEqual(handled, False)


if __name__ == "__main__":
    unittest.main()
