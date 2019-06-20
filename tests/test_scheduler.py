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

import os.path
import unittest

from arthur.common import Q_CREATION_JOBS
from arthur.errors import NotFoundError
from arthur.tasks import (SchedulingTaskConfig,
                          TaskRegistry,
                          TaskStatus)
from arthur.scheduler import Scheduler

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
        job_id = schlr.schedule_task(task.task_id)
        self.assertEqual(task.status, TaskStatus.SCHEDULED)

        schlr.schedule()

        job = schlr._scheduler._queues[Q_CREATION_JOBS].fetch_job(job_id)
        result = job.return_value

        self.assertEqual(result.last_uuid, '1375b60d3c23ac9b81da92523e4144abc4489d4c')
        self.assertEqual(result.max_date, 1392185439.0)
        self.assertEqual(result.nitems, 9)

    def test_not_found_task(self):
        """Raises an error when the task to schedule does not exist"""

        registry = TaskRegistry()

        schlr = Scheduler(self.conn, registry, async_mode=False)
        self.assertRaises(NotFoundError, schlr.schedule_task, 'mytask')


if __name__ == "__main__":
    unittest.main()
