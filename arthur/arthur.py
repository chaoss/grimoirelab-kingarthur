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
#     Alvaro del Castillo San Felix <acs@bitergia.com>
#

import logging
import os
import pickle

import rq

from .common import Q_STORAGE_ITEMS, MAX_JOB_RETRIES, WAIT_FOR_QUEUING
from .errors import AlreadyExistsError, NotFoundError
from .scheduler import Scheduler
from .tasks import TaskRegistry


logger = logging.getLogger(__name__)


class Arthur:
    """Main class to retrieve data from software repositories."""

    def __init__(self, conn, base_cache_path=None, async_mode=True):
        self.conn = conn
        self.conn.flushdb()
        rq.push_connection(self.conn)

        self.base_cache_path = base_cache_path
        self._tasks = TaskRegistry()
        self._scheduler = Scheduler(self.conn, self._tasks,
                                    async_mode=async_mode)

    def start(self):
        self._scheduler.schedule()

    def add_task(self, task_id, backend, backend_args,
                 cache_args=None, sched_args=None):
        """Add and schedule a task."""

        if not cache_args:
            cache_args = {}
        if not sched_args:
            sched_args = {}

        if self.base_cache_path and cache_args['cache']:
            cache_args['cache_path'] = os.path.join(self.base_cache_path, task_id)
        else:
            cache_args['cache_path'] = None
        if 'fetch_from_cache' not in cache_args:
            cache_args['fetch_from_cache'] = False
        if 'delay' not in sched_args:
            sched_args['delay'] = WAIT_FOR_QUEUING
        if 'max_retries_job' not in sched_args:
            sched_args['max_retries_job'] = MAX_JOB_RETRIES

        try:
            task = self._tasks.add(task_id, backend, backend_args,
                                   cache_args=cache_args,
                                   sched_args=sched_args)
        except AlreadyExistsError as e:
            raise e

        self._scheduler.schedule_task(task.task_id)

        return task

    def remove_task(self, task_id):
        """Remove and cancel a task."""

        try:
            self._scheduler.cancel_task(task_id)
        except NotFoundError as e:
            logger.info("Cannot cancel %s task because it does not exist.",
                        task_id)
            return False
        else:
            return True

    def items(self):
        """Get the items fetched by the jobs."""

        # Get and remove queued items in an atomic transaction
        pipe = self.conn.pipeline()
        pipe.lrange(Q_STORAGE_ITEMS, 0, -1)
        pipe.ltrim(Q_STORAGE_ITEMS, 1, 0)
        items = pipe.execute()[0]

        for item in items:
            item = pickle.loads(item)
            yield item
