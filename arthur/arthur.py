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
#     Alvaro del Castillo San Felix <acs@bitergia.com>
#

import copy
import logging
import os
import pickle

import rq

from .common import CH_PUBSUB, ARCHIVES_DEFAULT_PATH, Q_STORAGE_ITEMS
from .errors import AlreadyExistsError, NotFoundError, TaskRegistryError
from .scheduler import Scheduler
from .tasks import (ArchivingTaskConfig,
                    SchedulingTaskConfig,
                    TaskRegistry,
                    TaskStatus)

logger = logging.getLogger(__name__)


class Arthur:
    """Main class to retrieve data from software repositories.

    :param conn: connection Object to Redis
    :param base_archive_path: path where the archive manager is located
    :param async_mode: run in async mode (with workers); set to `False`
        for debugging purposes
    """
    def __init__(self, conn, base_archive_path=None, async_mode=True, pubsub_channel=CH_PUBSUB):
        self.conn = conn
        self.conn.flushdb()
        rq.push_connection(self.conn)

        self.archive_path = base_archive_path
        self._tasks = TaskRegistry(self.conn)
        self._scheduler = Scheduler(self.conn, self._tasks,
                                    pubsub_channel=pubsub_channel,
                                    async_mode=async_mode)

    def start(self):
        self._scheduler.schedule()

    def add_task(self, task_id, backend, category, backend_args,
                 archive_args=None, sched_args=None):
        """Add and schedule a task.

        :param task_id: id of the task
        :param backend: name of the backend
        :param category: category of the items to fetch
        :param backend_args: args needed to initialize the backend
        :param archive_args: args needed to initialize the archive
        :param sched_args: scheduling args for this task

        :returns: the task created
        """
        try:
            archiving_cfg = self.__parse_archive_args(archive_args)
            scheduling_cfg = self.__parse_schedule_args(sched_args)
            self.__validate_args(task_id, backend, category, backend_args)
        except ValueError as e:
            raise e

        try:
            task = self._tasks.add(task_id, backend, category, backend_args,
                                   archiving_cfg=archiving_cfg,
                                   scheduling_cfg=scheduling_cfg)
        except AlreadyExistsError as e:
            raise e
        except TaskRegistryError as e:
            raise e

        self._scheduler.schedule_task(task.task_id)

        return task

    def remove_task(self, task_id):
        """Remove and cancel a task.

        :param task_id: id of the task to be removed
        """
        try:
            self._scheduler.cancel_task(task_id)
        except NotFoundError:
            logger.info("Cannot cancel %s task because it does not exist.",
                        task_id)
            return False
        else:
            return True

    def reschedule_task(self, task_id):
        """Re-schedule a failed task.

        :param task_id: id of the task
        """
        try:
            task = self._tasks.get(task_id)
        except NotFoundError as e:
            logger.info("Cannot re-schedule %s task because it does not exist.",
                        task_id)
            return False

        if task.status == TaskStatus.FAILED:
            self._scheduler.schedule_task(task_id, reset=True)
            return True
        else:
            logger.info("Cannot re-schedule task %s; only FAILED tasks can be rescheduled.",
                        task_id)
            return False

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

    @staticmethod
    def __validate_args(task_id, backend, category, backend_args):
        """Check that the task arguments received are valid"""

        if not task_id or task_id.strip() == "":
            msg = "Missing task_id for task"
            raise ValueError(msg)

        if not backend or backend.strip() == "":
            msg = "Missing backend for task '%s'" % task_id
            raise ValueError(msg)

        if backend_args and not isinstance(backend_args, dict):
            msg = "Backend_args is not a dict, task '%s'" % task_id
            raise ValueError(msg)

        if not category or category.strip() == "":
            msg = "Missing category for task '%s'" % task_id
            raise ValueError(msg)

    def __parse_archive_args(self, archive_args):
        """Parse the archive arguments of a task"""

        if not archive_args:
            return None

        archiving_args = copy.deepcopy(archive_args)

        if self.archive_path:
            archiving_args['archive_path'] = self.archive_path
        else:
            archiving_args['archive_path'] = os.path.expanduser(ARCHIVES_DEFAULT_PATH)

        return ArchivingTaskConfig.from_dict(archiving_args)

    def __parse_schedule_args(self, sched_args):
        """Parse the schedule arguments of a task"""

        if not sched_args:
            return None

        return SchedulingTaskConfig.from_dict(sched_args)
