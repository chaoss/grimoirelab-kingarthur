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

from grimoirelab.toolkit.datetime import str_to_datetime, InvalidDateError

from .common import ARCHIVES_DEFAULT_PATH, Q_STORAGE_ITEMS, MAX_JOB_RETRIES, WAIT_FOR_QUEUING
from .errors import AlreadyExistsError, NotFoundError
from .scheduler import Scheduler
from .tasks import TaskRegistry

logger = logging.getLogger(__name__)


class Arthur:
    """Main class to retrieve data from software repositories.

    :param conn: connection Object to Redis
    :param base_archive_path: path where the archive manager is located
    :param async_mode: run in async mode (with workers); set to `False`
        for debugging purposes
    """

    def __init__(self, conn, base_archive_path=None, async_mode=True):
        self.conn = conn
        self.conn.flushdb()
        rq.push_connection(self.conn)

        self.archive_path = base_archive_path
        self._tasks = TaskRegistry()
        self._scheduler = Scheduler(self.conn, self._tasks,
                                    async_mode=async_mode)

    def start(self):
        self._scheduler.schedule()

    def add_task(self, task_id, backend, category, backend_args,
                 archive_args=None, sched_args=None):
        """Add and schedule a task.

        :param task_id: id of the task
        :param backend: name of the backend
        :param category: category of the items to fecth
        :param backend_args: args needed to initialize the backend
        :param archive_args: args needed to initialize the archive
        :param sched_args: args needed to initialize the sceduler

        :returns: the task created
        """

        try:
            archive_args = self.__parse_archive_args(archive_args)
            sched_args = self.__parse_schedule_args(sched_args)
        except ValueError as e:
            raise e

        try:
            task = self._tasks.add(task_id, backend, category, backend_args,
                                   archive_args=archive_args,
                                   sched_args=sched_args)
        except AlreadyExistsError as e:
            raise e

        self._scheduler.schedule_task(task.task_id)

        return task

    def remove_task(self, task_id):
        """Remove and cancel a task.

        :param task_id: id of the task to be removed
        """

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

    def __parse_archive_args(self, archive_args):
        """Parse the archive arguments of a task"""

        if not archive_args:
            return {}

        if self.archive_path:
            archive_args['archive_path'] = self.archive_path
        else:
            archive_args['archive_path'] = os.path.expanduser(ARCHIVES_DEFAULT_PATH)

        if 'fetch_from_archive' not in archive_args:
            raise ValueError("archive_args.fetch_from_archive not defined")

        if archive_args['fetch_from_archive'] and 'archived_after' not in archive_args:
            raise ValueError("archive_args.archived_after not defined")

        for arg in archive_args.keys():
            if arg == 'archive_path':
                continue
            elif arg == 'fetch_from_archive':
                if type(archive_args['fetch_from_archive']) is not bool:
                    raise ValueError("archive_args.fetch_from_archive not boolean")
            elif arg == 'archived_after':
                if archive_args['fetch_from_archive']:
                    try:
                        archive_args['archived_after'] = str_to_datetime(archive_args['archived_after'])
                    except InvalidDateError:
                        raise ValueError("archive_args.archived_after datetime format not valid")
                else:
                    archive_args['archived_after'] = None
            else:
                raise ValueError("%s not accepted in archive_args" % arg)

        return archive_args

    def __parse_schedule_args(self, sched_args):
        """Parse the schedule arguments of a task"""

        if not sched_args:
            return {}

        if 'delay' not in sched_args:
            sched_args['delay'] = WAIT_FOR_QUEUING

        if 'max_retries_job' not in sched_args:
            sched_args['max_retries_job'] = MAX_JOB_RETRIES

        for arg in sched_args.keys():
            if arg == 'delay':
                if type(sched_args['delay']) is not int:
                    raise ValueError("sched_args.delay not int")
            elif arg == 'max_retries_job':
                if type(sched_args['max_retries_job']) is not int:
                    raise ValueError("sched_args.max_retries_job not int")
            else:
                raise ValueError("%s not accepted in schedule_args" % arg)

        return sched_args
