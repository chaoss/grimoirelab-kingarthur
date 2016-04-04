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
import pickle

from rq import push_connection

from .common import Q_CREATION_JOBS, Q_STORAGE_ITEMS
from .repositories import RepositoryManager
from .scheduler import Scheduler


logger = logging.getLogger(__name__)


class Arthur:
    """Main class to retrieve data from software repositories."""

    def __init__(self, conn, async_mode=True):
        self.conn = conn
        self.conn.flushdb()
        push_connection(self.conn)

        self.repositories = RepositoryManager()
        self.scheduler = Scheduler(self.conn, async_mode=async_mode)
        self.scheduler.start()

    def add(self, origin, backend, args):
        """Add and schedule a repository."""

        self.repositories.add(origin, backend, **args)
        repository = self.repositories.get(origin)
        self.scheduler.add_job(Q_CREATION_JOBS, repository)

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
