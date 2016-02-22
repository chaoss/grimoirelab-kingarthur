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

from rq import Queue

from .common import (Q_CREATION_JOBS,
                     Q_UPDATING_JOBS,
                     Q_STORAGE_ITEMS,
                     TIMEOUT)
from .errors import NotFoundError
from .jobs import execute_perceval_job


logger = logging.getLogger(__name__)


class Scheduler:
    """Scheduler of jobs.

    This class is able to schedule Perceval jobs. Jobs are added to
    two predefined queues: one for creation of repositories and
    one for updating those repositories.

    :param asyc_mode: set to run in asynchronous mode
    """
    def __init__(self, async_mode=True):
        self.queues = {
                       Q_CREATION_JOBS : Queue(Q_CREATION_JOBS, async=async_mode),
                       Q_UPDATING_JOBS : Queue(Q_UPDATING_JOBS, async=async_mode)
                      }

    def add_job(self, queue_id, repository):
        """Add a Perceval job to a queue.

        :param queue_id: identifier of the queue where the job will be added
        :param repository: input repository for the job

        :returns: the scheduled job

        :raises NotFoundError: when the queue set to run the job does not
            exist.
        """
        if queue_id not in self.queues:
            raise NotFoundError(element=queue_id)

        job = self.queues[queue_id].enqueue(execute_perceval_job,
                                            timeout=TIMEOUT,
                                            qitems=Q_STORAGE_ITEMS,
                                            origin=repository.origin,
                                            backend=repository.backend,
                                            **repository.kwargs)

        logging.debug("Job #%s %s (%s) enqueued in '%s' queue",
                      job.get_id(), repository.origin,
                      repository.backend, queue_id)
        return job
