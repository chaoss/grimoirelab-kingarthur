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

import rq

from .common import CH_PUBSUB


logger = logging.getLogger(__name__)


class ArthurWorker(rq.Worker):
    """Worker class for Arthur"""

    def perform_job(self, job):
        """Custom method to execute a job and notify of its result"""

        result = super().perform_job(job)

        data = {
                "job_id" : job.id,
                "status"  : job.get_status()
               }

        msg = pickle.dumps(data)
        self.connection.publish(CH_PUBSUB, msg)

        return result
