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

import cherrypy

from .arthur import Arthur
from .utils import str_to_datetime


logger = logging.getLogger(__name__)


class ArthurServer(Arthur):
    """Arthur REST server"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @cherrypy.expose
    @cherrypy.tools.json_in()
    def add(self):
        payload = cherrypy.request.json

        logger.debug("Reading repositories...")
        for repo in payload['repositories']:
            from_date = repo['args'].get('from_date', None)
            if from_date:
                repo['args']['from_date'] = str_to_datetime(from_date)

            super().add(repo['origin'], repo['backend'], repo['args'])
        logger.debug("Done. Ready to work!")

        return "Repositories added"
