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
import threading
import time

import cherrypy

from grimoirelab_toolkit.datetime import str_to_datetime

from .arthur import Arthur
from .utils import JSONEncoder


logger = logging.getLogger(__name__)


def json_encoder(*args, **kwargs):
    """Custom JSON encoder handler"""

    obj = cherrypy.serving.request._json_inner_handler(*args, **kwargs)

    for chunk in JSONEncoder().iterencode(obj):
        yield chunk.encode('utf-8')


class ArthurServer(Arthur):
    """Arthur REST server"""

    def __init__(self, *args, **kwargs):
        if 'writer' in kwargs:
            writer = kwargs.pop('writer')

        super().__init__(*args, **kwargs)

        if writer:
            self.writer_th = threading.Thread(target=self.write_items,
                                              args=(writer, self.items))
        else:
            self.writer_th = None

        cherrypy.engine.subscribe('start', self.start, 100)

    def start(self):
        """Start the server and the writer"""

        super().start()
        if self.writer_th:
            self.writer_th.start()

    @classmethod
    def write_items(cls, writer, items_generator):
        """Write items to the queue

        :param writer: the writer object
        :param items_generator: items to be written in the queue
        """
        while True:
            items = items_generator()
            writer.write(items)
            time.sleep(1)

    @cherrypy.expose
    @cherrypy.tools.json_in()
    def add(self):
        """Add tasks"""

        payload = cherrypy.request.json

        logger.debug("Reading tasks...")
        for task_data in payload['tasks']:
            try:
                category = task_data['category']
                backend_args = task_data['backend_args']
                archive_args = task_data.get('archive', None)
                sched_args = task_data.get('scheduler', None)
            except KeyError as ex:
                logger.error("Task badly formed")
                raise ex

            from_date = backend_args.get('from_date', None)

            if from_date:
                backend_args['from_date'] = str_to_datetime(from_date)

            super().add_task(task_data['task_id'],
                             task_data['backend'],
                             category,
                             backend_args,
                             archive_args=archive_args,
                             sched_args=sched_args)
        logger.debug("Done. Ready to work!")

        return "Tasks added"

    @cherrypy.expose
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out(handler=json_encoder)
    def remove(self):
        """Remove tasks"""

        payload = cherrypy.request.json
        logger.debug("Reading tasks to remove...")

        task_ids = {}

        for task_data in payload['tasks']:
            task_id = task_data['task_id']
            removed = super().remove_task(task_id)
            task_ids[task_id] = removed

        result = {'tasks': task_ids}

        return result

    @cherrypy.expose
    @cherrypy.tools.json_out(handler=json_encoder)
    def tasks(self):
        """List tasks"""

        logger.debug("API 'tasks' method called")

        result = [task.to_dict() for task in self._tasks.tasks]
        result = {'tasks': result}

        logger.debug("Tasks registry read")

        return result
