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
import json

import requests

from .errors import BaseError


logger = logging.getLogger(__name__)


class ElasticSearchError(BaseError):

    message = "%(cause)s"


class ElasticItemsWriter:

    def __init__(self,  idx_url, clean=False):
        self.idx_url = idx_url
        self.create_index(self.idx_url, clean=clean)

    def write(self, items, max_items=100):
        url = self.idx_url + '/items/_bulk'

        nitems = 0
        bulk_size = 0
        bulk = ''
        packages = []

        for item in items:
            if bulk_size >= max_items:
                packages.append((bulk, bulk_size))
                nitems += bulk_size
                bulk = ''
                bulk_size = 0

            item_json = '{"index" : {"_id" : "%s" } }\n' % item['uuid']
            item_json += json.dumps(item) + '\n'

            bulk += item_json
            bulk_size += 1

        # Add the last bulk to the packages
        if bulk_size > 0:
            packages.append((bulk, bulk_size))
            nitems += bulk_size

        if packages:
            logger.debug("Writting %i items to %s (%i packs of %i items max)",
                         nitems, url, len(packages), max_items)

        for bulk, nitems in packages:
            task_init = time.time()

            try:
                requests.put(url, data=bulk)
            except UnicodeEncodeError:
                # Related to body.encode('iso-8859-1'). mbox data
                logger.error("Encondig error ... converting bulk to iso-8859-1")
                bulk = bulk.encode('iso-8859-1','ignore')
                requests.put(url, data=bulk)

            logger.debug("Bulk package sent (%.2f sec prev, %i total)",
                         time.time() - task_init, nitems)

    @staticmethod
    def create_index(idx_url, clean=False):
        """Configure the index to work with"""

        try:
            r = requests.get(idx_url)
        except requests.exceptions.ConnectionError:
            cause = "Error connecting to Elastic Search (index: %s)" % idx_url
            raise ElasticSearchError(cause=cause)

        if r.status_code != 200:
            # The index does not exist
            r = requests.post(idx_url)

            if r.status_code != 200:
                logger.info("Can't create index %s (%s)", idx_url, r.status_code)
                cause = "Error creating Elastic Search index %s" % idx_url
                raise ElasticSearchError(cause=cause)

            logger.info("Index %s created", idx_url)
        elif r.status_code == 200 and clean:
            requests.delete(idx_url)
            requests.post(idx_url)
            logger.info("Index deleted and created" + idx_url)
