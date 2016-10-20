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

import inspect
import logging

import rq
import pickle

import perceval.backends
import perceval.cache

from ._version import __version__
from .errors import NotFoundError


logger = logging.getLogger(__name__)


class JobResult:
    """Class to store the result of a Perceval job.

    It stores useful data such as the taks_id, the UUID of the last
    item generated or the number of items fetched by the backend.

    :param job_id: job identifier
    :param backend: backend used to fetch the items
    :param last_uuid: UUID of the last item
    :param max_date: maximum date fetched among items
    :param nitems: number of items fetched by the backend
    :param offset: maximum offset fetched among items
    """
    def __init__(self, job_id, backend, last_uuid,
                 max_date, nitems, offset=None):
        self.job_id = job_id
        self.backend = backend
        self.last_uuid = last_uuid
        self.max_date = max_date
        self.nitems = nitems
        self.offset = offset


def execute_perceval_job(backend, backend_args, qitems,
                         cache_path=None, fetch_from_cache=False):
    """Execute a Perceval job on RQ.

    The items fetched during the process will be stored in a
    Redis queue named `queue`.

    Setting the parameter `cache_path`, raw data will be stored
    in the cache. The contents from the cache can be retrieved
    setting the pameter `fetch_from_cache` to `True`, too. Take into
    account this behaviour will be only available when the backend
    supports the use of the cache.

    :param backend: backend to execute
    :param bakend_args: dict of arguments for running the backend
    :param qitems: name of the RQ queue used to store the items
    :param cache_path: path to the cache
    :param fetch_from_cache: retrieve items from the cache

    :returns: a `JobResult` instance

    :raises NotFoundError: raised when the backend is not found
    """
    job = rq.get_current_job()

    logger.debug("Running job %s (%s)", job.id, backend)

    conn = job.connection

    if cache_path:
        backup = not fetch_from_cache
        cache = __initialize_perceval_cache(cache_path, backup)
    else:
        cache = None

    backend_args['cache'] = cache

    nitems = 0
    last_uuid = None
    max_date = 0
    offset = None

    try:
        items = execute_perceval_backend(backend, backend_args,
                                         fetch_from_cache=fetch_from_cache)

        for item in items:
            item = add_arthur_metadata(item, job)
            conn.rpush(qitems, pickle.dumps(item))
            nitems += 1
            last_uuid = item['uuid']

            if max_date < item['updated_on']:
                max_date = item['updated_on']
            if 'offset' in item:
                offset = item['offset']
    except Exception as e:
        logger.debug("Error running job of task %s (%s) - %s",
                      task_id, backend, str(e))

        if cache and not cache_fetch:
            cache.recover()
        raise e

    if nitems > 0:
        result = JobResult(job.get_id(), backend,
                           last_uuid, max_date, nitems,
                           offset=offset)
    else:
        result = JobResult(job.get_id(), backend, None, None, 0)

    logger.debug("Job %s completed (%s) - %s items fetched",
                 result.job_id, result.backend, str(result.nitems))

    return result


def execute_perceval_backend(backend, backend_args, fetch_from_cache=False):
    """Execute a backend of Perceval.

    Run a backend of Perceval using the given backend. The backend
    parameters are also needed to run the process. Items will be tagged
    using the label assigned to `tag`.

    It will raise a `NotFoundError` in two cases: when the
    backend needed is not available or when any of the required
    parameters to run the backend are not found.

    :param backend: backend to execute
    :param bakend_args: arguments to execute the backend
    :param fetch_from_cache: retieve items from the cache

    :returns: iterator of items fetched by the backend

    :raises NotFoundError: raised when the backend is not found
    """
    if backend not in perceval.backends.PERCEVAL_BACKENDS:
        raise NotFoundError(element=backend)

    klass = perceval.backends.PERCEVAL_BACKENDS[backend]
    kinit = find_signature_parameters(backend_args, klass.__init__)
    obj = klass(**kinit)

    if not fetch_from_cache:
        fnc_fetch = obj.fetch
    else:
        fnc_fetch = obj.fetch_from_cache

    kfetch = find_signature_parameters(backend_args, fnc_fetch)

    for item in fnc_fetch(**kfetch):
        yield item


def find_signature_parameters(params, callable):
    """Find on a dict the parameters of a callable.

    Returns a dict with the parameters found on a callable. When
    any of the required parameters of a callable is not found,
    it raises a `NotFoundError` exception.
    """
    to_match = inspect_signature_parameters(callable)

    result = {}

    for p in to_match:
        name = p.name
        if name in params:
            result[name] =  params[name]
        elif p.default == inspect.Parameter.empty:
            # Parameters which its default value is empty are
            # considered as required
            raise NotFoundError(element=name)
    return result


def inspect_signature_parameters(callable):
    """Get the parameter of a callable.

    Parameters 'self' and 'cls' are filtered from the result.
    """
    signature = inspect.signature(callable)
    params = [v for p, v in signature.parameters.items() \
              if p not in ('self', 'cls')]
    return params


def add_arthur_metadata(item, job):
    """Add metadata to an item.

    Function that adds metadata to the given item such as the identifier
    of the job that generated it or the version of the system. The
    function returns a shallow copy of the item with the new data added.

    :param item: fetched item
    :param item: job object

    :returns: a shallow copy of the item with the metadata
    """
    new_item = item.copy()

    new_item['arthur_version'] = __version__
    new_item['job_id'] = job.get_id()

    return new_item


def __initialize_perceval_cache(dirpath, backup=False):
    """Initializes a cache object.

    The function initializes and returns a `Cache` handler which
    stores its data under `dirpath`. When `backup` is set, the
    object will keep a copy of the data for restoring.

    :param dirpath: path to the cache data
    :param backup: keep a copy of the cache data

    :returns: a cache object
    """
    if not dirpath:
        raise ValueError("dirpath requieres a value")

    logger.debug("Initializing cache on '%s' completed", dirpath)

    cache = perceval.cache.Cache(dirpath)

    if backup:
        cache.backup()
        logger.debug("Cache backup on '%s' completed", dirpath)

    logger.debug("Cache on '%s' initialized", dirpath)

    return cache
