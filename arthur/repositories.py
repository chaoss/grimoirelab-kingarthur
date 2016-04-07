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

from .errors import NotFoundError


logger = logging.getLogger(__name__)


class Repository:
    """Basic class to store information about a repository.

    This class stores the basic information needed to retrieve data
    from a repository. The parameters needed to run the backend
    are given in `kwargs` as keywords arguments.

    :param origin: repository identifier
    :param backend: backend used to fetch data from the repository
    :param cache_path: path to store the cache
    :param kwargs: keyword arguments required to run the backend
    """
    def __init__(self, origin, backend, cache_path, **kwargs):
        self.origin = origin
        self.backend = backend
        self.cache_path = cache_path
        self.kwargs = kwargs


class RepositoryManager:
    """Basic structure to manage repositories.

    Repositories are stored using instances of `Repository` class. Each
    repository is added using a unique identifier. Following accesses to
    the repository (i.e, to get or to remove) will require of this
    identifier.
    """
    def __init__(self):
        self._repositories = {}

    def add(self, origin, backend, cache_path=None, **kwargs):
        """Add or update a repository.

        This method adds or updates a repository using `origin` as
        identifier.

        :param origin: repository identifier to add/update
        :param backend: backend used to fetch data from the repository
        :param cache_path: path to store the cache
        :param kwargs: keyword arguments required to run the backend
        """
        repo = Repository(origin, backend, cache_path, **kwargs)
        self._repositories[origin] = repo
        logger.debug("%s repository added", str(origin))

    def remove(self, origin):
        """Remove a repository from the registry.

        To remove it, pass its identifier with `origin` parameter.
        When the identifier is not found, a `NotFoundError` exception
        is raised.

        :param origin: repository identifier to remove

        :raises NotFoundError: raised when the given repository identifier
            is not found on the registry
        """
        try:
            del self._repositories[origin]
            logger.debug("%s repository removed", str(origin))
        except KeyError:
            raise NotFoundError(element=str(origin))

    def get(self, origin):
        """Get a repository.

        :param origin: repository identifier

        :returns: a repository object

        :raises NotFoundError: raised when the requested repository identifier
            is not found on the registry
        """
        try:
            return self._repositories[origin]
        except KeyError:
            raise NotFoundError(element=str(origin))

    @property
    def repositories(self):
        """Get a list of repositories"""

        repos = [v for v in self._repositories.values()]
        repos.sort(key=lambda x: x.origin)
        return repos
