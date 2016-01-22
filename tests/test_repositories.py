#!/usr/bin/env python3
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
#

import sys

if not '..' in sys.path:
    sys.path.insert(0, '..')

import unittest

from arthur.errors import NotFoundError
from arthur.repositories import Repository, RepositoryManager


class TestRepository(unittest.TestCase):
    """Unit tests for Repository"""

    def test_init(self):
        """Check arguments initialization"""

        args = {'from_date' : '1970-01-01',
                'component' : 'test'}
        repo = Repository('http://example.com/', 'mock_backend', **args)

        self.assertEqual(repo.origin, 'http://example.com/')
        self.assertEqual(repo.backend, 'mock_backend')
        self.assertDictEqual(repo.kwargs, args)

        repo = Repository('http://example.com/', 'mock_backend',
                          from_date='1970-01-01', component='test')

        self.assertEqual(repo.origin, 'http://example.com/')
        self.assertEqual(repo.backend, 'mock_backend')
        self.assertDictEqual(repo.kwargs, args)


class TestRepositoryManager(unittest.TestCase):
    """Unit tests for RepositoryManager"""

    def test_empty_registry(self):
        """Check empty registry on init"""

        manager = RepositoryManager()
        repos = manager.repositories

        self.assertListEqual(repos, [])

    def test_add_repository(self):
        """Add a repository"""

        args = {'from_date' : '1970-01-01',
                'component' : 'test'}

        manager = RepositoryManager()
        manager.add('http://example.com/', 'mock_backend', **args)

        repos = manager.repositories
        self.assertEqual(len(repos), 1)

        repo = repos[0]
        self.assertIsInstance(repo, Repository)
        self.assertEqual(repo.origin, 'http://example.com/')
        self.assertEqual(repo.backend, 'mock_backend')
        self.assertDictEqual(repo.kwargs, args)

    def test_update_repository(self):
        """Update a repository"""

        args = {'from_date' : '1970-01-01',
                'component' : 'test'}

        manager = RepositoryManager()
        manager.add('http://example.com/', 'mock_backend', **args)

        args['module'] = 'mock'
        manager.add('http://example.com/', 'test_backend', **args)

        repos = manager.repositories
        self.assertEqual(len(repos), 1)

        repo = repos[0]
        self.assertIsInstance(repo, Repository)
        self.assertEqual(repo.origin, 'http://example.com/')
        self.assertEqual(repo.backend, 'test_backend')
        self.assertDictEqual(repo.kwargs, args)

    def test_remove_repository(self):
        """Remove a repository"""

        args = {'from_date' : '1970-01-01',
                'component' : 'test'}

        manager = RepositoryManager()
        manager.add('http://example.com/', 'mock_backend', **args)
        manager.add('http://example.org/', 'to_remove')
        manager.add('http://example.net/', 'test_backend')

        repos = manager.repositories
        self.assertEqual(len(repos), 3)

        manager.remove('http://example.org/')

        repos = manager.repositories
        self.assertEqual(len(repos), 2)
        self.assertEqual(repos[0].origin, 'http://example.com/')
        self.assertEqual(repos[1].origin, 'http://example.net/')

    def test_remove_not_found(self):
        """Check whether it raises an exception when a repo is not found"""

        manager = RepositoryManager()

        with self.assertRaises(NotFoundError):
            manager.remove('http://example.com/')

        manager.add('http://example.org/', 'mock_backend')

        with self.assertRaises(NotFoundError):
            manager.remove('http://example.com/')

    def test_get_repository(self):
        """Get a repository"""

        args = {'from_date' : '1970-01-01',
                'component' : 'test'}

        manager = RepositoryManager()
        manager.add('http://example.com/', 'mock_backend', **args)
        manager.add('http://example.org/', 'to_remove')
        manager.add('http://example.net/', 'test_backend')

        repo = manager.get('http://example.net/')
        self.assertIsInstance(repo, Repository)
        self.assertEqual(repo.origin, 'http://example.net/')
        self.assertEqual(repo.backend, 'test_backend')

    def test_get_not_found(self):
        """Check whether it raises an exception when a repo is not found"""

        manager = RepositoryManager()

        with self.assertRaises(NotFoundError):
            manager.get('http://example.com/')

        manager.add('http://example.org/', 'mock_backend')

        with self.assertRaises(NotFoundError):
            manager.get('http://example.com/')


if __name__ == "__main__":
    unittest.main()
