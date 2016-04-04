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

import pickle
import sys
import unittest

import rq

if not '..' in sys.path:
    sys.path.insert(0, '..')

from arthur.errors import NotFoundError
from arthur.jobs import (execute_perceval_job,
                         execute_perceval_backend,
                         find_signature_parameters,
                         inspect_signature_parameters)

from tests import TestBaseRQ


class TestExecuteJob(TestBaseRQ):
    """Unit tests for execute_perceval_job"""

    def test_job(self):
        """Execute Git backend job"""

        args = {'uri' : 'http://example.com/',
                'gitpath' : 'data/git_log.txt'}

        q = rq.Queue('queue', async=False)
        job = q.enqueue(execute_perceval_job, qitems='items',
                        origin='test', backend='git', **args)

        self.assertEqual(job.return_value, 1344965413.0)

        commits = self.conn.lrange('items', 0, -1)
        commits = [pickle.loads(c) for c in commits]
        commits = [commit['data']['commit'] for commit in commits]

        expected = ['456a68ee1407a77f3e804a30dff245bb6c6b872f',
                    '51a3b654f252210572297f47597b31527c475fb8',
                    'ce8e0b86a1e9877f42fe9453ede418519115f367',
                    '589bb080f059834829a2a5955bebfd7c2baa110a',
                    'c6ba8f7a1058db3e6b4bc6f1090e932b107605fb',
                    'c0d66f92a95e31c77be08dc9d0f11a16715d1885',
                    '7debcf8a2f57f86663809c58b5c07a398be7674c',
                    '87783129c3f00d2c81a3a8e585eb86a47e39891a',
                    'bc57a9209f096a130dcc5ba7089a8663f758a703']

        self.assertListEqual(commits, expected)


class TestExecuteBackend(unittest.TestCase):
    """Unit tests for execute_perceval_backend"""

    def test_backend(self):
        """Execute Git backend"""

        args = {'uri' : 'http://example.com/',
                'gitpath' : 'data/git_log.txt'}

        commits = execute_perceval_backend('test', 'git', args)
        commits = [commit['data']['commit'] for commit in commits]

        expected = ['456a68ee1407a77f3e804a30dff245bb6c6b872f',
                    '51a3b654f252210572297f47597b31527c475fb8',
                    'ce8e0b86a1e9877f42fe9453ede418519115f367',
                    '589bb080f059834829a2a5955bebfd7c2baa110a',
                    'c6ba8f7a1058db3e6b4bc6f1090e932b107605fb',
                    'c0d66f92a95e31c77be08dc9d0f11a16715d1885',
                    '7debcf8a2f57f86663809c58b5c07a398be7674c',
                    '87783129c3f00d2c81a3a8e585eb86a47e39891a',
                    'bc57a9209f096a130dcc5ba7089a8663f758a703']

        self.assertListEqual(commits, expected)

    def test_not_found_backend(self):
        """Check if it fails when a backend is not found"""

        with self.assertRaises(NotFoundError):
            _ = [item for item in execute_perceval_backend('test', 'mock_backend', {})]
            self.assertEqual(e.exception.element, 'mock_backend')

    def test_not_found_parameters(self):
        """Check if it fails when a required backend parameter is not found"""

        with self.assertRaises(NotFoundError):
            _ = [item for item in execute_perceval_backend('test', 'git', {})]
            self.assertEqual(e.exception.element, 'gitlog')


class MockCallable:
    """Mock class for testing purposes"""

    def __init__(self, *args, **kwargs):
        pass

    def test(self, a, b, c=None):
        pass

    @classmethod
    def class_test(cls, a, b):
        pass


class TestFindSignature(unittest.TestCase):
    """Unit tests for find_signature_parameters"""

    def test_find_parameters(self):
        """Find a list of parameters"""

        expected = {'a' : 1, 'b' : 2, 'c' : 3}
        params = {'a' : 1, 'b' : 2, 'c' : 3}
        found = find_signature_parameters(params, MockCallable.test)
        self.assertDictEqual(found, expected)

        expected = {'a' : 1, 'b' : 2}
        params = {'a' : 1, 'b' : 2, 'd' : 3}
        found = find_signature_parameters(params, MockCallable.test)
        self.assertDictEqual(found, expected)

        with self.assertRaises(NotFoundError):
            params = {'a' : 1, 'd' : 3}
            found = find_signature_parameters(params, MockCallable.test)


class TestInspectSignature(unittest.TestCase):
    """Unit tests for inspect_signature_parameters"""

    def test_inspect(self):
        """Check the parameters from a callable"""

        expected = ['args', 'kwargs']
        params = inspect_signature_parameters(MockCallable)
        params = [p.name for p in params]
        self.assertListEqual(params, expected)

        expected = ['args', 'kwargs']
        params = inspect_signature_parameters(MockCallable.__init__)
        params = [p.name for p in params]
        self.assertListEqual(params, expected)

        expected = ['a', 'b', 'c']
        params = inspect_signature_parameters(MockCallable.test)
        params = [p.name for p in params]
        self.assertListEqual(params, expected)

        expected = ['a', 'b']
        params = inspect_signature_parameters(MockCallable.class_test)
        params = [p.name for p in params]
        self.assertListEqual(params, expected)


if __name__ == "__main__":
    unittest.main()
