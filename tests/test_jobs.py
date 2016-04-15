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

import datetime
import pickle
import shutil
import sys
import tempfile
import unittest

import httpretty
import rq

if not '..' in sys.path:
    sys.path.insert(0, '..')

from arthur.errors import NotFoundError
from arthur.jobs import (JobResult,
                         execute_perceval_job,
                         execute_perceval_backend,
                         find_signature_parameters,
                         inspect_signature_parameters)

from tests import TestBaseRQ


BUGZILLA_SERVER_URL = 'http://example.com'
BUGZILLA_BUGLIST_URL = BUGZILLA_SERVER_URL + '/buglist.cgi'
BUGZILLA_BUG_URL = BUGZILLA_SERVER_URL + '/show_bug.cgi'
BUGZILLA_BUG_ACTIVITY_URL = BUGZILLA_SERVER_URL + '/show_activity.cgi'


def read_file(filename, mode='r'):
    with open(filename, mode) as f:
        content = f.read()
    return content


class TestJobResult(unittest.TestCase):
    """Unit tests for JobResult class"""

    def test_job_result_init(self):
        result = JobResult('http://example.com/', 'mock_backend',
                           'ABCDEFGHIJK', 1344965413.0, 58)

        self.assertEqual(result.origin, 'http://example.com/')
        self.assertEqual(result.backend, 'mock_backend')
        self.assertEqual(result.last_uuid, 'ABCDEFGHIJK')
        self.assertEqual(result.max_date, 1344965413.0)
        self.assertEqual(result.nitems, 58)


class TestExecuteJob(TestBaseRQ):
    """Unit tests for execute_perceval_job"""

    def setUp(self):
        self.tmp_path = tempfile.mkdtemp(prefix='arthur_')
        super().setUp()

    def tearDown(self):
        shutil.rmtree(self.tmp_path)
        super().tearDown()

    def test_job(self):
        """Execute Git backend job"""

        args = {'uri' : 'http://example.com/',
                'gitpath' : 'data/git_log.txt'}

        q = rq.Queue('queue', async=False)
        job = q.enqueue(execute_perceval_job, qitems='items',
                        origin='test', backend='git', **args)

        result = job.return_value
        self.assertEqual(result.origin, 'test')
        self.assertEqual(result.backend, 'git')
        self.assertEqual(result.last_uuid, '1375b60d3c23ac9b81da92523e4144abc4489d4c')
        self.assertEqual(result.max_date, 1392185439.0)
        self.assertEqual(result.nitems, 9)

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

    def test_job_no_result(self):
        """Execute a Git backend job that will not produce any results"""

        args = {'uri' : 'http://example.com/',
                'gitpath' : 'data/git_log_empty.txt',
                'from_date' : datetime.datetime(2020, 1, 1, 1, 1, 1)}

        q = rq.Queue('queue', async=False)
        job = q.enqueue(execute_perceval_job, qitems='items',
                        origin='test', backend='git', **args)

        result = job.return_value
        self.assertEqual(result.origin, 'test')
        self.assertEqual(result.backend, 'git')
        self.assertEqual(result.last_uuid, None)
        self.assertEqual(result.max_date, None)
        self.assertEqual(result.nitems, 0)

        commits = self.conn.lrange('items', 0, -1)
        commits = [pickle.loads(c) for c in commits]
        self.assertListEqual(commits, [])

    @httpretty.activate
    def test_job_cache(self):
        """Execute a Bugzilla backend job to fetch data from the cache"""

        requests = []
        bodies_csv = [read_file('data/bugzilla_buglist.csv'),
                      read_file('data/bugzilla_buglist_next.csv'),
                      ""]
        bodies_xml = [read_file('data/bugzilla_version.xml', mode='rb'),
                      read_file('data/bugzilla_bugs_details.xml', mode='rb'),
                      read_file('data/bugzilla_bugs_details_next.xml', mode='rb')]
        bodies_html = [read_file('data/bugzilla_bug_activity.html', mode='rb'),
                       read_file('data/bugzilla_bug_activity_empty.html', mode='rb')]

        def request_callback(method, uri, headers):
            if uri.startswith(BUGZILLA_BUGLIST_URL):
                body = bodies_csv.pop(0)
            elif uri.startswith(BUGZILLA_BUG_URL):
                body = bodies_xml.pop(0)
            else:
                body = bodies_html[len(requests) % 2]

            requests.append(httpretty.last_request())

            return (200, headers, body)

        httpretty.register_uri(httpretty.GET,
                               BUGZILLA_BUGLIST_URL,
                               responses=[
                                    httpretty.Response(body=request_callback) \
                                    for _ in range(3)
                               ])
        httpretty.register_uri(httpretty.GET,
                               BUGZILLA_BUG_URL,
                               responses=[
                                    httpretty.Response(body=request_callback) \
                                    for _ in range(3)
                               ])
        httpretty.register_uri(httpretty.GET,
                               BUGZILLA_BUG_ACTIVITY_URL,
                               responses=[
                                    httpretty.Response(body=request_callback) \
                                    for _ in range(7)
                               ])

        expected = ['5a8a1e25dfda86b961b4146050883cbfc928f8ec',
                    '1fd4514e56f25a39ffd78eab19e77cfe4dfb7769',
                    '6a4cb244067c3cfa38f9f563e2ab3cd8ac21762f',
                    '7e033ed0110032ead6b918be43c1f3f88cd98fd7',
                    'f90d12b380ffdb47f2b6e96b321f08000181a9d6',
                    '4b166308f205121bc57704032acdc81b6c9bb8b1',
                    'b4009442d38f4241a4e22e3e61b7cd8ef5ced35c']

        q = rq.Queue('queue', async=False)

        # First, we fetch the bugs from the server, storing them
        # in a cache
        args = {'url' : BUGZILLA_SERVER_URL,
                'max_bugs' : 5}

        job = q.enqueue(execute_perceval_job, qitems='items',
                        origin=BUGZILLA_SERVER_URL, backend='bugzilla',
                        cache_path=self.tmp_path, **args)

        bugs = self.conn.lrange('items', 0, -1)
        bugs = [pickle.loads(b) for b in bugs]
        bugs = [bug['uuid'] for bug in bugs]
        self.conn.ltrim('items', 1, 0)

        result = job.return_value
        self.assertEqual(result.origin, BUGZILLA_SERVER_URL)
        self.assertEqual(result.backend, 'bugzilla')
        self.assertEqual(result.last_uuid, 'b4009442d38f4241a4e22e3e61b7cd8ef5ced35c')
        self.assertEqual(result.max_date, 1439404330.0)
        self.assertEqual(result.nitems, 7)

        self.assertEqual(len(requests), 13)
        self.assertListEqual(bugs, expected)

        # Now, we get the bugs from the cache.
        # The contents should be the same and there won't be
        # any new request to the server

        job = q.enqueue(execute_perceval_job, qitems='items',
                        origin=BUGZILLA_SERVER_URL, backend='bugzilla',
                        cache_path=self.tmp_path, cache_fetch=True,
                        **args)

        cached_bugs = self.conn.lrange('items', 0, -1)
        cached_bugs = [pickle.loads(b) for b in cached_bugs]
        cached_bugs = [bug['uuid'] for bug in cached_bugs]
        self.conn.ltrim('items', 1, 0)

        result = job.return_value
        self.assertEqual(result.origin, BUGZILLA_SERVER_URL)
        self.assertEqual(result.backend, 'bugzilla')
        self.assertEqual(result.last_uuid, 'b4009442d38f4241a4e22e3e61b7cd8ef5ced35c')
        self.assertEqual(result.max_date, 1439404330.0)
        self.assertEqual(result.nitems, 7)

        self.assertEqual(len(requests), 13)
        self.assertListEqual(cached_bugs, bugs)


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
