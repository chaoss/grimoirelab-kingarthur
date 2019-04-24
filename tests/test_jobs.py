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
import os
import os.path
import pickle
import shutil
import tempfile
import unittest

import httpretty
import requests
import rq

from arthur import __version__
from arthur.errors import NotFoundError
from arthur.jobs import (JobResult,
                         PercevalJob,
                         execute_perceval_job,
                         metadata)
from grimoirelab_toolkit.datetime import datetime_utcnow
from perceval.archive import ArchiveManager

from base import TestBaseRQ


BUGZILLA_SERVER_URL = 'http://example.com'
BUGZILLA_BUGLIST_URL = BUGZILLA_SERVER_URL + '/buglist.cgi'
BUGZILLA_BUG_URL = BUGZILLA_SERVER_URL + '/show_bug.cgi'
BUGZILLA_BUG_ACTIVITY_URL = BUGZILLA_SERVER_URL + '/show_activity.cgi'

REDMINE_URL = 'http://example.com'
REDMINE_ISSUES_URL = REDMINE_URL + '/issues.json'
REDMINE_ISSUE_2_URL = REDMINE_URL + '/issues/2.json'
REDMINE_ISSUE_5_URL = REDMINE_URL + '/issues/5.json'
REDMINE_ISSUE_9_URL = REDMINE_URL + '/issues/9.json'
REDMINE_ISSUE_7311_URL = REDMINE_URL + '/issues/7311.json'
REDMINE_USER_3_URL = REDMINE_URL + '/users/3.json'
REDMINE_USER_4_URL = REDMINE_URL + '/users/4.json'
REDMINE_USER_24_URL = REDMINE_URL + '/users/24.json'
REDMINE_USER_25_URL = REDMINE_URL + '/users/25.json'

REDMINE_URL_LIST = [
    REDMINE_ISSUES_URL, REDMINE_ISSUE_2_URL, REDMINE_ISSUE_5_URL,
    REDMINE_ISSUE_9_URL, REDMINE_ISSUE_7311_URL, REDMINE_USER_3_URL,
    REDMINE_USER_4_URL, REDMINE_USER_24_URL, REDMINE_USER_25_URL
]


def read_file(filename, mode='r'):
    dir = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(dir, filename), mode) as f:
        content = f.read()
    return content


def setup_mock_bugzilla_server():
    """Setup a mock Bugzilla server for testing"""

    http_requests = []
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
            body = bodies_html[len(http_requests) % 2]

        http_requests.append(httpretty.last_request())

        return (200, headers, body)

    httpretty.register_uri(httpretty.GET,
                           BUGZILLA_BUGLIST_URL,
                           responses=[
                               httpretty.Response(body=request_callback)
                               for _ in range(3)
                           ])
    httpretty.register_uri(httpretty.GET,
                           BUGZILLA_BUG_URL,
                           responses=[
                               httpretty.Response(body=request_callback)
                               for _ in range(3)
                           ])
    httpretty.register_uri(httpretty.GET,
                           BUGZILLA_BUG_ACTIVITY_URL,
                           responses=[
                               httpretty.Response(body=request_callback)
                               for _ in range(7)
                           ])

    return http_requests


def setup_mock_redmine_server(max_failures=0):
    """Setup a mock Redmine HTTP server"""

    http_requests = []
    failures = max_failures

    issues_body = read_file('data/redmine/redmine_issues.json', 'rb')
    issues_next_body = read_file('data/redmine/redmine_issues_next.json', 'rb')
    issues_empty_body = read_file('data/redmine/redmine_issues_empty.json', 'rb')
    issue_2_body = read_file('data/redmine/redmine_issue_2.json', 'rb')
    issue_5_body = read_file('data/redmine/redmine_issue_5.json', 'rb')
    issue_9_body = read_file('data/redmine/redmine_issue_9.json', 'rb')
    issue_7311_body = read_file('data/redmine/redmine_issue_7311.json', 'rb')
    user_3_body = read_file('data/redmine/redmine_user_3.json', 'rb')
    user_4_body = read_file('data/redmine/redmine_user_4.json', 'rb')
    user_24_body = read_file('data/redmine/redmine_user_24.json', 'rb')
    user_25_body = read_file('data/redmine/redmine_user_25.json', 'rb')

    def request_callback(method, uri, headers):
        nonlocal failures

        status = 200
        last_request = httpretty.last_request()
        params = last_request.querystring

        if uri.startswith(REDMINE_ISSUES_URL):
            updated_on = params['updated_on'][0]
            offset = params['offset'][0]

            if (updated_on == '>=1970-01-01T00:00:00Z' and offset == '0'):
                body = issues_body
            elif (updated_on == '>=1970-01-01T00:00:00Z' and offset == '3'):
                body = issues_next_body
            elif (updated_on == '>=2016-07-27T00:00:00Z' and offset == '0'):
                body = issues_next_body
            elif (updated_on == '>=2011-12-08T17:58:37Z' and offset == '0'):
                body = issues_next_body
            else:
                body = issues_empty_body
        elif uri.startswith(REDMINE_ISSUE_2_URL):
            body = issue_2_body
        elif uri.startswith(REDMINE_ISSUE_5_URL):
            body = issue_5_body
        elif uri.startswith(REDMINE_ISSUE_9_URL):
            body = issue_9_body
        elif uri.startswith(REDMINE_ISSUE_7311_URL):
            if failures > 0:
                status = 500
                body = "Internal Server Error"
                failures -= 1
            else:
                body = issue_7311_body
        elif uri.startswith(REDMINE_USER_3_URL):
            body = user_3_body
        elif uri.startswith(REDMINE_USER_4_URL):
            body = user_4_body
        elif uri.startswith(REDMINE_USER_24_URL):
            body = user_24_body
        elif uri.startswith(REDMINE_USER_25_URL):
            body = user_25_body
        else:
            raise

        http_requests.append(last_request)

        return (status, headers, body)

    for url in REDMINE_URL_LIST:
        httpretty.register_uri(httpretty.GET,
                               url,
                               responses=[
                                   httpretty.Response(body=request_callback)
                               ])

    return http_requests


class MockJob:
    """Mock job class for testing purposes"""

    def __init__(self, job_id):
        self.job_id = job_id

    @metadata
    def execute(self):
        for x in range(5):
            item = {'item': x}
            yield item


class TestMetadata(unittest.TestCase):
    """Unit tests for metadata decorator"""

    def test_decorator(self):
        """Check if the decorator works"""

        job = MockJob(8)

        items = [item for item in job.execute()]

        for x in range(5):
            item = items[x]

            self.assertEqual(item['arthur_version'], __version__)
            self.assertEqual(item['job_id'], 8)
            self.assertEqual(item['item'], x)


class TestJobResult(unittest.TestCase):
    """Unit tests for JobResult class"""

    def test_job_result_init(self):
        result = JobResult('arthur-job-1234567890', 'mytask', 'mock_backend', 'category',
                           'ABCDEFGHIJK', 1344965413.0, 58)

        self.assertEqual(result.job_id, 'arthur-job-1234567890')
        self.assertEqual(result.task_id, 'mytask')
        self.assertEqual(result.backend, 'mock_backend')
        self.assertEqual(result.category, 'category')
        self.assertEqual(result.last_uuid, 'ABCDEFGHIJK')
        self.assertEqual(result.max_date, 1344965413.0)
        self.assertEqual(result.nitems, 58)
        self.assertEqual(result.offset, None)
        self.assertEqual(result.nresumed, 0)

        result = JobResult('arthur-job-1234567890', 'mytask', 'mock_backend', 'category',
                           'ABCDEFGHIJK', 1344965413.0, 58,
                           offset=128, nresumed=10)

        self.assertEqual(result.offset, 128)
        self.assertEqual(result.nresumed, 10)


class TestPercevalJob(TestBaseRQ):
    """Unit tests for PercevalJob class"""

    def setUp(self):
        self.tmp_path = tempfile.mkdtemp(prefix='arthur_')
        super().setUp()

    def tearDown(self):
        shutil.rmtree(self.tmp_path)
        super().tearDown()

    def test_init(self):
        """Test the initialization of the object"""

        job = PercevalJob('arthur-job-1234567890', 'mytask', 'git', 'commit',
                          self.conn, 'items')

        self.assertEqual(job.job_id, 'arthur-job-1234567890')
        self.assertEqual(job.task_id, 'mytask')
        self.assertEqual(job.backend, 'git')
        self.assertEqual(job.category, 'commit')
        self.assertEqual(job.conn, self.conn)
        self.assertEqual(job.qitems, 'items')
        self.assertEqual(job.archive_manager, None)

        result = job.result
        self.assertIsInstance(job.result, JobResult)
        self.assertEqual(result.job_id, 'arthur-job-1234567890')
        self.assertEqual(result.task_id, 'mytask')
        self.assertEqual(result.backend, 'git')
        self.assertEqual(job.category, 'commit')
        self.assertEqual(result.last_uuid, None)
        self.assertEqual(result.max_date, None)
        self.assertEqual(result.nitems, 0)
        self.assertEqual(result.offset, None)
        self.assertEqual(result.nresumed, 0)

    def test_backend_not_found(self):
        """Test if it raises an exception when a backend is not found"""

        with self.assertRaises(NotFoundError) as e:
            _ = PercevalJob('arthur-job-1234567890', 'mytask', 'mock_backend', 'acme-category',
                            self.conn, 'items')
            self.assertEqual(e.exception.element, 'mock_backend')

    def test_run(self):
        """Test run method using the Git backend"""

        job = PercevalJob('arthur-job-1234567890', 'mytask', 'git', 'commit',
                          self.conn, 'items')
        args = {
            'uri': 'http://example.com/',
            'gitpath': os.path.join(self.dir, 'data/git_log.txt')
        }
        archive_args = {
            'archive_path': self.tmp_path,
            'fetch_from_archive': False
        }

        job.run(args, archive_args)

        self.assertIsInstance(job.archive_manager, ArchiveManager)
        result = job.result
        self.assertIsInstance(job.result, JobResult)
        self.assertEqual(result.job_id, 'arthur-job-1234567890')
        self.assertEqual(result.task_id, 'mytask')
        self.assertEqual(result.backend, 'git')
        self.assertEqual(result.category, 'commit')
        self.assertEqual(result.last_uuid, '1375b60d3c23ac9b81da92523e4144abc4489d4c')
        self.assertEqual(result.max_date, 1392185439.0)
        self.assertEqual(result.nitems, 9)
        self.assertEqual(result.offset, None)
        self.assertEqual(result.nresumed, 0)

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

        self.assertEqual(commits, expected)

    def test_run_not_found_parameters(self):
        """Check if it fails when a required backend parameter is not found"""

        job = PercevalJob('arthur-job-1234567890', 'mytask', 'git', 'commit',
                          self.conn, 'items')
        args = {
            'uri': 'http://example.com/'
        }
        archive_args = {
            'archive_path': self.tmp_path,
            'fetch_from_archive': False
        }

        with self.assertRaises(AttributeError) as e:
            job.run(args, archive_args)
            self.assertEqual(e.exception.args[1], 'gitlog')

    @httpretty.activate
    def _test_fetch_from_archive(self):
        """Test run method fetching from the archive"""

        http_requests = setup_mock_bugzilla_server()

        expected = ['5a8a1e25dfda86b961b4146050883cbfc928f8ec',
                    '1fd4514e56f25a39ffd78eab19e77cfe4dfb7769',
                    '6a4cb244067c3cfa38f9f563e2ab3cd8ac21762f',
                    '7e033ed0110032ead6b918be43c1f3f88cd98fd7',
                    'f90d12b380ffdb47f2b6e96b321f08000181a9d6',
                    '4b166308f205121bc57704032acdc81b6c9bb8b1',
                    'b4009442d38f4241a4e22e3e61b7cd8ef5ced35c']

        # First, we fetch the bugs from the server, storing them
        # in a archive
        args = {
            'url': BUGZILLA_SERVER_URL,
            'max_bugs': 5
        }
        archive_args = {
            'archive_path': self.tmp_path,
            'fetch_from_archive': False
        }

        job = PercevalJob('arthur-job-1234567890', 'mytask', 'bugzilla', 'issue',
                          self.conn, 'items')
        job.run(args, archive_args=archive_args)

        bugs = self.conn.lrange('items', 0, -1)
        bugs = [pickle.loads(b) for b in bugs]
        bugs = [bug['uuid'] for bug in bugs]
        self.conn.ltrim('items', 1, 0)

        result = job.result
        self.assertIsInstance(job.archive_manager, ArchiveManager)
        self.assertEqual(result.job_id, job.job_id)
        self.assertEqual(result.task_id, 'mytask')
        self.assertEqual(result.backend, 'bugzilla')
        self.assertEqual(result.category, 'issue')
        self.assertEqual(result.last_uuid, 'b4009442d38f4241a4e22e3e61b7cd8ef5ced35c')
        self.assertEqual(result.max_date, 1439404330.0)
        self.assertEqual(result.nitems, 7)
        self.assertEqual(result.offset, None)
        self.assertEqual(result.nresumed, 0)

        self.assertEqual(len(http_requests), 13)
        self.assertListEqual(bugs, expected)

        # Now, we get the bugs from the archive.
        # The contents should be the same and there won't be
        # any new request to the server
        job_archive = PercevalJob('arthur-job-1234567890-bis', 'mytask', 'bugzilla', 'issue',
                                  self.conn, 'items')

        archive_args['fetch_from_archive'] = True
        job_archive.run(args, archive_args=archive_args)

        archived_bugs = self.conn.lrange('items', 0, -1)
        archived_bugs = [pickle.loads(b) for b in archived_bugs]
        archived_bugs = [bug['uuid'] for bug in archived_bugs]
        self.conn.ltrim('items', 1, 0)

        result = job_archive.result
        self.assertIsInstance(job.archive_manager, ArchiveManager)
        self.assertEqual(result.job_id, job_archive.job_id)
        self.assertEqual(result.task_id, 'mytask')
        self.assertEqual(result.backend, 'bugzilla')
        self.assertEqual(result.category, 'issue')
        self.assertEqual(result.last_uuid, 'b4009442d38f4241a4e22e3e61b7cd8ef5ced35c')
        self.assertEqual(result.max_date, 1439404330.0)
        self.assertEqual(result.nitems, 7)
        self.assertEqual(result.offset, None)
        self.assertEqual(result.nresumed, 0)

        self.assertEqual(len(http_requests), 13)

        self.assertListEqual(archived_bugs, bugs)

    @httpretty.activate
    def test_resuming(self):
        """Test if it resumes when a failure is reached"""

        http_requests = setup_mock_redmine_server(max_failures=1)

        args = {
            'url': REDMINE_URL,
            'api_token': 'AAAA',
            'max_issues': 3
        }

        job = PercevalJob('arthur-job-1234567890', 'mytask', 'redmine', 'issue',
                          self.conn, 'items')

        with self.assertRaises(requests.exceptions.HTTPError):
            job.run(args)

        result = job.result
        self.assertEqual(result.job_id, 'arthur-job-1234567890')
        self.assertEqual(result.task_id, 'mytask')
        self.assertEqual(result.backend, 'redmine')
        self.assertEqual(result.category, 'issue')
        self.assertEqual(result.last_uuid, '3c3d67925b108a37f88cc6663f7f7dd493fa818c')
        self.assertEqual(result.max_date, 1323367117.0)
        self.assertEqual(result.nitems, 3)
        self.assertEqual(result.offset, None)
        self.assertEqual(result.nresumed, 0)

        issues = self.conn.lrange('items', 0, -1)
        issues = [pickle.loads(i) for i in issues]
        issues = [i['uuid'] for i in issues]
        self.conn.ltrim('items', 1, 0)

        expected = ['91a8349c2f6ebffcccc49409529c61cfd3825563',
                    'c4aeb9e77fec8e4679caa23d4012e7cc36ae8b98',
                    '3c3d67925b108a37f88cc6663f7f7dd493fa818c']
        self.assertEqual(issues, expected)

        job.run(args, resume=True)

        result = job.result
        self.assertEqual(result.job_id, 'arthur-job-1234567890')
        self.assertEqual(result.task_id, 'mytask')
        self.assertEqual(result.backend, 'redmine')
        self.assertEqual(result.last_uuid, '4ab289ab60aee93a66e5490529799cf4a2b4d94c')
        self.assertEqual(result.max_date, 1469607427.0)
        self.assertEqual(result.nitems, 4)
        self.assertEqual(result.offset, None)
        self.assertEqual(result.nresumed, 1)

        issues = self.conn.lrange('items', 0, -1)
        issues = [pickle.loads(i) for i in issues]
        issues = [i['uuid'] for i in issues]
        self.conn.ltrim('items', 1, 0)

        expected = ['4ab289ab60aee93a66e5490529799cf4a2b4d94c']
        self.assertEqual(issues, expected)

    def test_initialize_archive_manager(self):
        """Test if the archive is initialized"""

        job = PercevalJob('arthur-job-1234567890', 'mytask', 'git', 'commit',
                          self.conn, 'items')

        self.assertIsNone(job.archive_manager)

    def test_invalid_path_for_archive(self):
        """Test whether it raises an exception when the archive path is invalid"""

        job = PercevalJob('arthur-job-1234567890', 'mytaks', 'git', 'commit',
                          self.conn, 'items')

        job.initialize_archive_manager(None)
        self.assertIsNone(job.archive_manager)

        with self.assertRaises(ValueError):
            job.initialize_archive_manager("")


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

        backend_args = {
            'uri': 'http://example.com/',
            'gitpath': os.path.join(self.dir, 'data/git_log.txt')
        }
        archive_args = {}

        q = rq.Queue('queue', is_async=False)  # noqa: W606

        job = q.enqueue(execute_perceval_job,
                        backend='git', backend_args=backend_args, category='commit',
                        archive_args=archive_args, max_retries=0,
                        qitems='items', task_id='mytask')

        result = job.return_value
        self.assertEqual(result.job_id, job.get_id())
        self.assertEqual(result.task_id, 'mytask')
        self.assertEqual(result.backend, 'git')
        self.assertEqual(result.category, 'commit')
        self.assertEqual(result.last_uuid, '1375b60d3c23ac9b81da92523e4144abc4489d4c')
        self.assertEqual(result.max_date, 1392185439.0)
        self.assertEqual(result.nitems, 9)
        self.assertEqual(result.offset, None)
        self.assertEqual(result.nresumed, 0)

        commits = self.conn.lrange('items', 0, -1)
        commits = [pickle.loads(c) for c in commits]
        commits = [(commit['job_id'], commit['data']['commit']) for commit in commits]

        expected = ['456a68ee1407a77f3e804a30dff245bb6c6b872f',
                    '51a3b654f252210572297f47597b31527c475fb8',
                    'ce8e0b86a1e9877f42fe9453ede418519115f367',
                    '589bb080f059834829a2a5955bebfd7c2baa110a',
                    'c6ba8f7a1058db3e6b4bc6f1090e932b107605fb',
                    'c0d66f92a95e31c77be08dc9d0f11a16715d1885',
                    '7debcf8a2f57f86663809c58b5c07a398be7674c',
                    '87783129c3f00d2c81a3a8e585eb86a47e39891a',
                    'bc57a9209f096a130dcc5ba7089a8663f758a703']

        for x in range(len(expected)):
            item = commits[x]
            self.assertEqual(item[0], result.job_id)
            self.assertEqual(item[1], expected[x])

    @httpretty.activate
    def test_retry_job(self):
        """Test if the job will be succesful after some retries"""

        http_requests = setup_mock_redmine_server(max_failures=2)

        backend_args = {
            'url': REDMINE_URL,
            'api_token': 'AAAA',
            'max_issues': 3
        }

        q = rq.Queue('queue', is_async=False)  # noqa: W606
        job = q.enqueue(execute_perceval_job,
                        backend='redmine', backend_args=backend_args,
                        category='issue',
                        qitems='items', task_id='mytask',
                        max_retries=3)

        result = job.return_value
        self.assertEqual(result.job_id, job.get_id())
        self.assertEqual(result.task_id, 'mytask')
        self.assertEqual(result.backend, 'redmine')
        self.assertEqual(result.last_uuid, '4ab289ab60aee93a66e5490529799cf4a2b4d94c')
        self.assertEqual(result.max_date, 1469607427.0)
        self.assertEqual(result.nitems, 4)
        self.assertEqual(result.offset, None)
        self.assertEqual(result.nresumed, 2)

        issues = self.conn.lrange('items', 0, -1)
        issues = [pickle.loads(i) for i in issues]
        issues = [(issue['job_id'], issue['uuid']) for issue in issues]
        self.conn.ltrim('items', 1, 0)

        expected = ['91a8349c2f6ebffcccc49409529c61cfd3825563',
                    'c4aeb9e77fec8e4679caa23d4012e7cc36ae8b98',
                    '3c3d67925b108a37f88cc6663f7f7dd493fa818c',
                    '4ab289ab60aee93a66e5490529799cf4a2b4d94c']

        self.assertEqual(len(issues), len(expected))

        for x in range(len(expected)):
            item = issues[x]
            self.assertEqual(item[0], result.job_id)
            self.assertEqual(item[1], expected[x])

    @httpretty.activate
    def test_max_retries_job_failure(self):
        """Test if the job will fail after max_retries limit is reached"""

        http_requests = setup_mock_redmine_server(max_failures=2)

        args = {
            'url': REDMINE_URL,
            'api_token': 'AAAA',
            'max_issues': 3
        }

        q = rq.Queue('queue', is_async=False)  # noqa: W606

        with self.assertRaises(requests.exceptions.HTTPError):
            job = q.enqueue(execute_perceval_job,
                            backend='redmine', backend_args=args,
                            category='issue',
                            qitems='items', task_id='mytask',
                            max_retries=1)
            self.assertEqual(job.is_failed, True)

    def test_job_no_result(self):
        """Execute a Git backend job that will not produce any results"""

        backend_args = {
            'uri': 'http://example.com/',
            'gitpath': os.path.join(self.dir, 'data/git_log_empty.txt'),
            'from_date': datetime.datetime(2020, 1, 1, 1, 1, 1)
        }

        q = rq.Queue('queue', is_async=False)  # noqa: W606
        job = q.enqueue(execute_perceval_job,
                        backend='git', backend_args=backend_args,
                        category='commit',
                        qitems='items', task_id='mytask')

        result = job.return_value
        self.assertEqual(result.job_id, job.get_id())
        self.assertEqual(result.task_id, 'mytask')
        self.assertEqual(result.backend, 'git')
        self.assertEqual(result.category, 'commit')
        self.assertEqual(result.last_uuid, None)
        self.assertEqual(result.max_date, 1577840461.0)
        self.assertEqual(result.nitems, 0)
        self.assertEqual(result.offset, None)
        self.assertEqual(result.nresumed, 0)

        commits = self.conn.lrange('items', 0, -1)
        commits = [pickle.loads(c) for c in commits]
        self.assertListEqual(commits, [])

    @httpretty.activate
    def test_job_archive(self):
        """Execute a Bugzilla backend job to fetch data from the archive"""

        after_dt = datetime_utcnow()
        http_requests = setup_mock_bugzilla_server()

        expected = ['5a8a1e25dfda86b961b4146050883cbfc928f8ec',
                    '1fd4514e56f25a39ffd78eab19e77cfe4dfb7769',
                    '6a4cb244067c3cfa38f9f563e2ab3cd8ac21762f',
                    '7e033ed0110032ead6b918be43c1f3f88cd98fd7',
                    'f90d12b380ffdb47f2b6e96b321f08000181a9d6',
                    '4b166308f205121bc57704032acdc81b6c9bb8b1',
                    'b4009442d38f4241a4e22e3e61b7cd8ef5ced35c']

        q = rq.Queue('queue', is_async=False)  # noqa: W606

        # First, we fetch the bugs from the server, storing them
        # in an archive
        backend_args = {
            'url': BUGZILLA_SERVER_URL,
            'max_bugs': 5
        }
        archive_args = {
            'archive_path': self.tmp_path,
            'fetch_from_archive': False,
            'archived_after': after_dt
        }

        job = q.enqueue(execute_perceval_job,
                        backend='bugzilla', backend_args=backend_args, category='bug',
                        qitems='items', task_id='mytask',
                        archive_args=archive_args)

        bugs = self.conn.lrange('items', 0, -1)
        bugs = [pickle.loads(b) for b in bugs]
        bugs = [bug['uuid'] for bug in bugs]
        self.conn.ltrim('items', 1, 0)

        result = job.return_value
        self.assertEqual(result.job_id, job.get_id())
        self.assertEqual(result.task_id, 'mytask')
        self.assertEqual(result.backend, 'bugzilla')
        self.assertEqual(result.last_uuid, 'b4009442d38f4241a4e22e3e61b7cd8ef5ced35c')
        self.assertEqual(result.max_date, 1439404330.0)
        self.assertEqual(result.nitems, 7)
        self.assertEqual(result.nresumed, 0)

        self.assertEqual(len(http_requests), 13)
        self.assertListEqual(bugs, expected)

        # Now, we get the bugs from the archive.
        # The contents should be the same and there won't be
        # any new request to the server

        archive_args['fetch_from_archive'] = True
        job = q.enqueue(execute_perceval_job,
                        backend='bugzilla', backend_args=backend_args,
                        qitems='items', task_id='mytask', category='bug',
                        archive_args=archive_args)

        archived_bugs = self.conn.lrange('items', 0, -1)
        archived_bugs = [pickle.loads(b) for b in archived_bugs]
        archived_bugs = [bug['uuid'] for bug in archived_bugs]
        self.conn.ltrim('items', 1, 0)

        result = job.return_value
        self.assertEqual(result.job_id, job.get_id())
        self.assertEqual(result.task_id, 'mytask')
        self.assertEqual(result.backend, 'bugzilla')
        self.assertEqual(result.last_uuid, 'b4009442d38f4241a4e22e3e61b7cd8ef5ced35c')
        self.assertEqual(result.max_date, 1439404330.0)
        self.assertEqual(result.nitems, 7)
        self.assertEqual(result.nresumed, 0)

        self.assertEqual(len(http_requests), 13)

        self.assertListEqual(archived_bugs, bugs)

    def test_job_archiving_not_supported(self):
        """Check if it fails when archiving is not supported"""

        backend_args = {
            'uri': 'http://example.com/',
            'gitpath': os.path.join(self.dir, 'data/git_log.txt')
        }
        archive_args = {
            'archive_path': self.tmp_path,
            'fetch_from_archive': True
        }

        q = rq.Queue('queue', is_async=False)  # noqa: W606

        with self.assertRaises(AttributeError):
            _ = q.enqueue(execute_perceval_job,
                          backend='git', backend_args=backend_args,
                          category='commit',
                          qitems='items', task_id='mytask',
                          archive_args=archive_args)


if __name__ == "__main__":
    unittest.main()
