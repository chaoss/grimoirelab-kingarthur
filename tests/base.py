# -*- coding: utf-8 -*-
#
# Copyright (C) 2015-2019 Bitergia
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
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Santiago Dueñas <sduenas@bitergia.com>
#

import os.path
import unittest

from fakeredis import FakeStrictRedis
from rq import pop_connection, push_connection


# The next code is based on  'rq/tests/__init__.py' file of
# rq project. This code was licensed as BSD-like. All credits
# to their authors.

def find_empty_redis_database():
    """Connect to a fake Redis database"""
    conn = FakeStrictRedis()
    return conn


class TestBaseRQ(unittest.TestCase):
    """Base class to inherit test cases from for RQ"""

    @classmethod
    def setUpClass(cls):
        cls.conn = find_empty_redis_database()
        push_connection(cls.conn)
        cls.dir = os.path.dirname(os.path.realpath(__file__))

    @classmethod
    def tearDownClass(cls):
        conn = pop_connection()
        assert conn == cls.conn, \
            "Wow, something really nasty happened to the FakeRedis connection stack. Check your setup."

    def setUp(self):
        self.conn.flushdb()

    def tearDown(self):
        self.conn.flushdb()


def mock_sum(task_id, a, b):
    return a + b


def mock_failure(task_id):
    raise Exception
