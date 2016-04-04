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
#

import unittest

from redis import StrictRedis
from rq import pop_connection, push_connection


# The next code is based on  'rq/tests/__init__.py' file of
# rq project. This code was licensed as BSD-like. All credits
# to their authors.

def find_empty_redis_database():
    """Connect to a random Redis database.

    Tries to connect a random Redis database (starting on 8) and
    will use/connect it when no keys are stored in there.
    """
    for db in range(8, 18):
        conn = StrictRedis(db=db)
        empty = len(conn.keys('*')) == 0
        if empty:
            return conn
    assert False, "No empty Redis database found to run tests in."


class TestBaseRQ(unittest.TestCase):
    """Base class to inherit test cases from for RQ"""

    @classmethod
    def setUpClass(cls):
        cls.conn = find_empty_redis_database()
        push_connection(cls.conn)

    @classmethod
    def tearDownClass(cls):
        conn = pop_connection()
        assert conn == cls.conn, \
            "Wow, something really nasty happened to the Redis connection stack. Check your setup."

    def setUp(self):
        self.conn.flushdb()

    def tearDown(self):
        self.conn.flushdb()


def mock_sum(a, b):
    return a + b

def mock_failure():
    raise Exception
