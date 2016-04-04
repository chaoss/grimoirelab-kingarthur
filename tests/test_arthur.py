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

import os
import shutil
import subprocess
import sys
import tempfile
import unittest

if not '..' in sys.path:
    sys.path.insert(0, '..')

from arthur.arthur import Arthur

from tests import find_empty_redis_database


class TestArthur(unittest.TestCase):
    """Unit tests for Scheduler class"""

    @classmethod
    def setUpClass(cls):
        cls.tmp_path = tempfile.mkdtemp(prefix='arthur_')
        cls.git_path = os.path.join(cls.tmp_path, 'gittest')

        subprocess.check_call(['tar', '-xzf', 'data/gittest.tar.gz',
                               '-C', cls.tmp_path])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp_path)

    def setUp(self):
        self.conn = find_empty_redis_database()
        self.conn.flushdb()

    def tearDown(self):
        self.conn.flushdb()

    def test_items(self):
        new_path = os.path.join(self.tmp_path, 'newgit')

        app = Arthur(self.conn, async_mode=False)
        app.add('test', 'git', args={'uri': self.git_path,
                                     'gitpath' : new_path})
        commits = [item['data']['commit'] for item in app.items()]

        expected = ['bc57a9209f096a130dcc5ba7089a8663f758a703',
                    '87783129c3f00d2c81a3a8e585eb86a47e39891a',
                    '7debcf8a2f57f86663809c58b5c07a398be7674c',
                    'c0d66f92a95e31c77be08dc9d0f11a16715d1885',
                    'c6ba8f7a1058db3e6b4bc6f1090e932b107605fb',
                    '589bb080f059834829a2a5955bebfd7c2baa110a',
                    'ce8e0b86a1e9877f42fe9453ede418519115f367',
                    '51a3b654f252210572297f47597b31527c475fb8',
                    '456a68ee1407a77f3e804a30dff245bb6c6b872f']

        self.assertListEqual(commits, expected)

        commits = [item for item in app.items()]

        self.assertListEqual(commits, [])

        shutil.rmtree(new_path)


if __name__ == "__main__":
    unittest.main()
