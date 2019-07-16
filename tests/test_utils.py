#!/usr/bin/env python3
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

import copy
import datetime
import json
import threading
import time
import unittest

from arthur.utils import RWLock, JSONEncoder


class RWLockThread(threading.Thread):

    def __init__(self, lock, rw_buffer, sleep_before, sleep_after):
        super().__init__()
        self.lock = lock
        self.rw_buffer = rw_buffer
        self.sleep_before = sleep_before
        self.sleep_after = sleep_after
        self.entry_time = None
        self.exit_time = None


class Reader(RWLockThread):

    def __init__(self, lock, rw_buffer, sleep_before, sleep_after):
        super().__init__(lock, rw_buffer, sleep_before, sleep_after)
        self.rdata = None

    def run(self):
        time.sleep(self.sleep_before)
        self.lock.reader_acquire()
        self.entry_time = time.time()
        self.rdata = copy.deepcopy(self.rw_buffer)
        time.sleep(self.sleep_after)
        self.exit_time = time.time()
        self.lock.reader_release()


class Writer(RWLockThread):

    def __init__(self, lock, rw_buffer, sleep_before, sleep_after, data):
        super().__init__(lock, rw_buffer, sleep_before, sleep_after)
        self.wdata = data

    def run(self):
        time.sleep(self.sleep_before)
        self.lock.writer_acquire()
        self.entry_time = time.time()
        self.rw_buffer[0] = self.wdata
        time.sleep(self.sleep_after)
        self.exit_time = time.time()
        self.lock.writer_release()


class TestRWLock(unittest.TestCase):
    """Unit tests for RWLock class"""

    def test_multiple_readers(self):
        """Test non-exclusive access to readers"""

        rw_lock = RWLock()
        rw_buffer = ['A']

        threads = [Reader(rw_lock, rw_buffer, 0, 0.05),
                   Reader(rw_lock, rw_buffer, 0.01, 0),
                   Writer(rw_lock, rw_buffer, 0.02, 0.02, 'Z'),
                   Reader(rw_lock, rw_buffer, 0.03, 0)]

        for th in threads:
            th.start()
        for th in threads:
            th.join()

        self.assertEqual(rw_buffer, ['Z'])
        self.assertEqual(threads[0].rdata, ['A'])
        self.assertEqual(threads[1].rdata, ['A'])
        self.assertEqual(threads[3].rdata, ['Z'])

        # Second reader started after the first one finishing
        # before it. This means there is no mutual exclusion
        # between readers.
        self.assertLess(threads[0].entry_time, threads[1].entry_time)
        self.assertGreater(threads[0].exit_time, threads[1].exit_time)

    def test_exclusion_writers(self):
        """Test if only one writer can access the resource at the same time"""

        rw_lock = RWLock()
        rw_buffer = ['A']

        threads = [Reader(rw_lock, rw_buffer, 0, 0.02),
                   Writer(rw_lock, rw_buffer, 0.02, 0.03, 'Z'),
                   Writer(rw_lock, rw_buffer, 0.03, 0, 'X'),
                   Reader(rw_lock, rw_buffer, 0.04, 0)]

        for th in threads:
            th.start()
        for th in threads:
            th.join()

        self.assertEqual(rw_buffer, ['X'])
        self.assertEqual(threads[0].rdata, ['A'])
        self.assertEqual(threads[3].rdata, ['X'])

        # First writting lasts more than the time that the second
        # writer needs to access the critical section. But, the
        # second writer will wait till the first writer ends its work.
        self.assertLess(threads[1].entry_time, threads[2].entry_time)
        self.assertLess(threads[1].exit_time, threads[2].exit_time)

        # The last thread (a reader) will wait till the last writer ends.
        self.assertLess(threads[2].entry_time, threads[3].entry_time)
        self.assertLess(threads[2].exit_time, threads[3].exit_time)


class TestJSONEncoder(unittest.TestCase):
    """Unit tests for JSONEncoder class"""

    def test_default(self):
        """Test default method"""

        encoder = JSONEncoder()

        dt = datetime.datetime(2016, 1, 1, 8, 8, 8)
        value = encoder.default(dt)
        self.assertEqual(value, "2016-01-01T08:08:08")

        # This method will raise TypeError exceptions for those
        # objects that are not instances of `datetime`
        self.assertRaises(TypeError, encoder.default, 8)
        self.assertRaises(TypeError, encoder.default, 'test')
        self.assertRaises(TypeError, encoder.default, {})
        self.assertRaises(TypeError, encoder.default, [1, 2, 3])

    def test_iterencode(self):
        """Test iterencode method"""

        encoder = JSONEncoder()

        obj = {
            'l': [None],
            'dt': datetime.datetime(2016, 1, 1, 8, 8, 8),
            's': "a string",
            'i': 8
        }

        expected = obj
        expected['dt'] = "2016-01-01T08:08:08"

        # Join the encoded JSON parts and decode the result
        # into a new object. This is needed because the joined
        # string does not follow always the same order.
        result = ''.join([chunk for chunk in encoder.iterencode(obj)])
        result = json.loads(result)
        self.assertEqual(result, obj)


if __name__ == "__main__":
    unittest.main()
