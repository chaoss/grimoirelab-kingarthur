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
#     Alvaro del Castillo San Felix <acs@bitergia.com>
#

import datetime
import json
import threading


class RWLock:
    """Read Write lock to avoid starvation.

    This lock solves the problem of the so-called 'third readers-writers
    problem'. The implementation is based on the algorithm proposed by
    Samuel Tardieu on his article 'The third readers-writers problem' from
    his blog page. Please visit the next link for more information:

    https://www.rfc1149.net/blog/2011/01/07/the-third-readers-writers-problem/
    """
    def __init__(self):
        self._readers = 0
        self._order_mutex = threading.Semaphore()
        self._readers_mutex = threading.Semaphore()
        self._access_mutex = threading.Semaphore()

    def reader_acquire(self):
        """Acquire the lock to read"""

        self._order_mutex.acquire()
        self._readers_mutex.acquire()

        if self._readers == 0:
            self._access_mutex.acquire()
        self._readers += 1

        self._order_mutex.release()
        self._readers_mutex.release()

    def reader_release(self):
        """Release the lock after reading"""

        self._readers_mutex.acquire()

        self._readers -= 1
        if self._readers == 0:
            self._access_mutex.release()

        self._readers_mutex.release()

    def writer_acquire(self):
        """Acquire the lock to write"""

        self._order_mutex.acquire()
        self._access_mutex.acquire()
        self._order_mutex.release()

    def writer_release(self):
        """Release the lock after writting"""

        self._access_mutex.release()


class JSONEncoder(json.JSONEncoder):
    """JSON encoder which encodes datetime objects too"""

    def default(self, o):
        if isinstance(o, datetime.datetime):
            return o.isoformat()
        return super().default(o)

    def iterencode(self, o, _one_shot=False):
        for chunk in super().iterencode(o, _one_shot=_one_shot):
            yield chunk
