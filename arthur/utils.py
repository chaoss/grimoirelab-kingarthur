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

import datetime
import json
import threading

import dateutil.parser
import dateutil.tz

from .errors import InvalidDateError


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


def str_to_datetime(ts):
    """Format a string to a datetime object.

    This functions supports several date formats like YYYY-MM-DD,
    MM-DD-YYYY, YY-MM-DD, YYYY-MM-DD HH:mm:SS +HH:MM, among others.
    When the timezone is not provided, UTC+0 will be set as default
    (using `dateutil.tz.tzutc` object).

    :param ts: string to convert

    :returns: a datetime object

    :raises IvalidDateError: when the given string cannot be converted into
        a valid date
    """
    if not ts:
        raise InvalidDateError(date=str(ts))

    try:
        # Try to remove parentheses section from dates
        # because they cannot be parsed, like in
        # 'Wed, 26 Oct 2005 15:20:32 -0100 (GMT+1)'.
        ts = ts.split('(')[0]

        dt = dateutil.parser.parse(ts)

        if not dt.tzinfo:
            dt = dt.replace(tzinfo=dateutil.tz.tzutc())
        return dt
    except Exception:
        raise InvalidDateError(date=str(ts))


def unixtime_to_datetime(ut):
    """Convert a unixtime timestamp to a datetime object.

    The function converts a timestamp in Unix format to a
    datetime object. UTC timezone will also be set.

    :param ut: Unix timestamp to convert

    :returns: a datetime object

    :raises InvalidDateError: when the given timestamp cannot be
        converted into a valid date
    """
    try:
        dt = datetime.datetime.utcfromtimestamp(ut)
        dt = dt.replace(tzinfo=dateutil.tz.tzutc())
        return dt
    except Exception:
        raise InvalidDateError(date=str(ut))
