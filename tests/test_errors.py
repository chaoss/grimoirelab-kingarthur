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

import unittest

import arthur.errors as errors


# Mock classes to test BaseError class
class MockErrorNoArgs(errors.BaseError):
    message = "Mock error without args"


class MockErrorArgs(errors.BaseError):
    message = "Mock error with args. Error: %(code)s %(msg)s"


class TestBaseError(unittest.TestCase):

    def test_subclass_with_no_args(self):
        """Check subclasses that do not require arguments.

        Arguments passed to the constructor should be ignored.
        """
        e = MockErrorNoArgs(code=1, msg='Fatal error')

        self.assertEqual(str(e), "Mock error without args")

    def test_subclass_args(self):
        """Check subclasses that require arguments"""

        e = MockErrorArgs(code=1, msg='Fatal error')

        self.assertEqual(str(e), "Mock error with args. Error: 1 Fatal error")

    def test_subclass_invalid_args(self):
        """Check when required arguments are not given.

        When this happens, it raises a KeyError exception.
        """
        kwargs = {'code': 1, 'error': 'Fatal error'}
        self.assertRaises(KeyError, MockErrorArgs, **kwargs)


class TestAlreadyExistsError(unittest.TestCase):
    """Tests for AlreadyExistsError class"""

    def test_message(self):
        """Make sure that prints the correct error"""

        e = errors.AlreadyExistsError(element='task1')
        self.assertEqual(str(e), 'task1 already exists')

    def test_entity(self):
        """Entity attribute should have a value"""

        e = errors.AlreadyExistsError(element='task1')
        self.assertEqual(e.element, 'task1')


class TestNotFoundError(unittest.TestCase):

    def test_message(self):
        """Make sure that prints the correct error"""

        e = errors.NotFoundError(element='repository')
        self.assertEqual(str(e), 'repository not found')

    def test_entity(self):
        """Entity attribute should have a value"""

        e = errors.NotFoundError(element='repository')
        self.assertEqual(e.element, 'repository')


class TestTaskRegistryError(unittest.TestCase):

    def test_message(self):
        """Make sure that prints the correct error"""

        e = errors.TaskRegistryError(cause='error on registry')
        self.assertEqual('error on registry', str(e))


if __name__ == "__main__":
    unittest.main()
