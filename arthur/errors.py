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


class BaseError(Exception):
    """Base class for Arthur exceptions.

    Derived classes can overwrite the error message declaring ``message``
    property.
    """
    message = "Arthur base error"

    def __init__(self, **kwargs):
        super().__init__()
        self.msg = self.message % kwargs

    def __str__(self):
        return self.msg


class AlreadyExistsError(BaseError):
    """Exception raised when an element already exists"""

    message = "%(element)s already exists"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.element = kwargs['element']


class NotFoundError(BaseError):
    """Exception raised when an element is not found"""

    message = "%(element)s not found"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.element = kwargs['element']


class TaskRegistryError(BaseError):
    """Generic error for TaskRegistry"""

    message = "%(cause)s"
