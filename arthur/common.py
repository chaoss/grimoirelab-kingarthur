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
#     Miguel Ángel Fernández <mafesan@bitergia.com>
#

ARCHIVES_DEFAULT_PATH = '~/.arthur/archives/'

CH_PUBSUB = 'ch_arthur'

Q_ARCHIVE_JOBS = 'archive'
Q_CREATION_JOBS = 'create'
Q_RETRYING_JOBS = 'retry'
Q_UPDATING_JOBS = 'update'
Q_STORAGE_ITEMS = 'items'

TIMEOUT = 3600 * 24

WAIT_FOR_QUEUING = 10  # In seconds

MAX_JOB_RETRIES = 3
