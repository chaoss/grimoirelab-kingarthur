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

import codecs
import os
import re

from distutils.core import setup


here = os.path.abspath(os.path.dirname(__file__))
version_py = os.path.join(here, 'arthur', '_version.py')

with codecs.open(version_py, 'r', encoding='utf-8') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)


setup(name="arthur",
      version=version,
      author="Bitergia",
      author_email="sduenas@bitergia.com",
      packages=["arthur"],
      install_requires=[
        'python-dateutil>=2.6.0',
        'redis>=2.10.0',
        'rq>=0.6.0',
        'cherrypy>=8.1',
        'perceval>=0.5.0'
      ],
      scripts=["bin/arthur", "bin/arthurd", "bin/arthurw"])
