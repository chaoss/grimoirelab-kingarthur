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
#     Jesus M. Gonzalez-Barahona <jgb@gsyc.es>
#

import codecs
import os.path
import re
import sys
import unittest

from setuptools import setup, Command


here = os.path.abspath(os.path.dirname(__file__))
readme_md = os.path.join(here, 'README.md')
version_py = os.path.join(here, 'arthur', '_version.py')

# Get the package description from the README.md file
with codecs.open(readme_md, encoding='utf-8') as f:
    long_description = f.read()

with codecs.open(version_py, 'r', encoding='utf-8') as fd:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fd.read(), re.MULTILINE).group(1)


class TestCommand(Command):

    user_options = []
    __dir__ = os.path.dirname(os.path.realpath(__file__))

    def initialize_options(self):
        os.chdir(os.path.join(self.__dir__, 'tests'))

    def finalize_options(self):
        pass

    def run(self):
        test_suite = unittest.TestLoader().discover('.', pattern='test_*.py')
        result = unittest.TextTestRunner(buffer=True).run(test_suite)
        sys.exit(not result.wasSuccessful())


cmdclass = {'test': TestCommand}

setup(name="kingarthur",
      description="Distributed job queue platform for scheduling Perceval jobs",
      long_description=long_description,
      long_description_content_type='text/markdown',
      url="https://github.com/chaoss/grimoirelab-kingarthur",
      version=version,
      author="Bitergia",
      author_email="sduenas@bitergia.com",
      license="GPLv3",
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'Topic :: Software Development',
          'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
          'Programming Language :: Python :: 3'
      ],
      keywords="development repositories analytics perceval rq",
      packages=[
          'arthur'
      ],
      python_requires='>=3.7',
      setup_requires=['wheel'],
      install_requires=[
          'python-dateutil>=2.8.0',
          'redis==3.0.0',
          'rq==1.0.0',
          'cheroot>=8.2.1',
          'cherrypy>=17.4.2',
          'perceval>=0.12.23',
          'grimoirelab-toolkit>=0.1.10'
      ],
      tests_require=['httpretty==0.8.6', 'fakeredis'],
      test_suite='tests',
      entry_points={
          'console_scripts': [
              'arthurd=arthur.bin.arthurd:main',
              'arthurw=arthur.bin.arthurw:main'
          ]
      },
      cmdclass=cmdclass,
      zip_safe=False)
