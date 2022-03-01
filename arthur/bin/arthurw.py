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
#     Miguel Ángel Fernández <mafesan@bitergia.com>
#

import argparse
import logging
import sys

import redis
import rq
import rq.utils

from arthur.common import (Q_ARCHIVE_JOBS,
                           Q_CREATION_JOBS,
                           Q_RETRYING_JOBS,
                           Q_UPDATING_JOBS,
                           CH_PUBSUB)
from arthur.worker import ArthurWorker


ARTHURW_USAGE_MSG = \
    """%(prog)s [-g] [-d <database>] [--burst] [<queue1>...<queueN>] | --help """

ARTHURW_DESC_MSG = \
    """King Arthur's worker. It will run Perceval jobs on the quest
to retrieve data from software repositories.

positional arguments:
   queues               list of queues this worker will listen for
                        ('create' and 'update', by default)

optional arguments:
  -?, --help            show this help message and exit
  -g, --debug           set debug mode on
  -d, --database        URL database connection (default: 'redis://localhost/8')
  -b, --burst           Run in burst mode (quit after all work is done)
  --pubsub-channel      name of redis pubsub channel on which to publish updates
"""

# Logging formats
ARTHURW_LOG_FORMAT = "[%(asctime)s] - %(message)s"
ARTHURW_DEBUG_LOG_FORMAT = "[%(asctime)s - %(name)s - %(levelname)s] - %(message)s"


def main():
    """Run King Arthur's worker"""

    args = parse_args()

    conn = connect_to_redis(args.database)

    queues = args.queues or [Q_ARCHIVE_JOBS, Q_CREATION_JOBS, Q_RETRYING_JOBS, Q_UPDATING_JOBS]
    burst = args.burst

    configure_logging(args.debug)

    try:
        queues = [rq.Queue(queue, connection=conn) for queue in queues]

        w = ArthurWorker(queues, connection=conn)
        w.pubsub_channel = args.pubsub_channel
        w.work(burst=burst)
    except ConnectionError as e:
        print(e)
        sys.exit(1)


def parse_args():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(usage=ARTHURW_USAGE_MSG,
                                     description=ARTHURW_DESC_MSG,
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     add_help=False)

    parser.add_argument('-?', '--help', action='help',
                        help=argparse.SUPPRESS)
    parser.add_argument('-g', '--debug', dest='debug',
                        action='store_true',
                        help=argparse.SUPPRESS)
    parser.add_argument('-d', '--database', dest='database',
                        default='redis://localhost/8',
                        help=argparse.SUPPRESS)
    parser.add_argument('-b', '--burst', dest='burst',
                        action='store_true',
                        help=argparse.SUPPRESS)
    parser.add_argument('queues', nargs='*',
                        help=argparse.SUPPRESS)
    parser.add_argument('--pubsub-channel', dest='pubsub_channel',
                        default=CH_PUBSUB,
                        help=argparse.SUPPRESS)
    return parser.parse_args()


def configure_logging(debug=False):
    """Configure Arthur's worker logging.

    The function configures the log messages produced by Arthur
    worker and Perceval backends. By default, log messages are sent
    to stderr. Set the parameter `debug` to activate the debug
    mode.

    :param log_path: path where logs will be stored
    :param debug: set the debug mode
    """
    logger = logging.getLogger()

    if not debug:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(fmt=ARTHURW_LOG_FORMAT)
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('urrlib3').setLevel(logging.WARNING)
    else:
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(fmt=ARTHURW_DEBUG_LOG_FORMAT)

    handler = rq.utils.ColorizingStreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def connect_to_redis(url):
    """Create a connection with a Redis database"""

    conn = redis.StrictRedis.from_url(url)

    return conn


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        s = "\n\nReceived Ctrl-C or other break signal. Exiting.\n"
        sys.stderr.write(s)
        sys.exit(0)
