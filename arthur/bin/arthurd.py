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
import configparser
import logging
import os.path
import sys

import cherrypy
import redis

from arthur.common import ARCHIVES_DEFAULT_PATH, CH_PUBSUB
from arthur.server import ArthurServer
from arthur.writers import ElasticItemsWriter


ARTHURD_USAGE_MSG = \
    """%(prog)s [-c <file>] [-g] [-h <host>] [-p <port>] [-d <database>]
               [--es-index <index>] [--log-path <path>] [--archive-path <cpath>]
               [--no-archive] [--no-daemon] | --help """

ARTHURD_DESC_MSG = \
    """King Arthur commands his loyal knight Perceval on the quest
to retrieve data from software repositories.

This command runs an Arthur daemon that waits for HTTP requests
on port 8080. Repositories to analyze are added using an REST API.
Repositories are transformed into Perceval jobs that will be
scheduled and run using a distributed job queue platform.

optional arguments:
  -?, --help            show this help message and exit
  -c FILE, --config FILE
                        set configuration file
  -g, --debug           set debug mode on
  -h, --host            set the host name or IP address on which to listen for connections
  -p, --port            set listening TCP port (default: 8080)
  -d, --database        URL database connection (default: 'redis://localhost/8')
  -s, --sync            work in synchronous mode (without workers)
  --es-index            output ElasticSearch server index
  --log-path            path where logs are stored
  --archive-path        path to archive manager directory
  --no-archive          do not archive fetched raw data
  --no-daemon           do not run arthur in daemon mode
  --pubsub-channel      name of redis pubsub channel on which to publish updates
"""

# Logging formats
ARTHURD_LOG_FORMAT = "[%(asctime)s] - %(message)s"
ARTHURD_DEBUG_LOG_FORMAT = "[%(asctime)s - %(name)s - %(levelname)s] - %(message)s"


def main():

    args = parse_args()
    run_daemon = not args.no_daemon

    configure_logging(args.log_path, args.debug, run_daemon)

    logging.info("King Arthur is on command.")
    if not run_daemon:
        logging.info("Log path is ignored, logs will be redirected to stderr.")

    conn = connect_to_redis(args.database)

    if args.es_index:
        writer = ElasticItemsWriter(args.es_index)
    else:
        writer = None

    # Set archive manager directory
    base_archive_path = None if args.no_archive else args.archive_path

    app = ArthurServer(conn, base_archive_path,
                       async_mode=args.sync_mode,
                       pubsub_channel=args.pubsub_channel,
                       writer=writer)

    if run_daemon:
        logging.info("King Arthur running in daemon mode.")
        cherrypy.process.plugins.Daemonizer(cherrypy.engine).subscribe()

    cfg = {
        'server.socket_host': args.host,
        'server.socket_port': args.port
    }
    cherrypy.config.update(cfg)

    cherrypy.quickstart(app)


def parse_args():
    """Parse command line arguments"""

    # Parse first configuration file parameter
    config_parser = create_config_arguments_parser()
    config_args, args = config_parser.parse_known_args()

    # And then, read default parameters from a configuration file
    if config_args.config_file:
        defaults = read_config_file(config_args.config_file)
    else:
        defaults = {}

    # Parse common arguments using the command parser
    parser = create_common_arguments_parser(defaults)

    # Parse arguments
    return parser.parse_args(args)


def read_config_file(filepath):
    """Read a Arthur configuration file.

    This function reads common configuration parameters
    from the given file.

    :param filepath: path to the configuration file

    :returns: a configuration parameters dictionary
    """
    config = configparser.ConfigParser()
    config.read(filepath)

    args = {}
    sections = ['arthur', 'connection', 'elasticsearch', 'redis']

    for section in sections:
        if section in config.sections():
            args.update(dict(config.items(section)))

    args = cast_boolean_args(args)
    return args


def cast_boolean_args(args):
    """Check args dictionary setting boolean values properly

    :param args: Dictionary of arguments

    :return: The same dictionary but with proper boolean values
    """
    for key in args.keys():
        value = args[key]
        if (type(value) == str) and (value in ["True", "False"]):
            if value == "True":
                args[key] = True
            elif value == "False":
                args[key] = False

    return args


def create_common_arguments_parser(defaults):
    """Set parser for common arguments"""

    parser = argparse.ArgumentParser(usage=ARTHURD_USAGE_MSG,
                                     description=ARTHURD_DESC_MSG,
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     add_help=False)

    # Options
    parser.add_argument('-?', '--help', action='help',
                        help=argparse.SUPPRESS)
    parser.add_argument('-g', '--debug', dest='debug',
                        action='store_true',
                        help=argparse.SUPPRESS)
    parser.add_argument('-h', '--host', dest='host',
                        default='127.0.0.1',
                        help=argparse.SUPPRESS)
    parser.add_argument('-p', '--port', dest='port',
                        type=int, default=8080,
                        help=argparse.SUPPRESS)
    parser.add_argument('-d', '--database', dest='database',
                        default='redis://localhost/8',
                        help=argparse.SUPPRESS)
    parser.add_argument('-s', '--sync', dest='sync_mode',
                        action='store_false',
                        help=argparse.SUPPRESS)
    parser.add_argument('--es-index', dest='es_index',
                        help=argparse.SUPPRESS)
    parser.add_argument('--log-path', dest='log_path',
                        default=os.path.expanduser('~/.arthur/logs/'),
                        help=argparse.SUPPRESS)
    parser.add_argument('--archive-path', dest='archive_path',
                        default=os.path.expanduser(ARCHIVES_DEFAULT_PATH),
                        help=argparse.SUPPRESS)
    parser.add_argument('--no-archive', dest='no_archive',
                        action='store_true',
                        help=argparse.SUPPRESS)
    parser.add_argument('--no-daemon', dest='no_daemon',
                        action='store_true',
                        help=argparse.SUPPRESS)
    parser.add_argument('--pubsub-channel', dest='pubsub_channel',
                        default=CH_PUBSUB,
                        help=argparse.SUPPRESS)

    # Set default values
    parser.set_defaults(**defaults)

    return parser


def create_config_arguments_parser():
    """Set parser for configuration file"""

    parser = argparse.ArgumentParser(usage=ARTHURD_USAGE_MSG,
                                     add_help=False)

    parser.add_argument('-c', '--config', dest='config_file',
                        help=argparse.SUPPRESS)

    # Set default values
    defaults = {'config_file': os.path.expanduser('~/.arthur/config.ini')}

    parser.set_defaults(**defaults)

    return parser


def configure_logging(log_path, debug=False, run_daemon=True):
    """Configure Arthur daemon logging.

    The function configures the log messages produced by Arthur
    and Perceval backends. By default, log messages are sent
    to stderr. Set the parameter `debug` to activate the debug
    mode.

    :param log_path: path where logs will be stored
    :param debug: set the debug mode
    :param run_daemon: run Arthur in daemon mode
    """
    cherrypy.config.update({'log.screen': False})

    if run_daemon:
        if not os.path.exists(log_path):
            os.makedirs(log_path)

        logfile = os.path.join(log_path, 'arthur.log')
        if not debug:
            logging.basicConfig(filename=logfile,
                                level=logging.INFO,
                                format=ARTHURD_LOG_FORMAT)
            logging.getLogger('requests').setLevel(logging.WARNING)
            logging.getLogger('urrlib3').setLevel(logging.WARNING)
        else:
            logging.basicConfig(filename=logfile,
                                level=logging.DEBUG,
                                format=ARTHURD_DEBUG_LOG_FORMAT)
    else:
        if not debug:
            logging.basicConfig(stream=sys.stderr,
                                level=logging.INFO,
                                format=ARTHURD_LOG_FORMAT)
            logging.getLogger('requests').setLevel(logging.WARNING)
            logging.getLogger('urrlib3').setLevel(logging.WARNING)
        else:
            logging.basicConfig(stream=sys.stderr,
                                level=logging.DEBUG,
                                format=ARTHURD_DEBUG_LOG_FORMAT)


def connect_to_redis(db_url):
    """Create a connection with a Redis database"""

    conn = redis.StrictRedis.from_url(db_url)
    logging.debug("Redis connection established with %s.", db_url)

    return conn


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        s = "\n\nReceived Ctrl-C or other break signal. Exiting.\n"
        sys.stderr.write(s)
        sys.exit(0)
