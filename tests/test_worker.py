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

import pickle
import sys
import unittest

import rq

if not '..' in sys.path:
    sys.path.insert(0, '..')

from arthur.common import CH_PUBSUB
from arthur.worker import ArthurWorker

from tests import TestBaseRQ, mock_sum, mock_failure


class TestArthurWorker(TestBaseRQ):
    """Unit tests for ArthurWorker class"""

    def test_publish_finished_job_status(self):
        """Test whether the worker publishes the status of a finished job"""

        pubsub = self.conn.pubsub()
        pubsub.subscribe(CH_PUBSUB)

        q = rq.Queue('foo')
        w = ArthurWorker([q])

        job_a = q.enqueue(mock_sum, a=2, b=3)
        job_b = q.enqueue(mock_failure)

        status = w.work(burst=True)
        self.assertEqual(status, True)

        # Ignore the first message because it is a
        # subscription notification
        _ = pubsub.get_message()
        msg_a = pubsub.get_message()
        msg_b = pubsub.get_message()

        data = pickle.loads(msg_a['data'])
        self.assertEqual(job_a.result, 5)
        self.assertEqual(data['job_id'], job_a.id)
        self.assertEqual(data['status'], 'finished')

        data = pickle.loads(msg_b['data'])
        self.assertEqual(job_b.result, None)
        self.assertEqual(data['job_id'], job_b.id)
        self.assertEqual(data['status'], 'failed')


if __name__ == "__main__":
    unittest.main()
