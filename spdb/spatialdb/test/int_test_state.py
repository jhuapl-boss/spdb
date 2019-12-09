# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from datetime import datetime

from spdb.project import BossResourceBasic
from spdb.spatialdb import CacheStateDB
from spdb.spatialdb.test import CacheStateDBTestMixin
from spdb.spatialdb.error import SpdbError
from spdb.spatialdb.test.setup import load_test_config_file

import redis

import time
from spdb.project.test.resource_setup import get_image_dict


class IntegrationCacheStateDBTestMixin(object):

    def test_page_in_channel(self):
        """Test Page in channel creation and basic message passing"""
        # Create test instance
        csdb1 = CacheStateDB(self.config_data)
        csdb2 = CacheStateDB(self.config_data)

        # Create page in channel in the first instance
        ch = csdb1.create_page_in_channel()
        time.sleep(1.5)

        # Publish a message
        csdb2.notify_page_in_complete(ch, "MY_TEST_KEY")

        # Get message (ignore first message which is the subscribe)
        while True:
            msg = csdb1.status_client_listener.get_message()
            if not msg:
                continue
            if msg['type'] == "message":
                break

        assert msg['channel'].decode() == ch
        assert msg['data'].decode() == "MY_TEST_KEY"

    def test_wait_for_page_in_timeout(self):
        """Test to make sure page in timeout works properly"""
        start_time = datetime.now()
        with self.assertRaises(SpdbError):
            csdb = CacheStateDB(self.config_data)
            ch = csdb.create_page_in_channel()

            csdb.wait_for_page_in(["MY_TEST_KEY1", "MY_TEST_KEY2"], ch, 1)

        assert (datetime.now() - start_time).seconds < 3

    def test_wait_for_page_in(self):
        """Test to make sure waiting for all the keys to be paged in works properly"""
        # Create test instance
        csdb1 = CacheStateDB(self.config_data)
        csdb2 = CacheStateDB(self.config_data)

        # Create page in channel in the first instance
        ch = csdb1.create_page_in_channel()

        # Publish a message
        csdb2.notify_page_in_complete(ch, "MY_TEST_KEY1")
        csdb2.notify_page_in_complete(ch, "MY_TEST_KEY2")

        # Wait for page in
        csdb1.wait_for_page_in(["MY_TEST_KEY1", "MY_TEST_KEY2"], ch, 5)


class TestCacheStateDB(CacheStateDBTestMixin, IntegrationCacheStateDBTestMixin, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Setup the redis client at the start of the test"""
        cls.data = get_image_dict()
        cls.resource = BossResourceBasic(cls.data)

        cls.config = load_test_config_file()

        cls.state_client = redis.StrictRedis(host=cls.config["aws"]["cache-state"], port=6379, db=1,
                                             decode_responses=False)

        cls.config_data = {"state_client": cls.state_client}

    def setUp(self):
        """Clean out the cache DB between tests"""
        self.state_client.flushdb()
