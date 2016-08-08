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

import redis

from bossutils import configuration
import time


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
                break
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

    def setUp(self):
        """ Create a diction of configuration values for the test resource. """
        self.data = {}
        self.data['collection'] = {}
        self.data['collection']['name'] = "col1"
        self.data['collection']['description'] = "Test collection 1"

        self.data['coord_frame'] = {}
        self.data['coord_frame']['name'] = "coord_frame_1"
        self.data['coord_frame']['description'] = "Test coordinate frame"
        self.data['coord_frame']['x_start'] = 0
        self.data['coord_frame']['x_stop'] = 2000
        self.data['coord_frame']['y_start'] = 0
        self.data['coord_frame']['y_stop'] = 5000
        self.data['coord_frame']['z_start'] = 0
        self.data['coord_frame']['z_stop'] = 200
        self.data['coord_frame']['x_voxel_size'] = 4
        self.data['coord_frame']['y_voxel_size'] = 4
        self.data['coord_frame']['z_voxel_size'] = 35
        self.data['coord_frame']['voxel_unit'] = "nanometers"
        self.data['coord_frame']['time_step'] = 0
        self.data['coord_frame']['time_step_unit'] = "na"

        self.data['experiment'] = {}
        self.data['experiment']['name'] = "exp1"
        self.data['experiment']['description'] = "Test experiment 1"
        self.data['experiment']['num_hierarchy_levels'] = 7
        self.data['experiment']['hierarchy_method'] = 'slice'

        self.data['channel_layer'] = {}
        self.data['channel_layer']['name'] = "ch1"
        self.data['channel_layer']['description'] = "Test channel 1"
        self.data['channel_layer']['is_channel'] = True
        self.data['channel_layer']['datatype'] = 'uint8'
        self.data['channel_layer']['max_time_step'] = 0

        self.data['boss_key'] = 'col1&exp1&ch1'
        self.data['lookup_key'] = '4&2&1'

        self.resource = BossResourceBasic(self.data)

        self.config = configuration.BossConfig()

        self.state_client = redis.StrictRedis(host=self.config["aws"]["cache-state"],
                                              port=6379, db=1,
                                              decode_responses=False)
        self.state_client.flushdb()

        self.config_data = {"state_client": self.state_client}
