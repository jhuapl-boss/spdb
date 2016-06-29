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
from unittest.mock import patch
from mockredis import mock_strict_redis_client

from spdb.project import BossResourceBasic
from spdb.spatialdb import CacheStateDB
from spdb.spatialdb.test import CacheStateDBTestMixin


import redis

from bossutils import configuration
import bossutils


class MockBossConfig(bossutils.configuration.BossConfig):
    """Basic mock for BossConfig to contain the properties needed for this test"""
    def __init__(self):
        super().__init__()
        self.config["aws"]["cache-state-db"] = 1


class IntegrationCacheStateDBTestMixin(object):

    def test_create_page_in_channel(self):
        """Test if cache cuboid keys are formatted properly"""
        csdb = CacheStateDB(self.config_data)
        ch1 = csdb.create_page_in_channel()
        ch2 = csdb.create_page_in_channel()
        assert ch1 != ch2
        assert self.state_client.exists(ch1) == True
        assert self.state_client.exists(ch2) == True


@patch('bossutils.configuration.BossConfig', MockBossConfig)
class TestCacheStateDB(CacheStateDBTestMixin, IntegrationCacheStateDBTestMixin, unittest.TestCase):

    def setUp(self):
        """ Create a diction of configuration values for the test resource. """
        self.patcher = patch('redis.StrictRedis', mock_strict_redis_client)
        self.mock_tests = self.patcher.start()

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

        self.status_client = redis.StrictRedis(host=self.config["aws"]["cache-state"],
                                               port=6379, db=self.config["aws"]["cache-state-db"],
                                               decode_responses=True)
        self.status_client.flushdb()

        self.config_data = {"state_client": self.status_client}

    def tearDown(self):
        self.mock_tests = self.patcher.stop()
