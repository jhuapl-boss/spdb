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

import redis

import numpy as np
import blosc

from bossutils import configuration
import bossutils


class MockBossConfig(bossutils.configuration.BossConfig):
    """Basic mock for BossConfig to contain the properties needed for this test"""
    def __init__(self):
        super().__init__()
        self.config["aws"]["cache-state-db"] = 1


class CacheStateDBTestMixin(object):

    def test_generate_cached_cuboid_keys(self):
        """Test if cache cuboid keys are formatted properly"""
        rkv = RedisKVIO(self.config_data)
        keys = rkv.generate_cached_cuboid_keys(self.resource, 2, [0, 1, 2], [34, 35, 36])
        assert len(keys) == 9
        assert keys[0] == "CACHED-CUBOID&4&2&1&2&0&34"
        assert keys[2] == "CACHED-CUBOID&4&2&1&2&0&36"
        assert keys[5] == "CACHED-CUBOID&4&2&1&2&1&36"
        assert keys[8] == "CACHED-CUBOID&4&2&1&2&2&36"



@patch('redis.StrictRedis', mock_strict_redis_client)
@patch('bossutils.configuration.BossConfig', MockBossConfig)
class TestRedisKVIOImageData(CacheStateDBTestMixin, unittest.TestCase):

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

        self.config_data = {"state_client": self.cache_client}

    def tearDown(self):
        self.mock_tests = self.patcher.stop()
