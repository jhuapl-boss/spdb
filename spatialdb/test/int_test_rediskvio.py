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

from spdb.project import BossResourceBasic
from spdb.spatialdb.test import RedisKVIOTestMixin

import redis

from bossutils import configuration

CONFIG_UNMOCKED = configuration.BossConfig()


class MockBossIntegrationConfig:
    """Basic mock for BossConfig to contain the properties needed for this test"""
    def __init__(self):
        self.config = {}
        self.config["aws"] = {}
        self.config["aws"]["cache"] = CONFIG_UNMOCKED["aws"]["cache"]
        self.config["aws"]["cache-state"] = CONFIG_UNMOCKED["aws"]["cache-state"]
        self.config["aws"]["cache-db"] = 1
        self.config["aws"]["cache-state-db"] = 1

    def read(self, filename):
        pass

    def __getitem__(self, key):
        return self.config[key]


@patch('configparser.ConfigParser', MockBossIntegrationConfig)
class TestIntegrationRedisKVIOImageData(RedisKVIOTestMixin, unittest.TestCase):

    def setUp(self):
        """ Create a diction of configuration values for the test resource. """
        self.patcher = patch('configparser.ConfigParser', MockBossIntegrationConfig)
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

        self.redis_client = None
        
        # Flush the db for testing
        self.cache_client = redis.StrictRedis(host=self.config["aws"]["cache"], port=6379,
                                              db=self.config["aws"]["cache-db"])
        self.cache_client.flushdb()

        self.status_client = redis.StrictRedis(host=self.config["aws"]["cache-state"], port=6379,
                                               db=self.config["aws"]["cache-state-db"])
        self.status_client.flushdb()

    def tearDown(self):

        # Flush the db after
        self.cache_client.flushdb()
        self.status_client.flushdb()

        # Stop patching
        self.mock_tests = self.patcher.stop()
