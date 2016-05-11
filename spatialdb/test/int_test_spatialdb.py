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
from spdb.spatialdb.test import SpatialDBImageDataTestMixin

import copy
import redis

from bossutils import configuration

CONFIG_UNMOCKED = configuration.BossConfig()


class MockBossIntegrationConfig:
    """Mock the config to set the database to 1 instead of the default 0"""
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
class TestIntegrationSpatialDBImageData(SpatialDBImageDataTestMixin, unittest.TestCase):

    def setUp(self):
        """ Create a diction of configuration values for the test resource. """
        self.patcher = patch('configparser.ConfigParser', MockBossIntegrationConfig)
        self.mock_tests = self.patcher.start()

        data = {}
        data['collection'] = {}
        data['collection']['name'] = "col1"
        data['collection']['description'] = "Test collection 1"

        data['coord_frame'] = {}
        data['coord_frame']['name'] = "coord_frame_1"
        data['coord_frame']['description'] = "Test coordinate frame"
        data['coord_frame']['x_start'] = 0
        data['coord_frame']['x_stop'] = 2000
        data['coord_frame']['y_start'] = 0
        data['coord_frame']['y_stop'] = 5000
        data['coord_frame']['z_start'] = 0
        data['coord_frame']['z_stop'] = 200
        data['coord_frame']['x_voxel_size'] = 4
        data['coord_frame']['y_voxel_size'] = 4
        data['coord_frame']['z_voxel_size'] = 35
        data['coord_frame']['voxel_unit'] = "nanometers"
        data['coord_frame']['time_step'] = 0
        data['coord_frame']['time_step_unit'] = "na"

        data['experiment'] = {}
        data['experiment']['name'] = "exp1"
        data['experiment']['description'] = "Test experiment 1"
        data['experiment']['num_hierarchy_levels'] = 7
        data['experiment']['hierarchy_method'] = 'slice'
        data['experiment']['base_resolution'] = 0

        data['channel_layer'] = {}
        data['channel_layer']['name'] = "ch1"
        data['channel_layer']['description'] = "Test channel 1"
        data['channel_layer']['is_channel'] = True
        data['channel_layer']['datatype'] = 'uint8'
        data['channel_layer']['max_time_sample'] = 0

        data['boss_key'] = 'col1&exp1&ch1'
        data['lookup_key'] = '4&2&1'

        self.resource8 = BossResourceBasic(data)

        data16 = copy.deepcopy(data)
        data16['channel_layer']['datatype'] = 'uint16'
        self.resource16 = BossResourceBasic(data16)

        # Get direct clients to redis to make sure data is getting written properly
        config = configuration.BossConfig()
        self.cache_client = redis.StrictRedis(host=config["aws"]["cache"], port=6379,
                                              db=config["aws"]["cache-db"])

        self.status_client = redis.StrictRedis(host=config["aws"]["cache-state"], port=6379,
                                               db=config["aws"]["cache-state-db"])

    def tearDown(self):
        # Stop mocking
        self.mock_tests = self.patcher.stop()
        self.cache_client.flushdb()
        self.status_client.flushdb()

    def get_num_cache_keys(self, spdb):
        return len(self.cache_client.keys("*"))

    def get_num_status_keys(self, spdb):
        return len(self.status_client.keys("*"))
