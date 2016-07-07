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

from bossutils import configuration


class CacheStateDBTestMixin(object):

    def test_add_cache_misses(self):
        """Test if cache cuboid keys are formatted properly"""
        csdb = CacheStateDB(self.config_data)
        assert not self.state_client.get("CACHE-MISS")

        keys = ['key1', 'key2', 'key3']

        csdb.add_cache_misses(keys)

        for k in keys:
            assert k == self.state_client.lpop("CACHE-MISS").decode()

    def test_project_locked(self):
        """Test if a channel/layer is locked"""
        csdb = CacheStateDB(self.config_data)

        assert csdb.project_locked("1&1&1") == False

        self.state_client.set("WRITE-LOCK&1&1&1", 'true')

        assert csdb.project_locked("1&1&1") == True

    def test_add_to_page_out(self):
        """Test if a cube is in page out"""
        csdb = CacheStateDB(self.config_data)

        temp_page_out_key = "temp"
        lookup_key = "1&1&1"
        resolution = 1
        morton = 234
        time_sample = 1

        page_out_key = "PAGE-OUT&{}&{}".format(lookup_key, resolution)
        assert not self.state_client.get(page_out_key)

        assert not csdb.in_page_out(temp_page_out_key, lookup_key, resolution, morton, time_sample)

        success, in_page_out = csdb.add_to_page_out(temp_page_out_key, lookup_key, resolution, morton, time_sample)
        assert success
        assert not in_page_out

        assert csdb.in_page_out(temp_page_out_key, lookup_key, resolution, morton, time_sample)

    def test_add_to_delayed_write(self):
        """Test if a cube is in delayed write"""
        csdb = CacheStateDB(self.config_data)

        lookup_key = "1&1&1"
        resolution = 1
        time_sample = 1
        morton = 234
        write_cuboid_key1 = "WRITE-CUBOID&{}&{}&{}&daadsfjk".format(lookup_key,
                                                                    resolution,
                                                                    time_sample,
                                                                    morton)
        write_cuboid_key2 = "WRITE-CUBOID&{}&{}&{}&fghfghjg".format(lookup_key,
                                                                    resolution,
                                                                    time_sample,
                                                                    morton)

        keys = csdb.get_delayed_write_keys()
        assert not keys

        csdb.add_to_delayed_write(write_cuboid_key1, lookup_key, resolution, morton, time_sample)
        csdb.add_to_delayed_write(write_cuboid_key2, lookup_key, resolution, morton, time_sample)

        keys = csdb.get_delayed_write_keys()
        assert len(keys) == 1
        assert keys[0][1].decode() == write_cuboid_key1

        keys = csdb.get_delayed_write_keys()
        assert len(keys) == 1
        assert keys[0][1].decode() == write_cuboid_key2

        keys = csdb.get_delayed_write_keys()
        assert not keys


@patch('redis.StrictRedis', mock_strict_redis_client)
class TestCacheStateDB(CacheStateDBTestMixin, unittest.TestCase):

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

        self.state_client = redis.StrictRedis(host=self.config["aws"]["cache-state"],
                                              port=6379, db=1,
                                              decode_responses=False)
        self.state_client.flushdb()

        self.config_data = {"state_client": self.state_client}

    def tearDown(self):
        self.mock_tests = self.patcher.stop()
