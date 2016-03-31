"""
Copyright 2016 The Johns Hopkins University Applied Physics Laboratory

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import unittest
from unittest.mock import patch

import os

from spdb.project import BossResourceBasic
from spdb.spatialdb import RedisKVIO

import redis

import numpy as np

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


@unittest.skipIf(os.environ.get('UNIT_ONLY') is not None, "Only running unit tests")
class TestIntegrationRedisKVIOImageDataOneTimeSample(unittest.TestCase):

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

        self.data['boss_key'] = ['col1&exp1&ch1']
        self.data['lookup_key'] = ['4&2&1&0']

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

    def test_get_cache_index_base_key(self):
        """Test the base key getter function for the cuboid index (cuboids that exist in the cache"""
        self.cache_client.flushdb()
        self.status_client.flushdb()
        rkv = RedisKVIO(self.cache_client, self.status_client)
        assert rkv.get_cache_index_base_key(self.resource, 2) == ["CUBOID_IDX&4&2&1&0&2"]

    def test_get_cache_base_key(self):
        """Test the base key getter function for the cuboids"""
        self.cache_client.flushdb()
        self.status_client.flushdb()
        rkv = RedisKVIO(self.cache_client, self.status_client)
        assert rkv.get_cache_base_key(self.resource, 2) == ["CUBOID&4&2&1&0&2"]

    def test_generate_cache_index_keys_single(self):
        """Test the generate cache index key generation for a single key"""
        self.cache_client.flushdb()
        self.status_client.flushdb()
        rkv = RedisKVIO(self.cache_client, self.status_client)

        keys = rkv.generate_cache_index_keys(self.resource, 2)

        assert isinstance(keys, list)
        assert len(keys) == 1
        assert keys[0] == "CUBOID_IDX&4&2&1&0&2"

    def test_generate_cuboid_keys(self):
        """Test the generate cache cuboid keys"""
        self.cache_client.flushdb()
        self.status_client.flushdb()
        rkv = RedisKVIO(self.cache_client, self.status_client)
        resolution = 5
        morton_ids = list(range(2345, 2350))

        keys = rkv.generate_cuboid_keys(self.resource, resolution, morton_ids)

        assert isinstance(keys, list)
        assert len(keys) == 5
        assert keys[0] == "CUBOID&4&2&1&0&5&2345"
        assert keys[1] == "CUBOID&4&2&1&0&5&2346"
        assert keys[2] == "CUBOID&4&2&1&0&5&2347"
        assert keys[3] == "CUBOID&4&2&1&0&5&2348"
        assert keys[4] == "CUBOID&4&2&1&0&5&2349"

    #def test_generate_keys_multiple(self):
    #    # TODO Move to time sample tests once fully implemented
    #    """Test the generate cache index key generation for a multiple time samples"""
    #    self.cache_client.flushdb()
    #    self.status_client.flushdb()
    #    rkv = RedisKVIO(self.cache_client, self.status_client)
#
    #    self.resource.set_time_samples([0, 1, 2, 3, 4, 5, 6, 7])
    #    keys = rkv.generate_cache_index_keys(self.resource, 1)
#
    #    assert isinstance(keys, list)
    #    assert len(keys) == len(self.resource.get_time_samples())
#
    #    for time_samaple, key in zip(self.resource.get_time_samples(), keys):
    #        assert key == "CUBOID_IDX&4&2&1&1&{}".format(time_samaple)
#
    #    self.resource.set_time_samples([0])

    def test_put_cube_index(self):
        """Test adding cubes to the cuboid index"""
        self.cache_client.flushdb()
        self.status_client.flushdb()
        rkv = RedisKVIO(self.cache_client, self.status_client)
        resolution = 1

        # Make sure there are no items in the index
        base_key = rkv.generate_cache_index_keys(self.resource, resolution)[0]
        keys = self.status_client.keys("{}*".format(base_key))

        assert not keys

        # Update the index
        morton_ids = list(range(10, 23))
        rkv.put_cube_index(self.resource, resolution, morton_ids)

        keys = self.status_client.keys("{}*".format(base_key))
        assert len(keys) > 0

        # Make sure the keys are correct
        morton_in_index = self.status_client.smembers(base_key)

        assert len(morton_ids) == len(morton_in_index)

        # Explicitly decode all values so you can compare since mockredis doesn't seem to do this automatically.
        decoded_morton_in_index = [int(x) for x in morton_in_index]
        for morton in morton_ids:
            assert morton in decoded_morton_in_index

    def test_get_missing_cube_index_single_time_sample(self):
        """Test checking the index for cuboids that are missing"""
        self.cache_client.flushdb()
        self.status_client.flushdb()
        rkv = RedisKVIO(self.cache_client, self.status_client)
        resolution = 1

        # Put some stuff in the index
        morton_ids = list(range(10, 25))
        rkv.put_cube_index(self.resource, resolution, morton_ids)

        desired_morton_ids = list(range(15, 33))
        missing_keys = rkv.get_missing_cube_index(self.resource, resolution, desired_morton_ids)

        assert len(missing_keys) == 8

        missing_keys_true = list(set(desired_morton_ids) - set(morton_ids))
        decoded_missing_keys = [int(x) for x in missing_keys]
        for idx in decoded_missing_keys:
            assert idx in missing_keys_true

    def test_put_cubes_single(self):
        """Test adding cubes to the cache"""
        self.cache_client.flushdb()
        self.status_client.flushdb()
        rkv = RedisKVIO(self.cache_client, self.status_client)
        resolution = 1
        data = np.random.randint(50, size=[10, 15, 5])

        # Make sure there are no cuboids in the cache
        base_key = rkv.get_cache_base_key(self.resource, resolution)
        keys = self.cache_client.keys("{}*".format(base_key))
        assert not keys

        # Add items
        morton_id = 53342
        rkv.put_cubes(self.resource, resolution, [morton_id], [data])

        keys = self.cache_client.keys("{}*".format(base_key))
        assert len(keys) == 1

    def test_put_cubes_multiple(self):
        """Test adding cubes to the cache"""
        self.cache_client.flushdb()
        self.status_client.flushdb()
        rkv = RedisKVIO(self.cache_client, self.status_client)
        resolution = 1
        data = np.random.randint(50, size=[10, 15, 5])

        # Make sure there are no cuboids in the cache
        base_key = rkv.get_cache_base_key(self.resource, resolution)
        keys = self.cache_client.keys("{}*".format(base_key))
        assert not keys

        # Add items
        rkv.put_cubes(self.resource, resolution, [651, 315, 561], [data, data, data])

        keys = self.cache_client.keys("{}*".format(base_key))
        assert len(keys) == 3

    def test_get_cubes_single(self):
        """Test adding cubes to the cache"""
        self.cache_client.flushdb()
        self.status_client.flushdb()
        rkv = RedisKVIO(self.cache_client, self.status_client)
        resolution = 1
        data = "A test string since just checking for key retrieval"

        # Make sure there are no cuboids in the cache
        base_key = rkv.get_cache_base_key(self.resource, resolution)
        keys = self.cache_client.keys("{}*".format(base_key))
        assert not keys

        # Add items
        morton_id = 53342
        rkv.put_cubes(self.resource, resolution, [morton_id], [data])

        # Get cube
        for cnt, cube in enumerate(rkv.get_cubes(self.resource, resolution, [morton_id])):
            assert 53342 == cube[0]
            assert "A test string since just checking for key retrieval" == cube[1].decode()

        assert cnt == 0

    def test_get_cubes_multiple(self):
        """Test adding cubes to the cache"""
        self.cache_client.flushdb()
        self.status_client.flushdb()
        rkv = RedisKVIO(self.cache_client, self.status_client)
        resolution = 1
        data = "A test string since just checking for key retrieval - "

        data_list = []
        morton_id = list(range(234, 240))
        for ii in morton_id:
            data_list.append("{}{}".format(data, ii))

        # Make sure there are no cuboids in the cache
        base_key = rkv.get_cache_base_key(self.resource, resolution)
        keys = self.cache_client.keys("{}*".format(base_key))
        assert not keys

        # Add items
        rkv.put_cubes(self.resource, resolution, morton_id, data_list)

        # Get cube
        for cnt, cube in enumerate(rkv.get_cubes(self.resource, resolution, morton_id)):
            assert morton_id[cnt] == cube[0]
            assert data_list[cnt] == cube[1].decode()

        assert cnt == 5

