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
from spdb.spatialdb import RedisKVIO

import redis

import numpy as np

from bossutils import configuration


class MockBossConfig:
    """Basic mock for BossConfig to contain the properties needed for this test"""
    def __init__(self):
        self.config = {}
        self.config["aws"] = {}
        self.config["aws"]["cache"] = {"https://some.url.com"}
        self.config["aws"]["cache-state"] = {"https://some.url2.com"}
        self.config["aws"]["cache-db"] = 1
        self.config["aws"]["cache-state-db"] = 1

    def read(self, filename):
        pass

    def __getitem__(self, key):
        return self.config[key]


@patch('configparser.ConfigParser', MockBossConfig)
@patch('redis.StrictRedis', mock_strict_redis_client)
class TestRedisKVIOImageDataOneTimeSample(unittest.TestCase):

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

        self.cache_client = redis.StrictRedis(host=self.config["aws"]["cache-state"], port=6379, db=1,
                                              decode_responses=True)
        self.cache_client.flushdb()

        self.status_client = redis.StrictRedis(host=self.config["aws"]["cache-state"], port=6379, db=1,
                                               decode_responses=True)
        self.status_client.flushdb()

    def tearDown(self):
        self.mock_tests = self.patcher.stop()

    def test_generate_cuboid_index_key(self):
        """Test the base key getter function for the cuboid index (cuboids that exist in the cache"""
        rkv = RedisKVIO()
        assert rkv.generate_cuboid_index_key(self.resource, 2) == "CUBOID_IDX&4&2&1&2"

    def test_generate_cuboid_data_keys_single(self):
        """Test the base key getter function for the cuboids"""
        rkv = RedisKVIO()
        assert rkv.generate_cuboid_data_keys(self.resource, 2, [0], [23445]) == ["CUBOID&4&2&1&2&0&23445"]

    def test_generate_cuboid_data_keys_multiple(self):
        """Test the generate cache index key generation for a single key"""
        rkv = RedisKVIO()

        keys = rkv.generate_cuboid_data_keys(self.resource, 2, [0], [123, 124, 125])

        assert isinstance(keys, list)
        assert len(keys) == 3
        assert keys[0] == "CUBOID&4&2&1&2&0&123"
        assert keys[1] == "CUBOID&4&2&1&2&0&124"
        assert keys[2] == "CUBOID&4&2&1&2&0&125"

    def test_put_cube_index(self):
        """Test adding cubes to the cuboid index"""
        #redis_client = redis.StrictRedis(host=self.config["aws"]["cache-state"], port=6379, db=1,
        #                                 decode_responses=True)
        #rkv = RedisKVIO(None, redis_client)

        resolution = 1
        rkv = RedisKVIO(self.cache_client, self.status_client)
        #TODO FINISH TESTS STARTING HERE!!!
        # Make sure there are no items in the index
        self.status_client.flushdb()
        keys = self.status_client.keys("{}*".format(rkv.generate_cuboid_index_key(self.resource, resolution)))
        assert not keys

        # Update the index
        morton_ids = list(range(10, 23))
        rkv.put_cube_index(self.resource, resolution, [0], morton_ids)

        keys = self.status_client.keys("{}*".format(rkv.generate_cuboid_index_key(self.resource, resolution)))
        assert len(keys) > 0

        # Make sure the keys are correct
        morton_in_index = self.status_client.smembers(rkv.generate_cuboid_index_key(self.resource, resolution))
        assert len(morton_ids) == len(morton_in_index)

        # Explicitly decode all values so you can compare since mockredis doesn't seem to do this automatically.
        expected_index = [int(x.decode()) for x in morton_in_index]
        for morton in morton_ids:
            assert morton in decoded_morton_in_index

    def test_get_missing_cube_index(self):
        """Test checking the index for cuboids that are missing"""
        #redis_client = redis.StrictRedis(host=self.config["aws"]["cache-state"], port=6379, db=1,
        #                                 decode_responses=True)
#
        #rkv = RedisKVIO(None, redis_client)
        resolution = 1

        # Put some stuff in the index
        morton_ids = list(range(10, 25))
        rkv.put_cube_index(self.resource, resolution, morton_ids)

        desired_morton_ids = list(range(15, 33))
        missing_keys = rkv.get_missing_cube_index(self.resource, resolution, desired_morton_ids)

        assert len(missing_keys) == 8

        missing_keys_true = list(set(desired_morton_ids) - set(morton_ids))
        decoded_missing_keys = [int(x.decode()) for x in missing_keys]
        for idx in decoded_missing_keys:
            assert idx in missing_keys_true

    def test_put_cubes_single(self):
        """Test adding cubes to the cache"""
        redis_client = redis.StrictRedis(host=self.config["aws"]["cache-state"], port=6379, db=0,
                                         decode_responses=True)
        rkv = RedisKVIO(redis_client, redis_client)
        resolution = 1
        data = np.random.randint(50, size=[10, 15, 5])

        # Make sure there are no cuboids in the cache
        base_key = rkv.generate_cuboid_data_keys(self.resource, resolution)
        keys = redis_client.keys("{}*".format(base_key))
        assert not keys

        # Add items
        morton_id = 53342
        rkv.put_cubes(self.resource, resolution, [morton_id], [data])

        keys = redis_client.keys("{}*".format(base_key))
        assert len(keys) == 1

    def test_put_cubes_multiple(self):
        """Test adding cubes to the cache"""
        redis_client = redis.StrictRedis(host=self.config["aws"]["cache-state"], port=6379, db=0,
                                         decode_responses=True)
        rkv = RedisKVIO(redis_client, redis_client)
        resolution = 1
        data = np.random.randint(50, size=[10, 15, 5])

        # Make sure there are no cuboids in the cache
        base_key = rkv.generate_cuboid_data_keys(self.resource, resolution)
        keys = redis_client.keys("{}*".format(base_key))
        assert not keys

        # Add items
        rkv.put_cubes(self.resource, resolution, [651, 315, 561], [data, data, data])

        keys = redis_client.keys("{}*".format(base_key))
        assert len(keys) == 3

    def test_get_cube_single(self):
        """Test adding cubes to the cache"""
        redis_client = redis.StrictRedis(host=self.config["aws"]["cache-state"], port=6379, db=0,
                                         decode_responses=True)
        rkv = RedisKVIO(redis_client, redis_client)
        resolution = 1
        data = "A test string since just checking for key retrieval"

        # Make sure there are no cuboids in the cache
        base_key = rkv.generate_cuboid_data_keys(self.resource, resolution)
        keys = redis_client.keys("{}*".format(base_key))
        assert not keys

        # Add items
        morton_id = 53342
        rkv.put_cubes(self.resource, resolution, [morton_id], [data])

        # Get cube
        cube = rkv.get_cube(self.resource, resolution, morton_id)
        assert "A test string since just checking for key retrieval" == cube.decode()

    def test_get_cubes_single(self):
        """Test adding cubes to the cache"""
        redis_client = redis.StrictRedis(host=self.config["aws"]["cache-state"], port=6379, db=0,
                                         decode_responses=True)
        rkv = RedisKVIO(redis_client, redis_client)
        resolution = 1
        data = "A test string since just checking for key retrieval"

        # Make sure there are no cuboids in the cache
        base_key = rkv.generate_cuboid_data_keys(self.resource, resolution)
        keys = redis_client.keys("{}*".format(base_key))
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
        redis_client = redis.StrictRedis(host=self.config["aws"]["cache-state"], port=6379, db=0,
                                         decode_responses=True)
        rkv = RedisKVIO(redis_client, redis_client)
        resolution = 1
        data = "A test string since just checking for key retrieval - "

        data_list = []
        morton_id = list(range(234, 240))
        for ii in morton_id:
            data_list.append("{}{}".format(data, ii))

        # Make sure there are no cuboids in the cache
        base_key = rkv.generate_cuboid_data_keys(self.resource, resolution)
        keys = redis_client.keys("{}*".format(base_key))
        assert not keys

        # Add items
        rkv.put_cubes(self.resource, resolution, morton_id, data_list)

        # Get cube
        for cnt, cube in enumerate(rkv.get_cubes(self.resource, resolution, morton_id)):
            assert morton_id[cnt] == cube[0]
            assert data_list[cnt] == cube[1].decode()

        assert cnt == 5

