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
from fakeredis import FakeStrictRedis

from spdb.project import BossResourceBasic
from spdb.spatialdb import RedisKVIO

import redis

import numpy as np
import blosc

from spdb.project.test.resource_setup import get_image_dict


class RedisKVIOTestMixin(object):

    def test_generate_cached_cuboid_keys(self):
        """Test if cache cuboid keys are formatted properly"""
        rkv = RedisKVIO(self.config_data)
        keys = rkv.generate_cached_cuboid_keys(self.resource, 2, [0, 1, 2], [34, 35, 36])
        assert len(keys) == 9
        assert keys[0] == "CACHED-CUBOID&4&3&2&2&0&34"
        assert keys[2] == "CACHED-CUBOID&4&3&2&2&0&36"
        assert keys[5] == "CACHED-CUBOID&4&3&2&2&1&36"
        assert keys[8] == "CACHED-CUBOID&4&3&2&2&2&36"

    def test_generate_write_cuboid_keys(self):
        """Test if write-cuboid keys are formatted properly"""
        rkv = RedisKVIO(self.config_data)
        keys = rkv.generate_write_cuboid_keys(self.resource, 2, [0, 1, 2], [34, 35, 36])
        assert len(keys) == 9
        uuids = []
        for key in keys:
            uuids.append(key.rsplit("&", 1)[1])
        assert len(set(uuids)) == 9

        assert keys[0].rsplit("&", 1)[0] == "WRITE-CUBOID&4&3&2&2&0&34"
        assert keys[2].rsplit("&", 1)[0] == "WRITE-CUBOID&4&3&2&2&0&36"
        assert keys[5].rsplit("&", 1)[0] == "WRITE-CUBOID&4&3&2&2&1&36"
        assert keys[8].rsplit("&", 1)[0] == "WRITE-CUBOID&4&3&2&2&2&36"

    def test_generate_black_cuboid_keys(self):
        """Test if black-cuboid keys are formatted properly"""
        rkv = RedisKVIO(self.config_data)
        keys = rkv.generate_black_cuboid_keys(self.resource, 2, [0, 1, 2], [34, 35, 36])
        assert len(keys) == 9
        uuids = []
        for key in keys:
            uuids.append(key.rsplit("&", 1)[1])
        assert len(set(uuids)) == 9

        assert keys[0].rsplit("&", 1)[0] == "BLACK-CUBOID&4&3&2&2&0&34"
        assert keys[1].rsplit("&", 1)[0] == "BLACK-CUBOID&4&3&2&2&0&35"
        assert keys[2].rsplit("&", 1)[0] == "BLACK-CUBOID&4&3&2&2&0&36"
        assert keys[3].rsplit("&", 1)[0] == "BLACK-CUBOID&4&3&2&2&1&34"
        assert keys[4].rsplit("&", 1)[0] == "BLACK-CUBOID&4&3&2&2&1&35"
        assert keys[5].rsplit("&", 1)[0] == "BLACK-CUBOID&4&3&2&2&1&36"
        assert keys[6].rsplit("&", 1)[0] == "BLACK-CUBOID&4&3&2&2&2&34"
        assert keys[7].rsplit("&", 1)[0] == "BLACK-CUBOID&4&3&2&2&2&35"
        assert keys[8].rsplit("&", 1)[0] == "BLACK-CUBOID&4&3&2&2&2&36"

    def test_get_missing_read_cache_keys(self):
        """Test for querying for keys missing in the cache"""
        # Put some keys in the cache
        rkv = RedisKVIO(self.config_data)
        keys = rkv.generate_cached_cuboid_keys(self.resource, 2, [0], [34, 35, 36])
        for k in keys:
            self.cache_client.set(k, "dummy")

        missing, cached, all_keys = rkv.get_missing_read_cache_keys(self.resource, 2, [0, 2], [33, 34, 35])

        assert len(missing) == 4
        assert len(cached) == 2

        assert all_keys[cached[0]] == "CACHED-CUBOID&4&3&2&2&0&34"
        assert all_keys[cached[1]] == "CACHED-CUBOID&4&3&2&2&0&35"

    def test_get_missing_read_cache_keys_iso_below_res(self):
        """Test for querying for keys missing in the cache when asking for iso data but below res fork"""
        # Put some keys in the cache
        rkv = RedisKVIO(self.config_data)
        keys = rkv.generate_cached_cuboid_keys(self.resource, 0, [0], [34, 35, 36], iso=True)
        for k in keys:
            self.cache_client.set(k, "dummy")

        missing, cached, all_keys = rkv.get_missing_read_cache_keys(self.resource, 0, [0, 2], [33, 34, 35], iso=True)

        assert len(missing) == 4
        assert len(cached) == 2

        assert all_keys[cached[0]] == "CACHED-CUBOID&4&3&2&0&0&34"
        assert all_keys[cached[1]] == "CACHED-CUBOID&4&3&2&0&0&35"

    def test_get_missing_read_cache_keys_iso_above_res(self):
        """Test for querying for keys missing in the cache when asking for iso data but below res fork"""
        # Put some keys in the cache
        rkv = RedisKVIO(self.config_data)
        keys = rkv.generate_cached_cuboid_keys(self.resource, 6, [0], [34, 35, 36], iso=True)
        for k in keys:
            self.cache_client.set(k, "dummy")

        missing, cached, all_keys = rkv.get_missing_read_cache_keys(self.resource, 6, [0, 2], [33, 34, 35], iso=True)

        assert len(missing) == 4
        assert len(cached) == 2

        assert all_keys[cached[0]] == "CACHED-CUBOID&ISO&4&3&2&6&0&34"
        assert all_keys[cached[1]] == "CACHED-CUBOID&ISO&4&3&2&6&0&35"

    def test_write_cuboid_to_cache_key(self):
        """Test converting from write cuboid keys to cache keys"""
        # Put some keys in the cache
        rkv = RedisKVIO(self.config_data)
        write_cuboid_keys = rkv.generate_write_cuboid_keys(self.resource, 2, [0], [34])
        cache_cuboid_key = rkv.write_cuboid_key_to_cache_key(write_cuboid_keys[0])

        assert cache_cuboid_key == "CACHED-CUBOID&4&3&2&2&0&34"

    def test_put_cubes(self):
        """Test adding cubes to the cache"""
        resolution = 1
        rkv = RedisKVIO(self.config_data)

        # Clean up data
        self.cache_client.flushdb()

        data1 = np.random.randint(50, size=[10, 15, 5])
        data2 = np.random.randint(50, size=[10, 15, 5])
        data3 = np.random.randint(50, size=[10, 15, 5])
        data_packed1 = blosc.pack_array(data1)
        data_packed2 = blosc.pack_array(data2)
        data_packed3 = blosc.pack_array(data3)
        data = [data_packed1, data_packed2, data_packed3]

        # Make sure there are no cuboids in the cache
        keys = self.cache_client.keys('CACHED-CUBOID&{}&{}*'.format(self.resource.get_lookup_key(), resolution))
        assert not keys

        # Add items
        keys = rkv.generate_cached_cuboid_keys(self.resource, 2, [0], [123, 124, 126])
        rkv.put_cubes(keys, data)

        db_keys = self.cache_client.keys('CACHED-CUBOID*')
        db_keys = [x.decode() for x in db_keys]
        assert len(set(keys)) == 3
        for k, d in zip(keys, db_keys):
            assert k in db_keys

    def test_get_cubes(self):
        """Test adding cubes to the cache"""
        resolution = 1
        rkv = RedisKVIO(self.config_data)

        # Clean up data
        self.cache_client.flushdb()

        data1 = np.random.randint(50, size=[10, 15, 5])
        data2 = np.random.randint(50, size=[10, 15, 5])
        data3 = np.random.randint(50, size=[10, 15, 5])
        data_packed1 = blosc.pack_array(data1)
        data_packed2 = blosc.pack_array(data2)
        data_packed3 = blosc.pack_array(data3)
        data = [data_packed1, data_packed2, data_packed3]

        # Add items
        morton_id = [112, 125, 516]
        keys = rkv.generate_cached_cuboid_keys(self.resource, 2, [0], morton_id)
        rkv.put_cubes(keys, data)

        # Get cube
        cubes = rkv.get_cubes(keys)
        assert len(cubes) == 3

        for m, c, d in zip(morton_id, cubes, data):
            assert c[0] == m
            assert c[1] == 0
            data_retrieved = blosc.unpack_array(c[2])
            np.testing.assert_array_equal(data_retrieved, blosc.unpack_array(d))

    def test_cube_exists(self):
        """Test checking if cubes exist"""
        resolution = 1
        rkv = RedisKVIO(self.config_data)

        # Clean up data
        self.cache_client.flushdb()

        data1 = np.random.randint(50, size=[10, 15, 5])
        data_packed1 = blosc.pack_array(data1)
        data = [data_packed1]

        # Add items
        morton_id = [112]
        keys = rkv.generate_cached_cuboid_keys(self.resource, 2, [0], morton_id)

        result = rkv.cube_exists(keys[0])
        assert not result

        rkv.put_cubes(keys, data)

        result = rkv.cube_exists(keys[0])
        assert result

    def test_delete_cube(self):
        """Test cube delete method"""
        resolution = 1
        rkv = RedisKVIO(self.config_data)

        # Clean up data
        self.cache_client.flushdb()

        data1 = np.random.randint(50, size=[10, 15, 5])
        data_packed1 = blosc.pack_array(data1)
        data = [data_packed1]

        # Add items
        morton_id = [112]
        keys = rkv.generate_cached_cuboid_keys(self.resource, 2, [0], morton_id)

        result = rkv.cube_exists(keys[0])
        assert not result

        rkv.put_cubes(keys, data)

        result = rkv.cube_exists(keys[0])
        assert result

        rkv.delete_cube(keys[0])

        result = rkv.cube_exists(keys[0])
        assert not result

    def test_is_dirty(self):
        """Test cube delete method"""
        resolution = 1
        rkv = RedisKVIO(self.config_data)

        # Clean up data
        self.cache_client.flushdb()

        data1 = np.random.randint(50, size=[10, 15, 5])
        data_packed1 = blosc.pack_array(data1)
        data = [data_packed1]

        # Add item to cached-cuboid
        morton_id = [112]
        keys = rkv.generate_cached_cuboid_keys(self.resource, 2, [0], morton_id)
        rkv.put_cubes(keys, data)

        result = rkv.is_dirty(keys)
        assert not result[0]

        # Fake a write
        write_cuboid_key = "WRITE-CUBOID&{}".format(keys[0].split('&', 1)[1])
        write_cuboid_key, time_sample, morton = write_cuboid_key.rsplit('&', 2)
        rkv.insert_cube_in_write_buffer(write_cuboid_key, time_sample, morton, data[0])

        result = rkv.is_dirty(keys)
        assert result[0]

    def test_write_buffer_io(self):
        """Test methods specific to single cube write buffer io"""
        resolution = 1
        rkv = RedisKVIO(self.config_data)

        # Clean up data
        self.cache_client.flushdb()

        data1 = np.random.randint(50, size=[10, 15, 5])
        data_packed = blosc.pack_array(data1)

        key = rkv.insert_cube_in_write_buffer("WRITE-CUBOID&4&1&1&1", 3, 234, data_packed)

        data_rx = rkv.get_cube_from_write_buffer(key)

        assert data_packed == data_rx


@patch('redis.StrictRedis', FakeStrictRedis)
class TestRedisKVIOImageData(RedisKVIOTestMixin, unittest.TestCase):

    @classmethod
    @patch('redis.StrictRedis', FakeStrictRedis)
    def setUpClass(cls):
        """Setup the redis client at the start of the test"""
        cls.data = get_image_dict()
        cls.resource = BossResourceBasic(cls.data)

        cls.cache_client = redis.StrictRedis(host='localhost', port=6379, db=1,
                                             decode_responses=False)

        cls.config_data = {"cache_client": cls.cache_client, "read_timeout": 86400}

    def setUp(self):
        """Clean out the cache DB between tests"""
        self.patcher = patch('redis.StrictRedis', FakeStrictRedis)
        self.mock_tests = self.patcher.start()

        self.cache_client.flushdb()

    def tearDown(self):
        self.mock_tests = self.patcher.stop()

