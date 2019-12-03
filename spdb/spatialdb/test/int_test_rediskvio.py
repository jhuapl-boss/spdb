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

from spdb.project import BossResourceBasic
from spdb.spatialdb import RedisKVIO
from spdb.spatialdb.test import RedisKVIOTestMixin
from spdb.spatialdb.test.setup import load_test_config_file

import redis

import numpy as np
import blosc

from spdb.project.test.resource_setup import get_image_dict


class TestIntegrationRedisKVIOImageData(RedisKVIOTestMixin, unittest.TestCase):

    def test_param_constructor(self):
        """Re-run a testing using the parameter based constructor"""
        config = {
                    "cache_host": self.config["aws"]["cache"],
                    "cache_db": 1,
                    "read_timeout": 86400
                }
        rkv = RedisKVIO(config)

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

        cube = [x for x in cubes]

        assert len(cube) == 3

        for m, c, d in zip(morton_id, cube, data):
            assert c[0] == m
            assert c[1] == 0
            data_retrieved = blosc.unpack_array(c[2])
            np.testing.assert_array_equal(data_retrieved, blosc.unpack_array(d))

    @classmethod
    def setUpClass(cls):
        """Setup the redis client at the start of the test"""
        cls.data = get_image_dict()
        cls.resource = BossResourceBasic(cls.data)

        cls.config = load_test_config_file()

        cls.cache_client = redis.StrictRedis(host=cls.config["aws"]["cache"], port=6379, db=1,
                                             decode_responses=False)

        cls.config_data = {"cache_client": cls.cache_client, "read_timeout": 86400}

    def setUp(self):
        """Clean out the cache DB between tests"""
        self.cache_client.flushdb()

    def tearDown(self):
        pass
