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

import redis

import numpy as np
import blosc

from bossutils import configuration


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

        self.cache_client = redis.StrictRedis(host=self.config["aws"]["cache"], port=6379, db=1,
                                              decode_responses=False)

        self.cache_client.flushdb()

        self.config_data = {"cache_client": self.cache_client, "read_timeout": 86400}

    def tearDown(self):
        pass
