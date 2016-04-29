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
import os
from unittest.mock import patch
from mockredis import mock_strict_redis_client

from spdb.project import BossResourceBasic
from spdb.spatialdb import Cube, SpatialDB

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


class TestIntegrationSpatialDBImageDataOneTimeSample(unittest.TestCase):

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
        self.data['experiment']['base_resolution'] = 0

        self.data['channel_layer'] = {}
        self.data['channel_layer']['name'] = "ch1"
        self.data['channel_layer']['description'] = "Test channel 1"
        self.data['channel_layer']['is_channel'] = True
        self.data['channel_layer']['datatype'] = 'uint8'
        self.data['channel_layer']['max_time_sample'] = 0

        self.data['boss_key'] = 'col1&exp1&ch1'
        self.data['lookup_key'] = '4&2&1'

        self.resource = BossResourceBasic(self.data)

        self.redis_client = None

    def tearDown(self):
        # Stop mocking
        self.mock_tests = self.patcher.stop()

    def test_put_cubes_get_cube(self):
        """Test the put_cubes and get_cube methods"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [128, 128, 16])
        cube2 = Cube.create_cube(self.resource, [128, 128, 16])
        cube1.data = np.random.randint(0, 254, (16, 128, 128))
        cube2.data = np.random.randint(0, 254, (16, 128, 128))

        spdb = SpatialDB()

        spdb.put_cubes(self.resource, 0, [12, 13], [cube1, cube2])

        cubes1_test = spdb.get_cube(self.resource, 0, 12)
        cubes2_test = spdb.get_cube(self.resource, 0, 13)

        np.testing.assert_array_equal(cube1.data, cubes1_test.data)
        np.testing.assert_array_equal(cube2.data, cubes2_test.data)

    def test_put_cubes_get_cubes(self):
        """Test the put_cubes and get_cube methods"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [128, 128, 16])
        cube2 = Cube.create_cube(self.resource, [128, 128, 16])
        cube1.data = np.random.randint(0, 254, (16, 128, 128))
        cube2.data = np.random.randint(0, 254, (16, 128, 128))
        morton_ids = [12, 13]

        spdb = SpatialDB()

        spdb.put_cubes(self.resource, 0, [12, 13], [cube1, cube2])

        cubes = spdb.get_cubes(self.resource, 0, [12, 13])

        for c, t, m in zip(cubes, [cube1, cube2], morton_ids):
            assert c[0] == m
            loaded_cube = Cube.create_cube(self.resource, [16, 128, 128])
            loaded_cube.from_blosc_numpy(c[1])
            np.testing.assert_array_equal(t.data, loaded_cube.data)

    def test_cutout_no_offset_uint8(self):
        """Test the cutout method"""
        data = np.random.randint(0, 254, (30, 500, 300))
        data = data.astype(np.uint8)

        spdb = SpatialDB()

        # Write an arbitrary chunk into the cache
        spdb.write_cuboid(self.resource, (0, 0, 0), 0, data)

        # Get it back out
        cutout = spdb.cutout(self.resource, (0, 0, 0), (300, 500, 30), 0)
        assert cutout.data.shape == data.shape
        assert cutout.data.dtype == data.dtype
        np.testing.assert_array_equal(cutout.data, data)


    # TODO: Add up_sample and down_sample methods once annotation interfaces are integrated.

