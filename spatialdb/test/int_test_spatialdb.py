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
from spdb.spatialdb import Cube, SpatialDB

import numpy as np
import copy

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
class TestIntegrationSpatialDBImageData(unittest.TestCase):

    def setUp(self):
        """ Create a diction of configuration values for the test resource8. """
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

        self.redis_client = None

    def tearDown(self):
        # Stop mocking
        self.mock_tests = self.patcher.stop()

    def test_put_single_cube_get_single_cube_no_time(self):
        """Test the put_cubes and get_cube methods"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource8, [128, 128, 16])
        cube1.data = np.random.randint(0, 254, (1, 16, 128, 128))

        db = SpatialDB()

        db.put_single_cube(self.resource8, 0, 34, cube1)

        cube2 = db.get_single_cube(self.resource8, 0, 0, 34)

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_put_single_cube_get_single_cube(self):
        """Test the put_cubes and get_cube methods"""
        # Generate random data
        cube_true = Cube.create_cube(self.resource8, [128, 128, 16], [0, 4])
        cube_true.data = np.random.randint(0, 254, (4, 16, 128, 128))

        db = SpatialDB()

        db.put_single_cube(self.resource8, 0, 24, cube_true)

        cube0 = db.get_single_cube(self.resource8, 0, 0, 24)
        cube1 = db.get_single_cube(self.resource8, 0, 1, 24)
        cube2 = db.get_single_cube(self.resource8, 0, 2, 24)
        cube3 = db.get_single_cube(self.resource8, 0, 3, 24)

        np.testing.assert_array_equal(cube_true.data[0, :, :, :], np.squeeze(cube0.data))
        np.testing.assert_array_equal(cube_true.data[1, :, :, :], np.squeeze(cube1.data))
        np.testing.assert_array_equal(cube_true.data[2, :, :, :], np.squeeze(cube2.data))
        np.testing.assert_array_equal(cube_true.data[3, :, :, :], np.squeeze(cube3.data))

    def test_put_cubes_get_cubes_no_time(self):
        """Test the put_cubes and get_cube methods"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource8, [128, 128, 16])
        cube2 = Cube.create_cube(self.resource8, [128, 128, 16])
        cube3 = Cube.create_cube(self.resource8, [128, 128, 16])
        cube1.data = np.random.randint(0, 254, (1, 16, 128, 128))
        cube2.data = np.random.randint(0, 254, (1, 16, 128, 128))
        cube3.data = np.random.randint(0, 254, (1, 16, 128, 128))

        input_cubes = [cube1, cube2, cube3]
        morton_ids = [12, 456, 13]

        spdb = SpatialDB()

        spdb.put_cubes(self.resource8, 0, morton_ids, input_cubes)

        cube_array = spdb.get_cubes(self.resource8, 0, [0, 1], morton_ids)

        # Shuffle cubes since spdb sorts by morton
        truth = [cube1, cube3, cube2]
        for morton_id, test, true in zip(morton_ids, cube_array, truth):
            np.testing.assert_array_equal(true.data, test.data)

    def test_put_cubes_get_cubes(self):
        """Test the put_cubes and get_cube methods"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource8, [128, 128, 16], [5, 10])
        cube2 = Cube.create_cube(self.resource8, [128, 128, 16], [5, 10])
        cube3 = Cube.create_cube(self.resource8, [128, 128, 16], [5, 10])
        cube1.data = np.random.randint(0, 254, (5, 16, 128, 128))
        cube2.data = np.random.randint(0, 254, (5, 16, 128, 128))
        cube3.data = np.random.randint(0, 254, (5, 16, 128, 128))

        input_cubes = [cube1, cube2, cube3]
        morton_ids = [122, 4562, 132]

        spdb = SpatialDB()

        spdb.put_cubes(self.resource8, 0, morton_ids, input_cubes)

        cube_array = spdb.get_cubes(self.resource8, 0, [5, 10], [122, 4562, 132])

        # Shuffle cubes since spdb sorts by morton
        truth = [cube1, cube3, cube2]
        morton_ids = sorted(morton_ids)
        for morton_id, test, true in zip(morton_ids, cube_array, truth):
            assert morton_id == test.morton_id
            np.testing.assert_array_equal(true.data, test.data)

    def test_put_cubes_get_cubes_missing(self):
        """Test the put_cubes and get_cube methods"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource8, [128, 128, 16], [5, 10])
        cube2 = Cube.create_cube(self.resource8, [128, 128, 16], [5, 10])
        cube3 = Cube.create_cube(self.resource8, [128, 128, 16], [5, 10])
        cube4 = Cube.create_cube(self.resource8, [128, 128, 16], [5, 10])
        cube1.data = np.random.randint(0, 254, (5, 16, 128, 128))
        cube2.data = np.random.randint(0, 254, (5, 16, 128, 128))
        cube3.data = np.random.randint(0, 254, (5, 16, 128, 128))
        cube4.zeros()

        input_cubes = [cube1, cube2, cube3]
        morton_ids = [122, 4562, 132]

        spdb = SpatialDB()

        spdb.put_cubes(self.resource8, 0, morton_ids, input_cubes)

        cube_array = spdb.get_cubes(self.resource8, 0, [5, 10], [122, 4562, 132, 23446])

        # Shuffle cubes since spdb sorts by morton
        truth = [cube1, cube3, cube2, cube4]
        morton_ids = sorted([122, 4562, 132, 23446])
        for morton_id, test, true in zip(morton_ids, cube_array, truth):
            assert morton_id == test.morton_id
            np.testing.assert_array_equal(true.data, test.data)

    def test_write_cuboid_aligned_single_no_time(self):
        """Test the write_cuboid method"""
        # At this point data should be in zyx
        data = np.random.randint(0, 254, (16, 128, 128))
        data = data.astype(np.uint8)

        spdb = SpatialDB()

        # Make sure no data is in the database
        assert len(spdb.kvio.cache_client.redis) == 0
        spdb.write_cuboid(self.resource8, (0, 0, 0), 0, data)

        # make sure data was written
        assert len(spdb.kvio.cache_client.redis) == 1

    def test_write_cuboid_aligned_single(self):
        """Test the write_cuboid method"""
        # At this point data should be in zyx
        data = np.random.randint(0, 254, (4, 16, 128, 128))
        data = data.astype(np.uint8)

        spdb = SpatialDB()

        # Make sure no data is in the database
        assert len(spdb.kvio.cache_client.redis) == 0
        assert len(spdb.kvio.status_client.redis) == 0
        spdb.write_cuboid(self.resource8, (0, 0, 0), 0, data)

        # make sure data was written
        assert len(spdb.kvio.cache_client.redis) == 4
        assert len(spdb.kvio.status_client.redis) == 1

    def test_write_cuboid_aligned_multiple_no_time(self):
        """Test the write_cuboid method"""
        # At this point data should be in zyx
        data = np.random.randint(0, 254, (16, 256, 128))
        data = data.astype(np.uint8)

        spdb = SpatialDB()

        # Make sure no data is in the database
        assert len(spdb.kvio.cache_client.redis) == 0
        spdb.write_cuboid(self.resource8, (0, 0, 0), 0, data)

        # make sure data was written
        assert len(spdb.kvio.cache_client.redis) == 2

    def test_write_cuboid_aligned_multiple(self):
        """Test the write_cuboid method"""
        # At this point data should be in zyx
        data = np.random.randint(0, 254, (4, 16, 256, 128))
        data = data.astype(np.uint8)

        spdb = SpatialDB()

        # Make sure no data is in the database
        assert len(spdb.kvio.cache_client.redis) == 0
        assert len(spdb.kvio.status_client.redis) == 0
        spdb.write_cuboid(self.resource8, (0, 0, 0), 0, data)

        # make sure data was written
        assert len(spdb.kvio.cache_client.redis) == 8
        assert len(spdb.kvio.status_client.redis) == 1

    def test_write_cuboid_multiple_no_time(self):
        """Test the write_cuboid method"""
        # At this point data should be in zyx
        data = np.random.randint(0, 254, (20, 300, 120))
        data = data.astype(np.uint8)

        spdb = SpatialDB()

        # Make sure no data is in the database
        assert len(spdb.kvio.cache_client.redis) == 0
        spdb.write_cuboid(self.resource8, (0, 0, 0), 0, data)

        # make sure data was written
        assert len(spdb.kvio.cache_client.redis) == 6

    def test_write_cuboid_multiple(self):
        """Test the write_cuboid method"""
        # At this point data should be in zyx
        data = np.random.randint(0, 254, (4, 20, 300, 120))
        data = data.astype(np.uint8)

        spdb = SpatialDB()

        # Make sure no data is in the database
        assert len(spdb.kvio.cache_client.redis) == 0
        assert len(spdb.kvio.status_client.redis) == 0
        spdb.write_cuboid(self.resource8, (0, 0, 0), 0, data)

        # make sure data was written
        assert len(spdb.kvio.cache_client.redis) == 24
        assert len(spdb.kvio.status_client.redis) == 1

    def test_cutout_aligned_single_no_time_uint8(self):
        """Test the write_cuboid method"""
        # At this point data should be in zyx
        data = np.random.randint(0, 254, (16, 128, 128))
        data = data.astype(np.uint8)

        spdb = SpatialDB()

        # Make sure no data is in the database
        spdb.write_cuboid(self.resource8, (0, 0, 0), 0, data)

        # Get it back out
        cutout = spdb.cutout(self.resource8, (0, 0, 0), (128, 128, 16), 0)
        data = np.expand_dims(data, axis=0)
        assert cutout.data.shape == data.shape
        assert cutout.data.dtype == data.dtype
        np.testing.assert_array_equal(cutout.data, data)

    def test_cutout_aligned_single_uint8(self):
        """Test the write_cuboid method"""
        # At this point data should be in zyx
        data = np.random.randint(0, 254, (4, 16, 128, 128))
        data = data.astype(np.uint8)

        spdb = SpatialDB()

        # Make sure no data is in the database
        spdb.write_cuboid(self.resource8, (0, 0, 0), 0, data, 4)

        # Get it back out
        cutout = spdb.cutout(self.resource8, (0, 0, 0), (128, 128, 16), 0, [4, 8])
        assert cutout.data.shape == data.shape
        assert cutout.data.dtype == data.dtype
        np.testing.assert_array_equal(cutout.data, data)

    def test_cutout_multiple_no_time_uint8(self):
        """Test the write_cuboid method"""
        # At this point data should be in zyx
        data = np.random.randint(0, 254, (20, 300, 120))
        data = data.astype(np.uint8)

        spdb = SpatialDB()

        # Make sure no data is in the database
        spdb.write_cuboid(self.resource8, (0, 0, 0), 0, data)

        # Get it back out
        cutout = spdb.cutout(self.resource8, (0, 0, 0), (120, 300, 20), 0)
        data = np.expand_dims(data, axis=0)
        assert cutout.data.shape == data.shape
        assert cutout.data.dtype == data.dtype
        np.testing.assert_array_equal(cutout.data, data)

    def test_cutout_multiple_uint8(self):
        """Test the write_cuboid method"""
        # At this point data should be in zyx
        data = np.random.randint(0, 254, (4, 20, 300, 120))
        data = data.astype(np.uint8)

        spdb = SpatialDB()

        # Make sure no data is in the database
        spdb.write_cuboid(self.resource8, (0, 0, 0), 0, data, 4)

        # Get it back out
        cutout = spdb.cutout(self.resource8, (0, 0, 0), (120, 300, 20), 0, [4, 8])
        assert cutout.data.shape == data.shape
        assert cutout.data.dtype == data.dtype
        np.testing.assert_array_equal(cutout.data, data)

    def test_cutout_multiple_offset_uint8(self):
        """Test the write_cuboid method"""
        # At this point data should be in zyx
        data = np.random.randint(0, 254, (4, 20, 300, 120))
        data = data.astype(np.uint8)

        spdb = SpatialDB()

        # Make sure no data is in the database
        spdb.write_cuboid(self.resource8, (500, 700, 30), 0, data, 4)

        # Get it back out
        cutout = spdb.cutout(self.resource8, (500, 700, 30), (120, 300, 20), 0, [4, 8])
        assert cutout.data.shape == data.shape
        assert cutout.data.dtype == data.dtype
        np.testing.assert_array_equal(cutout.data, data)

    def test_cutout_aligned_single_no_time_uint16(self):
        """Test the write_cuboid method"""
        # At this point data should be in zyx
        data = np.random.randint(0, 5000, (16, 128, 128))
        data = data.astype(np.uint16)

        spdb = SpatialDB()

        # Make sure no data is in the database
        spdb.write_cuboid(self.resource16, (0, 0, 0), 0, data)

        # Get it back out
        cutout = spdb.cutout(self.resource16, (0, 0, 0), (128, 128, 16), 0)
        data = np.expand_dims(data, axis=0)
        assert cutout.data.shape == data.shape
        assert cutout.data.dtype == data.dtype
        np.testing.assert_array_equal(cutout.data, data)

    def test_cutout_aligned_single_uint16(self):
        """Test the write_cuboid method"""
        # At this point data should be in zyx
        data = np.random.randint(0, 5000, (4, 16, 128, 128))
        data = data.astype(np.uint16)

        spdb = SpatialDB()

        # Make sure no data is in the database
        spdb.write_cuboid(self.resource16, (0, 0, 0), 0, data, 4)

        # Get it back out
        cutout = spdb.cutout(self.resource16, (0, 0, 0), (128, 128, 16), 0, [4, 8])
        assert cutout.data.shape == data.shape
        assert cutout.data.dtype == data.dtype
        np.testing.assert_array_equal(cutout.data, data)

    def test_cutout_multiple_no_time_uint16(self):
        """Test the write_cuboid method"""
        # At this point data should be in zyx
        data = np.random.randint(0, 5000, (20, 300, 120))
        data = data.astype(np.uint16)

        spdb = SpatialDB()

        # Make sure no data is in the database
        spdb.write_cuboid(self.resource16, (0, 0, 0), 0, data)

        # Get it back out
        cutout = spdb.cutout(self.resource16, (0, 0, 0), (120, 300, 20), 0)
        data = np.expand_dims(data, axis=0)
        assert cutout.data.shape == data.shape
        assert cutout.data.dtype == data.dtype
        np.testing.assert_array_equal(cutout.data, data)

    def test_cutout_multiple_uint16(self):
        """Test the write_cuboid method"""
        # At this point data should be in zyx
        data = np.random.randint(0, 5000, (4, 20, 300, 120))
        data = data.astype(np.uint16)

        spdb = SpatialDB()

        # Make sure no data is in the database
        spdb.write_cuboid(self.resource16, (0, 0, 0), 0, data, 4)

        # Get it back out
        cutout = spdb.cutout(self.resource16, (0, 0, 0), (120, 300, 20), 0, [4, 8])
        assert cutout.data.shape == data.shape
        assert cutout.data.dtype == data.dtype
        np.testing.assert_array_equal(cutout.data, data)

    def test_cutout_multiple_offset_uint16(self):
        """Test the write_cuboid method"""
        # At this point data should be in zyx
        data = np.random.randint(0, 5000, (4, 20, 300, 120))
        data = data.astype(np.uint16)

        spdb = SpatialDB()

        # Make sure no data is in the database
        spdb.write_cuboid(self.resource16, (500, 700, 30), 0, data, 4)

        # Get it back out
        cutout = spdb.cutout(self.resource16, (500, 700, 30), (120, 300, 20), 0, [4, 8])
        assert cutout.data.shape == data.shape
        assert cutout.data.dtype == data.dtype
        np.testing.assert_array_equal(cutout.data, data)
