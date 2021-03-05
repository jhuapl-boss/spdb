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
import numpy as np
import time
import random

from spdb.spatialdb.test.test_spatialdb import SpatialDBImageDataTestMixin
from spdb.spatialdb import Cube, SpatialDB
from spdb.spatialdb.error import SpdbError
from spdb.spatialdb.test.setup import AWSSetupLayer
from spdb.c_lib.ndtype import CUBOIDSIZE
from spdb.c_lib.ndlib import XYZMorton
from spdb.project.test.resource_setup import get_anno_dict
from spdb.project import BossResourceBasic

import redis


class SpatialDBImageDataIntegrationTestMixin(object):

    cuboid_size = CUBOIDSIZE[0]
    x_dim = cuboid_size[0]
    y_dim = cuboid_size[1]
    z_dim = cuboid_size[2]

    def test_cutout_no_time_single_no_cache(self):
        """Test the get_cubes method - no time - single - bypass cache"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.random()
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data)

        # Cutout using cache to make sure cube finished writing to S3 first.
        sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0)

        cube2 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0, access_mode="no_cache")

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_cutout_no_time_single_raw(self):
        """Test the get_cubes method - no time - single - raw mode"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.random()
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data)

        # Cutout using cache to make sure cube finished writing to S3 first.
        sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0)

        cube2 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0, access_mode="raw")

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_cutout_no_time_single_aligned_hit(self):
        """Test the get_cubes method - no time - single - hit"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.random()
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data)

        start = time.time()
        cube2 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0)
        cutout1_time = time.time() - start

        np.testing.assert_array_equal(cube1.data, cube2.data)

        start = time.time()
        cube2 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0)
        cutout2_time = time.time() - start

        np.testing.assert_array_equal(cube1.data, cube2.data)
        assert cutout2_time < cutout1_time

    def test_cutout_no_time_single_aligned_miss(self):
        """Test the get_cubes method - no time - single - miss"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.random()
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (1, 0, 0), 0, cube1.data)

        cube2 = sp.cutout(self.resource, (1, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0)

        # Make sure data is the same
        np.testing.assert_array_equal(cube1.data, cube2.data)

        # Delete everything in the cache
        sp.kvio.cache_client.flushdb()

        # Get the data again
        cube3 = sp.cutout(self.resource, (1, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0)

        # Make sure the data is the same
        np.testing.assert_array_equal(cube1.data, cube2.data)
        np.testing.assert_array_equal(cube1.data, cube3.data)

    def test_cutout_no_time_single_aligned_existing_hit(self):
        """Test the get_cubes method - no time - aligned - existing data - miss"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.random()

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data)

        cube2 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0)

        np.testing.assert_array_equal(cube1.data, cube2.data)
        del cube1
        del cube2

        # now write to cuboid again
        cube3 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube3.random()

        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube3.data)

        cube4 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0)
        np.testing.assert_array_equal(cube3.data, cube4.data)

    def test_cutout_no_time_single_aligned_hit_shifted(self):
        """Test the get_cubes method - no time - single - hit - shifted into a different location"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.random()
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (self.x_dim, self.y_dim, 0), 0, cube1.data)

        cube2 = sp.cutout(self.resource, (self.x_dim, self.y_dim, 0), (self.x_dim, self.y_dim, self.z_dim), 0)

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_cutout_no_time_single_aligned_hit_shifted_no_cache(self):
        """Test the get_cubes method - no time - single - hit - shifted into a different location - bypass cache"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.random()
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (self.x_dim, self.y_dim, 0), 0, cube1.data)

        # Cutout using cache to make sure cube finished writing to S3 first.
        sp.cutout(self.resource, (self.x_dim, self.y_dim, 0), (self.x_dim, self.y_dim, self.z_dim), 0)

        cube2 = sp.cutout(self.resource, (self.x_dim, self.y_dim, 0), (self.x_dim, self.y_dim, self.z_dim), 0, access_mode="no_cache")

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_cutout_no_time_single_unaligned_no_cache(self):
        """Test the get_cubes method - no time - single - unaligned - bypass cache"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.random()
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (600, 0, 0), 0, cube1.data)

        # Cutout using cache to make sure cube finished writing to S3 first.
        sp.cutout(self.resource, (600, 0, 0), (self.x_dim, self.y_dim, 16), 0)

        cube2 = sp.cutout(self.resource, (600, 0, 0), (self.x_dim, self.y_dim, 16), 0, access_mode="no_cache")

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_cutout_no_time_single_unaligned_hit(self):
        """Test the get_cubes method - no time - single - unaligned - hit"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.random()
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (600, 0, 0), 0, cube1.data)

        cube2 = sp.cutout(self.resource, (600, 0, 0), (self.x_dim, self.y_dim, 16), 0)

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_cutout_time0_single_aligned_no_cache(self):
        """Test the get_cubes method - w/ time - single - bypass cache"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim], time_range=[0, 5])
        cube1.random()
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data)

        # Cutout using cache to make sure cube finished writing to S3 first.
        sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0, time_sample_range=[0, 5])

        cube2 = sp.cutout(
            self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0, time_sample_range=[0, 5], access_mode="no_cache")

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_cutout_time0_single_aligned_hit(self):
        """Test the get_cubes method - w/ time - single - hit"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim], time_range=[0, 5])
        cube1.random()
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data)

        cube2 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0, time_sample_range=[0, 5])

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_cutout_time_offset_single_aligned_no_cache(self):
        """Test the get_cubes method - w/ time - single - bypass cache"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim], time_range=[0, 3])
        cube1.random()
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data, time_sample_start=6)

        # Cutout using cache to make sure cube finished writing to S3 first.
        sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0, time_sample_range=[6, 9])

        cube2 = sp.cutout(
            self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0, time_sample_range=[6, 9], access_mode="no_cache")

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_cutout_time_offset_single_aligned_hit(self):
        """Test the get_cubes method - w/ time - single - hit"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim], time_range=[0, 3])
        cube1.random()
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data, time_sample_start=6)

        cube2 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0, time_sample_range=[6, 9])

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_cutout_no_time_multi_unaligned_no_cache(self):
        """Test the get_cubes method - no time - multi - unaligned - bypass cache"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [400, 400, 8])
        cube1.random()
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (200, 600, 3), 0, cube1.data)

        # Cutout using cache to make sure cube finished writing to S3 first.
        sp.cutout(self.resource, (200, 600, 3), (400, 400, 8), 0)

        cube2 = sp.cutout(self.resource, (200, 600, 3), (400, 400, 8), 0, access_mode="no_cache")

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_cutout_no_time_multi_unaligned_hit(self):
        """Test the get_cubes method - no time - multi - unaligned - hit"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [400, 400, 8])
        cube1.random()
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (200, 600, 3), 0, cube1.data)

        cube2 = sp.cutout(self.resource, (200, 600, 3), (400, 400, 8), 0)

        np.testing.assert_array_equal(cube1.data, cube2.data)

        # do it again...shoudl be in cache
        cube2 = sp.cutout(self.resource, (200, 600, 3), (400, 400, 8), 0)

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_cutout_no_time_multi_unaligned_hit_iso_below(self):
        """Test write_cuboid and cutout methods - no time - multi - unaligned - hit - isotropic, below iso fork"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [400, 400, 8])
        cube1.random()
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (200, 600, 3), 0, cube1.data, iso=True)

        cube2 = sp.cutout(self.resource, (200, 600, 3), (400, 400, 8), 0, iso=True)

        np.testing.assert_array_equal(cube1.data, cube2.data)

        # do it again...should be in cache
        cube2 = sp.cutout(self.resource, (200, 600, 3), (400, 400, 8), 0, iso=True)

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_cutout_no_time_multi_unaligned_hit_iso_above(self):
        """Test write_cuboid and cutout methods - no time - multi - unaligned - hit - isotropic, above iso fork"""
        data = self.data
        data["channel"]["base_resolution"] = 5
        resource = BossResourceBasic(data)

        # Generate random data
        cube1 = Cube.create_cube(resource, [400, 400, 8])
        cube1.random()
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(resource, (200, 600, 3), 5, cube1.data, iso=True)

        cube2 = sp.cutout(resource, (200, 600, 3), (400, 400, 8), 5, iso=True)

        np.testing.assert_array_equal(cube1.data, cube2.data)

        # do it again...should be in cache
        cube2 = sp.cutout(resource, (200, 600, 3), (400, 400, 8), 5, iso=True)

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_cutout_iso_not_present(self):
        """Test write_cuboid and cutout methods with iso option, testing iso is stored in parallel"""
        data = self.data
        data["channel"]["base_resolution"] = 5
        resource = BossResourceBasic(data)

        # Generate random data
        cube1 = Cube.create_cube(resource, [400, 400, 8])
        cube1.random()
        cube1.morton_id = 0

        cubez = Cube.create_cube(resource, [400, 400, 8])
        cubez.zeros()
        cubez.morton_id = 0

        # Write at 5, not iso, and verify
        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(resource, (200, 600, 3), 5, cube1.data, iso=False)

        cube2 = sp.cutout(resource, (200, 600, 3), (400, 400, 8), 5, iso=False)

        np.testing.assert_array_equal(cube1.data, cube2.data)

        # Get at res 5 iso, which should be blank
        cube2 = sp.cutout(resource, (200, 600, 3), (400, 400, 8), 5, iso=True)

        np.testing.assert_array_equal(cubez.data, cube2.data)

    def test_cutout_iso_below_fork(self):
        """Test write_cuboid and cutout methods with iso option, testing iso is equal below the res fork"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [400, 400, 8])
        cube1.random()
        cube1.morton_id = 0

        cubez = Cube.create_cube(self.resource, [400, 400, 8])
        cubez.zeros()
        cubez.morton_id = 0

        # Write at 5, not iso, and verify
        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (200, 600, 3), 0, cube1.data, iso=False)

        cube2 = sp.cutout(self.resource, (200, 600, 3), (400, 400, 8), 0, iso=False)

        np.testing.assert_array_equal(cube1.data, cube2.data)

        # Get at res 5 iso, which should be equal to non-iso call
        cube2 = sp.cutout(self.resource, (200, 600, 3), (400, 400, 8), 0, iso=True)

        np.testing.assert_array_equal(cube1.data, cube2.data)
    
    def test_cutout_to_black_no_time_single_aligned_no_iso(self):
        """Test the write_cuboid method - to black - no time - single - aligned - no iso"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.random()
        cube1.morton_id = 0
        
        cubeb = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cubeb.ones()
        cubeb.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        # write data cuboid
        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data)

        # write to_black
        sp.write_cuboid(self.resource, (0, 0, 0), 0, cubeb.data, to_black=True)

        # get cuboid back
        cube2 = sp.cutout(
            self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0)

        # expected result
        cubez = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cubez.zeros()
        cubez.morton_id = 0

        np.testing.assert_array_equal(cube2.data, cubez.data)
    
    def test_cutout_to_black_no_time_single_unaligned_no_iso(self):
        """Test the write_cuboid method - to black - no time - single - unaligned - no iso"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.random()
        cube1.morton_id = 0
        
        # Only blacking out half the cuboid.
        cubeb = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim//2])
        cubeb.ones()
        cubeb.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        # write data cuboid
        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data)

        # write to_black
        sp.write_cuboid(self.resource, (0, 0, 0), 0, cubeb.data, to_black=True)

        # get cuboid back
        cube2 = sp.cutout(
            self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0)

        # expected result
        expected_data = cube1.data
        expected_data[:, :self.z_dim//2, :, :] = 0

        np.testing.assert_array_equal(cube2.data, expected_data)

    def test_cutout_to_black_no_time_single_aligned_iso(self):
        """Test the write_cuboid method - to black - no time - single - aligned - iso"""
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.random()
        cube1.morton_id = 0
        
        cubeb = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cubeb.ones()
        cubeb.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        # write data cuboid
        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data, iso=True)

        # write to_black
        sp.write_cuboid(self.resource, (0, 0, 0), 0, cubeb.data, iso=True, to_black=True)

        # get cuboid back
        cube2 = sp.cutout(
            self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0, iso=True)

        # expected result
        cubez = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cubez.zeros()
        cubez.morton_id = 0

        np.testing.assert_array_equal(cube2.data, cubez.data)
    
    def test_cutout_to_black_time_single_aligned_no_iso(self):
        """Test the write_cuboid method - to black - time - single - aligned - no iso"""
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim], time_range=[0, 3])
        cube1.random()
        cube1.morton_id = 0
        
        cubeb = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim], time_range=[0, 3])
        cubeb.ones()
        cubeb.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        # write data cuboid
        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data)

        # write to_black
        sp.write_cuboid(self.resource, (0, 0, 0), 0, cubeb.data, to_black=True)

        # get cuboid back
        cube2 = sp.cutout(
            self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0, time_sample_range=[0, 3])

        # expected result
        cubez = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim], time_range=[0, 3])
        cubez.zeros()
        cubez.morton_id = 0

        np.testing.assert_array_equal(cube2.data, cubez.data)

class TestIntegrationSpatialDBImage8Data(SpatialDBImageDataTestMixin,
                                         SpatialDBImageDataIntegrationTestMixin, unittest.TestCase):
    layer = AWSSetupLayer

    @classmethod
    def setUpClass(cls):
        """Clean kv store in between tests"""
        client = redis.StrictRedis(host=cls.kvio_config['cache_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()
        client = redis.StrictRedis(host=cls.state_config['cache_state_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()

    def setUp(self):
        """ Copy params from the Layer setUpClass
        """
        # Setup Data
        self.data = self.layer.setup_helper.get_image8_dict()
        self.resource = BossResourceBasic(self.data)

        # Setup config
        self.kvio_config = self.layer.kvio_config
        self.state_config = self.layer.state_config
        self.object_store_config = self.layer.object_store_config

    def tearDown(self):
        """Clean kv store in between tests"""
        client = redis.StrictRedis(host=self.kvio_config['cache_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()
        client = redis.StrictRedis(host=self.state_config['cache_state_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()


class TestIntegrationSpatialDBImage16Data(SpatialDBImageDataTestMixin,
                                          SpatialDBImageDataIntegrationTestMixin, unittest.TestCase):
    layer = AWSSetupLayer

    @classmethod
    def setUpClass(cls):
        """Clean kv store in between tests"""
        client = redis.StrictRedis(host=cls.kvio_config['cache_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()
        client = redis.StrictRedis(host=cls.state_config['cache_state_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()

    def setUp(self):
        """ Copy params from the Layer setUpClass
        """
        # Setup Data
        self.data = self.layer.setup_helper.get_image16_dict()
        self.resource = BossResourceBasic(self.data)

        # Setup config
        self.kvio_config = self.layer.kvio_config
        self.state_config = self.layer.state_config
        self.object_store_config = self.layer.object_store_config

    def tearDown(self):
        """Clean kv store in between tests"""
        client = redis.StrictRedis(host=self.kvio_config['cache_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()
        client = redis.StrictRedis(host=self.state_config['cache_state_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()


class TestIntegrationSpatialDBImage64Data(SpatialDBImageDataTestMixin,
                                          SpatialDBImageDataIntegrationTestMixin,
                                          unittest.TestCase):
    layer = AWSSetupLayer

    @classmethod
    def setUpClass(cls):
        """Clean kv store in between tests"""
        client = redis.StrictRedis(host=cls.kvio_config['cache_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()
        client = redis.StrictRedis(host=cls.state_config['cache_state_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()

    def setUp(self):
        """ Copy params from the Layer setUpClass
        """
        # Setup Data
        #self.data = self.layer.setup_helper.get_anno64_dict()
        self.data = get_anno_dict()

        # Make the coord frame extra large for this test suite.
        self.data['coord_frame']['x_stop'] = 10000
        self.data['coord_frame']['y_stop'] = 10000
        self.data['coord_frame']['z_stop'] = 10000
        self.resource = BossResourceBasic(self.data)

        # Setup config
        self.kvio_config = self.layer.kvio_config
        self.state_config = self.layer.state_config
        self.object_store_config = self.layer.object_store_config

    def tearDown(self):
        """Clean kv store in between tests"""
        client = redis.StrictRedis(host=self.kvio_config['cache_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()
        client = redis.StrictRedis(host=self.state_config['cache_state_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()

    def test_reserve_id_init(self):
        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        data = self.layer.setup_helper.get_anno64_dict()
        data['lookup_key'] = "100&20124&{}".format(random.randint(3, 999))
        resource = BossResourceBasic(data)

        start_id = sp.reserve_ids(resource, 10)
        self.assertEqual(start_id, 1)

    def test_reserve_id_increment(self):
        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        data = self.layer.setup_helper.get_anno64_dict()
        data['lookup_key'] = "100&20124&{}".format(random.randint(3, 999))
        resource = BossResourceBasic(data)

        start_id = sp.reserve_ids(resource, 10)
        self.assertEqual(start_id, 1)
        start_id = sp.reserve_ids(resource, 5)
        self.assertEqual(start_id, 11)

    def test_filtered_cutout(self):
        time_axis = [1]
        cube_dim = [self.x_dim, self.y_dim, self.z_dim]
        cube_dim_tuple = (self.x_dim, self.y_dim, self.z_dim)
        cube1 = Cube.create_cube(self.resource, cube_dim)
        cube1.data = np.ones(time_axis + [cube_dim[2], cube_dim[1], cube_dim[0]], 
            dtype='uint64')
        cube1.morton_id = 0
        corner = (0, 0, 0)

        expected = np.zeros(time_axis + [cube_dim[2], cube_dim[1], cube_dim[0]], 
            dtype='uint64')

        # Will filter by these ids.
        id1 = 55555
        id2 = 66666
        cube1.data[0][0][40][0] = id1
        cube1.data[0][0][50][0] = id2
        expected[0][0][40][0] = id1
        expected[0][0][50][0] = id2

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)
        resolution = 0
        sp.write_cuboid(self.resource, corner, resolution, cube1.data, time_sample_start=0)

        # Make sure cube written correctly.
        actual_cube = sp.cutout(self.resource, corner, cube_dim_tuple, resolution)
        np.testing.assert_array_equal(cube1.data, actual_cube.data)

        # Method under test.
        actual_filtered = sp.cutout(self.resource, corner, cube_dim_tuple, resolution, 
            filter_ids=[id1, id2])

        np.testing.assert_array_equal(expected, actual_filtered.data)

    def test_filtered_cutout_bad_id_list(self):
        time_axis = [1]
        cube_dim = [self.x_dim, self.y_dim, self.z_dim]
        cube_dim_tuple = (self.x_dim, self.y_dim, self.z_dim)
        cube1 = Cube.create_cube(self.resource, cube_dim)
        cube1.data = np.ones(time_axis + [cube_dim[2], cube_dim[1], cube_dim[0]], dtype='uint64')
        cube1.morton_id = 0
        corner = (6*self.x_dim, 6*self.y_dim, 2*self.z_dim)

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)
        resolution = 0
        sp.write_cuboid(self.resource, corner, resolution, cube1.data, 
            time_sample_start=0)

        # Method under test.
        with self.assertRaises(SpdbError):
            sp.cutout(self.resource, corner, cube_dim_tuple, resolution, 
                filter_ids=['foo', 55555])

    @unittest.skip('Skipping - currently indexing disabled')
    def test_get_ids_in_region_single_cube(self):
        """Test single cuboid using DynamoDB index."""
        cube_dim_tuple = (self.x_dim, self.y_dim, self.z_dim)
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.zeros()
        cube1.data[0][0][40][0] = 55555
        cube1.data[0][0][50][0] = 66666000000000
        pos1 = [2*self.x_dim, 3*self.y_dim, 2*self.z_dim]
        cube1.morton_id = XYZMorton(pos1)

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        resolution = 0
        sp.write_cuboid(self.resource, pos1, resolution, cube1.data, time_sample_start=0)

        # Make sure cube write complete and correct.
        actual_cube = sp.cutout(self.resource, pos1, cube_dim_tuple, resolution)
        np.testing.assert_array_equal(cube1.data, actual_cube.data)

        corner = (2*self.x_dim, 3*self.y_dim, 2*self.z_dim)
        extent = (self.x_dim, self.y_dim, self.z_dim)
        t_range = [0, 1]
        version = 0
        expected = ['55555', '66666000000000']

        # Method under test.
        actual = sp.get_ids_in_region(
            self.resource, resolution, corner, extent, t_range, version)

        self.assertIn('ids', actual)
        self.assertCountEqual(expected, actual['ids'])

    @unittest.skip('Skipping - currently indexing disabled')
    def test_get_ids_in_region_multiple_partial_cubes(self):
        """
        Region cuboid aligned in x, but doesn't span full cuboids in the y 
        and z.
        """
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.zeros()
        cube1.data[0][0][40][0] = 55555
        cube1.data[0][0][50][0] = 66666
        pos1 = [4*self.x_dim, 4*self.y_dim, 2*self.z_dim]
        cube1.morton_id = XYZMorton(pos1)

        cube2 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube2.zeros()
        cube2.data[0][0][40][0] = 55555
        cube2.data[0][0][50][0] = 77777
        pos2 = [5*self.x_dim, 4*self.y_dim, 2*self.z_dim]
        cube2.morton_id = XYZMorton(pos2)

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        resolution = 0
        sp.write_cuboid(self.resource, pos1, resolution, cube1.data, time_sample_start=0)
        sp.write_cuboid(self.resource, pos2, resolution, cube2.data, time_sample_start=0)

        # Not verifying writes here because get_ids_in_region() should be doing
        # cutouts due to the region not containing full cuboids.

        corner = (4*self.x_dim, 4*self.y_dim, 2*self.z_dim)
        extent = (2*self.x_dim, 60, 10)
        t_range = [0, 1]
        version = 0
        expected = ['55555', '66666', '77777']

        # Method under test.
        actual = sp.get_ids_in_region(
            self.resource, resolution, corner, extent, t_range, version)

        self.assertIn('ids', actual)
        self.assertCountEqual(expected, actual['ids'])

    @unittest.skip('Skipping - currently indexing disabled')
    def test_get_ids_in_region_multiple_cubes_and_x_partials(self):
        """
        Region has some full cuboids and some partial cuboids along the x axis.
        """
        cube_dim_tuple = (self.x_dim, self.y_dim, self.z_dim)
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.zeros()
        cube1.data[0][0][40][105] = 55555
        cube1.data[0][0][50][105] = 66666
        pos1 = [7*self.x_dim, 5*self.y_dim, 2*self.z_dim]
        cube1.morton_id = XYZMorton(pos1)

        cube2 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube2.zeros()
        cube2.data[0][0][40][105] = 55555
        cube2.data[0][0][50][105] = 77777
        pos2 = [8*self.x_dim, 5*self.y_dim, 2*self.z_dim]
        cube2.morton_id = XYZMorton(pos2)

        cube3 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube3.zeros()
        cube3.data[0][0][0][105] = 88888
        pos3 = [9*self.x_dim, 5*self.y_dim, 2*self.z_dim]
        cube3.morton_id = XYZMorton(pos3)

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        resolution = 0
        sp.write_cuboid(self.resource, pos1, resolution, cube1.data, time_sample_start=0)
        sp.write_cuboid(self.resource, pos2, resolution, cube2.data, time_sample_start=0)
        sp.write_cuboid(self.resource, pos3, resolution, cube3.data, time_sample_start=0)

        # Make sure cube write complete and correct.
        actual_cube = sp.cutout(self.resource, pos1, cube_dim_tuple, resolution)
        np.testing.assert_array_equal(cube1.data, actual_cube.data)
        actual_cube = sp.cutout(self.resource, pos2, cube_dim_tuple, resolution)
        np.testing.assert_array_equal(cube2.data, actual_cube.data)
        actual_cube = sp.cutout(self.resource, pos3, cube_dim_tuple, resolution)
        np.testing.assert_array_equal(cube3.data, actual_cube.data)

        corner = (7*self.x_dim+100, 5*self.y_dim, 2*self.z_dim)
        extent = (2*self.x_dim+self.x_dim//2, self.y_dim, self.z_dim)
        t_range = [0, 1]
        version = 0
        expected = ['55555', '66666', '77777', '88888']

        # Method under test.
        actual = sp.get_ids_in_region(
            self.resource, resolution, corner, extent, t_range, version)

        self.assertIn('ids', actual)
        self.assertCountEqual(expected, actual['ids'])

    @unittest.skip('Skipping - currently indexing disabled')
    def test_get_ids_in_region_multiple_cubes_and_y_partials(self):
        """
        Region has some full cuboids and some partial cuboids along the y axis.
        """
        cube_dim_tuple = (self.x_dim, self.y_dim, self.z_dim)
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.zeros()
        cube1.data[0][0][500][105] = 43434
        pos1 = [8*self.x_dim, 4*self.y_dim, 2*self.z_dim]
        cube1.morton_id = XYZMorton(pos1)

        cube2 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube2.zeros()
        cube2.data[0][0][40][105] = 55555
        cube2.data[0][0][50][105] = 77777
        pos2 = [8*self.x_dim, 5*self.y_dim, 2*self.z_dim]
        cube2.morton_id = XYZMorton(pos2)

        cube3 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube3.zeros()
        cube3.data[0][0][0][105] = 99999
        pos3 = [8*self.x_dim, 6*self.y_dim, 2*self.z_dim]
        cube3.morton_id = XYZMorton(pos3)

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        resolution = 0
        sp.write_cuboid(self.resource, pos1, resolution, cube1.data, time_sample_start=0)
        sp.write_cuboid(self.resource, pos2, resolution, cube2.data, time_sample_start=0)
        sp.write_cuboid(self.resource, pos3, resolution, cube3.data, time_sample_start=0)

        # Make sure cube write complete and correct.
        actual_cube = sp.cutout(self.resource, pos1, cube_dim_tuple, resolution)
        np.testing.assert_array_equal(cube1.data, actual_cube.data)
        actual_cube = sp.cutout(self.resource, pos2, cube_dim_tuple, resolution)
        np.testing.assert_array_equal(cube2.data, actual_cube.data)
        actual_cube = sp.cutout(self.resource, pos3, cube_dim_tuple, resolution)
        np.testing.assert_array_equal(cube3.data, actual_cube.data)

        corner = (8*self.x_dim, 4*self.y_dim+self.y_dim//2, 2*self.z_dim)
        extent = (self.x_dim, 2*self.y_dim, self.z_dim)
        t_range = [0, 1]
        version = 0
        expected = ['43434', '55555', '77777', '99999']

        # Method under test.
        actual = sp.get_ids_in_region(
            self.resource, resolution, corner, extent, t_range, version)

        self.assertIn('ids', actual)
        self.assertCountEqual(expected, actual['ids'])

    @unittest.skip('Skipping - currently indexing disabled')
    def test_get_ids_in_region_multiple_cubes_and_z_partials(self):
        """
        Region has some full cuboids and some partial cuboids along the z axis.
        """
        cube_dim_tuple = (self.x_dim, self.y_dim, self.z_dim)
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.zeros()
        cube1.data[0][15][500][105] = 35353
        pos1 = [8*self.x_dim, 5*self.y_dim, 1*self.z_dim]
        cube1.morton_id = XYZMorton(pos1)

        cube2 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube2.zeros()
        cube2.data[0][0][40][105] = 55555
        cube2.data[0][0][50][105] = 77777
        pos2 = [8*self.x_dim, 5*self.y_dim, 2*self.z_dim]
        cube2.morton_id = XYZMorton(pos2)

        cube3 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube3.zeros()
        cube3.data[0][0][0][105] = 98989
        pos3 = [8*self.x_dim, 5*self.y_dim, 3*self.z_dim]
        cube3.morton_id = XYZMorton(pos3)

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        resolution = 0
        sp.write_cuboid(self.resource, pos1, resolution, cube1.data, time_sample_start=0)
        sp.write_cuboid(self.resource, pos2, resolution, cube2.data, time_sample_start=0)
        sp.write_cuboid(self.resource, pos3, resolution, cube3.data, time_sample_start=0)

        # Make sure cube write complete and correct.
        actual_cube = sp.cutout(self.resource, pos1, cube_dim_tuple, resolution)
        np.testing.assert_array_equal(cube1.data, actual_cube.data)
        actual_cube = sp.cutout(self.resource, pos2, cube_dim_tuple, resolution)
        np.testing.assert_array_equal(cube2.data, actual_cube.data)
        actual_cube = sp.cutout(self.resource, pos3, cube_dim_tuple, resolution)
        np.testing.assert_array_equal(cube3.data, actual_cube.data)

        corner = (8*self.x_dim, 5*self.y_dim, 2*self.z_dim-1)
        extent = (self.x_dim, self.y_dim, self.z_dim+3)
        t_range = [0, 1]
        version = 0
        expected = ['35353', '55555', '77777', '98989']

        # Method under test.
        actual = sp.get_ids_in_region(
            self.resource, resolution, corner, extent, t_range, version)

        self.assertIn('ids', actual)
        self.assertCountEqual(expected, actual['ids'])

    @unittest.skip('Skipping - currently indexing disabled')
    def test_get_tight_bounding_box_single_cuboid(self):
        """
        Get the tight bounding box for an object that exists within a single cuboid.
        """
        resolution = 0
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]

        id = 33333
        id_as_str = '33333'
        # Customize resource with so it writes to its own channel and uses a
        # coord frame large enough to encompass the data written.  This is
        # important for proper loose bounding box calculations.
        data = get_anno_dict(boss_key='col1&exp1&ch50', lookup_key='1&1&50')
        data['coord_frame']['x_stop'] = 10000
        data['coord_frame']['y_stop'] = 10000
        data['coord_frame']['z_stop'] = 10000
        resource = BossResourceBasic(data)
        time_sample = 0
        version = 0
        x_rng = [0, x_cube_dim]
        y_rng = [0, y_cube_dim]
        z_rng = [0, z_cube_dim]
        t_rng = [0, 1]

        cube_dim_tuple = (self.x_dim, self.y_dim, self.z_dim)
        cube1 = Cube.create_cube(resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.zeros()
        cube1.data[0][14][500][104] = id
        cube1.data[0][15][501][105] = id
        cube1.data[0][15][502][104] = id
        cube1.data[0][14][503][105] = id

        pos1 = [10*self.x_dim, 15*self.y_dim, 2*self.z_dim]
        cube1.morton_id = XYZMorton(pos1)

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)
        sp.write_cuboid(resource, pos1, resolution, cube1.data, time_sample_start=0)

        # Make sure cube write complete and correct.
        actual_cube = sp.cutout(resource, pos1, cube_dim_tuple, resolution)
        np.testing.assert_array_equal(cube1.data, actual_cube.data)

        # Method under test.
        actual = sp.get_bounding_box(resource, resolution, id_as_str, bb_type='tight')

        expected = {
            'x_range': [pos1[0]+104, pos1[0]+106],
            'y_range': [pos1[1]+500, pos1[1]+504],
            'z_range': [pos1[2]+14, pos1[2]+16],
            't_range': t_rng
        }

        self.assertEqual(expected, actual)

    @unittest.skip('Skipping - currently indexing disabled')
    def test_get_tight_bounding_box_multi_cuboids_x_axis(self):
        """
        Get the tight bounding box for an object that exists in two cuboids on the x axis.
        """
        resolution = 0
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]

        id = 40000000000
        # Customize resource with so it writes to its own channel and uses a
        # coord frame large enough to encompass the data written.  This is
        # important for proper loose bounding box calculations.
        data = get_anno_dict(boss_key='col1&exp1&ch30', lookup_key='1&1&30')
        data['coord_frame']['x_stop'] = 10000
        data['coord_frame']['y_stop'] = 10000
        data['coord_frame']['z_stop'] = 10000
        resource = BossResourceBasic(data)
        time_sample = 0
        version = 0
        x_rng = [0, x_cube_dim]
        y_rng = [0, y_cube_dim]
        z_rng = [0, z_cube_dim]
        t_rng = [0, 1]

        cube_dim_tuple = (self.x_dim, self.y_dim, self.z_dim)
        cube1 = Cube.create_cube(resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.zeros()
        cube1.data[0][14][500][508] = id
        cube1.data[0][15][501][509] = id
        cube1.data[0][15][502][510] = id
        cube1.data[0][14][503][511] = id

        pos1 = [10*self.x_dim, 15*self.y_dim, 2*self.z_dim]
        cube1.morton_id = XYZMorton(pos1)

        cube2 = Cube.create_cube(resource, [self.x_dim, self.y_dim, self.z_dim])
        cube2.zeros()
        cube2.data[0][14][500][0] = id
        cube2.data[0][15][501][1] = id
        cube2.data[0][15][502][1] = id
        cube2.data[0][14][503][2] = id

        pos2 = [11*self.x_dim, 15*self.y_dim, 2*self.z_dim]
        cube2.morton_id = XYZMorton(pos2)

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)
        sp.write_cuboid(resource, pos1, resolution, cube1.data, time_sample_start=0)
        sp.write_cuboid(resource, pos2, resolution, cube2.data, time_sample_start=0)

        # Make sure cube write complete and correct.
        actual_cube = sp.cutout(resource, pos1, cube_dim_tuple, resolution)
        np.testing.assert_array_equal(cube1.data, actual_cube.data)
        actual_cube2 = sp.cutout(resource, pos2, cube_dim_tuple, resolution)
        np.testing.assert_array_equal(cube2.data, actual_cube2.data)

        # Method under test.
        actual = sp.get_bounding_box(resource, resolution, id, bb_type='tight')

        expected = {
            'x_range': [pos1[0]+508, pos2[0]+3],
            'y_range': [pos1[1]+500, pos2[1]+504],
            'z_range': [pos1[2]+14, pos2[2]+16],
            't_range': t_rng
        }

        self.assertEqual(expected, actual)

    @unittest.skip('Skipping - currently indexing disabled')
    def test_get_tight_bounding_box_multi_cuboids_y_axis(self):
        """
        Get the tight bounding box for an object that exists in two cuboids on the y axis.
        """
        resolution = 0
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]

        id = 33333
        # Customize resource with so it writes to its own channel and uses a
        # coord frame large enough to encompass the data written.  This is
        # important for proper loose bounding box calculations.
        data = get_anno_dict(boss_key='col1&exp1&ch80', lookup_key='1&1&80')
        data['coord_frame']['x_stop'] = 10000
        data['coord_frame']['y_stop'] = 10000
        data['coord_frame']['z_stop'] = 10000
        resource = BossResourceBasic(data)
        time_sample = 0
        version = 0
        x_rng = [0, x_cube_dim]
        y_rng = [0, y_cube_dim]
        z_rng = [0, z_cube_dim]
        t_rng = [0, 1]

        cube_dim_tuple = (self.x_dim, self.y_dim, self.z_dim)
        cube1 = Cube.create_cube(resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.zeros()
        cube1.data[0][14][509][508] = id
        cube1.data[0][15][510][509] = id
        cube1.data[0][15][510][510] = id
        cube1.data[0][14][511][511] = id

        pos1 = [10*self.x_dim, 15*self.y_dim, 2*self.z_dim]
        cube1.morton_id = XYZMorton(pos1)

        cube2 = Cube.create_cube(resource, [self.x_dim, self.y_dim, self.z_dim])
        cube2.zeros()
        cube2.data[0][14][0][508] = id
        cube2.data[0][15][1][509] = id
        cube2.data[0][15][2][510] = id
        cube2.data[0][14][3][511] = id

        pos2 = [10*self.x_dim, 16*self.y_dim, 2*self.z_dim]
        cube2.morton_id = XYZMorton(pos2)

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)
        sp.write_cuboid(resource, pos1, resolution, cube1.data, time_sample_start=0)
        sp.write_cuboid(resource, pos2, resolution, cube2.data, time_sample_start=0)

        # Make sure cube write complete and correct.
        actual_cube = sp.cutout(resource, pos1, cube_dim_tuple, resolution)
        np.testing.assert_array_equal(cube1.data, actual_cube.data)
        actual_cube2 = sp.cutout(resource, pos2, cube_dim_tuple, resolution)
        np.testing.assert_array_equal(cube2.data, actual_cube2.data)

        # Method under test.
        actual = sp.get_bounding_box(resource, resolution, id, bb_type='tight')

        expected = {
            'x_range': [pos1[0]+508, pos2[0]+512],
            'y_range': [pos1[1]+509, pos2[1]+4],
            'z_range': [pos1[2]+14, pos2[2]+16],
            't_range': t_rng
        }

        self.assertEqual(expected, actual)

    @unittest.skip('Skipping - currently indexing disabled')
    def test_get_tight_bounding_box_multi_cuboids_z_axis(self):
        """
        Get the tight bounding box for an object that exists in two cuboids on the y axis.
        """
        resolution = 0
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]

        id = 33333
        # Customize resource with so it writes to its own channel and uses a
        # coord frame large enough to encompass the data written.  This is
        # important for proper loose bounding box calculations.
        data = get_anno_dict(boss_key='col1&exp1&ch100', lookup_key='1&1&100')
        data['coord_frame']['x_stop'] = 10000
        data['coord_frame']['y_stop'] = 10000
        data['coord_frame']['z_stop'] = 10000
        resource = BossResourceBasic(data)
        time_sample = 0
        version = 0
        x_rng = [0, x_cube_dim]
        y_rng = [0, y_cube_dim]
        z_rng = [0, z_cube_dim]
        t_rng = [0, 1]

        cube_dim_tuple = (self.x_dim, self.y_dim, self.z_dim)
        cube1 = Cube.create_cube(resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.zeros()
        cube1.data[0][14][509][508] = id
        cube1.data[0][15][510][509] = id
        cube1.data[0][15][510][510] = id
        cube1.data[0][14][511][511] = id

        pos1 = [10*self.x_dim, 15*self.y_dim, 2*self.z_dim]
        cube1.morton_id = XYZMorton(pos1)

        cube2 = Cube.create_cube(resource, [self.x_dim, self.y_dim, self.z_dim])
        cube2.zeros()
        cube2.data[0][0][509][508] = id
        cube2.data[0][0][510][509] = id
        cube2.data[0][1][510][510] = id
        cube2.data[0][2][511][511] = id

        pos2 = [10*self.x_dim, 15*self.y_dim, 3*self.z_dim]
        cube2.morton_id = XYZMorton(pos2)

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)
        sp.write_cuboid(resource, pos1, resolution, cube1.data, time_sample_start=0)
        sp.write_cuboid(resource, pos2, resolution, cube2.data, time_sample_start=0)

        # Make sure cube write complete and correct.
        actual_cube = sp.cutout(resource, pos1, cube_dim_tuple, resolution)
        np.testing.assert_array_equal(cube1.data, actual_cube.data)
        actual_cube2 = sp.cutout(resource, pos2, cube_dim_tuple, resolution)
        np.testing.assert_array_equal(cube2.data, actual_cube2.data)
        del cube1
        del actual_cube
        del cube2
        del actual_cube2


        # Method under test.
        actual = sp.get_bounding_box(resource, resolution, id, bb_type='tight')

        expected = {
            'x_range': [pos1[0]+508, pos2[0]+512],
            'y_range': [pos1[1]+509, pos2[1]+512],
            'z_range': [pos1[2]+14, pos2[2]+3],
            't_range': t_rng
        }

        self.assertEqual(expected, actual)
