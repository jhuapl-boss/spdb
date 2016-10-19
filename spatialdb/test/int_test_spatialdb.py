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

from spdb.spatialdb.test.test_spatialdb import SpatialDBImageDataTestMixin
from spdb.spatialdb import Cube, SpatialDB
from spdb.spatialdb.test.setup import AWSSetupLayer
from spdb.c_lib.ndtype import CUBOIDSIZE
from spdb.project.test.resource_setup import get_anno_dict
from spdb.project import BossResourceBasic

import redis


class SpatialDBImageDataIntegrationTestMixin(object):

    cuboid_size = CUBOIDSIZE[0]
    x_dim = cuboid_size[0]
    y_dim = cuboid_size[1]
    z_dim = cuboid_size[2]

    def test_cutout_no_time_single_aligned_hit(self):
        """Test the get_cubes method - no time - single - hit"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.data = np.random.randint(1, 254, (1, self.z_dim, self.y_dim, self.x_dim))
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data)

        cube2 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0)

        np.testing.assert_array_equal(cube1.data, cube2.data)

        cube2 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0)

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_cutout_no_time_single_aligned_miss(self):
        """Test the get_cubes method - no time - single - miss"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.data = np.random.randint(1, 254, (1, self.z_dim, self.y_dim, self.x_dim))
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
        np.testing.assert_array_equal(cube1.data, cube3.data)

    def test_cutout_no_time_single_aligned_existing_hit(self):
        """Test the get_cubes method - no time - aligned - existing data - miss"""
        # Generate random data
        data1 = np.random.randint(1, 254, (self.z_dim, self.y_dim, self.x_dim))

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (0, 0, 0), 0, data1)

        cube2 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0)

        np.testing.assert_array_equal(data1, np.squeeze(cube2.data))
        del data1
        del cube2

        # now write to cuboid again
        data3 = np.random.randint(1, 254, (self.z_dim, self.y_dim, self.x_dim))

        sp.write_cuboid(self.resource, (0, 0, 0), 0, data3)

        cube4 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0)
        np.testing.assert_array_equal(data3, np.squeeze(cube4.data))

    def test_cutout_no_time_single_aligned_hit_shifted(self):
        """Test the get_cubes method - no time - single - hit - shifted into a different location"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.data = np.random.randint(1, 254, (1, self.z_dim, self.y_dim, self.x_dim))
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (self.x_dim, self.y_dim, 0), 0, cube1.data)

        cube2 = sp.cutout(self.resource, (self.x_dim, self.y_dim, 0), (self.x_dim, self.y_dim, self.z_dim), 0)

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_cutout_no_time_single_unaligned_hit(self):
        """Test the get_cubes method - no time - single - unaligned - hit"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.data = np.random.randint(1, 254, (1, self.z_dim, self.y_dim, self.x_dim))
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (600, 0, 0), 0, cube1.data)

        cube2 = sp.cutout(self.resource, (600, 0, 0), (self.x_dim, self.y_dim, 16), 0)

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_cutout_time0_single_aligned_hit(self):
        """Test the get_cubes method - w/ time - single - hit"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.data = np.random.randint(1, 254, (5, self.z_dim, self.y_dim, self.x_dim))
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data)

        cube2 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0, time_sample_range=[0, 5])

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_cutout_time_offset_single_aligned_hit(self):
        """Test the get_cubes method - w/ time - single - hit"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.data = np.random.randint(1, 254, (3, self.z_dim, self.y_dim, self.x_dim))
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data, time_sample_start=6)

        cube2 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0, time_sample_range=[6, 9])

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_cutout_no_time_multi_unaligned_hit(self):
        """Test the get_cubes method - no time - multi - unaligned - hit"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.data = np.random.randint(1, 254, (1, 8, 400, 400))
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (200, 600, 3), 0, cube1.data)

        cube2 = sp.cutout(self.resource, (200, 600, 3), (400, 400, 8), 0)

        np.testing.assert_array_equal(cube1.data, cube2.data)

        # do it again...shoudl be in cache
        cube2 = sp.cutout(self.resource, (200, 600, 3), (400, 400, 8), 0)

        np.testing.assert_array_equal(cube1.data, cube2.data)


class TestIntegrationSpatialDBImage8Data(SpatialDBImageDataTestMixin,
                                         SpatialDBImageDataIntegrationTestMixin, unittest.TestCase):
    layer = AWSSetupLayer

    def setUp(self):
        """ Copy params from the Layer setUpClass
        """
        self.data = self.layer.data
        self.resource = self.layer.resource
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

    def setUp(self):
        """ Copy params from the Layer setUpClass
        """
        self.data = self.layer.data
        self.data['channel']['datatype'] = 'uint16'
        self.resource = self.layer.resource
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


#class TestIntegrationSpatialDBImage64Data(SpatialDBImageDataTestMixin,
#                                          SpatialDBImageDataIntegrationTestMixin, unittest.TestCase):
#    layer = AWSSetupLayer
#
#    def setUp(self):
#        """ Copy params from the Layer setUpClass
#        """
#        self.data = get_anno_dict()
#        self.resource = BossResourceBasic(self.data)
#        self.kvio_config = self.layer.kvio_config
#        self.state_config = self.layer.state_config
#        self.object_store_config = self.layer.object_store_config
#
#    def tearDown(self):
#        """Clean kv store in between tests"""
#        client = redis.StrictRedis(host=self.kvio_config['cache_host'],
#                                   port=6379, db=1, decode_responses=False)
#        client.flushdb()
#        client = redis.StrictRedis(host=self.state_config['cache_state_host'],
#                                   port=6379, db=1, decode_responses=False)
#        client.flushdb()
#