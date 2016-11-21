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
                                          SpatialDBImageDataIntegrationTestMixin, unittest.TestCase):
    layer = AWSSetupLayer

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
        self.data = self.layer.setup_helper.get_anno64_dict()
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
