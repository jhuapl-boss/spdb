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

from spdb.project import BossResourceBasic
from spdb.spatialdb import Cube, SpatialDB
from spdb.spatialdb.test.setup import AWSSetupLayer
from spdb.c_lib.ndtype import CUBOIDSIZE

import redis
import time
from botocore.exceptions import ClientError

from bossutils import configuration


"""
Test lambda page in function.  Note, tests assume cuboid size is 
(512, 512, 16).
"""


class TestIntegrationLambdaPageInImage8Data(unittest.TestCase):
    layer = AWSSetupLayer

    cuboid_size = CUBOIDSIZE[0]
    x_dim = cuboid_size[0]
    y_dim = cuboid_size[1]
    z_dim = cuboid_size[2]

    def test_page_in_single_cuboid(self):
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.random()
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data)

        cube2 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0)

        # Make sure data is the same
        np.testing.assert_array_equal(cube1.data, cube2.data)

        # Delete everything in the cache
        sp.kvio.cache_client.flushdb()

        # Force use of lambda function.
        sp.read_lambda_threshold = 0

        # Get the data again, which should trigger lambda page in.
        cube3 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0)

        # Make sure the data is the same
        np.testing.assert_array_equal(cube1.data, cube3.data)

    def test_page_in_multi_cuboids_x_dir(self):
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim * 2, self.y_dim, self.z_dim])
        cube1.random()
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data)

        cube2 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim * 2, self.y_dim, self.z_dim), 0)

        # Make sure data is the same
        np.testing.assert_array_equal(cube1.data, cube2.data)

        # Delete everything in the cache
        sp.kvio.cache_client.flushdb()

        # Force use of lambda function.
        sp.read_lambda_threshold = 0

        # Get the data again, which should trigger lambda page in.
        cube3 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim * 2, self.y_dim, self.z_dim), 0)

        # Make sure the data is the same
        np.testing.assert_array_equal(cube1.data, cube3.data)

    def test_page_in_multi_cuboids_y_dir(self):
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim * 2, self.z_dim])
        cube1.random()
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data)

        cube2 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim * 2, self.z_dim), 0)

        np.testing.assert_array_equal(cube1.data, cube2.data)

        # Make sure data is the same
        np.testing.assert_array_equal(cube1.data, cube2.data)

        # Delete everything in the cache
        sp.kvio.cache_client.flushdb()

        # Force use of lambda function.
        sp.read_lambda_threshold = 0

        # Get the data again, which should trigger lambda page in.
        cube3 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim * 2, self.z_dim), 0)

        # Make sure the data is the same
        np.testing.assert_array_equal(cube1.data, cube3.data)

    def test_page_in_multi_cuboids_z_dir(self):
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim * 2])
        cube1.random()
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data)

        cube2 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim * 2), 0)

        np.testing.assert_array_equal(cube1.data, cube2.data)

        # Make sure data is the same
        np.testing.assert_array_equal(cube1.data, cube2.data)

        # Delete everything in the cache
        sp.kvio.cache_client.flushdb()

        # Force use of lambda function.
        sp.read_lambda_threshold = 0

        # Get the data again, which should trigger lambda page in.
        cube3 = sp.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim * 2), 0)

        # Make sure the data is the same
        np.testing.assert_array_equal(cube1.data, cube3.data)

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
        """ Copy params from the nose2 Layer setUpClass
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
