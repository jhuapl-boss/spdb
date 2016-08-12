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
from spdb.spatialdb.test.test_spatialdb import SpatialDBImageDataTestMixin
from spdb.spatialdb import Cube, SpatialDB
from spdb.spatialdb.test.setup import SetupTests

import redis
import time
from botocore.exceptions import ClientError

from bossutils import configuration


"""
Test lambda page in function.  Note, tests assume cuboid size is 
(128, 128, 16).
"""
class TestIntegrationLambdaPageInImage8Data(unittest.TestCase):

    def tearDown(self):
        """Clean kv store in between tests"""
        client = redis.StrictRedis(host=self.kvio_config['cache_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()
        client = redis.StrictRedis(host=self.state_config['cache_state_host'],
                                   port=6379, db=1, decode_responses=False)
        client.flushdb()

    def setUpParams(self):
        """ Create a diction of configuration values for the test resource. """
        # setup resources
        self.setup_helper = SetupTests()
        self.setup_helper.mock = False

        self.data = self.setup_helper.get_image8_dict()
        self.resource = BossResourceBasic(self.data)

        self.config = configuration.BossConfig()

        # kvio settings
        self.kvio_config = {"cache_host": self.config['aws']['cache'],
                            "cache_db": "1",
                            "read_timeout": 86400}

        # state settings
        self.state_config = {"cache_state_host": self.config['aws']['cache-state'], "cache_state_db": "1"}

        # object store settings
        _, domain = self.config['aws']['cuboid_bucket'].split('.', 1)
        self.s3_flush_queue_name = "intTest.S3FlushQueue.{}".format(domain).replace('.', '-')
        self.object_store_config = {"s3_flush_queue": "",
                                    "cuboid_bucket": "intTest.{}".format(self.config['aws']['cuboid_bucket']),
                                    "page_in_lambda_function": self.config['lambda']['page_in_function'],
                                    "page_out_lambda_function": self.config['lambda']['flush_function'],
                                    "s3_index_table": "intTest.{}".format(self.config['aws']['s3-index-table'])}

    @classmethod
    def setUpClass(cls):
        """ get_some_resource() is slow, to avoid calling it for each test use setUpClass()
            and store the result as class variable
        """
        super(TestIntegrationLambdaPageInImage8Data, cls).setUpClass()
        cls.setUpParams(cls)
        try:
            cls.setup_helper.create_s3_index_table(cls.object_store_config["s3_index_table"])
        except ClientError:
            cls.setup_helper.delete_s3_index_table(cls.object_store_config["s3_index_table"])
            cls.setup_helper.create_s3_index_table(cls.object_store_config["s3_index_table"])

        try:
            cls.setup_helper.create_cuboid_bucket(cls.object_store_config["cuboid_bucket"])
        except ClientError:
            cls.setup_helper.delete_cuboid_bucket(cls.object_store_config["cuboid_bucket"])
            cls.setup_helper.create_cuboid_bucket(cls.object_store_config["cuboid_bucket"])

        try:
            cls.object_store_config["s3_flush_queue"] = cls.setup_helper.create_flush_queue(cls.s3_flush_queue_name)
        except ClientError:
            cls.setup_helper.delete_flush_queue(cls.object_store_config["s3_flush_queue"])
            time.sleep(60)
            cls.object_store_config["s3_flush_queue"] = cls.setup_helper.create_flush_queue(cls.s3_flush_queue_name)

    @classmethod
    def tearDownClass(cls):
        super(TestIntegrationLambdaPageInImage8Data, cls).tearDownClass()
        try:
            cls.setup_helper.delete_s3_index_table(cls.object_store_config["s3_index_table"])
        except:
            pass

        try:
            cls.setup_helper.delete_cuboid_bucket(cls.object_store_config["cuboid_bucket"])
        except:
            pass

        try:
            cls.setup_helper.delete_flush_queue(cls.object_store_config["s3_flush_queue"])
        except:
            pass

    def get_num_cache_keys(self, spdb):
        return len(self.cache_client.keys("*"))

    def get_num_status_keys(self, spdb):
        return len(self.status_client.keys("*"))

    def test_page_in_single_cuboid(self):
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [128, 128, 16])
        cube1.data = np.random.randint(1, 254, (1, 16, 128, 128))
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data)

        cube2 = sp.cutout(self.resource, (0, 0, 0), (128, 128, 16), 0)

        np.testing.assert_array_equal(cube1.data, cube2.data)

        # Make sure data is the same
        np.testing.assert_array_equal(cube1.data, cube2.data)

        # Delete everything in the cache
        sp.kvio.cache_client.flushdb()

        # Force use of lambda function.
        sp.read_lambda_threshold = 0

        # Get the data again, which should trigger lambda page in.
        cube3 = sp.cutout(self.resource, (0, 0, 0), (128, 128, 16), 0)

        # Make sure the data is the same
        np.testing.assert_array_equal(cube1.data, cube3.data)

    def test_page_in_multi_cuboids_x_dir(self):
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [256, 128, 16])
        cube1.data = np.random.randint(1, 254, (1, 16, 128, 256))
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data)

        cube2 = sp.cutout(self.resource, (0, 0, 0), (256, 128, 16), 0)

        np.testing.assert_array_equal(cube1.data, cube2.data)

        # Make sure data is the same
        np.testing.assert_array_equal(cube1.data, cube2.data)

        # Delete everything in the cache
        sp.kvio.cache_client.flushdb()

        # Force use of lambda function.
        sp.read_lambda_threshold = 0

        # Get the data again, which should trigger lambda page in.
        cube3 = sp.cutout(self.resource, (0, 0, 0), (256, 128, 16), 0)

        # Make sure the data is the same
        np.testing.assert_array_equal(cube1.data, cube3.data)

    def test_page_in_multi_cuboids_y_dir(self):
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [128, 256, 16])
        cube1.data = np.random.randint(1, 254, (1, 16, 256, 128))
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data)

        cube2 = sp.cutout(self.resource, (0, 0, 0), (128, 256, 16), 0)

        np.testing.assert_array_equal(cube1.data, cube2.data)

        # Make sure data is the same
        np.testing.assert_array_equal(cube1.data, cube2.data)

        # Delete everything in the cache
        sp.kvio.cache_client.flushdb()

        # Force use of lambda function.
        sp.read_lambda_threshold = 0

        # Get the data again, which should trigger lambda page in.
        cube3 = sp.cutout(self.resource, (0, 0, 0), (128, 256, 16), 0)

        # Make sure the data is the same
        np.testing.assert_array_equal(cube1.data, cube3.data)

    def test_page_in_multi_cuboids_z_dir(self):
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [128, 128, 32])
        cube1.data = np.random.randint(1, 254, (1, 32, 128, 128))
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data)

        cube2 = sp.cutout(self.resource, (0, 0, 0), (128, 128, 32), 0)

        np.testing.assert_array_equal(cube1.data, cube2.data)

        # Make sure data is the same
        np.testing.assert_array_equal(cube1.data, cube2.data)

        # Delete everything in the cache
        sp.kvio.cache_client.flushdb()

        # Force use of lambda function.
        sp.read_lambda_threshold = 0

        # Get the data again, which should trigger lambda page in.
        cube3 = sp.cutout(self.resource, (0, 0, 0), (128, 128, 32), 0)

        # Make sure the data is the same
        np.testing.assert_array_equal(cube1.data, cube3.data)

