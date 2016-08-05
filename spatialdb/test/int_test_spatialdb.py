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


class SpatialDBImageDataIntegrationTestMixin(object):

    def test_cutout_no_time_single_aligned_zero(self):
        """Test the get_cubes method - no time - single"""
        db = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        cube = db.cutout(self.resource, (0, 0, 0), (128, 128, 16), 0)

        np.testing.assert_array_equal(np.sum(cube.data), 0)

    def test_cutout_no_time_single_aligned_hit(self):
        """Test the get_cubes method - no time - single"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [128, 128, 16])
        cube1.data = np.random.randint(0, 254, (1, 16, 128, 128))
        cube1.morton_id = 0

        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        sp.write_cuboid(self.resource, (0, 0, 0), 0, cube1.data)

        cube2 = sp.cutout(self.resource, (0, 0, 0), (128, 128, 16), 0)

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_cutout_no_time_single_aligned_miss(self):
        """Test the get_cubes method - no time - single"""
        # Generate random data
        #cube1 = Cube.create_cube(self.resource, [128, 128, 16])
        #cube1.data = np.random.randint(0, 254, (1, 16, 128, 128))
        #cube1.morton_id = 0
#
        #db = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)
#
        ## populate dummy data
        #self.write_test_cube(db, self.resource, 0, cube1, cache=False, s3=True)
#
        #cube2 = db.cutout(self.resource, (0, 0, 0), (128, 128, 16), 0)
#
        #np.testing.assert_array_equal(cube1.data, cube2.data)
        assert 1==0


class TestIntegrationSpatialDBImage8Data(SpatialDBImageDataTestMixin,
                                         SpatialDBImageDataIntegrationTestMixin, unittest.TestCase):

    def setUpParams(self):
        """ Create a diction of configuration values for the test resource. """
        # setup resources
        self.setup_helper = SetupTests()
        self.setup_helper.mock = False

        self.data = self.setup_helper.get_image8_dict()
        self.resource = BossResourceBasic(self.data)

        self.config = configuration.BossConfig()

        # kvio settings
        self.cache_client = redis.StrictRedis(host=self.config['aws']['cache'], port=6379,
                                              db=1,
                                              decode_responses=False)
        self.kvio_config = {"cache_client": self.cache_client, "read_timeout": 86400}

        # state settings
        self.state_client = redis.StrictRedis(host=self.config['aws']['cache_state'],
                                              port=6379, db=1,
                                              decode_responses=False)
        self.state_config = {"state_client": self.state_client}

        # object store settings
        self.object_store_config = {"s3_flush_queue": self.config['aws']['flush_topic_arn'],
                                    "cuboid_bucket": "intTest.{}".format(self.config['aws']['s3-bucket']),
                                    "page_in_lambda_function": self.config['lambda']['page_in_function'],
                                    "page_out_lambda_function": self.config['lambda']['flush_function'],
                                    "s3_index_table": "intTest.{}".format(self.config['aws']['s3-index-table'])}

        # Create AWS Resources needed for tests
        self.setup_helper.create_s3_index_table(self.object_store_config["s3_index_table"])
        self.setup_helper.create_cuboid_bucket(self.object_store_config["cuboid_bucket"])

    @classmethod
    def setUpClass(cls):
        """ get_some_resource() is slow, to avoid calling it for each test use setUpClass()
            and store the result as class variable
        """
        super(TestIntegrationSpatialDBImage8Data, cls).setUpClass()
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
            cls.object_store_config["s3_flush_queue"] = cls.setup_helper.create_flush_queue(
                "intTest.{}".format(cls.config['aws']['flush_topic_arn']))
        except ClientError:
            cls.setup_helper.delete_flush_queue(cls.object_store_config["cuboid_bucket"])
            time.sleep(60)
            cls.object_store_config["s3_flush_queue"] = cls.setup_helper.create_flush_queue(
                "intTest.{}".format(cls.config['aws']['flush_topic_arn']))

    @classmethod
    def tearDownClass(cls):
        super(TestIntegrationSpatialDBImage8Data, cls).tearDownClass()
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
