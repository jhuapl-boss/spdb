# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from spdb.spatialdb.object_indices import ObjectIndices

from bossutils.aws import get_region
import numpy as np
from spdb.c_lib.ndlib import MortonXYZ, XYZMorton
from spdb.project import BossResourceBasic
from spdb.project.test.resource_setup import get_anno_dict
from spdb.spatialdb.object import AWSObjectStore
import unittest
from unittest.mock import patch
import random

from bossutils import configuration

from spdb.project import BossResourceBasic
from spdb.spatialdb.test.setup import SetupTests
from spdb.spatialdb.error import SpdbError


class TestObjectIndicesMixin(object):
    def setUp(self):
        # Randomize the look-up key so tests don't mess with each other
        self.resource._lookup_key = "1&2&{}".format(random.randint(4, 1000))


    def test_make_ids_strings_ignore_zeros(self):
        zeros = np.zeros(4, dtype='uint64')
        expected = []
        actual = self.obj_ind._make_ids_strings(zeros)
        self.assertEqual(expected, actual)

    def test_make_ids_strings_mix(self):
        arr = np.zeros(4, dtype='uint64')
        arr[0] = 12345
        arr[2] = 9876

        expected = ['12345', '9876']
        actual = self.obj_ind._make_ids_strings(arr)
        self.assertEqual(expected, actual)

    def test_get_bounding_box_loose(self):

        # Only need for the AWSObjectStore's generate_object_key() method, so
        # can provide dummy values to initialize it.
        with patch('spdb.spatialdb.object.get_region') as fake_get_region:
            # Force us-east-1 region for testing.
            fake_get_region.return_value = 'us-east-1'
            obj_store = AWSObjectStore(self.object_store_config)

        pos0 = [4, 4, 4]
        pos1 = [2, 2, 2]
        pos2 = [6, 6, 6]

        mort0 = XYZMorton(pos0)
        mort1 = XYZMorton(pos1)
        mort2 = XYZMorton(pos2)

        resolution = 0
        time_sample = 0

        resource = BossResourceBasic(data=get_anno_dict())

        key0 = self.obj_store.generate_object_key(resource, resolution, time_sample, mort0)
        key1 = self.obj_store.generate_object_key(resource, resolution, time_sample, mort1)
        key2 = self.obj_store.generate_object_key(resource, resolution, time_sample, mort2)

        with patch.object(self.obj_ind, 'get_cuboids') as fake_get_cuboids:
            pass

    def test_create_id_counter_key(self):
        self.resource._lookup_key = "1&2&3"
        key = self.obj_ind.generate_reserve_id_key(self.resource)
        self.assertEqual(key, '14a343245e1adb6297e43c12e22770ad&1&2&3')

    def test_reserve_id_wrong_type(self):
        img_data = self.setup_helper.get_image8_dict()
        img_resource = BossResourceBasic(img_data)

        with self.assertRaises(SpdbError):
            start_id = self.obj_ind.reserve_ids(img_resource, 10)

    def test_reserve_id_init(self):
        start_id = self.obj_ind.reserve_ids(self.resource, 10)
        self.assertEqual(start_id, 11)

    def test_reserve_id_increment(self):
        start_id = self.obj_ind.reserve_ids(self.resource, 10)
        self.assertEqual(start_id, 11)
        start_id = self.obj_ind.reserve_ids(self.resource, 5)
        self.assertEqual(start_id, 16)


class TestObjectIndices(TestObjectIndicesMixin, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """ Create a diction of configuration values for the test resource. """
        # Create resource
        cls.setup_helper = SetupTests()
        cls.data = cls.setup_helper.get_anno64_dict()
        cls.resource = BossResourceBasic(cls.data)

        # Load config
        cls.config = configuration.BossConfig()
        cls.object_store_config = {"s3_flush_queue": 'https://mytestqueue.com',
                                   "cuboid_bucket": "test_bucket",
                                   "page_in_lambda_function": "page_in.test.boss",
                                   "page_out_lambda_function": "page_out.test.boss",
                                   "s3_index_table": "test_s3_table",
                                   "id_index_table": "test_id_table",
                                   "id_count_table": "test_count_table",
                                   }

        # Create AWS Resources needed for tests while mocking
        cls.setup_helper.start_mocking()
        cls.setup_helper.create_index_table(cls.object_store_config["id_count_table"], cls.setup_helper.ID_COUNT_SCHEMA)

        cls.obj_ind = ObjectIndices(cls.object_store_config["s3_index_table"],
                                    cls.object_store_config["id_index_table"],
                                    cls.object_store_config["id_count_table"],
                                    'us-east-1')


    @classmethod
    def tearDownClass(cls):
        cls.setup_helper.stop_mocking()

if __name__ == '__main__':
    unittest.main()

