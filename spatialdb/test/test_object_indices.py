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

class TestObjectIndices(unittest.TestCase):
    def setUp(self):
        self.obj_ind = ObjectIndices('s3_index', 'id_index', 'us-east-1')

        # Only need for the AWSObjectStore's generate_object_key() method, so
        # can provide dummy values to initialize it.
        with patch('spdb.spatialdb.object.get_region') as fake_get_region:
            # Force us-east-1 region for testing.
            fake_get_region.return_value = 'us-east-1'
            self.obj_store = AWSObjectStore({
                's3_flush_queue': 'foo',
                'cuboid_bucket': 'foo',
                'page_in_lambda_function': 'foo',
                'page_out_lambda_function': 'foo',
                's3_index_table': 'foo',
                'id_index_table': 'foo',
                'id_count_table': 'foo'
            })
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

if __name__ == '__main__':
    unittest.main()

