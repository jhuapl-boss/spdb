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

import numpy as np
import unittest

class TestObjectIndices(unittest.TestCase):
    def setUp(self):
        self.obj_ind = ObjectIndices('s3_index', 'id_index', 'us-east-1')

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

if __name__ == '__main__':
    unittest.main()

