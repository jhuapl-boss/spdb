"""
Copyright 2016 The Johns Hopkins University Applied Physics Laboratory

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import unittest
from spdb.spatialdb import SpdbError
import os


@unittest.skipIf(os.environ.get('UNIT_ONLY') is not None, "Only running unit tests")
class SpdbErrorTests(unittest.TestCase):

    def test_creation(self):
        with self.assertRaises(SpdbError):
            raise SpdbError('whoops', 'Something went wrong!', 2000)

    def test_params(self):
        try:
            raise SpdbError('whoops', 'Something went wrong!', 2000)
        except SpdbError as err:
            assert err.args[0] == 'whoops'
            assert err.args[1] == 'Something went wrong!'
            assert err.args[2] == 2000
