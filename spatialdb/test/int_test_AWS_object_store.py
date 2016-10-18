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

from spdb.spatialdb import AWSObjectStore
from .test_AWS_object_store import AWSObjectStoreTestMixin

from spdb.spatialdb.test.setup import AWSSetupLayer


class AWSObjectStoreTestIntegrationMixin(object):
    # TODO: implement tests here or remove
    def test_put_get_objects_async(self):
        """Method to test putting and getting objects to and from S3"""
        #os = AWSObjectStore(self.object_store_config)

        #cached_cuboid_keys = ["CACHED-CUBOID&1&1&1&0&0&12", "CACHED-CUBOID&1&1&1&0&0&13"]
        #fake_data = [b"aaaadddffffaadddfffaadddfff", b"fffddaaffddffdfffaaa"]

        #object_keys = os.cached_cuboid_to_object_keys(cached_cuboid_keys)

        #os.put_objects(object_keys, fake_data)

        #returned_data = os.get_objects_async(object_keys)
        #for rdata, sdata in zip(returned_data, fake_data):
        #    assert rdata == sdata
        pass

    def test_page_in_objects(self):
        """Test method for paging in objects from S3 via lambda"""
        # os = AWSObjectStore(self.object_store_config)
        #
        # cached_cuboid_keys = ["CACHED-CUBOID&1&1&1&0&0&12", "CACHED-CUBOID&1&1&1&0&0&13"]
        # page_in_channel = "dummy_channel"
        # kv_config = {"param1": 1, "param2": 2}
        # state_config = {"param1": 1, "param2": 2}
        #
        # object_keys = os.page_in_objects(cached_cuboid_keys,
        #                                page_in_channel,
        #                                kv_config,
        #                                state_config)

        pass

    def test_trigger_page_out(self):
        """Test method for paging out objects to S3 via lambda"""
        # os = AWSObjectStore(self.object_store_config)
        #
        # cached_cuboid_keys = ["CACHED-CUBOID&1&1&1&0&0&12", "CACHED-CUBOID&1&1&1&0&0&13"]
        # page_in_channel = "dummy_channel"
        # kv_config = {"param1": 1, "param2": 2}
        # state_config = {"param1": 1, "param2": 2}
        #
        # object_keys = os.page_in_objects(cached_cuboid_keys,
        #                                page_in_channel,
        #                                kv_config,
        #                                state_config)

        pass


class TestAWSObjectStoreInt(AWSObjectStoreTestIntegrationMixin, AWSObjectStoreTestMixin, unittest.TestCase):
    layer = AWSSetupLayer

    def setUp(self):
        """ Copy params from the Layer setUpClass
        """
        self.data = self.layer.data
        self.resource = self.layer.resource
        self.object_store_config = self.layer.object_store_config



