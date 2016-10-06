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

from spdb.project import BossResourceBasic
from spdb.spatialdb import AWSObjectStore

from bossutils import configuration

from spdb.spatialdb.test.setup import SetupTests

import boto3
from bossutils.aws import get_region


class AWSObjectStoreTestMixin(object):

    def test_object_key_chunks(self):
        """Test method to return object keys in chunks"""
        keys = ['1', '2', '3', '4', '5', '6', '7']
        expected = [['1', '2', '3'],
                    ['4', '5', '6'],
                    ['7']]

        for cnt, chunk in enumerate(AWSObjectStore.object_key_chunks(keys, 3)):
            assert chunk == expected[cnt]

    def test_generate_object_keys(self):
        """Test to create object keys"""
        os = AWSObjectStore(self.object_store_config)
        object_keys = os.generate_object_key(self.resource, 0, 2, 56)

        assert object_keys == '30a1be1564a8b1a1cf7f8e9c4deb643d&4&2&1&0&2&56'

    def test_cached_cuboid_to_object_keys(self):
        """Test to check key conversion from cached cuboid to object"""

        cached_cuboid_keys = ["CACHED-CUBOID&1&1&1&0&0&12", "CACHED-CUBOID&1&1&1&0&0&13"]

        os = AWSObjectStore(self.object_store_config)
        object_keys = os.cached_cuboid_to_object_keys(cached_cuboid_keys)

        assert len(object_keys) == 2
        assert object_keys[0] == '6b5ebb14395dec6cd9d7edaa1fbcd748&1&1&1&0&0&12'
        assert object_keys[1] == '592ed5f40528bb16bce769fed5b2e9c6&1&1&1&0&0&13'

    def test_cached_cuboid_to_object_keys_str(self):
        """Test to check key conversion from cached cuboid to object"""

        cached_cuboid_keys = "CACHED-CUBOID&1&1&1&0&0&12"

        os = AWSObjectStore(self.object_store_config)
        object_keys = os.cached_cuboid_to_object_keys(cached_cuboid_keys)

        assert len(object_keys) == 1
        assert object_keys[0] == '6b5ebb14395dec6cd9d7edaa1fbcd748&1&1&1&0&0&12'

    def test_write_cuboid_to_object_keys(self):
        """Test to check key conversion from cached cuboid to object"""

        write_cuboid_keys = ["WRITE-CUBOID&1&1&1&0&0&12&SDFJlskDJasdfniasdf",
                             "WRITE-CUBOID&1&1&1&0&0&13&KJHDLFHjsdhfshdfhsdfdsf"]

        os = AWSObjectStore(self.object_store_config)
        object_keys = os.write_cuboid_to_object_keys(write_cuboid_keys)

        assert len(object_keys) == 2
        assert object_keys[0] == '6b5ebb14395dec6cd9d7edaa1fbcd748&1&1&1&0&0&12'
        assert object_keys[1] == '592ed5f40528bb16bce769fed5b2e9c6&1&1&1&0&0&13'

    def test_write_cuboid_to_object_atr(self):
        """Test to check key conversion from cached cuboid to object when a string instead of a list is passed"""

        write_cuboid_keys = "WRITE-CUBOID&1&1&1&0&0&12&SDFJlskDJasdfniasdf"

        os = AWSObjectStore(self.object_store_config)
        object_keys = os.write_cuboid_to_object_keys(write_cuboid_keys)

        assert len(object_keys) == 1
        assert object_keys[0] == '6b5ebb14395dec6cd9d7edaa1fbcd748&1&1&1&0&0&12'

    def test_object_to_cached_cuboid_keys(self):
        """Test to check key conversion from cached cuboid to object"""

        object_keys = ['a4931d58076dc47773957809380f206e4228517c9fa6daed536043782024e480&1&1&1&0&0&12',
                       'f2b449f7e247c8aec6ecf754388a65ee6ea9dc245cd5ef149aebb2e0d20b4251&1&1&1&0&0&13']

        os = AWSObjectStore(self.object_store_config)
        cached_cuboid_keys = os.object_to_cached_cuboid_keys(object_keys)

    def test_object_to_cached_cuboid_keys_str(self):
        """Test to check key conversion from cached cuboid to object when a string instead of a list is passed"""

        object_keys = 'a4931d58076dc47773957809380f206e4228517c9fa6daed536043782024e480&1&1&1&0&0&12'

        os = AWSObjectStore(self.object_store_config)
        cached_cuboid_keys = os.object_to_cached_cuboid_keys(object_keys)

        assert len(cached_cuboid_keys) == 1
        assert cached_cuboid_keys[0] == "CACHED-CUBOID&1&1&1&0&0&12"

    def test_add_cuboid_to_index(self):
        """Test method to compute final object key and add to S3"""
        dummy_key = "SLDKFJDSHG&1&1&1&0&0&12"
        os = AWSObjectStore(self.object_store_config)
        os.add_cuboid_to_index(dummy_key)

        # Get item
        dynamodb = boto3.client('dynamodb', region_name=get_region())
        response = dynamodb.get_item(
            TableName=self.object_store_config['s3_index_table'],
            Key={'object-key': {'S': dummy_key},
                 'version-node': {'N': "0"}},
            ReturnConsumedCapacity='NONE'
        )

        assert response['Item']['object-key']['S'] == dummy_key
        assert response['Item']['version-node']['N'] == "0"
        assert response['Item']['ingest-job-hash']['S'] == '1'
        assert response['Item']['ingest-job-range']['S'] == '1&1&0&0'

    def test_cuboids_exist(self):
        """Test method for checking if cuboids exist in S3 index"""
        os = AWSObjectStore(self.object_store_config)

        expected_keys = ["1&1&1&0&0&12", "1&1&1&0&0&13", "1&1&1&0&0&14"]
        test_keys = ["1&1&1&0&0&100", "1&1&1&0&0&13", "1&1&1&0&0&14",
                     "1&1&1&0&0&15"]

        expected_object_keys = os.cached_cuboid_to_object_keys(expected_keys)

        # Populate table
        for k in expected_object_keys:
            os.add_cuboid_to_index(k)

        # Check for keys
        exist_keys, missing_keys = os.cuboids_exist(test_keys)

        assert exist_keys == [1, 2]
        assert missing_keys == [0, 3]

    def test_cuboids_exist_with_cache_miss(self):
        """Test method for checking if cuboids exist in S3 index while supporting
        the cache miss key index parameter"""
        os = AWSObjectStore(self.object_store_config)

        expected_keys = ["1&1&1&0&0&12", "1&1&1&0&0&13", "1&1&1&0&0&14"]
        test_keys = ["1&1&1&0&0&100", "1&1&1&0&0&13", "1&1&1&0&0&14",
                     "1&1&1&0&0&15"]

        expected_object_keys = os.cached_cuboid_to_object_keys(expected_keys)

        # Populate table
        for k in expected_object_keys:
            os.add_cuboid_to_index(k)

        # Check for keys
        exist_keys, missing_keys = os.cuboids_exist(test_keys, [1, 2])

        assert exist_keys == [1, 2]
        assert missing_keys == []

    def test_put_get_single_object(self):
        """Method to test putting and getting objects to and from S3"""
        os = AWSObjectStore(self.object_store_config)

        cached_cuboid_keys = ["CACHED-CUBOID&1&1&1&0&0&12"]
        fake_data = [b"aaaadddffffaadddfffaadddfff"]

        object_keys = os.cached_cuboid_to_object_keys(cached_cuboid_keys)

        os.put_objects(object_keys, fake_data)

        returned_data = os.get_single_object(object_keys[0])
        assert fake_data[0] == returned_data

    def test_put_get_objects_syncronous(self):
        """Method to test putting and getting objects to and from S3"""
        os = AWSObjectStore(self.object_store_config)

        cached_cuboid_keys = ["CACHED-CUBOID&1&1&1&0&0&12", "CACHED-CUBOID&1&1&1&0&0&13"]
        fake_data = [b"aaaadddffffaadddfffaadddfff", b"fffddaaffddffdfffaaa"]

        object_keys = os.cached_cuboid_to_object_keys(cached_cuboid_keys)

        os.put_objects(object_keys, fake_data)

        returned_data = os.get_objects(object_keys)
        for rdata, sdata in zip(returned_data, fake_data):
            assert rdata == sdata


class TestAWSObjectStore(AWSObjectStoreTestMixin, unittest.TestCase):

    def setUp(self):
        """ Create a diction of configuration values for the test resource. """
        # Create resource
        self.setup_helper = SetupTests()
        self.data = self.setup_helper.get_image8_dict()
        self.resource = BossResourceBasic(self.data)

        # Load config
        self.config = configuration.BossConfig()
        self.object_store_config = {"s3_flush_queue": 'https://mytestqueue.com',
                                    "cuboid_bucket": "test_bucket",
                                    "page_in_lambda_function": "page_in.test.boss",
                                    "page_out_lambda_function": "page_out.test.boss",
                                    "s3_index_table": "test_table"}

        # Create AWS Resources needed for tests
        self.setup_helper.start_mocking()
        self.setup_helper.create_s3_index_table(self.object_store_config["s3_index_table"])
        self.setup_helper.create_cuboid_bucket(self.object_store_config["cuboid_bucket"])

    def tearDown(self):
        self.setup_helper.stop_mocking()

