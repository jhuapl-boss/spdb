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

import boto3
from moto import mock_s3
from moto import mock_dynamodb2
from moto import mock_sqs
from moto import mock_lambda


class AWSObjectStoreTestMixin(object):

    def test_object_key_chunks(self):
        """Test method to return object keys in chunks"""
        keys = ['1', '2', '3', '4', '5', '6', '7']
        expected = [['1', '2', '3'],
                    ['4', '5', '6'],
                    ['7']]

        for cnt, chunk in enumerate(AWSObjectStore.object_key_chunks(keys, 3)):
            assert chunk == expected[cnt]

    def test_cached_cuboid_to_object_keys(self):
        """Test to check key conversion from cached cuboid to object"""

        cached_cuboid_keys = ["CACHED-CUBOID&1&1&1&0&0&12", "CACHED-CUBOID&1&1&1&0&0&13"]

        os = AWSObjectStore(self.object_store_config)
        object_keys = os.cached_cuboid_to_object_keys(cached_cuboid_keys)

        assert len(object_keys) == 2
        assert object_keys[0] == 'a4931d58076dc47773957809380f206e4228517c9fa6daed536043782024e480&1&1&1&0&0&12'
        assert object_keys[1] == 'f2b449f7e247c8aec6ecf754388a65ee6ea9dc245cd5ef149aebb2e0d20b4251&1&1&1&0&0&13'

    def test_object_to_cached_cuboid_keys(self):
        """Test to check key conversion from cached cuboid to object"""

        object_keys = ['a4931d58076dc47773957809380f206e4228517c9fa6daed536043782024e480&1&1&1&0&0&12',
                       'f2b449f7e247c8aec6ecf754388a65ee6ea9dc245cd5ef149aebb2e0d20b4251&1&1&1&0&0&13']

        os = AWSObjectStore(self.object_store_config)
        cached_cuboid_keys = os.object_to_cached_cuboid_keys(object_keys)

        assert len(cached_cuboid_keys) == 2
        assert cached_cuboid_keys[0] == "CACHED-CUBOID&1&1&1&0&0&12"
        assert cached_cuboid_keys[1] == "CACHED-CUBOID&1&1&1&0&0&13"

    def test_add_cuboid_to_index(self):
        """Test method to compute final object key and add to S3"""
        dummy_key = "SLDKFJDSHG&1&1&1&0&0&12"
        os = AWSObjectStore(self.object_store_config)
        os.add_cuboid_to_index(dummy_key)

        # Get item
        dynamodb = boto3.client('dynamodb')
        response = dynamodb.get_item(
            TableName=self.object_store_config['s3_index_table'],
            Key={'object-key': {'S': dummy_key},
                 'version': {'S': 'a'}},
            ReturnConsumedCapacity='NONE'
        )

        assert response['Item']['object-key']['S'] == dummy_key
        assert response['Item']['version']['S'] == 'a'
        assert response['Item']['ingest-job']['S'] == '1&1&1&0&0'

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
    @mock_dynamodb2
    def create_dynamodb_table(self):
        """Create the s3 index table"""
        client = boto3.client('dynamodb')

        response = client.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'object-key',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'version',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'ingest-job',
                    'AttributeType': 'S'
                }
            ],
            TableName=self.object_store_config['s3_index_table'],
            KeySchema=[
                {
                    'AttributeName': 'object-key',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'version',
                    'KeyType': 'RANGE'
                }
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'ingest-job-index',
                    'KeySchema': [
                        {
                            'AttributeName': 'ingest-job',
                            'KeyType': 'HASH'
                        },
                    ],
                    'Projection': {
                        'ProjectionType': 'KEYS_ONLY',
                    },
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5
                    }
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )

        self.table_created = True

    @mock_s3
    def create_bucket(self):
        client = boto3.client('s3')
        response = client.create_bucket(
            ACL='private',
            Bucket=self.object_store_config['cuboid_bucket']
        )

    def setUp(self):
        """ Create a diction of configuration values for the test resource. """
        self.mock_s3 = mock_s3()
        self.mock_dynamodb = mock_dynamodb2()
        self.mock_sqs = mock_sqs()
        self.mock_s3.start()
        self.mock_dynamodb.start()
        self.mock_sqs.start()

        self.data = {}
        self.data['collection'] = {}
        self.data['collection']['name'] = "col1"
        self.data['collection']['description'] = "Test collection 1"

        self.data['coord_frame'] = {}
        self.data['coord_frame']['name'] = "coord_frame_1"
        self.data['coord_frame']['description'] = "Test coordinate frame"
        self.data['coord_frame']['x_start'] = 0
        self.data['coord_frame']['x_stop'] = 2000
        self.data['coord_frame']['y_start'] = 0
        self.data['coord_frame']['y_stop'] = 5000
        self.data['coord_frame']['z_start'] = 0
        self.data['coord_frame']['z_stop'] = 200
        self.data['coord_frame']['x_voxel_size'] = 4
        self.data['coord_frame']['y_voxel_size'] = 4
        self.data['coord_frame']['z_voxel_size'] = 35
        self.data['coord_frame']['voxel_unit'] = "nanometers"
        self.data['coord_frame']['time_step'] = 0
        self.data['coord_frame']['time_step_unit'] = "na"

        self.data['experiment'] = {}
        self.data['experiment']['name'] = "exp1"
        self.data['experiment']['description'] = "Test experiment 1"
        self.data['experiment']['num_hierarchy_levels'] = 7
        self.data['experiment']['hierarchy_method'] = 'slice'

        self.data['channel_layer'] = {}
        self.data['channel_layer']['name'] = "ch1"
        self.data['channel_layer']['description'] = "Test channel 1"
        self.data['channel_layer']['is_channel'] = True
        self.data['channel_layer']['datatype'] = 'uint8'
        self.data['channel_layer']['max_time_step'] = 0

        self.data['boss_key'] = 'col1&exp1&ch1'
        self.data['lookup_key'] = '4&2&1'

        self.resource = BossResourceBasic(self.data)

        self.config = configuration.BossConfig()

        self.object_store_config = {"s3_flush_queue": 'https://mytestqueue.com',
                                    "cuboid_bucket": "test_bucket",
                                    "page_in_lambda_function": "page_in.test.boss",
                                    "page_out_lambda_function": "page_out.test.boss",
                                    "s3_index_table": "test_table"}


        # Create AWS Resources needed for tests
        self.create_dynamodb_table()
        self.create_bucket()

    def tearDown(self):
        self.mock_s3.stop()
        self.mock_dynamodb.stop()
        self.mock_sqs.stop()

