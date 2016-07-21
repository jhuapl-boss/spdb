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
from .test_AWS_object_store import AWSObjectStoreTestMixin

from bossutils import configuration
import time

import boto3
from botocore.exceptions import ClientError


class AWSObjectStoreTestIntegrationMixin(object):

    def test_put_get_objects_async(self):
        """Method to test putting and getting objects to and from S3"""
        os = AWSObjectStore(self.object_store_config)

        cached_cuboid_keys = ["CACHED-CUBOID&1&1&1&0&0&12", "CACHED-CUBOID&1&1&1&0&0&13"]
        fake_data = [b"aaaadddffffaadddfffaadddfff", b"fffddaaffddffdfffaaa"]

        object_keys = os.cached_cuboid_to_object_keys(cached_cuboid_keys)

        os.put_objects(object_keys, fake_data)

        returned_data = os.get_objects_async(object_keys)
        for rdata, sdata in zip(returned_data, fake_data):
            assert rdata == sdata

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

        assert 1 == 0

    def test_trigger_page_out(self):
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

        assert 1 == 0


class TestAWSObjectStoreInt(AWSObjectStoreTestIntegrationMixin, AWSObjectStoreTestMixin, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """ get_some_resource() is slow, to avoid calling it for each test use setUpClass()
            and store the result as class variable
        """
        super(TestAWSObjectStoreInt, cls).setUpClass()
        cls.setUpParams(cls)
        try:
            cls.create_dynamodb_table(cls)
        except ClientError:
            cls.delete_dynamodb_table(cls)
            time.sleep(20)
            cls.create_dynamodb_table(cls)

        try:
            cls.create_bucket(cls)
        except ClientError:
            cls.delete_bucket(cls)
            time.sleep(20)
            cls.create_bucket(cls)

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
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )
        time.sleep(10)

    def delete_dynamodb_table(self):
        """Create the s3 index table"""
        client = boto3.client('dynamodb')
        client.delete_table(TableName=self.object_store_config['s3_index_table'])

    def create_bucket(self):
        client = boto3.client('s3')
        response = client.create_bucket(
            ACL='private',
            Bucket=self.object_store_config['cuboid_bucket']
        )
        time.sleep(10)

    def delete_bucket(self):

        # Delete objects:
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(self.object_store_config['cuboid_bucket'])
        for obj in bucket.objects.all():
            obj.delete()

        # Delete bucket
        bucket.delete()

    def setUpParams(self):
        """ Create a diction of configuration values for the test resource. """
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

        # Get domain info
        parts = self.config['aws']['db'].split('.')
        domain = "{}.{}".format(parts[1], parts[2])

        self.object_store_config = {"s3_flush_queue": 'https://mytestqueue.com',
                                    "cuboid_bucket": "int_test_bucket.{}".format(domain),
                                    "page_in_lambda_function": "page_in.{}".format(domain),
                                    "page_out_lambda_function": "page_out.{}".format(domain),
                                    "s3_index_table": "int_test_s3_index_table.{}".format(domain)}

    @classmethod
    def tearDownClass(cls):
        super(TestAWSObjectStoreInt, cls).tearDownClass()
        try:
            cls.delete_dynamodb_table(cls)
        except:
            pass

        try:
            cls.delete_bucket(cls)
        except:
            pass


