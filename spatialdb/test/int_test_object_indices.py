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
import boto3
import json
import numpy as np
import os
from pkg_resources import resource_filename
from random import randint
import time
import unittest
import warnings

class TestObjectIndicesWithDynamoDb(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        warnings.simplefilter('ignore')

        cls.s3_index = 'test_s3_index{}'.format(randint(1, 999))
        cls.id_index = 'test_id_index{}'.format(randint(1, 999))
        cls.s3_index_schema = resource_filename(
            'spdb', 'spatialdb/dynamo/s3_index_table.json')
        cls.id_index_schema = resource_filename(
            'spdb', 'spatialdb/dynamo/dynamo_id_index_schema.json')

        # Use local DynamoDB if env variable set.
        cls.endpoint_url = None
        if 'LOCAL_DYNAMODB_URL' in os.environ:
            cls.endpoint_url = os.environ['LOCAL_DYNAMODB_URL']

        cls.region = 'us-east-1'
        cls.dynamodb = boto3.client(
            'dynamodb', region_name=cls.region, endpoint_url=cls.endpoint_url)

        with open(cls.s3_index_schema) as handle:
            json_str = handle.read()
            s3_index_params = json.loads(json_str)

        cls.dynamodb.create_table(TableName=cls.s3_index, **s3_index_params)

        with open(cls.id_index_schema) as handle:
            json_str = handle.read()
            id_index_params = json.loads(json_str)

        cls.dynamodb.create_table(TableName=cls.id_index, **id_index_params)

        # Don't start tests until tables are done creating.
        cls.wait_table_create(cls.id_index)
        cls.wait_table_create(cls.s3_index)

    @classmethod
    def tearDownClass(cls):
        cls.dynamodb.delete_table(TableName=cls.id_index)
        cls.dynamodb.delete_table(TableName=cls.s3_index)

        # Don't exit until tables actually deleted.
        cls.wait_table_delete(cls.id_index)
        cls.wait_table_delete(cls.s3_index)

    @classmethod
    def wait_table_create(cls, table_name):
        """Poll dynamodb at a 2s interval until the table creates."""
        print('Waiting for creation of table {}'.format(
            table_name), end='', flush=True)
        cnt = 0
        while True:
            cnt += 1
            if cnt > 50:
                # Give up waiting.
                return
            try:
                print('.', end='', flush=True)
                resp = cls.dynamodb.describe_table(TableName=table_name)
                if resp['Table']['TableStatus'] == 'ACTIVE':
                    print('')
                    return
            except:
                # May get an exception if table doesn't currently exist.
                pass
            time.sleep(2)

    @classmethod
    def wait_table_delete(cls, table_name):
        """Poll dynamodb at a 2s interval until the table deletes."""
        print('Waiting for deletion of table {}'.format(
            table_name), end='', flush=True)
        cnt = 0
        while True:
            cnt += 1
            if cnt > 50:
                # Give up waiting.
                return
            try:
                print('.', end='', flush=True)
                resp = cls.dynamodb.describe_table(TableName=table_name)
            except:
                # Exception thrown when table doesn't exist.
                print('')
                return
            time.sleep(2)

    def setUp(self):
        self.obj_ind = ObjectIndices(
            self.s3_index, self.id_index, self.region, self.endpoint_url)

    def test_update_id_indices_new_entry_in_cuboid_index(self):
        bytes = np.zeros(10, dtype='uint64')
        bytes[1] = 20
        bytes[2] = 20
        bytes[5] = 55
        bytes[8] = 1000
        bytes[9] = 55
        expected = ['20', '55', '1000']
        key = 'hash_coll_exp_chan_key'
        version = 0

        # Method under test.
        self.obj_ind.update_id_indices([key], [bytes], version)

        response = self.dynamodb.get_item(
            TableName=self.s3_index,
            Key={'object-key': {'S': key}, 'version-node': {'N': "{}".format(version)}},
            ConsistentRead=True,
            ReturnConsumedCapacity='NONE')

        self.assertIn('Item', response)
        self.assertIn('id-set', response['Item'])
        self.assertIn('SS', response['Item']['id-set'])
        self.assertCountEqual(expected, response['Item']['id-set']['SS'])

    def test_update_id_indices_update_existing_entry_in_cuboid_index(self):
        bytes = np.zeros(10, dtype='uint64')
        bytes[1] = 20
        bytes[2] = 20
        bytes[5] = 55
        bytes[8] = 1000
        bytes[9] = 55
        key = 'hash_coll_exp_chan_key_existing'
        version = 0

        # Place initial ids for cuboid.
        self.obj_ind.update_id_indices([key], [bytes], version)

        new_bytes = np.zeros(4, dtype='uint64')
        new_bytes[0] = 1000
        new_bytes[1] = 4444
        new_bytes[3] = 55

        # Test adding one new id to the index.
        self.obj_ind.update_id_indices([key], [new_bytes], version)

        response = self.dynamodb.get_item(
            TableName=self.s3_index,
            Key={'object-key': {'S': key}, 'version-node': {'N': "{}".format(version)}},
            ConsistentRead=True,
            ReturnConsumedCapacity='NONE')

        self.assertIn('Item', response)
        self.assertIn('id-set', response['Item'])
        self.assertIn('SS', response['Item']['id-set'])

        expected = ['20', '55', '1000', '4444']
        self.assertCountEqual(expected, response['Item']['id-set']['SS'])

    def test_update_id_indices_new_entry_for_id_index(self):
        bytes = np.zeros(10, dtype='uint64')
        bytes[1] = 20
        bytes[2] = 20
        bytes[5] = 55
        bytes[8] = 1000
        bytes[9] = 55
        expected = ['20', '55', '1000']
        key = 'hash_coll_exp_chan_key'
        version = 0

        # Method under test.
        self.obj_ind.update_id_indices([key], [bytes], version)

        response = self.dynamodb.get_item(
            TableName=self.id_index,
            Key={'channel-id-key': {'S': key}, 'version': {'N': "{}".format(version)}},
            ConsistentRead=True,
            ReturnConsumedCapacity='NONE')

        self.assertIn('Item', response)
        self.assertIn('id-set', response['Item'])
        self.assertIn('SS', response['Item']['id-set'])
        self.assertCountEqual(expected, response['Item']['id-set']['SS'])

if __name__ == '__main__':
    unittest.main()

