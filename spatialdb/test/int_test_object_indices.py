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
from spdb.c_lib.ndlib import XYZMorton
from spdb.project import BossResourceBasic
from spdb.project.test.resource_setup import get_anno_dict

import boto3
import numpy as np
import os
import unittest
from spdb.spatialdb.test.setup import AWSSetupLayer, SetupTests
from spdb.spatialdb.object import AWSObjectStore
import random


class TestObjectIndicesWithDynamoDb(unittest.TestCase):
    layer = AWSSetupLayer

    def setUp(self):
        """ Copy params from the Layer setUpClass
        """
        self.data = self.layer.setup_helper.get_anno64_dict()
        self.data['lookup_key'] = "1&2&{}".format(random.randint(3, 999))
        self.resource = BossResourceBasic(self.data)
        self.kvio_config = self.layer.kvio_config
        self.state_config = self.layer.state_config
        self.object_store_config = self.layer.object_store_config
        self.region = 'us-east-1'

        self.endpoint_url = None
        if 'LOCAL_DYNAMODB_URL' in os.environ:
            self.endpoint_url = os.environ['LOCAL_DYNAMODB_URL']

        self.dynamodb = boto3.client(
            'dynamodb', region_name=self.region, endpoint_url=self.endpoint_url)

        self.obj_ind = ObjectIndices(self.object_store_config["s3_index_table"],
                                     self.object_store_config["id_index_table"],
                                     self.object_store_config["id_count_table"],
                                     self.region,
                                     self.endpoint_url)

        self.obj_store = AWSObjectStore(self.object_store_config)

    def test_update_id_indices_new_entry_in_cuboid_index(self):
        """
        Test adding ids to new cuboids in the s3 cuboid index.
        """
        bytes = np.zeros(10, dtype='uint64')
        bytes[1] = 20
        bytes[2] = 20
        bytes[5] = 55
        bytes[8] = 1000
        bytes[9] = 55
        expected = ['20', '55', '1000']
        key = 'hash_coll_exp_chan_key'
        version = 0
        resource = BossResourceBasic(data=get_anno_dict())
        resolution = 1

        # Method under test.
        self.obj_ind.update_id_indices(resource, resolution, [key], [bytes], version)

        response = self.dynamodb.get_item(
            TableName=self.object_store_config["s3_index_table"],
            Key={'object-key': {'S': key}, 'version-node': {'N': "{}".format(version)}},
            ConsistentRead=True,
            ReturnConsumedCapacity='NONE')

        self.assertIn('Item', response)
        self.assertIn('id-set', response['Item'])
        self.assertIn('SS', response['Item']['id-set'])
        self.assertCountEqual(expected, response['Item']['id-set']['SS'])

    def test_update_id_indices_update_existing_entry_in_cuboid_index(self):
        """
        Test adding additional ids to existing cuboids in the s3 cuboid index.
        """
        bytes = np.zeros(10, dtype='uint64')
        bytes[1] = 20
        bytes[2] = 20
        bytes[5] = 55
        bytes[8] = 1000
        bytes[9] = 55
        key = 'hash_coll_exp_chan_key_existing'
        version = 0
        resource = BossResourceBasic(data=get_anno_dict())
        resolution = 1

        # Place initial ids for cuboid.
        self.obj_ind.update_id_indices(resource, resolution, [key], [bytes], version)

        new_bytes = np.zeros(4, dtype='uint64')
        new_bytes[0] = 1000
        new_bytes[1] = 4444
        new_bytes[3] = 55

        # Test adding one new id to the index.
        self.obj_ind.update_id_indices(resource, resolution, [key], [new_bytes], version)

        response = self.dynamodb.get_item(
            TableName=self.object_store_config["s3_index_table"],
            Key={'object-key': {'S': key}, 'version-node': {'N': "{}".format(version)}},
            ConsistentRead=True,
            ReturnConsumedCapacity='NONE')

        self.assertIn('Item', response)
        self.assertIn('id-set', response['Item'])
        self.assertIn('SS', response['Item']['id-set'])

        expected = ['20', '55', '1000', '4444']
        self.assertCountEqual(expected, response['Item']['id-set']['SS'])

    def test_update_id_indices_new_entry_for_id_index(self):
        """
        Test adding new ids to the id index.
        """
        bytes = np.zeros(10, dtype='uint64')
        bytes[1] = 20
        bytes[2] = 20
        bytes[5] = 55
        bytes[8] = 1000
        bytes[9] = 55
        expected_ids = ['20', '55', '1000']
        version = 0
        resource = BossResourceBasic(data=get_anno_dict())
        resolution = 1
        time_sample = 0
        morton_id = 20
        object_key = self.obj_store.generate_object_key(
            resource, resolution, time_sample, morton_id)

        # Method under test.
        self.obj_ind.update_id_indices(resource, resolution, [object_key], [bytes], version)

        # Confirm each id has the object_key in its cuboid-set attribute.
        for id in expected_ids:
            key = self.obj_ind.generate_channel_id_key(resource, resolution, id)

            response = self.dynamodb.get_item(
                TableName=self.object_store_config["id_index_table"],
                Key={'channel-id-key': {'S': key}, 'version': {'N': "{}".format(version)}},
                ConsistentRead=True,
                ReturnConsumedCapacity='NONE')

            self.assertIn('Item', response)
            self.assertIn('cuboid-set', response['Item'])
            self.assertIn('SS', response['Item']['cuboid-set'])
            self.assertIn(object_key, response['Item']['cuboid-set']['SS'])

    def test_update_id_indices_add_new_cuboids_to_existing_ids(self):
        """
        Test that new cuboid object keys are added to the cuboid-set attributes of pre-existing ids.
        """
        bytes = np.zeros(10, dtype='uint64')
        bytes[1] = 20
        bytes[2] = 20
        bytes[5] = 55
        bytes[8] = 1000
        bytes[9] = 55
        expected_ids = ['20', '55', '1000']
        version = 0
        resource = BossResourceBasic(data=get_anno_dict())
        resolution = 1
        time_sample = 0
        morton_id = 20
        object_key = self.obj_store.generate_object_key(
            resource, resolution, time_sample, morton_id)

        self.obj_ind.update_id_indices(resource, resolution, [object_key], [bytes], version)

        new_bytes = np.zeros(4, dtype='uint64')
        new_bytes[0] = 1000     # Pre-existing id.
        new_bytes[1] = 4444
        new_bytes[3] = 55       # Pre-existing id.

        new_morton_id = 90
        new_object_key = self.obj_store.generate_object_key(
            resource, resolution, time_sample, new_morton_id)

        # Method under test.
        self.obj_ind.update_id_indices(resource, resolution, [new_object_key], [new_bytes], version)

        # Confirm cuboids for id 55.
        key55 = self.obj_ind.generate_channel_id_key(resource, resolution, 55)

        response = self.dynamodb.get_item(
            TableName=self.object_store_config["id_index_table"],
            Key={'channel-id-key': {'S': key55}, 'version': {'N': '{}'.format(version)}},
            ConsistentRead=True,
            ReturnConsumedCapacity='NONE')

        self.assertIn('Item', response)
        self.assertIn('cuboid-set', response['Item'])
        self.assertIn('SS', response['Item']['cuboid-set'])
        self.assertIn(object_key, response['Item']['cuboid-set']['SS'])
        self.assertIn(new_object_key, response['Item']['cuboid-set']['SS'])

        # Confirm cuboids for id 1000.
        key1000 = self.obj_ind.generate_channel_id_key(resource, resolution, 1000)

        response2 = self.dynamodb.get_item(
            TableName=self.object_store_config["id_index_table"],
            Key={'channel-id-key': {'S': key1000}, 'version': {'N': '{}'.format(version)}},
            ConsistentRead=True,
            ReturnConsumedCapacity='NONE')

        self.assertIn('Item', response2)
        self.assertIn('cuboid-set', response2['Item'])
        self.assertIn('SS', response2['Item']['cuboid-set'])
        self.assertIn(object_key, response2['Item']['cuboid-set']['SS'])
        self.assertIn(new_object_key, response2['Item']['cuboid-set']['SS'])

    def test_get_cuboids(self):
        id = 22222
        bytes = np.zeros(10, dtype='uint64')
        bytes[1] = id
        key = 'hash_coll_exp_chan_key_cuboids'
        version = 0
        resource = BossResourceBasic(data=get_anno_dict())
        resolution = 1

        new_bytes = np.zeros(4, dtype='uint64')
        new_bytes[0] = id     # Pre-existing id.
        new_key = 'hash_coll_exp_chan_key_cuboids2'

        self.obj_ind.update_id_indices(
            resource, resolution, [key, new_key], [bytes, new_bytes], version)

        # Method under test.
        actual = self.obj_ind.get_cuboids(resource, resolution, id)

        expected = [key, new_key]
        self.assertCountEqual(expected, actual)

    def test_get_bounding_box_loose(self):
        id = 33333
        resource = BossResourceBasic(data=get_anno_dict())
        resolution = 1
        time_sample = 0
        version = 0

        bytes0 = np.zeros(10, dtype='uint64')
        bytes0[1] = id
        pos0 = [1, 2, 3]
        morton_id0 = XYZMorton(pos0)
        key0 = self.obj_store.generate_object_key(
            resource, resolution, time_sample, morton_id0)

        bytes1 = np.zeros(4, dtype='uint64')
        bytes1[0] = id     # Pre-existing id.
        pos1 = [3, 5, 6]
        morton_id1 = XYZMorton(pos1)
        key1 = self.obj_store.generate_object_key(
            resource, resolution, time_sample, morton_id1)

        self.obj_ind.update_id_indices(
            resource, resolution, [key0, key1], [bytes0, bytes1], version)

        actual = self.obj_ind.get_bounding_box(resource, resolution, id, 'loose')
        expected = {
            'x_range': [pos0[0], pos1[0]+1],
            'y_range': [pos0[1], pos1[1]+1],
            'z_range': [pos0[2], pos1[2]+1],
            't_range': [0, 1]
        }
        self.assertEqual(expected, actual)

    def test_reserve_id_init(self):
        start_id = self.obj_ind.reserve_ids(self.resource, 10)
        self.assertEqual(start_id, 11)

    def test_reserve_id_increment(self):
        start_id = self.obj_ind.reserve_ids(self.resource, 10)
        self.assertEqual(start_id, 11)
        start_id = self.obj_ind.reserve_ids(self.resource, 5)
        self.assertEqual(start_id, 16)

if __name__ == '__main__':
    unittest.main()

