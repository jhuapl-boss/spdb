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

from spdb.c_lib.ndlib import unique
from bossutils.aws import get_region
import boto3
import botocore
import hashlib
import datetime

from spdb.spatialdb.error import SpdbError, ErrorCodes

from bossutils.logger import BossLogger


class ObjectIndices:
    """
    Class that handles the DynamoDB tracking of object IDs.  This class
    supports the AWS Object Store.
    """
    def __init__(self, s3_index_table, id_index_table, id_count_table, region, dynamodb_url=None):
        self.s3_index_table = s3_index_table
        self.id_index_table = id_index_table
        self.id_count_table = id_count_table

        self.dynamodb = boto3.client(
            'dynamodb', region_name=region, endpoint_url=dynamodb_url)

    def _make_ids_strings(self, ids):
        """
        Convert numpy array of uint64 ids into a list of strings for transmission via json.

        *Note* that any ids that are zeros are *dropped*.

        Args:
            ids (numpy.ndarray): Array of uint64 ids.

        Returns:
            (list[string]): ids as list of strings.
        """
        strs = []
        for id in ids:
            if not id == 0:
                strs.append('{}'.format(id))

        return strs

    def generate_channel_id_key(self, resource, resolution, id):
        """
        Generate key used by DynamoDB id index table to store cuboids keys associated with the given resource and id.

        Args:
            resource (BossResource): Data model info based on the request or target resource.
            resolution (int): Resolution level.
            id (string|uint64): Object id.

        Returns:
            (string): key to get cuboids associated with the given resource and id.
        """
        base_key = '{}&{}&{}'.format(resource.get_lookup_key(), resolution, id)
        hash_str = hashlib.md5(base_key.encode()).hexdigest()
        return '{}&{}'.format(hash_str, base_key)

    def generate_reserve_id_key(self, resource):
        """
        Generate key used by DynamoDB id count table to store unique ID counters

        Args:
            resource (BossResource): Data model info based on the request or target resource.

        Returns:
            (string): key to store unique ID counter for a channel
        """
        base_key = '{}'.format(resource.get_lookup_key())
        hash_str = hashlib.md5(base_key.encode()).hexdigest()
        return '{}&{}'.format(hash_str, base_key)

    def update_id_indices(self, resource, resolution, key_list, cube_list, version=0):
        """
        Update annotation id index and s3 cuboid index with ids in the given cuboids.

        Any ids that are zeros will not be added to the indices.

        Args:
            resource (BossResource): Data model info based on the request or target resource.
            resolution (int): Resolution level.
            key_list (list[string]): keys for each cuboid.
            cube_list (list[bytes]): bytes comprising each cuboid.
            version (optional[int]): Defaults to zero, reserved for future use.
        """
        for obj_key, cube in zip(key_list, cube_list):
            # Find unique ids in this cube.
            ids = unique(cube)

            # Convert ids to a string.
            ids_str_list = self._make_ids_strings(ids)

            # Add these ids to the s3 cuboid index table.
            response = self.dynamodb.update_item(
                TableName=self.s3_index_table,
                Key={'object-key': {'S': obj_key}, 'version-node': {'N': "{}".format(version)}},
                UpdateExpression='ADD #idset :ids',
                ExpressionAttributeNames={'#idset': 'id-set'},
                ExpressionAttributeValues={':ids': {'SS': ids_str_list}},
                ReturnConsumedCapacity='NONE')

            # Add object key to this id's cuboid set.
            for id in ids:
                channel_id_key = self.generate_channel_id_key(resource, resolution, id)
                response = self.dynamodb.update_item(
                    TableName=self.id_index_table,
                    Key={'channel-id-key': {'S': channel_id_key}, 'version': {'N': "{}".format(version)}},
                    UpdateExpression='ADD #cuboidset :objkey',
                    ExpressionAttributeNames={'#cuboidset': 'cuboid-set'},
                    ExpressionAttributeValues={':objkey': {'SS': [obj_key]}},
                    ReturnConsumedCapacity='NONE')

    def reserve_ids(self, resource, num_ids, version=0):
        """Method to reserve a block of ids for a given channel at a version.

        Args:
            resource (spdb.project.resource.BossResource): Data model info based on the request or target resource.
            num_ids (int): Number of IDs to reserve
            version (optional[int]): Defaults to zero, reserved for future use.

        Returns:
            (int): starting ID for the block of ID successfully reserved
        """
        # Make sure this is an annotation channel
        if resource.get_channel().is_image():
            raise SpdbError('Image Channel', 'Can only reserve IDs for annotation channels',
                            ErrorCodes.DATATYPE_NOT_SUPPORTED)

        time_start = datetime.datetime.now()
        start_id = None
        # Try to get a block of IDs for 10 seconds
        ch_key = self.generate_reserve_id_key(resource)
        while (datetime.datetime.now() - time_start).seconds < 10:
            # Get the current value
            next_id = self.dynamodb.get_item(TableName=self.id_count_table,
                                             Key={'channel-key': {'S': ch_key},
                                                  'version': {'N': "{}".format(version)}},
                                             AttributesToGet=['next-id'],
                                             ConsistentRead=True)
            if "Item" not in next_id:
                # Initialize the key since it doesn't exist yet
                result = self.dynamodb.put_item(TableName=self.id_count_table,
                                                Item={'channel-key': {'S': ch_key},
                                                      'version': {'N': "{}".format(version)},
                                                      'next-id': {'N': '1'}})

                next_id = 1
            else:
                next_id = next_id["Item"]['next-id']

            new_next_id = next_id + num_ids

            # Increment value conditionally, if failed try again until timeout
            try:
                result2 = self.dynamodb.update_item(TableName=self.id_count_table,
                                                    Key={'channel-key': {'S': ch_key},
                                                         'version': {'N': "{}".format(version)}},
                                                    ExpressionAttributeValues={":inc": {"N": str(num_ids)},
                                                                               ":exp": {"N": str(new_next_id)}},
                                                    ConditionExpression="next-id = :exp",
                                                    UpdateExpression="set next-id = next-id + :inc",
                                                    ReturnValues="ALL_NEW")

                start_id = new_next_id
                break

            except botocore.exceptions.ClientError as e:
                # Ignore the ConditionalCheckFailedException, bubble up
                # other exceptions.
                if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                    raise

        if not start_id:
            raise SpdbError('Reserve ID Fail', 'Failed to reserve the requested ID block within 10 seconds',
                            ErrorCodes.SPDB_ERROR)

        return start_id
