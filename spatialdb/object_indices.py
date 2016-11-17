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
from .error import SpdbError, ErrorCodes
import boto3
import hashlib

class ObjectIndices:
    """
    Class that handles the DynamoDB tracking of object IDs.  This class
    supports the AWS Object Store.
    """
    def __init__(self, s3_index_table, id_index_table, region, dynamodb_url=None):
        self.s3_index_table = s3_index_table
        self.id_index_table = id_index_table

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

        Raises:
            (SpdbError): Failure performing update_item operation on DynamoDB.
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
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                raise SpdbError(
                    "Error updating {} in cuboid index table in DynamoDB.".format(obj_key),
                    ErrorCodes.OBJECT_STORE_ERROR)

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
                if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                    raise SpdbError(
                        "Error updating {} in id index table in DynamoDB.".format(channel_id_key),
                        ErrorCodes.OBJECT_STORE_ERROR)

    def get_cuboids(self, resource, resolution, id, version=0):
        """
        Get object keys of cuboids that contain the given id.

        Args:
            resource (BossResource): Data model info based on the request or target resource.
            resolution (int): Resolution level.
            id (string|uint64): Object id.
            version (optional[int]): Defaults to zero, reserved for future use.

        Returns:
            (list[string]): List of object keys of cuboids that contain the given id.

        Raises:
            (SpdbError): Can't talk to DynamoDB or table data corrupted.
        """

        channel_id_key = self.generate_channel_id_key(resource, resolution, id)

        #TODO: consider using batch_get_items() in the future.
        response = self.dynamodb.get_item(
            TableName=self.id_index_table,
            Key={'channel-id-key': {'S': channel_id_key}, 'version': {'N': '{}'.format(version)}},
            ConsistentRead=True,
            ReturnConsumedCapacity='NONE')

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise SpdbError(
                "Error reading id index table from DynamoDB.",
                ErrorCodes.OBJECT_STORE_ERROR)

        # Id not in table.  Should we raise instead?
        if 'Item' not in response:
            return []

        # This is not an error condition.  DynamoDB does not allow a set to be
        # empty.
        if 'cuboid-set' not in response['Item']:
            return []

        if 'SS' not in response['Item']['cuboid-set']:
            raise SpdbError(
                "Error cuboid-set attribute is not string set in id index table of DynamoDB.",
                ErrorCodes.OBJECT_STORE_ERROR)

        return response['Item']['cuboid-set']['SS']

