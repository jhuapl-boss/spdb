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
from spdb.c_lib.ndlib import MortonXYZ, XYZMorton
from .error import SpdbError, ErrorCodes
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

        Raises:
            (SpdbError): Failure performing update_item operation on DynamoDB.
        """
        for obj_key, cube in zip(key_list, cube_list):
            # Find unique ids in this cube.
            ids = unique(cube)

            # Convert ids to a string.
            ids_str_list = self._make_ids_strings(ids)

            if len(ids_str_list) == 0:
                # No need to update if there are no non-zero ids in the cuboid.
                print('Object key: {} has no ids'.format(obj_key))
                continue

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

    def get_bounding_box(self, resource, resolution, id, bb_type='loose'):
        """
        Get the bounding box that contains the object labeled with id.

        Bounding box ranges follow the Python range convention.  For example,
        if x_range = [0, 10], then x >= 0 and x < 10.

        Args:
            resource (project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            id (uint64|string): object's id
            bb_type (optional[string]): 'loose' | 'tight'. Defaults to 'loose'

        Returns:
            (dict): {'x_range': [0, 10], 'y_range': [0, 10], 'z_range': [0, 10], 't_range': [0, 10]}

        Raises:
            (SpdbError): Can't talk to id index database or database corrupt.
        """
        if not bb_type == 'loose':
            raise SpdbError(
                "Only loose bounding box currently supported",
                ErrorCodes.SPDB_ERROR )

        cf = resource.get_coord_frame()
        x_min = cf.x_stop
        x_max = cf.x_start
        y_min = cf.y_stop
        y_max = cf.y_start
        z_min = cf.z_stop
        z_max = cf.z_start

        obj_keys = self.get_cuboids(resource, resolution, id)
        for key in obj_keys:
            morton = int(key.split('&')[6])
            xyz = MortonXYZ(morton)
            if xyz[0] < x_min:
                x_min = xyz[0]
            if xyz[0] > x_max:
                x_max = xyz[0]
            if xyz[1] < y_min:
                y_min = xyz[1]
            if xyz[1] > z_max:
                y_max = xyz[1]
            if xyz[2] < z_min:
                z_min = xyz[2]
            if xyz[2] > z_max:
                z_max = xyz[2]

        return {
            'x_range': [x_min, x_max+1],
            'y_range': [y_min, y_max+1],
            'z_range': [z_min, z_max+1],
            't_range': [0, 1]
        }


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
                next_id = 1
                new_next_id = next_id + num_ids
                # Dynamo starts at 0, so add an additional 1
                num_ids += 1
            else:
                next_id = next_id["Item"]['next-id']
                new_next_id = next_id + num_ids

            # Increment value conditionally, if failed try again until timeout
            try:
                result = self.dynamodb.update_item(TableName=self.id_count_table,
                                                   Key={'channel-key': {'S': ch_key},
                                                        'version': {'N': "{}".format(version)}},
                                                   ConditionExpression="next-id = :expected_val",
                                                   UpdateExpression="set next-id = next-id + :increment_val",
                                                   ExpressionAttributeValues={":increment_val": {"N": str(num_ids)},
                                                                              ":expected_val": {"N": str(new_next_id)}})

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
