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
from spdb.c_lib.ndtype import CUBOIDSIZE
from .annocube import AnnotateCube64
from .error import SpdbError, ErrorCodes
import boto3
import botocore
import hashlib
import datetime
import numpy as np
import time
import random

from spdb.spatialdb.error import SpdbError, ErrorCodes

# Note there are additional imports at the bottom of the file.

# Object id's cuboids could be spread over multiple chunks.  This Dynamo
# table attribute stores the number of the last chunk.  This number is
# appended to the end of the key to get to the last chunk.
LAST_PARTITION_KEY = 'lastPartitionKey'

# This Dynamo table attribute is incremented every time the cuboid-set
# attribute is updated.  This revision id is used to prevent simultaneous 
# updates of the cuboid-set.
REV_ID = 'revId'

class ObjectIndices:
    """
    Class that handles the DynamoDB tracking of object IDs.  This class
    supports the AWS Object Store.
    """

    def __init__(
        self, s3_index_table, id_index_table, id_count_table, cuboid_bucket, 
        region, dynamodb_url=None):
        """
        Constructor.

        Args:
            s3_index_table (string): Name of the cuboid index Dynamo table.
            id_index_table (string): Name of the id index Dynamo table.
            id_count_table (string): Name of the id count Dynamo table.
            cuboid_bucket (string): Name of the S3 bucket that stores cuboids.
            region (string): AWS region to run in.
            dynamodb_url (optional[string]): Specific Dynamo URL to use (supply for testing).
        """
        self.s3_index_table = s3_index_table
        self.id_index_table = id_index_table
        self.id_count_table = id_count_table
        self.cuboid_bucket = cuboid_bucket

        self.dynamodb = boto3.client(
            'dynamodb', region_name=region, endpoint_url=dynamodb_url)
        self.s3 = boto3.client('s3', region_name=region)

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
        Generate key used by DynamoDB id index table to store cuboids morton ids associated with the given resource and id.

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

    def generate_channel_id_key_from_parts(self, key_parts, id):
        """
        Generate key used by DynamoDB id index table to store cuboids morton ids associated with the given resource and id.

        Args:
            key_parts (AWSObjectStore.KeyParts): Decomposed object key.
            id (string|uint64): Object id.

        Returns:
            (string): key to get cuboids associated with the given resource and id.
        """
        base_key = '{}&{}&{}&{}&{}'.format(
            key_parts.collection_id, key_parts.experiment_id,key_parts.channel_id,
            key_parts.resolution, id)
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

        While processing cuboid data in the cube_list, ids found will replace
        any existing ids previously associated with the same cuboid in the
        index.

        Args:
            resource (BossResource): Data model info based on the request or target resource.
            resolution (int): Resolution level.
            key_list (list[string]): keys for each cuboid.
            cube_list (list[bytes]): bytes comprising each cuboid.
            version (optional[int]): Defaults to zero, reserved for future use.

        Raises:
            (SpdbError): Failure performing update_item operation on DynamoDB.
        """
        # # TODO SH Hotfix put in place to stop idIndex from populating.  IdIndex Table is preventing data loading.
        # for obj_key, cube in zip(key_list, cube_list):
        #     # Find unique ids in this cube.
        #     ids = np.unique(cube)
        #
        #     # Convert ids to a string.
        #     ids_str_list = self._make_ids_strings(ids)
        #
        #     num_ids = len(ids_str_list)
        #     print("ID Index Update - Num unique IDs in cube: {}".format(num_ids))
        #     if num_ids == 0:
        #         # No need to update if there are no non-zero ids in the cuboid.
        #         print('Object key: {} has no ids'.format(obj_key))
        #         continue
        #
        #     # Associate these ids with their cuboid in the s3 cuboid index table.
        #     # TODO: Generalize backoff and use in all DynamoDB requests
        #     for backoff in range(0, 6):
        #         try:
        #             response = self.dynamodb.update_item(
        #                 TableName=self.s3_index_table,
        #                 Key={'object-key': {'S': obj_key}, 'version-node': {'N': "{}".format(version)}},
        #                 UpdateExpression='SET #idset = :ids',
        #                 ExpressionAttributeNames={'#idset': 'id-set'},
        #                 ExpressionAttributeValues={':ids': {'NS': ids_str_list}},
        #                 ReturnConsumedCapacity='NONE')
        #
        #             if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        #                 # Update Failed, but not at the client level
        #                 raise SpdbError(
        #                     "Failed to update ID index for cube: {}".format(obj_key),
        #                     ErrorCodes.OBJECT_STORE_ERROR)
        #
        #             # If you got here good to move on
        #             break
        #         except botocore.exceptions.ClientError as ex:
        #             if ex.response["Error"]["Code"] == "413":
        #                 # DynamoDB Key is too big to write or update
        #                 print('WARNING: ID Index Update: Too many IDs present. Failed to update ID index for cube: {}'.format(obj_key))
        #                 return False
        #
        #             elif ex.response["Error"]["Code"] == "ProvisionedThroughputExceededException":
        #                 print('INFO: ID Index Update: Backoff required to update ID index for cube: {}'.format(obj_key))
        #                 # Need to back off!
        #                 time.sleep(((2 ** backoff) + (random.randint(0, 1000) / 1000.0))/10.0)
        #
        #             else:
        #                 # Something else bad happened
        #                 raise SpdbError(
        #                     "Error updating {} in cuboid index table in DynamoDB: {} ".format(obj_key, ex),
        #                     ErrorCodes.OBJECT_STORE_ERROR)
        #
        #     # Get the morton of the object key. Since we only support annotation indices at t=0
        #     obj_morton = obj_key.split("&")[-1]
        #
        #     # Add object key to every id's cuboid set.
        #     for id in ids:
        #         if id == 0:
        #             # 0 is not a valid id (unclassified pixel).
        #             continue
        #
        #         channel_id_key = self.generate_channel_id_key(resource, resolution, id)
        #         for backoff in range(0, 6):
        #             try:
        #
        #                 # Make request to dynamodb
        #                 response = self.dynamodb.update_item(
        #                     TableName=self.id_index_table,
        #                     Key={'channel-id-key': {'S': channel_id_key}, 'version': {'N': "{}".format(version)}},
        #                     UpdateExpression='ADD #cuboidset :objkey',
        #                     ExpressionAttributeNames={'#cuboidset': 'cuboid-set'},
        #                     ExpressionAttributeValues={':objkey': {'SS': [obj_morton]}},
        #                     ReturnConsumedCapacity='NONE')
        #
        #                 if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        #                     raise SpdbError("Failed to update cube index for ID: {}".format(channel_id_key),
        #                         ErrorCodes.OBJECT_STORE_ERROR)
        #
        #                 # If you got here good to move on
        #                 break
        #
        #             except botocore.exceptions.ClientError as ex:
        #                 if ex.response["Error"]["Code"] == "413":
        #                     # DynamoDB Key is too big to write or update. Just skip it.
        #                     print('WARNING: ID Index Update: ID in too many cubes. Failed to update cube index for ID: {}'.format(channel_id_key))
        #                     break
        #
        #                 elif ex.response["Error"]["Code"] == "ProvisionedThroughputExceededException":
        #                     print('INFO: ID Index Update: Backoff required to update cube index for ID: {}'.format(channel_id_key))
        #                     # Need to back off!
        #                     time.sleep(((2 ** backoff) + (random.randint(0, 1000) / 1000.0))/10.0)
        #                 else:
        #                     # Something else bad happened
        #                     raise SpdbError(
        #                         "Error updating cube index for id {}: {} ".format(channel_id_key, ex),
        #                         ErrorCodes.OBJECT_STORE_ERROR)

        return True

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
        response = self._get_morton_ids_from_dynamo(channel_id_key, version)
        if not self._validate_dynamo_morton_ids_response(response):
            return []

        cuboid_set = self._extract_morton_ids(
            response['Item']['cuboid-set']['SS'], resource, resolution)

        if (LAST_PARTITION_KEY in response['Item'] and 
                'N' in response['Item'][LAST_PARTITION_KEY]):
            last_chunk = response['Item'][LAST_PARTITION_KEY]['N']
            if int(last_chunk) > 0:
                # Ids span multiple keys, so load those keys also.
                for i in range(1, int(last_chunk)+1):
                    partition_key = '{}&{}'.format(channel_id_key, i)
                    next_response = self._get_morton_ids_from_dynamo(
                        partition_key, version)
                    if self._validate_dynamo_morton_ids_response(next_response):
                        cuboid_set += self._extract_morton_ids(
                            next_response['Item']['cuboid-set']['SS'], resource, 
                            resolution)

        return cuboid_set

    def _validate_dynamo_morton_ids_response(self, response):
        """
        Make sure 'Item': { 'cuboid-set': { 'SS': [] } } exists in response.

        Args:
            response (dict): Response returned by DynamoDB.Client.get_item()

        Returns:
            (bool): True if 'cuboid-set' in response.
        """

        # Id not in table.
        if 'Item' not in response:
            return False

        # This is not an error condition, but there are no morton ids for this
        # object id.  DynamoDB does not allow a set to be empty.
        if 'cuboid-set' not in response['Item']:
            return False

        if 'SS' not in response['Item']['cuboid-set']:
            raise SpdbError(
                "Error cuboid-set attribute is not string set in id index table of DynamoDB.",
                ErrorCodes.OBJECT_STORE_ERROR)

        return True

    def _get_morton_ids_from_dynamo(self, key, version=0):
        response = self.dynamodb.get_item(
            TableName=self.id_index_table,
            Key={'channel-id-key': {
                'S': key}, 'version': {'N': '{}'.format(version)}
            },
            ConsistentRead=True,
            ReturnConsumedCapacity='NONE')

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise SpdbError(
                "Error reading id index table from DynamoDB.",
                ErrorCodes.OBJECT_STORE_ERROR)
        return response

    def _extract_morton_ids(self, ids_list, resource, resolution):
        """
        Extract morton ids from list and create full S3 object key.

        Args:
            ids_list ([string]): List of morton ids as strings.
            resource (BossResource): Data model info based on the request or target resource.
            resolution (int): Resolution level.

        Returns:
            ([string]): List of object keys.
        """

        # Handle legacy vs. updated index values
        # Legacy version stored the entire object key. The updated version stores only the morton and we need to
        # add the rest of the object key information at runtime
        # TODO: Migrate all legacy indices and remove if statement.  This will
        # fail when ids get really big!
        cuboid_set = []
        for cuboid_str in ids_list:
            if len(cuboid_str) < 21:
                # Compute cuboid object-keys as this is a "new" index value. Use t=0
                cuboid_set.append(AWSObjectStore.generate_object_key(resource, resolution, 0, cuboid_str))
            else:
                cuboid_set.append(cuboid_str)

        return cuboid_set

    def get_loose_bounding_box(self, resource, resolution, id):
        """
        Get the loose bounding box that contains the object labeled with id.

        A loose bounding box is always cuboid aligned.

        Bounding box ranges follow the Python range convention.  For example,
        if x_range = [0, 10], then x >= 0 and x < 10.

        Args:
            resource (project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            id (uint64|string): object's id

        Returns:
            (dict|None): {'x_range': [0, 512], 'y_range': [0, 512], 'z_range': [0, 16], 't_range': [0, 1]} or None if the id is not found.

        Raises:
            (SpdbError): Can't talk to id index database or database corrupt.
        """
        cf = resource.get_coord_frame()
        x_min = cf.x_stop
        x_max = cf.x_start
        y_min = cf.y_stop
        y_max = cf.y_start
        z_min = cf.z_stop
        z_max = cf.z_start

        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]
        obj_keys = self.get_cuboids(resource, resolution, id)

        if len(obj_keys) == 0:
            return None

        for key in obj_keys:
            morton = int(key.split('&')[6])
            xyz = MortonXYZ(morton)
            x = xyz[0] * x_cube_dim
            y = xyz[1] * y_cube_dim
            z = xyz[2] * z_cube_dim

            if x < x_min:
                x_min = x
            if x > x_max:
                x_max = x
            if y < y_min:
                y_min = y
            if y > y_max:
                y_max = y
            if z < z_min:
                z_min = z
            if z > z_max:
                z_max = z

        return {
            'x_range': [x_min, x_max+x_cube_dim],
            'y_range': [y_min, y_max+y_cube_dim],
            'z_range': [z_min, z_max+z_cube_dim],
            't_range': [0, 1]
        }

    def get_tight_bounding_box(self, cutout_fcn, resource, resolution, id, x_rng, y_rng, z_rng, t_rng):
        """Computes the exact bounding box for an id.

        Use ranges from the cuboid aligned "loose" bounding box as input.

        Note: assumes that VALID ranges are provided for the loose bounding box.

        See ./diagrams/tight_bounding_boxes.png for illustration of cutouts
        made to calculate the bounds.  Note that the diagram only shows the
        cutouts made in the x and y dimensions.

        Args:
            cutout_fcn (function): SpatialDB's cutout method.  Provided for naive search of cuboids on the edges of the loose bounding box.
            resource (project.BossResource): Data model info based on the request or target resource.
            resolution (int): the resolution level.
            id (int): id to find bounding box of.
            x_rng (list[int]): 2 element list representing range.
            y_rng (list[int]): 2 element list representing range.
            z_rng (list[int]): 2 element list representing range.
            t_rng (list[int]): 2 element list representing range.

        Returns:
            (dict): {'x_range': [0, 10], 'y_range': [0, 10], 'z_range': [0, 10], 't_range': [0, 10]}
        """
        x_min_max = self._get_tight_bounding_box_x_axis(
            cutout_fcn, resource, resolution, id, x_rng, y_rng, z_rng, t_rng)
        y_min_max = self._get_tight_bounding_box_y_axis(
            cutout_fcn, resource, resolution, id, x_rng, y_rng, z_rng, t_rng)
        z_min_max = self._get_tight_bounding_box_z_axis(
            cutout_fcn, resource, resolution, id, x_rng, y_rng, z_rng, t_rng)

        return {
            'x_range': [x_min_max[0], x_min_max[1]+1],
            'y_range': [y_min_max[0], y_min_max[1]+1],
            'z_range': [z_min_max[0], z_min_max[1]+1],
            't_range': t_rng
        }

    def _get_tight_bounding_box_x_axis(self, cutout_fcn, resource, resolution, id, x_rng, y_rng, z_rng, t_rng):
        """Computes the min and max indices of the  an id.

        Use ranges from the cuboid aligned "loose" bounding box as input.

        Note: assumes that VALID ranges are provided for the loose bounding box.

        Args:
            cutout_fcn (function): SpatialDB's cutout method.  Provided for naive search of cuboids on the edges of the loose bounding box.
            resource (project.BossResource): Data model info based on the request or target resource.
            resolution (int): the resolution level.
            id (int): id to find bounding box of.
            x_rng (list[int]): 2 element list representing range.
            y_rng (list[int]): 2 element list representing range.
            z_rng (list[int]): 2 element list representing range.
            t_rng (list[int]): 2 element list representing range.

        Returns:
            (tuple): (x_min_index, x_max_index)
        """
        x_cube_dim = CUBOIDSIZE[resolution][0]

        # Cutout the side closest to the origin along the x axis.
        near_x_corner = (x_rng[0], y_rng[0], z_rng[0])
        near_x_extent = (x_cube_dim, y_rng[1]-y_rng[0], z_rng[1]-z_rng[0])
        near_x_cube = cutout_fcn(
            resource, near_x_corner, near_x_extent, resolution, t_rng)
        near_x_ind = np.where(near_x_cube.data == id)

        min_x = x_rng[0] + min(near_x_ind[3])
        max_x = x_rng[0] + max(near_x_ind[3])

        # Cutout the side farthest from the origin along the x axis.
        far_x_corner = (x_rng[1] - x_cube_dim, y_rng[0], z_rng[0])
        if far_x_corner[0] <= x_rng[0]:
            # Only 1 cuboid in the x direction, so the far side is included by
            # the near side.
            return (min_x, max_x)

        far_x_extent = (x_rng[1] - far_x_corner[0], near_x_extent[1], near_x_extent[2])
        far_x_cube = cutout_fcn(
            resource, far_x_corner, far_x_extent, resolution, t_rng)
        far_x_ind = np.where(far_x_cube.data == id)

        if len(far_x_ind[3]) == 0:
            # This shouldn't happen if loose cuboid computed correctly.
            return (min_x, max_x)

        max_x = far_x_corner[0] + max(far_x_ind[3])
        return (min_x, max_x)

    def _get_tight_bounding_box_y_axis(self, cutout_fcn, resource, resolution, id, x_rng, y_rng, z_rng, t_rng):
        """Computes the min and max y indices of an id.

        Use ranges from the cuboid aligned "loose" bounding box as input.

        Note: assumes that VALID ranges are provided for the loose bounding box.

        Args:
            cutout_fcn (function): SpatialDB's cutout method.  Provided for naive search of cuboids on the edges of the loose bounding box.
            resource (project.BossResource): Data model info based on the request or target resource.
            resolution (int): the resolution level.
            id (int): id to find bounding box of.
            x_rng (list[int]): 2 element list representing range.
            y_rng (list[int]): 2 element list representing range.
            z_rng (list[int]): 2 element list representing range.
            t_rng (list[int]): 2 element list representing range.

        Returns:
            (tuple): (x_min_index, x_max_index)
        """
        y_cube_dim = CUBOIDSIZE[resolution][1]

        # Cutout the side closest to the origin along the x axis.
        near_y_corner = (x_rng[0], y_rng[0], z_rng[0])
        near_y_extent = (x_rng[1]-x_rng[0], y_cube_dim, z_rng[1]-z_rng[0])
        near_y_cube = cutout_fcn(
            resource, near_y_corner, near_y_extent, resolution, t_rng)
        near_y_ind = np.where(near_y_cube.data == id)

        min_y = y_rng[0] + min(near_y_ind[2])
        max_y = y_rng[0] + max(near_y_ind[2])

        # Cutout the side farthest from the origin along the y axis.
        far_y_corner = (x_rng[0], y_rng[1] - y_cube_dim, z_rng[0])
        if far_y_corner[1] <= y_rng[0]:
            # Only 1 cuboid in the x direction, so the far side is included by
            # the near side.
            return (min_y, max_y)

        far_y_extent = (near_y_extent[0], y_rng[1] - far_y_corner[1], near_y_extent[2])
        far_y_cube = cutout_fcn(
            resource, far_y_corner, far_y_extent, resolution, t_rng)
        far_y_ind = np.where(far_y_cube.data == id)

        if len(far_y_ind[3]) == 0:
            # This shouldn't happen if loose cuboid computed correctly.
            return (min_y, max_y)

        max_y = far_y_corner[1] + max(far_y_ind[2])
        return (min_y, max_y)

    def _get_tight_bounding_box_z_axis(self, cutout_fcn, resource, resolution, id, x_rng, y_rng, z_rng, t_rng):
        """Computes the min and max z indices of the id.

        Use ranges from the cuboid aligned "loose" bounding box as input.

        Note: assumes that VALID ranges are provided for the loose bounding box.

        Args:
            cutout_fcn (function): SpatialDB's cutout method.  Provided for naive search of cuboids on the edges of the loose bounding box.
            resource (project.BossResource): Data model info based on the request or target resource.
            resolution (int): the resolution level.
            id (int): id to find bounding box of.
            x_rng (list[int]): 2 element list representing range.
            y_rng (list[int]): 2 element list representing range.
            z_rng (list[int]): 2 element list representing range.
            t_rng (list[int]): 2 element list representing range.

        Returns:
            (tuple): (x_min_index, x_max_index)
        """
        z_cube_dim = CUBOIDSIZE[resolution][2]

        # Cutout the side closest to the origin along the x axis.
        near_z_corner = (x_rng[0], y_rng[0], z_rng[0])
        near_z_extent = (x_rng[1]-x_rng[0], y_rng[1]-y_rng[0], z_cube_dim)
        near_z_cube = cutout_fcn(
            resource, near_z_corner, near_z_extent, resolution, t_rng)
        near_z_ind = np.where(near_z_cube.data == id)

        min_z = z_rng[0] + min(near_z_ind[1])
        max_z = z_rng[0] + max(near_z_ind[1])

        # Cutout the side farthest from the origin along the z axis.
        far_z_corner = (x_rng[0], y_rng[0], z_rng[1] - z_cube_dim)
        if far_z_corner[2] <= z_rng[0]:
            # Only 1 cuboid in the z direction, so the far side is included by
            # the near side.
            return (min_z, max_z)

        far_z_extent = (near_z_extent[0], near_z_extent[1], z_rng[1] - far_z_corner[2])
        far_z_cube = cutout_fcn(
            resource, far_z_corner, far_z_extent, resolution, t_rng)
        far_z_ind = np.where(far_z_cube.data == id)

        if len(far_z_ind[3]) == 0:
            # This shouldn't happen if loose cuboid computed correctly.
            return (min_z, max_z)

        max_z = far_z_corner[2] + max(far_z_ind[1])
        return (min_z, max_z)

    def get_ids_in_cuboids(self, obj_keys, version=0):
        """
        Get all ids from the given cuboids.

        Args:
            obj_keys (list[string]): List of cuboid object keys to aggregate ids from.
            version (optional[int]): Defaults to zero, reserved for future use.

        Returns:
            (list[string]): { ['1', '4', '8'] }

        Raises:
            (SpdbError): Can't talk to id index database or database corrupt.
        """
        id_set = set()
        for key in obj_keys:
            #TODO: consider using batch_get_items() in the future.
            response = self.dynamodb.get_item(
                TableName=self.s3_index_table,
                Key={'object-key': {'S': key}, 'version-node': {'N': "{}".format(version)}},
                ConsistentRead=True,
                ReturnConsumedCapacity='NONE')

            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                raise SpdbError(
                    "Error reading cuboid index table from DynamoDB.",
                    ErrorCodes.OBJECT_STORE_ERROR)

            if 'Item' not in response:
                continue
            if 'id-set' not in response['Item']:
                continue
            if 'NS' not in response['Item']['id-set']:
                raise SpdbError(
                    "Error id-set attribute is not number set in cuboid index table of DynamoDB.",
                    ErrorCodes.OBJECT_STORE_ERROR)

            for id in response['Item']['id-set']['NS']:
                id_set.add(id)

        return list(id_set)

    def reserve_ids(self, resource, num_ids, version=0):
        """Method to reserve a block of ids for a given channel at a version.

        Args:
            resource (spdb.project.resource.BossResource): Data model info based on the request or target resource.
            num_ids (int): Number of IDs to reserve
            version (optional[int]): Defaults to zero, reserved for future use.

        Returns:
            (np.array): starting ID for the block of ID successfully reserved as a numpy array to insure uint64
        """
        # Make sure this is an annotation channel
        if resource.get_channel().is_image():
            raise SpdbError('Image Channel', 'Can only reserve IDs for annotation channels',
                            ErrorCodes.DATATYPE_NOT_SUPPORTED)

        time_start = datetime.datetime.now()
        # Try to get a block of IDs for 10 seconds
        ch_key = self.generate_reserve_id_key(resource)
        next_id = None
        while (datetime.datetime.now() - time_start).seconds < 10:
            # Get the current value
            next_id = self.dynamodb.get_item(TableName=self.id_count_table,
                                             Key={'channel-key': {'S': ch_key},
                                                  'version': {'N': "{}".format(version)}},
                                             AttributesToGet=['next_id'],
                                             ConsistentRead=True)
            if "Item" not in next_id:
                # Initialize the key since it doesn't exist yet
                self.dynamodb.put_item(TableName=self.id_count_table,
                                       Item={'channel-key': {'S': ch_key},
                                             'version': {'N': "{}".format(version)},
                                             'next_id': {'N': '1'}})

                next_id = np.fromstring("1", dtype=np.uint64, sep=' ')
            else:
                next_id = np.fromstring(next_id["Item"]['next_id']['N'], dtype=np.uint64, sep=' ')

            # Increment value conditionally, if failed try again until timeout
            try:
                self.dynamodb.update_item(TableName=self.id_count_table,
                                          Key={'channel-key': {'S': ch_key},
                                               'version': {'N': "{}".format(version)}},
                                          ExpressionAttributeValues={":inc": {"N": str(num_ids)},
                                                                     ":exp": {"N": "{}".format(next_id[0])}},
                                          ConditionExpression="next_id = :exp",
                                          UpdateExpression="set next_id = next_id + :inc",
                                          ReturnValues="ALL_NEW")

                break

            except botocore.exceptions.ClientError as e:
                # Ignore the ConditionalCheckFailedException, bubble up
                # other exceptions.
                if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                    raise

        if not next_id:
            raise SpdbError('Reserve ID Fail', 'Failed to reserve the requested ID block within 10 seconds',
                            ErrorCodes.SPDB_ERROR)

        return next_id

    def write_s3_index(self, obj_key, version=0):
        """
        Loads the cuboid from S3, extracts its ids, and writes them to the S3 index.

        Args:
            obj_key (string): Object key of the cuboid in the S3 bucket.
            version (optional[string|int]): Reserved for future use.

        Returns:
            ([string]): List of unique ids (as strings) contained in cuboid.
        """
        response = self.s3.get_object(Key=obj_key, Bucket=self.cuboid_bucket)
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise SpdbError("Error reading cuboid from S3.",
                ErrorCodes.OBJECT_STORE_ERROR)
        cuboid_bytes_blosc = response['Body'].read()
        cube = AnnotateCube64()
        cuboid_bytes = cube.unpack_array(cuboid_bytes_blosc)

        ids = np.unique(cuboid_bytes)
        
        # Convert ids to a string.
        ids_str_list = self._make_ids_strings(ids)

        self.dynamodb.update_item(
            TableName=self.s3_index_table,
            Key={'object-key': {'S': obj_key}, 'version-node': {'N': "{}".format(version)}},
            UpdateExpression='SET #idset = :ids',
            ExpressionAttributeNames={'#idset': 'id-set'},
            ExpressionAttributeValues={':ids': {'NS': ids_str_list}},
            ReturnConsumedCapacity='NONE')
        return ids_str_list

    def write_id_index(self, max_used_capacity, obj_key, obj_id, version=0):
        """
        Extracts the morton id from the given object key and writes it to the 
        id index table.

        Args:
            max_used_capacity (int): After updating the table, if used write capacity exceeded this value, update the LAST_PARTITION_KEY for this object id.
            obj_key (string): Cuboid object key in S3 cuboid index.
            obj_id (int): Object (annotation) id to write cuboid's morton id to.
            version (optional[int]): Version - reserved for future use.
        """
        key_parts = AWSObjectStore.get_object_key_parts(obj_key)
        cuboid_morton = key_parts.morton_id
        key = self.generate_channel_id_key_from_parts(key_parts, obj_id)

        try:
            (chunk_num, rev_id) = self.get_last_partition_key_and_rev_id(
                key, version)
            (found, _) = self.lookup(cuboid_morton, key, chunk_num, version)
            if found:
                return
        except KeyError:
            # No key exists in table for this id.
            chunk_num = 0
            rev_id = None

        actual_chunk = self.write_cuboid(
            max_used_capacity, cuboid_morton, key, chunk_num, rev_id, version)

        if actual_chunk > chunk_num:
            self.update_last_partition_key(key, actual_chunk, version)

    def get_last_partition_key_and_rev_id(self, key, version=0):
        """
        Look up the last partition used to store mortons for the given key and the key's revision id.

        Args:
            key (string): Key in the id index table without a chunk_num.
            version (optional[int]): Version - reserved for future use.

        Returns:
            (int, int) Chunk number of last partition for given key and revision id.

        Raises:
            (KeyError): if given key and version not found in table.
        """
        resp = self.dynamodb.get_item(
            TableName=self.id_index_table,
            Key={'channel-id-key': {'S': key}, 'version': {'N': "{}".format(version)}},
            ProjectionExpression='{},{}'.format(LAST_PARTITION_KEY, REV_ID))

        if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise SpdbError("Failed to read attributes of ID: {}".format(key),
                ErrorCodes.OBJECT_STORE_ERROR)

        # Key doesn't exist in table.
        if 'Item' not in resp:
            raise KeyError('key: {} - version: {} not found.'.format(key, version))

        item = resp['Item']

        if LAST_PARTITION_KEY in item and 'N' in item[LAST_PARTITION_KEY]:
            chunk_num = int(item[LAST_PARTITION_KEY]['N'])
        else:
            chunk_num = 0

        if chunk_num == 0:
            rev_dict = item
        else:
            # Read the last chunk to get the correct revision id, if it exists.
            last_key = '{}&{}'.format(key, chunk_num)
            last_resp = self.dynamodb.get_item(
                TableName=self.id_index_table,
                Key={'channel-id-key': {'S': last_key}, 'version': {'N': "{}".format(version)}},
                ProjectionExpression='{},{}'.format(LAST_PARTITION_KEY, REV_ID))

            if last_resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                raise SpdbError("Failed to read attributes of ID: {}".format(last_key),
                    ErrorCodes.OBJECT_STORE_ERROR)

            if 'Item' not in last_resp:
                # Key doesn't exist yet.  This is a valid state.  The previous
                # key is considered full, so the LAST_PARTITION_KEY now points
                # at key to be created.
                rev_dict = {}
            else:
                rev_dict = last_resp['Item']

        if REV_ID in rev_dict and 'N' in rev_dict[REV_ID]:
            rev_id = int(rev_dict[REV_ID]['N'])
        else:
            rev_id = None

        return chunk_num, rev_id

    def lookup(self, cuboid_morton, default_key, last_chunk_num, version=0):
        """
        Look for the given morton in the id index.

        Args:
            cuboid_morton (string|int): Morton id of the cuboid.
            default_key (string): Key in the id index table without a chunk_num.
            last_chunk_num (int): Chunk number of last partition used for this object id.
            version (optional[int]): Version - reserved for future use.

        Returns:
            (bool, int): True if cuboid found and the chunk_num of the key that contains the cuboid.  False, -1 if cuboid isn't found.
        """
        for i in range(last_chunk_num + 1):
            if i == 0:
                key = default_key
            else:
                key = '{}&{}'.format(default_key, i)

            # Using a query so we don't actually return the set to the client.
            resp = self.dynamodb.query(
                TableName=self.id_index_table,
                Select='COUNT',
                KeyConditionExpression='#key=:keyvalue AND version=:version',
                ExpressionAttributeNames={
                    '#key': 'channel-id-key',
                    '#cuboidset': 'cuboid-set'},
                ExpressionAttributeValues={
                    ':keyvalue': {'S': key},
                    ':version': {'N': str(version)},
                    ':morton': {'S': str(cuboid_morton)}},
                FilterExpression='contains(#cuboidset, :morton)')

            # If result set non empty, return True
            if resp['Count'] > 0:
                return (True, i)

        return (False, -1)

    def write_cuboid(
            self, max_used_capacity, cuboid_morton, key, chunk_num, rev_id, 
            version=0):
        """
        Write the morton id to the given key.

        Will increment the key's chunk_num as necessary if the given key and chunk_num
        combination's set is full or max_used_capacity is exceeded.

        Args:
            max_used_capacity (int): After updating the table, if used write capacity exceeded this value, update the LAST_PARTITION_KEY for this object id.
            cuboid_morton (string|int): Morton id of the cuboid.
            key (string): Key in the id index table without a chunk_num.
            chunk_num (int): Number to be appended to the key if greater than 0.
            rev_id (int|None): Expected revision id when updating cuboid-set attribute.
            version (optional[int]): Version - reserved for future use.

        Returns:
            (int): chunk that the morton id was written to.
        """
        new_chunk_num = chunk_num
        exp_rev_id = rev_id

        done = False

        while not done:
            if new_chunk_num > 0:
                dynamo_key = '{}&{}'.format(key, new_chunk_num)
            else:
                dynamo_key = key

            try:
                response = self.write_cuboid_dynamo(
                    cuboid_morton, dynamo_key, exp_rev_id, version)
                if 'ConsumedCapacity' in response:
                    used = response['ConsumedCapacity']
                    if used >= max_used_capacity:
                        new_chunk_num = new_chunk_num + 1
                done = True
            except botocore.exceptions.ClientError as ex:
                if ex.response['Error']['Code'] == '413':
                    # Set full, try the next chunk.
                    new_chunk_num = new_chunk_num + 1
                    exp_rev_id = None
                else:
                    raise ex

        return new_chunk_num

    def write_cuboid_dynamo(self, cuboid_morton, key, rev_id, version=0):
        """
        Writes cuboid's morton id to the given key.

        Helper for write_cuboid().
        The rev_id is used to prevent concurrent writes which will cause data
        loss.  If the rev_id isn't equal to expected value, then the update
        fails.

        Args:
            cuboid_morton (string|int): Morton id of the cuboid.
            key (string): Key in the id index table.
            rev_id (int|None): Expected revision id when updating cuboid-set attribute.
            version (optional[int]): Version - reserved for future use.

        Returns:
            (dict): Response dictionary from DynamoDB.Client.update_item().

        Raises:
            (SpdbError): Can't talk to Dynamo or Dynamo condition check failed.
        """

        # Expression attribute value for the revision id.
        REV_VALUE = ':revId'
        update_args = {
            'TableName': self.id_index_table,
            'Key': {
                'channel-id-key': {'S': key}, 
                'version': {'N': '{}'.format(version)}
            },
            'UpdateExpression':'ADD #cuboidset :objkey SET {} = {}'.format(
                REV_ID, REV_VALUE),
            'ExpressionAttributeNames': {'#cuboidset': 'cuboid-set'},
            'ExpressionAttributeValues': {':objkey': {'SS': [str(cuboid_morton)]}},
            'ReturnConsumedCapacity':'TOTAL'
        }

        if rev_id is not None:
            update_args['ConditionExpression'] = (
                'attribute_exists({0}) AND {0} = {1}'.format(REV_ID, REV_VALUE))
            update_args['ExpressionAttributeValues'][REV_VALUE] = (
                {'N': str(rev_id)})
        else:
            update_args['ConditionExpression'] = (
                'attribute_not_exists({0})'.format(REV_ID))
            update_args['ExpressionAttributeValues'][REV_VALUE] = {'N': '0'}

        # Do DynamoDB set add op.
        response = self.dynamodb.update_item(**update_args)

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise SpdbError(
                "Failed to update id index for ID: {} with morton id: {}".format(
                    key, cuboid_morton),
                ErrorCodes.OBJECT_STORE_ERROR)

        return response

    def update_last_partition_key(self, key, chunk_num, version=0):
        """
        Update the last-partition-key to the given chunk_num if it greater than the current value.

        This method does not retry if throttled.  If throttled, another invocation will
        eventually update the last-partition-key.

        Args:
            key (string): Key in the id index table without a chunk_num.
            chunk_num (int): Number to be appended to the key if greater than 0.
            version (optional[int]): Version - reserved for future use.
        """
        response = self.dynamodb.update_item(
            TableName=self.id_index_table,
            Key={'channel-id-key': {'S': key}, 'version': {'N': "{}".format(version)}},
            UpdateExpression='SET {} = :chunk_num'.format(LAST_PARTITION_KEY),
            ExpressionAttributeValues={':chunk_num': {'N': str(chunk_num)}},
            ConditionExpression=
                'attribute_not_exists({0}) OR :chunk_num > {0}'.format(LAST_PARTITION_KEY))

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise SpdbError("Failed to update {} for ID: {}".format(LAST_PARTITION_KEY, key),
                ErrorCodes.OBJECT_STORE_ERROR)



# Import statement at the bottom to avoid problems with circular imports.
# http://effbot.org/zone/import-confusion.htm
from .object import AWSObjectStore

