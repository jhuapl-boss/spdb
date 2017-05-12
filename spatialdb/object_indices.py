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
from .error import SpdbError, ErrorCodes
import boto3
import botocore
import hashlib
import datetime
import numpy as np
import time
import random

from spdb.spatialdb.error import SpdbError, ErrorCodes


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

    def generate_object_key(self, resource, resolution, time_sample, morton):
        """
        Generate key used by DynamoDB id index table to store cuboids keys associated with the given resource and id.

        Args:
            resource (BossResource): Data model info based on the request or target resource.
            resolution (int): Resolution level.
            time_sample (string|int): Time sample for the object, typically always 0 since we don't store indices off 0
            morton (string|uint64): Morton ID of the object

        Returns:
            (string): key to get cuboids associated with the given resource and id.
        """
        # TODO: Consolidate all key operations
        base_key = '{}&{}&{}&{}'.format(resource.get_lookup_key(), resolution, time_sample, morton)
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

        response = self.dynamodb.get_item(
            TableName=self.id_index_table,
            Key={'channel-id-key': {'S': channel_id_key}, 'version': {'N': '{}'.format(version)}},
            ConsistentRead=True,
            ReturnConsumedCapacity='NONE')

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise SpdbError(
                "Error reading id index table from DynamoDB.",
                ErrorCodes.OBJECT_STORE_ERROR)

        # Id not in table.
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

        # Handle legacy vs. updated index values
        # Legacy version stored the entire object key. The updated version stores only the morton and we need to
        # add the rest of the object key information at runtime
        # TODO: Migrate all legacy indices and remove this for loop
        cuboid_set = []
        for cuboid_str in response['Item']['cuboid-set']['SS']:
            if len(cuboid_str) < 21:
                # Compute cuboid object-keys as this is a "new" index value. Use t=0
                cuboid_set.append(self.generate_object_key(resource, resolution, 0, cuboid_str))
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
