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

    def update_id_indices(self, key_list, cube_list, version=0):
        """
        Update annotation id index and s3 cuboid index with ids in the given cuboids.

        Any ids that are zeros will not be added to the indices.

        Args:
            key_list (list[string]):
            cube_list (list[bytes]):
            version (optional[int]):

        Returns:
        """
        for key, cube in zip(key_list, cube_list):
            # Find unique ids in this cube.
            ids = unique(cube)

            # Convert ids to a string.
            ids_str_list = self._make_ids_strings(ids)

            # Add these ids to the s3 cuboid index table.
            response = self.dynamodb.update_item(
                TableName=self.s3_index_table,
                Key={'object-key': {'S': key}, 'version-node': {'N': "{}".format(version)}},
                UpdateExpression='ADD #idset :ids',
                ExpressionAttributeNames={'#idset': 'id-set'},
                ExpressionAttributeValues={':ids': {'SS': ids_str_list}},
                ReturnConsumedCapacity='NONE')

            for id in ids:
                # Add key to cuboid set for this id.
                pass
