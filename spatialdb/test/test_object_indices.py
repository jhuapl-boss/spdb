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

from spdb.spatialdb.object_indices import (ObjectIndices, LAST_PARTITION_KEY, 
    REV_ID)
from bossutils.aws import get_region
import botocore
import numpy as np
import os
from spdb.c_lib.ndlib import XYZMorton
from spdb.c_lib.ndtype import CUBOIDSIZE
from spdb.project import BossResourceBasic
from spdb.project.test.resource_setup import get_anno_dict
from spdb.spatialdb.object import AWSObjectStore
from spdb.spatialdb import SpatialDB
from spdb.spatialdb.cube import Cube
import unittest
from unittest.mock import patch, DEFAULT
import random

from bossutils import configuration

from spdb.project import BossResourceBasic
from spdb.spatialdb.test.setup import SetupTests
from spdb.spatialdb.error import SpdbError


class ObjectIndicesTestMixin(object):
    def setUp(self):
        # Randomize the look-up key so tests don't mess with each other
        self.resource._lookup_key = "1&2&{}".format(random.randint(4, 1000))

    def test_make_ids_strings_ignore_zeros(self):
        zeros = np.zeros(4, dtype='uint64')
        expected = []
        actual = self.obj_ind._make_ids_strings(zeros)
        self.assertEqual(expected, actual)

    def test_make_ids_strings_mix(self):
        arr = np.zeros(4, dtype='uint64')
        arr[0] = 12345
        arr[2] = 9876

        expected = ['12345', '9876']
        actual = self.obj_ind._make_ids_strings(arr)
        self.assertEqual(expected, actual)

    @unittest.skip('Method under test will be replaced')
    def test_update_id_indices_ignores_zeros(self):
        """
        Never send id 0 to the DynamoDB id index or cuboid index!  Since
        0 is the default value before an id is assigned to a voxel, this
        would blow way past DynamoDB limits.
        """

        resolution = 0
        version = 0
        _id = 300
        id_str_list = ['{}'.format(_id)]
        cube_data = np.zeros(5, dtype='uint64')
        cube_data[2] = _id
        key = 'some_obj_key'

        exp_channel_key = self.obj_ind.generate_channel_id_key(self.resource, resolution, _id)

        with patch.object(self.obj_ind.dynamodb, 'update_item') as mock_update_item:
            mock_update_item.return_value = {
                'ResponseMetadata': { 'HTTPStatusCode': 200 }
            }

            # Method under test.
            self.obj_ind.update_id_indices(self.resource, resolution, [key], [cube_data], version)

            # Expect only 2 calls because there's only 1 non-zero id.
            self.assertEqual(2, mock_update_item.call_count)

            # First call should update s3 cuboid index.
            kall0 = mock_update_item.mock_calls[0]
            _, _, kwargs0 = kall0
            self.assertEqual(id_str_list, kwargs0['ExpressionAttributeValues'][':ids']['NS'])

            # Second call should update id index.
            kall1 = mock_update_item.mock_calls[1]
            _, _, kwargs1 = kall1
            self.assertEqual(exp_channel_key, kwargs1['Key']['channel-id-key']['S'])


    def test_get_cuboids_single_chunk(self):
        """
        Test behavior when there is only one chunk of cuboids associated with
        an object id.
        """
        res = 0
        obj_id = 2555
        version = 0
        morton_id = '23'

        with patch.object(self.obj_ind.dynamodb, 'get_item') as mock_get_item:
            mock_get_item.return_value = {
                'ResponseMetadata': {'HTTPStatusCode': 200},
                'Item': { 
                    'cuboid-set': {'SS': [morton_id]}
                }
            }

            exp_key = AWSObjectStore.generate_object_key(
                self.resource, res, 0, morton_id)

            # Method under test.
            actual = self.obj_ind.get_cuboids(
                self.resource, res, obj_id, version)

            self.assertEqual([exp_key], actual)

    def test_get_cuboids_multiple_chunks(self):
        """
        Test behavior when morton ids associated with an id span more than one
        chunk in Dynamo.
        """
        res = 0
        obj_id = 2555
        version = 0
        morton_id1 = '23'
        morton_id2 = '58'

        with patch.object(self.obj_ind.dynamodb, 'get_item') as mock_get_item:
            mock_get_item.side_effect = [
                {
                    'ResponseMetadata': {'HTTPStatusCode': 200},
                    'Item': { 
                        'cuboid-set': {'SS': [morton_id1]},
                        LAST_PARTITION_KEY: {'N': '1'}
                    }
                },
                {
                    'ResponseMetadata': {'HTTPStatusCode': 200},
                    'Item': { 
                        'cuboid-set': {'SS': [morton_id2]}
                    }
                }
            ]

            exp_key1 = AWSObjectStore.generate_object_key(
                self.resource, res, 0, morton_id1)
            exp_key2 = AWSObjectStore.generate_object_key(
                self.resource, res, 0, morton_id2)

            # Method under test.
            actual = self.obj_ind.get_cuboids(
                self.resource, res, obj_id, version)

            self.assertCountEqual([exp_key1, exp_key2], actual)

    def test_get_loose_bounding_box(self):
        resolution = 0
        time_sample = 0

        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]

        pos0 = [4, 4, 4]
        pos1 = [2, 1, 3]
        pos2 = [6, 7, 5]

        mort0 = XYZMorton(pos0)
        mort1 = XYZMorton(pos1)
        mort2 = XYZMorton(pos2)


        key0 = AWSObjectStore.generate_object_key(self.resource, resolution, time_sample, mort0)
        key1 = AWSObjectStore.generate_object_key(self.resource, resolution, time_sample, mort1)
        key2 = AWSObjectStore.generate_object_key(self.resource, resolution, time_sample, mort2)

        id = 2234

        with patch.object(self.obj_ind, 'get_cuboids') as fake_get_cuboids:
            fake_get_cuboids.return_value = [key0, key1, key2]

            # Method under test.
            actual = self.obj_ind.get_loose_bounding_box(self.resource, resolution, id)

            expected = {
                'x_range': [2*x_cube_dim, (6+1)*x_cube_dim],
                'y_range': [1*y_cube_dim, (7+1)*y_cube_dim],
                'z_range': [3*z_cube_dim, (5+1)*z_cube_dim],
                't_range': [0, 1]
            }
            self.assertEqual(expected, actual)

    def test_get_loose_bounding_box_not_found(self):
        """Make sure None returned if id is not in channel."""
        resolution = 0
        time_sample = 0
        id = 2234

        with patch.object(self.obj_ind, 'get_cuboids') as fake_get_cuboids:
            fake_get_cuboids.return_value = []

            actual = self.obj_ind.get_loose_bounding_box(
                self.resource, resolution, id)

            expected = None
            self.assertEqual(expected, actual)

    @patch('spdb.spatialdb.SpatialDB', autospec=True)
    def test_tight_bounding_box_x_axis_single_cuboid(self, mock_spdb):
        """Loose bounding box only spans a single cuboid."""
        resolution = 0
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]
        id = 12345
        x_rng = [0, x_cube_dim]
        y_rng = [0, y_cube_dim]
        z_rng = [0, z_cube_dim]
        t_rng = [0, 1]

        cube = Cube.create_cube(
            self.resource, (x_cube_dim, y_cube_dim, z_cube_dim))
        cube.data = np.zeros((1, z_cube_dim, y_cube_dim, x_cube_dim))
        cube.data[0][7][128][10] = id
        cube.data[0][7][128][11] = id
        cube.data[0][7][128][12] = id
        mock_spdb.cutout.return_value = cube

        expected = (10, 12)

        # Method under test.
        actual = self.obj_ind._get_tight_bounding_box_x_axis(
            mock_spdb.cutout, self.resource, resolution, id,
            x_rng, y_rng, z_rng, t_rng)

        self.assertEqual(expected, actual)
        self.assertEqual(1, mock_spdb.cutout.call_count)

    @patch('spdb.spatialdb.SpatialDB', autospec=True)
    def test_tight_bounding_box_x_axis_multiple_cuboids(self, mock_spdb):
        """Loose bounding box spans multiple cuboids."""
        resolution = 0
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]
        id = 12345
        x_rng = [0, 2*x_cube_dim]
        y_rng = [0, y_cube_dim]
        z_rng = [0, z_cube_dim]
        t_rng = [0, 1]

        cube = Cube.create_cube(
            self.resource, (x_cube_dim, y_cube_dim, z_cube_dim))
        cube.data = np.zeros((1, z_cube_dim, y_cube_dim, x_cube_dim))
        cube.data[0][7][128][10] = id
        cube.data[0][7][128][11] = id
        cube.data[0][7][128][12] = id

        cube2 = Cube.create_cube(
            self.resource, (x_cube_dim, y_cube_dim, z_cube_dim))
        cube2.data = np.zeros((1, z_cube_dim, y_cube_dim, x_cube_dim))
        cube2.data[0][7][128][3] = id
        cube2.data[0][7][128][4] = id

        # Return cube on the 1st call to cutout and cube2 on the 2nd call.
        mock_spdb.cutout.side_effect = [cube, cube2]

        expected = (10, 516)

        # Method under test.
        actual = self.obj_ind._get_tight_bounding_box_x_axis(
            mock_spdb.cutout, self.resource, resolution, id,
            x_rng, y_rng, z_rng, t_rng)

        self.assertEqual(expected, actual)
        self.assertEqual(2, mock_spdb.cutout.call_count)

    @patch('spdb.spatialdb.SpatialDB', autospec=True)
    def test_tight_bounding_box_y_axis_single_cuboid(self, mock_spdb):
        """Loose bounding box only spans a single cuboid."""
        resolution = 0
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]
        id = 12345
        x_rng = [0, x_cube_dim]
        y_rng = [0, y_cube_dim]
        z_rng = [0, z_cube_dim]
        t_rng = [0, 1]

        cube = Cube.create_cube(
            self.resource, (x_cube_dim, y_cube_dim, z_cube_dim))
        cube.data = np.zeros((1, z_cube_dim, y_cube_dim, x_cube_dim))
        cube.data[0][7][200][10] = id
        cube.data[0][7][201][10] = id
        cube.data[0][7][202][10] = id
        mock_spdb.cutout.return_value = cube

        expected = (200, 202)

        # Method under test.
        actual = self.obj_ind._get_tight_bounding_box_y_axis(
            mock_spdb.cutout, self.resource, resolution, id,
            x_rng, y_rng, z_rng, t_rng)

        self.assertEqual(expected, actual)
        self.assertEqual(1, mock_spdb.cutout.call_count)

    @patch('spdb.spatialdb.SpatialDB', autospec=True)
    def test_tight_bounding_box_y_axis_multiple_cuboids(self, mock_spdb):
        """Loose bounding box spans multiple cuboids."""
        resolution = 0
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]
        id = 12345
        x_rng = [0, x_cube_dim]
        y_rng = [0, 2*y_cube_dim]
        z_rng = [0, z_cube_dim]
        t_rng = [0, 1]

        cube = Cube.create_cube(
            self.resource, (x_cube_dim, y_cube_dim, z_cube_dim))
        cube.data = np.zeros((1, z_cube_dim, y_cube_dim, x_cube_dim))
        cube.data[0][7][509][11] = id
        cube.data[0][7][510][11] = id
        cube.data[0][7][511][11] = id

        cube2 = Cube.create_cube(
            self.resource, (x_cube_dim, y_cube_dim, z_cube_dim))
        cube2.data = np.zeros((1, z_cube_dim, y_cube_dim, x_cube_dim))
        cube2.data[0][7][0][11] = id
        cube2.data[0][7][1][11] = id

        # Return cube on the 1st call to cutout and cube2 on the 2nd call.
        mock_spdb.cutout.side_effect = [cube, cube2]

        expected = (509, 513)

        # Method under test.
        actual = self.obj_ind._get_tight_bounding_box_y_axis(
            mock_spdb.cutout, self.resource, resolution, id,
            x_rng, y_rng, z_rng, t_rng)

        self.assertEqual(expected, actual)
        self.assertEqual(2, mock_spdb.cutout.call_count)

    @patch('spdb.spatialdb.SpatialDB', autospec=True)
    def test_tight_bounding_box_z_axis_single_cuboid(self, mock_spdb):
        """Loose bounding box only spans a single cuboid."""
        resolution = 0
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]
        id = 12345
        x_rng = [0, x_cube_dim]
        y_rng = [0, y_cube_dim]
        z_rng = [0, z_cube_dim]
        t_rng = [0, 1]

        cube = Cube.create_cube(
            self.resource, (x_cube_dim, y_cube_dim, z_cube_dim))
        cube.data = np.zeros((1, z_cube_dim, y_cube_dim, x_cube_dim))
        cube.data[0][12][200][10] = id
        cube.data[0][13][200][10] = id
        cube.data[0][14][200][10] = id
        mock_spdb.cutout.return_value = cube

        expected = (12, 14)

        # Method under test.
        actual = self.obj_ind._get_tight_bounding_box_z_axis(
            mock_spdb.cutout, self.resource, resolution, id,
            x_rng, y_rng, z_rng, t_rng)

        self.assertEqual(expected, actual)
        self.assertEqual(1, mock_spdb.cutout.call_count)

    def test_create_id_counter_key(self):
        self.resource._lookup_key = "1&2&3"
        key = self.obj_ind.generate_reserve_id_key(self.resource)
        self.assertEqual(key, '14a343245e1adb6297e43c12e22770ad&1&2&3')

    def test_reserve_id_wrong_type(self):
        img_data = self.setup_helper.get_image8_dict()
        img_resource = BossResourceBasic(img_data)

        with self.assertRaises(SpdbError):
            start_id = self.obj_ind.reserve_ids(img_resource, 10)

    @patch('spdb.spatialdb.SpatialDB', autospec=True)
    def test_tight_bounding_box_z_axis_multiple_cuboids(self, mock_spdb):
        """Loose bounding box spans multiple cuboids."""
        resolution = 0
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]
        id = 12345
        x_rng = [0, x_cube_dim]
        y_rng = [0, y_cube_dim]
        z_rng = [0, 2*z_cube_dim]
        t_rng = [0, 1]

        cube = Cube.create_cube(
            self.resource, (x_cube_dim, y_cube_dim, z_cube_dim))
        cube.data = np.zeros((1, z_cube_dim, y_cube_dim, x_cube_dim))
        cube.data[0][13][509][11] = id
        cube.data[0][14][509][11] = id
        cube.data[0][15][509][11] = id

        cube2 = Cube.create_cube(
            self.resource, (x_cube_dim, y_cube_dim, z_cube_dim))
        cube2.data = np.zeros((1, z_cube_dim, y_cube_dim, x_cube_dim))
        cube2.data[0][0][509][11] = id
        cube2.data[0][1][509][11] = id

        # Return cube on the 1st call to cutout and cube2 on the 2nd call.
        mock_spdb.cutout.side_effect = [cube, cube2]

        expected = (13, 17)

        # Method under test.
        actual = self.obj_ind._get_tight_bounding_box_z_axis(
            mock_spdb.cutout, self.resource, resolution, id,
            x_rng, y_rng, z_rng, t_rng)

        self.assertEqual(expected, actual)
        self.assertEqual(2, mock_spdb.cutout.call_count)

    def test_get_tight_bounding_box_ranges(self):
        """Ensure that ranges are Python style ranges: [x, y).

        In other words, make sure the max indices are incremented by 1.
        """
        resolution = 0
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]
        id = 12345
        x_rng = [0, x_cube_dim]
        y_rng = [0, y_cube_dim]
        z_rng = [0, 2*z_cube_dim]
        t_rng = [0, 1]

        # Don't need real one because will provide fake
        # _get_tight_bounding_box_*_axis().
        cutout_fcn = None

        with patch.object(self.obj_ind, '_get_tight_bounding_box_x_axis') as fake_get_x_axis:
            with patch.object(self.obj_ind, '_get_tight_bounding_box_y_axis') as fake_get_y_axis:
                with patch.object(self.obj_ind, '_get_tight_bounding_box_z_axis') as fake_get_z_axis:
                    x_min_max = (35, 40)
                    y_min_max = (100, 105)
                    z_min_max = (22, 26)

                    fake_get_x_axis.return_value = x_min_max
                    fake_get_y_axis.return_value = y_min_max
                    fake_get_z_axis.return_value = z_min_max

                    # Method under test.
                    actual = self.obj_ind.get_tight_bounding_box(
                        cutout_fcn, self.resource, resolution, id,
                        x_rng, y_rng, z_rng, t_rng)

                    self.assertIn('x_range', actual)
                    self.assertIn('y_range', actual)
                    self.assertIn('z_range', actual)
                    self.assertIn('t_range', actual)
                    self.assertEqual(x_min_max[0], actual['x_range'][0])
                    self.assertEqual(1+x_min_max[1], actual['x_range'][1])
                    self.assertEqual(y_min_max[0], actual['y_range'][0])
                    self.assertEqual(1+y_min_max[1], actual['y_range'][1])
                    self.assertEqual(z_min_max[0], actual['z_range'][0])
                    self.assertEqual(1+z_min_max[1], actual['z_range'][1])
                    self.assertEqual(t_rng, actual['t_range'])

    def test_write_cuboid_chunk_0(self):
        """
        When chunk number is 0, the key passed to write_cuboid_dynamo() should
        be unmodified.
        """
        with patch.object(self.obj_ind, 'write_cuboid_dynamo') as fake_write_cuboid_dynamo:
            res = 0
            id = 5555
            key = self.obj_ind.generate_channel_id_key(self.resource, res, id)
            chunk_num = 0
            version = 0
            morton = 3
            rev_id = 10
            lookup_key = '1&4&2&0'
            max_capacity = 100

            # Method under test.
            actual = self.obj_ind.write_cuboid(
                max_capacity, morton, key, chunk_num, rev_id, lookup_key, version)

            fake_write_cuboid_dynamo.assert_called_with(morton, key, rev_id, lookup_key, version)
            self.assertEqual(chunk_num, actual)

    def test_write_cuboid_chunk_n(self):
        """
        Key sent to write_cuboid_dynamo() should have the chunk number appended
        to it when it is non-zero.
        """
        with patch.object(self.obj_ind, 'write_cuboid_dynamo') as fake_write_cuboid_dynamo:
            res = 0
            id = 5555
            chunk_num = 2
            key = self.obj_ind.generate_channel_id_key(self.resource, res, id)
            exp_key = '{}&{}'.format(key, chunk_num)
            version = 0
            morton = 3
            rev_id = 10
            lookup_key = '1&4&2&0'
            max_capacity = 100

            # Method under test.
            actual = self.obj_ind.write_cuboid(
                max_capacity, morton, key, chunk_num, rev_id, lookup_key, version)

            fake_write_cuboid_dynamo.assert_called_with(
                morton, exp_key, rev_id, lookup_key, version)
            self.assertEqual(chunk_num, actual)

    def test_write_cuboid_partition_full(self):
        """
        Write of id to partition specified by key is full, so chunk_num should
        be incremented and id should be written to new parition.
        """
        with patch.object(self.obj_ind, 'write_cuboid_dynamo') as fake_write_cuboid_dynamo:

            # Raise exception on first call to simulate a full partition.
            resp = { 'Error': { 'Code': '413' } }
            fake_write_cuboid_dynamo.side_effect = [
                botocore.exceptions.ClientError(resp, 'update_item'),
                {}
            ]

            res = 0
            id = 5555
            chunk_num = 2
            new_chunk_num = chunk_num + 1

            key = self.obj_ind.generate_channel_id_key(self.resource, res, id)
            exp_key1 = '{}&{}'.format(key, chunk_num)
            exp_key2 = '{}&{}'.format(key, new_chunk_num)

            version = 0
            morton = 8
            rev_id = 10
            lookup_key = '1&4&2&0'
            max_capacity = 100

            # Method under test.
            actual = self.obj_ind.write_cuboid(
                max_capacity, morton, key, chunk_num, rev_id, lookup_key, version)

            # Should try to write to new partition after first try raises.
            exp_calls = [
                unittest.mock.call(morton, exp_key1, rev_id, lookup_key, version),
                unittest.mock.call(morton, exp_key2, None, lookup_key, version)
            ]
            fake_write_cuboid_dynamo.assert_has_calls(exp_calls)

            # Should return chunk number of new partition.
            self.assertEqual(new_chunk_num, actual)

    def test_write_cuboid_max_capacity_exceeded(self):
        """
        Write of id to partition specified by key succeeds but set max capacity
        for that chunk is exceeded.  In this case, the returned chunk_num 
        should be incremented.
        """
        with patch.object(self.obj_ind, 'write_cuboid_dynamo') as fake_write_cuboid_dynamo:

            fake_write_cuboid_dynamo.return_value = {
                'ResponseMetadata': {'HTTPStatusCode': 200},
                'ConsumedCapacity': {'CapacityUnits': 105.0}
            }

            res = 0
            id = 5555
            chunk_num = 2
            new_chunk_num = chunk_num + 1

            key = self.obj_ind.generate_channel_id_key(self.resource, res, id)
            exp_key1 = '{}&{}'.format(key, chunk_num)

            version = 0
            morton = 8
            rev_id = 10
            lookup_key = '1&4&2&0'
            max_capacity = 100

            # Method under test.
            actual = self.obj_ind.write_cuboid(
                max_capacity, morton, key, chunk_num, rev_id, lookup_key, version)

            exp_calls = [
                unittest.mock.call(morton, exp_key1, rev_id, lookup_key, version)
            ]
            fake_write_cuboid_dynamo.assert_has_calls(exp_calls)

            # Should return chunk number of new partition.
            self.assertEqual(new_chunk_num, actual)

    def test_lookup_found(self):
        with patch.object(self.obj_ind.dynamodb, 'query') as fake_dynamodb_query:
            fake_dynamodb_query.side_effect = [{'Count': 0}, {'Count': 1}]
            res = 0
            id = 5555
            key = self.obj_ind.generate_channel_id_key(self.resource, res, id)
            morton = 8
            last_chunk_num = 1
            version = 0

            actual = self.obj_ind.lookup(morton, key, last_chunk_num, version)

            self.assertTrue(actual[0])
            self.assertEqual(1, actual[1])

    def test_lookup_not_found(self):
        with patch.object(self.obj_ind.dynamodb, 'query') as fake_dynamodb_query:
            fake_dynamodb_query.side_effect = [{'Count': 0}, {'Count': 0}]
            res = 0
            id = 5555
            key = self.obj_ind.generate_channel_id_key(self.resource, res, id)
            morton = 8
            last_chunk_num = 1
            version = 0

            actual = self.obj_ind.lookup(morton, key, last_chunk_num, version)

            self.assertFalse(actual[0])
            self.assertEqual(-1, actual[1])

    # moto not parsing KeyConditionExpression properly - last tried v1.1.25.
    @unittest.skip('Waiting for moto to be fixed')
    def test_lookup_with_dynamo(self):
        res = 0
        id = 5555
        key = self.obj_ind.generate_channel_id_key(self.resource, res, id)
        morton = 8
        last_chunk_num = 1
        version = 0

        actual = self.obj_ind.lookup(morton, key, last_chunk_num, version)

        self.assertFalse(actual[0])
        self.assertEqual(-1, actual[1])

    def test_get_last_partition_key_and_rev_id(self):
        """
        Test when there is only one chunk for the entire object id.
        """
        with patch.object(self.obj_ind.dynamodb, 'get_item') as fake_dynamodb_get_item:
            expected_chunk = 0
            expected_rev_id = 25
            fake_dynamodb_get_item.return_value = { 
                'Item': {
                    LAST_PARTITION_KEY: { 'N': str(expected_chunk) },
                    REV_ID: { 'N': str(expected_rev_id) }
                },
                'ResponseMetadata': { 'HTTPStatusCode': 200 }
            }
            res = 0
            id = 5555
            version = 0
            key = self.obj_ind.generate_channel_id_key(self.resource, res, id)

            # Method under test.
            actual = self.obj_ind.get_last_partition_key_and_rev_id(key, version)

            self.assertEqual(expected_chunk, actual[0])
            self.assertEqual(expected_rev_id, actual[1])

    def test_get_last_partition_key_and_rev_id_multiple_chunks(self):
        """
        When there multiple chunks, the revision id must come from the last 
        chunk.
        """
        with patch.object(self.obj_ind.dynamodb, 'get_item') as fake_dynamodb_get_item:
            expected_chunk = 2
            first_chunk_rev_id = 229
            expected_rev_id = 25
            fake_dynamodb_get_item.side_effect = [
                { 
                    # Data from chunk 0.
                    'Item': {
                        LAST_PARTITION_KEY: { 'N': str(expected_chunk) },
                        REV_ID: { 'N': str(first_chunk_rev_id) }
                    },
                    'ResponseMetadata': { 'HTTPStatusCode': 200 }
                },
                {
                    # Data from chunk 2 (the last chunk).
                    'Item': {
                        REV_ID: { 'N': str(expected_rev_id) }
                    },
                    'ResponseMetadata': { 'HTTPStatusCode': 200 }
                }
            ]
            res = 0
            id = 5555
            version = 0
            key = self.obj_ind.generate_channel_id_key(self.resource, res, id)

            # Method under test.
            actual = self.obj_ind.get_last_partition_key_and_rev_id(key, version)

            expected_last_chunk_key = '{}&{}'.format(key, expected_chunk)
            self.assertEqual(2, fake_dynamodb_get_item.call_count)
            (_, _, kwargs) = fake_dynamodb_get_item.mock_calls[1]
            self.assertEqual(expected_last_chunk_key, kwargs['Key']['channel-id-key']['S'])

            self.assertEqual(expected_chunk, actual[0])
            self.assertEqual(expected_rev_id, actual[1])

    def test_get_last_partition_key_and_rev_id_no_last_partition_key_or_rev_id(self):
        """
        If there is no lastPartitionKey or revId, then should return (0, None).
        """
        with patch.object(self.obj_ind.dynamodb, 'get_item') as fake_dynamodb_get_item:
            expected_chunk = 0
            expected_rev_id = None
            fake_dynamodb_get_item.return_value = { 
                'ResponseMetadata': { 'HTTPStatusCode': 200 },
                'Item': {}
            }
            res = 0
            id = 5555
            version = 0
            key = self.obj_ind.generate_channel_id_key(self.resource, res, id)

            # Method under test.
            actual = self.obj_ind.get_last_partition_key_and_rev_id(key, version)

            self.assertEqual(expected_chunk, actual[0])
            self.assertEqual(expected_rev_id, actual[1])

    def test_get_last_partition_key_and_rev_id_item_does_not_exist(self):
        """
        If the key does not exist at all, then should raise KeyError.
        """
        with patch.object(self.obj_ind.dynamodb, 'get_item') as fake_dynamodb_get_item:
            fake_dynamodb_get_item.return_value = { 
                'ResponseMetadata': { 'HTTPStatusCode': 200 }
            }
            res = 0
            id = 5555
            version = 0
            key = self.obj_ind.generate_channel_id_key(self.resource, res, id)
            with self.assertRaises(KeyError):
                self.obj_ind.get_last_partition_key_and_rev_id(key, version)

    def test_write_id_index(self):
        """
        Standard case where a new Dynamo key does not need to be created.
        """
        res = 0
        time_sample = 0
        morton = 11
        id = 4
        version = 0
        last_partition_key = 2
        rev_id = 521
        max_capacity = 100

        obj_key = AWSObjectStore.generate_object_key(
            self.resource, res, time_sample, morton)
        chan_key = self.obj_ind.generate_channel_id_key(self.resource, res, id)
        key_parts = AWSObjectStore.get_object_key_parts(obj_key)
        lookup_key = AWSObjectStore.generate_lookup_key(
            key_parts.collection_id, key_parts.experiment_id, 
            key_parts.channel_id, key_parts.resolution)


        with patch.multiple(
            self.obj_ind, 
            get_last_partition_key_and_rev_id=DEFAULT,
            lookup=DEFAULT,
            write_cuboid=DEFAULT,
            update_last_partition_key=DEFAULT
        ) as mocks:

            mocks['get_last_partition_key_and_rev_id'].return_value = (
                last_partition_key, rev_id
            )
            mocks['write_cuboid'].return_value = last_partition_key
            mocks['lookup'].return_value = (False, -1)

            # Method under test.
            self.obj_ind.write_id_index(max_capacity, obj_key, id, version)

            mocks['write_cuboid'].assert_called_with(
                max_capacity, str(morton), chan_key, last_partition_key, 
                rev_id, lookup_key, version)
            self.assertFalse(mocks['update_last_partition_key'].called)

    def test_write_id_index_new_id(self):
        """
        Case where id is written to Dynamo for the first time.
        """
        res = 0
        time_sample = 0
        morton = 11
        id = 4
        version = 0
        last_partition_key = 0
        rev_id = None
        max_capacity = 100

        obj_key = AWSObjectStore.generate_object_key(
            self.resource, res, time_sample, morton)
        chan_key = self.obj_ind.generate_channel_id_key(self.resource, res, id)
        key_parts = AWSObjectStore.get_object_key_parts(obj_key)
        lookup_key = AWSObjectStore.generate_lookup_key(
            key_parts.collection_id, key_parts.experiment_id, 
            key_parts.channel_id, key_parts.resolution)


        with patch.multiple(
            self.obj_ind, 
            get_last_partition_key_and_rev_id=DEFAULT,
            lookup=DEFAULT,
            write_cuboid=DEFAULT,
            update_last_partition_key=DEFAULT
        ) as mocks:

            # Id doesn't exist in Dynamo table, yet.
            mocks['get_last_partition_key_and_rev_id'].side_effect = (
                KeyError()
            )
            mocks['write_cuboid'].return_value = last_partition_key
            mocks['lookup'].return_value = (False, -1)

            # Method under test.
            self.obj_ind.write_id_index(max_capacity, obj_key, id, version)

            mocks['write_cuboid'].assert_called_with(
                max_capacity, str(morton), chan_key, last_partition_key, 
                rev_id, lookup_key, version)
            self.assertFalse(mocks['update_last_partition_key'].called)

    def test_write_id_index_overflow(self):
        """
        Case where a new Dynamo key needs to be created because the
        current key is full.  The LAST_PARTITION_KEY should be updated.
        """
        res = 0
        time_sample = 0
        morton = 11
        id = 4
        version = 0
        last_partition_key = 2
        rev_id = 224
        no_rev_id = None
        max_capacity = 100

        obj_key = AWSObjectStore.generate_object_key(
            self.resource, res, time_sample, morton)
        chan_key = self.obj_ind.generate_channel_id_key(self.resource, res, id)
        key_parts = AWSObjectStore.get_object_key_parts(obj_key)
        lookup_key = AWSObjectStore.generate_lookup_key(
            key_parts.collection_id, key_parts.experiment_id, 
            key_parts.channel_id, key_parts.resolution)

        with patch.multiple(
            self.obj_ind, 
            get_last_partition_key_and_rev_id=DEFAULT,
            lookup=DEFAULT,
            write_cuboid=DEFAULT,
            update_last_partition_key=DEFAULT
        ) as mocks:

            mocks['get_last_partition_key_and_rev_id'].return_value = (
                last_partition_key, rev_id
            )
            mocks['write_cuboid'].return_value = last_partition_key + 1
            mocks['lookup'].return_value = (False, -1)

            # Method under test.
            self.obj_ind.write_id_index(max_capacity, obj_key, id, version)

            mocks['write_cuboid'].assert_called_with(
                max_capacity, str(morton), chan_key, last_partition_key, 
                rev_id, lookup_key, version)
            mocks['update_last_partition_key'].assert_called_with(
                chan_key, last_partition_key + 1,  version)

    def test_update_last_partition_key(self):
        """
        Just exercise the Dynamo update_item call.
        """
        res = 0
        time_sample = 0
        morton = 11
        id = 4
        version = 0
        chunk_num = 2
        chan_key = self.obj_ind.generate_channel_id_key(self.resource, res, id)

        self.obj_ind.update_last_partition_key(chan_key, chunk_num, version)

    @unittest.skip('Moto 1.2 fails now that if_not_exists added to UpdateExpression')
    def test_write_cuboid_dynamo_no_revision_id(self):
        """
        Just exercise the Dynamo update_item call with no revision id.
        """
        res = 0
        time_sample = 0
        morton = 11
        id = 4
        version = 0
        rev_id = None
        lookup_key = '1&4&2&0'
        chan_key = self.obj_ind.generate_channel_id_key(self.resource, res, id)

        self.obj_ind.write_cuboid_dynamo(
            morton, chan_key, rev_id, lookup_key, version)


class TestObjectIndices(ObjectIndicesTestMixin, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """ Create a diction of configuration values for the test resource. """
        # Create resource
        cls.setup_helper = SetupTests()
        cls.data = cls.setup_helper.get_anno64_dict()
        cls.resource = BossResourceBasic(cls.data)

        # Load config
        cls.config = configuration.BossConfig()
        cls.object_store_config = {"s3_flush_queue": 'https://mytestqueue.com',
                                   "cuboid_bucket": "test_bucket",
                                   "page_in_lambda_function": "page_in.test.boss",
                                   "page_out_lambda_function": "page_out.test.boss",
                                   "s3_index_table": "test_s3_table",
                                   "id_index_table": "test_id_table",
                                   "id_count_table": "test_count_table",
                                   }

        # Create AWS Resources needed for tests while mocking
        cls.setup_helper.start_mocking()
        with patch('spdb.spatialdb.test.setup.get_region') as fake_get_region:
            fake_get_region.return_value = 'us-east-1'
            cls.setup_helper.create_index_table(cls.object_store_config["id_count_table"], cls.setup_helper.ID_COUNT_SCHEMA)
            cls.setup_helper.create_index_table(cls.object_store_config["id_index_table"], cls.setup_helper.ID_INDEX_SCHEMA)

        cls.obj_ind = ObjectIndices(cls.object_store_config["s3_index_table"],
                                    cls.object_store_config["id_index_table"],
                                    cls.object_store_config["id_count_table"],
                                    cls.object_store_config["cuboid_bucket"],
                                    'us-east-1')

    @classmethod
    def tearDownClass(cls):
        cls.setup_helper.stop_mocking()

if __name__ == '__main__':
    unittest.main()
