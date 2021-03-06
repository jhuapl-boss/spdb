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
from unittest.mock import patch
from fakeredis import FakeStrictRedis

import redis
import collections

from spdb.project import BossResourceBasic
from spdb.spatialdb import Cube, SpatialDB, SpdbError
from spdb.c_lib.ndtype import CUBOIDSIZE

import numpy as np

from spdb.spatialdb.test.setup import SetupTests

import spdb.spatialdb.object


@patch('spdb.spatialdb.object.get_region', autospec=True, return_value='us-east-1')

class SpatialDBImageDataTestMixin(object):

    cuboid_size = CUBOIDSIZE[0]
    x_dim = cuboid_size[0]
    y_dim = cuboid_size[1]
    z_dim = cuboid_size[2]

    def write_test_cube(self, sp, resource, res, cube, cache=True, s3=False):
        """
        Method to write data to test read operations
        Args:
            sp (spdb.spatialdb.SpatialDB): spdb instance
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            res (int): resolution
            morton_idx_list (list(int)): list of morton IDs to add
            time_sample_list (list(int)): list of time samples to add
            cube (list(bytes)): list of time samples to add
            cache (bool): boolean indicating if cubes should be written to cache
            s3 (bool): boolean indicating if cubes should be written to S3

        Returns:
            (list(str)): a list of the cached-cuboid keys written
        """
        # Get cache key
        t = []
        cube_bytes = []
        for time_point in range(cube.time_range[0], cube.time_range[1]):
            t.append(time_point)
            cube_bytes.append(cube.to_blosc_by_time_index(time_point))
        keys = sp.kvio.generate_cached_cuboid_keys(resource, res, t, [cube.morton_id])

        # Write cuboid to cache
        if cache:
            sp.kvio.put_cubes(keys, cube_bytes)

        # Write cuboid to S3
        if s3:
            obj_keys = sp.objectio.cached_cuboid_to_object_keys(keys)
            sp.objectio.put_objects(obj_keys, cube_bytes)

            # Add to S3 Index
            for key in obj_keys:
                sp.objectio.add_cuboid_to_index(key)

        return keys

    def test_resource_locked(self, fake_get_region):
        """Method to test if the resource is locked"""
        sp = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        assert not sp.resource_locked(self.resource.get_lookup_key())

        # Fake locking a project
        sp.cache_state.set_project_lock(self.resource.get_lookup_key(), True)
        assert sp.resource_locked(self.resource.get_lookup_key())

        # Fake unlocking a project
        sp.cache_state.set_project_lock(self.resource.get_lookup_key(), False)
        assert not sp.resource_locked(self.resource.get_lookup_key())

    def test_get_cubes_no_time_single(self, fake_get_region):
        """Test the get_cubes method - no time - single"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.random()
        cube1.morton_id = 32

        db = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        # populate dummy data
        keys = self.write_test_cube(db, self.resource, 0, cube1, cache=True, s3=False)

        cube2 = db.get_cubes(self.resource, keys)

        np.testing.assert_array_equal(cube1.data, cube2[0].data)

    def test_get_cubes_no_time_multiple(self, fake_get_region):
        """Test the get_cubes method - no time - multiple cubes"""
        # Generate random data

        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.random()
        cube1.morton_id = 32
        cube2 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube2.random()
        cube2.morton_id = 33
        cube3 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube3.random()
        cube3.morton_id = 36

        db = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        # populate dummy data
        keys = self.write_test_cube(db, self.resource, 0, cube1, cache=True, s3=False)
        keys.extend(self.write_test_cube(db, self.resource, 0, cube2, cache=True, s3=False))
        keys.extend(self.write_test_cube(db, self.resource, 0, cube3, cache=True, s3=False))

        cube_read = db.get_cubes(self.resource, keys)

        np.testing.assert_array_equal(cube1.data, cube_read[0].data)
        np.testing.assert_array_equal(cube2.data, cube_read[1].data)
        np.testing.assert_array_equal(cube3.data, cube_read[2].data)

    def test_get_cubes_time_single(self, fake_get_region):
        """Test the get_cubes method - time - single"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim], [0, 2])
        cube1.random()
        cube1.morton_id = 76

        db = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        # populate dummy data
        keys = self.write_test_cube(db, self.resource, 0, cube1, cache=True, s3=False)

        cube2 = db.get_cubes(self.resource, keys)

        np.testing.assert_array_equal(cube1.data, cube2[0].data)

    def test_get_cubes_time_multiple(self, fake_get_region):
        """Test the get_cubes method - time - multiple"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim], [0, 4])
        cube1.random()
        cube1.morton_id = 32
        cube2 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim], [0, 4])
        cube2.random()
        cube2.morton_id = 33

        db = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        # populate dummy data
        keys = self.write_test_cube(db, self.resource, 0, cube1, cache=True, s3=False)
        keys.extend(self.write_test_cube(db, self.resource, 0, cube2, cache=True, s3=False))

        cube_read = db.get_cubes(self.resource, keys)

        np.testing.assert_array_equal(cube1.data, cube_read[0].data)
        np.testing.assert_array_equal(cube2.data, cube_read[1].data)

    def test_get_cubes_missing_time_step(self, fake_get_region):
        """Test get_cubes() when not supplying keys for all time steps in a 
        time range.
        """
        EXTENTS = [self.x_dim, self.y_dim, self.z_dim]
        FIRST_T_RNG = (0, 4)
        cube1 = Cube.create_cube(self.resource, EXTENTS, FIRST_T_RNG)
        cube1.random()
        cube1.morton_id = 70

        # Note, no data for time steps 4 and 5 provided.

        SECOND_T_RNG = (6, 9)
        cube2 = Cube.create_cube(self.resource, EXTENTS, SECOND_T_RNG)
        cube2.random()
        cube2.morton_id = 70

        TOTAL_T_RNG = (0, 9)
        exp_cube = Cube.create_cube(self.resource, EXTENTS, TOTAL_T_RNG)
        exp_cube.zeros()
        exp_cube.morton_id = 70
        exp_cube.overwrite(cube1.data, FIRST_T_RNG)
        exp_cube.overwrite(cube2.data, SECOND_T_RNG)
            

        db = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        # populate dummy data
        keys = self.write_test_cube(db, self.resource, 0, cube1, cache=True, s3=False)
        keys.extend(self.write_test_cube(db, self.resource, 0, cube2, cache=True, s3=False))

        # Method under test.
        cube_read = db.get_cubes(self.resource, keys)

        np.testing.assert_array_equal(exp_cube.data, cube_read[0].data)

    def test_cutout_no_time_single_aligned_zero(self, fake_get_region):
        """Test the get_cubes method - no time - single"""
        db = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        cube = db.cutout(self.resource, (7, 88, 243), (self.x_dim, self.y_dim, self.z_dim), 0)

        np.testing.assert_array_equal(np.sum(cube.data), 0)

    def test_cutout_no_time_single_aligned_zero_access_mode_no_cache(self, fake_get_region):
        """Test the get_cubes method - no time - single - bypass cache"""
        db = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        cube = db.cutout(self.resource, (7, 88, 243), (self.x_dim, self.y_dim, self.z_dim), 0, access_mode="no_cache")

        np.testing.assert_array_equal(np.sum(cube.data), 0)

    def test_cutout_no_time_single_aligned_zero_access_mode_raw(self, fake_get_region):
        """Test the get_cubes method - no time - single - bypass cache and bypass dirty key check"""
        db = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        cube = db.cutout(self.resource, (7, 88, 243), (self.x_dim, self.y_dim, self.z_dim), 0, access_mode="raw")

        np.testing.assert_array_equal(np.sum(cube.data), 0)

    def test_cutout_no_time_single_aligned_zero_access_mode_cache(self, fake_get_region):
        """Test the get_cubes method - no time - single - DO NOT bypass cache"""
        db = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        cube = db.cutout(self.resource, (7, 88, 243), (self.x_dim, self.y_dim, self.z_dim), 0, access_mode="cache")

        np.testing.assert_array_equal(np.sum(cube.data), 0)

    def test_cutout_no_time_single_aligned_zero_access_mode_invalid(self, fake_get_region):
        """Test the get_cubes method - no time - single - Raise error due to invalid access_mode"""
        db = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        with self.assertRaises(SpdbError):
            db.cutout(self.resource, (7, 88, 243), (self.x_dim, self.y_dim, self.z_dim), 0, access_mode="wrong")

    def test_cutout_no_time_single_aligned_hit(self, fake_get_region):
        """Test the get_cubes method - no time - single"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.random()
        cube1.morton_id = 0

        db = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        # populate dummy data
        self.write_test_cube(db, self.resource, 0, cube1, cache=True, s3=False)

        cube2 = db.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0)

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_cutout_no_time_single_aligned_miss(self, fake_get_region):
        """Test the get_cubes method - no time - single"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.random()
        cube1.morton_id = 0

        db = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        # populate dummy data
        self.write_test_cube(db, self.resource, 0, cube1, cache=False, s3=True)

        cube2 = db.cutout(self.resource, (0, 0, 0), (self.x_dim, self.y_dim, self.z_dim), 0)

        np.testing.assert_array_equal(cube1.data, cube2.data)

    def test_write_cuboid_off_base_res(self, fake_get_region):
        """Test writing a cuboid to not the base resolution"""
        # Generate random data
        cube1 = Cube.create_cube(self.resource, [self.x_dim, self.y_dim, self.z_dim])
        cube1.random()
        cube1.morton_id = 0

        db = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        # populate dummy data
        with self.assertRaises(SpdbError):
            db.write_cuboid(self.resource, (0, 0, 0), 5, cube1.data, time_sample_start=0)

    def test_mark_missing_time_steps_none(self, fake_get_region):
        samples = [0, 1, 2, 3, 4, 5, 6]

        db = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        actual = db.mark_missing_time_steps(samples, 2, 5)

        self.assertEqual([], actual)

    def test_mark_missing_time_steps(self, fake_get_region):
        samples = [0, 1, 3, 5, 6, 7]

        db = SpatialDB(self.kvio_config, self.state_config, self.object_store_config)

        actual = db.mark_missing_time_steps(samples, 1, 4)

        self.assertEqual([2, 4], actual)


@patch('redis.StrictRedis', FakeStrictRedis)
class TestSpatialDBImage8Data(SpatialDBImageDataTestMixin, unittest.TestCase):

    @patch('spdb.spatialdb.test.setup.get_region')
    @patch('redis.StrictRedis', FakeStrictRedis)
    def setUp(self, fake_get_region):
        """ Set everything up for testing """
        # setup resources
        fake_get_region.return_value = 'us-east-1'
        self.setup_helper = SetupTests()
        self.setup_helper.mock = True

        self.data = self.setup_helper.get_image8_dict()
        self.resource = BossResourceBasic(self.data)

        # kvio settings
        self.cache_client = redis.StrictRedis(host='https://mytestcache.com', port=6379,
                                              db=1,
                                              decode_responses=False)
        self.kvio_config = {"cache_client": self.cache_client, "read_timeout": 86400}

        # state settings
        self.state_client = redis.StrictRedis(host='https://mytestcache2.com',
                                              port=6379, db=1,
                                              decode_responses=False)
        self.state_config = {"state_client": self.state_client}

        # object store settings
        self.object_store_config = {"s3_flush_queue": 'https://mytestqueue.com',
                                    "cuboid_bucket": "test_bucket",
                                    "page_in_lambda_function": "page_in.test.boss",
                                    "page_out_lambda_function": "page_out.test.boss",
                                    "s3_index_table": "test_table",
                                    "id_index_table": "test_id_table",
                                    "id_count_table": "test_count_table",
                                    }

        # Create AWS Resources needed for tests
        self.setup_helper.start_mocking()
        with patch('spdb.spatialdb.test.setup.get_region') as fake_get_region:
            fake_get_region.return_value = 'us-east-1'
            self.setup_helper.create_index_table(self.object_store_config["s3_index_table"], self.setup_helper.DYNAMODB_SCHEMA)
            self.setup_helper.create_cuboid_bucket(self.object_store_config["cuboid_bucket"])

    def tearDown(self):
        # Stop mocking
        self.setup_helper.stop_mocking()


@patch('redis.StrictRedis', FakeStrictRedis)
class TestSpatialDBImage16Data(SpatialDBImageDataTestMixin, unittest.TestCase):


    @patch('spdb.spatialdb.test.setup.get_region')
    @patch('redis.StrictRedis', FakeStrictRedis)
    def setUp(self, fake_get_region):
        """ Set everything up for testing """
        # setup resources
        fake_get_region.return_value = 'us-east-1'
        self.setup_helper = SetupTests()
        self.setup_helper.mock = True

        self.data = self.setup_helper.get_image16_dict()
        self.resource = BossResourceBasic(self.data)

        # kvio settings
        self.cache_client = redis.StrictRedis(host='https://mytestcache.com', port=6379,
                                              db=1,
                                              decode_responses=False)
        self.kvio_config = {"cache_client": self.cache_client, "read_timeout": 86400}

        # state settings
        self.state_client = redis.StrictRedis(host='https://mytestcache2.com',
                                              port=6379, db=1,
                                              decode_responses=False)
        self.state_config = {"state_client": self.state_client}

        # object store settings
        self.object_store_config = {"s3_flush_queue": 'https://mytestqueue.com',
                                    "cuboid_bucket": "test_bucket",
                                    "page_in_lambda_function": "page_in.test.boss",
                                    "page_out_lambda_function": "page_out.test.boss",
                                    "s3_index_table": "test_table",
                                    "id_index_table": "test_id_table",
                                    "id_count_table": "test_count_table",
                                    }

        # Create AWS Resources needed for tests
        self.setup_helper.start_mocking()
        with patch('spdb.spatialdb.test.setup.get_region') as fake_get_region:
            fake_get_region.return_value = 'us-east-1'
            self.setup_helper.create_index_table(self.object_store_config["s3_index_table"], self.setup_helper.DYNAMODB_SCHEMA)
            self.setup_helper.create_cuboid_bucket(self.object_store_config["cuboid_bucket"])

    def tearDown(self):
        # Stop mocking
        self.setup_helper.stop_mocking()

