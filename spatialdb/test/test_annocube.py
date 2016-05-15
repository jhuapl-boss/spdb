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

from spdb.spatialdb import AnnotateCube64, Cube, SpdbError
from spdb.project import BossResourceBasic
import numpy as np


class TestAnnotateCube64(unittest.TestCase):
    """Test the ImageCube8 Class parent class functionality"""

    def test_constructor_no_dim_no_time(self):
        """Test the Cube class constructor"""
        c = AnnotateCube64()

        assert c.cube_size == [64, 64, 64]
        assert c.x_dim == 64
        assert c.y_dim == 64
        assert c.z_dim == 64

        assert c.from_zeros() == False
        assert c.time_range == [0, 1]
        assert c.is_time_series == False

    def test_constructor_no_time(self):
        """Test the Cube class constructor"""
        c = AnnotateCube64([128, 64, 16])

        assert c.cube_size == [16, 64, 128]
        assert c.x_dim == 128
        assert c.y_dim == 64
        assert c.z_dim == 16

        assert c.from_zeros() == False
        assert c.time_range == [0, 1]
        assert c.is_time_series == False

    def test_constructor(self):
        """Test the Cube class constructor"""
        c = AnnotateCube64([128, 64, 16], [0, 10])

        assert c.cube_size == [16, 64, 128]
        assert c.x_dim == 128
        assert c.y_dim == 64
        assert c.z_dim == 16

        assert c.from_zeros() == False
        assert c.time_range == [0, 10]
        assert c.is_time_series == True
        assert c.data.dtype == np.uint64

    def test_zeros(self):
        """Test populating a default Cube instance with zeros"""
        c = AnnotateCube64()
        c.zeros()

        size = c.data.shape

        assert size == (1, 64, 64, 64)
        assert c.from_zeros() == True
        assert c.data.size == 64 * 64 * 64
        assert c.data.dtype == np.uint64
        assert c.data.sum() == 0

    def test_zeros_time_samples(self):
        """Test populating a default Cube instance with zeros"""
        c = AnnotateCube64([128, 64, 16], [2, 10])
        c.zeros()

        size = c.data.shape

        assert size == (8, 16, 64, 128)
        assert c.from_zeros() == True
        assert c.data.size == 8 * 16 * 64 * 128
        assert c.data.dtype == np.uint64
        assert c.data.sum() == 0

    def test_add_data_no_time(self):
        """Test adding data from a smaller cube to a bigger one"""

        c_base = AnnotateCube64([20, 15, 10])
        c_base.zeros()
        assert c_base.is_not_zeros() == False

        c_add = AnnotateCube64([5, 5, 5])
        c_add.zeros()
        c_add.data += 1
        assert c_add.is_not_zeros() == True

        # Make sure c_base empty
        assert c_base.data.sum() == 0

        # Insert c_add into c_base
        c_base.add_data(c_add, [0, 0, 0])

        # Make sure insertion happened
        assert c_base.data.sum() == 5 * 5 * 5

        # Make sure it was in the right spot
        assert c_base.data[0, 1, 2, 3] == 1
        assert c_base.data[0, 4, 4, 4] == 1
        assert c_base.data[0, 0, 0, 0] == 1
        assert c_base.data[0, 4, 4, 6] == 0
        assert c_base.data[0, 6, 4, 4] == 0
        assert c_base.data[0, 4, 6, 4] == 0

        # Try an offset in x insert
        c_base.zeros()
        c_base.add_data(c_add, [1, 0, 0])
        # Make sure insertion happened
        assert c_base.data.sum() == 5 * 5 * 5

        # Make sure it was in the right spot (remember data is still stored in zyx under the hood)
        assert c_base.data[0, 1, 1, 1] == 0
        assert c_base.data[0, 4, 4, 6] == 1

        # Try an offset in y insert
        c_base.zeros()
        c_base.add_data(c_add, [0, 1, 0])
        # Make sure insertion happened
        assert c_base.data.sum() == 5 * 5 * 5

        # Make sure it was in the right spot (remember data is still stored in zyx under the hood)
        assert c_base.data[0, 1, 1, 1] == 0
        assert c_base.data[0, 4, 6, 4] == 1

        # Try an offset in z insert
        c_base.zeros()
        c_base.add_data(c_add, [0, 0, 1])
        # Make sure insertion happened
        assert c_base.data.sum() == 5 * 5 * 5

        # Make sure it was in the right spot (remember data is still stored in zyx under the hood)
        assert c_base.data[0, 1, 1, 1] == 0
        assert c_base.data[0, 6, 4, 4] == 1

    def test_overwrite_no_time(self):
        """Test overwriting data - for ImageCub8 this just does a copy."""
        c_base = AnnotateCube64([10, 20, 5])
        c_base.zeros()

        data = np.zeros((1, 5, 20, 10), np.uint64)
        data[0, 1, 2, 5] = 1

        # Make sure c_base empty
        assert c_base.data.sum() == 0

        # Insert c_add into c_base
        c_base.overwrite(data)

        # Make sure insertion happened
        assert c_base.data.sum() == 1

        # Make sure it was in the right spot
        assert c_base.data[0, 1, 2, 5] == 1

    def test_overwrite_simple(self):
        """Test overwriting data - for ImageCub8 this just does a copy."""
        c_base = AnnotateCube64([10, 20, 5], [0, 10])
        c_base.zeros()

        data = np.zeros((3, 5, 20, 10), np.uint64)
        data[0, 1, 2, 5] = 1
        data[2, 3, 15, 7] = 3

        # Make sure c_base empty
        assert c_base.data.sum() == 0
        assert c_base.data[0, 1, 2, 5] == 0
        assert c_base.data[2, 3, 15, 7] == 0

        # Insert c_add into c_base
        c_base.overwrite(data, [4, 7])

        # Make sure insertion happened
        assert c_base.data.sum() == 4

        # Make sure it was in the right spot
        assert c_base.data[0, 1, 2, 5] == 0
        assert c_base.data[2, 3, 15, 7] == 0

        # Should insert starting at T=4
        assert c_base.data[4, 1, 2, 5] == 1
        assert c_base.data[6, 3, 15, 7] == 3

    def test_overwrite(self):
        """Test overwriting data with existing data in place."""
        # Create base data
        c_base = AnnotateCube64([10, 20, 5], [0, 10])
        c_base.zeros()

        c_base.data[1, 1, 1, 1] = 1
        c_base.data[2, 4, 4, 1] = 2

        # Create overwrite data - all zero locations
        data = np.zeros((2, 5, 20, 10), np.uint64)
        data[0, 2, 2, 2] = 3
        data[1, 4, 5, 6] = 4

        # Insert c_add into c_base
        c_base.overwrite(data, [1, 3])

        # Make sure insertion happened
        assert c_base.data.sum() == 1 + 2 + 3 + 4

        # Make sure it was in the right spot
        assert c_base.data[1, 1, 1, 1] == 1
        assert c_base.data[2, 4, 4, 1] == 2
        assert c_base.data[1, 2, 2, 2] == 3
        assert c_base.data[2, 4, 5, 6] == 4

        # Place data in existing voxel locations and re-do overwrite
        data[0, 1, 1, 1] = 5
        data[1, 4, 4, 1] = 6

        # Insert into c_base
        c_base.overwrite(data, [1, 3])

        # Make sure insertion happened
        assert c_base.data.sum() == 3 + 4 + 5 + 6

        # Make sure it was in the right spot
        assert c_base.data[1, 1, 1, 1] == 5
        assert c_base.data[2, 4, 4, 1] == 6
        assert c_base.data[1, 2, 2, 2] == 3
        assert c_base.data[2, 4, 5, 6] == 4

    def test_trim_no_time(self):
        """Test trimming off part of a cube"""
        c = AnnotateCube64([10, 20, 5])
        c.zeros()
        c.data += 1
        assert c.data.sum() == 10*20*5

        c.data[0, 2, 7, 5] = 5

        c.trim(5, 5, 7, 6, 2, 2)

        assert c.data[0, 0, 0, 0] == 5
        assert c.data.sum() == 5 * 6 * 2 + 4

    def test_trim(self):
        """Test trimming off part of a cube"""
        c = AnnotateCube64([10, 20, 5], [0, 4])
        c.zeros()
        c.data += 1
        assert c.data.sum() == 10*20*5*4

        c.data[0, 2, 7, 5] = 5
        c.data[1, 3, 10, 7] = 2

        c.trim(5, 5, 7, 6, 2, 2)

        assert c.data[0, 0, 0, 0] == 5
        assert c.data[0, 1, 3, 2] == 1
        assert c.data[1, 1, 3, 2] == 2
        assert c.data.sum() == 4 * 2 * 6 * 5 + 4 + 1

    def test_blosc_time_index_no_time(self):
        """Test blosc compression of Cube data"""

        c = AnnotateCube64([10, 20, 5])
        c2 = AnnotateCube64([10, 20, 5])
        data = np.random.randint(0, 255, size=[1, 5, 20, 10])
        c.data = data

        byte_array = c.get_blosc_numpy_by_time_index()
        c2.from_blosc_numpy([byte_array])

        np.testing.assert_array_equal(c.data, c2.data)
        assert c.cube_size == c2.cube_size
        assert c.z_dim == c2.z_dim
        assert c.y_dim == c2.y_dim
        assert c.x_dim == c2.x_dim

    def test_blosc_time_index_specific_time(self):
        """Test blosc compression of Cube data"""
        c = AnnotateCube64([10, 20, 5], [0, 4])
        c2 = AnnotateCube64([10, 20, 5], [0, 4])
        data = np.random.randint(0, 255, size=[4, 5, 20, 10])
        c.data = data

        byte_array = c.get_blosc_numpy_by_time_index(2)
        c2.from_blosc_numpy([byte_array])

        np.testing.assert_array_equal(np.expand_dims(c.data[2, :, :, :], axis=0), c2.data)
        assert c.cube_size == c2.cube_size
        assert c.z_dim == c2.z_dim
        assert c.y_dim == c2.y_dim
        assert c.x_dim == c2.x_dim

    def test_blosc_all_time_samples(self):
        """Test blosc compression of Cube data"""

        c = AnnotateCube64([10, 20, 5], [0, 4])
        c2 = AnnotateCube64([10, 20, 5], [0, 4])
        data = np.random.randint(0, 255, size=[4, 5, 20, 10])
        c.data = data

        byte_array = [x for x in c.get_all_blosc_numpy_arrays()]

        # Unpack tuples
        time_list, byte_list = zip(*byte_array)

        c2.from_blosc_numpy(byte_list, [time_list[0], time_list[-1] + 1])

        np.testing.assert_array_equal(c.data, c2.data)
        assert c.cube_size == c2.cube_size
        assert c.z_dim == c2.z_dim
        assert c.y_dim == c2.y_dim
        assert c.x_dim == c2.x_dim
        assert c.time_range == c2.time_range
        assert c.is_time_series == True
        assert c2.is_time_series == True

    def test_blosc_all_time_samples_single_array(self):
        """Test blosc compression of Cube data"""

        c = AnnotateCube64([10, 20, 5], [0, 4])
        c2 = AnnotateCube64([10, 20, 5], [0, 4])
        data = np.random.randint(0, 255, size=[4, 5, 20, 10])
        c.data = data

        byte_array = c.to_blosc_numpy()

        c2.from_blosc_numpy(byte_array, [0, 4])

        np.testing.assert_array_equal(c.data, c2.data)
        assert c.cube_size == c2.cube_size
        assert c.z_dim == c2.z_dim
        assert c.y_dim == c2.y_dim
        assert c.x_dim == c2.x_dim
        assert c.time_range == c2.time_range
        assert c.is_time_series == True
        assert c2.is_time_series == True

    def test_factory_no_time(self):
        """Test the Cube factory in Cube"""

        data = {}
        data['boss_key'] = ['col1&exp1&ch1&0']
        data['lookup_key'] = ['1&1&1&0']
        data['collection'] = {}
        data['collection']['name'] = "col1"
        data['collection']['description'] = "Test collection 1"

        data['coord_frame'] = {}
        data['coord_frame']['name'] = "coord_frame_1"
        data['coord_frame']['description'] = "Test coordinate frame"
        data['coord_frame']['x_start'] = 0
        data['coord_frame']['x_stop'] = 2000
        data['coord_frame']['y_start'] = 0
        data['coord_frame']['y_stop'] = 5000
        data['coord_frame']['z_start'] = 0
        data['coord_frame']['z_stop'] = 200
        data['coord_frame']['x_voxel_size'] = 4
        data['coord_frame']['y_voxel_size'] = 4
        data['coord_frame']['z_voxel_size'] = 35
        data['coord_frame']['voxel_unit'] = "nanometers"
        data['coord_frame']['time_step'] = 0
        data['coord_frame']['time_step_unit'] = "na"

        data['experiment'] = {}
        data['experiment']['name'] = "exp1"
        data['experiment']['description'] = "Test experiment 1"
        data['experiment']['num_hierarchy_levels'] = 7
        data['experiment']['hierarchy_method'] = 'slice'

        data['channel_layer'] = {}
        data['channel_layer']['name'] = "layer1"
        data['channel_layer']['description'] = "Test layer 1"
        data['channel_layer']['is_channel'] = False
        data['channel_layer']['datatype'] = 'uint64'
        data['channel_layer']['max_time_step'] = 0
        data['channel_layer']['base_resolution'] = 0
        data['channel_layer']['layer_map'] = [0]

        resource = BossResourceBasic(data)

        c = Cube.create_cube(resource, [30, 20, 13])
        assert isinstance(c, AnnotateCube64) == True
        assert c.cube_size == [13, 20, 30]
        assert c.is_time_series == False
        assert c.time_range == [0, 1]

    def test_factory(self):
        """Test the Cube factory in Cube"""

        data = {}
        data['boss_key'] = ['col1&exp1&ch1&0']
        data['lookup_key'] = ['1&1&1&0']
        data['collection'] = {}
        data['collection']['name'] = "col1"
        data['collection']['description'] = "Test collection 1"

        data['coord_frame'] = {}
        data['coord_frame']['name'] = "coord_frame_1"
        data['coord_frame']['description'] = "Test coordinate frame"
        data['coord_frame']['x_start'] = 0
        data['coord_frame']['x_stop'] = 2000
        data['coord_frame']['y_start'] = 0
        data['coord_frame']['y_stop'] = 5000
        data['coord_frame']['z_start'] = 0
        data['coord_frame']['z_stop'] = 200
        data['coord_frame']['x_voxel_size'] = 4
        data['coord_frame']['y_voxel_size'] = 4
        data['coord_frame']['z_voxel_size'] = 35
        data['coord_frame']['voxel_unit'] = "nanometers"
        data['coord_frame']['time_step'] = 0
        data['coord_frame']['time_step_unit'] = "na"

        data['experiment'] = {}
        data['experiment']['name'] = "exp1"
        data['experiment']['description'] = "Test experiment 1"
        data['experiment']['num_hierarchy_levels'] = 7
        data['experiment']['hierarchy_method'] = 'slice'

        data['channel_layer'] = {}
        data['channel_layer']['name'] = "layer1"
        data['channel_layer']['description'] = "Test layer 1"
        data['channel_layer']['is_channel'] = False
        data['channel_layer']['datatype'] = 'uint64'
        data['channel_layer']['max_time_step'] = 0
        data['channel_layer']['base_resolution'] = 0
        data['channel_layer']['layer_map'] = [0]

        resource = BossResourceBasic(data)

        c = Cube.create_cube(resource, [30, 20, 13], [0, 15])
        assert isinstance(c, AnnotateCube64) == True
        assert c.cube_size == [13, 20, 30]
        assert c.is_time_series == True
        assert c.time_range == [0, 15]

