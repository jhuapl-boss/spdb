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

from spdb.spatialdb import ImageCube8, ImageCube16, Cube, SpdbError
from spdb.project import BossResourceBasic
from spdb.c_lib.ndtype import CUBOIDSIZE
import numpy as np

from spdb.project.test.resource_setup import get_image_dict

from PIL import Image


class TestImageCube8(unittest.TestCase):
    """Test the ImageCube8 Class parent class functionality"""

    cuboid_size = CUBOIDSIZE[0]
    cuboid_x_dim = cuboid_size[0]
    cuboid_y_dim = cuboid_size[1]
    cuboid_z_dim = cuboid_size[2]

    def test_constructor_no_dim_no_time(self):
        """Test the Cube class constructor"""
        c = ImageCube8()

        assert c.cube_size == [self.cuboid_z_dim, self.cuboid_y_dim, self.cuboid_x_dim]
        assert c.x_dim == self.cuboid_x_dim
        assert c.y_dim == self.cuboid_y_dim
        assert c.z_dim == self.cuboid_z_dim

        assert c.from_zeros() is False
        assert c.time_range == [0, 1]
        assert c.is_time_series is False

    def test_constructor_no_time(self):
        """Test the Cube class constructor"""
        c = ImageCube8([128, 64, 16])

        assert c.cube_size == [16, 64, 128]
        assert c.x_dim == 128
        assert c.y_dim == 64
        assert c.z_dim == 16

        assert c.from_zeros() is False
        assert c.time_range == [0, 1]
        assert c.is_time_series is False

    def test_constructor(self):
        """Test the Cube class constructor"""
        c = ImageCube8([128, 64, 16], [0, 10])

        assert c.cube_size == [16, 64, 128]
        assert c.x_dim == 128
        assert c.y_dim == 64
        assert c.z_dim == 16

        assert c.from_zeros() is False
        assert c.time_range == [0, 10]
        assert c.is_time_series is True
        assert c.data.dtype == np.uint8

    def test_zeros(self):
        """Test populating a default Cube instance with zeros"""
        c = ImageCube8()
        c.zeros()

        size = c.data.shape

        assert size == (1, self.cuboid_z_dim, self.cuboid_y_dim, self.cuboid_x_dim)
        assert c.from_zeros() is True
        assert c.data.size == self.cuboid_z_dim * self.cuboid_y_dim * self.cuboid_x_dim
        assert c.data.dtype == np.uint8
        assert c.data.sum() == 0

    def test_random(self):
        """Test populating a default Cube instance with zeros"""
        c = ImageCube8()
        c.random()

        size = c.data.shape

        assert size == (1, self.cuboid_z_dim, self.cuboid_y_dim, self.cuboid_x_dim)
        assert c.from_zeros() is False
        assert c.data.size == self.cuboid_z_dim * self.cuboid_y_dim * self.cuboid_x_dim
        assert c.data.dtype == np.uint8
        assert c.data.sum() > 0

        c2 = ImageCube8()
        c2.random()

        self.assertRaises(AssertionError, np.testing.assert_array_equal, c.data, c2.data)

    def test_zeros_time_samples(self):
        """Test populating a default Cube instance with zeros"""
        c = ImageCube8([128, 64, 16], [2, 10])
        c.zeros()

        size = c.data.shape

        assert size == (8, 16, 64, 128)
        assert c.from_zeros() is True
        assert c.data.size == 8 * 16 * 64 * 128
        assert c.data.dtype == np.uint8
        assert c.data.sum() == 0

    def test_add_data_no_time(self):
        """Test adding data from a smaller cube to a bigger one"""

        c_base = ImageCube8([20, 15, 10])
        c_base.zeros()
        assert c_base.is_not_zeros() is False

        c_add = ImageCube8([5, 5, 5])
        c_add.zeros()
        c_add.data += 1
        assert c_add.is_not_zeros() is True

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
        c_base = ImageCube8([10, 20, 5])
        c_base.zeros()

        data = np.zeros((1, 5, 20, 10), np.uint8)
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
        c_base = ImageCube8([10, 20, 5], [0, 10])
        c_base.zeros()

        data = np.zeros((3, 5, 20, 10), np.uint8)
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
        c_base = ImageCube8([10, 20, 5], [0, 10])
        c_base.zeros()

        c_base.data[1, 1, 1, 1] = 1
        c_base.data[2, 4, 4, 1] = 2

        # Create overwrite data - all zero locations
        data = np.zeros((2, 5, 20, 10), np.uint8)
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
        c = ImageCube8([10, 20, 5])
        c.zeros()
        c.data += 1
        assert c.data.sum() == 10*20*5

        c.data[0, 2, 7, 5] = 5

        c.trim(5, 5, 7, 6, 2, 2)

        assert c.data[0, 0, 0, 0] == 5
        assert c.data.sum() == 5 * 6 * 2 + 4

    def test_trim(self):
        """Test trimming off part of a cube"""
        c = ImageCube8([10, 20, 5], [0, 4])
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

        c = ImageCube8([10, 20, 5])
        c.random()
        c2 = ImageCube8([10, 20, 5])

        byte_array = c.to_blosc_by_time_index()
        c2.from_blosc([byte_array])

        np.testing.assert_array_equal(c.data, c2.data)
        assert c.cube_size == c2.cube_size
        assert c.z_dim == c2.z_dim
        assert c.y_dim == c2.y_dim
        assert c.x_dim == c2.x_dim

    def test_blosc_time_index_specific_time(self):
        """Test blosc compression of Cube data"""
        c = ImageCube8([10, 20, 5], [0, 4])
        c.random()
        c2 = ImageCube8([10, 20, 5], [0, 4])

        byte_array = c.to_blosc_by_time_index(2)
        c2.from_blosc([byte_array])

        np.testing.assert_array_equal(np.expand_dims(c.data[2, :, :, :], axis=0), c2.data)
        assert c.cube_size == c2.cube_size
        assert c.z_dim == c2.z_dim
        assert c.y_dim == c2.y_dim
        assert c.x_dim == c2.x_dim

    #def test_blosc_all_time_samples(self):
    #    """Test blosc compression of Cube data"""
#
    #    c = ImageCube8([10, 20, 5], [0, 4])
    #    c2 = ImageCube8([10, 20, 5], [0, 4])
    #    data = np.random.randint(0, 255, size=[4, 5, 20, 10])
    #    c.data = data
#
    #    byte_array = [x for x in c.get_all_blosc_numpy_arrays()]
#
    #    # Unpack tuples
    #    time_list, byte_list = zip(*byte_array)
#
    #    c2.from_blosc(byte_list, [time_list[0], time_list[-1] + 1])
#
    #    np.testing.assert_array_equal(c.data, c2.data)
    #    assert c.cube_size == c2.cube_size
    #    assert c.z_dim == c2.z_dim
    #    assert c.y_dim == c2.y_dim
    #    assert c.x_dim == c2.x_dim
    #    assert c.time_range == c2.time_range
    #    assert c.is_time_series is True
    #    assert c2.is_time_series is True

    def test_blosc_all_time_samples_single_array(self):
        """Test blosc compression of Cube data"""

        c = ImageCube8([10, 20, 5], [0, 4])
        c.random()
        c2 = ImageCube8([10, 20, 5], [0, 4])

        byte_array = c.to_blosc()

        c2.from_blosc(byte_array, [0, 4])

        np.testing.assert_array_equal(c.data, c2.data)
        assert c.cube_size == c2.cube_size
        assert c.z_dim == c2.z_dim
        assert c.y_dim == c2.y_dim
        assert c.x_dim == c2.x_dim
        assert c.time_range == c2.time_range
        assert c.is_time_series is True
        assert c2.is_time_series is True

    def test_blosc_missing_time_step(self):
        """Test case when one of the cuboids at a time step is missing.  This
        happens when trying to load from the cache.  Only keys in the cache
        are passed to from_blosc().
        """
        EXTENTS = [10, 20, 5]
        cube_t0_1 = ImageCube8(EXTENTS, [0, 2])
        cube_t3 = ImageCube8(EXTENTS)
        exp_cube = ImageCube8(EXTENTS, [0, 4])
        exp_cube.zeros()
        exp_cube.overwrite(cube_t0_1.data, [0, 2])
        exp_cube.overwrite(cube_t3.data, [3, 4])

        cube_bytes_t0 = cube_t0_1.to_blosc_by_time_index(0)
        cube_bytes_t1 = cube_t0_1.to_blosc_by_time_index(1)
        cube_bytes_t3 = cube_t3.to_blosc_by_time_index(0)
        cube_list = (cube_bytes_t0, cube_bytes_t1, cube_bytes_t3)

        actual_cube = ImageCube8(EXTENTS, [0, 4])

        missing_time_step = [2]

        # Method under test.
        actual_cube.from_blosc(cube_list, [0, 4], missing_time_step)

        np.testing.assert_array_equal(exp_cube.data, actual_cube.data)
        assert actual_cube.is_time_series is True

    def test_factory_no_time(self):
        """Test the Cube factory in Cube"""

        data = get_image_dict()
        resource = BossResourceBasic(data)

        c = Cube.create_cube(resource, [30, 20, 13])
        assert isinstance(c, ImageCube8) is True
        assert c.cube_size == [13, 20, 30]
        assert c.is_time_series is False
        assert c.time_range == [0, 1]

    def test_factory(self):
        """Test the Cube factory in Cube"""

        data = get_image_dict()
        resource = BossResourceBasic(data)

        c = Cube.create_cube(resource, [30, 20, 13], [0, 15])
        assert isinstance(c, ImageCube8) is True
        assert c.cube_size == [13, 20, 30]
        assert c.is_time_series is True
        assert c.time_range == [0, 15]

    def test_tile_xy(self):
        """Test getting an xy tile."""
        # Create base data
        c_base = ImageCube8([128, 128, 16], [0, 1])
        c_base.zeros()

        c_base.data = np.random.randint(1, 254, (1, 16, 128, 128)).astype(np.uint8)

        img = c_base.xy_image(z_index=1)
        assert img.size == (128, 128)
        assert img.im[0] == c_base.data[0, 1, 0, 0]

    def test_tile_yz(self):
        """Test getting an xy tile."""
        # Create base data
        c_base = ImageCube8([128, 100, 16], [0, 1])
        c_base.zeros()

        c_base.data = np.random.randint(1, 254, (1, 16, 100, 128)).astype(np.uint8)

        img = c_base.yz_image(x_index=1)
        assert img.size == (100, 16)

    def test_tile_xz(self):
        """Test getting an xy tile."""
        # Create base data
        c_base = ImageCube8([128, 100, 16], [0, 1])
        c_base.zeros()

        c_base.data = np.random.randint(1, 254, (1, 16, 100, 128)).astype(np.uint8)

        img = c_base.xz_image(y_index=1)
        assert img.size == (128, 16)


class TestImageCube16(unittest.TestCase):
    """Test the ImageCube16 Class parent class functionality"""
    cuboid_size = CUBOIDSIZE[0]
    cuboid_x_dim = cuboid_size[0]
    cuboid_y_dim = cuboid_size[1]
    cuboid_z_dim = cuboid_size[2]

    def test_constructor_no_dim_no_time(self):
        """Test the Cube class constructor"""
        c = ImageCube16()

        assert c.cube_size == [self.cuboid_z_dim, self.cuboid_y_dim, self.cuboid_x_dim]
        assert c.x_dim == self.cuboid_x_dim
        assert c.y_dim == self.cuboid_y_dim
        assert c.z_dim == self.cuboid_z_dim

        assert c.from_zeros() is False
        assert c.time_range == [0, 1]
        assert c.is_time_series is False

    def test_constructor_no_time(self):
        """Test the Cube class constructor"""
        c = ImageCube16([128, 64, 16])

        assert c.cube_size == [16, 64, 128]
        assert c.x_dim == 128
        assert c.y_dim == 64
        assert c.z_dim == 16

        assert c.from_zeros() is False
        assert c.time_range == [0, 1]
        assert c.is_time_series is False

    def test_constructor(self):
        """Test the Cube class constructor"""
        c = ImageCube16([128, 64, 16], [0, 10])

        assert c.cube_size == [16, 64, 128]
        assert c.x_dim == 128
        assert c.y_dim == 64
        assert c.z_dim == 16

        assert c.from_zeros() is False
        assert c.time_range == [0, 10]
        assert c.is_time_series is True
        assert c.data.dtype == np.uint16

    def test_zeros(self):
        """Test populating a default Cube instance with zeros"""
        c = ImageCube16()
        c.zeros()

        size = c.data.shape

        assert size == (1, self.cuboid_z_dim, self.cuboid_y_dim, self.cuboid_x_dim)
        assert c.from_zeros() is True
        assert c.data.size == self.cuboid_z_dim * self.cuboid_y_dim * self.cuboid_x_dim
        assert c.data.dtype == np.uint16
        assert c.data.sum() == 0

    def test_zeros_time_samples(self):
        """Test populating a default Cube instance with zeros"""
        c = ImageCube16([128, 64, 16], [2, 10])
        c.zeros()

        size = c.data.shape

        assert size == (8, 16, 64, 128)
        assert c.from_zeros() is True
        assert c.data.size == 8 * 16 * 64 * 128
        assert c.data.dtype == np.uint16
        assert c.data.sum() == 0

    def test_add_data_no_time(self):
        """Test adding data from a smaller cube to a bigger one"""

        c_base = ImageCube16([20, 15, 10])
        c_base.zeros()
        assert c_base.is_not_zeros() is False

        c_add = ImageCube16([5, 5, 5])
        c_add.zeros()
        c_add.data += 1
        assert c_add.is_not_zeros() is True

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

        assert c_base.data.dtype == np.uint16

    def test_overwrite_no_time(self):
        """Test overwriting data - for ImageCub8 this just does a copy."""
        c_base = ImageCube16([10, 20, 5])
        c_base.zeros()

        data = np.zeros((1, 5, 20, 10), np.uint16)
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
        c_base = ImageCube16([10, 20, 5], [0, 10])
        c_base.zeros()

        data = np.zeros((3, 5, 20, 10), np.uint16)
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
        c_base = ImageCube16([10, 20, 5], [0, 10])
        c_base.zeros()

        c_base.data[1, 1, 1, 1] = 1
        c_base.data[2, 4, 4, 1] = 2

        # Create overwrite data - all zero locations
        data = np.zeros((2, 5, 20, 10), np.uint16)
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
        c = ImageCube16([10, 20, 5])
        c.zeros()
        c.data += 1
        assert c.data.sum() == 10 * 20 * 5

        c.data[0, 2, 7, 5] = 5

        c.trim(5, 5, 7, 6, 2, 2)

        assert c.data[0, 0, 0, 0] == 5
        assert c.data.sum() == 5 * 6 * 2 + 4

    def test_trim(self):
        """Test trimming off part of a cube"""
        c = ImageCube16([10, 20, 5], [0, 4])
        c.zeros()
        c.data += 1
        assert c.data.sum() == 10 * 20 * 5 * 4

        c.data[0, 2, 7, 5] = 5
        c.data[1, 3, 10, 7] = 2

        c.trim(5, 5, 7, 6, 2, 2)

        assert c.data[0, 0, 0, 0] == 5
        assert c.data[0, 1, 3, 2] == 1
        assert c.data[1, 1, 3, 2] == 2
        assert c.data.sum() == 4 * 2 * 6 * 5 + 4 + 1

    def test_blosc_time_index_no_time(self):
        """Test blosc compression of Cube data"""

        c = ImageCube16([10, 20, 5])
        c.random()
        c2 = ImageCube16([10, 20, 5])

        byte_array = c.to_blosc_by_time_index()
        c2.from_blosc([byte_array])

        np.testing.assert_array_equal(c.data, c2.data)
        assert c.cube_size == c2.cube_size
        assert c.z_dim == c2.z_dim
        assert c.y_dim == c2.y_dim
        assert c.x_dim == c2.x_dim

    def test_blosc_time_index_specific_time(self):
        """Test blosc compression of Cube data"""
        c = ImageCube16([10, 20, 5], [0, 4])
        c.random()
        c2 = ImageCube16([10, 20, 5], [0, 4])

        byte_array = c.to_blosc_by_time_index(2)
        c2.from_blosc([byte_array])

        np.testing.assert_array_equal(np.expand_dims(c.data[2, :, :, :], axis=0), c2.data)
        assert c.cube_size == c2.cube_size
        assert c.z_dim == c2.z_dim
        assert c.y_dim == c2.y_dim
        assert c.x_dim == c2.x_dim

#    #def test_blosc_all_time_samples(self):
    #    """Test blosc compression of Cube data"""
#
    #    c = ImageCube16([10, 20, 5], [0, 4])
    #    c2 = ImageCube16([10, 20, 5], [0, 4])
    #    data = np.random.randint(0, 5000, size=[4, 5, 20, 10])
    #    c.data = data
#
    #    byte_array = [x for x in c.get_all_blosc_numpy_arrays()]
#
    #    # Unpack tuples
    #    time_list, byte_list = zip(*byte_array)
#
    #    c2.from_blosc(byte_list, [time_list[0], time_list[-1] + 1])
#
    #    np.testing.assert_array_equal(c.data, c2.data)
    #    assert c.cube_size == c2.cube_size
    #    assert c.z_dim == c2.z_dim
    #    assert c.y_dim == c2.y_dim
    #    assert c.x_dim == c2.x_dim
    #    assert c.time_range == c2.time_range
    #    assert c.is_time_series is True
    #    assert c2.is_time_series is True

    def test_blosc_all_time_samples_single_array(self):
        """Test blosc compression of Cube data"""

        c = ImageCube16([10, 20, 5], [0, 4])
        c.random()
        c2 = ImageCube16([10, 20, 5], [0, 4])
        #data = np.random.randint(0, 5000, size=[4, 5, 20, 10])
        #c.data = data

        byte_array = c.to_blosc()

        c2.from_blosc(byte_array, [0, 4])

        np.testing.assert_array_equal(c.data, c2.data)
        assert c.cube_size == c2.cube_size
        assert c.z_dim == c2.z_dim
        assert c.y_dim == c2.y_dim
        assert c.x_dim == c2.x_dim
        assert c.time_range == c2.time_range
        assert c.is_time_series is True
        assert c2.is_time_series is True

    def test_blosc_missing_time_step(self):
        """Test case when one of the cuboids at a time step is missing.  This
        happens when trying to load from the cache.  Only keys in the cache
        are passed to from_blosc().
        """
        EXTENTS = [10, 20, 5]
        cube_t0_1 = ImageCube16(EXTENTS, [0, 2])
        cube_t3 = ImageCube16(EXTENTS)
        exp_cube = ImageCube16(EXTENTS, [0, 4])
        exp_cube.zeros()
        exp_cube.overwrite(cube_t0_1.data, [0, 2])
        exp_cube.overwrite(cube_t3.data, [3, 4])

        cube_bytes_t0 = cube_t0_1.to_blosc_by_time_index(0)
        cube_bytes_t1 = cube_t0_1.to_blosc_by_time_index(1)
        cube_bytes_t3 = cube_t3.to_blosc_by_time_index(0)
        cube_list = (cube_bytes_t0, cube_bytes_t1, cube_bytes_t3)

        actual_cube = ImageCube16(EXTENTS, [0, 4])

        missing_time_step = [2]

        # Method under test.
        actual_cube.from_blosc(cube_list, [0, 4], missing_time_step)

        np.testing.assert_array_equal(exp_cube.data, actual_cube.data)
        assert actual_cube.is_time_series is True

    def test_overwrite_dtype_mismatch(self):
        c_base = ImageCube16([10, 10, 10])
        c_base.zeros()

        data = np.random.randint(0, 5000, size=[4, 5, 20, 10])

        # Make sure c_base empty
        assert c_base.data.sum() == 0

        # Insert c_add into c_base
        with self.assertRaises(SpdbError):
             c_base.overwrite(data)

    def test_factory_no_time(self):
        """Test the Cube factory in Cube"""

        data = get_image_dict()
        data['channel']['datatype'] = 'uint16'
        resource = BossResourceBasic(data)

        c = Cube.create_cube(resource, [30, 20, 13])
        assert isinstance(c, ImageCube16) is True
        assert c.cube_size == [13, 20, 30]
        assert c.is_time_series is False
        assert c.time_range == [0, 1]

    def test_factory(self):
        """Test the Cube factory in Cube"""
        data = get_image_dict()
        data['channel']['datatype'] = 'uint16'

        resource = BossResourceBasic(data)

        c = Cube.create_cube(resource, [30, 20, 13], [0, 15])
        assert isinstance(c, ImageCube16) is True
        assert c.cube_size == [13, 20, 30]
        assert c.is_time_series is True
        assert c.time_range == [0, 15]

    def test_tile_xy(self):
        """Test getting an xy tile."""
        # Create base data
        c_base = ImageCube16([128, 128, 16], [0, 1])
        c_base.zeros()

        c_base.data = np.random.randint(1, 60000, (1, 16, 128, 128)).astype(np.uint16)

        img = c_base.xy_image(z_index=1)
        assert img.size == (128, 128)


    def test_tile_yz(self):
        """Test getting an xy tile."""
        # Create base data
        c_base = ImageCube16([128, 100, 16], [0, 1])
        c_base.zeros()

        c_base.data = np.random.randint(1, 254, (1, 16, 100, 128)).astype(np.uint16)

        img = c_base.yz_image(x_index=1)
        assert img.size == (100, 16)

    def test_tile_xz(self):
        """Test getting an xy tile."""
        # Create base data
        c_base = ImageCube16([128, 100, 16], [0, 1])
        c_base.zeros()

        c_base.data = np.random.randint(1, 254, (1, 16, 100, 128)).astype(np.uint16)

        img = c_base.xz_image(y_index=1)
        assert img.size == (128, 16)
