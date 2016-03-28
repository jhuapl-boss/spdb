import unittest

from spdb.spdb.imagecube import ImageCube8, ImageCube16
from spdb.spdb.cube import Cube
from spdb.project import BossResourceBasic
import numpy as np


class TestImageCube8(unittest.TestCase):
    """Test the ImageCube8 Class parent class functionality"""

    def test_constructor(self):
        """Test the Cube class constructor"""
        c = ImageCube8()

        assert c.cube_size == [64, 64, 64]
        assert c.x_dim == 64
        assert c.y_dim == 64
        assert c.z_dim == 64

        c = ImageCube8([10, 12, 5])

        assert c.cube_size == [5, 12, 10]
        assert c.x_dim == 10
        assert c.y_dim == 12
        assert c.z_dim == 5

        assert c.from_zeros() == False

    def test_zeros(self):
        """Test populating a Cube instance with zeros"""
        c = ImageCube8()
        c.zeros()

        assert c.from_zeros() == True
        assert c.data.size == 64 * 64 * 64
        assert c.data.dtype == "uint8"

    def test_add_data(self):
        """Test adding data from a smaller cube to a bigger one"""

        c_base = ImageCube8([10, 10, 10])
        c_base.zeros()
        assert c_base.is_not_zeros() == False

        c_add = ImageCube8([5, 5, 5])
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
        assert c_base.data[1, 2, 3] == 1
        assert c_base.data[4, 4, 4] == 1
        assert c_base.data[0, 0, 0] == 1
        assert c_base.data[4, 4, 6] == 0
        assert c_base.data[6, 4, 4] == 0
        assert c_base.data[4, 6, 4] == 0

        # Try an offset in x insert
        c_base.zeros()
        c_base.add_data(c_add, [1, 0, 0])
        # Make sure insertion happened
        assert c_base.data.sum() == 5 * 5 * 5

        # Make sure it was in the right spot (remember data is still stored in zyx under the hood)
        assert c_base.data[1, 1, 1] == 0
        assert c_base.data[4, 4, 6] == 1

        # Try an offset in y insert
        c_base.zeros()
        c_base.add_data(c_add, [0, 1, 0])
        # Make sure insertion happened
        assert c_base.data.sum() == 5 * 5 * 5

        # Make sure it was in the right spot (remember data is still stored in zyx under the hood)
        assert c_base.data[1, 1, 1] == 0
        assert c_base.data[4, 6, 4] == 1

        # Try an offset in z insert
        c_base.zeros()
        c_base.add_data(c_add, [0, 0, 1])
        # Make sure insertion happened
        assert c_base.data.sum() == 5 * 5 * 5

        # Make sure it was in the right spot (remember data is still stored in zyx under the hood)
        assert c_base.data[1, 1, 1] == 0
        assert c_base.data[6, 4, 4] == 1

    def test_trim(self):
        """Test trimming off part of a cube"""
        c = ImageCube8([10, 20, 5])
        c.zeros()
        c.data += 1
        assert c.data.sum() == 10*20*5

        c.data[2, 7, 5] = 5

        c.trim(5, 5, 7, 6, 2, 2)

        assert c.data[0, 0, 0] == 5
        assert c.data.sum() == 5 * 6 * 2 + 4

    def test_blosc(self):
        """Test blosc compression of Cube data"""

        c = ImageCube8([10, 20, 5])
        c2 = ImageCube8([10, 20, 5])
        data = np.random.randint(50, size=[10, 20, 5])
        c.data = data

        byte_array = c.to_blosc_numpy()
        c2.from_blosc_numpy(byte_array)

        np.testing.assert_array_equal(c.data, c2.data)
        assert c.cube_size == c2.cube_size
        assert c.z_dim == c2.z_dim
        assert c.y_dim == c2.y_dim
        assert c.x_dim == c2.x_dim

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
            data['channel_layer']['name'] = "ch1"
            data['channel_layer']['description'] = "Test channel 1"
            data['channel_layer']['is_channel'] = True
            data['channel_layer']['datatype'] = 'uint8'
            data['channel_layer']['max_time_step'] = 0

            resource = BossResourceBasic(data)

            c = Cube.create_cube(resource, [30, 20, 13])
            assert isinstance(c, ImageCube8) == True
            assert c.cube_size == [13, 20, 30]


class TestImageCube16(unittest.TestCase):
    """Test the ImageCube16 Class parent class functionality"""

    def test_constructor(self):
        """Test the Cube class constructor"""
        c = ImageCube16()

        assert c.cube_size == [64, 64, 64]
        assert c.x_dim == 64
        assert c.y_dim == 64
        assert c.z_dim == 64

        c = ImageCube16([10, 12, 5])

        assert c.cube_size == [5, 12, 10]
        assert c.x_dim == 10
        assert c.y_dim == 12
        assert c.z_dim == 5

        assert c.from_zeros() == False

    def test_zeros(self):
        """Test populating a Cube instance with zeros"""
        c = ImageCube16()
        c.zeros()

        assert c.from_zeros() == True
        assert c.data.size == 64 * 64 * 64
        assert c.data.dtype == "uint16"

    def test_add_data(self):
        """Test adding data from a smaller cube to a bigger one"""

        c_base = ImageCube16([10, 10, 10])
        c_base.zeros()
        assert c_base.is_not_zeros() == False

        c_add = ImageCube16([5, 5, 5])
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
        assert c_base.data[1, 2, 3] == 1
        assert c_base.data[4, 4, 4] == 1
        assert c_base.data[0, 0, 0] == 1
        assert c_base.data[4, 4, 6] == 0
        assert c_base.data[6, 4, 4] == 0
        assert c_base.data[4, 6, 4] == 0

        # Try an offset in x insert
        c_base.zeros()
        c_base.add_data(c_add, [1, 0, 0])
        # Make sure insertion happened
        assert c_base.data.sum() == 5 * 5 * 5

        # Make sure it was in the right spot (remember data is still stored in zyx under the hood)
        assert c_base.data[1, 1, 1] == 0
        assert c_base.data[4, 4, 6] == 1

        # Try an offset in y insert
        c_base.zeros()
        c_base.add_data(c_add, [0, 1, 0])
        # Make sure insertion happened
        assert c_base.data.sum() == 5 * 5 * 5

        # Make sure it was in the right spot (remember data is still stored in zyx under the hood)
        assert c_base.data[1, 1, 1] == 0
        assert c_base.data[4, 6, 4] == 1

        # Try an offset in z insert
        c_base.zeros()
        c_base.add_data(c_add, [0, 0, 1])
        # Make sure insertion happened
        assert c_base.data.sum() == 5 * 5 * 5

        # Make sure it was in the right spot (remember data is still stored in zyx under the hood)
        assert c_base.data[1, 1, 1] == 0
        assert c_base.data[6, 4, 4] == 1

    def test_trim(self):
        """Test trimming off part of a cube"""
        c = ImageCube16([10, 20, 5])
        c.zeros()
        c.data += 1
        assert c.data.sum() == 10*20*5

        c.data[2, 7, 5] = 5

        c.trim(5, 5, 7, 6, 2, 2)

        assert c.data[0, 0, 0] == 5
        assert c.data.sum() == 5 * 6 * 2 + 4

    def test_blosc(self):
        """Test blosc compression of Cube data"""

        c = ImageCube16([10, 20, 5])
        c2 = ImageCube16([10, 20, 5])
        data = np.random.randint(50, size=[10, 20, 5])
        c.data = data

        byte_array = c.to_blosc_numpy()
        c2.from_blosc_numpy(byte_array)

        np.testing.assert_array_equal(c.data, c2.data)
        assert c.cube_size == c2.cube_size
        assert c.z_dim == c2.z_dim
        assert c.y_dim == c2.y_dim
        assert c.x_dim == c2.x_dim

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
            data['channel_layer']['name'] = "ch1"
            data['channel_layer']['description'] = "Test channel 1"
            data['channel_layer']['is_channel'] = True
            data['channel_layer']['datatype'] = 'uint16'
            data['channel_layer']['max_time_step'] = 0

            resource = BossResourceBasic(data)

            c = Cube.create_cube(resource, [30, 20, 13])
            assert isinstance(c, ImageCube16) == True
            assert c.cube_size == [13, 20, 30]

    # TODO This test is for annotation data only...move
    #def test_overwrite(self):
    #    """Test overwriting non-zero values. Uses C acceleration"""
    #    c = ImageCube8([10, 20, 5])
    #    c.zeros()
    #    c.data[2, 5, 8] = 2
    #    c.data[4, 14, 0] = 3
#
    #    assert c.data.sum() == 5
#
    #    data = np.zeros([10, 20, 5], dtype=np.uint8)
    #    data[1, 1, 1] = 2
#
    #    c.overwrite(data)
#
    #    assert c.data.sum() == 7
    #    assert c.data[1, 1, 1] == 2
#
    #    data[4, 14, 0] = 10
    #    c.overwrite(data)
#
    #    assert c.data.sum() == 7
    #    assert c.data[1, 1, 1] == 2
    #    assert c.data[4, 14, 0] == 10
    #    assert c.data.sum() == 14

    # TODO: Add image rendering tests


