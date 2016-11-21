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

from spdb.spatialdb.region import Region
import unittest

class TestRegion(unittest.TestCase):
    def test_get_cuboid_aligned_sub_region_cuboid_aligned(self):
        """Region already cuboid aligned case."""
        resolution = 0
        corner = (512, 1024, 32)
        extent = (1024, 512, 32)
        expected = {
            'x_cuboids': range(1, 4),
            'y_cuboids': range(2, 4),
            'z_cuboids': range(2, 5),
        }
        actual = Region.get_cuboid_aligned_sub_region(resolution, corner, extent)

        self.assertEqual(expected, actual)

    def test_get_cuboid_aligned_sub_region_x_not_cuboid_aligned(self):
        """Region not cuboid aligned along x axis."""
        resolution = 0
        corner = (511, 1024, 32)
        extent = (1026, 512, 32)
        expected = {
            'x_cuboids': range(1, 4),
            'y_cuboids': range(2, 4),
            'z_cuboids': range(2, 5),
        }
        actual = Region.get_cuboid_aligned_sub_region(resolution, corner, extent)

        self.assertEqual(expected, actual)

    def test_get_cuboid_aligned_sub_region_y_not_cuboid_aligned(self):
        """Region not cuboid aligned along y axis."""
        resolution = 0
        corner = (512, 1023, 32)
        extent = (1024, 514, 32)
        expected = {
            'x_cuboids': range(1, 4),
            'y_cuboids': range(2, 4),
            'z_cuboids': range(2, 5),
        }
        actual = Region.get_cuboid_aligned_sub_region(resolution, corner, extent)

        self.assertEqual(expected, actual)

    def test_get_cuboid_aligned_sub_region_z_not_cuboid_aligned(self):
        """Region not cuboid aligned along z axis."""
        resolution = 0
        corner = (512, 1024, 15)
        extent = (1024, 512, 18)
        expected = {
            'x_cuboids': range(1, 4),
            'y_cuboids': range(2, 4),
            'z_cuboids': range(1, 3),
        }
        actual = Region.get_cuboid_aligned_sub_region(resolution, corner, extent)

        self.assertEqual(expected, actual)

if __name__ == '__main__':
    unittest.main()
