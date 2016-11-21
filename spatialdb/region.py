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

from spdb.c_lib import ndlib
from spdb.c_lib.ndtype import CUBOIDSIZE

from bossutils.logger import BossLogger

class Region:
    """
    Class that helps calculate cuboid aligned and non-cuboid aligned
    sub-regions.
    """

    @classmethod
    def get_cuboid_aligned_sub_region(cls, resolution, corner, extent):
        """
        Given a region, return the sub-region that spans entire cuboids.

        The sub-region returned fills entire cuboids.  The edges of the
        original region may not fill entire cuboids.

        Args:
            resolution (int): Resolution level.
            corner ((int, int, int)): xyz location of the corner of the region.
            extent ((int, int, int)): xyz extents of the region (equivalent to size).

        Returns:
            (dict): { 'x_cuboids': range(), 'y_cuboids': range(), 'z_cuboids': range() }
        """
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]

        x_start_cube = Region._get_first_cuboid(corner[0], extent[0], x_cube_dim)
        y_start_cube = Region._get_first_cuboid(corner[1], extent[1], y_cube_dim)
        z_start_cube = Region._get_first_cuboid(corner[2], extent[2], z_cube_dim)

        x_end_cube = Region._get_last_cuboid(corner[0], extent[0], x_cube_dim)
        y_end_cube = Region._get_last_cuboid(corner[1], extent[1], y_cube_dim)
        z_end_cube = Region._get_last_cuboid(corner[2], extent[2], z_cube_dim)

        return {
            'x_cuboids': range(x_start_cube, x_end_cube),
            'y_cuboids': range(y_start_cube, y_end_cube),
            'z_cuboids': range(z_start_cube, z_end_cube)
        }

    @classmethod
    def _get_first_cuboid(cls, start, extent, cube_dim):
        """
        Get the index of the first full cuboid within start and extent.

        Args:
            start (int): Starting coordinate along the single axis.
            extent (int): Size along the single axis.
            cube_dim (int):  Size of a cuboid along the single axis.

        Returns:
            (int): Index of cuboid.
        """
        if start % cube_dim != 0:
            # Corner not on cuboid boundary so start at next cuboid boundary.
            c_start = (1+(start // cube_dim)) * cube_dim
        else:
            c_start = start

        return c_start // cube_dim

    @classmethod
    def _get_last_cuboid(cls, start, extent, cube_dim):
        """
        Get the index of the last cuboid fully contained by start and extent.

        Args:
            start (int): Starting coordinate along the single axis.
            extent (int): Size along the single axis.
            cube_dim (int):  Size of a cuboid along the single axis.

        Returns:
            (int): Index of cuboid such that it can be used as the 2nd argument to range().
        """
        end = start + extent
        end_cube = end // cube_dim + 1
        if end % cube_dim != 0:
            # End not on cuboid boundary so start at previous cuboid boundary.
            end = (end // cube_dim) * cube_dim
            _end_cube = end // cube_dim + 1
            if end < start + cube_dim:
                # Less than a cuboid's worth of data on this axis.
                end_cube -= 1

        return end_cube

    @classmethod
    def get_sub_region_x_y_block_near_side(cls, resolution, corner, extent):
        """
        Get the non-cuboid aligned sub-region in the x-y plane closest to the origin.

        Args:
            resolution (int): Resolution level.
            corner ((int, int, int)): xyz location of the corner of the region.
            extent ((int, int, int)): xyz extents of the region (equivalent to size).

        """
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]

        if corner[2] % z_cube_dim == 0:
            # No sub-region, already cuboid aligned on the near side.
            return { 'corner': corner, 'extent': (extent[0], extent[1], 0) }

        # Set at boundary of next cuboid along the z axis.
        z_end = (1+(corner[2] // z_cube_dim)) * z_cube_dim

        # Make sure next cuboid doesn't exceed extents of region.
        z_end = min(z_end, corner[2] + extent[2])

        return {
            'corner': corner,
            'extent': (extent[0], extent[1], z_end - corner[2])
        }

    @classmethod
    def get_sub_region_x_y_block_far_side(cls, resolution, corner, extent):
        """
        Get the non-cuboid aligned sub-region in the x-y plane farthest to the origin.

        Args:
            resolution (int): Resolution level.
            corner ((int, int, int)): xyz location of the corner of the region.
            extent ((int, int, int)): xyz extents of the region.

        """
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]

        # Set to the boundary of last full cuboid along the z axis.
        z_start = corner[2] + extent[2] - 1
        if z_start % z_cube_dim != 0:
            # End not on cuboid boundary so start at previous cuboid boundary.
            z_start = (z_start // z_cube_dim) * z_cube_dim
            if z_start < corner[2]:
                # This region is smaller than a cuboid so there is no far side.
                return {
                    'corner': (corner[0], corner[1], z_start),
                    'extent': (extent[0], extent[1], 0)
                }

        return {
            'corner': (corner[0], corner[1], z_start),
            'extent': (extent[0], extent[1], corner[2] + extent[2] - z_start - 1)
        }

