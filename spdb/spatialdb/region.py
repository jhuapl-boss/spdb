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

from collections import namedtuple
from spdb.c_lib import ndlib
from spdb.c_lib.ndtype import CUBOIDSIZE

class Region:
    """
    Class that helps calculate cuboid aligned and non-cuboid aligned
    sub-regions.

    See ./diagrams/ids_in_region.png for an illustration of what is calculated.
    Note that the diagram only shows what happens in the x and y dimensions.
    """


    """
    Return type from get_cuboid_aligned_sub_region().

    Attributes:
        x_cuboids (range): Indices of x cuboids in sub-region.
        y_cuboids (range): Indices of y cuboids in sub-region.
        z_cuboids (range): Indices of z cuboids in sub-region.
    """
    Cuboids = namedtuple('Cuboids', 'x_cuboids, y_cuboids, z_cuboids')

    """
    Return type from get_sub_region_*() methods.

    Attributes:
        corner (tuple): x, y, z coords of corner.
        extents (tuple): Number of elements in the x, y, z directions.
    """
    Bounds = namedtuple('Bounds', 'corner, extent')

    @classmethod
    def get_cuboid_aligned_sub_region(cls, resolution, corner, extent):
        """
        Given a region, return the sub-region that spans entire cuboids.

        The sub-region returned fills entire cuboids.  The edges of the
        original region may not fill entire cuboids.  The sub-region returned
        is represented by ranges of cuboid indices in x, y, and z.

        Args:
            resolution (int): Resolution level.
            corner ((int, int, int)): xyz location of the corner of the region.
            extent ((int, int, int)): xyz extents of the region (equivalent to size).

        Returns:
            (Region.Cuboids): ranges of cuboid indices in the x, y, z dimensions.
        """
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]

        x_start_cube = Region._get_first_cuboid(corner[0], extent[0], x_cube_dim)
        y_start_cube = Region._get_first_cuboid(corner[1], extent[1], y_cube_dim)
        z_start_cube = Region._get_first_cuboid(corner[2], extent[2], z_cube_dim)

        x_end_cube = Region._get_last_cuboid(corner[0], extent[0], x_cube_dim)
        y_end_cube = Region._get_last_cuboid(corner[1], extent[1], y_cube_dim)
        z_end_cube = Region._get_last_cuboid(corner[2], extent[2], z_cube_dim)

        return Region.Cuboids(
            x_cuboids=range(x_start_cube, x_end_cube),
            y_cuboids=range(y_start_cube, y_end_cube),
            z_cuboids=range(z_start_cube, z_end_cube)
        )

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
        Get the index+1 of the last cuboid fully contained by start and extent.

        Returns index + 1 because this is used as part of a range().

        Args:
            start (int): Starting coordinate along the single axis.
            extent (int): Size along the single axis.
            cube_dim (int):  Size of a cuboid along the single axis.

        Returns:
            (int): Index of cuboid such that it can be used as the 2nd argument to range().
        """
        end = start + extent
        end_cube = end // cube_dim
        if end % cube_dim != 0:
            # End not on cuboid boundary so start at previous cuboid boundary.
            end = (end // cube_dim) * cube_dim
            if end < start + cube_dim:
                # Less than a cuboid's worth of data on this axis.
                end_cube -= 1

        return end_cube

    @classmethod
    def get_all_partial_sub_regions(cls, resolution, corner, extent):
        """

        Args:
            resolution (int): Resolution level.
            corner ((int, int, int)): xyz location of the corner of the region.
            extent ((int, int, int)): xyz extents of the region (equivalent to size).

        Returns:
            (list[dict]): List of dicts with keys 'corner' and 'extent' in this order:
                x_y near side,
                x_y far side,
                x_z near side,
                x_z far side,
                y_z near side,
                y_z far side
        """

        # Identify non-cuboid aligned sub-region in x-y plane closest to origin.
        near_x_y_region = Region.get_sub_region_x_y_block_near_side(
            resolution, corner, extent)

        # Identify non-cuboid aligned sub-region in x-y plane farthest from
        # origin.
        far_x_y_region = Region.get_sub_region_x_y_block_far_side(
            resolution, corner, extent)

        # Identify non-cuboid aligned sub-region in x-z plane closest to origin
        # (but cuboid aligned in the x-y plane).
        near_x_z_region = Region.get_sub_region_x_z_block_near_side(
            resolution, corner, extent)

        # Identify non-cuboid aligned sub-region in x-z plane farthest from
        # origin (but cuboid aligned in the x-y plane).
        far_x_z_region = Region.get_sub_region_x_z_block_far_side(
            resolution, corner, extent)

        # Identify non-cuboid aligned sub-region in y-z plane closest to origin
        # (but cuboid aligned in the x-y and x-z planes).
        near_y_z_region = Region.get_sub_region_y_z_block_near_side(
            resolution, corner, extent)

        # Identify non-cuboid aligned sub-region in y-z plane farthest from
        # origin (but cuboid aligned in the x-y and x-z planes).
        far_y_z_region = Region.get_sub_region_y_z_block_far_side(
            resolution, corner, extent)

        return [
            near_x_y_region, far_x_y_region,
            near_x_z_region, far_x_z_region,
            near_y_z_region, far_y_z_region
        ]


    @classmethod
    def get_sub_region_x_y_block_near_side(cls, resolution, corner, extent):
        """
        Get the non-cuboid aligned sub-region in the x-y plane closest to the origin.

        Args:
            resolution (int): Resolution level.
            corner ((int, int, int)): xyz location of the corner of the region.
            extent ((int, int, int)): xyz extents of the region (equivalent to size).

        Returns:
            (Region.Bounds): Corner and extent of sub region.
        """
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]

        if corner[2] % z_cube_dim == 0 and extent[2] >= z_cube_dim:
            # No sub-region, already cuboid aligned on the near side.
            return Region.Bounds(
                corner=corner, extent=(extent[0], extent[1], 0))

        # Set at boundary of next cuboid along the z axis.
        z_end = (1+(corner[2] // z_cube_dim)) * z_cube_dim

        if z_end + z_cube_dim > corner[2] + extent[2]:
            # Don't have a full cuboid, so include entire region along this axis.
            z_end = corner[2] + extent[2]
        else:
            # Make sure setting edge at next cuboid boundary doesn't exceed
            # extents of region.
            z_end = min(z_end, corner[2] + extent[2])

        return Region.Bounds(
            corner=corner,
            extent=(extent[0], extent[1], z_end - corner[2])
        )

    @classmethod
    def get_sub_region_x_y_block_far_side(cls, resolution, corner, extent):
        """
        Get the non-cuboid aligned sub-region in the x-y plane farthest to the origin.

        Args:
            resolution (int): Resolution level.
            corner ((int, int, int)): xyz location of the corner of the region.
            extent ((int, int, int)): xyz extents of the region.

        Returns:
            (Region.Bounds): Corner and extent of sub region.
        """
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]

        # Set to the boundary of last full cuboid along the z axis.
        z_start = corner[2] + extent[2]
        z_extent = 0
        if z_start % z_cube_dim != 0:
            # End not on cuboid boundary so start at previous cuboid boundary.
            z_start = (z_start // z_cube_dim) * z_cube_dim
            if z_start > corner[2]:
                z_extent = corner[2] + extent[2] - z_start

        return Region.Bounds(
            corner=(corner[0], corner[1], z_start),
            extent=(extent[0], extent[1], z_extent)
        )

    @classmethod
    def get_sub_region_x_z_block_near_side(cls, resolution, corner, extent):
        """
        Get the non-cuboid aligned sub-region in the x-z plane closest to the origin.

        Args:
            resolution (int): Resolution level.
            corner ((int, int, int)): xyz location of the corner of the region.
            extent ((int, int, int)): xyz extents of the region (equivalent to size).

        Returns:
            (Region.Bounds): Corner and extent of sub region.
        """
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]

        if corner[1] % y_cube_dim == 0 and extent[1] >= y_cube_dim:
            # No sub-region, already cuboid aligned on the near side.
            return Region.Bounds(
                corner=corner, extent=(extent[0], 0, extent[2]))

        # Set at boundary of next cuboid along the y axis.
        y_end = (1+(corner[1] // y_cube_dim)) * y_cube_dim

        if y_end + y_cube_dim > corner[1] + extent[1]:
            # Don't have a full cuboid, so include entire region along this axis.
            y_end = corner[1] + extent[1]
        else:
            # Make sure setting edge at next cuboid boundary doesn't exceed
            # extents of region.
            y_end = min(y_end, corner[1] + extent[1])

        return Region.Bounds(
            corner=corner,
            extent=(extent[0], y_end - corner[1], extent[2])
        )

    @classmethod
    def get_sub_region_x_z_block_far_side(cls, resolution, corner, extent):
        """
        Get the non-cuboid aligned sub-region in the x-z plane farthest to the origin.

        Args:
            resolution (int): Resolution level.
            corner ((int, int, int)): xyz location of the corner of the region.
            extent ((int, int, int)): xyz extents of the region.

        Returns:
            (Region.Bounds): Corner and extent of sub region.
        """
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]

        # Set to the boundary of last full cuboid along the y axis.
        y_start = corner[1] + extent[1]
        y_extent = 0
        if y_start % y_cube_dim != 0:
            # End not on cuboid boundary so start at previous cuboid boundary.
            y_start = (y_start // y_cube_dim) * y_cube_dim
            if y_start > corner[1]:
                y_extent = corner[1] + extent[1] - y_start

        return Region.Bounds(
            corner=(corner[0], y_start, corner[2]),
            extent=(extent[0], y_extent, extent[2])
        )

    @classmethod
    def get_sub_region_y_z_block_near_side(cls, resolution, corner, extent):
        """
        Get the non-cuboid aligned sub-region in the y-z plane closest to the origin.

        Args:
            resolution (int): Resolution level.
            corner ((int, int, int)): xyz location of the corner of the region.
            extent ((int, int, int)): xyz extents of the region (equivalent to size).

        Returns:
            (Region.Bounds): Corner and extent of sub region.
        """
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]

        if corner[0] % x_cube_dim == 0 and extent[0] >= x_cube_dim:
            # No sub-region, already cuboid aligned on the near side.
            return Region.Bounds(
                corner=corner, extent=(0, extent[1], extent[2]))

        # Set at boundary of next cuboid along the x axis.
        x_end = (1+(corner[0] // x_cube_dim)) * x_cube_dim

        if x_end + x_cube_dim > corner[0] + extent[0]:
            # Don't have a full cuboid, so include entire region along this axis.
            x_end = corner[0] + extent[0]
        else:
            # Make sure setting edge at next cuboid boundary doesn't exceed
            # extents of region.
            x_end = min(x_end, corner[0] + extent[0])

        return Region.Bounds(
            corner=corner,
            extent=(x_end - corner[0], extent[1], extent[2])
        )

    @classmethod
    def get_sub_region_y_z_block_far_side(cls, resolution, corner, extent):
        """
        Get the non-cuboid aligned sub-region in the y-z plane farthest to the origin.

        Args:
            resolution (int): Resolution level.
            corner ((int, int, int)): xyz location of the corner of the region.
            extent ((int, int, int)): xyz extents of the region.

        Returns:
            (Region.Bounds): Corner and extent of sub region.
        """
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]

        # Set to the boundary of last full cuboid along the x axis.
        x_start = corner[0] + extent[0]
        x_extent = 0
        if x_start % x_cube_dim != 0:
            # End not on cuboid boundary so start at previous cuboid boundary.
            x_start = (x_start // x_cube_dim) * x_cube_dim
            if x_start > corner[0]:
                x_extent = corner[0] + extent[0] - x_start

        return Region.Bounds(
            corner=(x_start, corner[1], corner[2]),
            extent=(x_extent, extent[1], extent[2])
        )
