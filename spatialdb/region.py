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
    def get_cuboid_aligned_sub_region(self, resolution, corner, extent):
        """
        Given a region, return the sub-region that spans entire cuboids.

        The sub-region returned fills entire cuboids.  The edges of the
        original region may not fill entire cuboids.

        Args:
            resolution (int): Resolution level.
            corner ((int, int, int)): xyz location of the corner of the region.
            extent ((int, int, int)): xyz extents of the region.

        Returns:
            (dict): { 'x_cuboids': range(), 'y_cuboids': range(), 'z_cuboids': range() }
        """
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]
        if corner[0] % x_cube_dim != 0:
            # Corner not on cuboid boundary so start at next cuboid boundary.
            x_start = (1+(corner[0] // x_cube_dim)) * x_cube_dim
        else:
            x_start = corner[0]

        if corner[1] % y_cube_dim != 0:
            # Corner not on cuboid boundary so start at next cuboid boundary.
            y_start = (1+(corner[1] // y_cube_dim)) * y_cube_dim
        else:
            y_start = corner[1]

        if corner[2] % z_cube_dim != 0:
            # Corner not on cuboid boundary so start at next cuboid boundary.
            z_start = (1+(corner[2] // z_cube_dim)) * z_cube_dim
        else:
            z_start = corner[2]


        x_end = corner[0] + extent[0]
        if x_end % x_cube_dim != 0:
            # End not on cuboid boundary so start at previous cuboid boundary.
            x_end = (x_end // x_cube_dim) * x_cube_dim
            x_end = max(0, x_end)

        y_end = corner[1] + extent[1]
        if y_end % y_cube_dim != 0:
            # End not on cuboid boundary so start at previous cuboid boundary.
            y_end = (y_end // y_cube_dim) * y_cube_dim
            y_end = max(0, y_end)

        z_end = corner[2] + extent[2]
        if z_end % z_cube_dim != 0:
            # End not on cuboid boundary so start at previous cuboid boundary.
            z_end = (z_end // z_cube_dim) * z_cube_dim
            z_end = max(0, z_end)

        return {
            'x_cuboids': range(x_start // x_cube_dim, x_end // x_cube_dim + 1),
            'y_cuboids': range(y_start // y_cube_dim, y_end // y_cube_dim + 1),
            'z_cuboids': range(z_start // z_cube_dim, (z_end // z_cube_dim) + 1)
        }

