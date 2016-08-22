# Copyright 2014 NeuroData (http://neurodata.io)
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

import numpy as np
from PIL import Image

from .cube import Cube

from spdb.c_lib import ndlib

from .error import SpdbError, ErrorCodes


class AnnotateCube64(Cube):
    def __init__(self, cube_size=None, time_range=None):
        """Create empty array of cube_size"""

        if not cube_size:
            cube_size = [64, 64, 64]

        # call the base class constructor
        Cube.__init__(self, cube_size, time_range)

        # Note that this is self.cube_size (which is transposed) in Cube
        self.data = np.zeros([self.time_range[1] - self.time_range[0]] + self.cube_size, dtype=np.uint64, order='C')

        # variable that describes when a cube is created from zeros rather than loaded from another source
        self._created_from_zeros = False

    # create an all zeros cube
    def zeros(self):
        """Create a cube of all 0"""
        self._created_from_zeros = True
        self.data = np.zeros([self.time_range[1]-self.time_range[0]] + self.cube_size, dtype=np.uint64, order='C')

    def overwrite(self, input_data, time_sample_range=None):
        """ Overwrite data with all non-zero values in the input_data

        Function is accelerated via ctypes lib.

        If time_sample_range is provided, data will be inserted at the appropriate time sample

        Args:
            input_data (numpy.ndarray): Input data matrix to overwrite the current Cube data
            time_sample_range list(int): The min and max time samples that input_data represents in python convention
            (start inclusive, stop exclusive)

        Returns:
            None

        """
        if self.data.dtype != input_data.dtype:
            raise SpdbError("Conflicting data types for overwrite.",
                            ErrorCodes.DATATYPE_MISMATCH)

        if not time_sample_range:
            # If no time sample range provided use default of 0
            time_sample_range = [0, 1]

        if input_data.ndim == 4:
            for t in range(*time_sample_range):
                self.data[t, :, :, :] = ndlib.overwriteDense64_ctype(
                    self.data[t, :, :, :], input_data[t - time_sample_range[0], :, :, :])
        else:
            # Input data doesn't have any time indices
            self.data[time_sample_range[0], :, :, :] = ndlib.overwriteDense64_ctype(
                self.data[time_sample_range[0], :, :, :], input_data[time_sample_range[0], :, :, :])

    def xy_image(self, z_index=0, t_index=0):
        """Render an image in the XY plane.

        Args:
            z_index: Optional Z index into the data matrix from which to render the image.
            t_index: Optional time sample index into the data matrix from which to render the image.


        Returns:
            Image
        """
        _, zdim, ydim, xdim = self.data.shape
        imagemap = np.zeros([ydim, xdim], dtype=np.uint64)

        # false color redrawing of the region
        ndlib.recolor_ctype(self.data[t_index, z_index, :, :].reshape((ydim, xdim)), imagemap)

        return Image.frombuffer('RGBA', (xdim, ydim), imagemap.astype(dtype=np.uint32), 'raw', 'RGBA', 0, 1)

    def xz_image(self, z_scale=1, y_index=0, t_index=0):
        """Render an image in the xz plane.

        Args:
            z_scale: Scaling factor for the z-dimension. Useful for rendering non-isotropic data
            y_index: Optional Y index into the data matrix from which to render the image.
            t_index: Optional time sample index into the data matrix from which to render the image.

        Returns:
            Image
        """
        _, zdim, ydim, xdim = self.data.shape
        imagemap = np.zeros([zdim, xdim], dtype=np.uint64)

        # false color redrawing of the region
        ndlib.recolor_ctype(self.data[t_index, :, y_index, :].reshape((zdim, xdim)), imagemap)

        outimage = Image.frombuffer('RGBA', (xdim, zdim), imagemap.astype(dtype=np.uint32), 'raw', 'RGBA', 0, 1)
        return outimage.resize([xdim, int(zdim * z_scale)])

    def yz_image(self, z_scale=1, x_index=0, t_index=0):
        """Render an image in the yz plane.

        Args:
            z_scale: Scaling factor for the z-dimension. Useful for rendering non-isotropic data
            x_index: Optional X index into the data matrix from which to render the image.
            t_index: Optional time sample index into the data matrix from which to render the image.

        Returns:
            Image
        """
        _, zdim, ydim, xdim = self.data.shape
        imagemap = np.zeros([zdim, ydim], dtype=np.uint64)

        # false color redrawing of the region
        ndlib.recolor_ctype(self.data[t_index, :, :, x_index].reshape((zdim, ydim)), imagemap)

        outimage = Image.frombuffer('RGBA', (ydim, zdim), imagemap.astype(dtype=np.uint32), 'raw', 'RGBA', 0, 1)
        return outimage.resize([ydim, int(zdim * z_scale)])

    # TODO: Implement zoom in/zoom out once propagation is implemented
