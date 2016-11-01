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
from spdb.c_lib.ndtype import CUBOIDSIZE
from .error import SpdbError, ErrorCodes


class ImageCube8(Cube):
    def __init__(self, cube_size=None, time_range=None):
        """Create empty array of cube_size"""

        if not cube_size:
            cube_size = CUBOIDSIZE[0]

        # call the base class constructor
        Cube.__init__(self, cube_size, time_range)

        # Note that this is self.cube_size (which is transposed) in Cube
        self.data = np.zeros([self.time_range[1]-self.time_range[0]] + self.cube_size, dtype=np.uint8, order='C')

        # variable that describes when a cube is created from zeros rather than loaded from another source
        self._created_from_zeros = False

        self.datatype = np.uint8

    def zeros(self):
        """Create a cube of all zeros

        Returns:
            None
        """
        self._created_from_zeros = True
        self.data = np.zeros([self.time_range[1]-self.time_range[0]] + self.cube_size, dtype=np.uint8, order='C')

    def random(self):
        """Create a random cube

        Returns:
            None
        """
        self.data = np.random.randint(1, 255,
                                      size=[self.time_range[1]-self.time_range[0]] + self.cube_size,
                                      dtype=np.uint8)

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
                self.data[t, :, :, :] = ndlib.overwriteDense8_ctype(
                    self.data[t, :, :, :], input_data[t - time_sample_range[0], :, :, :])
        else:
            # Input data doesn't have any time indices
            self.data[time_sample_range[0], :, :, :] = ndlib.overwriteDense8_ctype(
                self.data[time_sample_range[0], :, :, :], input_data[time_sample_range[0], :, :, :])

    def xy_image(self, z_index=0, t_index=0):
        """Render an image in the XY plane.

        Args:
            z_index: Optional Z index into the data matrix from which to render the image.
            t_index: Optional time sample index into the data matrix from which to render the image.

        Returns:
            Image
        """

        time, z_dim, y_dim, x_dim = self.data.shape
        return Image.frombuffer('L', (x_dim, y_dim), self.data[t_index, z_index, :, :].flatten(), 'raw', 'L', 0, 1)

    def xz_image(self, z_scale=1, y_index=0, t_index=0):
        """Render an image in the xz plane.

        Args:
            z_scale: Scaling factor for the z-dimension. Useful for rendering non-isotropic data
            y_index: Optional Y index into the data matrix from which to render the image.
            t_index: Optional time sample index into the data matrix from which to render the image.

        Returns:
            Image
        """
        time, z_dim, y_dim, x_dim = self.data.shape
        out_image = Image.frombuffer('L', (x_dim, z_dim), self.data[t_index, :, y_index, :].flatten(), 'raw', 'L', 0, 1)
        # TODO: DMK - ask KL about this comment:
        # if the image scales to 0 pixels it don't work
        return out_image.resize([x_dim, int(z_dim * z_scale)])

    def yz_image(self, z_scale=1, x_index=0, t_index=0):
        """Render an image in the yz plane.

        Args:
            z_scale: Scaling factor for the z-dimension. Useful for rendering non-isotropic data
            x_index: Optional X index into the data matrix from which to render the image.
            t_index: Optional time sample index into the data matrix from which to render the image.

        Returns:
            Image
        """
        time, z_dim, y_dim, x_dim = self.data.shape
        out_image = Image.frombuffer('L', (y_dim, z_dim), self.data[t_index, :, :, x_index].flatten(), 'raw', 'L', 0, 1)
        # TODO: DMK - ask KL about this comment:
        # if the image scales to 0 pixels it don't work
        return out_image.resize([y_dim, int(z_dim * z_scale)])


class ImageCube16(Cube):
    def __init__(self, cube_size=None, time_range=None):
        """Create empty array of cube_size"""

        if not cube_size:
            cube_size = CUBOIDSIZE[0]

        # call the base class constructor
        Cube.__init__(self, cube_size, time_range)

        # note that this is self.cube_size (which is transposed) in Cube
        self.data = np.zeros([self.time_range[1] - self.time_range[0]] + self.cube_size, dtype=np.uint16, order='C')

        # variable that describes when a cube is created from zeros rather than loaded from another source
        self._created_from_zeros = False

        self.datatype = np.uint16

    def zeros(self):
        """Create a cube of all zeros

        Returns:
            None
        """
        self._created_from_zeros = True
        self.data = np.zeros([self.time_range[1] - self.time_range[0]] + self.cube_size, dtype=np.uint16, order='C')

    def random(self):
        """Create a random cube

        Returns:
            None
        """
        self.data = np.random.randint(1, 65534,
                                      size=[self.time_range[1]-self.time_range[0]] + self.cube_size,
                                      dtype=np.uint16)

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
                self.data[t, :, :, :] = ndlib.overwriteDense16_ctype(
                    self.data[t, :, :, :], input_data[t - time_sample_range[0], :, :, :])
        else:
            # Input data doesn't have any time indices
            self.data[time_sample_range[0], :, :, :] = ndlib.overwriteDense16_ctype(
                self.data[time_sample_range[0], :, :, :], input_data[time_sample_range[0], :, :, :])

    def xy_image(self, z_index=0, t_index=0):
        """Render an image in the XY plane.

        Args:
            z_index: Optional Z index into the data matrix from which to render the image.
            t_index: Optional time sample index into the data matrix from which to render the image.


        Returns:
            Image
        """

        # This works for 16-> conversions
        _, z_dim, y_dim, x_dim = self.data.shape

        # If data type is uint8 you got windowed data FROM the API layer. Otherwise limit output range.
        if self.data.dtype == np.uint8:
            return Image.frombuffer('L', (x_dim, y_dim), self.data[t_index, z_index, :, :].flatten(), 'raw', 'L', 0, 1)
        else:
            out_image = Image.frombuffer('I;16', (x_dim, y_dim), self.data[t_index, z_index, :, :].flatten(),
                                         'raw', 'I;16', 0, 1)
            return out_image.point(lambda i: i * (1. / 256)).convert('L')

    def xz_image(self, z_scale=1, y_index=0, t_index=0):
        """Render an image in the xz plane.

        Args:
            z_scale: Scaling factor for the z-dimension. Useful for rendering non-isotropic data
            y_index: Optional Y index into the data matrix from which to render the image.
            t_index: Optional time sample index into the data matrix from which to render the image.

        Returns:
            Image
        """
        _, z_dim, y_dim, x_dim = self.data.shape

        # If data type is uint8 you got windowed data FROM the API layer. Otherwise limit output range.
        if self.data.dtype == np.uint8:
            out_image = Image.frombuffer('L', (x_dim, z_dim), self.data[t_index, :, y_index, :].flatten(),
                                         'raw', 'L', 0, 1)
        else:
            out_image = Image.frombuffer('I;16', (x_dim, z_dim), self.data[t_index, :, y_index, :].flatten(),
                                         'raw', 'I;16', 0, 1)
            out_image = out_image.point(lambda i: i * (1. / 256)).convert('L')

        return out_image.resize([x_dim, int(z_dim * z_scale)])

    def yz_image(self, z_scale=1, x_index=0, t_index=0):
        """Render an image in the yz plane.

        Args:
            z_scale: Scaling factor for the z-dimension. Useful for rendering non-isotropic data
            x_index: Optional X index into the data matrix from which to render the image.
            t_index: Optional time sample index into the data matrix from which to render the image.

        Returns:
            Image
        """
        _, z_dim, y_dim, x_dim = self.data.shape

        # If data type is uint8 you got windowed data FROM the API layer. Otherwise limit output range.
        if self.data.dtype == np.uint8:
            out_image = Image.frombuffer('L', (y_dim, z_dim), self.data[t_index, :, :, x_index].flatten(),
                                         'raw', 'L', 0, 1)
        else:
            out_image = Image.frombuffer('I;16', (y_dim, z_dim), self.data[t_index, :, :, x_index].flatten(),
                                         'raw', 'I;16', 0, 1)
            out_image = out_image.point(lambda i: i * (1. / 256)).convert('L')

        return out_image.resize([y_dim, int(z_dim * z_scale)])
