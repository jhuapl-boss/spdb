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
import blosc
from PIL import Image

from abc import ABCMeta, abstractmethod

import spdb.c_lib.ndtype as ndtype
from spdb.c_lib import ndlib
import blosc

from .error import SpdbError, ErrorCodes

"""
.. module:: Cube
    :synopsis: Manipulate the in-memory data representation of the 3-D cube of data that contains image or annotations.
"""


class Cube(metaclass=ABCMeta):
    """An abstract base class to store 3D matrix data with time-series support and perform common operations

    The create_cube method is a factory method that will return the proper Cube child class instance based on the
    provided resource instance

    Args:
      cube_size list(int): Dimensions of the matrix in [x, y, z]
      time_range list(int): The contiguous range of time samples stored in this cube instance

    Attributes:
      cube_size list(int):  Dimensions of the matrix in [x, y, z]
      time_samples list(int): The min and max time samples store in cube, in python convention (start inclusive, stop
      exclusive)
      is_time_series bool: A flag indicating if the cube contains a time-series or single time sample
      z_dim (int): The Z dimension of the data matrix
      y_dim (int): The Y dimension of the data matrix
      x_dim (int): The X dimension of the data matrix
      data (numpy.ndarray): The 3D matrix of data as a numpy array in [t, z, y, x]
      _created_from_zeros (bool): Flag indicates if the data was generated by this instance or pre-existing
    """
    def __init__(self, cube_size, time_range=None):
        # cube_size is represented in x,y,z but data is stored c-ordered internally as z,y,x
        # cube_size is in z,y,x for interactions with tile/image data
        # time_range specified with time points stored in this cube instance
        self.z_dim, self.y_dim, self.x_dim = self.cube_size = [cube_size[2], cube_size[1], cube_size[0]]

        # _created_from_zeros flag indicates if the data was generated by this instance or pre-existing
        self._created_from_zeros = False

        self.data = None
        self.morton_id = None
        self.datatype = None

        # Setup time sample properties
        if time_range:
            self.is_time_series = True
            self.time_range = time_range
        else:
            self.is_time_series = False
            self.time_range = [0, 1]

    def set_data(self, data):
        """Method to set the cube data matrix

        Args:
            data(np.ndarray):

        Returns:
            None
        """
        self.data = data
        self.datatype = data.dtype

    def add_data(self, input_cube, index):
        """Add data to a larger cube (this instance) from a smaller cube (input_cube)

        Assumes all time samples are present in the smaller cube

        Args:
            input_cube (spdb.cube.Cube): Input Cube instance from which to merge data
            index: relative morton ID indicating where to insert the data

        Returns:
            None
        """
        x_offset = index[0] * input_cube.x_dim
        y_offset = index[1] * input_cube.y_dim
        z_offset = index[2] * input_cube.z_dim

        np.copyto(self.data[input_cube.time_range[0] - self.time_range[0]:input_cube.time_range[1] - self.time_range[0],
                            z_offset:z_offset + input_cube.z_dim,
                            y_offset:y_offset + input_cube.y_dim,
                            x_offset:x_offset + input_cube.x_dim], input_cube.data[:, :, :, :])

    def trim(self, x_offset, x_size, y_offset, y_size, z_offset, z_size):
        """Trim off excess data if not cuboid aligned. Applies to ALL time samples.

        Args:
            x_offset (int): Start X index of data to keep
            x_size (int): X extent of data to keep
            y_offset (int): Start Y index of data to keep
            y_size (int): Y extent of data to keep
            z_offset (int): Start Z index of data to keep
            z_size (int): Y extent of data to keep

        Returns:
            None
        """
        self.data = self.data[:, z_offset:z_offset + z_size, y_offset:y_offset + y_size, x_offset:x_offset + x_size]

        # update the cube dimensions, ignoring the time component since it does not change
        self.z_dim, self.y_dim, self.x_dim = self.cube_size = list(self.data.shape[1:])

    def pack_array(self, data):
        """Method to serialize and compress data using the blosc compressor.
          Assumes the datatype of the passed in array if the datatype property is not set

        Args:
            data (np.ndarray): The array to pack

        Returns:
            (bytes): The resulting serialized and compressed byte array
        """
        if not self.datatype:
            self.datatype = data.dtype

        return blosc.compress(data, typesize=(np.dtype(self.datatype).itemsize * 8))

    def to_blosc(self):
        """A method that packs data in this Cube instance using blosc compressor for all
        time samples (assumes a 4-D matrix).

        If the datatype property has not been set, the datatype of self.data is assumed.

        Args:

        Returns:
            bytes - the compressed, serialized byte array of Cube matrix data for a given time sample
        """
        try:
            return self.pack_array(self.data[:, :, :, :])
        except Exception as e:
            raise SpdbError("Failed to compress cube. {}".format(e),
                            ErrorCodes.SERIALIZATION_ERROR)

    def to_blosc_by_time_index(self, time_index=0):
        """A method that packs data in this Cube instance using Blosc compressor for a
        single time sample.  The time index is the time sample index value.  It will be converted to an actual index
        into self.data by removing the cube's time offset

        If the time_index isn't specified, 0 is used, effectively selecting the first sample.

        Always packs a 4D array with a single time point for consistency internally.

        Args:
            time_index (int): Time sample to get.

        Returns:
            bytes - the compressed, serialized byte array of Cube matrix data for a given time sample

        """
        try:
            # Index into the data array with time.  Return a 4D array
            return self.pack_array(np.expand_dims(self.data[time_index - self.time_range[0], :, :, :], axis=0))
        except Exception as e:
            raise SpdbError("Failed to compress cube. {}".format(e),
                            ErrorCodes.SERIALIZATION_ERROR)

    def unpack_array(self, data, num_time_points=1):
        """Method to uncompress and deserialize the provided data.

        If only a single time point provided,

        Args:
            data (bytes): The array to pack
            num_time_points (int): Number of time samples in the compressed data

        Returns:
            (np.ndarray): The resulting serialized and compressed byte array
        """
        if not self.datatype:
            raise SpdbError("Cube instance must have datatype parameter set to enable deserialization.",
                            ErrorCodes.SERIALIZATION_ERROR)

        raw_data = blosc.decompress(data)
        data_mat = np.fromstring(raw_data, dtype=self.datatype)
        data_mat = np.reshape(data_mat, (num_time_points, self.z_dim, self.y_dim, self.x_dim), order='C')

        return data_mat

    def from_blosc(self, byte_arrays, time_sample_range=None, missing_time_steps=[]):
        # TODO: Conditional properties of this method are challenging for the developer. break into multiple methods
        """Uncompress and populate Cube data from a Blosc serialized and compressed byte array using the numpy interface

        If byte_arrays is a list, assume data is stored internally in this Cube instance in tzyx ordering and
        each byte array is a single tzyx ordered time sample, in order, matching time_sample_range.

        If byte_arrays is a single bytearray, assume it contains the entire Cube's data for all time samples and is of the
        format tzyx. Directly decompress and replace data in the Cube instance.

        Args:
            byte_arrays list[str]:  list of time ordered, compressed, serialized byte array of Cube matrix data
            time_sample_range list(int): The min and max time samples that input_data represents in python convention
            (start inclusive, stop exclusive)

        Returns:
            None

        """
        try:
            if not time_sample_range:
                # This isn't a time-series cube, so use default
                self.is_time_series = False
                self.time_range = time_sample_range = [0, 1]
            else:
                self.is_time_series = True
                self.time_range = time_sample_range

            if isinstance(byte_arrays, list) or isinstance(byte_arrays, tuple):
                # Got a list of byte arrays, so assume they are each 4-D, corresponding to time samples

                # Unpack all of the arrays into the cube
                b_arr_idx = 0
                missing_gen = self.missing_ts_gen(missing_time_steps)
                missing_t = next(missing_gen)
                for data_idx, t in enumerate(range(time_sample_range[0], time_sample_range[1])):
                    if data_idx == 0:
                        # On first cube get the size and allocate properly
                        self.data = np.zeros(shape=(time_sample_range[1] - time_sample_range[0],
                                                    self.z_dim, self.y_dim, self.x_dim), dtype=self.data.dtype)
                    if t == missing_t:
                        # No data for this time step.
                        self.data[data_idx, :, :, :] = np.zeros(
                            shape=(1, self.z_dim, self.y_dim, self.x_dim), 
                            dtype=self.data.dtype)
                        missing_t = next(missing_gen)
                    else:
                        self.data[data_idx, :, :, :] = self.unpack_array(byte_arrays[b_arr_idx], 1)
                        b_arr_idx += 1
            else:
                # If you get a single array assume it is the complete 4D array
                self.data[:, :, :, :] = self.unpack_array(byte_arrays, self.time_range[1] - self.time_range[0])
                #self.z_dim, self.y_dim, self.x_dim = self.cube_size = list(self.data.shape)[1:]

        except Exception as e:
            raise SpdbError("Failed to decompress database cube. {}".format(e),
                            ErrorCodes.SERIALIZATION_ERROR)

        self._created_from_zeros = False

    def overwrite_to_black(self, input_data, time_sample_range=None):
        """ Overwrite data with zero values in the input_data

        If time_sample_range is provided, data will be inserted at the appropriate time sample

        Args:
            input_data (numpy.ndarray): Input mask matrix to overwrite the current Cube data
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
                self.data[t, :, :, :][input_data[t - time_sample_range[0], :, :, :]==1] = 0
        else:
            # Input data doesn't have any time indices
            self.data[time_sample_range[0], :, :, :][input_data[time_sample_range[0], :, :, :]==1] = 0

    def missing_ts_gen(self, missing_time_samples):
        """
        Generator for tracking which time samples are missing. 

        Args:
            (list[int]): List of missing time samples in ascending order.

        Yields:
            (int|None): Current missing time sample or None.
        """
        for sample in missing_time_samples:
            yield sample
        while True:
            yield None

    def is_not_zeros(self):
        """Check if the data matrix is all zeros

        Returns:
            bool
        """
        return bool(np.any(self.data))

    def from_zeros(self):
        """Determine if the Cube instance was created from all zeros

        Returns:
            bool
        """
        return self._created_from_zeros

    @abstractmethod
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
        return NotImplemented

    @abstractmethod
    def zeros(self):
        """Initialize Cube instance to all zeros. Must override in child classes to properly deal with datatype and
        other unique properties.

        Example for uin8 based cube:
            self._created_from_zeros = True
            self.data = np.zeros(self.cube_size, dtype=np.uint8)

        Returns:
            None
        """
        return NotImplemented

    @abstractmethod
    def random(self):
        """Create a random cube, used primarily in testing

        Returns:
            None
        """
        return NotImplemented

    @abstractmethod
    def xy_image(self, z_index=0):
        """Render an image in the XY plane. Mut be overridden in child class to deal with data types and shape

        Example for uin8 based cube:
            zdim, ydim, xdim = self.data.shape
            return Image.frombuffer('L', (xdim, ydim), self.data[z_index, :, :].flatten(), 'raw', 'L', 0, 1)

        Args:
            z_index: Optional Z index into the data matrix from which to render the image.

        Returns:
            Image
        """
        return NotImplemented

    @abstractmethod
    def xz_image(self, z_scale=1, y_index=0):
        """Render an image in the xz plane. Mut be overridden in child class to deal with data types and shape

        Example for uin8 based cube:
            zdim, ydim, xdim = self.data.shape
            out_image = Image.frombuffer('L', (xdim, zdim), self.data[:, y_index, :].flatten(), 'raw', 'L', 0, 1)
            return out_image.resize([xdim, int(zdim*z_scale)])

        Args:
            z_scale: Scaling factor for the z-dimension. Useful for rendering non-isotropic data
            y_index: Optional Y index into the data matrix from which to render the image.

        Returns:
            Image
        """
        return NotImplemented

    @abstractmethod
    def yz_image(self, z_scale=1, x_index=0):
        """Render an image in the yz plane. Mut be overridden in child class to deal with data types and shape

        Example for uin8 based cube:
            zdim, ydim, xdim = self.data.shape
            out_image = Image.frombuffer('L', (ydim, zdim), self.data[:, :, 0].flatten(), 'raw', 'L', 0, 1)
            return out_image.resize([ydim, int(zdim*z_scale)])

        Args:
            z_scale: Scaling factor for the z-dimension. Useful for rendering non-isotropic data
            x_index: Optional X index into the data matrix from which to render the image.

        Returns:
            Image
        """
        return NotImplemented

    @staticmethod
    def create_cube(resource, cube_size=None, time_range=None):
        """Static factory method that creates the proper child class instance type based on the resource being accessed

        Args:
            resource (project.BossResource): Data model info based on the request or target resource
            cube_size ([int, int int]): Dimensions of the matrix in [x, y, z]
            time_range (list(int)): The contiguous range of time samples stored in this cube instance [start, stop)

        Returns:
            cube.Cube - Instance of a child class of Cube
        """
        channel = resource.get_channel()
        data_type = resource.get_data_type()

        if not channel.is_image() and data_type == "uint64":
            from .annocube import AnnotateCube64
            return AnnotateCube64(cube_size, time_range)

        elif data_type == "uint8":
            from .imagecube import ImageCube8
            return ImageCube8(cube_size, time_range)
        elif data_type == "uint16":
            from .imagecube import ImageCube16
            return ImageCube16(cube_size, time_range)
        else:
            return Cube(cube_size, time_range)




#TODO: REMOVE
#    def get_all_blosc_numpy_arrays(self):
#        """A generator that packs data in this Cube instance using Blosc and the numpy array specific interface.
#
#        Returns:
#            (int, bytes) - a tuple of the time sample and the compressed, serialized byte array of Cube matrix data
#
#        """
#        # Create compressed byte arrays for each time point, and return in order as a tuple, indicating
#        # the time point
#        try:
#            for cnt, t in enumerate(range(self.time_range[0], self.time_range[1])):
#                yield (t, self.pack_array(np.expand_dims(self.data[cnt, :, :, :], axis=0)))
#        except Exception as e:
#            raise SpdbError("Failed to compress cube. {}".format(e),
#                            ErrorCodes.SERIALIZATION_ERROR)
