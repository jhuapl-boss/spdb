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

import os
import ctypes as cp
import numpy as np
import numpy.ctypeslib as npct
from spdb.c_lib import rgbColor

#
# Cube Locations using ctypes
#

# Load the shared C library using ctype mechanism and the directory path is always local
BASE_PATH = os.path.dirname(__file__)
ndlib_ctypes = npct.load_library("ndlib.so", BASE_PATH + "/c_version")

# Defining numpy array times for C
array_1d_uint8 = npct.ndpointer(dtype=np.uint8, ndim=1, flags='C_CONTIGUOUS')
array_2d_uint8 = npct.ndpointer(dtype=np.uint8, ndim=2, flags='C_CONTIGUOUS')
array_3d_uint8 = npct.ndpointer(dtype=np.uint8, ndim=3, flags='C_CONTIGUOUS')
array_1d_uint16 = npct.ndpointer(dtype=np.uint16, ndim=1, flags='C_CONTIGUOUS')
array_2d_uint16 = npct.ndpointer(dtype=np.uint16, ndim=2, flags='C_CONTIGUOUS')
array_3d_uint16 = npct.ndpointer(dtype=np.uint16, ndim=3, flags='C_CONTIGUOUS')
array_1d_uint32 = npct.ndpointer(dtype=np.uint32, ndim=1, flags='C_CONTIGUOUS')
array_2d_uint32 = npct.ndpointer(dtype=np.uint32, ndim=2, flags='C_CONTIGUOUS')
array_3d_uint32 = npct.ndpointer(dtype=np.uint32, ndim=3, flags='C_CONTIGUOUS')
array_1d_uint64 = npct.ndpointer(dtype=np.uint64, ndim=1, flags='C_CONTIGUOUS')
array_2d_uint64 = npct.ndpointer(dtype=np.uint64, ndim=2, flags='C_CONTIGUOUS')
array_3d_uint64 = npct.ndpointer(dtype=np.uint64, ndim=3, flags='C_CONTIGUOUS')
array_2d_float32 = npct.ndpointer(dtype=np.float32, ndim=2, flags='C_CONTIGUOUS')

# defining the parameter types of the functions in C
# FORMAT: <library_name>,<functiona_name>.argtypes = [ ctype.<argtype> , ctype.<argtype> ....]

ndlib_ctypes.filterCutout.argtypes = [array_1d_uint32, cp.c_int, array_1d_uint32, cp.c_int]
ndlib_ctypes.filterCutoutOMP32.argtypes = [array_1d_uint32, cp.c_int, array_1d_uint32, cp.c_int]
ndlib_ctypes.filterCutoutOMP64.argtypes = [array_1d_uint64, cp.c_int, array_1d_uint64, cp.c_int]
ndlib_ctypes.locateCube.argtypes = [array_2d_uint64, cp.c_int, array_2d_uint32, cp.c_int, cp.POINTER(cp.c_int)]
ndlib_ctypes.annotateCube.argtypes = [array_1d_uint32, cp.c_int, cp.POINTER(cp.c_int), cp.c_int, array_1d_uint32,
                                      array_2d_uint32, cp.c_int, cp.c_char, array_2d_uint32]
ndlib_ctypes.XYZMorton.argtypes = [array_1d_uint64]
ndlib_ctypes.MortonXYZ.argtypes = [npct.ctypes.c_int64, array_1d_uint64]
ndlib_ctypes.recolorCubeOMP32.argtypes = [ array_2d_uint32, cp.c_int, cp.c_int, array_2d_uint32, array_1d_uint32 ]
ndlib_ctypes.recolorCubeOMP64.argtypes = [ array_2d_uint64, cp.c_int, cp.c_int, array_2d_uint64, array_1d_uint64 ]
ndlib_ctypes.quicksort.argtypes = [array_2d_uint64, cp.c_int]
ndlib_ctypes.shaveCube.argtypes = [array_1d_uint32, cp.c_int, cp.POINTER(cp.c_int), cp.c_int, array_1d_uint32, array_2d_uint32,
                                   cp.c_int, array_2d_uint32, cp.c_int, array_2d_uint32]
ndlib_ctypes.annotateEntityDense.argtypes = [array_3d_uint32, cp.POINTER(cp.c_int), cp.c_int]
ndlib_ctypes.shaveDense.argtypes = [array_3d_uint32, array_3d_uint32, cp.POINTER(cp.c_int)]
ndlib_ctypes.exceptionDense.argtypes = [array_3d_uint32, array_3d_uint32, cp.POINTER(cp.c_int)]
ndlib_ctypes.overwriteDense.argtypes = [array_3d_uint32, array_3d_uint32, cp.POINTER(cp.c_int)]
ndlib_ctypes.overwriteDense8.argtypes = [array_3d_uint8, array_3d_uint8, cp.POINTER(cp.c_int)]
ndlib_ctypes.overwriteDense16.argtypes = [array_3d_uint16, array_3d_uint16, cp.POINTER(cp.c_int)]
ndlib_ctypes.overwriteDense64.argtypes = [array_3d_uint64, array_3d_uint64, cp.POINTER(cp.c_int)]
ndlib_ctypes.zoomOutData.argtypes = [array_3d_uint32, array_3d_uint32, cp.POINTER(cp.c_int), cp.c_int]
ndlib_ctypes.zoomOutDataOMP.argtypes = [array_3d_uint32, array_3d_uint32, cp.POINTER(cp.c_int), cp.c_int]
ndlib_ctypes.zoomInData.argtypes = [array_3d_uint32, array_3d_uint32, cp.POINTER(cp.c_int), cp.c_int]
ndlib_ctypes.zoomInDataOMP16.argtypes = [array_3d_uint16, array_3d_uint16, cp.POINTER(cp.c_int), cp.c_int]
ndlib_ctypes.zoomInDataOMP32.argtypes = [array_3d_uint32, array_3d_uint32, cp.POINTER(cp.c_int), cp.c_int]
ndlib_ctypes.mergeCube.argtypes = [array_3d_uint32, cp.POINTER(cp.c_int), cp.c_int, cp.c_int]
ndlib_ctypes.isotropicBuild8.argtypes = [array_2d_uint8, array_2d_uint8, array_2d_uint8, cp.POINTER(cp.c_int)]
ndlib_ctypes.isotropicBuild16.argtypes = [array_2d_uint16, array_2d_uint16, array_2d_uint16, cp.POINTER(cp.c_int)]
ndlib_ctypes.isotropicBuild32.argtypes = [array_2d_uint32, array_2d_uint32, array_2d_uint32, cp.POINTER(cp.c_int)]
ndlib_ctypes.isotropicBuildF32.argtypes = [array_2d_float32, array_2d_float32, array_2d_float32, cp.POINTER(cp.c_int)]
ndlib_ctypes.addDataZSlice.argtypes = [array_3d_uint32, array_3d_uint32, cp.POINTER(cp.c_int), cp.POINTER(cp.c_int)]
ndlib_ctypes.addDataIsotropic.argtypes = [array_3d_uint32, array_3d_uint32, cp.POINTER(cp.c_int), cp.POINTER(cp.c_int)]
ndlib_ctypes.unique.argtypes = [array_1d_uint64, array_1d_uint64, cp.c_int]

# setting the return type of the function in C
# FORMAT: <library_name>.<function_name>.restype = [ ctype.<argtype> ]

ndlib_ctypes.filterCutout.restype = None
ndlib_ctypes.filterCutoutOMP32.restype = None
ndlib_ctypes.filterCutoutOMP64.restype = None
ndlib_ctypes.locateCube.restype = None
ndlib_ctypes.annotateCube.restype = cp.c_int
ndlib_ctypes.XYZMorton.restype = npct.ctypes.c_uint64
ndlib_ctypes.MortonXYZ.restype = None
ndlib_ctypes.recolorCubeOMP32.restype = None
ndlib_ctypes.recolorCubeOMP64.restype = None
ndlib_ctypes.quicksort.restype = None
ndlib_ctypes.shaveCube.restype = None
ndlib_ctypes.annotateEntityDense.restype = None
ndlib_ctypes.shaveDense.restype = None
ndlib_ctypes.exceptionDense.restype = None
ndlib_ctypes.overwriteDense.restype = None
ndlib_ctypes.overwriteDense8.restype = None
ndlib_ctypes.overwriteDense16.restype = None
ndlib_ctypes.overwriteDense64.restype = None
ndlib_ctypes.zoomOutData.restype = None
ndlib_ctypes.zoomOutDataOMP.restype = None
ndlib_ctypes.zoomInData.restype = None
ndlib_ctypes.zoomInDataOMP16.restype = None
ndlib_ctypes.zoomInDataOMP32.restype = None
ndlib_ctypes.mergeCube.restype = None
ndlib_ctypes.isotropicBuild8.restype = None
ndlib_ctypes.isotropicBuild16.restype = None
ndlib_ctypes.isotropicBuild32.restype = None
ndlib_ctypes.isotropicBuildF32.restype = None
ndlib_ctypes.addDataZSlice.restype = None
ndlib_ctypes.addDataIsotropic.restype = None
ndlib_ctypes.unique.restype = cp.c_int


def filter_ctype_OMP(cutout, filterlist):
    """Remove all annotations in a cutout that do not match the filterlist using OpenMP"""

    cutout_shape = cutout.shape
    # Temp Fix
    if cutout.dtype == np.uint32:
        # get a copy of the iterator as a 1-D array
        cutout = np.asarray(cutout, dtype=np.uint32)
        cutout = cutout.ravel()
        filterlist = np.asarray(filterlist, dtype=np.uint32)
        # Calling the C openmp funtion
        ndlib_ctypes.filterCutoutOMP32(cutout, cp.c_int(len(cutout)),
                                       np.sort(filterlist),
                                       cp.c_int(len(filterlist)))
    elif cutout.dtype == np.uint64:
        # get a copy of the iterator as a 1-D array
        cutout = np.asarray(cutout, dtype=np.uint64)
        cutout = cutout.ravel()
        filterlist = np.asarray(filterlist, dtype=np.uint64)
        # Calling the C openmp funtion
        ndlib_ctypes.filterCutoutOMP64(cutout, cp.c_int(len(cutout)),
                                       np.sort(filterlist),
                                       cp.c_int(len(filterlist)))
    else:
        raise ValueError('cutout must be uint32 or uint64 data type')
    return cutout.reshape(cutout_shape)


def filter_ctype(cutout, filterlist):
    """Remove all annotations in a cutout that do not match the filterlist"""

    # get a copy of the iterator as a 1-D array
    flatcutout = cutout.flat.copy()

    # Calling the C naive function
    ndlib_ctypes.filterCutout(flatcutout, cp.c_int(len(flatcutout)), filterlist, cp.c_int(len(filterlist)))

    return flatcutout.reshape(cutout.shape[0], cutout.shape[1], cutout.shape[2])


def annotate_ctype(data, annid, offset, locations, conflictopt):
    """ Remove all annotations in a cutout that do not match the filterlist """

    # get a copy of the iterator as a 1-D array
    datashape = data.shape
    dims = [i for i in data.shape]
    data = data.ravel()

    exceptions = np.zeros((len(locations), 3), dtype=np.uint32)

    # Calling the C native function
    exceptionIndex = ndlib_ctypes.annotateCube(data, cp.c_int(len(data)), (cp.c_int * len(dims))(*dims), cp.c_int(annid),
                                               offset, locations, cp.c_int(len(locations)), cp.c_char(conflictopt), exceptions)

    if exceptionIndex > 0:
        exceptions = exceptions[:(exceptionIndex + 1)]
    else:
        exceptions = np.zeros((0), dtype=np.uint32)

    return (data.reshape(datashape), exceptions)


def locate_ctype(locations, dims):
    """ Find the morton ID of all locations passed in.

    Args:
        locations (numpy.Array): Array is uint32[][3].

    Returns:
        (numpy.Array): an array with elements consisting of [mortonid, x, y, z].

    """

    # get a copy of the iterator as a 1-D array
    cubeLocs = np.zeros([len(locations), 4], dtype=np.uint64)

    # Calling the C native function
    ndlib_ctypes.locateCube(cubeLocs, cp.c_int(len(cubeLocs)), locations, cp.c_int(len(locations)),
                            (cp.c_int * len(dims))(*dims))

    return cubeLocs


def XYZMorton(xyz):
    """ Get morton order from XYZ coordinates

    Args:
        xyz (list): Index of the cuboid in the x, y, z dimensions.

    Returns:
        (int): Morton id.
    """

    # Calling the C native function
    xyz = np.uint64(xyz)
    morton = ndlib_ctypes.XYZMorton(xyz)

    return morton


def MortonXYZ(morton):
    """ Get XYZ indices from Morton id

    Args:
        morton (int): Morton id.

    Returns:
        (list): Index of the cuboid in the x, y, z dimensions.
    """

    # Calling the C native function
    morton = np.uint64(morton)
    cubeoff = np.zeros((3), dtype=np.uint64)
    ndlib_ctypes.MortonXYZ(morton, cubeoff)

    cubeoff = np.uint32(cubeoff)
    return [i for i in cubeoff]


def recolor_ctype(cutout, imagemap):
    """ Annotation recoloring function """

    xdim, ydim = cutout.shape
    if not cutout.flags['C_CONTIGUOUS']:
        cutout = np.ascontiguousarray(cutout, dtype=cutout.dtype)

    # Calling the c native function
    if cutout.dtype == np.uint32:
        ndlib_ctypes.recolorCubeOMP32(cutout, cp.c_int(xdim), cp.c_int(ydim), imagemap,
                                      np.asarray(rgbColor.rgbcolor, dtype=np.uint32))
    else:
        ndlib_ctypes.recolorCubeOMP64(cutout, cp.c_int(xdim), cp.c_int(ydim), imagemap,
                                      np.asarray(rgbColor.rgbcolor, dtype=np.uint64))
    return imagemap


def quicksort(locs):
    """ Sort the cube on Morton Id """

    # Calling the C native language
    ndlib_ctypes.quicksort(locs, len(locs))
    return locs


def shave_ctype(data, annid, offset, locations):
    """ Remove annotations by a list of locations """

    # get a copy of the iterator as a 1-D array
    datashape = data.shape
    dims = [i for i in data.shape]
    data = data.ravel()

    exceptions = np.zeros((len(locations), 3), dtype=np.uint32)
    zeroed = np.zeros((len(locations), 3), dtype=np.uint32)

    exceptionIndex = -1
    zeroedIndex = -1

    # Calling the C native function
    ndlib_ctypes.shaveCube(data, cp.c_int(len(data)), (cp.c_int * len(dims))(*dims), cp.c_int(annid), offset, locations,
                           cp.c_int(len(locations)), exceptions, cp.c_int(exceptionIndex), zeroed, cp.c_int(zeroedIndex))

    if exceptionIndex > 0:
        exceptions = exceptions[:(exceptionIndex + 1)]
    else:
        exceptions = np.zeros((0), dtype=np.uint32)

    if zeroedIndex > 0:
        zeroed = zeroed[:(zeroedIndex + 1)]
    else:
        zeroed = np.zeros((0), dtype=np.uint32)

    return (data.reshape(datashape), exceptions, zeroed)


def annotateEntityDense_ctype(data, entityid):
    """ Relabel all non zero pixels to annotation id """

    dims = [i for i in data.shape]
    ndlib_ctypes.annotateEntityDense(data, (cp.c_int * len(dims))(*dims), cp.c_int(entityid))
    return (data)


def shaveDense_ctype(data, shavedata):
    """ Remove the specified voxels from the annotation """

    dims = [i for i in data.shape]
    ndlib_ctypes.shaveDense(data, shavedata, (cp.c_int * len(dims))(*dims))
    return (data)


def exceptionDense_ctype(data, annodata):
    """ Get a dense voxel region and overwrite all the non-zero values """

    data = np.uint32(data)
    annodata = np.uint32(annodata)
    if not annodata.flags['C_CONTIGUOUS']:
        annodata = np.ascontiguousarray(annodata, np.uint32)
    dims = [i for i in data.shape]
    ndlib_ctypes.exceptionDense(data, annodata, (cp.c_int * len(dims))(*dims))
    return (data)


def overwriteDense_ctype(data, annodata):
    """ Get a dense voxel region and overwrite all the non-zero values """

    orginal_dtype = data.dtype
    data = np.uint32(data)
    annodata = np.uint32(annodata)
    # data = np.ascontiguousarray(data,dtype=np.uint32)
    if not annodata.flags['C_CONTIGUOUS']:
        annodata = np.ascontiguousarray(annodata, dtype=np.uint32)
    dims = [i for i in data.shape]
    ndlib_ctypes.overwriteDense(data, annodata, (cp.c_int * len(dims))(*dims))
    return (data.astype(orginal_dtype, copy=False))


def overwriteDense8_ctype(data, annodata):
    """ Get a dense voxel region and overwrite all the non-zero values """

    orginal_dtype = data.dtype
    if not annodata.flags['C_CONTIGUOUS']:
        annodata = np.ascontiguousarray(annodata, dtype=np.uint8)
    dims = list(data.shape)
    ndlib_ctypes.overwriteDense8(data, annodata, (cp.c_int * len(dims))(*dims))
    return data.astype(orginal_dtype, copy=False)



def overwriteDense16_ctype(data, annodata):
    """ Get a dense voxel region and overwrite all the non-zero values """

    orginal_dtype = data.dtype
    if not annodata.flags['C_CONTIGUOUS']:
        annodata = np.ascontiguousarray(annodata, dtype=np.uint16)
    dims = [i for i in data.shape]
    ndlib_ctypes.overwriteDense16(data, annodata, (cp.c_int * len(dims))(*dims))
    return data.astype(orginal_dtype, copy=False)


def overwriteDense64_ctype(data, annodata):
    """ Get a dense voxel region and overwrite all the non-zero values """

    orginal_dtype = data.dtype
    if not annodata.flags['C_CONTIGUOUS']:
        annodata = np.ascontiguousarray(annodata, dtype=np.uint64)
    dims = [i for i in data.shape]
    ndlib_ctypes.overwriteDense64(data, annodata, (cp.c_int * len(dims))(*dims))
    return data.astype(orginal_dtype, copy=False)


def zoomOutData_ctype(olddata, newdata, factor):
    """ Add the contribution of the input data to the next level at the given offset in the output cube """

    dims = [i for i in newdata.shape]
    ndlib_ctypes.zoomOutData(olddata, newdata, (cp.c_int * len(dims))(*dims), cp.c_int(factor))
    return (newdata)


def zoomOutData64_ctype(olddata, newdata, factor):
    """ Add the contribution of the input data to the next level at the given offset in the output cube """

    dims = [i for i in newdata.shape]
    ndlib_ctypes.zoomOutData64(olddata, newdata, (cp.c_int * len(dims))(*dims), cp.c_int(factor))
    return (newdata)


def zoomOutData_ctype_OMP(olddata, newdata, factor):
    """ Add the contribution of the input data to the next level at the given offset in the output cube """

    dims = [i for i in newdata.shape]
    ndlib_ctypes.zoomOutDataOMP(olddata, newdata, (cp.c_int * len(dims))(*dims), cp.c_int(factor))
    return (newdata)


def zoomInData_ctype(olddata, newdata, factor):
    """ Add the contribution of the input data to the next level at the given offset in the output cube """

    dims = [i for i in newdata.shape]
    ndlib_ctypes.zoomInData(olddata, newdata, (cp.c_int * len(dims))(*dims), cp.c_int(factor))
    return (newdata)


def zoomInData_ctype_OMP(olddata, newdata, factor):
    """ Add the contribution of the input data to the next level at the given offset in the output cube """

    dims = [i for i in newdata.shape]
    if olddata.dtype == np.uint16:
        ndlib_ctypes.zoomInDataOMP16(olddata, newdata, (cp.c_int * len(dims))(*dims), cp.c_int(factor))
    else:
        ndlib_ctypes.zoomInDataOMP32(olddata, newdata, (cp.c_int * len(dims))(*dims), cp.c_int(factor))
    return (newdata)


def mergeCube_ctype(data, newid, oldid):
    """ Relabel voxels in cube from oldid to newid """

    dims = [i for i in data.shape]
    ndlib_ctypes.mergeCube(data, (cp.c_int * len(dims))(*dims), cp.c_int(newid), cp.c_int(oldid))
    return (data)


def isotropicBuild_ctype(data1, data2):
    """ Merging Data """

    dims = [i for i in data1.shape]
    newdata = np.zeros(data1.shape, dtype=data1.dtype)
    if data1.dtype == np.uint32:
        ndlib_ctypes.isotropicBuild32(data1, data2, newdata, (cp.c_int * len(dims))(*dims))
    elif data1.dtype == np.uint8:
        ndlib_ctypes.isotropicBuild8(data1, data2, newdata, (cp.c_int * len(dims))(*dims))
    elif data1.dtype == np.uint16:
        ndlib_ctypes.isotropicBuild16(data1, data2, newdata, (cp.c_int * len(dims))(*dims))
    elif data1.dtype == np.float32:
        ndlib_ctypes.isotropicBuildF32(data1, data2, newdata, (cp.c_int * len(dims))(*dims))
    else:
        raise
    return (newdata)


def addDataToIsotropicStack_ctype(cube, output, offset):
    """Add the contribution of the input data to the next level at the given offset in the output cube"""

    dims = [i for i in cube.data.shape]
    ndlib_ctypes.addDataIsotropic(cube.data, output, (cp.c_int * len(offset))(*offset), (cp.c_int * len(dims))(*dims))


def addDataToZSliceStack_ctype(cube, output, offset):
    """Add the contribution of the input data to the next level at the given offset in the output cube"""

    dims = [i for i in cube.data.shape]
    ndlib_ctypes.addDataZSlice(cube.data, output, (cp.c_int * len(offset))(*offset), (cp.c_int * len(dims))(*dims))


def unique(data):
    """Return the unique elements in the array.

    Args:
        data (numpy.Array): 2D array.

    Returns:
        (numpy.Array): Array of all unique elements in the input array.

    """

    data = data.ravel()
    unique_array = np.zeros(len(data), dtype=data.dtype)
    unique_length = ndlib_ctypes.unique(data, unique_array, cp.c_int(len(data)))

    return unique_array[:unique_length]

# def annoidIntersect_ctype_OMP(cutout, annoid_list):
# """Remove all annotations in a cutout that do not match the filterlist using OpenMP"""

## get a copy of the iterator as a 1-D array
# cutout = cutout.ravel()
# annoid_list = np.asarray(annoid_list, dtype=np.uint32)

## Calling the C openmp funtion
# ndlib_ctypes.annoidIntersectOMP(cutout, cp.c_int(len(cutout)), np.sort(annoid_list), cp.c_int(len(annoid_list)))

# return cutout.reshape( cutout_shape )
