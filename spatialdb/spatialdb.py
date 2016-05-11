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
from collections import namedtuple
import collections
import itertools

from operator import mod, floordiv

from spdb.c_lib import ndlib
from spdb.c_lib.ndtype import CUBOIDSIZE

from .error import SpdbError, ErrorCode
from .kvio import KVIO
from .cube import Cube

# Todo: fix logger so you can mock it. Then reinsert into class
#from bossutils.logger import BossLogger
#logger = BossLogger()


class SpatialDB:
    """
    Main interface class to the spatial database system/cache engine

    Args:
      resource (project.BossResource): Data model info based on the request or target resource

    Attributes:
      resource (project.BossResource): Data model info based on the request or target resource
      s3io (): FUTURE
      kvio (KVIO): A key-value store engine instance
    """

    def __init__(self):

        # TODO: DMK Add S3 interface or move elsewhere
        # Set the S3 backend for the data
        # self.s3io = s3io.S3IO(self)
        self.s3io = None

        self.kvio = KVIO.get_kv_engine('redis')

        # TODO: DMK Add annotation support
        # self.annoIdx = annindex.AnnotateIndex(self.kvio, self.proj)

    def close(self):
        """
        Close the cache key-value engine

        Returns:
            None

        """
        self.kvio.close()

    # Cube Processing Methods
    def get_cubes(self, resource, resolution, time_sample_range, morton_idx_list):
        """Load an array of cuboids from the cache key-value store as raw compressed byte arrays dealing with
        cache misses if necessary (future)

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            time_sample_range (list(int)): a range of time samples to get [start, stop)
            morton_idx_list (list(int)): a list of Morton ID of the cuboids to get

        Returns:
            list(cube.Cube): The cuboid data with time-series support, packed into Cube instances, sorted by morton id
        """
        if self.s3io:
            raise SpdbError('Not Supported', 'S3 backend not yet supported',
                            ErrorCode.FUTURE)
            #TODO: Insert S3 integration here
            ## Get the ids of the cuboids you need - only gives ids to be fetched
            #ids_to_fetch = self.kvio.get_cube_index(resource, resolution, morton_idx_list)

            ## Check if the index exists inside the cache database or if you must fetch from S3
            #if ids_to_fetch:
            #    logger.info("Cache miss on get_cubes. Loading from S3: {}".format(morton_idx_list))
            #    super_cuboids = self.s3io.get_cubes(resource, ids_to_fetch, resolution)

            #    # Iterating over super cuboids and load into the cache kv-stores
            #    for supercuboid_idx_list, supercuboid_list in super_cuboids:
            #        # call put_cubes and update index in the table before returning data
            #        self.put_cubes(resource, supercuboid_idx_list, resolution, supercuboid_list, update=True)

        # Get all cuboids
        cuboids = [x for x in self.kvio.get_cubes(resource, resolution, range(*time_sample_range), morton_idx_list)]

        # Group by morton
        cuboids = sorted(cuboids, key=lambda element: (element[1], element[0]))

        # Unpack to lists
        time, morton, cube_bytes = zip(*cuboids)

        # Get groups of time samples by morton ID
        morton = np.array(morton)
        morton_boundaries = np.where(morton[:-1] != morton[1:])[0]
        morton_boundaries += 1
        morton_boundaries = np.append(morton_boundaries, len(morton))

        if len(morton_boundaries) == len(morton):
            # Single time samples only!
            not_time_series = True
        else:
            not_time_series = False

        start = 0
        output_cubes = []
        for end in morton_boundaries:
            # Create a temporary cube instance
            temp_cube = Cube.create_cube(resource)
            temp_cube.morton_id = morton[start]

            # populate with all the time samples
            if not_time_series:
                temp_cube.from_blosc_numpy(cube_bytes[start:end])
            else:
                temp_cube.from_blosc_numpy(cube_bytes[start:end], [time[start], time[end - 1] + 1])

            # Save for output
            output_cubes.append(temp_cube)

            start = end

        return output_cubes

    def get_single_cube(self, resource, resolution, time_sample, morton_idx):
        """Return a single cuboid (single time sample) from the cache key-value store as a Cube instance

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            time_sample (int): the time sample to get
            morton_idx (int): the Morton ID of the cuboid to get

        Returns:
            spdb.cube.Cube: A cuboid instance
        """
        # Consume generator
        cube = collections.deque(self.kvio.get_cubes(resource, resolution, [time_sample], [morton_idx]),
                                 maxlen=1)

        temp_cube = Cube.create_cube(resource, cube_size=None, time_range=[time_sample, time_sample + 1])
        temp_cube.from_blosc_numpy([cube[0][2]])

        return temp_cube

    def put_cubes(self, resource, resolution, morton_idx_list, cube_list):
        """Insert a list of Cube instances into the cache key-value store.

        ALL TIME SAMPLES IN THE CUBE WILL BE INSERTED

        Args:
            resource (project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            morton_idx_list (list(int)): a list of Morton ID with 1-1 mapping to the cube_list
            cube_list (list[cube.Cube]): list of Cube instances to store in the cache key-value store

        Returns:

        """
        # If cubes are time-series, insert 1 cube at a time, but all time points. If not, insert all cubes at once
        if cube_list[0].is_time_series:
            for m, c in zip(morton_idx_list, cube_list):
                byte_arrays = []
                time = []
                for cnt, t in enumerate(range(*c.time_range)):
                    time.append(t)
                    byte_arrays.append(c.get_blosc_numpy_by_time_index(cnt))

                # Add cubes to cache
                self.kvio.put_cubes(resource, resolution, time, [m], byte_arrays)

                # Add cubes to cache index if successful
                self.kvio.put_cube_index(resource, resolution, time, [m])
        else:
            # Collect all cubes and insert at once
            byte_arrays = []
            time = []
            for c in cube_list:
                for t in range(*c.time_range):
                    time.append(0)
                    byte_arrays.append(c.get_blosc_numpy_by_time_index(0))

            # Add cubes to cache
            self.kvio.put_cubes(resource, resolution, time, morton_idx_list, byte_arrays)

            # Add cubes to cache index if successful
            self.kvio.put_cube_index(resource, resolution, time, morton_idx_list)

    def put_single_cube(self, resource, resolution, morton_idx, cube):
        """Insert a Cube into the cache key-value store. Supports time series and will put all time points in db.

        ALL TIME SAMPLES IN THE CUBE WILL BE INSERTED

        Args:
            resource (project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            morton_idx (int): Morton ID of the cube
            cube (cube.Cube): Cube instance to store in the cache key-value store


        Returns:

        """
        time_points = range(*cube.time_range)

        t, byte_arrays = zip(*collections.deque(cube.get_all_blosc_numpy_arrays()))

        # Add cube to cache
        self.kvio.put_cubes(resource, resolution, time_points, [morton_idx], byte_arrays)

        # Add cubes to cache index if successful
        self.kvio.put_cube_index(resource, resolution, time_points, [morton_idx])

    def _up_sample_cutout(self, resource, corner, extent, resolution):
        """Transform coordinates of a base resolution cutout to a lower res level by up-sampling.

        Only applicable to Layers.

        When you make an annotation cutout and request a zoom level that is LOWER (higher resolution) than your base
        resolution (the resolution annotations should be written to by default) you must take a base resolution cutout
        and up-sample the data, creating a physically large image.

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource:
            corner ((int, int, int)): the XYZ corner point of the cutout
            extent: ((int, int, int)): the XYZ extent of the cutout
            resolution: the requested resolution level

        Returns:
            collection.namedtuple: A named tuple that stores 4 values:

                corner - the new XYZ corner of the up-sampled cuboid
                extent - new XYZ extent of the up-sampled cuboid
                x_pixel_offset - new XYZ extent of the up-sampled cuboid
                y_pixel_offset - new XYZ extent of the up-sampled cuboid

        """
        # TODO: This currently only works with slice based resolution hierarchies.  Update to handle all cases.
        if resource.get_experiment().hierarchy_method.lower() != "slice":
            raise SpdbError('Not Implemented',
                            'Dynamic up-sampling of only slice based resolution hierarchies is currently supported',
                            ErrorCode.FUTURE)

        # Create namedtuple so you can return multiple things
        result_tuple = namedtuple('ResampleCoords',
                                  ['corner', 'extent', 'x_pixel_offset', 'y_pixel_offset'])

        # Get base resolution for the layer
        base_res = resource.get_layer().base_resolution

        # scale the corner to lower resolution
        effcorner = (corner[0] / (2 ** (base_res - resolution)),
                     corner[1] / (2 ** (base_res - resolution)),
                     corner[2])

        # pixels offset within big range
        xpixeloffset = corner[0] % (2 ** (base_res - resolution))
        ypixeloffset = corner[1] % (2 ** (base_res - resolution))

        # get the new dimension, snap up to power of 2
        outcorner = (corner[0] + extent[0], corner[1] + extent[1], corner[2] + extent[2])

        newoutcorner = ((outcorner[0] - 1) / (2 ** (base_res - resolution)) + 1,
                        (outcorner[1] - 1) / (2 ** (base_res - resolution)) + 1,
                        outcorner[2])

        effdim = (newoutcorner[0] - effcorner[0], newoutcorner[1] - effcorner[1], newoutcorner[2] - effcorner[2])

        return result_tuple(effcorner, effdim, xpixeloffset, ypixeloffset)

    def _down_sample_cutout(self, resource, corner, dim, resolution):
        """Transform coordinates of a base resolution cutout to a higher res level by down-sampling.

        Only applicable to Layers.

        When you make an annotation cutout and request a zoom level that is HIGHER (lower resolution) than your base
        resolution (the resolution annotations should be written to by default) you must take a base resolution cutout
        and down-sample the data, creating a physically smaller image.

        Args:
            resource (project.BossResource): Data model info based on the request or target resource:
            corner ((int, int, int)): the XYZ corner point of the cutout
            extent: ((int, int, int)): the XYZ extent of the cutout
            resolution: the requested resolution level

        Returns:
            collection.namedtuple: A named tuple that stores 4 values:

                corner - the new XYZ corner of the up-sampled cuboid
                extent - new XYZ extent of the up-sampled cuboid

        """

        # Create namedtuple so you can return multiple things
        result_tuple = namedtuple('ResampleCoords',
                                  ['corner', 'extent', 'x_pixel_offset', 'y_pixel_offset'])

        # Get base resolution for the layer
        base_res = resource.get_layer().base_resolution

        # scale the corner to higher resolution
        effcorner = (corner[0] * (2 ** (resolution - base_res)),
                     corner[1] * (2 ** (resolution - base_res)),
                     corner[2])

        effdim = (dim[0] * (2 ** (resolution - base_res)),
                  dim[1] * (2 ** (resolution - base_res)),
                  dim[2])

        return result_tuple(effcorner, effdim, None, None)

    # Main Interface Methods
    def cutout(self, resource, corner, extent, resolution, time_sample_range=None):
        """Extract a cube of arbitrary size. Need not be aligned to cuboid boundaries.

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            corner ((int, int, int)): a list of Morton ID of the cuboids to get
            extent ((int, int, int)): a list of Morton ID of the cuboids to get
            resolution (int): the resolution level
            time_sample_range list((int)):  a range of time samples to get [start, stop). Default is [0,1) if omitted

        Returns:
            cube.Cube: The cutout data stored in a Cube instance
        """
        # TODO: DMK move CUBOIDSIZE into Resource

        if not time_sample_range:
            # If not time sample list defined, used default of 0
            time_sample_range = [0, 1]

        # if cutout is below resolution, get a smaller cube and scaleup
        # ONLY FOR ANNO CHANNELS - if data is missing on the current resolution but exists else where...extrapolate
        # resource.get_layer().base_resolution is the "base" resolution and you assume data exists there.
        # If propagated you don't have to worry about this.
        # currently we don't upsample annotations when hardening the database, so don't need to check for propagated.

        # Create namedtuple for consistency with re-sampling paths through the code
        result_tuple = namedtuple('ResampleCoords',
                                  ['corner', 'extent', 'x_pixel_offset', 'y_pixel_offset'])

        # Check if you need to scale a cutout due to off-base resolution cutouts/propagation state
        if not resource.is_channel():
            # Get base resolution for the layer
            base_res = resource.get_layer().base_resolution

            if base_res > resolution:
                # Must up-sample cutout dynamically find the effective dimensions of the up-sampled cutout
                cutout_coords = self._up_sample_cutout(resource, corner, extent, resolution)

                [x_cube_dim, y_cube_dim, z_cube_dim] = cube_dim = CUBOIDSIZE[base_res]
                cutout_resolution = base_res

            elif not resource.is_channel() and base_res < resolution and not resource.is_propagated():
                # If cutout is a layer, above base resolution (lower res), and NOT propagated, down-sample
                cutout_coords = self._down_sample_cutout(resource, corner, extent, resolution)

                [x_cube_dim, y_cube_dim, z_cube_dim] = cube_dim = CUBOIDSIZE[base_res]
                cutout_resolution = base_res
            else:
                # this is the default path when not DYNAMICALLY scaling the resolution

                # get the size of the image and cube
                [x_cube_dim, y_cube_dim, z_cube_dim] = cube_dim = CUBOIDSIZE[resolution]
                cutout_resolution = resolution

                # Create namedtuple for consistency with re-sampling paths through the code
                cutout_coords = result_tuple(corner, extent, None, None)
        else:
            # Resouce is a channel, so no re-sampling
            # get the size of the image and cube
            [x_cube_dim, y_cube_dim, z_cube_dim] = cube_dim = CUBOIDSIZE[resolution]
            cutout_resolution = resolution

            # Create namedtuple for consistency with re-sampling paths through the code
            cutout_coords = result_tuple(corner, extent, None, None)

        # Round to the nearest larger cube in all dimensions
        z_start = cutout_coords.corner[2] // z_cube_dim
        y_start = cutout_coords.corner[1] // y_cube_dim
        x_start = cutout_coords.corner[0] // x_cube_dim

        z_num_cubes = (cutout_coords.corner[2] + cutout_coords.extent[2] + z_cube_dim - 1) // z_cube_dim - z_start
        y_num_cubes = (cutout_coords.corner[1] + cutout_coords.extent[1] + y_cube_dim - 1) // y_cube_dim - y_start
        x_num_cubes = (cutout_coords.corner[0] + cutout_coords.extent[0] + x_cube_dim - 1) // x_cube_dim - x_start

        #in_cube = Cube.create_cube(resource, cube_dim)
        out_cube = Cube.create_cube(resource,
                                    [x_num_cubes * x_cube_dim, y_num_cubes * y_cube_dim, z_num_cubes * z_cube_dim],
                                    time_sample_range)

        # Build a list of indexes to access
        list_of_idxs = []
        for z in range(z_num_cubes):
            for y in range(y_num_cubes):
                for x in range(x_num_cubes):
                    morton_idx = ndlib.XYZMorton([x + x_start, y + y_start, z + z_start])
                    list_of_idxs.append(morton_idx)

        # Sort the indexes in Morton order
        list_of_idxs.sort()

        # xyz offset stored for later use
        lowxyz = ndlib.MortonXYZ(list_of_idxs[0])

        # Perform cutout
        cuboids = self.get_cubes(resource, cutout_resolution, time_sample_range, list_of_idxs)

        # use the batch generator interface
        for idx, cube in zip(list_of_idxs, cuboids):
            # add the query result cube to the bigger cube
            curxyz = ndlib.MortonXYZ(int(idx))
            offset = [curxyz[0] - lowxyz[0], curxyz[1] - lowxyz[1], curxyz[2] - lowxyz[2]]

            # TODO: DMK commented out exception code since exceptions are not yet implemented.
            # apply exceptions if it's an annotation project
            #if annoids != None and ch.getChannelType() in ANNOTATION_CHANNELS:
            #    in_cube.data = c_lib.filter_ctype_OMP(in_cube.data, annoids)
            #    if ch.getExceptions() == EXCEPTION_TRUE:
            #        self.applyCubeExceptions(ch, annoids, cutout_resolution, idx, in_cube)

            # add it to the output cube
            out_cube.add_data(cube, offset)

        # Get the base resolution if channel or layer for logic below
        base_res = None
        if not resource.is_channel():
            # Get base resolution for the layer
            base_res = resource.get_layer().base_resolution

        # A smaller cube was cutout due to off-base resolution query: up-sample and trim
        if not resource.is_channel() and base_res > resolution:
            # TODO: look into optimizing zoomData and rename
            out_cube.zoomData(base_res - resolution)

            # need to trim based on the cube cutout at new resolution
            out_cube.trim(corner[0] % (x_cube_dim * (2 ** (base_res - resolution))) + cutout_coords.x_pixel_offset,
                          extent[0],
                          corner[1] % (y_cube_dim * (2 ** (base_res - resolution))) + cutout_coords.y_pixel_offset,
                          extent[1],
                          corner[2] % z_cube_dim,
                          extent[2])

        # A larger cube was cutout due to off-base resolution query: down-sample and trim
        elif not resource.is_channel() and base_res < resolution and not resource.is_propagated():
            # TODO: look into optimizing zoomData and rename
            out_cube.downScale(resolution - base_res)

            # need to trim based on the cube cutout at new resolution
            out_cube.trim(corner[0] % (x_cube_dim * (2 ** (base_res - resolution))),
                          extent[0],
                          corner[1] % (y_cube_dim * (2 ** (base_res - resolution))),
                          extent[1],
                          corner[2] % z_cube_dim,
                          extent[2])

        # Trim cube since cutout was not cuboid aligned
        elif extent[0] % x_cube_dim == 0 and \
             extent[1] % y_cube_dim == 0 and \
             extent[2] % z_cube_dim == 0 and \
             corner[0] % x_cube_dim == 0 and \
             corner[1] % y_cube_dim == 0 and \
             corner[2] % z_cube_dim == 0:
            # Cube is already the correct dimensions
            pass
        else:
            out_cube.trim(corner[0] % x_cube_dim,
                          extent[0],
                          corner[1] % y_cube_dim,
                          extent[1],
                          corner[2] % z_cube_dim,
                          extent[2])

        return out_cube

    def write_cuboid(self, resource, corner, resolution, cuboid_data, time_sample_start=0):
        """ Write a 3D/4D volume to the key-value store. Used by API/cache in consistent mode as it reconciles writes

        If cuboid_data.ndim == 4, data in time-series format - assume t,z,y,x
        If cuboid_data.ndim == 3, data not in time-series format - assume z,y,x

        Args:
            resource (project.BossResource): Data model info based on the request or target resource
            corner ((int, int, int)): a list of Morton ID of the cuboids to get
            resolution (int): the resolution level
            cuboid_data (numpy.ndarray): Matrix of data to write as cuboids
            time_sample_start (int): if cuboid_data.ndim == 3, the time sample for the data
                                     if cuboid_data.ndim == 4, the time sample for cuboid_data[0, :, :, :]

        Returns:
            None
        """
        # Check if time-series
        if cuboid_data.ndim == 4:
            # Time-series - coords in xyz, data in zyx so shuffle to be consistent and drop time value
            dim = cuboid_data.shape[::-1][:-1]
            time_sample_stop = time_sample_start + cuboid_data.shape[0]

        elif cuboid_data.ndim == 3:
            # Not time-series - coords in xyz, data in zyx so shuffle to be consistent
            dim = cuboid_data.shape[::-1]
            cuboid_data = np.expand_dims(cuboid_data, axis=0)
            time_sample_stop = time_sample_start + 1
        else:
            raise SpdbError('Invalid Data Shape', 'Matrix must be 4D or 3D',
                            ErrorCode.SPDB_ERROR)

        # Get the size of cuboids
        [x_cube_dim, y_cube_dim, z_cube_dim] = cube_dim = CUBOIDSIZE[resolution]

        # Round to the nearest larger cube in all dimensions
        [x_start, y_start, z_start] = list(map(floordiv, corner, cube_dim))

        z_num_cubes = (corner[2] + dim[2] + z_cube_dim - 1) // z_cube_dim - z_start
        y_num_cubes = (corner[1] + dim[1] + y_cube_dim - 1) // y_cube_dim - y_start
        x_num_cubes = (corner[0] + dim[0] + x_cube_dim - 1) // x_cube_dim - x_start

        [x_offset, y_offset, z_offset] = list(map(mod, corner, cube_dim))

        # Populate the data buffer
        data_buffer = np.zeros([time_sample_stop - time_sample_start] +
                               [z_num_cubes * z_cube_dim, y_num_cubes * y_cube_dim, x_num_cubes * x_cube_dim],
                               dtype=cuboid_data.dtype)

        data_buffer[:, z_offset:z_offset + dim[2],
                       y_offset:y_offset + dim[1],
                       x_offset:x_offset + dim[0]] = cuboid_data

        # Get current cube from db, merge with new cube, write back to the to db
        for z in range(z_num_cubes):
            for y in range(y_num_cubes):
                for x in range(x_num_cubes):
                    # Get the morton ID for the cube
                    morton_idx = ndlib.XYZMorton([x + x_start, y + y_start, z + z_start])

                    # Get the existing cube from the cache. Put all time samples in a single cube instance
                    cube = self.get_cubes(resource, resolution, [time_sample_start, time_sample_stop], [morton_idx])[0]

                    # overwrite the cube
                    cube.overwrite(data_buffer[:,
                                               z * z_cube_dim:(z + 1) * z_cube_dim,
                                               y * y_cube_dim:(y + 1) * y_cube_dim,
                                               x * x_cube_dim:(x + 1) * x_cube_dim],
                                   [time_sample_start - time_sample_start, time_sample_stop - time_sample_start])

                    # update in the database with the new merged cube
                    self.put_cubes(resource, resolution, [morton_idx], [cube])
