# Original Copyright 2014 NeuroData (http://neurodata.io)
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
#
# Modified from original source by Johns Hopkins University Applied Physics Laboratory
# Copyright 2016 Johns Hopkins University Applied Physics Laboratory

import numpy as np
from collections import namedtuple

from operator import mod, floordiv

from spdb.c_lib import ndlib
from spdb.c_lib.ndtype import CUBOIDSIZE

from .error import SpdbError, ErrorCode
from .kvio import KVIO
from .cube import Cube

"""
.. module:: spatialdb
    :synopsis: Manipulate/create/read from the Morton-order cube store

.. moduleauthor:: Kunal Lillaney <lillaney@jhu.edu> and Dean Kleissas <dean.kleissas@jhuapl.edu>
"""

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
      s3io ():
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

    # GET Method
    def get_cube(self, resource, resolution, morton_idx, update=False):
        """
        Load a single cuboid from the cache key-value store - primarily used by Blaze interface

        Args:
            resource (project.BossResource): Data model info based on the request or target resource
            morton_idx (int): the Morton ID of the cuboid
            resolution (int): the resolution level
            update (bool): True if this an update operation. False if the first time you are inserting the cuboid

        Returns:
            cube.Cube: The cuboid data
        """
        # TODO: DMK to add cuboid tracking indexing here
        # This is referring to adding redis/s3 index call here like below in get_cubes.  occasionally get_cube is called
        # directly so you need it here. Ultimately this might get replaced with a call to the cache manager?
        cube = Cube.create_cube(resource, CUBOIDSIZE[resolution])

        # get the block from the database
        cube_bytes = self.kvio.get_cube(resource, resolution, morton_idx)

        if not cube_bytes:
            # There wasn't a cuboid so return zeros
            cube.zeros()
        else:
            # Handle the cube format here and decompress the cube
            cube.from_blosc_numpy(cube_bytes)

        return cube

    def get_cubes(self, resource, resolution, morton_idx_list):
        """Load an array of cuboids from the cache key-value store

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            morton_idx_list (list[int]): a list of Morton ID of the cuboids to get
            resolution (int): the resolution level

        Returns:
            list[(int, bytes)]: The cuboid data in a tuple, (mortonID, compressed bytes)
        """
        if len(resource.get_time_samples()) == 1:
            # TODO: Update this block of code once S3 integration has occurred
            if self.s3io:
                # Get the ids of the cuboids you need - only gives ids to be fetched
                ids_to_fetch = self.kvio.get_cube_index(resource, resolution, morton_idx_list)

                # Check if the index exists inside the cache database or if you must fetch from S3
                if ids_to_fetch:
                    logger.info("Cache miss on get_cubes. Loading from S3: {}".format(morton_idx_list))
                    super_cuboids = self.s3io.get_cubes(resource, ids_to_fetch, resolution)

                    # Iterating over super cuboids and load into the cache kv-stores
                    for supercuboid_idx_list, supercuboid_list in super_cuboids:
                        # call put_cubes and update index in the table before returning data
                        self.put_cubes(resource, supercuboid_idx_list, resolution, supercuboid_list, update=True)

            return self.kvio.get_cubes(resource, resolution, morton_idx_list)
        else:
            raise SpdbError('Not Supported', 'Time Series data not yet fully supported', ErrorCode.DATATYPE_NOT_SUPPORTED)
            # return self.kvio.getTimeCubes(resource, morton_idx_list, timestamp_list, resolution)

    # PUT Methods
    def put_cube(self, resource, zidx, resolution, cube):
        """Insert a cuboid into the cache key-value store

        Args:
            resource (project.BossResource): Data model info based on the request or target resource
            zidx (int): a list of Morton ID of the cuboids to get
            resolution (int): the resolution level
            cube (cube.Cube): list of cuboids to store in the cache key-value store

        Returns:
            list[numpy.ndarray]: The cuboid data
        """
        # If using S3, update index of cuboids that exist in the cache key-value store
        if self.s3io:
            # TODO: Update after S3 integration. Move index IO into new class
            self.kvio.put_cube_index(resource, resolution, [zidx])

        self.kvio.put_cubes(resource, zidx, resolution, [cube.to_blosc_numpy()], not cube.from_zeros())

    def put_cubes(self, resource, resolution, morton_idx_list, cube_list, update=False):
        """Insert a list of cubes into the cache key-value store

        Args:
            resource (project.BossResource): Data model info based on the request or target resource
            morton_idx_list (list[int]): a list of Morton ID of the cuboids to get
            resolution (int): the resolution level
            cube_list (list[cube.Cube]): list of cuboids to store in the cache key-value store
            update (bool): True if this an update operation. False if the first time you are inserting the cuboid

        Returns:
            list[numpy.ndarray]: The cuboid data
        """

        #if self.s3io:
        self.kvio.put_cube_index(resource, resolution, morton_idx_list)

        return self.kvio.put_cubes(resource, resolution, morton_idx_list, [x.to_blosc_numpy() for x in cube_list],
                                   update)

    def _up_sample_cutout(self, resource, corner, extent, resolution):
        """Transform coordinates of a base resolution cutout to a lower res level by up-sampling.

        Only applicable to Layers.

        When you make an annotation cutout and request a zoom level that is LOWER (higher resolution) than your base
        resolution (the resolution annotations should be written to by default) you must take a base resolution cutout
        and up-sample the data, creating a physically large image.

        Args:
            resource (project.BossResource): Data model info based on the request or target resource:
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

    def cutout(self, resource, corner, extent, resolution):
        """Extract a cube of arbitrary size. Need not be aligned to cuboid boundaries.

        Args:
            resource (project.BossResource): Data model info based on the request or target resource
            corner ((int, int, int)): a list of Morton ID of the cuboids to get
            extent ((int, int, int)): a list of Morton ID of the cuboids to get
            resolution (int): the resolution level

        Returns:
            cube.Cube: The cutout data stored in a Cube instance
        """

        # if cutout is below resolution, get a smaller cube and scaleup
        # ONLY FOR ANNO CHANNELS - if data is missing on the current resolution but exists else where...extrapolate
        # TODO: ask kunal what below resolution means? what is the getResolution method doing?
        # ch.getResolution is the "base" resolution and you assume data exists there.
        # If propagated you don't have to worry about this. -> currently they don't upsample annotations when hardening
        # the database, so don't need to check for propagated.

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
                #TODO: DMK move CUBOIDSIZE into Resource

                cutout_coords = self._down_sample_cutout(resource, corner, extent, resolution)

                [x_cube_dim, y_cube_dim, z_cube_dim] = cube_dim = CUBOIDSIZE[base_res]
                cutout_resolution = base_res
            else:
                # this is the default path when not DYNAMICALLY scaling the resolution

                # get the size of the image and cube
                [x_cube_dim, y_cube_dim, z_cube_dim] = cube_dim = CUBOIDSIZE[resolution]
                cutout_resolution = resolution

                # Create namedtuple for consistency with re-sampling paths through the code
                result_tuple = namedtuple('ResampleCoords',
                                          ['corner', 'extent', 'x_pixel_offset', 'y_pixel_offset'])
                cutout_coords = result_tuple(corner, extent, None, None)
        else:
            # Resouce is a channel, so no re-sampling
            # get the size of the image and cube
            [x_cube_dim, y_cube_dim, z_cube_dim] = cube_dim = CUBOIDSIZE[resolution]
            cutout_resolution = resolution

            # Create namedtuple for consistency with re-sampling paths through the code
            result_tuple = namedtuple('ResampleCoords',
                                      ['corner', 'extent', 'x_pixel_offset', 'y_pixel_offset'])
            cutout_coords = result_tuple(corner, extent, None, None)

        # Round to the nearest larger cube in all dimensions
        z_start = cutout_coords.corner[2] // z_cube_dim
        y_start = cutout_coords.corner[1] // y_cube_dim
        x_start = cutout_coords.corner[0] // x_cube_dim

        z_num_cubes = (cutout_coords.corner[2] + cutout_coords.extent[2] + z_cube_dim - 1) // z_cube_dim - z_start
        y_num_cubes = (cutout_coords.corner[1] + cutout_coords.extent[1] + y_cube_dim - 1) // y_cube_dim - y_start
        x_num_cubes = (cutout_coords.corner[0] + cutout_coords.extent[0] + x_cube_dim - 1) // x_cube_dim - x_start

        in_cube = Cube.create_cube(resource, cube_dim)
        out_cube = Cube.create_cube(resource,
                                    [x_num_cubes * x_cube_dim, y_num_cubes * y_cube_dim, z_num_cubes * z_cube_dim])

        # Build a list of indexes to access
        list_of_idxs = []
        for z in range(z_num_cubes):
            for y in range(y_num_cubes):
                for x in range(x_num_cubes):
                    morton_idx = ndlib.XYZMorton(np.asarray([x + x_start, y + y_start, z + z_start],
                                                                   dtype=np.uint64))
                    list_of_idxs.append(morton_idx)

        # Sort the indexes in Morton order
        list_of_idxs.sort()

        # xyz offset stored for later use
        lowxyz = ndlib.MortonXYZ(list_of_idxs[0])

        #TODO: We may not need time-series optimized cutouts. Consider removing.
        # checking for timeseries data and doing an optimized cutout here in timeseries column
        if len(resource.get_time_samples()) > 1:
            for idx in list_of_idxs:
                cuboids = self.get_cubes(resource, idx, resolution)

                # use the batch generator interface
                for idx, timestamp, data_string in cuboids:

                    # add the query result cube to the bigger cube
                    curxyz = ndlib.MortonXYZ(int(idx))
                    offset = [curxyz[0] - lowxyz[0], curxyz[1] - lowxyz[1], curxyz[2] - lowxyz[2]]

                    in_cube.from_blosc_numpy(data_string[:])

                    # add it to the output cube
                    out_cube.add_data(in_cube, offset, timestamp)

        else:
            cuboids = self.get_cubes(resource, cutout_resolution, list_of_idxs)

            # use the batch generator interface
            for idx, data_string in cuboids:

                # add the query result cube to the bigger cube
                curxyz = ndlib.MortonXYZ(int(idx))
                offset = [curxyz[0] - lowxyz[0], curxyz[1] - lowxyz[1], curxyz[2] - lowxyz[2]]

                in_cube.from_blosc_numpy(data_string[:])

                # TODO: DMK commented out exception code since exceptions are not yet implemented.
                # apply exceptions if it's an annotation project
                #if annoids != None and ch.getChannelType() in ANNOTATION_CHANNELS:
                #    in_cube.data = c_lib.filter_ctype_OMP(in_cube.data, annoids)
                #    if ch.getExceptions() == EXCEPTION_TRUE:
                #        self.applyCubeExceptions(ch, annoids, cutout_resolution, idx, in_cube)

                # add it to the output cube
                out_cube.add_data(in_cube, offset)

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

    def write_cuboids(self, resource, corner, resolution, cuboid_data):
        """ Write an arbitary size data to the database

        Main use is in OCP Blaze/cache in inconsistent mode since it reconciles writes in memory asynchronously

        Args:
            resource (project.BossResource): Data model info based on the request or target resource
            corner ((int, int, int)): a list of Morton ID of the cuboids to get
            resolution (int): the resolution level
            cuboid_data (cube.Cube): arbitrary sized matrix of data to write to cuboids in the cache db


        Returns:
            None
        """
        # dim is in xyz, data is in zyx order
        # TODO: Confirm if data should continue to be stored in zyx
        dim = cuboid_data.shape[::-1]

        # get the size of the image and cube
        [x_cube_dim, y_cube_dim, z_cube_dim] = cube_dim = CUBOIDSIZE[resolution]

        # TODO: DMK Double check that the div operation in python2 should be ported to the floordiv operation
        # Round to the nearest larger cube in all dimensions
        [x_start, y_start, z_start] = list(map(floordiv, corner, cube_dim))

        z_num_cubes = (corner[2] + dim[2] + z_cube_dim - 1) / z_cube_dim - z_start
        y_num_cubes = (corner[1] + dim[1] + y_cube_dim - 1) / y_cube_dim - y_start
        x_num_cubes = (corner[0] + dim[0] + x_cube_dim - 1) / x_cube_dim - x_start

        [x_offset, y_offset, z_offset] = list(map(mod, corner, cube_dim))

        # TODO: Double check this...inserting data into a specific spot?
        data_buffer = np.zeros([z_num_cubes * z_cube_dim, y_num_cubes * y_cube_dim, x_num_cubes * x_cube_dim],
                               dtype=cuboid_data.dtype)
        data_buffer[z_offset:z_offset + dim[2], y_offset:y_offset + dim[1], x_offset:x_offset + dim[0]] = cuboid_data

        # TODO: What is this line?
        incube = Cube.create_cube(cube_dim, resource)

        list_of_idxs = []
        list_of_cubes = []
        for z in range(z_num_cubes):
            for y in range(y_num_cubes):
                for x in range(x_num_cubes):
                    list_of_idxs.append(ndlib.XYZMorton([x + x_start, y + y_start, z + z_start]))
                    incube.data = data_buffer[z * z_cube_dim:(z + 1) * z_cube_dim,
                                              y * y_cube_dim:(y + 1) * y_cube_dim,
                                              x * x_cube_dim:(x + 1) * x_cube_dim]
                    list_of_cubes.append(incube.to_blosc_numpy())

        self.put_cubes(resource, list_of_idxs, resolution, list_of_cubes, update=False)

    def write_cuboid(self, resource, corner, resolution, cuboid_data):
        """ Write a 3D/4D volume to the key-value store. Used by API/cache in consistent mode as it reconciles writes
        Args:
            resource (project.BossResource): Data model info based on the request or target resource
            corner ((int, int, int)): a list of Morton ID of the cuboids to get
            resolution (int): the resolution level
            cuboid_data (numpy.ndarray): Matrix of data to write as cuboids

        Returns:
            None
        """
        # TODO: Look into data ordering when storing time series data and if it needs to be different

        # dim is in xyz, data is in zyx order
        if len(resource.get_time_samples()) == 1:
            # Single time point - reorder from xyz to zyx
            dim = cuboid_data.shape[::-1]
        else:
            # Reshape based on optimizing cuboid organization for time access
            # TODO: Look into if the cuboid index needs to get updated to reflect writes to the cache
            dim = cuboid_data.shape[::-1][:-1]

        # get the size of the image and cube
        [x_cube_dim, y_cube_dim, z_cube_dim] = cube_dim = CUBOIDSIZE[resolution]

        # Round to the nearest larger cube in all dimensions
        [x_start, y_start, z_start] = list(map(floordiv, corner, cube_dim))

        z_num_cubes = (corner[2] + dim[2] + z_cube_dim - 1) // z_cube_dim - z_start
        y_num_cubes = (corner[1] + dim[1] + y_cube_dim - 1) // y_cube_dim - y_start
        x_num_cubes = (corner[0] + dim[0] + x_cube_dim - 1) // x_cube_dim - x_start

        [x_offset, y_offset, z_offset] = list(map(mod, corner, cube_dim))

        if len(resource.get_time_samples()) == 1:
            # Single time point
            data_buffer = np.zeros([z_num_cubes * z_cube_dim, y_num_cubes * y_cube_dim, x_num_cubes * x_cube_dim],
                                   dtype=cuboid_data.dtype)

            data_buffer[z_offset:z_offset + dim[2],
                        y_offset:y_offset + dim[1],
                        x_offset:x_offset + dim[0]] = cuboid_data
        else:
            data_buffer = np.zeros([resource.get_time_samples()[-1] -
                                   resource.get_time_samples()[0]] +
                                   [z_num_cubes * z_cube_dim, y_num_cubes * y_cube_dim, x_num_cubes * x_cube_dim],
                                   dtype=cuboid_data.dtype)

            data_buffer[:, z_offset:z_offset + dim[2],
                           y_offset:y_offset + dim[1],
                           x_offset:x_offset + dim[0]] = cuboid_data

        # Get current cube from db, merge with new cube, write to db
        if len(resource.get_time_samples()) == 1:
            # Single time point
            for z in range(z_num_cubes):
                for y in range(y_num_cubes):
                    for x in range(x_num_cubes):
                        morton_idx = ndlib.XYZMorton(np.asarray([x + x_start, y + y_start, z + z_start],
                                                                       dtype=np.uint64))
                        cube = self.get_cube(resource, resolution, morton_idx, update=True)

                        # overwrite the cube
                        cube.overwrite(data_buffer[z * z_cube_dim:(z + 1) * z_cube_dim,
                                                   y * y_cube_dim:(y + 1) * y_cube_dim,
                                                   x * x_cube_dim:(x + 1) * x_cube_dim])

                        # update in the database
                        self.put_cube(resource, resolution, morton_idx, cube)
        else:
            for z in range(z_num_cubes):
                for y in range(y_num_cubes):
                    for x in range(x_num_cubes):
                        for time_sample in resource.get_time_samples():
                            morton_idx = ndlib.XYZMorton(np.asarray([x + x_start, y + y_start, z + z_start],
                                                                           dtype=np.uint64))

                            cube = self.get_cube(resource, morton_idx, resolution, update=True)

                            # overwrite the cube
                            cube.overwrite(data_buffer[time_sample - resource.get_time_samples()[0],
                                                       z * z_cube_dim:(z + 1) * z_cube_dim,
                                                       y * y_cube_dim:(y + 1) * y_cube_dim,
                                                       x * x_cube_dim:(x + 1) * x_cube_dim])

                            # update in the database
                            self.put_cube(resource, morton_idx, resolution, cube)

