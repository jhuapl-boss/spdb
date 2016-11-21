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
from datetime import datetime
import time

from operator import mod, floordiv
from operator import itemgetter
import uuid

from spdb.c_lib import ndlib
from spdb.c_lib.ndtype import CUBOIDSIZE

from bossutils.logger import BossLogger

from .error import SpdbError, ErrorCodes
from .rediskvio import RedisKVIO
from .cube import Cube
from .object import AWSObjectStore
from .state import CacheStateDB


class SpatialDB:
    """
    Main interface class to the spatial database system/cache engine

    Supported Key-Value databases: Redis
    kv_conf:
        RedisKVIO:{
                    "cache_client": Optional instance of actual redis client. Must either set database info or provide client
                    "cache_host": If cache_client not provided, a string indicating the database host
                    "cache_db": If cache_client not provided, an integer indicating the database to use
                    "read_timeout": Integer indicating number of seconds a read cache key expires
                  }


    Supported Object Stores: AWS S3+DynamoDB
    object_store_conf:
        AWSObjectStore:{
                          "cache_client": Optional instance of actual redis client. Must either set database info or provide client
                          "cache_host": If cache_client not provided, a string indicating the database host
                          "cache_host": If cache_client not provided, an integer indicating the database to use
                          "read_timeout": Integer indicating number of seconds a read cache key expires
                        }


    Cache State interface ONLY works with a redis backend:
    state_conf = {
                    "state_client": Optional instance of actual redis client. Must either set database info or provide client
                    "cache_state_host": If cache_client not provided, a string indicating the database host
                    "cache_state_db": If cache_client not provided, an integer indicating the database to use
                }


    Args:
      kv_conf (dict): Configuration information for the key-value engine interface
      state_conf (dict): Configuration information for the state database interface
      object_store_conf (dict): Configuration information for the object store interface

    Attributes:
      kv_conf (dict): Configuration information for the key-value engine interface
      state_conf (dict): Configuration information for the state database interface
      object_store_conf (dict): Configuration information for the object store
      kvio (KVIO): A key-value store engine instance
      objectio (spdb.rediskvio.RedisKVIO): An object storage engine instance
      cache_state (spdb.state.CacheStateDB): A cache state interface
    """
    def __init__(self, kv_conf, state_conf, object_store_conf):
        self.kv_config = kv_conf
        self.state_conf = state_conf
        self.object_store_config = object_store_conf

        # Threshold number of cuboids for using lambda on reads
        self.read_lambda_threshold = 600  # Currently high since read lambda not implemented
        # Number of seconds to wait for dirty cubes to get clean
        self.dirty_read_timeout = 60

        # Currently only a AWS object store is supported, so create interface instance
        self.objectio = AWSObjectStore(object_store_conf)

        # Currently only a redis based cache db is supported, so create interface instance
        self.kvio = RedisKVIO(kv_conf)

        # Create interface instance for the cache state db (redis backed)
        self.cache_state = CacheStateDB(state_conf)

        # TODO: Add annotation support
        # self.annoIdx = annindex.AnnotateIndex(self.kvio, self.proj)

    def close(self):
        """
        Close the cache key-value engine

        Returns:
            None

        """
        self.kvio.close()

    # Cube Processing Methods
    def get_cubes(self, resource, key_list):
        """Load an array of cuboids from the cache key-value store as raw compressed byte arrays

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            key_list (list(str)): List of cached-cuboid keys to read from the database

        Returns:
            list(cube.Cube): The cuboid data with time-series support, packed into Cube instances, sorted by morton id
        """
        # If you didn't pass in a list it's a single key. Carefully put it into a list without splitting characters
        if isinstance(key_list, str):
            key_list = [key_list]

        # Get all cuboid byte arrays from db
        cuboids = self.kvio.get_cubes(key_list)

        # Group by morton
        cuboids = sorted(cuboids, key=lambda element: (element[0], element[1]))

        # Unpack to lists
        morton, time_sample, cube_bytes = zip(*cuboids)

        # Get groups of time samples by morton ID
        morton = np.array(morton)
        morton_boundaries = np.where(morton[:-1] != morton[1:])[0]
        morton_boundaries += 1
        morton_boundaries = np.append(morton_boundaries, morton.size)

        if morton_boundaries.size == morton.size:
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
                temp_cube.from_blosc(cube_bytes[start:end], [time_sample[start], time_sample[start] + 1])
            else:
                temp_cube.from_blosc(cube_bytes[start:end], [time_sample[start], time_sample[end - 1] + 1])

            # Save for output
            output_cubes.append(temp_cube)

            start = end

        return output_cubes

    # Lambda Page In Methods
    def page_in_cubes(self, key_list, timeout=60):
        """
        Method to trigger the page-in of cubes from the object store, waiting until all are available

        Args:
            key_list (list(str)): List of cached-cuboid keys to page in from the object store
            timeout (int): Number of seconds page in which the operation should complete before an error is raised

        Returns:
            None
        """
        # Setup status channel
        page_in_chan = self.cache_state.create_page_in_channel()

        # Trigger page in operations
        object_keys = self.objectio.page_in_objects(key_list, page_in_chan, self.kv_config, self.state_conf)

        # Wait for page in operation to complete
        self.cache_state.wait_for_page_in(object_keys, page_in_chan, timeout)

        # If you got here everything successfully paged in!
        self.cache_state.delete_page_in_channel(page_in_chan)

    def page_object_into_cache(self, object_key, page_in_channel):
        """Move compressed byte array from the object store to the cache database

        Args:
            object_key (str): Object key for the cuboid that is being moved to the cache
            page_in_channel (str): Page in channel for the current operation

        Returns:
            None
        """
        # Get Cube data from object store
        data = self.objectio.get_objects([object_key])

        # Write data to read-cache
        key_list = self.objectio.object_to_cached_cuboid_keys([object_key])
        self.kvio.put_cubes(key_list, [data])

        # Notify complete
        self.cache_state.notify_page_in_complete(page_in_channel, key_list[0])

    # Status Methods
    def resource_locked(self, lookup_key):
        """
        Method to check if a given channel is locked for writing due to an error

        Args:
            lookup_key (str): Lookup key for a channel

        Returns:
            (bool): True if the channel is locked, false if not
        """
        return self.cache_state.project_locked(lookup_key)

    # Private Cube Processing Methods
    def _up_sample_cutout(self, resource, corner, extent, resolution):
        """Transform coordinates of a base resolution cutout to a lower res level by up-sampling.

        Only applicable to annotation channels.

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
                            ErrorCodes.FUTURE)

        # Create namedtuple so you can return multiple things
        result_tuple = namedtuple('ResampleCoords',
                                  ['corner', 'extent', 'x_pixel_offset', 'y_pixel_offset'])

        # Get base resolution for the annotation channel
        base_res = resource.get_channel().base_resolution

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

        Only applicable to Annotation Channels.

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

        # Get base resolution for the annotation channel
        base_res = resource.get_channel().base_resolution

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

        corner represents the location of the cutout and extent the size.  As an example in 1D, if asking for
        a corner of 3 and extent of 2, this would be the values at 3 and 4.

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            corner ((int, int, int)): the xyz location of the corner of the cutout
            extent ((int, int, int)): the xyz extents
            resolution (int): the resolution level
            time_sample_range list((int)):  a range of time samples to get [start, stop). Default is [0,1) if omitted

        Returns:
            cube.Cube: The cutout data stored in a Cube instance
        """
        boss_logger = BossLogger()
        boss_logger.setLevel("info")
        blog = boss_logger.logger

        if not time_sample_range:
            # If not time sample list defined, used default of 0
            time_sample_range = [0, 1]

        # if cutout is below resolution, get a smaller cube and scaleup
        # ONLY FOR ANNO CHANNELS - if data is missing on the current resolution but exists else where...extrapolate
        # resource.get_channel().base_resolution is the "base" resolution and you assume data exists there.
        # If propagated you don't have to worry about this.
        # currently we don't upsample annotations when hardening the database, so don't need to check for propagated.

        # Create namedtuple for consistency with re-sampling paths through the code
        result_tuple = namedtuple('ResampleCoords',
                                  ['corner', 'extent', 'x_pixel_offset', 'y_pixel_offset'])

        # Check if you need to scale a cutout due to off-base resolution cutouts/propagation state
        channel = resource.get_channel()
        if not channel.is_image():
            # The channel is an annotation!
            base_res = channel.base_resolution

            if base_res > resolution:
                raise SpdbError('Not Implemented',
                                'Dynamic resolution scaling not yet implemented.',
                                ErrorCodes.FUTURE)
                # Must up-sample cutout dynamically find the effective dimensions of the up-sampled cutout
                #cutout_coords = self._up_sample_cutout(resource, corner, extent, resolution)

                #[x_cube_dim, y_cube_dim, z_cube_dim] = cube_dim = CUBOIDSIZE[base_res]
                #cutout_resolution = base_res

            elif not channel.is_image() and base_res < resolution and not resource.is_propagated():
                raise SpdbError('Not Implemented',
                                'Dynamic resolution scaling not yet implemented.',
                                ErrorCodes.FUTURE)
                # If cutout is an annotation channel, above base resolution (lower res), and NOT propagated, down-sample
                #cutout_coords = self._down_sample_cutout(resource, corner, extent, resolution)

                #[x_cube_dim, y_cube_dim, z_cube_dim] = cube_dim = CUBOIDSIZE[base_res]
                #cutout_resolution = base_res
            else:
                # this is the default path when not DYNAMICALLY scaling the resolution

                # get the size of the image and cube
                [x_cube_dim, y_cube_dim, z_cube_dim] = cube_dim = CUBOIDSIZE[resolution]
                cutout_resolution = resolution

                # Create namedtuple for consistency with re-sampling paths through the code
                cutout_coords = result_tuple(corner, extent, None, None)
        else:
            # Resource is an image channel, so no re-sampling
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

        # Initialize the final output cube (before trim operation since adding full cuboids)
        out_cube = Cube.create_cube(resource,
                                    [x_num_cubes * x_cube_dim, y_num_cubes * y_cube_dim, z_num_cubes * z_cube_dim],
                                    time_sample_range)

        # Build a list of indexes to access
        # TODO: Move this for loop directly into c-lib
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

        # Get index of missing keys for cuboids to read
        missing_key_idx, cached_key_idx, all_keys = self.kvio.get_missing_read_cache_keys(resource,
                                                                                          cutout_resolution,
                                                                                          time_sample_range,
                                                                                          list_of_idxs)
        # Wait for cuboids that are currently being written to finish
        start_time = datetime.now()
        dirty_keys = all_keys
        blog.debug("Waiting for {} writes to finish before read can complete".format(len(dirty_keys)))
        while dirty_keys:
            dirty_flags = self.kvio.is_dirty(dirty_keys)
            dirty_keys_temp, clean_keys = [], []
            for key, flag in zip(dirty_keys, dirty_flags):
                (dirty_keys_temp if flag else clean_keys).append(key)
            dirty_keys = dirty_keys_temp

            if (datetime.now() - start_time).seconds > self.dirty_read_timeout:
                # Took too long! Something must have crashed
                raise SpdbError('{} second timeout reached while waiting for dirty cubes to be flushed.'.format(
                    self.dirty_read_timeout),
                                ErrorCodes.ASYNC_ERROR)
            # Sleep a bit so you don't kill the DB
            time.sleep(0.05)

        s3_key_idx = []
        cache_cuboids = []
        s3_cuboids = []
        zero_cuboids = []
        if len(missing_key_idx) > 0:
            # There are keys that are missing in the cache
            # Get index of missing keys that are in S3
            s3_key_idx, zero_key_idx = self.objectio.cuboids_exist(all_keys, missing_key_idx)

            if len(s3_key_idx) > 0:
                blog.debug("Data missing from cache, but present in S3")

                if len(s3_key_idx) > self.read_lambda_threshold:
                    # Trigger page-in of available blocks from object store and wait for completion
                    blog.debug("Triggering Lambda Page-in")
                    self.page_in_cubes(itemgetter(*s3_key_idx)(all_keys))
                else:
                    # Read cuboids from S3 into cache directly
                    # Convert cuboid-cache keys to object keys
                    blog.debug("Paging-in Keys Directly")
                    temp_keys = self.objectio.cached_cuboid_to_object_keys(itemgetter(*s3_key_idx)(all_keys))

                    # Get objects
                    temp_cubes = self.objectio.get_objects(temp_keys)

                    # write to cache
                    blog.debug("put keys on direct page in: {}".format(itemgetter(*s3_key_idx)(all_keys)))
                    self.kvio.put_cubes(itemgetter(*s3_key_idx)(all_keys), temp_cubes)

            if len(zero_key_idx) > 0:
                blog.debug("Data missing in cache, but not in S3")

                # Keys that don't exist in object store render as zeros
                [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]
                for idx in zero_key_idx:
                    parts, m_id = all_keys[idx].rsplit("&", 1)
                    _, t_start = parts.rsplit("&", 1)
                    temp_cube = Cube.create_cube(resource, [x_cube_dim, y_cube_dim, z_cube_dim], [int(t_start), int(t_start) + 1])
                    temp_cube.morton_id = int(m_id)
                    temp_cube.zeros()
                    zero_cuboids.append(temp_cube)

        # Get cubes from the cache database (either already there or freshly paged in)
        # TODO: Optimize access to cache data and checking for dirty cubes
        if len(s3_key_idx) > 0:
            blog.debug("Get cubes from cache that were paged in from S3")
            blog.debug(itemgetter(*s3_key_idx)(all_keys))

            s3_cuboids = self.get_cubes(resource, itemgetter(*s3_key_idx)(all_keys))

            # Record misses that were found in S3 for possible pre-fetching
            self.cache_state.add_cache_misses(itemgetter(*s3_key_idx)(all_keys))

        # Get previously cached cubes, waiting for dirty cubes to be updated if needed
        if len(cached_key_idx) > 0:
            blog.debug("Get cubes that were already present in the cache")

            # Get the cached keys once in list form
            cached_keys_list = itemgetter(*cached_key_idx)(all_keys)
            if isinstance(cached_keys_list, str):
                cached_keys_list = [cached_keys_list]
            if isinstance(cached_keys_list, tuple):
                cached_keys_list = list(cached_keys_list)

            # Split clean and dirty keys
            dirty_flags = self.kvio.is_dirty(cached_keys_list)
            dirty_keys, clean_keys = [], []
            for key, flag in zip(cached_keys_list, dirty_flags):
                (dirty_keys if flag else clean_keys).append(key)

            # Get all the clean cubes immediately, removing them from the list of cached keys to get
            for k in clean_keys:
                cached_keys_list.remove(k)
            cache_cuboids.extend(self.get_cubes(resource, clean_keys))

            # Get the dirty ones when you can with a timeout
            start_time = datetime.now()
            while dirty_keys:
                dirty_flags = self.kvio.is_dirty(cached_keys_list)
                dirty_keys, clean_keys = [], []
                for key, flag in zip(cached_keys_list, dirty_flags):
                    (dirty_keys if flag else clean_keys).append(key)

                if clean_keys:
                    # Some keys are ready now. Remove from list and get them
                    for k in clean_keys:
                        cached_keys_list.remove(k)
                    cache_cuboids.extend(self.get_cubes(resource, clean_keys))

                if (datetime.now() - start_time).seconds > self.dirty_read_timeout:
                    # Took too long! Something must have crashed
                    raise SpdbError('{} second timeout reached while waiting for dirty cubes to be flushed.'.format(self.dirty_read_timeout),
                                    ErrorCodes.ASYNC_ERROR)

                # Sleep a bit so you don't kill the DB
                time.sleep(0.05)

        # Add all cuboids (which have all time samples packed in already) to final cube of data
        for cube in cache_cuboids + s3_cuboids + zero_cuboids:
            # Compute offset so data inserted properly
            curxyz = ndlib.MortonXYZ(cube.morton_id)
            offset = [curxyz[0] - lowxyz[0], curxyz[1] - lowxyz[1], curxyz[2] - lowxyz[2]]

            # add it to the output cube
            out_cube.add_data(cube, offset)

        # A smaller cube was cutout due to off-base resolution query: up-sample and trim
        base_res = channel.base_resolution
        if not channel.is_image() and base_res > resolution:
            # TODO: optimizing zoomData and rename when implementing propagate
            out_cube.zoomData(base_res - resolution)

            # need to trim based on the cube cutout at new resolution
            out_cube.trim(corner[0] % (x_cube_dim * (2 ** (base_res - resolution))) + cutout_coords.x_pixel_offset,
                          extent[0],
                          corner[1] % (y_cube_dim * (2 ** (base_res - resolution))) + cutout_coords.y_pixel_offset,
                          extent[1],
                          corner[2] % z_cube_dim,
                          extent[2])

        # A larger cube was cutout due to off-base resolution query: down-sample and trim
        elif not channel.is_image() and base_res < resolution and not resource.is_propagated():
            # TODO: optimizing downScale and rename when implementing propagate
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
            corner ((int, int, int)): the xyz location of the corner of the cutout
            resolution (int): the resolution level
            cuboid_data (numpy.ndarray): Matrix of data to write as cuboids
            time_sample_start (int): if cuboid_data.ndim == 3, the time sample for the data
                                     if cuboid_data.ndim == 4, the time sample for cuboid_data[0, :, :, :]

        Returns:
            None
        """
        boss_logger = BossLogger()
        boss_logger.setLevel("info")
        blog = boss_logger.logger

        # Check if the resource is locked
        if self.resource_locked(resource.get_lookup_key()):
            raise SpdbError('Resource Locked',
                            'The requested resource is locked due to excessive write errors. Contact support.',
                            ErrorCodes.RESOURCE_LOCKED)

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
                            ErrorCodes.SPDB_ERROR)

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
                               dtype=cuboid_data.dtype, order="C")

        data_buffer[:, z_offset:z_offset + dim[2],
                       y_offset:y_offset + dim[1],
                       x_offset:x_offset + dim[0]] = cuboid_data

        # Get keys ready
        base_write_cuboid_key = "WRITE-CUBOID&{}&{}".format(resource.get_lookup_key(), resolution)

        blog.info("Writing Cuboid - Base Key: {}".format(base_write_cuboid_key))

        # Get current cube from db, merge with new cube, write back to the to db
        # TODO: Move splitting up data and computing morton into c-lib as single method
        for z in range(z_num_cubes):
            for y in range(y_num_cubes):
                for x in range(x_num_cubes):
                    # Get the morton ID for the cube
                    morton_idx = ndlib.XYZMorton([x + x_start, y + y_start, z + z_start])

                    # Get sub-cube
                    temp_cube = Cube.create_cube(resource, [x_cube_dim, y_cube_dim, z_cube_dim],
                                                 [time_sample_start, time_sample_stop])
                    temp_cube.data = np.ascontiguousarray(data_buffer[:,
                                                          z * z_cube_dim:(z + 1) * z_cube_dim,
                                                          y * y_cube_dim:(y + 1) * y_cube_dim,
                                                          x * x_cube_dim:(x + 1) * x_cube_dim], dtype=data_buffer.dtype)

                    # For each time sample put cube into write-buffer and add to temp page out key
                    for t in range(time_sample_start, time_sample_stop):
                        # Add cuboid to write buffer
                        write_cuboid_key = self.kvio.insert_cube_in_write_buffer(base_write_cuboid_key, t, morton_idx,
                                                                                 temp_cube.to_blosc_by_time_index(t))

                        # Page Out Attempt Loop
                        temp_page_out_key = "TEMP&{}".format(uuid.uuid4().hex)
                        # Check for page out
                        if self.cache_state.in_page_out(temp_page_out_key, resource.get_lookup_key(),
                                                        resolution, morton_idx, t):
                            blog.info("Writing Cuboid - Delayed Write: {}".format(write_cuboid_key))
                            # Delay Write!
                            self.cache_state.add_to_delayed_write(write_cuboid_key,
                                                                  resource.get_lookup_key(),
                                                                  resolution,
                                                                  morton_idx,
                                                                  t,
                                                                  resource.to_json())
                            # You are done. continue
                        else:
                            # Attempt to get write slot by checking page out
                            in_page_out = self.cache_state.add_to_page_out(temp_page_out_key,
                                                                           resource.get_lookup_key(),
                                                                           resolution,
                                                                           morton_idx,
                                                                           t)

                            if not in_page_out:
                                # Good to trigger lambda!
                                blog.debug("Writing Cuboid - Trying to trigger page out")
                                self.objectio.trigger_page_out({"kv_config": self.kv_config,
                                                                "state_config": self.state_conf,
                                                                "object_store_config": self.object_store_config},
                                                               write_cuboid_key,
                                                               resource)
                                blog.info("Writing Cuboid - Triggered Page Out: {}".format(write_cuboid_key))
                                # All done. continue.
                            else:
                                # Ended up in page out during transaction. Make delayed write.
                                blog.info("Writing Cuboid - Delayed Write: {}".format(write_cuboid_key))
                                self.cache_state.add_to_delayed_write(write_cuboid_key,
                                                                      resource.get_lookup_key(),
                                                                      resolution,
                                                                      morton_idx,
                                                                      t, resource.to_json())

    def get_bounding_box(self, resource, resolution, id, bb_type='loose'):
        """
        Get the bounding box that contains the object labeled with id.

        Args:
            resource (project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            id (uint64|string): object's id
            bb_type (optional[string]): 'loose' | 'tight'. Defaults to 'loose'

        Returns:
            (dict): {'x_range': [0, 10], 'y_range': [0, 10], 'z_range': [0, 10], 't_range': [0, 10]}

        Raises:
            (SpdbError): Can't talk to id index database or database corrupt.
        """
        return self.objectio.get_bounding_box(resource, resolution, id, bb_type)

    def reserve_ids(self, resource, num_ids, version=0):
        """Method to reserve a block of ids for a given channel at a version.

        Args:
            resource (spdb.project.resource.BossResource): Data model info based on the request or target resource.
            num_ids (int): Number of IDs to reserve
            version (optional[int]): Defaults to zero, reserved for future use.

        Returns:
            (np.array): starting ID for the block of ID successfully reserved as a numpy array to insure uint64
        """
        return self.objectio.reserve_ids(resource, num_ids, version)


