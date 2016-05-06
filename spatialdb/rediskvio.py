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

import redis
import itertools

from spdb.c_lib.ndtype import CUBOIDSIZE

from .error import SpdbError, ErrorCode
from .kvio import KVIO
from .cube import Cube

from bossutils import configuration


class RedisKVIO(KVIO):
    def __init__(self, cache_client=None, status_client=None):
        """Connect to the Redis backend"""

        # call the base class constructor
        KVIO.__init__(self)

        # Get the boss config to find redis db urls
        config = configuration.BossConfig()

        # Get the redis clients
        if cache_client:
            self.cache_client = cache_client
        else:
            self.cache_client = redis.StrictRedis(host=config["aws"]["cache"], port=6379,
                                                  db=config["aws"]["cache-db"])

        if status_client:
            self.status_client = status_client
        else:
            self.status_client = redis.StrictRedis(host=config["aws"]["cache-state"], port=6379,
                                                   db=config["aws"]["cache-state-db"])

    def close(self):
        """Close the connection to the KV engine"""
        pass

    def start_txn(self):
        """Start a transaction. Ensure database is in multi-statement mode.

        No transactions with redis

        """
        pass

    def commit(self):
        """Commit the transaction. Moved out of __del__ to make explicit.

        No transactions with redis"""
        pass

    def rollback(self):
        """Rollback the transaction. To be called on exceptions.

        No transactions with redis"""
        pass

    def generate_cuboid_index_key(self, resource, resolution):
        """Generate the key used to store index of cuboids that are present in the cache

        Cuboids are indexed by collection/experiment/channel_layer/resolution with the values in the set being a
        combination of the time sample and morton ID.

        """
        return 'CUBOID_IDX&{}&{}'.format(resource.get_lookup_key(), resolution)

    def generate_cuboid_data_keys(self, resource, resolution, time_sample_list, morton_idx_list):
        """Generate Keys for cuboid storage in the redis cache db

        The keys are ordered by time sample followed by morton ID (e.g. 1&1, 1&2, 1&3, 2&1, 2&2, 2&3)

        The key contains the base lookup key with the time samples and morton ids appended with the format:

            CUBOID&{lookup_key}&time_sample&morton_id

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            morton_idx_list (list[int]): a list of Morton ID of the cuboids to get
            time_sample_list (list[int]): a list of time samples of the cuboids to get

        Returns:
            list[str]: A list of keys for each cuboid

        """
        base_key = 'CUBOID&{}&{}'.format(resource.get_lookup_key(), resolution)

        # Get the combinations of time and morton, properly ordered
        key_suffix_list = itertools.product(time_sample_list, morton_idx_list)

        # Return a list of all keys
        return ['{}&{}&{}'.format(base_key, s[0], s[1]) for s in key_suffix_list]

    def get_missing_cube_index(self, resource, resolution, time_sample_list, morton_idx_list):
        """Retrieve the indexes of missing cubes in the cache db based on a morton ID list and time samples

        The index of cuboids in the cache is maintained by a compound value of the time sample and morton ID in a
        redis set for each {lookup_key}&resolution as the key of the set.

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            time_sample_list (list[int]): a list of time samples
            morton_idx_list (list[int]): a list of Morton ID of the cuboids to get

        Returns:
            list[str]: A list of cuboid index values to query
        """
        # Get the key of the SET that stores the index
        index_key = self.generate_cuboid_index_key(resource, resolution)
        desired_index_key = '{}_temp'.format(self.generate_cuboid_index_key(resource, resolution))

        # Generate index values for all requested cuboids
        index_vals = ["{}&{}".format(x[0], x[1]) for x in itertools.product(time_sample_list, morton_idx_list)]

        try:
            # Add expected values to a temp key
            self.status_client.sadd(desired_index_key, *index_vals)

            # Perform server side set operation (index_key contents can be huge so currently this makes sense)
            idxs_to_fetch = list(self.status_client.sdiff(desired_index_key, index_key))

            # Remove temporary set
            self.status_client.delete(desired_index_key)

        except Exception as e:
            raise SpdbError("Redis Error", "Error retrieving cube indexes into the database. {}".format(e),
                            ErrorCode.REDIS_ERROR)

        return idxs_to_fetch

    def index_to_time_and_morton(self, index_list):
        """ Method to convert cuboid index values (time_sample&morton_id) to a list of time samples and morton ids
        that are readily consumed by get_cubes

        Args:
            index_list (list(str)): A list of index values from the cache status db

        Returns:
            (list(int), list(int)): A tuple of two lists containing the time samples and morton ids
        """
        # Split index values into time samples and morton IDs
        missing_time_samples, missing_morton_ids = zip(*(int(value.split("&")) for value in index_list))
        missing_time_samples = list(set(missing_time_samples))
        missing_morton_ids = list(set(missing_morton_ids))

        return missing_time_samples, missing_morton_ids

    def put_cube_index(self, resource, resolution, time_sample_list, morton_idx_list):
        """Add cuboid indices that are loaded into the cache db

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            time_sample_list (list(int)): a list of time samples for the cubes that have that have been inserted
            morton_idx_list (list(int)): a list of Morton IDs for the cubes that have that have been inserted

        Returns:
            None
        """
        # Generate index values for all cuboids
        index_vals = ["{}&{}".format(x[0], x[1]) for x in itertools.product(time_sample_list, morton_idx_list)]

        try:
            # Add index values to the set
            self.status_client.sadd(self.generate_cuboid_index_key(resource, resolution), *index_vals)

        except Exception as e:
            raise SpdbError("Redis Error", "Error inserting cube indexes into the database. {}".format(e),
                            ErrorCode.REDIS_ERROR)

    def get_cubes(self, resource, resolution, time_sample_list, morton_idx_list):
        """Retrieve multiple cubes from the cache database, yield one at a time via generator

        Cubes are returned in order, by incrementing time sample followed by incrementing morton ID

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            morton_idx_list (list(int)): the list of Morton IDs of the cuboids to get
            time_sample_list (list(int)): list of time sample points

        Returns:
            (int, int, bytes): A tuple of the time sample, morton index, and the blosc compressed byte array using
             the numpy interface
        """
        try:
            # Get the data from the DB
            rows = self.cache_client.mget(self.generate_cuboid_data_keys(resource,
                                                                         resolution,
                                                                         time_sample_list,
                                                                         morton_idx_list))
        except Exception as e:
            raise SpdbError("Redis Error", "Error retrieving cubes from the cache database. {}".format(e),
                            ErrorCode.REDIS_ERROR)

        # TODO: Currently, since no S3 integration, if missing in cache generate just an all 0 cube
        [x_cube_dim, y_cube_dim, z_cube_dim] = CUBOIDSIZE[resolution]

        # Yield the resulting cuboids as RAW blosc compressed numpy arrays
        index_values = itertools.product(time_sample_list, morton_idx_list)
        time_samples, morton_ids = zip(*index_values)
        for time, idx, row in zip(time_samples, morton_ids, rows):
            if not row:
                temp_cube = Cube.create_cube(resource, [x_cube_dim, y_cube_dim, z_cube_dim])
                temp_cube.zeros()
                row = temp_cube.to_blosc_numpy()
            # end blank cube shim

            yield (time, idx, row)

    def put_cubes(self, resource, resolution, time_sample_list, morton_idx_list, cube_list, update=False):
        """Store multiple cubes in the cache database

        The length of time_sample_list, morton_idx_list, and cube_list should all be the same and linked together
        (e.g. time_sample_list[12] and morton_idx_list[12] are for the data in cube_list[12]

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            morton_idx_list (list[int]): a list of Morton ID of the cuboids to get
            cube_list: list of cubes in a blosc compressed byte arrays using the numpy interface
            update: ???

        Returns:

        """
        # Generate the list of keys
        key_list = self.generate_cuboid_data_keys(resource, resolution, time_sample_list, morton_idx_list)

        try:
            # Write data to redis
            self.cache_client.mset(dict(list(zip(key_list, cube_list))))
        except Exception as e:
            raise SpdbError("Redis Error", "Error inserting cubes into the cache database. {}".format(e),
                            ErrorCode.REDIS_ERROR)

    # TODO: Add if needed.  DMK pretty sure this was from OCPBlaze and not needed at the moment
    #def put_cube(self, resource, resolution, morton_idx, cube_bytes, update=False):
    #    """Store a single cube into the database

    #    Args:
    #        resource (spdb.project.BossResource): Data model info based on the request or target resource
    #        resolution (int): the resolution level
    #        morton_idx (int): the Morton ID of the cuboid to get
    #        cube_bytes (bytes): a cube in a blosc compressed byte array using the numpy interface
    #        update:

    #    Returns:
    #        None
    #    """
    #    if not isinstance(morton_idx, list):
    #        morton_idx = [morton_idx]

    #    # Generate the cuboid key
    #    cuboid_key = self.generate_cuboid_data_keys(resource, resolution, morton_idx)
    #    cuboid_index_key = self.generate_cache_index_keys(resource, resolution)

    #    try:
    #        # Put cuboid
    #        # Update cuboid index
    #        self.put_cube_index(resource, resolution, morton_idx)

    #    except Exception as e:
    #        raise SpdbError("Redis Error", "Error inserting cube into the cache database. {}".format(e),
    #                        ErrorCode.REDIS_ERROR)

    #def get_cube(self, resource, resolution, morton_idx):
    #    """Retrieve a single cuboid from the cache database

    #    Args:
    #        resource (spdb.project.BossResource): Data model info based on the request or target resource
    #        resolution (int): the resolution level
    #        morton_idx (int): the Morton ID of the cuboid to get

    #    Returns:

    #    """

    #    try:
    #        rows = self.cache_client.mget(self.generate_cuboid_data_keys(resource, resolution, [morton_idx]))
    #    except Exception as e:
    #        raise SpdbError("Redis Error", "Error retrieving cubes from the cache database. {}".format(e),
    #                        ErrorCode.REDIS_ERROR)

    #    if rows[0]:
    #        return rows[0]
    #    else:
    #        return None
