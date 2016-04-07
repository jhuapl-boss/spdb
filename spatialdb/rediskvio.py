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

from .error import SpdbError, ErrorCode
from .kvio import KVIO

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
        """Start a transaction. Ensure database is in multi-statement mode."""
        pass

    def commit(self):
        """Commit the transaction. Moved out of __del__ to make explicit."""
        pass

    def rollback(self):
        """Rollback the transaction. To be called on exceptions."""
        pass

    def get_cache_index_base_key(self, resource, resolution):
        """Generate the base name of the key used to store status if cuboids are present in the cache"""
        return ['CUBOID_IDX&{}&{}'.format(key, resolution) for key in resource.get_lookup_key()]

    def get_cache_base_key(self, resource, resolution):
        """Generate the base name of the key used to store cuboids in the cache"""
        # TODO: Possibly update boss request to return the look up key so time isn't before resolution
        return ['CUBOID&{}&{}'.format(key, resolution) for key in resource.get_lookup_key()]

    def generate_cache_index_keys(self, resource, resolution):
        """Generate Keys for cuboid index sets in the redis cache db

        The key contains the base lookup key with the time samples included in the resource.

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level

        Returns:
            list[str]: A list of keys for each time sample index

        """
        return self.get_cache_index_base_key(resource, resolution)

    def generate_cuboid_keys(self, resource, resolution, morton_idx_list):
        """Generate Keys for cuboid access in the redis cache db

        The key contains the base lookup key with the time samples and morton ids

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            morton_idx_list (list[int]): a list of Morton ID of the cuboids to get

        Returns:
            list[str]: A list of keys for each cuboid

        """
        if not isinstance(morton_idx_list, list):
            morton_idx_list = [morton_idx_list]

        key_list = []
        for base_key in self.get_cache_base_key(resource, resolution):
            for z_idx in morton_idx_list:
                key_list.append('{}&{}'.format(base_key, z_idx))

        return key_list

    def get_missing_cube_index(self, resource, resolution, morton_idx_list):
        """Retrieve the indexes of missing cubes in the cache db

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            morton_idx_list (list[int]): a list of Morton ID of the cuboids to get

        Returns:
            list[str]: A list of cuboid keys to query
        """

        index_keys = self.generate_cache_index_keys(resource, resolution)
        index_store_temp = [x + '_temp' for x in index_keys]

        try:
            # Loop through each time sample, which has it's own index currently
            ids_to_fetch = []
            for index_key, temp_key in zip(index_keys, index_store_temp):
                # Add
                self.status_client.sadd(temp_key, *morton_idx_list)

                ids_to_fetch.extend(list(self.status_client.sdiff(temp_key, index_key)))

                self.status_client.delete(temp_key)
        except Exception as e:
            raise SpdbError("Redis Error", "Error retrieving cube indexes into the database. {}".format(e),
                            ErrorCode.REDIS_ERROR)

        return ids_to_fetch

    def put_cube_index(self, resource, resolution, morton_idx_list):
        """Add cuboid indicies that are loaded into the cache db

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            morton_idx_list (list[int]): a list of Morton ID of the cuboids to get

        Returns:
            None
        """
        resolution = 1
        try:
            for key in self.generate_cache_index_keys(resource, resolution):
                self.status_client.sadd(key, *morton_idx_list)

        except Exception as e:
            raise SpdbError("Redis Error", "Error inserting cube indexes into the database. {}".format(e),
                            ErrorCode.REDIS_ERROR)

    def get_cube(self, resource, resolution, morton_idx):
        """Retrieve a single cuboid from the cache database

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            morton_idx (int): the Morton ID of the cuboid to get

        Returns:

        """
        # TODO: Add tracking of access time for LRU eviction

        try:
            rows = self.cache_client.mget(self.generate_cuboid_keys(resource, resolution, [morton_idx]))
        except Exception as e:
            raise SpdbError("Redis Error", "Error retrieving cubes from the cache database. {}".format(e),
                            ErrorCode.REDIS_ERROR)

        if rows[0]:
            return rows[0]
        else:
            return None

    def get_cubes(self, resource, resolution, morton_idx_list):
        """Retrieve multiple cubes from the cache database, yield one at a time via generator

        Args:s
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            morton_idx (int): the Morton ID of the cuboid to get

        Returns:
            (int, bytes): A tuple of the morton index and the blosc compressed byte array using the numpy interface
        """

        try:
            rows = self.cache_client.mget(self.generate_cuboid_keys(resource, resolution, morton_idx_list))
        except Exception as e:
            raise SpdbError("Redis Error", "Error retrieving cubes from the cache database. {}".format(e),
                            ErrorCode.REDIS_ERROR)

        for idx, row in zip(morton_idx_list, rows):
            yield (idx, row)

    # TODO: Investigate if unique representation of time-series data is needed and if this is needed
    #def getTimeCubes(self, ch, idx, listoftimestamps, resolution):
    #   """Retrieve multiple cubes from the database"""

    #   try:
    #       rows = self.client.mget(self.generate_cache_index_keys(ch, resolution, [idx], listoftimestamps))
    #   except Exception as e:
    #       logger.error("Error inserting cubes into the database. {}".format(e))
    #       raise SpatialDBError("Error inserting cubes into the database. {}".format(e))

    #   for idx, timestamp, row in zip([idx] * len(listoftimestamps), listoftimestamps, rows):
    #       yield (idx, timestamp, row)

    # TODO: Add if needed.  DMK pretty sure this was from OCPBlaze and not needed at the moment
    #def put_cube(self, resource, resolution, morton_idx, cube_bytes, update=False):
    #    """Store a single cube into the database
#
    #    Args:
    #        resource (spdb.project.BossResource): Data model info based on the request or target resource
    #        resolution (int): the resolution level
    #        morton_idx (int): the Morton ID of the cuboid to get
    #        cube_bytes (bytes): a cube in a blosc compressed byte array using the numpy interface
    #        update:
#
    #    Returns:
    #        None
    #    """
    #    if not isinstance(morton_idx, list):
    #        morton_idx = [morton_idx]
#
    #    # Generate the cuboid key
    #    cuboid_key = self.generate_cuboid_keys(resource, resolution, morton_idx)
    #    cuboid_index_key = self.generate_cache_index_keys(resource, resolution)
#
    #    try:
    #        # Put cuboid
    #        # Update cuboid index
    #        self.put_cube_index(resource, resolution, morton_idx)
#
    #    except Exception as e:
    #        raise SpdbError("Redis Error", "Error inserting cube into the cache database. {}".format(e),
    #                        ErrorCode.REDIS_ERROR)

    def put_cubes(self, resource, resolution, morton_idx_list, cube_list, update=False):
        """Store multiple cubes in the cache database

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            morton_idx_list (list[int]): a list of Morton ID of the cuboids to get
            cube_list: list of cubes in a blosc compressed byte arrays using the numpy interface
            update: ???

        Returns:

        """

        # generating the list of keys
        key_list = self.generate_cuboid_keys(resource, resolution, morton_idx_list)
        try:
            self.cache_client.mset(dict(list(zip(key_list, cube_list))))
        except Exception as e:
            raise SpdbError("Redis Error", "Error inserting cubes into the cache database. {}".format(e),
                            ErrorCode.REDIS_ERROR)
