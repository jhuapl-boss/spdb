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

from spdb.kvio import KVIO
import redis

from spdb import SpdbError, ErrorCode


class RedisKVIO(KVIO):
    def __init__(self, testing=False):
        """Connect to the Redis backend"""

        # call the base class constructor
        KVIO.__init__(self)

        if testing:
            pass
        else:
            # TODO: Ask derek where the connection info is for redis
            self.cache_client = redis.StrictRedis(host=self.db.proj.getDBHost(), port=6379, db=0)
            self.status_client = redis.StrictRedis(host=self.db.proj.getDBHost(), port=6379, db=0)

        self.cache_pipe = self.cache_client.pipeline(transaction=False)
        self.status_pipe = self.status_client.pipeline(transaction=False)

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
        """Generate the base name of the key used to store status about cuboids in the cache"""
        return 'CUBOID_IDX&{}_{}'.format(resource.get_lookup_key(), resolution)

    def generate_keys(self, resource, resolution, morton_idx_list):
        """Generate Keys for storing data in the redis cache db

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            morton_idx_list (list[int]): a list of Morton ID of the cuboids to get

        Returns:
            list[str]: A list of keys for each item

        """
        key_list = []
        for sample in resource.get_time_samples():
            for z_idx in morton_idx_list:
                key_list.append(
                    '{}&{}&Z{}&T{}'.format(resource.get_lookup_key(), resolution, z_idx, sample))

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

        index_store = self.get_cache_index_base_key(resource, resolution)
        index_store_temp = index_store + '_temp'

        try:
            self.client.sadd(index_store_temp, *list(zip(resource.get_time_samples(), morton_idx_list)))

            ids_to_fetch = self.client.sdiff(index_store_temp, index_store)

            self.client.delete(index_store_temp)
        except Exception as e:
            raise SpdbError("Redis Error", "Error retrieving cube indexes into the database. {}".format(e),
                            ErrorCode.REDIS_ERROR)

        return list(ids_to_fetch)

    def put_cube_index(self, resource, resolution, morton_idx_list):
        """Add cuboid indicies that are loaded into the cache db

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            morton_idx_list (list[int]): a list of Morton ID of the cuboids to get

        Returns:
            None
        """

        try:
            self.client.sadd(self.get_cache_index_base_key(resource, resolution),
                             *list(zip(resource.get_time_samples(), morton_idx_list)))
        except Exception as e:
            raise SpdbError("Redis Error", "Error inserting cube indexes into the database. {}".format(e),
                            ErrorCode.REDIS_ERROR)

    def get_cube(self, resource, resolution, morton_idx, update=False):
        """Retrieve a single cuboid from the cache database

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            morton_idx (int): the Morton ID of the cuboid to get
            update:

        Returns:

        """
        # TODO: Add tracking of access time for LRU eviction

        try:
            rows = self.client.mget(self.generate_keys(resource, resolution, [morton_idx]))
        except Exception as e:
            raise SpdbError("Redis Error", "Error retrieving cubes from the cache database. {}".format(e),
                            ErrorCode.REDIS_ERROR)

        if rows[0]:
            return rows[0]
        else:
            return None

    def get_cubes(self, resource, resolution, morton_idx_list):
        """Retrieve multiple cubes from the cache database

        Args:s
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            morton_idx (int): the Morton ID of the cuboid to get

        Returns:
            (int, bytes): A tuple of the morton index and the blosc compressed byte array using the numpy interface
        """

        try:
            rows = self.client.mget(self.generate_keys(resource, resolution, morton_idx_list))
        except Exception as e:
            raise SpdbError("Redis Error", "Error retrieving cubes from the cache database. {}".format(e),
                            ErrorCode.REDIS_ERROR)

        for idx, row in zip(morton_idx_list, rows):
            yield (idx, row)

    #ef getTimeCubes(self, ch, idx, listoftimestamps, resolution):
    #   """Retrieve multiple cubes from the database"""

    #   try:
    #       rows = self.client.mget(self.generate_keys(ch, resolution, [idx], listoftimestamps))
    #   except Exception as e:
    #       logger.error("Error inserting cubes into the database. {}".format(e))
    #       raise SpatialDBError("Error inserting cubes into the database. {}".format(e))

    #   for idx, timestamp, row in zip([idx] * len(listoftimestamps), listoftimestamps, rows):
    #       yield (idx, timestamp, row)

    def put_cube(self, resource, resolution, morton_idx, cube_bytes, update=False):
        """Store a single cube into the database

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            morton_idx (int): the Morton ID of the cuboid to get
            cube_bytes (bytes): a cube in a blosc compressed byte array using the numpy interface
            update:

        Returns:
            None
        """

        # generating the key
        key_list = self.generate_keys(resource, resolution, [morton_idx])

        try:
            self.client.mset(dict(list(zip(key_list, [cube_bytes]))))
        except Exception as e:
            raise SpdbError("Redis Error", "Error inserting cube into the cache database. {}".format(e),
                            ErrorCode.REDIS_ERROR)

    def put_cubes(self, resource, resolution, morton_idx_list, cube_list, update=False):
        """Store multiple cubes into the database

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            morton_idx_list (list[int]): a list of Morton ID of the cuboids to get
            cube_list: list of cubes in a blosc compressed byte arrays using the numpy interface
            update: ???

        Returns:

        """

        # generating the list of keys
        key_list = self.generate_keys(resource, resolution, morton_idx_list)

        try:
            self.client.mset(dict(list(zip(key_list, cube_list))))
        except Exception as e:
            raise SpdbError("Redis Error", "Error inserting cubes into the cache database. {}".format(e),
                            ErrorCode.REDIS_ERROR)
