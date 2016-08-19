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
import uuid

from .error import SpdbError, ErrorCodes
from .kvio import KVIO


class RedisKVIO(KVIO):
    def __init__(self, kv_conf):
        """Connect to the Redis backend

        Params in the kv_config dictionary:
            cache_client: Optional instance of a redis client that will be used directly
            cache_host: If cache_client not provided, a string indicating the database host
            cache_db: If cache_client not provided, an integer indicating the database to use
            read_timeout: Integer indicating number of seconds a read cache key expires
        """
        # call the base class constructor
        KVIO.__init__(self, kv_conf)

        # If a client instance was provided, use it. Otherwise configure a new client
        if "cache_client" in self.kv_conf:
            if self.kv_conf["cache_client"]:
                self.cache_client = self.kv_conf["cache_client"]
            else:
                self.cache_client = redis.StrictRedis(host=self.kv_conf["cache_host"], port=6379,
                                                      db=self.kv_conf["cache_db"])
        else:
            self.cache_client = redis.StrictRedis(host=self.kv_conf["cache_host"], port=6379,
                                                  db=self.kv_conf["cache_db"])

    def close(self):
        """Close the connection to the KV engine

        No close op for redis
        """
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

        No rollback with redis"""
        pass

    def get_missing_read_cache_keys(self, resource, resolution, time_sample_range, morton_idx_list):
        """Retrieve the indexes of missing cubes in the cache db based on a morton ID list and time samples

        When using redis as the cache backend, you don't need to keep a secondary index and can get this info
        directly from redis.

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            time_sample_range (list[int]): the start and stop index of the time samples
            morton_idx_list (list[int]): a list of Morton ID of the cuboids to get

        Returns:
            (list(int), list(int), list(str)): A tuple of lists with the first being an index of missing keys
            the second a list of keys in the cache, and the third being the cached-cuboid keys for the query
        """
        # Get the cached-cuboid keys
        all_cuboid_keys = self.generate_cached_cuboid_keys(resource, resolution,
                                                           list(range(*time_sample_range)), morton_idx_list)

        # Query Redis for key existence, refreshing the cache timeout if exists
        try:
            pipe = self.cache_client.pipeline()
            pipe.multi()

            # Build check
            for key in all_cuboid_keys:
                pipe.expire(key, self.kv_conf["read_timeout"])
                pipe.exists(key)

            # Run Pipelined commands
            result = pipe.execute()

        except Exception as e:
            raise SpdbError("Error retrieving cube indexes into the database. {}".format(e),
                            ErrorCodes.REDIS_ERROR)

        # Parse Response
        missing_key_idx = []
        cached_key_idx = []
        for idx, key in enumerate(all_cuboid_keys):
            if not result[idx*2]:
                missing_key_idx.append(idx)
            else:
                cached_key_idx.append(idx)

        return missing_key_idx, cached_key_idx, all_cuboid_keys

    def get_cubes(self, key_list):
        """Retrieve multiple cubes from the cache database

        Cubes are returned in order matching the key_list provided to the method

        Args:
            key_list (list(str)): the list of cuboid keys to read from the database

        Returns:
            (int, int, bytes): A tuple of the morton id, time sample and the blosc compressed byte array using the
             numpy interface
        """
        try:
            # Get the data from the DB
            rows = self.cache_client.mget(key_list)
        except Exception as e:
            raise SpdbError("Error retrieving cuboids from the cache database. {}".format(e),
                            ErrorCodes.REDIS_ERROR)

        result = []
        for key, data in zip(key_list, rows):
            if not data:
                raise SpdbError("Received unexpected empty cuboid. {}".format(key),
                                ErrorCodes.REDIS_ERROR)
            vals = key.split("&")
            result.append((int(vals[-1]), int(vals[-2]), data))

        return result

    def cube_exists(self, key):
        """Check if a cube exists in the cache by checking it's key

        Args:
            key (str): the cuboid key to check

        Returns:
            (bool): A boolean indicating if they key exists
        """
        try:
            # Check in a transaction so you can reset the ttl for the key
            pipe = self.cache_client.pipeline()
            pipe.multi()
            pipe.expire(key, self.kv_conf["read_timeout"])
            pipe.exists(key)

            # Run Pipelined commands
            result = pipe.execute()
        except Exception as e:
            raise SpdbError("Error retrieving cuboid status from cache database. {}".format(e),
                            ErrorCodes.REDIS_ERROR)

        return result[1]

    def delete_cube(self, key):
        """Delete a cube from the cache db

        Args:
            key (str): the cuboid key to remove

        Returns:
            None
        """
        try:
            # Get the data from the DB
            result = self.cache_client.delete(key)
        except Exception as e:
            raise SpdbError("Error deleting cuboids from the cache database. {}".format(e),
                            ErrorCodes.REDIS_ERROR)

        return result

    def put_cubes(self, key_list, cube_list):
        """Store multiple cubes in the cache database

        The key_list values should correspond to the cubes in cube_list

        Args:
            key_list (list(str)): a list of Morton ID of the cuboids to get
            cube_list (list(bytes)): list of cubes in a blosc compressed byte arrays using the numpy interface

        Returns:

        """
        if isinstance(key_list, str):
            key_list = [key_list]

        try:
            # Write data to redis
            self.cache_client.mset(dict(list(zip(key_list, cube_list))))

            # Set expire times
            for key in key_list:
                self.cache_client.expire(key, self.kv_conf["read_timeout"])

        except Exception as e:
            raise SpdbError("Error inserting cubes into the cache database. {}".format(e),
                            ErrorCodes.REDIS_ERROR)

    def insert_cube_in_write_buffer(self, base_key, time_sample, morton_id, data):
        """Store a single cube (single time point) in the write buffer

        Args:
            base_key (str): the base write-buffer key (does not include uuid)
            data (bytes): cube in a blosc compressed byte arrays using the numpy interface

        Returns:
            (str): The complete write-buffer key
        """
        # TODO: Move to parent class if this method sticks after optimization of write_cuboid method
        # Create write buffer key
        key = "{}&{}&{}&{}".format(base_key, time_sample, morton_id, uuid.uuid4().hex)

        try:
            # Write data to redis
            self.cache_client.set(key, data)

            return key

        except Exception as e:
            raise SpdbError("Error inserting cube into the write buffer. {}".format(e),
                            ErrorCodes.REDIS_ERROR)

    def get_cube_from_write_buffer(self, write_cuboid_key):
        """Retrieve a single cube from the write buffer

        Args:
            write_cuboid_key (str): the list of cuboid keys to read from the database

        Returns:
            (bytes): The blosc compressed byte array using the numpy interface
        """
        try:
            # Get the data from the DB
            return self.cache_client.get(write_cuboid_key)
        except Exception as e:
            raise SpdbError("Error retrieving cuboid from the write buffer. {}".format(e),
                            ErrorCodes.REDIS_ERROR)

    def is_dirty(self, cache_key_list):
        """
        Check if a cuboid is dirty based on its cache key
        Args:
            cache_key_list (list(str)): A list of cached-cuboid keys

        Returns:
            (list(bool)): A list of booleans, indicating if each key is dirty or not
        """
        if isinstance(cache_key_list, str):
            cache_key_list = [cache_key_list]

        # Convert cached-keys to write-cuboid keys without UUID
        # (we can't recreate UUIDs and want to check for ALL write-cuboid keys for a given cuboid anyway)
        result = None
        with self.cache_client.pipeline() as pipe:
            for key in cache_key_list:
                parts = key.split("&", 1)
                pipe.keys('WRITE-CUBOID&{}*'.format(parts[1]))

            result = pipe.execute()

        # Check all keys in a pipelined command
        return [len(x) > 0 for x in result]


# TODO: CHECK IF THIS METHOD CAN BE REMOVED AFTER REFACTOR
#    def index_to_time_and_morton(self, index_list):
#        """ Method to convert cuboid index values (time_sample&morton_id) to a list of time samples and morton ids
#        that are readily consumed by get_cubes
#
#        Args:
#            index_list (list(str)): A list of index values from the cache status db
#
#        Returns:
#            (list(int), list(int)): A tuple of two lists containing the time samples and morton ids
#        """
#        # Split index values into time samples and morton IDs
#        missing_time_samples, missing_morton_ids = zip(*(int(value.split("&")) for value in index_list))
#        missing_time_samples = list(set(missing_time_samples))
#        missing_morton_ids = list(set(missing_morton_ids))
#
#        return missing_time_samples, missing_morton_ids
#
#        # TODO: CHECK IF THIS METHOD CAN BE REMOVED AFTER UPDATES
#
#
#    def put_cube_index(self, resource, resolution, time_sample_list, morton_idx_list):
#        """Add cuboid indices that are loaded into the cache db
#
#        Don't need to do anything for the redis backend
#
#        Args:
#            resource (spdb.project.BossResource): Data model info based on the request or target resource
#            resolution (int): the resolution level
#            time_sample_list (list(int)): a list of time samples for the cubes that have that have been inserted
#            morton_idx_list (list(int)): a list of Morton IDs for the cubes that have that have been inserted
#
#        Returns:
#            None
#        """
#        pass

