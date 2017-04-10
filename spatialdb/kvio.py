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

from abc import ABCMeta, abstractmethod
import itertools
import uuid


class KVIO(metaclass=ABCMeta):
    """An abstract base class for Key-Value engines that act as a cache DB
    """
    def __init__(self, kv_conf):
        """

        Args:
            kv_conf(dict): Dictionary containing configuration details for the key-value store
        """
        self.kv_conf = kv_conf

    @abstractmethod
    def close(self):
        """Close the connection to the KV engine"""
        return NotImplemented

    @abstractmethod
    def start_txn(self):
        """Start a transaction. Ensure database is in multi-statement mode."""
        return NotImplemented

    @abstractmethod
    def commit(self):
        """Commit the transaction. Moved out of __del__ to make explicit."""
        return NotImplemented

    @abstractmethod
    def rollback(self):
        """Rollback the transaction. To be called on exceptions."""
        return NotImplemented

    def generate_cached_cuboid_keys(self, resource, resolution, time_sample_list, morton_idx_list, iso=False):
        """Generate Keys for cuboids that are in the READ CACHE of the redis cache db

        The keys are ordered by time sample followed by morton ID (e.g. 1&1, 1&2, 1&3, 2&1, 2&2, 2&3)

        The key contains the base lookup key with the time samples and morton ids appended with the format:

            CACHED-CUBOID&{lookup_key}&time_sample&morton_id

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            morton_idx_list (list[int]): a list of Morton ID of the cuboids to get
            time_sample_list (list[int]): a list of time samples of the cuboids to get
            iso (bool): A flag indicating if you want the isotropic version of a channel

        Returns:
            list[str]: A list of keys for each cuboid

        """
        experiment = resource.get_experiment()
        if iso is True and resolution > resource.get_isotropic_level() and experiment.hierarchy_method.lower() == "anisotropic":
            base_key = 'CACHED-CUBOID&ISO&{}&{}'.format(resource.get_lookup_key(), resolution)
        else:
            base_key = 'CACHED-CUBOID&{}&{}'.format(resource.get_lookup_key(), resolution)

        # Get the combinations of time and morton, properly ordered
        key_suffix_list = itertools.product(time_sample_list, morton_idx_list)

        # Return a list of all keys
        return ['{}&{}&{}'.format(base_key, s[0], s[1]) for s in key_suffix_list]

    def generate_write_cuboid_keys(self, resource, resolution, time_sample_list, morton_idx_list):
        """Generate Keys for cuboids that are in the WRITE BUFFER of the redis cache db

        The keys are ordered by time sample followed by morton ID (e.g. 1&1, 1&2, 1&3, 2&1, 2&2, 2&3)

        The key contains the base lookup key with the time samples and morton ids appended with the format:

            WRITE-CUBOID&{lookup_key}&time_sample&morton_id&UUID

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            morton_idx_list (list[int]): a list of Morton ID of the cuboids to get
            time_sample_list (list[int]): a list of time samples of the cuboids to get

        Returns:
            list[str]: A list of keys for each cuboid

        """
        base_key = 'WRITE-CUBOID&{}&{}'.format(resource.get_lookup_key(), resolution)

        # Get the combinations of time and morton, properly ordered
        key_suffix_list = itertools.product(time_sample_list, morton_idx_list)

        # Return a list of all keys
        return ['{}&{}&{}&{}'.format(base_key, s[0], s[1], uuid.uuid4().__str__()) for s in key_suffix_list]

    def write_cuboid_key_to_cache_key(self, write_cuboid_key):
        """Converts a write cuboid key to a cache key

        The key contains the base lookup key with the time samples and morton ids appended with the format:

            WRITE-CUBOID&{lookup_key}&res&time_sample&morton_id&UUID
                                    to
            CACHED-CUBOID&{lookup_key}&res&time_sample&morton_id

        Args:
            write_cuboid_key (str): the write cuboid key to convert

        Returns:
            str: the associated cached cuboid key

        """
        parts = write_cuboid_key.split("&", 1)
        base = parts[1].rsplit("&", 1)[0]
        return 'CACHED-CUBOID&{}'.format(base)

    @abstractmethod
    def get_missing_read_cache_keys(self, resource, resolution, time_sample_list, morton_idx_list):
        """Return the cache-cuboid key list of cubes that are missing in the cache DB"""
        return NotImplemented

    @abstractmethod
    def get_cubes(self, key_list):
        """Retrieve multiple cubes from the database"""
        return NotImplemented

    @abstractmethod
    def put_cubes(self, key_list, cube_list):
        """Store multiple cubes into the database"""
        return NotImplemented

    @abstractmethod
    def is_dirty(self, cache_key_list):
        """Check if a cuboid is dirty based on its cache key"""
        return NotImplemented

    #    @abstractmethod
    #    def put_cube_index(self, resource, resolution, time_sample_list, morton_idx_list):
    #        """Insert the index list of cubes that exist in teh read cache"""
    #        return NotImplemented

        # TODO: Add if needed.  DMK pretty sure this was from OCPBlaze and not needed at the moment
    # @abstractmethod
    # def get_cube(self, resource, resolution, morton_idx, update=False):
    #    """Retrieve a single cube from the database"""
    #    return NotImplemented

    # TODO: Add if needed.  DMK pretty sure this was from OCPBlaze and not needed at the moment
    # @abstractmethod
    # def put_cube(self, resource, resolution, morton_idx, cube_bytes, update=False):
    #    """Store a single cube into the database"""
    #    return NotImplemented
