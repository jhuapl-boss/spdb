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
from .error import SpdbError, ErrorCode


class KVIO(metaclass=ABCMeta):
    """An abstract base class for Key-Value engines that act as a cache DB or work with OCP blaze
    """

    def __init__(self):
        pass

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

    @abstractmethod
    def get_missing_cube_index(self, resource, resolution, idx_list):
        """Return the cuboid index list of inserted cubes in the cache DB"""
        return NotImplemented

    @abstractmethod
    def put_cube_index(self, resource, resolution, idx_list):
        """Insert the index list of fetched cubes"""
        return NotImplemented

    # TODO: Add if needed.  DMK pretty sure this was from OCPBlaze and not needed at the moment
    #@abstractmethod
    #def get_cube(self, resource, resolution, morton_idx, update=False):
    #    """Retrieve a single cube from the database"""
    #    return NotImplemented

    @abstractmethod
    def get_cubes(self, resource, resolution, morton_idx_list):
        """Retrieve multiple cubes from the database"""
        return NotImplemented

    # TODO: Investigate if unique representation of time-series data is needed and if this is needed
    # @abstractmethod
    # def getTimeCubes(self, ch, idx, listoftimestamps, resolution):
    #     """Retrieve multiple cubes from the database"""
    #     return NotImplemented

    # TODO: Add if needed.  DMK pretty sure this was from OCPBlaze and not needed at the moment
    #@abstractmethod
    #def put_cube(self, resource, resolution, morton_idx, cube_bytes, update=False):
    #    """Store a single cube into the database"""
    #    return NotImplemented

    @abstractmethod
    def put_cubes(self, resource, resolution, morton_idx_list, cube_list, update=False):
        """Store multiple cubes into the database"""
        return NotImplemented

    # Factory method for KVIO Engine
    @staticmethod
    def get_kv_engine(engine):
        if engine == "redis":
            from spdb.spatialdb import RedisKVIO
            return RedisKVIO()
        else:
            raise SpdbError("KVIO Error", "Failed to create key-value engine.",
                            ErrorCode.REDIS_ERROR)

