# Copyright 2014 NeuroData (http://neurodata.io)
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

import types

from kvio import KVIO
import redis

from spatialdberror import SpatialDBError
import logging

logger = logging.getLogger("neurodata")


class RedisKVIO(KVIO):
    def __init__(self, db):
        """Connect to the Redis backend"""

        self.db = db
        self.client = redis.StrictRedis(host=self.db.proj.getDBHost(), port=6379, db=0)
        self.pipe = self.client.pipeline(transaction=False)

    def getIndexStore(self, ch, resolution):
        """Generate the name of the Index Store"""
        return '{}_{}_{}'.format(self.db.proj.getProjectName(), ch.getChannelName(), resolution)

    def generateKeys(self, ch, resolution, zidx_list, timestamp):
        """Generate a key for Redis"""

        key_list = []
        if isinstance(timestamp, list):
            for tvalue in timestamp:
                key_list.append(
                    '{}_{}_{}_{}_{}'.format(self.db.proj.getProjectName(), ch.getChannelName(), resolution, tvalue,
                                            zidx_list[0]))
        else:
            for zidx in zidx_list:
                if timestamp == None:
                    key_list.append(
                        '{}_{}_{}_{}'.format(self.db.proj.getProjectName(), ch.getChannelName(), resolution, zidx))
                else:
                    key_list.append(
                        '{}_{}_{}_{}_{}'.format(self.db.proj.getProjectName(), ch.getChannelName(), resolution,
                                                timestamp, zidx))

        return key_list

    def getCubeIndex(self, ch, resolution, listofidxs, listoftimestamps=None):
        """Retrieve the indexes of inserted cubes"""

        index_store = self.getIndexStore(ch, resolution)
        index_store_temp = index_store + '_temp'

        try:
            if listoftimestamps:
                self.client.sadd(index_store_temp, *list(zip(listoftimestamps, listofidxs)))
            else:
                self.client.sadd(index_store_temp, *listofidxs)
            ids_to_fetch = self.client.sdiff(index_store_temp, index_store)
            self.client.delete(index_store_temp)
        except Exception as e:
            logger.error("Error retrieving cube indexes into the database. {}".format(e))
            raise SpatialDBError("Error retrieving cube indexes into the database. {}".format(e))

        return list(ids_to_fetch)

    def putCubeIndex(self, ch, resolution, listofidxs, listoftimestamps=None):
        """Add the listofidxs to the store"""

        try:
            if listoftimestamps:
                self.client.sadd(self.getIndexStore(ch, resolution), *list(zip(listoftimestamps, listofidxs)))
            else:
                self.client.sadd(self.getIndexStore(ch, resolution), *listofidxs)
        except Exception as e:
            logger.error("Error inserting cube indexes into the database. {}".format(e))
            raise SpatialDBError("Error inserting cube indexes into the database. {}".format(e))

    def getCube(self, ch, zidx, resolution, update=False, timestamp=None):
        """Retrieve a single cube from the database"""

        try:
            rows = self.client.mget(self.generateKeys(ch, resolution, [zidx], timestamp))
        except Exception as e:
            logger.error("Error retrieving cubes into the database. {}".format(e))
            raise SpatialDBError("Error retrieving cubes into the database. {}".format(e))

        if rows[0]:
            return rows[0]
        else:
            return None

    def getCubes(self, ch, listofidxs, resolution, neariso=False, timestamp=None):
        """Retrieve multiple cubes from the database"""

        try:
            rows = self.client.mget(self.generateKeys(ch, resolution, listofidxs, timestamp))
        except Exception as e:
            logger.error("Error retrieving cubes into the database. {}".format(e))
            raise SpatialDBError("Error retrieving cubes into the database. {}".format(e))

        for idx, row in zip(listofidxs, rows):
            yield (idx, row)

    def getTimeCubes(self, ch, idx, listoftimestamps, resolution):
        """Retrieve multiple cubes from the database"""

        try:
            rows = self.client.mget(self.generateKeys(ch, resolution, [idx], listoftimestamps))
        except Exception as e:
            logger.error("Error inserting cubes into the database. {}".format(e))
            raise SpatialDBError("Error inserting cubes into the database. {}".format(e))

        for idx, timestamp, row in zip([idx] * len(listoftimestamps), listoftimestamps, rows):
            yield (idx, timestamp, row)

    def putCube(self, ch, zidx, resolution, cubestr, update=False, timestamp=None):
        """Store a single cube into the database"""

        # generating the key
        key_list = self.generateKeys(ch, resolution, [zidx], timestamp=timestamp)

        try:
            self.client.mset(dict(list(zip(key_list, [cubestr]))))
        except Exception as e:
            logger.error("Error inserting cube into the database. {}".format(e))
            raise SpatialDBError("Error inserting cube into the database. {}".format(e))

    def putCubes(self, ch, listofidxs, resolution, listofcubes, update=False, timestamp=None):
        """Store multiple cubes into the database"""

        # generating the list of keys
        key_list = self.generateKeys(ch, resolution, listofidxs, timestamp=timestamp)

        try:
            self.client.mset(dict(list(zip(key_list, listofcubes))))
        except Exception as e:
            logger.error("Error inserting cubes into the database. {}".format(e))
            raise SpatialDBError("Error inserting cubes into the database. {}".format(e))
