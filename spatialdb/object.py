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
import json
import hashlib

import boto3

from bossutils.aws import *


class ObjectStore(metaclass=ABCMeta):
    def __init__(self, object_store_conf):
        """
        A class to implement the object store for cuboid storage

        Args:
            conf(dict): Dictionary containing configuration details for the object store
        """
        self.config = object_store_conf

    @abstractmethod
    def cuboids_exist(self, key_list, version=None):
        """

        Args:
            key_list (list(str)): A list of cached-cuboid keys to check for existence in the object store
            version: TBD version of the cuboid

        Returns:
            (list(bool)): A list of booleans indicating if each key exists or does not

        """
        return NotImplemented

    @abstractmethod
    def get_objects(self, key_list, version=None):
        """

        Args:
            key_list (list(str)): A list of cached-cuboid keys to retrieve from the object store
            version: TBD version of the cuboid

        Returns:
            (list(bytes)): A list of blosc compressed cuboid data

        """
        return NotImplemented

    @abstractmethod
    def put_objects(self, key_list, cube_list, version=None):
        """
        Method to write cubes to the object store

        Args:
            key_list (list(str)): A list of cached-cuboid keys to retrieve from the object store
            cube_list (list(bytes)): A list of blosc compressed cuboid data
            version: TBD version of the cuboid

        Returns:

        """
        return NotImplemented

    @abstractmethod
    def page_in_objects(self, key_list, page_in_chan, timeout, kv_config, state_config):
        """
        Method to page in objects from S3 to the Cache Database

        Args:
            key_list (list(str)): A list of cached-cuboid keys to retrieve from the object store
            page_in_chan (str): Redis channel used for sending status of page in operations
            timeout: Number of seconds page in which the operation should complete before an error should be raised
            kv_config (dict): Configuration information for the key-value engine interface
            state_config (dict): Configuration information for the state database interface

        Returns:

        """
        return NotImplemented


class AWSObjectStore(ObjectStore):
    def __init__(self, conf):
        """
        A class to implement the object store for cuboid storage using AWS (using S3 and DynamoDB)

        Args:
            conf(dict): Dictionary containing configuration details for the object store
        """
        # call the base class constructor
        ObjectStore.__init__(self, conf)

        # Get an authorized boto3 session
        aws_mngr = get_aws_manager()
        self.__session = aws_mngr.get_session()

    def __del__(self):
        # Clean up session by returning it to the pool
        aws_mngr = get_aws_manager()
        aws_mngr.put_session(self.__session)

    @staticmethod
    def object_key_chunks(object_keys, chunk_size):
        """Yield successive chunk_size chunks from the list of keys in object_keys"""
        for ii in range(0, len(object_keys), chunk_size):
            yield object_keys[ii:ii + chunk_size]

    def cuboids_exist(self, key_list, cache_miss_key_idx=None, version=0):
        """
        Method to check if cuboids exist in S3 by checking the S3 Index table.

        Currently versioning is not implemented, so a version of "0" is simply used

        Args:
            key_list (list(str)): A list of cached-cuboid keys to check for existence in the object store
            cache_miss_key_idx (list(int)): A list of ints indexing the keys in key_list that should be checked
            version: TBD version of the cuboid

        Returns:
            (list(int)), (list(int)): A tuple of 2 lists.  The first is the index into key_list of keys IN S3.  The
            second is the index into key_list of keys not in S3

        """
        if not cache_miss_key_idx:
            cache_miss_key_idx = range(0, len(key_list))

        object_keys = self.cached_cuboid_to_object_keys(key_list)

        # TODO: Possibly could use batch read to speed up
        # TODO: This needs tested for sure. Probably has bugs at the moment but working to get all code in the right place
        dynamodb = self.__session.client('dynamodb')
        table = dynamodb.Table(self.config["aws"]["s3-index-table"])

        s3_key_index = []
        zero_key_index = []
        for idx, key in enumerate(object_keys):
            if idx not in cache_miss_key_idx:
                continue

            response = table.get_item(Key={'hash-key': key, 'version': version},
                                      ConsistentRead=True,
                                      ReturnConsumedCapacity='NONE')
            if not response:
                # Item not in S3
                zero_key_index.append(idx)
            else:
                s3_key_index.append(idx)

        return s3_key_index, zero_key_index

    def cached_cuboid_to_object_keys(self, keys):
        """
        Method to convert cached-cuboid keys to object-keys
        Args:
            keys (list(str)): A list of cached-cuboid keys

        Returns:
            (list(str)): A list of object keys
        """
        output_keys = []
        for key in keys:
            # Strip off front
            temp_key = key.split("&", 1)[1]

            # Hash
            hash_str = hashlib.sha256(temp_key.encode()).hexdigest()

            # Combine
            output_keys.append("{}&{}".format(hash_str, temp_key))

        return output_keys

    def object_to_cached_cuboid_keys(self, keys):
        """
        Method to convert object-keys to cached-cuboid keys
        Args:
            keys (list(str)): A list of object-keys

        Returns:
            (list(str)): A list of cached-cuboid keys
        """
        output_keys = []
        for key in keys:
            # Strip off hash
            temp_key = key.split("&", 1)[1]

            # Combine
            output_keys.append("CACHED-CUBOID&{}".format(temp_key))

        return output_keys

    def page_in_objects(self, key_list, page_in_chan, timeout, kv_config, state_config):
        """
        Method to page in objects from S3 to the Cache Database via Lambda invocation directly

        Args:
            key_list (list(str)): A list of cached-cuboid keys to retrieve from the object store
            page_in_chan (str): Redis channel used for sending status of page in operations
            timeout (int): Number of seconds in which the operation should complete before an error should be raised
            kv_config (dict): Configuration information for the key-value engine interface
            state_config (dict): Configuration information for the state database interface

        Returns:
            key_list (list(str)): A list of object keys

        """
        # Convert cuboid-cached keys into object keys
        object_keys = self.cached_cuboid_to_object_keys(key_list)

        # Trigger lambda for all keys
        client = self.__session.client('lambda')

        params = {"page_in_channel": page_in_chan,
                  "kv_config": kv_config,
                  "state_config": state_config,
                  "object_store_config": self.config}

        for key in object_keys:
            params["object_key"] = key

            response = client.invoke(
                FunctionName=self.config["lambda"]["page_in_function"],
                InvocationType='Event',
                Payload=json.dumps(params).encode())

            # TODO: Check if response comes back on an EVENT type invoke and throw error on error if so

        return object_keys

    def get_objects(self, key_list, version=None):
        """

        Args:
            key_list (list(str)): A list of cached-cuboid keys to retrieve from the object store
            version: TBD version of the cuboid

        Returns:
            (list(bytes)): A list of blosc compressed cuboid data

        """
        if not isinstance(key_list, list):
            key_list = [key_list]

        client = self.__session.client('s3')

        byte_arrays = []
        for key in key_list:
            byte_arrays.append(client.get_object(Bucket=self.config["bucket"]))

        return byte_arrays

    def put_objects(self, key_list, cube_list, version=None):
        """

        Args:
            key_list (list(str)): A list of cached-cuboid keys to retrieve from the object store
            cube_list (list(bytes)): A list of blosc compressed cuboid data
            version: TBD version of the cuboid

        Returns:

        """
        return NotImplemented

    def trigger_page_out(self, config_data, write_cuboid_key):
        """
        Method to trigger lambda function to page out via SNS message that is collected by SQS

        Args:
            config_data (dict): Dictionary of configuration dictionaries
            write_cuboid_key (str): Unique write-cuboid to be flushed to S3

        Returns:
            None
        """
        # TODO Double check sns boto3 call
        # TODO is target arn the same as topic arn?
        sns = self.__session.client('sns')
        topic = sns.Topic(self.config["lambda"]["topic_arn"])

        msg_data = {"config": config_data,
                    "write_cuboid_key": write_cuboid_key}

        response = topic.publish(
                        TargetArn=self.config["lambda"]["target_arn"],
                        Message=json.dumps({"default": msg_data}).encode(),
                        MessageStructure='json')

        # TODO: Need error handling here?
