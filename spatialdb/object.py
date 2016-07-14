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
from .error import SpdbError, ErrorCodes

import boto3


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

    @abstractmethod
    def cached_cuboid_to_object_keys(self, keys):
        """
        Method to convert cached-cuboid keys to object-keys
        Args:
            keys (list(str)): A list of cached-cuboid keys

        Returns:
            (list(str)): A list of object keys
        """
        raise NotImplemented

    @abstractmethod
    def object_to_cached_cuboid_keys(self, keys):
        """
        Method to convert object-keys to cached-cuboid keys
        Args:
            keys (list(str)): A list of object-keys

        Returns:
            (list(str)): A list of cached-cuboid keys
        """
        raise NotImplemented

    @abstractmethod
    def trigger_page_out(self, config_data, write_cuboid_key):
        """
        Method to trigger an page out to the object storage system

        Args:
            config_data (dict): Dictionary of configuration information
            write_cuboid_key (str): Unique write-cuboid to be flushed to S3

        Returns:
            None
        """


class AWSObjectStore(ObjectStore):
    def __init__(self, conf):
        """
        A class to implement the object store for cuboid storage using AWS (using S3 and DynamoDB)

        Args:
            conf(dict): Dictionary containing configuration details for the object store


        Params in the conf dictionary:
            s3_flush_queue: URL for the SQS queue tracking flush tasks
            cuboid_bucket: Bucket for storage of cuboid objects in S3
            page_in_lambda_function: name of lambda function for page in operation (e.g. page_in.handler)
            page_out_lambda_function: name of lambda function for page out operation (e.g. page_in.handler)
            s3_index_table: name of the dynamoDB table for storing the s3 cuboid index
        """
        # call the base class constructor
        ObjectStore.__init__(self, conf)

    @staticmethod
    def object_key_chunks(object_keys, chunk_size):
        """Yield successive chunk_size chunks from the list of keys in object_keys"""
        for ii in range(0, len(object_keys), chunk_size):
            yield object_keys[ii:ii + chunk_size]

    def cuboids_exist(self, key_list, cache_miss_key_idx=None, version="a"):
        """
        Method to check if cuboids exist in S3 by checking the S3 Index table.

        Currently versioning is not implemented, so a version of "a" is simply used

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
        dynamodb = boto3.client('dynamodb')

        s3_key_index = []
        zero_key_index = []
        for idx, key in enumerate(object_keys):
            if idx not in cache_miss_key_idx:
                continue

            response = dynamodb.get_item(
                TableName=self.config['s3_index_table'],
                Key={'object-key': {'S': key}, 'version': {'S': version}},
                ConsistentRead=True,
                ReturnConsumedCapacity='NONE')

            if "Item" not in response:
                # Item not in S3
                zero_key_index.append(idx)
            else:
                s3_key_index.append(idx)

        return s3_key_index, zero_key_index

    def add_cuboid_to_index(self, object_key, version="a"):
        """
        Method to add a cuboid's object_key to the S3 index table

        Currently versioning is not implemented, so a version of "a" is simply used

        Args:
            object_key (str): An object-keys for a cuboid to add to the index
            version: TBD version of the cuboid

        Returns:
            None
        """
        dynamodb = boto3.client('dynamodb')

        # Get lookup key and resolution from object key
        vals = object_key.split("&")
        lookup_key = "{}&{}&{}".format(vals[1], vals[2], vals[3])
        resolution = vals[4]

        try:
            dynamodb.put_item(
                TableName=self.config['s3_index_table'],
                Item={'object-key': {'S': object_key},
                      'version': {'S': version},
                      'ingest-job': {'S': "{}&{}&0".format(lookup_key, resolution)}},
                ReturnConsumedCapacity='NONE',
                ReturnItemCollectionMetrics='NONE',
            )
        except:
            raise SpdbError("Error adding object-key to index.",
                            ErrorCodes.SPDB_ERROR)

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
            # TODO: sha256 best hash to use?
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
        client = boto3.client('lambda')

        params = {"page_in_channel": page_in_chan,
                  "kv_config": kv_config,
                  "state_config": state_config,
                  "object_store_config": self.config}

        for key in object_keys:
            params["object_key"] = key

            response = client.invoke(
                FunctionName=self.config["page_in_lambda_function"],
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

        s3 = boto3.client('s3')

        byte_arrays = []
        for key in key_list:
            response = s3.get_object(
                Key=key,
                Bucket=self.config["cuboid_bucket"],
            )
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                raise SpdbError("Error reading cuboid from S3.",
                                ErrorCodes.OBJECT_STORE_ERROR)

            byte_arrays.append(response['Body'].read())

        return byte_arrays

    def put_objects(self, key_list, cube_list, version=None):
        """

        Args:
            key_list (list(str)): A list of cached-cuboid keys to retrieve from the object store
            cube_list (list(bytes)): A list of blosc compressed cuboid data
            version: TBD version of the cuboid

        Returns:

        """
        s3 = boto3.client('s3')

        for key, cube in zip(key_list, cube_list):
            response = s3.put_object(
                Body=cube,
                Key=key,
                Bucket=self.config["cuboid_bucket"],
            )
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                raise SpdbError("Error writing cuboid to S3.",
                                ErrorCodes.OBJECT_STORE_ERROR)

    def trigger_page_out(self, config_data, write_cuboid_key):
        """
        Method to trigger lambda function to page out via SNS message that is collected by SQS

        Args:
            config_data (dict): Dictionary of configuration dictionaries
            write_cuboid_key (str): Unique write-cuboid to be flushed to S3

        Returns:
            None
        """
        # Put page out job on the queue
        sqs = boto3.client('sqs')

        msg_data = {"config": config_data,
                    "write_cuboid_key": write_cuboid_key}

        response = sqs.send_message(QueueUrl=self.config["s3_flush_queue"],
                                    Message=json.dumps(msg_data))

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise SpdbError("Error sending SNS message to trigger page out operation.",
                            ErrorCodes.SPDB_ERROR)

        # Trigger lambda to handle it
        client = boto3.client('lambda')

        response = client.invoke(
            FunctionName=self.config["page_out_lambda_function"],
            InvocationType='Event',
            Payload=json.dumps(msg_data).encode())

        # TODO: Add Error handling
