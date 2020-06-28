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
import boto3
import collections
import json
import hashlib
import numpy as np
from .error import SpdbError, ErrorCodes
from .region import Region
from random import randrange, randint
from spdb.c_lib.ndlib import XYZMorton
import traceback

import boto3

import os
import urllib.request
from urllib.error import URLError

# Note there are additional imports at the bottom of the file.

"""
Append a number between 0 and LOOKUP_KEY_MAX_N when generating a lookup_key.
Because lookup-key-index uses lookup_key as its key, this needs to spread over
multiple keys to avoid DynamoDB throttling during ingest.

If this value is updated after use in production, it may be increased but
NEVER decreased unless every instance in the table is rewritten to be within
the smaller range.
"""
LOOKUP_KEY_MAX_N = 100

"""
Max integer suffix appended to ingest-id attribute in the DynamoDB s3 index
table.  This attribute is the key of the ingest-id-index.  This GSI is used
during deletion of a channel.
"""
INGEST_ID_MAX_N = 100

def get_region():
    """
    Return the  aws region based on the machine's meta data

    If mocking with moto, metadata is not supported and "us-east-1" is always returned

    Returns: aws region

    """
    if 'LOCAL_DYNAMODB_URL' in os.environ:
        # If you get here, you are testing locally
        return "us-east-1"
    else:
        try:
            url = 'http://169.254.169.254/latest/meta-data/placement/availability-zone'
            resp = urllib.request.urlopen(url).read().decode('utf-8')
            region = resp[:-1]
            return region
        except NotImplementedError:
            # If you get here, you are mocking and metadata is not supported.
            return "us-east-1"
        except URLError:
            return None

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
    def trigger_page_out(self, config_data, write_cuboid_key, resource, is_black):
        """
        Method to trigger an page out to the object storage system

        Args:
            config_data (dict): Dictionary of configuration information
            write_cuboid_key (str): Unique write-cuboid to be flushed to S3
            resource (spdb.project.resource.BossResource): resource for the given write cuboid key
            is_black (bool): message flag for black overwrite

        Returns:
            None
        """

    @abstractmethod
    def get_loose_bounding_box(self, resource, resolution, id):
        """
        Get the loose bounding box that contains the object labeled with id.

        A loose bounding box is always cuboid aligned.

        Bounding box ranges follow the Python range convention.  For example,
        if x_range = [0, 10], then x >= 0 and x < 10.

        Args:
            resource (project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            id (uint64|string): object's id

        Returns:
            (dict): {'x_range': [0, 10], 'y_range': [0, 10], 'z_range': [0, 10], 't_range': [0, 10]}

        Raises:
            (SpdbError): Can't talk to id index database or database corrupt.
        """
        raise NotImplemented

    @abstractmethod
    def get_tight_bounding_box(self, cutout_fcn, resource, resolution, id, x_rng, y_rng, z_rng, t_rng):
        """Computes the exact bounding box for an id.

        Use ranges from the cuboid aligned "loose" bounding box as input.

        Args:
            cutout_fcn (function): SpatialDB's cutout method.  Provided for naive search of cuboids on the edges of the loose bounding box.
            resource (project.BossResource): Data model info based on the request or target resource.
            resolution (int): the resolution level.
            id (int): id to find bounding box of.
            x_rng (list[int]): 2 element list representing range.
            y_rng (list[int]): 2 element list representing range.
            z_rng (list[int]): 2 element list representing range.
            t_rng (list[int]): 2 element list representing range.

        Returns:
            (dict): {'x_range': [0, 10], 'y_range': [0, 10], 'z_range': [0, 10], 't_range': [0, 10]}
        """
        raise NotImplemented

    @abstractmethod
    def get_ids_in_region(
            self, cutout_fcn, resource, resolution, corner, extent,
            t_range=[0, 1], version=0):
        """
        Method to get all the ids within a defined region.

        Args:
            cutout_fcn (function): SpatialDB's cutout method.  Provided for naive search of ids in sub-regions
            resource (project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            corner ((int, int, int)): xyz location of the corner of the region
            extent ((int, int, int)): xyz extents of the region
            t_range (optional[list[int]]): time range, defaults to [0, 1]
            version (optional[int]): Reserved for future use.  Defaults to 0

        Returns:
            (dict): { 'ids': ['1', '4', '8'] }

        """
        raise NotImplemented


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
            id_index_table: name of DynamoDB table that maps object ids to cuboid object keys
            id_count_table: name of DynamoDB table that reserves objects ids for channels
        """
        # call the base class constructor
        ObjectStore.__init__(self, conf)
        self.obj_ind = ObjectIndices(
            conf['s3_index_table'], conf['id_index_table'], 
            conf['id_count_table'], conf['cuboid_bucket'], 
            get_region())

    @staticmethod
    def object_key_chunks(object_keys, chunk_size):
        """Yield successive chunk_size chunks from the list of keys in object_keys"""
        for ii in range(0, len(object_keys), chunk_size):
            yield object_keys[ii:ii + chunk_size]

    @staticmethod
    def get_object_key_parts(object_key):
        """

        Args:
            object_key (str): An object-key for a cuboid

        Returns:
            (collections.namedtuple)
        """
        KeyParts = collections.namedtuple('KeyParts', ['hash', 'collection_id', 'experiment_id', 'channel_id',
                                                       'resolution', 'time_sample', 'morton_id', 'is_iso'])
        # Parse key
        parts = object_key.split("&")

        hash = parts[0]

        if parts[1] == "ISO":
            iso_offset = 1
            is_iso = True
        else:
            iso_offset = 0
            is_iso = False

        collection_id = parts[1 + iso_offset]
        experiment_id = parts[2 + iso_offset]
        channel_id = parts[3 + iso_offset]
        resolution = parts[4 + iso_offset]
        time_sample = parts[5 + iso_offset]
        morton_id = parts[6 + iso_offset]

        return KeyParts(hash=hash, collection_id=collection_id, experiment_id=experiment_id, channel_id=channel_id,
                        resolution=resolution, time_sample=time_sample, morton_id=morton_id, is_iso=is_iso)

    @staticmethod
    def get_ingest_id_hash(coll_id, exp_id, chan_id, res, job_id, i):
        """
        Generate the key used to represent a particular ingest job.  This should
        match the ingest-id-hash attribute in the s3 index.

        Args:
            coll_id (int): Collection id.
            exp_id (int): Experiment id.
            chan_id (int): Channel id.
            res (int): Resolution.
            job_id (int): Ingest job id.
            i (int): Suffix used to prevent hot partitions during ingest (<= INGEST_ID_MAX_N).

        Returns:
            (str):
        """
        key = '{}&{}&{}&{}&{}#{}'.format(coll_id, exp_id, chan_id, res, job_id, i) 
        return key

    @staticmethod
    def generate_object_key(resource, resolution, time_sample, morton_id, iso=False):
        """Generate Key for an object stored in the S3 cuboid bucket

            hash&{lookup_key}&resolution&time_sample&morton_id

        Args:
            resource (spdb.project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            morton_id (int): Morton ID of the cuboids
            time_sample (int):  time samples of the cuboids
            iso (bool): Flag indicating if the isotropic version of a downsampled channel should be requested

        Returns:
            list[str]: A list of keys for each cuboid

        """
        experiment = resource.get_experiment()
        if iso is True and resolution > resource.get_isotropic_level() and experiment.hierarchy_method.lower() == "anisotropic":
            base_key = 'ISO&{}&{}&{}&{}'.format(resource.get_lookup_key(), resolution, time_sample, morton_id)
        else:
            base_key = '{}&{}&{}&{}'.format(resource.get_lookup_key(), resolution, time_sample, morton_id)

        # Hash
        hash_str = hashlib.md5(base_key.encode()).hexdigest()

        return "{}&{}".format(hash_str, base_key)

    @staticmethod
    def generate_lookup_key(collection_id, experiment_id, channel_id, resolution):
        """
        Generate the lookup key for storing as an attribute in the S3 index table.

        This value will be the key of a global secondary index that allows
        finding all the cuboids belonging to a particular channel in an 
        efficient manner.

        Note that the cuboids for a channel are spread over 
        LOOKUP_KEY_MAX_N + 1 keys to avoid hot spots and throttling during
        ingest.
        """
        lookup_key = '{}&{}&{}&{}#{}'.format(
            collection_id, experiment_id, channel_id, resolution, 
            randrange(LOOKUP_KEY_MAX_N))
        return lookup_key

    def cuboids_exist(self, key_list, cache_miss_key_idx=None, version=0):
        """
        Method to check if cuboids exist in S3 by checking the S3 Index table.

        Currently versioning is not implemented, so a version of "a" is simply used

        Args:
            key_list (list(str)): A list of cached-cuboid keys to check for existence in the object store
            cache_miss_key_idx (list(int)): A list of ints indexing the keys in key_list that should be checked
            version (int): The ID of the version node - Default to 0 until fully implemented, but will eliminate
                           need to do a migration

        Returns:
            (list(int)), (list(int)): A tuple of 2 lists.  The first is the index into key_list of keys IN S3.  The
            second is the index into key_list of keys not in S3

        """
        if not cache_miss_key_idx:
            cache_miss_key_idx = range(0, len(key_list))

        object_keys = self.cached_cuboid_to_object_keys(key_list)

        # TODO: Should use batch read to speed up
        dynamodb = boto3.client('dynamodb', region_name=get_region())

        s3_key_index = []
        zero_key_index = []
        for idx, key in enumerate(object_keys):
            if idx not in cache_miss_key_idx:
                continue
            response = dynamodb.get_item(
                TableName=self.config['s3_index_table'],
                Key={'object-key': {'S': key}, 'version-node': {'N': "{}".format(version)}},
                ConsistentRead=True,
                ReturnConsumedCapacity='NONE')

            if "Item" not in response:
                # Item not in S3
                zero_key_index.append(idx)
            else:
                s3_key_index.append(idx)

        return s3_key_index, zero_key_index

    def add_cuboid_to_index(self, object_key, version=0, ingest_job=0):
        """
        Method to add a cuboid's object_key to the S3 index table

        Currently versioning is not implemented, so a version of "0" is simply used

        Args:
            object_key (str): An object-keys for a cuboid to add to the index
            version (int): The ID of the version node - Default to 0 until fully implemented, but will eliminate
                           need to do a migration
            ingest_job (int): Id of ingest job that added this cuboid - default to 0 (if this was added via the cutout service, for example).

        Returns:
            None
        """
        dynamodb = boto3.client('dynamodb', region_name=get_region())

        # Get lookup key and resolution from object key
        parts = self.get_object_key_parts(object_key)

        # Partial lookup key stored so we can use a Dynamo query to find all cuboids
        # tha belong to a channel.
        lookup_key = self.generate_lookup_key(
            parts.collection_id, parts.experiment_id, parts.channel_id,
            parts.resolution)

        try:
            dynamodb.put_item(
                TableName=self.config['s3_index_table'],
                Item={
                    'object-key': {'S': object_key},
                      'version-node': {'N': "{}".format(version)},
                      'ingest-id-hash': {'S': AWSObjectStore.get_ingest_id_hash(
                          parts.collection_id, parts.experiment_id,
                          parts.channel_id, parts.resolution,
                          ingest_job, randint(0, INGEST_ID_MAX_N))},
                      'lookup-key': {'S': lookup_key}
                      },
                ReturnConsumedCapacity='NONE',
                ReturnItemCollectionMetrics='NONE'
            )
        except Exception as ex:
            traceback.print_exc()
            raise SpdbError("Error adding object-key to index: {}".format(ex),
                            ErrorCodes.SPDB_ERROR)

    def cached_cuboid_to_object_keys(self, keys):
        """
        Method to convert cached-cuboid keys to object-keys
        Args:
            keys (list(str)): A list of cached-cuboid keys

        Returns:
            (list(str)): A list of object keys
        """
        if isinstance(keys, str):
            keys = [keys]

        output_keys = []
        for key in keys:
            # Strip off front
            temp_key = key.split("&", 1)[1]

            # Hash
            hash_str = hashlib.md5(temp_key.encode()).hexdigest()

            # Combine
            output_keys.append("{}&{}".format(hash_str, temp_key))

        return output_keys

    def write_cuboid_to_object_keys(self, keys):
        """
        Method to convert write-cuboid keys to object-keys
        Args:
            keys (list(str)): A list of cached-cuboid keys

        Returns:
            (list(str)): A list of object keys
        """
        if isinstance(keys, str):
            keys = [keys]

        output_keys = []
        for key in keys:
            # Strip off front
            temp_key = key.split("&", 1)[1]
            temp_key = temp_key.rsplit("&", 1)[0]

            # Hash
            hash_str = hashlib.md5(temp_key.encode()).hexdigest()

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
        if isinstance(keys, str):
            keys = [keys]

        output_keys = []
        for key in keys:
            # Strip off hash
            temp_key = key.split("&", 1)[1]

            # Combine
            output_keys.append("CACHED-CUBOID&{}".format(temp_key))

        return output_keys

    def page_in_objects(self, key_list, page_in_chan, kv_config, state_config):
        # TODO Update parent class once tested
        """
        Method to page in objects from S3 to the Cache Database via Lambda invocation directly

        Args:
            key_list (list(str)): A list of cached-cuboid keys to retrieve from the object store
            page_in_chan (str): Redis channel used for sending status of page in operations
            kv_config (dict): Configuration information for the key-value engine interface
            state_config (dict): Configuration information for the state database interface

        Returns:
            key_list (list(str)): A list of object keys

        """
        # Convert cuboid-cached keys into object keys
        object_keys = self.cached_cuboid_to_object_keys(key_list)

        # Trigger lambda for all keys
        client = boto3.client('lambda', region_name=get_region())

        params = {"page_in_channel": page_in_chan,
                  "kv_config": kv_config,
                  "state_config": state_config,
                  "lambda-name": "page_in_lambda_function",
                  "object_store_config": self.config}

        # TODO: Make concurrent
        for key in object_keys:
            params["object_key"] = key

            response = client.invoke(
                FunctionName=self.config["page_in_lambda_function"],
                InvocationType='Event',
                Payload=json.dumps(params).encode())

        return object_keys

    def get_single_object(self, key, version=0):
        """ Method to get a single object. Used in the lambda page-in function and non-parallelized version

        Args:
            key (list(str)): A list of cached-cuboid keys to retrieve from the object store
            version (int): The ID of the version node - Default to 0 until fully implemented, but will eliminate
                           need to do a migration

        Returns:
            (bytes): A list of blosc compressed cuboid data

        """
        s3 = boto3.client('s3', region_name=get_region())

        # Append version to key
        key = "{}&{}".format(key, version)

        response = s3.get_object(
            Key=key,
            Bucket=self.config["cuboid_bucket"],
        )
        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise SpdbError("Error reading cuboid from S3.",
                            ErrorCodes.OBJECT_STORE_ERROR)

        return response['Body'].read()

    def get_objects(self, key_list, version=0):
        """ Method to get multiple objects serially in a loop

        Args:
            key_list (list(str)): A list of object keys to retrieve from the object store
            version (int): The ID of the version node - Default to 0 until fully implemented, but will eliminate
                           need to do a migration

        Returns:
            (list(bytes)): A list of blosc compressed cuboid data

        """
        s3 = boto3.client('s3', region_name=get_region())

        results = []

        for key in key_list:
            # Append version to key
            key = "{}&{}".format(key, version)

            response = s3.get_object(
                Key=key,
                Bucket=self.config["cuboid_bucket"],
            )
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                raise SpdbError("Error reading cuboid from S3.",
                                ErrorCodes.OBJECT_STORE_ERROR)

            results.append(response['Body'].read())

        return results

    def put_objects(self, key_list, cube_list, version=0):
        """

        Args:
            key_list (list(str)): A list of object keys to put into the object store
            cube_list (list(bytes)): A list of blosc compressed cuboid data
            version (int): The ID of the version node - Default to 0 until fully implemented, but will eliminate
                           need to do a migration

        Returns:

        """
        s3 = boto3.client('s3', region_name=get_region())

        for key, cube in zip(key_list, cube_list):
            # Append version to key
            key = "{}&{}".format(key, version)

            response = s3.put_object(
                Body=cube,
                Key=key,
                Bucket=self.config["cuboid_bucket"],
            )
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                raise SpdbError("Error writing cuboid to S3.",
                                ErrorCodes.OBJECT_STORE_ERROR)

    def update_id_indices(self, resource, resolution, key_list, cube_list, version=0):
        """
        Update annotation id index and s3 cuboid index with ids in the given cuboids.

        Any ids that are zeros will not be added to the indices.

        Args:
            resource (BossResource): Data model info based on the request or target resource.
            resolution (int): Resolution level.
            key_list (list[string]): keys for each cuboid.
            cube_list (list[bytes]): bytes comprising each cuboid.
            version (optional[int]): Defaults to zero, reserved for future use.
        """
        self.obj_ind.update_id_indices(
            resource, resolution, key_list, cube_list, version)

    def get_loose_bounding_box(self, resource, resolution, id):
        """
        Get the loose bounding box that contains the object labeled with id.

        A loose bounding box is always cuboid aligned.

        Bounding box ranges follow the Python range convention.  For example,
        if x_range = [0, 10], then x >= 0 and x < 10.

        Args:
            resource (project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            id (uint64|string): object's id

        Returns:
            (dict): {'x_range': [0, 10], 'y_range': [0, 10], 'z_range': [0, 10], 't_range': [0, 10]}

        Raises:
            (SpdbError): Can't talk to id index database or database corrupt.
        """
        return self.obj_ind.get_loose_bounding_box(resource, resolution, id)

    def get_tight_bounding_box(self, cutout_fcn, resource, resolution, id, x_rng, y_rng, z_rng, t_rng):
        """Computes the exact bounding box for an id.

        Use ranges from the cuboid aligned "loose" bounding box as input.

        Args:
            cutout_fcn (function): SpatialDB's cutout method.  Provided for naive search of cuboids on the edges of the loose bounding box.
            resource (project.BossResource): Data model info based on the request or target resource.
            resolution (int): the resolution level.
            id (int): id to find bounding box of.
            x_rng (list[int]): 2 element list representing range.
            y_rng (list[int]): 2 element list representing range.
            z_rng (list[int]): 2 element list representing range.
            t_rng (list[int]): 2 element list representing range.

        Returns:
            (dict): {'x_range': [0, 10], 'y_range': [0, 10], 'z_range': [0, 10], 't_range': [0, 10]}
        """
        return self.obj_ind.get_tight_bounding_box(
            cutout_fcn, resource, resolution, id, x_rng, y_rng, z_rng, t_rng)

    def trigger_page_out(self, config_data, write_cuboid_key, resource, to_black=False):
        """
        Method to invoke lambda function to page out via data in an SQS message

        Args:
            config_data (dict): Dictionary of configuration dictionaries
            write_cuboid_key (str): Unique write-cuboid to be flushed to S3
            resource (spdb.project.resource.BossResource): resource for the given write cuboid key
            is_black (bool): message flag for black overwrite

        Returns:
            None
        """
        # Put page out job on the queue
        sqs = boto3.client('sqs', region_name=get_region())

        msg_data = {"config": config_data,
                    "write_cuboid_key": write_cuboid_key,
                    "lambda-name": "s3_flush",
                    "resource": resource.to_dict(),
                    "to_black": to_black
                    }

        response = sqs.send_message(QueueUrl=self.config["s3_flush_queue"],
                                    MessageBody=json.dumps(msg_data))

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise SpdbError("Error sending SQS message to trigger page out operation.",
                            ErrorCodes.SPDB_ERROR)

        # Trigger lambda to handle it
        client = boto3.client('lambda', region_name=get_region())

        response = client.invoke(
            FunctionName=self.config["page_out_lambda_function"],
            InvocationType='Event',
            Payload=json.dumps(msg_data).encode())

    def reserve_ids(self, resource, num_ids, version=0):
        """Method to reserve a block of ids for a given channel at a version.

        Args:
            resource (spdb.project.resource.BossResource): Data model info based on the request or target resource.
            num_ids (int): Number of IDs to reserve
            version (optional[int]): Defaults to zero, reserved for future use.

        Returns:
            (np.array): starting ID for the block of ID successfully reserved as a numpy array to insure uint64
        """
        return self.obj_ind.reserve_ids(resource, num_ids, version)

    def get_ids_in_region(
            self, cutout_fcn, resource, resolution, corner, extent,
            t_range=[0, 1], version=0):
        """
        Method to get all the ids within a defined region.

        Args:
            cutout_fcn (function): SpatialDB's cutout method.  Provided for naive search of ids in sub-regions
            resource (project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            corner ((int, int, int)): xyz location of the corner of the region
            extent ((int, int, int)): xyz extents of the region
            t_range (optional[list[int]]): time range, defaults to [0, 1]
            version (optional[int]): Reserved for future use.  Defaults to 0

        Returns:
            (dict): { 'ids': ['1', '4', '8'] }

        """

        # Identify sub-region entirely contained by cuboids.
        cuboids = Region.get_cuboid_aligned_sub_region(
            resolution, corner, extent)

        # Get all non-cuboid aligned sub-regions.
        non_cuboid_list = Region.get_all_partial_sub_regions(
            resolution, corner, extent)

        # Do cutouts on each partial region and build id set.
        id_set = np.array([], dtype='uint64')
        for partial_region in non_cuboid_list:
            extent = partial_region.extent
            if extent[0] == 0 or extent[1] == 0 or extent[2] == 0:
                continue
            id_arr = self._get_ids_from_cutout(
                cutout_fcn, resource, resolution,
                partial_region.corner, partial_region.extent,
                t_range, version)
            # TODO: do a unique first?  perf test
            id_set = np.union1d(id_set, id_arr)

        # Get ids from dynamo for sub-region that's 100% cuboid aligned.
        obj_key_list = self._get_object_keys(
            resource, resolution, cuboids, t_range)
        cuboid_ids = self.obj_ind.get_ids_in_cuboids(obj_key_list, version)
        cuboid_ids_arr = np.asarray([int(id) for id in cuboid_ids], dtype='uint64')

        # Union ids from cuboid aligned sub-region.
        id_set = np.union1d(id_set, cuboid_ids_arr)

        # Convert ids back to strings for transmission via HTTP.
        ids_as_str = ['%d' % n for n in id_set]

        return { 'ids': ids_as_str }

    def _get_ids_from_cutout(
            self, cutout_fcn, resource, resolution, corner, extent,
            t_range=[0, 1], version=0):
        """
        Do a cutout and return the unique ids within the specified region.

        0 is never returned as an id.

        Args:
            cutout_fcn (function): SpatialDB's cutout method.  Provided for naive search of ids in sub-regions
            resource (project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            corner ((int, int, int)): xyz location of the corner of the region
            extent ((int, int, int)): xyz extents of the region
            t_range (optional[list[int]]): time range, defaults to [0, 1]
            version (optional[int]): Reserved for future use.  Defaults to 0

        Returns:
            (numpy.array): unique ids in a numpy array.
        """
        cube = cutout_fcn(resource, corner, extent, resolution, t_range)
        id_arr = np.unique(cube.data)
        # 0 is not a valid id.
        id_arr_no_zero = np.trim_zeros(id_arr, trim='f')
        return id_arr_no_zero

    def _get_object_keys(self, resource, resolution, cuboid_bounds, t_range=[0, 1]):
        """
        Retrieves objects keys for cuboids specified in cuboid_bounds.

        Args:
            resource (project.BossResource): Data model info based on the request or target resource
            resolution (int): the resolution level
            cuboid_bounds (Region.Cuboids): ranges of cuboids to get keys for
            t_range (optional[list[int]]): time range, defaults to [0, 1]

        Returns:

        """
        key_list = []
        for x in cuboid_bounds.x_cuboids:
            for y in cuboid_bounds.y_cuboids:
                for z in cuboid_bounds.z_cuboids:
                    morton = XYZMorton([x, y, z])
                    for t in range(t_range[0], t_range[1]):
                        key_list.append(AWSObjectStore.generate_object_key(
                            resource, resolution, t, morton))

        return key_list



# Import statement at the bottom to avoid problems with circular imports.
# http://effbot.org/zone/import-confusion.htm
from .object_indices import ObjectIndices

