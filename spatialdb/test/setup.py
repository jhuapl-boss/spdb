# Copyright 2016 The Johns Hopkins University Applied Physics Laboratory
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pkg_resources import resource_filename
import json

from bossutils.aws import get_region

import boto3
from moto import mock_s3
from moto import mock_sqs
from moto import mock_dynamodb2
from moto import mock_sqs

import time


class SetupTests(object):
    """ Class to handle setting up tests, including support for mocking

    """
    def __init__(self):
        self.mock = True
        self.mock_s3 = None
        self.mock_dynamodb = None
        self.mock_sqs = None

        self.DYNAMODB_SCHEMA = resource_filename('spdb', 'spatialdb/dynamo/s3_index_table.json')

    def start_mocking(self):
        """Method to start mocking"""
        self.mock = True
        self.mock_s3 = mock_s3()
        self.mock_dynamodb = mock_dynamodb2()
        self.mock_sqs = mock_sqs()
        self.mock_s3.start()
        self.mock_dynamodb.start()
        self.mock_sqs.start()

    def stop_mocking(self):
        """Method to stop mocking"""
        self.mock_s3.stop()
        self.mock_dynamodb.stop()
        self.mock_sqs.stop()

    # ***** Cuboid Index Table *****
    def _create_s3_index_table(self, table_name):
        """Method to create the S3 index table"""

        # Load json spec
        with open(self.DYNAMODB_SCHEMA) as handle:
            json_str = handle.read()
            table_params = json.loads(json_str)

        # Create table
        client = boto3.client('dynamodb', region_name=get_region())
        _ = client.create_table(TableName=table_name, **table_params)

        return client.get_waiter('table_exists')

    def create_s3_index_table(self, table_name):
        """Method to create the S3 index table"""
        if self.mock:
            mock_dynamodb2(self._create_s3_index_table(table_name))
        else:
            waiter = self._create_s3_index_table(table_name)

            # Wait for actual table to be ready.
            self.wait_table_create(table_name)

    def _delete_s3_index_table(self, table_name):
        """Method to delete the S3 index table"""
        client = boto3.client('dynamodb', region_name=get_region())
        client.delete_table(TableName=table_name)

    def delete_s3_index_table(self, table_name):
        """Method to create the S3 index table"""
        if self.mock:
            mock_dynamodb2(self._delete_s3_index_table(table_name))
        else:
            self._delete_s3_index_table(table_name)

            # Wait for table to be deleted (since this is real)
            self.wait_table_delete(table_name)

    def wait_table_create(self, table_name):
        """Poll dynamodb at a 2s interval until the table creates."""
        print('Waiting for creation of table {}'.format(
            table_name), end='', flush=True)
        client = boto3.client('dynamodb', region_name=get_region())
        cnt = 0
        while True:
            time.sleep(2)
            cnt += 1
            if cnt > 50:
                # Give up waiting.
                return
            try:
                print('.', end='', flush=True)
                resp = client.describe_table(TableName=table_name)
                if resp['Table']['TableStatus'] == 'ACTIVE':
                    print('')
                    return
            except:
                # May get an exception if table doesn't currently exist.
                pass

    def wait_table_delete(self, table_name):
        """Poll dynamodb at a 2s interval until the table deletes."""
        print('Waiting for deletion of table {}'.format(
            table_name), end='', flush=True)
        client = boto3.client('dynamodb', region_name=get_region())
        cnt = 0
        while True:
            time.sleep(2)
            cnt += 1
            if cnt > 50:
                # Give up waiting.
                return
            try:
                print('.', end='', flush=True)
                resp = client.describe_table(TableName=table_name)
            except:
                # Exception thrown when table doesn't exist.
                print('')
                return

    # ***** END Cuboid Index Table END *****

    # ***** Cuboid Bucket *****
    def _create_cuboid_bucket(self, bucket_name):
        """Method to create the S3 bucket for cuboid storage"""
        client = boto3.client('s3', region_name=get_region())
        _ = client.create_bucket(
            ACL='private',
            Bucket=bucket_name
        )
        return client.get_waiter('bucket_exists')

    def create_cuboid_bucket(self, bucket_name):
        """Method to create the S3 bucket for cuboid storage"""
        if self.mock:
            mock_s3(self._create_cuboid_bucket(bucket_name))
        else:
            waiter = self._create_cuboid_bucket(bucket_name)

            # Wait for bucket to exist
            waiter.wait(Bucket=bucket_name)

    def _delete_cuboid_bucket(self, bucket_name):
        """Method to delete the S3 bucket for cuboid storage"""
        s3 = boto3.resource('s3', region_name=get_region())
        bucket = s3.Bucket(bucket_name)
        for obj in bucket.objects.all():
            obj.delete()

        # Delete bucket
        bucket.delete()
        return bucket

    def delete_cuboid_bucket(self, bucket_name):
        """Method to create the S3 bucket for cuboid storage"""
        if self.mock:
            mock_s3(self._delete_cuboid_bucket(bucket_name))
        else:
            bucket = self._delete_cuboid_bucket(bucket_name)
            # Wait for table to be deleted (since this is real)
            bucket.wait_until_not_exists()
    # ***** END Cuboid Bucket *****

    # ***** Flush SQS Queue *****
    def _create_flush_queue(self, queue_name):
        """Method to create a test sqs for flushing cubes"""
        client = boto3.client('sqs', region_name=get_region())
        response = client.create_queue(QueueName=queue_name)
        url = response['QueueUrl']
        return url

    def create_flush_queue(self, queue_name):
        """Method to create a test sqs for flushing cubes"""
        if self.mock:
            url = mock_sqs(self._create_flush_queue(queue_name))
        else:
            url = self._create_flush_queue(queue_name)
            time.sleep(60)
        return url

    def _delete_flush_queue(self, queue_url):
        """Method to delete a test sqs for flushing cubes"""
        client = boto3.client('sqs', region_name=get_region())
        client.delete_queue(QueueUrl=queue_url)

    def delete_flush_queue(self, queue_name):
        """Method to delete a test sqs for flushing cubes"""
        if self.mock:
            mock_sqs(self._delete_flush_queue(queue_name))
        else:
            self._delete_flush_queue(queue_name)
    # ***** END Flush SQS Queue *****

    def get_image8_dict(self):
        """Method to get the config dictionary for an image8 resource"""
        data = {}
        data['collection'] = {}
        data['collection']['name'] = "col1"
        data['collection']['description'] = "Test collection 1"

        data['coord_frame'] = {}
        data['coord_frame']['name'] = "coord_frame_1"
        data['coord_frame']['description'] = "Test coordinate frame"
        data['coord_frame']['x_start'] = 0
        data['coord_frame']['x_stop'] = 2000
        data['coord_frame']['y_start'] = 0
        data['coord_frame']['y_stop'] = 5000
        data['coord_frame']['z_start'] = 0
        data['coord_frame']['z_stop'] = 200
        data['coord_frame']['x_voxel_size'] = 4
        data['coord_frame']['y_voxel_size'] = 4
        data['coord_frame']['z_voxel_size'] = 35
        data['coord_frame']['voxel_unit'] = "nanometers"
        data['coord_frame']['time_step'] = 0
        data['coord_frame']['time_step_unit'] = "na"

        data['experiment'] = {}
        data['experiment']['name'] = "exp1"
        data['experiment']['description'] = "Test experiment 1"
        data['experiment']['num_hierarchy_levels'] = 7
        data['experiment']['hierarchy_method'] = 'slice'
        data['experiment']['base_resolution'] = 0
        data['experiment']['max_time_sample'] = 0

        data['channel_layer'] = {}
        data['channel_layer']['name'] = "ch1"
        data['channel_layer']['description'] = "Test channel 1"
        data['channel_layer']['is_channel'] = True
        data['channel_layer']['datatype'] = 'uint8'

        data['boss_key'] = 'col1&exp1&ch1'
        data['lookup_key'] = '4&2&1'
        return data

    def get_image16_dict(self):
        """Method to get the config dictionary for an image16 resource"""
        data = self.get_image8_dict()
        data['channel_layer']['datatype'] = 'uint16'
        return data
