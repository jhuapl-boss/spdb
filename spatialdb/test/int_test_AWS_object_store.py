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

import unittest

from spdb.project import BossResourceBasic
from spdb.spatialdb import AWSObjectStore
from .test_AWS_object_store import AWSObjectStoreTestMixin

from bossutils import configuration
import time

import boto3
from botocore.exceptions import ClientError

from spdb.spatialdb.test.setup import SetupTests


class AWSObjectStoreTestIntegrationMixin(object):
    # todo: implement tests here or remove
    def test_put_get_objects_async(self):
        """Method to test putting and getting objects to and from S3"""
        #os = AWSObjectStore(self.object_store_config)

        #cached_cuboid_keys = ["CACHED-CUBOID&1&1&1&0&0&12", "CACHED-CUBOID&1&1&1&0&0&13"]
        #fake_data = [b"aaaadddffffaadddfffaadddfff", b"fffddaaffddffdfffaaa"]

        #object_keys = os.cached_cuboid_to_object_keys(cached_cuboid_keys)

        #os.put_objects(object_keys, fake_data)

        #returned_data = os.get_objects_async(object_keys)
        #for rdata, sdata in zip(returned_data, fake_data):
        #    assert rdata == sdata
        pass

    def test_page_in_objects(self):
        """Test method for paging in objects from S3 via lambda"""
        # os = AWSObjectStore(self.object_store_config)
        #
        # cached_cuboid_keys = ["CACHED-CUBOID&1&1&1&0&0&12", "CACHED-CUBOID&1&1&1&0&0&13"]
        # page_in_channel = "dummy_channel"
        # kv_config = {"param1": 1, "param2": 2}
        # state_config = {"param1": 1, "param2": 2}
        #
        # object_keys = os.page_in_objects(cached_cuboid_keys,
        #                                page_in_channel,
        #                                kv_config,
        #                                state_config)

        pass

    def test_trigger_page_out(self):
        """Test method for paging out objects to S3 via lambda"""
        # os = AWSObjectStore(self.object_store_config)
        #
        # cached_cuboid_keys = ["CACHED-CUBOID&1&1&1&0&0&12", "CACHED-CUBOID&1&1&1&0&0&13"]
        # page_in_channel = "dummy_channel"
        # kv_config = {"param1": 1, "param2": 2}
        # state_config = {"param1": 1, "param2": 2}
        #
        # object_keys = os.page_in_objects(cached_cuboid_keys,
        #                                page_in_channel,
        #                                kv_config,
        #                                state_config)

        pass

    def test_table_timing(self):
        """Test method for paging out objects to S3 via lambda"""
        # os = AWSObjectStore(self.object_store_config)
        #
        # cached_cuboid_keys = ["CACHED-CUBOID&1&1&1&0&0&12", "CACHED-CUBOID&1&1&1&0&0&13"]
        # page_in_channel = "dummy_channel"
        # kv_config = {"param1": 1, "param2": 2}
        # state_config = {"param1": 1, "param2": 2}
        #
        # object_keys = os.page_in_objects(cached_cuboid_keys,
        #                                page_in_channel,
        #                                kv_config,
        #                                state_config)

        from bossutils.aws import get_region
        import uuid
        import pickle

        from spdb.spatialdb import AWSObjectStore
        os = AWSObjectStore(self.object_store_config)

        import boto3
        s3 = boto3.client("s3", region_name=get_region())

        import timeit

        print("Time for checking cuboids_exist")
        time_samples = []
        for cnt in range(1, 750):
            start_time = timeit.default_timer()
            exist_keys, missing_keys = os.cuboids_exist("1&50&65&0&0&{}".format(cnt))
            time_samples.append(timeit.default_timer() - start_time)

        print("Maximum time: {}".format(max(time_samples)))
        print("Minimum time: {}".format(min(time_samples)))

        tt = sorted(time_samples)
        tt = tt[1:]
        tt = tt[:-1]
        print("Average time: {}".format(float(sum(tt)) / max(len(tt), 1)))


        print("Time for checking dynamodb")
        dynamodb = boto3.client('dynamodb', region_name=get_region())
        time_samples = []
        for cnt in range(1, 750):
            start_time = timeit.default_timer()
            response = dynamodb.get_item(
                TableName=self.object_store_config['s3_index_table'],
                Key={'object-key': {'S': "{}&1&50&65&0&0&{}".format(uuid.uuid4().hex, cnt)}, 'version': {'S': 'a'}},
                ConsistentRead=True,
                ReturnConsumedCapacity='NONE')

            time_samples.append(timeit.default_timer() - start_time)

        print("Maximum time: {}".format(max(time_samples)))
        print("Minimum time: {}".format(min(time_samples)))

        with open("/home/ubuntu/dynamo.pickle", 'wb') as file_handle:
            pickle.dump(time_samples, file_handle)

        tt = sorted(time_samples)
        tt = tt[1:]
        tt = tt[:-1]
        print("Average time: {}".format(float(sum(tt)) / max(len(tt), 1)))



        print("Time for checking s3")
        time_samples = []
        for cnt in range(1, 750):
            start_time = timeit.default_timer()
            try:
                response = s3.get_object(Key="{}&1&50&65&0&0&{}".format(uuid.uuid4().hex, cnt), Bucket=self.object_store_config["cuboid_bucket"])
            except:
                pass
            time_samples.append(timeit.default_timer() - start_time)

        print("Maximum time: {}".format(max(time_samples)))
        print("Minimum time: {}".format(min(time_samples)))

        with open("/home/ubuntu/s3.pickle", 'wb') as file_handle:
            pickle.dump(time_samples, file_handle)

        tt = sorted(time_samples)
        tt = tt[1:]
        tt = tt[:-1]
        print("Average time: {}".format(float(sum(tt)) / max(len(tt), 1)))



class TestAWSObjectStoreInt(AWSObjectStoreTestIntegrationMixin, AWSObjectStoreTestMixin, unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """ get_some_resource() is slow, to avoid calling it for each test use setUpClass()
            and store the result as class variable
        """
        super(TestAWSObjectStoreInt, cls).setUpClass()
        cls.setUpParams(cls)
        try:
            cls.setup_helper.create_s3_index_table(cls.object_store_config["s3_index_table"])
        except ClientError:
            cls.setup_helper.delete_s3_index_table(cls.object_store_config["s3_index_table"])
            cls.setup_helper.create_s3_index_table(cls.object_store_config["s3_index_table"])

        try:
            cls.setup_helper.create_cuboid_bucket(cls.object_store_config["cuboid_bucket"])
        except ClientError:
            cls.setup_helper.delete_cuboid_bucket(cls.object_store_config["cuboid_bucket"])
            cls.setup_helper.create_cuboid_bucket(cls.object_store_config["cuboid_bucket"])

    def setUpParams(self):
        """ Create a diction of configuration values for the test resource. """
        self.data = {}
        self.data['collection'] = {}
        self.data['collection']['name'] = "col1"
        self.data['collection']['description'] = "Test collection 1"

        self.data['coord_frame'] = {}
        self.data['coord_frame']['name'] = "coord_frame_1"
        self.data['coord_frame']['description'] = "Test coordinate frame"
        self.data['coord_frame']['x_start'] = 0
        self.data['coord_frame']['x_stop'] = 2000
        self.data['coord_frame']['y_start'] = 0
        self.data['coord_frame']['y_stop'] = 5000
        self.data['coord_frame']['z_start'] = 0
        self.data['coord_frame']['z_stop'] = 200
        self.data['coord_frame']['x_voxel_size'] = 4
        self.data['coord_frame']['y_voxel_size'] = 4
        self.data['coord_frame']['z_voxel_size'] = 35
        self.data['coord_frame']['voxel_unit'] = "nanometers"
        self.data['coord_frame']['time_step'] = 0
        self.data['coord_frame']['time_step_unit'] = "na"

        self.data['experiment'] = {}
        self.data['experiment']['name'] = "exp1"
        self.data['experiment']['description'] = "Test experiment 1"
        self.data['experiment']['num_hierarchy_levels'] = 7
        self.data['experiment']['hierarchy_method'] = 'slice'

        self.data['channel_layer'] = {}
        self.data['channel_layer']['name'] = "ch1"
        self.data['channel_layer']['description'] = "Test channel 1"
        self.data['channel_layer']['is_channel'] = True
        self.data['channel_layer']['datatype'] = 'uint8'
        self.data['channel_layer']['max_time_step'] = 0

        self.data['boss_key'] = 'col1&exp1&ch1'
        self.data['lookup_key'] = '4&2&1'

        self.resource = BossResourceBasic(self.data)

        self.config = configuration.BossConfig()

        # Get domain info
        parts = self.config['aws']['cache'].split('.')
        domain = "{}.{}".format(parts[1], parts[2])

        self.object_store_config = {"s3_flush_queue": 'https://mytestqueue.com',
                                    "cuboid_bucket": "int_test_bucket.{}".format(domain),
                                    "page_in_lambda_function": "page_in.{}".format(domain),
                                    "page_out_lambda_function": "page_out.{}".format(domain),
                                    "s3_index_table": "int_test_s3_index_table.{}".format(domain)}
        self.setup_helper = SetupTests()
        self.setup_helper.mock = False

    @classmethod
    def tearDownClass(cls):
        super(TestAWSObjectStoreInt, cls).tearDownClass()
        try:
            cls.setup_helper.delete_s3_index_table(cls.object_store_config["s3_index_table"])
        except:
            pass

        try:
            cls.setup_helper.delete_cuboid_bucket(cls.object_store_config["cuboid_bucket"])
        except:
            pass


