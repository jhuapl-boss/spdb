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

from spdb.spatialdb import AWSObjectStore
import argparse
import botocore
import boto3
import random
import time

# Attribute names in S3 index table.
LOOKUP_KEY = 'lookup-key'
OBJ_KEY = 'object-key'
VERSION_NODE = 'version-node'

class LookupKeyWriter(object):
    """
    Adds lookup key to legacy items in the S3 index table.  The lookup key is
    collection&experiment&channel&resolution.  A global secondary index (GSI)
    will use the lookup key to allow finding all cuboids that belong to a
    particular channel via a DynamoDB query.

    An instance of this class may be used as one worker in a parallelized
    scan of the S3 index table.  See the AWS documentation here:

    https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan

    Attributes:
        dynamodb (DynamoDB.Client): boto3 interface to DynamoDB.
        table (str): Name of S3 index table.
        max_items (int): Max number of items to return with each call to DynamoDB.Client.scan().
        worker_num (int): Zero-based worker number if parallelizing.
        num_workers (int): Total number of workers.
    """

    def __init__(
        self, table_name, region, max_items, worker_num=0, num_workers=1):
        """
        Constructor.

        If parallelizing, supply worker_num and num_workers.  See the AWS
        documentation for parallel scan here:

        https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan

        Args:
            table_name (str): Name of Dynamo S3 index table to operate on.
            region (str): AWS region table lives in.
            max_items (int): Max number of items to return with each call to DynamoDB.Client.scan().
            worker_num (optional[int]): Zero-based worker number if parallelizing.
            num_workers (optional[int]): Total number of workers.
        """
        self.dynamodb = boto3.client('dynamodb', region_name=region)
        self.table = table_name
        self.max_items = max_items
        self.worker_num = worker_num
        self.num_workers = num_workers
        if worker_num >= num_workers:
            raise ValueError('worker_num must be less than num_workers')

    def start(self):
        """
        Starts scan and update of S3 index table.
        """
        exclusive_start_key = None
        done = False
        count = 0
        while not done:
            resp = self.scan(exclusive_start_key)
            if resp is None:
                continue

            if 'LastEvaluatedKey' not in resp or len(resp['LastEvaluatedKey']) == 0:
                done = True
            else:
                exclusive_start_key = resp['LastEvaluatedKey']

            print('Consumed read capacity: {}'.format(resp['ConsumedCapacity']))

            for item in resp['Items']:
                print(item)
                self.add_lookup_key(item)

            if exclusive_start_key is not None:
                print('Continuing scan following {} - {}.'.format(
                    exclusive_start_key[OBJ_KEY]['S'], 
                    exclusive_start_key[VERSION_NODE]['N']))

            count+=1

            # Terminate early for testing.
            #if count > 2:
            #    done = True

        print('Update complete.')

    def scan(self, exclusive_start_key=None):
        """
        Invoke DynamoDB.Client.scan() and get up to self.max_items.

        Will use exponential backoff and retry 4 times if throttled by Dynamo.

        Args:
            exclusive_start_key (dict): If defined, start scan from this point in the table.

        Returns:
            (dict|None): Response dictionary from DynamoDB.Client.scan() or None if throttled repeatedly.
        """
        scan_args = {
            'TableName': self.table,
            'Limit': self.max_items,
            'ProjectionExpression': '#objkey,#vernode,#lookupkey',
            'FilterExpression':'attribute_not_exists(#lookupkey)',
            'ExpressionAttributeNames': {
                '#lookupkey': LOOKUP_KEY,
                '#objkey': OBJ_KEY,
                '#vernode': VERSION_NODE
            },
            'ConsistentRead': True,
            'ReturnConsumedCapacity': 'TOTAL'
        }

        if exclusive_start_key is not None:
            scan_args['ExclusiveStartKey'] = exclusive_start_key

        if self.num_workers > 1:
            scan_args['Segment'] = self.worker_num
            scan_args['TotalSegments'] = self.num_workers

        for backoff in range(0, 6):
            try:
                resp = self.dynamodb.scan(**scan_args)
                return resp
            except botocore.exceptions.ClientError as ex:
                if ex.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                    print('Throttled during scan . . .')
                    time.sleep(((2 ** backoff) + (random.randint(0, 1000) / 1000.0))/10.0)
                else:
                    raise

        return None

    def add_lookup_key(self, item):
        """
        Using the given item from the S3 index table, extract the lookup key
        from the object key and write it back to the item as a new attribute.

        If throttled, will use exponential backoff and retry 5 times.

        Args:
            item (dict): An item from the response dictionary returned by DynamoDB.Client.scan().
        """
        if OBJ_KEY not in item or 'S' not in item[OBJ_KEY]:
            return

        if VERSION_NODE not in item:
            return

        parts = AWSObjectStore.get_object_key_parts(item[OBJ_KEY]['S'])
        lookup_key = AWSObjectStore.generate_lookup_key(
            parts.collection_id, parts.experiment_id, parts.channel_id,
            parts.resolution)

        NUM_RETRIES = 5
        for backoff in range(0, NUM_RETRIES + 1):
            try:
                self.dynamodb.update_item(
                    TableName=self.table,
                    Key={OBJ_KEY: item[OBJ_KEY], VERSION_NODE: item[VERSION_NODE]},
                    ExpressionAttributeNames = {'#lookupkey': LOOKUP_KEY},
                    ExpressionAttributeValues = {':lookupkey': {'S': lookup_key}},
                    UpdateExpression='set #lookupkey = :lookupkey'
                )
                return
            except botocore.exceptions.ClientError as ex:
                if ex.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                    print('Throttled during update of item: {} - {}'.format(
                        item[OBJ_KEY]['S'], item[VERSION_NODE]['N']))
                    time.sleep(((2 ** backoff) + (random.randint(0, 1000) / 1000.0))/10.0)
                else:
                    print('Failed updating item: {} - {}'.format(
                        item[OBJ_KEY]['S'], item[VERSION_NODE]['N']))
                    raise
            except:
                print('Failed updating item: {} - {}'.format(
                    item[OBJ_KEY]['S'], item[VERSION_NODE]['N']))
                raise


        print('Failed and giving up after {} retries trying to update item: {} - {}'
            .format(NUM_RETRIES, item[OBJ_KEY]['S'], item[VERSION_NODE]['N']))


def script_args():
    """
    Parse command line arguments.

    Returns:
        (argparse.Namespace): Parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description='Script to update legacy items in the S3 index table with a lookup key.'
    )
    parser.add_argument(
        '--table-name', '-t',
        required=True,
        help='Name of S3 index table such as s3index.production.boss'
    )
    parser.add_argument(
        '--region', '-r',
        default='us-east-1',
        help='AWS region of table, default: us-east-1'
    )
    parser.add_argument(
        '--max-items', '-m',
        type=int,
        required=True,
        help='Max items to retrieve from DynamoDB in one scan operation'
    )
    parser.add_argument(
        '--worker_num',
        type=int,
        default=0,
        help='Zero-based worker id for this process when parallelizing.  Must be < --num_workers'
    )
    parser.add_argument(
        '--num_workers',
        type=int,
        default=1,
        help='Total number of parallel processes that will be used'
    )

    return parser.parse_args()


if __name__ == '__main__':
    args = script_args()
    writer = LookupKeyWriter(
        args.table_name, args.region, args.max_items,
        args.worker_num, args.num_workers)
    writer.start()

