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
import time
from datetime import datetime
from .error import SpdbError, ErrorCodes


class CacheStateDB(object):
    def __init__(self, kv_conf):
        """
        A class to implement the Boss cache state database and associated functionality

        Args:
            kv_conf(dict): Dictionary containing configuration details for the key-value store
        """
        self.kv_conf = kv_conf

        # Create client
        if "state_client" in self.kv_conf:
            self.status_client = self.kv_conf["state_client"]
        else:
            self.status_client = redis.StrictRedis(host=self.kv_conf["cache_state_host"], port=6379,
                                                   db=self.kv_conf["cache_state_db"])

        self.status_client_listener = None

    def create_page_in_channel(self):
        """
        Create a page in channel for monitoring a page-in operation

        Returns:
            (str): the page in channel name
        """
        channel_name = "PAGE-IN-CHANNEL&{}".format(uuid.uuid4().hex)
        self.status_client_listener = self.status_client.pubsub()
        self.status_client_listener.subscribe(channel_name)
        return channel_name

    def delete_page_in_channel(self, page_in_channel):
        """
        Method to remove a page in channel (after use) and close the pubsub connection
        Args:
            page_in_channel (str): Name of the subscription

        Returns:
            None
        """
        self.status_client_listener.punsubscribe(page_in_channel)
        self.status_client_listener.close()

    def wait_for_page_in(self, keys, page_in_channel, timeout):
        """
        Method to monitor page in operation and wait for all operations to complete

        Args:
            keys (list(str)): List of object keys to wait for
            page_in_channel (str): Name of the subscription
            timeout (int): Max # of seconds page in should take before an exception is raised.

        Returns:
            None
        """
        start_time = datetime.now()

        keys_set = set(keys)

        while True:
            msg = self.status_client_listener.get_message()

            # Parse message
            if msg["channel"] != page_in_channel:
                raise SpdbError('Message from incorrect channel received. Read operation aborted.',
                                ErrorCodes.ASYNC_ERROR)

            keys_set.remove(msg["data"])

            # Check if you have completed
            if len(keys_set) == 0:
                # Done!
                break

            # Check if too much time has passed
            if (start_time - datetime.now()).seconds > timeout:
                # Took too long! Something must have crashed
                self.delete_page_in_channel(page_in_channel)
                raise SpdbError('All data failed to page in before timeout elapsed.',
                                ErrorCodes.ASYNC_ERROR)

            # Sleep a bit
            time.sleep(0.05)





    add_cache_misses()