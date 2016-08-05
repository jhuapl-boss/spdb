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

from bossutils.logger import BossLogger
from enum import IntEnum


class ErrorCodes(IntEnum):
    """
    Enumeration of Error codes to support consistency

    SPDB errors are 100-199
    """
    SPDB_ERROR = 100
    DATATYPE_NOT_SUPPORTED = 101
    FUTURE = 102
    REDIS_ERROR = 103
    ASYNC_ERROR = 104
    SERIALIZATION_ERROR = 105
    DATATYPE_MISMATCH = 106
    OBJECT_STORE_ERROR = 107
    RESOURCE_LOCKED = 108


class SpdbError(Exception):
    """
    Custom Error class that automatically logs the error for you

    When you reach a point in your code where you want to raise an exceptions

        raise SpdbError("The key already exists.  When trying to create key it must not exist", ErrorCodes.SPDB_ERROR)

    """

    def __init__(self, *args):
        # Log
        # TODO: Look into removing boss logger dependency
        blog = BossLogger().logger
        blog.error("SpdbError - Message: {0} - Code: {1}".format(args[0], args[1]))
