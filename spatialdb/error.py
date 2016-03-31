"""
Copyright 2016 The Johns Hopkins University Applied Physics Laboratory

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from bossutils.logger import BossLogger
from enum import Enum


class ErrorCode(Enum):
    """
    Enumeration of error codes for the SPDB library.
    SPDB errors start at 1000
    """
    SPDB_ERROR = 1000
    DATATYPE_NOT_SUPPORTED = 1001
    FUTURE = 1002
    IO_ERROR = 1003
    REDIS_ERROR = 1004


class SpdbError(Exception):
    """
    Custom Error class that automatically logs the error for you

    When you reach a point in your code where you want to raise an exceptions

        raise SpdbError("Key already exists", "The key already exists.  When trying to create key it must not exist", 20001)

    """

    def __init__(self, *args):
        # Log
        blog = BossLogger().logger
        blog.error("SpdbError - Message: {0} - Description: {1} - Code: {2}".format(args[0], args[1], args[2]))
