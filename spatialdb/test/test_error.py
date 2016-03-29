import unittest
from spatialdb.error import SpdbError
import bossutils
import tempfile
import os
from bossutils.logger import BossLogger
from unittest.mock import patch


class MockBossLogger(BossLogger):
    """Basic mock for BossConfig to contain the properties needed for this test"""
    LOG_FILE = os.path.join(tempfile.TemporaryDirectory().name, 'test_logfile.log')

    def __init__(self):
        BossLogger.__init__(self)
        self.LOG_FILE = os.path.join(tempfile.TemporaryDirectory().name, 'test_logfile.log')


@patch('bossutils.logger.BossLogger', MockBossLogger)
class SpdbErrorTests(unittest.TestCase):

    def test_creation(self):
        with self.assertRaises(SpdbError):
            raise SpdbError('whoops', 'Something went wrong!', 2000)

    def test_params(self):
        try:
            raise SpdbError('whoops', 'Something went wrong!', 2000)
        except SpdbError as err:
            assert err.args[0] == 'whoops'
            assert err.args[1] == 'Something went wrong!'
            assert err.args[2] == 2000
