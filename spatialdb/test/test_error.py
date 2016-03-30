import unittest
from spdb.spatialdb import SpdbError


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
