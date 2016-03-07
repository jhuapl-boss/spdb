from bossutils.logger import BossLogger


class SpdbError(Exception):
    """
    Custom Error class that automatically logs the error for you

    When you reach a point in your code where you want to raise an exceptions

        raise SpdbError("Key already exists", "The key already exists.  When trying to create key it must not exist", 20001)

    """

    def __init__(self, *args):
        # Log
        blog = BossLogger()
        blog.error("SpdbError - Message: {0} - Description: {1} - Code: {2}".format(args[0], args[1], args[2]))
