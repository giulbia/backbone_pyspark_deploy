# from pyspark.sql import Row

from backbone_pyspark_deploy.cleaning.cleaner import Cleaner
from tests import TestBase

import unittest
import datetime


class TestCleaner(TestBase):
    """
    Test functions in Cleaner
    """

    @classmethod
    def setUpTest(cls):
        """

        :return:
        """

        cls.cleaner = Cleaner(logger=cls.logger)

    def test_xxx(self):

        self.assertEqual(1, 1)


if __name__ == "__main__":
    unittest.main()
