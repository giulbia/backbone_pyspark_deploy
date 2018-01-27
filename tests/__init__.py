import unittest
import logging

from pyspark.sql import SparkSession

from backbone_pyspark_deploy.logger import create_logger


class TestBase(unittest.TestCase):
    """
    Base class for testing. Initialize spark session
    """

    spark = None

    @classmethod
    def setUpClass(cls):
        """
        Setup spark session

        :return: spark session
        """

        cls.spark = (SparkSession
                     .builder
                     .master("local")
                     .enableHiveSupport()
                     .getOrCreate())

        cls.logger = create_logger(name="backbone_pyspark_deploy", level=logging.DEBUG, tests=True, log_file_path=None)

        cls.setUpTest()

        cls.logger.debug("SET UP CLASS")

    @classmethod
    def tearDownClass(cls):
        """
        Stop spark session
        """

        # cls.spark.stop()

        cls.logger.debug("TEAR DOWN CLASS")

        cls.tearDownTest()

    @classmethod
    def setUpTest(cls):
        pass

    @classmethod
    def tearDownTest(cls):
        pass