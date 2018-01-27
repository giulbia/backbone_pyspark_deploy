from pyspark.sql import functions as F

__all__ = ["Cleaner"]


class Cleaner:
    """
    Class to clean tables
    """

    def __init__(self, logger):
        self.logger = logger
        self.call_cleaner_dict = self._init_call_cleaner_dict()

    def _clean_table_name(self, df):
        """
        Clean table_name

        :param df: table_name
        :return: cleaned data frame
        """

        self.logger.debug("Clean table_name")

        return df

    def _init_call_cleaner_dict(self):
        """
        Initialize call_cleaner_dict

        :return: dictinary
        """

        call_cleaner_dict = {
            "table_name": self._clean_table_name
        }

        return call_cleaner_dict

    def clean_table(self, table):
        """
        Clean each table

        :param table: data frame to be cleaned
        :return: cleaned data frame
        """

        self.logger.debug("Cleaning table {}".fomat(table.name))

        return self.call_cleaner_dict[table.table](table)