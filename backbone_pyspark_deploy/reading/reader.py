__all__ = ["TableReader"]


class TableReader:
    """
    Class to read input tables
    """

    def __init__(self, spark_session):
        self.spark_session = spark_session

    def read_table(self, table):
        """
        Read hive table into data frame

        :param table: str as database.table_name
        :return: data frame
        """

        return self.spark_session.read.table(table)

    def read_sample(self, table, n_lines):
        """
        Read n_lines from hive table into data frame

        :param table: str as database.table_name
        :param n_lines: # lines to read
        :return: data frame
        """

        df = self.spark_session.read.table(table)

        return df.limit(n_lines)
