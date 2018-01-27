import os
import argparse
import json
import logging
import datetime
from backbone_pyspark_deploy.logger import create_logger
from backbone_pyspark_deploy.reading.reader import TableReader
from backbone_pyspark_deploy.cleaning.cleaner import Cleaner

from pyspark.sql import SparkSession


def driver():
    """
    - parse arguments
    - initialise spark session
    - read tables
    - clean tables
    - feature engineering
    - modeling

    :return: save model or prediction results
    """

    # ------------------------------------------------------------------------------------------------------------------
    # PARSE ARGUMENTS

    parser = argparse.ArgumentParser()

    parser.add_argument("--log-path", metavar="log", help="Path to log files", required=True)
    parser.add_argument("--params", metavar="FILE", required=True, help="parameter file .json")
    parser.add_argument("--predict", help="train model (predict=0 [default]) or make prediction (predict=1)")

    args = parser.parse_args()

    # logger
    log_path = args.log_path
    logger = create_logger(name="backbone_pyspark_deploy", level=logging.DEBUG, log_file_path=log_path)

    # Load user_config file
    with open(args.params, 'rb') as fp:
        params = json.load(fp)

    # predict
    predict = bool(int(args.predict))
    logger.debug("Predict parameter: {}".format(predict))

    # input tables input-data-01
    hive_db_data_01 = params["input-data-01"]["hive-db"]
    logger.debug("hive-db-data-01: {}".format(hive_db_data_01))

    tables_source_01 = params["input-data-01"]["input-tables"]
    logger.debug("list-tables-source-01: {}".format(str(tables_source_01)))

    # input tables input-data-02
    hive_db_data_02 = params["input-data-02"]["hive-db"]
    logger.debug("hive-db-data-02: {}".format(hive_db_data_02))

    tables_source_02 = params["input-data-02"]["input-tables"]
    logger.debug("list-tables-source-02: {}".format(str(tables_source_02)))

    # start date
    start_date = datetime.datetime.strptime(params["time"]["start-date"], "%Y-%m-%d")
    logger.debug("start date: {}".format(start_date))

    # end date
    end_date = datetime.datetime.strptime(params["time"]["end-date"], "%Y-%m-%d")
    logger.debug("end date: {}".format(end_date))

    # model output path
    model_output_path = os.path.join(params["output-path"]["hdfs"], *params["output-path"]["model"])
    logger.debug("model_output_path: {}".format(model_output_path))

    # prediction output path
    pred_output_path = os.path.join(params["output-path"]["hdfs"], *params["output-path"]["prediciton"])
    logger.debug("pred_output_path: {}".format(pred_output_path))

    # model name
    model_name = params["output-path"]["model-name"]
    logger.debug("model_name: {}".format(model_name))

    # ------------------------------------------------------------------------------------------------------------------
    # INITIALIZE SPARK SESSION

    spark = (SparkSession
             .builder
             # .master("local")
             .enableHiveSupport()
             .getOrCreate())

    # ------------------------------------------------------------------------------------------------------------------
    # READ TABLES

    logger.debug("----- READING -----")
    reader = TableReader(spark_session=spark)

    read_dict = {}

    for key in tables_source_01:
        read_dict[key] = reader.read_table(table="{}.{}".format(hive_db_data_01, tables_source_01[key]))
        read_dict[key].name = key
        logger.debug("Reading {} DONE".format(key))

    for key in tables_source_02:
        read_dict[key] = reader.read_table(table="{}.{}".format(hive_db_data_02, tables_source_02[key]))
        read_dict[key].name = key
        logger.debug("Reading {} DONE".format(key))

    # ------------------------------------------------------------------------------------------------------------------
    # CLEAN TABLES

    logger.debug("----- CLEANING -----")
    cleaner = Cleaner(logger=logger)

    clean_dict = {}

    for key in read_dict:
        clean_dict[key] = cleaner.clean_table(table=read_dict[key])

    # ------------------------------------------------------------------------------------------------------------------
    # FEATURE ENGINEERING

    logger.debug("----- FEATURE ENGINEERING -----")

    # ------------------------------------------------------------------------------------------------------------------
    # MODELING

    logger.debug("----- MODELING -----")

    logger.debug("----- THE END -----")

if __name__ == "__main__":
    try:
        driver()
    except Exception as e:
        logging.error("ERROR IN DRIVER", e)