import argparse
import configparser
import subprocess
import os
import sys
import logging
import zipfile
import shutil

from backbone_pyspark_deploy.arg_parser import ArgParser
from backbone_pyspark_deploy.logger import create_logger


def set_env(key, value):
    """

    :param key:
    :param value:
    :return:
    """

    if os.getenv(key) is None:
        if value is None:
            sys.exit(-1)
        else:
            os.environ[key] = value

    return os.getenv(key)


def main():
    """
    Entry point for .pex

    :return: spark-submit
    """

    # ------------------------------------------------------------------------------------------------------------------
    # CONFIGURATION FILES default values

    CUR_DIR = os.getcwd()

    # ------------------------------------------------------------------------------------------------------------------
    # PARSE ARGUMENTS

    parser = argparse.ArgumentParser()

    parser.add_argument("--properties", metavar="FILE", required=True, help="configuration file .ini")
    parser.add_argument("--params", metavar="FILE", required=True, help="parameters file .json")

    parser.add_argument("--predict", action="store_true",
                        help="train model (--predict not present [default]) or make prediction (--predict)")

    parser.add_argument("--spark-home", metavar="SPARK_HOME", help="Path to SPARK_HOME", required=False)
    parser.add_argument("--master", metavar="SPARK MASTER", help="Spark master", default=None)

    parser.add_argument("--deploy-mode", metavar="DEPLOY MODE", choices=["cluster", "client"], help="Spark deploy mode",
                        default=None)

    parser.add_argument("--num-executors", metavar="", help="Number of executors (ex: 2)",  default=None)
    parser.add_argument("--executor-memory", metavar="", help="Executor memory (ex: 8G)",  default=None)
    parser.add_argument("--executor-cores", metavar="", help="Executor cores (ex: 8)",  default=None)
    parser.add_argument("--py-files", metavar="PKG", help="Path to .pex file",  default=None)
    parser.add_argument("--name", metavar="", help="Application name", default=None)

    parser.add_argument("log-path", metavar="FILE", help="Path to log file", default=None)

    args = parser.parse_args()

    # Load run_config.ini file
    runconf = configparser.ConfigParser()
    runconf.read(args.properties)

    arg_parser_ini = ArgParser(args=vars(args), config_file=runconf)

    # logger
    log_path = arg_parser_ini.arg_parse(argument_key="log_path", config_section="logging", config_key="log-path")
    logger = create_logger(name="backone_pyspark_deploy", level=logging.DEBUG, log_file_path=log_path)

    # spark-home
    spark_home = arg_parser_ini.arg_parse(argument_key="spark_home", config_section="spark-cli", config_key="spark-home")
    set_env("SPARK_HOME", spark_home)

    logger.debug("SPARK HOME: {}".format(os.getenv("SPARK_HOME")))

    # master
    master = arg_parser_ini.arg_parse(argument_key="master", config_section="spark-cli", config_key="master")
    logger.debug("Master parameter: {}".format(master))

    # deploy-mode
    deploy_mode = arg_parser_ini.arg_parse(argument_key="deploy_mode", config_section="spark-cli",
                                           config_key="deploy-mode")
    logger.debug("Deploy mode parameter: {}".format(deploy_mode))

    # num-executors
    num_executors = arg_parser_ini.arg_parse(argument_key="num_executors", config_section="spark-cli",
                                             config_key="num-executors")
    logger.debug("Number executors parameter: {}".format(num_executors))

    # executor-memory
    executor_memory = arg_parser_ini.arg_parse(argument_key="executor_memory", config_section="spark-cli",
                                               config_key="executor-memory")
    logger.debug("Executor memory parameter: {}".format(executor_memory))

    # executor-cores
    executor_cores = arg_parser_ini.arg_parse(argument_key="executor_cores", config_section="spark-cli",
                                              config_key="executor-cores")
    logger.debug("Executor cores parameter: {}".format(executor_cores))

    # py-files
    py_files = arg_parser_ini.arg_parse(argument_key="py_files", config_section="spark-cli", config_key="py-files")
    logger.debug("Py files parameter: {}".format(py_files))

    # name
    name = arg_parser_ini.arg_parse(argument_key="name", config_section="spark-cli", config_key="name")
    logger.debug("Name parameter: {}".format(name))

    zip_list = os.listdir(os.path.join(spark_home, "python", "lib"))

    sys.path.append([os.path.join(spark_home, "python", "lib", zip_file) for zip_file in zip_list])

    # ------------------------------------------------------------------------------------------------------------------
    # UNZIP PEX

    pex = sys.argv[0]
    logger.debug("PEX: {}".format(pex))

    zipper = zipfile.ZipFile(pex)
    zipper.extractall(path=os.path.join(CUR_DIR, ".backbone_pyspark_deploy"))

    # ------------------------------------------------------------------------------------------------------------------
    # SPARK SUBMIT

    backbone_pyspark_egg = ""
    egg_list = os.listdir(os.path.join(CUR_DIR, ".backbone_pyspark_deploy", ".deps"))
    logger.debug("egg-list: {}".format(str(egg_list)))
    py_files = ("" if py_files is None else py_files)

    for egg in egg_list:
        zipped = shutil.make_archive(base_name=egg, format="zip",
                                     root_dir=os.path.join(CUR_DIR, ".backbone_pyspark_deploy", ".deps", egg))
        logger.debug("zipped: {}".format(zipped))
        if "backbone_pyspark_deploy" in egg:
            backbone_pyspark_egg = egg
            logger.debug("backbone_pyspark_egg: {}".format(backbone_pyspark_egg))
        py_files += "," + os.path.join(CUR_DIR, ".backbone_pyspark_deploy", ".deps", zipped)
    py_files = py_files.strip(",")

    spark_conf_list = arg_parser_ini.spark_conf_list()

    logger.debug("--py-files: {}".format(py_files))

    config_files = args.properties + "," + args.params + "," + os.path.join(spark_home, "conf", "hive-site.xml")

    logger.debug("--files: {}".format(config_files))

    logger.debug("--params: {}".format(args.params))

    logger.debug("--predict: {}".format(args.predict))

    logger.debug("--log-path: {}".format(log_path))

    spark_submit = os.path.join(spark_home, "bin", "spark-submit")

    predict = int(args.predict)

    params = (os.path.join(args.params) if deploy_mode == "cluster" else args.params)

    cmd = [
              spark_submit,
              "--master", master,
              "--deploy-mode", deploy_mode,
              "--num-executors", num_executors,
              "--executor-memory", executor_memory,
              "--executor-cores", executor_cores,
              "--name", name,
              "--files", config_files,
              "--py-files", py_files
          ] + spark_conf_list + \
          [
              os.path.join(CUR_DIR, ".backbone_pyspark_deploy", ".deps",
                           backbone_pyspark_egg, "backbone_pyspark_deploy", "driver.py"),
              "--params", params,
              "--predict", str(predict),
              "--log-path", log_path
          ]

    logger.debug("cmd spark-submit: {}".format(str(cmd)))

    subprocess.run(cmd)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error("Error in main", )


