__all__ = ["ArgParser"]


class ArgParser:
    """
    Handle arguments with the following priority:
    - command line
    - configuration file

    Raise error if missing
    """

    def __init__(self, args, config_file):
        self.args = args
        self.config = config_file

    def arg_parse(self, argument_key, config_section, config_key):
        """

        :param argument_key:
        :param config_section:
        :param config_key:
        :return:
        """

        if argument_key in self.args and self.arg[argument_key] is not None:
            return self.args[argument_key]

        elif config_section in self.config and config_key in self.config[config_section].keys() and \
                        self.config[config_section][config_key] != "":
            return self.config[config_section][config_key]

    def spark_conf_list(self):
        """

        :return:
        """

        spark_conf_list = []

        if "spark-conf" in self.config.sections():
            spark_conf = self.config["spark-conf"]

            for k in spark_conf.keys():
                spark_conf_list.append("--conf")
                spark_conf_list.append("{}={}".format(k, spark_conf[k]))