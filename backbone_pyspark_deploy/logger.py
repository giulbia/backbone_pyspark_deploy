import logging


def create_logger(name, level, log_file_path, tests=False):
    """

    :param name:
    :param level:
    :param log_file_path:
    :param tests:
    :return:
    """

    process_logger = logging.getLogger(name)

    # set level
    process_logger.setLevel(level)

    # create a logging format
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - % (message)s")

    # console log handler
    console_log = logging.StreamHandler()
    console_log.setFormatter(formatter)
    process_logger.addHandler(console_log)

    # we don't want to store logs for unit testing, only console displaying
    if not tests:
        # file log handler
        file_log = logging.FileHandler(log_file_path)
        file_log.setFormatter(formatter)
        process_logger.addHandler(file_log)

    return process_logger
