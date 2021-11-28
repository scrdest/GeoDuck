import os
import logging
import logging.handlers

from app import constants as const

loggers = {}


def build_logger(name=None, level=None, log_filename=None):
    _name = name or const.APP_NAME
    _log_filename = log_filename or os.path.join(
        const.LOG_DIR,
        f"{const.APP_NAME}-log"
    )
    os.makedirs(const.LOG_DIR, exist_ok=True)

    new_logger = logging.getLogger(_name)

    formatter = logging.Formatter(
        # fmt="%(asctime)s - %(funcName)s@%(filename)s:%(lineno)d | %(levelname)s: %(message)s"
        fmt="%(asctime)s - %(threadName)s | %(levelname)s: %(message)s"
    )

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    file_handler = logging.handlers.TimedRotatingFileHandler(
        _log_filename,
        when="midnight",
        utc=True,
        encoding="utf8"
    )
    file_handler.setFormatter(formatter)

    new_logger.setLevel(level or logging.INFO)
    new_logger.addHandler(stream_handler)
    new_logger.addHandler(file_handler)

    loggers[_name] = new_logger

    return new_logger


def get_logger(name=None, **builder_kwargs):
    _name = name or const.APP_NAME

    logger = loggers.get(_name) or build_logger(
        name=_name,
        **builder_kwargs
    )
    return logger


logger = get_logger()
