import logging
from logging import handlers
import threading


LOGGING_FORMAT_DEFAULT = '%(asctime)s [%(threadName)-12.12s] [%(levelname)-4.4s] {%(name)s} %(message)s'


class ConsoleLoggingFormatter(logging.Formatter):
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    logging_format = LOGGING_FORMAT_DEFAULT

    FORMATS = {
        logging.DEBUG: grey + logging_format + reset,
        logging.INFO: grey + logging_format + reset,
        logging.WARNING: yellow + logging_format + reset,
        logging.ERROR: red + logging_format + reset,
        logging.CRITICAL: bold_red + logging_format + reset
    }

    def __init__(self, logging_format=LOGGING_FORMAT_DEFAULT):
        super(ConsoleLoggingFormatter, self).__init__(logging_format)

    def format(self, record):
        logging_format = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(logging_format)
        return formatter.format(record)


_lock = threading.Lock()


class LoggerSingletonMetaClass(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            with _lock:
                if cls not in cls._instances:
                    cls._instances[cls] = super(LoggerSingletonMetaClass, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class LoggerSingleton(metaclass=LoggerSingletonMetaClass):

    def __init__(self, logging_level=logging.INFO):        
        root_logger = logging.getLogger()

        file_logging_formatter = logging.Formatter(
            LOGGING_FORMAT_DEFAULT
        )
        file_handler = handlers.RotatingFileHandler(
            "logs/dione.log", mode='a', maxBytes=5*1024*1024, backupCount=10, encoding=None, delay=0)
        file_handler.setFormatter(file_logging_formatter)
        root_logger.addHandler(file_handler)

        console_logging_formatter = ConsoleLoggingFormatter(
            LOGGING_FORMAT_DEFAULT
        )
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_logging_formatter)
        root_logger.addHandler(console_handler)

        root_logger.setLevel(logging_level)

        self.root_logger = root_logger


def init_logger() -> logging.Logger:
    logger_singleton = LoggerSingleton()
    return logger_singleton.root_logger


def get_logger(name) -> logging.Logger:
    return logging.getLogger(name)


init_logger()