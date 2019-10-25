import datetime
import logging
import os
import sys
from functools import wraps
from logging.handlers import TimedRotatingFileHandler

logger = logging.getLogger()


def log_setup(parent_dir, log_filename, logger_level):
    # Set up logging directory, if necessary
    if not os.path.exists(parent_dir + '/logs'):
        os.makedirs(parent_dir + '/logs/')
    log_filename_path = parent_dir + '/logs/{}.log'.format(log_filename)

    # Start setup of logger
    logger = logging.getLogger()
    logger.setLevel(logger_level)
    handler = TimedRotatingFileHandler(log_filename_path, when='midnight', backupCount=10)
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s: line %(lineno)s - %(message)s',
        datefmt='%Y-%m-%d %I:%M:%S %p')
    handler.setFormatter(formatter)
    handler.suffix = '_%Y-%m-%d_%H%M%S.log'
    logger.addHandler(handler)

    # Create console handler with logging level at INFO
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logger_level)
    formatter = logging.Formatter("%(levelname)s - %(funcName)s: line %(lineno)s - %(message)s")
    console.setFormatter(formatter)
    logger.addHandler(console)

    return logger


def log_time(msg=''):
    def real_decorator(func):
        @wraps(func)  # To log the actual name of function passed (works in Python 3.2 and above)
        def log_wrapper(*args, **kwargs):
            logger.info('{}'.format(msg))
            start = datetime.datetime.now()

            result = func(*args, **kwargs)

            duration = datetime.datetime.now() - start
            logger.info('Duration for {}: {}'.format(msg, duration))

            return result
        return log_wrapper
    return real_decorator

