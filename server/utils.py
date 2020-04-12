import logging
import sys


def setup_logger(logger):
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(pathname)s:%(lineno)d - %(message)s',
                                  '%Y-%m-%d %H:%M:%S')
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
