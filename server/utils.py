import logging
import os
import sys
from pathlib import Path
from datetime import datetime


def setup_logger(logger, log_file):
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(pathname)s:%(lineno)d - %(message)s',
                                  '%Y-%m-%d %H:%M:%S')
    if log_file:
        log_file = datetime.now().strftime(log_file)
        os.makedirs(Path(log_file).parent, exist_ok=True)
        handler = logging.FileHandler(log_file, mode='a')
    else:
        handler = logging.StreamHandler(sys.stdout)

    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
