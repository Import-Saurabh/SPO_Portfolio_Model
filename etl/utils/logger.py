import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "etl.log"

def get_logger(name: str = "etl", level: int = logging.INFO):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")

    stream_h = logging.StreamHandler(sys.stdout)
    stream_h.setFormatter(fmt)
    logger.addHandler(stream_h)

    file_h = RotatingFileHandler(LOG_FILE, maxBytes=10_000_000, backupCount=5)
    file_h.setFormatter(fmt)
    logger.addHandler(file_h)

    logger.propagate = False
    return logger
