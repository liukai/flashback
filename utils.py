"""Globally shared common utilities functions/classes/variables"""
import logging
import config


def _make_logger():
    """Create a new logger"""
    logger = logging.getLogger("parse.flashback")
    logger.setLevel(config.APP_CONFIG["logging_level"])
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s:%(processName)s] %(message)s",
        "%m-%d %H:%M:%S"))
    logger.addHandler(handler)

    return logger

# log will be used globally for all logging code.
LOG = _make_logger()
