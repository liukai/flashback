"""Globally shared common utilities functions/classes/variables"""
import logging
import config
import pickle
import time
import pymongo


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


def unpickle(input_file):
    """Safely unpack entry from the file"""
    try:
        return pickle.load(input_file)
    except EOFError:
        return None


def unpickle_iterator(filename):
    """Return the unpickled objects as a sequence of objects"""
    f = open(filename)
    while True:
        result = unpickle(f)
        if result:
            yield result
        else:
            raise StopIteration


def now_in_utc_secs():
    """Get current time in seconds since UTC epoch"""
    return int(time.time())


def create_tailing_cursor(collection, criteria):
    """Create a cursor that constantly tail the latest documents from the
       database"""
    tailor = collection.find(
        criteria, slave_okay=True, tailable=True, await_data=False)
    return tailor


def get_start_time(collection):
    """Get the latest element's timestamp from a collection with "ts" field"""
    result = collection.find().limit(1).sort([("ts", pymongo.DESCENDING)])
    try:
        return result.next()["ts"]
    except StopIteration:
        return None
