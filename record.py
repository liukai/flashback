#!/usr/bin/python
r""" TODO(kailiu) Please revise the docstrings
Track the MongoDB activities by tailing oplog and profiler output"""

from pymongo import MongoClient
import pymongo
import time
import pickle
import constants
import config


def now_in_utc_secs():
    """Get current time in seconds since UTC epoch"""
    return int(time.time())


def create_tailing_cursor(collection, criteria):
    """Create a cursor that constantly tail the latest documents from the
       database"""
    return collection.find(criteria, slave_okay=True, tailable=True)


def tail_to_file(tailor, filename, end_time_in_utc_secs, check_duration_secs=1):
    """Accepts a tailing cursor and serialize the retrieved documents to a
    file
    @param check_duration_secs: if we cannot retrieve the latest document,
        it will sleep for a period of time and then try again.
    """
    with open(filename, "w") as output:
        while tailor.alive:
            if now_in_utc_secs() > end_time_in_utc_secs:
                return
            print "trying to fetch the entry"
            try:
                doc = tailor.next()
                print "got it: ", str(doc)
                pickle.dump(doc, output)
            except StopIteration:
                time.sleep(check_duration_secs)


def get_start_time(collection):
    """Get the latest element's timestamp from a collection with "ts" field"""
    result = collection.find().limit(1).sort([("ts", pymongo.DESCENDING)])
    try:
        return result.next()["ts"]
    except StopIteration:
        return None


class MongoQueryRecorder:

    def __init__(self, db_config):
        self.config = db_config
        oplog_server = self.config["oplog_server"]
        profiler_server = self.config["profiler_server"]

        self.oplog_client = MongoClient(oplog_server["host"],
                                        oplog_server["port"])
        # Create a db client to profiler server only when it differs from oplog
        # server.
        self.profiler_client = \
            self.oplog_client if oplog_server == profiler_server else \
            MongoClient(profiler_server["host"], profiler_server["port"])

    def start_recording():
        pass

    def record_oplog(self, duration_secs=60 * 60):
        """Start recording the oplog entries starting from now.
        We only care about "insert" operations since all other queries will
        be captured by mongodb profiler.

        REQUIRED: the specific mongodb database has enabled profiling.
        """
        oplog_collection = \
            self.oplog_client[constants.LOCAL_DB][constants.OPLOG_COLLECTION]
        start_time = get_start_time(oplog_collection)
        criteria = {"op": "i"}
        if start_time:
            criteria["ts"] = {"$gt": start_time}

        tailor = create_tailing_cursor(oplog_collection, criteria)
        tail_to_file(tailor,
                     self.config["oplog_output_file"],
                     end_time_in_utc_secs=now_in_utc_secs() + duration_secs,
                     check_duration_secs=5)

    def record_profiler(self, duration_secs=60 * 60):
        """Start recording the profiler entries"""
        profiler_collection = \
            self.profiler_client[self.config["profiler_database"]] \
                                [constants.PROFILER_COLLECTION]
        start_time = get_start_time(profiler_collection)
        # ignore the insert
        profiler_namespace = "{0}.{1}".format(
            self.config["profiler_database"],
            constants.PROFILER_COLLECTION)

        criteria = {
            "op": {"$ne": "insert"}, "ns": {"$ne": profiler_namespace}
        }
        if start_time:
            criteria["ts"] = {"$gt": start_time}

        tailor = create_tailing_cursor(profiler_collection, criteria)
        tail_to_file(tailor,
                     self.config["profiler_output_file"],
                     now_in_utc_secs() + duration_secs,
                     check_duration_secs=5)


def main():
    """Recording the inbound traffic for a database."""
    db_config = config.DB_CONFIG
    recorder = MongoQueryRecorder(db_config)
    recorder.record_profiler(now_in_utc_secs())

if __name__ == '__main__':
    main()
