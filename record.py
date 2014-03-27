#!/usr/bin/python
r""" Track the MongoDB activities by tailing oplog and profiler output"""

from pymongo import MongoClient
from threading import Thread
import Queue
import config
import constants
import pickle
import pymongo
import time
import utils


def now_in_utc_secs():
    """Get current time in seconds since UTC epoch"""
    return int(time.time())


def create_tailing_cursor(collection, criteria):
    """Create a cursor that constantly tail the latest documents from the
       database"""
    return collection.find(criteria, slave_okay=True, tailable=True)


def get_start_time(collection):
    """Get the latest element's timestamp from a collection with "ts" field"""
    result = collection.find().limit(1).sort([("ts", pymongo.DESCENDING)])
    try:
        return result.next()["ts"]
    except StopIteration:
        return None


class MongoQueryRecorder(object):

    """Record MongoDB database's activities by polling the oplog and profiler
    results"""

    class RecordingState(object):

        """Keeps the running status of a recording request"""

        def __init__(self):
            self.processing = True
            # hardcode 2 entry counts for oplog and profiler results.
            # Logically, any element in the lists below will be accessed by
            # one and only one thread, thus we don't use any lock to ensure
            # the consistency.
            self.entries_received = [0] * 2
            self.entries_written = [0] * 2

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

    @staticmethod
    def _process_doc_queue(doc_queue, files, state):
        """Writes the incoming docs to the corresponding files"""
        while state.processing:
            try:
                index, doc = doc_queue.get(block=True, timeout=1)
                state.entries_written[index] += 1
                pickle.dump(doc, files[index])
            except Queue.Empty:
                # gets nothing after timeout
                continue

    @staticmethod
    def _tail_to_queue(tailor, identifier, doc_queue, state,
                       check_duration_secs=1):
        """Accepts a tailing cursor and serialize the retrieved documents to a
        fifo queue
        @param identifier: when passing the retrieved document to the queue, we
            will attach a unique identifier that allows the queue consumers to
            process different sources of documents accordingly.
        @param check_duration_secs: if we cannot retrieve the latest document,
            it will sleep for a period of time and then try again.
        """
        while tailor.alive and state.processing:
            try:
                doc = tailor.next()
                doc_queue.put_nowait((identifier, doc))
                state.entries_received[identifier] += 1
            except StopIteration:
                time.sleep(check_duration_secs)

    @staticmethod
    def _report_status(state):
        """report current processing status"""
        msgs = []
        for idx, source in enumerate(["<oplog>", "<profiler>"]):
            msg = "{0}: received {1} entries, {2} of them were written". \
                  format(source,
                         state.entries_received[idx],
                         state.entries_received[idx])
            msgs.append(msg)

        utils.LOG.info("; ".join(msgs))

    def get_oplog_tailor(self):
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

        return create_tailing_cursor(oplog_collection, criteria)

    def get_profiler_tailor(self):
        """Start recording the profiler entries"""
        profiler_collection = \
            self.profiler_client[self.config["target_database"]] \
                                [constants.PROFILER_COLLECTION]
        start_time = get_start_time(profiler_collection)
        # ignore the inserts
        profiler_namespace = "{0}.{1}".format(
            self.config["target_database"],
            constants.PROFILER_COLLECTION)
        index_namespace = "{0}.{1}".format(
            self.config["target_database"],
            constants.INDEX_COLLECTION)

        print [profiler_namespace, index_namespace]
        criteria = {
            "op": {"$ne": "insert"},
            "ns": {"$nin": [profiler_namespace, index_namespace]}
        }
        if start_time:
            criteria["ts"] = {"$gt": start_time}

        return create_tailing_cursor(profiler_collection, criteria)

    def start_recording(self):
        """record the activities in the multithreading way"""
        end_time = now_in_utc_secs() + self.config["duration_secs"]

        doc_queue = Queue.Queue()
        state = MongoQueryRecorder. RecordingState()
        # We'll dump the recorded activities to `files`.
        files = [
            open(self.config["oplog_output_file"], "wb"),
            open(self.config["profiler_output_file"], "wb")
        ]

        # Create background threads to handle to track/dump mongodb activities
        threads = []
        # Writer thread, we only have one writer since we assume all files will
        # be written to the same device (disk or SSD), as a result it yields
        # not much benefit to have multiple writers.
        threads.append(Thread(
            target=MongoQueryRecorder._process_doc_queue,
            args=(doc_queue, files, state)
        ))
        threads.append(Thread(
            target=MongoQueryRecorder._tail_to_queue,
            args=(self.get_oplog_tailor(), 0, doc_queue, state)
        ))
        threads.append(Thread(
            target=MongoQueryRecorder._tail_to_queue,
            args=(self.get_profiler_tailor(), 1, doc_queue, state)
        ))

        for thread in threads:
            thread.start()

        # Processing for a time range
        while now_in_utc_secs() < end_time:
            MongoQueryRecorder._report_status(state)
            time.sleep(5)
        MongoQueryRecorder._report_status(state)

        state.processing = False
        for thread in threads:
            thread.join()


def main():
    """Recording the inbound traffic for a database."""
    db_config = config.DB_CONFIG
    recorder = MongoQueryRecorder(db_config)
    recorder.start_recording()

if __name__ == '__main__':
    main()
