#!/usr/bin/python
r""" Track the MongoDB activities by tailing oplog and profiler output"""

from pymongo import MongoClient
from threading import Thread
from datetime import datetime
from bson.timestamp import Timestamp
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


def _test_final_output(filename):
    """Just for test: help the manual check of the final output"""
    for i, doc in enumerate(unpickle_iterator(filename)):
        print i, ":", doc


def complete_insert_ops(oplog_output_file, profiler_output_file, output_file):
    """
    * Why merge files:
        we need to merge the docs from two sources into one.
    * Why not merge earlier:
        It's definitely inefficient to merge the entries when we just retrieve
        these documents from mongodb. However we designed this script to be able
        to pull the docs from differnt servers, as a result it's hard to do the
        on-time merge since you cannot determine if some "old" entries will come
        later. """
    oplog = open(oplog_output_file, "rb")
    profiler = open(profiler_output_file, "rb")
    output = open(output_file, "wb")
    logger = utils.LOG

    logger.info("Starts completing the insert options")
    oplog_doc = unpickle(oplog)
    profiler_doc = unpickle(profiler)
    inserts = 0
    noninserts = 0

    while oplog_doc and profiler_doc:
        if profiler_doc["op"] != "insert":
            pickle.dump(profiler_doc, output)
            noninserts += 1
            profiler_doc = unpickle(profiler)
        else:
            # Replace the the profiler's insert operation doc with oplog's,
            # but keeping the canonical form of "ts".
            profiler_ts = profiler_doc["ts"]
            oplog_ts = oplog_doc["ts"].as_datetime()
            # only care about the second-level precision.
            if profiler_ts.timetuple()[:6] != oplog_ts.timetuple()[:6]:
                # TODO strictly speaking, this ain't good since the files are
                # not propertly closed.
                logger.error(
                    "oplog and profiler results are inconsitent `ts`\n"
                    "  oplog:    %s\n"
                    "  profiler: %s", str(oplog_doc), str(profiler_doc))
                return False

            oplog_doc["ts"] = profiler_doc["ts"]
            # make sure "op" is "insert" instead of "i".
            oplog_doc["op"] = profiler_doc["op"]
            pickle.dump(oplog_doc, output)
            inserts += 1
            oplog_doc = unpickle(oplog)
            profiler_doc = unpickle(profiler)

    while profiler_doc:
        pickle.dump(profiler_doc, output)
        noninserts += 1
        profiler_doc = unpickle(profiler)

    logger.info("Finished completing the insert options, %d inserts and"
                " %d noninserts", inserts, noninserts)
    for f in [oplog, profiler, output]:
        f.close()

    return True


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

    def get_oplog_tailor(self, start_time):
        """Start recording the oplog entries starting from now.
        We only care about "insert" operations since all other queries will
        be captured by mongodb profiler.

        REQUIRED: the specific mongodb database has enabled profiling.
        """
        oplog_collection = \
            self.oplog_client[constants.LOCAL_DB][constants.OPLOG_COLLECTION]
        criteria = {"op": "i", "ts": {"$gte": start_time}}

        return create_tailing_cursor(oplog_collection, criteria)

    def get_profiler_tailor(self, start_time):
        """Start recording the profiler entries"""
        profiler_collection = \
            self.profiler_client[self.config["target_database"]] \
                                [constants.PROFILER_COLLECTION]
        # ignore the inserts
        profiler_namespace = "{0}.{1}".format(
            self.config["target_database"],
            constants.PROFILER_COLLECTION)
        index_namespace = "{0}.{1}".format(
            self.config["target_database"],
            constants.INDEX_COLLECTION)

        criteria = {
            "ns": {"$nin": [profiler_namespace, index_namespace]},
            "ts":  {"$gte": start_time}
        }

        return create_tailing_cursor(profiler_collection, criteria)

    def record(self):
        """record the activities in the multithreading way"""
        start_time = now_in_utc_secs()
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
            args=(self.get_oplog_tailor(Timestamp(start_time, 0)),
                  0, doc_queue, state)
        ))
        start_datetime = datetime.utcfromtimestamp(start_time)
        threads.append(Thread(
            target=MongoQueryRecorder._tail_to_queue,
            args=(self.get_profiler_tailor(start_datetime),
                  1, doc_queue, state)
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

        for f in files:
            f.close()

        # Final step
        complete_insert_ops(self.config["oplog_output_file"],
                            self.config["profiler_output_file"],
                            self.config["output_file"])


def main():
    """Recording the inbound traffic for a database."""
    db_config = config.DB_CONFIG
    recorder = MongoQueryRecorder(db_config)
    recorder.record()
    _test_final_output(db_config["output_file"])

if __name__ == '__main__':
    main()
