#!/usr/bin/python
r""" Track the MongoDB activities by tailing oplog and profiler output"""

from bson.timestamp import Timestamp
from datetime import datetime
from pymongo import MongoClient
from threading import Thread
import config
import constants
import pickle
import Queue
import time
import utils


def _test_final_output(filename):
    """Just for test: help the manual check of the final output"""
    for i, doc in enumerate(utils.unpickle_iterator(filename)):
        print i, ":", doc


def make_ns_selector(database, target_collections):
    system_collections = \
        set([constants.PROFILER_COLLECTION, constants.INDEX_COLLECTION])

    if target_collections is not None:
        target_collections -= system_collections

    if target_collections is not None and len(target_collections) > 0:
        return {"$in": ["{0}.{1}".format(database, coll)
                for coll in target_collections]}
    else:
        return {"$nin": ["{0}.{1}".format(database, coll)
                for coll in system_collections]}


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
    oplog_doc = utils.unpickle(oplog)
    profiler_doc = utils.unpickle(profiler)
    inserts = 0
    noninserts = 0

    while oplog_doc and profiler_doc:
        if profiler_doc["op"] != "insert":
            pickle.dump(profiler_doc, output)
            noninserts += 1
            profiler_doc = utils.unpickle(profiler)
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
            oplog_doc = utils.unpickle(oplog)
            profiler_doc = utils.unpickle(profiler)

    while profiler_doc:
        pickle.dump(profiler_doc, output)
        noninserts += 1
        profiler_doc = utils.unpickle(profiler)

    logger.info("Finished completing the insert options, %d inserts and"
                " %d noninserts", inserts, noninserts)
    for f in [oplog, profiler, output]:
        f.close()

    return True


class MongoQueryRecorder(object):

    """Record MongoDB database's activities by polling the oplog and profiler
    results"""

    OPLOG = 0
    PROFILER = 1

    class RecordingState(object):

        """Keeps the running status of a recording request"""

        @staticmethod
        def make_tailor_state():
            """Return the tailor state "struct" """
            s = utils.EmptyClass()
            s.entries_received = 0
            s. entries_written = 0
            s.alive = True
            s.last_ts = None
            return s

        def __init__(self):
            self.timeout = False

            self.tailor_states = [
                self.make_tailor_state(),
                self.make_tailor_state(),
            ]

    def __init__(self, db_config):
        self.config = db_config
        # sanitize the options
        if self.config["target_collections"] is not None:
            self.config["target_collections"] = set(
                [coll.strip() for coll in self.config["target_collections"]])

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
        while not state.timeout:
            try:
                index, doc = doc_queue.get(block=True, timeout=1)
                state.tailor_states[index].entries_written += 1
                pickle.dump(doc, files[index])
            except Queue.Empty:
                # gets nothing after timeout
                continue
        for f in files:
            f.flush()
        utils.LOG.info("All received docs are processed!")

    @staticmethod
    def _tail_to_queue(tailor, identifier, doc_queue, state, end_time,
                       check_duration_secs=1):
        """Accepts a tailing cursor and serialize the retrieved documents to a
        fifo queue
        @param identifier: when passing the retrieved document to the queue, we
            will attach a unique identifier that allows the queue consumers to
            process different sources of documents accordingly.
        @param check_duration_secs: if we cannot retrieve the latest document,
            it will sleep for a period of time and then try again.
        """
        tailor_state = state.tailor_states[identifier]
        while tailor.alive:
            try:
                doc = tailor.next()
                tailor_state.last_ts = doc["ts"]
                if state.timeout and (tailor_state.last_ts is None or
                                      tailor_state.last_ts >= end_time):
                    break

                doc_queue.put_nowait((identifier, doc))
                tailor_state.entries_received += 1
            except StopIteration:
                if state.timeout:
                    break
                time.sleep(check_duration_secs)
        utils.LOG.info("source #%d: Tailing to queue completed!", identifier)

    @staticmethod
    def _report_status(state):
        """report current processing status"""
        msgs = []
        for idx, source in enumerate(["<oplog>", "<profiler>"]):
            tailor_state = state.tailor_states[idx]
            msg = "\n\t{0}: received {1} entries, {2} of them were written, "\
                  "last received entry ts: {3}" .format(
                      source,
                      tailor_state.entries_received,
                      tailor_state.entries_written,
                      str(tailor_state.last_ts))
            msgs.append(msg)

        utils.LOG.info("".join(msgs))

    def get_oplog_tailor(self, start_time):
        """Start recording the oplog entries starting from now.
        We only care about "insert" operations since all other queries will
        be captured by mongodb profiler.

        REQUIRED: the specific mongodb database has enabled profiling.
        """
        oplog_collection = \
            self.oplog_client[constants.LOCAL_DB][constants.OPLOG_COLLECTION]
        criteria = {
            "op": "i",
            "ts": {"$gte": start_time},
            "ns": make_ns_selector(self.config["target_database"],
                                   self.config["target_collections"])
        }

        return utils.create_tailing_cursor(oplog_collection, criteria)

    def get_profiler_tailor(self, start_time):
        """Start recording the profiler entries"""
        profiler_collection = \
            self.profiler_client[self.config["target_database"]] \
                                [constants.PROFILER_COLLECTION]
        criteria = {
            "ns": make_ns_selector(self.config["target_database"],
                                   self.config["target_collections"]),
            "ts": {"$gte": start_time}
        }

        return utils.create_tailing_cursor(profiler_collection, criteria)

    def record(self):
        """record the activities in the multithreading way"""
        start_time = utils.now_in_utc_secs()
        end_time = utils.now_in_utc_secs() + self.config["duration_secs"]

        doc_queue = Queue.Queue()
        state = MongoQueryRecorder. RecordingState()
        # We'll dump the recorded activities to `files`.
        files = [
            open(self.config["oplog_output_file"], "wb"),
            open(self.config["profiler_output_file"], "wb")
        ]

        # Create background threads to handle to track/dump mongodb activities
        thread_info_list = []
        # Writer thread, we only have one writer since we assume all files will
        # be written to the same device (disk or SSD), as a result it yields
        # not much benefit to have multiple writers.
        thread_info_list.append({
            "name": "write-all-docs-to-file",
            "thread": Thread(
                target=MongoQueryRecorder._process_doc_queue,
                args=(doc_queue, files, state))
        })
        thread_info_list.append({
            "name": "tailing-oplogs",
            "thread": Thread(
            target=MongoQueryRecorder._tail_to_queue,
            args=(self.get_oplog_tailor(Timestamp(start_time, 0)),
                  MongoQueryRecorder.OPLOG, doc_queue, state,
                  Timestamp(end_time, 0)))
        })

        start_datetime = datetime.utcfromtimestamp(start_time)
        end_datetime = datetime.utcfromtimestamp(end_time)
        thread_info_list.append({
            "name": "tailing-profiler",
            "thread": Thread(
            target=MongoQueryRecorder._tail_to_queue,
            args=(self.get_profiler_tailor(start_datetime),
                  MongoQueryRecorder.PROFILER, doc_queue, state, end_datetime))
        })

        for thread_info in thread_info_list:
            utils.LOG.info("Starting thread: %s", thread_info["name"])
            thread_info["thread"].setDaemon(True)
            thread_info["thread"].start()

        # Processing for a time range
        while all(s.alive for s in state.tailor_states) \
                and (utils.now_in_utc_secs() < end_time):
            MongoQueryRecorder._report_status(state)
            time.sleep(5)
        MongoQueryRecorder._report_status(state)

        state.timeout = True
        for thread_info in thread_info_list:
            utils.LOG.info(
                "Time to stop, waiting for thread: %s to finish",
                thread_info["name"])
            thread = thread_info["thread"]
            name = thread_info["name"]
            # Idempotently wait for thread
            wait_secs = 5
            while thread.is_alive():
                thread.join(wait_secs)
                if thread.is_alive():
                    utils.LOG.error(
                        "Thread %s didn't exit after %d seconds. Will wait for "
                        "another %d seconds", name, wait_secs, 2 * wait_secs)
                    wait_secs *= 2
                    thread.join(wait_secs)
                else:
                    utils.LOG.info("Thread %s exits normally.", name)
        utils.LOG.info("Preliminary recording completed!")

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
    # _test_final_output(db_config["output_file"])

if __name__ == '__main__':
    main()
