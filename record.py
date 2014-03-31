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
import signal
import merge


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
        self.force_quit = False
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
        # Keep waiting if any of the tailor thread is still at work.
        while any(s.alive for s in state.tailor_states):
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
        tailor_state.alive = False
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

    def force_quit_all(self):
        """Gracefully quite all recording activities"""
        self.force_quit = True

    def _generate_workers(self, files, state, start_utc_secs, end_utc_secs):
        """Generate the threads that tails the data sources and put the fetched
        entries to the files"""
        # Create working threads to handle to track/dump mongodb activities
        workers_info = []
        doc_queue = Queue.Queue()

        # Writer thread, we only have one writer since we assume all files will
        # be written to the same device (disk or SSD), as a result it yields
        # not much benefit to have multiple writers.
        workers_info.append({
            "name": "write-all-docs-to-file",
            "thread": Thread(
                target=MongoQueryRecorder._process_doc_queue,
                args=(doc_queue, files, state))
        })
        tailor = self.get_oplog_tailor(Timestamp(start_utc_secs, 0))
        oplog_cursor_id = tailor.cursor_id
        workers_info.append({
            "name": "tailing-oplogs",
            "on_close":
            lambda: self.oplog_client.kill_cursors([oplog_cursor_id]),
            "thread": Thread(
                target=MongoQueryRecorder._tail_to_queue,
                args=(tailor, MongoQueryRecorder.OPLOG, doc_queue, state,
                      Timestamp(end_utc_secs, 0)))
        })

        start_datetime = datetime.utcfromtimestamp(start_utc_secs)
        end_datetime = datetime.utcfromtimestamp(end_utc_secs)
        tailor = self.get_profiler_tailor(start_datetime)
        profiler_cursor_id = tailor.cursor_id
        workers_info.append({
            "name": "tailing-profiler",
            "on_close":
            lambda: self.profiler_client.kill_cursors([profiler_cursor_id]),
            "thread": Thread(
                target=MongoQueryRecorder._tail_to_queue,
                args=(tailor, MongoQueryRecorder.PROFILER, doc_queue, state,
                      end_datetime))
        })

        for worker_info in workers_info:
            utils.LOG.info("Starting thread: %s", worker_info["name"])
            worker_info["thread"].setDaemon(True)
            worker_info["thread"].start()

        return workers_info

    def _join_workers(self, workers_info):
        """Ready to exit all workers"""
        for worker_info in workers_info:
            utils.LOG.info(
                "Time to stop, waiting for thread: %s to finish",
                worker_info["name"])
            thread = worker_info["thread"]
            name = worker_info["name"]
            # Idempotently wait for thread to exit
            wait_secs = 5
            while thread.is_alive():
                thread.join(wait_secs)
                if thread.is_alive():
                    if self.force_quit and worker_info["on_close"]:
                        worker_info["on_close"]()
                    utils.LOG.error(
                        "Thread %s didn't exit after %d seconds. Will wait for "
                        "another %d seconds", name, wait_secs, 2 * wait_secs)
                    wait_secs *= 2
                    thread.join(wait_secs)
                else:
                    utils.LOG.info("Thread %s exits normally.", name)

    @utils.set_interval(3)
    def _periodically_report_status(self, state):
        return MongoQueryRecorder._report_status(state)

    def record(self):
        """record the activities in the multithreading way"""
        start_utc_secs = utils.now_in_utc_secs()
        end_utc_secs = utils.now_in_utc_secs() + self.config["duration_secs"]
        state = MongoQueryRecorder. RecordingState()
        # We'll dump the recorded activities to `files`.
        files = [
            open(self.config["oplog_output_file"], "wb"),
            open(self.config["profiler_output_file"], "wb")
        ]

        # Create a series working threads to handle to track/dump mongodb
        # activities. On return, these threads have already started.
        workers_info = self._generate_workers(files, state, start_utc_secs,
                                              end_utc_secs)
        timer_control = self._periodically_report_status(state)

        # Waiting till due time arrives
        while all(s.alive for s in state.tailor_states) \
                and (utils.now_in_utc_secs() < end_utc_secs) \
                and not self.force_quit:
            time.sleep(1)

        state.timeout = True

        self._join_workers(workers_info)
        timer_control.set()  # stop status report
        utils.LOG.info("Preliminary recording completed!")

        for f in files:
            f.close()

        # Fill the missing insert op details from oplog
        merge.merge_to_final_output(self.config["oplog_output_file"],
                                    self.config["profiler_output_file"],
                                    self.config["output_file"])


def main():
    """Recording the inbound traffic for a database."""
    db_config = config.DB_CONFIG
    recorder = MongoQueryRecorder(db_config)

    def signal_handler(sig, dummy):
        """Handle the Ctrl+C signal"""
        print 'Trying to gracefully exiting program...'
        recorder.force_quit_all()
    signal.signal(signal.SIGINT, signal_handler)

    recorder.record()

if __name__ == '__main__':
    main()
