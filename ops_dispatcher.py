import time
import datetime
import Queue
import threading
import utils
import heapq


def merged_iterator(ops_sources):
    """Merge the ops sources and presents a view of a "single iterator" that
    returns ops ordered by "ts"
    REQUIRES: this method is not thread safe.
    """
    comparable_ops_sources = []
    for source in ops_sources:
        comparable_ops_sources.append((op["ts"], op) for op in source)
    for dummy_ts, op in heapq.merge(*comparable_ops_sources):
        yield op


class OpsDispatcher(object):

    @staticmethod
    def make_loading_stats():
        stats = utils.EmptyClass()
        stats.num_loading_stalls = 0
        stats.num_loaded_ops = 0
        stats.loading_ops_time = datetime.timedelta()
        return stats

    @staticmethod
    def make_get_ops_stats():
        stats = utils.EmptyClass()
        stats.loading_ops_time = datetime.timedelta()
        stats.num_nones = 0
        stats.num_ops = 0
        stats.get_ops_time = datetime.timedelta()
        return stats

    def __init__(self, ops_sources, start_time=None, end_time=None,
                 pre_fetching=5000, min_batch_size=1000):
        """
        @params ops_sources: we can dispatch ops from multiple sources, where
            all ops will be issued by the order of their timestamps. Each source
            should have iterator/generator like interface.
        """
        self.ops = merged_iterator(ops_sources)
        self.queue = Queue.Queue()
        self.processing = False
        self.pre_fetching = pre_fetching
        self.loading_thread = None
        self.all_loaded = True
        self.min_batch_size = min_batch_size
        self.loading_stats = OpsDispatcher.make_loading_stats()
        self.start_time = start_time
        self.end_time = end_time

    def get_ops(self, timeout=1):
        """returns a generator provides a thread-safe way to fetch the sequences
        of unprocessed ops.
        @params: timeout decides how long we should wait if we cannot fetch any
                 item by then.
        REQUIRES: start() had been called.
        """
        while not self.all_loaded:
            try:
                op = self.queue.get(block=True, timeout=timeout)
                yield op
            except Queue.Empty:
                yield None

    def start(self):
        assert not self.processing
        self.processing = True
        self.all_loaded = False
        utils.LOG.info("Starting to grab the ops")

        if self.start_time:
            # Fast forward to the first time that >= `self.start_time`
            for op in self.ops:
                if op["ts"] >= self.start_time:
                    self.queue.put(op)
                    break

        # Pre-fetching ops to avoid unnecessary initial waiting.
        self._load_batch(self.pre_fetching)
        utils.LOG.info("Pre-fetched %d ops", self.pre_fetching)

        self.loading_thread = threading.Thread(target=self._fill_queue)
        self.loading_thread.setDaemon(True)
        self.loading_thread.start()

    def stop(self):
        self.processing = False
        self.join()

    def join(self):
        if self.loading_thread:
            self.loading_thread.join()

    def _load_batch(self, batch_size):
        loaded = 0
        for op in self.ops:
            if self.end_time and op["ts"] >= self.end_time:
                break
            loaded += 1
            self.queue.put(op)
            if loaded == self.pre_fetching:
                break
        return loaded

    def _fill_queue(self):
        """Keeps monitor and filling the queue size and if it is less than
        `pre_fetching`."""
        try:
            while True:
                # If we've already loaded enough ops, just keep waiting till
                # the number falls under threshold.
                continuous_sleeps = 0
                while self.processing and \
                        self.queue.qsize() > self.pre_fetching:
                    time.sleep(0.1)
                    if continuous_sleeps % 20 == 0:
                        # To avoid flooding warnings, we only print out the
                        # message every N seconds.
                        utils.LOG.warn(
                            "Stop fetching more ops to the queue for 0.1 sec "
                            "because there are already %d item in the queue",
                            self.pre_fetching)
                    continuous_sleeps += 1
                    self.loading_stats.num_loading_stalls += 1
                if not self.processing:
                    break
                start_time = datetime.datetime.now()
                ops_loaded = self._load_batch(max(
                    self.min_batch_size,
                    self.pre_fetching - self.queue.qsize()
                ))
                self.loading_stats.loading_ops_time += \
                    (datetime.datetime.now() - start_time)
                self.loading_stats.num_loaded_ops += ops_loaded
                if ops_loaded == 0:
                    break
        finally:
            # To make sure the correctness of this class stands true even after
            # crash.
            self.all_loaded = True


def test():
    """Test correctness of the functions/classes in this module"""
    import pprint

    def make_random_sources():
        import random
        sources = []
        for dummy_idx_1 in xrange(5):
            s = [{"ts": random.randint(1, 100000)}
                 for dummy_idx in xrange(5000)]
            s.sort()
            sources.append(s.__iter__())
        return sources.__iter__()

    def check_grabbed_ops():
        def grab_ops(idx):
            ops = list(op for op in dispatch.get_ops() if op != None)
            utils.LOG.info("Thread #%d got %d ops.", idx, len(ops))
            # all fetched results are sorted
            assert all(
                ops[i]["ts"] <= ops[i + 1]["ts"] and
                    ops[i]["ts"] >= dispatch.start_time and
                    ops[i]["ts"] <= dispatch.end_time
                for i in xrange(len(ops) - 1)
            )
        # kick off multithreads to fetch the ops
        threads = [
            threading.Thread(target=grab_ops, args=(i,)) for i in xrange(5)
        ]
        for t in threads:
            t.start()

        for t in threads:
            t.join()

    sources = make_random_sources()
    dispatch = OpsDispatcher(
        sources, pre_fetching=1000, min_batch_size=100, start_time=10000,
        end_time=40000)

    dispatch.start()
    consumer = threading.Thread(target=check_grabbed_ops)
    consumer.start()
    dispatch.join()
    consumer.join()
    pprint.pprint(dispatch.loading_stats.__dict__)

if __name__ == '__main__':
    test()
