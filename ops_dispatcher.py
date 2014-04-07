import threading
import utils
import heapq
import queue


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

    def __init__(self, ops_sources, start_time=None, end_time=None,
                 batch_size=10000):
        """
        @params ops_sources: we can dispatch ops from multiple sources, where
            all ops will be issued by the order of their timestamps. Each source
            should have iterator/generator like interface.
        """
        self.ops = merged_iterator(ops_sources)
        self.queue = None
        self.batch_size = batch_size
        self.processing = False
        self.start_time = start_time
        self.end_time = end_time

    def __iter__(self):
        assert self.queue is not None
        for op in iter(self.queue):
            if op["ts"] > self.end_time:
                break
            yield op

    def start(self):
        assert not self.processing
        self.processing = True
        utils.LOG.info("Starting to grab the ops")

        if self.start_time:
            # Fast forward to the first time that >= `self.start_time`
            for op in self.ops:
                if op["ts"] >= self.start_time:
                    # TODO sacrifice the first valid item.
                    break
        self.queue = queue.PreloadingQueue(self.ops, self.batch_size)
        self.report_status()

    def stop(self):
        self.processing = False
        # TODO Damn, this is not thread safe, but anyway...
        if self.queue is not None:
            self.queue.stop()

    @utils.set_interval(interval=3, start_immediately=True, exec_on_exit=True)
    def report_status(self):
        """Keeps monitor and filling the queue size and if it is less than
        `pre_fetching`."""
        q = self.queue
        loading_stats = q.loading_stats
        reading_stats = q.reading_stats
        qsize = q.active_batch.size
        if q.next_batch:
            qsize += q.next_batch.size

        loading_rate = 0
        if loading_stats.num_items_loaded != 0:
            loading_rate = loading_stats.num_items_loaded / \
                loading_stats.total_loading_time.total_seconds()

        utils.LOG.info("Current queue size %d, ops loaded %d, loading time %s, "
                       "ops read %d, waiting time: %s, rate %d/sec",
                       qsize, loading_stats.num_items_loaded,
                       str(loading_stats.total_loading_time),
                       reading_stats.num_items_read,
                       str(loading_stats.total_waiting_time),
                       loading_rate)

# TODO Move this to unit test


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
            ops = list(op for op in iter(dispatch) if op != None)
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
        sources, start_time=10000, end_time=40000, batch_size=100)

    dispatch.start()
    consumer = threading.Thread(target=check_grabbed_ops)
    consumer.start()
    consumer.join()

if __name__ == '__main__':
    test()
