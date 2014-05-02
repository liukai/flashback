from Queue import Full
import heapq
import utils
import multiprocessing


def merged_iterator(ops_sources, start_time, end_time):
    """Merge the ops sources and presents a view of a "single iterator" that
    returns ops ordered by "ts"
    REQUIRES: this method is not thread safe.
    """
    comparable_ops_sources = []
    for source in ops_sources:
        comparable_ops_sources.append((op["ts"], op) for op in source)

    ops = heapq.merge(*comparable_ops_sources)

    # Fast forward to the first time that >= `self.start_time`
    first_op = None
    if start_time:
        utils.LOG.info("Fast forward and skips ops with ts smaller than %s",
                       str(start_time))
        skip_num = 0
        for ts, op in ops:
            if ts >= start_time:
                first_op = op
                break
        utils.LOG.info("Fast forward completed, %d items skipped.", skip_num)
    if first_op:
        yield first_op

    # yield the rest of the ops
    for ts, op in heapq.merge(*comparable_ops_sources):
        if end_time and ts > end_time:
            break
        yield op


def load_ops(ops_sources, queue, items_loaded, load_wait_secs, should_stop,
             all_ops_loaded, start_time=None, end_time=None):
    """Reads ops from sources and put them in to the queue
    @params ops_sources: we can dispatch ops from multiple sources, where
            all ops will be issued by the order of their timestamps. Each source
            should have iterator/generator like interface.
    """
    ops = merged_iterator(ops_sources, start_time, end_time)
    timeout_sec = 1

    utils.LOG.info("Started loading ops...")

    try:
        for op in ops:
            if should_stop.value:
                return

            while not should_stop.value:
                try:
                    queue.put(op, timeout_sec)
                    items_loaded.value += 1
                    break
                except Full:
                    load_wait_secs.value += timeout_sec

            if should_stop.value:
                return
    finally:
        all_ops_loaded.value = True


def async_load_ops(ops_sources, queue, should_stop, start_time=None,
                   end_time=None):
    items_loaded = multiprocessing.Value('i', 0)
    load_wait_secs = multiprocessing.Value('i', 0)
    all_ops_loaded = multiprocessing.Value('i', 0)

    proc = multiprocessing.Process(
        target=load_ops,
        args=(ops_sources, queue, items_loaded, load_wait_secs, should_stop,
              all_ops_loaded, start_time, end_time))

    proc.daemon = True
    proc.start()

    return proc, items_loaded, load_wait_secs, all_ops_loaded
