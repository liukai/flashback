from ops_executor import parallel_exec_ops
from ops_loader import async_load_ops
from pymongo import MongoClient
import datetime
import multiprocessing
import sys
import utils
import threading
import signal
import time


def parse_time(human_readable_time):
    time_format = "%y/%m/%d-%H:%M:%S"
    return datetime.datetime.strptime(human_readable_time, time_format)


@utils.set_interval(5, exec_on_exit=True)
def report_status(queue, items_loaded, load_wait_secs, worker_total_ops,
                  worker_failed_ops, epoches, last_total):
    total_read = sum(num.value for num in worker_total_ops)
    utils.LOG.info("queue size: %d, "
                   "items loaded: %d, load wait secs: %d, items read: %d, "
                   "failed items: %d",
                   queue.qsize(),
                   items_loaded.value, load_wait_secs.value,
                   sum(num.value for num in worker_total_ops),
                   sum(num.value for num in worker_failed_ops))
    now_secs = utils.now_in_utc_secs()
    interval, last_interval = [now_secs - epoch for epoch in epoches]
    epoches[1] = now_secs
    last_read = total_read - last_total[0]
    last_load = items_loaded.value - last_total[1]
    last_total[0] = total_read
    last_total[1] = items_loaded.value

    utils.LOG.info("total read ops/sec: %.2f, last interval read ops/sec: %.2f "
                   "total load ops/sec %.2f, last interval load ops/sec: %.2f ",
                   0 if interval == 0 else total_read / interval,
                   0 if last_interval == 0 else last_read / last_interval,
                   0 if interval == 0 else items_loaded.value / interval,
                   0 if last_interval == 0 else last_load / last_interval)
    return threading.Event()


def main():
    host = sys.argv[1]
    port = int(sys.argv[2])
    start_time = parse_time(sys.argv[3])
    end_time = parse_time(sys.argv[4])
    worker_num = int(sys.argv[5])
    batch_size = int(sys.argv[6])
    ops_sources = [utils.unpickle_iterator(fname)
                   for fname in sys.argv[7:]]

    def signal_handler(sig, dummy):
        """Handle the Ctrl+C signal"""
        utils.LOG.info("Exiting program...")
        should_stop.value = True
    signal.signal(signal.SIGINT, signal_handler)

    queue = multiprocessing.Queue(batch_size)
    should_stop = multiprocessing.Value('i', 0)
    load_proc, items_loaded, load_wait_secs, all_ops_loaded =\
        async_load_ops(ops_sources, queue, should_stop, start_time, end_time)
    while queue.qsize() < batch_size and not all_ops_loaded.value:
        time.sleep(0.5)

    mongo_client = MongoClient(host, port)
    workers, worker_total_ops, worker_failed_ops = parallel_exec_ops(
        mongo_client, queue, worker_num, should_stop, all_ops_loaded)

    now_secs = utils.now_in_utc_secs()
    epoches = [now_secs, now_secs]
    last_total = multiprocessing.Array('i', [0, 0])
    stop_reporting = report_status(queue, items_loaded, load_wait_secs,
                                   worker_total_ops, worker_failed_ops, epoches,
                                   last_total)
    load_proc.join()
    for worker in workers:
        worker.join()
    stop_reporting.set()

if __name__ == '__main__':
    main()
