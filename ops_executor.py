from pymongo.errors import OperationFailure
from threading import Thread
import utils
import time

ALL_OP_TYPES = set([
    "insert",
    "query",
    "update",
    "remove",
    "getmore",
    "command",
])


def exec_command(db, coll_name, op_doc):
    """TODO we only pick up the commands that we're most interested in"""
    cmd = op_doc["command"]
    if "findandmodify" in cmd:
        coll_name = cmd["findandmodify"]
        db[coll_name].find_and_modify(cmd["query"], cmd["update"])
    elif "count" in cmd:
        coll_name = cmd["count"]
        db[coll_name].count()
    else:
        # Intentionally left blank
        pass


def exec_query(db, coll_name, op_doc):
    limit = op_doc["ntoreturn"] if "ntoreturn" in op_doc else 0
    skip = op_doc["ntoskip"] if "ntoskip" in op_doc else 0
    db[coll_name].find(op_doc["query"], limit=limit, skip=skip)


SUPPORTED_OP_TYPES = {
    "insert": lambda db, coll_name, op_doc: db[coll_name].insert(
        op_doc["o"], manipulate=False),
    "query": exec_query,
    "update": lambda db, coll_name, op_doc: db[coll_name].update(
        op_doc["query"], op_doc["updateobj"]),
    "remove": lambda db, coll_name, op_doc: db[coll_name].remove(
        op_doc["query"]),
    "command": exec_command,
}


def make_op_stats():
    stats = utils.EmptyClass()
    stats.op_counts = dict.fromkeys(ALL_OP_TYPES, 0)
    stats.failed_ops = dict.fromkeys(ALL_OP_TYPES, 0)
    stats.slow_ops = dict.fromkeys(ALL_OP_TYPES, 0)
    stats.get_none = 0
    stats.latency_buckets = [0] * 1000

    return stats


def parse_namespace(ns):
    """the "ns" contains database name and collection name, concatenated by "."
    """
    return ns.split('.', 1)


def exec_single_op(mongo_client, op, op_stats):
    op_type = op["op"]
    if op_type not in SUPPORTED_OP_TYPES:
        return

    op_stats.op_counts[op_type] += 1
    try:
        db_name, coll_name = parse_namespace(op["ns"])
    except ValueError:
        utils.LOG.error("Unknown namespace: %s", op["ns"])
        return

    try:
        db = mongo_client[db_name]
        SUPPORTED_OP_TYPES[op_type](db, coll_name, op)
    except OperationFailure:
        op_stats.failed_ops[op_type] += 1


@utils.set_interval(10, start_immediately=False, exec_on_exit=True)
def report_exec_ops_status(stats_list, final_stats, time_info):
    def merge_counts(base_count, addition_count):
        for key in ALL_OP_TYPES:
            base_count[key] += addition_count[key]

    def update_stats(base, addition):
        merge_counts(base.op_counts, addition.op_counts)
        merge_counts(base.failed_ops, addition.failed_ops)
        merge_counts(base.slow_ops, addition.slow_ops)

    def log_subtype_message(name, op_type, op_counts, failed_ops, slow_ops,
                            seconds):
        template = "[%s-%s] total ops: %d, failed ops: %d, slow ops: %d, "
        utils.LOG.info(
            template, name, op_type, op_counts, failed_ops, slow_ops)

    def log_total_message(name, op_type, op_counts, failed_ops, slow_ops,
                          get_none, seconds):
        template = "[%s-%s] total ops: %d, failed ops: %d, slow ops: %d, " \
                   "get-none: %d, ops/sec: %.2f"
        utils.LOG.info(template, name, op_type, op_counts, failed_ops,
                       slow_ops, get_none, float(op_counts) / seconds)

    now = time.time()
    total_time = now - time_info["epoch"]
    interval_time = now - time_info["last_epoch"]
    time_info["last_epoch"] = now

    interval_stats = make_op_stats()
    for idx, stats in enumerate(stats_list):
        stats_list[idx] = make_op_stats()
        update_stats(interval_stats, stats)

    update_stats(final_stats, interval_stats)
    stats_list = (("LAST-N-SECS", interval_stats, interval_time),
                  ("TOTAL", final_stats, total_time))
    for name, stats, timespan in stats_list:
        for op_type in ALL_OP_TYPES:
            log_subtype_message(name, op_type, stats.op_counts[op_type],
                                stats.failed_ops[
                                op_type], stats.slow_ops[op_type],
                                interval_time)
        total_ops = sum(stats.op_counts[op_type] for op_type in ALL_OP_TYPES)
        total_failed_ops = sum(stats.failed_ops[op_type]
                               for op_type in ALL_OP_TYPES)
        total_slow_ops = sum(stats.slow_ops[op_type]
                             for op_type in ALL_OP_TYPES)
        log_total_message(
            name, "total", total_ops, total_failed_ops, total_slow_ops,
            stats.get_none, timespan)

    return final_stats


def parallel_exec_ops(mongo_client, ops_list):
    """
    REQUIRED: ops is the iterator or generator that can be safely accessed by
              multiple threads.
    """
    def exec_ops(mongo_client, idx):
        for op in ops_list[idx]:
            if op is None:
                stats_list[idx].get_none += 1
                continue
            exec_single_op(mongo_client, op, stats_list[idx])

    threads = []
    stats_list = []
    final_stats = make_op_stats()
    now = time.time()
    stop_status_report = report_exec_ops_status(
        stats_list, final_stats, {"epoch": now, "last_epoch": now}
    )
    for idx in xrange(len(ops_list)):
        stats_list.append(make_op_stats())
        threads.append(Thread(target=exec_ops, args=(mongo_client, idx)))
        threads[-1].setDaemon(True)
        threads[-1].start()

    for t in threads:
        t.join()
    stop_status_report.set()
