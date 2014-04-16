from pymongo.errors import OperationFailure
from Queue import Empty
import multiprocessing
import utils

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


def parse_namespace(ns):
    """the "ns" contains database name and collection name, concatenated by "."
    """
    return ns.split('.', 1)


def exec_single_op(mongo_client, op):
    op_type = op["op"]
    if op_type not in SUPPORTED_OP_TYPES:
        return True

    try:
        db_name, coll_name = parse_namespace(op["ns"])
    except ValueError:
        utils.LOG.error("Unknown namespace: %s", op["ns"])
        return False

    try:
        db = mongo_client[db_name]
        SUPPORTED_OP_TYPES[op_type](db, coll_name, op)
    except OperationFailure:
        return False
    return True


def parallel_exec_ops(mongo_client, queue, worker_num, should_stop,
                      all_ops_loaded):
    def exec_ops(mongo_client, queue, total_ops, failed_ops, should_stop,
                 all_ops_loaded):
        while not should_stop.value and \
            (not all_ops_loaded.value or queue.qsize() > 0):
            try:
                op = queue.get(timeout=1)
                total_ops.value += 1
                if not exec_single_op(mongo_client, op):
                    failed_ops.value += 1
            except Empty:
                pass

    worker_total_ops = []
    worker_failed_ops = []
    workers = []

    for dummy_idx in xrange(worker_num):
        total_ops = multiprocessing.Value('i', 0)
        failed_ops = multiprocessing.Value('i', 0)
        worker = multiprocessing.Process(
            target=exec_ops,
            args=(mongo_client, queue, total_ops, failed_ops, should_stop,
                  all_ops_loaded))
        worker.daemon = True
        worker.start()

        workers.append(worker)
        worker_total_ops.append(total_ops)
        worker_failed_ops.append(failed_ops)
    return workers, worker_total_ops, worker_failed_ops
