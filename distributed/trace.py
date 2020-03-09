import atexit
import contextlib
import json
import os
import time

TRACE_FILE_PATH = os.environ.get("DASK_TRACE_FILE")
TRACE_FILE = None


def trace_close():
    global TRACE_FILE

    if TRACE_FILE is not None:
        TRACE_FILE.flush()
        TRACE_FILE.close()
        TRACE_FILE = None


def trace(data):
    if TRACE_FILE is None:
        return

    # time in microseconds
    duration = int(time.time() * 1000000)
    serialized = json.dumps({
        "timestamp": duration,
        "fields": data
    })
    TRACE_FILE.write(f"{serialized}\n")


@contextlib.contextmanager
def trace_time(process, method):
    trace_method(process, method, True)
    yield
    trace_method(process, method, False)


def trace_method(process, method, start):
    trace({
        "action": "measure",
        "process": process,
        "method": method,
        "event": "start" if start else "end"
    })


def trace_task_new(task_id, task_key, inputs):
    trace({
        "action": "task",
        "event": "create",
        "task": task_id,
        "key": task_key,
        "inputs": ",".join(str(i) for i in inputs)
    })


def trace_task_new_finished(task_id, task_key, worker_id, size):
    trace({
        "action": "task",
        "event": "create",
        "task": task_id,
        "key": task_key,
        "worker": worker_id,
        "size": size
    })


def trace_task_finish(task_id, worker_id, size, duration):
    def normalize_time(t):
        if t is None:
            return 0
        return t * 1000000

    trace({
        "action": "task",
        "event": "finish",
        "task": task_id,
        "worker": worker_id,
        "start": normalize_time(duration[0]),
        "stop": normalize_time(duration[1]),
        "size": size
    })


def trace_task_place(task_id, worker_id):
    trace({
        "action": "task",
        "event": "place",
        "task": task_id,
        "worker": worker_id
    })


def trace_task_assign(task_id, worker_id):
    trace({
        "action": "task",
        "event": "assign",
        "task": task_id,
        "worker": worker_id
    })


def trace_worker_new(worker_id, ncpus, address):
    trace({
        "action": "new-worker",
        "worker_id": worker_id,
        "cpus": ncpus,
        "address": address
    })


def trace_worker_steal(task_id, worker_victim, worker_to):
    trace({
        "action": "steal",
        "task": task_id,
        "from": worker_victim,
        "to": worker_to
    })


def trace_worker_steal_response(task_id, worker_from, worker_to, result):
    trace({
        "action": "steal-response",
        "task": task_id,
        "from": worker_from,
        "to": 0,
        "result": result
    })


def trace_worker_steal_response_missing(task_key, worker_from):
    trace({
        "action": "steal-response",
        "task": task_key,
        "from": worker_from,
        "to": 0,
        "result": "missing"
    })


def trace_packet_send(size):
    trace({
        "action": "packet-send",
        "size": size
    })


def trace_packet_receive(size):
    trace({
        "action": "packet-receive",
        "size": size
    })


if TRACE_FILE_PATH is not None:
    TRACE_FILE = open(TRACE_FILE_PATH, "w")
    atexit.register(trace_close)
