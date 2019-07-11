import collections
import concurrent.futures
import datetime
import itertools
import logging
import threading
import time

import grpc

import key_value_pb2
import key_value_pb2_grpc

_ONE_DAY = datetime.timedelta(days=1)

_REACTION_TIME = datetime.timedelta(milliseconds=200)
_RECORD_QUEUE_SIZE = 128


class Record:
    def __init__(self, value):
        self._values = collections.deque((value, ), _RECORD_QUEUE_SIZE)
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._epoch = 0

    def update(self, value):
        with self._lock:
            self._values.append(value)
            self._epoch += 1
            self._condition.notify_all()

    def await_update(self, stop_event):
        initial_epoch = self._epoch
        while not stop_event.is_set():
            with self._lock:
                self._condition.wait(timeout=_REACTION_TIME.total_seconds())
                if self._epoch == initial_epoch:
                    continue
                epoch_difference = min(self._epoch - initial_epoch,
                                       _RECORD_QUEUE_SIZE)
                return itertools.islice(self._values,
                                        len(self._values) - epoch_difference,
                                        len(self._values))
        return iter(())

    def value(self):
        return self._values[-1]


class KeyValueStore:
    def __init__(self):
        self._records = {}

    def store(self, key, value):
        if key not in self._records:
            self._records[key] = Record(value)
        else:
            self._records[key].update(value)

    def get(self, key):
        return self._records[key].value()

    def exists(self, key):
        return key in self._records

    def watch(self, key, stop_event):
        while not stop_event.is_set():
            for value in self._records[key].await_update(stop_event):
                yield value


class KeyValueStoreServer(key_value_pb2_grpc.KeyValueStoreServicer):
    def __init__(self):
        self._kv_store = KeyValueStore()

    def GetRecord(self, request, context):
        logging.info("Received Get request from {}".format(context.peer()))
        if not self._kv_store.exists(request.name):
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                "Record at key '{}' does not exist.".format(request.name))
        value = self._kv_store.get(request.name)
        return key_value_pb2.Record(name=request.name, value=value)

    def CreateRecord(self, request, context):
        logging.info("Received Store request from {}".format(context.peer()))
        if not request.HasField("record"):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          "Request must have 'record' field.")
        if self._kv_store.exists(request.record.name):
            context.abort(
                grpc.StatusCode.ALREADY_EXISTS,
                "Record at key '{}' already exists.".format(
                    request.record.name))
        self._kv_store.store(request.record.name, request.record.value)
        return request.record

    def UpdateRecord(self, request, context):
        logging.info("Received Update request from {}".format(context.peer()))
        if not request.HasField("record"):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          "Request must have 'record' field.")
        if not self._kv_store.exists(request.record.name):
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                "Record at key '{}' does not exist.".format(
                    request.record.name))
        self._kv_store.store(request.record.name, request.record.value)
        return request.record

    def WatchRecord(self, request, context):
        logging.info("Establishing Watch request with {}".format(
            context.peer()))
        if not self._kv_store.exists(request.name):
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                "Record at key '{}' does not exist.".format(request.name))
        stop_event = threading.Event()

        def on_rpc_done():
            stop_event.set()

        context.add_callback(on_rpc_done)
        for value in self._kv_store.watch(request.name, stop_event):
            yield key_value_pb2.Record(name=request.name, value=value)
        logging.info("Terminated Watch request with {}".format(context.peer()))


def _await_termination(server):
    try:
        while True:
            time.sleep(_ONE_DAY.total_seconds())
    except KeyboardInterrupt:
        logging.info("Terminating server.")
        server.stop(0)


def _run_server_non_blocking(host, port):
    server = grpc.server(concurrent.futures.ThreadPoolExecutor())
    key_value_pb2_grpc.add_KeyValueStoreServicer_to_server(
        KeyValueStoreServer(), server)
    actual_port = server.add_insecure_port('{}:{}'.format(host, port))
    logging.info("Server listening at {}:{}".format(host, actual_port))
    server.start()
    return server, actual_port


def run_server(host='localhost', port=50051):
    server, _ = _run_server_non_blocking(host, port)
    _await_termination(server)


__all__ = [run_server, KeyValueStore]
