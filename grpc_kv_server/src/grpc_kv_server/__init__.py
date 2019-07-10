import datetime
import concurrent.futures
import logging
import time

import grpc

import key_value_pb2
import key_value_pb2_grpc

_ONE_DAY = datetime.timedelta(days=1)


class KeyValueStore:
    def __init__(self):
        self._data = {}

    def store(self, key, value):
        self._data[key] = value

    def get(self, key):
        return self._data[key]

    def exists(self, key):
        return key in self._data


class KeyValueStoreServer(key_value_pb2_grpc.KeyValueStoreServicer):
    def __init__(self):
        self._kv_store = KeyValueStore()

    def GetRecord(self, request, context):
        logging.info("Received Get request from {}".format(context.peer()))
        if not self._kv_store.exists(request.name):
            context.abort(
                grpc.StatusCode.NOT_FOUND,
                "Record at key '{}' does not exist.".format(
                    request.name))
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
