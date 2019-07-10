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


class KeyValueStoreServer(key_value_pb2_grpc.KeyValueStoreServicer):
    def __init__(self):
        self._kv_store = KeyValueStore()

    def GetValue(self, request, context):
        value = self._kv_store.get(request.key)
        key_value_pair = key_value_pb2.KeyValuePair(key=request.key,
                                                    value=value)
        return key_value_pb2.GetResponse(key_value_pair=key_value_pair)

    def StoreValue(self, request, context):
        if not request.HasField("key_value_pair"):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                          "Request must have 'key_value_pair' field.")
        self._kv_store.store(request.key_value_pair.key,
                             request.key_value_pair.value)
        return key_value_pb2.StoreResponse(
            key_value_pair=request.key_value_pair)


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
    _run_server_non_blocking(host, port)
    _await_termination(server)


__all__ = [run_server, KeyValueStore]
