import datetime
import concurrent.futures
import logging
import time

import grpc

import key_value_pb2
import key_value_pb2_grpc

TEST_VALUE = 2

_ONE_DAY = datetime.timedelta(days=1)

class KeyValueStore(key_value_pb2_grpc.KeyValueStoreServicer):
    pass


def _await_termination(server):
    try:
        while True:
            time.sleep(_ONE_DAY.total_seconds())
    except KeyboardInterrupt:
        logging.info("Terminating server.")
        server.stop(0)


def run_server(host='localhost', port=50051):
    server = grpc.server(concurrent.futures.ThreadPoolExecutor())
    key_value_pb2_grpc.add_KeyValueStoreServicer_to_server(KeyValueStore(), server)
    actual_port = server.add_insecure_port('{}:{}'.format(host, port))
    logging.info("Server listening at {}:{}".format(host, actual_port))
    _await_termination(server)

__all__ = [run_server]