import unittest
import contextlib

import grpc_kv_server

import grpc
import key_value_pb2
import key_value_pb2_grpc


@contextlib.contextmanager
def _test_server():
    server, port = grpc_kv_server._run_server_non_blocking('localhost', 0)
    try:
        yield server, port
    finally:
        server.stop(0)


class TestGrpcKvServer(unittest.TestCase):
    def test_without_grpc(self):
        kv_store = grpc_kv_server.KeyValueStore()
        kv_store.store("golden-retriever", "pancakes")
        self.assertEqual("pancakes", kv_store.get("golden-retriever"))

    def test_with_grpc_in_a_single_process(self):
        with _test_server() as (server, port):
            with grpc.insecure_channel('localhost:{}'.format(port)) as channel:
                stub = key_value_pb2_grpc.KeyValueStoreStub(channel)
                create_request = key_value_pb2.CreateRecordRequest(
                    record=key_value_pb2.Record(
                        name="golden-retriever",
                        value="pancakes",
                    ))
                create_response = stub.CreateRecord(
                    create_request, wait_for_ready=True)
                self.assertEqual(create_request.record, create_response)

                get_request = key_value_pb2.GetRecordRequest(
                    name="golden-retriever")
                get_response = stub.GetRecord(get_request)
                self.assertEqual(get_response, create_request.record)


if __name__ == "__main__":
    unittest.main()
