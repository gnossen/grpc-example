import concurrent.futures
import contextlib
import unittest
import threading
import queue

import grpc_kv_server

import grpc
import key_value_pb2
import key_value_pb2_grpc

_TEST_VALUES = ("air-bud", "doug-the-dog", "the-one-from-full-house")


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

    def test_watch_without_grpc(self):
        kv_store = grpc_kv_server.KeyValueStore()
        kv_store.store("golden-retriever", "pancakes")
        start_event = threading.Event()
        stop_event = threading.Event()

        def _gather_responses():
            start_event.set()
            return list(kv_store.watch("golden-retriever", stop_event))

        thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        responses_future = thread_pool.submit(_gather_responses)
        start_event.wait()
        for value in _TEST_VALUES:
            kv_store.store("golden-retriever", value)
        stop_event.set()
        responses = responses_future.result()
        self.assertSequenceEqual(_TEST_VALUES, responses)

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

    def test_server_watch(self):
        with _test_server() as (server, port):
            with grpc.insecure_channel('localhost:{}'.format(port)) as channel:
                stub = key_value_pb2_grpc.KeyValueStoreStub(channel)
                create_request = key_value_pb2.CreateRecordRequest(
                    record=key_value_pb2.Record(
                        name="golden-retriever",
                        value="pancakes",
                    ))
                stub.CreateRecord(create_request, wait_for_ready=True)
                watch_request = key_value_pb2.WatchRecordRequest(
                    name="golden-retriever")

                responses = queue.Queue()

                def _gather_responses(stub, request):
                    response_iterator = stub.WatchRecord(request)
                    for response in response_iterator:
                        responses.put(response.value)
                        if response.value == _TEST_VALUES[-1]:
                            break
                    response_iterator.cancel()

                thread_pool = concurrent.futures.ThreadPoolExecutor(
                    max_workers=1)
                gather_future = thread_pool.submit(_gather_responses, stub,
                                                   watch_request)
                _SENTINEL = "sentinel"
                # Add meaningless values until the Watch connection has been confirmed
                # to be established.
                while responses.empty():
                    stub.UpdateRecord(
                        key_value_pb2.UpdateRecordRequest(
                            record=key_value_pb2.Record(
                                name="golden-retriever", value=_SENTINEL)))
                # Append test values.
                for value in _TEST_VALUES:
                    stub.UpdateRecord(
                        key_value_pb2.UpdateRecordRequest(
                            record=key_value_pb2.Record(
                                name="golden-retriever", value=value)))
                # Ensure the queue has been filled with the collected values.
                gather_future.result()
                filtered_responses = [
                    response for response in responses.queue
                    if response != _SENTINEL
                ]
                self.assertSequenceEqual(_TEST_VALUES, filtered_responses)


if __name__ == "__main__":
    unittest.main()
