import concurrent.futures
import os
import unittest
import sys

import grpc
import grpc_testing

import grpc_kv_client


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

protos = grpc.protos("key_value.proto")
services = grpc.services("key_value.proto")


def _get_method(method_name):
    return protos.DESCRIPTOR.services_by_name[
        "KeyValueStore"].methods_by_name[method_name]


class TestGrpcKeyValueClient(unittest.TestCase):
    def test_create_with_mock_server(self):
        thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        fake_channel = grpc_testing.channel(
            protos.DESCRIPTOR.services_by_name.values(),
            grpc_testing.strict_real_time())
        # Offload the request to another thread so we can use this thread to
        # fake the server-side results.
        result_future = thread_pool.submit(
            grpc_kv_client.create,
            "localhost:1234",
            "golden-retriever",
            "pancakes",
            channel=fake_channel)
        # Fake the server-side results.
        invocation_metadata, request, rpc = (fake_channel.take_unary_unary(
            _get_method("CreateRecord")))
        rpc.send_initial_metadata(())
        rpc.terminate(
            protos.Record(name="golden-retriever", value="pancakes"),
            (), grpc.StatusCode.OK, "")
        # Ensure the client had the correct response.
        result = result_future.result()
        self.assertIsNone(result)
