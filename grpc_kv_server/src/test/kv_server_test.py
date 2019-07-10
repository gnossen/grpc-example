from unittest import TestCase

import grpc_kv_server

class TestGrpcKvServer(TestCase):
    def test_without_grpc(self):
        kv_store = grpc_kv_server.KeyValueStore()
        kv_store.store("golden-retriever", "pancakes")
        self.assertEqual("pancakes", kv_store.get("golden-retriever"))

    def test_with_grpc_in_a_single_process(self):
        pass

if __name__ == "__main__":
    unittest.main()
