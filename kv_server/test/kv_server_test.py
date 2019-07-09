from unittest import TestCase

import grpc_kv_server

class TestGrpcKvServer(TestCase):
    def test_has_test_value(self):
        self.assertEqual(2, grpc_kv_server.TEST_VALUE)

if __name__ == "__main__":
    unittest.main()
