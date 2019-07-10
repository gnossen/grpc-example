import grpc

import key_value_pb2
import key_value_pb2_grpc


def get(server, key):
    with grpc.insecure_channel(server) as channel:
        stub = key_value_pb2_grpc.KeyValueStoreStub(channel)
        get_request = key_value_pb2.GetRequest(key=key)
        get_response = stub.GetValue(get_request)
        return get_response.key_value_pair.value


def store(server, key, value):
    with grpc.insecure_channel(server) as channel:
        stub = key_value_pb2_grpc.KeyValueStoreStub(channel)
        store_request = key_value_pb2.StoreRequest(
            key_value_pair=key_value_pb2.KeyValuePair(
                key=key,
                value=value,
            ))
        store_response = stub.StoreValue(store_request)


__all__ = [get, store]
