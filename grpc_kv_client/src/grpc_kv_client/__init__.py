import grpc

import key_value_pb2
import key_value_pb2_grpc


def get(server, key):
    with grpc.insecure_channel(server) as channel:
        stub = key_value_pb2_grpc.KeyValueStoreStub(channel)
        get_request = key_value_pb2.GetRecordRequest(name=key)
        record = stub.GetRecord(get_request)
        return record.value


def create(server, key, value):
    with grpc.insecure_channel(server) as channel:
        stub = key_value_pb2_grpc.KeyValueStoreStub(channel)
        create_request = key_value_pb2.CreateRecordRequest(
            record=key_value_pb2.Record(
                name=key,
                value=value,
            ))
        record = stub.CreateRecord(create_request)


def update(server, key, value):
    with grpc.insecure_channel(server) as channel:
        stub = key_value_pb2_grpc.KeyValueStoreStub(channel)
        update_request = key_value_pb2.UpdateRecordRequest(
            record=key_value_pb2.Record(
                name=key,
                value=value,
            ))
        update_response = stub.UpdateRecord(update_request)


__all__ = [get, create, update]
