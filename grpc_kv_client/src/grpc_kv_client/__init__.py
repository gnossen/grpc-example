import grpc
import grpc.experimental

protos = grpc.protos("key_value.proto")
services = grpc.services("key_value.proto")
KeyValueStore = services.KeyValueStore

def get(server, key):
    record = KeyValueStore.GetRecord(
            protos.GetRecordRequest(name=key),
            server,
            insecure=True)
    return record.value


def create(server, key, value):
    KeyValueStore.CreateRecord(
        protos.CreateRecordRequest(
            record=protos.Record(name=key, value=value)),
        server,
        insecure=True)


def update(server, key, value):
    KeyValueStore.UpdateRecord(
            protos.UpdateRecordRequest(
                record=protos.Record(name=key, value=value)),
            server,
            insecure=True)


def watch(server, key):
    values = KeyValueStore.WatchRecord(
        protos.WatchRecordRequest(name=key),
        server,
        insecure=True)
    for value in values:
        yield value


__all__ = [get, create, update]
