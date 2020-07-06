import grpc

protos = grpc.protos("key_value.proto")
services = grpc.services("key_value.proto")

def _get(channel, key):
    stub = services.KeyValueStoreStub(channel)
    get_request = protos.GetRecordRequest(name=key)
    record = stub.GetRecord(get_request)
    return record.value


def get(server, key, channel=None):
    if channel is not None:
        return _get(channel, key)
    else:
        with grpc.insecure_channel(server) as channel:
            return _get(channel, key)


def _create(channel, key, value):
    stub = services.KeyValueStoreStub(channel)
    create_request = protos.CreateRecordRequest(
        record=protos.Record(
            name=key,
            value=value,
        ))
    record = stub.CreateRecord(create_request)


def create(server, key, value, channel=None):
    if channel is not None:
        _create(channel, key, value)
    else:
        with grpc.insecure_channel(server) as channel:
            _create(channel, key, value)


def _update(channel, key, value):
    stub = services.KeyValueStoreStub(channel)
    update_request = protos.UpdateRecordRequest(
        record=protos.Record(
            name=key,
            value=value,
        ))
    update_response = stub.UpdateRecord(update_request)


def update(server, key, value, channel=None):
    if channel is not None:
        _update(channel, key, value)
    else:
        with grpc.insecure_channel(server) as channel:
            _update(channel, key, value)


def _watch(channel, key):
    stub = services.KeyValueStoreStub(channel)
    watch_request = protos.WatchRecordRequest(name=key)
    for record in stub.WatchRecord(watch_request):
        yield record.value


def watch(server, key, channel=None):
    if channel is not None:
        for value in _watch(channel, key):
            yield value
    else:
        with grpc.insecure_channel(server) as channel:
            for value in _watch(channel, key):
                yield value


__all__ = [get, create, update]
