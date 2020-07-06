#!/usr/bin/env python3

import argparse
import logging
import sys

import grpc
import grpc.experimental

protos = grpc.protos("key_value.proto")
services = grpc.services("key_value.proto")
KeyValueStore = services.KeyValueStore

def get(args):
    record = KeyValueStore.GetRecord(
            protos.GetRecordRequest(name=args.key),
            args.server,
            insecure=True)
    print(record.value)


def create(args):
    KeyValueStore.CreateRecord(
        protos.CreateRecordRequest(
            record=protos.Record(name=args.key, value=args.value)),
        args.server,
        insecure=True)


def update(args):
    KeyValueStore.UpdateRecord(
            protos.UpdateRecordRequest(
                record=protos.Record(name=args.key, value=args.value)),
            args.server,
            insecure=True)


def watch(args):
    values = KeyValueStore.WatchRecord(
        protos.WatchRecordRequest(name=key),
        server,
        insecure=True)
    for value in values:
        print(value)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Client to an in-memory key-value store.")
    parser.add_argument(
        "--server",
        help="The server target to which to connect.",
        default="localhost:50051")
    subparsers = parser.add_subparsers(dest="subparser_name")
    create_parser = subparsers.add_parser(
        "create", help="Create a new record.")
    create_parser.add_argument("key")
    create_parser.add_argument("value")
    create_parser.set_defaults(func=create)
    update_parser = subparsers.add_parser(
        "update", help="Update an existing record.")
    update_parser.add_argument("key")
    update_parser.add_argument("value")
    update_parser.set_defaults(func=update)
    get_parser = subparsers.add_parser(
        "get", help="Get the value associated with a key.")
    get_parser.add_argument("key")
    get_parser.set_defaults(func=get)
    watch_parser = subparsers.add_parser(
        "watch", help="Watch the value associated with a key as it changes.")
    watch_parser.add_argument("key")
    watch_parser.set_defaults(func=watch)
    args = parser.parse_args()
    if 'func' not in args:
        parser.print_usage()
        sys.exit(1)
    args.func(args)
