import setuptools

if __name__ == "__main__":
    setuptools.setup(
        name="grpckvserver",
        install_requires=["grpcio"],
        author="Richard Belleville",
        author_email="rbellevi@google.com",
        description="An example usage of gRPC Python.",
        packages=["grpc_kv_server"],
        package_dir={"grpc_kv_server": "grpc_kv_server"},
        scripts=["bin/grpc_kv_server"],
        test_suite="test",
    )
