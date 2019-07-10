import setuptools
from distutils.core import Command
import grpc_tools.command

if __name__ == "__main__":
    setuptools.setup(
        name="grpc-kv-client",
        install_requires=["grpcio"],
        setup_requires=["grpcio-tools"],
        author="Richard Belleville",
        author_email="rbellevi@google.com",
        description="An example usage of gRPC Python.",
        packages=["grpc_kv_client", ""],
        package_dir={"": "src"},
        scripts=["bin/grpc_kv_client"],
        test_suite="test",
        cmdclass={"generate": grpc_tools.command.BuildPackageProtos},
        classifiers=[
            'Programming Language :: Python',
            'Programming Language :: Python :: 3',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
        ],
    )
