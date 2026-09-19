#!/bin/bash
# Regenerates food_ordering_pb2.py and food_ordering_pb2_grpc.py from the .proto.
# Run this once after setup, and again any time food_ordering.proto changes.
cd "$(dirname "$0")"
if [ ! -f food_ordering.proto ]; then
    echo "ERROR: food_ordering.proto not found in $(pwd)" >&2
    exit 1
fi
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. food_ordering.proto \
    && echo "Generated food_ordering_pb2.py and food_ordering_pb2_grpc.py"
