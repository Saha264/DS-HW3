#!/bin/bash
# One-time setup: creates a virtual environment, installs gRPC, generates stubs.
#
#   ./setup.sh
#   source venv/bin/activate      <- do this in every new terminal before running
cd "$(dirname "$0")"
python3 -m venv venv || { echo "python3 -m venv failed (try: sudo apt install python3-venv)"; exit 1; }
source venv/bin/activate
pip install -q -r requirements.txt && echo "Installed grpcio + grpcio-tools"
./generate_stubs.sh
echo
echo "Done. In each terminal run:  source venv/bin/activate"
