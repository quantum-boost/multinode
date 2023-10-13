#!/bin/bash
set -e  # exit on first error
cd "${0%/*}"  # cd into script's parent directory

bash ./generate_client.sh
poetry self add poetry-multiproject-plugin

cd ../multinode-shared
poetry install

cd ../multinode
poetry install

cd ../multinode-external
poetry install