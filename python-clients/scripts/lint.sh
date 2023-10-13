#!/bin/bash
set -e  # exit on first error
cd "${0%/*}"  # cd into script's parent directory

cd ../multinode-shared
poetry run mypy .
poetry run black --check .
poetry run isort --check-only .
poetry run flake8 .

cd ../multinode
poetry run mypy .
poetry run black --check .
poetry run isort --check-only .
poetry run flake8 .

cd ../multinode-external
poetry run mypy .
poetry run black --check .
poetry run isort --check-only .
poetry run flake8 .
