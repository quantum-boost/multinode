#!/bin/bash
set -e  # exit on first error
cd "${0%/*}/.."  # cd into the python-client root directory

poetry run mypy .
poetry run black --check .
poetry run isort --check-only .
poetry run flake8 .
