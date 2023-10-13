#!/bin/bash
set -e  # exit on first error
cd "${0%/*}/../multinode-shared"  # cd into the shared directory

API_SCHEMAS_DIR="../../api-schemas"
JAR_FILEPATH="${API_SCHEMAS_DIR}/openapi-generator-cli.jar"
SCHEMA_FILEPATH="${API_SCHEMAS_DIR}/control-plane.json"

if [ ! -f "${SCHEMA_FILEPATH}" ]; then
    ABSOLUTE_SCHEMA_PATH="$(cd "$(dirname "${SCHEMA_FILEPATH}")"; pwd)/$(basename "${SCHEMA_FILEPATH}")"
    echo "Couldn't find control plane schema at ${ABSOLUTE_SCHEMA_PATH}"
    exit 1
fi

if [ ! -f "${JAR_FILEPATH}" ]; then
    echo "Downloading openapi-generator-cli.jar..."
    wget https://repo1.maven.org/maven2/org/openapitools/openapi-generator-cli/7.0.1/openapi-generator-cli-7.0.1.jar -O "${JAR_FILEPATH}"
fi

java -jar "${JAR_FILEPATH}" generate \
  -i "${SCHEMA_FILEPATH}" \
  -g python \
  --additional-properties=generateSourceCodeOnly=True,packageName=multinode_shared.api_client