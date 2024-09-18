#!/bin/bash -e

cwd=$(pwd)
pushd "$cwd" > /dev/null

source ../env.sh

(minio server --address localhost:9000 --console-address localhost:9001 "$MINIO_DATA_DIR" &) > /dev/null

popd > /dev/null
