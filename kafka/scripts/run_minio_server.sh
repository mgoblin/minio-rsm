#!/bin/bash


cwd=$(pwd)
pushd "$cwd" || exit

source ./env.sh

minio server --address localhost:9000 --console-address localhost:9001 "$MINIO_DATA_DIR" &

pgrep minio > minio.pid

popd || exit
