#!/bin/bash

cwd=$(pwd)
pushd "$cwd" || exit

source ./env.sh

cd "$KAFKA_BASE_DIR" || exit

ls -la || exit

bin/kafka-server-stop.sh

cd "$cwd" || exit

pid=$(pgrep minio)

kill "$pid"

cd "$MINIO_DATA_DIR" || exit
rm -rf ./*

popd || exit