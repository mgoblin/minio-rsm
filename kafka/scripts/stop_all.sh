#!/bin/bash -e

cwd=$(pwd)
pushd "$cwd" > /dev/null

source ./env.sh

cd "$KAFKA_BASE_DIR"

bin/kafka-server-stop.sh

cd "$cwd"

pid=$(pgrep minio)

kill "$pid"

cd "$MINIO_DATA_DIR"
rm -rf ./*

popd > /dev/null