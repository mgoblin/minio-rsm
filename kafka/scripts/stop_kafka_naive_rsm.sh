#!/bin/bash

cwd=$(pwd)
pushd "$cwd" || exit

cd "$KAFKA_BASE_DIR" || exit

bin/kafka-server-stop.sh

cd "$cwd" || exit

pid=$(pgrep minio)

kill "$pid"

popd || exit