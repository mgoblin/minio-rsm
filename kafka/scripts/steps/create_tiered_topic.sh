#!/bin/bash

cwd=$(pwd)
pushd "$cwd" || exit

source ./env.sh

cd "$KAFKA_BASE_DIR" || exit

bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic topic1 \
    --config remote.storage.enable=true \
    --config segment.bytes=512000 \
    --config local.retention.bytes=1 \
    --config retention.bytes=5120000

popd || exit