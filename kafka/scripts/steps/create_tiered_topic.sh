#!/bin/bash -e

cwd=$(pwd)
pushd "$cwd" > /dev/null

source ../env.sh

cd "$KAFKA_BASE_DIR" || exit

bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic "$1" \
    --config remote.storage.enable=true \
    --config segment.bytes=512000 \
    --config local.retention.bytes=1 \
    --config retention.bytes=5120000 \
    --config retention.ms=-1 \
    --config local.retention.ms=-1

popd > /dev/null
