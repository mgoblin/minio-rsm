#!/bin/bash -e

cwd=$(pwd)
pushd "$cwd" > /dev/null

source ../env.sh

cd "$KAFKA_BASE_DIR"

log_file="$KAFKA_BASE_DIR/logs/server.log"

touch "$log_file"

bin/kafka-server-start.sh -daemon \
"$KAFKA_BASE_DIR/config/$1"

timeout 30s grep -q 'Kafka Server started' <(tail -f "$log_file")

popd > /dev/null