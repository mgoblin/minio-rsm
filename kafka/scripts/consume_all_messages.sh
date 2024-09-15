#!/bin/bash

cwd=$(pwd)
pushd "$cwd" || exit

source ./env.sh
cd "$KAFKA_BASE_DIR" || exit

./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
--topic topic1 --from-beginning --timeout-ms 10000

popd || exit
