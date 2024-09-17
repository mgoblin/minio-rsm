#!/bin/bash -e

cwd=$(pwd)
pushd "$cwd" > /dev/null

source ../env.sh
cd "$KAFKA_BASE_DIR"

./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
--topic topic1 --from-beginning --timeout-ms 10000

popd > /dev/null
