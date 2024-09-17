#!/bin/bash -e

cwd=$(pwd)
pushd "$cwd" > /dev/null

source ../env.sh
cd "$KAFKA_BASE_DIR"

bin/kafka-producer-perf-test.sh \
   --topic topic1 --num-records=10000 --throughput -1 --record-size 1000 \
   --producer-props acks=1 batch.size=16384 bootstrap.servers=localhost:9092

popd > /dev/null