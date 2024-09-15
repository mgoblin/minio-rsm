#!/bin/bash

cwd=$(pwd)
pushd "$cwd" || exit

source ./env.sh

rm -rf "${KAFKA_BASE_DIR:?}"/logs/*

cp "$cwd"/../config/kraft/naive_rsm_server.properties "$KAFKA_BASE_DIR/config/kraft/naive_rsm_server.properties"
./copy_libs.sh

./run_minio_server.sh

cd "$KAFKA_BASE_DIR" || exit

bin/kafka-server-start.sh -daemon \
"$KAFKA_BASE_DIR/config/kraft/naive_rsm_server.properties"
sleep 5s
timeout 3m grep -q 'Kafka Server started' <(tail -f "$KAFKA_BASE_DIR/logs/server.log") || exit 1

popd || exit