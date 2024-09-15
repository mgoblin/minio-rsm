#!/bin/bash

cwd=$(pwd)
pushd "$cwd" || exit

source ./env.sh

cd ./steps || exit
./run_minio_server.sh
./prepare_kafka_storage.sh
./copy_libs.sh
cd .. || exit

rm -rf "${KAFKA_BASE_DIR:?}"/logs/*

cp "$cwd"/../config/kraft/naive_rsm_server.properties "$KAFKA_BASE_DIR/config/kraft/naive_rsm_server.properties"

cd "$KAFKA_BASE_DIR" || exit

bin/kafka-server-start.sh -daemon \
"$KAFKA_BASE_DIR/config/kraft/naive_rsm_server.properties"
sleep 1s
timeout 30s grep -q 'Kafka Server started' <(tail -f "$KAFKA_BASE_DIR/logs/server.log") || exit 1

cd "$cwd"/steps || exit
./create_tiered_topic.sh

popd || exit