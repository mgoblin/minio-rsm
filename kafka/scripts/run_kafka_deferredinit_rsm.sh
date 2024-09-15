#!/bin/bash

cwd=$(pwd)
pushd "$cwd" || exit

source ./env.sh

./steps/prepare_kafka.sh
timeout 30s grep -q 'Stopping SharedServer' <(tail -f "$KAFKA_BASE_DIR/logs/server.log") || exit 1

rm -rf "${KAFKA_BASE_DIR:?}"/logs/*

cp "$cwd"/../config/kraft/deferredinit_rsm_server.properties "$KAFKA_BASE_DIR/config/kraft/deferredinit_rsm_server.properties"
./steps/copy_libs.sh

cd "$KAFKA_BASE_DIR" || exit

bin/kafka-server-start.sh -daemon \
"$KAFKA_BASE_DIR/config/kraft/deferredinit_rsm_server.properties"
sleep 1s
timeout 30s grep -q 'Kafka Server started' <(tail -f "$KAFKA_BASE_DIR/logs/server.log") || exit 1

popd || exit