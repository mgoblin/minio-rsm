#!/bin/bash

cwd=$(pwd)
pushd "$cwd" || exit

source ./env.sh

rm -rf "${KAFKA_DATA_DIR:?}"/*

cp "$cwd"/../config/kraft/server.properties "$KAFKA_BASE_DIR/config/kraft/server.properties"

./copy_libs.sh

cd "$KAFKA_BASE_DIR" || exit
UUID="$(bin/kafka-storage.sh random-uuid)"

bin/kafka-storage.sh format \
-g \
-t "$UUID" \
-c "$KAFKA_BASE_DIR/config/kraft/server.properties"

bin/kafka-server-start.sh -daemon \
"$KAFKA_BASE_DIR/config/kraft/server.properties"
sleep 5s
timeout 3m grep -q 'Kafka Server started' <(tail -f "$KAFKA_BASE_DIR/logs/server.log") || exit 1

bin/kafka-server-stop.sh

popd || exit