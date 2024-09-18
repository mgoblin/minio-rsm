#!/bin/bash -e

cwd=$(pwd)
pushd "$cwd" > /dev/null

source ../env.sh

./clear_kafka_data.sh
./copy_kafka_config_file.sh kraft/server.properties

# Get UUID
cd "$KAFKA_BASE_DIR"
UUID="$(bin/kafka-storage.sh random-uuid)"

# Format storage
bin/kafka-storage.sh format \
-g \
-t "$UUID" \
-c "$KAFKA_BASE_DIR/config/kraft/server.properties" > /dev/null

bin/kafka-server-start.sh -daemon \
"$KAFKA_BASE_DIR/config/kraft/server.properties"
sleep 5s
timeout 3m grep -q 'Kafka Server started' <(tail -f "$KAFKA_BASE_DIR/logs/server.log")

bin/kafka-server-stop.sh
timeout 30s grep -q 'Stopping SharedServer' <(tail -f "$KAFKA_BASE_DIR/logs/server.log")

popd > /dev/null