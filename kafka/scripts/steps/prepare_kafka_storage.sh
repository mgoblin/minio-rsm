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

popd > /dev/null