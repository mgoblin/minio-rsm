#!/bin/bash -e

cwd=$(pwd)
pushd "$cwd" > /dev/null

source ../env.sh

cd "$KAFKA_BASE_DIR"

bin/kafka-server-stop.sh

popd > /dev/null