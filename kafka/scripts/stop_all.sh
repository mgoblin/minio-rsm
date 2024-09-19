#!/bin/bash -e

cwd=$(pwd)
pushd "$cwd" > /dev/null

source ./env.sh

cd ./steps
echo "Stop Kafka server"
./stop_kafka_server.sh
echo "Done"
cd ..

pid=$(pgrep minio)

kill "$pid"

cd "$MINIO_DATA_DIR"
rm -rf ./*

popd > /dev/null