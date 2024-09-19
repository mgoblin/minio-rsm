#!/bin/bash -e

cwd=$(pwd)
pushd "$cwd" > /dev/null

source ./env.sh

cd ./steps
echo "Stopping Kafka server"
./stop_kafka_server.sh
echo "Done"
echo "Stopping Minio server"
./stop_minio_server.sh
echo "Done"
cd ..

cd "$MINIO_DATA_DIR"
rm -rf ./*

popd > /dev/null