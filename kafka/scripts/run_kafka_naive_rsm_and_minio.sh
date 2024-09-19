#!/bin/bash -e

cwd=$(pwd)
pushd "$cwd" > /dev//null

source ./env.sh

cd ./steps
echo "Start Minio server"
./run_minio_server.sh
echo "Done"
echo "Prepare and start Kafka server"
./prepare_kafka_storage.sh
./copy_libs.sh
./clear_kafka_logs.sh
./copy_kafka_config_file.sh kraft/naive_rsm_server.properties
echo "Prepare Kafka server complete"
./run_kafka_server.sh kraft/naive_rsm_server.properties
echo "Kafka server started"
./create_tiered_topic.sh topic1
cd ..

popd > /dev/null