#!/bin/bash

cwd=$(pwd)
pushd "$cwd" || exit

source ./env.sh

cd ./steps || exit
./run_minio_server.sh
./prepare_kafka_storage.sh
./copy_libs.sh
./clear_kafka_logs.sh
./copy_kafka_config_file.sh kraft/naive_rsm_server.properties
./run_kafka_server.sh kraft/naive_rsm_server.properties
cd .. || exit

cd "$cwd"/steps || exit
./create_tiered_topic.sh topic1

popd || exit