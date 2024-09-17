#!/bin/bash -e

cwd=$(pwd)
pushd "$cwd" > /dev/null

source ../env.sh

cd ../../config

cp "$1" "$KAFKA_BASE_DIR/config/$1"

popd > /dev/null