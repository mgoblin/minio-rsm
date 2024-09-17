#!/bin/bash -e

cwd=$(pwd)
pushd "$cwd" > /dev/null

source ../env.sh

rm -rf "${KAFKA_BASE_DIR:?}"/logs/*

popd > /dev/null