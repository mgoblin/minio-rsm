#!/bin/bash -e

cwd=$(pwd)
pushd "$cwd" > /dev/null

source ../env.sh

if [ ! -f "$DISTRIBUTION_FILE" ]
then
  echo "Build libs"
  cd ../../
  gradle clean build
fi

tar -C "$KAFKA_BASE_DIR/libs" -xvf "$DISTRIBUTION_FILE" --strip-components 1 > /dev/null

popd > /dev/null