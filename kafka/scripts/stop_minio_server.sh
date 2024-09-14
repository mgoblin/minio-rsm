#!/bin/bash

pid=$(pgrep minio)

kill "$pid"