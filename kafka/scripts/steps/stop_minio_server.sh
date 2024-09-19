#!/bin/bash

pid=$(ps ax | grep "minio server" | grep -v "grep minio"| awk '{print $1}')
kill "$pid"