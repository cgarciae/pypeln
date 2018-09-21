#!/usr/bin/env bash

python server.py &

sleep 1 # Wait for server to start

/usr/bin/time --format "Memory usage: %MKB\tTime: %e seconds\tCPU usage: %P" "$@"

kill %1