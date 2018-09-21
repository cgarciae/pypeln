#!/usr/bin/env bash

python benchmarks/server.py &
sleep 1 # Wait for server to start

/usr/bin/time --format "Memory usage: %MKB\tTime: %e seconds\tCPU usage: %P" "$@"
# /usr/bin/time -v "$@"

# %e Elapsed real (wall clock) time used by the process, in seconds.
# %M Maximum resident set size of the process in Kilobytes.

kill %1