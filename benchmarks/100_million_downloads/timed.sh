# timed.sh

ulimit -n 3000 # increase to avoid "out of file descriptors" error

python server.py &

sleep 1 # Wait for server to start

/usr/bin/time --format "Memory usage: %MKB\tTime: %e seconds\tCPU usage: %P" "$@"

kill %1