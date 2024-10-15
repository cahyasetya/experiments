#!/bin/bash

# Read all keys from keys.txt into an array
IFS=',' read -r -a keys < keys.txt

# Function to run benchmark for a single key
run_benchmark() {
    local key=$1
    echo "Benchmarking LRANGE for key: $key"
    redis-benchmark -n 1 -c 10 -q LRANGE "$key" 0 -1
}

# Run benchmarks for all keys
echo "key,requests_per_second,average_latency_ms"
for key in "${keys[@]}"; do
    result=$(run_benchmark "$key")
    echo $result
    rps=$(echo "$result" | awk -F',' '{print $2}')
    latency=$(echo "$result" | awk -F',' '{print $3}')
    echo "$key,$rps,$latency"
done
