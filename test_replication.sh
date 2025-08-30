#!/bin/bash

# Test script to verify replication works correctly

echo "Starting master server on port 6379..."
./your_program.sh --port 6379 &
MASTER_PID=$!

sleep 2

echo "Starting replica server on port 6380..."
./your_program.sh --port 6380 --replicaof localhost 6379 &
REPLICA_PID=$!

sleep 3

echo "Testing SET command propagation..."
redis-cli -p 6379 SET foo 123

sleep 1

echo "Checking if replica received the SET command..."
redis-cli -p 6380 GET foo

echo "Cleaning up..."
kill $MASTER_PID $REPLICA_PID 2>/dev/null
wait $MASTER_PID $REPLICA_PID 2>/dev/null

echo "Test completed."
