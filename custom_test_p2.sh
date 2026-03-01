#!/bin/bash

echo "=== Partition Failure Test ==="
mkdir -p ./data/server0 ./data/server1 ./data/server2 logs

# Step 1: Start cluster
./bin/manager localhost:3666 localhost:3777,localhost:3778,localhost:3779 > logs/manager.log 2>&1 &
MANAGER_PID=$!

./bin/server localhost:3666 localhost:3777 0 ./data/server0 > logs/server0.log 2>&1 &
SERVER0_PID=$!

./bin/server localhost:3666 localhost:3778 1 ./data/server1 > logs/server1.log 2>&1 &
SERVER1_PID=$!

./bin/server localhost:3666 localhost:3779 2 ./data/server2 > logs/server2.log 2>&1 &
SERVER2_PID=$!

sleep 3
echo "Cluster started!"

# Step 2-3: Run test1 and test2
echo "Running test1 (PUTs and SWAPs)..."
./bin/client localhost:3666 < tests2/test1.txt

echo "Running test2 (GETs and SCANs)..."
./bin/client localhost:3666 < tests2/test2.txt

# Step 4: Kill Server 1
echo "Killing Server 1..."
kill $SERVER1_PID
sleep 2
echo "Server 1 killed!"

# Step 5: Test unaffected partitions
echo "Testing unaffected partitions (should succeed)..."
./bin/client localhost:3666 < tests2/test3.txt

# Step 6: Test affected partition (will retry)
echo "Testing affected partition (will retry)..."
./bin/client localhost:3666 < tests2/test4.txt > logs/test4.log 2>&1 &
CLIENT_PID=$!

# Wait to see retries in logs
echo "Waiting 10 seconds to observe retries..."
sleep 10

# Step 7: Restart Server 1
echo "Restarting Server 1..."
./bin/server localhost:3666 localhost:3778 1 ./data/server1 > logs/server1_recovered.log 2>&1 &
SERVER1_PID=$!

# Wait for client to succeed
echo "Waiting for client to succeed..."
wait $CLIENT_PID
echo "Client succeeded after recovery!"

# Step 8: Verify persistence
echo "Verifying persistence..."
./bin/client localhost:3666 < tests2/test5.txt

# Cleanup
echo "Cleaning up..."
kill $MANAGER_PID $SERVER0_PID $SERVER1_PID $SERVER2_PID 2>/dev/null

echo "Test complete!"