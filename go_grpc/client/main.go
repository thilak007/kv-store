package main

import (
	"bufio"
	"context"
	"fmt"
	pb "go_grpc/proto"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// global partition configuration (set in main)
var numPartitions int32
var partitionMap map[int32][]string            // partition -> list of replica addresses
var targetIndex map[int32]int                  // partition -> index in partitionMap[] currently connected to
var targetClients map[int32]pb.KVServiceClient // partition -> cached client to current target
var targetConns map[int32]*grpc.ClientConn     // partition -> cached conn to current target
var keyspace string

// parseNodeId extracts replica index and partition ID from "replicaIdx.partitionId".
func parseNodeId(id string) (int, int) {
	parts := strings.Split(id, ".")
	if len(parts) != 2 {
		return -1, -1
	}
	var serverIdx, pid int
	fmt.Sscanf(parts[0], "%d", &serverIdx)
	fmt.Sscanf(parts[1], "%d", &pid)
	return serverIdx, pid
}

// parseManagerLeaderIndex extracts the replica index from a manager LeaderId (e.g. "0.0" → 0).
func parseManagerLeaderIndex(leaderId string) int {
	parts := strings.Split(leaderId, ".")
	if len(parts) != 2 {
		return -1
	}
	var idx int
	fmt.Sscanf(parts[0], "%d", &idx)
	return idx
}

func handlePut(partitionId int32, key, value string) {
	req := &pb.PutRequest{
		Key:   key,
		Value: value,
	}

	client := targetClients[partitionId]
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		res, err := client.Put(ctx, req)
		cancel()

		if err == nil {
			// Case1: Server doesn't know who the leader is — retry same server after delay
			if res.LeaderId == "" {
				log.Printf("PUT %s: server doesn't know leader. Retrying same replica...", key)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			// case2: Contacted server is the leader itself
			leaderIdx, _ := parseNodeId(res.LeaderId)
			if leaderIdx == targetIndex[partitionId] {
				status := "not_found"
				if res.AlreadyExists {
					status = "found"
				}
				fmt.Printf("PUT %s %s\n", key, status)
				return
			}

			// Case3: Redirect to leader
			log.Printf("PUT %s: redirecting to leader %s", key, res.LeaderId)
			client = connectToTarget(partitionId, int(leaderIdx))
			continue
		}
		// Failed — try next replica
		log.Printf("PUT failed: %v. Trying next replica...", err)
		newIdx := (targetIndex[partitionId] + 1) % len(partitionMap[partitionId])
		client = connectToTarget(partitionId, newIdx)
	}
}

func handleGet(partitionId int32, key string) {
	req := &pb.GetRequest{Key: key}

	client := targetClients[partitionId]
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		res, err := client.Get(ctx, req)
		cancel()

		if err == nil {
			// Case1: Server doesn't know who the leader is — retry same server after delay
			if res.LeaderId == "" {
				log.Printf("GET %s: server doesn't know leader. Retrying same replica...", key)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			// case2: Contacted server is the leader itself
			leaderIdx, _ := parseNodeId(res.LeaderId)
			if leaderIdx == targetIndex[partitionId] {
				value := "null"
				if res.Exists {
					value = res.Value
				}
				fmt.Printf("GET %s %s\n", key, value)
				return
			}

			// Case3: Redirect to leader
			log.Printf("GET %s: redirecting to leader %s", key, res.LeaderId)
			client = connectToTarget(partitionId, int(leaderIdx))
			continue
		}
		// Failed — try next replica
		log.Printf("GET failed: %v. Trying next replica...", err)
		newIdx := (targetIndex[partitionId] + 1) % len(partitionMap[partitionId])
		client = connectToTarget(partitionId, newIdx)
	}
}

func handleSwap(partitionId int32, key, value string) {
	req := &pb.SwapRequest{
		Key:   key,
		Value: value,
	}

	client := targetClients[partitionId]
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		res, err := client.Swap(ctx, req)
		cancel()

		if err == nil {
			// Case1: Server doesn't know who the leader is — retry same server after delay
			if res.LeaderId == "" {
				log.Printf("SWAP %s: server doesn't know leader. Retrying same replica...", key)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			// case2: Contacted server is the leader itself
			leaderIdx, _ := parseNodeId(res.LeaderId)
			if leaderIdx == targetIndex[partitionId] {
				oldValue := "null"
				if res.Exists {
					oldValue = res.OldValue
				}
				fmt.Printf("SWAP %s %s\n", key, oldValue)
				return
			}

			// Case3: Redirect to leader
			log.Printf("SWAP %s: redirecting to leader %s", key, res.LeaderId)
			client = connectToTarget(partitionId, int(leaderIdx))
			continue
		}
		// Failed — try next replica
		log.Printf("SWAP failed: %v. Trying next replica...", err)
		newIdx := (targetIndex[partitionId] + 1) % len(partitionMap[partitionId])
		client = connectToTarget(partitionId, newIdx)
	}
}

func handleSingleServerScan(partitionId int32, startKey, endKey string) (map[string]string, error) {
	var kvPairs map[string]string

	req := &pb.ScanRequest{
		StartKey: startKey,
		EndKey:   endKey,
	}

	client := targetClients[partitionId]

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		res, err := client.Scan(ctx, req)

		if err != nil {
			cancel()
			// Failed — try next replica
			log.Printf("SCAN error from partition %d: %v. Trying next replica...", partitionId, err)
			newIdx := (targetIndex[partitionId] + 1) % len(partitionMap[partitionId])
			client = connectToTarget(partitionId, newIdx)
			continue
		}

		kvPairs = map[string]string{}

		// Receive all results from this server
		for {
			kv, err := res.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				// Failed — try next replica
				log.Printf("SCAN streaming error (recv) from partition %d: %v. Trying next replica...", partitionId, err)
				newIdx := (targetIndex[partitionId] + 1) % len(partitionMap[partitionId])
				client = connectToTarget(partitionId, newIdx)
				kvPairs = nil
				break
			}
			// Case1: Server doesn't know who the leader is — retry same server after delay
			if kv.LeaderId == "" {
				log.Printf("SCAN %s: partition %d server doesn't know leader. Retrying same replica...", startKey, partitionId)
				time.Sleep(500 * time.Millisecond)
				kvPairs = nil
				break
			}

			// case2: Contacted server is a follower
			leaderIdx, _ := parseNodeId(kv.LeaderId)
			if leaderIdx != targetIndex[partitionId] {
				log.Printf("SCAN %s: partition %d server is follower, redirecting to %s", startKey, partitionId, kv.LeaderId)
				client = connectToTarget(partitionId, int(leaderIdx))
				kvPairs = nil
				break
			}
			kvPairs[kv.Key] = kv.Value
		}
		cancel()

		if kvPairs != nil {
			return kvPairs, nil
		}
		// kvPairs is nil → redirect, unknown leader, or streaming error happened, retry loop continues
	}
}

func allServerScansComplete(succeeded map[int32]bool, startPid, endPid int32) bool {
	for pid := startPid; pid <= endPid; pid++ {
		if !succeeded[pid] {
			return false
		}
	}
	return true
}

func handleScan(startKey, endKey string) {

	// determine which partitions actually need scanning
	startPid := hashKey(startKey, keyspace) // Start server ID
	endPid := hashKey(endKey, keyspace)     // End server ID

	allResp := make(map[string]string)
	allSucceeded := make(map[int32]bool)

	// build a reduced client set containing only the relevant partitions
	for i := startPid; i <= endPid; i++ {
		if _, ok := partitionMap[i]; ok {
			allSucceeded[i] = false
		}
	}

	for !allServerScansComplete(allSucceeded, startPid, endPid) {
		// Query all partitions whose scan request hasn't completed successfully.
		for pid := range allSucceeded {

			if allSucceeded[pid] {
				continue
			}

			respKVPairs, err := handleSingleServerScan(pid, startKey, endKey)

			if err != nil {
				allSucceeded[pid] = false
				continue
			}
			allSucceeded[pid] = true

			for key, value := range respKVPairs {
				allResp[key] = value
			}

		}
	}

	fmt.Printf("SCAN %s %s BEGIN\n", startKey, endKey)

	// Extract and sort keys
	var keys []string
	for key := range allResp {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Print results
	for _, key := range keys {
		fmt.Printf("  %s %s\n", key, allResp[key])
	}

	fmt.Println("SCAN END")
	return
}

func handleDelete(partitionId int32, key string) {
	req := &pb.DeleteRequest{Key: key}

	client := targetClients[partitionId]
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		res, err := client.Delete(ctx, req)
		cancel()

		if err == nil {
			// Case1: Server doesn't know who the leader is — retry same server after delay
			if res.LeaderId == "" {
				log.Printf("DELETE %s: server doesn't know leader. Retrying same replica...", key)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			// case2: Contacted server is the leader itself
			leaderIdx, _ := parseNodeId(res.LeaderId)
			if leaderIdx == targetIndex[partitionId] {
				value := "not_found"
				if res.Exists {
					value = "found"
				}
				fmt.Printf("DELETE %s %s\n", key, value)
				return
			}

			// Case3: Redirect to leader
			log.Printf("DELETE %s: redirecting to leader %s", key, res.LeaderId)
			client = connectToTarget(partitionId, int(leaderIdx))
			continue
		}
		// Failed — try next replica
		log.Printf("DELETE failed: %v. Trying next replica...", err)
		newIdx := (targetIndex[partitionId] + 1) % len(partitionMap[partitionId])
		client = connectToTarget(partitionId, newIdx)
	}
}

func getPartitionMap(managerAddrs []string) (int32, map[int32][]string) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	targetIdx := int(0)
	targetAddr := managerAddrs[0]

	for {
		log.Printf("Attempting to connect to Manager at %s", targetAddr)

		managerConn, err := grpc.NewClient(targetAddr, opts...)
		if err != nil {
			log.Printf("Failed to connect to Manager at %s: %v. Trying next replica...", targetAddr, err)
			targetIdx = (targetIdx + 1) % len(managerAddrs)
			targetAddr = managerAddrs[targetIdx]
			continue
		}

		managerClient := pb.NewClusterManagerClient(managerConn)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		res, err := managerClient.GetPartitionMap(ctx, &pb.PartitionMapRequest{})
		cancel()
		managerConn.Close()

		if err != nil {
			log.Printf("Get Partition Map Error from %s: %v. Trying next replica...", targetAddr, err)
			targetIdx = (targetIdx + 1) % len(managerAddrs)
			targetAddr = managerAddrs[targetIdx]
			continue
		}

		// Manager doesn't know who the leader is — retry same after delay
		if res.LeaderId == "" {
			log.Printf("Manager doesn't know leader. Retrying same replica...")
			time.Sleep(500 * time.Millisecond)
			continue
		}

		leaderIdx, _ := parseNodeId(res.LeaderId)
		if leaderIdx != targetIdx {
			log.Printf("Manager %s is follower. Redirecting to leader: %s", targetAddr, res.LeaderId)
			targetIdx = leaderIdx
			targetAddr = managerAddrs[targetIdx]
			continue
		}

		// We got the partition map from the leader
		partitionAddrs := make(map[int32][]string)
		for pid, entry := range res.PartitionMap {
			partitionAddrs[pid] = entry.GetAddresses()
		}

		log.Printf("Successfully retrieved partition map: %d partitions", res.NumPartitions)
		return res.NumPartitions, partitionAddrs
	}
}

func connectToServer(serverAddr string) (*grpc.ClientConn, pb.KVServiceClient) {
	retryDelay := 2 * time.Second
	attempt := 0

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	for {
		attempt++
		log.Printf("Connecting to server %s (attempt %d)...", serverAddr, attempt)

		conn, err := grpc.NewClient(serverAddr, opts...)
		if err != nil {
			log.Printf("Failed to connect to server %s: %v. Retrying in %v...", serverAddr, err, retryDelay)
			time.Sleep(retryDelay)
			continue
		}

		client := pb.NewKVServiceClient(conn)
		log.Printf("Successfully connected to server %s", serverAddr)
		return conn, client
	}
}

// connectToTarget closes the current connection for a partition and connects to partitionMap[partitionId][newIndex].
// Updates targetIndex, targetConns, targetClients.
// Returns the new client for immediate reuse.
func connectToTarget(partitionId int32, newIndex int) pb.KVServiceClient {
	// Close existing connection
	targetConns[partitionId].Close()

	// Connect to new target
	addr := partitionMap[partitionId][newIndex]
	conn, client := connectToServer(addr)

	// Update target variable
	targetIndex[partitionId] = newIndex
	targetConns[partitionId] = conn
	targetClients[partitionId] = client

	return client
}

func randomKeyPartition(key string) int32 {
	c := key[0]
	var idx int32
	switch {
	case '0' <= c && c <= '9':
		idx = int32(c - '0') // 0..9
	case 'A' <= c && c <= 'Z':
		idx = int32(c-'A') + 10 // 10..35
	case 'a' <= c && c <= 'z':
		idx = int32(c-'a') + 36 // 36..61
	default:
		idx = 0
	}
	return (idx * numPartitions) / 62
}

func keySuffixPartition(key string) int32 {
	for _, c := range key {
		if c != '0' {
			idx := int32(c - '0')
			return (idx * numPartitions) / 10
		}
	}
	return 0
}

func hashKey(key, keyspace string) int32 {
	if len(key) == 0 {
		return 0
	}
	switch keyspace {
	case "random":
		return randomKeyPartition(key)
	case "fuzz":
		return keySuffixPartition(key[3:]) // Skip "key" prefix to increase variability
	case "ycsb":
		return keySuffixPartition(key[14:]) // Skip "user_usertable" prefix to increase variability
	default:
		return randomKeyPartition(key)
	}
}

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("Usage: %s <manager_address> <keyspace>", os.Args[0])
	}

	// Get partition map from Manager
	managerAddrs := strings.Split(os.Args[1], ",")
	keyspace = os.Args[2]
	log.Printf("Keyspace value: %s \n", keyspace)

	numPartitions, partitionMap = getPartitionMap(managerAddrs)

	// Build target maps (default to 0th index = assumed leader) and connect
	targetClients = make(map[int32]pb.KVServiceClient)
	targetConns = make(map[int32]*grpc.ClientConn)
	targetIndex = make(map[int32]int)

	for pid, addrs := range partitionMap {
		if len(addrs) == 0 {
			continue
		}
		conn, client := connectToServer(addrs[0])

		targetIndex[pid] = 0
		targetClients[pid] = client
		targetConns[pid] = conn
	}
	defer func() {
		for _, conn := range targetConns {
			conn.Close()
		}
	}()

	// Read commands from stdin
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		line := scanner.Text()
		args := strings.Fields(line)

		if len(args) == 0 {
			continue
		}
		cmd := args[0]

		if cmd == "STOP" {
			fmt.Println("STOP")
			return
		}

		switch cmd {
		case "SCAN":
			handleScan(args[1], args[2])
		default:
			key := args[1]
			partitionId := hashKey(key, keyspace)

			switch cmd {
			case "PUT":
				handlePut(partitionId, key, args[2])
			case "GET":
				handleGet(partitionId, key)
			case "SWAP":
				handleSwap(partitionId, key, args[2])
			case "DELETE":
				handleDelete(partitionId, key)
			}
		}
	}
}
