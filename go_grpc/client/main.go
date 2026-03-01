package main

import (
	"bufio"
	"context"
	"fmt"
	pb "go_grpc/proto"
	"hash/fnv"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func handlePut(client pb.KVServiceClient, key, value string) {
	attempt := 0
	maxDelay := 30 * time.Second

	req := &pb.PutRequest{
		Key:   key,
		Value: value,
	}

	for {
		attempt++
		retryDelay := time.Duration(2*attempt) * time.Second
		if retryDelay > maxDelay {
			retryDelay = maxDelay
		}
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)

		res, err := client.Put(ctx, req)
		cancel()

		if err == nil {
			status := "not_found"
			if res.AlreadyExists {
				status = "found"
			}
			fmt.Printf("PUT %s %s\n", key, status)
			return
		}
		// Failed - retry indefinitely
		log.Printf("PUT failed (attempt %d): %v. Retrying in %v...", attempt, err, retryDelay)
		time.Sleep(retryDelay)
	}
}

func handleGet(client pb.KVServiceClient, key string) {
	attempt := 0
	maxDelay := 30 * time.Second

	req := &pb.GetRequest{Key: key}

	for {
		attempt++
		retryDelay := time.Duration(2*attempt) * time.Second
		if retryDelay > maxDelay {
			retryDelay = maxDelay
		}
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)

		res, err := client.Get(ctx, req)
		cancel()
		if err == nil {
			value := "null"
			if res.Exists {
				value = res.Value
			}
			fmt.Printf("GET %s %s\n", key, value)
			return
		}
		// Failed - retry indefinitely
		log.Printf("GET failed (attempt %d): %v. Retrying in %v...", attempt, err, retryDelay)
		time.Sleep(retryDelay)
	}
}

func handleSwap(client pb.KVServiceClient, key, value string) {
	attempt := 0
	maxDelay := 30 * time.Second

	req := &pb.SwapRequest{
		Key:   key,
		Value: value,
	}

	for {
		attempt++
		retryDelay := time.Duration(2*attempt) * time.Second
		if retryDelay > maxDelay {
			retryDelay = maxDelay
		}
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)

		res, err := client.Swap(ctx, req)
		cancel()

		if err == nil {
			oldValue := "null"
			if res.Exists {
				oldValue = res.OldValue
			}
			fmt.Printf("SWAP %s %s\n", key, oldValue)
			return
		}
		// Failed - retry indefinitely
		log.Printf("SWAP failed (attempt %d): %v. Retrying in %v...", attempt, err, retryDelay)
		time.Sleep(retryDelay)
	}
}

func handleSingeServerScan(serverAddr string, client pb.KVServiceClient, startKey, endKey string) (map[string]string, error) {
	attempt := 1
	retryDelay := time.Duration(2*attempt) * time.Second
	var kvPairs map[string]string

	req := &pb.ScanRequest{
		StartKey: startKey,
		EndKey:   endKey,
	}

	isSuccess := false

	// TODO: Should we have a outer timeout of 15minutes and return gracefully instead of indefinite retires?
	for !isSuccess {
		attempt++

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute) // 5 Minute timeout for each request.
		res, err := client.Scan(ctx, req)
		cancel()

		if err != nil {
			// Failed - retry
			log.Printf("SCAN error from %s (attempt %d): %v.  Retrying in %v..", serverAddr, attempt, err, retryDelay)
			time.Sleep(retryDelay)
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
				log.Printf("SCAN streaming error (recv) from %s (attempt %d): %v. Retrying in %v..", serverAddr, attempt, err, retryDelay)
				time.Sleep(retryDelay)
				break
			}
			kvPairs[kv.Key] = kv.Value
		}
		isSuccess = true
		return kvPairs, err
	}

	return nil, fmt.Errorf("scan failed after all retries")
}

func isAllServerScansComplete(mp map[string]bool) bool {
	for _, v := range mp {
		if !v {
			return false
		}
	}
	return true
}

func handleScan(serverClients map[string]pb.KVServiceClient, startKey, endKey string) {

	allResp := make(map[string]string)
	allSucceeded := make(map[string]bool)

	for serverAddr, _ := range serverClients {
		allSucceeded[serverAddr] = false
	}

	for !isAllServerScansComplete(allSucceeded) {
		// Query all servers whose scan request hasn't completed successfully.
		for serverAddr, client := range serverClients {

			if allSucceeded[serverAddr] {
				continue
			}

			respKVPairs, err := handleSingeServerScan(serverAddr, client, startKey, endKey)

			if err != nil {
				allSucceeded[serverAddr] = false
				break
			}
			allSucceeded[serverAddr] = true

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

func handleDelete(client pb.KVServiceClient, key string) {
	attempt := 0
	maxDelay := 30 * time.Second

	req := &pb.DeleteRequest{Key: key}

	for {
		attempt++
		retryDelay := time.Duration(2*attempt) * time.Second
		if retryDelay > maxDelay {
			retryDelay = maxDelay
		}
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)

		res, err := client.Delete(ctx, req)
		cancel()

		if err == nil {
			value := "not_found"
			if res.Exists {
				value = "found"
			}
			fmt.Printf("DELETE %s %s\n", key, value)
			return
		}
		// Failed - retry indefinitely
		log.Printf("DELETE failed (attempt %d): %v. Retrying in %v...", attempt, err, retryDelay)
		time.Sleep(retryDelay)
	}
}

func getPartitionMap(ManagerAddr string) (int32, map[int32]string) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	attempt := 1
	retryDelay := time.Duration(2*attempt) * time.Second

	for {
		attempt++
		log.Printf("Attempting to connect to Manager at %s (attempt %d)", ManagerAddr, attempt)

		managerConn, err := grpc.NewClient(ManagerAddr, opts...)
		if err != nil {
			log.Fatalf("Failed to connect: %v. Retrying in %v...", err, retryDelay)
			time.Sleep(retryDelay)
			continue
		}

		managerClient := pb.NewClusterManagerClient(managerConn)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		res, err := managerClient.GetPartitionMap(ctx, &pb.PartitionMapRequest{})

		cancel()
		managerConn.Close()

		if err != nil {
			log.Printf("Get Partition Map Error: %v. Retrying in %v...", err, retryDelay)
			time.Sleep(retryDelay)
			continue
		}

		log.Printf("Successfully retrieved partition map: %d partitions", res.NumPartitions)
		return res.NumPartitions, res.PartitionMap
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

func hashKey(key string, numPartitions int32) int32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int32(h.Sum32()) % numPartitions
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <manager_address>", os.Args[0])
	}

	// Get partition map from Manager
	managerAddr := os.Args[1]
	numPartitions, partitionMap := getPartitionMap(managerAddr)

	// Connect to all servers
	serverClients := make(map[string]pb.KVServiceClient)
	serverConns := make(map[string]*grpc.ClientConn)

	for _, serverAddr := range partitionMap {
		conn, client := connectToServer(serverAddr)
		serverClients[serverAddr] = client // Store client object for making RPC calls
		serverConns[serverAddr] = conn
	}
	defer func() {
		for _, conn := range serverConns {
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
			handleScan(serverClients, args[1], args[2])
		default:
			key := args[1]
			partitionId := hashKey(key, numPartitions)
			serverAddr := partitionMap[partitionId]
			client := serverClients[serverAddr]

			switch cmd {
			case "PUT":
				handlePut(client, key, args[2])
			case "GET":
				handleGet(client, key)
			case "SWAP":
				handleSwap(client, key, args[2])
			case "DELETE":
				handleDelete(client, key)
			}
		}
	}
}
