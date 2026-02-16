package main

import (
	"bufio"
	"context"
	"fmt"
	pb "go_grpc/proto"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func handlePut(client pb.KVServiceClient, key, value string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	req := &pb.PutRequest{
		Key:   key,
		Value: value,
	}
	res, err := client.Put(ctx, req)
	if err != nil {
		log.Fatalf("PUT Error: %v", err)
		return
	}

	status := "not_found"
	if res.AlreadyExists {
		status = "found"
	}
	fmt.Printf("PUT %s %s\n", key, status)
}

func handleGet(client pb.KVServiceClient, key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	req := &pb.GetRequest{
		Key: key,
	}
	res, err := client.Get(ctx, req)
	if err != nil {
		log.Fatalf("GET Error: %v", err)
		return
	}
	value := "null"
	if res.Exists {
		value = res.Value
	}
	fmt.Printf("GET %s %s\n", key, value)
}

func handleSwap(client pb.KVServiceClient, key, value string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	req := &pb.SwapRequest{
		Key:   key,
		Value: value,
	}
	res, err := client.Swap(ctx, req)
	if err != nil {
		log.Fatalf("SWAP Error: %v", err)
		return
	}
	oldValue := "null"
	if res.Exists {
		oldValue = res.OldValue
	}
	fmt.Printf("SWAP %s %s\n", key, oldValue)
}

func handleScan(client pb.KVServiceClient, startKey, endKey string) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	req := &pb.ScanRequest{
		StartKey: startKey,
		EndKey:   endKey,
	}
	res, err := client.Scan(ctx, req)
	if err != nil {
		log.Fatalf("SCAN Error: %v", err)
		return
	}
	fmt.Printf("SCAN %s %s BEGIN\n", startKey, endKey)

	for {
		kv, err := res.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println("SCAN recv error:", err)
			return
		}
		fmt.Printf("  %s %s\n", kv.Key, kv.Value)
	}

	fmt.Println("SCAN END")
}

func handleDelete(client pb.KVServiceClient, key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	req := &pb.DeleteRequest{
		Key: key,
	}
	res, err := client.Delete(ctx, req)
	if err != nil {
		log.Fatalf("DELETE Error: %v", err)
		return
	}
	value := "not_found"
	if res.Exists {
		value = "found"
	}
	fmt.Printf("DELETE %s %s\n", key, value)
}

func main() {
	// Connect to the gRPC server
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if len(os.Args) < 2 {
		log.Fatalf("Usage: %s <listen_address>", os.Args[0])
	}

	listenAddr := os.Args[1]

	conn, err := grpc.NewClient(listenAddr, opts...)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewKVServiceClient(conn)

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		line := scanner.Text()
		args := strings.Fields(line)

		if len(args) == 0 {
			continue
		}
		cmd := args[0]
		switch cmd {
		case "PUT":
			handlePut(client, args[1], args[2])
		case "GET":
			handleGet(client, args[1])
		case "SWAP":
			handleSwap(client, args[1], args[2])
		case "SCAN":
			handleScan(client, args[1], args[2])
		case "DELETE":
			handleDelete(client, args[1])
		case "STOP":
			fmt.Print("STOP")
		}
	}
}
