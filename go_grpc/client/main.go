package main

import (
	"context"
	"fmt"
	pb "go_grpc/proto"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Connect to the gRPC server
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.NewClient("localhost:8080", opts...)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewKVServiceClient(conn)

	// Timeout for context
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()

	// Example 1: Create a new person
	fmt.Println("Putting key...")
	PutReq := &pb.PutRequest{
		Key:   "abc",
		Value: "124",
	}
	PutRes, err := client.Put(ctx, PutReq)
	if err != nil {
		log.Fatalf("Error during Create: %v", err)
	}
	log.Printf("Value for key %v put in Store. Key already existed: %v", PutReq.Key, PutRes.AlreadyExists)

	fmt.Println("Performing a get request...")

	GetReq := &pb.GetRequest{
		Key: "abc",
	}
	GetRes, err := client.Get(ctx, GetReq)
	if err != nil {
		log.Fatalf("Error during Create: %v", err)
	}
	fmt.Printf("Key's Value: %+v exists: %v\n", GetRes.Value, GetRes.Exists)
}
