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

	// 1) Put a new key
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

	// 2) Get key value
	fmt.Println("Performing a get request...")

	GetReq := &pb.GetRequest{
		Key: "abc",
	}
	GetRes, err := client.Get(ctx, GetReq)
	if err != nil {
		log.Fatalf("Error during Create: %v", err)
	}
	fmt.Printf("Key's Value: %+v exists: %v\n", GetRes.Value, GetRes.Exists)

	// 3) Swap key value
	fmt.Println("Performing a swap request...")

	SwapReq := &pb.SwapRequest{
		Key:   "abc",
		Value: "234",
	}
	SwapRes, err := client.Swap(ctx, SwapReq)
	if err != nil {
		log.Fatalf("Error during Create: %v", err)
	}
	fmt.Printf("Key's Value: %+v exists: %v\n", SwapRes.OldValue, SwapRes.Exists)

	// 4) Put a new key
	fmt.Println("Putting key...")
	PutReq2 := &pb.PutRequest{
		Key:   "abd",
		Value: "124",
	}
	PutRes2, err := client.Put(ctx, PutReq2)
	if err != nil {
		log.Fatalf("Error during Create: %v", err)
	}
	log.Printf("Value for key %v put in Store. Key already existed: %v", PutReq2.Key, PutRes2.AlreadyExists)

	// 5) Scan key values
	fmt.Println("Performing a scan request...")

	ScanReq := &pb.ScanRequest{
		StartKey: "abc",
		EndKey:   "zzz",
	}
	ScanRes, err := client.Scan(ctx, ScanReq)
	if err != nil {
		log.Fatalf("Error during Create: %v", err)
	}
	fmt.Printf("Key's Value: %+v\n", ScanRes.Entries)

	// 6) Delete key values
	fmt.Println("Performing a delete request...")

	DeleteReq := &pb.DeleteRequest{
		Key: "abc",
	}
	DeleteRes, err := client.Delete(ctx, DeleteReq)
	if err != nil {
		log.Fatalf("Error during Create: %v", err)
	}
	fmt.Printf("Did key exist: %+v\n", DeleteRes.Exists)

	// 5) Scan key values
	fmt.Println("Performing a scan request...")

	ScanReq2 := &pb.ScanRequest{
		StartKey: "abc",
		EndKey:   "zzz",
	}
	ScanRes2, err := client.Scan(ctx, ScanReq2)
	if err != nil {
		log.Fatalf("Error during Create: %v", err)
	}
	fmt.Printf("Key's Value: %+v\n", ScanRes2.Entries)

}
