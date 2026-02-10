package main

import (
	"context"
	"fmt"
	pb "go_grpc/proto"
	"log"
	"net"
	"sort"
	"sync"

	"google.golang.org/grpc"
)

var (
	records = make(map[string]string)
	mu      sync.RWMutex
)

type server struct {
	pb.UnimplementedKVServiceServer
}

func (s *server) Put(ctx context.Context, in *pb.PutRequest) (*pb.PutResponse, error) {
	log.Printf("Received PUT request for key: %s and value: %s", in.Key, in.Value)

	mu.Lock()
	defer mu.Unlock()

	key := in.Key
	value := in.Value
	_, exists := records[key]

	records[key] = value

	return &pb.PutResponse{
		AlreadyExists: exists,
	}, nil
}

func (s *server) Swap(ctx context.Context, in *pb.SwapRequest) (*pb.SwapResponse, error) {
	log.Printf("Received SWAP request for key: %s and new value: %s", in.Key, in.Value)

	mu.Lock()
	defer mu.Unlock()

	key := in.Key
	newvalue := in.Value
	oldvalue, exists := records[key]

	if exists {
		records[key] = newvalue
	}

	return &pb.SwapResponse{
		OldValue: oldvalue,
		Exists:   exists,
	}, nil
}

func (s *server) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	log.Printf("Received GET request for key: %s", in.Key)

	mu.Lock()
	defer mu.Unlock()

	key := in.Key
	value, exists := records[key]

	return &pb.GetResponse{
		Value:  value,
		Exists: exists,
	}, nil
}

func (s *server) Scan(in *pb.ScanRequest, stream pb.KVService_ScanServer) error {
	log.Printf("Received SCAN request from key: %s to key: %s", in.StartKey, in.EndKey)

	startKey := in.StartKey
	endKey := in.EndKey

	mu.Lock()
	snapshot := make(map[string]string, len(records))
	var keys []string
	for k, v := range records {
		if k >= startKey && k <= endKey {
			snapshot[k] = v
			keys = append(keys, k)
		}
	}
	mu.Unlock()

	sort.Strings(keys)
	for _, key := range keys {
		ScanRes := &pb.ScanResponse{
			Key:   key,
			Value: snapshot[key],
		}
		if err := stream.Send(ScanRes); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) Delete(ctx context.Context, in *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	log.Printf("Received DELETE request for key: %s", in.Key)

	mu.Lock()
	defer mu.Unlock()

	key := in.Key
	_, exists := records[key]
	if exists {
		delete(records, key)
	}

	return &pb.DeleteResponse{
		Exists: exists,
	}, nil
}

func main() {
	fmt.Println("Inside main ----->")
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterKVServiceServer(s, &server{})

	log.Printf("gRPC server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
