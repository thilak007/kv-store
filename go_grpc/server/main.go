package main

import (
	"context"
	"fmt"
	pb "go_grpc/proto"
	"log"
	"net"

	"google.golang.org/grpc"
)

var (
	records = make(map[string]string)
)

type server struct {
	pb.UnimplementedKVServiceServer
}

func (s *server) Put(ctx context.Context, in *pb.PutRequest) (*pb.PutResponse, error) {
	log.Printf("Received PUT request for key: %s and value: %s", in.Key, in.Value)


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


	key := in.Key
	value, exists := records[key]

	return &pb.GetResponse{
		Value:  value,
		Exists: exists,
	}, nil
}

func (s *server) Scan(ctx context.Context, in *pb.ScanRequest) (*pb.ScanResponse, error) {
	log.Printf("Received SCAN request from key: %s to key: %s", in.StartKey, in.EndKey)


	startKey := in.StartKey
	endKey := in.EndKey
	var entries []*pb.KeyValue

	for key, value := range records {
		if key >= startKey && key <= endKey {
			entries = append(entries, &pb.KeyValue{
				Key:   key,
				Value: value,
			})
		}
	}

	return &pb.ScanResponse{
		Entries: entries,
	}, nil
}

func (s *server) Delete(ctx context.Context, in *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	log.Printf("Received DELETE request for key: %s", in.Key)


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
