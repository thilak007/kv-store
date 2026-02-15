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
	"google.golang.org/grpc/peer"
)

var (
	records = make(map[string]string)
	mu      sync.RWMutex
)

type server struct {
	pb.UnimplementedKVServiceServer
}

func (s *server) Put(ctx context.Context, in *pb.PutRequest) (*pb.PutResponse, error) {
	mu.Lock()
	defer mu.Unlock()

	p, ok := peer.FromContext(ctx)

	if ok {
		log.Printf("Received PUT from %s for key: %s and value: %s", p.Addr.String(), in.Key, in.Value)
	}

	key := in.Key
	value := in.Value
	_, exists := records[key]

	records[key] = value
	log.Printf("Sent PUT from %s for key: %s and value: %s. AlreadyExists: %t", p.Addr.String(), in.Key, in.Value, exists)

	return &pb.PutResponse{
		AlreadyExists: exists,
	}, nil
}

func (s *server) Swap(ctx context.Context, in *pb.SwapRequest) (*pb.SwapResponse, error) {

	mu.Lock()
	defer mu.Unlock()
	p, ok := peer.FromContext(ctx)
	if ok {
		log.Printf("Received SWAP from %s for key: %s and new value: %s", p.Addr.String(), in.Key, in.Value)
	}

	key := in.Key
	newvalue := in.Value
	oldvalue, exists := records[key]

	if exists {
		records[key] = newvalue
	}

	log.Printf("Sent SWAP from %s for key: %s. OldValue: %s changed to NewValue: %s", p.Addr.String(), in.Key, oldvalue, newvalue)

	return &pb.SwapResponse{
		OldValue: oldvalue,
		Exists:   exists,
	}, nil
}

func (s *server) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	mu.Lock()
	defer mu.Unlock()
	p, ok := peer.FromContext(ctx)
	if ok {
		log.Printf("Received GET from %s for key: %s", p.Addr.String(), in.Key)
	}

	key := in.Key
	value, exists := records[key]

	log.Printf("Sent GET from %s for key: %s. Got value: %s, exists: %t", p.Addr.String(), in.Key, value, exists)

	return &pb.GetResponse{
		Value:  value,
		Exists: exists,
	}, nil
}

func (s *server) Scan(in *pb.ScanRequest, stream pb.KVService_ScanServer) error {
	mu.Lock()
	p, ok := peer.FromContext(stream.Context())
	if ok {
		log.Printf("Received SCAN from %s from key: %s to key: %s", p.Addr.String(), in.StartKey, in.EndKey)
	}

	startKey := in.StartKey
	endKey := in.EndKey

	snapshot := make(map[string]string, len(records))
	var keys []string
	for k, v := range records {
		if k >= startKey && k <= endKey {
			snapshot[k] = v
			keys = append(keys, k)
		}
	}

	sort.Strings(keys)
	for _, key := range keys {
		ScanRes := &pb.ScanResponse{
			Key:   key,
			Value: snapshot[key],
		}
		log.Printf("Sent SCAN from %s from key: %s to key: %s. Key: %s, Value: %s", p.Addr.String(), in.StartKey, in.EndKey, key, snapshot[key])
		if err := stream.Send(ScanRes); err != nil {
			return err
		}
	}
	mu.Unlock()
	return nil
}

func (s *server) Delete(ctx context.Context, in *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	mu.Lock()
	defer mu.Unlock()
	p, ok := peer.FromContext(ctx)
	if ok {
		log.Printf("Received DELETE from %s for key: %s", p.Addr.String(), in.Key)
	}

	key := in.Key
	_, exists := records[key]
	if exists {
		delete(records, key)
	}

	log.Printf("Sent DELETE from %s for key: %s. Exists: %t", p.Addr.String(), in.Key, exists)

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

	s := grpc.NewServer(
		grpc.NumStreamWorkers(1),
		grpc.MaxConcurrentStreams(1),
	)
	pb.RegisterKVServiceServer(s, &server{})

	log.Printf("gRPC server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
