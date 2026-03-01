package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	pb "go_grpc/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
)

var (
	numPartitions int32
	serverAddrs   []string
	mu            sync.RWMutex
	partitionMap  = make(map[int32]string)
	requestID     uint64
)

type manager struct {
	pb.UnimplementedClusterManagerServer
}

func (m *manager) RegisterServer(ctx context.Context, in *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	reqID := atomic.AddUint64(&requestID, 1)
	mu.RLock()
	defer mu.RUnlock()

	p, ok := peer.FromContext(ctx)

	if ok {
		log.Printf("[ReqID: %d] Registering Server %s", reqID, in.ServerId)
	}

	partitionId := in.ServerId
	success := true

	if in.ServerId < 0 || in.ServerId >= numPartitions {
		log.Printf("[ReqID: %d] Invalid Server ID %d from %s", reqID, in.ServerId, p.Addr.String())
		success = false
		partitionId = -1
	}

	return &pb.RegisterResponse{
		PartitionId: partitionId,
		Success:     success,
	}, nil
}

func (m *manager) GetPartitionMap(ctx context.Context, in *pb.PartitionMapRequest) (*pb.PartitionMapResponse, error) {
	reqID := atomic.AddUint64(&requestID, 1)
	mu.RLock()
	defer mu.RUnlock()

	_, ok := peer.FromContext(ctx)

	if ok {
		log.Printf("[ReqID: %d] Returning PartitionMap to client(s)", reqID)
	}

	return &pb.PartitionMapResponse{
		NumPartitions: numPartitions,
		PartitionMap:  partitionMap,
	}, nil
}

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("Usage: %s <listen_address> <server_addrs>", os.Args[0])
	}
	fmt.Println("Manager service started. Awaiting server registrations...")

	// Args
	listenAddr := os.Args[1]
	serverAddrStr := os.Args[2]
	serverAddrs = strings.Split(serverAddrStr, ",")
	for i, addr := range serverAddrs {
		partitionMap[int32(i)] = addr
	}
	numPartitions = int32(len(serverAddrs))

	log.Printf("Configured %d partitions:", numPartitions)
	for id, addr := range partitionMap {
		log.Printf("  Partition %d → %s", id, addr)
	}

	// Start gRPC server for ClusterManager
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterClusterManagerServer(s, &manager{})

	log.Printf("Cluster Manager listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}

}
