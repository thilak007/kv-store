package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
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
	partitionMap  = make(map[int32][]string)
	requestID     uint64
	replicaID     int
	manListen     string
	p2pListen     string
	peerAddrs     string
	serverRF      int
	backerPath    string
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
		log.Printf("[ReqID: %d] Registering Server %d", reqID, in.ServerId)
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

	// Convert map[int32][]string -> map[int32]*PartitionEntry
	partitionEntryMap := make(map[int32]*pb.PartitionEntry)
	for pid, addrs := range partitionMap {
		partitionEntryMap[pid] = &pb.PartitionEntry{
			Addresses: addrs,
		}
	}

	return &pb.PartitionMapResponse{
		NumPartitions: numPartitions,
		PartitionMap:  partitionEntryMap,
		LeaderId:      fmt.Sprintf("0.0"),
	}, nil
}

func main() {
	var serverAddrsStr string
	flag.IntVar(&replicaID, "replica_id", 0, "Manager replica ID")
	flag.StringVar(&manListen, "man_listen", "", "Manager management API listen address")
	flag.StringVar(&p2pListen, "p2p_listen", "", "Manager P2P listen address (future)")
	flag.StringVar(&peerAddrs, "peer_addrs", "", "Comma-separated peer manager addresses (future)")
	flag.IntVar(&serverRF, "server_rf", 3, "Server replication factor")
	flag.StringVar(&serverAddrsStr, "server_addrs", "", "Comma-separated server addresses")
	flag.StringVar(&backerPath, "backer_path", "", "Backer file path (future)")
	flag.Parse()

	if manListen == "" || serverAddrsStr == "" {
		flag.Usage()
		log.Fatalf("Error: --man_listen and --server_addrs are required")
	}

	fmt.Println("Manager service started. Awaiting server registrations...")

	serverAddrs = strings.Split(serverAddrsStr, ",")
	numPartitions = int32(len(serverAddrs)) / int32(serverRF)

	for i := int32(0); i < numPartitions; i++ {
		partitionMap[i] = serverAddrs[i*int32(serverRF) : (i+1)*int32(serverRF)]
	}

	log.Printf("Configured %d partitions (RF=%d):", numPartitions, serverRF)
	for id, addr := range partitionMap {
		log.Printf("  Partition %d → %s", id, addr)
	}

	// Start gRPC server for ClusterManager
	lis, err := net.Listen("tcp", manListen)
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
