package main

import (
	"context"
	"fmt"
	pb "go_grpc/proto"
	"go_grpc/raft"
	raftpb "go_grpc/raft/proto"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"

	"github.com/cockroachdb/pebble"
)

var (
	records   = make(map[string]string)
	mu        sync.RWMutex
	requestID uint64
)

type ResponseMessage struct {
	Exists   bool
	OldValue string
	Err      error
}

type server struct {
	pb.UnimplementedKVServiceServer
	db            *pebble.DB
	bucketName    string
	replicaId     int32
	myPartitionId int32
	raftNode      *raft.RaftNode
	responseCh    chan ResponseMessage
	serverNodeId  string // "<replicaID.partitionID>"
}

type kvStateMachine struct {
	db         *pebble.DB
	bucketName string
	responseCh chan ResponseMessage
}

// This function is called from Apply goroutine thread
func (sm *kvStateMachine) Apply(rawCmd []byte, isLeader bool) error {
	cmd, err := raft.DeserializeCommand(rawCmd)
	if err != nil {
		return err
	}

	switch cmd.Op {
	case "SWAP":
		exists, oldValue, err := insertOrUpdateRecord(cmd.Key, cmd.Value, sm.db, sm.bucketName)
		log.Printf("[Inside Apply]: Completed SWAP for key: %s, oldValue: %s, newValue: %s, error: %v", cmd.Key, oldValue, cmd.Value, err)

		if isLeader {
			// Send response back to the waiting Swap handler
			sm.responseCh <- ResponseMessage{
				Exists:   exists,
				OldValue: oldValue,
				Err:      err,
			}
		}
		return err
	case "PUT":
		exists, oldValue, err := insertOrUpdateRecord(cmd.Key, cmd.Value, sm.db, sm.bucketName)
		log.Printf("[Inside Apply]: Completed PUT for key: %s, exists: %t, error: %v", cmd.Key, exists, err)

		if isLeader {
			// Send response back to the waiting Put handler
			sm.responseCh <- ResponseMessage{
				Exists:   exists,
				OldValue: oldValue,
				Err:      err,
			}
		}
		return err
	case "DELETE":
		exists, err := deleteRecord(cmd.Key, sm.db, sm.bucketName)
		log.Printf("[Inside Apply]: Completed DELETE for key: %s, exists: %t, error: %v", cmd.Key, exists, err)

		if isLeader {
			sm.responseCh <- ResponseMessage{
				Exists: exists,
				Err:    err,
			}
		}
		return err
	}

	return fmt.Errorf("Invalid command: %s", cmd.Op)
}

func insertOrUpdateRecord(key string, value string, db *pebble.DB, bucketName string) (bool, string, error) {

	oldvalue, exists := records[key]

	// Persist to PebbleDB
	err := db.Set(kvKey(bucketName, key), []byte(value), pebble.Sync)

	if err != nil {
		log.Printf("Failed to insert/update record to PebbleDB: %v", err)
		return exists, oldvalue, err
	}

	// Update in-memory map
	records[key] = value
	return exists, oldvalue, nil
}

func deleteRecord(key string, db *pebble.DB, bucketName string) (bool, error) {
	_, exists := records[key]

	// The deleteRecord is always called for an existing key, this is just an additional check.
	if !exists {
		return false, nil
	}

	// Persist to PebbleDB
	err := db.Delete(kvKey(bucketName, key), pebble.Sync)

	if err != nil {
		log.Printf("Failed to delete record from PebbleDB: %v", err)
		return exists, err
	}

	// Update in-memory map
	if exists {
		delete(records, key)
	}
	return exists, nil
}

func kvKey(bucketName string, key string) []byte {
	return []byte(bucketName + "/" + key)
}

func kvPrefix(bucketName string) []byte {
	return []byte(bucketName + "/")
}

func kvPrefixUpperBound(bucketName string) []byte {
	prefix := kvPrefix(bucketName)
	upper := make([]byte, len(prefix)+1)
	copy(upper, prefix)
	upper[len(prefix)] = 0xFF
	return upper
}

func (s *server) isLeader() (bool, string) {
	role, leaderId := s.raftNode.GetState()
	return role == raft.Leader, leaderId
}

/*
1) If follower or candidate, then return with leader ID
2a) If leader, propose for log replication - call raft library function.
2b) Call apply from raft
*/
func (s *server) Put(ctx context.Context, in *pb.PutRequest) (*pb.PutResponse, error) {
	reqID := atomic.AddUint64(&requestID, 1)
	mu.Lock()
	defer mu.Unlock()

	p, ok := peer.FromContext(ctx)

	if ok {
		log.Printf("[ReqID: %s--%d] Received PUT from %s for key: %s and value: %s", s.serverNodeId, reqID, p.Addr.String(), in.Key, in.Value)
	}

	isLeader, leaderId := s.isLeader()
	if !isLeader {
		log.Printf("[ReqID: %s--%d] Rejecting PUT from %s. Not the leader; Redirecting to leader: %s", s.serverNodeId, reqID, p.Addr.String(), leaderId)
		return &pb.PutResponse{
			LeaderId: leaderId,
		}, nil
	}

	key := in.Key
	value := in.Value

	var cmd *raft.Command = raft.NewCommand("PUT", key, value)

	s.raftNode.ProposeCmd(cmd)

	// Wait for the Apply thread to complete and send the response through the channel
	resp := <-s.responseCh

	if resp.Err != nil {
		log.Printf("[ReqID: %d] Failed to persist PUT to PebbleDB: %v", reqID, resp.Err)
		return nil, resp.Err
	}

	log.Printf("[ReqID: %s--%d] Sent PUT from client %s for key: %s and value: %s. AlreadyExists: %t", s.serverNodeId,
		reqID, p.Addr.String(), in.Key, in.Value, resp.Exists)

	return &pb.PutResponse{
		AlreadyExists: resp.Exists,
		LeaderId:      leaderId,
	}, nil
}

func (s *server) Swap(ctx context.Context, in *pb.SwapRequest) (*pb.SwapResponse, error) {
	reqID := atomic.AddUint64(&requestID, 1)
	mu.Lock()
	defer mu.Unlock()

	p, ok := peer.FromContext(ctx)
	if ok {
		log.Printf("[ReqID: %s--%d] Received SWAP from %s for key: %s and new value: %s", s.serverNodeId, reqID, p.Addr.String(), in.Key, in.Value)
	}

	isLeader, leaderId := s.isLeader()
	if !isLeader {
		log.Printf("[ReqID: %s--%d] Rejecting SWAP from %s. Not the leader; Redirecting to leader: %s", s.serverNodeId, reqID, p.Addr.String(), leaderId)
		return &pb.SwapResponse{
			LeaderId: leaderId,
		}, nil
	}

	key := in.Key
	value := in.Value

	var cmd *raft.Command = raft.NewCommand("SWAP", key, value)

	s.raftNode.ProposeCmd(cmd)

	// Wait for the Apply thread to complete and send the response through the channel
	resp := <-s.responseCh

	if resp.Err != nil {
		log.Printf("[ReqID: %d] Failed to persist SWAP to PebbleDB: %v", reqID, resp.Err)
		return nil, resp.Err
	}

	log.Printf("[ReqID: %s--%d] Sent SWAP from client %s for key: %s. OldValue: %s changed to NewValue: %s", s.serverNodeId,
		reqID, p.Addr.String(), in.Key, resp.OldValue, value)

	return &pb.SwapResponse{
		OldValue: resp.OldValue,
		Exists:   resp.Exists,
		LeaderId: leaderId,
	}, nil
}

func (s *server) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	reqID := atomic.AddUint64(&requestID, 1)
	mu.Lock()
	defer mu.Unlock()
	p, ok := peer.FromContext(ctx)
	if ok {
		log.Printf("[ReqID: %s--%d] Received GET from %s for key: %s", s.serverNodeId, reqID, p.Addr.String(), in.Key)
	}

	// Check if Leader
	isLeader, leaderId := s.isLeader()
	if !isLeader {
		log.Printf("[ReqID: %s--%d] Rejecting GET from %s. Not the leader; Redirecting to leader: %s", s.serverNodeId, reqID, p.Addr.String(), leaderId)
		return &pb.GetResponse{
			LeaderId: leaderId,
		}, nil
	}

	key := in.Key
	value, exists := records[key]

	log.Printf("[ReqID: %s--%d] Sent GET from %s for key: %s. Got value: %s, exists: %t", s.serverNodeId, reqID, p.Addr.String(), in.Key, value, exists)

	return &pb.GetResponse{
		Value:    value,
		Exists:   exists,
		LeaderId: leaderId,
	}, nil
}

func (s *server) Scan(in *pb.ScanRequest, stream pb.KVService_ScanServer) error {
	reqID := atomic.AddUint64(&requestID, 1)
	p, ok := peer.FromContext(stream.Context())
	if ok {
		log.Printf("[ReqID: %s--%d] Received SCAN from %s from key: %s to key: %s", s.serverNodeId, reqID, p.Addr.String(), in.StartKey, in.EndKey)
	}

	// Check if Leader
	isLeader, leaderId := s.isLeader()
	if !isLeader {
		log.Printf("[ReqID: %s--%d] Rejecting SCAN from %s. Not the leader; Redirecting to leader: %s", s.serverNodeId, reqID, p.Addr.String(), leaderId)
		stream.Send(&pb.ScanResponse{LeaderId: leaderId})
		return nil
	}

	startKey := in.StartKey
	endKey := in.EndKey

	var snapshot map[string]string
	var keys []string
	func() {
		mu.Lock()
		defer mu.Unlock()

		snapshot = make(map[string]string, len(records))
		for k, v := range records {
			if k >= startKey && k <= endKey {
				snapshot[k] = v
				keys = append(keys, k)
			}
		}
	}()

	sort.Strings(keys)
	for _, key := range keys {
		ScanRes := &pb.ScanResponse{
			Key:      key,
			Value:    snapshot[key],
			LeaderId: leaderId,
		}
		log.Printf("[ReqID: %s--%d] Sent SCAN from %s from key: %s to key: %s. Key: %s, Value: %s", s.serverNodeId, reqID, p.Addr.String(), in.StartKey, in.EndKey, key, snapshot[key])
		if err := stream.Send(ScanRes); err != nil {
			return err
		}
	}
	return nil
}

func (s *server) Delete(ctx context.Context, in *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	reqID := atomic.AddUint64(&requestID, 1)
	mu.Lock()
	defer mu.Unlock()

	p, ok := peer.FromContext(ctx)
	if ok {
		log.Printf("[ReqID: %s--%d] Received DELETE from %s for key: %s", s.serverNodeId, reqID, p.Addr.String(), in.Key)
	}

	// Check if Leader
	isLeader, leaderId := s.isLeader()
	if !isLeader {
		log.Printf("[ReqID: %s--%d] Rejecting DELETE from %s. Not the leader; Redirecting to leader: %s", s.serverNodeId, reqID, p.Addr.String(), leaderId)
		return &pb.DeleteResponse{
			LeaderId: leaderId,
		}, nil
	}

	key := in.Key

	_, exists := records[key]

	if !exists {
		// Don't have to replicate the log as the deletion of a non-existent key doesn't change state.
		return &pb.DeleteResponse{
			Exists:   exists,
			LeaderId: leaderId,
		}, nil
	}

	var cmd *raft.Command = raft.NewCommand("DELETE", key, "")

	s.raftNode.ProposeCmd(cmd)

	// Wait for the Apply thread to complete and send the response through the channel
	resp := <-s.responseCh

	if resp.Err != nil {
		log.Printf("[ReqID: %d] Failed to persist DELETE to PebbleDB: %v", reqID, resp.Err)
		return nil, resp.Err
	}

	log.Printf("[ReqID: %s--%d] Sent DELETE from %s for key: %s. Exists: %t", s.serverNodeId, reqID, p.Addr.String(), in.Key, resp.Exists)

	return &pb.DeleteResponse{
		Exists:   resp.Exists,
		LeaderId: leaderId,
	}, nil
}

func Register(ManagerAddr string, serverId int32) int32 {
	attempt := 1
	retryDelay := time.Duration(2*attempt) * time.Second

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	for {
		attempt++
		log.Printf("Attempting to register with Manager at %s (Attempt %d)", ManagerAddr, attempt)

		managerConn, err := grpc.NewClient(ManagerAddr, opts...)
		if err != nil {
			log.Fatalf("Failed to connect: %v. Retrying...", err)
			time.Sleep(retryDelay)
			continue
		}

		managerClient := pb.NewClusterManagerClient(managerConn)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)

		req := &pb.RegisterRequest{ServerId: serverId}
		res, err := managerClient.RegisterServer(ctx, req)
		cancel()
		managerConn.Close()

		if err != nil {
			log.Printf("Register Server Error: %v. Retrying...", err)
			time.Sleep(retryDelay)
			continue
		}
		// If unable to register, log the error and retry registration after a delay
		if !res.Success {
			log.Fatalf("Manager rejected registration for server ID %d.", serverId)
		}

		log.Printf("Successfully registered with Manager. Assigned Partition ID: %d", res.PartitionId)
		return res.PartitionId
	}
}

func createListener(listenAddr string, serviceName string) net.Listener {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("Failed to create listener for %s service at %s: %v", serviceName, listenAddr, err)
	}
	log.Printf("Created listener on %s\n for %s service", listenAddr, serviceName)
	return lis
}

func parseAddressList(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "none" {
		return nil
	}

	parts := strings.Split(raw, ",")
	addrs := make([]string, 0, len(parts))
	for _, p := range parts {
		addr := strings.TrimSpace(p)
		if addr != "" {
			addrs = append(addrs, addr)
		}
	}
	return addrs
}

func buildPeerNodeIDs(replicaId int, partitionId int32, peerCount int) []string {
	totalReplicas := peerCount + 1
	peerIDs := make([]string, 0, peerCount)
	for rid := 0; rid < totalReplicas; rid++ {
		if rid == replicaId {
			continue
		}
		peerIDs = append(peerIDs, fmt.Sprintf("%d.%d", rid, partitionId))
	}
	return peerIDs
}

func buildPeerClients(peerIDs []string, peerAddrs []string) map[string]raftpb.RaftClient {
	if len(peerIDs) != len(peerAddrs) {
		log.Fatalf("peer ID count (%d) does not match peer address count (%d)", len(peerIDs), len(peerAddrs))
	}

	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	peerClients := make(map[string]raftpb.RaftClient, len(peerIDs))
	for i, peerID := range peerIDs {
		conn, err := grpc.NewClient(peerAddrs[i], opts...)
		if err != nil {
			log.Fatalf("failed to connect to peer %s at %s: %v", peerID, peerAddrs[i], err)
		}
		peerClients[peerID] = raftpb.NewRaftClient(conn)
		log.Printf("Connected peer client %s -> %s", peerID, peerAddrs[i])
	}
	return peerClients
}

/*
p2 call:
./bin/server {{manager}} {{api_ip}}:{{api_port}} {{id}} {{backer_path}}

p3 call:
./yourserver --partition_id 0 --replica_id 0 --manager_addrs 1.2.3.4:3666,8.7.6.5:3667,12.11.10.9:3668 --api_listen 0.0.0.0:3777 --p2p_listen 0.0.0.0:3707 --peer_addrs 5.6.7.8:3708,9.10.11.12:3709 --backer_path ./backer.s0.0
*/
func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if len(os.Args) < 8 {
		log.Fatalf("Usage: %s <partition_id> <replica_id> <manager_addrs> <api_listen_addrs> <p2p_listen_addrs> <peer_addrs> <storage_dir>", os.Args[0])
	}

	fmt.Println("Booting up the server for the KV store...")

	partitionId, _ := strconv.ParseInt(os.Args[1], 10, 32)
	replicaId, _ := strconv.ParseInt(os.Args[2], 10, 32)

	// Args
	// ManagerAddr := os.Args[3] // Address of the manager: chose the index 0.
	apiListenAddr := os.Args[4]
	raftListenAddr := os.Args[5]
	peerAddrsRaw := os.Args[6]
	peerAddrs := parseAddressList(peerAddrsRaw)

	storageDir := os.Args[7] // Path to the directory where PebbleDB will store its data files
	dbPath := filepath.Join(storageDir, "kvstore.db")
	bucketName := "kvstore_bucket"
	// PebbleDB bucket for Raft log and it's state persistence
	raftStateBucket := "raft_log_bucket"

	log.Printf("API Listen addr is %s, Peer Listern Addr is %s", apiListenAddr, raftListenAddr)

	// Register with Manager to get partition ID
	// partitionId := Register(ManagerAddr, int32(serverId)) // Verify that partitionId is same as one being initialized with like 0 as I'm using it to define nodeId.

	// Ensure the storage directory exists
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		log.Fatalf("Failed to create storage directory: %v", err)
	}

	// Open or create the PebbleDB database
	db, err := pebble.Open(dbPath, &pebble.Options{})
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Load existing data from PebbleDB into the in-memory map
	log.Println("Loading data from persistent storage...")
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: kvPrefix(bucketName),
		UpperBound: kvPrefixUpperBound(bucketName),
	})
	if err != nil {
		log.Fatalf("Failed to create Pebble iterator: %v", err)
	}
	for iter.First(); iter.Valid(); iter.Next() {
		trimmedKey := strings.TrimPrefix(string(iter.Key()), string(kvPrefix(bucketName)))
		records[trimmedKey] = string(iter.Value())
	}
	err = iter.Close()

	if err != nil {
		log.Fatalf("Failed to load data from PebbleDB: %v", err)
	}
	log.Printf("Loaded %d key-value pairs from persistent storage", len(records))

	// Create a Raft node and initialize the KV state machine instance.
	responseCh := make(chan ResponseMessage)
	stateMachine := &kvStateMachine{
		db:         db,
		bucketName: bucketName,
		responseCh: responseCh,
	}
	nodeID := fmt.Sprintf("%d.%d", replicaId, partitionId)
	peerNodeIDs := buildPeerNodeIDs(int(replicaId), int32(partitionId), len(peerAddrs))
	peerClients := buildPeerClients(peerNodeIDs, peerAddrs)
	raftNode := raft.NewRaftNode(nodeID, peerNodeIDs, stateMachine, db, raftStateBucket, peerClients)

	log.Printf("Initialized raft node: %s", raftNode.String())
	// Load persisted Raft state (currentTerm, votedFor, log entries) from PebbleDB
	if err := raftNode.LoadState(); err != nil {
		log.Fatalf("failed to load raft state: %v", err)
	}
	log.Printf("after loading persisted state, raft node: %s", raftNode.String())

	// Start listening for KV Store RPC requests and Raft RPCs
	lis := createListener(apiListenAddr, "Kvstore APIs")
	raftlistener := createListener(raftListenAddr, "Raft")

	s := grpc.NewServer()
	pb.RegisterKVServiceServer(s, &server{
		db:            db,
		bucketName:    bucketName,
		myPartitionId: int32(partitionId),
		replicaId:     int32(replicaId),
		serverNodeId:  nodeID,
		raftNode:      raftNode,
		responseCh:    responseCh,
	})

	raftGRPCServer := grpc.NewServer()
	raftpb.RegisterRaftServer(raftGRPCServer, raft.NewRaftService(raftNode))

	go func() {
		log.Printf("Raft gRPC server listening at %v", raftlistener.Addr())
		if err := raftGRPCServer.Serve(raftlistener); err != nil {
			log.Fatalf("failed to serve raft gRPC server: %v", err)
		}
	}()

	raftNode.Start()

	log.Printf("gRPC server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
