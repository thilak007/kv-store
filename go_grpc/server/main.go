package main

import (
	"context"
	"fmt"
	pb "go_grpc/proto"
	"go_grpc/raft"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"

	bolt "go.etcd.io/bbolt"
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
	db            *bolt.DB
	bucketName    string
	myPartitionId int32
	raftNode      *raft.RaftNode
	responseCh    chan ResponseMessage
}

type kvStateMachine struct {
	db         *bolt.DB
	bucketName string
	responseCh chan ResponseMessage
}

// This function is called from Apply goroutine thread
func (sm *kvStateMachine) Apply(rawCmd []byte) error {
	cmd, err := raft.DeserializeCommand(rawCmd)
	if err != nil {
		return err
	}

	switch cmd.Op {
	case "SWAP":
	case "PUT":
		exists, oldValue, err := insertOrUpdateRecord(cmd.Key, cmd.Value, sm.db, sm.bucketName)
		if cmd.Op == "SWAP" {
			// message for SWAP
			log.Printf("[Inside Apply]: Completed SWAP for key: %s, oldValue: %s, newValue: %s, error: %v", cmd.Key, oldValue, cmd.Value, err)
		} else {
			log.Printf("[Inside Apply]: Completed PUT for key: %s, exists: %t, error: %v", cmd.Key, exists, err)
		}
		// Send response back to the waiting Put handler
		sm.responseCh <- ResponseMessage{
			Exists:   exists,
			OldValue: oldValue,
			Err:      err,
		}
		return err
	case "DELETE":
		exists, err := deleteRecord(cmd.Key, sm.db, sm.bucketName)
		log.Printf("[Inside Apply]: Completed DELETE for key: %s, exists: %t, error: %v", cmd.Key, exists, err)
		sm.responseCh <- ResponseMessage{
			Exists: exists,
			Err:    err,
		}
		return err
	}

	return fmt.Errorf("Invalid command: %s", cmd.Op)
}

func insertOrUpdateRecord(key string, value string, db *bolt.DB, bucketName string) (bool, string, error) {

	oldvalue, exists := records[key]

	// Persist to BoltDB
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		return b.Put([]byte(key), []byte(value))
	})

	if err != nil {
		log.Printf("Failed to insert/update record to BoltDB: %v", err)
		return exists, oldvalue, err
	}

	// Update in-memory map
	records[key] = value
	return exists, oldvalue, nil
}

func deleteRecord(key string, db *bolt.DB, bucketName string) (bool, error) {
	_, exists := records[key]

	// The deleteRecord is always called for an existing key, this is just an additional check.
	if !exists {
		return false, nil
	}

	// Persist to BoltDB
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		return b.Delete([]byte(key))
	})

	if err != nil {
		log.Printf("Failed to delete record from BoltDB: %v", err)
		return exists, err
	}

	// Update in-memory map
	if exists {
		delete(records, key)
	}
	return exists, nil
}

func (s *server) Put(ctx context.Context, in *pb.PutRequest) (*pb.PutResponse, error) {
	reqID := atomic.AddUint64(&requestID, 1)
	mu.Lock()
	defer mu.Unlock()

	p, ok := peer.FromContext(ctx)

	if ok {
		log.Printf("[ReqID: %d.%d] Received PUT from %s for key: %s and value: %s", s.myPartitionId, reqID, p.Addr.String(), in.Key, in.Value)
	}

	key := in.Key
	value := in.Value

	/*
		Todo:
		- If follower, then return with leader ID
		- If leader, propose for log replication - call raft library function.
		- Call apply from raft
	*/

	var cmd *raft.Command = raft.NewCommand("PUT", key, value)

	s.raftNode.ProposeCmd(cmd)

	// Wait for the Apply thread to complete and send the response through the channel
	resp := <-s.responseCh

	if resp.Err != nil {
		log.Printf("[ReqID: %d] Failed to persist PUT to BoltDB: %v", reqID, resp.Err)
		return nil, resp.Err
	}

	log.Printf("[ReqID: %d.%d] Sent PUT from client %s for key: %s and value: %s. AlreadyExists: %t", s.myPartitionId,
		reqID, p.Addr.String(), in.Key, in.Value, resp.Exists)

	return &pb.PutResponse{
		AlreadyExists: resp.Exists,
	}, nil
}

func (s *server) Swap(ctx context.Context, in *pb.SwapRequest) (*pb.SwapResponse, error) {
	reqID := atomic.AddUint64(&requestID, 1)
	mu.Lock()
	defer mu.Unlock()

	p, ok := peer.FromContext(ctx)
	if ok {
		log.Printf("[ReqID: %d.%d] Received SWAP from %s for key: %s and new value: %s", s.myPartitionId, reqID, p.Addr.String(), in.Key, in.Value)
	}

	key := in.Key
	value := in.Value

	var cmd *raft.Command = raft.NewCommand("SWAP", key, value)

	s.raftNode.ProposeCmd(cmd)

	// Wait for the Apply thread to complete and send the response through the channel
	resp := <-s.responseCh

	if resp.Err != nil {
		log.Printf("[ReqID: %d] Failed to persist SWAP to BoltDB: %v", reqID, resp.Err)
		return nil, resp.Err
	}

	log.Printf("[ReqID: %d.%d] Sent SWAP from client %s for key: %s. OldValue: %s changed to NewValue: %s", s.myPartitionId,
		reqID, p.Addr.String(), in.Key, resp.OldValue, value)

	return &pb.SwapResponse{
		OldValue: resp.OldValue,
		Exists:   resp.Exists,
	}, nil
}

func (s *server) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	reqID := atomic.AddUint64(&requestID, 1)
	mu.Lock()
	defer mu.Unlock()
	p, ok := peer.FromContext(ctx)
	if ok {
		log.Printf("[ReqID: %d.%d] Received GET from %s for key: %s", s.myPartitionId, reqID, p.Addr.String(), in.Key)
	}

	key := in.Key
	value, exists := records[key]

	log.Printf("[ReqID: %d.%d] Sent GET from %s for key: %s. Got value: %s, exists: %t", s.myPartitionId, reqID, p.Addr.String(), in.Key, value, exists)

	return &pb.GetResponse{
		Value:  value,
		Exists: exists,
	}, nil
}

func (s *server) Scan(in *pb.ScanRequest, stream pb.KVService_ScanServer) error {
	reqID := atomic.AddUint64(&requestID, 1)
	p, ok := peer.FromContext(stream.Context())
	if ok {
		log.Printf("[ReqID: %d.%d] Received SCAN from %s from key: %s to key: %s", s.myPartitionId, reqID, p.Addr.String(), in.StartKey, in.EndKey)
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
			Key:   key,
			Value: snapshot[key],
		}
		log.Printf("[ReqID: %d.%d] Sent SCAN from %s from key: %s to key: %s. Key: %s, Value: %s", s.myPartitionId, reqID, p.Addr.String(), in.StartKey, in.EndKey, key, snapshot[key])
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
		log.Printf("[ReqID: %d.%d] Received DELETE from %s for key: %s", s.myPartitionId, reqID, p.Addr.String(), in.Key)
	}

	key := in.Key

	_, exists := records[key]

	if !exists {
		// Don't have to replicate the log as the deletion of a non-existent key doesn't change state.
		return &pb.DeleteResponse{
			Exists: exists,
		}, nil
	}

	var cmd *raft.Command = raft.NewCommand("DELETE", key, "")

	s.raftNode.ProposeCmd(cmd)

	// Wait for the Apply thread to complete and send the response through the channel
	resp := <-s.responseCh

	if resp.Err != nil {
		log.Printf("[ReqID: %d] Failed to persist DELETE to BoltDB: %v", reqID, resp.Err)
		return nil, resp.Err
	}

	log.Printf("[ReqID: %d.%d] Sent DELETE from %s for key: %s. Exists: %t", s.myPartitionId, reqID, p.Addr.String(), in.Key, resp.Exists)

	return &pb.DeleteResponse{
		Exists: resp.Exists,
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

func main() {
	if len(os.Args) < 5 {
		log.Fatalf("Usage: %s <manager_address> <listen_address> <server_id> <storage_dir>", os.Args[0])
	}

	fmt.Println("Inside main ----->")

	// Args
	ManagerAddr := os.Args[1]
	listenAddr := os.Args[2]
	log.Printf("listening address: %s\n", listenAddr)
	serverIdStr := os.Args[3]
	storageDir := os.Args[4] // Path to the directory where BoltDB will store its data files
	dbPath := filepath.Join(storageDir, "kvstore.db")
	bucketName := "kvstore_bucket"

	// Register with Manager to get partition ID
	serverId, err := strconv.ParseInt(serverIdStr, 10, 32)
	if err != nil {
		log.Fatalf("Invalid server ID: %v", err)
	}
	partitionId := Register(ManagerAddr, int32(serverId))

	// Ensure the storage directory exists
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		log.Fatalf("Failed to create storage directory: %v", err)
	}

	// Open or create the BoltDB database
	db, err := bolt.Open(dbPath, 0600, nil)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create a bucket for our key-value pairs if it doesn't exist
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
	if err != nil {
		log.Fatalf("Failed to create bucket: %v", err)
	}

	// Load existing data from BoltDB into the in-memory map
	log.Println("Loading data from persistent storage...")
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		return b.ForEach(func(k, v []byte) error {
			records[string(k)] = string(v)
			return nil
		})
	})

	if err != nil {
		log.Fatalf("Failed to load data from BoltDB: %v", err)
	}
	log.Printf("Loaded %d key-value pairs from persistent storage", len(records))

	fmt.Printf("Server listening on %s\n", listenAddr)
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// Create a Raft node and initialize the KV state machine instance.
	responseCh := make(chan ResponseMessage)
	stateMachine := &kvStateMachine{
		db:         db,
		bucketName: bucketName,
		responseCh: responseCh,
	}
	nodeID := fmt.Sprintf("%d.%d", serverId, partitionId)
	raftNode := raft.NewRaftNode(nodeID, []string{}, stateMachine, db, "raft_log")
	log.Printf("Initialized raft node: %s", raftNode.String())

	s := grpc.NewServer()
	pb.RegisterKVServiceServer(s, &server{
		db:            db,
		bucketName:    bucketName,
		myPartitionId: partitionId,
		raftNode:      raftNode,
		responseCh:    responseCh,
	})

	log.Printf("gRPC server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
