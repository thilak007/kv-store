package main

import (
	"context"
	"fmt"
	pb "go_grpc/proto"
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

type server struct {
	pb.UnimplementedKVServiceServer
	db            *bolt.DB
	bucketName    string
	myPartitionId int32
}

func (s *server) Put(ctx context.Context, in *pb.PutRequest) (*pb.PutResponse, error) {
	reqID := atomic.AddUint64(&requestID, 1)
	mu.Lock()
	defer mu.Unlock()

	p, ok := peer.FromContext(ctx)

	if ok {
		log.Printf("[ReqID: %d] Received PUT from %s for key: %s and value: %s", reqID, p.Addr.String(), in.Key, in.Value)
	}

	key := in.Key
	value := in.Value
	_, exists := records[key]

	// Persist to BoltDB
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.bucketName))
		return b.Put([]byte(key), []byte(value))
	})

	if err != nil {
		log.Printf("[ReqID: %d] Failed to persist PUT to BoltDB: %v", reqID, err)
		return nil, err
	}

	// Update in-memory map
	records[key] = value

	log.Printf("[ReqID: %d] Sent PUT from %s for key: %s and value: %s. AlreadyExists: %t",
		reqID, p.Addr.String(), in.Key, in.Value, exists)

	return &pb.PutResponse{
		AlreadyExists: exists,
	}, nil
}

func (s *server) Swap(ctx context.Context, in *pb.SwapRequest) (*pb.SwapResponse, error) {
	reqID := atomic.AddUint64(&requestID, 1)
	mu.Lock()
	defer mu.Unlock()
	p, ok := peer.FromContext(ctx)
	if ok {
		log.Printf("[ReqID: %d] Received SWAP from %s for key: %s and new value: %s", reqID, p.Addr.String(), in.Key, in.Value)
	}

	key := in.Key
	newvalue := in.Value
	oldvalue, exists := records[key]

	// Persist to BoltDB
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.bucketName))
		return b.Put([]byte(key), []byte(newvalue))
	})

	if err != nil {
		log.Printf("[ReqID: %d] Failed to persist SWAP to BoltDB: %v", reqID, err)
		return nil, err
	}

	records[key] = newvalue
	log.Printf("[ReqID: %d] Sent SWAP from %s for key: %s. OldValue: %s changed to NewValue: %s", reqID, p.Addr.String(), in.Key, oldvalue, newvalue)

	return &pb.SwapResponse{
		OldValue: oldvalue,
		Exists:   exists,
	}, nil
}

func (s *server) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetResponse, error) {
	reqID := atomic.AddUint64(&requestID, 1)
	mu.Lock()
	defer mu.Unlock()
	p, ok := peer.FromContext(ctx)
	if ok {
		log.Printf("[ReqID: %d] Received GET from %s for key: %s", reqID, p.Addr.String(), in.Key)
	}

	key := in.Key
	value, exists := records[key]

	log.Printf("[ReqID: %d] Sent GET from %s for key: %s. Got value: %s, exists: %t", reqID, p.Addr.String(), in.Key, value, exists)

	return &pb.GetResponse{
		Value:  value,
		Exists: exists,
	}, nil
}

func (s *server) Scan(in *pb.ScanRequest, stream pb.KVService_ScanServer) error {
	reqID := atomic.AddUint64(&requestID, 1)
	p, ok := peer.FromContext(stream.Context())
	if ok {
		log.Printf("[ReqID: %d] Received SCAN from %s from key: %s to key: %s", reqID, p.Addr.String(), in.StartKey, in.EndKey)
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
		log.Printf("[ReqID: %d] Sent SCAN from %s from key: %s to key: %s. Key: %s, Value: %s", reqID, p.Addr.String(), in.StartKey, in.EndKey, key, snapshot[key])
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
		log.Printf("[ReqID: %d] Received DELETE from %s for key: %s", reqID, p.Addr.String(), in.Key)
	}

	key := in.Key
	_, exists := records[key]

	// Persist to BoltDB
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.bucketName))
		return b.Delete([]byte(key))
	})

	if err != nil {
		log.Printf("[ReqID: %d] Failed to persist DELETE to BoltDB: %v", reqID, err)
		return nil, err
	}

	if exists {
		delete(records, key)
	}

	log.Printf("[ReqID: %d] Sent DELETE from %s for key: %s. Exists: %t", reqID, p.Addr.String(), in.Key, exists)

	return &pb.DeleteResponse{
		Exists: exists,
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

	s := grpc.NewServer()
	pb.RegisterKVServiceServer(s, &server{
		db:            db,
		bucketName:    bucketName,
		myPartitionId: partitionId,
	})

	log.Printf("gRPC server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
