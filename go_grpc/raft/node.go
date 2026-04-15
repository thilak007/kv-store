package raft

import (
	"fmt"
	pb "go_grpc/raft/proto"
	"log"
	"sync"

	bolt "go.etcd.io/bbolt"
)

// NodeRole represents the current role of a Raft node.
type NodeRole int

const (
	Follower NodeRole = iota
	Candidate
	Leader
)

func (r NodeRole) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown Raft State, Error"
	}
}

type StateMachine interface {
	Apply(cmd []byte, isLeader bool) error
}

// RaftNode holds all state for a single Raft replica.
//
// Persistent state (survives crashes):
//   - currentTerm, votedFor, log
//
// Volatile state:
//   - commitIndex, lastApplied
//
// Volatile state on leaders (reinitialized after election):
//   - nextIndex, matchIndex
//
// Thread safety:
//   - raftmu protects ALL Raft fields.
type RaftNode struct {
	raftmu sync.RWMutex

	// ── Persistent State ──────────────────────────────────────────
	currentTerm uint64 // Latest term this node has seen
	votedFor    string // Candidate that received vote in current term ("" if none)
	log         *Log   // Log entries; each contains command + term + index

	// ── Snapshot State (persistent) ───────────────────────────────
	snapshotIndex uint64 // Index of last entry included in snapshot
	snapshotTerm  uint64 // Term of last entry included in snapshot
	snapshotBuf   []byte // Snapshot data (compact representation of state machine)

	// ── Persistence tracking ──────────────────────────────────────
	persistedUpTo uint64 // Highest log index known to be correctly on disk

	// ── Volatile State ────────────────────────────────────────────
	commitIndex uint64 // Highest log entry known to be committed
	lastApplied uint64 // Highest log entry applied to state machine

	// ── Volatile State on Leaders (reinitialized after election) ──
	nextIndex  map[string]uint64 // For each peer: index of next log entry to send
	matchIndex map[string]uint64 // For each peer: highest log entry known replicated

	// ── Configuration ─────────────────────────────────────────────
	nodeId   string   // This node's unique identifier (e.g., "1.0, <replicaID.partitionID>" ) Here severID is the replica ID instead of IP address.
	peers    []string // Peer node identifiers (e.g., ["0.1", "0.2"])
	leaderId string   // Current leader's ID (for redirecting clients)

	// ── State Machine & Storage ───────────────────────────────────
	sm         StateMachine // Interface to apply committed commands to the KV store
	db         *bolt.DB     // bbolt database (shared with server)
	raftBucket string       // Bucket name for Raft log in bbolt

	// ── Role & Channels ──────────────────────────────────────────
	role            NodeRole
	applyCh         chan struct{}            // Signal for apply loop when commitIndex advances
	stopCh          chan struct{}            // Signal to shut down background goroutines
	replicateCh     chan struct{}            // Signal for leader to broadcast AppendEntries
	electionCh      chan struct{}            // Signal to start a new election
	resetElectionCh chan struct{}            // Signal to reset election timeout
	peerClients     map[string]pb.RaftClient // gRPC clients for each peer
	votes           int                      // Votes received in current election
}

// NewRaftNode creates and initializes a new Raft node.
func NewRaftNode(id string, peers []string, kvsm StateMachine, db *bolt.DB, raftBucket string, peerClients map[string]pb.RaftClient) *RaftNode {
	if peerClients == nil {
		peerClients = make(map[string]pb.RaftClient)
	}

	return &RaftNode{
		currentTerm: 0,
		votedFor:    "",
		log:         NewLog(),
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		nodeId:      id,
		peers:       peers,
		sm:          kvsm,
		db:          db,
		raftBucket:  raftBucket,
		role:        Follower,
		peerClients: peerClients,
	}
}

// GetState returns a snapshot of the node's current state for debugging.
func (rf *RaftNode) GetState() (role NodeRole, leaderId string) {
	rf.raftmu.RLock()
	defer rf.raftmu.RUnlock()
	return rf.role, rf.leaderId
}

// Start launches all background goroutines for the Raft node.
func (rf *RaftNode) Start() {
	rf.applyCh = make(chan struct{}, 1)
	rf.stopCh = make(chan struct{})
	rf.replicateCh = make(chan struct{}, 1)
	rf.electionCh = make(chan struct{}, 1)
	rf.resetElectionCh = make(chan struct{}, 1)
	go rf.applyLoop()
	go rf.replicateAndHeartbeatLoop()
	go rf.electionTimerLoop()

	log.Printf("[Node %s] Started (role=%s, term=%d, logLen=%d, peers=%v)",
		rf.nodeId, rf.role, rf.currentTerm, rf.log.Len(), rf.peers)
}

// SetPeerClient registers a gRPC client for a peer.
// Must be called before the node becomes a leader.
func (rf *RaftNode) SetPeerClient(peerID string, client pb.RaftClient) {
	rf.raftmu.Lock()
	defer rf.raftmu.Unlock()
	rf.peerClients[peerID] = client
}

// Stop signals all background goroutines to shut down.
func (rf *RaftNode) Stop() {
	close(rf.stopCh)
}

// String returns a human-readable representation of the node's state.
func (rf *RaftNode) String() string {
	rf.raftmu.RLock()
	defer rf.raftmu.RUnlock()

	return fmt.Sprintf(
		"RaftNode{id=%s, role=%s, term=%d, votedFor=%s, commitIndex=%d, lastApplied=%d, log=%s}",
		rf.nodeId, rf.role, rf.currentTerm, rf.votedFor, rf.commitIndex, rf.lastApplied, rf.log.String(),
	)
}
