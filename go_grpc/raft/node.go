// AppendEntries
// RequestVote
// LogReplication
// LeaderElection
package raft

import (
	"context"
	"fmt"
	pb "go_grpc/raft/proto"
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
	Apply(cmd []byte) error
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
//   - mu protects ALL Raft fields. The kvMu protects the KV store.
type RaftNode struct {
	raftmu sync.Mutex

	// ── Persistent State ──────────────────────────────────────────
	currentTerm uint64 // Latest term this node has seen
	votedFor    string // Candidate that received vote in current term ("" if none)
	log         *Log   // Log entries; each contains command + term + index

	// ── Volatile State ────────────────────────────────────────────
	commitIndex uint64 // Highest log entry known to be committed
	lastApplied uint64 // Highest log entry applied to state machine

	// ── Volatile State on Leaders (reinitialized after election) ──
	nextIndex  map[string]uint64 // For each peer: index of next log entry to send
	matchIndex map[string]uint64 // For each peer: highest log entry known replicated

	// ── Configuration ─────────────────────────────────────────────
	nodeId string   // This node's unique identifier (e.g., "0.0, <serverID.partitionID>" )
	peers  []string // Peer node identifiers (e.g., ["0.1", "0.2"])

	// ── State Machine & Storage ───────────────────────────────────
	sm         StateMachine // Interface to apply committed commands to the KV store
	db         *bolt.DB     // bbolt database (shared with server)
	raftBucket string       // Bucket name for Raft log in bbolt

	// ── Role & Channels ──────────────────────────────────────────
	role NodeRole
}

// NewRaftNode creates and initializes a new Raft node.
//
// Parameters:
//   - id:         Unique identifier for this node (e.g., "0.0")
//   - peers:      List of peer node identifiers
//   - kvsm:       State machine interface for applying commands
//   - db:         bbolt database (shared with server)
//   - raftBucket: Bucket name for Raft log in bbolt
//
// The node starts in Follower state with term 0.
// Background goroutines (election timer, apply loop) are NOT started here —
// they are started by Start().
func NewRaftNode(id string, peers []string, kvsm StateMachine, db *bolt.DB, raftBucket string) *RaftNode {
	rf := &RaftNode{
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
	}

	return rf
}

// GetState returns a snapshot of the node's current state for debugging.
func (rf *RaftNode) GetState() (term uint64, role NodeRole, logLen int) {
	rf.raftmu.Lock()
	defer rf.raftmu.Unlock()
	return rf.currentTerm, rf.role, rf.log.Len()
}

// String returns a human-readable representation of the node's state.
func (rf *RaftNode) String() string {
	rf.raftmu.Lock()
	defer rf.raftmu.Unlock()

	return fmt.Sprintf(
		"RaftNode{id=%s, role=%s, term=%d, votedFor=%s, commitIndex=%d, lastApplied=%d, log=%s}",
		rf.nodeId, rf.role, rf.currentTerm, rf.votedFor, rf.commitIndex, rf.lastApplied, rf.log.String(),
	)
}

type raftService struct {
	pb.UnimplementedRaftServer
}

func (s *raftService) AppendEntries(ctx context.Context, in *pb.AppendRequest) (*pb.AppendResponse, error) {

	// 1. Reply false if term < currentTerm (§5.1)
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	// 4. Append any new entries not already in the log
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

}

// message AppendResponse{
//   uint64 term   = 1;
//   bool success = 2;
// }

// message VoteResponse{
//   uint64 term = 1;
//   bool vote_granted = 2;
// }

func (s *raftService) RequestVote(ctx context.Context, in *pb.VoteRequest) (*pb.VoteResponse, error) {
	// 1. Reply false if term < currentTerm (§5.1)
	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
}
