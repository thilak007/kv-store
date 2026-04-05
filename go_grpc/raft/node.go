// AppendEntries
// RequestVote
// LogReplication
// LeaderElection
package raft

import (
	"context"
	pb "go_grpc/raft/proto"
	"log"
	"sync/atomic"

	"google.golang.org/grpc/peer"
)

type raftService struct {
	pb.UnimplementedRaftServer
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

func (s *raftService) AppendEntries(ctx context.Context, in *pb.AppendRequest) (*pb.AppendResponse, error) {
	reqID := atomic.AddUint64(&requestID, 1)
	mu.Lock()
	defer mu.Unlock()
	p, ok := peer.FromContext(ctx)
	if ok {
		log.Printf("[ReqID: %d.%d] Received AppendEntries for entry %d.%d from leader: %s", s.myPartitionId, reqID, in.Entries[0].Term, in.Entries[0].Index, in.LeaderId)
	} // 1. Reply false if term < currentTerm (§5.1)
	if currentTerm < in.Term {
		return &pb.AppendResponse{
			Term:    0,
			Success: false,
		}
	}
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
