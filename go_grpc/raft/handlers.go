package raft

import (
	"context"
	"fmt"
	"log"

	pb "go_grpc/raft/proto"
)

type raftService struct {
	pb.UnimplementedRaftServer
	node *RaftNode
}

// NewRaftService returns a Raft gRPC server implementation bound to a RaftNode.
func NewRaftService(node *RaftNode) pb.RaftServer {
	return &raftService{node: node}
}

func (s *raftService) AppendEntries(ctx context.Context, in *pb.AppendRequest) (*pb.AppendResponse, error) {
	s.node.raftmu.Lock()
	defer s.node.raftmu.Unlock()

	// 0. If not a follower or candidate, reject (leader should not receive AppendEntries)
	if s.node.role == Leader {
		log.Printf("[Node %s] Reject AppendEntries: already leader in term %d (got term %d from %s)",
			s.node.nodeId, s.node.currentTerm, in.Term, in.LeaderId)
		return &pb.AppendResponse{
			Term:          s.node.currentTerm,
			Success:       false,
			ConflictIndex: 0,
			MatchIndex:    0,
		}, nil
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if in.Term < s.node.currentTerm {
		log.Printf("[Node %s] Reject AppendEntries: stale term %d < currentTerm %d (from %s)",
			s.node.nodeId, in.Term, s.node.currentTerm, in.LeaderId)
		return &pb.AppendResponse{
			Term:          s.node.currentTerm,
			Success:       false,
			ConflictIndex: 0,
			MatchIndex:    0,
		}, nil
	}

	s.node.leaderId = in.LeaderId
	// Valid leader heartbeat received — reset election timeout
	log.Printf("[Node %s] Resetting Election Timer, RECEIVED HEARTBEAT", s.node.nodeId)
	s.node.resetElectionTimer()

	// Update term if leader's term is newer
	if in.Term > s.node.currentTerm {
		log.Printf("[Node %s] Term update: %d → %d (from leader %s), stepping down to Follower",
			s.node.nodeId, s.node.currentTerm, in.Term, in.LeaderId)
		s.node.currentTerm = in.Term
		s.node.votedFor = ""
		s.node.role = Follower
		s.node.persistState()
	}

	// 2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if in.PrevLogIndex > 0 {
		if in.PrevLogIndex > s.node.log.LastIndex() || !s.node.log.MatchIndexTerm(in.PrevLogIndex, in.PrevLogTerm) {
			// Return conflict index for optimization (§5.3 optimization)
			conflictIdx := s.node.findConflictIndex(in.PrevLogIndex, in.PrevLogTerm)
			log.Printf("[Node %s] AppendEntries rejected: prevLogIndex=%d term=%d mismatch (my lastIndex=%d). conflictIndex=%d",
				s.node.nodeId, in.PrevLogIndex, in.PrevLogTerm, s.node.log.LastIndex(), conflictIdx)
			return &pb.AppendResponse{
				Term:          s.node.currentTerm,
				Success:       false,
				ConflictIndex: conflictIdx,
				MatchIndex:    0,
			}, nil
		}
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms),
	//    delete the existing entry and all that follow it (§5.3)
	appended := 0
	if len(in.Entries) > 0 {
		for _, entry := range in.Entries {
			if entry.Index <= s.node.log.LastIndex() {
				existing := s.node.log.Get(entry.Index)
				if existing != nil && existing.Term != entry.Term {
					log.Printf("[Node %s] Log conflict at index %d: my term %d vs leader term %d, truncating",
						s.node.nodeId, entry.Index, existing.Term, entry.Term)
					s.node.log.TruncateFrom(entry.Index)
					// Invalidate persistence watermark — entries from index onward are now stale on disk
					if s.node.persistedUpTo >= entry.Index {
						s.node.persistedUpTo = entry.Index - 1
					}
				}
			}
		}

		// 4. Append any new entries not already in the log
		for _, entry := range in.Entries {
			if entry.Index > s.node.log.LastIndex() {
				raftEntry := &LogEntry{
					Term:    entry.Term,
					Index:   entry.Index,
					Type:    EntryType(entry.EntryType),
					Command: entry.Command,
				}
				s.node.log.AppendEntry(raftEntry)
				appended++
			}
		}

		// Persist new log entries
		s.node.persistState()
	}

	if appended > 0 {
		log.Printf("[Node %s] Appended %d entries from leader %s (term %d), log length now %d",
			s.node.nodeId, appended, in.LeaderId, in.Term, s.node.log.Len())
	}

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if in.LeaderCommit > s.node.commitIndex {
		oldCommit := s.node.commitIndex
		s.node.commitIndex = min(in.LeaderCommit, s.node.log.LastIndex())
		log.Printf("[Node %s] commitIndex advanced: %d → %d (leaderCommit=%d)",
			s.node.nodeId, oldCommit, s.node.commitIndex, in.LeaderCommit)
		// Signal the apply loop to apply newly committed entries asynchronously
		s.node.signalApply()
	}

	// log.Printf("[Node %s] AppendEntries SUCCESS from leader %s (term %d), prevLogIndex=%d, entries=%d, leaderCommit=%d",
	// s.node.nodeId, in.LeaderId, in.Term, in.PrevLogIndex, len(in.Entries), in.LeaderCommit)
	return &pb.AppendResponse{
		Term:          s.node.currentTerm,
		Success:       true,
		ConflictIndex: 0,
		MatchIndex:    s.node.log.LastIndex(),
	}, nil
}

func (s *raftService) InstallSnapshot(ctx context.Context, in *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	s.node.raftmu.Lock()
	defer s.node.raftmu.Unlock()

	// 1. Reply false if term < currentTerm
	if in.Term < s.node.currentTerm {
		return &pb.InstallSnapshotResponse{
			Term:    s.node.currentTerm,
			Success: false,
		}, nil
	}

	// Update term if leader's term is newer
	if in.Term > s.node.currentTerm {
		log.Printf("[Node %s] InstallSnapshot: term update %d → %d (from leader %s)",
			s.node.nodeId, s.node.currentTerm, in.Term, in.LeaderId)
		s.node.currentTerm = in.Term
		s.node.votedFor = ""
		s.node.role = Follower
		s.node.persistState()
	}

	// 2. If existing log entry conflicts with snapshot (index ≤ last_included_index,
	//    different term), discard log up to last_included_index
	if in.LastIncludedIndex <= s.node.snapshotIndex {
		// Duplicate snapshot chunk — ignore
		return &pb.InstallSnapshotResponse{
			Term:    s.node.currentTerm,
			Success: true,
		}, nil
	}

	// 3. If this is the final chunk (done=true), apply snapshot
	if in.Done {
		s.node.snapshotIndex = in.LastIncludedIndex
		s.node.snapshotTerm = in.LastIncludedTerm

		// Discard log entries now covered by the snapshot
		s.node.log.CompactBefore(in.LastIncludedIndex)
		// Snapshot replaces entries 1..LastIncludedIndex — reset persistence watermark
		s.node.persistedUpTo = 0

		// Apply snapshot to state machine
		if len(in.Data) > 0 {
			if err := s.node.sm.Apply(in.Data, s.node.role == Leader); err != nil {
				return nil, fmt.Errorf("failed to apply snapshot: %w", err)
			}
		}

		// Update commitIndex and lastApplied
		if s.node.commitIndex < in.LastIncludedIndex {
			s.node.commitIndex = in.LastIncludedIndex
		}
		if s.node.lastApplied < in.LastIncludedIndex {
			s.node.lastApplied = in.LastIncludedIndex
		}

		// Persist snapshot state
		s.node.persistState()

		// Reset election timer — valid leader heartbeat
		s.node.resetElectionTimer()

		log.Printf("[Node %s] Installed snapshot at index %d (term %d)",
			s.node.nodeId, in.LastIncludedIndex, in.LastIncludedTerm)
	}

	return &pb.InstallSnapshotResponse{
		Term:    s.node.currentTerm,
		Success: true,
	}, nil
}

func (s *raftService) RequestVote(ctx context.Context, in *pb.VoteRequest) (*pb.VoteResponse, error) {
	s.node.raftmu.Lock()
	defer s.node.raftmu.Unlock()

	// 0. If not a follower or candidate, reject
	if s.node.role == Leader {
		log.Printf("[Node %s] Reject RequestVote: already leader in term %d (candidate=%s)",
			s.node.nodeId, s.node.currentTerm, in.CandidateId)
		return &pb.VoteResponse{
			Term:        s.node.currentTerm,
			VoteGranted: false,
		}, nil
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if in.Term < s.node.currentTerm {
		log.Printf("[Node %s] Reject RequestVote: stale term %d < currentTerm %d (candidate=%s)",
			s.node.nodeId, in.Term, s.node.currentTerm, in.CandidateId)
		return &pb.VoteResponse{
			Term:        s.node.currentTerm,
			VoteGranted: false,
		}, nil
	}

	// If candidate's term is newer, update our term and step down to follower
	if in.Term > s.node.currentTerm {
		log.Printf("[Node %s] Term update: %d → %d (from candidate %s), stepping down to Follower",
			s.node.nodeId, s.node.currentTerm, in.Term, in.CandidateId)
		s.node.currentTerm = in.Term
		s.node.votedFor = ""
		s.node.role = Follower
		s.node.persistState()
		s.node.resetElectionTimer()
	}

	// 2. If votedFor is null or candidateId, and candidate's log is at least
	//    as up-to-date as receiver's log, grant vote (§5.2, §5.4)
	logOK := s.node.log.IsUpToDate(in.LastLogTerm, in.LastLogIndex)
	voteOK := (s.node.votedFor == "" || s.node.votedFor == in.CandidateId) && logOK

	if voteOK {
		reason := "first vote"
		if s.node.votedFor != "" {
			reason = "re-vote for same candidate"
		}
		log.Printf("[Node %s] Vote GRANTED to %s (term %d, %s). votedFor=%s→%s",
			s.node.nodeId, in.CandidateId, in.Term, reason, s.node.votedFor, in.CandidateId)
		s.node.votedFor = in.CandidateId
		// Persist vote
		s.node.persistState()
		return &pb.VoteResponse{
			Term:        s.node.currentTerm,
			VoteGranted: true,
		}, nil
	}

	denyReason := ""
	if s.node.votedFor != "" && s.node.votedFor != in.CandidateId {
		denyReason = fmt.Sprintf("already voted for %s", s.node.votedFor)
	} else if !logOK {
		denyReason = fmt.Sprintf("candidate log not up-to-date (lastTerm=%d, lastIndex=%d vs my lastTerm=%d, lastIndex=%d)",
			in.LastLogTerm, in.LastLogIndex, s.node.log.LastTerm(), s.node.log.LastIndex())
	}
	log.Printf("[Node %s] Vote REJECTED for %s: %s",
		s.node.nodeId, in.CandidateId, denyReason)

	return &pb.VoteResponse{
		Term:        s.node.currentTerm,
		VoteGranted: false,
	}, nil
}

// findConflictIndex returns the index where the follower's log first diverges
// from the leader's expected term at prevLogIndex/prevLogTerm.
// Used to optimize AppendEntries retries (§5.3 optimization).
func (rf *RaftNode) findConflictIndex(prevLogIndex uint64, prevLogTerm uint64) uint64 {
	// If prevLogIndex is beyond our log, tell leader to try our last index
	if prevLogIndex > rf.log.LastIndex() {
		return rf.log.LastIndex()
	}

	// Walk backward to find where terms first differ
	idx := prevLogIndex
	for idx > 0 {
		if rf.log.TermAt(idx) == prevLogTerm {
			return idx
		}
		idx--
	}
	return 0
}
