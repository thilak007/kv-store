package raft

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	pb "go_grpc/raft/proto"
)

func (rf *RaftNode) becomeCandidate() {
	log.Println("Inside becomeCandidate. Let's get this thing")
	rf.raftmu.Lock()
	// defer rf.raftmu.Unlock()

	// 1. Increment currentTerm
	rf.currentTerm++

	// 2. Vote for self
	rf.votedFor = rf.nodeId
	rf.votes = 1

	// 3. Transition to candidate
	rf.role = Candidate

	// Persist term change and votedFor
	rf.persistState()

	lastLogIndex := rf.log.LastIndex()
	lastLogTerm := rf.log.LastTerm()
	log.Printf("[Node %s] ELECTION STARTED: Became candidate (term %d, lastLogIndex=%d, lastLogTerm=%d)",
		rf.nodeId, rf.currentTerm, lastLogIndex, lastLogTerm)

	// 4. Send RequestVote RPCs to all peers (release lock during RPC)
	peers := make([]string, len(rf.peers))
	copy(peers, rf.peers)
	votesNeeded := len(rf.peers)/2 + 1

	rf.raftmu.Unlock()

	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			rf.sendRequestVote(p, lastLogIndex, lastLogTerm)
		}(peer)
	}
	wg.Wait()

	rf.raftmu.Lock()
	defer rf.raftmu.Unlock()

	// 5. If majority granted, become leader
	if rf.votes >= votesNeeded {
		rf.becomeLeader()
	} else {
		log.Printf("[Node %s] Election FAILED: got %d/%d votes, staying candidate",
			rf.nodeId, rf.votes, votesNeeded)
	}
}

func (rf *RaftNode) becomeLeader() {
	rf.role = Leader
	rf.leaderId = rf.nodeId
	rf.votes = 0

	log.Printf("[Node %s] has been elected leader for term %d", rf.nodeId, rf.currentTerm)

	// Initialize leader volatile state
	for _, peer := range rf.peers {
		rf.nextIndex[peer] = rf.log.LastIndex() + 1
		rf.matchIndex[peer] = 0
	}

	// Append NoOp entry to establish leadership (§5.4.2)
	noOpEntry := &LogEntry{
		Term:    rf.currentTerm,
		Index:   rf.log.LastIndex() + 1,
		Type:    EntryTypeNoOp,
		Command: nil,
	}
	rf.log.AppendEntry(noOpEntry)

	// Persist NoOp entry to disk
	rf.persistState()

	log.Printf("[Node %s] Appended NoOp entry at index %d (term %d)",
		rf.nodeId, noOpEntry.Index, rf.currentTerm)

	// Signal replicate loop to broadcast heartbeat (with NoOp) to all followers
	rf.signalReplicate()
}

func (rf *RaftNode) sendRequestVote(peer string, lastLogIndex, lastLogTerm uint64) {
	req := &pb.VoteRequest{
		Term:         rf.currentTerm,
		CandidateId:  rf.nodeId,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	log.Printf("[Node %s] → RequestVote to %s (term %d, lastLogIndex=%d)",
		rf.nodeId, peer, rf.currentTerm, lastLogIndex)

	client, ok := rf.peerClients[peer]
	if !ok {
		log.Printf("[Node %s] No gRPC client for peer %s", rf.nodeId, peer)
		return
	}

	resp, err := client.RequestVote(context.Background(), req)
	if err != nil {
		log.Printf("[Node %s] ✗ RequestVote to %s failed: %v", rf.nodeId, peer, err)
		return
	}

	rf.raftmu.Lock()
	defer rf.raftmu.Unlock()

	// If peer's term is higher, step down
	if resp.Term > rf.currentTerm {
		log.Printf("[Node %s] Stepping down: peer %s has higher term %d > currentTerm %d",
			rf.nodeId, peer, resp.Term, rf.currentTerm)
		rf.currentTerm = resp.Term
		rf.votedFor = ""
		rf.role = Follower
		rf.persistState()
		return
	}

	// Only count vote if we're still a candidate in the same term
	if rf.role == Candidate && resp.Term == rf.currentTerm && resp.VoteGranted {
		rf.votes++
		votesNeeded := len(rf.peers)/2 + 1
		log.Printf("[Node %s] Vote from %s: GRANTED (votes=%d/%d needed)",
			rf.nodeId, peer, rf.votes, votesNeeded)
	} else if resp.VoteGranted {
		log.Printf("[Node %s] Vote from %s: GRANTED but ignored (not candidate or stale term)",
			rf.nodeId, peer)
	} else {
		log.Printf("[Node %s] Vote from %s: REJECTED", rf.nodeId, peer)
	}
}

// electionTimerLoop monitors the node's role and triggers elections
// when no heartbeat is received within the election timeout.
func (rf *RaftNode) electionTimerLoop() {
	for {

		// Random election timeout: 150-300ms
		timeout := time.Duration(150+rand.Intn(151)) * time.Millisecond
		rf.raftmu.Lock()
		currentRole := rf.role
		rf.raftmu.Unlock()
		if currentRole == Leader {
			// Leaders don't run election timers
			break
		}

		timer := time.NewTimer(timeout)

		select {
		case <-rf.stopCh:
			timer.Stop()
			return
		case <-rf.electionCh:
			timer.Stop()
			log.Printf("[Node %s] Election triggered externally", rf.nodeId)
			rf.becomeCandidate()
		case <-rf.resetElectionCh:
			// Valid leader heartbeat received — restart timeout
			timer.Stop()
		case <-timer.C:
			// Election timeout expired — start new election
			log.Printf("[Node %s] ELECTION TIMEOUT (no heartbeat in %v)", rf.nodeId, timeout)
			rf.becomeCandidate()
		}
	}
}

// triggerElection sends a signal to start a new election.
func (rf *RaftNode) triggerElection() {
	select {
	case rf.electionCh <- struct{}{}:
	default: // Already pending
	}
}

// resetElectionTimer signals the election timer loop to restart the countdown.
// Called when a valid leader heartbeat (AppendEntries) is received.
func (rf *RaftNode) resetElectionTimer() {
	select {
	case rf.resetElectionCh <- struct{}{}:
	default: // Timer already reset — nothing to do
	}
}
