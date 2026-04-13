package raft

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pb "go_grpc/raft/proto"
)

// ProposeCmd appends a new command to the leader's log.
// Must only be called on a leader node.
func (rf *RaftNode) ProposeCmd(cmd *Command) (uint64, error) {
	rf.raftmu.Lock()
	defer rf.raftmu.Unlock()

	if rf.role != Leader {
		return 0, fmt.Errorf("propose: node %s is not leader (role=%s)", rf.nodeId, rf.role)
	}

	// Serialize the command into the log entry
	serialized, err := cmd.Serialize()
	if err != nil {
		return 0, fmt.Errorf("propose: failed to serialize command: %w", err)
	}

	index := rf.log.Append(rf.currentTerm, serialized)
	log.Printf("[Node %s] Proposed command %s at index %d (term %d)", rf.nodeId, cmd.Op, index, rf.currentTerm)

	// Persist log entry to disk
	rf.persistState()

	// Signal the replicate loop to broadcast AppendEntries to followers
	rf.signalReplicate()

	return index, nil
}

// replicateAndHeartbeatLoop combines two responsibilities:
// 1. Replicates new log entries to followers when signaled via replicateCh
// 2. Sends periodic heartbeats to prevent follower election timeouts
//
// Heartbeat interval: 100ms (must be < minimum election timeout of 500ms)
func (rf *RaftNode) replicateAndHeartbeatLoop() {
	const heartbeatInterval = 100 * time.Millisecond
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rf.stopCh:
			ticker.Stop()
			return
		case <-rf.replicateCh:
			// New entries to replicate — broadcast immediately
			rf.broadcastAppendEntries()
		case <-ticker.C:
			// Periodic heartbeat — broadcast empty AppendEntries
			rf.broadcastAppendEntries()
		}
	}
}

// broadcastAppendEntries sends an AppendEntries RPC to all followers.
// This is the shared heartbeat/replication sender.
func (rf *RaftNode) broadcastAppendEntries() {
	rf.raftmu.Lock()
	if rf.role != Leader {
		rf.raftmu.Unlock()
		return
	}
	rf.raftmu.Unlock()

	var wg sync.WaitGroup
	for _, peer := range rf.peers {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			rf.replicateTo(p)
		}(peer)
	}
	wg.Wait()

	// After all responses, check if we can commit
	rf.raftmu.Lock()
	oldCommit := rf.commitIndex
	rf.tryCommit()
	if rf.commitIndex > oldCommit {
		log.Printf("[Node %s] Heartbeat: commitIndex advanced to %d", rf.nodeId, rf.commitIndex)
	}
	rf.raftmu.Unlock()
}

// replicateTo sends AppendEntries or InstallSnapshot to a single follower.
// This runs in its own goroutine and handles the response.
func (rf *RaftNode) replicateTo(peer string) {
	rf.raftmu.Lock()

	// If the follower's nextIndex is before our snapshot, we must send a snapshot
	if rf.snapshotIndex > 0 && rf.nextIndex[peer] <= rf.snapshotIndex {
		log.Printf("[Node %s] → InstallSnapshot to %s (index %d, term %d)",
			rf.nodeId, peer, rf.snapshotIndex, rf.snapshotTerm)
		rf.sendInstallSnapshotLocked(peer)
		return
	}

	// Build the AppendEntries request
	nextIdx := rf.nextIndex[peer]
	prevLogIndex := nextIdx - 1
	prevLogTerm := uint64(0)
	if prevLogIndex > 0 {
		prevLogTerm = rf.log.TermAt(prevLogIndex)
	}

	// Grab entries to send (convert local LogEntry to proto LogEntry)
	localEntries := rf.log.Slice(nextIdx, rf.log.LastIndex()+1)
	entries := make([]*pb.LogEntry, len(localEntries))
	for i, e := range localEntries {
		entries[i] = &pb.LogEntry{
			Term:      e.Term,
			Index:     e.Index,
			EntryType: uint32(e.Type),
			Command:   e.Command,
		}
	}

	req := &pb.AppendRequest{
		Term:         rf.currentTerm,
		LeaderCommit: rf.commitIndex,
		LeaderId:     rf.nodeId,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
	}

	rf.raftmu.Unlock()

	// Send RPC (outside lock)
	client, ok := rf.peerClients[peer]
	if !ok {
		log.Printf("[Node %s] No gRPC client for peer %s", rf.nodeId, peer)
		return
	}

	resp, err := client.AppendEntries(context.Background(), req)
	if err != nil {
		log.Printf("[Node %s] ✗ AppendEntries to %s failed: %v", rf.nodeId, peer, err)
		return
	}

	rf.raftmu.Lock()
	defer rf.raftmu.Unlock()

	// Handle stale term
	if resp.Term > rf.currentTerm {
		log.Printf("[Node %s] Stepping down: peer %s has higher term %d > currentTerm %d",
			rf.nodeId, peer, resp.Term, rf.currentTerm)
		rf.currentTerm = resp.Term
		rf.votedFor = ""
		rf.role = Follower
		rf.persistState()
		return
	}

	if resp.Success {
		// Update matchIndex and nextIndex
		if resp.MatchIndex > rf.matchIndex[peer] {
			rf.matchIndex[peer] = resp.MatchIndex
		}
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	} else {
		// Decrement nextIndex using conflict hint
		oldNext := rf.nextIndex[peer]
		if resp.ConflictIndex > 0 {
			rf.nextIndex[peer] = resp.ConflictIndex
		} else if rf.nextIndex[peer] > 1 {
			rf.nextIndex[peer]--
		}
		log.Printf("[Node %s] AppendEntries to %s rejected: nextIndex %d→%d (conflictIndex=%d)",
			rf.nodeId, peer, oldNext, rf.nextIndex[peer], resp.ConflictIndex)
	}
}

// tryCommit advances commitIndex if a majority of followers have replicated
// the entry at commitIndex+1. Must be called with raftmu held.
func (rf *RaftNode) tryCommit() {
	for rf.commitIndex < rf.log.LastIndex() {
		nextCommit := rf.commitIndex + 1
		count := 1 // Leader itself has the entry

		for _, peer := range rf.peers {
			if rf.matchIndex[peer] >= nextCommit {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			oldCommit := rf.commitIndex
			rf.commitIndex = nextCommit
			log.Printf("[Node %s] COMMIT: %d → %d (count=%d/%d)",
				rf.nodeId, oldCommit, rf.commitIndex, count, len(rf.peers)+1)
			rf.signalApply()
		} else {
			break
		}
	}
}

// sendInstallSnapshotLocked sends an InstallSnapshot RPC to a follower.
// Must be called with raftmu held (releases lock during RPC).
func (rf *RaftNode) sendInstallSnapshotLocked(peer string) {
	req := &pb.InstallSnapshotRequest{
		Term:              rf.currentTerm,
		LeaderId:          rf.nodeId,
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.snapshotTerm,
		Offset:            0,
		Data:              rf.snapshotBuf,
		Done:              true,
	}

	rf.raftmu.Unlock()

	client, ok := rf.peerClients[peer]
	if !ok {
		log.Printf("[Node %s] No gRPC client for peer %s", rf.nodeId, peer)
		return
	}

	resp, err := client.InstallSnapshot(context.Background(), req)
	if err != nil {
		log.Printf("[Node %s] InstallSnapshot to %s failed: %v", rf.nodeId, peer, err)
		return
	}

	rf.raftmu.Lock()
	defer rf.raftmu.Unlock()

	if resp.Term > rf.currentTerm {
		rf.currentTerm = resp.Term
		rf.votedFor = ""
		rf.role = Follower
		rf.persistState()
		return
	}

	if resp.Success {
		// Follower now has the snapshot — advance nextIndex
		oldNext := rf.nextIndex[peer]
		rf.nextIndex[peer] = rf.snapshotIndex + 1
		rf.matchIndex[peer] = rf.snapshotIndex
		log.Printf("[Node %s] InstallSnapshot to %s success: nextIndex %d→%d, matchIndex=%d",
			rf.nodeId, peer, oldNext, rf.nextIndex[peer], rf.matchIndex[peer])
	}
}

// signalReplicate sends a non-blocking signal to the replicate loop.
// Must be called with raftmu held (or immediately after unlocking).
func (rf *RaftNode) signalReplicate() {
	select {
	case rf.replicateCh <- struct{}{}:
	default: // Already pending — don't block
	}
}
