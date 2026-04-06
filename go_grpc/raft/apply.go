package raft

import "log"

// applyLoop runs indefinitely, applying committed entries to the state machine.
// It is triggered whenever commitIndex advances via AppendEntries or local commits.
func (rf *RaftNode) applyLoop() {
	for {
		select {
		case <-rf.stopCh:
			return
		case <-rf.applyCh:
			rf.raftmu.Lock()
			rf.applyCommittedEntries()
			rf.raftmu.Unlock()
		}
	}
}

// applyCommittedEntries applies all entries from lastApplied+1 to commitIndex.
// Must be called with raftmu held.
func (rf *RaftNode) applyCommittedEntries() {
	applied := 0
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		entry := rf.log.Get(i)
		if entry != nil && !entry.IsNoOp() {
			if err := rf.sm.Apply(entry.Command); err != nil {
				log.Printf("[Node %s] ✗ Failed to apply entry %d: %v", rf.nodeId, i, err)
				return
			}
			applied++
		}
		rf.lastApplied = i
	}
	if applied > 0 {
		log.Printf("[Node %s] Applied %d entries to state machine (lastApplied=%d)",
			rf.nodeId, applied, rf.lastApplied)
	}
}

// signalApply sends a non-blocking signal to the apply loop.
// Must be called with raftmu held (or immediately after unlocking).
func (rf *RaftNode) signalApply() {
	select {
	case rf.applyCh <- struct{}{}:
	default: // Already pending — don't block
	}
}
