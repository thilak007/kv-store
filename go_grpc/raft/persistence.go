package raft

import (
	"encoding/binary"
	"fmt"
	"log"

	bolt "go.etcd.io/bbolt"
)

// takeSnapshot creates a snapshot of the current state machine up to the given index.
// This is called by the state machine layer when log compaction is needed.
func (rf *RaftNode) takeSnapshot(index uint64, data []byte) {
	rf.raftmu.Lock()
	defer rf.raftmu.Unlock()

	if index <= rf.snapshotIndex {
		return // Already have a newer snapshot
	}

	rf.snapshotIndex = index
	rf.snapshotTerm = rf.log.TermAt(index)
	rf.snapshotBuf = data

	// Discard log entries that are now part of the snapshot (1 through index)
	rf.log.CompactBefore(index)
	// Snapshot replaces entries 1..index — reset persistence watermark
	rf.persistedUpTo = 0

	// Persist snapshot state
	rf.persistState()

	log.Printf("[Node %s] Created snapshot at index %d (term %d), log compacted to %d entries",
		rf.nodeId, index, rf.snapshotTerm, rf.log.Len())
}

// persistState incrementally writes only changed log entries to bbolt.
// Must be called with raftmu held.
//
// Strategy:
//   - persistedUpTo tracks the highest index known to be correctly on disk.
//   - On truncation, persistedUpTo is lowered to the truncation point.
//   - On each call, we write entries [persistedUpTo+1 .. memLen] and delete
//     any stale trailing entries [memLen+1 .. diskLen].
func (rf *RaftNode) persistState() error {
	return rf.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(rf.raftBucket))
		if err != nil {
			return fmt.Errorf("failed to create bucket %s: %w", rf.raftBucket, err)
		}

		// ── Persistent State: currentTerm ──────────────────────────
		b.Put([]byte("currentTerm"), b64(rf.currentTerm))

		// ── Persistent State: votedFor ─────────────────────────────
		b.Put([]byte("votedFor"), []byte(rf.votedFor))

		// ── Persistent State: snapshotIndex ────────────────────────
		b.Put([]byte("snapshotIndex"), b64(rf.snapshotIndex))

		// ── Persistent State: snapshotTerm ─────────────────────────
		b.Put([]byte("snapshotTerm"), b64(rf.snapshotTerm))

		// ── Persistent State: snapshotBuf ──────────────────────────
		if len(rf.snapshotBuf) > 0 {
			b.Put([]byte("snapshotBuf"), rf.snapshotBuf)
		}

		// ── Incremental Log Persistence ────────────────────────────
		memLen := rf.log.LastIndex()
		diskLen := uint64(0)
		if v := b.Get([]byte("logLen")); v != nil {
			diskLen = u64(v)
		}

		// Write entries from persistedUpTo+1 to memLen (covers new + replaced entries)
		for i := rf.persistedUpTo + 1; i <= memLen; i++ {
			entry := rf.log.Get(i)
			if entry == nil {
				continue
			}
			data, err := encodeEntry(entry)
			if err != nil {
				return fmt.Errorf("failed to encode log entry %d: %w", i, err)
			}
			if err := b.Put([]byte(fmt.Sprintf("log.%d", i)), data); err != nil {
				return fmt.Errorf("failed to persist log entry %d: %w", i, err)
			}
		}

		// Delete stale trailing entries that no longer exist in memory
		for i := memLen + 1; i <= diskLen; i++ {
			if err := b.Delete([]byte(fmt.Sprintf("log.%d", i))); err != nil {
				return fmt.Errorf("failed to delete stale log entry %d: %w", i, err)
			}
		}

		// Update log length and persistence watermark
		if err := b.Put([]byte("logLen"), b64(memLen)); err != nil {
			return fmt.Errorf("failed to persist log length: %w", err)
		}
		rf.persistedUpTo = memLen

		return nil
	})
}

// LoadState reads currentTerm, votedFor, and the log from bbolt.
// Must be called before Start() during server initialization.
func (rf *RaftNode) LoadState() error {
	return rf.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(rf.raftBucket))
		if b == nil {
			log.Printf("[Node %s] No persisted state found in bucket %s (first run)",
				rf.nodeId, rf.raftBucket)
			return nil // No state to load — first run
		}

		if v := b.Get([]byte("currentTerm")); v != nil {
			rf.currentTerm = u64(v)
		}
		if v := b.Get([]byte("votedFor")); v != nil {
			rf.votedFor = string(v)
		}
		if v := b.Get([]byte("snapshotIndex")); v != nil {
			rf.snapshotIndex = u64(v)
		}
		if v := b.Get([]byte("snapshotTerm")); v != nil {
			rf.snapshotTerm = u64(v)
		}
		if v := b.Get([]byte("snapshotBuf")); v != nil {
			rf.snapshotBuf = make([]byte, len(v))
			copy(rf.snapshotBuf, v)
		}

		if v := b.Get([]byte("logLen")); v != nil {
			logLen := u64(v)
			for i := uint64(1); i <= logLen; i++ {
				key := []byte(fmt.Sprintf("log.%d", i))
				v := b.Get(key)
				if v == nil {
					continue
				}
				entry, err := decodeEntry(v)
				if err != nil {
					return fmt.Errorf("failed to decode log entry %d: %w", i, err)
				}
				rf.log.AppendEntry(entry)
			}
			// All loaded entries are now on disk — set watermark
			rf.persistedUpTo = logLen
		}

		log.Printf("[Node %s] Loaded state from disk: term=%d, votedFor=%s, snapshotIndex=%d, logLen=%d",
			rf.nodeId, rf.currentTerm, rf.votedFor, rf.snapshotIndex, rf.log.Len())

		return nil
	})
}

// b64 converts uint64 to 8-byte big-endian for bbolt storage.
func b64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// u64 converts 8-byte big-endian from bbolt back to uint64.
func u64(b []byte) uint64 {
	if len(b) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

// encodeEntry serializes a LogEntry to bytes using binary encoding (fast, no reflection).
//
// Format:
//
//	[term:8][index:8][type:4][cmdLen:8][cmd:N]
func encodeEntry(e *LogEntry) ([]byte, error) {
	cmdLen := 0
	if e.Command != nil {
		cmdLen = len(e.Command)
	}

	buf := make([]byte, 8+8+4+8+cmdLen)
	binary.BigEndian.PutUint64(buf[0:8], e.Term)
	binary.BigEndian.PutUint64(buf[8:16], e.Index)
	binary.BigEndian.PutUint32(buf[16:20], uint32(e.Type))
	binary.BigEndian.PutUint64(buf[20:28], uint64(cmdLen))
	if cmdLen > 0 {
		copy(buf[28:], e.Command)
	}
	return buf, nil
}

// decodeEntry reconstructs a LogEntry from binary-encoded bytes.
func decodeEntry(data []byte) (*LogEntry, error) {
	if len(data) < 28 {
		return nil, fmt.Errorf("log entry too short: %d bytes", len(data))
	}

	cmdLen := int(binary.BigEndian.Uint64(data[20:28]))
	if len(data) < 28+cmdLen {
		return nil, fmt.Errorf("log entry truncated: expected %d bytes of command, got %d",
			cmdLen, len(data)-28)
	}

	var cmd []byte
	if cmdLen > 0 {
		cmd = make([]byte, cmdLen)
		copy(cmd, data[28:28+cmdLen])
	}

	return &LogEntry{
		Term:    binary.BigEndian.Uint64(data[0:8]),
		Index:   binary.BigEndian.Uint64(data[8:16]),
		Type:    EntryType(binary.BigEndian.Uint32(data[16:20])),
		Command: cmd,
	}, nil
}
