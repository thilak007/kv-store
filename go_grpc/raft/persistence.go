package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"log"
	"strconv"

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

	// Persist snapshot state
	rf.persistState()

	log.Printf("[Node %s] Created snapshot at index %d (term %d), log compacted to %d entries",
		rf.nodeId, index, rf.snapshotTerm, rf.log.Len())
}

// persistState writes currentTerm, votedFor, and the log to bbolt.
// Must be called with raftmu held.
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

		memLast := rf.log.LastIndex()
		diskLast := uint64(0)
		if v := b.Get([]byte("logLen")); v != nil {
			diskLast = u64(v)
		}

		// Find first divergent index between in-memory and on-disk logs.
		firstMismatch := uint64(1)
		commonLast := memLast
		if diskLast < commonLast {
			commonLast = diskLast
		}

		// Preload on-disk log entries once to avoid repeated point lookups in the compare loop.
		diskEntries := make(map[uint64][]byte, commonLast)
		prefix := []byte("log.")
		c := b.Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			idx, parseErr := strconv.ParseUint(string(k[len(prefix):]), 10, 64)
			if parseErr != nil {
				continue
			}
			if idx >= 1 && idx <= commonLast {
				diskEntries[idx] = v
			}
		}

		for j := uint64(1); j <= commonLast; j++ {
			entry := rf.log.Get(j)
			if entry == nil {
				firstMismatch = j
				break
			}

			memData, err := entry.serializeEntry()
			if err != nil {
				return fmt.Errorf("failed to serialize log entry %d: %w", j, err)
			}
			diskData := diskEntries[j]
			if !bytes.Equal(memData, diskData) {
				firstMismatch = j
				break
			}
			firstMismatch = j + 1
		}

		// Overwrite entries on disk from first mismatch through memory tail.
		for j := firstMismatch; j <= memLast; j++ {
			entry := rf.log.Get(j)
			if entry == nil {
				continue
			}
			data, err := entry.serializeEntry()
			if err != nil {
				return fmt.Errorf("failed to serialize log entry %d: %w", j, err)
			}
			if err := b.Put([]byte(fmt.Sprintf("log.%d", j)), data); err != nil {
				return fmt.Errorf("failed to persist log entry %d: %w", j, err)
			}
		}

		// Delete stale trailing entries that no longer exist in memory.
		if memLast < diskLast {
			for j := memLast + 1; j <= diskLast; j++ {
				if err := b.Delete([]byte(fmt.Sprintf("log.%d", j))); err != nil {
					return fmt.Errorf("failed to delete stale log entry %d: %w", j, err)
				}
			}
		}

		// Write current log length.
		if err := b.Put([]byte("logLen"), b64(memLast)); err != nil {
			return fmt.Errorf("failed to persist log length: %w", err)
		}

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
				entry, err := deserializeEntry(v)
				if err != nil {
					return fmt.Errorf("failed to deserialize log entry %d: %w", i, err)
				}
				rf.log.AppendEntry(entry)
			}
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

// serializeEntry converts a LogEntry to bytes for bbolt storage.
func (e *LogEntry) serializeEntry() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(e); err != nil {
		return nil, fmt.Errorf("failed to gob encode LogEntry: %w", err)
	}
	return buf.Bytes(), nil
}

// deserializeEntry reconstructs a LogEntry from bytes.
func deserializeEntry(data []byte) (*LogEntry, error) {
	var entry LogEntry
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&entry); err != nil {
		return nil, fmt.Errorf("failed to gob decode LogEntry: %w", err)
	}
	return &entry, nil
}
