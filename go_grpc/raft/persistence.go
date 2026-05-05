package raft

import (
	"encoding/binary"
	"fmt"
	"log"

	"github.com/cockroachdb/pebble"
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

// persistState incrementally writes only changed log entries to PebbleDB.
// Must be called with raftmu held.
//
// Strategy:
//   - persistedUpTo tracks the highest index known to be correctly on disk.
//   - On truncation, persistedUpTo is lowered to the truncation point.
//   - On each call, we write entries [persistedUpTo+1 .. memLen] and delete
//     any stale trailing entries [memLen+1 .. diskLen].
func (rf *RaftNode) persistState() error {
	if err := rf.db.Set(raftKVKey(rf.raftBucket, "currentTerm"), b64(rf.currentTerm), pebble.Sync); err != nil {
		return fmt.Errorf("failed to persist currentTerm: %w", err)
	}
	if err := rf.db.Set(raftKVKey(rf.raftBucket, "votedFor"), []byte(rf.votedFor), pebble.Sync); err != nil {
		return fmt.Errorf("failed to persist votedFor: %w", err)
	}
	if err := rf.db.Set(raftKVKey(rf.raftBucket, "snapshotIndex"), b64(rf.snapshotIndex), pebble.Sync); err != nil {
		return fmt.Errorf("failed to persist snapshotIndex: %w", err)
	}
	if err := rf.db.Set(raftKVKey(rf.raftBucket, "snapshotTerm"), b64(rf.snapshotTerm), pebble.Sync); err != nil {
		return fmt.Errorf("failed to persist snapshotTerm: %w", err)
	}

	if len(rf.snapshotBuf) > 0 {
		if err := rf.db.Set(raftKVKey(rf.raftBucket, "snapshotBuf"), rf.snapshotBuf, pebble.Sync); err != nil {
			return fmt.Errorf("failed to persist snapshotBuf: %w", err)
		}
	} else {
		if err := rf.db.Delete(raftKVKey(rf.raftBucket, "snapshotBuf"), pebble.Sync); err != nil && err != pebble.ErrNotFound {
			return fmt.Errorf("failed to delete snapshotBuf: %w", err)
		}
	}

	// ── Incremental Log Persistence ────────────────────────────
	memLen := rf.log.LastIndex()
	diskLen := uint64(0)
	if v, ok, err := getRaftValue(rf.db, rf.raftBucket, "logLen"); err != nil {
		return err
	} else if ok {
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
		if err := rf.db.Set(raftKVKey(rf.raftBucket, fmt.Sprintf("log.%d", i)), data, pebble.Sync); err != nil {
			return fmt.Errorf("failed to persist log entry %d: %w", i, err)
		}
	}

	// Delete stale trailing entries that no longer exist in memory
	for i := memLen + 1; i <= diskLen; i++ {
		if err := rf.db.Delete(raftKVKey(rf.raftBucket, fmt.Sprintf("log.%d", i)), pebble.Sync); err != nil && err != pebble.ErrNotFound {
			return fmt.Errorf("failed to delete stale log entry %d: %w", i, err)
		}
	}

	// Update log length and persistence watermark
	if err := rf.db.Set(raftKVKey(rf.raftBucket, "logLen"), b64(memLen), pebble.Sync); err != nil {
		return fmt.Errorf("failed to persist log length: %w", err)
	}
	rf.persistedUpTo = memLen

	return nil
}

// LoadState reads currentTerm, votedFor, and the log from PebbleDB.
// Must be called before Start() during server initialization.
func (rf *RaftNode) LoadState() error {
	if v, ok, err := getRaftValue(rf.db, rf.raftBucket, "currentTerm"); err != nil {
		return err
	} else if ok {
		rf.currentTerm = u64(v)
	}
	if v, ok, err := getRaftValue(rf.db, rf.raftBucket, "votedFor"); err != nil {
		return err
	} else if ok {
		rf.votedFor = string(v)
	}
	if v, ok, err := getRaftValue(rf.db, rf.raftBucket, "snapshotIndex"); err != nil {
		return err
	} else if ok {
		rf.snapshotIndex = u64(v)
	}
	if v, ok, err := getRaftValue(rf.db, rf.raftBucket, "snapshotTerm"); err != nil {
		return err
	} else if ok {
		rf.snapshotTerm = u64(v)
	}
	if v, ok, err := getRaftValue(rf.db, rf.raftBucket, "snapshotBuf"); err != nil {
		return err
	} else if ok {
		rf.snapshotBuf = v
	}

	if v, ok, err := getRaftValue(rf.db, rf.raftBucket, "logLen"); err != nil {
		return err
	} else if ok {
		logLen := u64(v)
		for i := uint64(1); i <= logLen; i++ {
			v, entryOK, err := getRaftValue(rf.db, rf.raftBucket, fmt.Sprintf("log.%d", i))
			if err != nil {
				return err
			}
			if !entryOK {
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
	} else {
		log.Printf("[Node %s] No persisted state found in bucket %s (first run)",
			rf.nodeId, rf.raftBucket)
	}

	log.Printf("[Node %s] Loaded state from disk: term=%d, votedFor=%s, snapshotIndex=%d, logLen=%d",
		rf.nodeId, rf.currentTerm, rf.votedFor, rf.snapshotIndex, rf.log.Len())

	return nil
}

func raftKVKey(bucket string, key string) []byte {
	return []byte(bucket + "/" + key)
}

func getRaftValue(db *pebble.DB, bucket string, key string) ([]byte, bool, error) {
	v, closer, err := db.Get(raftKVKey(bucket, key))
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("failed to read %s/%s: %w", bucket, key, err)
	}
	defer closer.Close()

	out := make([]byte, len(v))
	copy(out, v)
	return out, true, nil
}

// b64 converts uint64 to 8-byte big-endian for PebbleDB storage.
func b64(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// u64 converts 8-byte big-endian from PebbleDB back to uint64.
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
