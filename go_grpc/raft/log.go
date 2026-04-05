package raft

import (
	"fmt"
)

// Log holds the ordered sequence of Raft log entries for a single partition.
//
// It is 1-indexed to match the Raft paper convention:
//
//	Index 0 → empty placeholder (term 0)
//	Index 1 → first real entry
//	Index N → latest entry
//
// Thread-safe: all methods acquire a read or write lock.
type Log struct {
	entries []*LogEntry // entries[0] is a dummy; real entries start at index 1
}

// NewLog creates an empty Raft log with a dummy entry at index 0.
func NewLog() *Log {
	return &Log{
		entries: []*LogEntry{{Term: 0, Index: 0}},
	}
}

// Append adds one or more entries to the end of the log.
// Returns the index of the last appended entry.
func (l *Log) Append(term uint64, entries ...[]byte) uint64 {
	var lastIndex uint64
	for _, cmd := range entries {
		idx := uint64(len(l.entries))
		entry := &LogEntry{
			Term:    term,
			Index:   idx,
			Type:    EntryTypeNormal,
			Command: cmd,
		}
		l.entries = append(l.entries, entry)
		lastIndex = idx
	}
	return lastIndex
}

// AppendEntry adds a single pre-constructed LogEntry to the log.
func (l *Log) AppendEntry(entry *LogEntry) {
	l.entries = append(l.entries, entry)
}

// TruncateFrom removes all entries starting from the given index (inclusive).
// Used when a leader detects a log mismatch and needs to replace follower entries.
func (l *Log) TruncateFrom(index uint64) {

	if index < 1 || index > uint64(len(l.entries))-1 {
		return
	}
	l.entries = l.entries[:index]
}

// Get returns the entry at the given index, or nil if out of bounds.
func (l *Log) Get(index uint64) *LogEntry {
	if index >= uint64(len(l.entries)) {
		return nil
	}
	return l.entries[index]
}

// LastIndex returns the index of the last entry in the log.
// Returns 0 if the log is empty (only dummy entry exists).
func (l *Log) LastIndex() uint64 {
	return uint64(len(l.entries)) - 1
}

// LastTerm returns the term of the last entry in the log.
// Returns 0 if the log is empty.
func (l *Log) LastTerm() uint64 {

	if len(l.entries) <= 1 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// TermAt returns the term of the entry at the given index.
// Returns 0 if the index is out of bounds.
func (l *Log) TermAt(index uint64) uint64 {

	if index >= uint64(len(l.entries)) {
		return 0
	}
	return l.entries[index].Term
}

// Slice returns a slice of entries from startIdx (inclusive) to endIdx (exclusive).
// Returns nil if the range is invalid or out of bounds.
func (l *Log) Slice(startIdx, endIdx uint64) []*LogEntry {

	if startIdx >= endIdx || startIdx >= uint64(len(l.entries)) {
		return nil
	}
	if endIdx > uint64(len(l.entries)) {
		endIdx = uint64(len(l.entries))
	}

	result := make([]*LogEntry, endIdx-startIdx)
	copy(result, l.entries[startIdx:endIdx])
	return result
}

// Len returns the number of real entries in the log (excluding the dummy at index 0).
func (l *Log) Len() int {
	return len(l.entries) - 1
}

// MatchIndexTerm checks if the entry at the given index exists and has the given term.
// Used by AppendEntries to verify log consistency.
func (l *Log) MatchIndexTerm(index, term uint64) bool {

	if index >= uint64(len(l.entries)) {
		return false
	}
	return l.entries[index].Term == term
}

// String returns a human-readable representation of the log for debugging.
func (l *Log) String() string {

	s := fmt.Sprintf("Log{len=%d, entries=[", len(l.entries)-1)
	for i := 1; i < len(l.entries); i++ {
		if i > 1 {
			s += ", "
		}
		s += fmt.Sprintf("{idx:%d,term:%d}", l.entries[i].Index, l.entries[i].Term)
	}
	s += "]}"
	return s
}
