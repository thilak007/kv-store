package raft

import (
	"encoding/json"
	"time"
)

// EntryType represents the type of a Raft log entry.
type EntryType uint8

const (
	// EntryTypeNormal is a regular KV command.
	EntryTypeNormal EntryType = iota
	// EntryTypeNoOp is a no-op entry written when a leader is elected.
	EntryTypeNoOp
	// EntryTypeConfig is a configuration change entry (reserved for future use).
	EntryTypeConfig
)

// LogEntry represents a single entry in the Raft log.
//
// This is the core unit of replication. Each entry contains a command
// that will be applied to the state machine once committed.
//
// Structure:
//
//	┌─────────────────────────────────────────────┐
//	│  Term:      The election term this entry    │
//	│             belongs to.                     │
//	├─────────────────────────────────────────────┤
//	│  Index:     Position in the log (1-based).  │
//	├─────────────────────────────────────────────┤
//	│  Type:      Normal / NoOp / Config          │
//	├─────────────────────────────────────────────┤
//	│  Command:   Serialized KV operation         │
//	├─────────────────────────────────────────────┤
//	│  Timestamp: When the entry was created      │
//	└─────────────────────────────────────────────┘
type LogEntry struct {
	Term      uint64    // Raft term number when entry was created
	Index     uint64    // Position in the log (1-based, monotonically increasing)
	Type      EntryType // Type of entry (Normal, NoOp, Config)
	Command   []byte    // Serialized KV command (e.g., PUT, GET, DELETE)
	Timestamp time.Time // When the client submitted the command
}

// Command represents a KV operation that can be applied to the state machine.
// This is serialized into LogEntry.Command.
type Command struct {
	Op     string `json:"op"`                // "PUT", "GET", "SWAP", "DELETE", "SCAN"
	Key    string `json:"key"`               // Primary key (or start key for SCAN)
	Value  string `json:"value,omitempty"`   // Only for PUT/SWAP
	EndKey string `json:"end_key,omitempty"` // Only for SCAN
}

// Serialize converts a Command to bytes for storage in a LogEntry.
func (c *Command) Serialize() ([]byte, error) {
	return json.Marshal(c)
}

// DeserializeCommand parses a Command from bytes stored in a LogEntry.
func DeserializeCommand(data []byte) (*Command, error) {
	var cmd Command
	err := json.Unmarshal(data, &cmd)
	return &cmd, err
}

// IsNoOp returns true if this entry is a no-op.
func (e *LogEntry) IsNoOp() bool {
	return e.Type == EntryTypeNoOp
}

// IsNormal returns true if this entry is a regular KV command.
func (e *LogEntry) IsNormal() bool {
	return e.Type == EntryTypeNormal
}
