# Raft Library for MadKV

## Overview

This package implements the Raft consensus algorithm for state machine replication of the MadKV key-value store. Each partition has `r` replicas that form an independent Raft group.

## Subtasks

Subtasks


    ┌────┬──────────────────────┬─────────┬──────────────────────────────────────────────────────────────────────────────────────┐
    │ #  │ Subtask              │ File    │ What                                                                                 │
    ├────┼──────────────────────┼─────────┼──────────────────────────────────────────────────────────────────────────────────────┤
    │ 1  │ RaftNode struct      │ node.go │ All Raft state: term, votedFor, log, commitIndex, lastApplied, role, peers, channels │
    │ 2  │ Constructor          │ node.go │ NewRaftNode(id, peers, kvStore, db) — takes your existing KV store + bbolt DB        │
    │ 3  │ Propose              │ node.go │ Propose(cmd Command) → result — entry point from KV server                           │
    │ 4  │ Election timer       │ node.go │ Randomized timeout → start election → RequestVote → become leader on majority        │
    │ 5  │ Handle RequestVote   │ node.go │ Follower receives vote request → grant/deny                                          │
    │ 6  │ Handle Vote Response │ node.go │ Count votes → if majority, become leader                                             │
    │ 7  │ Heartbeat ticker     │ node.go │ Periodic AppendEntries (empty) to maintain leadership                                │
    │ 8  │ Send AppendEntries   │ node.go │ Leader replicates entries to followers, handles log mismatch                         │
    │ 9  │ Handle AppendEntries │ node.go │ Follower validates + appends entries, updates commitIndex                            │
    │ 10 │ Advance commitIndex  │ node.go │ Majority replicated → update commitIndex                                             │
    │ 11 │ Apply loop           │ node.go │ Committed entries → call your KV store's Put/Delete/etc. → persist to bbolt          │
    └────┴──────────────────────┴─────────┴──────────────────────────────────────────────────────────────────────────────────────┘

---

## File Structure

```
go_grpc/raft/
├── log_entry.go       # LogEntry, Command (done)
├── log.go             # Log data structure (done)
├── node.go            # RaftNode struct, constructor, Propose(), apply loop
├── election.go        # Election timer, RequestVote, becomeLeader
├── replication.go     # Heartbeat, AppendEntries, commit tracking
└── config.go          # Timeout constants
```

---

## How It Wires Together

### Current Flow (Project 2)

```
Client: PUT foo bar
    │
    ▼ hashKey("foo") → partition 0
    │
    ▼ gRPC → server at partitionMap[0] (single server)
    │
    ▼ server.Put() handler in main.go
    │
    ▼ records["foo"] = "bar"
    ▼ db.Update(...)  ← direct write
    │
    ▼ return PutResponse
```

### With Raft (Project 3)

```
Client: PUT foo bar
    │
    ▼ hashKey("foo") → partition 0
    │
    ▼ gRPC → ANY replica of partition 0  (s0.0, s0.1, or s0.2)
    │
    ▼ server.Put() handler
    │
    ▼ raftNode.Propose(Command{Op:"PUT", Key:"foo", Value:"bar"})
    │     │
    │     ├─ If NOT leader → forward to leader or reject
    │     └─ If leader → append to log, replicate to peers
    │           │
    │           ▼ majority ACK → commit
    │           │
    │           ▼ apply loop → records["foo"] = "bar", db.Update(...)
    │
    ▼ return PutResponse
```

### What Changes

| Component | Before | After |
|-----------|--------|-------|
| **Proto (client-facing)** | `KVService.Put/Get/etc.` | **Unchanged** — client sees same API |
| **Server handler** | `records[key]=val; db.Update()` | `raftNode.Propose(cmd)` |
| **New** | — | `RaftService` gRPC for peer-to-peer (election + replication) |
| **Manager** | Returns 1 server per partition | Returns all replicas per partition |

### Key Insight

**The client proto doesn't change.** The `KVService` in `kv.proto` stays the same. What changes is:

1. **Server's `Put()` handler** — instead of writing directly, it proposes to Raft
2. **New `RaftService`** — internal gRPC for Raft peers to talk to each other (election, replication)

```
go_grpc/proto/
├── kv.proto      ← client-facing, unchanged
└── raft.proto    ← new, internal peer-to-peer only
```

---

## Data Flow (End-to-End)

```
1. Client → server.Put("foo", "bar")
2. server → raft.Propose(Command{Op:"PUT", Key:"foo", Value:"bar"})
3. Raft → append to log (index=5, term=1)
4. Raft → replicate to followers (parallel)
5. Majority ACK → commitIndex = 5
6. Raft → applyCh ← log.Get(5)
7. Apply loop → records["foo"] = "bar"; db.Update(...)
8. Apply loop → signal Propose() that it's done
9. Propose() → return result to client
```

---

## Apply Loop

The apply loop is a background goroutine that takes committed log entries and executes them on the KV store.

### Why a channel?

Multiple goroutines produce committed entries (one replication goroutine per follower), but they must be **applied in order by one goroutine**.

```
Goroutine A ──► applyCh ──┐
Goroutine B ──► applyCh ──┼──► Apply Loop (single goroutine)
Goroutine C ──► applyCh ──┘    1. Receives entries in order
                               2. Applies to KV store
                               3. Updates lastApplied
```

### Without channel (wrong)

- Race conditions on `records` map
- Out-of-order apply
- Replication blocked by disk I/O

### With channel (correct)

- Single goroutine owns the map
- Guaranteed in-order apply
- Replication decoupled from apply

---

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| **No unnecessary interfaces** | RaftNode does the work directly. KV store, bbolt, gRPC are concrete. |
| **Channels for apply** | Go idiomatic. Decouples replication from state machine apply. |
| **Goroutines per peer** | Each follower gets its own replication goroutine. |
| **No compaction yet** | Simpler. Can add later when log grows too large. |
| **bbolt for persistence** | Reuse existing storage. One file per partition. |



### Additional

### Raft implementation

## Todo: decide if we need EntryType as a field in LogEntry

In Raft, a no-op entry is written by a new leader immediately after election. Its purpose:

* Leader elected → writes no-op → no-op committed → all previous entries implicitly committed

This ensures:

1. Committing old entries — A leader can only commit entries from its own term. The no-op commits all uncommitted entries from previous terms.
2. Log consistency — Guarantees the leader has a known committed point.

But — this is an optimization. Many simple Raft implementations skip it initially. If you want to keep it simple, we can remove it.