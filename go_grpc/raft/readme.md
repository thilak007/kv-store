# Raft Library

## Overview

This package implements the Raft consensus algorithm for state machine replication of the key-value store. Each partition has `r` replicas that form an independent Raft group.

## How It Wires Together

```
Client: PUT foo bar
    │
    ▼ hashKey("foo") → partition 0
    │
    ▼ gRPC → ANY replica of partition 0  (s0.0, s1.0, or s2.0)
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
---

## Apply Loop

The apply loop is a background goroutine that takes committed log entries and executes them on the KV store.