# CS 739 MadKV Project 3

**Group members**: Name `email`, Name `email`

## Design Walkthrough

*FIXME: add your design walkthrough text*

## Self-provided Testcases

You will run the four described testcase scenarios during demo time.

### Explanations

## Server Changes:

1. Initialize RaftNode with node ID, peer IDs, state machine, BoltDB instance, and gRPC clients for peers.
2. Load persisted Raft state (currentTerm, votedFor, log entries) from BoltDB before starting the node.
3. Start background goroutines for Raft: apply loop, replication/heartbeat loop, and election timer loop.
4. Create another gRPC server for Raft RPCs (RequestVote, AppendEntries, InstallSnapshot).
4. Implement gRPC handlers for KV operations (Put, Get, Swap, Delete, Scan) that:
	 - Check if the node is the leader; if not, return the current leader ID for redirection.
	 - For write operations (Put, Swap, Delete), propose a command to the Raft node and wait for the response from the state machine via a channel.
	 - For read operations (Get, Scan), serve directly from the in-memory map protected by a mutex.

## Fuzz Testing

<u>Parsed the following fuzz testing results:</u>

server_rf | crashing | outcome
:-: | :-: | :-:
5 | no | PASSED
5 | yes | PASSED

You may be asked to run a crashing fuzz test during demo time.

### Comments

*FIXME: add your comments on fuzz testing*

## YCSB Benchmarking

<u>10 clients throughput/latency across workloads & replication factors:</u>

![ten-clients](plots-p3/ycsb-ten-clients.png)

<u>Agg. throughput trend vs. number of clients with different replication factors:</u>

![tput-trend](plots-p3/ycsb-tput-trend.png)

### Comments

*FIXME: add your discussions of benchmarking results*

## Additional Discussion

*OPTIONAL: add extra discussions if applicable*

