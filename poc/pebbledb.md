# PebbleDB Migration Breakdown

## Where `bbolt` is actually used

The migration surface is concentrated in a few places:

- Dependency wiring: [go.mod](/Users/thilak/spring-26/kv-store/go_grpc/go.mod:1), [go.sum](/Users/thilak/spring-26/kv-store/go_grpc/go.sum:1)
- Raft node storage type: [raft/node.go](/Users/thilak/spring-26/kv-store/go_grpc/raft/node.go:82)
- Raft persistence implementation: [raft/persistence.go](/Users/thilak/spring-26/kv-store/go_grpc/raft/persistence.go:45)
- KV server persistence and startup loading: [server/main.go](/Users/thilak/spring-26/kv-store/go_grpc/server/main.go:35), [server/main.go](/Users/thilak/spring-26/kv-store/go_grpc/server/main.go:106), [server/main.go](/Users/thilak/spring-26/kv-store/go_grpc/server/main.go:517)

The client and manager do not use `bbolt` directly, so they likely do not need logic changes. They do matter for end-to-end validation after the swap.

## Recommended task breakdown

1. Define the Pebble-backed storage design before touching code.
   Decide whether to keep one Pebble DB per server process, shared by KV state and Raft state, like today.
   Replace `bbolt` buckets with key prefixes.
   Suggested prefixes:
   - `kv/<user-key>`
   - `raft/<raftBucket>/currentTerm`
   - `raft/<raftBucket>/votedFor`
   - `raft/<raftBucket>/snapshotIndex`
   - `raft/<raftBucket>/snapshotTerm`
   - `raft/<raftBucket>/snapshotBuf`
   - `raft/<raftBucket>/logLen`
   - `raft/<raftBucket>/log/<index>`

2. Introduce a storage abstraction instead of swapping imports inline.
   Today `RaftNode` and the server both depend on `*bolt.DB`.
   Create a small internal storage interface for:
   - `Get`
   - `Set`
   - `Delete`
   - `ScanPrefix` or iterator-based range scan
   - optional `Batch` or `WriteBatch`
   - `Close`
   This keeps Raft and server code independent of Pebble specifics.

3. Replace the server KV persistence path.
   Update [server/main.go](/Users/thilak/spring-26/kv-store/go_grpc/server/main.go:106) and [server/main.go](/Users/thilak/spring-26/kv-store/go_grpc/server/main.go:126) to write/delete keys via Pebble.
   Remove bucket assumptions; map `bucketName` to a key prefix or remove `bucketName` entirely.
   Keep the in-memory `records` map behavior unchanged first, then optimize later.

4. Replace server startup DB initialization and reload logic.
   Replace `bolt.Open(...)` in [server/main.go](/Users/thilak/spring-26/kv-store/go_grpc/server/main.go:517) with `pebble.Open(...)`.
   Remove `CreateBucketIfNotExists`.
   Replace the `db.View(... ForEach ...)` reload in [server/main.go](/Users/thilak/spring-26/kv-store/go_grpc/server/main.go:534) with a Pebble iterator over the `kv/` prefix.

5. Replace Raft’s DB type and constructor wiring.
   Change [raft/node.go](/Users/thilak/spring-26/kv-store/go_grpc/raft/node.go:82) and [raft/node.go](/Users/thilak/spring-26/kv-store/go_grpc/raft/node.go:97) so `RaftNode` receives the new storage abstraction or `*pebble.DB`.
   Update server construction path accordingly.

6. Reimplement `persistState()` for Pebble semantics.
   This is the core migration point: [raft/persistence.go](/Users/thilak/spring-26/kv-store/go_grpc/raft/persistence.go:45)
   Replace the `Update(tx)` block with Pebble writes, ideally using a batch so each persist call is atomic at the storage layer.
   Preserve current behavior:
   - write `currentTerm`, `votedFor`, snapshot metadata
   - append new log entries from `persistedUpTo+1`
   - delete stale trailing entries after truncation
   - update `logLen`
   - update `persistedUpTo`

7. Reimplement `LoadState()` for Pebble.
   Replace the `View(tx)` logic in [raft/persistence.go](/Users/thilak/spring-26/kv-store/go_grpc/raft/persistence.go:110)
   Read scalar metadata keys directly.
   Rebuild the log by iterating `1..logLen` or by scanning the `raft/.../log/` prefix.
   Be careful with key ordering if you scan lexicographically; fixed-width encoded indexes are safer than decimal strings.

8. Fix key encoding for Raft log entries.
   Right now log keys use `fmt.Sprintf("log.%d", i)`.
   With Pebble iterators, lexicographic ordering matters, so `log.10` sorts before `log.2`.
   Use big-endian 8-byte encoded indexes or zero-padded numeric strings for log keys.

9. Audit all Raft persistence call sites after the storage change.
   `persistState()` is called from election, replication, and RPC handlers:
   - [raft/election.go](/Users/thilak/spring-26/kv-store/go_grpc/raft/election.go:27)
   - [raft/replication.go](/Users/thilak/spring-26/kv-store/go_grpc/raft/replication.go:33)
   - [raft/handlers.go](/Users/thilak/spring-26/kv-store/go_grpc/raft/handlers.go:61)
   Make sure error handling remains acceptable. Right now several callers ignore `persistState()` errors; this is a good place to tighten that.

10. Update dependencies and build plumbing.
    Remove `go.etcd.io/bbolt` from [go.mod](/Users/thilak/spring-26/kv-store/go_grpc/go.mod:1)
    Add Pebble dependency.
    Regenerate `go.sum`.

11. Update docs and operational assumptions.
    Proposal/report should reflect the actual implementation approach.
    Update any README/run notes that still say BoltDB or bucket-based storage.
    Note that Pebble uses directory-based storage layout and different cleanup behavior.

12. Add migration verification tests.
    Minimum checks:
    - server restart preserves KV data
    - Raft restart preserves `currentTerm`, `votedFor`, log, and snapshot metadata
    - leader can still append/apply after restart
    - delete and overwrite paths still behave correctly
    If no test framework exists yet, at least add a repeatable manual test checklist.

13. Run end-to-end benchmarks for the project deliverable.
    Use the same workloads from the proposal.
    Compare:
    - throughput
    - mean/tail latency
    - restart/recovery time
    Keep the client and manager unchanged so the storage engine is the only real variable.

## Suggested implementation order

1. Add Pebble dependency and a thin storage wrapper.
2. Migrate server KV reads/writes and startup reload.
3. Migrate Raft `persistState()` and `LoadState()`.
4. Fix log key encoding and batch atomicity.
5. Run restart and Raft replication tests.
6. Run benchmark workloads and document results.

## Main risks to watch

- Pebble has no bucket abstraction, so prefix design must be clean.
- Lexicographic ordering will break Raft log scans if indexes are stored as plain decimal strings.
- `persistState()` is effectively relied on as atomic state persistence; use Pebble batches to preserve that behavior.
- Several Raft paths ignore persistence errors today, which may become more visible during migration.
