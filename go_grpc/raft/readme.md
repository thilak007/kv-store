### Raft implementation

## Todo: decide if we need EntryType as a field in LogEntry

In Raft, a no-op entry is written by a new leader immediately after election. Its purpose:

* Leader elected → writes no-op → no-op committed → all previous entries implicitly committed

This ensures:

1. Committing old entries — A leader can only commit entries from its own term. The no-op commits all uncommitted entries from previous terms.
2. Log consistency — Guarantees the leader has a known committed point.

But — this is an optimization. Many simple Raft implementations skip it initially. If you want to keep it simple, we can remove it.