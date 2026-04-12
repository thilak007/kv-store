# Todo p3

## Server side 
1. Fix arguments

## Client Side:
1. Update args based on just file expectation
2. Update just file
3. Find the leader
4. If a leader goes down, then the client has to retry and connect to a new leader

## Manager side:

1. Registration of multiple servers for a single partition.
Note: We don't support dynamic addition of servers to a cluster in between. 

## bugs:

- Why is there a  100  when clearing old logs?

https://github.com/thilak007/kv-store/blob/c877d368186083f95022d8280e01c379891ab264/go_grpc/raft/persistence.go#L65  