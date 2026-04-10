# Todo p3

## Server side 
1. Add success logs,
2. Add channel to send back response to client from the server
3. Implement Delete

## Client Side:
1. Update args based on just file expectation
2. Update just file
3. Find the leader
4. If a leader goes down, then the client has to retry and connect to a new leader

## Manager side:

1. Registration of multiple servers for a single partition.
Note: We don't support dynamic addition of servers to a cluster in between. 
