# CS 739 MadKV Project 2

**Group members**: 
- Name `Gokulnath Sourirajan`,  Email `sourirajan@wisc.edu`
- Name `Thilak Raj Murugan`,    Email `tmurugan2@wisc.edu`


## Design Walkthrough

### Overview

Durability and Paritioning of key space is introduced to the system in the following manner:

- **Durability**: 
  - Added durable storage using BoltDB (bbolt fork): https://github.com/etcd-io/bbolt. Bolt is a pure Go, embedded key/value storage engine based on a memory-mapped B+tree. It is designed for fast read access, ACID compliance, and simplicity.
  - Updates are first persisted on disk before updating the in-memory map data structure.
- **Partition**: 
  - We use range based partitioning. 
  - Before each RPC call, the client uses range partitioning to find the correct partition to make a RPC. 

### Client:

- During the client initialization, the clients gets the list of servers from the manager and connects to all the servers. 

### Server:

- When the server boots up, it registers itself with the manager and obtains the partition ID.
- Given a disk file path to durably persist the in-memory key-value pairs, we perform the following:

  1. Create a directory to store the file for the DB if it doesn't exist. bbolt mmaps the entire file to memory. This provides fast disk I/O.
  2. The key-value pairs are stored in a bucket within a file for a single partition.
  3. After registering with the manager, the server loads the key-value pairs present on disk, which is initially empty.

Whenever a new command arrives, bbolt uses transactions to perform a read or write. This ensures ACID properties. Updated are first persisted on disk before updating the in-memory map data structure.

### Proto: 
 
#### Add new service for manager with 2 functions:
 - RegisterServer - Used by kv store server to register it be part of the cluster whenever the server gets initiated. Returns partion ID of the server.
 - GetPartitionMap - Used by clients to get the list of servers part of the cluster. A map of partition ID and the server address is returned. Using this partition map, the clients connect to the servers.

## Self-provided Testcase

You will run the described testcase during demo time.

### Explanations

- No. of servers: 3
- No. of clients: 1
- Test1: PUT 9 keys, SWAP some (key0 ... key9)
- Test2: GET the 9 keys, SCAN some
- Test3: GET and SCAN only keys from 2nd and 3rd servers
- Test4: GET and SCAN only keys from 1st server
- Test5: GET, SCAN and SWAP keys from all servers

**Procedure**:
1. Start 3 servers.
2. Run Test1
    Observed: all 9 keys are PUT successfully, and SWAP works as expected.
3. Run Test2
    Observed: all 9 keys are GET successfully, and SCAN works as expected.
4. Kill the 1st server.
5. Run Test3
    Observed: all keys from 2nd and 3rd servers are GET successfully, and SCAN works as expected.
6. Run Test4
    Observed: Client fails to GET and SCAN keys from the 1st server, as expected. Indefinitely waits for the 1st server to recover, retrying indefinitely.
7. Restart the 1st server.
    Observed: the 1st server recovers successfully, and RPCs from Test4 completes.
8. Run Test5
    Observed: all keys from all servers are GET successfully, and SCAN and SWAP work as expected,successfully regaining connectivity to the 1st server.

## Fuzz Testing

<u>Parsed the following fuzz testing results:</u>

num_servers | crashing | outcome
:-: | :-: | :-:
3 | no | PASSED
3 | yes | PASSED
5 | yes | PASSED

You will run a crashing/recovering fuzz test during demo time.

### Comments

We observed that the server correctly handles concurrent operations on shared keys, maintaining linearizability and returning consistent results. The fuzz testing revealed no assertion failures, indicating that our concurrency control mechanisms are robust under various workloads and contention levels.

We also noticed that crashing few partitions blocked the fuzz test, as it was retrying the request till a successful response was returned by restarting the crashed server. After restarting the server, the fuzz test completed successfully. 

## YCSB Benchmarking

<u>10 clients throughput/latency across workloads & number of partitions:</u>

![ten-clients](plots-p2/ycsb-ten-clients.png)

<u>Agg. throughput trend vs. number of clients w/ and w/o partitioning:</u>

![tput-trend](plots-p2/ycsb-tput-trend.png)

### Comments

*FIXME: add your discussions of benchmarking results*

## Additional Optimizations

- Better range partitioning to distribute load in a balanced manner.
- SCAN Optimization: TODO


