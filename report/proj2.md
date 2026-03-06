# CS 739 MadKV Project 2

**Group members**: `email`, Name `email`

## Design Walkthrough

*FIXME: add your design walkthrough text*

## Self-provided Testcase

You will run the described testcase during demo time.

### Explanations

No. of servers: 3
No. of clients: 1
Test1: PUT 9 keys, SWAP some (key0 ... key9)
Test2: GET the 9 keys, SCAN some
Test3: GET and SCAN only keys from 2nd and 3rd servers
Test4: GET and SCAN only keys from 1st server
Test5: GET, SCAN and SWAP keys from all servers

Procedure:
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

*FIXME: add your comments on fuzz testing*

## YCSB Benchmarking

<u>10 clients throughput/latency across workloads & number of partitions:</u>

![ten-clients](plots-p2/ycsb-ten-clients.png)

<u>Agg. throughput trend vs. number of clients w/ and w/o partitioning:</u>

![tput-trend](plots-p2/ycsb-tput-trend.png)

### Comments

*FIXME: add your discussions of benchmarking results*

## Additional Discussion

*OPTIONAL: add extra discussions if applicable*

