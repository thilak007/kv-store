# p1 Testing in local
just p1::test1

just p1::test4

just p1::fuzz 1 "no" "127.0.0.1:3777"
just p1::fuzz 1 "yes" "127.0.0.1:3777"

just p1::fuzz 2 "no" "127.0.0.1:3777"
just p1::fuzz 2 "yes" "127.0.0.1:3777"

# p2 testing in Clouldlab

# Server & Manager IP - 10.10.1.2

### Client side :
just p2::fuzz "2" "no" "10.10.1.2:3666"
##################################################################

### Server side :
just p2::setupcluster 2 10.10.1.2

# Note - Remote disk file after each run of server
just p2::kill


############
# Fuzz testing with 5 services and 1 manager, for demo-ing in local
#############
just p2::manager 3666 "127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781"

just p2::service s0 "127.0.0.1:3666" "127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781"
just p2::service s1 "127.0.0.1:3666" "127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781"
just p2::service s2 "127.0.0.1:3666" "127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781"
just p2::service s3 "127.0.0.1:3666" "127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781"
just p2::service s4 "127.0.0.1:3666" "127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781"


just p2::fuzz 5 "yes" 


###########
# Start cluster and do custom test

just p2::manager 3666 "127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779"

just p2::service s0 "127.0.0.1:3666" "127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779"
just p2::service s1 "127.0.0.1:3666" "127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779"
just p2::service s2 "127.0.0.1:3666" "127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779"

# Step 2-3: Run test1 and test2
./bin/client localhost:3666 "fuzz" < tests2/test1.txt

./bin/client localhost:3666 "fuzz" < tests2/test2.txt

# Kill server 0
# Do a Get and a Scan on unaffected partitions; both should succeed
./bin/client localhost:3666 "fuzz" < tests2/test3.txt

# Do a Get or a Scan that involves the failed partition; should timeout
./bin/client localhost:3666 "fuzz" < tests2/test4.txt

### Restart server 0
just p2::service s0 "127.0.0.1:3666" "127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779"

# Do a Get to an existing fuzz in the just-recovered partition; should succeed and return its latest put value
./bin/client localhost:3666 "fuzz" < tests2/test5.txt

### Partition mapping for reference:
# 0,1,2,3 -> Partition 0
# 4,5,6 -> Partition 1
# 7,8,9 -> Partition 2