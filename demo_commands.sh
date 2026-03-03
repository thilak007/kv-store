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
