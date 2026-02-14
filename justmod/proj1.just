set working-directory := '..'

# list project 1 just recipes
default:
    @just --list p1 --unsorted

# install extra dependencies of your KV code
deps:
    cd go_grpc && go mod tidy

# build your executables in release mode
build:
    cd go_grpc/server && go build -o ../../bin/server main.go
    cd go_grpc/client && go build -o ../../bin/client main.go

# clean the build of your executables
clean:
    rm -rf bin/

# run your KV store server
server listen="0.0.0.0:3777":
    ./bin/server {{listen}}

# run you KV store client in stdin/out interface mode
client server="127.0.0.1:3777":
    ./bin/client {{server}}

# run your KV store testcase 1 client
test1 server="127.0.0.1:3777":
    ./bin/client {{server}} < tests/test1.txt > output/actual/out1.txt
    just p1::kill

    # Verify if output matches expected output
    diff -u output/expected/out1.txt output/actual/out1.txt

# run your KV store testcase 2 client
test2 server="127.0.0.1:3777":
    ./bin/client {{server}} < tests/test2.txt > output/actual/out2.txt
    just p1::kill

    # Verify if output matches expected output
    diff -u output/expected/out2.txt output/actual/out2.txt

# run your KV store testcase 3 client
test3 server="127.0.0.1:3777":
    ./bin/client {{server}} < tests/test3_a.txt > output/actual/out3_a.txt & \
    ./bin/client {{server}} < tests/test3_b.txt > output/actual/out3_b.txt & \
    wait
    just p1::kill

    # Verify if output matches expected output
    diff -u output/expected/out3_a.txt output/actual/out3_a.txt
    diff -u output/expected/out3_b.txt output/actual/out3_b.txt

# run your KV store testcase 4 client(s)
test4 server="127.0.0.1:3777":
    ./bin/client {{server}} < tests/test4_a.txt > output/actual/out4_a.txt & \
    ./bin/client {{server}} < tests/test4_b.txt > output/actual/out4_b.txt & \
    wait

    just p1::kill

# run your KV store testcase 5 client(s)
test5 server="127.0.0.1:3777":
    ./bin/client {{server}} < tests/test5_a.txt & \
    ./bin/client {{server}} < tests/test5_b.txt & \
    ./bin/client {{server}} < tests/test5_c.txt & \
    wait

# kill all processes of your KV store system
kill:
    # Comments:  kills all server, clients, and any extra helper
    #        processes of your system; redirecting both out & err to /dev/null is recommended
    #
    #   TIP: commands such as 'pkill' may return non-zero exit code on success,
    #        which would by default abort the 'just' recipe early; prepend the
    #        command with a '-' sign to ignore its exit code
    -pkill -f "bin/server" > /dev/null 2>&1
    -pkill -f "bin/client" > /dev/null 2>&1

# NOTE: feel free to add more recipes as you see fit...
#       also feel free to add extra parameters to the recipes as you see fit,
#       but don't change the existing parameters

python_run := if `which uv || true` != "" { "uv run" } else { "python3" }
tmpdir_prefix := "/tmp/madkv-p1"

# launch a long-running KV store service
service listen="0.0.0.0:3777":
    just p1::build
    just utils::build
    cargo run -p runner -r --bin service -- \
        --server-just-args p1::server "{{listen}}" \
        --manager-just-args none

# ensure a subdir under 'tmp/' exists
tmpdir subdir:
    mkdir -p "{{tmpdir_prefix}}/{{subdir}}"

# run a student-provided testcase
testcase num server="127.0.0.1:3777": (tmpdir "tests")
    just p1::build
    just "p1::test{{num}}" "{{server}}" \
        | tee "{{tmpdir_prefix}}/tests/test{{num}}.log"
    just p1::kill

# run a fuzz testing scenario
fuzz nclis conflict="no" server="127.0.0.1:3777": (tmpdir "fuzz")
    just p1::build
    just utils::build
    cargo run -p runner -r --bin fuzzer -- \
        --num-clis "{{nclis}}" \
        {{ if conflict == "no" { "" } else { "--conflict" } }} \
        --client-just-args p1::client "{{server}}" \
        | tee "{{tmpdir_prefix}}/fuzz/fuzz-{{nclis}}-{{conflict}}.log"
    just p1::kill

# run a YCSB benchmark workload
bench nclis wload server="127.0.0.1:3777": (tmpdir "bench")
    just p1::build
    just utils::build
    just utils::ycsb
    cargo run -p runner -r --bin bencher -- \
        --num-clis "{{nclis}}" \
        --workload "{{wload}}" \
        --client-just-args p1::client "{{server}}" \
        | tee "/tmp/madkv-p1/bench/bench-{{nclis}}-{{wload}}.log"
    just p1::kill

# generate .md report template from existing results (wip)
report:
    {{python_run}} sumgen/proj1.py
