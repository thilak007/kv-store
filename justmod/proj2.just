set working-directory := '..'

# list project 2 just recipes
default:
    @just --list p2 --unsorted

# install extra dependencies of your KV code
deps:
    cd go_grpc && go mod tidy

# build your executables in release mode
build: deps
    cd go_grpc/server && go build -o ../../bin/server main.go
    cd go_grpc/client && go build -o ../../bin/client main.go
    cd go_grpc/manager && go build -o ../../bin/manager main.go

# clean the build of your executables
clean:
    rm -rf bin/

# run your KV store manager
manager man_port="3666" servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779":
    ./bin/manager 0.0.0.0:{{man_port}} "{{servers}}"

# run your KV store server
server id="0" manager="127.0.0.1:3666" api_port="3777" backer_path="./backer.s0" api_ip="127.0.0.1":
    ./bin/server {{manager}} {{api_ip}}:{{api_port}} {{id}} {{backer_path}}

# run you KV store client in stdin/out interface mode
client manager="127.0.0.1:3666" keyspace="random":
    ./bin/client {{manager}} {{keyspace}}

# kill all processes of your KV store system
kill:
    -pkill -f './bin/server' > /dev/null 2>&1
    -pkill -f './bin/client' > /dev/null 2>&1
    -pkill -f './bin/manager' > /dev/null 2>&1
    rm -rf ./backer.*

# NOTE: feel free to add more recipes as you see fit...
#       also feel free to add extra parameters to the recipes as you see fit,
#       but don't change the existing parameters

python_run := if `which uv || true` != "" { "uv run" } else { "python3" }
tmpdir_prefix := "/tmp/madkv-p2"

# launch the KV service components on node (see README for conventions)
service node="s0" manager="127.0.0.1:3666" servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779" backer_prefix="./backer" api_ip="127.0.0.1":
    just p2::build
    just utils::build
    cargo run -p runner -r --bin service -- \
        --server-just-args p2::server \
            $({{python_run}} justmod/xtract.py partid "{{node}}") \
            "{{manager}}" \
            $({{python_run}} justmod/xtract.py portof "{{servers}}" "{{node}}") \
            "{{backer_prefix}}.{{node}}" {{api_ip}} \
        --manager-just-args p2::manager \
            $({{python_run}} justmod/xtract.py portof "{{manager}}" "{{node}}") \
            "{{servers}}" \
        --node-id "{{node}}"

# launch all the KV servers 
allservice manager="127.0.0.1:3666" servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779" backer_prefix="./backer" api_ip="127.0.0.1":
    #!/usr/bin/env bash
    IFS=',' read -ra ADDRS <<< "{{servers}}"
    for i in "${!ADDRS[@]}"; do
        just p2::service "s$i" "{{manager}}" "{{servers}}" "{{backer_prefix}}" "{{api_ip}}" &
    done
    wait

setupcluster nservers="1" server_ip="127.0.0.1":
    #!/usr/bin/env bash
    servers=""
    for i in $(seq 0 $(({{nservers}} - 1))); do
        port=$((3777 + i))
        if [ $i -gt 0 ]; then
            servers="${servers},"
        fi
        servers="${servers}{{server_ip}}:${port}"
    done
    just p2::manager "3666" "$servers" &
    just p2::allservice "{{server_ip}}:3666" "$servers" "./backer" "{{server_ip}}"

# ensure a subdir under 'tmp/' exists
tmpdir subdir:
    mkdir -p "{{tmpdir_prefix}}/{{subdir}}"

# run a fuzz testing scenario
fuzz nservers="1" crashing="no" manager="127.0.0.1:3666" conflict="yes": (tmpdir "fuzz")
    just p2::build
    just utils::build
    cargo run -p runner -r --bin fuzzer -- \
        --num-clis 5 \
        {{ if conflict == "yes" { "--conflict" } else { "" } }} \
        --client-just-args p2::client "{{manager}}" {{ if conflict == "yes" { "fuzz" } else { "random" } }} \
        | tee "{{tmpdir_prefix}}/fuzz/fuzz-{{nservers}}-{{crashing}}.log"
    just p2::kill

# run a YCSB benchmark workload
bench nclis wload nservers="1" manager="127.0.0.1:3666": (tmpdir "bench")
    just p2::build
    just utils::build
    just utils::ycsb
    cargo run -p runner -r --bin bencher -- \
        --num-clis "{{nclis}}" \
        --workload "{{wload}}" \
        --client-just-args p2::client "{{manager}}" "ycsb"\
        | tee "{{tmpdir_prefix}}/bench/bench-{{nclis}}-{{wload}}-{{nservers}}.log"
    just p2::kill

# generate .md report template from existing results (wip)
report:
    {{python_run}} sumgen/proj2.py
