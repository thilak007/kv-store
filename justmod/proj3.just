set working-directory := '..'

# list project 3 just recipes
default:
    @just --list p3 --unsorted

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

# run your KV store manager replica
manager rep_id="0" \
        man_port="3666" \
        p2p_port="3606" \
        peers="127.0.0.1:3607,127.0.0.1:3608" \
        server_rf="3" \
        servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781,127.0.0.1:3782"\
        backer_path="./backer.m.0":
    ./bin/manager --replica_id {{rep_id}} --man_listen 0.0.0.0:{{man_port}} --p2p_listen 0.0.0.0:{{p2p_port}} --peer_addrs {{peers}} --server_rf {{server_rf}} --server_addrs "{{servers}}" --backer_path {{backer_path}}


# run your KV store server replica
server part_id="0" \
       rep_id="0" \
       managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
       ip="127.0.0.1" \
       api_port="3777" \
       p2p_port="3707" \
       peers="127.0.0.1:3708,127.0.0.1:3709" \
       backer_path="./backer.s0.0":
    ./bin/server {{part_id}} {{rep_id}} {{managers}} {{ip}}:{{api_port}} {{ip}}:{{p2p_port}} {{peers}} {{backer_path}}

# run you KV store client in stdin/out interface mode
client managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" keyspace="random":
    ./bin/client {{managers}} {{keyspace}}

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
tmpdir_prefix := "/tmp/madkv-p3"

# launch the KV service components on node (see README for conventions)
service node="s0.0" \
        managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668" \
        manager_p2ps="127.0.0.1:3606,127.0.0.1:3607,127.0.0.1:3608" \
        server_rf="3" \
        ip="127.0.0.1" \
        servers="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781,127.0.0.1:3782" \
        server_p2ps="127.0.0.1:3707,127.0.0.1:3708,127.0.0.1:3709,127.0.0.1:3710,127.0.0.1:3711,127.0.0.1:3712" \
        backer_prefix="./backer":
    just p3::build
    just utils::build
    cargo run -p runner -r --bin service -- \
        --server-just-args p3::server \
            $({{python_run}} justmod/xtract.py partid "{{node}}") \
            $({{python_run}} justmod/xtract.py repid "{{node}}") \
            "{{managers}}" \
            "{{ip}}" \
            $({{python_run}} justmod/xtract.py portof "{{servers}}" "{{node}}" "{{server_rf}}") \
            $({{python_run}} justmod/xtract.py portof "{{server_p2ps}}" "{{node}}" "{{server_rf}}") \
            $({{python_run}} justmod/xtract.py peersof "{{server_p2ps}}" "{{node}}" "{{server_rf}}") \
            "{{backer_prefix}}.{{node}}" \
        --manager-just-args p3::manager \
            $({{python_run}} justmod/xtract.py repid "{{node}}") \
            $({{python_run}} justmod/xtract.py portof "{{managers}}" "{{node}}") \
            $({{python_run}} justmod/xtract.py portof "{{manager_p2ps}}" "{{node}}") \
            $({{python_run}} justmod/xtract.py peersof "{{manager_p2ps}}" "{{node}}") \
            "{{server_rf}}" \
            "{{servers}}" \
            "{{backer_prefix}}.{{node}}" \
        --node-id "{{node}}"

# ensure a subdir under 'tmp/' exists
tmpdir subdir:
    mkdir -p "{{tmpdir_prefix}}/{{subdir}}"

# run a fuzz testing scenario
fuzz server_rf="5" crashing="no" \
     managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668": (tmpdir "fuzz")
    just p3::build
    just utils::build
    cargo run -p runner -r --bin fuzzer -- \
        --num-clis 5 \
        --conflict \
        --client-just-args p3::client "{{managers}}"  "fuzz" \
        | tee "{{tmpdir_prefix}}/fuzz/fuzz-{{server_rf}}-{{crashing}}.log"
    just p3::kill

# run a YCSB benchmark workload
bench nclis wload server_rf="3" \
      managers="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668": (tmpdir "bench")
    just p3::build
    just utils::build
    just utils::ycsb
    cargo run -p runner -r --bin bencher -- \
        --num-clis "{{nclis}}" \
        --workload "{{wload}}" \
        --client-just-args p3::client "{{managers}}"  "ycsb"\
        | tee "{{tmpdir_prefix}}/bench/bench-{{nclis}}-{{wload}}-{{server_rf}}.log"
    # just p3::kill

# generate .md report template from existing results (wip)
report:
    {{python_run}} sumgen/proj3.py
