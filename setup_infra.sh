#!/bin/bash
set -e

# =============================================================================
# Configuration (shared between fuzz and ycsb)
# =============================================================================
MANAGERS="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668"
MANAGER_P2PS="127.0.0.1:3606,127.0.0.1:3607,127.0.0.1:3608"
SERVERS="127.0.0.1:3777,127.0.0.1:3778,127.0.0.1:3779,127.0.0.1:3780,127.0.0.1:3781,127.0.0.1:3877,127.0.0.1:3878,127.0.0.1:3879,127.0.0.1:3880,127.0.0.1:3881"
SERVER_P2PS="127.0.0.1:3707,127.0.0.1:3708,127.0.0.1:3709,127.0.0.1:3710,127.0.0.1:3711,127.0.0.1:3807,127.0.0.1:3808,127.0.0.1:3809,127.0.0.1:3810,127.0.0.1:3811"
IP="127.0.0.1"
BACKER="./backer"

# Number of partitions (p0 = s0.*, p1 = s1.*)
NUM_PARTITIONS=1
server_RF=5

# =============================================================================
# Helper: setup manager + service nodes
# Usage: setup_infrastructure <server_rf>
# =============================================================================
setup_infrastructure() {
    local server_rf=$1

    echo "  Starting manager (server_rf=${server_rf})..."
    just p3::manager "0" "3666" "3606" \
        "127.0.0.1:3607,127.0.0.1:3608" \
        "${server_rf}" \
        "${SERVERS}" \
        "./backer.m.0" &

    echo "  Starting service nodes (server_rf=${server_rf})..."
    for (( part=0; part<NUM_PARTITIONS; part++ )); do
        for (( i=0; i<server_rf; i++ )); do
            local node="s${part}.${i}"
            just p3::service "${node}" \
                "${MANAGERS}" \
                "${MANAGER_P2PS}" \
                "${server_rf}" \
                "${IP}" \
                "${SERVERS}" \
                "${SERVER_P2PS}" \
                "${BACKER}" &
        done
    done

    # Wait for services to register
    sleep 3
}
setup_infrastructure "${server_rf}"