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
NUM_PARTITIONS=2

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

# =============================================================================
# Helper: cleanup
# =============================================================================
cleanup_infrastructure() {
    just p3::kill
}

# =============================================================================
# FUZZ test runner
# Usage: run_fuzz_test <server_rf> <crashing>
# =============================================================================
run_fuzz_test() {
    local server_rf=$1
    local crashing=$2          # "no" or "yes"
    local managers="${MANAGERS}"
    local test_name="fuzz ${server_rf} servers ${crashing}"

    echo ""
    echo "========================================="
    echo "Running test: ${test_name}"
    echo "========================================="

    # Setup infrastructure
    setup_infrastructure "${server_rf}"

    # Run the fuzz test
    just p3::fuzz "${server_rf}" "${crashing}" "${managers}"
    local test_result=$?

    # Cleanup
    cleanup_infrastructure

    # Check result
    if [ ${test_result} -eq 0 ]; then
        echo "✓ SUCCESS: ${test_name}"
        echo "========================================="
    else
        echo "✗ FAILURE: ${test_name}"
        echo "========================================="
        exit 1
    fi
}

# =============================================================================
# YCSB test runner
# Usage: run_ycsb_test <nclis> <workload> <server_rf>
# =============================================================================
run_ycsb_test() {
    local nclis=$1
    local workload=$2
    local server_rf=$3
    local managers="${MANAGERS}"
    local test_name="ycsb-${workload} ${nclis} clients rf ${server_rf}"

    echo ""
    echo "========================================="
    echo "Running test: ${test_name}"
    echo "========================================="

    # Setup infrastructure
    setup_infrastructure "${server_rf}"

    # Run the YCSB benchmark
    just p3::bench "${nclis}" "${workload}" "${server_rf}" "${managers}"
    local test_result=$?

    # Cleanup
    cleanup_infrastructure

    # Check result
    if [ ${test_result} -eq 0 ]; then
        echo "✓ SUCCESS: ${test_name}"
        echo "========================================="
    else
        echo "✗ FAILURE: ${test_name}"
        echo "========================================="
        exit 1
    fi
}

# =============================================================================
# FUZZ tests
# =============================================================================
echo "========================================="
echo "Starting Fuzz Test Suite"
echo "========================================="

# 1.  Fuzz 5 servers, healthy
run_fuzz_test 5 no

# 2.  Fuzz 5 servers, crashing
run_fuzz_test 5 yes

echo ""
echo "========================================="
echo "✓ All Fuzz tests completed successfully!"
echo "========================================="

# =============================================================================
# YCSB tests
# =============================================================================
echo ""
echo "========================================="
echo "Starting YCSB Test Suite"
echo "========================================="

# --- YCSB workloads A–F with RF=1 ---
for workload in a b c d e f; do
    run_ycsb_test 1 "${workload}" 1
    run_ycsb_test 3 "${workload}" 1
    run_ycsb_test 5 "${workload}" 1
done

# --- YCSB-A with varying RF ---
run_ycsb_test 1 a 1
run_ycsb_test 1 a 20
run_ycsb_test 1 a 30

# --- YCSB-A with 5 clients and varying RF ---
run_ycsb_test 5 a 1
run_ycsb_test 5 a 20
run_ycsb_test 5 a 30

echo ""
echo "========================================="
echo "✓ All YCSB tests completed successfully!"
echo "========================================="
