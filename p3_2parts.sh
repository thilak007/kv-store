#!/bin/bash
set -e

# =============================================================================
# Configuration (shared between fuzz and ycsb)
# =============================================================================
MANAGERS="127.0.0.1:3666,127.0.0.1:3667,127.0.0.1:3668"
MANAGER_P2PS="127.0.0.1:3606,127.0.0.1:3607,127.0.0.1:3608"
IP="127.0.0.1"
BACKER="./backer"
SERVER_API_BASE_PORT=3777
SERVER_P2P_BASE_PORT=3707
YCSB_LOG_DIR="./output/benchmarks"

# Number of partitions (single partition only: p0 = s0.*)
NUM_PARTITIONS=3

# Build comma-separated server API and p2p address lists for the active RF.
build_server_lists() {
    local server_rf=$1
    local total_servers=$((NUM_PARTITIONS * server_rf))
    local api_list=""
    local p2p_list=""

    for (( idx=0; idx<total_servers; idx++ )); do
        local api_addr="${IP}:$((SERVER_API_BASE_PORT + idx))"
        local p2p_addr="${IP}:$((SERVER_P2P_BASE_PORT + idx))"

        if [ -z "${api_list}" ]; then
            api_list="${api_addr}"
            p2p_list="${p2p_addr}"
        else
            api_list="${api_list},${api_addr}"
            p2p_list="${p2p_list},${p2p_addr}"
        fi
    done

    echo "${api_list}|${p2p_list}"
}

# =============================================================================
# Helper: setup manager + service nodes
# Usage: setup_infrastructure <server_rf>
# =============================================================================
setup_infrastructure() {
    local server_rf=$1
    local generated_lists
    generated_lists="$(build_server_lists "${server_rf}")"
    local servers="${generated_lists%%|*}"
    local server_p2ps="${generated_lists#*|}"

    echo "  Starting manager (server_rf=${server_rf})..."
    just p3::manager "0" "3666" "3606" \
        "127.0.0.1:3607,127.0.0.1:3608" \
        "${server_rf}" \
        "${servers}" \
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
                "${servers}" \
                "${server_p2ps}" \
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
    local workload_start_ts
    local workload_end_ts
    local workload_elapsed
    local timestamp
    timestamp="$(date +%Y%m%d_%H%M%S)"
    local ycsb_log_file="${YCSB_LOG_DIR}/ycsb_w${workload}_c${nclis}_rf${server_rf}_${timestamp}.log"

    echo ""
    echo "========================================="
    echo "Running test: ${test_name}"
    echo "========================================="

    # Setup infrastructure
    setup_infrastructure "${server_rf}"

    # Run the YCSB benchmark
    mkdir -p "${YCSB_LOG_DIR}"
    echo "Logging YCSB output to: ${ycsb_log_file}"
    workload_start_ts=$(date +%s)
    just p3::bench "${nclis}" "${workload}" "${server_rf}" "${managers}" 2>&1 | tee "${ycsb_log_file}"
    local test_result=${PIPESTATUS[0]}
    workload_end_ts=$(date +%s)
    workload_elapsed=$((workload_end_ts - workload_start_ts))
    echo "Workload '${workload}' with ${nclis} client(s) and rf=${server_rf} completed in ${workload_elapsed}s"

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
# run_fuzz_test 5 no

# 2.  Fuzz 5 servers, crashing
# run_fuzz_test 5 yes

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

# run_ycsb_test 1 a 5

# # 1) 10 clients on workloads A-F for RF in {1,3,5}
# for workload in a b c d e f; do
#     for rf in 1 3 5; do
#         run_ycsb_test 10 "${workload}" "${rf}"
#     done
# done

# 2) Workload A, scale clients from 1, 20, 30 for RF in {1,5}
# Chosen client counts to represent the 1->30 scaling curve.
for nclis in 20; do
    for rf in 5; do
        run_ycsb_test "${nclis}" a "${rf}"
    done
done

# for nclis in 30; do
#     for rf in 1 5; do
#         run_ycsb_test "${nclis}" a "${rf}"
#     done
# done

echo ""
echo "========================================="
echo "✓ All YCSB tests completed successfully!"
echo "========================================="


# ./p3_2parts.sh 2>&1 | tee "output/benchmarks/p3_2parts_c20_30_rf_5_1_parts_3_$(date +%Y%m%d_%H%M%S).log"