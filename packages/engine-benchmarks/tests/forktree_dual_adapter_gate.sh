#!/usr/bin/env bash
set -Eeuo pipefail

# Test-only ForkTree acceptance runner.  It intentionally composes existing
# public/integration tests; it does not add a storage owner or alter production
# initialization, publication, or compatibility behavior.

ROOT_DIR="${1:-$(git rev-parse --show-toplevel)}"
cd "${ROOT_DIR}"

TARGET_DIR="${CARGO_TARGET_DIR:-${ROOT_DIR}/target-forktree-dual-adapter-gate}"
BUILD_JOBS="${CARGO_BUILD_JOBS:-2}"
RUN_RUNTIME="${FORKTREE_GATE_RUN:-0}"

run_cell() {
    local label="$1"
    shift
    printf '\n=== %s ===\n' "${label}"
    printf 'timeout 1200s env CARGO_TARGET_DIR=%q CARGO_BUILD_JOBS=%q cargo' "${TARGET_DIR}" "${BUILD_JOBS}"
    printf ' %q' "$@"
    printf '\n'
    timeout --signal=TERM --kill-after=30s 1200s env \
        CARGO_TARGET_DIR="${TARGET_DIR}" \
        CARGO_BUILD_JOBS="${BUILD_JOBS}" \
        cargo "$@"
}

printf '\n=== source diff check ===\n'
git diff --check
run_cell "Rust formatting" fmt --all -- --check
run_cell "Memory library compile" test -p lix --lib --no-run
run_cell "Memory integration compile (all simulations)" \
    test -p lix --test integration --features all-simulations --no-run
run_cell "RocksDB/SlateDB checkpoint adapter compile" \
    test -p lix_benchmarks --features rocksdb,slatedb \
    --test checkpoint_gc_replay_reopen --no-run
run_cell "RocksDB/SlateDB replacement/delete adapter compile" \
    test -p lix_benchmarks --features rocksdb,slatedb \
    --test certified_replacement_delete_reopen --no-run
run_cell "RocksDB/SlateDB BlobRef and GC adapter compile" \
    test -p lix_benchmarks --features storage-benches,slatedb \
    --test cas_gc_history_retention --no-run
run_cell "RocksDB/SlateDB corruption adapter compile" \
    test -p lix_benchmarks --features storage-benches,slatedb \
    --test corruption_recovery_qualification --no-run
run_cell "RocksDB/SlateDB media adapter compile" \
    test -p lix_benchmarks --features storage-benches,slatedb \
    --test large_payload_read_qualification --no-run
run_cell "RocksDB/SlateDB upload/reopen adapter compile" \
    test -p lix_benchmarks --features storage-benches,slatedb \
    --test movie_workspace_qualification --no-run

if [[ "${RUN_RUNTIME}" != "1" ]]; then
    printf '\nFORKTREE_GATE_RUN is not 1; compile-only stop after Memory compilation.\n'
    exit 0
fi

# Memory: each filter is deliberately narrow and includes both base and
# tracked-state-rebuild variants when all-simulations is enabled.
run_cell "Memory OLTP CRUD and transaction" \
    test -p lix --test integration --features all-simulations transaction::
run_cell "Memory key/value point and range CRUD" \
    test -p lix --test integration --features all-simulations sql::lix_key_value::
run_cell "Memory parsed-file projection and BlobRef identity" \
    test -p lix --test integration --features all-simulations \
    sql::lix_file::lix_file_insert_on_conflict_path_updates_existing_content_and_preserves_id
run_cell "Memory branch, diff, merge, and history" \
    test -p lix --test integration --features all-simulations branching::merge_branch_
run_cell "Memory diff/checkpoint publication" \
    test -p lix --test integration --features all-simulations sql::diff_commands::
run_cell "Memory commit/history projection" \
    test -p lix --test integration --features all-simulations sql::lix_commit::
run_cell "Memory checkpoint and GC" \
    test -p lix --test integration --features all-simulations checkpoint_gc::checkpoint_gc_
run_cell "Memory corruption fail-closed" \
    test -p lix --test integration --features all-simulations corruption_fuzz::
run_cell "Memory file history and ordered range" \
    test -p lix --test integration --features all-simulations sql::lix_file_history::

# RocksDB: adapter-owned correctness first, then the focused large BlobRef
# identity/range fixture.  No benchmark or broad matrix is invoked here.
run_cell "RocksDB media restart and parsed-file publication" \
    test -p lix_benchmarks --features storage-benches,slatedb \
    --test movie_workspace_qualification rocksdb_resumes_media_upload_after_engine_restart -- --nocapture
run_cell "RocksDB checkpoint/reopen" \
    test -p lix_benchmarks --features rocksdb,slatedb \
    --test checkpoint_gc_replay_reopen rocksdb_checkpoint_gc_retains_replay_and_selected_owners_after_reopen -- --nocapture
run_cell "RocksDB replacement/delete/reopen" \
    test -p lix_benchmarks --features rocksdb,slatedb \
    --test certified_replacement_delete_reopen rocksdb_reopens_after_certified_replacement_delete_checkpoint -- --nocapture
run_cell "RocksDB final-reference GC/reopen" \
    test -p lix_benchmarks --features storage-benches,slatedb \
    --test cas_gc_history_retention rocksdb_ -- --nocapture
run_cell "RocksDB corruption/recovery" \
    test -p lix_benchmarks --features storage-benches,slatedb \
    --test corruption_recovery_qualification rocksdb_cold_reopen_corruption_qualification -- --nocapture
run_cell "RocksDB 64MiB BlobRef identity/range" \
    test -p lix_benchmarks --features storage-benches,slatedb \
    --test large_payload_read_qualification large_media_foreground_lifecycle --ignored -- --nocapture

# SlateDB: repeat the same semantic cells, in the same order, without mixing
# backend state or accepting a Rocks-only result.
run_cell "SlateDB media restart and parsed-file publication" \
    test -p lix_benchmarks --features storage-benches,slatedb \
    --test movie_workspace_qualification slatedb_resumes_media_upload_after_engine_restart -- --nocapture
run_cell "SlateDB checkpoint/reopen" \
    test -p lix_benchmarks --features rocksdb,slatedb \
    --test checkpoint_gc_replay_reopen slatedb_checkpoint_gc_retains_replay_and_selected_owners_after_reopen -- --nocapture
run_cell "SlateDB replacement/delete/reopen" \
    test -p lix_benchmarks --features rocksdb,slatedb \
    --test certified_replacement_delete_reopen slatedb_reopens_after_certified_replacement_delete_checkpoint -- --nocapture
run_cell "SlateDB final-reference GC/reopen" \
    test -p lix_benchmarks --features storage-benches,slatedb \
    --test cas_gc_history_retention slatedb_ -- --nocapture
run_cell "SlateDB corruption/recovery" \
    test -p lix_benchmarks --features storage-benches,slatedb \
    --test corruption_recovery_qualification slatedb_cold_reopen_corruption_qualification -- --nocapture
run_cell "SlateDB 64MiB BlobRef identity/range" \
    test -p lix_benchmarks --features storage-benches,slatedb \
    --test large_payload_read_qualification large_media_foreground_lifecycle --ignored -- --nocapture

printf '\nFORKTREE DUAL-ADAPTER GATE: GREEN\n'
