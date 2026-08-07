#!/usr/bin/env bash
set -euo pipefail

CONTROL_SHA=a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3
STAGE1_SHA=138b55e1de90806c380ad27b2b349f4c66a1387f
APP_ORACLE_SHA=5a6a2cb037668c8dc6256d9b0975d0b39068f07a
VC_ORACLE_SHA=ae3b9bf13676a79e01b25e5d1a2cc624517326e9
OLAP_ORACLE_SHA=2a0e8512bb37c9da2050c99c366e5ac05bb01553
RECOVERY_ORACLE_SHA=ee402a098a991f7e91eb9c62e2cefe960f8e547e
BOUNDED_GC_SHA=73f191fbb960bdb9bb647f63dc909fba606a5c40
MEDIA_FAMILY_SHA=c2042c0e447950e261a2ca8674e49549acca8078
MEDIA_SHAPE_SHA=d8ddc071cc4ef05874df947787f2212812dd2564

ROOT=${CHECKOUT:-$(git rev-parse --show-toplevel)}
TARGET=${CARGO_TARGET_DIR:-${TMPDIR:-/tmp}/forktree-post-stage2-target}
CELL_CAP=${CELL_CAP:-20m}
EXECUTE=${EXECUTE:-0}

cells() {
  sed -n 's/^+//p' <<'CELLS'
+build-core
+owner-memory
+owner-rocks
+owner-slate
+sql-core
+sql-rocks
+sql-slate
+vc-build
+vc-rocks-1k
+vc-slate-1k
+vc-corrupt-rocks
+vc-corrupt-slate
+recovery-build
+recovery-rocks
+recovery-slate
+recovery-adversarial
+bounded-gc-conformance
+olap-build
+olap-rocks-10k
+olap-slate-10k
+media-build
+media-image-rocks-64
+media-image-slate-64
+media-audio-rocks-64
+media-audio-slate-64
+media-archive-rocks-64
+media-archive-slate-64
+media-video-rocks-64
+media-video-slate-64
+blob-rocks-64
+blob-slate-64
+blob-rocks-512
+blob-slate-512
CELLS
}

say_command() {
  printf '%s\n' "$1"
}

run_command() {
  local command=$1
  say_command "$command"
  if [[ "$EXECUTE" != 1 ]]; then
    return 0
  fi
  timeout --signal=TERM --kill-after=30s "$CELL_CAP" \
    bash -lc "cd '$ROOT' && $command"
}

bench_binary() {
  local name=$1
  local found
  if [[ "$EXECUTE" != 1 ]]; then
    printf '%s/release/deps/%s-<hash>\n' "$TARGET" "$name"
    return
  fi
  found=$(find "$TARGET/release/deps" -maxdepth 1 -type f -executable \
    -name "${name}-*" | LC_ALL=C sort | tail -n 1)
  [[ -n "$found" ]] || {
    printf 'missing built benchmark binary for %s\n' "$name" >&2
    return 1
  }
  printf '%s\n' "$found"
}

fresh_path() {
  local label=$1
  if [[ "$EXECUTE" != 1 ]]; then
    printf '%s/forktree-%s-<fresh>\n' "${TMPDIR:-/tmp}" "$label"
    return
  fi
  mktemp -d "${TMPDIR:-/tmp}/forktree-${label}.XXXXXX"
}

require_file() {
  if [[ ! -f "$ROOT/$1" ]]; then
    printf 'missing required test-only harness path: %s\n' "$1" >&2
    return 1
  fi
}

verify_ref() {
  git -C "$ROOT" cat-file -e "$1^{commit}"
}

verify() {
  local sha role commit tree path blob hash actual
  for sha in \
    "$CONTROL_SHA" "$STAGE1_SHA" "$APP_ORACLE_SHA" "$VC_ORACLE_SHA" \
    "$OLAP_ORACLE_SHA" "$RECOVERY_ORACLE_SHA" "$BOUNDED_GC_SHA" \
    "$MEDIA_FAMILY_SHA" "$MEDIA_SHAPE_SHA"; do
    verify_ref "$sha"
  done
  while IFS=$'\t' read -r role commit tree path blob hash; do
    [[ "$role" == role ]] && continue
    actual=$(git -C "$ROOT" rev-parse "$commit^{tree}")
    [[ "$actual" == "$tree" ]] || {
      printf 'tree mismatch for %s: %s != %s\n' "$commit" "$actual" "$tree" >&2
      return 1
    }
    [[ "$path" == - ]] && continue
    actual=$(git -C "$ROOT" rev-parse "$commit:$path")
    [[ "$actual" == "$blob" ]] || {
      printf 'blob mismatch for %s:%s\n' "$commit" "$path" >&2
      return 1
    }
    actual=$(git -C "$ROOT" show "$commit:$path" | sha256sum | cut -d' ' -f1)
    [[ "$actual" == "$hash" ]] || {
      printf 'content hash mismatch for %s:%s\n' "$commit" "$path" >&2
      return 1
    }
  done < "$ROOT/packages/lix/tests/forktree_post_stage2_acceptance/SOURCE_REFS.tsv"
  git -C "$ROOT" merge-base --is-ancestor "$CONTROL_SHA" HEAD
  printf 'verified control ancestry and all frozen source/tree/blob identities\n'
}

run_cell() {
  local cell=$1
  local bin path backend family kind size
  case "$cell" in
    build-core)
      run_command "CARGO_TARGET_DIR='$TARGET' CARGO_BUILD_JOBS=2 cargo check -p lix --all-features"
      ;;
    owner-memory|owner-rocks|owner-slate)
      require_file packages/rs-sdk-tests/tests/forktree_stage1_application_oracle.rs
      backend=${cell#owner-}
      run_command "CARGO_TARGET_DIR='$TARGET' CARGO_BUILD_JOBS=2 cargo test -p lix_tests --test forktree_stage1_application_oracle forktree_stage1_application_${backend} -- --exact --nocapture --test-threads=1"
      ;;
    sql-core)
      run_command "CARGO_TARGET_DIR='$TARGET' CARGO_BUILD_JOBS=2 cargo test -p lix --lib sql2::exec::datafusion::tests::target_only_delete_returning_executes_with_selected_provider -- --exact --nocapture && CARGO_TARGET_DIR='$TARGET' cargo test -p lix --lib session::execute::tests::execute_batch_metadata_preserves_returning_rows_and_duplicate_labels -- --exact --nocapture && CARGO_TARGET_DIR='$TARGET' cargo test -p lix --lib session::execute::tests::execute_batch_parameter_batch_preserves_failing_statement_index -- --exact --nocapture"
      ;;
    sql-rocks|sql-slate)
      backend=${cell#sql-}
      if [[ "$backend" == rocks ]]; then
        backend=rocksdb
      else
        backend=slatedb
      fi
      run_command "CARGO_TARGET_DIR='$TARGET' CARGO_BUILD_JOBS=2 cargo test -p lix_tests --test e2e lix_owned_sql_write_semantics_${backend}_reopen -- --exact --nocapture --test-threads=1"
      ;;
    vc-build)
      require_file packages/engine-benchmarks/benches/forktree_stage2_acceptance.rs
      run_command "CARGO_TARGET_DIR='$TARGET' CARGO_BUILD_JOBS=2 cargo bench -p lix_benchmarks --bench forktree_stage2_acceptance --features storage-benches,slatedb --no-run"
      ;;
    vc-rocks-1k|vc-slate-1k)
      backend=${cell#vc-}; backend=${backend%-1k}
      [[ "$backend" == rocks ]] && backend=rocksdb
      [[ "$backend" == slate ]] && backend=slatedb
      bin=$(bench_binary forktree_stage2_acceptance)
      path=$(fresh_path "vc-${backend}")
      run_command "'$bin' control '$backend' '$path/db' 1000"
      ;;
    vc-corrupt-rocks|vc-corrupt-slate)
      backend=${cell#vc-corrupt-}
      [[ "$backend" == rocks ]] && backend=rocksdb
      [[ "$backend" == slate ]] && backend=slatedb
      bin=$(bench_binary forktree_stage2_acceptance)
      path=$(fresh_path "vc-corrupt-${backend}")
      run_command "for kind in graph catalog object selector; do '$bin' corrupt '$backend' '$path/'\"\$kind\" 1000 \"\$kind\"; done"
      ;;
    recovery-build)
      require_file packages/engine-benchmarks/benches/forktree_stage2_recovery_no_lease.rs
      run_command "CARGO_TARGET_DIR='$TARGET' CARGO_BUILD_JOBS=2 cargo bench -p lix_benchmarks --bench forktree_stage2_recovery_no_lease --features storage-benches,slatedb --no-run"
      ;;
    recovery-rocks|recovery-slate)
      backend=${cell#recovery-}
      [[ "$backend" == rocks ]] && backend=rocksdb
      [[ "$backend" == slate ]] && backend=slatedb
      bin=$(bench_binary forktree_stage2_recovery_no_lease)
      run_command "'$bin' '$backend'"
      ;;
    recovery-adversarial)
      require_file packages/engine-benchmarks/tests/forktree_stage2_recovery_no_lease_adversarial.rs
      run_command "CARGO_TARGET_DIR='$TARGET' CARGO_BUILD_JOBS=2 cargo test --release -p lix_benchmarks --test forktree_stage2_recovery_no_lease_adversarial --features storage-benches,slatedb -- --test-threads=1 --nocapture"
      ;;
    bounded-gc-conformance)
      require_file packages/lix/tests/forktree_bounded_gc_oracle.rs
      run_command "rustc --edition=2024 -D warnings packages/lix/tests/forktree_bounded_gc_oracle.rs -O -o '$TARGET/forktree_bounded_gc_oracle' && '$TARGET/forktree_bounded_gc_oracle' conformance"
      ;;
    olap-build)
      require_file packages/engine-benchmarks/benches/forktree_replacement/olap_datafusion.rs
      run_command "CARGO_TARGET_DIR='$TARGET' CARGO_BUILD_JOBS=2 cargo bench -p lix_benchmarks --bench forktree_replacement --features storage-benches,slatedb --no-run"
      ;;
    olap-rocks-10k|olap-slate-10k)
      backend=${cell#olap-}; backend=${backend%-10k}
      [[ "$backend" == rocks ]] && backend=rocksdb
      [[ "$backend" == slate ]] && backend=slatedb
      bin=$(bench_binary forktree_replacement)
      run_command "'$bin' olap-datafusion '$backend' forktree 10000 32 3 1 1"
      ;;
    media-build)
      require_file packages/engine-benchmarks/tests/large_payload_read_qualification.rs
      run_command "CARGO_TARGET_DIR='$TARGET' CARGO_BUILD_JOBS=2 cargo test -p lix_benchmarks --test large_payload_read_qualification --features storage-benches,slatedb --no-run"
      ;;
    media-*-rocks-64|media-*-slate-64)
      family=${cell#media-}; family=${family%-rocks-64}; family=${family%-slate-64}
      backend=rocksdb
      [[ "$cell" == *-slate-64 ]] && backend=slatedb
      run_command "LIX_MEDIA_QUAL_FAMILY='$family' LIX_MEDIA_QUAL_MIB=64 LIX_MEDIA_QUAL_BACKEND='$backend' CARGO_TARGET_DIR='$TARGET' cargo test -p lix_benchmarks --test large_payload_read_qualification --features storage-benches,slatedb large_media_foreground_lifecycle -- --ignored --exact --nocapture --test-threads=1"
      ;;
    blob-rocks-64|blob-slate-64|blob-rocks-512|blob-slate-512)
      backend=${cell#blob-}; size=${backend##*-}; backend=${backend%-*}
      [[ "$backend" == rocks ]] && backend=rocksdb
      [[ "$backend" == slate ]] && backend=slatedb
      bin=$(bench_binary forktree_replacement)
      run_command "FORKTREE_BLOB_MIB='$size' '$bin' blob '$backend' forktree 1000 1 1 0 1"
      ;;
    *)
      printf 'unknown cell: %s\n' "$cell" >&2
      return 2
      ;;
  esac
}

case ${1:-list} in
  list)
    cells
    ;;
  verify)
    verify
    ;;
  run)
    [[ $# -eq 2 ]] || { printf 'usage: %s run <cell>\n' "$0" >&2; exit 2; }
    run_cell "$2"
    ;;
  *)
    printf 'usage: %s <list|verify|run CELL>\n' "$0" >&2
    exit 2
    ;;
esac
