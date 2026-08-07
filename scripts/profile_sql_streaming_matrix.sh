#!/usr/bin/env bash

set -euo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"

rows="${LIX_SQL_PROFILE_ROWS:-32768}"
repetitions="${LIX_SQL_PROFILE_REPETITIONS:-5}"
warmups="${LIX_SQL_PROFILE_WARMUPS:-1}"
limit="${LIX_SQL_PROFILE_EARLY_LIMIT:-100}"

build_log="$(mktemp)"
trap 'rm -f "$build_log"' EXIT

cargo bench -p lix \
  --no-default-features \
  --features storage-benches \
  --bench profile_sql_result_streaming \
  --no-run >"$build_log" 2>&1

bench_bin="$(sed -nE \
  's#.*\((target/release/deps/profile_sql_result_streaming-[^)]*)\).*#\1#p' \
  "$build_log" | tail -1)"
if [[ -z "$bench_bin" ]]; then
    bench_bin="$(rg --files target/release/deps \
        | rg '/profile_sql_result_streaming-[[:alnum:]]+$' \
        | tail -1)"
fi
if [[ -z "$bench_bin" || ! -x "$bench_bin" ]]; then
    printf 'could not locate compiled benchmark binary\n' >&2
    exit 1
fi

printf '%s\n' \
  'scenario,rep,rows,limit,wall_median_us,profile_total_us,arrow_execution_us,public_result_materialization_us,scan_rows,scan_batches,result_rows_consumed,result_rows_materialized,result_rows_retained,result_checksum,max_rss_bytes'

scenarios=(
    fixture_only
    full_all
    full_early
    collected_all
    collected_early
    live_all
    live_early
    count_all
)

kv() {
    local key="$1"
    local line="$2"
    sed -n "s/.* ${key}=\([^ ]*\).*/\1/p" <<<"$line"
}

rss_bytes() {
    local time_output="$1"
    local value
    if rg -q 'Maximum resident set size' "$time_output"; then
        value="$(awk '/Maximum resident set size/ {print $NF; exit}' "$time_output")"
    else
        value="$(awk '/maximum resident set size/ {print $1; exit}' "$time_output")"
    fi
    if [[ -z "$value" ]]; then
        printf 'could not parse maximum RSS from %s\n' "$time_output" >&2
        exit 1
    fi
    if rg -qi 'resident set size \(kbytes\)' "$time_output"; then
        printf '%s\n' "$((value * 1024))"
    else
        printf '%s\n' "$value"
    fi
}

for ((rep = 1; rep <= repetitions; rep++)); do
    order="$(python3 - <<'PY'
import random

cases = [
    "fixture_only",
    "full_all",
    "full_early",
    "collected_all",
    "collected_early",
    "live_all",
    "live_early",
    "count_all",
]
random.shuffle(cases)
print(" ".join(cases))
PY
)"

    for scenario in $order; do
        case "$scenario" in
            fixture_only)
                mode=fixture_only
                row_limit=all
                ;;
            full_all)
                mode=full
                row_limit=all
                ;;
            full_early)
                mode=full
                row_limit="$limit"
                ;;
            collected_early)
                mode=stream
                row_limit="$limit"
                ;;
            collected_all)
                mode=stream
                row_limit=all
                ;;
            live_all)
                mode=live
                row_limit=all
                ;;
            live_early)
                mode=live
                row_limit="$limit"
                ;;
            count_all)
                mode=count_only
                row_limit=all
                ;;
            *)
                printf 'unknown scenario: %s\n' "$scenario" >&2
                exit 1
                ;;
        esac

        output_file="$(mktemp)"
        time_file="$(mktemp)"
        trap 'rm -f "$output_file" "$time_file" "$build_log"' EXIT

        /usr/bin/time -l env \
            LIX_SQL_PROFILE_RESULT_MODE="$mode" \
            LIX_SQL_PROFILE_ROW_LIMIT="$row_limit" \
            LIX_SQL_PROFILE_ROWS="$rows" \
            LIX_SQL_PROFILE_ROUNDS=1 \
            LIX_SQL_PROFILE_WARMUPS="$warmups" \
            "$bench_bin" >"$output_file" 2>"$time_file"

        profile_line="$(rg '^execute_result_streaming_profile ' "$output_file" | tail -1)"
        if [[ -z "$profile_line" ]]; then
            printf 'benchmark produced no profile line for %s\n' "$scenario" >&2
            exit 1
        fi

        printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
            "$scenario" \
            "$rep" \
            "$rows" \
            "$row_limit" \
            "$(kv wall_median_us "$profile_line")" \
            "$(kv profile_total_us "$profile_line")" \
            "$(kv arrow_execution_us "$profile_line")" \
            "$(kv public_result_materialization_us "$profile_line")" \
            "$(kv scan_rows "$profile_line")" \
            "$(kv scan_batches "$profile_line")" \
            "$(kv result_rows_consumed "$profile_line")" \
            "$(kv result_rows_materialized "$profile_line")" \
            "$(kv result_rows_retained "$profile_line")" \
            "$(kv result_checksum "$profile_line")" \
            "$(rss_bytes "$time_file")"

        rm -f "$output_file" "$time_file"
    done
done
