//! External base-versus-candidate scorecard for the ignored batch benchmarks.
//!
//! Capture the `--nocapture` output from the same release benchmark commands
//! in the frozen baseline and candidate worktrees, then run:
//!
//! ```text
//! LIX_BATCH_BENCHMARK_BASELINE=/path/to/base.jsonl \
//! LIX_BATCH_BENCHMARK_CANDIDATE=/path/to/candidate.jsonl \
//! cargo test --release -p lix_sdk_tests --test batch_benchmark_scorecard \
//!   batch_benchmark_scorecard_compares_base_and_candidate \
//!   -- --ignored --exact --nocapture
//! ```
//!
//! Each capture must concatenate the summary records from the JSON import
//! (which also measures bulk UPDATE and DELETE), CSV import, ordinary sparse
//! update, public read, unrelated merge/diff-preview, and same-entity merge
//! ignored release tests. Missing lanes are an acceptance failure.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

const MACHINE_RECORD_PREFIX: &str = "LIX_BATCH_BENCHMARK_JSON=";
const COMPARISON_RECORD_PREFIX: &str = "LIX_BATCH_BENCHMARK_COMPARISON_JSON=";

const REQUIRED_SUMMARIES: &[&str] = &[
    "v2_json_ten_mib_rocksdb_import_parity_benchmark/plugin",
    "v2_json_ten_mib_rocksdb_import_parity_benchmark/direct_no_file",
    "v2_json_ten_mib_rocksdb_import_parity_benchmark/direct_file_scoped",
    "v2_csv_ten_mib_rocksdb_import_parity_benchmark/plugin",
    "v2_csv_ten_mib_rocksdb_import_parity_benchmark/direct_file_scoped",
    "v2_json_ten_mib_ordinary_sql_byte_edit_benchmark/sparse_plugin_update",
    "v2_json_ten_mib_bulk_sql_mutation_benchmark/bulk_update",
    "v2_json_ten_mib_bulk_sql_mutation_benchmark/bulk_delete",
    "v2_json_ten_mib_rocksdb_read_benchmark/warm_read",
    "v2_json_ten_mib_rocksdb_read_benchmark/cold_open_read",
    "v2_json_ten_mib_unrelated_entity_merge_benchmark/tracked_diff_preview",
    "v2_json_ten_mib_unrelated_entity_merge_benchmark/unrelated_entity",
    "v2_json_ten_mib_same_entity_canonical_b_merge_benchmark/same_entity_conflict",
];

#[test]
#[ignore = "requires frozen baseline and candidate JSONL benchmark outputs"]
fn batch_benchmark_scorecard_compares_base_and_candidate() {
    let baseline_path = std::env::var_os("LIX_BATCH_BENCHMARK_BASELINE")
        .expect("LIX_BATCH_BENCHMARK_BASELINE must point to captured baseline output");
    let candidate_path = std::env::var_os("LIX_BATCH_BENCHMARK_CANDIDATE")
        .expect("LIX_BATCH_BENCHMARK_CANDIDATE must point to captured candidate output");
    let baseline = read_summaries(Path::new(&baseline_path));
    let candidate = read_summaries(Path::new(&candidate_path));

    for required in REQUIRED_SUMMARIES {
        assert!(
            baseline.contains_key(*required),
            "baseline output is missing required summary {required}"
        );
        assert!(
            candidate.contains_key(*required),
            "candidate output is missing required summary {required}"
        );
        assert_expected_candidate_gate(required, &candidate[*required]);
    }

    let mut comparisons = Vec::new();
    for (key, candidate_summary) in &candidate {
        let Some(thresholds) = candidate_summary
            .pointer("/gate/max_candidate_over_baseline")
            .and_then(serde_json::Value::as_object)
        else {
            continue;
        };
        if thresholds.is_empty() {
            continue;
        }

        let baseline_summary = baseline
            .get(key)
            .unwrap_or_else(|| panic!("baseline output is missing gated candidate summary {key}"));
        assert_eq!(
            baseline_summary.get("fixture"),
            candidate_summary.get("fixture"),
            "fixture identity changed for {key}"
        );
        assert_eq!(
            baseline_summary.get("allocator"),
            candidate_summary.get("allocator"),
            "allocator configuration changed for {key}"
        );
        assert_eq!(
            baseline_summary.get("samples"),
            candidate_summary.get("samples"),
            "sample count changed between baseline and candidate for {key}"
        );
        // The frozen engine commit may carry an older emitter. The candidate
        // gate is pinned against REQUIRED_SUMMARIES above, rather than
        // trusting threshold metadata supplied by either measurement file.
        for (metric, maximum_ratio) in thresholds {
            let maximum_ratio = maximum_ratio
                .as_f64()
                .unwrap_or_else(|| panic!("gate for {key}/{metric} must be numeric"));
            let baseline_value = p50(baseline_summary, key, metric);
            let candidate_value = p50(candidate_summary, key, metric);
            let ratio = if baseline_value == 0.0 {
                if candidate_value == 0.0 {
                    0.0
                } else {
                    f64::INFINITY
                }
            } else {
                candidate_value / baseline_value
            };
            let passed = ratio <= maximum_ratio;
            let record = serde_json::json!({
                "schema": "lix.shared-batch-benchmark-comparison.v1",
                "benchmark_lane": key,
                "metric": metric,
                "baseline_p50": baseline_value,
                "candidate_p50": candidate_value,
                "candidate_over_baseline": ratio,
                "maximum_ratio": maximum_ratio,
                "passed": passed
            });
            eprintln!(
                "{COMPARISON_RECORD_PREFIX}{}",
                serde_json::to_string(&record).expect("comparison record must serialize")
            );
            comparisons.push((key.clone(), metric.clone(), ratio, maximum_ratio, passed));
        }
    }

    assert!(
        !comparisons.is_empty(),
        "candidate output contained no benchmark summaries with ratio gates"
    );
    let failures = comparisons
        .iter()
        .filter(|(_, _, _, _, passed)| !passed)
        .map(|(key, metric, ratio, maximum, _)| {
            format!("{key}/{metric}: ratio {ratio:.4} exceeds {maximum:.4}")
        })
        .collect::<Vec<_>>();
    assert!(
        failures.is_empty(),
        "batch benchmark acceptance failures:\n{}",
        failures.join("\n")
    );
}

fn assert_expected_candidate_gate(key: &str, summary: &serde_json::Value) {
    assert_eq!(
        summary
            .pointer("/gate/comparison")
            .and_then(serde_json::Value::as_str),
        Some("candidate_p50_over_baseline_p50"),
        "required summary {key} must use a baseline-ratio gate"
    );
    let actual = summary
        .pointer("/gate/max_candidate_over_baseline")
        .and_then(serde_json::Value::as_object)
        .unwrap_or_else(|| panic!("required summary {key} is missing a ratio gate"));
    let expected = if matches!(
        key,
        "v2_json_ten_mib_rocksdb_import_parity_benchmark/plugin"
            | "v2_csv_ten_mib_rocksdb_import_parity_benchmark/plugin"
    ) {
        serde_json::json!({
            "elapsed_ms": 0.70,
            "allocation_count": 0.40,
            "allocated_bytes": 0.50,
            "peak_live_bytes_delta": 0.70,
            "large_allocation_count": 1.00
        })
    } else {
        serde_json::json!({"elapsed_ms": 1.05})
    };
    assert_eq!(
        actual,
        expected
            .as_object()
            .expect("scorecard gate fixture must be an object"),
        "candidate changed the acceptance gate for required summary {key}"
    );
}

fn read_summaries(path: &Path) -> BTreeMap<String, serde_json::Value> {
    let contents = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("read benchmark output {}: {error}", path.display()));
    let mut summaries = BTreeMap::new();
    for (line_index, line) in contents.lines().enumerate() {
        let Some(prefix_index) = line.find(MACHINE_RECORD_PREFIX) else {
            continue;
        };
        let json = &line[prefix_index + MACHINE_RECORD_PREFIX.len()..];
        let value: serde_json::Value = serde_json::from_str(json).unwrap_or_else(|error| {
            panic!(
                "parse benchmark JSON at {}:{}: {error}",
                path.display(),
                line_index + 1
            )
        });
        if value.get("kind").and_then(serde_json::Value::as_str) != Some("summary") {
            continue;
        }
        assert_eq!(
            value.get("schema").and_then(serde_json::Value::as_str),
            Some("lix.shared-batch-benchmark.v1"),
            "unsupported benchmark schema at {}:{}",
            path.display(),
            line_index + 1
        );
        let benchmark = value
            .get("benchmark")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_else(|| {
                panic!(
                    "summary benchmark missing at {}:{}",
                    path.display(),
                    line_index + 1
                )
            });
        let lane = value
            .get("lane")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_else(|| {
                panic!(
                    "summary lane missing at {}:{}",
                    path.display(),
                    line_index + 1
                )
            });
        let key = format!("{benchmark}/{lane}");
        assert!(
            summaries.insert(key.clone(), value).is_none(),
            "duplicate summary {key} in {}",
            path.display()
        );
    }
    summaries
}

fn p50(summary: &serde_json::Value, key: &str, metric: &str) -> f64 {
    summary
        .pointer(&format!("/metrics/{metric}/p50"))
        .and_then(serde_json::Value::as_f64)
        .unwrap_or_else(|| panic!("summary {key} is missing numeric p50 metric {metric}"))
}

#[test]
fn required_scorecard_lanes_are_unique_and_cover_the_full_batch_pipeline() {
    let unique = REQUIRED_SUMMARIES
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(unique.len(), REQUIRED_SUMMARIES.len());
    for required_fragment in [
        "/plugin",
        "/direct_no_file",
        "/direct_file_scoped",
        "/sparse_plugin_update",
        "/bulk_update",
        "/bulk_delete",
        "/warm_read",
        "/cold_open_read",
        "/tracked_diff_preview",
        "/unrelated_entity",
        "/same_entity_conflict",
    ] {
        assert!(
            REQUIRED_SUMMARIES
                .iter()
                .any(|lane| lane.ends_with(required_fragment)),
            "scorecard contract omitted {required_fragment}"
        );
    }
}
