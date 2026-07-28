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

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

const MACHINE_RECORD_PREFIX: &str = "LIX_BATCH_BENCHMARK_JSON=";
const COMPARISON_RECORD_PREFIX: &str = "LIX_BATCH_BENCHMARK_COMPARISON_JSON=";

#[test]
#[ignore = "requires frozen baseline and candidate JSONL benchmark outputs"]
fn batch_benchmark_scorecard_compares_base_and_candidate() {
    let baseline_path = std::env::var_os("LIX_BATCH_BENCHMARK_BASELINE")
        .expect("LIX_BATCH_BENCHMARK_BASELINE must point to captured baseline output");
    let candidate_path = std::env::var_os("LIX_BATCH_BENCHMARK_CANDIDATE")
        .expect("LIX_BATCH_BENCHMARK_CANDIDATE must point to captured candidate output");
    let baseline = read_summaries(Path::new(&baseline_path));
    let candidate = read_summaries(Path::new(&candidate_path));

    for required in [
        "v2_json_ten_mib_rocksdb_import_parity_benchmark/plugin",
        "v2_csv_ten_mib_rocksdb_import_parity_benchmark/plugin",
    ] {
        assert!(
            baseline.contains_key(required),
            "baseline output is missing required summary {required}"
        );
        assert!(
            candidate.contains_key(required),
            "candidate output is missing required summary {required}"
        );
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
            baseline_summary.get("gate"),
            candidate_summary.get("gate"),
            "acceptance gate changed between baseline and candidate for {key}"
        );

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
