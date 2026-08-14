//! Matched frozen-baseline scorecard for the universal plugin API.
//!
//! Concatenate the machine records from the same release commands in the
//! frozen baseline and candidate worktrees, then set
//! `LIX_BATCH_BENCHMARK_BASELINE` and `LIX_BATCH_BENCHMARK_CANDIDATE` to those
//! captures. Process RSS and directional counter evidence are recorded in the
//! final universal-API report because RSS is sampled by an external process.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

const MACHINE_RECORD_PREFIX: &str = "LIX_BATCH_BENCHMARK_JSON=";
const COMPARISON_RECORD_PREFIX: &str = "LIX_BATCH_BENCHMARK_COMPARISON_JSON=";
const REQUIRED_SAMPLES: u64 = 21;
const REQUIRED_EXTERNAL_LANES: &[&str] = &[
    "csv-import",
    "csv-sparse",
    "csv-cold",
    "json-import",
    "json-sparse",
    "json-cold",
    "markdown",
    "excalidraw",
];

const REQUIRED_SUMMARIES: &[&str] = &[
    "csv_ten_mib_universal_row_benchmark/universal_rows",
    "v3_file_changed_push_sink_benchmark/v3_push_sink",
    "v3_cold_successor_csv_and_json_benchmark/csv-220k-rows",
    "v3_json_ten_mib_push_sink_benchmark/v3_push_sink",
    "v3_json_ten_mib_sparse_successor_benchmark/v3_arena",
    "v3_json_ten_mib_cold_successor_benchmark/cold_successor",
    "v3_markdown_vscode_api_exact_transition_benchmark/v3_push_sink",
    "v3_excalidraw_large_transition_benchmark/v3_push_sink",
];

const SCORED_METRICS: &[(&str, &str, &str)] = &[
    ("elapsed_ms_p50", "elapsed_ms", "p50"),
    ("elapsed_ms_p95", "elapsed_ms", "p95"),
    ("allocated_bytes_p50", "allocated_bytes", "p50"),
    ("peak_live_bytes_delta_p50", "peak_live_bytes_delta", "p50"),
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
    let results_dir = std::env::var_os("LIX_BATCH_BENCHMARK_RESULTS_DIR")
        .expect("LIX_BATCH_BENCHMARK_RESULTS_DIR must point to captured logs and RSS files");

    let mut failures = Vec::new();
    for key in REQUIRED_SUMMARIES {
        let baseline = baseline
            .get(*key)
            .unwrap_or_else(|| panic!("baseline output is missing required summary {key}"));
        let candidate = candidate
            .get(*key)
            .unwrap_or_else(|| panic!("candidate output is missing required summary {key}"));
        assert_eq!(
            baseline.get("fixture"),
            candidate.get("fixture"),
            "fixture changed for {key}"
        );
        assert_eq!(
            baseline.get("allocator"),
            candidate.get("allocator"),
            "allocator changed for {key}"
        );
        assert_eq!(
            baseline.get("samples"),
            candidate.get("samples"),
            "sample count changed for {key}"
        );
        assert_eq!(
            baseline.get("samples").and_then(serde_json::Value::as_u64),
            Some(REQUIRED_SAMPLES),
            "baseline summary {key} must contain exactly {REQUIRED_SAMPLES} samples"
        );
        assert_eq!(
            candidate.get("samples").and_then(serde_json::Value::as_u64),
            Some(REQUIRED_SAMPLES),
            "candidate summary {key} must contain exactly {REQUIRED_SAMPLES} samples"
        );
        assert_expected_candidate_gate(key, candidate);

        for (gate_key, metric, percentile) in SCORED_METRICS {
            let maximum = candidate
                .pointer(&format!("/gate/max_candidate_over_baseline/{gate_key}"))
                .and_then(serde_json::Value::as_f64)
                .unwrap_or_else(|| panic!("candidate gate omits {gate_key} for {key}"));
            compare(
                key,
                gate_key,
                metric_value(baseline, key, metric, percentile),
                metric_value(candidate, key, metric, percentile),
                maximum,
                &mut failures,
            );
        }

        let baseline_bytes = metric_value(baseline, key, "allocated_bytes", "p50");
        let candidate_bytes = metric_value(candidate, key, "allocated_bytes", "p50");
        if candidate_bytes < baseline_bytes {
            let maximum = candidate
                .pointer("/gate/allocation_count/max_candidate_over_baseline")
                .and_then(serde_json::Value::as_f64)
                .unwrap_or(1.20);
            compare(
                key,
                "allocation_count_p50",
                metric_value(baseline, key, "allocation_count", "p50"),
                metric_value(candidate, key, "allocation_count", "p50"),
                maximum,
                &mut failures,
            );
        }
    }

    assert!(
        failures.is_empty(),
        "universal plugin benchmark acceptance failures:\n{}",
        failures.join("\n")
    );
    assert_external_memory_gates(Path::new(&results_dir));
}

fn assert_external_memory_gates(results_dir: &Path) {
    for lane in REQUIRED_EXTERNAL_LANES {
        let baseline_log = results_dir.join(format!("baseline-{lane}.log"));
        let candidate_log = results_dir.join(format!("candidate-{lane}.log"));
        let baseline_guest = maximum_guest_high_water(&baseline_log);
        let candidate_guest = maximum_guest_high_water(&candidate_log);
        assert_ratio_gate(
            lane,
            "guest_linear_memory_peak",
            baseline_guest,
            candidate_guest,
            1.10,
        );

        let baseline_rss = process_peak_rss(&results_dir.join(format!("baseline-{lane}.rss")));
        let candidate_rss = process_peak_rss(&results_dir.join(format!("candidate-{lane}.rss")));
        assert_ratio_gate(lane, "process_rss_peak", baseline_rss, candidate_rss, 1.10);
    }
}

fn maximum_guest_high_water(path: &Path) -> f64 {
    let contents = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("read guest-memory log {}: {error}", path.display()));
    let values = contents
        .lines()
        .flat_map(|line| line.split_ascii_whitespace())
        .filter_map(|word| {
            if let Some(value) = word.strip_prefix("guest_high_water_bytes=") {
                return value
                    .trim_end_matches(|c: char| !c.is_ascii_digit())
                    .parse::<f64>()
                    .ok();
            }
            word.strip_prefix("guest_high_water_mb=").and_then(|value| {
                value
                    .trim_end_matches(|c: char| !(c.is_ascii_digit() || c == '.'))
                    .parse::<f64>()
                    .ok()
                    .map(|megabytes| megabytes * 1_000_000.0)
            })
        });
    values
        .reduce(f64::max)
        .unwrap_or_else(|| panic!("{} has no guest high-water samples", path.display()))
}

fn process_peak_rss(path: &Path) -> f64 {
    let contents = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("read RSS result {}: {error}", path.display()));
    contents
        .trim()
        .strip_prefix("process_peak_rss_kib=")
        .unwrap_or_else(|| panic!("{} has an invalid RSS record", path.display()))
        .parse::<f64>()
        .unwrap_or_else(|error| panic!("parse RSS result {}: {error}", path.display()))
}

fn assert_ratio_gate(lane: &str, metric: &str, baseline: f64, candidate: f64, maximum: f64) {
    let ratio = candidate / baseline;
    eprintln!(
        "{COMPARISON_RECORD_PREFIX}{}",
        serde_json::json!({
            "schema": "lix.universal-plugin-benchmark-comparison.v1",
            "benchmark_lane": lane,
            "metric": metric,
            "baseline": baseline,
            "candidate": candidate,
            "candidate_over_baseline": ratio,
            "maximum_ratio": maximum,
            "passed": ratio <= maximum,
        })
    );
    assert!(
        ratio <= maximum,
        "{lane}/{metric}: ratio {ratio:.4} exceeds {maximum:.4}"
    );
}

fn compare(
    key: &str,
    metric: &str,
    baseline: f64,
    candidate: f64,
    maximum: f64,
    failures: &mut Vec<String>,
) {
    let ratio = if baseline == 0.0 {
        if candidate == 0.0 { 1.0 } else { f64::INFINITY }
    } else {
        candidate / baseline
    };
    let passed = ratio <= maximum;
    let record = serde_json::json!({
        "schema": "lix.universal-plugin-benchmark-comparison.v1",
        "benchmark_lane": key,
        "metric": metric,
        "baseline": baseline,
        "candidate": candidate,
        "candidate_over_baseline": ratio,
        "maximum_ratio": maximum,
        "passed": passed
    });
    eprintln!(
        "{COMPARISON_RECORD_PREFIX}{}",
        serde_json::to_string(&record).expect("comparison record serializes")
    );
    if !passed {
        failures.push(format!(
            "{key}/{metric}: ratio {ratio:.4} exceeds {maximum:.4}"
        ));
    }
}

fn assert_expected_candidate_gate(key: &str, summary: &serde_json::Value) {
    assert_eq!(
        summary
            .pointer("/gate/comparison")
            .and_then(serde_json::Value::as_str),
        Some("candidate_over_matched_baseline"),
        "required summary {key} uses the wrong comparison contract"
    );
    let thresholds = summary
        .pointer("/gate/max_candidate_over_baseline")
        .and_then(serde_json::Value::as_object)
        .unwrap_or_else(|| panic!("required summary {key} has no ratio gate"));
    for (metric, expected) in [
        ("elapsed_ms_p50", 1.10),
        ("elapsed_ms_p95", 1.15),
        ("allocated_bytes_p50", 1.10),
        ("peak_live_bytes_delta_p50", 1.10),
        ("guest_linear_memory_peak", 1.10),
        ("process_rss_peak", 1.10),
    ] {
        assert_eq!(
            thresholds.get(metric).and_then(serde_json::Value::as_f64),
            Some(expected),
            "required summary {key} changed gate {metric}"
        );
    }
    assert_eq!(
        summary
            .pointer("/gate/correctness")
            .and_then(serde_json::Value::as_str),
        Some("exact_hashes_and_cardinality")
    );
}

fn read_summaries(path: &Path) -> BTreeMap<String, serde_json::Value> {
    let contents = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("read benchmark output {}: {error}", path.display()));
    let mut summaries = BTreeMap::new();
    for (line_index, line) in contents.lines().enumerate() {
        let Some(prefix) = line.find(MACHINE_RECORD_PREFIX) else {
            continue;
        };
        let value: serde_json::Value = serde_json::from_str(
            &line[prefix + MACHINE_RECORD_PREFIX.len()..],
        )
        .unwrap_or_else(|error| {
            panic!(
                "parse benchmark JSON at {}:{}: {error}",
                path.display(),
                line_index + 1
            )
        });
        if value.get("kind").and_then(serde_json::Value::as_str) != Some("summary") {
            continue;
        }
        let benchmark = value["benchmark"].as_str().expect("summary benchmark");
        let lane = value["lane"].as_str().expect("summary lane");
        let key = format!("{benchmark}/{lane}");
        assert!(
            summaries.insert(key.clone(), value).is_none(),
            "duplicate summary {key} in {}",
            path.display()
        );
    }
    summaries
}

fn metric_value(summary: &serde_json::Value, key: &str, metric: &str, percentile: &str) -> f64 {
    summary
        .pointer(&format!("/metrics/{metric}/{percentile}"))
        .and_then(serde_json::Value::as_f64)
        .unwrap_or_else(|| panic!("summary {key} omits {metric}/{percentile}"))
}

#[test]
fn required_scorecard_lanes_are_unique_and_cover_every_format_and_direction() {
    let unique = REQUIRED_SUMMARIES.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(unique.len(), REQUIRED_SUMMARIES.len());
    for fragment in ["csv", "json", "markdown", "excalidraw", "cold_successor"] {
        assert!(
            REQUIRED_SUMMARIES
                .iter()
                .any(|lane| lane.contains(fragment)),
            "scorecard omits {fragment}"
        );
    }
}
