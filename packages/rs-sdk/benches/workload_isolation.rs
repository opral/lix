use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Instant;
use std::{env, process::Command};

use lix_sdk::{
    CallbackTelemetrySink, CompletedTelemetrySpan, ExecuteOptions, ExecutionPriority, Lix, Memory,
    OpenLixOptions, TelemetryValue, Value, open_lix_with_telemetry,
};

const WARMUP: usize = 500;
const SAMPLES: usize = 2_000;
const ROWS: usize = 400;
const POINT_SQL: &str = "SELECT value FROM lix_key_value WHERE key = $1";
const BACKGROUND_SQL: &str = "SELECT 1";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Baseline,
    LoadedDefault,
    LoadedPriority,
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime")
}

fn pin_current(index_from_end: usize) {
    let configured = env::var("LIX_BENCH_CPUS").ok().map(|value| {
        value
            .split(',')
            .map(|cpu| core_affinity::CoreId {
                id: cpu.trim().parse().expect("LIX_BENCH_CPUS contains CPU ids"),
            })
            .collect::<Vec<_>>()
    });
    let Some(cores) = configured.or_else(core_affinity::get_core_ids) else {
        return;
    };
    let index = cores.len().saturating_sub(index_from_end + 1);
    if let Some(core) = cores.get(index).copied() {
        assert!(core_affinity::set_for_current(core), "set CPU affinity");
    }
}

async fn setup(
    spans: Arc<Mutex<Vec<CompletedTelemetrySpan>>>,
    measuring: Arc<AtomicBool>,
) -> (Lix, Lix) {
    let telemetry = Arc::new(CallbackTelemetrySink::new(move |span| {
        if measuring.load(Ordering::Acquire) && is_background(&span) {
            spans.lock().expect("telemetry lock").push(span);
        }
    }));
    let lix = open_lix_with_telemetry(OpenLixOptions::new(Memory::new()), telemetry)
        .await
        .expect("open lix");
    let mut sql = String::from("INSERT INTO lix_key_value (key, value, lixcol_untracked) VALUES ");
    let mut params = Vec::with_capacity(ROWS * 2);
    for i in 0..ROWS {
        if i != 0 {
            sql.push(',');
        }
        let parameter = i * 2 + 1;
        sql.push_str(&format!("(${parameter}, ${}, true)", parameter + 1));
        params.push(Value::Text(format!("bench-{i:04}").into()));
        params.push(Value::Text(
            format!("value-{i:04}-{}", "x".repeat(120)).into(),
        ));
    }
    lix.execute(&sql, &params).await.expect("seed");
    let foreground = lix
        .open_workspace_session()
        .await
        .expect("foreground session");
    let background = lix
        .open_workspace_session()
        .await
        .expect("background session");
    lix.close().await.expect("close setup session");
    (foreground, background)
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((sorted.len() * percentile) + 99) / 100;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn attribute_u64(span: &CompletedTelemetrySpan, key: &str) -> Option<u64> {
    span.end
        .attributes
        .iter()
        .find_map(|attribute| match (&attribute.key, &attribute.value) {
            (attribute_key, TelemetryValue::U64(value)) if *attribute_key == key => Some(*value),
            _ => None,
        })
}

fn is_background(span: &CompletedTelemetrySpan) -> bool {
    span.end
        .attributes
        .iter()
        .any(|attribute| match (&attribute.key, &attribute.value) {
            (&"lix.workload.priority", TelemetryValue::String(value)) => value == "background",
            _ => false,
        })
}

fn parse_mode() -> Mode {
    match env::args().nth(1).as_deref() {
        None | Some("baseline") => Mode::Baseline,
        Some("loaded-default") => Mode::LoadedDefault,
        Some("loaded-priority") => Mode::LoadedPriority,
        Some(mode) => {
            panic!("unknown mode {mode:?}; expected baseline, loaded-default, or loaded-priority")
        }
    }
}

fn main() {
    if env::args().nth(1).as_deref() == Some("suite") {
        run_suite();
        return;
    }
    let mode = parse_mode();
    let measuring = Arc::new(AtomicBool::new(false));
    let spans = Arc::new(Mutex::new(Vec::new()));
    let (foreground, background) =
        runtime().block_on(setup(Arc::clone(&spans), Arc::clone(&measuring)));
    let point_param = [Value::Text("bench-0200".into())];
    runtime().block_on(async {
        for _ in 0..WARMUP {
            foreground
                .execute(POINT_SQL, &point_param)
                .await
                .expect("foreground warmup");
        }
    });

    let loaded = mode != Mode::Baseline;
    let ready = Arc::new(Barrier::new(if loaded { 2 } else { 1 }));
    let start = Arc::new(Barrier::new(if loaded { 2 } else { 1 }));
    let running = Arc::new(AtomicBool::new(true));
    let background_queries = Arc::new(AtomicU64::new(0));
    let background_thread = loaded.then(|| {
        let ready = Arc::clone(&ready);
        let start = Arc::clone(&start);
        let running = Arc::clone(&running);
        let background_queries = Arc::clone(&background_queries);
        std::thread::spawn(move || {
            pin_current(1);
            runtime().block_on(async move {
                let options = ExecuteOptions {
                    priority: if mode == Mode::LoadedPriority {
                        ExecutionPriority::Background
                    } else {
                        ExecutionPriority::Foreground
                    },
                    ..ExecuteOptions::default()
                };
                for _ in 0..32 {
                    background
                        .execute_with_options(BACKGROUND_SQL, &[], options.clone())
                        .await
                        .expect("background warmup");
                }
                ready.wait();
                start.wait();
                while running.load(Ordering::Acquire) {
                    background
                        .execute_with_options(BACKGROUND_SQL, &[], options.clone())
                        .await
                        .expect("background query");
                    background_queries.fetch_add(1, Ordering::Relaxed);
                }
                background.close().await.expect("close background");
            });
        })
    });

    pin_current(0);
    ready.wait();
    measuring.store(true, Ordering::Release);
    start.wait();
    let wall_started = Instant::now();
    let mut foreground_nanos = Vec::with_capacity(SAMPLES);
    runtime().block_on(async {
        for _ in 0..SAMPLES {
            let started = Instant::now();
            let result = foreground
                .execute(POINT_SQL, &point_param)
                .await
                .expect("foreground point query");
            assert_eq!(result.rows().len(), 1);
            foreground_nanos.push(started.elapsed().as_nanos() as u64);
        }
        foreground.close().await.expect("close foreground");
    });
    let wall_nanos = wall_started.elapsed().as_nanos() as u64;
    measuring.store(false, Ordering::Release);
    running.store(false, Ordering::Release);
    let shutdown_started = Instant::now();
    if let Some(background_thread) = background_thread {
        background_thread.join().expect("background thread");
    }
    let shutdown_nanos = shutdown_started.elapsed().as_nanos() as u64;

    foreground_nanos.sort_unstable();
    let captured = spans.lock().expect("telemetry lock");
    let mut queue_waits: Vec<u64> = captured
        .iter()
        .filter_map(|span| attribute_u64(span, "lix.workload.queue_wait_ns"))
        .collect();
    let mut executions: Vec<u64> = captured
        .iter()
        .filter_map(|span| attribute_u64(span, "lix.workload.execution_ns"))
        .collect();
    queue_waits.sort_unstable();
    executions.sort_unstable();

    println!(
        "{{\"mode\":\"{}\",\"samples\":{},\"p50_us\":{:.3},\"p95_us\":{:.3},\"p99_us\":{:.3},\"wall_ms\":{:.3},\"background_queries\":{},\"background_queue_p50_us\":{:.3},\"background_queue_p95_us\":{:.3},\"background_execution_p50_us\":{:.3},\"background_execution_p95_us\":{:.3},\"shutdown_ms\":{:.3}}}",
        match mode {
            Mode::Baseline => "baseline",
            Mode::LoadedDefault => "loaded-default",
            Mode::LoadedPriority => "loaded-priority",
        },
        SAMPLES,
        percentile(&foreground_nanos, 50) as f64 / 1_000.0,
        percentile(&foreground_nanos, 95) as f64 / 1_000.0,
        percentile(&foreground_nanos, 99) as f64 / 1_000.0,
        wall_nanos as f64 / 1_000_000.0,
        background_queries.load(Ordering::Acquire),
        percentile(&queue_waits, 50) as f64 / 1_000.0,
        percentile(&queue_waits, 95) as f64 / 1_000.0,
        percentile(&executions, 50) as f64 / 1_000.0,
        percentile(&executions, 95) as f64 / 1_000.0,
        shutdown_nanos as f64 / 1_000_000.0,
    );
}

fn run_suite() {
    let repetitions = env::var("LIX_BENCH_REPETITIONS")
        .ok()
        .map(|value| value.parse::<usize>().expect("positive repetition count"))
        .unwrap_or(21);
    assert!(repetitions > 0, "repetition count must be positive");
    let executable = env::current_exe().expect("current benchmark executable");
    let mut rows = Vec::<serde_json::Value>::with_capacity(repetitions * 3);
    for repetition in 0..repetitions {
        let modes = if repetition % 2 == 0 {
            ["baseline", "loaded-default", "loaded-priority"]
        } else {
            ["loaded-priority", "loaded-default", "baseline"]
        };
        for mode in modes {
            let output = Command::new(&executable)
                .arg(mode)
                .output()
                .expect("spawn fresh benchmark worker");
            assert!(
                output.status.success(),
                "{mode} worker failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let line = String::from_utf8(output.stdout).expect("worker emits UTF-8 JSON");
            print!("{line}");
            rows.push(serde_json::from_str(line.trim()).expect("worker emits one JSON object"));
        }
    }

    let modes = ["baseline", "loaded-default", "loaded-priority"];
    let mut medians = serde_json::Map::new();
    for mode in modes {
        let selected: Vec<&serde_json::Value> =
            rows.iter().filter(|row| row["mode"] == mode).collect();
        let metric = |key: &str| {
            let mut values: Vec<f64> = selected
                .iter()
                .map(|row| row[key].as_f64().expect("numeric benchmark metric"))
                .collect();
            median(&mut values)
        };
        let background_qps = {
            let mut values: Vec<f64> = selected
                .iter()
                .map(|row| {
                    row["background_queries"]
                        .as_f64()
                        .expect("background count")
                        / (row["wall_ms"].as_f64().expect("wall time") / 1_000.0)
                })
                .collect();
            median(&mut values)
        };
        medians.insert(
            mode.to_string(),
            serde_json::json!({
                "p50_us": metric("p50_us"),
                "p95_us": metric("p95_us"),
                "p99_us": metric("p99_us"),
                "background_qps": background_qps,
                "background_queue_p50_us": metric("background_queue_p50_us"),
                "background_queue_p95_us": metric("background_queue_p95_us"),
                "background_execution_p50_us": metric("background_execution_p50_us"),
                "background_execution_p95_us": metric("background_execution_p95_us"),
                "shutdown_ms": metric("shutdown_ms"),
            }),
        );
    }

    let mut default_regressions = Vec::with_capacity(repetitions);
    let mut priority_regressions = Vec::with_capacity(repetitions);
    for triple in rows.chunks_exact(3) {
        let baseline = triple
            .iter()
            .find(|row| row["mode"] == "baseline")
            .expect("baseline in triple")["p95_us"]
            .as_f64()
            .expect("baseline p95");
        for (mode, target) in [
            ("loaded-default", &mut default_regressions),
            ("loaded-priority", &mut priority_regressions),
        ] {
            let loaded = triple
                .iter()
                .find(|row| row["mode"] == mode)
                .expect("loaded mode in triple")["p95_us"]
                .as_f64()
                .expect("loaded p95");
            target.push((loaded / baseline - 1.0) * 100.0);
        }
    }
    let default_median = median(&mut default_regressions);
    let priority_median = median(&mut priority_regressions);
    let (lower, upper) = bootstrap_median_interval(&priority_regressions, 20_000, 1_133);
    assert!(
        rows.iter()
            .filter(|row| row["mode"] != "baseline")
            .all(|row| {
                row["background_queries"]
                    .as_u64()
                    .is_some_and(|queries| queries > 0)
            }),
        "background work must make progress in every loaded worker"
    );
    assert!(
        priority_median < 20.0,
        "median paired priority p95 regression {priority_median:.2}% exceeds 20%"
    );
    println!(
        "{}",
        serde_json::json!({
            "summary": true,
            "repetitions": repetitions,
            "medians": medians,
            "paired_p95_regression_percent": {
                "loaded_default_median": default_median,
                "loaded_priority_median": priority_median,
                "loaded_priority_bootstrap_95": [lower, upper],
                "bootstrap_resamples": 20_000,
                "bootstrap_seed": 1_133,
            }
        })
    );
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    }
}

fn bootstrap_median_interval(values: &[f64], resamples: usize, seed: u64) -> (f64, f64) {
    let mut state = seed;
    let mut medians = Vec::with_capacity(resamples);
    let mut sample = vec![0.0; values.len()];
    for _ in 0..resamples {
        for value in &mut sample {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            *value = values[((state >> 32) as usize) % values.len()];
        }
        medians.push(median(&mut sample));
    }
    medians.sort_by(f64::total_cmp);
    (
        medians[(resamples * 25 / 1_000).min(resamples - 1)],
        medians[(resamples * 975 / 1_000).min(resamples - 1)],
    )
}
