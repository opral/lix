use clap::Parser;
use lix_rs_sdk::{open_lix, ExecuteResult, Lix, LixError, OpenLixOptions, Value};
use serde::Serialize;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Builder;

mod sqlite_backend;

use sqlite_backend::Engine2SqliteBackend;

const DEFAULT_OUTPUT_DIR: &str = "artifact/benchmarks/engine2-json-pointer";
const DEFAULT_ROWS: usize = 10_000;
const DEFAULT_WARMUPS: usize = 1;
const DEFAULT_ITERATIONS: usize = 5;
const DEFAULT_CHUNK_SIZE: usize = 500;
const JSON_POINTER_SCHEMA_JSON: &str =
    include_str!("../../../packages/plugin-json-v2/schema/json_pointer.json");

type BenchResult<T> = Result<T, String>;

#[derive(Parser, Debug)]
#[command(
    name = "engine2-json-pointer-benchmark",
    about = "Benchmark engine2 json_pointer writes on an on-disk SQLite KV backend"
)]
struct Args {
    #[arg(long, default_value_t = DEFAULT_ROWS)]
    rows: usize,

    #[arg(long, default_value_t = DEFAULT_WARMUPS)]
    warmups: usize,

    #[arg(long, default_value_t = DEFAULT_ITERATIONS)]
    iterations: usize,

    #[arg(long, default_value_t = DEFAULT_CHUNK_SIZE)]
    chunk_size: usize,

    #[arg(long, default_value = DEFAULT_OUTPUT_DIR)]
    output_dir: PathBuf,

    #[arg(long)]
    keep_databases: bool,
}

#[derive(Debug, Serialize)]
struct Report {
    generated_at_unix_ms: u128,
    benchmark: &'static str,
    rows: usize,
    chunk_size: usize,
    warmups: Vec<RunSample>,
    samples: Vec<RunSample>,
    timing_ms: TimingSummary,
}

#[derive(Debug, Clone, Serialize)]
struct RunSample {
    index: usize,
    sqlite_path: String,
    insert_ms: f64,
    verify_ms: f64,
    total_ms: f64,
    committed_rows: usize,
}

#[derive(Debug, Serialize)]
struct TimingSummary {
    sample_count: usize,
    insert: PhaseSummary,
    verify: PhaseSummary,
    total: PhaseSummary,
}

#[derive(Debug, Serialize)]
struct PhaseSummary {
    mean_ms: f64,
    median_ms: f64,
    min_ms: f64,
    max_ms: f64,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> BenchResult<()> {
    let args = Args::parse();
    fs::create_dir_all(&args.output_dir).map_err(|error| {
        format!(
            "failed to create output directory {}: {error}",
            args.output_dir.display()
        )
    })?;

    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("failed to create tokio runtime: {error}"))?;

    let mut warmups = Vec::new();
    for index in 0..args.warmups {
        warmups.push(runtime.block_on(run_insert_case(&args, "warmup", index))?);
    }

    let mut samples = Vec::new();
    for index in 0..args.iterations {
        samples.push(runtime.block_on(run_insert_case(&args, "sample", index))?);
    }

    let report = Report {
        generated_at_unix_ms: unix_ms(),
        benchmark: "engine2_json_pointer_insert",
        rows: args.rows,
        chunk_size: args.chunk_size,
        timing_ms: summarize_samples(&samples),
        warmups,
        samples,
    };

    let json_path = args.output_dir.join("report.json");
    let md_path = args.output_dir.join("report.md");
    fs::write(
        &json_path,
        serde_json::to_string_pretty(&report)
            .map_err(|error| format!("failed to serialize report: {error}"))?,
    )
    .map_err(|error| format!("failed to write {}: {error}", json_path.display()))?;
    fs::write(&md_path, render_markdown_report(&report))
        .map_err(|error| format!("failed to write {}: {error}", md_path.display()))?;

    println!("wrote {}", json_path.display());
    println!("wrote {}", md_path.display());
    println!(
        "insert_{}: mean {:.2}ms, median {:.2}ms",
        args.rows, report.timing_ms.insert.mean_ms, report.timing_ms.insert.median_ms
    );

    Ok(())
}

async fn run_insert_case(args: &Args, label: &str, index: usize) -> BenchResult<RunSample> {
    let db_path = args
        .output_dir
        .join(format!("{label}-{index}-{}.sqlite", std::process::id()));
    let cleanup = CleanupDatabase {
        path: db_path.clone(),
        keep: args.keep_databases,
    };
    cleanup.remove_existing()?;

    let backend = Engine2SqliteBackend::file_backed(&db_path).map_err(display_lix_error)?;
    let lix = open_lix(OpenLixOptions {
        backend: Some(Box::new(backend)),
    })
    .await
    .map_err(display_lix_error)?;

    ensure_benchmark_file_descriptor(&lix).await?;
    register_json_pointer_schema(&lix).await?;

    let started = Instant::now();
    let insert_started = Instant::now();
    for sql in build_insert_batches(args.rows, args.chunk_size)? {
        let result = lix.execute(&sql, &[]).await.map_err(display_lix_error)?;
        let ExecuteResult::AffectedRows(affected_rows) = result else {
            return Err("json pointer insert should return affected rows".to_string());
        };
        if affected_rows == 0 {
            return Err("json pointer insert unexpectedly affected zero rows".to_string());
        }
    }
    let insert_elapsed = insert_started.elapsed();

    let verify_started = Instant::now();
    let committed_rows = count_json_pointer_rows(&lix).await?;
    let verify_elapsed = verify_started.elapsed();
    if committed_rows != args.rows {
        return Err(format!(
            "committed json_pointer row count mismatch: expected {}, got {committed_rows}",
            args.rows
        ));
    }

    let total_elapsed = started.elapsed();
    let sample = RunSample {
        index,
        sqlite_path: db_path.display().to_string(),
        insert_ms: millis(insert_elapsed),
        verify_ms: millis(verify_elapsed),
        total_ms: millis(total_elapsed),
        committed_rows,
    };

    drop(cleanup);
    Ok(sample)
}

async fn register_json_pointer_schema(lix: &Lix) -> BenchResult<()> {
    let schema = sql_string(JSON_POINTER_SCHEMA_JSON);
    let sql = format!(
        "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
         VALUES (lix_json('{schema}'), true, true)"
    );
    match lix.execute(&sql, &[]).await.map_err(display_lix_error)? {
        ExecuteResult::AffectedRows(1) => Ok(()),
        other => Err(format!(
            "schema registration returned unexpected result: {other:?}"
        )),
    }
}

async fn ensure_benchmark_file_descriptor(lix: &Lix) -> BenchResult<()> {
    let snapshot = serde_json::json!({
        "id": "bench.json",
        "directory_id": null,
        "name": "bench",
        "extension": "json",
        "hidden": false
    });
    let sql = format!(
        "INSERT INTO lix_state (\
         entity_id, schema_key, file_id, snapshot_content, global, untracked\
         ) VALUES (\
         'bench.json', 'lix_file_descriptor', NULL, lix_json('{}'), false, false\
         )",
        sql_string(&snapshot.to_string())
    );
    match lix.execute(&sql, &[]).await.map_err(display_lix_error)? {
        ExecuteResult::AffectedRows(1) => Ok(()),
        other => Err(format!(
            "file descriptor insert returned unexpected result: {other:?}"
        )),
    }
}

fn build_insert_batches(row_count: usize, chunk_size: usize) -> BenchResult<Vec<String>> {
    if chunk_size == 0 {
        return Err("chunk_size must be greater than zero".to_string());
    }

    let mut batches = Vec::new();
    let mut next = 0;
    while next < row_count {
        let end = (next + chunk_size).min(row_count);
        let mut sql = String::from(
            "INSERT INTO lix_state (\
             entity_id, schema_key, file_id, snapshot_content, global, untracked\
             ) VALUES ",
        );
        for index in next..end {
            if index > next {
                sql.push(',');
            }
            let pointer = format!("/prop_{index}");
            let snapshot = serde_json::json!({
                "path": pointer,
                "value": {
                    "index": index,
                    "label": format!("value-{index}")
                }
            });
            sql.push_str(&format!(
                "('{}','json_pointer','bench.json',lix_json('{}'),false,false)",
                sql_string(&pointer),
                sql_string(&snapshot.to_string())
            ));
        }
        batches.push(sql);
        next = end;
    }
    Ok(batches)
}

async fn count_json_pointer_rows(lix: &Lix) -> BenchResult<usize> {
    let result = lix
        .execute(
            "SELECT COUNT(*) \
             FROM lix_state \
             WHERE schema_key = 'json_pointer' \
               AND file_id = 'bench.json' \
               AND snapshot_content IS NOT NULL",
            &[],
        )
        .await
        .map_err(display_lix_error)?;
    let ExecuteResult::Rows(rows) = result else {
        return Err("COUNT query should return rows".to_string());
    };
    let Some(row) = rows.rows().first() else {
        return Err("COUNT query returned no rows".to_string());
    };
    match row.values().first() {
        Some(Value::Integer(value)) => {
            usize::try_from(*value).map_err(|_| format!("COUNT returned negative value: {value}"))
        }
        other => Err(format!("COUNT returned unexpected value: {other:?}")),
    }
}

fn summarize_samples(samples: &[RunSample]) -> TimingSummary {
    TimingSummary {
        sample_count: samples.len(),
        insert: summarize_phase(samples.iter().map(|sample| sample.insert_ms).collect()),
        verify: summarize_phase(samples.iter().map(|sample| sample.verify_ms).collect()),
        total: summarize_phase(samples.iter().map(|sample| sample.total_ms).collect()),
    }
}

fn summarize_phase(mut values: Vec<f64>) -> PhaseSummary {
    if values.is_empty() {
        return PhaseSummary {
            mean_ms: 0.0,
            median_ms: 0.0,
            min_ms: 0.0,
            max_ms: 0.0,
        };
    }
    values.sort_by(|left, right| left.total_cmp(right));
    let sum = values.iter().sum::<f64>();
    let midpoint = values.len() / 2;
    let median = if values.len() % 2 == 0 {
        (values[midpoint - 1] + values[midpoint]) / 2.0
    } else {
        values[midpoint]
    };
    PhaseSummary {
        mean_ms: sum / values.len() as f64,
        median_ms: median,
        min_ms: values[0],
        max_ms: values[values.len() - 1],
    }
}

fn render_markdown_report(report: &Report) -> String {
    format!(
        "# Engine2 JSON Pointer Benchmark\n\n\
         - Rows: `{}`\n\
         - Chunk size: `{}`\n\
         - Samples: `{}`\n\n\
         | Phase | Mean ms | Median ms | Min ms | Max ms |\n\
         | --- | ---: | ---: | ---: | ---: |\n\
         | Insert | {:.2} | {:.2} | {:.2} | {:.2} |\n\
         | Verify | {:.2} | {:.2} | {:.2} | {:.2} |\n\
         | Total | {:.2} | {:.2} | {:.2} | {:.2} |\n",
        report.rows,
        report.chunk_size,
        report.timing_ms.sample_count,
        report.timing_ms.insert.mean_ms,
        report.timing_ms.insert.median_ms,
        report.timing_ms.insert.min_ms,
        report.timing_ms.insert.max_ms,
        report.timing_ms.verify.mean_ms,
        report.timing_ms.verify.median_ms,
        report.timing_ms.verify.min_ms,
        report.timing_ms.verify.max_ms,
        report.timing_ms.total.mean_ms,
        report.timing_ms.total.median_ms,
        report.timing_ms.total.min_ms,
        report.timing_ms.total.max_ms,
    )
}

fn sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn display_lix_error(error: LixError) -> String {
    format!("{}: {}", error.code, error.description)
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}

struct CleanupDatabase {
    path: PathBuf,
    keep: bool,
}

impl CleanupDatabase {
    fn remove_existing(&self) -> BenchResult<()> {
        for path in self.paths() {
            if path.exists() {
                fs::remove_file(&path)
                    .map_err(|error| format!("failed to remove {}: {error}", path.display()))?;
            }
        }
        Ok(())
    }

    fn paths(&self) -> Vec<PathBuf> {
        ["", "-wal", "-shm", "-journal"]
            .into_iter()
            .map(|suffix| PathBuf::from(format!("{}{}", self.path.display(), suffix)))
            .collect()
    }
}

impl Drop for CleanupDatabase {
    fn drop(&mut self) {
        if self.keep {
            return;
        }
        for path in self.paths() {
            let _ = fs::remove_file(path);
        }
    }
}
