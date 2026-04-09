use clap::Parser;
use lix_engine::wasm::WasmRuntime;
use lix_engine::{boot, BootArgs, ExecuteOptions, LixError, Session, Value};
use serde::Serialize;
use std::fs;
use std::io::{Cursor, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

mod sqlite_backend;
mod wasmtime_runtime;

const DEFAULT_OUTPUT_DIR: &str = "artifact/benchmarks/10k-entities";
const DEFAULT_PROPS: usize = 10_000;
const DEFAULT_WARMUPS: usize = 2;
const DEFAULT_ITERATIONS: usize = 10;
const DIRECT_ENTITY_WRITE_CHUNK_SIZE: usize = 250;

const PLUGIN_KEY: &str = "json";
const PLUGIN_SCHEMA_KEY: &str = "json_pointer";
const PLUGIN_SCHEMA_VERSION: &str = "1";
const PLUGIN_ARCHIVE_MANIFEST_JSON: &str = r#"{
  "key": "json",
  "runtime": "wasm-component-v1",
  "api_version": "0.1.0",
  "match": {"path_glob": "*.json"},
  "detect_changes": {},
  "entry": "plugin.wasm",
  "schemas": ["schema/json_pointer.json"]
}"#;

const JSON_POINTER_SCHEMA_JSON: &str =
    include_str!("../../../packages/plugin-json-v2/schema/json_pointer.json");

type BenchResult<T> = Result<T, String>;

#[derive(Parser, Debug)]
#[command(
    name = "10k-entities-benchmark",
    about = "Benchmark file-write vs direct-entity-write paths for a 10k-prop JSON document"
)]
struct Args {
    #[arg(long, default_value_t = DEFAULT_PROPS)]
    props: usize,

    #[arg(long, default_value_t = DEFAULT_WARMUPS)]
    warmups: usize,

    #[arg(long, default_value_t = DEFAULT_ITERATIONS)]
    iterations: usize,

    #[arg(long, default_value = DEFAULT_OUTPUT_DIR)]
    output_dir: PathBuf,
}

#[derive(Debug, Clone, Copy)]
enum BenchmarkCaseKind {
    FileWriteJson,
    DirectEntityWrites,
}

impl BenchmarkCaseKind {
    fn id(self) -> &'static str {
        match self {
            Self::FileWriteJson => "file_write_json_10k_props",
            Self::DirectEntityWrites => "direct_entity_writes_10k",
        }
    }

    fn title(self) -> &'static str {
        match self {
            Self::FileWriteJson => "File Write JSON With 10k Props",
            Self::DirectEntityWrites => "Direct Entity Writes 10k",
        }
    }

    fn timed_operation(self) -> &'static str {
        match self {
            Self::FileWriteJson => {
                "INSERT INTO lix_file for one 10k-prop JSON payload inside a buffered write transaction, then commit"
            }
            Self::DirectEntityWrites => {
                "UPDATE the root json_pointer row and INSERT 10k property json_pointer rows inside a buffered write transaction, then commit"
            }
        }
    }

    fn notes(self) -> Vec<&'static str> {
        match self {
            Self::FileWriteJson => vec![
                "This is the real file-write path with plugin detect-changes enabled.",
                "The timed write is one INSERT INTO lix_file statement.",
                "The semantic layer derives json_pointer rows during commit.",
                "This case includes plugin detect-changes cost plus direct semantic row commit cost.",
            ],
            Self::DirectEntityWrites => vec![
                "This isolates direct semantic writes through the engine without detect-changes.",
                "Outside the timer, the benchmark inserts an empty {} JSON file to establish the file descriptor and root entity.",
                "Inside the timer, it updates the root json_pointer row and inserts the 10k property rows through chunked lix_state statements.",
                "This case still includes normal commit, live-state rebuild, and file-cache refresh work for direct entity writes.",
                "The report records whether lix_file matched the expected payload after commit, but row-count verification is the hard invariant for this case.",
            ],
        }
    }

    fn timed_sql(self) -> &'static str {
        match self {
            Self::FileWriteJson => "INSERT INTO lix_file (id, path, data) VALUES (?1, ?2, ?3)",
            Self::DirectEntityWrites => {
                "UPDATE lix_state root row; INSERT INTO lix_state (...) VALUES (... x chunk_size), repeated until props rows are written"
            }
        }
    }

    fn verification(self) -> &'static str {
        match self {
            Self::FileWriteJson => {
                "Verify committed json_pointer row count for the file and verify lix_file JSON matches the input payload."
            }
            Self::DirectEntityWrites => {
                "Verify committed json_pointer row count for the file and record whether lix_file JSON matched the expected 10k-prop payload."
            }
        }
    }

    fn setup_outside_timer(self) -> Vec<&'static str> {
        match self {
            Self::FileWriteJson => vec![
                "Build plugin-json-v2 wasm.",
                "Create a fresh SQLite database.",
                "Boot the engine and install the JSON plugin.",
            ],
            Self::DirectEntityWrites => vec![
                "Build plugin-json-v2 wasm.",
                "Create a fresh SQLite database.",
                "Boot the engine and install the JSON plugin.",
                "Insert an empty {} JSON file so direct state writes target an existing JSON file.",
                "Load the committed root json_pointer entity id for that file.",
            ],
        }
    }
}

#[derive(Debug, Serialize)]
struct Report {
    generated_at_unix_ms: u128,
    benchmark: BenchmarkMetadata,
    shared_setup: SharedSetupReport,
    cases: Vec<CaseReport>,
    comparison: ComparisonSummary,
}

#[derive(Debug, Serialize)]
struct BenchmarkMetadata {
    name: &'static str,
    notes: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
struct SharedSetupReport {
    props: usize,
    input_bytes: usize,
    direct_property_rows: usize,
    expected_state_rows_after_commit: u64,
    plugin_key: &'static str,
    schema_key: &'static str,
    schema_version: &'static str,
    plugin_wasm_path: String,
    sqlite_mode: &'static str,
}

#[derive(Debug, Serialize)]
struct CaseReport {
    case_id: &'static str,
    title: &'static str,
    timed_operation: &'static str,
    notes: Vec<&'static str>,
    setup: CaseSetupReport,
    warmups: Vec<RunSample>,
    samples: Vec<RunSample>,
    timing_ms: TimingSummary,
}

#[derive(Debug, Serialize)]
struct CaseSetupReport {
    timed_rows: usize,
    timed_sql: &'static str,
    setup_outside_timer: Vec<&'static str>,
    verification: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct RunSample {
    index: usize,
    write_ms: f64,
    commit_ms: f64,
    total_ms: f64,
    committed_state_rows: u64,
    file_matches_expected: bool,
}

#[derive(Debug, Serialize)]
struct TimingSummary {
    sample_count: usize,
    write: PhaseSummary,
    commit: PhaseSummary,
    total: PhaseSummary,
}

#[derive(Debug, Serialize)]
struct PhaseSummary {
    mean_ms: f64,
    median_ms: f64,
    min_ms: f64,
    max_ms: f64,
}

#[derive(Debug, Serialize)]
struct ComparisonSummary {
    file_write_total_mean_ms: f64,
    direct_entity_total_mean_ms: f64,
    file_write_minus_direct_entity_total_mean_ms: f64,
    file_write_commit_mean_ms: f64,
    direct_entity_commit_mean_ms: f64,
    file_write_minus_direct_entity_commit_mean_ms: f64,
    file_write_write_mean_ms: f64,
    direct_entity_write_mean_ms: f64,
    file_write_minus_direct_entity_write_mean_ms: f64,
    file_write_to_direct_entity_total_ratio: f64,
}

struct TempSqlitePath {
    path: PathBuf,
}

impl TempSqlitePath {
    fn new(label: &str) -> Self {
        Self {
            path: temp_sqlite_path(label),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempSqlitePath {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm", "-journal"] {
            let _ = std::fs::remove_file(format!("{}{}", self.path.display(), suffix));
        }
    }
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should initialize");

    if let Err(error) = runtime.block_on(run(Args::parse())) {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> BenchResult<()> {
    if args.props == 0 {
        return Err("--props must be greater than 0".to_string());
    }
    if args.iterations == 0 {
        return Err("--iterations must be greater than 0".to_string());
    }

    fs::create_dir_all(&args.output_dir).map_err(io_err)?;

    let repo_root = repo_root()?;
    let plugin_wasm_path = build_plugin_json_v2_wasm(&repo_root)?;
    let plugin_wasm_bytes = fs::read(&plugin_wasm_path).map_err(io_err)?;
    let plugin_archive = build_plugin_archive(&plugin_wasm_bytes)?;
    let payload = build_flat_json_payload(args.props)?;
    let expected_state_rows_after_commit = (args.props + 1) as u64;

    let wasm_runtime: Arc<dyn WasmRuntime> =
        Arc::new(wasmtime_runtime::TestWasmtimeRuntime::new().map_err(lix_err)?);

    let file_write_case = run_case(
        BenchmarkCaseKind::FileWriteJson,
        &args,
        Arc::clone(&wasm_runtime),
        &plugin_archive,
        &payload,
        expected_state_rows_after_commit,
    )
    .await?;
    let direct_entity_case = run_case(
        BenchmarkCaseKind::DirectEntityWrites,
        &args,
        Arc::clone(&wasm_runtime),
        &plugin_archive,
        &payload,
        expected_state_rows_after_commit,
    )
    .await?;
    let comparison = build_comparison_summary(&file_write_case, &direct_entity_case)?;

    let report = Report {
        generated_at_unix_ms: now_unix_ms()?,
        benchmark: BenchmarkMetadata {
            name: "10k-entities-json-file-vs-direct-state",
            notes: vec![
                "Both cases use a fresh file-backed SQLite database per run.",
                "Plugin wasm build, engine init, plugin install, and database setup are outside the timer.",
                "Each case reports write_ms, commit_ms, and total_ms separately.",
                "The goal is to separate file/plugin detect overhead from direct 10k entity write overhead.",
            ],
        },
        shared_setup: SharedSetupReport {
            props: args.props,
            input_bytes: payload.len(),
            direct_property_rows: args.props,
            expected_state_rows_after_commit,
            plugin_key: PLUGIN_KEY,
            schema_key: PLUGIN_SCHEMA_KEY,
            schema_version: PLUGIN_SCHEMA_VERSION,
            plugin_wasm_path: plugin_wasm_path.display().to_string(),
            sqlite_mode: "fresh file-backed SQLite database per run",
        },
        cases: vec![file_write_case, direct_entity_case],
        comparison,
    };

    let report_json_path = args.output_dir.join("report.json");
    let report_markdown_path = args.output_dir.join("report.md");
    fs::write(
        &report_json_path,
        serde_json::to_vec_pretty(&report).map_err(serde_err)?,
    )
    .map_err(io_err)?;
    fs::write(&report_markdown_path, render_markdown_report(&report)).map_err(io_err)?;

    print_summary(&report, &report_json_path, &report_markdown_path);
    Ok(())
}

async fn run_case(
    kind: BenchmarkCaseKind,
    args: &Args,
    wasm_runtime: Arc<dyn WasmRuntime>,
    plugin_archive: &[u8],
    payload: &[u8],
    expected_state_rows_after_commit: u64,
) -> BenchResult<CaseReport> {
    let mut warmups = Vec::with_capacity(args.warmups);
    for index in 0..args.warmups {
        warmups.push(
            run_sample(
                kind,
                index,
                Arc::clone(&wasm_runtime),
                plugin_archive,
                payload,
                expected_state_rows_after_commit,
            )
            .await?,
        );
    }

    let mut samples = Vec::with_capacity(args.iterations);
    for index in 0..args.iterations {
        samples.push(
            run_sample(
                kind,
                index,
                Arc::clone(&wasm_runtime),
                plugin_archive,
                payload,
                expected_state_rows_after_commit,
            )
            .await?,
        );
    }

    Ok(CaseReport {
        case_id: kind.id(),
        title: kind.title(),
        timed_operation: kind.timed_operation(),
        notes: kind.notes(),
        setup: CaseSetupReport {
            timed_rows: match kind {
                BenchmarkCaseKind::FileWriteJson => 1,
                BenchmarkCaseKind::DirectEntityWrites => args.props + 1,
            },
            timed_sql: kind.timed_sql(),
            setup_outside_timer: kind.setup_outside_timer(),
            verification: kind.verification(),
        },
        warmups,
        samples: samples.clone(),
        timing_ms: summarize_timings(&samples)?,
    })
}

async fn run_sample(
    kind: BenchmarkCaseKind,
    index: usize,
    wasm_runtime: Arc<dyn WasmRuntime>,
    plugin_archive: &[u8],
    payload: &[u8],
    expected_state_rows_after_commit: u64,
) -> BenchResult<RunSample> {
    match kind {
        BenchmarkCaseKind::FileWriteJson => {
            run_file_write_sample(
                index,
                wasm_runtime,
                plugin_archive,
                payload,
                expected_state_rows_after_commit,
            )
            .await
        }
        BenchmarkCaseKind::DirectEntityWrites => {
            run_direct_entity_write_sample(
                index,
                wasm_runtime,
                plugin_archive,
                payload,
                expected_state_rows_after_commit,
            )
            .await
        }
    }
}

async fn run_file_write_sample(
    index: usize,
    wasm_runtime: Arc<dyn WasmRuntime>,
    plugin_archive: &[u8],
    payload: &[u8],
    expected_state_rows_after_commit: u64,
) -> BenchResult<RunSample> {
    let sqlite_path = TempSqlitePath::new(&format!("10k-entities-file-write-{index}"));
    let session = open_prepared_session(sqlite_path.path(), wasm_runtime, plugin_archive).await?;

    let file_id = format!("json-file-write-{index}");
    let file_path = format!("/{file_id}.json");
    let active_version_id = session.active_version_id();

    let mut transaction = Some(
        session
            .begin_transaction_with_options(ExecuteOptions::default())
            .await
            .map_err(lix_err)?,
    );

    let started_at = Instant::now();

    let write_started_at = Instant::now();
    let write_result = {
        let transaction = transaction
            .as_mut()
            .expect("transaction should be available during write phase");
        transaction
            .execute(
                "INSERT INTO lix_file (id, path, data) VALUES (?1, ?2, ?3)",
                &[
                    Value::Text(file_id.clone()),
                    Value::Text(file_path),
                    Value::Blob(payload.to_vec()),
                ],
            )
            .await
            .map_err(lix_err)
    };
    if let Err(error) = write_result {
        if let Some(transaction) = transaction.take() {
            let _ = transaction.rollback().await;
        }
        return Err(error);
    }
    let write_ms = write_started_at.elapsed().as_secs_f64() * 1000.0;

    let commit_started_at = Instant::now();
    transaction
        .take()
        .expect("transaction should be available for commit")
        .commit()
        .await
        .map_err(lix_err)?;
    let commit_ms = commit_started_at.elapsed().as_secs_f64() * 1000.0;

    let total_ms = started_at.elapsed().as_secs_f64() * 1000.0;

    finish_sample(
        index,
        &session,
        &file_id,
        &active_version_id,
        payload,
        expected_state_rows_after_commit,
        true,
        write_ms,
        commit_ms,
        total_ms,
    )
    .await
}

async fn run_direct_entity_write_sample(
    index: usize,
    wasm_runtime: Arc<dyn WasmRuntime>,
    plugin_archive: &[u8],
    payload: &[u8],
    expected_state_rows_after_commit: u64,
) -> BenchResult<RunSample> {
    let sqlite_path = TempSqlitePath::new(&format!("10k-entities-direct-state-{index}"));
    let session = open_prepared_session(sqlite_path.path(), wasm_runtime, plugin_archive).await?;

    let file_id = format!("json-direct-state-{index}");
    let file_path = format!("/{file_id}.json");
    let active_version_id = session.active_version_id();

    bootstrap_empty_json_file(&session, &file_id, &file_path).await?;
    let root_entity_id =
        load_root_json_pointer_entity_id(&session, &file_id, &active_version_id).await?;
    let direct_write_sql_batches = build_direct_entity_write_sql_batches(
        &file_id,
        &root_entity_id,
        payload,
        DIRECT_ENTITY_WRITE_CHUNK_SIZE,
    )?;

    let mut transaction = Some(
        session
            .begin_transaction_with_options(ExecuteOptions::default())
            .await
            .map_err(lix_err)?,
    );

    let started_at = Instant::now();

    let write_started_at = Instant::now();
    let write_result = {
        let transaction = transaction
            .as_mut()
            .expect("transaction should be available during write phase");
        let mut result = Ok(());
        for sql in &direct_write_sql_batches {
            if let Err(error) = transaction.execute(sql, &[]).await.map_err(lix_err) {
                result = Err(error);
                break;
            }
        }
        result
    };
    if let Err(error) = write_result {
        if let Some(transaction) = transaction.take() {
            let _ = transaction.rollback().await;
        }
        return Err(error);
    }
    let write_ms = write_started_at.elapsed().as_secs_f64() * 1000.0;

    let commit_started_at = Instant::now();
    transaction
        .take()
        .expect("transaction should be available for commit")
        .commit()
        .await
        .map_err(lix_err)?;
    let commit_ms = commit_started_at.elapsed().as_secs_f64() * 1000.0;

    let total_ms = started_at.elapsed().as_secs_f64() * 1000.0;

    finish_sample(
        index,
        &session,
        &file_id,
        &active_version_id,
        payload,
        expected_state_rows_after_commit,
        false,
        write_ms,
        commit_ms,
        total_ms,
    )
    .await
}

async fn finish_sample(
    index: usize,
    session: &Session,
    file_id: &str,
    active_version_id: &str,
    expected_payload: &[u8],
    expected_state_rows_after_commit: u64,
    enforce_file_match: bool,
    write_ms: f64,
    commit_ms: f64,
    total_ms: f64,
) -> BenchResult<RunSample> {
    let committed_state_rows = scalar_count(
        session,
        "SELECT COUNT(*) \
         FROM lix_state_by_version \
         WHERE file_id = ?1 \
           AND version_id = ?2 \
           AND schema_key = ?3 \
           AND snapshot_content IS NOT NULL",
        &[
            Value::Text(file_id.to_string()),
            Value::Text(active_version_id.to_string()),
            Value::Text(PLUGIN_SCHEMA_KEY.to_string()),
        ],
    )
    .await?;

    if committed_state_rows != expected_state_rows_after_commit {
        return Err(format!(
            "expected {expected_state_rows_after_commit} committed json_pointer rows for '{file_id}', got {committed_state_rows}"
        ));
    }

    let file_matches_expected = match verify_file_json_matches(session, file_id, expected_payload).await
    {
        Ok(()) => true,
        Err(error) if !enforce_file_match => {
            let _ = error;
            false
        }
        Err(error) => return Err(error),
    };

    Ok(RunSample {
        index,
        write_ms,
        commit_ms,
        total_ms,
        committed_state_rows,
        file_matches_expected,
    })
}

async fn open_prepared_session(
    sqlite_path: &Path,
    wasm_runtime: Arc<dyn WasmRuntime>,
    plugin_archive: &[u8],
) -> BenchResult<Session> {
    let backend = sqlite_backend::BenchSqliteBackend::file_backed(sqlite_path).map_err(lix_err)?;
    let mut boot_args = BootArgs::new(Box::new(backend), wasm_runtime);
    boot_args.access_to_internal = true;

    let engine = Arc::new(boot(boot_args));
    engine.initialize().await.map_err(lix_err)?;
    let session = engine.open_session().await.map_err(lix_err)?;
    session
        .install_plugin(plugin_archive)
        .await
        .map_err(lix_err)?;

    Ok(session)
}

async fn bootstrap_empty_json_file(
    session: &Session,
    file_id: &str,
    file_path: &str,
) -> BenchResult<()> {
    session
        .execute(
            "INSERT INTO lix_file (id, path, data) VALUES (?1, ?2, ?3)",
            &[
                Value::Text(file_id.to_string()),
                Value::Text(file_path.to_string()),
                Value::Blob(b"{}".to_vec()),
            ],
        )
        .await
        .map_err(lix_err)?;
    Ok(())
}

async fn load_root_json_pointer_entity_id(
    session: &Session,
    file_id: &str,
    active_version_id: &str,
) -> BenchResult<String> {
    let result = session
        .execute(
            "SELECT entity_id \
             FROM lix_state_by_version \
             WHERE file_id = ?1 \
               AND version_id = ?2 \
               AND schema_key = ?3 \
               AND snapshot_content IS NOT NULL \
             ORDER BY entity_id ASC \
             LIMIT 1",
            &[
                Value::Text(file_id.to_string()),
                Value::Text(active_version_id.to_string()),
                Value::Text(PLUGIN_SCHEMA_KEY.to_string()),
            ],
        )
        .await
        .map_err(lix_err)?;
    let value = result
        .statements
        .first()
        .and_then(|statement| statement.rows.first())
        .and_then(|row| row.first())
        .ok_or_else(|| format!("query returned no root json_pointer row for '{file_id}'"))?;

    match value {
        Value::Text(text) => Ok(text.clone()),
        other => Err(format!(
            "expected text entity_id for root json_pointer row of '{file_id}', got {other:?}"
        )),
    }
}

fn build_direct_entity_write_sql_batches(
    file_id: &str,
    root_entity_id: &str,
    payload: &[u8],
    chunk_size: usize,
) -> BenchResult<Vec<String>> {
    if chunk_size == 0 {
        return Err("direct entity write chunk size must be greater than 0".to_string());
    }

    let expected_json: serde_json::Value = serde_json::from_slice(payload).map_err(serde_err)?;
    let object = expected_json
        .as_object()
        .ok_or_else(|| "expected generated payload to be a JSON object".to_string())?;

    let root_snapshot_content = serde_json::json!({
        "path": root_entity_id,
        "value": expected_json,
    });
    let root_snapshot_content = serde_json::to_string(&root_snapshot_content).map_err(serde_err)?;

    let mut statements = vec![format!(
        "UPDATE lix_state \
         SET snapshot_content = '{}' \
         WHERE entity_id = '{}' \
           AND file_id = '{}' \
           AND schema_key = '{}' \
           AND plugin_key = '{}' \
           AND schema_version = '{}'",
        escape_sql_string(&root_snapshot_content),
        escape_sql_string(root_entity_id),
        escape_sql_string(file_id),
        PLUGIN_SCHEMA_KEY,
        PLUGIN_KEY,
        PLUGIN_SCHEMA_VERSION,
    )];

    let entries = object
        .iter()
        .map(|(key, value)| -> BenchResult<String> {
            let entity_id = format!("/{}", escape_json_pointer_segment(key));
            let snapshot_content = serde_json::json!({
                "path": entity_id,
                "value": value,
            });
            let snapshot_content = serde_json::to_string(&snapshot_content).map_err(serde_err)?;
            Ok(format!(
                "('{}', '{}', '{}', '{}', '{}', '{}')",
                escape_sql_string(&entity_id),
                escape_sql_string(file_id),
                PLUGIN_SCHEMA_KEY,
                PLUGIN_KEY,
                PLUGIN_SCHEMA_VERSION,
                escape_sql_string(&snapshot_content),
            ))
        })
        .collect::<BenchResult<Vec<_>>>()?;

    for chunk in entries.chunks(chunk_size) {
        statements.push(format!(
            "INSERT INTO lix_state (entity_id, file_id, schema_key, plugin_key, schema_version, snapshot_content) VALUES {}",
            chunk.join(", ")
        ));
    }

    Ok(statements)
}

async fn verify_file_json_matches(
    session: &Session,
    file_id: &str,
    expected_payload: &[u8],
) -> BenchResult<()> {
    let result = session
        .execute(
            "SELECT data FROM lix_file WHERE id = ?1 LIMIT 1",
            &[Value::Text(file_id.to_string())],
        )
        .await
        .map_err(lix_err)?;
    let value = result
        .statements
        .first()
        .and_then(|statement| statement.rows.first())
        .and_then(|row| row.first())
        .ok_or_else(|| format!("query returned no file data row for '{file_id}'"))?;

    let actual_bytes = match value {
        Value::Blob(bytes) => bytes.clone(),
        other => {
            return Err(format!(
                "expected blob data from lix_file for '{file_id}', got {other:?}"
            ));
        }
    };

    let actual_json: serde_json::Value = serde_json::from_slice(&actual_bytes).map_err(serde_err)?;
    let expected_json: serde_json::Value =
        serde_json::from_slice(expected_payload).map_err(serde_err)?;
    if actual_json != expected_json {
        return Err(format!(
            "lix_file JSON for '{file_id}' did not match expected payload"
        ));
    }

    Ok(())
}

fn build_plugin_archive(plugin_wasm_bytes: &[u8]) -> BenchResult<Vec<u8>> {
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));

    writer
        .start_file("manifest.json", options)
        .map_err(io_err)?;
    writer
        .write_all(PLUGIN_ARCHIVE_MANIFEST_JSON.as_bytes())
        .map_err(io_err)?;

    writer.start_file("plugin.wasm", options).map_err(io_err)?;
    writer.write_all(plugin_wasm_bytes).map_err(io_err)?;

    writer
        .start_file("schema/json_pointer.json", options)
        .map_err(io_err)?;
    writer
        .write_all(JSON_POINTER_SCHEMA_JSON.as_bytes())
        .map_err(io_err)?;

    writer
        .finish()
        .map(|cursor| cursor.into_inner())
        .map_err(io_err)
}

async fn scalar_count(session: &Session, sql: &str, params: &[Value]) -> BenchResult<u64> {
    let result = session.execute(sql, params).await.map_err(lix_err)?;
    let value = result
        .statements
        .first()
        .and_then(|statement| statement.rows.first())
        .and_then(|row| row.first())
        .ok_or_else(|| format!("query returned no scalar value: {sql}"))?;

    match value {
        Value::Integer(number) => {
            if *number < 0 {
                Err(format!("query returned negative count {number}: {sql}"))
            } else {
                Ok(*number as u64)
            }
        }
        other => Err(format!(
            "query returned non-integer scalar {other:?}: {sql}"
        )),
    }
}

fn summarize_timings(samples: &[RunSample]) -> BenchResult<TimingSummary> {
    if samples.is_empty() {
        return Err("cannot summarize empty samples".to_string());
    }

    Ok(TimingSummary {
        sample_count: samples.len(),
        write: summarize_phase(samples.iter().map(|sample| sample.write_ms).collect())?,
        commit: summarize_phase(samples.iter().map(|sample| sample.commit_ms).collect())?,
        total: summarize_phase(samples.iter().map(|sample| sample.total_ms).collect())?,
    })
}

fn summarize_phase(mut values: Vec<f64>) -> BenchResult<PhaseSummary> {
    if values.is_empty() {
        return Err("cannot summarize empty timing phase".to_string());
    }

    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));

    let sum = values.iter().sum::<f64>();
    let median_ms = if values.len() % 2 == 0 {
        let upper = values.len() / 2;
        (values[upper - 1] + values[upper]) / 2.0
    } else {
        values[values.len() / 2]
    };

    Ok(PhaseSummary {
        mean_ms: sum / values.len() as f64,
        median_ms,
        min_ms: values[0],
        max_ms: values[values.len() - 1],
    })
}

fn build_comparison_summary(
    file_write_case: &CaseReport,
    direct_entity_case: &CaseReport,
) -> BenchResult<ComparisonSummary> {
    let file_write_total_mean_ms = file_write_case.timing_ms.total.mean_ms;
    let direct_entity_total_mean_ms = direct_entity_case.timing_ms.total.mean_ms;
    let ratio = if direct_entity_total_mean_ms == 0.0 {
        return Err("cannot compare cases: direct-entity total mean is zero".to_string());
    } else {
        file_write_total_mean_ms / direct_entity_total_mean_ms
    };

    Ok(ComparisonSummary {
        file_write_total_mean_ms,
        direct_entity_total_mean_ms,
        file_write_minus_direct_entity_total_mean_ms: file_write_total_mean_ms
            - direct_entity_total_mean_ms,
        file_write_commit_mean_ms: file_write_case.timing_ms.commit.mean_ms,
        direct_entity_commit_mean_ms: direct_entity_case.timing_ms.commit.mean_ms,
        file_write_minus_direct_entity_commit_mean_ms: file_write_case.timing_ms.commit.mean_ms
            - direct_entity_case.timing_ms.commit.mean_ms,
        file_write_write_mean_ms: file_write_case.timing_ms.write.mean_ms,
        direct_entity_write_mean_ms: direct_entity_case.timing_ms.write.mean_ms,
        file_write_minus_direct_entity_write_mean_ms: file_write_case.timing_ms.write.mean_ms
            - direct_entity_case.timing_ms.write.mean_ms,
        file_write_to_direct_entity_total_ratio: ratio,
    })
}

fn build_flat_json_payload(props: usize) -> BenchResult<Vec<u8>> {
    let mut root = serde_json::Map::new();
    for index in 0..props {
        root.insert(
            format!("prop_{index:05}"),
            serde_json::Value::String(format!("value_{index:05}")),
        );
    }
    serde_json::to_vec(&serde_json::Value::Object(root)).map_err(serde_err)
}

fn build_plugin_json_v2_wasm(repo_root: &Path) -> BenchResult<PathBuf> {
    let manifest_path = repo_root.join("packages/plugin-json-v2/Cargo.toml");
    let wasm_path =
        repo_root.join("packages/plugin-json-v2/target/wasm32-wasip2/release/plugin_json_v2.wasm");

    let build = || {
        Command::new("cargo")
            .arg("build")
            .arg("--manifest-path")
            .arg(&manifest_path)
            .arg("--target")
            .arg("wasm32-wasip2")
            .arg("--release")
            .output()
            .map_err(io_err)
    };

    let output = build()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if stderr.contains("wasm32-wasip2")
            && (stderr.contains("target may not be installed")
                || stderr.contains("can't find crate for `core`"))
        {
            let rustup = Command::new("rustup")
                .arg("target")
                .arg("add")
                .arg("wasm32-wasip2")
                .output()
                .map_err(io_err)?;
            if !rustup.status.success() {
                return Err(format!(
                    "rustup target add wasm32-wasip2 failed:\n{}",
                    String::from_utf8_lossy(&rustup.stderr)
                ));
            }
            let retry = build()?;
            if !retry.status.success() {
                return Err(format!(
                    "cargo build for plugin_json_v2 failed after installing wasm32-wasip2:\n{}",
                    String::from_utf8_lossy(&retry.stderr)
                ));
            }
        } else {
            return Err(format!(
                "cargo build for plugin_json_v2 failed:\n{}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }

    if !wasm_path.exists() {
        return Err(format!(
            "plugin wasm build succeeded but output was missing at {}",
            wasm_path.display()
        ));
    }

    Ok(wasm_path)
}

fn render_markdown_report(report: &Report) -> String {
    let case_sections = report
        .cases
        .iter()
        .map(render_case_markdown)
        .collect::<Vec<_>>()
        .join("\n\n");

    format!(
        "# 10k Entities Benchmark Comparison\n\n\
- Props: {}\n\
- Input bytes: {}\n\
- Direct property rows inside timed direct-write case: {}\n\
- Expected committed json_pointer rows after each case: {}\n\
- Plugin key: `{}`\n\
- Schema key: `{}`\n\
- SQLite mode: `{}`\n\
- Plugin wasm: `{}`\n\n\
## Comparison\n\n\
| metric | file write | direct entities | delta |\n\
| --- | ---: | ---: | ---: |\n\
| write mean ms | {:.3} | {:.3} | {:.3} |\n\
| commit mean ms | {:.3} | {:.3} | {:.3} |\n\
| total mean ms | {:.3} | {:.3} | {:.3} |\n\
| total ratio | {:.3}x | 1.000x | {:.3}x |\n\n\
{}\n",
        report.shared_setup.props,
        report.shared_setup.input_bytes,
        report.shared_setup.direct_property_rows,
        report.shared_setup.expected_state_rows_after_commit,
        report.shared_setup.plugin_key,
        report.shared_setup.schema_key,
        report.shared_setup.sqlite_mode,
        report.shared_setup.plugin_wasm_path,
        report.comparison.file_write_write_mean_ms,
        report.comparison.direct_entity_write_mean_ms,
        report.comparison.file_write_minus_direct_entity_write_mean_ms,
        report.comparison.file_write_commit_mean_ms,
        report.comparison.direct_entity_commit_mean_ms,
        report.comparison.file_write_minus_direct_entity_commit_mean_ms,
        report.comparison.file_write_total_mean_ms,
        report.comparison.direct_entity_total_mean_ms,
        report.comparison.file_write_minus_direct_entity_total_mean_ms,
        report.comparison.file_write_to_direct_entity_total_ratio,
        report.comparison.file_write_to_direct_entity_total_ratio,
        case_sections,
    )
}

fn render_case_markdown(case: &CaseReport) -> String {
    let sample_rows = case
        .samples
        .iter()
        .map(|sample| {
            format!(
                "| {} | {:.3} | {:.3} | {:.3} | {} | {} |",
                sample.index,
                sample.write_ms,
                sample.commit_ms,
                sample.total_ms,
                sample.committed_state_rows,
                sample.file_matches_expected
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let notes = case
        .notes
        .iter()
        .map(|note| format!("- {note}"))
        .collect::<Vec<_>>()
        .join("\n");
    let setup_notes = case
        .setup
        .setup_outside_timer
        .iter()
        .map(|note| format!("- {note}"))
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        "## {}\n\n\
Timed operation: {}\n\n\
{}\n\n\
Setup outside timer:\n\
{}\n\n\
- Timed rows: {}\n\
- Timed SQL: `{}`\n\
- Verification: {}\n\n\
### Timing\n\n\
| phase | mean ms | median ms | min ms | max ms |\n\
| --- | ---: | ---: | ---: | ---: |\n\
| write | {:.3} | {:.3} | {:.3} | {:.3} |\n\
| commit | {:.3} | {:.3} | {:.3} | {:.3} |\n\
| total | {:.3} | {:.3} | {:.3} | {:.3} |\n\n\
### Samples\n\n\
| run | write ms | commit ms | total ms | committed state rows | file matches expected |\n\
| --- | ---: | ---: | ---: | ---: | --- |\n\
{}\n",
        case.title,
        case.timed_operation,
        notes,
        setup_notes,
        case.setup.timed_rows,
        case.setup.timed_sql,
        case.setup.verification,
        case.timing_ms.write.mean_ms,
        case.timing_ms.write.median_ms,
        case.timing_ms.write.min_ms,
        case.timing_ms.write.max_ms,
        case.timing_ms.commit.mean_ms,
        case.timing_ms.commit.median_ms,
        case.timing_ms.commit.min_ms,
        case.timing_ms.commit.max_ms,
        case.timing_ms.total.mean_ms,
        case.timing_ms.total.median_ms,
        case.timing_ms.total.min_ms,
        case.timing_ms.total.max_ms,
        sample_rows,
    )
}

fn print_summary(report: &Report, report_json_path: &Path, report_markdown_path: &Path) {
    println!("10k entities benchmark comparison");
    println!(
        "props={} input_bytes={} expected_state_rows_after_commit={}",
        report.shared_setup.props,
        report.shared_setup.input_bytes,
        report.shared_setup.expected_state_rows_after_commit
    );

    for case in &report.cases {
        println!("case={} title={}", case.case_id, case.title);
        println!(
            "write_ms mean={:.3} median={:.3} min={:.3} max={:.3}",
            case.timing_ms.write.mean_ms,
            case.timing_ms.write.median_ms,
            case.timing_ms.write.min_ms,
            case.timing_ms.write.max_ms,
        );
        println!(
            "commit_ms mean={:.3} median={:.3} min={:.3} max={:.3}",
            case.timing_ms.commit.mean_ms,
            case.timing_ms.commit.median_ms,
            case.timing_ms.commit.min_ms,
            case.timing_ms.commit.max_ms,
        );
        println!(
            "total_ms mean={:.3} median={:.3} min={:.3} max={:.3} samples={}",
            case.timing_ms.total.mean_ms,
            case.timing_ms.total.median_ms,
            case.timing_ms.total.min_ms,
            case.timing_ms.total.max_ms,
            case.timing_ms.sample_count,
        );
    }

    println!(
        "comparison total_mean_delta_ms={:.3} total_ratio={:.3}x",
        report.comparison.file_write_minus_direct_entity_total_mean_ms,
        report.comparison.file_write_to_direct_entity_total_ratio,
    );
    println!("report_json={}", report_json_path.display());
    println!("report_markdown={}", report_markdown_path.display());
}

fn repo_root() -> BenchResult<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .map_err(io_err)
}

fn temp_sqlite_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("lix-{label}-{nanos}.sqlite"))
}

fn now_unix_ms() -> BenchResult<u128> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io_err)?
        .as_millis())
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn escape_json_pointer_segment(segment: &str) -> String {
    segment.replace('~', "~0").replace('/', "~1")
}

fn io_err(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn serde_err(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn lix_err(error: LixError) -> String {
    format!("{}: {}", error.code, error.description)
}
