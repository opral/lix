//! Why does a single write cost 30x more in a 10,000-file repository than in a
//! 100-file one, while writing the same handful of rows?
//!
//! `e10_branch_file_cost` measured branch creation as O(1) and, as a control,
//! measured one file write on the default branch at 2.4 / 8.4 / 72.0 ms across
//! 100 / 1,000 / 10,000 files. That control is the subject here: the write path
//! carries a term in *total files in the repository*, not in files touched.
//!
//! This harness holds the written payload fixed and grows only the resident
//! file count, then splits the write into four shapes so the term can be
//! attributed:
//!
//! * `insert_file_80line` — a new plugin-backed file, 80 entity rows. The
//!   original control shape.
//! * `insert_file_1line`  — a new plugin-backed file, 1 entity row. Isolates
//!   per-entity plugin work from per-write work.
//! * `update_file_1line`  — rewrite one already-resident file with a one-line
//!   edit. The common agent-workload shape.
//! * `key_value_write`    — a single `lix_key_value` row, touching no file at
//!   all. If *this* scales with the resident file count, the term is in the
//!   commit machinery and is independent of what the write touched.
//!
//! ```text
//! cargo run --release -p lix_tests --example e16_write_file_scaling -- 100 1000 10000
//! E16_PROBES=200 E16_SIZES=10000 cargo run --release ...   # long write phase for perf
//! ```

use std::collections::BTreeMap;
use std::io::{Cursor, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tracing::Subscriber;
use tracing::span::{Attributes, Id};
use tracing::subscriber::Interest;
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context as TracingContext, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;

use lix::storage::{ReadOptions, Storage};
use lix::storage_adapter::StorageAdapter;
use lix::storage_bench::layout_accounting;
use lix::{Lix, Value, open_lix};
use lix_storage_rocksdb::RocksDB;

const INSERT_BATCH: usize = 100;
/// Directories the seeded files are spread across. Held at 1 by default so the
/// original file-count curve is unchanged; raised to isolate a term that is
/// linear in *directories* rather than in resident files. A resolver seeded
/// from directory descriptors alone is flat in files but not in directories,
/// and a single-directory fixture cannot tell those apart.
fn seed_dirs() -> usize {
    std::env::var("E16_DIRS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value >= 1)
        .unwrap_or(1)
}

fn seed_path(index: usize) -> String {
    let dirs = seed_dirs();
    if dirs <= 1 {
        format!("/docs/file-{index:07}.txt")
    } else {
        format!("/docs/d{:05}/file-{index:07}.txt", index % dirs)
    }
}
/// Above `HOST_CERTIFIED_PACKET_MIN_ROWS` so each seeded file certifies a
/// dense batch, matching the fixture `e10_branch_file_cost` established.
const LINES_PER_FILE: usize = 80;

fn main() {
    let sizes: Vec<usize> = std::env::args()
        .skip(1)
        .filter_map(|value| value.parse().ok())
        .collect();
    let sizes = if sizes.is_empty() {
        vec![100, 1_000, 10_000]
    } else {
        sizes
    };
    let shapes: Vec<&'static str> = match std::env::var("E16_SHAPES") {
        Ok(value) => SHAPES
            .into_iter()
            .filter(|shape| value.split(',').any(|entry| entry.trim() == *shape))
            .collect(),
        Err(_) => SHAPES.to_vec(),
    };
    assert!(!shapes.is_empty(), "E16_SHAPES selected no known shape");
    let probes = std::env::var("E16_PROBES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(9usize);

    let collector = PerfSpanCollector::default();
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(collector.clone()));
    tracing::dispatcher::with_default(&dispatch, || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("create write-scaling runtime");

        runtime.block_on(async {
            for files in sizes {
                run_case(files, probes, &shapes, &collector).await;
            }
        });
    });
}

async fn run_case(
    files: usize,
    probes: usize,
    shapes: &[&'static str],
    collector: &PerfSpanCollector,
) {
    let root = tempfile::Builder::new()
        .prefix("e16-write-scaling-")
        .tempdir()
        .expect("create write-scaling directory");
    let db_path = root.path().join("db");
    let storage = RocksDB::open(&db_path).expect("open write-scaling RocksDB");
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open write-scaling Lix");

    install_text_plugin(&lix).await;
    let seed_started = Instant::now();
    seed_files(&lix, files).await;
    println!(
        "write_scaling_seed,files={files},dirs={},seed_ms={:.3}",
        seed_dirs(),
        seed_started.elapsed().as_secs_f64() * 1_000.0
    );

    // Every probe writes the same payload; only the resident file count differs
    // between cases. Probes run interleaved by shape so that any drift over the
    // measurement window hits all four shapes equally.
    let mut samples: BTreeMap<&'static str, Vec<f64>> = BTreeMap::new();
    let mut spans: BTreeMap<&'static str, BTreeMap<&'static str, (f64, u64)>> = BTreeMap::new();
    let before = snapshot(&storage, &db_path).await;
    let _ = collector.take_ms();
    for probe in 0..probes {
        for shape in shapes.iter().copied() {
            let elapsed = run_probe(&lix, shape, files, probe).await;
            samples.entry(shape).or_default().push(elapsed);
            let shape_spans = spans.entry(shape).or_default();
            for (name, (ms, count)) in collector.take_ms() {
                let entry = shape_spans.entry(name).or_insert((0.0, 0));
                entry.0 += ms;
                entry.1 += count;
            }
        }
    }
    let after = snapshot(&storage, &db_path).await;

    for shape in shapes.iter().copied() {
        let mut values = samples.remove(shape).unwrap_or_default();
        values.sort_by(f64::total_cmp);
        println!(
            "write_scaling,files={files},dirs={},shape={shape},probes={},\
p50_ms={:.3},min_ms={:.3},p95_ms={:.3},raw={}",
            seed_dirs(),
            values.len(),
            percentile(&values, 0.50),
            values.first().copied().unwrap_or(0.0),
            percentile(&values, 0.95),
            values
                .iter()
                .map(|value| format!("{value:.3}"))
                .collect::<Vec<_>>()
                .join("|"),
        );
    }

    for (shape, shape_spans) in &spans {
        let mut ranked: Vec<(&&'static str, &(f64, u64))> = shape_spans.iter().collect();
        ranked.sort_by(|left, right| right.1.0.total_cmp(&left.1.0));
        for (name, (ms, count)) in ranked {
            println!(
                "write_scaling_span,files={files},shape={shape},span={name},\
total_ms={ms:.3},per_probe_ms={:.4},entries={count}",
                ms / probes as f64,
            );
        }
    }

    // Written volume across the whole probe window, so a latency term can be
    // separated from a bytes term.
    println!(
        "write_scaling_volume,files={files},probes={probes},d_rows={},d_bytes={}",
        after.rows() as i64 - before.rows() as i64,
        after.bytes() as i64 - before.bytes() as i64,
    );
    for (name, a) in &before.spaces {
        let b = after.spaces.get(name).copied().unwrap_or_default();
        let d_rows = b.rows as i64 - a.rows as i64;
        let d_bytes = (b.key_bytes + b.value_bytes) as i64 - (a.key_bytes + a.value_bytes) as i64;
        if d_rows == 0 && d_bytes == 0 {
            continue;
        }
        println!(
            "write_scaling_space,files={files},space={name},d_rows={d_rows},d_bytes={d_bytes}"
        );
    }

    lix.close().await.expect("close write-scaling Lix");
}

/// Sums the engine's own `lix_perf` spans, so the O(files) term can be
/// attributed to a named phase of the write path instead of to a leaf symbol.
/// Spans nest, so a parent's total includes its children; read the tree, not
/// the sum.
#[derive(Clone, Default)]
struct PerfSpanCollector {
    samples: Arc<Mutex<Vec<(&'static str, Duration)>>>,
}

struct StartedPerfSpan {
    name: &'static str,
    started: Instant,
}

impl PerfSpanCollector {
    fn take_ms(&self) -> BTreeMap<&'static str, (f64, u64)> {
        let samples = std::mem::take(&mut *self.samples.lock().expect("span lock"));
        let mut result: BTreeMap<&'static str, (f64, u64)> = BTreeMap::new();
        for (name, elapsed) in samples {
            let entry = result.entry(name).or_insert((0.0, 0));
            entry.0 += elapsed.as_secs_f64() * 1_000.0;
            entry.1 += 1;
        }
        result
    }
}

impl<S> Layer<S> for PerfSpanCollector
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn register_callsite(&self, metadata: &'static tracing::Metadata<'static>) -> Interest {
        if metadata.target() == "lix_perf" {
            Interest::always()
        } else {
            Interest::never()
        }
    }

    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: TracingContext<'_, S>) {
        if attrs.metadata().target() != "lix_perf" {
            return;
        }
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(StartedPerfSpan {
                name: attrs.metadata().name(),
                started: Instant::now(),
            });
        }
    }

    fn on_close(&self, id: Id, ctx: TracingContext<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let Some(started) = span.extensions_mut().remove::<StartedPerfSpan>() else {
            return;
        };
        self.samples
            .lock()
            .expect("span lock")
            .push((started.name, started.started.elapsed()));
    }
}

const SHAPES: [&str; 12] = [
    "insert_file_80line",
    "insert_file_1line",
    // Ten single-row statements inside ONE explicit transaction. The
    // path-index cache documents an intra-transaction delta advance, so if that
    // works this costs one whole-repository rebuild rather than ten.
    "insert_10files_1txn",
    // The control for it: ten single-row statements in ten autocommit
    // transactions.
    "insert_10files_10txn",
    // Same total rows written as ten `insert_file_1line` probes, but in one
    // statement. If the O(files) term is paid once per statement rather than
    // once per written row, this costs about the same as a single-row insert.
    "insert_10files_1stmt",
    "insert_100files_1stmt",
    // Root-level path (one segment). `indexed_file_path_writes` only builds a
    // whole-repository `DirectoryPathResolver` when a *nested* path is missing,
    // so a root-level create isolates the path-index build from the resolver
    // build.
    "insert_root_file",
    "upsert_root_file",
    // Upsert over an already-resident file: nothing is missing, so no resolver
    // is built at all. What remains is the path-index acquisition alone.
    "upsert_existing_file",
    // Byte-identical work to `insert_file_1line`, but the ON CONFLICT clause
    // makes the planner classify the write as `UpdateContent` instead of
    // `None`, which is the discriminator for taking the indexed staging route
    // rather than the whole-branch descriptor scan.
    "upsert_file_1line",
    "update_file_1line",
    "key_value_write",
];

async fn run_probe<StorageImpl>(
    lix: &Lix<StorageImpl>,
    shape: &str,
    files: usize,
    probe: usize,
) -> f64
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let started = Instant::now();
    match shape {
        "insert_file_80line" => {
            lix.execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                &[
                    Value::Text(format!("/probe/big-{probe:05}.txt")),
                    Value::Blob(document(LINES_PER_FILE, probe).into()),
                ],
            )
            .await
            .expect("insert 80-line probe file");
        }
        "insert_file_1line" => {
            lix.execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                &[
                    Value::Text(format!("/probe/small-{probe:05}.txt")),
                    Value::Blob(document(1, probe).into()),
                ],
            )
            .await
            .expect("insert 1-line probe file");
        }
        "insert_10files_1txn" => {
            let mut transaction = lix
                .begin_transaction()
                .await
                .expect("begin write-scaling transaction");
            for offset in 0..10 {
                transaction
                    .execute(
                        "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                        &[
                            Value::Text(format!("/probe/t1-{probe:05}-{offset:04}.txt")),
                            Value::Blob(document(1, probe + offset).into()),
                        ],
                    )
                    .await
                    .expect("stage write-scaling transaction row");
            }
            transaction
                .commit()
                .await
                .expect("commit write-scaling transaction");
        }
        "insert_10files_10txn" => {
            for offset in 0..10 {
                lix.execute(
                    "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                    &[
                        Value::Text(format!("/probe/tN-{probe:05}-{offset:04}.txt")),
                        Value::Blob(document(1, probe + offset).into()),
                    ],
                )
                .await
                .expect("insert autocommit write-scaling row");
            }
        }
        "insert_10files_1stmt" => {
            insert_many(lix, "b10", probe, 10).await;
        }
        "insert_100files_1stmt" => {
            insert_many(lix, "b100", probe, 100).await;
        }
        "insert_root_file" => {
            lix.execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                &[
                    Value::Text(format!("/root-ins-{probe:05}.txt")),
                    Value::Blob(document(1, probe).into()),
                ],
            )
            .await
            .expect("insert root probe file");
        }
        "upsert_root_file" => {
            lix.execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
                 ON CONFLICT (path) DO UPDATE SET content = excluded.content",
                &[
                    Value::Text(format!("/root-ups-{probe:05}.txt")),
                    Value::Blob(document(1, probe).into()),
                ],
            )
            .await
            .expect("upsert root probe file");
        }
        "upsert_existing_file" => {
            let target = probe % files.max(1);
            lix.execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
                 ON CONFLICT (path) DO UPDATE SET content = excluded.content",
                &[
                    Value::Text(seed_path(target)),
                    Value::Blob(document(1, probe).into()),
                ],
            )
            .await
            .expect("upsert resident probe file");
        }
        "upsert_file_1line" => {
            lix.execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
                 ON CONFLICT (path) DO UPDATE SET content = excluded.content",
                &[
                    Value::Text(format!("/probe/upsert-{probe:05}.txt")),
                    Value::Blob(document(1, probe).into()),
                ],
            )
            .await
            .expect("upsert 1-line probe file");
        }
        "update_file_1line" => {
            // Rewrites a resident seeded file, changing exactly one line.
            let target = probe % files.max(1);
            let mut body = document_string(LINES_PER_FILE, target);
            body.push_str(&format!("edit {probe}\n"));
            lix.execute(
                "UPDATE lix_file SET content = $2 WHERE path = $1",
                &[
                    Value::Text(seed_path(target)),
                    Value::Blob(body.into_bytes().into()),
                ],
            )
            .await
            .expect("update resident probe file");
        }
        "key_value_write" => {
            lix.execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, lix_json($2))",
                &[
                    Value::Text(format!("e16-probe-{probe:05}")),
                    Value::Text(format!("\"{probe}\"")),
                ],
            )
            .await
            .expect("insert key-value probe row");
        }
        other => panic!("unknown probe shape {other}"),
    }
    started.elapsed().as_secs_f64() * 1_000.0
}

fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() - 1) as f64 * quantile).round() as usize;
    sorted[index]
}

#[derive(Clone, Default)]
struct Snapshot {
    spaces: BTreeMap<&'static str, SpaceCounts>,
}

#[derive(Clone, Copy, Default)]
struct SpaceCounts {
    rows: u64,
    key_bytes: u64,
    value_bytes: u64,
}

impl Snapshot {
    fn rows(&self) -> u64 {
        self.spaces.values().map(|counts| counts.rows).sum()
    }

    fn bytes(&self) -> u64 {
        self.spaces
            .values()
            .map(|counts| counts.key_bytes + counts.value_bytes)
            .sum()
    }
}

async fn snapshot(storage: &RocksDB, _directory: &Path) -> Snapshot {
    storage.flush().expect("flush write-scaling RocksDB");
    let adapter = StorageAdapter::new(storage.clone());
    let read = adapter
        .begin_read(ReadOptions::default())
        .await
        .expect("open write-scaling layout read");
    let accounting = layout_accounting(&read).await;
    drop(read);
    let mut spaces = BTreeMap::new();
    for entry in accounting {
        spaces.insert(
            entry.space,
            SpaceCounts {
                rows: entry.rows,
                key_bytes: entry.key_bytes,
                value_bytes: entry.value_bytes,
            },
        );
    }
    Snapshot { spaces }
}

async fn install_text_plugin<StorageImpl>(lix: &Lix<StorageImpl>)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
        &[
            Value::Text("/.lix/plugins/plugin_text.lixplugin".to_owned()),
            Value::Blob(build_text_plugin_archive().into()),
        ],
    )
    .await
    .expect("install write-scaling text plugin");
}

async fn seed_files<StorageImpl>(lix: &Lix<StorageImpl>, files: usize)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut written = 0usize;
    while written < files {
        let batch = (files - written).min(INSERT_BATCH);
        let mut sql = String::from("INSERT INTO lix_file (path, content) VALUES ");
        let mut params: Vec<Value> = Vec::with_capacity(batch * 2);
        for offset in 0..batch {
            if offset > 0 {
                sql.push(',');
            }
            let parameter = offset * 2;
            sql.push_str(&format!("(${}, ${})", parameter + 1, parameter + 2));
            let index = written + offset;
            params.push(Value::Text(seed_path(index)));
            params.push(Value::Blob(document(LINES_PER_FILE, index).into()));
        }
        lix.execute(&sql, &params)
            .await
            .expect("seed write-scaling files");
        written += batch;
    }
}

async fn insert_many<StorageImpl>(lix: &Lix<StorageImpl>, tag: &str, probe: usize, count: usize)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut sql = String::from("INSERT INTO lix_file (path, content) VALUES ");
    let mut params: Vec<Value> = Vec::with_capacity(count * 2);
    for offset in 0..count {
        if offset > 0 {
            sql.push(',');
        }
        let parameter = offset * 2;
        sql.push_str(&format!("(${}, ${})", parameter + 1, parameter + 2));
        params.push(Value::Text(format!(
            "/probe/{tag}-{probe:05}-{offset:04}.txt"
        )));
        params.push(Value::Blob(document(1, probe + offset).into()));
    }
    lix.execute(&sql, &params)
        .await
        .unwrap_or_else(|error| panic!("insert {count} probe files: {error:?}"));
}

fn document_string(lines: usize, index: usize) -> String {
    let mut body = String::with_capacity(lines * 48);
    for line in 0..lines {
        body.push_str(&format!(
            "document {index} line {line}: content padding text\n"
        ));
    }
    body
}

fn document(lines: usize, index: usize) -> Vec<u8> {
    document_string(lines, index).into_bytes()
}

fn build_text_plugin_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_TEXT_plugin_text"));
    let wasm = std::fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read bindep-built text wasm at {}: {error}",
            wasm_path.display()
        )
    });
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../plugins/text/manifest.json").as_bytes(),
        ),
        (
            "schema/text_line.json",
            include_str!("../../../plugins/text/schema/text_line.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer.start_file(path, options).expect("start archive file");
        writer.write_all(bytes).expect("write archive file");
    }
    writer.finish().expect("finish archive").into_inner()
}
