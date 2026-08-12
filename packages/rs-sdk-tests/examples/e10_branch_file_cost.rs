//! What does creating a branch cost, as a function of the number of
//! **plugin-backed files** already on the branch?
//!
//! `branch_storage_sharing` answers the same question for entity *rows* and
//! shows a flat curve: `hot_state.root_current_base.v1` publishes one 16-byte
//! reference instead of copying hot rows. That plane does not cover the
//! certified-entity-batch manifests, which are re-keyed from the source
//! generation to the new branch generation one row per (file, format) in
//! `hot_state/tracked_head/hot.rs`. Those rows only exist for files a WASM
//! plugin materialized, so no row-shaped fixture can see them.
//!
//! This example seeds `N` JSON files through the real plugin path, snapshots
//! exact logical layout accounting, creates one branch, and snapshots again.
//! The delta is the price of the branch. Byte and row counts are
//! deterministic, so one repetition per size is the whole measurement.
//!
//! ```text
//! cargo run --release -p lix_tests --example e10_branch_file_cost -- 100 1000 10000
//! ```

use std::collections::BTreeMap;
use std::io::{Cursor, Write};
use std::path::Path;
use std::time::Instant;

use lix::storage::{ReadOptions, Storage};
use lix::storage_adapter::StorageAdapter;
use lix::storage_bench::layout_accounting;
use lix::{CreateBranchOptions, Lix, Value, open_lix};
use lix_storage_rocksdb::RocksDB;

const INSERT_BATCH: usize = 100;

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

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create branch-file-cost runtime");

    runtime.block_on(async {
        for files in sizes {
            run_case(files).await;
        }
    });
}

async fn run_case(files: usize) {
    let root = tempfile::Builder::new()
        .prefix("e10-branch-file-cost-")
        .tempdir()
        .expect("create branch-file-cost directory");
    let db_path = root.path().join("db");
    let storage = RocksDB::open(&db_path).expect("open branch-file-cost RocksDB");
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open branch-file-cost Lix");

    install_json_plugin(&lix).await;
    let seed_started = Instant::now();
    seed_files(&lix, files).await;
    let seed_ms = seed_started.elapsed().as_secs_f64() * 1_000.0;

    let before = snapshot(&storage, &db_path).await;

    let branch_started = Instant::now();
    lix.create_branch(CreateBranchOptions {
        id: None,
        name: "e10-branch".to_owned(),
        from_commit_id: None,
    })
    .await
    .expect("create branch-file-cost branch");
    let branch_ms = branch_started.elapsed().as_secs_f64() * 1_000.0;

    let after = snapshot(&storage, &db_path).await;

    println!(
        "branch_file_cost,files={files},\
before_rows={},after_rows={},d_rows={},\
before_bytes={},after_bytes={},d_bytes={},\
before_physical_bytes={},after_physical_bytes={},d_physical_bytes={},\
seed_ms={seed_ms:.3},create_branch_ms={branch_ms:.3}",
        before.rows(),
        after.rows(),
        after.rows() as i64 - before.rows() as i64,
        before.bytes(),
        after.bytes(),
        after.bytes() as i64 - before.bytes() as i64,
        before.physical_bytes,
        after.physical_bytes,
        after.physical_bytes as i64 - before.physical_bytes as i64,
    );

    let mut names: Vec<&'static str> = before.spaces.keys().copied().collect();
    for name in after.spaces.keys() {
        if !names.contains(name) {
            names.push(name);
        }
    }
    names.sort_unstable();
    for name in names {
        let a = before.spaces.get(name).copied().unwrap_or_default();
        let b = after.spaces.get(name).copied().unwrap_or_default();
        let d_rows = b.rows as i64 - a.rows as i64;
        let d_bytes = (b.key_bytes + b.value_bytes) as i64 - (a.key_bytes + a.value_bytes) as i64;
        if d_rows == 0 && d_bytes == 0 {
            continue;
        }
        println!(
            "branch_file_cost_space,files={files},space={name},\
before_rows={},after_rows={},d_rows={d_rows},\
before_bytes={},after_bytes={},d_bytes={d_bytes}",
            a.rows,
            b.rows,
            a.key_bytes + a.value_bytes,
            b.key_bytes + b.value_bytes,
        );
    }

    lix.close().await.expect("close branch-file-cost Lix");
}

#[derive(Clone, Default)]
struct Snapshot {
    physical_bytes: u64,
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

async fn snapshot(storage: &RocksDB, directory: &Path) -> Snapshot {
    storage.flush().expect("flush branch-file-cost RocksDB");
    let adapter = StorageAdapter::new(storage.clone());
    let read = adapter
        .begin_read(ReadOptions::default())
        .await
        .expect("open branch-file-cost layout read");
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
    Snapshot {
        physical_bytes: directory_bytes(directory),
        spaces,
    }
}

async fn install_json_plugin<StorageImpl>(lix: &Lix<StorageImpl>)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
        &[
            Value::Text("/.lix/plugins/plugin_json.lixplugin".to_owned()),
            Value::Blob(build_json_plugin_archive().into()),
        ],
    )
    .await
    .expect("install branch-file-cost JSON plugin");
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
            params.push(Value::Text(format!("/docs/file-{index:07}.json")));
            params.push(Value::Blob(
                format!(
                    r#"{{"index":{index},"title":"document {index}","body":"{}"}}"#,
                    "content padding for a realistic small document"
                )
                .into_bytes()
                .into(),
            ));
        }
        lix.execute(&sql, &params)
            .await
            .expect("seed branch-file-cost files");
        written += batch;
    }
}

fn build_json_plugin_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_JSON_plugin_json"));
    let wasm = std::fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read bindep-built JSON wasm at {}: {error}",
            wasm_path.display()
        )
    });
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    for (path, bytes) in [
        (
            "manifest.json",
            include_str!("../../../plugins/json/manifest.json").as_bytes(),
        ),
        (
            "schema/json_root.json",
            include_str!("../../../plugins/json/schema/json_root.json").as_bytes(),
        ),
        (
            "schema/json_object_member.json",
            include_str!("../../../plugins/json/schema/json_object_member.json").as_bytes(),
        ),
        (
            "schema/json_array_item.json",
            include_str!("../../../plugins/json/schema/json_array_item.json").as_bytes(),
        ),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer.start_file(path, options).expect("start archive file");
        writer.write_all(bytes).expect("write archive file");
    }
    writer.finish().expect("finish archive").into_inner()
}

fn directory_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path).map_or(0, |entries| {
        entries
            .flatten()
            .map(|entry| {
                let path = entry.path();
                entry.metadata().map_or(0, |metadata| {
                    if metadata.is_dir() {
                        directory_bytes(&path)
                    } else {
                        metadata.len()
                    }
                })
            })
            .sum()
    })
}
