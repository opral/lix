//! Cold-open cost of a plugin file, per lane.
//!
//! The measured operation is the first file write after a cold reopen. That is
//! the durable checkpoint's only consumer: the entity-write path opens its
//! actor through `cold_open_semantic_actor`, which always rebuilds from live
//! entity rows, whereas a file write takes the cold-successor path that either
//! restores the durable document or re-parses the whole file.
//!
//! The `tracked` lane has always published a checkpoint and is unaffected by
//! the untracked change, so it doubles as the regression guard — same
//! operation, same magnitude, byte-identical code on both arms.
//!
//! Deterministic settled-byte accounting is printed beside the timings because
//! a checkpoint is a durable artifact per plugin file and that cost belongs in
//! the same table as the win.
//!
//! Ignored by default; it is a measurement driver, not an assertion.
//!
//! ```text
//! EXPCK_MEMBERS=64,512,4096 cargo test --release -p lix_e2e \
//!   --test untracked_plugin_cold_open_bench -- --ignored --nocapture
//! ```

use std::fmt::Write as _;
use std::fs;
use std::io::{Cursor, Write as _};
use std::ops::Bound;
use std::path::Path;
use std::time::Instant;

use lix::storage::{KeyRange, SpaceId, Storage, StorageSpace, StorageWrite, WriteOptions};
use lix::storage_adapter::{StorageAdapter, StorageReadOptions};
use lix::storage_bench::{layout_space_catalog, space_inventory};
use lix::{Lix, Value, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

const FILE_ID: &str = "01920000-0000-7000-8000-0000000000d1";
const PATH: &str = "/cold-open-bench.json";
const CHECKPOINT_SPACE: &str = "plugin.current_checkpoint.v2";

#[async_trait::async_trait]
trait ColdOpenBenchBackend: Storage + Clone + Send + Sync + Sized + 'static {
    const NAME: &'static str;

    fn open_bench_fixture(path: &Path) -> Self;
    async fn flush_bench_fixture(&self);
}

#[async_trait::async_trait]
impl ColdOpenBenchBackend for RocksDB {
    const NAME: &'static str = "rocksdb";

    fn open_bench_fixture(path: &Path) -> Self {
        Self::open(path).expect("open RocksDB cold-open bench fixture")
    }

    async fn flush_bench_fixture(&self) {
        self.flush().expect("flush RocksDB cold-open bench fixture");
    }
}

#[async_trait::async_trait]
impl ColdOpenBenchBackend for SlateDB {
    const NAME: &'static str = "slatedb";

    fn open_bench_fixture(path: &Path) -> Self {
        Self::open(path).expect("open SlateDB cold-open bench fixture")
    }

    async fn flush_bench_fixture(&self) {
        self.flush_memtable_for_diagnostics()
            .await
            .expect("flush SlateDB cold-open bench fixture");
    }
}

#[tokio::test]
#[ignore = "measurement driver"]
async fn rocksdb_plugin_cold_open_cost_by_lane() {
    measure_cold_open::<RocksDB>().await;
}

#[tokio::test]
#[ignore = "measurement driver"]
async fn slatedb_plugin_cold_open_cost_by_lane() {
    measure_cold_open::<SlateDB>().await;
}

async fn measure_cold_open<B: ColdOpenBenchBackend>() {
    for members in member_counts() {
        for (untracked, wipe) in [(false, false), (false, true), (true, false), (true, true)] {
            let lane = if untracked { "untracked" } else { "tracked" };
            let ckpt = if wipe { "wipe" } else { "keep" };
            let document = json_document(members);
            let directory = tempfile::tempdir().expect("create cold-open bench fixture");
            let storage_path = directory.path().join(".lix");

            let database = B::open_bench_fixture(&storage_path);
            let lix = open_lix()
                .with_storage(database.clone())
                .await
                .expect("open cold-open bench workspace");
            install_json_plugin(&lix).await;
            lix.execute(
                &format!(
                    "INSERT INTO lix_file (id, path, content, lixcol_untracked) \
                     VALUES ($1, $2, $3, {untracked})"
                ),
                &[
                    Value::Text(FILE_ID.to_owned()),
                    Value::Text(PATH.to_owned()),
                    Value::Blob(document.clone().into()),
                ],
            )
            .await
            .expect("seed plugin file");
            database.flush_bench_fixture().await;
            lix.close().await.expect("close seeded workspace");
            drop(database);

            let (checkpoint_bytes, checkpoint_entries, settled_bytes) =
                settled_accounting::<B>(&storage_path).await;
            if wipe {
                wipe_checkpoint_space::<B>(&storage_path).await;
            }

            let database = B::open_bench_fixture(&storage_path);
            let lix = open_lix()
                .with_storage(database.clone())
                .await
                .expect("cold reopen cold-open bench workspace");
            let started = Instant::now();
            lix.execute(
                "UPDATE lix_file SET content = $1 WHERE path = $2",
                &[
                    Value::Blob(json_document_edited(members).into()),
                    Value::Text(PATH.to_owned()),
                ],
            )
            .await
            .expect("first file write after cold reopen");
            let first_edit_us = started.elapsed().as_micros();
            lix.close().await.expect("close measured workspace");
            drop(database);

            println!(
                "\nexpck\t{backend}\t{lane}\t{ckpt}\tmembers={members}\tfile_bytes={file_bytes}\t\
                 first_edit_us={first_edit_us}\tcheckpoint_bytes={checkpoint_bytes}\t\
                 checkpoint_entries={checkpoint_entries}\tsettled_bytes={settled_bytes}",
                backend = B::NAME,
                file_bytes = document.len(),
            );
        }
    }
}

/// Removes every published checkpoint before the measured reopen.
///
/// This is what isolates the checkpoint's contribution from everything else a
/// first edit pays for. On an arm that never publishes for the lane under test
/// the wipe is a no-op over an empty range, which makes the pair a null control
/// running byte-identical work.
async fn wipe_checkpoint_space<B: ColdOpenBenchBackend>(path: &Path) {
    let database = B::open_bench_fixture(path);
    let (space_id, name) = layout_space_catalog()
        .into_iter()
        .find(|(_, name)| *name == CHECKPOINT_SPACE)
        .expect("plugin current-checkpoint space must be catalogued");
    let mut writer = database
        .begin_write(WriteOptions::default())
        .await
        .expect("open checkpoint wipe write");
    writer
        .delete_range(
            StorageSpace::mutable(SpaceId(space_id), name),
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
        )
        .await
        .expect("wipe the checkpoint space");
    writer.commit().await.expect("commit the checkpoint wipe");
    database.flush_bench_fixture().await;
    drop(database);
}

/// Logical settled key+value bytes across every registered space, plus the
/// checkpoint space on its own. Byte counts are deterministic, so one
/// observation per configuration is the whole measurement.
async fn settled_accounting<B: ColdOpenBenchBackend>(path: &Path) -> (usize, usize, usize) {
    let database = B::open_bench_fixture(path);
    let storage = StorageAdapter::new(database.clone());
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open settled-bytes read");
    let mut checkpoint_bytes = 0usize;
    let mut checkpoint_entries = 0usize;
    let mut settled_bytes = 0usize;
    for (_, name) in layout_space_catalog() {
        let entries = space_inventory(&read, name).await;
        let bytes = entries
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum::<usize>();
        settled_bytes += bytes;
        if name == CHECKPOINT_SPACE {
            checkpoint_bytes = bytes;
            checkpoint_entries = entries.len();
        }
    }
    drop(read);
    drop(storage);
    drop(database);
    (checkpoint_bytes, checkpoint_entries, settled_bytes)
}

fn member_counts() -> Vec<usize> {
    match std::env::var("EXPCK_MEMBERS") {
        Ok(raw) => raw
            .split(',')
            .map(|value| {
                value
                    .trim()
                    .parse()
                    .expect("EXPCK_MEMBERS holds comma-separated member counts")
            })
            .collect(),
        Err(_) => vec![64, 512, 4096],
    }
}

fn json_document(members: usize) -> Vec<u8> {
    render_document(members, "value-000000")
}

/// The seeded document with exactly one member's scalar changed, so the
/// measured write is an ordinary small edit whose cost is dominated by having
/// to make the plugin actor exist again rather than by the edit itself.
fn json_document_edited(members: usize) -> Vec<u8> {
    render_document(members, "value-edited")
}

fn render_document(members: usize, first_value: &str) -> Vec<u8> {
    let mut json = String::from("{");
    for index in 0..members {
        if index > 0 {
            json.push(',');
        }
        if index == 0 {
            write!(json, "\"key-{index:06}\":\"{first_value}\"").expect("string write");
        } else {
            write!(json, "\"key-{index:06}\":\"value-{index:06}\"").expect("string write");
        }
    }
    json.push('}');
    json.into_bytes()
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
    .expect("reference json plugin should install");
}

fn build_json_plugin_archive() -> Vec<u8> {
    let wasm_path = Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_JSON_plugin_json"));
    let wasm = fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read plugin component at {}: {error}",
            wasm_path.display()
        )
    });
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    writer
        .start_file("manifest.json", options)
        .expect("manifest entry should start");
    writer
        .write_all(include_str!("../../../plugins/json/manifest.json").as_bytes())
        .expect("manifest should write");
    for (path, schema) in [
        (
            "schema/json_root.json",
            include_str!("../../../plugins/json/schema/json_root.json"),
        ),
        (
            "schema/json_object_member.json",
            include_str!("../../../plugins/json/schema/json_object_member.json"),
        ),
        (
            "schema/json_array_item.json",
            include_str!("../../../plugins/json/schema/json_array_item.json"),
        ),
    ] {
        writer
            .start_file(path, options)
            .expect("schema entry should start");
        writer
            .write_all(schema.as_bytes())
            .expect("schema should write");
    }
    writer
        .start_file("plugin.wasm", options)
        .expect("component entry should start");
    writer.write_all(&wasm).expect("component should write");
    writer
        .finish()
        .expect("plugin archive should finish")
        .into_inner()
}
