//! Per-space storage cost of a write, and what a physical key is made of.
//!
//! Answers two questions no other harness in the tree answers:
//!
//! - **What does one committed row actually cost?** `space_growth` reports which
//!   spaces grow per commit; this reports how many bytes and storage rows each
//!   one grows by, and separates per-commit fixed cost from per-row marginal
//!   cost by varying the commit width.
//! - **What is a physical key made of?** `storage_layout` reports per-space
//!   `key_bytes`; this attributes those bytes to the identifiers embedded in
//!   them, so "the keys are wide" can be checked rather than assumed.
//!
//! Modes:
//!
//! ```text
//! write_amplification seed   <dir> <rows> [seed_width]
//! write_amplification update <dir> <updates> <width> <distinct>
//! write_amplification read   <dir> <reads> <distinct>
//! write_amplification census <dir>
//! ```
//!
//! `seed` inserts `rows` tracked rows through the real SQL commit path in
//! commits of `seed_width` rows (default 1000). Varying `seed_width` while
//! holding `rows` fixed changes how wide the commit that *wrote* a row was,
//! which is what `read` exists to probe: a current-value read locates a row
//! through its owning commit's packed delta, so if read cost tracks
//! `seed_width` the current-value path is paying for history it does not need.
//!
//! `update` applies `updates` single-row
//! `UPDATE`s committed in groups of `width`, cycling over `distinct` distinct
//! rows, so repeated edits to one row can be told apart from edits spread over
//! many. `census` walks every registered space and prints per-space rows, key
//! bytes and value bytes; the difference between two censuses is the cost of
//! whatever ran between them.
//!
//! The key census is decoder-free on purpose: it does not parse per-space key
//! grammars, which differ per space and would rot. It finds maximal runs of
//! printable ASCII inside the raw key and buckets them. Every identifier of
//! interest — branch ids, schema keys, file ids, entity primary keys — is
//! ASCII and cannot contain an unescaped NUL, so a raw run scan is exact.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use lix::integration::Engine;
use lix::storage::Storage;
use lix::storage_adapter::{StorageAdapter, StorageReadOptions};
use lix::storage_bench::{layout_accounting, space_inventory};
use lix::{PreparedDmlParameterBatch, Value};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

/// The one thing the two shipping backends do not expose identically: SlateDB
/// flushes asynchronously, RocksDB synchronously. Everything else this harness
/// needs is on `Storage`.
trait BenchStorage: Storage + Clone + Send + Sync + 'static {
    fn settle(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

impl BenchStorage for SlateDB {
    fn settle(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move { self.flush().await.expect("flush SlateDB") })
    }
}

impl BenchStorage for RocksDB {
    fn settle(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.flush().expect("flush RocksDB");
        Box::pin(async {})
    }
}

const SEED_SQL: &str = "INSERT INTO json_pointer (path, value) VALUES ($1, lix_json($2))";
const UPDATE_SQL: &str = "UPDATE json_pointer SET value = lix_json($1) WHERE path = $2";
const READ_SQL: &str = "SELECT value FROM json_pointer WHERE path = $1";
const SEED_COMMIT_ROWS: usize = 1_000;
const MIN_RUN: usize = 6;

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let mode = args
        .next()
        .expect("usage: write_amplification <mode> <dir> ...");
    let dir = args
        .next()
        .expect("usage: write_amplification <mode> <dir> ...");
    let mut next = |what: &str| -> usize {
        args.next()
            .unwrap_or_else(|| panic!("missing argument {what}"))
            .parse()
            .unwrap_or_else(|_| panic!("argument {what} must be an unsigned integer"))
    };
    let rocksdb = std::env::var("LIX_WA_BACKEND").is_ok_and(|value| value == "rocksdb");
    match mode.as_str() {
        "seed" => {
            let rows = next("rows");
            let seed_width = args
                .next()
                .map_or(SEED_COMMIT_ROWS, |v| v.parse().expect("seed_width"));
            if rocksdb {
                seed(RocksDB::open(&dir).expect("open RocksDB"), rows, seed_width).await;
            } else {
                seed(SlateDB::open(&dir).expect("open SlateDB"), rows, seed_width).await;
            }
        }
        "update" => {
            let updates = next("updates");
            let width = next("width");
            let distinct = next("distinct");
            if rocksdb {
                update(
                    RocksDB::open(&dir).expect("open RocksDB"),
                    updates,
                    width,
                    distinct,
                )
                .await;
            } else {
                update(
                    SlateDB::open(&dir).expect("open SlateDB"),
                    updates,
                    width,
                    distinct,
                )
                .await;
            }
        }
        "read" => {
            let reads = next("reads");
            let distinct = next("distinct");
            if rocksdb {
                read_rows(RocksDB::open(&dir).expect("open RocksDB"), reads, distinct).await;
            } else {
                read_rows(SlateDB::open(&dir).expect("open SlateDB"), reads, distinct).await;
            }
        }
        "census" => {
            if rocksdb {
                census(RocksDB::open(&dir).expect("open RocksDB")).await;
            } else {
                census(SlateDB::open(&dir).expect("open SlateDB")).await;
            }
        }
        other => panic!("unknown mode {other}"),
    }
}

async fn seed<S: BenchStorage>(storage: S, rows: usize, seed_width: usize) {
    Engine::initialize(storage.clone())
        .await
        .expect("initialize repository");
    let engine = Engine::new(storage.clone())
        .await
        .expect("open engine over initialized repository");
    let session = engine
        .open_session()
        .await
        .expect("open session");

    let schema = serde_json::json!({
        "x-lix-key": "json_pointer",
        "x-lix-primary-key": ["/path"],
        "type": "object",
        "required": ["path", "value"],
        "properties": {
            "path": { "type": "string" },
            "value": {
                "type": ["object", "array", "string", "number", "integer", "boolean", "null"]
            }
        },
        "additionalProperties": false
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (lix_json($1), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register json_pointer schema");

    let mut inserted = 0_usize;
    while inserted < rows {
        let chunk = seed_width.max(1).min(rows - inserted);
        let parameter_rows = (inserted..inserted + chunk).map(|index| {
            vec![
                Value::Text(format!("/fixture/path/{index:08}")),
                Value::Text(format!(
                    "{{\"ordinal\":{index},\"payload\":\"row-{index:08}\"}}"
                )),
            ]
        });
        let affected = session
            .execute_prepared_dml_batch(
                Arc::from(SEED_SQL),
                PreparedDmlParameterBatch::from_rows(parameter_rows)
                    .expect("seed parameter batch is rectangular"),
            )
            .await
            .expect("insert seed chunk")
            .iter()
            .map(lix::ExecuteResult::rows_affected)
            .sum::<u64>();
        assert_eq!(affected as usize, chunk);
        inserted += chunk;
    }

    drop(session);
    drop(engine);
    storage.settle().await;
    println!("SEEDED\trows={rows}\tseed_width={seed_width}");
}

/// Applies `updates` row updates in `updates / width` commits.
///
/// One `execute_prepared_dml_batch` is one commit, so `width` is exactly the
/// number of rows a commit carries. Holding `updates` fixed and varying `width`
/// separates per-commit fixed cost from per-row marginal cost.
async fn update<S: BenchStorage>(storage: S, updates: usize, width: usize, distinct: usize) {
    assert!(width > 0 && distinct > 0, "width and distinct must be > 0");
    assert!(
        updates % width == 0,
        "updates must be a whole number of commits"
    );
    let engine = Engine::new(storage.clone())
        .await
        .expect("open engine over initialized repository");
    let session = engine
        .open_session()
        .await
        .expect("open session");

    let mut applied = 0_usize;
    while applied < updates {
        let parameter_rows = (applied..applied + width).map(|index| {
            let target = index % distinct;
            vec![
                Value::Text(format!("{{\"ordinal\":{target},\"revision\":{index}}}")),
                Value::Text(format!("/fixture/path/{target:08}")),
            ]
        });
        session
            .execute_prepared_dml_batch(
                Arc::from(UPDATE_SQL),
                PreparedDmlParameterBatch::from_rows(parameter_rows)
                    .expect("update parameter batch is rectangular"),
            )
            .await
            .expect("apply update commit");
        applied += width;
    }

    drop(session);
    drop(engine);
    storage.settle().await;
    println!(
        "UPDATED\tupdates={updates}\twidth={width}\tdistinct={distinct}\tcommits={}",
        updates / width
    );
}

/// Times `reads` current-value point reads over `distinct` rows.
///
/// This is the read half of a single-row `UPDATE` in isolation: no transaction,
/// no commit, just "find the current value of this row".
async fn read_rows<S: BenchStorage>(storage: S, reads: usize, distinct: usize) {
    let engine = Engine::new(storage.clone())
        .await
        .expect("open engine over initialized repository");
    let session = engine
        .open_session()
        .await
        .expect("open session");

    // Warm the plan cache so the measurement is the read, not planning.
    for index in 0..distinct.min(16) {
        session
            .execute(
                READ_SQL,
                &[Value::Text(format!("/fixture/path/{index:08}"))],
            )
            .await
            .expect("warm read");
    }

    let start = std::time::Instant::now();
    let mut rows_seen = 0_u64;
    for index in 0..reads {
        let target = index % distinct;
        let result = session
            .execute(
                READ_SQL,
                &[Value::Text(format!("/fixture/path/{target:08}"))],
            )
            .await
            .expect("point read");
        rows_seen += result.len() as u64;
    }
    let elapsed = start.elapsed();
    assert_eq!(rows_seen as usize, reads, "every point read must find its row");
    println!(
        "READ\treads={reads}\tdistinct={distinct}\ttotal_us={}\tus_per_read={:.2}",
        elapsed.as_micros(),
        elapsed.as_micros() as f64 / reads as f64,
    );
}

#[derive(Default, Clone)]
struct RunClass {
    occurrences: u64,
    bytes: u64,
}

fn is_canonical_uuid(text: &str) -> bool {
    let bytes = text.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    bytes.iter().enumerate().all(|(index, byte)| match index {
        8 | 13 | 18 | 23 => *byte == b'-',
        _ => byte.is_ascii_hexdigit(),
    })
}

/// Identifier-shaped: a schema key, table name or column name — low cardinality
/// across the whole store, and therefore a dictionary candidate.
fn is_identifier(text: &str) -> bool {
    text.bytes().next().is_some_and(|b| b.is_ascii_alphabetic())
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'.')
}

fn ascii_runs(key: &[u8]) -> Vec<String> {
    let mut runs = Vec::new();
    let mut current: Vec<u8> = Vec::new();
    for &byte in key {
        // Printable ASCII excluding space, so binary noise is less likely to be
        // read as text.
        if (0x21..=0x7e).contains(&byte) {
            current.push(byte);
        } else {
            if current.len() >= MIN_RUN {
                runs.push(String::from_utf8_lossy(&current).into_owned());
            }
            current.clear();
        }
    }
    if current.len() >= MIN_RUN {
        runs.push(String::from_utf8_lossy(&current).into_owned());
    }
    runs
}

async fn census<S: BenchStorage>(backend: S) {
    let storage = StorageAdapter::new(backend);
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open storage snapshot");

    let mut total_rows = 0_u64;
    let mut total_keys = 0_u64;
    let mut total_values = 0_u64;
    let mut uuid_text = RunClass::default();
    let mut ident_text = RunClass::default();
    let mut ranked: BTreeMap<String, RunClass> = BTreeMap::new();

    println!(
        "HEADER\tspace\trows\tkey_bytes\tvalue_bytes\tuuid_occ\tuuid_bytes\tident_occ\tident_bytes"
    );
    for entry in layout_accounting(&read).await {
        if entry.rows == 0 {
            continue;
        }
        let mut space_uuid = RunClass::default();
        let mut space_ident = RunClass::default();
        for (key, _value) in space_inventory(&read, entry.space).await {
            for run in ascii_runs(&key) {
                let len = run.len() as u64;
                if is_canonical_uuid(&run) {
                    space_uuid.occurrences += 1;
                    space_uuid.bytes += len;
                } else if is_identifier(&run) {
                    space_ident.occurrences += 1;
                    space_ident.bytes += len;
                }
                let slot = ranked.entry(run).or_default();
                slot.occurrences += 1;
                slot.bytes += len;
            }
        }
        println!(
            "SPACE\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            entry.space,
            entry.rows,
            entry.key_bytes,
            entry.value_bytes,
            space_uuid.occurrences,
            space_uuid.bytes,
            space_ident.occurrences,
            space_ident.bytes,
        );
        total_rows += entry.rows;
        total_keys += entry.key_bytes;
        total_values += entry.value_bytes;
        uuid_text.occurrences += space_uuid.occurrences;
        uuid_text.bytes += space_uuid.bytes;
        ident_text.occurrences += space_ident.occurrences;
        ident_text.bytes += space_ident.bytes;
    }
    println!(
        "TOTAL\tALL\t{total_rows}\t{total_keys}\t{total_values}\t{}\t{}\t{}\t{}",
        uuid_text.occurrences, uuid_text.bytes, ident_text.occurrences, ident_text.bytes,
    );

    // What narrower identifiers in keys would be worth: a canonical UUID costs
    // 36 text bytes plus 2 terminator bytes where a tag plus 16 raw bytes would
    // do; an identifier costs its length plus 2 where a 4-byte dictionary id
    // would do.
    let uuid_saving = uuid_text.occurrences * (36 + 2 - 17);
    let ident_saving =
        (ident_text.bytes + 2 * ident_text.occurrences).saturating_sub(4 * ident_text.occurrences);
    println!(
        "KEY_TEXT_MODEL\tuuid_saving={uuid_saving}\tident_saving={ident_saving}\ttotal_saving={}\tpct_of_key_bytes={:.1}\tpct_of_logical_bytes={:.1}",
        uuid_saving + ident_saving,
        100.0 * (uuid_saving + ident_saving) as f64 / total_keys.max(1) as f64,
        100.0 * (uuid_saving + ident_saving) as f64 / (total_keys + total_values).max(1) as f64,
    );

    let mut ranked: Vec<_> = ranked.into_iter().collect();
    ranked.sort_by(|left, right| right.1.bytes.cmp(&left.1.bytes));
    for (text, class) in ranked.into_iter().take(20) {
        println!(
            "TOPRUN\tbytes={}\tocc={}\tlen={}\ttext={text}",
            class.bytes,
            class.occurrences,
            text.len(),
        );
    }
}
