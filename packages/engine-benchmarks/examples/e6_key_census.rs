//! E6 — physical key-byte census.
//!
//! Two modes:
//!   `e6_key_census seed <dir> <rows>`   — seed a repository through the real
//!                                        SQL commit path (tracked + untracked
//!                                        rows), then flush.
//!   `e6_key_census census <dir>`        — walk every registered storage space
//!                                        and attribute physical key bytes to
//!                                        embedded ASCII text runs.
//!
//! The census is decoder-free on purpose: it does not parse per-space key
//! grammars (which differ per space and would rot), it finds maximal runs of
//! printable ASCII inside the raw key and buckets them. Every identifier this
//! experiment cares about — branch ids, schema keys, file ids, entity primary
//! keys — is ASCII, and none of them can contain 0x00 unescaped, so a raw run
//! scan is exact for them.

use std::collections::BTreeMap;
use std::sync::Arc;

use lix::integration::Engine;
use lix::storage_adapter::{StorageAdapter, StorageReadOptions};
use lix::storage_bench::{layout_accounting, space_inventory};
use lix::{PreparedDmlParameterBatch, Value};
use lix_storage_slatedb::SlateDB;

const BOUND_SEED_JSON_SQL: &str =
    "INSERT INTO json_pointer (path, value) VALUES ($1, lix_json($2))";
const CHUNK_ROWS: usize = 1_000;
const MIN_RUN: usize = 6;

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let mode = args.next().expect("usage: e6_key_census <seed|census> ...");
    match mode.as_str() {
        "seed" => {
            let dir = args.next().expect("usage: e6_key_census seed <dir> <rows>");
            let rows: usize = args
                .next()
                .expect("usage: e6_key_census seed <dir> <rows>")
                .parse()
                .expect("row count");
            seed(&dir, rows).await;
        }
        "census" => {
            let dir = args.next().expect("usage: e6_key_census census <dir>");
            census(&dir).await;
        }
        other => panic!("unknown mode {other}"),
    }
}

async fn seed(path: &str, rows: usize) {
    let storage = SlateDB::open(path).expect("open SlateDB");
    Engine::initialize(storage.clone())
        .await
        .expect("initialize repository");
    let engine = Engine::new(storage.clone())
        .await
        .expect("open engine over initialized repository");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open workspace session");

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
        let chunk = CHUNK_ROWS.min(rows - inserted);
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
                Arc::from(BOUND_SEED_JSON_SQL),
                PreparedDmlParameterBatch::from_rows(parameter_rows)
                    .expect("fixture parameter batch is rectangular"),
            )
            .await
            .expect("insert fixture chunk")
            .iter()
            .map(lix::ExecuteResult::rows_affected)
            .sum::<u64>();
        assert_eq!(affected as usize, chunk);
        inserted += chunk;
    }

    for index in 0..64_usize {
        session
            .execute(
                "INSERT INTO json_pointer (path, value, lixcol_untracked) \
                 VALUES ($1, lix_json($2), true)",
                &[
                    Value::Text(format!("/fixture/untracked/{index:04}")),
                    Value::Text(format!("\"untracked-{index:04}\"")),
                ],
            )
            .await
            .expect("insert untracked fixture row");
    }

    drop(session);
    drop(engine);
    storage.flush().await.expect("flush SlateDB");
    println!("SEEDED\trows={rows}\tdir={path}");
}

#[derive(Default, Clone)]
struct RunClass {
    occurrences: u64,
    bytes: u64,
}

#[derive(Default)]
struct SpaceCensus {
    rows: u64,
    key_bytes: u64,
    value_bytes: u64,
    uuid_text: RunClass,
    ident_text: RunClass,
    other_text: RunClass,
    distinct: BTreeMap<String, RunClass>,
}

fn is_canonical_uuid(text: &str) -> bool {
    let bytes = text.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    for (index, byte) in bytes.iter().enumerate() {
        let ok = match index {
            8 | 13 | 18 | 23 => *byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        };
        if !ok {
            return false;
        }
    }
    true
}

/// Identifier-shaped: a schema key / table name / column name. Low cardinality
/// across the store, therefore a dictionary candidate.
fn is_identifier(text: &str) -> bool {
    !text.is_empty()
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'.')
        && text.bytes().next().is_some_and(|b| b.is_ascii_alphabetic())
}

fn ascii_runs(key: &[u8]) -> Vec<String> {
    let mut runs = Vec::new();
    let mut current: Vec<u8> = Vec::new();
    for &byte in key {
        // Printable ASCII, excluding space so binary noise is less likely to
        // be read as text.
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

async fn census(path: &str) {
    let storage = StorageAdapter::new(SlateDB::open(path).expect("open SlateDB"));
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open storage snapshot");

    let accounting = layout_accounting(&read).await;
    let mut totals = SpaceCensus::default();
    let mut per_space: Vec<(String, SpaceCensus)> = Vec::new();

    for entry in &accounting {
        if entry.rows == 0 {
            continue;
        }
        let mut census = SpaceCensus {
            rows: entry.rows,
            key_bytes: entry.key_bytes,
            value_bytes: entry.value_bytes,
            ..SpaceCensus::default()
        };
        for (key, _value) in space_inventory(&read, entry.space).await {
            for run in ascii_runs(&key) {
                let len = run.len() as u64;
                let class = if is_canonical_uuid(&run) {
                    &mut census.uuid_text
                } else if is_identifier(&run) {
                    &mut census.ident_text
                } else {
                    &mut census.other_text
                };
                class.occurrences += 1;
                class.bytes += len;
                let slot = census.distinct.entry(run).or_default();
                slot.occurrences += 1;
                slot.bytes += len;
            }
        }
        totals.rows += census.rows;
        totals.key_bytes += census.key_bytes;
        totals.value_bytes += census.value_bytes;
        totals.uuid_text.occurrences += census.uuid_text.occurrences;
        totals.uuid_text.bytes += census.uuid_text.bytes;
        totals.ident_text.occurrences += census.ident_text.occurrences;
        totals.ident_text.bytes += census.ident_text.bytes;
        totals.other_text.occurrences += census.other_text.occurrences;
        totals.other_text.bytes += census.other_text.bytes;
        per_space.push((entry.space.to_string(), census));
    }

    println!(
        "HEADER\tspace\trows\tkey_bytes\tvalue_bytes\tuuid_occ\tuuid_bytes\tident_occ\tident_bytes\tother_occ\tother_bytes\ttext_pct_of_key"
    );
    for (name, census) in &per_space {
        let text_bytes = census.uuid_text.bytes + census.ident_text.bytes + census.other_text.bytes;
        println!(
            "SPACE\t{name}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.1}",
            census.rows,
            census.key_bytes,
            census.value_bytes,
            census.uuid_text.occurrences,
            census.uuid_text.bytes,
            census.ident_text.occurrences,
            census.ident_text.bytes,
            census.other_text.occurrences,
            census.other_text.bytes,
            100.0 * text_bytes as f64 / census.key_bytes.max(1) as f64,
        );
    }
    let text_bytes = totals.uuid_text.bytes + totals.ident_text.bytes + totals.other_text.bytes;
    println!(
        "TOTAL\tALL\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.1}",
        totals.rows,
        totals.key_bytes,
        totals.value_bytes,
        totals.uuid_text.occurrences,
        totals.uuid_text.bytes,
        totals.ident_text.occurrences,
        totals.ident_text.bytes,
        totals.other_text.occurrences,
        totals.other_text.bytes,
        100.0 * text_bytes as f64 / totals.key_bytes.max(1) as f64,
    );

    // Savings model: a canonical UUID text run (36 bytes + 2 terminator bytes
    // in the order-preserving codec) becomes tag + 16 raw bytes = 17. An
    // identifier run becomes a 4-byte dictionary id, replacing len + 2.
    let uuid_saving = totals.uuid_text.occurrences * (36 + 2 - 17);
    let ident_saving = totals
        .ident_text
        .bytes
        .saturating_add(2 * totals.ident_text.occurrences)
        .saturating_sub(4 * totals.ident_text.occurrences);
    println!(
        "MODEL\tuuid_key_saving_bytes={uuid_saving}\tident_key_saving_bytes={ident_saving}\ttotal_key_saving_bytes={}\tpct_of_key_bytes={:.1}\tpct_of_logical_bytes={:.1}",
        uuid_saving + ident_saving,
        100.0 * (uuid_saving + ident_saving) as f64 / totals.key_bytes.max(1) as f64,
        100.0 * (uuid_saving + ident_saving) as f64
            / (totals.key_bytes + totals.value_bytes).max(1) as f64,
    );

    // Top distinct runs across the whole store, so the dictionary hypothesis
    // can be checked rather than assumed.
    let mut all: BTreeMap<String, RunClass> = BTreeMap::new();
    for (_name, census) in &per_space {
        for (text, class) in &census.distinct {
            let slot = all.entry(text.clone()).or_default();
            slot.occurrences += class.occurrences;
            slot.bytes += class.bytes;
        }
    }
    let mut ranked: Vec<_> = all.into_iter().collect();
    ranked.sort_by(|left, right| right.1.bytes.cmp(&left.1.bytes));
    for (text, class) in ranked.into_iter().take(40) {
        println!(
            "TOPRUN\tbytes={}\tocc={}\tlen={}\tuuid={}\ttext={text}",
            class.bytes,
            class.occurrences,
            text.len(),
            is_canonical_uuid(&text),
        );
    }
}
