//! Does delta encoding pay for the content-addressed JSON store?
//!
//! `duplicate_audit` established that `json_store.json` is full of *near*
//! duplicates: distinct payloads that agree with a neighbour outside a narrow
//! byte window. Near-duplication is a suggestive shape, not a case. The case
//! needs three numbers the auditor cannot produce:
//!
//! 1. **Edit size relative to payload size.** A delta only pays when the
//!    changed span is a small fraction of the document. The auditor probes
//!    fixed 8/16/32/64-byte windows against the *stored* (already compressed)
//!    bytes, so a "64-byte prefix" on a 181-byte compressed row is a third of
//!    the row, not a rounding error.
//! 2. **How much of that near-duplication zstd already captured.** Every
//!    payload is individually zstd-compressed at level 1 today. A delta only
//!    earns the bytes zstd left on the table.
//! 3. **What a delta base costs to reach.** A payload's natural base is the
//!    previous version of the same entity, which is a different content
//!    address in the same space.
//!
//! Modes:
//! ```text
//! expu_json_delta build  <corpus> <dir> <writelog>  # seed via the real SQL commit path
//! expu_json_delta oracle <writelog>                 # delta/dictionary/zstd accounting
//! expu_json_delta store  <dir>                      # decode the settled JSON space
//! expu_json_delta readcost <dir>                    # point-read cost versus chain depth
//! ```
//!
//! Every corpus is seeded through `SessionContext` SQL, so the write log is
//! exactly the payload stream an ordinary commit stages. The oracle never
//! touches lix internals: it replays the store's own encoder rule
//! (`>= 512` bytes, zstd level 1, keep raw unless it saves `>= 128` bytes,
//! plus a 20-byte envelope) so "bytes today" is the real stored size.

#![allow(clippy::large_futures)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use lix::Value;
use lix::registered_spaces::JSON_SPACE;
use lix::storage::Storage;
use lix::storage_adapter::{
    PointReadPlan, StorageAdapter, StorageGetOptions, StorageKey, StorageReadOptions,
};
use lix::storage_bench::{space_inventory, storage_space_by_name};
use lix::{Lix, open_lix};
use lix_storage_slatedb::SlateDB;

/// Mirrors `json_store::store`: magic + codec byte + u64 uncompressed length.
const STORED_JSON_MAGIC: &[u8] = b"lix-json:v1";
const STORED_JSON_HEADER_LEN: usize = STORED_JSON_MAGIC.len() + 1 + 8;
/// Mirrors `json_store::store::ZSTD_MIN_JSON_BYTES`.
const ZSTD_MIN_JSON_BYTES: usize = 512;
/// Mirrors `json_store::store::MIN_ZSTD_SAVINGS_BYTES`.
const MIN_ZSTD_SAVINGS_BYTES: usize = 128;
/// Mirrors `json_store::types::JSON_INLINE_MAX_BYTES`.
const JSON_INLINE_MAX_BYTES: usize = 1024;
/// Mirrors the store's compression level.
const ZSTD_LEVEL: i32 = 1;
/// A delta row must name its base. One content address, 32 bytes.
const DELTA_BASE_REF_BYTES: usize = 32;

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let usage = "usage: expu_json_delta (build <corpus> <dir> <writelog> | oracle <writelog> | store <dir>)";
    let mode = args.get(1).map(String::as_str).expect(usage);
    match mode {
        "build" => {
            let corpus = args.get(2).map(String::as_str).expect(usage);
            let dir = PathBuf::from(args.get(3).expect(usage));
            let log = PathBuf::from(args.get(4).expect(usage));
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
                .expect("build runtime");
            runtime.block_on(build(corpus, &dir, &log));
        }
        "oracle" => oracle(&PathBuf::from(args.get(2).expect(usage))),
        "store" => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
                .expect("build runtime");
            runtime.block_on(store_report(&PathBuf::from(args.get(2).expect(usage))));
        }
        "readcost" => {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
                .expect("build runtime");
            runtime.block_on(read_cost(&PathBuf::from(args.get(2).expect(usage))));
        }
        other => panic!("unknown mode '{other}'; {usage}"),
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

// ---------------------------------------------------------------------------
// Write log: the exact (entity, payload) stream the commit path staged.
// ---------------------------------------------------------------------------

struct WriteLog {
    file: std::io::BufWriter<std::fs::File>,
}

impl WriteLog {
    fn create(path: &Path) -> Self {
        Self {
            file: std::io::BufWriter::new(
                std::fs::File::create(path).expect("create expU write log"),
            ),
        }
    }

    fn push(&mut self, entity: &str, json: &str) {
        let entity = entity.as_bytes();
        let json = json.as_bytes();
        self.file
            .write_all(&(entity.len() as u32).to_le_bytes())
            .expect("write entity length");
        self.file.write_all(entity).expect("write entity");
        self.file
            .write_all(&(json.len() as u32).to_le_bytes())
            .expect("write payload length");
        self.file.write_all(json).expect("write payload");
    }

    fn finish(mut self) {
        self.file.flush().expect("flush expU write log");
    }
}

fn read_write_log(path: &Path) -> Vec<(String, Vec<u8>)> {
    let mut file = std::io::BufReader::new(std::fs::File::open(path).expect("open expU write log"));
    let mut entries = Vec::new();
    let mut length = [0_u8; 4];
    loop {
        match file.read_exact(&mut length) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(error) => panic!("read expU write log: {error}"),
        }
        let mut entity = vec![0_u8; u32::from_le_bytes(length) as usize];
        file.read_exact(&mut entity).expect("read entity");
        file.read_exact(&mut length).expect("read payload length");
        let mut json = vec![0_u8; u32::from_le_bytes(length) as usize];
        file.read_exact(&mut json).expect("read payload");
        entries.push((String::from_utf8(entity).expect("utf8 entity"), json));
    }
    entries
}

// ---------------------------------------------------------------------------
// Corpora
// ---------------------------------------------------------------------------

/// A structured document with a realistic edit surface.
///
/// `blocks` dominates the byte count, so a one-block edit is the JSON analogue
/// of "the user typed a sentence". `version`/`updated_at` sit in the header,
/// which is what makes the *leading* bytes differ on every single edit even
/// when the body is untouched — exactly the prefix-64 shape the auditor saw.
struct DocShape {
    blocks: usize,
    block_bytes: usize,
}

impl DocShape {
    /// Renders a document from its per-block revision vector.
    ///
    /// Edits *accumulate*: a block keeps the text it was last given. Two
    /// consecutive versions therefore differ in exactly the block that was
    /// edited plus the header, which is what makes the measured edit span an
    /// honest number rather than the envelope of two unrelated changes.
    fn document(&self, doc: usize, version: usize, block_revisions: &[usize]) -> String {
        let mut out = String::with_capacity(self.blocks * (self.block_bytes + 64) + 256);
        out.push_str(&format!(
            r#"{{"id":"doc-{doc:06}","schema":"expu/document/v1","version":{version},"updated_at":"2026-08-{:02}T{:02}:{:02}:00Z","author":{{"id":"user-{:04}","name":"Contributor {:04}"}},"tags":["draft","review","expu"],"blocks":["#,
            (version % 28) + 1,
            version % 24,
            version % 60,
            doc % 64,
            doc % 64,
        ));
        for (block, revision) in block_revisions.iter().enumerate() {
            if block > 0 {
                out.push(',');
            }
            out.push_str(&format!(
                r#"{{"id":"blk-{block:04}","type":"paragraph","text":"{}"}}"#,
                block_text(doc, block, *revision, self.block_bytes),
            ));
        }
        out.push_str(r#"],"meta":{"words":1234,"locale":"en-US","status":"active"}}"#);
        out
    }
}

/// Deterministic prose-shaped filler. Word-like tokens keep zstd's literal and
/// match distribution close to real text instead of a single long run.
fn block_text(doc: usize, block: usize, revision: usize, bytes: usize) -> String {
    const WORDS: [&str; 16] = [
        "the", "quick", "brown", "fox", "jumps", "over", "lazy", "dog", "storage", "layout",
        "commit", "payload", "version", "delta", "content", "address",
    ];
    let mut state = ((doc as u64) << 32 | (block as u64) << 16 | revision as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        | 1;
    let mut out = String::with_capacity(bytes + 16);
    while out.len() < bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(WORDS[(state % WORDS.len() as u64) as usize]);
    }
    out.truncate(bytes);
    out
}

async fn build(corpus: &str, dir: &Path, log_path: &Path) {
    assert!(
        !dir.exists(),
        "refusing to seed into an existing directory: {}",
        dir.display()
    );
    let storage = SlateDB::open(dir).expect("open SlateDB corpus");
    open_lix()
        .with_storage(storage.clone())
        .await
        .expect("initialize corpus repository");
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open corpus lix");
    let session = lix
        .open_another_session()
        .await
        .expect("open corpus workspace");
    let mut log = WriteLog::create(log_path);

    match corpus {
        // Many documents, each edited many times, one block per edit. This is
        // the shape a delta encoder is supposed to win on.
        "doc_edits" | "doc_small" | "doc_large" | "doc_scattered" | "doc_header_only" => {
            let shape = match corpus {
                "doc_small" => DocShape {
                    blocks: env_usize("LIX_EXPU_BLOCKS", 6),
                    block_bytes: env_usize("LIX_EXPU_BLOCK_BYTES", 180),
                },
                "doc_large" => DocShape {
                    blocks: env_usize("LIX_EXPU_BLOCKS", 200),
                    block_bytes: env_usize("LIX_EXPU_BLOCK_BYTES", 400),
                },
                _ => DocShape {
                    blocks: env_usize("LIX_EXPU_BLOCKS", 40),
                    block_bytes: env_usize("LIX_EXPU_BLOCK_BYTES", 220),
                },
            };
            register_document_schema(&session).await;
            let docs = env_usize("LIX_EXPU_DOCS", 400);
            let rounds = env_usize("LIX_EXPU_ROUNDS", 25);
            let commit_rows = env_usize("LIX_EXPU_COMMIT_ROWS", 200);
            let mut revisions = vec![vec![0_usize; shape.blocks]; docs];
            seed_documents(&session, &shape, &revisions, commit_rows, &mut log).await;
            edit_documents(
                &session,
                &shape,
                corpus,
                &mut revisions,
                rounds,
                commit_rows,
                &mut log,
            )
            .await;
        }
        // The built-in key/value plugin schema: a plugin-driven shape whose
        // payloads straddle the 1 KiB inline threshold.
        // `kv_rewrite` replaces the whole value body on every edit: the
        // negative control that shows what the oracle reports when there is no
        // delta to find.
        "kv" | "kv_rewrite" => {
            let keys = env_usize("LIX_EXPU_DOCS", 2_000);
            let rounds = env_usize("LIX_EXPU_ROUNDS", 10);
            let value_bytes = env_usize("LIX_EXPU_BLOCK_BYTES", 4_096);
            let rewrite = corpus == "kv_rewrite";
            seed_key_values(&session, keys, value_bytes, rewrite, &mut log).await;
            edit_key_values(&session, keys, rounds, value_bytes, rewrite, &mut log).await;
        }
        other => panic!("unknown corpus '{other}'"),
    }

    log.finish();
    drop(session);
    drop(lix);
    storage.flush().await.expect("flush SlateDB WAL");
    storage
        .flush_memtable_for_diagnostics()
        .await
        .expect("flush SlateDB memtable");
    drop(storage);
    println!("BUILT\tcorpus={corpus}\tdir={}", dir.display());
}

async fn register_document_schema<S>(session: &Lix<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "x-lix-key": "expu_document",
        "x-lix-primary-key": ["/path"],
        "type": "object",
        "required": ["path", "doc"],
        "properties": {
            "path": { "type": "string" },
            "doc": { "type": ["object", "array", "string", "number", "boolean", "null"] }
        },
        "additionalProperties": false
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (CAST($1 AS JSONB), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register expU document schema");
}

async fn seed_documents<S>(
    session: &Lix<S>,
    shape: &DocShape,
    revisions: &[Vec<usize>],
    commit_rows: usize,
    log: &mut WriteLog,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let docs = revisions.len();
    let mut index = 0;
    while index < docs {
        let end = (index + commit_rows).min(docs);
        let mut transaction = session.begin_transaction().await.expect("begin seed");
        while index < end {
            let path = format!("/doc/{index:06}");
            let document = shape.document(index, 0, &revisions[index]);
            transaction
                .execute(
                    "INSERT INTO expu_document (path, doc) VALUES ($1, CAST($2 AS JSONB))",
                    &[Value::Text(path.clone()), Value::Text(document.clone())],
                )
                .await
                .expect("stage seed document");
            log.push(&path, &document);
            index += 1;
        }
        transaction.commit().await.expect("commit seed batch");
    }
}

async fn edit_documents<S>(
    session: &Lix<S>,
    shape: &DocShape,
    corpus: &str,
    revisions: &mut [Vec<usize>],
    rounds: usize,
    commit_rows: usize,
    log: &mut WriteLog,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let docs = revisions.len();
    for round in 1..=rounds {
        let mut index = 0;
        while index < docs {
            let end = (index + commit_rows).min(docs);
            let mut transaction = session.begin_transaction().await.expect("begin edit");
            while index < end {
                // `doc_header_only` bumps only the header fields, the pure
                // "leading bytes differ" case. Everything else also rewrites
                // one body block, the realistic single-field edit.
                if corpus != "doc_header_only" {
                    let block = (round * 7 + index) % shape.blocks;
                    revisions[index][block] = round;
                }
                let path = format!("/doc/{index:06}");
                let document = shape.document(index, round, &revisions[index]);
                transaction
                    .execute(
                        "UPDATE expu_document SET doc = CAST($2 AS JSONB) WHERE path = $1",
                        &[Value::Text(path.clone()), Value::Text(document.clone())],
                    )
                    .await
                    .expect("stage document edit");
                log.push(&path, &document);
                index += 1;
            }
            transaction.commit().await.expect("commit edit batch");
        }
    }
}

fn key_value_payload(key: usize, revision: usize, bytes: usize, rewrite: bool) -> String {
    const STATUS: [&str; 4] = ["active", "paused", "draining", "retired"];
    serde_json::json!({
        "revision": revision,
        "owner": format!("service-{:03}", key % 32),
        "status": STATUS[revision % STATUS.len()],
        "payload": block_text(key, 0, if rewrite { revision } else { 0 }, bytes),
    })
    .to_string()
}

async fn seed_key_values<S>(
    session: &Lix<S>,
    keys: usize,
    value_bytes: usize,
    rewrite: bool,
    log: &mut WriteLog,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut transaction = session.begin_transaction().await.expect("begin kv seed");
    for key in 0..keys {
        let name = format!("expu_key_{key:06}");
        let payload = key_value_payload(key, 0, value_bytes, rewrite);
        transaction
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ($1, CAST($2 AS JSONB))",
                &[Value::Text(name.clone()), Value::Text(payload.clone())],
            )
            .await
            .expect("stage kv seed");
        log.push(&name, &payload);
        if key % 500 == 499 {
            transaction.commit().await.expect("commit kv seed batch");
            transaction = session.begin_transaction().await.expect("begin kv seed");
        }
    }
    transaction.commit().await.expect("commit kv seed tail");
}

async fn edit_key_values<S>(
    session: &Lix<S>,
    keys: usize,
    rounds: usize,
    value_bytes: usize,
    rewrite: bool,
    log: &mut WriteLog,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    for round in 1..=rounds {
        let mut transaction = session.begin_transaction().await.expect("begin kv edit");
        for key in 0..keys {
            let name = format!("expu_key_{key:06}");
            let payload = key_value_payload(key, round, value_bytes, rewrite);
            transaction
                .execute(
                    "UPDATE lix_key_value SET value = CAST($2 AS JSONB) WHERE key = $1",
                    &[Value::Text(name.clone()), Value::Text(payload.clone())],
                )
                .await
                .expect("stage kv edit");
            log.push(&name, &payload);
            if key % 500 == 499 {
                transaction.commit().await.expect("commit kv edit batch");
                transaction = session.begin_transaction().await.expect("begin kv edit");
            }
        }
        transaction.commit().await.expect("commit kv edit tail");
    }
}

// ---------------------------------------------------------------------------
// The store's own encoder rule, replayed offline.
// ---------------------------------------------------------------------------

fn zstd_compress(level: i32, data: &[u8]) -> Vec<u8> {
    zstd::bulk::compress(data, level).expect("zstd compress")
}

/// Exactly what `StoredJsonBatchEncoder::append_json_with_ref` produces today,
/// parameterized by compression level so a "just raise the level" arm can be
/// priced against a delta scheme without changing anything else.
fn stored_len_at_level(raw: &[u8], level: i32) -> usize {
    let selected = if raw.len() >= ZSTD_MIN_JSON_BYTES {
        let compressed = zstd_compress(level, raw);
        if raw.len().saturating_sub(compressed.len()) >= MIN_ZSTD_SAVINGS_BYTES {
            compressed.len()
        } else {
            raw.len()
        }
    } else {
        raw.len()
    };
    STORED_JSON_HEADER_LEN + selected
}

fn stored_len_today(raw: &[u8]) -> usize {
    stored_len_at_level(raw, ZSTD_LEVEL)
}

/// Decodes one settled `json_store.json` row back to its JSON text.
///
/// The envelope is replayed here rather than reached for through Lix so
/// this tool stays a pure observer of the format it is measuring, exactly like
/// `stored_len_at_level` replays the encoder side.
fn decode_stored_json(value: &[u8]) -> Vec<u8> {
    assert!(
        value.len() >= STORED_JSON_HEADER_LEN && value.starts_with(STORED_JSON_MAGIC),
        "stored JSON payload has an unexpected envelope"
    );
    let codec = value[STORED_JSON_MAGIC.len()];
    let length_start = STORED_JSON_MAGIC.len() + 1;
    let uncompressed_len = u64::from_be_bytes(
        value[length_start..length_start + 8]
            .try_into()
            .expect("stored JSON length header is fixed size"),
    ) as usize;
    let payload = &value[length_start + 8..];
    match codec {
        0 => payload.to_vec(),
        1 => zstd::bulk::decompress(payload, uncompressed_len).expect("decompress stored json"),
        other => panic!("stored JSON payload has unknown codec byte {other}"),
    }
}

/// Delta-encoded length: the same envelope, plus a base content address, with
/// the frame produced against the base payload as a raw zstd dictionary. This
/// is the `zstd --patch-from` model at the store's own compression level.
fn stored_len_delta(raw: &[u8], base: &[u8]) -> usize {
    let mut compressor =
        zstd::bulk::Compressor::with_dictionary(ZSTD_LEVEL, base).expect("dictionary compressor");
    let frame = compressor.compress(raw).expect("delta compress");
    STORED_JSON_HEADER_LEN + DELTA_BASE_REF_BYTES + frame.len()
}

fn common_prefix(left: &[u8], right: &[u8]) -> usize {
    left.iter().zip(right).take_while(|(a, b)| a == b).count()
}

fn common_suffix(left: &[u8], right: &[u8], skip: usize) -> usize {
    left.iter()
        .rev()
        .zip(right.iter().rev())
        .take(left.len().min(right.len()).saturating_sub(skip))
        .take_while(|(a, b)| a == b)
        .count()
}

fn percentile(sorted: &[f64], fraction: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() - 1) as f64 * fraction).round() as usize;
    sorted[index]
}

fn percentile_u64(sorted: &[u64], fraction: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() - 1) as f64 * fraction).round() as usize;
    sorted[index]
}

fn percent(part: u64, whole: u64) -> f64 {
    if whole == 0 {
        return 0.0;
    }
    100.0 * part as f64 / whole as f64
}

// ---------------------------------------------------------------------------
// Oracle
// ---------------------------------------------------------------------------

fn oracle(log_path: &Path) {
    let entries = read_write_log(log_path);

    // Per entity, the ordered stream of *distinct* payloads. A repeat write of
    // identical content is already free (content addressing), so it neither
    // costs bytes nor extends a chain.
    let mut order: Vec<String> = Vec::new();
    let mut by_entity: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
    let mut stored_digests: HashSet<[u8; 32]> = HashSet::new();
    let mut inline_writes = 0_u64;
    let mut out_of_band_writes = 0_u64;
    let mut raw_lengths: Vec<u64> = Vec::new();

    for (entity, json) in &entries {
        raw_lengths.push(json.len() as u64);
        if json.len() <= JSON_INLINE_MAX_BYTES {
            inline_writes += 1;
            continue;
        }
        out_of_band_writes += 1;
        if !stored_digests.insert(*blake3::hash(json).as_bytes()) {
            continue;
        }
        let versions = by_entity.entry(entity.clone()).or_insert_with(|| {
            order.push(entity.clone());
            Vec::new()
        });
        versions.push(json.clone());
    }

    raw_lengths.sort_unstable();
    println!(
        "PAYLOAD\twrites={}\tinline_writes={inline_writes}\tout_of_band_writes={out_of_band_writes}\tdistinct_out_of_band={}\tentities={}\traw_p10={}\traw_p50={}\traw_p90={}\traw_max={}",
        entries.len(),
        stored_digests.len(),
        by_entity.len(),
        percentile_u64(&raw_lengths, 0.10),
        percentile_u64(&raw_lengths, 0.50),
        percentile_u64(&raw_lengths, 0.90),
        raw_lengths.last().copied().unwrap_or_default(),
    );

    // Bounded-chain arms. `1` is "no delta at all" and the largest value is an
    // unbounded chain; everything between is a periodic full snapshot.
    let depths = [1_usize, 2, 4, 8, 16, usize::MAX];
    let levels = [ZSTD_LEVEL, 3, 9, 19];

    let mut raw_bytes = 0_u64;
    let mut level_bytes = [0_u64; 4];
    let mut delta_chain_bytes = 0_u64;
    let mut delta_anchor_bytes = 0_u64;
    let mut bounded_bytes = [0_u64; 6];
    let mut edit_ratios: Vec<f64> = Vec::new();
    let mut edit_spans: Vec<u64> = Vec::new();
    let mut chain_lengths: Vec<u64> = Vec::new();
    // Byte savings attributable only to the *successors*, so a corpus with one
    // version per entity cannot flatter the delta arm.
    let mut successor_today_bytes = 0_u64;
    let mut successor_delta_bytes = 0_u64;
    let mut successor_anchor_bytes = 0_u64;

    for entity in &order {
        let versions = &by_entity[entity];
        chain_lengths.push(versions.len() as u64);
        for (index, raw) in versions.iter().enumerate() {
            raw_bytes += raw.len() as u64;
            for (slot, level) in levels.iter().enumerate() {
                level_bytes[slot] += stored_len_at_level(raw, *level) as u64;
            }
            let today = stored_len_today(raw) as u64;
            if index == 0 {
                delta_chain_bytes += today;
                delta_anchor_bytes += today;
                for slot in 0..depths.len() {
                    bounded_bytes[slot] += today;
                }
                continue;
            }
            let base = &versions[index - 1];
            let delta = stored_len_delta(raw, base) as u64;
            // The anchor arm always deltas against version 0, so every payload
            // is reachable in exactly one extra read regardless of history
            // depth. It trades some compression for a hard depth bound of 1.
            let anchor = stored_len_delta(raw, &versions[0]) as u64;
            delta_chain_bytes += delta;
            delta_anchor_bytes += anchor;
            successor_today_bytes += today;
            successor_delta_bytes += delta;
            successor_anchor_bytes += anchor;
            for (slot, depth) in depths.iter().enumerate() {
                bounded_bytes[slot] += if *depth == 1 || index % depth == 0 {
                    today
                } else {
                    delta
                };
            }

            let prefix = common_prefix(raw, base);
            let suffix = common_suffix(raw, base, prefix);
            let span = raw.len().saturating_sub(prefix + suffix) as u64;
            edit_spans.push(span);
            edit_ratios.push(span as f64 / raw.len() as f64);
        }
    }

    edit_ratios.sort_by(|a, b| a.partial_cmp(b).expect("finite ratio"));
    edit_spans.sort_unstable();
    chain_lengths.sort_unstable();
    let today_bytes = level_bytes[0];

    println!(
        "EDIT_SPAN\tpairs={}\tspan_p10={}\tspan_p50={}\tspan_p90={}\tspan_p99={}\tratio_p10={:.4}\tratio_p50={:.4}\tratio_p90={:.4}\tratio_p99={:.4}",
        edit_spans.len(),
        percentile_u64(&edit_spans, 0.10),
        percentile_u64(&edit_spans, 0.50),
        percentile_u64(&edit_spans, 0.90),
        percentile_u64(&edit_spans, 0.99),
        percentile(&edit_ratios, 0.10),
        percentile(&edit_ratios, 0.50),
        percentile(&edit_ratios, 0.90),
        percentile(&edit_ratios, 0.99),
    );
    println!(
        "CHAIN\tentities={}\tlen_p50={}\tlen_p90={}\tlen_max={}",
        chain_lengths.len(),
        percentile_u64(&chain_lengths, 0.50),
        percentile_u64(&chain_lengths, 0.90),
        chain_lengths.last().copied().unwrap_or_default(),
    );

    // A single shared dictionary trained on the corpus: the delta idea without
    // per-payload bases, chains, or a base-liveness rule.
    let samples = order
        .iter()
        .flat_map(|entity| by_entity[entity].iter())
        .take(env_usize("LIX_EXPU_DICT_SAMPLES", 512))
        .map(Vec::as_slice)
        .collect::<Vec<_>>();
    let dictionary_bytes = env_usize("LIX_EXPU_DICT_BYTES", 110 * 1024);
    let dictionary = zstd::dict::from_samples(&samples, dictionary_bytes).ok();
    let mut dictionary_total = 0_u64;
    if let Some(dictionary) = dictionary.as_ref() {
        let mut compressor = zstd::bulk::Compressor::with_dictionary(ZSTD_LEVEL, dictionary)
            .expect("dictionary compressor");
        for entity in &order {
            for raw in &by_entity[entity] {
                let frame = compressor.compress(raw).expect("dictionary compress");
                dictionary_total += (STORED_JSON_HEADER_LEN + frame.len()) as u64;
            }
        }
        dictionary_total += dictionary.len() as u64;
    }

    println!(
        "BYTES\traw={raw_bytes}\ttoday={today_bytes}\tzstd_capture_pct={:.2}\tdelta_chain={delta_chain_bytes}\tdelta_chain_saving_pct={:.2}\tdelta_anchor={delta_anchor_bytes}\tdelta_anchor_saving_pct={:.2}\tshared_dictionary={dictionary_total}\tdictionary_saving_pct={:.2}",
        percent(raw_bytes.saturating_sub(today_bytes), raw_bytes),
        percent(today_bytes.saturating_sub(delta_chain_bytes), today_bytes),
        percent(today_bytes.saturating_sub(delta_anchor_bytes), today_bytes),
        percent(today_bytes.saturating_sub(dictionary_total), today_bytes),
    );
    // The cheap alternative: keep every payload independent and only raise the
    // compression level. No base, no chain, no GC edge, no read amplification.
    for (slot, level) in levels.iter().enumerate() {
        println!(
            "LEVEL\tzstd_level={level}\tbytes={}\tsaving_vs_today_pct={:.2}",
            level_bytes[slot],
            percent(today_bytes.saturating_sub(level_bytes[slot]), today_bytes),
        );
    }
    for (slot, depth) in depths.iter().enumerate() {
        let label = if *depth == usize::MAX {
            "unbounded".to_owned()
        } else {
            depth.to_string()
        };
        println!(
            "DEPTH\tsnapshot_every={label}\tbytes={}\tsaving_vs_today_pct={:.2}",
            bounded_bytes[slot],
            percent(today_bytes.saturating_sub(bounded_bytes[slot]), today_bytes),
        );
    }
    println!(
        "BYTES_SUCCESSORS\ttoday={successor_today_bytes}\tdelta_chain={successor_delta_bytes}\tdelta_anchor={successor_anchor_bytes}\tchain_saving_pct={:.2}\tanchor_saving_pct={:.2}",
        percent(
            successor_today_bytes.saturating_sub(successor_delta_bytes),
            successor_today_bytes
        ),
        percent(
            successor_today_bytes.saturating_sub(successor_anchor_bytes),
            successor_today_bytes
        ),
    );

    encode_cost(&order, &by_entity, &levels);
    reconstruction_latency(&order, &by_entity);
}

/// Write-path CPU of each encoding arm, per payload.
///
/// A delta encoder must build a fresh zstd dictionary context per row (the
/// base differs every time), which is where its write cost comes from; a
/// higher level pays in the compressor's own search effort instead.
fn encode_cost(order: &[String], by_entity: &HashMap<String, Vec<Vec<u8>>>, levels: &[i32]) {
    let samples = order
        .iter()
        .flat_map(|entity| {
            let versions = &by_entity[entity];
            versions
                .iter()
                .enumerate()
                .skip(1)
                .map(move |(index, raw)| (raw, &versions[index - 1]))
        })
        .take(env_usize("LIX_EXPU_ENCODE_SAMPLES", 2_000))
        .collect::<Vec<_>>();
    if samples.is_empty() {
        return;
    }
    // Interleave the arms across reps. Running each arm once back to back makes
    // whichever went first pay every cold-cache and page-fault cost, which is
    // how "level 3 is cheaper than level 1" appears out of nowhere.
    let reps = env_usize("LIX_EXPU_ENCODE_REPS", 9);
    let mut timings = vec![Vec::with_capacity(reps); levels.len() + 1];
    let mut sinks = vec![0_usize; levels.len() + 1];
    for _ in 0..reps {
        for (slot, level) in levels.iter().enumerate() {
            let start = std::time::Instant::now();
            let mut sink = 0_usize;
            for (raw, _) in &samples {
                sink += zstd_compress(*level, raw).len();
            }
            timings[slot].push(start.elapsed().as_nanos() as f64 / samples.len() as f64);
            sinks[slot] = sink;
        }
        let start = std::time::Instant::now();
        let mut sink = 0_usize;
        for (raw, base) in &samples {
            let mut compressor = zstd::bulk::Compressor::with_dictionary(ZSTD_LEVEL, base)
                .expect("dictionary compressor");
            sink += compressor.compress(raw).expect("delta compress").len();
        }
        timings[levels.len()].push(start.elapsed().as_nanos() as f64 / samples.len() as f64);
        sinks[levels.len()] = sink;
    }
    for (slot, timing) in timings.iter_mut().enumerate() {
        timing.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
        let arm = if slot == levels.len() {
            "delta_level1".to_owned()
        } else {
            format!("level{}", levels[slot])
        };
        println!(
            "ENCODE\tarm={arm}\treps={reps}\tns_median={:.0}\tns_p95={:.0}\tframe_bytes={}\tframe_bytes_per_payload={}",
            percentile(timing, 0.5),
            percentile(timing, 0.95),
            sinks[slot],
            sinks[slot] / samples.len(),
        );
    }
}

/// Read cost of reconstructing one payload at chain depth `d`.
///
/// A delta row cannot be decoded without its base, so a depth-`d` payload
/// costs `d` sequential point reads plus `d` zstd frames. Storage latency is
/// not modelled here; this isolates the CPU term, which is the floor.
fn reconstruction_latency(order: &[String], by_entity: &HashMap<String, Vec<Vec<u8>>>) {
    let Some(entity) = order.iter().find(|entity| by_entity[*entity].len() >= 9) else {
        println!("RECONSTRUCT\tskipped=no_chain_of_depth_9");
        return;
    };
    let versions = &by_entity[entity];
    let max_depth = versions.len().min(env_usize("LIX_EXPU_MAX_DEPTH", 16));

    // Encode the chain the way a delta store would hold it.
    let base_full = zstd_compress(ZSTD_LEVEL, &versions[0]);
    let mut frames = Vec::new();
    for index in 1..max_depth {
        let mut compressor =
            zstd::bulk::Compressor::with_dictionary(ZSTD_LEVEL, &versions[index - 1])
                .expect("dictionary compressor");
        frames.push(
            compressor
                .compress(&versions[index])
                .expect("delta compress"),
        );
    }

    let iterations = env_usize("LIX_EXPU_RECONSTRUCT_ITERS", 2_000);
    let mut rows = BTreeMap::new();
    for depth in 0..max_depth {
        let start = std::time::Instant::now();
        let mut sink = 0_usize;
        for _ in 0..iterations {
            let mut current =
                zstd::bulk::decompress(&base_full, versions[0].len()).expect("decompress base");
            for frame in frames.iter().take(depth) {
                let mut decompressor = zstd::bulk::Decompressor::with_dictionary(&current)
                    .expect("dictionary decompressor");
                current = decompressor
                    .decompress(frame, versions[0].len() * 2)
                    .expect("decompress delta");
            }
            sink += current.len();
        }
        let elapsed = start.elapsed().as_nanos() as f64 / iterations as f64;
        rows.insert(depth, (elapsed, sink));
    }
    for (depth, (nanos, _)) in rows {
        println!("RECONSTRUCT\tdepth={depth}\tns_per_payload={nanos:.0}");
    }
}

// ---------------------------------------------------------------------------
// Settled-store report
// ---------------------------------------------------------------------------

async fn store_report(dir: &Path) {
    let storage = StorageAdapter::new(SlateDB::open(dir).expect("open SlateDB for report"));
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open report snapshot");

    let inventory = space_inventory(&read, JSON_SPACE.name).await;
    let mut raw_bytes = 0_u64;
    let mut stored_bytes = 0_u64;
    let mut zstd_rows = 0_u64;
    let mut raw_rows = 0_u64;
    let mut raw_payloads = Vec::with_capacity(inventory.len());
    for (_, value) in &inventory {
        stored_bytes += value.len() as u64;
        if value.len() > 11 && value[11] == 1 {
            zstd_rows += 1;
        } else {
            raw_rows += 1;
        }
        let json = decode_stored_json(value);
        raw_bytes += json.len() as u64;
        raw_payloads.push(json);
    }

    println!(
        "JSON_STORE\trows={}\tstored_bytes={stored_bytes}\traw_bytes={raw_bytes}\tzstd_rows={zstd_rows}\traw_rows={raw_rows}\tzstd_capture_pct={:.2}",
        inventory.len(),
        percent(raw_bytes.saturating_sub(stored_bytes), raw_bytes),
    );

    // The same near-duplicate probe the auditor runs, but on the *logical*
    // JSON and with widths expressed as a fraction of payload length. A fixed
    // 64-byte window against a 181-byte compressed row is a third of the row;
    // against a 4.6 KiB document it is 1.4%. Only the second is a delta case.
    for fraction in [0.01_f64, 0.05, 0.25] {
        let mut prefix_buckets: HashMap<[u8; 32], HashSet<[u8; 32]>> = HashMap::new();
        let mut suffix_buckets: HashMap<[u8; 32], HashSet<[u8; 32]>> = HashMap::new();
        for json in &raw_payloads {
            let width = ((json.len() as f64) * fraction) as usize;
            if width == 0 || json.len() <= width {
                continue;
            }
            let digest = *blake3::hash(json).as_bytes();
            prefix_buckets
                .entry(*blake3::hash(&json[width..]).as_bytes())
                .or_default()
                .insert(digest);
            suffix_buckets
                .entry(*blake3::hash(&json[..json.len() - width]).as_bytes())
                .or_default()
                .insert(digest);
        }
        let near = |buckets: &HashMap<[u8; 32], HashSet<[u8; 32]>>| -> u64 {
            buckets
                .values()
                .map(|digests| digests.len().saturating_sub(1) as u64)
                .sum()
        };
        println!(
            "NEAR_RELATIVE\tfraction={fraction:.2}\tprefix={}\tsuffix={}\tdistinct={}",
            near(&prefix_buckets),
            near(&suffix_buckets),
            raw_payloads.len(),
        );
    }

    let total_dir = directory_bytes(dir);
    println!("PHYSICAL\tdirectory_bytes={total_dir}");
}

/// What a delta chain costs the OLTP read path, measured on the real store.
///
/// A delta row cannot name its base until it has been read, so resolving a
/// depth-`d` payload is a **pointer chase**: `d + 1` sequential batched point
/// reads, each waiting on the previous. The alternative — storing the whole
/// base list in every delta row — collapses it to one wider batched read. Both
/// are measured; neither is free, and the JSON space is keyed by digest, so
/// every base lands in a different block from its dependant.
async fn read_cost(dir: &Path) {
    let storage = StorageAdapter::new(SlateDB::open(dir).expect("open SlateDB for read cost"));
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open read-cost snapshot");
    let space = storage_space_by_name(JSON_SPACE.name);
    let keys = space_inventory(&read, JSON_SPACE.name)
        .await
        .into_iter()
        .map(|(key, _)| StorageKey(bytes::Bytes::from(key)))
        .collect::<Vec<_>>();
    assert!(!keys.is_empty(), "json_store.json is empty");

    let batch = env_usize("LIX_EXPU_READ_BATCH", 1_000).min(keys.len());
    let reps = env_usize("LIX_EXPU_READ_REPS", 9);
    let max_depth = env_usize("LIX_EXPU_MAX_DEPTH", 8);
    // Deterministic strided sampling: a digest-keyed space has no locality to
    // preserve, so a stride simply avoids re-reading one hot block.
    let pick = |round: usize, rep: usize| -> Vec<StorageKey> {
        let stride = 7919;
        (0..batch)
            .map(|index| {
                keys[(index * stride + round * 104_729 + rep * 15_485_863) % keys.len()].clone()
            })
            .collect()
    };

    // Warm the block cache over the whole key space first. Without it the arm
    // that runs second inherits the other arm's cache and the comparison is an
    // artefact of ordering rather than of chain depth.
    for chunk in keys.chunks(4_096) {
        PointReadPlan::new(space, chunk)
            .materialize(&read, StorageGetOptions::default())
            .await
            .expect("warm point read");
    }

    for depth in 0..=max_depth {
        let mut chased = Vec::with_capacity(reps);
        let mut widened = Vec::with_capacity(reps);
        for rep in 0..reps {
            let rounds = (0..=depth)
                .map(|round| pick(round, rep))
                .collect::<Vec<_>>();
            let wide = rounds.concat();
            // Alternate which arm goes first so neither one systematically
            // warms the other.
            if rep % 2 == 1 {
                widened.push(time_wide_read(&read, space, &wide, batch).await);
                chased.push(time_chased_read(&read, space, &rounds, batch).await);
            } else {
                chased.push(time_chased_read(&read, space, &rounds, batch).await);
                widened.push(time_wide_read(&read, space, &wide, batch).await);
            }
        }
        chased.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
        widened.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
        println!(
            "READCOST\tdepth={depth}\treps={reps}\tbatch={batch}\tchased_ns_median={:.0}\tchased_ns_p95={:.0}\twide_ns_median={:.0}\twide_ns_p95={:.0}",
            percentile(&chased, 0.5),
            percentile(&chased, 0.95),
            percentile(&widened, 0.5),
            percentile(&widened, 0.95),
        );
    }
}

/// `d + 1` dependent batched point reads: the pointer chase a delta row forces
/// because it cannot name its base before it has itself been read.
async fn time_chased_read<R>(
    read: &R,
    space: lix::storage_adapter::StorageSpace,
    rounds: &[Vec<StorageKey>],
    batch: usize,
) -> f64
where
    R: lix::storage_adapter::StorageAdapterRead,
{
    let start = std::time::Instant::now();
    let mut found = 0_usize;
    for round in rounds {
        let result = PointReadPlan::new(space, round)
            .materialize(read, StorageGetOptions::default())
            .await
            .expect("point read");
        found += result.value.iter().filter(|value| value.is_some()).count();
    }
    let elapsed = start.elapsed().as_nanos() as f64 / batch as f64;
    assert!(found >= batch, "point reads should hit");
    elapsed
}

/// One batched read of every base at once: the best case, available only if a
/// delta row carries its whole base list instead of a single parent link.
async fn time_wide_read<R>(
    read: &R,
    space: lix::storage_adapter::StorageSpace,
    wide: &[StorageKey],
    batch: usize,
) -> f64
where
    R: lix::storage_adapter::StorageAdapterRead,
{
    let start = std::time::Instant::now();
    let result = PointReadPlan::new(space, wide)
        .materialize(read, StorageGetOptions::default())
        .await
        .expect("wide point read");
    let elapsed = start.elapsed().as_nanos() as f64 / batch as f64;
    assert!(!result.value.is_empty());
    elapsed
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
