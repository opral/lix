#![recursion_limit = "256"]

//! Content-addressed storage audit: does lix store the same bytes twice?
//!
//! `storage_layout` answers "how many rows and bytes does each space hold".
//! This tool answers the next question: **how many of those bytes are copies
//! of bytes the repository already stored**, and does every content-addressed
//! space actually honour `key == digest(value)`.
//!
//! Usage:
//! ```text
//! duplicate_audit build <corpus> <dir>   # seed a corpus through the real commit path
//! duplicate_audit audit <dir>            # audit a settled SlateDB directory
//! duplicate_audit run   <corpus> <dir>   # build, settle, then audit
//! ```
//! Corpora: `plain`, `bigjson`, `edits`, `branches`, `media`, `gc_pre`, `gc`.
//!
//! Every corpus is seeded through `SessionContext` SQL so the audit observes
//! exactly what an ordinary commit stages. Nothing here reaches into a
//! seeding shortcut that bypasses the commit path.

#![allow(clippy::large_futures)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

use lix::storage::Storage;
use lix::storage_adapter::{StorageAdapter, StorageReadOptions};
use lix::storage_bench::{
    ContentAddressRule, collect_repository_gc_for_bench, content_address_rule,
    decode_binary_cas_chunk_reference, layout_space_catalog, recompute_content_address,
    space_inventory,
};
use lix::{CreateBranchOptions, Value};
use lix::{Lix, open_lix};
use lix_storage_slatedb::SlateDB;

/// Prefix/suffix widths probed for near-duplicates. A pair of rows that agree
/// on everything but `width` leading or trailing bytes is a chunking or
/// delta-encoding miss, not two genuinely different payloads.
const NEAR_DUPLICATE_WIDTHS: [usize; 4] = [8, 16, 32, 64];

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let usage = "usage: duplicate_audit (build <corpus> <dir> | audit <dir> | run <corpus> <dir>)";
    let mode = args.get(1).map(String::as_str).expect(usage);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build duplicate-audit runtime");
    match mode {
        "audit" => {
            let dir = PathBuf::from(args.get(2).expect(usage));
            runtime.block_on(audit(&dir));
        }
        "build" | "run" => {
            let corpus = args.get(2).map(String::as_str).expect(usage);
            let dir = PathBuf::from(args.get(3).expect(usage));
            runtime.block_on(build(corpus, &dir));
            if mode == "run" {
                runtime.block_on(audit(&dir));
            }
        }
        other => panic!("unknown mode '{other}'; {usage}"),
    }
}

// ---------------------------------------------------------------------------
// Corpus construction — every write goes through the real SQL commit path.
// ---------------------------------------------------------------------------

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

async fn build(corpus: &str, dir: &Path) {
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
    register_schema(&session).await;

    let rows = env_usize("LIX_AUDIT_ROWS", 10_000);
    let commit_rows = env_usize("LIX_AUDIT_COMMIT_ROWS", 500);
    // `bigjson` pushes every payload past the inline threshold and repeats
    // only ten distinct bodies, so the content-addressed JSON store carries
    // the workload instead of the inline row encoding.
    let shape = if corpus == "bigjson" {
        RowShape {
            pad: env_usize("LIX_AUDIT_JSON_PAD", 100),
            bodies: env_usize("LIX_AUDIT_DISTINCT_BODIES", 10),
        }
    } else {
        RowShape {
            pad: env_usize("LIX_AUDIT_JSON_PAD", 1),
            bodies: env_usize("LIX_AUDIT_DISTINCT_BODIES", usize::MAX),
        }
    };
    match corpus {
        "plain" | "bigjson" => {
            seed_rows(&session, shape, rows, commit_rows).await;
        }
        "edits" => {
            seed_rows(&session, shape, rows, commit_rows).await;
            edit_rows(
                &session,
                shape,
                env_usize("LIX_AUDIT_EDIT_ROUNDS", 20),
                100,
                rows,
            )
            .await;
        }
        "branches" => {
            let base = rows / 5;
            seed_rows(&session, shape, base, commit_rows).await;
            for branch_index in 0..env_usize("LIX_AUDIT_BRANCHES", 4) {
                let branch = session
                    .create_branch(CreateBranchOptions {
                        id: Some(format!("01990000-0000-7000-8000-{:012}", branch_index + 1)),
                        name: format!("audit-branch-{branch_index}"),
                        from_commit_id: None,
                    })
                    .await
                    .expect("create audit branch");
                let branch_session = lix
                    .open_another_session()
                    .await
                    .expect("open audit branch session");
                branch_session
                    .switch_branch(lix::SwitchBranchOptions {
                        branch_id: (branch.id.clone()).to_string(),
                    })
                    .await
                    .expect("switch session branch");
                edit_rows(&branch_session, shape, 3, 100, base).await;
            }
        }
        "media" => {
            seed_rows(&session, shape, rows / 10, commit_rows).await;
            seed_media(&session, env_usize("LIX_AUDIT_FILES", 40)).await;
            edit_media(&session, env_usize("LIX_AUDIT_FILES", 40), 3).await;
        }
        // `gc_pre` and `gc` are a matched pair: identical construction, the
        // second one additionally releases the disposable branch and sweeps.
        // Diffing their audits is what proves superseded content is reclaimed
        // rather than merely unreferenced.
        "gc_pre" | "gc" => {
            seed_rows(&session, shape, rows / 2, commit_rows).await;
            let branch = session
                .create_branch(CreateBranchOptions {
                    id: Some("01990000-0000-7000-8000-0000000000ff".to_owned()),
                    name: "audit-gc-disposable".to_owned(),
                    from_commit_id: None,
                })
                .await
                .expect("create GC-disposable branch");
            let branch_session = lix
                .open_another_session()
                .await
                .expect("open GC-disposable branch");
            branch_session
                .switch_branch(lix::SwitchBranchOptions {
                    branch_id: (branch.id.clone()).to_string(),
                })
                .await
                .expect("switch session branch");
            edit_rows(&branch_session, shape, 10, 200, rows / 2).await;
            seed_media(&branch_session, env_usize("LIX_AUDIT_FILES", 20)).await;
            edit_media(&branch_session, env_usize("LIX_AUDIT_FILES", 20), 3).await;
            drop(branch_session);
            if corpus == "gc" {
                session
                    .execute(
                        "DELETE FROM lix_branch WHERE id = $1",
                        &[Value::Text(branch.id)],
                    )
                    .await
                    .expect("delete GC-disposable branch");
                let adapter = StorageAdapter::new(storage.clone());
                let sweep = collect_repository_gc_for_bench(&adapter)
                    .await
                    .expect("collect repository GC");
                println!("GC\tsweep={sweep:?}");
            }
        }
        other => panic!("unknown corpus '{other}'"),
    }
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

async fn register_schema<S>(session: &Lix<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "audit_row",
        "columns": [
            { "name": "path", "type": "text", "nullable": false },
            { "name": "value", "type": "jsonb", "nullable": false },
        ],
        "primary_key": ["path"],
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (CAST($1 AS JSONB), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register audit schema");
}

/// Row payload shape.
///
/// `pad` repeats a filler sentence. Past `JSON_INLINE_MAX_BYTES` (1 KiB) the
/// payload stops being inlined into the row and reaches the content-addressed
/// JSON store, so `pad` selects which plane the audit exercises. `bodies` caps
/// how many *distinct* payload bodies exist: a real workspace repeats values,
/// and repetition is exactly what content addressing is supposed to collapse.
#[derive(Clone, Copy)]
struct RowShape {
    pad: usize,
    bodies: usize,
}

impl RowShape {
    fn json(self, index: usize, revision: usize) -> String {
        format!(
            r#"{{"revision":{revision},"index":{index},"status":"active","tags":["alpha","beta","gamma"],"body":{},"note":"{}"}}"#,
            index % self.bodies,
            "the quick brown fox jumps over the lazy dog. ".repeat(self.pad),
        )
    }
}

async fn seed_rows<S>(session: &Lix<S>, shape: RowShape, rows: usize, commit_rows: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut index = 0;
    while index < rows {
        let end = (index + commit_rows).min(rows);
        let mut transaction = session.begin_transaction().await.expect("begin seed");
        while index < end {
            transaction
                .execute(
                    "INSERT INTO audit_row (path, value) VALUES ($1, CAST($2 AS JSONB))",
                    &[
                        Value::Text(format!("/row/{index:08}")),
                        Value::Text(shape.json(index, 0)),
                    ],
                )
                .await
                .expect("stage seed row");
            index += 1;
        }
        transaction.commit().await.expect("commit seed batch");
    }
}

/// Rewrites a small window of rows repeatedly. If a rewrite re-stores
/// unchanged neighbouring content, the duplicate accounting will show it.
async fn edit_rows<S>(
    session: &Lix<S>,
    shape: RowShape,
    rounds: usize,
    window: usize,
    live_rows: usize,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let window = window.min(live_rows);
    for round in 1..=rounds {
        let mut transaction = session.begin_transaction().await.expect("begin edit");
        for offset in 0..window {
            let index = (round * 7 + offset) % live_rows;
            transaction
                .execute(
                    "UPDATE audit_row SET value = CAST($2 AS JSONB) WHERE path = $1",
                    &[
                        Value::Text(format!("/row/{index:08}")),
                        Value::Text(shape.json(index, round)),
                    ],
                )
                .await
                .expect("stage edit row");
        }
        transaction.commit().await.expect("commit edit batch");
    }
}

/// Deterministic pseudo-media: incompressible-ish bytes so chunk boundaries
/// are content-driven rather than run-length artefacts.
fn media_bytes(file: usize, revision: usize, len: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(len);
    let mut state = (file as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ revision as u64;
    while bytes.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        bytes.extend_from_slice(&state.to_le_bytes());
    }
    bytes.truncate(len);
    bytes
}

async fn seed_media<S>(session: &Lix<S>, files: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let len = env_usize("LIX_AUDIT_FILE_BYTES", 4 * 1024 * 1024);
    for file in 0..files {
        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                &[
                    Value::Text(format!("/media/{file:04}.bin")),
                    Value::Blob(media_bytes(file, 0, len).into()),
                ],
            )
            .await
            .expect("insert media file");
    }
}

/// Edits each media file in place by rewriting a 4 KiB window. A working
/// chunker should re-store only the chunks covering that window.
async fn edit_media<S>(session: &Lix<S>, files: usize, rounds: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let len = env_usize("LIX_AUDIT_FILE_BYTES", 4 * 1024 * 1024);
    for round in 1..=rounds {
        for file in 0..files {
            let mut bytes = media_bytes(file, 0, len);
            let start = (round * 37 * 1024) % (len - 4096);
            let patch = media_bytes(file, round, 4096);
            bytes[start..start + 4096].copy_from_slice(&patch);
            session
                .execute(
                    "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
                     ON CONFLICT (path) DO UPDATE SET content = excluded.content",
                    &[
                        Value::Text(format!("/media/{file:04}.bin")),
                        Value::Blob(bytes.into()),
                    ],
                )
                .await
                .expect("edit media file");
        }
    }
}

// ---------------------------------------------------------------------------
// Audit
// ---------------------------------------------------------------------------

#[derive(Default)]
struct SpaceAudit {
    rows: u64,
    key_bytes: u64,
    value_bytes: u64,
    empty_value_rows: u64,
    distinct_digests: u64,
    duplicate_rows: u64,
    duplicate_bytes: u64,
    verified_rows: u64,
    violations: Vec<String>,
    near_duplicate_rows: BTreeMap<(&'static str, usize), u64>,
}

async fn audit(dir: &Path) {
    let storage = StorageAdapter::new(SlateDB::open(dir).expect("open SlateDB for audit"));
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open audit snapshot");

    let catalog = layout_space_catalog();
    // digest -> (value length, spaces that hold it, total rows holding it)
    let mut global: HashMap<[u8; 32], (usize, Vec<u32>, u64)> = HashMap::new();
    let mut audits: Vec<(u32, &'static str, SpaceAudit)> = Vec::new();
    let mut manifest_chunk_refs: Vec<[u8; 32]> = Vec::new();
    let mut chunk_sizes: HashMap<[u8; 32], u64> = HashMap::new();

    for (space_id, space_name) in catalog {
        let inventory = space_inventory(&read, space_name).await;
        let mut audit = SpaceAudit::default();
        let mut digest_counts: HashMap<[u8; 32], (usize, u64)> = HashMap::new();
        let mut near: Vec<HashMap<[u8; 32], HashSet<[u8; 32]>>> =
            vec![HashMap::new(); NEAR_DUPLICATE_WIDTHS.len() * 2];

        for (key, value) in &inventory {
            audit.rows += 1;
            audit.key_bytes += key.len() as u64 + 4;
            audit.value_bytes += value.len() as u64;
            if value.is_empty() {
                audit.empty_value_rows += 1;
            } else {
                let digest = *blake3::hash(value).as_bytes();
                let entry = digest_counts.entry(digest).or_insert((value.len(), 0));
                entry.1 += 1;
                let global_entry = global
                    .entry(digest)
                    .or_insert_with(|| (value.len(), Vec::new(), 0));
                if !global_entry.1.contains(&space_id) {
                    global_entry.1.push(space_id);
                }
                global_entry.2 += 1;
                record_near_duplicates(&mut near, value, digest);
            }
            if let Some(expected) =
                recompute_content_address(space_id, value).unwrap_or_else(|error| {
                    panic!("{space_name}: content address decode failed: {error}")
                })
            {
                if key.as_slice() == expected.as_slice() {
                    audit.verified_rows += 1;
                } else if audit.violations.len() < 5 {
                    audit.violations.push(format!(
                        "key={} expected={}",
                        hex(key.get(..32).unwrap_or(key)),
                        hex(&expected),
                    ));
                } else {
                    audit.violations.push(String::new());
                }
            }
            if space_id == 0x0005_0002 {
                let (chunk, size) = decode_binary_cas_chunk_reference(value)
                    .expect("decode binary CAS manifest chunk reference");
                manifest_chunk_refs.push(chunk);
                chunk_sizes.insert(chunk, size);
            }
        }

        audit.distinct_digests = digest_counts.len() as u64;
        for (length, count) in digest_counts.values() {
            if *count > 1 {
                audit.duplicate_rows += count - 1;
                audit.duplicate_bytes += (count - 1) * *length as u64;
            }
        }
        for (index, width) in NEAR_DUPLICATE_WIDTHS.iter().enumerate() {
            audit
                .near_duplicate_rows
                .insert(("prefix", *width), near_duplicate_count(&near[index * 2]));
            audit.near_duplicate_rows.insert(
                ("suffix", *width),
                near_duplicate_count(&near[index * 2 + 1]),
            );
        }
        audits.push((space_id, space_name, audit));
    }

    print_space_table(&audits);
    print_near_duplicates(&audits);
    print_content_address_verdict(&audits);
    print_cross_space(&global, &audits);
    print_binary_cas_reuse(&manifest_chunk_refs, &chunk_sizes);
    print_headline(&audits, &global, directory_bytes(dir));
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

/// Near-duplicate index. Two rows land in the same bucket when they agree on
/// everything outside a `width`-byte prefix (or suffix); the stored value is
/// the full digest of the first row seen, so a *different* full digest in the
/// same bucket is a genuine near-duplicate pair.
fn record_near_duplicates(
    near: &mut [HashMap<[u8; 32], HashSet<[u8; 32]>>],
    value: &[u8],
    digest: [u8; 32],
) {
    for (index, width) in NEAR_DUPLICATE_WIDTHS.iter().enumerate() {
        if value.len() <= *width {
            continue;
        }
        let prefix_stripped = *blake3::hash(&value[*width..]).as_bytes();
        near[index * 2]
            .entry(prefix_stripped)
            .or_default()
            .insert(digest);
        let suffix_stripped = *blake3::hash(&value[..value.len() - *width]).as_bytes();
        near[index * 2 + 1]
            .entry(suffix_stripped)
            .or_default()
            .insert(digest);
    }
}

/// Distinct values that are a near-duplicate of an earlier distinct value in
/// the same bucket. A bucket with `n` distinct full digests contributes
/// `n - 1` avoidable payloads.
fn near_duplicate_count(bucket: &HashMap<[u8; 32], HashSet<[u8; 32]>>) -> u64 {
    bucket
        .values()
        .map(|digests| digests.len().saturating_sub(1) as u64)
        .sum()
}

fn print_near_duplicates(audits: &[(u32, &'static str, SpaceAudit)]) {
    for (_, space_name, audit) in audits {
        let total = audit
            .near_duplicate_rows
            .values()
            .copied()
            .max()
            .unwrap_or_default();
        if total == 0 {
            continue;
        }
        let detail = audit
            .near_duplicate_rows
            .iter()
            .map(|((side, width), count)| format!("{side}{width}={count}"))
            .collect::<Vec<_>>()
            .join("\t");
        println!(
            "NEAR_DUPLICATE\tspace={space_name}\tdistinct_values={}\t{detail}",
            audit.distinct_digests
        );
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn print_space_table(audits: &[(u32, &'static str, SpaceAudit)]) {
    println!(
        "SPACE\tspace_id\tspace\trows\tkey_bytes\tvalue_bytes\tempty_value_rows\tdistinct_digests\tduplicate_rows\tduplicate_bytes\twaste_pct"
    );
    let mut sorted = audits.iter().collect::<Vec<_>>();
    sorted.sort_unstable_by_key(|(_, _, audit)| std::cmp::Reverse(audit.duplicate_bytes));
    for (space_id, space_name, audit) in sorted {
        if audit.rows == 0 {
            continue;
        }
        println!(
            "SPACE\t{space_id}\t{space_name}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.2}",
            audit.rows,
            audit.key_bytes,
            audit.value_bytes,
            audit.empty_value_rows,
            audit.distinct_digests,
            audit.duplicate_rows,
            audit.duplicate_bytes,
            percent(audit.duplicate_bytes, audit.value_bytes),
        );
    }
}

fn print_content_address_verdict(audits: &[(u32, &'static str, SpaceAudit)]) {
    for (space_id, space_name, audit) in audits {
        let rule = content_address_rule(*space_id);
        if rule == ContentAddressRule::NotContentAddressed || audit.rows == 0 {
            continue;
        }
        let violations = audit.violations.len();
        println!(
            "CONTENT_ADDRESS\tspace={space_name}\trule={rule:?}\trows={}\tverified={}\tviolations={violations}",
            audit.rows, audit.verified_rows,
        );
        for violation in audit.violations.iter().filter(|entry| !entry.is_empty()) {
            println!("CONTENT_ADDRESS_VIOLATION\tspace={space_name}\t{violation}");
        }
    }
}

fn print_cross_space(
    global: &HashMap<[u8; 32], (usize, Vec<u32>, u64)>,
    audits: &[(u32, &'static str, SpaceAudit)],
) {
    let names = audits
        .iter()
        .map(|(space_id, space_name, _)| (*space_id, *space_name))
        .collect::<BTreeMap<_, _>>();
    let mut pairs: BTreeMap<Vec<u32>, (u64, u64)> = BTreeMap::new();
    let mut cross_values = 0_u64;
    let mut cross_bytes = 0_u64;
    for (length, spaces, rows) in global.values() {
        if spaces.len() < 2 {
            continue;
        }
        let mut spaces = spaces.clone();
        spaces.sort_unstable();
        cross_values += 1;
        // One copy is legitimate; every further space holding the same bytes
        // is a redundant copy.
        cross_bytes += (spaces.len() as u64 - 1) * *length as u64;
        let entry = pairs.entry(spaces).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += *length as u64 * (*rows);
    }
    println!("CROSS_SPACE_TOTAL\tdistinct_values={cross_values}\tredundant_bytes={cross_bytes}");
    for (spaces, (values, bytes)) in pairs {
        let labels = spaces
            .iter()
            .map(|space_id| names.get(space_id).copied().unwrap_or("<unknown>"))
            .collect::<Vec<_>>()
            .join("+");
        println!("CROSS_SPACE\tspaces={labels}\tdistinct_values={values}\ttotal_bytes={bytes}");
    }
}

fn print_binary_cas_reuse(refs: &[[u8; 32]], sizes: &HashMap<[u8; 32], u64>) {
    if refs.is_empty() {
        println!("BINARY_CAS_REUSE\treferences=0");
        return;
    }
    let mut counts: HashMap<[u8; 32], u64> = HashMap::new();
    for chunk in refs {
        *counts.entry(*chunk).or_default() += 1;
    }
    let distinct = counts.len() as u64;
    let single = counts.values().filter(|count| **count == 1).count() as u64;
    let shared_refs = counts
        .values()
        .filter(|count| **count > 1)
        .copied()
        .sum::<u64>();
    let logical_bytes = refs
        .iter()
        .map(|chunk| sizes.get(chunk).copied().unwrap_or_default())
        .sum::<u64>();
    let stored_bytes = counts
        .keys()
        .map(|chunk| sizes.get(chunk).copied().unwrap_or_default())
        .sum::<u64>();
    println!(
        "BINARY_CAS_REUSE\treferences={}\tdistinct_chunks={distinct}\tchunks_referenced_once={single}\treferences_to_shared_chunks={shared_refs}\tlogical_bytes={logical_bytes}\tstored_chunk_bytes={stored_bytes}\tdedup_saving_pct={:.2}",
        refs.len(),
        percent(logical_bytes.saturating_sub(stored_bytes), logical_bytes),
    );
}

fn print_headline(
    audits: &[(u32, &'static str, SpaceAudit)],
    global: &HashMap<[u8; 32], (usize, Vec<u32>, u64)>,
    physical_bytes: u64,
) {
    let rows = audits.iter().map(|(_, _, audit)| audit.rows).sum::<u64>();
    let key_bytes = audits
        .iter()
        .map(|(_, _, audit)| audit.key_bytes)
        .sum::<u64>();
    let value_bytes = audits
        .iter()
        .map(|(_, _, audit)| audit.value_bytes)
        .sum::<u64>();
    let within = audits
        .iter()
        .map(|(_, _, audit)| audit.duplicate_bytes)
        .sum::<u64>();
    let cross = global
        .values()
        .filter(|(_, spaces, _)| spaces.len() > 1)
        .map(|(length, spaces, _)| (spaces.len() as u64 - 1) * *length as u64)
        .sum::<u64>();
    println!(
        "TOTAL\trows={rows}\tkey_bytes={key_bytes}\tvalue_bytes={value_bytes}\tphysical_bytes={physical_bytes}\twithin_space_duplicate_bytes={within}\tcross_space_redundant_bytes={cross}\tduplicate_pct={:.3}",
        percent(within + cross, value_bytes),
    );
}

fn percent(part: u64, whole: u64) -> f64 {
    if whole == 0 {
        return 0.0;
    }
    100.0 * part as f64 / whole as f64
}
