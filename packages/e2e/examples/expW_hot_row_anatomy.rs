//! Anatomy of a `hot_state.row.v21` row, and what two branches share.
//!
//! Experiment T attributed 95.4% of the two-branch storage delta to
//! `hot_state.row.v21` and showed that two branches making *identical*
//! edits pay the same as two branches making *disjoint* edits. This tool
//! answers the follow-up questions directly on the bytes:
//!
//! 1. Where do the ~390 bytes of a branch-local HOT row go — key vs value, and
//!    inside the value, payload vs per-row identity vs working-diff baseline?
//! 2. Are the HOT values for byte-identical content byte-identical apart from
//!    the branch-scoped key? Reported byte-exact and under two normalizations.
//! 3. What does the base (unedited) row cost in its packed plane, so the
//!    amplification factor is measured rather than assumed?
//!
//! Usage:
//! ```text
//! expW_hot_row_anatomy <rows> <scenario> <dir>
//! ```
//! Scenarios: `identical`, `disjoint`, `single`, `base`.
//!
//! Every write goes through the real `SessionContext` SQL commit path, so the
//! inventory observes exactly what an ordinary commit stages.

#![allow(clippy::large_futures)]

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use lix::registered_spaces::HOT_ROW_SPACE;
use lix::storage::ReadOptions;
use lix::storage_adapter::StorageAdapter;
use lix::storage_bench::{layout_accounting, space_inventory};
use lix::{CreateBranchOptions, Value};
use lix::{Lix, open_lix};
use lix_storage_slatedb::SlateDB;

const SEED_BATCH_ROWS: usize = 5_000;
const PAD_UNIT: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// Payload padding. The default 64 bytes reproduces `branch_storage_sharing`
/// exactly. Raising it past `json_store`'s 1024-byte inline threshold moves
/// the snapshot slot from `INLINE_FINGERPRINTED` to `REF`, which is the
/// already-shipped content-addressed sharing path.
fn pad() -> String {
    let bytes: usize = std::env::var("LIX_EXPW_PAD_BYTES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(64);
    PAD_UNIT.repeat(bytes.div_ceil(PAD_UNIT.len()))[..bytes].to_owned()
}

// Mirrors `hot_state::tracked_head` value codec v8. Asserted against the
// version byte of every scanned row, so a codec change fails loudly here.
const HEAD_VALUE_VERSION: u8 = 8;
const HEAD_VALUE_HEADER_BYTES: usize = 59;
const HEAD_VALUE_SNAPSHOT_SHIFT: u8 = 1;
const HEAD_VALUE_METADATA_SHIFT: u8 = 3;
const HEAD_VALUE_WORKING_DIFF_SHIFT: u8 = 6;
const HEAD_VALUE_SLOT_MASK: u8 = 0b11;
const HEAD_VALUE_WORKING_DIFF_MASK: u8 = 0b11;
const HEAD_SLOT_NONE: u8 = 0;
const HEAD_SLOT_REF: u8 = 1;
const HEAD_SLOT_INLINE: u8 = 2;
const HEAD_SLOT_INLINE_FINGERPRINTED: u8 = 3;
const HEAD_WORKING_DIFF_DISABLED: u8 = 0;
const HEAD_WORKING_DIFF_CLEAN: u8 = 1;
const HEAD_WORKING_DIFF_BEFORE_ABSENT: u8 = 2;
const HEAD_WORKING_DIFF_BEFORE_PRESENT: u8 = 3;
const JSON_REF_BYTES: usize = 32;
const WORKING_DIFF_CHECKPOINT_BYTES: usize = 16;
const WORKING_DIFF_VERSION_BYTES: usize = 16 + 16 + 1 + 8 + 8 + 1 + 32 + 1 + 32;
const COLUMNAR_BASE_COORDINATE_BYTES: usize = 16 + 4 + 4;
const GENERATION_BYTES: usize = 16;

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let usage = "usage: expW_hot_row_anatomy <rows> <identical|disjoint|single|base> <dir>";
    let rows: usize = args.get(1).expect(usage).parse().expect(usage);
    let scenario = args.get(2).map(String::as_str).expect(usage).to_owned();
    let dir = PathBuf::from(args.get(3).expect(usage));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build anatomy runtime");
    runtime.block_on(run(rows, &scenario, &dir));
}

async fn run(rows: usize, scenario: &str, dir: &Path) {
    std::fs::create_dir_all(dir).expect("create anatomy directory");
    let storage = SlateDB::open(dir).expect("open anatomy SlateDB");
    open_lix()
        .with_storage(storage.clone())
        .await
        .expect("initialize anatomy repository");
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open anatomy lix");
    let main = lix
        .open_another_session()
        .await
        .expect("open anatomy session");
    register_schema(&main).await;
    seed_rows(&main, rows).await;

    settle(&storage).await;
    let base_spaces = space_totals(&storage).await;
    let base_hot: HashMap<Vec<u8>, Vec<u8>> = hot_inventory(&storage).await.into_iter().collect();
    println!(
        "expW_base,rows={rows},scenario={scenario},base_hot_rows={},base_logical_bytes={}",
        base_hot.len(),
        base_spaces.values().map(|c| c.0 + c.1).sum::<u64>(),
    );
    for (space, counts) in &base_spaces {
        if counts.2 == 0 {
            continue;
        }
        println!(
            "expW_base_space,rows={rows},space={space},rows_in_space={},key_bytes={},value_bytes={},bytes_per_row={:.2}",
            counts.2,
            counts.0,
            counts.1,
            (counts.0 + counts.1) as f64 / counts.2 as f64
        );
    }

    let one_percent = (rows / 100).max(1);
    let branch_ids = match scenario {
        "base" => Vec::new(),
        "single" => {
            let branch = create_branch(&main, "branch-0").await;
            modify_rows(&lix, &branch, 0, one_percent).await;
            vec![branch]
        }
        "identical" => {
            let first = create_branch(&main, "branch-0").await;
            modify_rows(&lix, &first, 0, one_percent).await;
            let second = create_branch(&main, "branch-1").await;
            modify_rows(&lix, &second, 0, one_percent).await;
            vec![first, second]
        }
        "disjoint" => {
            let first = create_branch(&main, "branch-0").await;
            modify_rows(&lix, &first, 0, one_percent).await;
            let second = create_branch(&main, "branch-1").await;
            modify_rows(&lix, &second, one_percent, one_percent).await;
            vec![first, second]
        }
        other => panic!("unknown scenario '{other}'"),
    };

    settle(&storage).await;
    let after_spaces = space_totals(&storage).await;
    let after_hot = hot_inventory(&storage).await;

    let mut names: Vec<String> = base_spaces.keys().cloned().collect();
    for name in after_spaces.keys() {
        if !names.contains(name) {
            names.push(name.clone());
        }
    }
    names.sort();
    let changed_rows = branch_ids.len() * one_percent;
    for name in &names {
        let before = base_spaces.get(name).copied().unwrap_or_default();
        let now = after_spaces.get(name).copied().unwrap_or_default();
        let d_bytes = (now.0 + now.1) as i64 - (before.0 + before.1) as i64;
        let d_rows = now.2 as i64 - before.2 as i64;
        if d_bytes == 0 && d_rows == 0 {
            continue;
        }
        println!(
            "expW_space_delta,rows={rows},scenario={scenario},space={name},d_rows={d_rows},d_bytes={d_bytes},d_bytes_per_changed_row={:.2}",
            if changed_rows == 0 {
                0.0
            } else {
                d_bytes as f64 / changed_rows as f64
            }
        );
    }

    // Only the rows this scenario added to HOT.
    let new_rows: Vec<(Vec<u8>, Vec<u8>)> = after_hot
        .into_iter()
        .filter(|(key, _)| !base_hot.contains_key(key))
        .collect();
    analyze(rows, scenario, &new_rows, changed_rows);
}

#[derive(Clone, Copy, Default)]
struct Anatomy {
    rows: u64,
    key_total: u64,
    key_scope: u64,
    key_identity: u64,
    value_total: u64,
    v_fixed: u64,
    v_identity: u64,
    v_snapshot_fingerprint: u64,
    v_snapshot_payload: u64,
    v_metadata_fingerprint: u64,
    v_metadata_payload: u64,
    v_working_diff: u64,
    v_columnar: u64,
    wd_disabled: u64,
    wd_clean: u64,
    wd_before_absent: u64,
    wd_before_present: u64,
    slot_none: u64,
    slot_ref: u64,
    slot_inline: u64,
    slot_inline_fingerprinted: u64,
    meta_slot_none: u64,
    meta_slot_ref: u64,
    meta_slot_inline: u64,
    meta_slot_inline_fingerprinted: u64,
}

fn analyze(rows: usize, scenario: &str, new_rows: &[(Vec<u8>, Vec<u8>)], changed_rows: usize) {
    if new_rows.is_empty() {
        println!("expW_anatomy,rows={rows},scenario={scenario},new_hot_rows=0");
        return;
    }
    let mut a = Anatomy::default();
    // key with the (branch_id, generation) scope stripped -> logical identity
    let mut by_identity: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
    let mut exact_values: HashMap<blake3::Hash, u64> = HashMap::new();
    let mut ident_norm: HashMap<blake3::Hash, u64> = HashMap::new();
    let mut content_only: HashMap<blake3::Hash, u64> = HashMap::new();
    let mut ident_norm_bytes: HashMap<blake3::Hash, u64> = HashMap::new();
    let mut content_only_bytes: HashMap<blake3::Hash, u64> = HashMap::new();

    for (index, (key, value)) in new_rows.iter().enumerate() {
        a.rows += 1;
        // `+ 4` matches layout_accounting's per-row key accounting.
        a.key_total += key.len() as u64 + 4;
        let scope_len = scope_prefix_len(key);
        a.key_scope += scope_len as u64;
        a.key_identity += (key.len() - scope_len) as u64;
        by_identity
            .entry(key[scope_len..].to_vec())
            .or_default()
            .push(index);

        let parsed = parse_value(value);
        a.value_total += value.len() as u64;
        a.v_fixed += 11; // version + flags + 2 lengths + columnar tag
        a.v_identity += 48; // change_id + commit_id + created_at + updated_at
        a.v_snapshot_fingerprint += parsed.snapshot_fingerprint as u64;
        a.v_snapshot_payload += parsed.snapshot_payload as u64;
        a.v_metadata_fingerprint += parsed.metadata_fingerprint as u64;
        a.v_metadata_payload += parsed.metadata_payload as u64;
        a.v_working_diff += parsed.working_diff as u64;
        a.v_columnar += parsed.columnar as u64;
        match parsed.working_diff_tag {
            HEAD_WORKING_DIFF_DISABLED => a.wd_disabled += 1,
            HEAD_WORKING_DIFF_CLEAN => a.wd_clean += 1,
            HEAD_WORKING_DIFF_BEFORE_ABSENT => a.wd_before_absent += 1,
            HEAD_WORKING_DIFF_BEFORE_PRESENT => a.wd_before_present += 1,
            _ => {}
        }
        match parsed.snapshot_kind {
            HEAD_SLOT_NONE => a.slot_none += 1,
            HEAD_SLOT_REF => a.slot_ref += 1,
            HEAD_SLOT_INLINE => a.slot_inline += 1,
            HEAD_SLOT_INLINE_FINGERPRINTED => a.slot_inline_fingerprinted += 1,
            _ => {}
        }
        match parsed.metadata_kind {
            HEAD_SLOT_NONE => a.meta_slot_none += 1,
            HEAD_SLOT_REF => a.meta_slot_ref += 1,
            HEAD_SLOT_INLINE => a.meta_slot_inline += 1,
            HEAD_SLOT_INLINE_FINGERPRINTED => a.meta_slot_inline_fingerprinted += 1,
            _ => {}
        }

        *exact_values.entry(blake3::hash(value)).or_insert(0) += 1;
        let n1 = normalize_identity(value, &parsed);
        let h1 = blake3::hash(&n1);
        *ident_norm.entry(h1).or_insert(0) += 1;
        ident_norm_bytes.insert(h1, n1.len() as u64);
        let n2 = content_only_bytes_of(value, &parsed);
        let h2 = blake3::hash(&n2);
        *content_only.entry(h2).or_insert(0) += 1;
        content_only_bytes.insert(h2, n2.len() as u64);
    }

    let per_row = |v: u64| v as f64 / a.rows as f64;
    println!(
        "expW_anatomy,rows={rows},scenario={scenario},new_hot_rows={},changed_rows={changed_rows},\
total_bytes={},bytes_per_row={:.2},\
key_bytes={},key_per_row={:.2},key_scope_per_row={:.2},key_identity_per_row={:.2},\
value_bytes={},value_per_row={:.2},\
v_fixed_per_row={:.2},v_identity_per_row={:.2},\
v_snapshot_fingerprint_per_row={:.2},v_snapshot_payload_per_row={:.2},\
v_metadata_fingerprint_per_row={:.2},v_metadata_payload_per_row={:.2},\
v_working_diff_per_row={:.2},v_columnar_per_row={:.2}",
        a.rows,
        a.key_total + a.value_total,
        per_row(a.key_total + a.value_total),
        a.key_total,
        per_row(a.key_total),
        per_row(a.key_scope),
        per_row(a.key_identity),
        a.value_total,
        per_row(a.value_total),
        per_row(a.v_fixed),
        per_row(a.v_identity),
        per_row(a.v_snapshot_fingerprint),
        per_row(a.v_snapshot_payload),
        per_row(a.v_metadata_fingerprint),
        per_row(a.v_metadata_payload),
        per_row(a.v_working_diff),
        per_row(a.v_columnar),
    );
    println!(
        "expW_anatomy_tags,rows={rows},scenario={scenario},\
wd_disabled={},wd_clean={},wd_before_absent={},wd_before_present={},\
snapshot_none={},snapshot_ref={},snapshot_inline={},snapshot_inline_fingerprinted={},\
metadata_none={},metadata_ref={},metadata_inline={},metadata_inline_fingerprinted={}",
        a.wd_disabled,
        a.wd_clean,
        a.wd_before_absent,
        a.wd_before_present,
        a.slot_none,
        a.slot_ref,
        a.slot_inline,
        a.slot_inline_fingerprinted,
        a.meta_slot_none,
        a.meta_slot_ref,
        a.meta_slot_inline,
        a.meta_slot_inline_fingerprinted,
    );

    // Sharing oracles over the new HOT rows.
    let dup = |map: &HashMap<blake3::Hash, u64>| -> (u64, u64) {
        let mut classes_gt1 = 0u64;
        let mut dup_rows = 0u64;
        for count in map.values() {
            if *count > 1 {
                classes_gt1 += 1;
                dup_rows += count - 1;
            }
        }
        (classes_gt1, dup_rows)
    };
    let (exact_classes, exact_dup) = dup(&exact_values);
    let (n1_classes, n1_dup) = dup(&ident_norm);
    let (n2_classes, n2_dup) = dup(&content_only);
    let n1_saved: u64 = ident_norm
        .iter()
        .filter(|(_, c)| **c > 1)
        .map(|(h, c)| (c - 1) * ident_norm_bytes[h])
        .sum();
    let n2_saved: u64 = content_only
        .iter()
        .filter(|(_, c)| **c > 1)
        .map(|(h, c)| (c - 1) * content_only_bytes[h])
        .sum();
    println!(
        "expW_sharing,rows={rows},scenario={scenario},new_hot_rows={},\
exact_distinct={},exact_dup_classes={exact_classes},exact_dup_rows={exact_dup},\
identity_normalized_distinct={},identity_normalized_dup_classes={n1_classes},identity_normalized_dup_rows={n1_dup},identity_normalized_saved_bytes={n1_saved},\
content_only_distinct={},content_only_dup_classes={n2_classes},content_only_dup_rows={n2_dup},content_only_saved_bytes={n2_saved}",
        a.rows,
        exact_values.len(),
        ident_norm.len(),
        content_only.len(),
    );

    // Cost model for the candidate fix: replace the payload in every HOT value
    // with a 32-byte content digest and store each distinct payload once in a
    // content-addressed side space (32-byte digest key + 4 accounting bytes).
    // Keys stay branch-scoped either way. This is the *optimistic* model: it
    // charges nothing for the presence/refcount plane that manifest-driven GC
    // would need, and nothing for the extra read hop.
    let mut model_hot_value_bytes = 0u64;
    let mut model_payloads: HashMap<blake3::Hash, u64> = HashMap::new();
    for (_, value) in new_rows.iter() {
        let p = parse_value(value);
        let payload_len = p.snapshot_payload + p.metadata_payload;
        model_hot_value_bytes += (value.len()
            - p.snapshot_fingerprint
            - p.snapshot_payload
            - p.metadata_fingerprint
            - p.metadata_payload
            + JSON_REF_BYTES) as u64;
        let digest = blake3::hash(
            &value[p.snapshot_start + p.snapshot_fingerprint
                ..p.snapshot_start + p.snapshot_fingerprint + p.snapshot_payload],
        );
        model_payloads.insert(digest, payload_len as u64);
    }
    let model_cas_bytes: u64 = model_payloads
        .values()
        .map(|len| len + JSON_REF_BYTES as u64 + 4)
        .sum();
    let today = a.key_total + a.value_total;
    let modeled = a.key_total + model_hot_value_bytes + model_cas_bytes;
    println!(
        "expW_indirection_model,rows={rows},scenario={scenario},new_hot_rows={},\
today_bytes={today},modeled_bytes={modeled},\
modeled_hot_value_bytes={model_hot_value_bytes},modeled_cas_rows={},modeled_cas_bytes={model_cas_bytes},\
delta_pct={:.2}",
        a.rows,
        model_payloads.len(),
        (modeled as f64 - today as f64) / today as f64 * 100.0,
    );

    // Cross-branch pairing: same logical identity in >1 branch scope.
    let mut paired_identities = 0u64;
    let mut paired_exact_equal = 0u64;
    let mut paired_ident_norm_equal = 0u64;
    let mut paired_content_equal = 0u64;
    let mut first_diff_report: Option<String> = None;
    for indices in by_identity.values() {
        if indices.len() < 2 {
            continue;
        }
        paired_identities += 1;
        let (_, v0) = &new_rows[indices[0]];
        let p0 = parse_value(v0);
        let mut exact = true;
        let mut n1eq = true;
        let mut n2eq = true;
        for &other in &indices[1..] {
            let (_, v1) = &new_rows[other];
            let p1 = parse_value(v1);
            if v0 != v1 {
                exact = false;
            }
            if normalize_identity(v0, &p0) != normalize_identity(v1, &p1) {
                n1eq = false;
            }
            if content_only_bytes_of(v0, &p0) != content_only_bytes_of(v1, &p1) {
                n2eq = false;
            }
            if first_diff_report.is_none() && v0 != v1 {
                first_diff_report = Some(describe_diff(v0, v1));
            }
        }
        paired_exact_equal += u64::from(exact);
        paired_ident_norm_equal += u64::from(n1eq);
        paired_content_equal += u64::from(n2eq);
    }
    println!(
        "expW_crossbranch,rows={rows},scenario={scenario},\
paired_identities={paired_identities},paired_exact_equal={paired_exact_equal},\
paired_identity_normalized_equal={paired_ident_norm_equal},paired_content_equal={paired_content_equal}"
    );
    if let Some(report) = first_diff_report {
        println!("expW_crossbranch_diff,rows={rows},scenario={scenario},{report}");
    }
}

fn describe_diff(a: &[u8], b: &[u8]) -> String {
    let common_prefix = a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count();
    let common_suffix = a
        .iter()
        .rev()
        .zip(b.iter().rev())
        .take_while(|(x, y)| x == y)
        .count();
    let differing = if a.len() == b.len() {
        a.iter().zip(b.iter()).filter(|(x, y)| x != y).count()
    } else {
        usize::MAX
    };
    format!(
        "len_a={},len_b={},common_prefix={common_prefix},common_suffix={common_suffix},differing_bytes={}",
        a.len(),
        b.len(),
        if differing == usize::MAX {
            -1
        } else {
            differing as i64
        }
    )
}

#[derive(Clone, Copy, Default)]
struct Parsed {
    snapshot_kind: u8,
    metadata_kind: u8,
    working_diff_tag: u8,
    snapshot_fingerprint: usize,
    snapshot_payload: usize,
    metadata_fingerprint: usize,
    metadata_payload: usize,
    working_diff: usize,
    columnar: usize,
    snapshot_start: usize,
    metadata_end: usize,
}

fn parse_value(value: &[u8]) -> Parsed {
    assert!(
        value.len() >= HEAD_VALUE_HEADER_BYTES,
        "hot row shorter than v8 header"
    );
    assert_eq!(
        value[0], HEAD_VALUE_VERSION,
        "hot row codec version changed; update expW_hot_row_anatomy"
    );
    let flags = value[1];
    let snapshot_kind = (flags >> HEAD_VALUE_SNAPSHOT_SHIFT) & HEAD_VALUE_SLOT_MASK;
    let metadata_kind = (flags >> HEAD_VALUE_METADATA_SHIFT) & HEAD_VALUE_SLOT_MASK;
    let working_diff_tag = (flags >> HEAD_VALUE_WORKING_DIFF_SHIFT) & HEAD_VALUE_WORKING_DIFF_MASK;
    let snapshot_len = u32::from_be_bytes(value[50..54].try_into().expect("snapshot len")) as usize;
    let metadata_len = u32::from_be_bytes(value[54..58].try_into().expect("metadata len")) as usize;
    let has_columnar = value[58] == 1;
    let snapshot_fingerprint = match snapshot_kind {
        HEAD_SLOT_REF => JSON_REF_BYTES,
        HEAD_SLOT_INLINE_FINGERPRINTED => JSON_REF_BYTES,
        _ => 0,
    };
    let metadata_fingerprint = match metadata_kind {
        HEAD_SLOT_REF => JSON_REF_BYTES,
        HEAD_SLOT_INLINE_FINGERPRINTED => JSON_REF_BYTES,
        _ => 0,
    };
    let working_diff = match working_diff_tag {
        HEAD_WORKING_DIFF_BEFORE_ABSENT => WORKING_DIFF_CHECKPOINT_BYTES,
        HEAD_WORKING_DIFF_BEFORE_PRESENT => {
            WORKING_DIFF_CHECKPOINT_BYTES + WORKING_DIFF_VERSION_BYTES
        }
        _ => 0,
    };
    Parsed {
        snapshot_kind,
        metadata_kind,
        working_diff_tag,
        snapshot_fingerprint,
        snapshot_payload: snapshot_len - snapshot_fingerprint,
        metadata_fingerprint,
        metadata_payload: metadata_len - metadata_fingerprint,
        working_diff,
        columnar: if has_columnar {
            COLUMNAR_BASE_COORDINATE_BYTES
        } else {
            0
        },
        snapshot_start: HEAD_VALUE_HEADER_BYTES,
        metadata_end: HEAD_VALUE_HEADER_BYTES + snapshot_len + metadata_len,
    }
}

/// Zeroes the fields whose only job is to name *this* change: change id, commit
/// id and both timestamps. Everything else, including the working-diff
/// baseline and columnar coordinate, is preserved.
fn normalize_identity(value: &[u8], _parsed: &Parsed) -> Vec<u8> {
    let mut out = value.to_vec();
    out[2..50].fill(0);
    out
}

/// Keeps only the parts that describe the row's *content*: flags, slot kinds
/// and both JSON payloads. Per-row identity, the working-diff baseline and the
/// columnar coordinate are dropped entirely. This is the upper bound on what a
/// cross-branch content-addressed HOT plane could ever share.
fn content_only_bytes_of(value: &[u8], parsed: &Parsed) -> Vec<u8> {
    let mut out = Vec::with_capacity(parsed.metadata_end - parsed.snapshot_start + 4);
    out.push(value[1] & 0b0011_1111); // flags without the working-diff tag
    out.extend_from_slice(&value[parsed.snapshot_start..parsed.metadata_end]);
    out
}

/// Length of the `branch_id ++ generation` scope prefix of a HOT row key.
///
/// `branch_id` is written by `write_key_string` (0x00 escaped as 0x00 0xFF,
/// terminated by 0x00 0x00), then a fixed 16-byte generation.
fn scope_prefix_len(key: &[u8]) -> usize {
    let mut index = 0usize;
    while index + 1 < key.len() {
        if key[index] == 0x00 {
            if key[index + 1] == 0xff {
                index += 2;
                continue;
            }
            assert_eq!(key[index + 1], 0x00, "unexpected branch-id terminator");
            return index + 2 + GENERATION_BYTES;
        }
        index += 1;
    }
    panic!("hot row key has no branch-id terminator");
}

// ---------------------------------------------------------------------------
// Fixture — identical to `benches/branch_storage_sharing.rs`.
// ---------------------------------------------------------------------------

async fn settle(storage: &SlateDB) {
    storage
        .flush_memtable_for_diagnostics()
        .await
        .expect("flush anatomy memtable");
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
}

async fn space_totals(storage: &SlateDB) -> BTreeMap<String, (u64, u64, u64)> {
    let adapter = StorageAdapter::new(storage.clone());
    let read = adapter
        .begin_read(ReadOptions::default())
        .await
        .expect("open anatomy layout read");
    let accounting = layout_accounting(&read).await;
    drop(read);
    accounting
        .into_iter()
        .map(|entry| {
            (
                entry.space.to_owned(),
                (entry.key_bytes, entry.value_bytes, entry.rows),
            )
        })
        .collect()
}

async fn hot_inventory(storage: &SlateDB) -> Vec<(Vec<u8>, Vec<u8>)> {
    let adapter = StorageAdapter::new(storage.clone());
    let read = adapter
        .begin_read(ReadOptions::default())
        .await
        .expect("open anatomy inventory read");
    let inventory = space_inventory(&read, HOT_ROW_SPACE.name).await;
    drop(read);
    inventory
}

async fn create_branch(main: &Lix<SlateDB>, name: &str) -> String {
    main.create_branch(CreateBranchOptions {
        id: None,
        name: name.to_owned(),
        from_commit_id: None,
    })
    .await
    .expect("create anatomy branch")
    .id
}

async fn modify_rows(lix: &Lix<SlateDB>, branch: &str, start: usize, count: usize) {
    let session = lix
        .open_another_session()
        .await
        .expect("open anatomy branch session");
    session
        .switch_branch(lix::SwitchBranchOptions {
            branch_id: (branch.to_owned()).to_string(),
        })
        .await
        .expect("switch session branch");
    let mut written = 0usize;
    while written < count {
        let batch = (count - written).min(SEED_BATCH_ROWS);
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("begin anatomy modification");
        for offset in 0..batch {
            let index = start + written + offset;
            transaction
                .execute(
                    "UPDATE branch_fixture SET value = CAST($1 AS JSONB) WHERE path = $2",
                    &[
                        Value::Text(format!(
                            r#"{{"seed":{index},"edited":true,"pad":"{}"}}"#,
                            pad()
                        )),
                        Value::Text(row_path(index)),
                    ],
                )
                .await
                .expect("stage anatomy modification");
        }
        transaction
            .commit()
            .await
            .expect("commit anatomy modification");
        written += batch;
    }
}

fn row_path(index: usize) -> String {
    format!("/branch/fixture/{index:09}")
}

async fn register_schema(session: &Lix<SlateDB>) {
    let schema = serde_json::json!({
        "x-lix-key": "branch_fixture",
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
             VALUES (CAST($1 AS JSONB), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register anatomy schema");
}

async fn seed_rows(session: &Lix<SlateDB>, rows: usize) {
    let mut written = 0usize;
    while written < rows {
        let batch = (rows - written).min(SEED_BATCH_ROWS);
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("begin anatomy seed");
        for offset in 0..batch {
            let index = written + offset;
            transaction
                .execute(
                    "INSERT INTO branch_fixture (path, value) VALUES ($1, CAST($2 AS JSONB))",
                    &[
                        Value::Text(row_path(index)),
                        Value::Text(format!(r#"{{"seed":{index},"pad":"{}"}}"#, pad())),
                    ],
                )
                .await
                .expect("stage anatomy seed row");
        }
        transaction.commit().await.expect("commit anatomy seed");
        written += batch;
    }
}
