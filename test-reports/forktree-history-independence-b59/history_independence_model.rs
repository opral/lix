//! Pure executable model for the b59 history-independence decision oracle.
//!
//! This is not a ForkTree codec and is not a production authority. It models
//! semantic equality, row-key-independent content identities, history-only
//! objects, authenticated publication, and the measurement/ceiling fields
//! that the future Memory/RocksDB/SlateDB harness must emit. Adapter commands
//! remain deliberately UNRUN in this package.

use std::collections::{BTreeMap, BTreeSet};

const BLOB_DOMAIN: &str = "lix oracle blob content v1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Row {
    pub key: String,
    pub blob_id: String,
    pub bytes: Vec<u8>,
    pub tombstone: bool,
}

impl Row {
    pub fn value(key: &str, bytes: &[u8]) -> Self {
        Self {
            key: key.to_owned(),
            blob_id: content_object_id(bytes),
            bytes: bytes.to_vec(),
            tombstone: false,
        }
    }

    pub fn tombstone(key: &str) -> Self {
        Self {
            key: key.to_owned(),
            blob_id: String::new(),
            bytes: Vec::new(),
            tombstone: true,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Event {
    Put(Row),
    Delete { key: String },
    BeginBatch,
    Branch { name: String },
    Checkpoint { name: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct History {
    pub events: Vec<Event>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    pub rows: BTreeMap<String, Row>,
    pub logical_digest: String,
    pub construction_root: String,
    pub history_digest: String,
    pub content_object_ids: BTreeSet<String>,
    pub history_object_ids: BTreeSet<String>,
    pub object_ids: BTreeSet<String>,
    pub object_sizes: BTreeMap<String, usize>,
    pub content_bytes: BTreeMap<String, Vec<u8>>,
    pub object_bytes: usize,
    pub history_only_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PairComparison {
    pub logical_equal: bool,
    pub content_identities_equal: bool,
    pub construction_roots_equal: bool,
    pub history_equal: bool,
    pub reopened_equal: bool,
    pub shared_content_bytes: usize,
    pub unique_content_bytes: (usize, usize),
    pub diff_reads: usize,
    pub history_reads: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Measurement {
    pub input_digest: String,
    pub logical_digest: String,
    pub reopened_digest: String,
    pub diff_reads: usize,
    pub history_reads: usize,
    pub synchronization_bytes: usize,
    pub publication_bytes: usize,
    pub allocated_bytes: usize,
    pub settled_disk_bytes: usize,
    pub history_only_bytes: usize,
    pub live_content_bytes: usize,
    pub gc_reclaimed_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcResult {
    pub retained_object_ids: BTreeSet<String>,
    pub reclaimed_object_ids: BTreeSet<String>,
    pub reclaimed_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PerfectEliminationCeiling {
    pub removable_bytes: usize,
    pub control_bytes: usize,
    pub ratio_ppm: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectRecord {
    pub id: String,
    pub domain: String,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedStore {
    objects: BTreeMap<String, ObjectRecord>,
    writes: usize,
    fingerprint: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CorruptionCase {
    WrongObjectId,
    WrongDomain,
    AlteredBytes,
    MissingRequiredObject,
    TransplantedObject,
    MalformedEdge,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CorruptionError {
    WrongObjectId,
    WrongDomain,
    AlteredBytes,
    MissingRequiredObject,
    TransplantedObject,
    MalformedEdge,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReopenError {
    LogicalFingerprintMismatch,
    ContentIdentityMismatch,
}

pub fn content_object_id(bytes: &[u8]) -> String {
    let bytes_hex = hex(bytes);
    digest_text(&[BLOB_DOMAIN, &bytes_hex])
}

pub fn apply(history: &History) -> Snapshot {
    let mut rows = BTreeMap::new();
    let mut construction = Vec::from(b"lix oracle construction v2\0".as_slice());
    let mut history_objects = BTreeMap::new();

    for (index, event) in history.events.iter().enumerate() {
        append_event(&mut construction, event);
        let mut event_bytes = Vec::new();
        append_text(&mut event_bytes, &index.to_string());
        append_event(&mut event_bytes, event);
        let event_id = digest_text(&["lix oracle history event v1", &hex(&event_bytes)]);
        history_objects.insert(event_id, event_bytes.len() + 32);

        match event {
            Event::Put(row) => {
                rows.insert(row.key.clone(), row.clone());
            }
            Event::Delete { key } => {
                rows.insert(key.clone(), Row::tombstone(key));
            }
            Event::BeginBatch | Event::Branch { .. } | Event::Checkpoint { .. } => {}
        }
    }

    let logical_digest = canonical_rows_digest(&rows);
    let mut content_object_ids = BTreeSet::new();
    let mut content_bytes = BTreeMap::new();
    let mut object_sizes = BTreeMap::new();
    for row in rows.values() {
        if row.tombstone {
            continue;
        }
        let id = content_object_id(&row.bytes);
        content_object_ids.insert(id.clone());
        content_bytes.entry(id.clone()).or_insert_with(|| row.bytes.clone());
        object_sizes
            .entry(id)
            .or_insert_with(|| row.bytes.len() + 32);
    }

    let history_object_ids = history_objects.keys().cloned().collect::<BTreeSet<_>>();
    object_sizes.extend(history_objects);
    let object_ids = content_object_ids
        .union(&history_object_ids)
        .cloned()
        .collect::<BTreeSet<_>>();
    let object_bytes = object_ids
        .iter()
        .map(|id| object_sizes.get(id).copied().unwrap_or_default())
        .sum();
    let history_only_bytes = history_object_ids
        .iter()
        .map(|id| object_sizes.get(id).copied().unwrap_or_default())
        .sum();

    Snapshot {
        rows,
        logical_digest,
        construction_root: digest_bytes(&construction),
        history_digest: digest_bytes(&construction),
        content_object_ids,
        history_object_ids,
        object_ids,
        object_sizes,
        content_bytes,
        object_bytes,
        history_only_bytes,
    }
}

pub fn compare(a: &Snapshot, b: &Snapshot) -> PairComparison {
    let shared_content_bytes = a
        .content_object_ids
        .intersection(&b.content_object_ids)
        .map(|id| a.object_sizes.get(id).copied().unwrap_or_default())
        .sum();
    let a_content_bytes = content_bytes(a);
    let b_content_bytes = content_bytes(b);
    let reopened_equal = reopen(a)
        .map(|snapshot| snapshot.logical_digest == a.logical_digest)
        .unwrap_or(false)
        && reopen(b)
            .map(|snapshot| snapshot.logical_digest == b.logical_digest)
            .unwrap_or(false);
    PairComparison {
        logical_equal: a.logical_digest == b.logical_digest && a.rows == b.rows,
        content_identities_equal: a.content_object_ids == b.content_object_ids,
        construction_roots_equal: a.construction_root == b.construction_root,
        history_equal: a.history_digest == b.history_digest,
        reopened_equal,
        shared_content_bytes,
        unique_content_bytes: (
            a_content_bytes.saturating_sub(shared_content_bytes),
            b_content_bytes.saturating_sub(shared_content_bytes),
        ),
        diff_reads: a.rows.len().max(b.rows.len()),
        history_reads: a.history_object_ids.len().max(b.history_object_ids.len()),
    }
}

pub fn measure(history: &History, snapshot: &Snapshot) -> Measurement {
    let encoded_history = encode_history(history);
    let gc = final_reference_gc(snapshot);
    let live_content_bytes = content_bytes(snapshot);
    let publication_bytes = live_content_bytes + snapshot.rows.len() * 24 + 64;
    Measurement {
        input_digest: digest_bytes(&encoded_history),
        logical_digest: snapshot.logical_digest.clone(),
        reopened_digest: reopen(snapshot)
            .expect("model snapshot must reopen")
            .logical_digest,
        diff_reads: snapshot.rows.len(),
        history_reads: snapshot.history_object_ids.len(),
        synchronization_bytes: encoded_history.len(),
        publication_bytes,
        allocated_bytes: encoded_history.len() + publication_bytes + snapshot.rows.len() * 64,
        settled_disk_bytes: snapshot.object_bytes + snapshot.rows.len() * 24 + 128,
        history_only_bytes: snapshot.history_only_bytes,
        live_content_bytes,
        gc_reclaimed_bytes: gc.reclaimed_bytes,
    }
}

pub fn reopen(snapshot: &Snapshot) -> Result<Snapshot, ReopenError> {
    if canonical_rows_digest(&snapshot.rows) != snapshot.logical_digest {
        return Err(ReopenError::LogicalFingerprintMismatch);
    }
    let expected_ids = snapshot
        .rows
        .values()
        .filter(|row| !row.tombstone)
        .map(|row| content_object_id(&row.bytes))
        .collect::<BTreeSet<_>>();
    if expected_ids != snapshot.content_object_ids {
        return Err(ReopenError::ContentIdentityMismatch);
    }
    Ok(snapshot.clone())
}

pub fn final_reference_gc(snapshot: &Snapshot) -> GcResult {
    let retained_object_ids = snapshot.content_object_ids.clone();
    let reclaimed_object_ids = snapshot
        .object_ids
        .difference(&retained_object_ids)
        .cloned()
        .collect::<BTreeSet<_>>();
    let reclaimed_bytes = reclaimed_object_ids
        .iter()
        .map(|id| snapshot.object_sizes.get(id).copied().unwrap_or_default())
        .sum();
    GcResult {
        retained_object_ids,
        reclaimed_object_ids,
        reclaimed_bytes,
    }
}

pub fn perfect_elimination_ceiling(
    control_bytes: usize,
    removable_bytes: usize,
) -> PerfectEliminationCeiling {
    let removable_bytes = removable_bytes.min(control_bytes);
    let ratio_ppm = if control_bytes == 0 {
        0
    } else {
        ((removable_bytes as u128 * 1_000_000) / control_bytes as u128) as u64
    };
    PerfectEliminationCeiling {
        removable_bytes,
        control_bytes,
        ratio_ppm,
    }
}

impl AuthenticatedStore {
    pub fn from_snapshot(snapshot: &Snapshot) -> Self {
        let objects = records_for_snapshot(snapshot)
            .into_iter()
            .map(|record| (record.id.clone(), record))
            .collect::<BTreeMap<_, _>>();
        let fingerprint = store_fingerprint(&objects, 0);
        Self {
            objects,
            writes: 0,
            fingerprint,
        }
    }

    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub fn object_count(&self) -> usize {
        self.objects.len()
    }

    pub fn writes(&self) -> usize {
        self.writes
    }

    pub fn publish(
        &mut self,
        expected: &BTreeSet<String>,
        records: &[ObjectRecord],
    ) -> Result<(), CorruptionError> {
        let mut incoming = BTreeMap::new();
        for record in records {
            if record.domain != BLOB_DOMAIN {
                return Err(CorruptionError::WrongDomain);
            }
            if content_object_id(&record.bytes) != record.id {
                return Err(if expected.contains(&record.id) {
                    CorruptionError::AlteredBytes
                } else {
                    CorruptionError::WrongObjectId
                });
            }
            if !expected.contains(&record.id) {
                return Err(CorruptionError::TransplantedObject);
            }
            if incoming.insert(record.id.clone(), record.clone()).is_some() {
                return Err(CorruptionError::MalformedEdge);
            }
        }
        for id in expected {
            if !incoming.contains_key(id) {
                return Err(CorruptionError::MissingRequiredObject);
            }
        }

        let mut next = self.objects.clone();
        next.extend(incoming);
        let next_writes = self.writes + 1;
        let next_fingerprint = store_fingerprint(&next, next_writes);
        self.objects = next;
        self.writes = next_writes;
        self.fingerprint = next_fingerprint;
        Ok(())
    }
}

pub fn paired_histories() -> Vec<(String, History, History)> {
    let rows = fixture_rows(8);
    let ascending = (0..rows.len()).collect::<Vec<_>>();
    let permutation = vec![3, 0, 7, 1, 6, 2, 5, 4];

    let insert_order_a = puts(&rows, &ascending);
    let insert_order_b = puts(&rows, &permutation);

    let mut batching_b = vec![Event::BeginBatch];
    batching_b.extend(puts(&rows, &ascending).events);
    let batching_a = puts(&rows, &ascending);

    let mut branch_b = puts(&rows[..3], &[0, 1, 2]).events;
    branch_b.push(Event::Branch {
        name: "feature".into(),
    });
    branch_b.push(Event::Checkpoint {
        name: "cp-1".into(),
    });
    branch_b.extend(puts(&rows[3..], &[0, 1, 2, 3, 4]).events);
    let branch_a = puts(&rows, &ascending);

    let mut intermediate_b = vec![
        Event::Put(Row::value("row/000", b"old")),
        Event::Delete {
            key: "row/000".into(),
        },
        Event::Put(rows[0].clone()),
    ];
    intermediate_b.extend(puts(&rows[1..], &[0, 1, 2, 3, 4, 5, 6]).events);
    let intermediate_a = puts(&rows, &ascending);

    let mut shared_b = vec![Event::Checkpoint {
        name: "shared-history".into(),
    }];
    shared_b.extend(puts(&rows, &permutation).events);
    let shared_a = puts(&rows, &ascending);

    vec![
        ("insert-order".into(), insert_order_a, insert_order_b),
        ("batching".into(), batching_a, History { events: batching_b }),
        (
            "branch-checkpoint".into(),
            branch_a,
            History { events: branch_b },
        ),
        (
            "intermediate-edits".into(),
            intermediate_a,
            History {
                events: intermediate_b,
            },
        ),
        ("shared-blobs".into(), shared_a, History { events: shared_b }),
    ]
}

fn fixture_rows(count: usize) -> Vec<Row> {
    (0..count)
        .map(|index| {
            let bytes = if index % 4 == 0 {
                b"shared-content".to_vec()
            } else {
                format!("content-{index:04}").into_bytes()
            };
            Row::value(&format!("row/{index:03}"), &bytes)
        })
        .collect()
}

fn puts(rows: &[Row], order: &[usize]) -> History {
    History {
        events: order
            .iter()
            .map(|index| Event::Put(rows[*index].clone()))
            .collect(),
    }
}

fn records_for_snapshot(snapshot: &Snapshot) -> Vec<ObjectRecord> {
    snapshot
        .content_bytes
        .iter()
        .map(|(id, bytes)| ObjectRecord {
            id: id.clone(),
            domain: BLOB_DOMAIN.to_owned(),
            bytes: bytes.clone(),
        })
        .collect()
}

fn content_bytes(snapshot: &Snapshot) -> usize {
    snapshot
        .content_object_ids
        .iter()
        .map(|id| snapshot.object_sizes.get(id).copied().unwrap_or_default())
        .sum()
}

fn canonical_rows_digest(rows: &BTreeMap<String, Row>) -> String {
    let mut bytes = Vec::from(b"lix oracle canonical rows v2\0".as_slice());
    for row in rows.values() {
        append_row(&mut bytes, row);
    }
    digest_bytes(&bytes)
}

fn encode_history(history: &History) -> Vec<u8> {
    let mut bytes = Vec::from(b"lix oracle history v2\0".as_slice());
    for event in &history.events {
        append_event(&mut bytes, event);
    }
    bytes
}

fn append_event(output: &mut Vec<u8>, event: &Event) {
    match event {
        Event::Put(row) => {
            append_text(output, "put");
            append_row(output, row);
        }
        Event::Delete { key } => {
            append_text(output, "delete");
            append_text(output, key);
        }
        Event::BeginBatch => append_text(output, "begin-batch"),
        Event::Branch { name } => {
            append_text(output, "branch");
            append_text(output, name);
        }
        Event::Checkpoint { name } => {
            append_text(output, "checkpoint");
            append_text(output, name);
        }
    }
}

fn append_row(output: &mut Vec<u8>, row: &Row) {
    append_text(output, "row");
    append_text(output, &row.key);
    append_text(output, &row.blob_id);
    append_text(output, if row.tombstone { "tombstone" } else { "value" });
    append_text(output, &hex(&row.bytes));
}

fn append_text(output: &mut Vec<u8>, value: &str) {
    output.extend_from_slice(value.len().to_string().as_bytes());
    output.push(b':');
    output.extend_from_slice(value.as_bytes());
    output.push(0);
}

fn digest_text(parts: &[&str]) -> String {
    let mut bytes = Vec::new();
    for part in parts {
        append_text(&mut bytes, part);
    }
    digest_bytes(&bytes)
}

fn digest_bytes(bytes: &[u8]) -> String {
    // FNV-1a is sufficient for this deterministic model. Production ObjectId
    // authentication is explicitly outside this file and outside this claim.
    let mut value = 0xcbf29ce484222325u64;
    for byte in bytes {
        value ^= u64::from(*byte);
        value = value.wrapping_mul(0x100000001b3);
    }
    format!("{value:016x}")
}

fn store_fingerprint(objects: &BTreeMap<String, ObjectRecord>, writes: usize) -> String {
    let mut bytes = Vec::new();
    append_text(&mut bytes, &writes.to_string());
    for record in objects.values() {
        append_text(&mut bytes, &record.id);
        append_text(&mut bytes, &record.domain);
        append_text(&mut bytes, &hex(&record.bytes));
    }
    digest_bytes(&bytes)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_pair_reaches_identical_logical_state_and_reopens() {
        for (name, a, b) in paired_histories() {
            let a_snapshot = apply(&a);
            let b_snapshot = apply(&b);
            let comparison = compare(&a_snapshot, &b_snapshot);
            assert!(comparison.logical_equal, "pair {name} diverged");
            assert!(comparison.content_identities_equal, "pair {name} blob IDs diverged");
            assert!(comparison.reopened_equal, "pair {name} reopen diverged");
            assert_eq!(a_snapshot.rows, b_snapshot.rows, "pair {name} rows");
            assert_eq!(
                a_snapshot.logical_digest, b_snapshot.logical_digest,
                "pair {name} logical digest"
            );
            assert_eq!(
                a_snapshot.content_object_ids, b_snapshot.content_object_ids,
                "pair {name} content identities"
            );
        }
    }

    #[test]
    fn content_object_ids_are_row_key_independent_and_shared() {
        let a = Row::value("row/a", b"same-content");
        let b = Row::value("row/b", b"same-content");
        let changed = Row::value("row/c", b"same-length!");
        assert_eq!(a.blob_id, b.blob_id);
        assert_eq!(content_object_id(&a.bytes), content_object_id(&b.bytes));
        assert_ne!(content_object_id(&a.bytes), content_object_id(&changed.bytes));

        let snapshot = apply(&History {
            events: vec![Event::Put(a), Event::Put(b)],
        });
        assert_eq!(snapshot.content_object_ids.len(), 1);
        assert_eq!(content_bytes(&snapshot), snapshot.object_sizes[&snapshot.content_object_ids
            .iter()
            .next()
            .cloned()
            .unwrap()]);
    }

    #[test]
    fn batching_branch_checkpoint_and_intermediate_pairs_have_measurements() {
        for (name, a, b) in paired_histories() {
            let a_snapshot = apply(&a);
            let b_snapshot = apply(&b);
            let a_measurement = measure(&a, &a_snapshot);
            let b_measurement = measure(&b, &b_snapshot);
            assert!(a_measurement.synchronization_bytes > 0, "pair {name}");
            assert!(a_measurement.publication_bytes > 0, "pair {name}");
            assert!(a_measurement.allocated_bytes >= a_measurement.publication_bytes);
            assert_eq!(a_measurement.logical_digest, b_measurement.logical_digest);
            assert_eq!(a_measurement.reopened_digest, b_measurement.reopened_digest);
            assert!(compare(&a_snapshot, &b_snapshot).diff_reads > 0);
            assert!(compare(&a_snapshot, &b_snapshot).history_reads > 0);
        }
    }

    #[test]
    fn reopen_diff_history_and_final_reference_gc_are_explicit() {
        let (_, a, b) = paired_histories()
            .into_iter()
            .find(|(name, _, _)| name == "branch-checkpoint")
            .unwrap();
        let a_snapshot = apply(&a);
        let b_snapshot = apply(&b);
        assert_eq!(reopen(&a_snapshot).unwrap().logical_digest, a_snapshot.logical_digest);
        assert!(compare(&a_snapshot, &b_snapshot).history_reads > 0);
        assert!(compare(&a_snapshot, &b_snapshot).diff_reads > 0);

        let gc = final_reference_gc(&a_snapshot);
        assert_eq!(gc.retained_object_ids, a_snapshot.content_object_ids);
        assert!(gc.reclaimed_bytes > 0);
        assert!(gc.reclaimed_object_ids.is_disjoint(&gc.retained_object_ids));
    }

    #[test]
    fn perfect_elimination_ceiling_is_bounded_and_explicit() {
        let (_, history, _) = paired_histories().into_iter().next().unwrap();
        let snapshot = apply(&history);
        let measurement = measure(&history, &snapshot);
        let ceiling = perfect_elimination_ceiling(
            measurement.settled_disk_bytes,
            measurement.history_only_bytes,
        );
        assert!(ceiling.removable_bytes <= ceiling.control_bytes);
        assert!(ceiling.ratio_ppm <= 1_000_000);
        assert_eq!(
            ceiling.removable_bytes,
            measurement.history_only_bytes.min(measurement.settled_disk_bytes)
        );
    }

    #[test]
    fn authenticated_corruption_has_typed_failure_and_zero_partial_writes() {
        let (_, history, _) = paired_histories().into_iter().next().unwrap();
        let snapshot = apply(&history);
        let expected = snapshot.content_object_ids.clone();
        let clean_records = records_for_snapshot(&snapshot);

        for case in [
            CorruptionCase::WrongObjectId,
            CorruptionCase::WrongDomain,
            CorruptionCase::AlteredBytes,
            CorruptionCase::MissingRequiredObject,
            CorruptionCase::TransplantedObject,
            CorruptionCase::MalformedEdge,
        ] {
            let mut store = AuthenticatedStore::from_snapshot(&snapshot);
            let before = store.clone();
            let mut records = clean_records.clone();
            match case {
                CorruptionCase::WrongObjectId => records[0].id = "wrong-id".into(),
                CorruptionCase::WrongDomain => records[0].domain = "wrong-domain".into(),
                CorruptionCase::AlteredBytes => records[0].bytes.push(0),
                CorruptionCase::MissingRequiredObject => {
                    records.pop();
                }
                CorruptionCase::TransplantedObject => {
                    records[0].bytes = b"transplanted".to_vec();
                    records[0].id = content_object_id(&records[0].bytes);
                }
                CorruptionCase::MalformedEdge => records.push(records[0].clone()),
            }
            let result = store.publish(&expected, &records);
            assert!(result.is_err(), "case {case:?} unexpectedly published");
            assert_eq!(
                store, before,
                "case {case:?} changed authenticated state or write count"
            );
        }
    }
}
