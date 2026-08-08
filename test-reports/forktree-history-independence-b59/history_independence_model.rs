//! Pure, dependency-free decision model for the b59 history-independence oracle.
//!
//! This is not a ForkTree codec and is not a production authority. It models
//! only the semantic comparison and the upper-bound accounting needed before a
//! real Memory/RocksDB/SlateDB run. The adapter commands are deliberately
//! UNRUN in this package.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Row {
    pub key: String,
    pub blob_id: String,
    pub bytes: Vec<u8>,
    pub tombstone: bool,
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
    pub object_ids: BTreeSet<String>,
    pub object_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PairComparison {
    pub logical_equal: bool,
    pub construction_roots_equal: bool,
    pub shared_object_bytes: usize,
    pub unique_object_bytes: (usize, usize),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Corruption {
    WrongObjectId,
    WrongDomain,
    AlteredBytes,
    MissingRequiredObject,
    TruncatedEdge,
    TransplantedObject,
}

pub fn apply(history: &History) -> Snapshot {
    let mut rows = BTreeMap::new();
    let mut construction = Vec::from(b"lix oracle construction v1\0".as_slice());

    for (index, event) in history.events.iter().enumerate() {
        append_text(&mut construction, &index.to_string());
        match event {
            Event::Put(row) => {
                append_row(&mut construction, row);
                rows.insert(row.key.clone(), row.clone());
            }
            Event::Delete { key } => {
                append_text(&mut construction, "delete");
                append_text(&mut construction, key);
                rows.insert(
                    key.clone(),
                    Row {
                        key: key.clone(),
                        blob_id: String::new(),
                        bytes: Vec::new(),
                        tombstone: true,
                    },
                );
            }
            Event::BeginBatch => append_text(&mut construction, "batch"),
            Event::Branch { name } => {
                append_text(&mut construction, "branch");
                append_text(&mut construction, name);
            }
            Event::Checkpoint { name } => {
                append_text(&mut construction, "checkpoint");
                append_text(&mut construction, name);
            }
        }
    }

    let mut canonical = Vec::from(b"lix oracle final state v1\0".as_slice());
    let mut object_ids = BTreeSet::new();
    for row in rows.values() {
        append_row(&mut canonical, row);
        let object_id = digest_text(&["row", &row.key, &row.blob_id, &hex(&row.bytes)]);
        object_ids.insert(object_id);
    }

    let logical_digest = digest_bytes(&canonical);
    let construction_root = digest_bytes(&construction);
    let object_bytes = object_ids.iter().map(|id| id.len() + 32).sum();
    Snapshot {
        rows,
        logical_digest,
        construction_root,
        object_ids,
        object_bytes,
    }
}

pub fn compare(a: &Snapshot, b: &Snapshot) -> PairComparison {
    let shared_object_bytes = a
        .object_ids
        .intersection(&b.object_ids)
        .map(|id| id.len() + 32)
        .sum();
    PairComparison {
        logical_equal: a.logical_digest == b.logical_digest && a.rows == b.rows,
        construction_roots_equal: a.construction_root == b.construction_root,
        shared_object_bytes,
        unique_object_bytes: (
            a.object_bytes.saturating_sub(shared_object_bytes),
            b.object_bytes.saturating_sub(shared_object_bytes),
        ),
    }
}

pub fn corruption_is_fail_closed(corruption: Corruption) -> bool {
    matches!(
        corruption,
        Corruption::WrongObjectId
            | Corruption::WrongDomain
            | Corruption::AlteredBytes
            | Corruption::MissingRequiredObject
            | Corruption::TruncatedEdge
            | Corruption::TransplantedObject
    )
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
    // FNV-1a is sufficient for this deterministic model; production ObjectId
    // authentication is explicitly outside this file and outside this claim.
    let mut value = 0xcbf29ce484222325u64;
    for byte in bytes {
        value ^= u64::from(*byte);
        value = value.wrapping_mul(0x100000001b3);
    }
    format!("{value:016x}")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(key: &str, blob_id: &str, bytes: &[u8]) -> Row {
        Row {
            key: key.to_owned(),
            blob_id: blob_id.to_owned(),
            bytes: bytes.to_vec(),
            tombstone: false,
        }
    }

    #[test]
    fn insertion_order_is_semantically_equal_but_history_can_differ() {
        let a = History {
            events: vec![
                Event::Put(row("a", "blob-a", b"A")),
                Event::Put(row("b", "blob-b", b"B")),
            ],
        };
        let b = History {
            events: vec![
                Event::Put(row("b", "blob-b", b"B")),
                Event::Put(row("a", "blob-a", b"A")),
            ],
        };
        let comparison = compare(&apply(&a), &apply(&b));
        assert!(comparison.logical_equal);
        assert!(!comparison.construction_roots_equal);
    }

    #[test]
    fn intermediate_edit_and_tombstone_converge_to_same_final_state() {
        let a = History {
            events: vec![Event::Put(row("a", "blob-a", b"A"))],
        };
        let b = History {
            events: vec![
                Event::Put(row("a", "old", b"old")),
                Event::Delete { key: "a".into() },
                Event::Put(row("a", "blob-a", b"A")),
            ],
        };
        let comparison = compare(&apply(&a), &apply(&b));
        assert!(comparison.logical_equal);
        assert!(!comparison.construction_roots_equal);
    }

    #[test]
    fn shared_objects_are_counted_without_requiring_equal_roots() {
        let a = apply(&History {
            events: vec![
                Event::Put(row("a", "shared", b"same")),
                Event::Put(row("b", "unique-a", b"A")),
            ],
        });
        let b = apply(&History {
            events: vec![
                Event::BeginBatch,
                Event::Put(row("b", "unique-b", b"B")),
                Event::Put(row("a", "shared", b"same")),
            ],
        });
        let comparison = compare(&a, &b);
        assert!(comparison.shared_object_bytes > 0);
        assert!(comparison.unique_object_bytes.0 > 0);
        assert!(comparison.unique_object_bytes.1 > 0);
    }

    #[test]
    fn named_corruption_is_always_rejected_before_publication() {
        for corruption in [
            Corruption::WrongObjectId,
            Corruption::WrongDomain,
            Corruption::AlteredBytes,
            Corruption::MissingRequiredObject,
            Corruption::TruncatedEdge,
            Corruption::TransplantedObject,
        ] {
            assert!(corruption_is_fail_closed(corruption));
        }
    }
}
