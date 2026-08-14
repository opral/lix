//! Authenticated repository-wide routing metadata for change/history segments.
//!
//! This module deliberately contains only the canonical format, atomic root
//! selector, and verified reader. Production writers and public query routing
//! must switch to it together: wiring only one producer or consumer would
//! create a second history authority, which this hard cut forbids.

use std::cmp::Ordering;

use bytes::Bytes;

use crate::LixError;
use crate::changelog::{ChangeId, CommitId};
use crate::common::LixTimestamp;
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageGetOptions, StorageKey, StoragePrecondition,
    StorageProjectedValue, StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
    ValueSemantics,
};
use crate::storage_codec;

pub(crate) const HISTORY_DIRECTORY_NODE_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_0031),
    "tracked_state.history_directory_node.v1",
    ValueSemantics::Immutable,
);
pub(crate) const HISTORY_DIRECTORY_ROOT_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_0034),
    "tracked_state.history_directory_root.v1",
    ValueSemantics::Mutable,
);

const ROOT_KEY: &[u8] = b"repository";
const NODE_MAGIC: &[u8; 4] = b"LHDN";
const ROOT_MAGIC: &[u8; 4] = b"LHDR";
const DIGEST_BYTES: usize = 32;
const NODE_DIGEST_CONTEXT: &str = "lix repository history directory node v1";
const ROOT_DIGEST_CONTEXT: &str = "lix repository history directory root v1";

pub(crate) const HISTORY_DOMAIN_TRACKED: u8 = 1;
pub(crate) const HISTORY_DOMAIN_STANDALONE: u8 = 2;
pub(crate) const HISTORY_DOMAIN_DERIVED: u8 = 3;

/// One canonical range-to-segment binding.
///
/// `segment_digest` authenticates the exact immutable segment bytes in
/// addition to its physical space/key. A directory hit is routing metadata,
/// never value authority: the selected segment must still validate its own
/// domain, owner, rows, and digest before rows are returned.
#[derive(Clone, Debug, Eq, PartialEq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct HistoryDirectoryEntry {
    pub(crate) domain: u8,
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
    pub(crate) generation: u64,
    pub(crate) committed_at: LixTimestamp,
    pub(crate) schema_key: String,
    pub(crate) first_state_key: Vec<u8>,
    pub(crate) last_state_key: Vec<u8>,
    pub(crate) segment_space_id: u32,
    pub(crate) segment_key: Vec<u8>,
    pub(crate) segment_digest: [u8; DIGEST_BYTES],
    pub(crate) row_count: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, musli::Encode, musli::Decode)]
#[musli(packed)]
struct HistoryDirectoryNode {
    entries: Vec<HistoryDirectoryEntry>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct HistoryDirectoryRoot {
    pub(crate) generation: u64,
    pub(crate) node_id: [u8; DIGEST_BYTES],
    pub(crate) entry_count: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct HistoryDirectoryObservation {
    pub(crate) root: Option<HistoryDirectoryRoot>,
    pub(crate) raw_token: Option<Bytes>,
}

pub(crate) struct HistoryDirectoryReader<S> {
    store: S,
}

impl<S> HistoryDirectoryReader<S>
where
    S: StorageAdapterRead,
{
    pub(crate) async fn load_observed(
        &self,
    ) -> Result<HistoryDirectoryObservation, LixError> {
        let values = PointReadPlan::new(HISTORY_DIRECTORY_ROOT_SPACE, &[root_key()])
            .materialize(&self.store, StorageGetOptions::default())
            .await?
            .value;
        let Some(value) = values.into_iter().next().flatten() else {
            return Ok(HistoryDirectoryObservation {
                root: None,
                raw_token: None,
            });
        };
        let StorageProjectedValue::FullValue(bytes) = value else {
            return Err(history_directory_corruption(
                "root point read omitted its value",
            ));
        };
        let root = decode_root(&bytes)?;
        Ok(HistoryDirectoryObservation {
            root: Some(root),
            raw_token: Some(bytes),
        })
    }

    pub(crate) async fn load_entries(
        &self,
        root: HistoryDirectoryRoot,
    ) -> Result<Vec<HistoryDirectoryEntry>, LixError> {
        let key = StorageKey(Bytes::copy_from_slice(&root.node_id));
        let values = PointReadPlan::new(HISTORY_DIRECTORY_NODE_SPACE, &[key])
            .materialize(&self.store, StorageGetOptions::default())
            .await?
            .value;
        let Some(value) = values.into_iter().next().flatten() else {
            return Err(history_directory_corruption(
                "root references a missing directory node",
            ));
        };
        let StorageProjectedValue::FullValue(bytes) = value else {
            return Err(history_directory_corruption(
                "node point read omitted its value",
            ));
        };
        let entries = decode_node(root.node_id, &bytes)?;
        if u64::try_from(entries.len()).ok() != Some(root.entry_count) {
            return Err(history_directory_corruption(
                "root entry count does not match directory node",
            ));
        }
        validate_entries(&entries)?;
        Ok(entries)
    }

    /// Returns only entries whose authenticated key interval intersects the
    /// requested schema/key interval.
    ///
    /// The two partition operations bound filtering to one schema after the
    /// root node has authenticated. Key intervals from different commits may
    /// overlap, so a flat node cannot safely binary-search by `last_key`; the
    /// production cut must page this same canonical order with authenticated
    /// interval metadata rather than pretend those intervals are disjoint.
    pub(crate) async fn query(
        &self,
        root: HistoryDirectoryRoot,
        schema_key: &str,
        first_state_key: &[u8],
        last_state_key: &[u8],
    ) -> Result<Vec<HistoryDirectoryEntry>, LixError> {
        if first_state_key > last_state_key {
            return Err(history_directory_corruption(
                "requested history interval is reversed",
            ));
        }
        let entries = self.load_entries(root).await?;
        let start = entries.partition_point(|entry| entry.schema_key.as_str() < schema_key);
        let end = entries.partition_point(|entry| entry.schema_key.as_str() <= schema_key);
        Ok(entries[start..end]
            .iter()
            .filter(|entry| {
                entry.first_state_key.as_slice() <= last_state_key
                    && entry.last_state_key.as_slice() >= first_state_key
            })
            .cloned()
            .collect())
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct HistoryDirectoryContext;

impl HistoryDirectoryContext {
    pub(crate) fn reader<S>(self, store: S) -> HistoryDirectoryReader<S>
    where
        S: StorageAdapterRead,
    {
        HistoryDirectoryReader { store }
    }
}

/// Stages one immutable canonical directory node and its mutable authenticated
/// root in the caller's existing publication write set.
pub(crate) fn stage_history_directory(
    writes: &mut StorageWriteSet,
    prior: &HistoryDirectoryObservation,
    entries: Vec<HistoryDirectoryEntry>,
) -> Result<HistoryDirectoryRoot, LixError> {
    validate_entries(&entries)?;
    let generation = match prior.root {
        None => 1,
        Some(root) => root.generation.checked_add(1).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "history directory root generation overflowed",
            )
        })?,
    };
    let node = encode_node(&entries)?;
    let node_id = *blake3::hash(&node).as_bytes();
    writes.put(
        HISTORY_DIRECTORY_NODE_SPACE,
        StorageKey(Bytes::copy_from_slice(&node_id)),
        StorageValue {
            bytes: Bytes::from(node),
        },
    );
    let root = HistoryDirectoryRoot {
        generation,
        node_id,
        entry_count: entries.len().try_into().map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "history directory entry count overflowed u64",
            )
        })?,
    };
    writes.put(
        HISTORY_DIRECTORY_ROOT_SPACE,
        root_key(),
        StorageValue {
            bytes: Bytes::from(encode_root(&root)?),
        },
    );
    Ok(root)
}

pub(crate) fn history_directory_precondition(
    observation: &HistoryDirectoryObservation,
) -> StoragePrecondition {
    match &observation.raw_token {
        None => StoragePrecondition::KeyAbsent {
            space: HISTORY_DIRECTORY_ROOT_SPACE,
            key: root_key(),
        },
        Some(expected) => StoragePrecondition::KeyValueEquals {
            space: HISTORY_DIRECTORY_ROOT_SPACE,
            key: root_key(),
            expected: expected.clone(),
        },
    }
}

fn root_key() -> StorageKey {
    StorageKey(Bytes::from_static(ROOT_KEY))
}

fn validate_entries(entries: &[HistoryDirectoryEntry]) -> Result<(), LixError> {
    for entry in entries {
        if !matches!(
            entry.domain,
            HISTORY_DOMAIN_TRACKED | HISTORY_DOMAIN_STANDALONE | HISTORY_DOMAIN_DERIVED
        ) || entry.first_state_key > entry.last_state_key
            || entry.segment_key.is_empty()
            || entry.row_count == 0
        {
            return Err(history_directory_corruption(
                "directory entry has an invalid domain, interval, segment key, or row count",
            ));
        }
    }
    if entries
        .windows(2)
        .any(|pair| canonical_entry_cmp(&pair[0], &pair[1]) != Ordering::Less)
    {
        return Err(history_directory_corruption(
            "directory entries are not in canonical distinct order",
        ));
    }
    Ok(())
}

fn canonical_entry_cmp(
    left: &HistoryDirectoryEntry,
    right: &HistoryDirectoryEntry,
) -> Ordering {
    (
        left.schema_key.as_str(),
        left.first_state_key.as_slice(),
        left.last_state_key.as_slice(),
        left.committed_at,
        left.generation,
        left.commit_id,
        left.change_id,
        left.domain,
        left.segment_space_id,
        left.segment_key.as_slice(),
        left.segment_digest,
        left.row_count,
    )
        .cmp(&(
            right.schema_key.as_str(),
            right.first_state_key.as_slice(),
            right.last_state_key.as_slice(),
            right.committed_at,
            right.generation,
            right.commit_id,
            right.change_id,
            right.domain,
            right.segment_space_id,
            right.segment_key.as_slice(),
            right.segment_digest,
            right.row_count,
        ))
}

fn encode_node(entries: &[HistoryDirectoryEntry]) -> Result<Vec<u8>, LixError> {
    let payload = storage_codec::encode(
        "repository history directory node",
        &HistoryDirectoryNode {
            entries: entries.to_vec(),
        },
    )?;
    let mut encoded = Vec::with_capacity(NODE_MAGIC.len() + payload.len() + DIGEST_BYTES);
    encoded.extend_from_slice(NODE_MAGIC);
    encoded.extend_from_slice(&payload);
    let mut hasher = blake3::Hasher::new_derive_key(NODE_DIGEST_CONTEXT);
    hasher.update(&encoded);
    encoded.extend_from_slice(hasher.finalize().as_bytes());
    Ok(encoded)
}

fn decode_node(
    expected_node_id: [u8; DIGEST_BYTES],
    bytes: &[u8],
) -> Result<Vec<HistoryDirectoryEntry>, LixError> {
    if *blake3::hash(bytes).as_bytes() != expected_node_id {
        return Err(history_directory_corruption(
            "directory node object identity mismatch",
        ));
    }
    let payload_end = bytes
        .len()
        .checked_sub(DIGEST_BYTES)
        .filter(|end| *end >= NODE_MAGIC.len())
        .ok_or_else(|| history_directory_corruption("directory node is truncated"))?;
    let (authenticated, stored_digest) = bytes.split_at(payload_end);
    let mut hasher = blake3::Hasher::new_derive_key(NODE_DIGEST_CONTEXT);
    hasher.update(authenticated);
    if !authenticated.starts_with(NODE_MAGIC)
        || stored_digest != hasher.finalize().as_bytes()
    {
        return Err(history_directory_corruption(
            "directory node authentication digest mismatch",
        ));
    }
    let node = storage_codec::decode::<HistoryDirectoryNode>(
        "repository history directory node",
        &authenticated[NODE_MAGIC.len()..],
    )?;
    Ok(node.entries)
}

fn encode_root(root: &HistoryDirectoryRoot) -> Result<Vec<u8>, LixError> {
    let payload = storage_codec::encode("repository history directory root", root)?;
    let mut encoded = Vec::with_capacity(ROOT_MAGIC.len() + payload.len() + DIGEST_BYTES);
    encoded.extend_from_slice(ROOT_MAGIC);
    encoded.extend_from_slice(&payload);
    let mut hasher = blake3::Hasher::new_derive_key(ROOT_DIGEST_CONTEXT);
    hasher.update(ROOT_KEY);
    hasher.update(&encoded);
    encoded.extend_from_slice(hasher.finalize().as_bytes());
    Ok(encoded)
}

fn decode_root(bytes: &[u8]) -> Result<HistoryDirectoryRoot, LixError> {
    let payload_end = bytes
        .len()
        .checked_sub(DIGEST_BYTES)
        .filter(|end| *end >= ROOT_MAGIC.len())
        .ok_or_else(|| history_directory_corruption("directory root is truncated"))?;
    let (authenticated, stored_digest) = bytes.split_at(payload_end);
    let mut hasher = blake3::Hasher::new_derive_key(ROOT_DIGEST_CONTEXT);
    hasher.update(ROOT_KEY);
    hasher.update(authenticated);
    if !authenticated.starts_with(ROOT_MAGIC)
        || stored_digest != hasher.finalize().as_bytes()
    {
        return Err(history_directory_corruption(
            "directory root authentication digest mismatch",
        ));
    }
    storage_codec::decode(
        "repository history directory root",
        &authenticated[ROOT_MAGIC.len()..],
    )
}

fn history_directory_corruption(detail: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("repository history directory corruption: {detail}"),
    )
}

#[cfg(test)]
mod tests {
    use crate::storage_adapter::{
        Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions,
    };

    use super::*;

    fn entry(label: &str, first: u8, last: u8) -> HistoryDirectoryEntry {
        HistoryDirectoryEntry {
            domain: HISTORY_DOMAIN_TRACKED,
            change_id: ChangeId::for_test_label(&format!("change-{label}")),
            commit_id: CommitId::for_test_label(&format!("commit-{label}")),
            generation: u64::from(first),
            committed_at: LixTimestamp::expect_parse("committed_at", "2026-01-01T00:00:00Z"),
            schema_key: "schema-a".to_string(),
            first_state_key: vec![first],
            last_state_key: vec![last],
            segment_space_id: 0x0004_0004,
            segment_key: label.as_bytes().to_vec(),
            segment_digest: *blake3::hash(label.as_bytes()).as_bytes(),
            row_count: 1,
        }
    }

    #[tokio::test]
    async fn stages_loads_queries_and_guards_the_root() {
        let storage = StorageAdapter::new(Memory::new());
        let absent = HistoryDirectoryObservation {
            root: None,
            raw_token: None,
        };
        let entries = vec![entry("a", 1, 3), entry("b", 5, 7)];
        let mut writes = storage.new_write_set();
        let root = stage_history_directory(&mut writes, &absent, entries.clone())
            .expect("directory should stage");
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions: vec![history_directory_precondition(&absent)],
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("directory should commit atomically");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let reader = HistoryDirectoryContext.reader(read);
        let observed = reader
            .load_observed()
            .await
            .expect("root should authenticate");
        assert_eq!(observed.root, Some(root));
        assert_eq!(reader.load_entries(root).await.unwrap(), entries);
        assert_eq!(reader.query(root, "schema-a", &[2], &[6]).await.unwrap().len(), 2);
        assert!(reader.query(root, "schema-a", &[8], &[9]).await.unwrap().is_empty());
    }

    #[test]
    fn rejects_noncanonical_entries_and_authenticated_substitution() {
        let ordered = vec![entry("a", 1, 3), entry("b", 5, 7)];
        let mut reversed = ordered.clone();
        reversed.reverse();
        assert!(validate_entries(&reversed).is_err());
        assert!(validate_entries(&[ordered[0].clone(), ordered[0].clone()]).is_err());

        let encoded = encode_node(&ordered).unwrap();
        let id = *blake3::hash(&encoded).as_bytes();
        assert_eq!(decode_node(id, &encoded).unwrap(), ordered);
        let mut wrong_id = id;
        wrong_id[0] ^= 1;
        assert!(decode_node(wrong_id, &encoded).is_err());
        let mut corrupted = encoded;
        corrupted[NODE_MAGIC.len()] ^= 1;
        assert!(decode_node(id, &corrupted).is_err());
    }
}
