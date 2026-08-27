//! V21 row-addressable current state with columnar-base coordinates.
//!
//! V12 packed every file member of one logical row into a group. That made
//! a logical-PK lookup cheap, but it also made every normal commit read,
//! decode, merge, and rewrite each predecessor group. V17 keeps the fixed row
//! value codec and branch-control publication fence, makes a full row identity
//! the physical mutation unit, and stores each value only in the authoritative
//! file-first row index.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::mem::size_of;
use std::ops::Range;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use crate::storage_adapter::ValueSemantics;
use bytes::Bytes;
use smallvec::SmallVec;
use tracing::Instrument as _;

use super::*;
use crate::plugin::runtime::WasmCertifiedRowBatch;
use crate::storage_adapter::{BufferRange, EncodedMutationBatch, EncodedPut};
use crate::tracked_state::TrackedStateReadColumns;

pub(crate) const ROW_NAMESPACE: &str = "hot_state.row.v21";
pub(crate) const FILE_NAMESPACE: &str = "hot_state.file_schema.v18";
pub(crate) const DIFF_NAMESPACE: &str = "hot_state.diff.v17";
pub(crate) const COLLECTION_CONTROL_NAMESPACE: &str = "hot_state.collection_control.v1";
pub(crate) const INDEX_NAMESPACE: &str = "hot_state.index.v1";
pub(crate) const ROW_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_001b),
    ROW_NAMESPACE,
    ValueSemantics::Mutable,
);
/// Conservative `(branch, generation, schema)` file-membership markers.
///
/// The authoritative hot row owns every value and file identity. Markers are
/// never removed within a generation, so they may produce a harmless false
/// positive after the last file member is deleted but cannot hide live rows.
pub(crate) const FILE_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_001c),
    FILE_NAMESPACE,
    ValueSemantics::Mutable,
);
/// Reserved for the row-level first-before working-diff index.
pub(crate) const DIFF_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_001d),
    DIFF_NAMESPACE,
    ValueSemantics::Mutable,
);
/// Declared-column access path over the hot rows: `value -> row_pk`.
///
/// A predicate on a non-primary-key column has no access path in the hot row
/// key, whose only searchable dimensions are `(branch, generation, schema,
/// file, row)`. Serving one costs a scan of the whole collection plus a
/// snapshot parse per row — on the read path, and again inside
/// `validate_committed_unique_constraints` on the write path.
///
/// This plane indexes exactly the columns a schema already declares through
/// `x-lix-unique` and `x-lix-foreign-keys`, so it introduces no new
/// user-facing concept. It is a disposable cache in the sense of layout
/// invariant 3: every entry is derivable from the hot rows, the rows remain
/// the only authority for content, and dropping the plane costs nothing but
/// speed.
///
/// **Maintenance is put-only, and the invariant is "never a false negative".**
/// An entry is written when a row's indexed value is written and is never
/// deleted when that value is superseded, exactly as [`FILE_SPACE`]
/// markers behave. A superseded or deleted row therefore leaves a stale entry
/// behind, so a lookup returns *candidates*, never answers. Candidates are
/// resolved through the ordinary exact-row-pk read and re-checked by the
/// caller's own predicate. That is what makes maintenance one key-only put per
/// changed row with no pre-image read, which is in turn what keeps write cost
/// flat in collection size — the property this whole plane exists to buy on
/// the read side.
pub(crate) const INDEX_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_0033),
    INDEX_NAMESPACE,
    ValueSemantics::Mutable,
);
pub(crate) const COLLECTION_CONTROL_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_0023),
    COLLECTION_CONTROL_NAMESPACE,
    ValueSemantics::Mutable,
);

/// Engagement counters for the canonical `created_at` recovery that shares the
/// retention fence's batch.
///
/// These sit *inside* the route they measure: `LOOKUPS` counts commits that
/// submitted a non-empty key list, `KEYS` the identities submitted, and `HITS`
/// the inheritances actually applied. A timing instrument one layer up cannot
/// distinguish "the recovery is free" from "the recovery never ran", so the
/// counts are read directly rather than inferred from a flat measurement.
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) static BROAD_CANONICAL_CREATED_AT_LOOKUPS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) static BROAD_CANONICAL_CREATED_AT_KEYS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) static BROAD_CANONICAL_CREATED_AT_HITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Engagement counters for serving-view tombstone compaction.
///
/// Both sit *inside* `hot_compaction_mask`, the route they measure, rather than
/// at the publication layer above it. `CANDIDATES` counts tombstones the
/// checkpoint offered, `COMPACTED` the ones the gates actually cleared. A
/// timing or row-count instrument one layer up cannot distinguish "compaction
/// reclaimed nothing here" from "compaction never ran", and the second is the
/// failure mode this design is most exposed to: every gate is conservative, so
/// one wrong polarity makes the pass silently inert.
///
/// `ROUTES` and `OFFERED` sit above both, at the mask's first line, because
/// "the checkpoint offered no tombstone" and "the checkpoint never reached the
/// mask" are different faults with identical readings at `CANDIDATES`.
/// Decode census for the hot row serving-view scan loops.
///
/// These sit inside the **per-entry decode loop** of both scan arms
/// (`hot_scan_entries`' wide fallback and `scan_hot_file_entries`), not at the
/// layer that returns the answer: a post-filter count reads identically under a
/// seek and under a full walk. `DECODED` counts every `ROW_SPACE` entry whose
/// key this request decoded, `MATCHED` the subset that passed
/// `matches_filter`, and `TOMBSTONE` the matched subset whose fixed header
/// carries `HEAD_VALUE_DELETED` - rows fetched, decoded and then discarded
/// because they are deletions.
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) static HOT_SCAN_DECODED_ENTRIES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) static HOT_SCAN_MATCHED_ENTRIES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) static HOT_SCAN_TOMBSTONE_ENTRIES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Engagement counters for interval-local tombstone elision.
///
/// The sibling of the compaction counters below, for the route that never
/// creates the tombstone rather than reclaiming it later. Kept separate so a
/// publication that elides can never be mistaken for one that compacted:
/// phase 11 measured `routes=1 offered=0` on the compaction route for exactly
/// the workload this route exists to serve, and a shared counter would have
/// hidden that.
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) static INTERVAL_LOCAL_TOMBSTONE_ROUTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) static INTERVAL_LOCAL_TOMBSTONE_OFFERED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) static INTERVAL_LOCAL_TOMBSTONE_CANDIDATES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) static INTERVAL_LOCAL_TOMBSTONE_ELIDED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) static COMPACTED_TOMBSTONE_ROUTES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) static COMPACTED_TOMBSTONE_OFFERED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) static COMPACTED_TOMBSTONE_CANDIDATES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) static COMPACTED_TOMBSTONE_COMPACTED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// Generation-local immutable current-state bases.
///
/// Each tiny record points at one already-authored packed commit delta. Fresh
/// validated inserts publish the reference instead of duplicating every row
/// into `HOT_ROW`; later updates and deletes remain sparse HOT overlays.
pub(crate) const PACKED_CURRENT_BASE_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_0024),
    "hot_state.packed_current_base.v1",
    ValueSemantics::Mutable,
);
pub(crate) const PACKED_CURRENT_BASE_CONTROL_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_0025),
    "hot_state.packed_current_base_control.v1",
    ValueSemantics::Mutable,
);
/// Generation-local index of packed bases containing exactly one schema.
///
/// Complete collection replacements retire every indexed predecessor for the
/// schema without inspecting or risking packed bases shared by unrelated
/// schemas.
pub(crate) const PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_0027),
    "hot_state.packed_current_exclusive_schema_base.v1",
    ValueSemantics::Mutable,
);
/// One immutable tracked-state root used as the baseline for a sparse branch
/// generation. Branch creation publishes this 16-byte reference instead of
/// copying every tracked row into branch-local HOT storage.
pub(crate) const ROOT_CURRENT_BASE_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_0028),
    "hot_state.root_current_base.v1",
    ValueSemantics::Mutable,
);
const HOT_DENSE_SCAN_MIN_IDENTITIES: usize = 64;
const HOT_DENSE_SCAN_MAX_OVERREAD: usize = 2;
const HOT_DIFF_SEGMENT_VERSION: u8 = 1;
const HOT_DIFF_SEGMENT_HEADER_BYTES: usize = 5;
const HOT_DIFF_SEGMENT_MAX_BYTES: usize = 256 * 1024;
const HOT_DIFF_SEGMENT_MAX_IDENTITIES: u32 = 4_096;
// Real-repository profiling showed that smaller batches contribute negligible
// storage amplification, so retain their allocation-free direct-key path.
const HOT_DIFF_PACK_MIN_IDENTITIES: usize = 64;
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const CERTIFIED_ROW_BATCH_MAGIC_V2: &[u8; 4] = b"CEB2";
pub(crate) const CERTIFIED_ROW_BATCH_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_001f),
    "hot_state.certified_row_batch.v1",
    ValueSemantics::Mutable,
);
/// Maps one branch generation to the certified batches published under it.
///
/// The value carries the batch's schema-key set ahead of the content key, so a
/// schema-filtered scan can decide from the manifest alone whether a batch can
/// contribute rows. Without it the only way to learn a batch's schemas was to
/// fetch and parse its content header, which made every schema-filtered scan
/// read every batch on the branch.
///
/// `.v2` because the value layout changed; `.v1` bytes must not parse.
pub(crate) const CERTIFIED_ROW_BATCH_MANIFEST_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_0021),
    "hot_state.certified_row_batch_manifest.v2",
    ValueSemantics::Mutable,
);
pub(crate) const CERTIFIED_ROW_BATCH_PAGE_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_0022),
    "hot_state.certified_row_batch_page.v1",
    ValueSemantics::Mutable,
);
pub(crate) struct CertifiedRowBatchFileRef<'a> {
    pub(crate) branch_id: &'a str,
    pub(crate) file_id: &'a str,
    pub(crate) batches: &'a [WasmCertifiedRowBatch],
}

/// Publishes one immutable semantic owner per certified file batch.
///
/// The storage value is the authority for the batch rows. Current-state and
/// history readers decode rows lazily; later sparse mutations remain ordinary
/// hot rows and therefore form overlays instead of rewriting this value.
pub(crate) async fn stage_certified_row_batches(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    file_writes: &[CertifiedRowBatchFileRef<'_>],
    controls: &BTreeMap<String, BranchHeadControl>,
    observations: &BTreeMap<String, crate::branch::BranchHeadControlObservation>,
    commit_created_at: &BTreeMap<CommitId, LixTimestamp>,
    root_backed_branch_publications: &BTreeSet<String>,
) -> Result<(), LixError> {
    let mut content_owners = BTreeSet::new();
    for file in file_writes {
        for batch in file.batches {
            if !content_owners.insert((file.branch_id, file.file_id, batch.format)) {
                return Err(head_value_error(format!(
                    "certified row batches duplicate branch '{}', file '{}', format {}",
                    file.branch_id, file.file_id, batch.format
                )));
            }
        }
    }
    let mut complete_manifest_suffixes = BTreeMap::<String, Vec<Vec<u8>>>::new();
    for file in file_writes {
        for batch in file
            .batches
            .iter()
            .filter(|batch| batch.complete_file_state)
        {
            let mut suffix = Vec::new();
            append_batch_text(&mut suffix, file.file_id)?;
            suffix.extend_from_slice(&batch.format.to_le_bytes());
            complete_manifest_suffixes
                .entry(file.branch_id.to_owned())
                .or_default()
                .push(suffix);
        }
    }

    let needs_branch_creation_donor = controls.keys().any(|branch_id| {
        !root_backed_branch_publications.contains(branch_id)
            && observations
                .get(branch_id)
                .is_none_or(|observation| observation.control.is_none())
    });
    let durable_controls = if needs_branch_creation_donor {
        BranchHeadControlContext::new().reader(read).scan().await?
    } else {
        Vec::new()
    };

    let mut inherited_manifests = BTreeMap::new();
    for (branch_id, control) in controls {
        if root_backed_branch_publications.contains(branch_id) {
            continue;
        }
        let source_generations = observations
            .get(branch_id)
            .and_then(|observation| observation.control)
            .map(|previous| BTreeSet::from([previous.tracked_generation]))
            .unwrap_or_else(|| {
                durable_controls
                    .iter()
                    .filter(|(_, candidate)| candidate.head_commit_id == control.head_commit_id)
                    .map(|(_, candidate)| candidate.tracked_generation)
                    .collect()
            });
        for source_generation in source_generations
            .into_iter()
            .filter(|generation| *generation != control.tracked_generation)
        {
            let previous_prefix = source_generation.as_uuid().as_bytes().to_vec();
            let range = StoragePrefix {
                bytes: Bytes::from(previous_prefix.clone()),
            }
            .to_range()?;
            let mut cursor = read
                .begin_scan(
                    CERTIFIED_ROW_BATCH_MANIFEST_SPACE,
                    range,
                    StorageBeginScanOptions::default(),
                )
                .await?;
            // The prefix is only the 16-byte generation, so this range covers
            // every file on the branch. Reading one page dropped every file
            // past row 1024 out of the new generation permanently.
            let manifests = cursor.collect_all().await?;
            for entry in manifests {
                let suffix = entry
                    .key
                    .0
                    .get(previous_prefix.len()..)
                    .ok_or_else(|| head_value_error("truncated certified manifest key"))?;
                if complete_manifest_suffixes
                    .get(branch_id)
                    .is_some_and(|prefixes| {
                        prefixes.iter().any(|prefix| suffix.starts_with(prefix))
                    })
                {
                    continue;
                }
                let mut key = control.tracked_generation.as_uuid().as_bytes().to_vec();
                key.extend_from_slice(suffix);
                let value = full_value_bytes(entry.value)?;
                match inherited_manifests.entry(key) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(value);
                    }
                    std::collections::btree_map::Entry::Occupied(entry)
                        if entry.get() == &value => {}
                    std::collections::btree_map::Entry::Occupied(_) => {
                        return Err(head_value_error(
                            "certified manifests disagree for the same inherited key",
                        ));
                    }
                }
            }
        }
    }
    for (key, value) in inherited_manifests {
        writes.put(
            CERTIFIED_ROW_BATCH_MANIFEST_SPACE,
            StorageKey(Bytes::from(key)),
            StorageValue { bytes: value },
        );
    }

    for file in file_writes {
        if file.batches.is_empty() {
            continue;
        }
        let control = controls.get(file.branch_id).ok_or_else(|| {
            head_value_error("certified row batch has no published branch control")
        })?;
        let created_at = commit_created_at
            .get(&control.head_commit_id)
            .copied()
            .ok_or_else(|| head_value_error("certified row batch has no commit timestamp"))?;
        for batch in file.batches {
            if batch.complete_file_state {
                let mut manifest_prefix = control.tracked_generation.as_uuid().as_bytes().to_vec();
                append_batch_text(&mut manifest_prefix, file.file_id)?;
                manifest_prefix.extend_from_slice(&batch.format.to_le_bytes());
                let range = StoragePrefix {
                    bytes: Bytes::from(manifest_prefix),
                }
                .to_range()?;
                let mut cursor = read
                    .begin_scan(
                        CERTIFIED_ROW_BATCH_MANIFEST_SPACE,
                        range,
                        StorageBeginScanOptions::default(),
                    )
                    .await?;
                // Every prior manifest for this file must be deleted, or a
                // superseded batch survives its replacement.
                let prior_manifests = cursor.collect_all().await?;
                for entry in prior_manifests {
                    writes.delete(CERTIFIED_ROW_BATCH_MANIFEST_SPACE, entry.key);
                }
            }
            let mut content_key = control.head_commit_id.as_uuid().as_bytes().to_vec();
            append_batch_text(&mut content_key, file.file_id)?;
            content_key.extend_from_slice(&batch.format.to_le_bytes());

            let schema_bytes = batch
                .schema_keys
                .iter()
                .try_fold(2usize, |total, schema| total.checked_add(2 + schema.len()))
                .ok_or_else(|| head_value_error("certified schema list size overflowed"))?;
            let mut value = Vec::with_capacity(
                4 + schema_bytes
                    + 2
                    + file.file_id.len()
                    + 16
                    + 2
                    + created_at.to_string().len()
                    + 2
                    + 8
                    + 8
                    + 4
                    + 4
                    + batch.pages.len().saturating_mul(12),
            );
            value.extend_from_slice(CERTIFIED_ROW_BATCH_MAGIC_V2);
            value.extend_from_slice(
                &u16::try_from(batch.schema_keys.len())
                    .map_err(|_| head_value_error("certified batch has too many schemas"))?
                    .to_le_bytes(),
            );
            for schema_key in &batch.schema_keys {
                append_batch_text(&mut value, schema_key)?;
            }
            append_batch_text(&mut value, file.file_id)?;
            value.extend_from_slice(control.head_commit_id.as_uuid().as_bytes());
            append_batch_text(&mut value, &created_at.to_string())?;
            value.extend_from_slice(&batch.format.to_le_bytes());
            value.extend_from_slice(&batch.row_count.to_le_bytes());
            value.extend_from_slice(&batch.creates.high.to_le_bytes());
            value.extend_from_slice(&batch.creates.low.to_le_bytes());
            value.extend_from_slice(
                &u32::try_from(batch.pages.len())
                    .map_err(|_| head_value_error("certified row batch has too many pages"))?
                    .to_le_bytes(),
            );
            for (page_index, page) in batch.pages.iter().enumerate() {
                let (first_local_ref, last_local_ref) = match batch.format {
                    crate::plugin::runtime::HOST_CERTIFIED_PACKET_FORMAT => {
                        certified_packet_page_local_ref_range(page)?.unwrap_or((0, u32::MAX))
                    }
                    crate::plugin::runtime::HOST_CERTIFIED_ZSTD_PACKET_FORMAT => {
                        certified_zstd_packet_page_header(page)?.0
                    }
                    format => {
                        return Err(head_value_error(format!(
                            "unsupported v69 certified row batch format {format}"
                        )));
                    }
                };
                value.extend_from_slice(&first_local_ref.to_le_bytes());
                value.extend_from_slice(&last_local_ref.to_le_bytes());
                value.extend_from_slice(
                    &u32::try_from(page.len())
                        .map_err(|_| head_value_error("certified row batch page exceeds 4GiB"))?
                        .to_le_bytes(),
                );
                writes.put(
                    CERTIFIED_ROW_BATCH_PAGE_SPACE,
                    certified_row_batch_page_key(
                        &content_key,
                        u32::try_from(page_index).map_err(|_| {
                            head_value_error("certified row batch has too many pages")
                        })?,
                    ),
                    StorageValue {
                        bytes: page.clone(),
                    },
                );
            }
            writes.put(
                CERTIFIED_ROW_BATCH_SPACE,
                StorageKey(Bytes::from(content_key.clone())),
                StorageValue {
                    bytes: Bytes::from(value),
                },
            );
            let mut manifest_key = control.tracked_generation.as_uuid().as_bytes().to_vec();
            append_batch_text(&mut manifest_key, file.file_id)?;
            manifest_key.extend_from_slice(&batch.format.to_le_bytes());
            manifest_key.extend_from_slice(control.head_commit_id.as_uuid().as_bytes());
            writes.put(
                CERTIFIED_ROW_BATCH_MANIFEST_SPACE,
                StorageKey(Bytes::from(manifest_key)),
                StorageValue {
                    bytes: Bytes::from(encode_certified_manifest_value(
                        &batch.schema_keys,
                        &content_key,
                    )?),
                },
            );
        }
    }
    Ok(())
}

/// Encodes one certified manifest value: the batch's schema-key set, then the
/// content key it points at.
///
/// The schema set is a covering column over the batch header's own list, not a
/// second authority: both are written from `batch.schema_keys` in the same
/// write set, and inheritance copies the whole value, so the two cannot drift.
fn encode_certified_manifest_value(
    schema_keys: &[String],
    content_key: &[u8],
) -> Result<Vec<u8>, LixError> {
    let schema_bytes = schema_keys
        .iter()
        .try_fold(2usize, |total, schema| total.checked_add(2 + schema.len()))
        .ok_or_else(|| head_value_error("certified manifest schema list size overflowed"))?;
    let mut value = Vec::with_capacity(schema_bytes + content_key.len());
    value.extend_from_slice(
        &u16::try_from(schema_keys.len())
            .map_err(|_| head_value_error("certified manifest has too many schemas"))?
            .to_le_bytes(),
    );
    for schema_key in schema_keys {
        append_batch_text(&mut value, schema_key)?;
    }
    value.extend_from_slice(content_key);
    Ok(value)
}

/// Returns where the content key starts, or `None` when this manifest's schema
/// set cannot satisfy `wanted` and the batch therefore need not be fetched.
///
/// A manifest with no declared schemas is never pruned: an empty set means the
/// batch declared nothing, not that it matches nothing.
fn append_batch_text(output: &mut Vec<u8>, value: &str) -> Result<(), LixError> {
    output.extend_from_slice(
        &u16::try_from(value.len())
            .map_err(|_| head_value_error("certified row batch text exceeds 64KiB"))?
            .to_le_bytes(),
    );
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn certified_row_batch_page_key(content_key: &[u8], page_index: u32) -> StorageKey {
    let mut key = Vec::with_capacity(content_key.len() + 4);
    key.extend_from_slice(content_key);
    key.extend_from_slice(&page_index.to_be_bytes());
    StorageKey(Bytes::from(key))
}

struct CertifiedPacketReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> CertifiedPacketReader<'a> {
    fn bytes(&mut self, length: usize) -> Result<&'a [u8], LixError> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| head_value_error("truncated certified packet page"))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, LixError> {
        Ok(self.bytes(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, LixError> {
        Ok(u32::from_le_bytes(
            self.bytes(4)?.try_into().expect("fixed packet u32 width"),
        ))
    }

    fn u64(&mut self) -> Result<u64, LixError> {
        Ok(u64::from_le_bytes(
            self.bytes(8)?.try_into().expect("fixed packet u64 width"),
        ))
    }
}

/// Returns an ordinal range only when every packet row is a keyless create
/// and those local references are strictly increasing. A keyed or mixed page
/// remains conservatively unindexed.
fn certified_packet_page_local_ref_range(page: &[u8]) -> Result<Option<(u32, u32)>, LixError> {
    let mut rows = CertifiedPacketReader {
        bytes: page,
        offset: 0,
    };
    let mut first = None;
    let mut last = None;
    while rows.offset < rows.bytes.len() {
        let record_len = rows.u32()? as usize;
        let record_bytes = rows.bytes(record_len)?;
        let mut record = CertifiedPacketReader {
            bytes: record_bytes,
            offset: 0,
        };
        if record.u8()? != 2 {
            return Ok(None);
        }
        let schema_len = record.u32()? as usize;
        let _schema = record.bytes(schema_len)?;
        let local_ref = u32::try_from(record.u64()?)
            .map_err(|_| head_value_error("certified packet local reference exceeds u32"))?;
        if last.is_some_and(|previous| previous >= local_ref) {
            return Ok(None);
        }
        first.get_or_insert(local_ref);
        last = Some(local_ref);
    }
    Ok(first.zip(last))
}

fn certified_zstd_packet_page_header(page: &[u8]) -> Result<((u32, u32), usize, &[u8]), LixError> {
    let (header, compressed) = page
        .split_at_checked(12)
        .ok_or_else(|| head_value_error("compressed certified packet page is truncated"))?;
    let first_local_ref = u32::from_le_bytes(
        header[..4]
            .try_into()
            .expect("compressed packet first local ref"),
    );
    let last_local_ref = u32::from_le_bytes(
        header[4..8]
            .try_into()
            .expect("compressed packet last local ref"),
    );
    if first_local_ref > last_local_ref {
        return Err(head_value_error(
            "compressed certified packet page has an inverted local-ref range",
        ));
    }
    let uncompressed_len = u32::from_le_bytes(
        header[8..12]
            .try_into()
            .expect("compressed packet uncompressed length"),
    ) as usize;
    if uncompressed_len == 0 || uncompressed_len > 64 * 1024 * 1024 {
        return Err(head_value_error(
            "compressed certified packet page has an invalid uncompressed length",
        ));
    }
    Ok((
        (first_local_ref, last_local_ref),
        uncompressed_len,
        compressed,
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
// The digest is the third positional field in repository protocol v40. Older
// two-field controls belong to v39 repositories, which Engine::new rejects at
// the protocol boundary before any HOT control is decoded.
struct HotCollectionControl {
    active_generation: CommitId,
    live_count: u64,
    ordered_identity_digest: Option<[u8; 32]>,
}

#[cfg(any(test, feature = "storage-benches"))]
fn test_snapshot(delta: &TrackedHeadDeltaRef<'_>) -> Result<Option<Vec<u8>>, LixError> {
    match (delta.deleted, delta.snapshot) {
        (true, _) => Ok(None),
        (false, Some(snapshot)) => {
            let value = serde_json::from_str(snapshot).map_err(|error| {
                head_value_error(&format!(
                    "test current-state snapshot is invalid JSON: {error}"
                ))
            })?;
            WasmTypedRow::from_builtin_json(delta.schema_key, delta.row_pk, &value)
                .or_else(|_| WasmTypedRow::from_test_json_unchecked(delta.row_pk, &value))
                .and_then(|row| encode_snapshot(&row))
                .map(Some)
        }
        (false, None) => Err(head_value_error(
            "live test current-state row is missing its payload",
        )),
    }
}

#[cfg(any(test, feature = "storage-benches"))]
fn test_current_delta<'a>(
    delta: &'a TrackedHeadDeltaRef<'a>,
    snapshot: Option<&'a [u8]>,
) -> CurrentStateDeltaRef<'a> {
    CurrentStateDeltaRef {
        schema_key: delta.schema_key,
        file_id: delta.file_id,
        row_pk: delta.row_pk,
        change_id: Some(delta.change_id),
        commit_id: Some(delta.commit_id),
        untracked: false,
        deleted: delta.deleted,
        created_at: delta.created_at,
        updated_at: delta.updated_at,
        snapshot,
        metadata: delta.metadata.as_ref(),
        columnar_base_coordinate: None,
    }
}

const COMPLETE_HOT_COLLECTION_DIGEST_DOMAIN: &[u8] = b"lix.complete-hot-collection-identities.v1";

/// Streaming certificate for one complete selected HOT collection.
///
/// Untouched unfiled single-string collections retain the historical compact
/// digest used by packed replacement proofs. Every other collection hashes
/// the complete canonical HOT key, including the file discriminator and the
/// typed/composite row-primary-key encoding. Callers feed keys in physical
/// storage order; strict ordering makes duplicate identities fail closed.
struct CompleteHotCollectionDigest {
    canonical: blake3::Hasher,
    single_string: blake3::Hasher,
    single_string_compatible: bool,
    previous_key: Vec<u8>,
}

impl CompleteHotCollectionDigest {
    fn new(
        branch_id: &str,
        branch_generation: CommitId,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Self {
        let mut canonical = blake3::Hasher::new();
        canonical.update(COMPLETE_HOT_COLLECTION_DIGEST_DOMAIN);
        let scope_key = hot_collection_control_key(branch_id, branch_generation, scope);
        canonical.update(&(scope_key.len() as u64).to_le_bytes());
        canonical.update(&scope_key);
        Self {
            canonical,
            single_string: blake3::Hasher::new(),
            single_string_compatible: scope.file_id.is_none(),
            previous_key: Vec::new(),
        }
    }

    fn push(&mut self, identity: &HeadRowIdentity, canonical_key: &[u8]) -> Result<(), LixError> {
        self.push_parts(&identity.row_pk, identity.file_id.as_deref(), canonical_key)
    }

    fn push_parts(
        &mut self,
        row_pk: &RowPk,
        file_id: Option<&str>,
        canonical_key: &[u8],
    ) -> Result<(), LixError> {
        if !self.previous_key.is_empty() {
            match self.previous_key.as_slice().cmp(canonical_key) {
                Ordering::Less => {}
                Ordering::Equal => {
                    return Err(head_value_error(
                        "complete collection contains a duplicate canonical identity",
                    ));
                }
                Ordering::Greater => {
                    return Err(head_value_error(
                        "complete collection identities are not in canonical order",
                    ));
                }
            }
        }
        self.previous_key.clear();
        self.previous_key.extend_from_slice(canonical_key);

        self.canonical
            .update(&(canonical_key.len() as u64).to_le_bytes());
        self.canonical.update(canonical_key);

        if self.single_string_compatible {
            match (file_id, row_pk.as_single_string()) {
                (None, Ok(value)) => {
                    self.single_string
                        .update(&(value.len() as u64).to_le_bytes());
                    self.single_string.update(value.as_bytes());
                }
                _ => self.single_string_compatible = false,
            }
        }
        Ok(())
    }

    fn finish(self) -> [u8; 32] {
        if self.single_string_compatible {
            *self.single_string.finalize().as_bytes()
        } else {
            *self.canonical.finalize().as_bytes()
        }
    }
}

// Root-backed branches defer collection cardinality until an operation
// actually asks for it. Ordinary sparse edits must not scan the immutable
// root merely to maintain an eager count.
// Root-backed branches deliberately avoid counting every inherited row during
// creation or the first sparse edit. This reserved persisted value means that
// the count must be derived by a live scan when an API actually requests it.
const DEFERRED_ROOT_LIVE_COUNT: u64 = crate::collection_generation::DEFERRED_LIVE_COUNT;

const TRANSACTION_HOT_STATE_CACHE_MAX_ENTRIES: usize = 64;
const TRANSACTION_PACKED_POINT_CACHE_MIN_OBSERVATIONS: u8 = 16;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct HotCollectionCacheKey {
    branch_id: String,
    generation: CommitId,
    schema_key: String,
    file_id: Option<String>,
}

/// Bounded serving metadata retained for one transaction snapshot.
#[derive(Default)]
pub(crate) struct HotStateTransactionCache {
    collection_controls: StdMutex<BTreeMap<HotCollectionCacheKey, HotCollectionControl>>,
    packed_point_generation_observations: StdMutex<SmallVec<[(CommitId, u8); 4]>>,
    packed_current_base_refs: StdMutex<BTreeMap<(String, CommitId), Vec<PackedCurrentBaseRef>>>,
    commit_delta_points: crate::tracked_state::CommitDeltaPointReadCache,
}

impl HotStateTransactionCache {
    fn should_reuse_packed_points(&self, generation: CommitId) -> Result<bool, LixError> {
        let mut generations = self
            .packed_point_generation_observations
            .lock()
            .map_err(|_| hot_state_cache_lock_error())?;
        if let Some((_, observations)) = generations
            .iter_mut()
            .find(|(candidate, _)| *candidate == generation)
        {
            *observations = observations.saturating_add(1);
            return Ok(*observations >= TRANSACTION_PACKED_POINT_CACHE_MIN_OBSERVATIONS);
        }
        if generations.len() < TRANSACTION_HOT_STATE_CACHE_MAX_ENTRIES {
            generations.push((generation, 1));
        }
        Ok(false)
    }

    fn packed_current_base_refs(
        &self,
        branch_id: &str,
        generation: CommitId,
    ) -> Result<Option<Vec<PackedCurrentBaseRef>>, LixError> {
        Ok(self
            .packed_current_base_refs
            .lock()
            .map_err(|_| hot_state_cache_lock_error())?
            .get(&(branch_id.to_owned(), generation))
            .cloned())
    }

    fn remember_packed_current_base_refs(
        &self,
        branch_id: &str,
        generation: CommitId,
        refs: &[PackedCurrentBaseRef],
    ) -> Result<(), LixError> {
        let mut entries = self
            .packed_current_base_refs
            .lock()
            .map_err(|_| hot_state_cache_lock_error())?;
        if entries.len() < TRANSACTION_HOT_STATE_CACHE_MAX_ENTRIES {
            entries
                .entry((branch_id.to_owned(), generation))
                .or_insert_with(|| refs.to_vec());
        }
        Ok(())
    }

    fn collection_control(
        &self,
        key: &HotCollectionCacheKey,
    ) -> Result<Option<HotCollectionControl>, LixError> {
        Ok(self
            .collection_controls
            .lock()
            .map_err(|_| hot_state_cache_lock_error())?
            .get(key)
            .copied())
    }

    fn remember_collection_control(
        &self,
        key: HotCollectionCacheKey,
        control: HotCollectionControl,
    ) -> Result<(), LixError> {
        let mut entries = self
            .collection_controls
            .lock()
            .map_err(|_| hot_state_cache_lock_error())?;
        if entries.len() < TRANSACTION_HOT_STATE_CACHE_MAX_ENTRIES {
            entries.entry(key).or_insert(control);
        }
        Ok(())
    }
}

pub(crate) struct PackedIdentityMembership {
    cache: Arc<HotStateTransactionCache>,
    cursor: crate::tracked_state::CommitDeltaLiveMembershipCursor,
    schema_key: String,
    live_count: u64,
    ordered_identity_digest: [u8; 32],
    encoded_key: Vec<u8>,
}

impl PackedIdentityMembership {
    pub(crate) async fn contains_single_string(
        &mut self,
        store: &(impl StorageAdapterRead + ?Sized),
        row_pk: &str,
    ) -> Result<Option<bool>, LixError> {
        self.encoded_key.clear();
        let encoded = crate::tracked_state::encode_single_string_key_ref_into(
            &mut self.encoded_key,
            &self.schema_key,
            None,
            row_pk,
        );
        self.cursor
            .live_member(
                store,
                &self.cache.commit_delta_points,
                &self.encoded_key[encoded],
            )
            .await
    }

    pub(crate) fn complete_generation(&self) -> (u64, [u8; 32]) {
        (self.live_count, self.ordered_identity_digest)
    }
}

fn hot_state_cache_lock_error() -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        "transaction hot-state metadata cache lock is poisoned",
    )
}

struct PackedCollectionIncrement {
    live_count: u64,
    ordered_identity_digest: Option<[u8; 32]>,
}

fn hot_collection_control_key(
    branch_id: &str,
    branch_generation: CommitId,
    scope: crate::collection_generation::CollectionScopeRef<'_>,
) -> Vec<u8> {
    let mut key = hot_scope_prefix(branch_id, branch_generation);
    write_key_string(&mut key, scope.schema_key, KEY_PART_FINAL);
    write_file_id(&mut key, scope.file_id);
    key
}

async fn load_root_current_base_commit(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
) -> Result<Option<CommitId>, LixError> {
    let key = StorageKey(Bytes::from(hot_scope_prefix(branch_id, generation)));
    let value = PointReadPlan::new(ROOT_CURRENT_BASE_SPACE, &[key])
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .next()
        .flatten();
    let Some(value) = value else {
        return Ok(None);
    };
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(head_value_error(
            "root current-base read unexpectedly omitted its value",
        ));
    };
    if bytes.len() != 16 {
        return Err(head_value_error(
            "root current-base reference must contain one commit UUID",
        ));
    }
    Ok(Some(CommitId::new(
        uuid::Uuid::from_slice(&bytes).map_err(|error| head_value_error(error.to_string()))?,
    )))
}

async fn load_hot_collection_control(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    branch_generation: CommitId,
    scope: crate::collection_generation::CollectionScopeRef<'_>,
) -> Result<HotCollectionControl, LixError> {
    if let Some(control) =
        load_stored_hot_collection_control(store, branch_id, branch_generation, scope).await?
    {
        return Ok(control);
    }
    if let Some(base_commit_id) =
        load_root_current_base_commit(store, branch_id, branch_generation).await?
    {
        load_root_collection_control_from_base(store, base_commit_id, branch_generation, scope)
            .await
    } else {
        Ok(HotCollectionControl {
            active_generation: branch_generation,
            live_count: 0,
            ordered_identity_digest: None,
        })
    }
}

async fn load_stored_hot_collection_control(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    branch_generation: CommitId,
    scope: crate::collection_generation::CollectionScopeRef<'_>,
) -> Result<Option<HotCollectionControl>, LixError> {
    let key = StorageKey(Bytes::from(hot_collection_control_key(
        branch_id,
        branch_generation,
        scope,
    )));
    let value = PointReadPlan::new(COLLECTION_CONTROL_SPACE, &[key])
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .next()
        .flatten();
    match value {
        Some(value) => {
            let StorageProjectedValue::FullValue(bytes) = value else {
                return Err(head_value_error(
                    "hot collection-control read unexpectedly omitted its value",
                ));
            };
            storage_codec::decode("hot collection control", &bytes).map(Some)
        }
        None => Ok(None),
    }
}

async fn load_hot_collection_visibility_control(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    branch_generation: CommitId,
    scope: crate::collection_generation::CollectionScopeRef<'_>,
) -> Result<HotCollectionControl, LixError> {
    let key = StorageKey(Bytes::from(hot_collection_control_key(
        branch_id,
        branch_generation,
        scope,
    )));
    let value = PointReadPlan::new(COLLECTION_CONTROL_SPACE, &[key])
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .next()
        .flatten();
    let Some(value) = value else {
        // Visibility does not need the immutable root's exact count.
        return Ok(HotCollectionControl {
            active_generation: branch_generation,
            live_count: 1,
            ordered_identity_digest: None,
        });
    };
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(head_value_error(
            "hot collection-control visibility read unexpectedly omitted its value",
        ));
    };
    storage_codec::decode("hot collection control", &bytes)
}

async fn load_root_collection_control_from_base(
    store: &(impl StorageAdapterRead + ?Sized),
    base_commit_id: CommitId,
    branch_generation: CommitId,
    scope: crate::collection_generation::CollectionScopeRef<'_>,
) -> Result<HotCollectionControl, LixError> {
    let active_generation = load_root_active_collection_generations(store, base_commit_id, [scope])
        .await?
        .get(&(
            scope.schema_key.to_owned(),
            scope.file_id.map(str::to_owned),
        ))
        .map(|generation| generation.commit_id)
        .unwrap_or(branch_generation);
    Ok(HotCollectionControl {
        active_generation,
        live_count: DEFERRED_ROOT_LIVE_COUNT,
        ordered_identity_digest: None,
    })
}

async fn load_hot_collection_controls(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    branch_generation: CommitId,
    scopes: &[crate::collection_generation::CollectionScopeRef<'_>],
) -> Result<Vec<HotCollectionControl>, LixError> {
    if scopes.is_empty() {
        return Ok(Vec::new());
    }
    let values =
        load_stored_hot_collection_controls(store, branch_id, branch_generation, scopes).await?;
    let missing_scopes = scopes
        .iter()
        .copied()
        .zip(&values)
        .filter_map(|(scope, value)| value.is_none().then_some(scope))
        .collect::<Vec<_>>();
    let root_generations = if missing_scopes.is_empty() {
        None
    } else if let Some(base_commit_id) =
        load_root_current_base_commit(store, branch_id, branch_generation).await?
    {
        Some(
            load_root_active_collection_generations(
                store,
                base_commit_id,
                missing_scopes.iter().copied(),
            )
            .await?,
        )
    } else {
        None
    };
    let mut controls = Vec::with_capacity(scopes.len());
    for (scope, value) in scopes.iter().copied().zip(values) {
        controls.push(match (value, root_generations.as_ref()) {
            (Some(control), _) => control,
            (None, Some(generations)) => HotCollectionControl {
                active_generation: generations
                    .get(&(
                        scope.schema_key.to_owned(),
                        scope.file_id.map(str::to_owned),
                    ))
                    .map(|generation| generation.commit_id)
                    .unwrap_or(branch_generation),
                live_count: DEFERRED_ROOT_LIVE_COUNT,
                ordered_identity_digest: None,
            },
            (None, None) => HotCollectionControl {
                active_generation: branch_generation,
                live_count: 0,
                ordered_identity_digest: None,
            },
        });
    }
    Ok(controls)
}

async fn load_stored_hot_collection_controls(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    branch_generation: CommitId,
    scopes: &[crate::collection_generation::CollectionScopeRef<'_>],
) -> Result<Vec<Option<HotCollectionControl>>, LixError> {
    let keys = scopes
        .iter()
        .copied()
        .map(|scope| {
            StorageKey(Bytes::from(hot_collection_control_key(
                branch_id,
                branch_generation,
                scope,
            )))
        })
        .collect::<Vec<_>>();
    let values = PointReadPlan::new(COLLECTION_CONTROL_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value;
    values
        .into_iter()
        .map(|value| match value {
            Some(value) => {
                let StorageProjectedValue::FullValue(bytes) = value else {
                    return Err(head_value_error(
                        "hot collection-control batch read unexpectedly omitted its value",
                    ));
                };
                storage_codec::decode("hot collection control", &bytes).map(Some)
            }
            None => Ok(None),
        })
        .collect()
}

fn stage_hot_collection_control(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    branch_generation: CommitId,
    scope: crate::collection_generation::CollectionScopeRef<'_>,
    control: HotCollectionControl,
) -> Result<(), LixError> {
    writes.put(
        COLLECTION_CONTROL_SPACE,
        StorageKey(Bytes::from(hot_collection_control_key(
            branch_id,
            branch_generation,
            scope,
        ))),
        StorageValue {
            bytes: Bytes::from(storage_codec::encode("hot collection control", &control)?),
        },
    );
    Ok(())
}

async fn load_incremental_collection_controls(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    branch_generation: CommitId,
    deltas: &[&CurrentStateDeltaRef<'_>],
) -> Result<BTreeMap<(String, Option<String>), HotCollectionControl>, LixError> {
    use crate::collection_generation::{
        COLLECTION_GENERATION_SCHEMA_KEY, CollectionScopeRef, collection_scope_from_row_pk,
    };

    let mut owned_scopes = BTreeSet::<(String, Option<String>)>::new();
    for delta in deltas {
        if delta.schema_key == COLLECTION_GENERATION_SCHEMA_KEY {
            owned_scopes.insert(collection_scope_from_row_pk(delta.row_pk)?);
            continue;
        }
        owned_scopes.insert((delta.schema_key.to_string(), None));
        if let Some(file_id) = delta.file_id {
            owned_scopes.insert((delta.schema_key.to_string(), Some(file_id.to_string())));
        }
    }
    if owned_scopes.is_empty() {
        return Ok(BTreeMap::new());
    }
    let scopes = owned_scopes
        .iter()
        .map(|(schema_key, file_id)| CollectionScopeRef {
            schema_key,
            file_id: file_id.as_deref(),
        })
        .collect::<Vec<_>>();
    let controls =
        load_hot_collection_controls(store, branch_id, branch_generation, &scopes).await?;
    Ok(scopes
        .iter()
        .copied()
        .zip(controls)
        .map(|(scope, control)| {
            (
                (
                    scope.schema_key.to_string(),
                    scope.file_id.map(str::to_string),
                ),
                control,
            )
        })
        .collect())
}

fn stage_incremental_collection_controls(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    branch_generation: CommitId,
    deltas: &[&CurrentStateDeltaRef<'_>],
    previous_values: &[Option<CertifiedCurrentStatePredecessor>],
    mut controls: BTreeMap<(String, Option<String>), HotCollectionControl>,
    certified_live_increments: &BTreeMap<(String, Option<String>), u64>,
) -> Result<(), LixError> {
    use crate::collection_generation::{
        COLLECTION_GENERATION_SCHEMA_KEY, CollectionScopeRef, collection_scope_from_row_pk,
    };
    let mut dirty_scopes = BTreeSet::new();
    for (delta, previous) in deltas.iter().zip(previous_values) {
        if delta.schema_key == COLLECTION_GENERATION_SCHEMA_KEY {
            let scope = collection_scope_from_row_pk(delta.row_pk)?;
            dirty_scopes.insert(scope);
            continue;
        }

        // The compact digest certifies one untouched packed generation. Any
        // row-shaped overlay invalidates it, including a value-only update;
        // later complete-replacement proofs then use the ordinary exact scan.
        for scope in [
            Some((delta.schema_key.to_string(), None)),
            delta
                .file_id
                .map(|file_id| (delta.schema_key.to_string(), Some(file_id.to_string()))),
        ]
        .into_iter()
        .flatten()
        {
            let control = controls
                .get_mut(&scope)
                .expect("row collection scope was loaded above");
            control.ordered_identity_digest = None;
            dirty_scopes.insert(scope);
        }

        let previous_live = previous
            .as_ref()
            .map(CertifiedCurrentStatePredecessor::view)
            .transpose()?
            .is_some_and(|value| {
                !value.deleted
                    && row_belongs_to_active_collection_generation(
                        &controls,
                        branch_generation,
                        delta.schema_key,
                        delta.file_id,
                        value.untracked,
                        value.commit_id,
                    )
            });
        let belongs_to_active_generation = row_belongs_to_active_collection_generation(
            &controls,
            branch_generation,
            delta.schema_key,
            delta.file_id,
            delta.untracked,
            delta.commit_id,
        );
        let next_live = !delta.deleted && belongs_to_active_generation;
        if previous_live == next_live {
            continue;
        }
        for scope in [
            Some((delta.schema_key.to_string(), None)),
            delta
                .file_id
                .map(|file_id| (delta.schema_key.to_string(), Some(file_id.to_string()))),
        ]
        .into_iter()
        .flatten()
        {
            let control = controls
                .get_mut(&scope)
                .expect("row collection scope was loaded above");
            if control.live_count == DEFERRED_ROOT_LIVE_COUNT {
                // Keep the inherited cardinality lazy. The sparse overlay is
                // already included when collection_generation derives it.
                dirty_scopes.insert(scope);
                continue;
            }
            control.live_count = if next_live {
                control
                    .live_count
                    .checked_add(1)
                    .ok_or_else(|| head_value_error("hot collection live count exceeds u64"))?
            } else {
                control
                    .live_count
                    .checked_sub(1)
                    .ok_or_else(|| head_value_error("hot collection live count underflow"))?
            };
            dirty_scopes.insert(scope);
        }
    }

    for (scope, increment) in certified_live_increments {
        let control = controls
            .get_mut(scope)
            .expect("certified collection scope was loaded above");
        if control.live_count != DEFERRED_ROOT_LIVE_COUNT {
            control.live_count = control
                .live_count
                .checked_add(*increment)
                .ok_or_else(|| head_value_error("hot collection live count exceeds u64"))?;
        }
        control.ordered_identity_digest = None;
        dirty_scopes.insert(scope.clone());
    }

    for ((schema_key, file_id), control) in controls {
        if !dirty_scopes.contains(&(schema_key.clone(), file_id.clone())) {
            continue;
        }
        // An exact-closure scope cannot be published incrementally: the
        // ordered digest this path drops is exactly what proves absence there.
        // `restage_exact_closure_collection_control` is its single writer, so
        // staging here as well would be a duplicate mutation.
        if scope_requires_exact_closure(branch_id, &schema_key, file_id.as_deref()) {
            continue;
        }
        stage_hot_collection_control(
            writes,
            branch_id,
            branch_generation,
            CollectionScopeRef {
                schema_key: &schema_key,
                file_id: file_id.as_deref(),
            },
            control,
        )?;
    }
    Ok(())
}

fn apply_incremental_collection_generation_deltas(
    controls: &mut BTreeMap<(String, Option<String>), HotCollectionControl>,
    deltas: &[&CurrentStateDeltaRef<'_>],
) -> Result<(), LixError> {
    use crate::collection_generation::{
        COLLECTION_GENERATION_SCHEMA_KEY, collection_scope_from_row_pk,
    };

    for delta in deltas {
        if delta.schema_key != COLLECTION_GENERATION_SCHEMA_KEY {
            continue;
        }
        if delta.deleted {
            return Err(head_value_error(
                "collection-generation controls cannot be tombstoned",
            ));
        }
        let scope = collection_scope_from_row_pk(delta.row_pk)?;
        let control = controls
            .get_mut(&scope)
            .expect("collection marker target was loaded above");
        control.active_generation = delta
            .commit_id
            .ok_or_else(|| head_value_error("tracked collection-generation row lacks commit_id"))?;
        control.live_count = 0;
        control.ordered_identity_digest = None;
    }
    Ok(())
}

/// A collection-generation fence is a commit-ordered statement about tracked
/// members: a row survives it only by being newer than the retired generation.
///
/// An untracked row has no `commit_id` and is never a member of a collection
/// generation, so the fence can neither retire it nor resurrect it. Tracked
/// and untracked rows now share one serving generation, so this exemption is
/// what keeps a tracked collection replacement from silently deleting the
/// branch's history-free rows in the same schema scope.
fn survives_collection_generation_fence(
    untracked: bool,
    commit_id: Option<CommitId>,
    active_generation: CommitId,
    inclusive: bool,
) -> bool {
    if untracked {
        return true;
    }
    commit_id.is_some_and(|commit_id| {
        if inclusive {
            commit_id >= active_generation
        } else {
            commit_id > active_generation
        }
    })
}

fn row_belongs_to_active_collection_generation(
    controls: &BTreeMap<(String, Option<String>), HotCollectionControl>,
    branch_generation: CommitId,
    schema_key: &str,
    file_id: Option<&str>,
    untracked: bool,
    commit_id: Option<CommitId>,
) -> bool {
    [
        Some((schema_key.to_string(), None)),
        file_id.map(|file_id| (schema_key.to_string(), Some(file_id.to_string()))),
    ]
    .into_iter()
    .flatten()
    .all(|scope| {
        let control = controls
            .get(&scope)
            .expect("row collection scope was loaded above");
        control.active_generation == branch_generation
            || survives_collection_generation_fence(
                untracked,
                commit_id,
                control.active_generation,
                false,
            )
    })
}

/// Collections whose *absence* is proven by an authenticated identity digest
/// rather than by a point read returning nothing.
///
/// Today there is exactly one: the global branch's `lix_key_value`, which
/// `functions::state::load_key_value_row` closes over to prove that a
/// deterministic engine row is genuinely absent instead of silently missing.
/// The digest is what makes that proof resistant to a same-count,
/// different-identity substitution; a generation fence or a CAS cannot see
/// that class of corruption.
///
/// The incremental control path deliberately drops the ordered digest, because
/// it is order-dependent and cannot be maintained without rescanning. A
/// publication touching one of these scopes therefore recomputes the complete
/// control, which costs O(scope). That is why this set is kept as narrow as
/// possible: bulk untracked state lives on ordinary branches under ordinary
/// schemas and never pays it.
fn scope_requires_exact_closure(branch_id: &str, schema_key: &str, file_id: Option<&str>) -> bool {
    branch_id == crate::GLOBAL_BRANCH_ID
        && schema_key == EXACT_CLOSURE_SCHEMA_KEY
        && file_id.is_none()
}

const EXACT_CLOSURE_SCHEMA_KEY: &str = "lix_key_value";

/// Recomputes the complete collection control for an exact-closure scope from
/// its stored pre-image plus the values this publication is staging.
///
/// `staged` maps an identity in the scope to its new encoded value, or `None`
/// when the publication removes it physically.
async fn restage_exact_closure_collection_control(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    branch_id: &str,
    generation: CommitId,
    staged: &BTreeMap<HeadRowIdentity, Option<Bytes>>,
) -> Result<(), LixError> {
    let scope = crate::collection_generation::CollectionScopeRef {
        schema_key: EXACT_CLOSURE_SCHEMA_KEY,
        file_id: None,
    };
    let marker_row_pk = RowPk::single(crate::collection_generation::collection_scope_key(scope));
    let filter = TrackedStateFilter {
        schema_keys: vec![
            EXACT_CLOSURE_SCHEMA_KEY.to_owned(),
            crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY.to_owned(),
        ],
        include_tombstones: true,
        ..TrackedStateFilter::default()
    };
    let HotScanEntries::Decoded(entries) =
        hot_scan_entries(store, branch_id, generation, &filter, None, None)
            .await?
            .expect("unbounded HOT scan cannot exhaust a byte budget")
    else {
        unreachable!("an unconstrained HOT scan cannot select the finite point-read route");
    };
    let mut rows: HotRowMap = BTreeMap::new();
    for (identity, bytes) in entries {
        let identity = identity.into_row_identity();
        // Markers for *other* scopes must not enter this map: they would make
        // the complete-control pass emit a zero-count control for a scope this
        // recompute never counted.
        if identity.schema_key == crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
            && identity.row_pk != marker_row_pk
        {
            continue;
        }
        rows.insert(identity, bytes);
    }
    // The publication's own values win over the stored pre-image.
    for (identity, value) in staged {
        match value {
            Some(bytes) => {
                rows.insert(identity.clone(), bytes.clone());
            }
            None => {
                rows.remove(identity);
            }
        }
    }
    stage_complete_collection_controls(writes, branch_id, generation, &rows)
}

fn stage_complete_collection_controls(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    branch_generation: CommitId,
    rows: &HotRowMap,
) -> Result<(), LixError> {
    use crate::collection_generation::{
        COLLECTION_GENERATION_SCHEMA_KEY, CollectionScopeRef, collection_scope_from_row_pk,
    };

    let mut controls = BTreeMap::<(String, Option<String>), HotCollectionControl>::new();
    let mut physical_buckets = BTreeMap::<(String, Option<String>), Vec<&HeadRowIdentity>>::new();
    for (identity, bytes) in rows {
        if identity.schema_key == COLLECTION_GENERATION_SCHEMA_KEY {
            let target = collection_scope_from_row_pk(&identity.row_pk)?;
            let marker = decode_head_value(bytes)?;
            let active_generation = marker.commit_id.ok_or_else(|| {
                head_value_error("tracked collection-generation row lacks commit_id")
            })?;
            controls.insert(
                target,
                HotCollectionControl {
                    active_generation,
                    live_count: 0,
                    ordered_identity_digest: None,
                },
            );
            continue;
        }
        controls
            .entry((identity.schema_key.clone(), None))
            .or_insert(HotCollectionControl {
                active_generation: branch_generation,
                live_count: 0,
                ordered_identity_digest: None,
            });
        if let Some(file_id) = &identity.file_id {
            controls
                .entry((identity.schema_key.clone(), Some(file_id.clone())))
                .or_insert(HotCollectionControl {
                    active_generation: branch_generation,
                    live_count: 0,
                    ordered_identity_digest: None,
                });
        }
    }

    for (identity, bytes) in rows {
        if identity.schema_key == COLLECTION_GENERATION_SCHEMA_KEY {
            continue;
        }
        let value = decode_head_value(bytes)?;
        if value.deleted {
            continue;
        }
        let schema_scope = (identity.schema_key.clone(), None);
        let schema_control = controls
            .get(&schema_scope)
            .expect("complete row schema control was initialized above");
        let visible_after_schema_generation = schema_control.active_generation == branch_generation
            || survives_collection_generation_fence(
                value.untracked,
                value.commit_id,
                schema_control.active_generation,
                false,
            );
        let file_scope = identity
            .file_id
            .as_ref()
            .map(|file_id| (identity.schema_key.clone(), Some(file_id.clone())));
        let visible_after_file_generation = file_scope
            .as_ref()
            .and_then(|scope| controls.get(scope))
            .is_none_or(|control| {
                control.active_generation == branch_generation
                    || survives_collection_generation_fence(
                        value.untracked,
                        value.commit_id,
                        control.active_generation,
                        false,
                    )
            });
        if !visible_after_schema_generation || !visible_after_file_generation {
            continue;
        }
        physical_buckets
            .entry((identity.schema_key.clone(), identity.file_id.clone()))
            .or_default()
            .push(identity);
        for scope in [Some(schema_scope), file_scope].into_iter().flatten() {
            let control = controls
                .get_mut(&scope)
                .expect("complete row collection control was initialized above");
            control.live_count = control
                .live_count
                .checked_add(1)
                .ok_or_else(|| head_value_error("hot collection live count exceeds u64"))?;
        }
    }

    let mut digests = controls
        .keys()
        .map(|(schema_key, file_id)| {
            (
                (schema_key.clone(), file_id.clone()),
                CompleteHotCollectionDigest::new(
                    branch_id,
                    branch_generation,
                    CollectionScopeRef {
                        schema_key,
                        file_id: file_id.as_deref(),
                    },
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for ((schema_key, file_id), identities) in physical_buckets {
        for identity in identities {
            let canonical_key = encode_hot_row_key_parts(
                branch_id,
                branch_generation,
                &identity.schema_key,
                &identity.row_pk,
                identity.file_id.as_deref(),
            );
            digests
                .get_mut(&(schema_key.clone(), None))
                .expect("complete row schema digest was initialized above")
                .push(identity, &canonical_key)?;
            if file_id.is_some() {
                digests
                    .get_mut(&(schema_key.clone(), file_id.clone()))
                    .expect("complete row file digest was initialized above")
                    .push(identity, &canonical_key)?;
            }
        }
    }

    for ((schema_key, file_id), mut control) in controls {
        control.ordered_identity_digest = Some(
            digests
                .remove(&(schema_key.clone(), file_id.clone()))
                .expect("complete collection digest was initialized above")
                .finish(),
        );
        stage_hot_collection_control(
            writes,
            branch_id,
            branch_generation,
            CollectionScopeRef {
                schema_key: &schema_key,
                file_id: file_id.as_deref(),
            },
            control,
        )?;
    }
    Ok(())
}

#[derive(Clone)]
struct PackedCurrentBaseRef {
    commit_id: CommitId,
    checkpoint_commit_id: Option<CommitId>,
    /// Exact file scope when the transaction's fresh-file certificate proves
    /// this base cannot contain rows from any other file. Unscoped generic
    /// bases retain `None` and remain candidates for every filter.
    file_id: Option<String>,
    coverage_key: Bytes,
}

const PACKED_CURRENT_BASE_FILE_SCOPE_MAGIC: &[u8; 4] = b"PBF1";

fn packed_current_base_value(
    checkpoint_commit_id: Option<CommitId>,
    file_id: Option<&str>,
) -> Result<Bytes, LixError> {
    let checkpoint = checkpoint_commit_id.unwrap_or_default();
    let Some(file_id) = file_id else {
        return Ok(Bytes::copy_from_slice(checkpoint.as_uuid().as_bytes()));
    };
    let file_len = u32::try_from(file_id.len())
        .map_err(|_| head_value_error("packed current-base file id exceeds u32"))?;
    let mut value = Vec::with_capacity(4 + 16 + 4 + file_id.len());
    value.extend_from_slice(PACKED_CURRENT_BASE_FILE_SCOPE_MAGIC);
    value.extend_from_slice(checkpoint.as_uuid().as_bytes());
    value.extend_from_slice(&file_len.to_be_bytes());
    value.extend_from_slice(file_id.as_bytes());
    Ok(Bytes::from(value))
}

fn decode_packed_current_base_value(
    bytes: &[u8],
) -> Result<(Option<CommitId>, Option<String>), LixError> {
    let (checkpoint, file_id) = if bytes.len() == 16 {
        (bytes, None)
    } else {
        if bytes.len() < 24 || &bytes[..4] != PACKED_CURRENT_BASE_FILE_SCOPE_MAGIC {
            return Err(head_value_error(
                "packed current-base manifest has invalid scope metadata",
            ));
        }
        let file_len = u32::from_be_bytes(
            bytes[20..24]
                .try_into()
                .expect("packed file scope length is fixed width"),
        ) as usize;
        if bytes.len() != 24 + file_len {
            return Err(head_value_error(
                "packed current-base manifest has a truncated file scope",
            ));
        }
        let file_id = std::str::from_utf8(&bytes[24..])
            .map_err(|_| head_value_error("packed current-base file scope is not UTF-8"))?
            .to_owned();
        (&bytes[4..20], Some(file_id))
    };
    let checkpoint_uuid =
        uuid::Uuid::from_slice(checkpoint).map_err(|error| head_value_error(error.to_string()))?;
    Ok((
        (!checkpoint_uuid.is_nil()).then(|| CommitId::new(checkpoint_uuid)),
        file_id,
    ))
}

fn packed_base_matches_file_filter(
    base_ref: &PackedCurrentBaseRef,
    file_ids: &[NullableKeyFilter<String>],
) -> bool {
    let Some(file_id) = base_ref.file_id.as_deref() else {
        return true;
    };
    file_ids.is_empty()
        || file_ids.iter().any(|filter| match filter {
            NullableKeyFilter::Any => true,
            NullableKeyFilter::Null => false,
            NullableKeyFilter::Value(requested) => requested == file_id,
        })
}

struct PackedExclusiveSchemaBaseRef {
    commit_id: CommitId,
    index_key: Bytes,
}

/// One bounded authoritative HOT row layered over an immutable columnar
/// base. The identity text is encoded exactly as the hidden sidecar column so
/// execution can suppress stale base rows without parsing row keys.
#[derive(Clone, Debug)]
pub(crate) struct RowColumnarOverlayRow {
    pub(crate) row_pk: RowPk,
    pub(crate) snapshot_content: Option<Bytes>,
    pub(crate) decoded_snapshot: Option<Arc<WasmTypedRow>>,
    pub(crate) raw_snapshot: Option<Bytes>,
    pub(crate) deleted: bool,
    pub(crate) columnar_base_coordinate: Option<ColumnarBaseCoordinate>,
}

// Columnar planning temporarily overlaps encoded HOT input, its materialized
// batch, and the final typed overlay. Reserve half of one 256 MiB admission
// envelope for each adjacent representation instead of using a row-count
// policy. These are conservative admission estimates, not allocator metering;
// exceeding either half falls back to the authoritative generic row path.
const ROW_COLUMNAR_OVERLAY_INPUT_ADMISSION_BYTES: usize = 128 * 1024 * 1024;
const ROW_COLUMNAR_OVERLAY_OUTPUT_ADMISSION_BYTES: usize = 128 * 1024 * 1024;

fn materialized_columnar_overlay_admission_bytes(
    rows: &MaterializedHotStateBatch,
) -> Result<usize, LixError> {
    rows.iter().try_fold(0_usize, |bytes, row| {
        bytes
            .checked_add(size_of::<MaterializedHotStateRow>())
            .and_then(|bytes| bytes.checked_add(row.schema_key().len()))
            .and_then(|bytes| bytes.checked_add(row.file_id().map_or(0, str::len)))
            .and_then(|bytes| bytes.checked_add(row.branch_id().len()))
            .and_then(|bytes| bytes.checked_add(row.row_pk().estimated_heap_bytes()))
            .and_then(|bytes| {
                bytes.checked_add(row.snapshot_content().map_or(0, |value| value.len()))
            })
            .and_then(|bytes| {
                bytes.checked_add(row.decoded_snapshot().map_or(0, |typed| {
                    usize::try_from(typed.estimated_size()).unwrap_or(usize::MAX)
                }))
            })
            .and_then(|bytes| bytes.checked_add(row.raw_snapshot().map_or(0, Bytes::len)))
            .and_then(|bytes| bytes.checked_add(row.metadata().map_or(0, |value| value.len())))
            .ok_or_else(|| head_value_error("row columnar overlay byte size overflow"))
    })
}

fn packed_exclusive_schema_base_prefix(
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
) -> Vec<u8> {
    let mut prefix = hot_scope_prefix(branch_id, generation);
    write_key_string(&mut prefix, schema_key, KEY_PART_MORE);
    prefix
}

fn packed_exclusive_schema_base_key(
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
    commit_id: CommitId,
) -> Vec<u8> {
    let mut key = packed_exclusive_schema_base_prefix(branch_id, generation, schema_key);
    key.reserve(16);
    key.extend_from_slice(commit_id.as_uuid().as_bytes());
    key
}

async fn packed_exclusive_schema_base_refs(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
) -> Result<Vec<PackedExclusiveSchemaBaseRef>, LixError> {
    let prefix = packed_exclusive_schema_base_prefix(branch_id, generation, schema_key);
    let range = StoragePrefix {
        bytes: Bytes::copy_from_slice(&prefix),
    }
    .to_range()?;
    let mut refs = Vec::new();
    let mut cursor = store
        .begin_scan(
            PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE,
            range,
            StorageBeginScanOptions::default(),
        )
        .await?;
    loop {
        let (page, has_more) = cursor
            .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
            .await?
            .into_parts();
        for entry in page {
            let bytes = entry.key.0.as_ref();
            if bytes.len() != prefix.len() + 16 || bytes[..prefix.len()] != prefix {
                return Err(head_value_error(
                    "packed exclusive-schema base index has an invalid key",
                ));
            }
            refs.push(PackedExclusiveSchemaBaseRef {
                commit_id: CommitId::new(
                    uuid::Uuid::from_slice(&bytes[prefix.len()..])
                        .map_err(|error| head_value_error(error.to_string()))?,
                ),
                index_key: entry.key.0,
            });
        }
        if !has_more {
            break;
        }
    }
    Ok(refs)
}

fn stage_packed_exclusive_schema_base_ref(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
    commit_id: CommitId,
) {
    writes.put(
        PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE,
        StorageKey(Bytes::from(packed_exclusive_schema_base_key(
            branch_id, generation, schema_key, commit_id,
        ))),
        StorageValue {
            bytes: Bytes::from_static(&[1]),
        },
    );
}

async fn packed_current_base_refs(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
) -> Result<Vec<PackedCurrentBaseRef>, LixError> {
    let prefix = hot_scope_prefix(branch_id, generation);
    let marker = PointReadPlan::new(
        PACKED_CURRENT_BASE_CONTROL_SPACE,
        &[StorageKey(Bytes::copy_from_slice(&prefix))],
    )
    .materialize(store, StorageGetOptions::default())
    .await?
    .value
    .into_iter()
    .next()
    .flatten();
    if marker.is_none() {
        return Ok(Vec::new());
    }
    let range = StoragePrefix {
        bytes: Bytes::copy_from_slice(&prefix),
    }
    .to_range()?;
    let mut refs = Vec::new();
    let mut cursor = store
        .begin_scan(
            PACKED_CURRENT_BASE_SPACE,
            range,
            StorageBeginScanOptions::default(),
        )
        .await?;
    loop {
        let (page, page_has_more) = cursor
            .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
            .await?
            .into_parts();
        for entry in page {
            let bytes = entry.key.0.as_ref();
            if bytes.len() != prefix.len() + 16 || bytes[..prefix.len()] != prefix {
                return Err(head_value_error(
                    "packed current-base manifest has an invalid key",
                ));
            }
            let commit_id = CommitId::new(
                uuid::Uuid::from_slice(&bytes[prefix.len()..])
                    .map_err(|error| head_value_error(error.to_string()))?,
            );
            let manifest_value = full_value_bytes(entry.value)?;
            let (checkpoint_commit_id, file_id) =
                decode_packed_current_base_value(&manifest_value)?;
            refs.push(PackedCurrentBaseRef {
                commit_id,
                checkpoint_commit_id,
                file_id,
                coverage_key: entry.key.0,
            });
        }
        if !page_has_more {
            break;
        }
    }
    Ok(refs)
}

async fn stage_retire_packed_current_bases(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    branch_id: &str,
    generation: CommitId,
) -> Result<(), LixError> {
    let control_key = StorageKey(Bytes::from(hot_scope_prefix(branch_id, generation)));
    let control = PointReadPlan::new(
        PACKED_CURRENT_BASE_CONTROL_SPACE,
        std::slice::from_ref(&control_key),
    )
    .materialize(store, StorageGetOptions::default())
    .await?
    .value
    .into_iter()
    .next()
    .flatten();
    if control.is_none() {
        return Ok(());
    }
    for base_ref in packed_current_base_refs(store, branch_id, generation).await? {
        writes.delete(PACKED_CURRENT_BASE_SPACE, StorageKey(base_ref.coverage_key));
    }
    let range = StoragePrefix {
        bytes: Bytes::copy_from_slice(&control_key.0),
    }
    .to_range()?;
    let mut cursor = store
        .begin_scan(
            PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE,
            range,
            StorageBeginScanOptions::default(),
        )
        .await?;
    loop {
        let (page, page_has_more) = cursor
            .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
            .await?
            .into_parts();
        for entry in page {
            writes.delete(PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE, entry.key);
        }
        if !page_has_more {
            break;
        }
    }
    writes.delete(PACKED_CURRENT_BASE_CONTROL_SPACE, control_key);
    Ok(())
}

async fn packed_current_base_has_schema(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
) -> Result<bool, LixError> {
    for base_ref in packed_current_base_refs(store, branch_id, generation).await? {
        if crate::tracked_state::commit_delta_contains_schema(store, base_ref.commit_id, schema_key)
            .await?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn packed_member_matches_filter(
    member: &crate::tracked_state::CommitDeltaMember,
    filter: &TrackedStateFilter,
) -> bool {
    packed_identity_matches_filter(
        &member.key.schema_key,
        &member.key.row_pk,
        member.key.file_id.as_deref(),
        filter,
    )
}

fn packed_identity_matches_filter(
    schema_key: &str,
    row_pk: &RowPk,
    file_id: Option<&str>,
    filter: &TrackedStateFilter,
) -> bool {
    (filter.schema_keys.is_empty()
        || filter
            .schema_keys
            .iter()
            .any(|requested| requested == schema_key))
        && filter.matches_row_pk(row_pk)
        && (filter.file_ids.is_empty()
            || filter.file_ids.iter().any(|filter| match filter {
                NullableKeyFilter::Any => true,
                NullableKeyFilter::Null => file_id.is_none(),
                NullableKeyFilter::Value(value) => file_id == Some(value.as_str()),
            }))
}

fn packed_exact_keys_for_filter(filter: &TrackedStateFilter) -> Option<Vec<TrackedStateKey>> {
    if filter.schema_keys.is_empty() || filter.row_pks.is_empty() {
        return None;
    }
    // An absent/Any file predicate can match an unbounded set of file-scoped
    // identities for the same logical row key, so it is not an exact point
    // request. Explicit Value/Null predicates are finite and can use the
    // packed point loader directly.
    if filter.file_ids.is_empty()
        || filter
            .file_ids
            .iter()
            .any(|file_id| matches!(file_id, NullableKeyFilter::Any))
    {
        return None;
    }
    let file_ids = filter
        .file_ids
        .iter()
        .map(|file_id| match file_id {
            NullableKeyFilter::Null => None,
            NullableKeyFilter::Value(file_id) => Some(file_id.clone()),
            NullableKeyFilter::Any => unreachable!("Any returned above"),
        })
        .collect::<Vec<_>>();
    let mut keys = Vec::with_capacity(
        filter
            .schema_keys
            .len()
            .saturating_mul(filter.row_pks.len())
            .saturating_mul(file_ids.len()),
    );
    for schema_key in &filter.schema_keys {
        for row_pk in &filter.row_pks {
            // These rows already come from `filter.row_pks`; checking
            // membership again makes an exact K-key lookup O(K^2).
            // Only the independent range conjunction remains here.
            if !crate::tracked_state::row_pk_satisfies_bounds(
                row_pk,
                filter.row_pk_lower.as_ref(),
                filter.row_pk_upper.as_ref(),
            ) {
                continue;
            }
            for file_id in &file_ids {
                keys.push(TrackedStateKey {
                    schema_key: schema_key.clone(),
                    file_id: file_id.clone(),
                    row_pk: row_pk.clone(),
                });
            }
        }
    }
    keys.sort_unstable();
    keys.dedup();
    Some(keys)
}

/// A packed current base is a collection published inside the active working
/// interval, so its rows were absent when that checkpoint was taken.
fn packed_current_base_working_diff_baseline(
    active_checkpoint_commit_id: Option<CommitId>,
    base_checkpoint_commit_id: Option<CommitId>,
) -> PackedWorkingDiffBaseline {
    match active_checkpoint_commit_id {
        Some(checkpoint_commit_id) if base_checkpoint_commit_id == Some(checkpoint_commit_id) => {
            PackedWorkingDiffBaseline::AbsentAtCheckpoint {
                checkpoint_commit_id,
            }
        }
        Some(_) => PackedWorkingDiffBaseline::CleanAtCheckpoint,
        None => PackedWorkingDiffBaseline::Disabled,
    }
}

fn push_root_current_base_row(
    rows: &mut MaterializedHotStateBatchBuilder,
    row: crate::tracked_state::MaterializedTrackedStateRowRef<'_>,
    branch_id: &str,
    active_checkpoint_commit_id: Option<CommitId>,
) {
    let ordinal = rows.push_materialized_ref(
        row.row_pk(),
        row.schema_key(),
        row.file_id(),
        row.snapshot_content().cloned(),
        row.metadata().cloned(),
        row.deleted(),
        row.created_at(),
        row.updated_at(),
        branch_id == crate::GLOBAL_BRANCH_ID || row.schema_key() == "lix_account",
        Some(row.change_id()),
        Some(row.commit_id()),
        false,
        branch_id,
    );
    rows.set_decoded_snapshot(ordinal, row.decoded_snapshot().cloned());
    if let Some(snapshot) = row.decoded_snapshot() {
        rows.set_raw_snapshot(
            ordinal,
            Some(Bytes::from_owner(snapshot.durable_payload().expect(
                "materialized root row retains its validated durable payload",
            ))),
        );
    }
    rows.set_durable_predecessor(
        ordinal,
        CertifiedCurrentStatePredecessor::Packed(PackedHeadValue {
            change_id: row.change_id(),
            commit_id: row.commit_id(),
            deleted: row.deleted(),
            created_at: row.created_at(),
            updated_at: row.updated_at(),
            // The root current base *is* the branch's checkpoint state, so
            // these rows are clean at the active checkpoint. Reporting them as
            // absent made the first branch-local mutation of a checkpointed
            // identity look like a creation and gave `lix_revert` an empty
            // before image.
            working_diff_baseline: match active_checkpoint_commit_id {
                Some(_) => PackedWorkingDiffBaseline::CleanAtCheckpoint,
                None => PackedWorkingDiffBaseline::Disabled,
            },
            columnar_base_coordinate: None,
        }),
    );
}

async fn scan_root_current_base_rows(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    active_checkpoint_commit_id: Option<CommitId>,
    request: &TrackedStateScanRequest,
    root_base_cache: Option<&RootBaseBatchCache>,
) -> Result<MaterializedHotStateBatch, LixError> {
    let Some(base_commit_id) = load_root_current_base_commit(store, branch_id, generation).await?
    else {
        return Ok(MaterializedHotStateBatch::default());
    };
    // Only unbounded requests are memoized. A pushed-down LIMIT varies with the
    // number of sparse candidates that could shadow a root row, so caching by
    // it would churn the cache with near-duplicate entries for no reuse.
    let cache = root_base_cache.filter(|_| request.limit.is_none());
    let tracked = match cache.and_then(|cache| cache.get(base_commit_id, request)) {
        Some(cached) => {
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_root_base_batch_cache_hit();
            cached
        }
        None => {
            #[cfg(feature = "storage-benches")]
            if cache.is_some() {
                crate::storage_bench::record_root_base_batch_cache_miss();
            }
            let mut reader = crate::tracked_state::TrackedStateContext::new().reader(store);
            let produced = Arc::new(
                Box::pin(reader.scan_batch_at_commit(&base_commit_id.to_string(), request)).await?,
            );
            if let Some(cache) = cache {
                cache.insert(base_commit_id, request.clone(), Arc::clone(&produced));
            }
            produced
        }
    };
    // Tracked rows arrive ordered by `(schema_key, file_id, row_pk)`, so a
    // scope repeats across a contiguous run and the set this builds is the
    // number of collections in the base — one, for an ordinary collection scan.
    // Allocating the identity per row to insert a duplicate is O(base) heap
    // traffic for an O(scopes) answer, so skip a row whose scope equals the one
    // before it. Comparing borrowed `&str` is allocation-free, and the skip is
    // sound irrespective of ordering: an equal scope is already in the set.
    let mut scopes = BTreeSet::<(String, Option<String>)>::new();
    let mut previous_scope: Option<(String, Option<String>)> = None;
    for row in tracked.iter() {
        if row.schema_key() == crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY {
            continue;
        }
        if previous_scope
            .as_ref()
            .is_some_and(|(schema_key, file_id)| {
                schema_key == row.schema_key() && file_id.as_deref() == row.file_id()
            })
        {
            continue;
        }
        let scope = (
            row.schema_key().to_owned(),
            row.file_id().map(str::to_owned),
        );
        scopes.insert((scope.0.clone(), None));
        if scope.1.is_some() {
            scopes.insert(scope.clone());
        }
        previous_scope = Some(scope);
    }
    let scope_refs = scopes
        .iter()
        .map(
            |(schema_key, file_id)| crate::collection_generation::CollectionScopeRef {
                schema_key,
                file_id: file_id.as_deref(),
            },
        )
        .collect::<Vec<_>>();
    let active_generations =
        load_root_active_collection_generations(store, base_commit_id, scope_refs.iter().copied())
            .await?;
    let stored_control_values =
        load_stored_hot_collection_controls(store, branch_id, generation, &scope_refs).await?;
    let stored_controls = scopes
        .iter()
        .cloned()
        .zip(stored_control_values)
        .filter_map(|(scope, control)| control.map(|control| (scope, control)))
        .collect::<BTreeMap<_, _>>();
    let mut rows = MaterializedHotStateBatchBuilder::with_capacity(tracked.len());
    let mut scope_memo = RootScopeMemo::default();
    for row in tracked.iter() {
        if !root_tracked_row_is_active(
            row,
            generation,
            &active_generations,
            &stored_controls,
            &mut scope_memo,
        ) {
            continue;
        }
        push_root_current_base_row(&mut rows, row, branch_id, active_checkpoint_commit_id);
    }
    Ok(rows.finish())
}

async fn scan_root_current_base_rows_for_merge(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    active_checkpoint_commit_id: Option<CommitId>,
    request: &TrackedStateScanRequest,
    other_candidate_count: usize,
    root_base_cache: Option<&RootBaseBatchCache>,
) -> Result<MaterializedHotStateBatch, LixError> {
    let Some(base_commit_id) = load_root_current_base_commit(store, branch_id, generation).await?
    else {
        return Ok(MaterializedHotStateBatch::default());
    };
    let exact_scopes = (!request.filter.schema_keys.is_empty()
        && !request.filter.file_ids.is_empty()
        && request
            .filter
            .file_ids
            .iter()
            .all(|file_id| !matches!(file_id, NullableKeyFilter::Any)))
    .then(|| {
        request
            .filter
            .schema_keys
            .iter()
            .flat_map(|schema_key| {
                std::iter::once(crate::collection_generation::CollectionScopeRef {
                    schema_key,
                    file_id: None,
                })
                .chain(request.filter.file_ids.iter().filter_map(|file_id| {
                    if let NullableKeyFilter::Value(file_id) = file_id {
                        Some(crate::collection_generation::CollectionScopeRef {
                            schema_key,
                            file_id: Some(file_id),
                        })
                    } else {
                        None
                    }
                }))
            })
            .collect::<Vec<_>>()
    });
    let has_local_collection_replacement = if let Some(scopes) = exact_scopes.as_deref() {
        load_stored_hot_collection_controls(store, branch_id, generation, scopes)
            .await?
            .into_iter()
            .flatten()
            .any(|control| control.active_generation != generation)
    } else {
        let mut control_entries = Vec::new();
        if request.filter.schema_keys.is_empty() {
            let range = StoragePrefix {
                bytes: Bytes::from(hot_scope_prefix(branch_id, generation)),
            }
            .to_range()?;
            let mut cursor = store
                .begin_scan(
                    COLLECTION_CONTROL_SPACE,
                    range,
                    StorageBeginScanOptions::default(),
                )
                .await?;
            // A replacement control anywhere in the generation decides whether
            // the root scan may keep a pushed-down LIMIT. Missing one past row
            // 1024 reads as "no replacement" and can select a retired row while
            // a live row still exists further along.
            control_entries = cursor.collect_all().await?;
        } else {
            for schema_key in &request.filter.schema_keys {
                let mut prefix = hot_scope_prefix(branch_id, generation);
                write_key_string(&mut prefix, schema_key, KEY_PART_FINAL);
                let range = StoragePrefix {
                    bytes: Bytes::from(prefix),
                }
                .to_range()?;
                let mut cursor = store
                    .begin_scan(
                        COLLECTION_CONTROL_SPACE,
                        range,
                        StorageBeginScanOptions::default(),
                    )
                    .await?;
                control_entries.extend(cursor.collect_all().await?);
            }
        }
        control_entries
            .into_iter()
            .try_fold(false, |found, entry| -> Result<_, LixError> {
                let value = full_value_bytes(entry.value)?;
                let control: HotCollectionControl =
                    storage_codec::decode("hot collection control", &value)?;
                Ok(found || control.active_generation != generation)
            })?
    };
    let root_has_collection_replacement = if let Some(scopes) = exact_scopes {
        !load_root_active_collection_generations(store, base_commit_id, scopes)
            .await?
            .is_empty()
    } else {
        let mut marker_reader = crate::tracked_state::TrackedStateContext::new().reader(store);
        let root_collection_markers = Box::pin(marker_reader.scan_batch_at_commit(
            &base_commit_id.to_string(),
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    schema_keys: vec![
                        crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY.to_owned(),
                    ],
                    ..TrackedStateFilter::default()
                },
                read_columns: TrackedStateReadColumns {
                    columns: vec!["change_id".to_owned()],
                },
                limit: Some(1),
            },
        ))
        .await?;
        root_collection_markers.iter().next().is_some()
    };
    let mut root_request = request.clone();
    if has_local_collection_replacement || root_has_collection_replacement {
        root_request.limit = None;
    } else if let Some(limit) = root_request.limit.as_mut() {
        // Every sparse candidate can shadow at most one root identity. Reading
        // that many extra ordered root rows preserves the caller's final LIMIT
        // without materializing history-sized state.
        *limit = limit.saturating_add(other_candidate_count);
    }
    scan_root_current_base_rows(
        store,
        branch_id,
        generation,
        active_checkpoint_commit_id,
        &root_request,
        root_base_cache,
    )
    .await
}

async fn load_root_current_base_exact(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    active_checkpoint_commit_id: Option<CommitId>,
    keys: &[TrackedStateKeyRef<'_>],
    projection: ChangeRecordProjection,
) -> Result<MaterializedHotStateExactBatch, LixError> {
    let Some(base_commit_id) = load_root_current_base_commit(store, branch_id, generation).await?
    else {
        return MaterializedHotStateExactBatch::new(
            MaterializedHotStateBatch::default(),
            vec![None; keys.len()],
        );
    };
    let mut reader = crate::tracked_state::TrackedStateContext::new().reader(store);
    let tracked = Box::pin(reader.load_projected_batch_at_commit_refs(
        &base_commit_id.to_string(),
        keys,
        &projection,
    ))
    .await?;
    let scopes = keys
        .iter()
        .filter(|key| {
            key.schema_key != crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
        })
        .flat_map(|key| {
            [
                Some((key.schema_key.to_owned(), None)),
                key.file_id
                    .map(|file_id| (key.schema_key.to_owned(), Some(file_id.to_owned()))),
            ]
            .into_iter()
            .flatten()
        })
        .collect::<BTreeSet<_>>();
    let scope_refs = scopes
        .iter()
        .map(
            |(schema_key, file_id)| crate::collection_generation::CollectionScopeRef {
                schema_key,
                file_id: file_id.as_deref(),
            },
        )
        .collect::<Vec<_>>();
    let active_generations =
        load_root_active_collection_generations(store, base_commit_id, scope_refs.iter().copied())
            .await?;
    let stored_control_values =
        load_stored_hot_collection_controls(store, branch_id, generation, &scope_refs).await?;
    let stored_controls = scopes
        .iter()
        .cloned()
        .zip(stored_control_values)
        .filter_map(|(scope, control)| control.map(|control| (scope, control)))
        .collect::<BTreeMap<_, _>>();
    let mut rows = MaterializedHotStateBatchBuilder::with_capacity(keys.len());
    let mut slots = Vec::with_capacity(keys.len());
    let mut scope_memo = RootScopeMemo::default();
    for index in 0..tracked.len() {
        slots.push(
            tracked
                .row(index)
                .filter(|row| {
                    root_tracked_row_is_active(
                        *row,
                        generation,
                        &active_generations,
                        &stored_controls,
                        &mut scope_memo,
                    )
                })
                .map(|row| {
                    let ordinal = u32::try_from(rows.len())
                        .expect("root current-base exact result exceeds u32 rows");
                    push_root_current_base_row(
                        &mut rows,
                        row,
                        branch_id,
                        active_checkpoint_commit_id,
                    );
                    ordinal
                }),
        );
    }
    MaterializedHotStateExactBatch::new(rows.finish(), slots)
}

async fn load_root_active_collection_generations<'a>(
    store: &(impl StorageAdapterRead + ?Sized),
    base_commit_id: CommitId,
    scopes: impl IntoIterator<Item = crate::collection_generation::CollectionScopeRef<'a>>,
) -> Result<BTreeMap<(String, Option<String>), RootCollectionGeneration>, LixError> {
    let scopes = scopes
        .into_iter()
        .map(|scope| {
            (
                scope.schema_key.to_owned(),
                scope.file_id.map(str::to_owned),
            )
        })
        .collect::<BTreeSet<_>>();
    if scopes.is_empty() {
        return Ok(BTreeMap::new());
    }
    let marker_keys = scopes
        .iter()
        .map(|(schema_key, file_id)| TrackedStateKey {
            schema_key: crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY.to_owned(),
            file_id: None,
            row_pk: RowPk::single(crate::collection_generation::collection_scope_key(
                crate::collection_generation::CollectionScopeRef {
                    schema_key,
                    file_id: file_id.as_deref(),
                },
            )),
        })
        .collect::<Vec<_>>();
    let marker_refs = marker_keys
        .iter()
        .map(|key| TrackedStateKeyRef {
            schema_key: &key.schema_key,
            file_id: key.file_id.as_deref(),
            row_pk: &key.row_pk,
        })
        .collect::<Vec<_>>();
    let mut reader = crate::tracked_state::TrackedStateContext::new().reader(store);
    let markers = Box::pin(reader.load_projected_batch_at_commit_refs(
        &base_commit_id.to_string(),
        &marker_refs,
        &ChangeRecordProjection::identity_only(),
    ))
    .await?;
    Ok(scopes
        .into_iter()
        .enumerate()
        .filter_map(|(index, scope)| {
            markers.row(index).map(|row| {
                (
                    scope,
                    RootCollectionGeneration {
                        commit_id: row.commit_id(),
                        created_at: row.created_at(),
                    },
                )
            })
        })
        .collect())
}

#[derive(Clone, Copy)]
struct RootCollectionGeneration {
    commit_id: CommitId,
    created_at: LixTimestamp,
}

/// The part of [`root_tracked_row_is_active`] that depends only on the scope.
///
/// Neither the stored-control mismatch nor the collection-generation floors
/// depend on the row, and `all(|scope| row.created_at() >= scope.created_at)`
/// is one comparison against the largest floor. So the whole per-row decision
/// reduces to a flag and one timestamp compare once the scope is known.
#[derive(Clone, Copy, Default)]
struct RootScopeVerdict {
    /// A stored collection control disagrees with the root's active
    /// generation, which retires every row of the scope regardless of age.
    disqualified: bool,
    /// `max` over the scope's collection-generation `created_at` values.
    floor: Option<LixTimestamp>,
}

/// One-entry memo over [`RootScopeVerdict`].
///
/// The verdict used to be recomputed per row, and computing it needed the
/// scope as an owned `(String, Option<String>)` — two heap allocations per row
/// to probe a map that holds one entry for an ordinary collection scan. Tracked
/// rows arrive ordered by `(schema_key, file_id, row_pk)`, so remembering
/// the previous scope collapses that to one computation per scope run. The
/// memo is keyed on the full scope, so a cache hit is an exact scope match and
/// a miss simply recomputes — it cannot serve another scope's verdict even if
/// the rows were unordered.
#[derive(Default)]
struct RootScopeMemo {
    schema_key: String,
    file_id: Option<String>,
    primed: bool,
    verdict: RootScopeVerdict,
}

impl RootScopeMemo {
    fn verdict(
        &mut self,
        schema_key: &str,
        file_id: Option<&str>,
        branch_generation: CommitId,
        active_generations: &BTreeMap<(String, Option<String>), RootCollectionGeneration>,
        stored_controls: &BTreeMap<(String, Option<String>), HotCollectionControl>,
    ) -> RootScopeVerdict {
        if self.primed && self.schema_key == schema_key && self.file_id.as_deref() == file_id {
            return self.verdict;
        }
        let mut verdict = RootScopeVerdict::default();
        for scope in [
            Some((schema_key.to_owned(), None)),
            file_id.map(|file_id| (schema_key.to_owned(), Some(file_id.to_owned()))),
        ]
        .into_iter()
        .flatten()
        {
            let root_generation = active_generations
                .get(&scope)
                .map_or(branch_generation, |generation| generation.commit_id);
            if stored_controls
                .get(&scope)
                .is_some_and(|control| control.active_generation != root_generation)
            {
                verdict.disqualified = true;
            }
            if let Some(generation) = active_generations.get(&scope) {
                verdict.floor = Some(match verdict.floor {
                    Some(floor) if floor >= generation.created_at => floor,
                    _ => generation.created_at,
                });
            }
        }
        // Reuse the buffers rather than reallocating them per scope run.
        self.schema_key.clear();
        self.schema_key.push_str(schema_key);
        match file_id {
            Some(file_id) => {
                let buffer = self.file_id.get_or_insert_with(String::new);
                buffer.clear();
                buffer.push_str(file_id);
            }
            None => self.file_id = None,
        }
        self.primed = true;
        self.verdict = verdict;
        verdict
    }
}

fn root_tracked_row_is_active(
    row: crate::tracked_state::MaterializedTrackedStateRowRef<'_>,
    branch_generation: CommitId,
    active_generations: &BTreeMap<(String, Option<String>), RootCollectionGeneration>,
    stored_controls: &BTreeMap<(String, Option<String>), HotCollectionControl>,
    memo: &mut RootScopeMemo,
) -> bool {
    if row.schema_key() == crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY {
        return true;
    }
    let verdict = memo.verdict(
        row.schema_key(),
        row.file_id(),
        branch_generation,
        active_generations,
        stored_controls,
    );
    if verdict.disqualified {
        return false;
    }
    verdict.floor.is_none_or(|floor| row.created_at() >= floor)
}

fn materialize_packed_slot(include: bool, slot: Option<lix_schema::Jsonb>) -> Option<SharedStr> {
    if !include {
        return None;
    }
    slot.map(|metadata| {
        SharedStr::from(
            metadata
                .to_json_string()
                .expect("validated native JSONB metadata must serialize"),
        )
    })
}

async fn scan_packed_current_base_rows(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    request: &TrackedStateScanRequest,
    limit: Option<usize>,
) -> Result<MaterializedHotStateBatch, LixError> {
    if matches!(limit, Some(0)) {
        return Ok(MaterializedHotStateBatch::default());
    }
    let mut base_refs = packed_current_base_refs(store, branch_id, generation).await?;
    base_refs
        .retain(|base_ref| packed_base_matches_file_filter(base_ref, &request.filter.file_ids));
    if base_refs.is_empty() {
        return Ok(MaterializedHotStateBatch::default());
    }
    if request.read_columns.columns.as_slice() == ["commit_id"] {
        return scan_packed_current_base_provenance_rows(
            store, branch_id, base_refs, request, limit,
        )
        .await;
    }
    let single_base = base_refs.len() == 1;
    let mut winners = BTreeMap::new();
    let mut ordered_winners = None;
    let payload_projection = ChangeRecordProjection::from_columns(&request.read_columns.columns);
    for base_ref in base_refs {
        let scan_members_with_payloads = request.filter.schema_keys.len() == 1
            && request.filter.row_pks.is_empty()
            && request.filter.file_ids.is_empty()
            && request.limit.is_none()
            && (payload_projection.snapshot_content
                || payload_projection.snapshot
                || payload_projection.raw_snapshot);
        if scan_members_with_payloads {
            // Complete row snapshot scans need every payload in the
            // selected schema. Decode each packed segment once with its payload sidecar
            // instead of first scanning the identity/value plane and then
            // issuing a second manifest + segment pass for the same rows.
            let members =
                crate::tracked_state::load_commit_delta_members_with_payloads_for_schemas(
                    store,
                    base_ref.commit_id,
                    &request.filter.schema_keys,
                    // Not narrowed on file id: this function is reached only when
                    // `request.filter.file_ids` is empty, so there is nothing to
                    // narrow on. Revisiting that guard is what would make this
                    // call site a candidate.
                    &[],
                    // This API materializes the complete requested public
                    // result. Segment count is a physical-layout detail, not
                    // a memory budget, so it must not select a different read
                    // algorithm as collections are repartitioned.
                    usize::MAX,
                )
                .await?;
            if single_base {
                if let Some(members) = members {
                    let mut ordered = Vec::with_capacity(members.len());
                    for member in members {
                        if member.value.deleted
                            || !packed_member_matches_filter(&member, &request.filter)
                        {
                            continue;
                        }
                        ordered.push((member.key, member.value, member.change));
                    }
                    if ordered.iter().any(|row| row.0.file_id.is_some()) {
                        ordered.sort_unstable_by(|left, right| {
                            (&left.0.schema_key, &left.0.row_pk, &left.0.file_id).cmp(&(
                                &right.0.schema_key,
                                &right.0.row_pk,
                                &right.0.file_id,
                            ))
                        });
                    }
                    ordered_winners = Some(ordered);
                    break;
                }
            } else if let Some(members) = members {
                for member in members {
                    if member.value.deleted
                        || !packed_member_matches_filter(&member, &request.filter)
                    {
                        continue;
                    }
                    let key = member.key;
                    let identity = (
                        key.schema_key.clone(),
                        key.row_pk.clone(),
                        key.file_id.clone(),
                    );
                    match winners.entry(identity) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert((key, member.value, member.change));
                        }
                        std::collections::btree_map::Entry::Occupied(mut entry)
                            if entry.get().1.commit_id < member.value.commit_id =>
                        {
                            entry.insert((key, member.value, member.change));
                        }
                        std::collections::btree_map::Entry::Occupied(_) => {}
                    }
                }
                continue;
            }
        }
        let compact = crate::tracked_state::scan_commit_delta_values(
            store,
            base_ref.commit_id,
            &request.filter.schema_keys,
        )
        .await?;
        let mut keys = Vec::new();
        for row in compact.iter() {
            let key = row.key_ref();
            if row.value().deleted
                || !packed_identity_matches_filter(
                    key.schema_key,
                    key.row_pk,
                    key.file_id,
                    &request.filter,
                )
            {
                continue;
            }
            keys.push(TrackedStateKey {
                schema_key: key.schema_key.to_owned(),
                row_pk: key.row_pk.clone(),
                file_id: key.file_id.map(str::to_owned),
            });
            if single_base && limit.is_some_and(|limit| keys.len() >= limit) {
                break;
            }
        }
        let requests = keys
            .iter()
            .cloned()
            .map(|key| (base_ref.commit_id, key))
            .collect::<Vec<_>>();
        let loaded =
            crate::tracked_state::load_owned_commit_delta_entries(store, &requests).await?;
        for (key, loaded_entry) in keys.into_iter().zip(loaded) {
            let Some(loaded_entry) = loaded_entry else {
                return Err(head_value_error(
                    "packed current-base manifest lost an indexed commit member",
                ));
            };
            let identity = (
                key.schema_key.clone(),
                key.row_pk.clone(),
                key.file_id.clone(),
            );
            match winners.entry(identity) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert((key, loaded_entry.value, loaded_entry.change_record));
                }
                std::collections::btree_map::Entry::Occupied(mut entry)
                    if entry.get().1.commit_id < loaded_entry.value.commit_id =>
                {
                    entry.insert((key, loaded_entry.value, loaded_entry.change_record));
                }
                std::collections::btree_map::Entry::Occupied(_) => {}
            }
        }
    }
    let projection = ChangeRecordProjection::from_columns(&request.read_columns.columns);
    let winner_rows = ordered_winners.unwrap_or_else(|| winners.into_values().collect::<Vec<_>>());
    let row_capacity = limit.map_or(winner_rows.len(), |limit| limit.min(winner_rows.len()));
    let mut rows = MaterializedHotStateBatchBuilder::with_capacity(row_capacity);
    let global = branch_id == crate::GLOBAL_BRANCH_ID;
    for (key, value, mut change) in winner_rows.into_iter().take(row_capacity) {
        let row_index = rows.len();
        let decoded_snapshot = if projection.snapshot_content || projection.snapshot {
            change
                .snapshot
                .as_deref()
                .map(|payload| {
                    WasmTypedRow::decode_durable_payload(
                        Arc::from(payload),
                        &key.schema_key,
                        &key.row_pk,
                    )
                })
                .transpose()?
                .map(Arc::new)
        } else {
            None
        };
        let raw_snapshot = if projection.raw_snapshot {
            change.snapshot.take().map(Bytes::from)
        } else {
            None
        };
        let snapshot = if projection.snapshot_content {
            decoded_snapshot
                .as_deref()
                .map(WasmTypedRow::to_json_shared)
                .transpose()?
        } else {
            None
        };
        let metadata = materialize_packed_slot(projection.metadata, change.metadata);
        rows.push_materialized(
            key.row_pk,
            key.schema_key,
            key.file_id,
            snapshot,
            metadata,
            false,
            value.created_at,
            value.updated_at,
            global,
            Some(value.change_id),
            Some(value.commit_id),
            false,
            branch_id,
        );
        if decoded_snapshot.is_some() {
            rows.set_decoded_snapshot(row_index, decoded_snapshot);
        }
        if raw_snapshot.is_some() {
            rows.set_raw_snapshot(row_index, raw_snapshot);
        }
    }
    Ok(rows.finish())
}

/// Provenance-only scan for authenticated packed current-state bases.
///
/// The compact identity/value plane already carries the owning commit ID.
/// Loading each payload-bearing commit member again would add one point-read
/// request and decoded change record per live row even though destructive
/// reachability needs no payload. Preserve the normal winner rule while
/// decoding each packed segment once and materializing only fixed provenance.
async fn scan_packed_current_base_provenance_rows(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    base_refs: Vec<PackedCurrentBaseRef>,
    request: &TrackedStateScanRequest,
    limit: Option<usize>,
) -> Result<MaterializedHotStateBatch, LixError> {
    let mut winners = BTreeMap::new();
    for base_ref in base_refs {
        let compact = crate::tracked_state::scan_commit_delta_values(
            store,
            base_ref.commit_id,
            &request.filter.schema_keys,
        )
        .await?;
        for row in compact.iter() {
            let key = row.key_ref();
            let value = row.value();
            if value.deleted
                || !packed_identity_matches_filter(
                    key.schema_key,
                    key.row_pk,
                    key.file_id,
                    &request.filter,
                )
            {
                continue;
            }
            let identity = (
                key.schema_key.to_owned(),
                key.row_pk.clone(),
                key.file_id.map(str::to_owned),
            );
            match winners.entry(identity) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(value.clone());
                }
                std::collections::btree_map::Entry::Occupied(mut entry)
                    if entry.get().commit_id < value.commit_id =>
                {
                    entry.insert(value.clone());
                }
                std::collections::btree_map::Entry::Occupied(_) => {}
            }
        }
    }

    let row_capacity = limit.map_or(winners.len(), |limit| limit.min(winners.len()));
    let mut rows = MaterializedHotStateBatchBuilder::with_capacity(row_capacity);
    let global = branch_id == crate::GLOBAL_BRANCH_ID;
    for ((schema_key, row_pk, file_id), value) in winners.into_iter().take(row_capacity) {
        rows.push_materialized(
            row_pk,
            schema_key,
            file_id,
            None,
            None,
            false,
            value.created_at,
            value.updated_at,
            global,
            Some(value.change_id),
            Some(value.commit_id),
            false,
            branch_id,
        );
    }
    Ok(rows.finish())
}

/// Aligns already-resolved HOT overlay rows to an exact key list so the packed
/// current base is only consulted for keys it could still win.
fn packed_current_base_shadow_from_rows(
    keys: &[TrackedStateKeyRef<'_>],
    rows: &MaterializedHotStateBatch,
) -> Vec<Option<CommitId>> {
    let mut resolved = BTreeMap::<(&str, Option<&str>, &RowPk), Option<CommitId>>::new();
    for row in rows.iter() {
        let commit_id = row.commit_id();
        let slot = resolved
            .entry((row.schema_key(), row.file_id(), row.row_pk()))
            .or_insert(commit_id);
        // Keep the newest resolved commit. A row without one proves nothing
        // about the base, so it disables the skip for that key entirely.
        *slot = match (*slot, commit_id) {
            (Some(current), Some(candidate)) => Some(current.max(candidate)),
            _ => None,
        };
    }
    keys.iter()
        .map(|key| {
            resolved
                .get(&(key.schema_key, key.file_id, key.row_pk))
                .copied()
                .flatten()
        })
        .collect()
}

async fn load_packed_current_base_exact(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    active_checkpoint_commit_id: Option<CommitId>,
    keys: &[TrackedStateKeyRef<'_>],
    shadow: PackedCurrentBaseShadow<'_>,
    projection: ChangeRecordProjection,
    transaction_cache: Option<&HotStateTransactionCache>,
) -> Result<MaterializedHotStateExactBatch, LixError> {
    if keys.is_empty() {
        return MaterializedHotStateExactBatch::new(
            MaterializedHotStateBatch::default(),
            Vec::new(),
        );
    }
    let winners = load_packed_current_base_exact_entries(
        store,
        branch_id,
        generation,
        keys,
        shadow,
        transaction_cache,
    )
    .await?;

    let mut rows = MaterializedHotStateBatchBuilder::with_capacity(keys.len());
    let mut slots = Vec::with_capacity(keys.len());
    let global = branch_id == crate::GLOBAL_BRANCH_ID;
    for (key, entry) in keys.iter().zip(winners) {
        let Some((value, mut change_record, base_coordinate, base_checkpoint_commit_id)) = entry
        else {
            slots.push(None);
            continue;
        };
        if value.deleted {
            slots.push(None);
            continue;
        }
        let row_index = rows.len();
        let decoded_snapshot =
            if projection.snapshot_content || projection.snapshot || projection.raw_snapshot {
                change_record
                    .snapshot
                    .as_deref()
                    .map(|payload| {
                        WasmTypedRow::decode_durable_payload(
                            Arc::from(payload),
                            key.schema_key,
                            key.row_pk,
                        )
                    })
                    .transpose()?
                    .map(Arc::new)
            } else {
                None
            };
        let columnar_base_coordinate = base_coordinate.map(|coordinate| ColumnarBaseCoordinate {
            base_commit_id: coordinate.base_commit_id,
            group_index: coordinate.group_index,
            row_index: coordinate.row_index,
        });
        let durable_predecessor = CertifiedCurrentStatePredecessor::Packed(PackedHeadValue {
            change_id: value.change_id,
            commit_id: value.commit_id,
            deleted: false,
            created_at: value.created_at,
            updated_at: value.updated_at,
            working_diff_baseline: packed_current_base_working_diff_baseline(
                active_checkpoint_commit_id,
                base_checkpoint_commit_id,
            ),
            columnar_base_coordinate,
        });
        let snapshot = if projection.snapshot_content {
            decoded_snapshot
                .as_deref()
                .map(WasmTypedRow::to_json_shared)
                .transpose()?
        } else {
            None
        };
        let raw_snapshot = if projection.raw_snapshot {
            change_record.snapshot.take().map(Bytes::from)
        } else {
            None
        };
        let metadata = materialize_packed_slot(projection.metadata, change_record.metadata);
        slots.push(Some(u32::try_from(row_index).map_err(|_| {
            head_value_error("packed exact row count exceeds u32")
        })?));
        rows.push_materialized(
            change_record.row_pk,
            change_record.schema_key,
            change_record.file_id,
            snapshot,
            metadata,
            false,
            value.created_at,
            value.updated_at,
            global,
            Some(value.change_id),
            Some(value.commit_id),
            false,
            branch_id,
        );
        if decoded_snapshot.is_some() {
            rows.set_decoded_snapshot(row_index, decoded_snapshot);
        }
        if raw_snapshot.is_some() {
            rows.set_raw_snapshot(row_index, raw_snapshot);
        }
        rows.set_durable_predecessor(row_index, durable_predecessor);
        if let Some(coordinate) = columnar_base_coordinate {
            rows.set_columnar_base_coordinate(row_index, coordinate);
        }
    }
    MaterializedHotStateExactBatch::new(rows.finish(), slots)
}

/// Per-key commit id already resolved from a plane that shadows the packed
/// current base — in practice a branch-local HOT row. Empty means "nothing is
/// known", which reads every key from the base as before.
///
/// A packed current base published at commit `C` can only serve rows whose
/// owning commit is an ancestor of `C`, so `C` is an upper bound on every
/// `commit_id` it can produce. The caller's merge keeps a packed candidate
/// only when it is *strictly newer* than what is already resolved; a key whose
/// resolved commit id is at least the newest base ref therefore cannot change
/// its winner, and its segment never has to be fetched or decoded.
type PackedCurrentBaseShadow<'a> = &'a [Option<CommitId>];

async fn load_packed_current_base_exact_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    keys: &[TrackedStateKeyRef<'_>],
    shadow: PackedCurrentBaseShadow<'_>,
    transaction_cache: Option<&HotStateTransactionCache>,
) -> Result<
    Vec<
        Option<(
            crate::tracked_state::TrackedStateIndexValue,
            crate::changelog::ChangeRecord,
            Option<crate::tracked_state::TrackedStateBaseCoordinate>,
            Option<CommitId>,
        )>,
    >,
    LixError,
> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    debug_assert!(shadow.is_empty() || shadow.len() == keys.len());
    let transaction_cache = match transaction_cache {
        Some(cache) if cache.should_reuse_packed_points(generation)? => Some(cache),
        Some(_) | None => None,
    };
    let mut base_refs = match transaction_cache
        .map(|cache| cache.packed_current_base_refs(branch_id, generation))
        .transpose()?
        .flatten()
    {
        Some(refs) => refs,
        None => {
            let refs = packed_current_base_refs(store, branch_id, generation).await?;
            if let Some(cache) = transaction_cache {
                cache.remember_packed_current_base_refs(branch_id, generation, &refs)?;
            }
            refs
        }
    };
    base_refs.retain(|base_ref| {
        base_ref
            .file_id
            .as_deref()
            .is_none_or(|file_id| keys.iter().any(|key| key.file_id == Some(file_id)))
    });
    if base_refs.is_empty() {
        return Ok((0..keys.len()).map(|_| None).collect());
    }
    let skipped = packed_current_base_unresolved_indices(keys, shadow, &base_refs);
    let unresolved_keys;
    let selected = match skipped.as_deref() {
        Some(indices) => {
            if indices.is_empty() {
                return Ok((0..keys.len()).map(|_| None).collect());
            }
            unresolved_keys = indices.iter().map(|&index| keys[index]).collect::<Vec<_>>();
            unresolved_keys.as_slice()
        }
        None => keys,
    };
    // The base loader is boxed rather than inlined: this wrapper sits deep in an
    // already-tall async write stack, and holding the loader's state machine in
    // this frame overflows libtest's 2 MiB worker stack.
    let loaded = Box::pin(load_packed_current_base_exact_entries_from_refs(
        store,
        &base_refs,
        selected,
        transaction_cache,
    ))
    .await?;
    let Some(indices) = skipped else {
        return Ok(loaded);
    };
    let mut output = (0..keys.len()).map(|_| None).collect::<Vec<_>>();
    for (index, entry) in indices.into_iter().zip(loaded) {
        output[index] = entry;
    }
    Ok(output)
}

/// Indices of the keys the packed current base could still win, or `None` when
/// nothing is known and every key must be read.
fn packed_current_base_unresolved_indices(
    keys: &[TrackedStateKeyRef<'_>],
    shadow: PackedCurrentBaseShadow<'_>,
    base_refs: &[PackedCurrentBaseRef],
) -> Option<Vec<usize>> {
    if shadow.len() != keys.len() {
        return None;
    }
    let newest_base = base_refs.iter().map(|base_ref| base_ref.commit_id).max()?;
    let unresolved = (0..keys.len())
        .filter(|&index| shadow[index].is_none_or(|resolved| resolved < newest_base))
        .collect::<Vec<_>>();
    (unresolved.len() != keys.len()).then_some(unresolved)
}

async fn load_packed_current_base_exact_entries_from_refs(
    store: &(impl StorageAdapterRead + ?Sized),
    base_refs: &[PackedCurrentBaseRef],
    keys: &[TrackedStateKeyRef<'_>],
    transaction_cache: Option<&HotStateTransactionCache>,
) -> Result<
    Vec<
        Option<(
            crate::tracked_state::TrackedStateIndexValue,
            crate::changelog::ChangeRecord,
            Option<crate::tracked_state::TrackedStateBaseCoordinate>,
            Option<CommitId>,
        )>,
    >,
    LixError,
> {
    if let [base_ref] = base_refs {
        return Ok(
            crate::tracked_state::load_owned_commit_delta_entries_one_ordered_ref(
                store,
                base_ref.commit_id,
                keys,
                transaction_cache.map(|cache| &cache.commit_delta_points),
            )
            .await?
            .into_iter()
            .map(|entry| {
                entry.map(|entry| {
                    (
                        entry.value,
                        entry.change_record,
                        entry.base_coordinate,
                        base_ref.checkpoint_commit_id,
                    )
                })
            })
            .collect(),
        );
    }
    let owned_keys = keys
        .iter()
        .map(|key| TrackedStateKey {
            schema_key: key.schema_key.to_owned(),
            file_id: key.file_id.map(str::to_owned),
            row_pk: key.row_pk.clone(),
        })
        .collect::<Vec<_>>();
    let mut requests = Vec::with_capacity(base_refs.len().saturating_mul(keys.len()));
    for base_ref in base_refs.iter() {
        requests.extend(
            owned_keys
                .iter()
                .cloned()
                .map(|key| (base_ref.commit_id, key)),
        );
    }
    let loaded = crate::tracked_state::load_owned_commit_delta_entries(store, &requests).await?;
    let mut winners = (0..keys.len()).map(|_| None).collect::<Vec<
        Option<(
            crate::tracked_state::TrackedStateIndexValue,
            crate::changelog::ChangeRecord,
            Option<crate::tracked_state::TrackedStateBaseCoordinate>,
            Option<CommitId>,
        )>,
    >>();
    for (base_ref, entries) in base_refs.iter().zip(loaded.chunks(keys.len())) {
        for (slot, entry) in winners.iter_mut().zip(entries) {
            let Some(entry) = entry else {
                continue;
            };
            if slot
                .as_ref()
                .is_none_or(|(previous, _, _, _)| previous.commit_id < entry.value.commit_id)
            {
                *slot = Some((
                    entry.value.clone(),
                    entry.change_record.clone(),
                    entry.base_coordinate,
                    base_ref.checkpoint_commit_id,
                ));
            }
        }
    }
    Ok(winners)
}

fn compare_materialized_live_identities(
    left: &MaterializedHotStateRow,
    right: &MaterializedHotStateRow,
) -> Ordering {
    left.schema_key
        .cmp(&right.schema_key)
        .then_with(|| left.row_pk.cmp(&right.row_pk))
        .then_with(|| left.file_id.cmp(&right.file_id))
}

/// Restores the identity-order contract at the producer boundary for one
/// certified content object.
///
/// Certified packet order is plugin-defined and therefore cannot be used for
/// SQL order or LIMIT. The common already-ordered batch remains borrowed and
/// allocation-free. Only a noncanonical batch expands into owned rows for one
/// sort. Repeated identities are valid only when every materialized payload
/// byte and authority field is identical.
fn merge_ordered_live_rows(
    left: Vec<MaterializedHotStateRow>,
    right: Vec<MaterializedHotStateRow>,
) -> Vec<MaterializedHotStateRow> {
    let mut left = VecDeque::from(left);
    let mut right = VecDeque::from(right);
    let mut merged = Vec::with_capacity(left.len().saturating_add(right.len()));
    while let (Some(left_row), Some(right_row)) = (left.front(), right.front()) {
        match compare_materialized_live_identities(left_row, right_row) {
            Ordering::Less => {
                merged.push(left.pop_front().expect("peeked left row exists"));
            }
            Ordering::Greater => {
                merged.push(right.pop_front().expect("peeked right row exists"));
            }
            Ordering::Equal => {
                let left_row = left.pop_front().expect("peeked left row exists");
                let right_row = right.pop_front().expect("peeked right row exists");
                if left_row.commit_id < right_row.commit_id {
                    merged.push(right_row);
                } else {
                    merged.push(left_row);
                }
            }
        }
    }
    merged.extend(left);
    merged.extend(right);
    merged
}

fn compare_materialized_live_identity_refs(
    left: MaterializedHotStateRowRef<'_>,
    right: MaterializedHotStateRowRef<'_>,
) -> Ordering {
    left.schema_key()
        .cmp(right.schema_key())
        .then_with(|| left.row_pk().cmp(right.row_pk()))
        .then_with(|| left.file_id().cmp(&right.file_id()))
}

/// Merge two identity-ordered materialized batches without expanding their
/// dictionary and payload columns into row-owned DTOs.
fn merge_ordered_live_batches(
    left: MaterializedHotStateBatch,
    right: MaterializedHotStateBatch,
) -> MaterializedHotStateBatch {
    if left.is_empty() {
        return right;
    }
    if right.is_empty() {
        return left;
    }
    let mut merged =
        MaterializedHotStateBatchBuilder::with_capacity(left.len().saturating_add(right.len()));
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.len() && right_index < right.len() {
        let left_row = left.row(left_index);
        let right_row = right.row(right_index);
        match compare_materialized_live_identity_refs(left_row, right_row) {
            Ordering::Less => {
                merged.push_ref(left_row, None);
                left_index += 1;
            }
            Ordering::Greater => {
                merged.push_ref(right_row, None);
                right_index += 1;
            }
            Ordering::Equal => {
                if left_row.commit_id() < right_row.commit_id() {
                    merged.push_ref(right_row, None);
                } else {
                    merged.push_ref(left_row, None);
                }
                left_index += 1;
                right_index += 1;
            }
        }
    }
    while left_index < left.len() {
        merged.push_ref(left.row(left_index), None);
        left_index += 1;
    }
    while right_index < right.len() {
        merged.push_ref(right.row(right_index), None);
        right_index += 1;
    }
    merged.finish()
}

/// Removes rows whose identity is already owned by another identity-ordered
/// authority batch. Both inputs are scan results in canonical identity order,
/// so one forward cursor replaces a per-row tree lookup and owned identity.
fn exclude_ordered_live_batch_identities(
    rows: MaterializedHotStateBatch,
    authority: &MaterializedHotStateBatch,
) -> MaterializedHotStateBatch {
    if rows.is_empty() || authority.is_empty() {
        return rows;
    }
    debug_assert!((1..rows.len()).all(|index| {
        compare_materialized_live_identity_refs(rows.row(index - 1), rows.row(index)).is_lt()
    }));
    debug_assert!((1..authority.len()).all(|index| {
        compare_materialized_live_identity_refs(authority.row(index - 1), authority.row(index))
            .is_lt()
    }));
    let mut authority_index = 0usize;
    rows.filter(
        |row| loop {
            let Some(authority_row) = authority.get(authority_index) else {
                return true;
            };
            match compare_materialized_live_identity_refs(authority_row, row) {
                Ordering::Less => authority_index += 1,
                Ordering::Equal => return false,
                Ordering::Greater => return true,
            }
        },
        None,
    )
}

/// Serving cache for the materialized tracked state at a root current base.
///
/// A rotated (branch) generation does not serve reads from materialized hot
/// rows. Branch lifecycle publication writes a 16-byte reference to a commit
/// and copies nothing, so every collection scan on that branch re-derives the
/// collection from canonical records: a tracked-state tree walk, a key decode
/// per row, and a commit-delta payload fetch per owning commit. Measured, that
/// is 2.733 µs per base row against 0.686 for the same answer served from a
/// branch's own hot generation, and the branch pays it on **every** read for
/// the rest of its life because nothing ever discharges the deferral.
///
/// What makes it cacheable with no invalidation rule at all is that the
/// re-derivation is a pure function of an **immutable** commit's tracked state
/// and the request. A commit's tracked state never changes, so a hit on
/// `(base_commit_id, request)` is exact by construction — there is no revision
/// to compare and no staleness window. Everything about a rotated generation
/// that *can* change is deliberately outside what is cached: the branch's
/// stored collection controls and its working-diff checkpoint are applied by
/// [`scan_root_current_base_rows`] to the rows this returns, after the fact.
///
/// This is a disposable derived view under layout invariant 3: never an
/// authority, rebuildable by re-running the scan, and dropping it costs only
/// time. Branch creation therefore stays O(1) — nothing is materialized when a
/// branch is created — and the first read of a rotated generation pays the
/// materialization once instead of every read paying it forever.
#[derive(Default)]
pub(crate) struct RootBaseBatchCache {
    entries: std::sync::Mutex<RootBaseBatchCacheEntries>,
}

#[derive(Default)]
struct RootBaseBatchCacheEntries {
    /// Most recently used first.
    resident: Vec<RootBaseBatchCacheEntry>,
    rows: usize,
}

struct RootBaseBatchCacheEntry {
    base_commit_id: CommitId,
    request: TrackedStateScanRequest,
    batch: Arc<crate::tracked_state::MaterializedTrackedStateBatch>,
}

/// A rotated generation holds one base commit and a handful of request shapes,
/// so a small cache covers it. The row budget is the real bound; the entry
/// count only keeps the linear scan short.
const ROOT_BASE_BATCH_CACHE_MAX_ENTRIES: usize = 16;
const ROOT_BASE_BATCH_CACHE_MAX_ROWS: usize = 250_000;

impl RootBaseBatchCache {
    /// A poisoned cache lock degrades to a miss rather than an error. A serving
    /// cache must never be able to fail a read that would otherwise succeed.
    fn entries(&self) -> std::sync::MutexGuard<'_, RootBaseBatchCacheEntries> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn get(
        &self,
        base_commit_id: CommitId,
        request: &TrackedStateScanRequest,
    ) -> Option<Arc<crate::tracked_state::MaterializedTrackedStateBatch>> {
        let mut entries = self.entries();
        let index = entries.resident.iter().position(|entry| {
            entry.base_commit_id == base_commit_id && &entry.request == request
        })?;
        let entry = entries.resident.remove(index);
        let batch = Arc::clone(&entry.batch);
        entries.resident.insert(0, entry);
        Some(batch)
    }

    fn insert(
        &self,
        base_commit_id: CommitId,
        request: TrackedStateScanRequest,
        batch: Arc<crate::tracked_state::MaterializedTrackedStateBatch>,
    ) {
        let rows = batch.len();
        if rows > ROOT_BASE_BATCH_CACHE_MAX_ROWS {
            return;
        }
        let mut entries = self.entries();
        if let Some(index) = entries
            .resident
            .iter()
            .position(|entry| entry.base_commit_id == base_commit_id && entry.request == request)
        {
            let previous = entries.resident.remove(index);
            entries.rows = entries.rows.saturating_sub(previous.batch.len());
        }
        entries.resident.insert(
            0,
            RootBaseBatchCacheEntry {
                base_commit_id,
                request,
                batch,
            },
        );
        entries.rows = entries.rows.saturating_add(rows);
        while entries.resident.len() > ROOT_BASE_BATCH_CACHE_MAX_ENTRIES
            || entries.rows > ROOT_BASE_BATCH_CACHE_MAX_ROWS
        {
            let Some(evicted) = entries.resident.pop() else {
                break;
            };
            entries.rows = entries.rows.saturating_sub(evicted.batch.len());
        }
    }
}

/// Direct reader for one published hot generation.
pub(crate) struct HotStateStoreReader<S> {
    pub(super) store: S,
    pub(super) transaction_cache: Option<Arc<HotStateTransactionCache>>,
    pub(super) root_base_cache: Option<Arc<RootBaseBatchCache>>,
}

impl<S> HotStateStoreReader<S> {
    /// Attaches the engine-lifetime root-base serving cache.
    ///
    /// Readers built for write/GC paths deliberately leave this unset: they
    /// read a generation once as part of producing a write set, so caching
    /// buys nothing and only holds memory.
    pub(crate) fn with_root_base_cache(mut self, cache: Arc<RootBaseBatchCache>) -> Self {
        self.root_base_cache = Some(cache);
        self
    }
}

impl<S> HotStateStoreReader<S>
where
    S: StorageAdapterRead,
{
    pub(crate) async fn prepare_packed_identity_membership(
        &self,
        branch_id: &str,
        generation: CommitId,
        schema_key: &str,
    ) -> Result<Option<PackedIdentityMembership>, LixError> {
        let Some(cache) = self.transaction_cache.as_ref() else {
            return Ok(None);
        };
        let base_refs =
            packed_exclusive_schema_base_refs(&self.store, branch_id, generation, schema_key)
                .await?;
        let [base_ref] = base_refs.as_slice() else {
            return Ok(None);
        };
        let collection = self
            .collection_control(
                branch_id,
                generation,
                crate::collection_generation::CollectionScopeRef {
                    schema_key,
                    file_id: None,
                },
            )
            .await?;
        let Some(ordered_identity_digest) = collection.ordered_identity_digest else {
            return Ok(None);
        };
        if collection.active_generation != generation
            || collection.live_count == DEFERRED_ROOT_LIVE_COUNT
        {
            return Ok(None);
        }
        let filter = TrackedStateFilter {
            schema_keys: vec![schema_key.to_owned()],
            file_ids: vec![NullableKeyFilter::Null],
            ..TrackedStateFilter::default()
        };
        let Some(hot) =
            hot_scan_entries(&self.store, branch_id, generation, &filter, Some(1), None).await?
        else {
            return Ok(None);
        };
        let has_hot_rows = match hot {
            HotScanEntries::Decoded(rows) => !rows.is_empty(),
            HotScanEntries::Finite(batches) => batches
                .iter()
                .flat_map(|batch| batch.values.iter())
                .any(Option::is_some),
        };
        if has_hot_rows {
            return Ok(None);
        }
        let cursor = cache
            .commit_delta_points
            .live_membership_cursor(base_ref.commit_id);
        Ok(Some(PackedIdentityMembership {
            cache: Arc::clone(cache),
            cursor,
            schema_key: schema_key.to_owned(),
            live_count: collection.live_count,
            ordered_identity_digest,
            encoded_key: Vec::new(),
        }))
    }

    async fn collection_control(
        &self,
        branch_id: &str,
        generation: CommitId,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<HotCollectionControl, LixError> {
        let key = HotCollectionCacheKey {
            branch_id: branch_id.to_owned(),
            generation,
            schema_key: scope.schema_key.to_owned(),
            file_id: scope.file_id.map(str::to_owned),
        };
        if let Some(cache) = self.transaction_cache.as_deref()
            && let Some(control) = cache.collection_control(&key)?
        {
            return Ok(control);
        }
        let control =
            load_hot_collection_control(&self.store, branch_id, generation, scope).await?;
        if let Some(cache) = self.transaction_cache.as_deref() {
            cache.remember_collection_control(key, control)?;
        }
        Ok(control)
    }

    pub(crate) async fn collection_generation(
        &self,
        branch_id: &str,
        branch_generation: CommitId,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<crate::collection_generation::CollectionGeneration, LixError> {
        let control = self
            .collection_control(branch_id, branch_generation, scope)
            .await?;
        Ok(crate::collection_generation::CollectionGeneration {
            active_generation: control.active_generation,
            live_count: control.live_count,
            ordered_identity_digest: control.ordered_identity_digest,
        })
    }

    pub(crate) async fn exact_collection_live_count(
        &self,
        branch_id: &str,
        branch_generation: CommitId,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<u64, LixError> {
        let rows = Box::pin(self.scan_live_batch_for_generation(
            branch_id,
            branch_generation,
            None,
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    schema_keys: vec![scope.schema_key.to_owned()],
                    file_ids: scope.file_id.map_or_else(Vec::new, |file_id| {
                        vec![NullableKeyFilter::Value(file_id.to_owned())]
                    }),
                    ..TrackedStateFilter::default()
                },
                read_columns: TrackedStateReadColumns {
                    columns: vec!["change_id".to_owned()],
                },
                limit: None,
            },
        ))
        .await?;
        u64::try_from(rows.len())
            .map_err(|_| head_value_error("hot collection live count exceeds u64"))
    }

    /// Validates that a selected generation's complete physical member set
    /// still closes to its generation-local collection inventory.
    ///
    /// Exact point reads ordinarily treat a missing key as logical absence.
    /// Required engine authority can call this after a point miss to
    /// distinguish legitimate absence from a missing selected HOT member
    /// without introducing another locator or persisted owner.
    pub(crate) async fn validate_exact_collection_closure(
        &self,
        branch_id: &str,
        branch_generation: CommitId,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
        required_identity: TrackedStateKeyRef<'_>,
        expected_domain: HotStateReadDomain,
        allow_bootstrap_absence: bool,
    ) -> Result<(), LixError> {
        let expected_untracked = match expected_domain {
            HotStateReadDomain::Tracked => false,
            HotStateReadDomain::Untracked => true,
            HotStateReadDomain::Combined => {
                return Err(head_value_error(
                    "exact collection closure requires one explicit state domain",
                ));
            }
        };
        let control =
            load_stored_hot_collection_control(&self.store, branch_id, branch_generation, scope)
                .await?;
        if let Some(control) = control {
            if control.active_generation != branch_generation {
                return Err(head_value_error(format!(
                    "selected collection '{}' control names stale generation {} instead of {branch_generation}",
                    scope.schema_key, control.active_generation
                )));
            }
            if control.live_count == DEFERRED_ROOT_LIVE_COUNT {
                return Err(head_value_error(format!(
                    "selected collection '{}' has no exact member count",
                    scope.schema_key
                )));
            }
            if control.ordered_identity_digest.is_none() {
                return Err(head_value_error(format!(
                    "selected collection '{}' has no exact identity digest",
                    scope.schema_key
                )));
            }
        }

        let scope_prefix = hot_scope_prefix(branch_id, branch_generation);
        let mut selected_prefix = scope_prefix.clone();
        write_key_string(&mut selected_prefix, scope.schema_key, KEY_PART_FINAL);
        if let Some(file_id) = scope.file_id {
            write_file_id(&mut selected_prefix, Some(file_id));
        }
        let range = StoragePrefix {
            bytes: Bytes::from(selected_prefix),
        }
        .to_range()?;
        let mut digest = CompleteHotCollectionDigest::new(branch_id, branch_generation, scope);
        let mut actual = 0_u64;
        let mut cursor = self
            .store
            .begin_scan(ROW_SPACE, range, StorageBeginScanOptions::default())
            .await?;
        loop {
            let (page, page_has_more) = cursor
                .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
                .await?
                .into_parts();
            for entry in page {
                let raw_key = entry.key.0;
                let raw_value = full_value_bytes(entry.value)?;
                let identity = validate_exact_collection_member(
                    branch_id,
                    branch_generation,
                    &scope_prefix,
                    scope,
                    required_identity,
                    expected_untracked,
                    raw_key.as_ref(),
                    raw_value.as_ref(),
                )?;
                if let Some(identity) = identity {
                    digest.push(&identity, raw_key.as_ref())?;
                    actual = actual
                        .checked_add(1)
                        .ok_or_else(|| head_value_error("hot collection live count exceeds u64"))?;
                }
            }
            if !page_has_more {
                break;
            }
        }
        let actual_digest = digest.finish();

        let Some(control) = control else {
            if allow_bootstrap_absence && actual == 0 {
                return Ok(());
            }
            return Err(head_value_error(format!(
                "selected collection '{}' is missing its exact control",
                scope.schema_key
            )));
        };
        if actual != control.live_count {
            return Err(head_value_error(format!(
                "selected collection '{}' declares {} live members but materializes {actual}",
                scope.schema_key, control.live_count
            )));
        }
        if control.ordered_identity_digest != Some(actual_digest) {
            return Err(head_value_error(format!(
                "selected collection '{}' identity digest does not match its canonical members",
                scope.schema_key
            )));
        }
        Ok(())
    }

    pub(crate) async fn scan_live_batch_for_retention(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        request: &TrackedStateScanRequest,
        requested_untracked: Option<bool>,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        // The branch has one serving generation. Tracked and history-free
        // rows share it and are separated only by their per-row flag, so an
        // explicit retention filter is a row predicate over a single scan,
        // never a second authority domain.
        let rows = self
            .scan_live_batch_for_generation(
                branch_id,
                control.tracked_generation,
                control.working_diff_checkpoint_commit_id,
                request,
            )
            .await?;
        Ok(match requested_untracked {
            None => rows,
            Some(untracked) => rows.filter(|row| row.untracked() == untracked, None),
        })
    }

    pub(crate) async fn scan_live_batches_for_controls(
        &self,
        controls: &[(String, BranchHeadControl)],
        request: &TrackedStateScanRequest,
        requested_untracked: Option<bool>,
    ) -> Result<Vec<(String, MaterializedHotStateBatch)>, LixError> {
        let mut rows = Vec::with_capacity(controls.len());
        for (branch_id, control) in controls {
            let branch_rows = self
                .scan_live_batch_for_retention(branch_id, *control, request, requested_untracked)
                .await?;
            rows.push((branch_id.clone(), branch_rows));
        }
        Ok(rows)
    }

    /// Resolves the semantic owners named by every authenticated tracked
    /// current-serving generation.
    ///
    /// A generation UUID is branch-scoped serving state, not a commit. Route
    /// through the normal tracked reader so root-backed, packed, columnar, and
    /// native parts all apply their existing authentication and visibility
    /// rules. Only the fixed-width commit provenance column is requested; the
    /// returned set is a read-only dependency projection for destructive GC.
    pub(crate) async fn tracked_serving_commit_dependencies(
        &self,
        projections: &[(String, BranchHeadTrackedReachability)],
    ) -> Result<BTreeSet<CommitId>, LixError> {
        let request = TrackedStateScanRequest {
            filter: TrackedStateFilter::default(),
            read_columns: TrackedStateReadColumns {
                columns: vec!["commit_id".to_owned()],
            },
            limit: None,
        };
        let mut dependencies = BTreeSet::new();
        for (branch_id, projection) in projections {
            let batch = self
                .scan_live_batch_for_generation(
                    branch_id,
                    projection.serving_generation,
                    projection.serving_checkpoint_commit_id,
                    &request,
                )
                .await?;
            for row in batch.iter() {
                if row.untracked() {
                    continue;
                }
                let commit_id = row.commit_id().ok_or_else(|| {
                    head_value_error(
                        "authenticated tracked current-state row has no semantic commit owner",
                    )
                })?;
                dependencies.insert(commit_id);
            }
        }
        Ok(dependencies)
    }

    /// Candidate row primary keys whose indexed column equals any of
    /// `values`.
    ///
    /// Returns `None` when the caller must not use the index and must fall
    /// back to its ordinary scan, which happens for three reasons: the
    /// collection has no completeness witness for the generation, the witness
    /// says the plane has degraded far enough that resolving its candidates
    /// would cost more than the scan (see [`hot_index_candidate_budget`]), or
    /// the caller asked for more distinct values than
    /// [`HOT_INDEX_PROBE_VALUE_LIMIT`]. `Some` is a candidate set, not an
    /// answer: entries are never deleted within a generation, so a candidate
    /// may name a row that has since changed value or been deleted. Callers
    /// resolve candidates through the exact-row-pk read and re-apply their
    /// own predicate. The set never *omits* a live matching row.
    ///
    /// The witness read and the candidate budget are shared across `values`:
    /// the budget bounds the *total* candidate count, so a multi-value probe
    /// can never read more index entries than a single-value probe was already
    /// allowed to.
    pub(crate) async fn scan_hot_index_candidates(
        &self,
        branch_id: &str,
        generation: CommitId,
        schema_key: &str,
        ordinal: u16,
        values: &[HotIndexValue],
    ) -> Result<Option<Vec<RowPk>>, LixError> {
        if values.is_empty() || values.len() > HOT_INDEX_PROBE_VALUE_LIMIT {
            return Ok(None);
        }
        let witness = StorageKey(Bytes::from(encode_hot_index_witness_key(
            branch_id, generation, schema_key, ordinal,
        )));
        let present = PointReadPlan::new(INDEX_SPACE, &[witness])
            .materialize(&self.store, StorageGetOptions::default())
            .await?;
        let Some(StorageProjectedValue::FullValue(witness_value)) =
            present.value.into_iter().next().flatten()
        else {
            return Ok(None);
        };
        // A witness that carries no readable count cannot size the plane, and
        // an unsized plane is exactly the case this guard exists to refuse.
        // The next commit that touches the collection republishes the witness
        // with a count, so this is self-healing rather than permanent.
        let Some(entries_published) = decode_hot_index_witness(&witness_value) else {
            return Ok(None);
        };
        let budget = hot_index_candidate_budget(entries_published);
        let mut candidates = Vec::new();
        for value in values {
            let range = StoragePrefix {
                bytes: Bytes::from(hot_index_value_prefix(
                    branch_id, generation, schema_key, ordinal, value,
                )),
            }
            .to_range()?;
            let mut cursor = self
                .store
                .begin_scan(INDEX_SPACE, range, StorageBeginScanOptions::default())
                .await?;
            loop {
                // Never read more than one entry past the budget: the extra
                // entry is what proves the bucket exceeds it, and everything
                // beyond is work this route has already decided not to do.
                let want = (budget + 1 - candidates.len()).min(HOT_INDEX_CANDIDATE_PAGE);
                let (page, page_has_more) = cursor.next_page(want).await?.into_parts();
                for entry in &page {
                    let StorageProjectedValue::FullValue(value) = &entry.value else {
                        continue;
                    };
                    let text = std::str::from_utf8(value).map_err(|error| {
                        head_value_error(format!("hot index entry is not utf-8: {error}"))
                    })?;
                    candidates.push(RowPk::from_json_array_text(text).map_err(|error| {
                        head_value_error(format!("hot index entry has an invalid row pk: {error}"))
                    })?);
                }
                if candidates.len() > budget {
                    return Ok(None);
                }
                if !page_has_more {
                    break;
                }
            }
        }
        Ok(Some(candidates))
    }

    /// Candidate row primary keys whose indexed column falls in a range.
    ///
    /// The equality sibling's contract holds unchanged: `Some` is a candidate
    /// **superset**, never an answer. Entries are not deleted within a
    /// generation, so a candidate may name a row that has since changed value
    /// or been deleted, and the caller re-applies its own predicate. The set
    /// never omits a live matching row.
    ///
    /// Bounds are half-open in key space and closed in value space, so each
    /// inclusive value bound becomes an exclusive key bound one successor up.
    pub(crate) async fn scan_hot_index_range_candidates(
        &self,
        branch_id: &str,
        generation: CommitId,
        schema_key: &str,
        ordinal: u16,
        lower: Option<(&HotIndexValue, bool)>,
        upper: Option<(&HotIndexValue, bool)>,
    ) -> Result<Option<Vec<RowPk>>, LixError> {
        if lower.is_none() && upper.is_none() {
            return Ok(None);
        }
        let witness = StorageKey(Bytes::from(encode_hot_index_witness_key(
            branch_id, generation, schema_key, ordinal,
        )));
        let present = PointReadPlan::new(INDEX_SPACE, &[witness])
            .materialize(&self.store, StorageGetOptions::default())
            .await?;
        let Some(StorageProjectedValue::FullValue(witness_value)) =
            present.value.into_iter().next().flatten()
        else {
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_hot_index_probe_refused_unwitnessed();
            return Ok(None);
        };
        let Some(entries_published) = decode_hot_index_witness(&witness_value) else {
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_hot_index_probe_refused_unwitnessed();
            return Ok(None);
        };
        let budget = hot_index_candidate_budget(entries_published);

        let column_prefix = hot_index_column_prefix(branch_id, generation, schema_key, ordinal);
        let value_prefix = |value: &HotIndexValue| {
            hot_index_value_prefix(branch_id, generation, schema_key, ordinal, value)
        };
        let lower_key = match lower {
            // `>= v` starts at the value prefix, which sorts below every entry
            // carrying that value.
            Some((value, true)) => value_prefix(value),
            // `> v` must clear every entry of `v`, so it starts one successor
            // up. No successor means no key can exceed `v`, so nothing matches.
            Some((value, false)) => match hot_index_key_successor(&value_prefix(value)) {
                Some(successor) => successor,
                None => return Ok(Some(Vec::new())),
            },
            None => column_prefix.clone(),
        };
        let upper_key = match upper {
            // `<= v` must retain every entry of `v`, which all sort above the
            // value prefix — so the bound is the successor, not the prefix.
            // This is the inclusive-upper trap: bounding at the prefix drops
            // every row equal to `v` and still returns a plausible answer.
            Some((value, true)) => hot_index_key_successor(&value_prefix(value)),
            // `< v` excludes them, and the value prefix sorts below them all.
            Some((value, false)) => Some(value_prefix(value)),
            None => hot_index_key_successor(&column_prefix),
        };
        // An inverted or empty interval — `BETWEEN 8 AND -5`, or two bounds
        // that meet — names no rows. The store rejects a cursor whose lower
        // bound does not sort below its upper, so this has to be answered
        // without opening one rather than by scanning and finding nothing.
        if let Some(upper_key) = upper_key.as_ref()
            && lower_key >= *upper_key
        {
            return Ok(Some(Vec::new()));
        }
        let lower_bound = std::ops::Bound::Included(StorageKey(Bytes::from(lower_key)));
        let upper_bound = match upper_key {
            Some(upper_key) => std::ops::Bound::Excluded(StorageKey(Bytes::from(upper_key))),
            None => std::ops::Bound::Unbounded,
        };

        let mut candidates = Vec::new();
        let mut cursor = self
            .store
            .begin_scan(
                INDEX_SPACE,
                crate::storage_adapter::StorageKeyRange {
                    lower: lower_bound,
                    upper: upper_bound,
                },
                StorageBeginScanOptions::default(),
            )
            .await?;
        loop {
            let want = (budget + 1 - candidates.len()).min(HOT_INDEX_CANDIDATE_PAGE);
            let (page, page_has_more) = cursor.next_page(want).await?.into_parts();
            for entry in &page {
                let StorageProjectedValue::FullValue(value) = &entry.value else {
                    continue;
                };
                let text = std::str::from_utf8(value).map_err(|error| {
                    head_value_error(format!("hot index entry is not utf-8: {error}"))
                })?;
                candidates.push(RowPk::from_json_array_text(text).map_err(|error| {
                    head_value_error(format!("hot index entry has an invalid row pk: {error}"))
                })?);
            }
            if candidates.len() > budget {
                #[cfg(feature = "storage-benches")]
                crate::storage_bench::record_hot_index_probe_refused_over_budget();
                return Ok(None);
            }
            if !page_has_more {
                break;
            }
        }
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_hot_index_range_probe_engaged(candidates.len());
        Ok(Some(candidates))
    }

    pub(crate) async fn has_schema_rows(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        schema_key: &str,
    ) -> Result<bool, LixError> {
        let mut prefix = hot_scope_prefix(branch_id, control.tracked_generation);
        write_key_string(&mut prefix, schema_key, KEY_PART_FINAL);
        let range = StoragePrefix {
            bytes: Bytes::from(prefix),
        }
        .to_range()?;
        let mut cursor = self
            .store
            .begin_scan(
                ROW_SPACE,
                range,
                StorageBeginScanOptions {
                    projection: StorageCoreProjection::KeyOnly,
                    ..StorageBeginScanOptions::default()
                },
            )
            .await?;
        let (page, _page_has_more) = cursor.next_page(1).await?.into_parts();
        if !page.is_empty() {
            return Ok(true);
        }
        if packed_current_base_has_schema(
            &self.store,
            branch_id,
            control.tracked_generation,
            schema_key,
        )
        .await?
        {
            return Ok(true);
        }
        let root =
            if load_root_current_base_commit(&self.store, branch_id, control.tracked_generation)
                .await?
                .is_some()
            {
                Box::pin(scan_root_current_base_rows(
                    &self.store,
                    branch_id,
                    control.tracked_generation,
                    control.working_diff_checkpoint_commit_id,
                    &TrackedStateScanRequest {
                        filter: TrackedStateFilter {
                            schema_keys: vec![schema_key.to_owned()],
                            ..TrackedStateFilter::default()
                        },
                        read_columns: TrackedStateReadColumns {
                            columns: vec!["change_id".to_owned()],
                        },
                        // Root collection-generation filtering happens after the
                        // tracked-tree scan, so an early limit could select only
                        // a retired row while a later live row still exists.
                        limit: None,
                    },
                    self.root_base_cache.as_deref(),
                ))
                .await?
            } else {
                MaterializedHotStateBatch::default()
            };
        if !root.is_empty() {
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) async fn scan_row_snapshots(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        schema_key: &str,
        row_pks: &[RowPk],
        limit: Option<usize>,
    ) -> Result<Vec<Option<Bytes>>, LixError> {
        self.scan_row_snapshots_for_generation(
            branch_id,
            control.tracked_generation,
            control.working_diff_checkpoint_commit_id,
            schema_key,
            row_pks,
            limit,
        )
        .await
    }

    /// Reads one atomically published exclusive collection straight from its
    /// packed commit payload column. The publication proof excludes HOT,
    /// root, certified, and multi-base winners, so manufacturing a generic
    /// live-state batch only to discard every column except the snapshot is
    /// unnecessary read and allocation amplification.
    pub(crate) async fn scan_exclusive_row_snapshots(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        schema_key: &str,
    ) -> Result<Option<crate::tracked_state::ExclusiveRowSnapshotBatch>, LixError> {
        let Some((commit_id, live_count)) = self
            .row_columnar_base(branch_id, control, schema_key)
            .await?
        else {
            return Ok(None);
        };
        let overlay_filter = TrackedStateFilter {
            schema_keys: vec![schema_key.to_owned()],
            include_tombstones: true,
            ..TrackedStateFilter::default()
        };
        let overlay = hot_scan_entries(
            &self.store,
            branch_id,
            control.tracked_generation,
            &overlay_filter,
            Some(1),
            None,
        )
        .await?
        .expect("unbounded HOT scan cannot exhaust a byte budget");
        let overlay_is_empty = match overlay {
            HotScanEntries::Decoded(rows) => rows.is_empty(),
            HotScanEntries::Finite(batches) => batches
                .iter()
                .all(|batch| batch.values.iter().all(Option::is_none)),
        };
        if !overlay_is_empty {
            return Ok(None);
        }
        let Some(rows) =
            crate::tracked_state::load_exclusive_row_snapshots(&self.store, commit_id, schema_key)
                .await?
        else {
            return Ok(None);
        };
        let capacity = usize::try_from(live_count)
            .map_err(|_| head_value_error("exclusive row live count exceeds usize"))?;
        if rows.len() != capacity {
            return Err(head_value_error(format!(
                "exclusive row base expected {live_count} live rows, decoded {}",
                rows.len()
            )));
        }
        Ok(Some(rows))
    }

    pub(crate) async fn scan_row_primary_keys(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        schema_key: &str,
        row_pks: &[RowPk],
        limit: Option<usize>,
    ) -> Result<Vec<RowPk>, LixError> {
        if matches!(limit, Some(0)) {
            return Ok(Vec::new());
        }
        let rows = self
            .scan_live_batch_for_generation(
                branch_id,
                control.tracked_generation,
                control.working_diff_checkpoint_commit_id,
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec![schema_key.to_owned()],
                        row_pks: row_pks.to_vec(),
                        include_tombstones: false,
                        ..TrackedStateFilter::default()
                    },
                    // Retain the packed broad-scan route, which resolves the
                    // same committed winners as snapshot scans. The provider
                    // drops these bytes before Arrow conversion because only
                    // the identity columns were requested.
                    read_columns: TrackedStateReadColumns {
                        columns: vec!["snapshot_content".to_owned()],
                    },
                    limit,
                },
            )
            .await?;
        Ok(rows.into_identity_ordered_primary_keys())
    }

    /// Resolves an exact identity request through the row-addressable serving
    /// path while retaining the v69 native payload bytes for direct Arrow
    /// projection.
    pub(crate) async fn scan_native_row_snapshots(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        schema_key: &str,
        row_pks: &[RowPk],
        limit: Option<usize>,
    ) -> Result<Vec<(RowPk, Bytes)>, LixError> {
        if matches!(limit, Some(0)) {
            return Ok(Vec::new());
        }
        let rows = self
            .scan_live_batch_for_generation(
                branch_id,
                control.tracked_generation,
                control.working_diff_checkpoint_commit_id,
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec![schema_key.to_owned()],
                        row_pks: row_pks.to_vec(),
                        include_tombstones: false,
                        ..TrackedStateFilter::default()
                    },
                    read_columns: TrackedStateReadColumns {
                        columns: vec!["raw_snapshot".to_owned()],
                    },
                    limit,
                },
            )
            .await?;
        let mut snapshots = rows
            .iter()
            .map(|row| {
                let snapshot = row.raw_snapshot().cloned().ok_or_else(|| {
                    head_value_error("live v69 row has no native snapshot payload")
                })?;
                Ok((row.row_pk().clone(), snapshot))
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        snapshots.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Ok(snapshots)
    }

    /// Plans a typed columnar scan when one immutable packed base plus a
    /// bounded HOT overlay is the complete current collection and its
    /// atomically staged row-group sidecar agrees with publication control.
    pub(crate) async fn row_columnar_layout(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        schema_key: &str,
    ) -> Result<
        Option<(
            crate::columnar_row_group::RowGroupSetId,
            crate::columnar_row_group::RowGroupManifest,
            Vec<RowColumnarOverlayRow>,
            u64,
        )>,
        LixError,
    > {
        let Some((base_commit_id, live_count)) = self
            .row_columnar_base(branch_id, control, schema_key)
            .await?
        else {
            return Ok(None);
        };
        let id = crate::hot_state::row_group_set_id(base_commit_id, schema_key);
        let Some(manifest) =
            crate::columnar_row_group::load_row_group_manifest(&self.store, id).await?
        else {
            return Ok(None);
        };
        if manifest.namespace != schema_key {
            return Err(head_value_error(
                "row columnar sidecar disagrees with its collection publication",
            ));
        }

        // Read at most one bounded HOT generation. This is deliberately
        // independent of the SQL predicate: an update or tombstone that no
        // longer matches the predicate must still suppress its stale base row.
        let filter = TrackedStateFilter {
            schema_keys: vec![schema_key.to_owned()],
            include_tombstones: true,
            ..TrackedStateFilter::default()
        };
        let Some(entries) = hot_scan_entries(
            &self.store,
            branch_id,
            control.tracked_generation,
            &filter,
            None,
            Some(ROW_COLUMNAR_OVERLAY_INPUT_ADMISSION_BYTES),
        )
        .await?
        else {
            return Ok(None);
        };
        let rows = materialize_hot_scan_entries(
            &self.store,
            entries,
            ChangeRecordProjection::from_columns(&["raw_snapshot".to_owned()]),
            branch_id,
            control.working_diff_checkpoint_commit_id,
        )
        .await?;
        if materialized_columnar_overlay_admission_bytes(&rows)?
            > ROW_COLUMNAR_OVERLAY_OUTPUT_ADMISSION_BYTES
        {
            return Ok(None);
        }
        let mut overlay = Vec::with_capacity(rows.len());
        let mut overlay_bytes = 0_usize;
        for row in rows.iter() {
            // Packed columnar bases contain tracked, unfiled members only.
            // Retain the established row path for a broader identity domain.
            if row.file_id().is_some() || row.untracked() || row.global() {
                return Ok(None);
            }
            let Some(row_commit_id) = row.commit_id() else {
                return Ok(None);
            };
            // A complete packed replacement can be newer than stale HOT
            // records in the same generation. Mirror the authoritative merge:
            // equal/newer HOT wins; older HOT is ignored.
            if row_commit_id < base_commit_id {
                continue;
            }
            overlay_bytes = overlay_bytes
                .checked_add(size_of::<RowColumnarOverlayRow>())
                .and_then(|bytes| bytes.checked_add(row.row_pk().estimated_heap_bytes()))
                .and_then(|bytes| {
                    bytes.checked_add(row.snapshot_content().map_or(0, |snapshot| snapshot.len()))
                })
                .and_then(|bytes| {
                    bytes.checked_add(row.decoded_snapshot().map_or(0, |typed| {
                        usize::try_from(typed.estimated_size()).unwrap_or(usize::MAX)
                    }))
                })
                .and_then(|bytes| bytes.checked_add(row.raw_snapshot().map_or(0, Bytes::len)))
                .ok_or_else(|| head_value_error("row columnar overlay byte size overflow"))?;
            if overlay_bytes > ROW_COLUMNAR_OVERLAY_OUTPUT_ADMISSION_BYTES {
                return Ok(None);
            }
            overlay.push(RowColumnarOverlayRow {
                row_pk: row.row_pk().clone(),
                snapshot_content: row
                    .snapshot_content()
                    .map(|snapshot| Bytes::copy_from_slice(snapshot.as_bytes())),
                decoded_snapshot: row.decoded_snapshot().cloned(),
                raw_snapshot: row.raw_snapshot().cloned(),
                deleted: row.deleted(),
                columnar_base_coordinate: row.columnar_base_coordinate(),
            });
        }
        Ok(Some((id, manifest, overlay, live_count)))
    }

    async fn row_columnar_base(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        schema_key: &str,
    ) -> Result<Option<(CommitId, u64)>, LixError> {
        let collection = load_hot_collection_control(
            &self.store,
            branch_id,
            control.tracked_generation,
            crate::collection_generation::CollectionScopeRef {
                schema_key,
                file_id: None,
            },
        )
        .await?;
        if collection.active_generation != control.tracked_generation {
            return Ok(None);
        }
        let base_refs = packed_exclusive_schema_base_refs(
            &self.store,
            branch_id,
            control.tracked_generation,
            schema_key,
        )
        .await?;
        let [base_ref] = base_refs.as_slice() else {
            return Ok(None);
        };
        let active_base_refs =
            packed_current_base_refs(&self.store, branch_id, control.tracked_generation).await?;
        if !active_base_refs
            .iter()
            .any(|active| active.commit_id == base_ref.commit_id)
        {
            return Err(head_value_error(
                "exclusive schema index references an inactive packed current base",
            ));
        }
        for active in active_base_refs
            .iter()
            .filter(|active| active.commit_id != base_ref.commit_id)
        {
            if crate::tracked_state::commit_delta_contains_schema(
                &self.store,
                active.commit_id,
                schema_key,
            )
            .await?
            {
                return Ok(None);
            }
        }
        Ok(Some((base_ref.commit_id, collection.live_count)))
    }

    pub(crate) async fn scan_tracked_tombstones_for_control(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
    ) -> Result<Vec<MaterializedHotStateRow>, LixError> {
        let filter = TrackedStateFilter {
            include_tombstones: true,
            ..TrackedStateFilter::default()
        };
        let HotScanEntries::Decoded(entries) = hot_scan_entries(
            &self.store,
            branch_id,
            control.tracked_generation,
            &filter,
            None,
            None,
        )
        .await?
        .expect("unbounded HOT scan cannot exhaust a byte budget") else {
            unreachable!("an unconstrained HOT scan cannot select the finite point-read route");
        };
        let mut tombstones = Vec::new();
        for (identity, bytes) in entries {
            let value = decode_head_value(&bytes)?;
            if value.deleted && !value.untracked {
                tombstones.push((identity, bytes));
            }
        }
        Ok(materialize_live_entries(
            &self.store,
            tombstones,
            ChangeRecordProjection::identity_only(),
            branch_id,
            control.working_diff_checkpoint_commit_id,
        )
        .await?
        .into_rows())
    }

    #[cfg(test)]
    pub(crate) async fn scan_live_rows_if_current(
        &self,
        branch_id: &str,
        expected_head: &str,
        request: &TrackedStateScanRequest,
    ) -> Result<Option<Vec<MaterializedHotStateRow>>, LixError> {
        let expected_head = CommitId::parse_lix(expected_head, "hot-state expected commit")?;
        let control = BranchHeadControlContext::new()
            .reader(&self.store)
            .load(branch_id)
            .await?;
        let Some(control) = control.filter(|control| control.head_commit_id == expected_head)
        else {
            return Ok(None);
        };
        Ok(Some(
            self.scan_live_batch_for_generation(
                branch_id,
                control.tracked_generation,
                control.working_diff_checkpoint_commit_id,
                request,
            )
            .await?
            .into_rows(),
        ))
    }

    async fn scan_live_batch_for_generation(
        &self,
        branch_id: &str,
        generation: CommitId,
        active_checkpoint_commit_id: Option<CommitId>,
        request: &TrackedStateScanRequest,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        self.scan_live_batch_for_generation_with_visibility(
            branch_id,
            generation,
            active_checkpoint_commit_id,
            request,
            true,
        )
        .await
    }

    async fn scan_live_batch_for_generation_with_visibility(
        &self,
        branch_id: &str,
        generation: CommitId,
        active_checkpoint_commit_id: Option<CommitId>,
        request: &TrackedStateScanRequest,
        apply_collection_visibility: bool,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        let collection_control = if apply_collection_visibility {
            match request.filter.schema_keys.as_slice() {
                [schema_key]
                    if schema_key
                        != crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY =>
                {
                    Some(
                        load_hot_collection_visibility_control(
                            &self.store,
                            branch_id,
                            generation,
                            crate::collection_generation::CollectionScopeRef {
                                schema_key,
                                file_id: None,
                            },
                        )
                        .await?,
                    )
                }
                _ => None,
            }
        } else {
            None
        };
        let replaced_generation =
            collection_control.filter(|control| control.active_generation != generation);
        if replaced_generation.is_some_and(|control| control.live_count == 0) {
            return Ok(MaterializedHotStateBatch::default());
        }
        // A storage prefix is ordered by identity, but tombstones are filtered
        // only after decoding the value. Applying SQL LIMIT to the raw scan
        // would therefore let one tombstone hide a later live row.
        let mut entries = hot_scan_entries(
            &self.store,
            branch_id,
            generation,
            &request.filter,
            None,
            None,
        )
        .await?
        .expect("unbounded HOT scan cannot exhaust a byte budget");
        if let Some(control) = replaced_generation {
            filter_hot_scan_entries_by_collection_generation(&mut entries, control)?;
        }
        let projection = ChangeRecordProjection::from_columns(&request.read_columns.columns);
        let rows = materialize_hot_scan_entries(
            &self.store,
            entries,
            projection,
            branch_id,
            active_checkpoint_commit_id,
        )
        .await?;
        let rows = rows.filter(
            |row| {
                replaced_generation.is_none_or(|control| {
                    survives_collection_generation_fence(
                        row.untracked(),
                        row.commit_id(),
                        control.active_generation,
                        false,
                    )
                })
            },
            None,
        );
        let has_overlay_rows = !rows.is_empty();
        let packed_limit = if !has_overlay_rows && replaced_generation.is_none() {
            request.limit.map(|limit| limit.saturating_sub(rows.len()))
        } else {
            None
        };
        let packed_rows = if let Some(keys) = packed_exact_keys_for_filter(&request.filter) {
            let key_refs = keys
                .iter()
                .map(|key| TrackedStateKeyRef {
                    schema_key: &key.schema_key,
                    file_id: key.file_id.as_deref(),
                    row_pk: &key.row_pk,
                })
                .collect::<Vec<_>>();
            let shadow = packed_current_base_shadow_from_rows(&key_refs, &rows);
            load_packed_current_base_exact(
                &self.store,
                branch_id,
                generation,
                active_checkpoint_commit_id,
                &key_refs,
                &shadow,
                projection,
                self.transaction_cache.as_deref(),
            )
            .await?
            .into_present_batch()
            .filter(|_| true, packed_limit)
        } else {
            scan_packed_current_base_rows(&self.store, branch_id, generation, request, packed_limit)
                .await?
        };
        // A pristine root-backed generation has no possible shadowing winner,
        // so preserve bounded-read behavior by pushing LIMIT into the tracked
        // tree. Once any collection control or overlay/base exists, select all
        // winners first and apply the limit to the final merged batch.
        let root_rows = Box::pin(scan_root_current_base_rows_for_merge(
            &self.store,
            branch_id,
            generation,
            active_checkpoint_commit_id,
            request,
            rows.len().saturating_add(packed_rows.len()),
            self.root_base_cache.as_deref(),
        ))
        .await?;
        let combined = merge_ordered_live_batches(rows, packed_rows);
        let rows = merge_ordered_live_batches(combined, root_rows);
        if request.filter.include_tombstones
            && request.limit.is_none()
            && replaced_generation.is_none()
        {
            return Ok(rows);
        }
        Ok(rows.filter(
            |row| request.filter.include_tombstones || !row.deleted(),
            request.limit,
        ))
    }

    #[cfg(test)]
    pub(crate) async fn load_projected_live_rows_if_current(
        &self,
        branch_id: &str,
        expected_head: &str,
        keys: &[TrackedStateKey],
        projection: &ChangeRecordProjection,
    ) -> Result<Option<Vec<Option<MaterializedHotStateRow>>>, LixError> {
        let expected_head = CommitId::parse_lix(expected_head, "hot-state expected commit")?;
        let control = BranchHeadControlContext::new()
            .reader(&self.store)
            .load(branch_id)
            .await?;
        let Some(control) = control.filter(|control| control.head_commit_id == expected_head)
        else {
            return Ok(None);
        };
        Ok(Some(
            self.load_projected_live_batch(branch_id, control, keys, projection)
                .await?
                .into_rows(),
        ))
    }

    pub(crate) async fn load_projected_live_rows(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        keys: &[TrackedStateKey],
        projection: &ChangeRecordProjection,
    ) -> Result<Vec<Option<MaterializedHotStateRow>>, LixError> {
        self.load_projected_live_batch(branch_id, control, keys, projection)
            .await
            .map(MaterializedHotStateExactBatch::into_rows)
    }

    pub(crate) async fn load_projected_live_batch(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        keys: &[TrackedStateKey],
        projection: &ChangeRecordProjection,
    ) -> Result<MaterializedHotStateExactBatch, LixError> {
        let keys = keys
            .iter()
            .map(|key| TrackedStateKeyRef {
                schema_key: key.schema_key.as_str(),
                file_id: key.file_id.as_deref(),
                row_pk: &key.row_pk,
            })
            .collect::<Vec<_>>();
        self.load_projected_live_batch_refs(branch_id, control, &keys, projection)
            .await
    }

    pub(crate) async fn load_projected_live_batch_refs(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        keys: &[TrackedStateKeyRef<'_>],
        projection: &ChangeRecordProjection,
    ) -> Result<MaterializedHotStateExactBatch, LixError> {
        self.load_projected_live_batch_refs_for_domain(
            branch_id,
            control,
            keys,
            projection,
            HotStateReadDomain::Combined,
        )
        .await
    }

    pub(crate) async fn load_projected_live_batch_refs_for_domain(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        keys: &[TrackedStateKeyRef<'_>],
        projection: &ChangeRecordProjection,
        domain: HotStateReadDomain,
    ) -> Result<MaterializedHotStateExactBatch, LixError> {
        // One serving generation holds at most one row per identity, so the
        // read domain is a predicate on the row that was found rather than a
        // precedence rule between two roots.
        let rows = Box::pin(self.load_projected_live_batch_for_generation_refs(
            branch_id,
            control.tracked_generation,
            control.working_diff_checkpoint_commit_id,
            keys,
            projection,
        ))
        .await?;
        match domain {
            HotStateReadDomain::Combined => Ok(rows),
            HotStateReadDomain::Tracked => rows.filter(|row| !row.untracked()),
            HotStateReadDomain::Untracked => rows.filter(|row| row.untracked()),
        }
    }

    async fn load_projected_live_batch_for_generation_refs(
        &self,
        branch_id: &str,
        generation: CommitId,
        active_checkpoint_commit_id: Option<CommitId>,
        keys: &[TrackedStateKeyRef<'_>],
        projection: &ChangeRecordProjection,
    ) -> Result<MaterializedHotStateExactBatch, LixError> {
        self.load_projected_live_batch_for_generation_refs_with_visibility(
            branch_id,
            generation,
            active_checkpoint_commit_id,
            keys,
            projection,
            true,
        )
        .await
    }

    async fn load_projected_live_batch_for_generation_refs_with_visibility(
        &self,
        branch_id: &str,
        generation: CommitId,
        active_checkpoint_commit_id: Option<CommitId>,
        keys: &[TrackedStateKeyRef<'_>],
        projection: &ChangeRecordProjection,
        apply_collection_visibility: bool,
    ) -> Result<MaterializedHotStateExactBatch, LixError> {
        if keys.is_empty() {
            return Ok(MaterializedHotStateExactBatch::default());
        }
        let replaced_generation = apply_collection_visibility
            .then(|| {
                keys.first()
                    .filter(|first| keys.iter().all(|key| key.schema_key == first.schema_key))
                    .filter(|first| {
                        first.schema_key
                            != crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
                    })
                    .map(|first| async {
                        load_hot_collection_visibility_control(
                            &self.store,
                            branch_id,
                            generation,
                            crate::collection_generation::CollectionScopeRef {
                                schema_key: first.schema_key,
                                file_id: None,
                            },
                        )
                        .await
                    })
            })
            .flatten();
        let replaced_generation = match replaced_generation {
            Some(control) => {
                let control = control.await?;
                (control.active_generation != generation).then_some(control)
            }
            None => None,
        };
        if replaced_generation.is_some_and(|control| control.live_count == 0) {
            return MaterializedHotStateExactBatch::new(
                MaterializedHotStateBatch::default(),
                vec![None; keys.len()],
            );
        }
        let mut values =
            hot_load_identity_ref_bytes(&self.store, branch_id, generation, keys).await?;
        if let Some(control) = replaced_generation {
            for value in &mut values {
                let visible = value
                    .as_deref()
                    .map(decode_head_value)
                    .transpose()?
                    .is_some_and(|value| {
                        survives_collection_generation_fence(
                            value.untracked,
                            value.commit_id,
                            control.active_generation,
                            false,
                        )
                    });
                if !visible {
                    *value = None;
                }
            }
        }
        let mut slots = Vec::with_capacity(values.len());
        let mut entries = Vec::with_capacity(values.iter().flatten().count());
        for (identity, value) in keys.iter().copied().zip(values) {
            slots.push(value.map(|value| {
                let ordinal =
                    u32::try_from(entries.len()).expect("live-state exact batch exceeds u32 rows");
                entries.push((identity, value));
                ordinal
            }));
        }
        let rows = materialize_live_entries(
            &self.store,
            entries,
            *projection,
            branch_id,
            active_checkpoint_commit_id,
        )
        .await?;

        // Certified immutable segments deliberately do not manufacture one
        // HOT_ROW entry per semantic row. Exact validation reads still need
        // to observe those identities—for example, a sparse plugin successor
        // proving that a keyed update or foreign-key target already exists.
        // Decode the matching segment rows only when the ordinary point-read
        // index misses, then preserve the original request alignment.
        let packed_shadow = slots
            .iter()
            .map(|slot| {
                slot.and_then(|slot| rows.get(slot as usize))
                    .and_then(|row| row.commit_id())
            })
            .collect::<Vec<_>>();
        let packed = load_packed_current_base_exact(
            &self.store,
            branch_id,
            generation,
            active_checkpoint_commit_id,
            keys,
            &packed_shadow,
            *projection,
            self.transaction_cache.as_deref(),
        )
        .await?;
        let root_backed = load_root_current_base_commit(&self.store, branch_id, generation)
            .await?
            .is_some();
        let root = if root_backed {
            Box::pin(load_root_current_base_exact(
                &self.store,
                branch_id,
                generation,
                active_checkpoint_commit_id,
                keys,
                *projection,
            ))
            .await?
        } else {
            MaterializedHotStateExactBatch::new(
                MaterializedHotStateBatch::default(),
                vec![None; keys.len()],
            )?
        };
        let mut resolved = Vec::with_capacity(keys.len());
        for (index, slot) in slots.into_iter().enumerate() {
            let mut row = slot.and_then(|slot| rows.get(slot as usize));
            for candidate in [packed.row(index), root.row(index)].into_iter().flatten() {
                if row.is_none_or(
                    |current| match (current.commit_id(), candidate.commit_id()) {
                        (Some(current), Some(candidate)) => candidate > current,
                        (None, Some(_)) => false,
                        (Some(_), None) => false,
                        (None, None) => false,
                    },
                ) {
                    row = Some(candidate);
                }
            }
            resolved.push(row.filter(|row| {
                replaced_generation.is_none_or(|control| {
                    survives_collection_generation_fence(
                        row.untracked(),
                        row.commit_id(),
                        control.active_generation,
                        true,
                    )
                })
            }));
        }
        let mut builder = MaterializedHotStateBatchBuilder::with_capacity(keys.len());
        let mut combined_slots = Vec::with_capacity(keys.len());
        for row in resolved {
            let row = row.filter(|row| {
                replaced_generation.is_none_or(|control| {
                    survives_collection_generation_fence(
                        row.untracked(),
                        row.commit_id(),
                        control.active_generation,
                        true,
                    )
                })
            });
            combined_slots.push(
                row.map(|row| {
                    u32::try_from(builder.push_ref(row, None)).map_err(|_| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "exact live-state result exceeds u32 rows",
                        )
                    })
                })
                .transpose()?,
            );
        }
        MaterializedHotStateExactBatch::new(builder.finish(), combined_slots)
    }

    pub(crate) async fn working_diff_epoch(
        &self,
        branch_id: &str,
    ) -> Result<Option<TrackedWorkingDiffEpoch>, LixError> {
        load_tracked_working_diff_epoch(&self.store, branch_id).await
    }

    #[cfg(test)]
    pub(crate) async fn untracked_json_refs(
        &self,
        controls: &[(String, BranchHeadControl)],
    ) -> Result<Vec<JsonRef>, LixError> {
        let mut refs = BTreeSet::new();
        self.collect_hot_json_refs(controls, true, &mut refs)
            .await?;
        Ok(refs.into_iter().map(JsonRef::from_hash_bytes).collect())
    }

    /// Collects the out-of-band JSON payload refs the published hot generation
    /// of every live branch names.
    ///
    /// `untracked_only` selects the *authority* subset: an untracked row exists
    /// nowhere else, so it is the only owner of its payload. Repository GC
    /// deliberately passes `false` and takes the tracked rows too. Those rows
    /// are a derived cache and their payloads are also named by a retained
    /// commit, so including them cannot change which payloads are provably
    /// dead — but a serving read materializes them straight out of this plane,
    /// so a ref here that no longer resolves is a read failure, and the cost of
    /// being wrong about the argument is unrecoverable.
    pub(crate) async fn collect_hot_json_refs(
        &self,
        _controls: &[(String, BranchHeadControl)],
        _untracked_only: bool,
        _refs: &mut BTreeSet<[u8; JSON_REF_BYTES]>,
    ) -> Result<(), LixError> {
        Ok(())
    }

    pub(crate) async fn working_diff_for_control(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        request: &TrackedStateDiffRequest,
    ) -> Result<Option<TrackedWorkingDiff>, LixError> {
        let Ok(Some(epoch)) = self.working_diff_epoch(branch_id).await else {
            return Ok(None);
        };
        let generation = epoch.generation;
        if generation != control.tracked_generation
            || control.working_diff_checkpoint_commit_id != Some(epoch.checkpoint_commit_id)
        {
            return Ok(None);
        }
        let Some(entries) = hot_working_diff_entries(
            &self.store,
            branch_id,
            epoch.checkpoint_commit_id,
            generation,
            epoch.coverage,
            &request.filter,
        )
        .await?
        else {
            return Ok(None);
        };
        Ok(Some(TrackedWorkingDiff {
            checkpoint_commit_id: epoch.checkpoint_commit_id,
            diff: TrackedStateDiff::from_entries(entries),
        }))
    }

    async fn scan_row_snapshots_for_generation(
        &self,
        branch_id: &str,
        generation: CommitId,
        active_checkpoint_commit_id: Option<CommitId>,
        schema_key: &str,
        row_pks: &[RowPk],
        limit: Option<usize>,
    ) -> Result<Vec<Option<Bytes>>, LixError> {
        if matches!(limit, Some(0)) {
            return Ok(Vec::new());
        }
        let rows = self
            .scan_live_batch_for_generation(
                branch_id,
                generation,
                active_checkpoint_commit_id,
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec![schema_key.to_owned()],
                        row_pks: row_pks.to_vec(),
                        include_tombstones: false,
                        ..TrackedStateFilter::default()
                    },
                    read_columns: TrackedStateReadColumns {
                        columns: vec!["snapshot_content".to_owned()],
                    },
                    limit,
                },
            )
            .await?;
        Ok(rows.into_identity_ordered_snapshots())
    }
}

type HotRowMap = BTreeMap<HeadRowIdentity, Bytes>;

/// An owned tracked snapshot used only while a lifecycle publication is being
/// staged. Normal commits mutate the published hot generation in place; a
/// checkpoint, merge, or branch move instead builds one complete replacement
/// generation before the branch control makes it visible.
///
/// The snapshot deliberately stores the already encoded native row values.
/// That makes a root staged in this write set usable as the parent of another
/// root without reading the uncommitted write set back through storage.
#[derive(Clone, Default)]
pub(crate) struct HotTrackedSnapshot {
    rows: HotRowMap,
}

pub(crate) enum CompleteWorkingDiffMode {
    Disabled,
    ResetClean,
    Rebase {
        checkpoint_commit_id: CommitId,
        checkpoint: HotTrackedSnapshot,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkingDiffBaselineAction {
    Continue,
    Reset,
    Rebase {
        previous_checkpoint_commit_id: CommitId,
    },
}

impl HotTrackedSnapshot {
    pub(crate) fn from_materialized_rows(
        tracked_rows: Vec<MaterializedTrackedStateRow>,
    ) -> Result<Self, LixError> {
        let mut rows = BTreeMap::new();
        for row in tracked_rows {
            let snapshot = if row.deleted {
                None
            } else {
                let typed = row.decoded_snapshot.as_ref().ok_or_else(|| {
                    head_value_error("live lifecycle row is missing its native typed payload")
                })?;
                Some(
                    typed
                        .durable_payload()
                        .map_err(|error| {
                            head_value_error(format!(
                                "cannot retain lifecycle typed payload: {error:?}"
                            ))
                        })?
                        .to_vec(),
                )
            };
            let identity = HeadRowIdentity {
                schema_key: row.schema_key,
                row_pk: row.row_pk,
                file_id: row.file_id,
            };
            let metadata = row
                .metadata
                .as_deref()
                .map(serde_json::from_str)
                .transpose()
                .map_err(|error| {
                    head_value_error(&format!("bootstrap metadata is invalid JSON: {error}"))
                })?
                .map(lix_schema::Jsonb::from_value);
            let value = HeadValueRef {
                change_id: Some(row.change_id),
                commit_id: Some(row.commit_id),
                untracked: false,
                deleted: row.deleted,
                created_at: LixTimestamp::expect_parse(
                    "hot tracked snapshot created_at",
                    &row.created_at,
                ),
                updated_at: LixTimestamp::expect_parse(
                    "hot tracked snapshot updated_at",
                    &row.updated_at,
                ),
                snapshot: snapshot.as_deref(),
                metadata: metadata.as_ref(),
                columnar_base_coordinate: None,
                working_diff_baseline: WorkingDiffBaseline::Disabled,
            };
            if rows
                .insert(identity, Bytes::from(encode_head_value(&value)?))
                .is_some()
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked hot snapshot contains duplicate row identity",
                ));
            }
        }
        Ok(Self { rows })
    }
}

/// Writer for row-addressable current state.
pub(crate) struct HotStateWriter<'a, S: ?Sized> {
    pub(super) store: &'a S,
    pub(super) writes: &'a mut StorageWriteSet,
    /// Schema keys this transaction publishes on the global branch, when the
    /// caller can enumerate them completely.
    ///
    /// A property of the transaction rather than of any one call, which is why
    /// it lives here instead of being threaded through the staging wrappers.
    /// Only serving-view tombstone compaction reads it; see
    /// [`hot_compaction_mask`].
    ///
    /// **Polarity.** `None` means "this caller cannot say", and it *disables*
    /// compaction — the safe verdict. A construction site that is unsure must
    /// leave it `None`; the factory default is `None` for exactly that reason.
    /// Only a caller holding the transaction's complete prepared inputs may
    /// supply a set, and a schema key present in that set likewise disables
    /// compaction for that schema.
    pub(super) transaction_global_schema_keys: Option<&'a BTreeSet<String>>,
}

impl<'a, S: ?Sized> HotStateWriter<'a, S> {
    /// Declares the schema keys this transaction publishes on the global
    /// branch, unlocking serving-view tombstone compaction for every other
    /// schema.
    ///
    /// The caller must derive the set from the transaction's prepared inputs,
    /// not from the write set: `tracked_roots_parent_first` orders roots by
    /// `parent_commit_id` alone and never consults `branch_id`, so a global
    /// root is not guaranteed to be staged before a non-global one and a
    /// write-set consultation would see an incomplete picture.
    pub(crate) fn with_transaction_global_schema_keys(
        mut self,
        schema_keys: &'a BTreeSet<String>,
    ) -> Self {
        self.transaction_global_schema_keys = Some(schema_keys);
        self
    }
}

impl<S> HotStateWriter<'_, S>
where
    S: StorageAdapterRead + ?Sized,
{
    /// Returns the checkpoint tombstones that can actually be removed under
    /// the current serving-base and cross-branch safety gates.
    ///
    /// Background GC uses this before pagination so permanently blocked rows
    /// do not occupy the first page forever.
    pub(crate) async fn checkpoint_tombstone_compaction_mask(
        &self,
        branch_id: &str,
        generation: CommitId,
        deltas: &[CurrentStateDeltaRef<'_>],
    ) -> Result<Vec<bool>, LixError> {
        let refs = deltas.iter().collect::<Vec<_>>();
        hot_compaction_mask(
            self.store,
            branch_id,
            generation,
            &refs,
            None,
            HotTombstoneMaskKind::Checkpoint,
            self.transaction_global_schema_keys,
        )
        .await
    }

    /// Publishes an immutable tracked root as the baseline of a new sparse
    /// branch generation. The root is already authoritative for every tracked
    /// identity at `head_commit_id`; later branch-local HOT rows shadow it.
    pub(crate) fn stage_root_current_base(
        &mut self,
        branch_id: &str,
        generation: CommitId,
        head_commit_id: CommitId,
    ) {
        self.writes.put(
            ROOT_CURRENT_BASE_SPACE,
            StorageKey(Bytes::from(hot_scope_prefix(branch_id, generation))),
            StorageValue {
                bytes: Bytes::copy_from_slice(head_commit_id.as_uuid().as_bytes()),
            },
        );
    }

    /// Publishes a transaction-certified ordered insert batch as an immutable
    /// current-state base without rebuilding row-shaped deltas or absence
    /// guards.
    ///
    /// The commit-delta writer has already proven strict physical-key order
    /// and assigned every row its direct address. Transaction validation has
    /// proven that the same complete batch is absent under the branch-control
    /// snapshot. Walking the prepared identity columns once for schema counts
    /// is therefore sufficient to publish the existing commit as current
    /// state.
    pub(crate) async fn stage_ordered_insert_current_base<'a, I>(
        &mut self,
        branch_id: &str,
        generation: CommitId,
        new_head: CommitId,
        rows: I,
        row_columnar_write_sets: &crate::hot_state::RowColumnarWriteSets,
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
        coverage: &mut WorkingDiffIndexCoverage,
    ) -> Result<CommitId, LixError>
    where
        I: ExactSizeIterator<Item = (&'a str, &'a RowPk)> + Clone,
    {
        if rows.len() == 0 {
            return Err(head_value_error(
                "ordered packed current base requires at least one inserted row",
            ));
        }
        let mut previous = None::<(&str, &RowPk)>;
        let mut schema_rows = BTreeMap::<&str, Vec<&RowPk>>::new();
        for (schema_key, row_pk) in rows {
            if previous.is_some_and(|(previous_schema, previous_row_pk)| {
                previous_schema
                    .cmp(schema_key)
                    .then_with(|| previous_row_pk.cmp(row_pk))
                    != Ordering::Less
            }) {
                return Err(head_value_error(
                    "ordered packed current-base identities are not strictly increasing",
                ));
            }
            previous = Some((schema_key, row_pk));
            schema_rows.entry(schema_key).or_default().push(row_pk);
        }
        let schema_increments = schema_rows
            .into_iter()
            .map(|(schema_key, row_pks)| {
                let live_count = u64::try_from(row_pks.len())
                    .map_err(|_| head_value_error("packed current-base row count exceeds u64"))?;
                Ok((
                    schema_key,
                    PackedCollectionIncrement {
                        live_count,
                        ordered_identity_digest:
                            crate::collection_generation::ordered_single_string_identity_digest(
                                row_pks,
                            ),
                    },
                ))
            })
            .collect::<Result<BTreeMap<_, _>, LixError>>()?;
        for schema_key in schema_increments.keys() {
            if let Some(encoded) =
                row_columnar_write_sets.get(&(new_head, (*schema_key).to_string()))
            {
                crate::columnar_row_group::stage_row_group_set(
                    self.writes,
                    crate::hot_state::row_group_set_id(new_head, schema_key),
                    encoded,
                )?;
            }
        }
        self.stage_packed_insert_current_base_manifest(
            branch_id,
            generation,
            new_head,
            schema_increments,
            None,
            working_diff_capture_checkpoint_commit_id,
            coverage,
        )
        .await
    }

    /// Publishes an already authenticated lossless columnar mutation set as
    /// the immutable current-state base. The mutation inventory has already
    /// walked the strictly ordered identities and sealed their digest, so
    /// repeating that O(rows) work here would create a second physical-layout
    /// authority and a large transient pointer index.
    pub(crate) async fn stage_certified_columnar_insert_current_base(
        &mut self,
        branch_id: &str,
        generation: CommitId,
        new_head: CommitId,
        parts: &crate::tracked_state::ColumnarMutationPartSet,
        lifecycle: &crate::tracked_state::CommitDeltaLifecycleSummary,
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
        coverage: &mut WorkingDiffIndexCoverage,
    ) -> Result<CommitId, LixError> {
        if parts.owner_commit_id != *new_head.as_uuid().as_bytes()
            || parts.row_count == 0
            || lifecycle.scope.schema_key != parts.schema_key
            || lifecycle.scope.file_id.is_some()
            || lifecycle.uniform_created_at != parts.uniform_created_at
        {
            return Err(head_value_error(
                "certified columnar current base disagrees with mutation authority",
            ));
        }
        let schema_increments = BTreeMap::from([(
            parts.schema_key.as_str(),
            PackedCollectionIncrement {
                live_count: u64::from(parts.row_count),
                ordered_identity_digest: Some(lifecycle.ordered_identity_digest),
            },
        )]);
        self.stage_packed_insert_current_base_manifest(
            branch_id,
            generation,
            new_head,
            schema_increments,
            None,
            working_diff_capture_checkpoint_commit_id,
            coverage,
        )
        .await
    }

    /// Publishes a commit whose ordered deltas replace every live member of
    /// one tracked, unfiled collection as a new packed base segment.
    ///
    /// Packed bases shared by other schemas remain available. Bases indexed
    /// as exclusive to this schema are superseded completely and retired, so
    /// repeated replacements retain the single-base read shape.
    pub(crate) async fn stage_complete_collection_replacement_current_base(
        &mut self,
        branch_id: &str,
        generation: CommitId,
        new_head: CommitId,
        schema_key: &str,
        row_count: usize,
        row_columnar_write_sets: &crate::hot_state::RowColumnarWriteSets,
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
        coverage: &mut WorkingDiffIndexCoverage,
    ) -> Result<(CommitId, bool), LixError> {
        let row_count = u64::try_from(row_count)
            .map_err(|_| head_value_error("packed replacement row count exceeds u64"))?;
        if row_count == 0 {
            return Err(head_value_error(
                "packed collection replacement requires at least one row",
            ));
        }
        let control = load_hot_collection_control(
            self.store,
            branch_id,
            generation,
            crate::collection_generation::CollectionScopeRef {
                schema_key,
                file_id: None,
            },
        )
        .await?;
        self.stage_complete_collection_replacement_current_base_with_control(
            branch_id,
            generation,
            new_head,
            schema_key,
            row_count,
            control,
            row_columnar_write_sets,
            working_diff_capture_checkpoint_commit_id,
            coverage,
        )
        .await
    }

    async fn stage_complete_collection_replacement_current_base_with_control(
        &mut self,
        branch_id: &str,
        generation: CommitId,
        new_head: CommitId,
        schema_key: &str,
        row_count: u64,
        control: HotCollectionControl,
        row_columnar_write_sets: &crate::hot_state::RowColumnarWriteSets,
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
        coverage: &mut WorkingDiffIndexCoverage,
    ) -> Result<(CommitId, bool), LixError> {
        let live_count = if control.live_count == DEFERRED_ROOT_LIVE_COUNT {
            let reader = HotStateStoreReader {
                store: &*self.store,
                transaction_cache: None,
                root_base_cache: None,
            };
            reader
                .exact_collection_live_count(
                    branch_id,
                    generation,
                    crate::collection_generation::CollectionScopeRef {
                        schema_key,
                        file_id: None,
                    },
                )
                .await?
        } else {
            control.live_count
        };
        if live_count != row_count {
            return Err(head_value_error(format!(
                "packed collection replacement expected {row_count} live rows in '{schema_key}', found {}",
                live_count
            )));
        }

        let replaced =
            packed_exclusive_schema_base_refs(self.store, branch_id, generation, schema_key)
                .await?;
        if replaced
            .iter()
            .any(|base_ref| base_ref.commit_id == new_head)
        {
            return Err(head_value_error(
                "packed collection replacement must publish a new commit",
            ));
        }
        let mut manifest_key = hot_scope_prefix(branch_id, generation);
        manifest_key.reserve(16);
        manifest_key.extend_from_slice(new_head.as_uuid().as_bytes());
        let expected_checkpoint = working_diff_capture_checkpoint_commit_id
            .map_or([0; 16], |checkpoint| *checkpoint.as_uuid().as_bytes());
        if replaced.is_empty() {
            if working_diff_capture_checkpoint_commit_id.is_some() {
                coverage
                    .add_encoded_group_key(&manifest_key)
                    .ok_or_else(|| {
                        head_value_error("packed current-base diff count exceeds u64")
                    })?;
            }
        } else {
            let base_keys = replaced
                .iter()
                .map(|base_ref| {
                    let mut key = hot_scope_prefix(branch_id, generation);
                    key.reserve(16);
                    key.extend_from_slice(base_ref.commit_id.as_uuid().as_bytes());
                    StorageKey(Bytes::from(key))
                })
                .collect::<Vec<_>>();
            let base_values = PointReadPlan::new(PACKED_CURRENT_BASE_SPACE, &base_keys)
                .materialize(self.store, StorageGetOptions::default())
                .await?
                .value;
            for ((base_ref, base_key), value) in replaced.iter().zip(&base_keys).zip(base_values) {
                let value = value.ok_or_else(|| {
                    head_value_error(
                        "exclusive-schema index references an inactive packed current base",
                    )
                })?;
                let base_checkpoint = full_value_bytes(value)?;
                let base_is_dirty_in_active_epoch = base_checkpoint.as_ref() == expected_checkpoint;
                if working_diff_capture_checkpoint_commit_id.is_none()
                    && !base_is_dirty_in_active_epoch
                {
                    return Err(head_value_error(
                        "packed collection replacement has a different working-diff owner",
                    ));
                }
                if working_diff_capture_checkpoint_commit_id.is_some()
                    && base_is_dirty_in_active_epoch
                {
                    coverage
                        .remove_encoded_group_key(&base_key.0)
                        .ok_or_else(|| {
                            head_value_error("packed current-base diff coverage underflow")
                        })?;
                }
                self.writes
                    .delete(PACKED_CURRENT_BASE_SPACE, base_key.clone());
                self.writes.delete(
                    PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE,
                    StorageKey(base_ref.index_key.clone()),
                );
            }
            if working_diff_capture_checkpoint_commit_id.is_some() {
                coverage
                    .add_encoded_group_key(&manifest_key)
                    .ok_or_else(|| {
                        head_value_error("packed current-base diff count exceeds u64")
                    })?;
            }
        }
        if let Some(encoded) = row_columnar_write_sets.get(&(new_head, schema_key.to_string())) {
            crate::columnar_row_group::stage_row_group_set(
                self.writes,
                crate::hot_state::row_group_set_id(new_head, schema_key),
                encoded,
            )?;
        }
        self.writes.put(
            PACKED_CURRENT_BASE_SPACE,
            StorageKey(Bytes::from(manifest_key)),
            StorageValue {
                bytes: Bytes::copy_from_slice(&expected_checkpoint),
            },
        );
        stage_packed_exclusive_schema_base_ref(
            self.writes,
            branch_id,
            generation,
            schema_key,
            new_head,
        );
        self.writes.put(
            PACKED_CURRENT_BASE_CONTROL_SPACE,
            StorageKey(Bytes::from(hot_scope_prefix(branch_id, generation))),
            StorageValue {
                bytes: Bytes::from_static(&[1]),
            },
        );
        Ok((generation, !replaced.is_empty()))
    }

    /// Attempts to prove that an ordinary Replace/upsert batch covers one
    /// complete packed collection, then publishes it through the replacement
    /// lane. The persisted count alone is insufficient: equality of the
    /// ordered identity digest proves that no current member is omitted and
    /// no unrelated identity is introduced.
    pub(crate) async fn try_stage_exact_collection_replacement_current_base(
        &mut self,
        branch_id: &str,
        generation: CommitId,
        parent_commit_id: CommitId,
        parent_authority_certified: bool,
        new_head: CommitId,
        deltas: &[CurrentStateDeltaRef<'_>],
        row_columnar_write_sets: &crate::hot_state::RowColumnarWriteSets,
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
        coverage: &mut WorkingDiffIndexCoverage,
    ) -> Result<Option<CommitId>, LixError> {
        let Some(first) = deltas.first() else {
            return Ok(None);
        };
        let schema_key = first.schema_key;
        if deltas.iter().any(|delta| {
            delta.schema_key != schema_key
                || delta.file_id.is_some()
                || delta.untracked
                || delta.deleted
                || delta.commit_id != Some(new_head)
                || delta.change_id.is_none()
        }) {
            return Ok(None);
        }
        let control = load_hot_collection_control(
            self.store,
            branch_id,
            generation,
            crate::collection_generation::CollectionScopeRef {
                schema_key,
                file_id: None,
            },
        )
        .await?;
        if control.active_generation != generation
            || control.live_count != u64::try_from(deltas.len()).unwrap_or(u64::MAX)
        {
            return Ok(None);
        }
        let mut row_pks = deltas.iter().map(|delta| delta.row_pk).collect::<Vec<_>>();
        row_pks.sort_unstable();
        if row_pks.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(head_value_error(
                "packed collection replacement contains duplicate identities",
            ));
        }
        let Some(identity_digest) =
            crate::collection_generation::ordered_single_string_identity_digest(
                row_pks.iter().copied(),
            )
        else {
            return Ok(None);
        };
        if control.ordered_identity_digest != Some(identity_digest) {
            return Ok(None);
        }
        if !parent_authority_certified
            && !self
                .authoritative_collection_matches(
                    parent_commit_id,
                    schema_key,
                    &row_pks,
                    identity_digest,
                )
                .await?
        {
            return Ok(None);
        }
        self.stage_complete_collection_replacement_current_base_with_control(
            branch_id,
            generation,
            new_head,
            schema_key,
            u64::try_from(row_pks.len()).unwrap_or(u64::MAX),
            control,
            row_columnar_write_sets,
            working_diff_capture_checkpoint_commit_id,
            coverage,
        )
        .await
        .map(|(generation, _)| Some(generation))
    }

    /// Attempts to collapse an exact row-wise deletion of one untouched
    /// packed collection into its collection-generation control. Historical
    /// tombstones remain authoritative in the commit delta; current state
    /// needs only the new empty-generation fence and retirement of the old
    /// exclusive packed base.
    pub(crate) async fn try_stage_exact_collection_delete_current_base(
        &mut self,
        branch_id: &str,
        generation: CommitId,
        parent_commit_id: CommitId,
        new_head: CommitId,
        deltas: &[CurrentStateDeltaRef<'_>],
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
    ) -> Result<Option<CommitId>, LixError> {
        if working_diff_capture_checkpoint_commit_id.is_some() {
            return Ok(None);
        }
        let Some(first) = deltas.first() else {
            return Ok(None);
        };
        let schema_key = first.schema_key;
        if deltas.iter().any(|delta| {
            delta.schema_key != schema_key
                || delta.file_id.is_some()
                || delta.untracked
                || !delta.deleted
                || delta.commit_id != Some(new_head)
                || delta.change_id.is_none()
        }) {
            return Ok(None);
        }
        let control = load_hot_collection_control(
            self.store,
            branch_id,
            generation,
            crate::collection_generation::CollectionScopeRef {
                schema_key,
                file_id: None,
            },
        )
        .await?;
        if control.active_generation != generation
            || control.live_count != u64::try_from(deltas.len()).unwrap_or(u64::MAX)
        {
            return Ok(None);
        }
        let mut row_pks = deltas.iter().map(|delta| delta.row_pk).collect::<Vec<_>>();
        row_pks.sort_unstable();
        if row_pks.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(head_value_error(
                "packed collection deletion contains duplicate identities",
            ));
        }
        let Some(identity_digest) =
            crate::collection_generation::ordered_single_string_identity_digest(
                row_pks.iter().copied(),
            )
        else {
            return Ok(None);
        };
        if control.ordered_identity_digest != Some(identity_digest) {
            return Ok(None);
        }
        if !self
            .authoritative_collection_matches(
                parent_commit_id,
                schema_key,
                &row_pks,
                identity_digest,
            )
            .await?
        {
            return Ok(None);
        }
        let replaced =
            packed_exclusive_schema_base_refs(self.store, branch_id, generation, schema_key)
                .await?;
        if replaced.is_empty() {
            return Ok(None);
        }
        let base_keys = replaced
            .iter()
            .map(|base_ref| {
                let mut key = hot_scope_prefix(branch_id, generation);
                key.reserve(16);
                key.extend_from_slice(base_ref.commit_id.as_uuid().as_bytes());
                StorageKey(Bytes::from(key))
            })
            .collect::<Vec<_>>();
        let base_values = PointReadPlan::new(PACKED_CURRENT_BASE_SPACE, &base_keys)
            .materialize(self.store, StorageGetOptions::default())
            .await?
            .value;
        for value in base_values {
            let value = value.ok_or_else(|| {
                head_value_error(
                    "exclusive-schema index references an inactive packed current base",
                )
            })?;
            if full_value_bytes(value)?.as_ref() != [0; 16] {
                return Ok(None);
            }
        }
        for (base_ref, base_key) in replaced.iter().zip(base_keys) {
            self.writes.delete(PACKED_CURRENT_BASE_SPACE, base_key);
            self.writes.delete(
                PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE,
                StorageKey(base_ref.index_key.clone()),
            );
        }
        stage_hot_collection_control(
            self.writes,
            branch_id,
            generation,
            crate::collection_generation::CollectionScopeRef {
                schema_key,
                file_id: None,
            },
            HotCollectionControl {
                active_generation: new_head,
                live_count: 0,
                ordered_identity_digest: None,
            },
        )?;
        Ok(Some(generation))
    }

    /// Recomputes a full-collection certificate from immutable tracked-state
    /// authority before a derived HOT control may retire a base. The scan
    /// projects identity columns only: payloads remain in their canonical
    /// owner and are neither fetched nor decoded for this set-equality proof.
    /// A corrupt or stale control can therefore disable the compact route,
    /// but it can never make omitted identities disappear from current state.
    async fn authoritative_collection_matches(
        &self,
        parent_commit_id: CommitId,
        schema_key: &str,
        expected_row_pks: &[&RowPk],
        expected_identity_digest: [u8; 32],
    ) -> Result<bool, LixError> {
        let mut reader = crate::tracked_state::TrackedStateContext::new().reader(&*self.store);
        let rows = reader
            .scan_batch_at_commit(
                &parent_commit_id.to_string(),
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec![schema_key.to_owned()],
                        file_ids: vec![NullableKeyFilter::Null],
                        ..TrackedStateFilter::default()
                    },
                    read_columns: TrackedStateReadColumns {
                        columns: vec!["schema_key".to_owned()],
                    },
                    limit: None,
                },
            )
            .await?;
        if rows.len() != expected_row_pks.len() {
            return Ok(false);
        }
        let mut authoritative_row_pks = rows.iter().map(|row| row.row_pk()).collect::<Vec<_>>();
        authoritative_row_pks.sort_unstable();
        if authoritative_row_pks.as_slice() != expected_row_pks {
            return Ok(false);
        }
        Ok(
            crate::collection_generation::ordered_single_string_identity_digest(
                authoritative_row_pks,
            ) == Some(expected_identity_digest),
        )
    }

    /// Publishes validated, tracked, unfiled creates as an immutable base.
    ///
    /// The commit-delta plane already owns the sorted identities and payloads,
    /// so manufacturing an equivalent HOT value and backend mutation for every
    /// row is pure write amplification. This path retains only collection
    /// counts plus one generation-to-commit reference. Ordinary mutations
    /// continue to shadow the base through HOT rows.
    pub(crate) async fn try_stage_packed_insert_current_base(
        &mut self,
        branch_id: &str,
        generation: CommitId,
        new_head: CommitId,
        deltas: &[CurrentStateDeltaRef<'_>],
        absence_guards: &[TrackedStateKeyRef<'_>],
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
        coverage: &mut WorkingDiffIndexCoverage,
    ) -> Result<Option<CommitId>, LixError> {
        if deltas.is_empty() {
            return Err(head_value_error(
                "packed current base requires at least one inserted row",
            ));
        }
        let mut sorted = deltas.iter().collect::<Vec<_>>();
        for delta in &sorted {
            delta.validate()?;
            if delta.untracked
                || delta.deleted
                || delta.file_id.is_some()
                || delta.commit_id != Some(new_head)
                || delta.change_id.is_none()
            {
                return Err(head_value_error(
                    "packed current base accepts only live tracked unfiled creates",
                ));
            }
        }
        sorted.sort_unstable_by(|left, right| compare_hot_deltas(left, right));
        if sorted
            .windows(2)
            .any(|pair| compare_hot_deltas(pair[0], pair[1]).is_eq())
        {
            return Err(current_state_duplicate_delta_error(sorted[1]));
        }
        let mut schema_rows = BTreeMap::<&str, Vec<&RowPk>>::new();
        for delta in &sorted {
            schema_rows
                .entry(delta.schema_key)
                .or_default()
                .push(delta.row_pk);
        }
        let preloaded_controls = if absence_guards.is_empty() {
            // Generic Replace/upsert batches do not carry INSERT-shaped
            // per-row guards. An exact empty collection control is a stronger,
            // constant-size proof that every identity in that scope is absent.
            // The caller's branch-head CAS keeps this snapshot valid through
            // publication. Root-backed deferred counts deliberately fail this
            // certificate and remain on the ordinary row-addressable path.
            let scopes = schema_rows
                .keys()
                .map(
                    |schema_key| crate::collection_generation::CollectionScopeRef {
                        schema_key,
                        file_id: None,
                    },
                )
                .collect::<Vec<_>>();
            let controls =
                load_hot_collection_controls(self.store, branch_id, generation, &scopes).await?;
            if controls.iter().any(|control| control.live_count != 0) {
                return Ok(None);
            }
            Some(controls)
        } else {
            let mut guarded = absence_guards
                .iter()
                .map(|guard| (guard.schema_key, guard.row_pk, guard.file_id))
                .collect::<Vec<_>>();
            guarded.sort_unstable();
            if sorted
                .iter()
                .map(|delta| (delta.schema_key, delta.row_pk, delta.file_id))
                .ne(guarded)
            {
                return Err(head_value_error(
                    "packed current base rows do not exactly match their validated absence proofs",
                ));
            }
            None
        };

        let schema_increments = schema_rows
            .into_iter()
            .map(|(schema_key, row_pks)| {
                let live_count = u64::try_from(row_pks.len())
                    .map_err(|_| head_value_error("packed current-base row count exceeds u64"))?;
                Ok((
                    schema_key,
                    PackedCollectionIncrement {
                        live_count,
                        ordered_identity_digest:
                            crate::collection_generation::ordered_single_string_identity_digest(
                                row_pks,
                            ),
                    },
                ))
            })
            .collect::<Result<BTreeMap<_, _>, LixError>>()?;
        self.stage_packed_insert_current_base_manifest(
            branch_id,
            generation,
            new_head,
            schema_increments,
            preloaded_controls,
            working_diff_capture_checkpoint_commit_id,
            coverage,
        )
        .await
        .map(Some)
    }

    /// Publishes every row of one planner-certified fresh plugin file through
    /// the commit-delta authority instead of emitting ROW/FILE/DIFF point puts
    /// for each semantic row.
    ///
    /// The transaction certificate proves the complete file namespace is a
    /// fresh insert. This method binds that proof to the exact file id again,
    /// requires every delta to be a live tracked create owned by the new
    /// commit, and independently requires every affected file-scoped
    /// collection control to be empty in the parent generation before
    /// publishing one packed base. Schema-global controls may already contain
    /// other files; their count is incremented and their exact digest is
    /// cleared because ordered digests are deliberately not composable.
    pub(crate) async fn try_stage_certified_fresh_file_current_base(
        &mut self,
        branch_id: &str,
        generation: CommitId,
        new_head: CommitId,
        certified_file_id: &str,
        deltas: &[CurrentStateDeltaRef<'_>],
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
        coverage: &mut WorkingDiffIndexCoverage,
    ) -> Result<Option<CommitId>, LixError> {
        if deltas.is_empty() {
            return Err(head_value_error(
                "certified fresh-file packed base requires at least one row",
            ));
        }
        let mut sorted = deltas.iter().collect::<Vec<_>>();
        for delta in &sorted {
            delta.validate()?;
            if delta.untracked
                || delta.deleted
                || delta.file_id != Some(certified_file_id)
                || delta.commit_id != Some(new_head)
                || delta.change_id.is_none()
            {
                return Err(head_value_error(
                    "certified fresh-file packed base accepts only live tracked creates for its exact file",
                ));
            }
        }
        sorted.sort_unstable_by(|left, right| compare_hot_deltas(left, right));
        if sorted
            .windows(2)
            .any(|pair| compare_hot_deltas(pair[0], pair[1]).is_eq())
        {
            return Err(current_state_duplicate_delta_error(sorted[1]));
        }

        let mut scope_members =
            BTreeMap::<(String, Option<String>), Vec<&CurrentStateDeltaRef<'_>>>::new();
        for delta in &sorted {
            scope_members
                .entry((delta.schema_key.to_owned(), None))
                .or_default()
                .push(delta);
            scope_members
                .entry((
                    delta.schema_key.to_owned(),
                    Some(certified_file_id.to_owned()),
                ))
                .or_default()
                .push(delta);
        }
        let scopes = scope_members
            .keys()
            .map(
                |(schema_key, file_id)| crate::collection_generation::CollectionScopeRef {
                    schema_key,
                    file_id: file_id.as_deref(),
                },
            )
            .collect::<Vec<_>>();
        let controls =
            load_hot_collection_controls(self.store, branch_id, generation, &scopes).await?;
        if scope_members
            .keys()
            .zip(&controls)
            .any(|((_, file_id), control)| file_id.is_some() && control.live_count != 0)
        {
            return Ok(None);
        }

        for (((schema_key, file_id), members), mut control) in
            scope_members.into_iter().zip(controls)
        {
            let scope = crate::collection_generation::CollectionScopeRef {
                schema_key: &schema_key,
                file_id: file_id.as_deref(),
            };
            let increment = u64::try_from(members.len())
                .map_err(|_| head_value_error("hot collection live count exceeds u64"))?;
            let was_empty = control.live_count == 0;
            let increment_digest = if was_empty {
                let mut digest = CompleteHotCollectionDigest::new(branch_id, generation, scope);
                let mut canonical_key = hot_scope_prefix(branch_id, generation);
                let scope_prefix_len = canonical_key.len();
                for delta in members {
                    canonical_key.truncate(scope_prefix_len);
                    write_key_string(&mut canonical_key, delta.schema_key, KEY_PART_FINAL);
                    write_file_id(&mut canonical_key, delta.file_id);
                    write_row_pk(&mut canonical_key, delta.row_pk);
                    digest.push_parts(delta.row_pk, delta.file_id, &canonical_key)?;
                }
                Some(digest.finish())
            } else {
                None
            };
            if control.live_count == DEFERRED_ROOT_LIVE_COUNT {
                control.ordered_identity_digest = None;
            } else {
                control.live_count = control
                    .live_count
                    .checked_add(increment)
                    .ok_or_else(|| head_value_error("hot collection live count exceeds u64"))?;
                control.ordered_identity_digest = if was_empty { increment_digest } else { None };
            }
            stage_hot_collection_control(self.writes, branch_id, generation, scope, control)?;
        }

        let mut manifest_key = hot_scope_prefix(branch_id, generation);
        manifest_key.extend_from_slice(new_head.as_uuid().as_bytes());
        if working_diff_capture_checkpoint_commit_id.is_some() {
            coverage
                .add_encoded_group_key(&manifest_key)
                .ok_or_else(|| head_value_error("packed current-base diff count exceeds u64"))?;
        }
        self.writes.put(
            PACKED_CURRENT_BASE_SPACE,
            StorageKey(Bytes::from(manifest_key)),
            StorageValue {
                bytes: packed_current_base_value(
                    working_diff_capture_checkpoint_commit_id,
                    Some(certified_file_id),
                )?,
            },
        );
        self.writes.put(
            PACKED_CURRENT_BASE_CONTROL_SPACE,
            StorageKey(Bytes::from(hot_scope_prefix(branch_id, generation))),
            StorageValue {
                bytes: Bytes::from_static(&[1]),
            },
        );
        Ok(Some(generation))
    }

    async fn stage_packed_insert_current_base_manifest(
        &mut self,
        branch_id: &str,
        generation: CommitId,
        new_head: CommitId,
        schema_increments: BTreeMap<&str, PackedCollectionIncrement>,
        preloaded_controls: Option<Vec<HotCollectionControl>>,
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
        coverage: &mut WorkingDiffIndexCoverage,
    ) -> Result<CommitId, LixError> {
        let exclusive_schema_key = (schema_increments.len() == 1).then(|| {
            *schema_increments
                .keys()
                .next()
                .expect("one schema increment")
        });
        let mut manifest_key = hot_scope_prefix(branch_id, generation);
        manifest_key.reserve(16);
        manifest_key.extend_from_slice(new_head.as_uuid().as_bytes());
        if working_diff_capture_checkpoint_commit_id.is_some() {
            coverage
                .add_encoded_group_key(&manifest_key)
                .ok_or_else(|| head_value_error("packed current-base diff count exceeds u64"))?;
        }

        let scopes = schema_increments
            .keys()
            .map(
                |schema_key| crate::collection_generation::CollectionScopeRef {
                    schema_key,
                    file_id: None,
                },
            )
            .collect::<Vec<_>>();
        let controls = match preloaded_controls {
            Some(controls) if controls.len() == scopes.len() => controls,
            Some(_) => {
                return Err(head_value_error(
                    "packed current-base control certificate has the wrong scope count",
                ));
            }
            None => {
                load_hot_collection_controls(self.store, branch_id, generation, &scopes).await?
            }
        };
        for ((schema_key, increment), mut control) in schema_increments.into_iter().zip(controls) {
            let was_empty = control.live_count == 0;
            if control.live_count == DEFERRED_ROOT_LIVE_COUNT {
                control.ordered_identity_digest = None;
            } else {
                control.live_count = control
                    .live_count
                    .checked_add(increment.live_count)
                    .ok_or_else(|| head_value_error("hot collection live count exceeds u64"))?;
                control.ordered_identity_digest = if was_empty {
                    increment.ordered_identity_digest
                } else {
                    None
                };
            }
            stage_hot_collection_control(
                self.writes,
                branch_id,
                generation,
                crate::collection_generation::CollectionScopeRef {
                    schema_key,
                    file_id: None,
                },
                control,
            )?;
        }
        self.writes.put(
            PACKED_CURRENT_BASE_SPACE,
            StorageKey(Bytes::from(manifest_key)),
            StorageValue {
                bytes: working_diff_capture_checkpoint_commit_id.map_or_else(
                    || Bytes::from_static(&[0; 16]),
                    |checkpoint| Bytes::copy_from_slice(checkpoint.as_uuid().as_bytes()),
                ),
            },
        );
        if let Some(schema_key) = exclusive_schema_key {
            stage_packed_exclusive_schema_base_ref(
                self.writes,
                branch_id,
                generation,
                schema_key,
                new_head,
            );
        }
        self.writes.put(
            PACKED_CURRENT_BASE_CONTROL_SPACE,
            StorageKey(Bytes::from(hot_scope_prefix(branch_id, generation))),
            StorageValue {
                bytes: Bytes::from_static(&[1]),
            },
        );
        Ok(generation)
    }

    #[cfg(any(test, feature = "storage-benches"))]
    pub(crate) async fn stage_commit(
        &mut self,
        branch_id: &str,
        parent_generation: Option<CommitId>,
        new_head: CommitId,
        deltas: &[TrackedHeadDeltaRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
        parent_rows: Option<Vec<MaterializedTrackedStateRow>>,
    ) -> Result<CommitId, LixError> {
        let typed_rows = deltas
            .iter()
            .map(test_snapshot)
            .collect::<Result<Vec<_>, _>>()?;
        let deltas = deltas
            .iter()
            .zip(&typed_rows)
            .map(|(delta, snapshot)| test_current_delta(delta, snapshot.as_deref()))
            .collect::<Vec<_>>();
        let mut coverage = WorkingDiffIndexCoverage::default();
        let generation = self
            .stage_current_state_with_working_diff(
                branch_id,
                parent_generation,
                new_head,
                &deltas,
                absence_guards,
                parent_rows,
                None,
                &mut coverage,
            )
            .await?;
        #[cfg(test)]
        stage_test_current_control(self.writes, branch_id, new_head, generation, None)?;
        Ok(generation)
    }

    #[cfg(test)]
    pub(crate) async fn stage_commit_with_working_diff(
        &mut self,
        branch_id: &str,
        parent_generation: Option<CommitId>,
        new_head: CommitId,
        deltas: &[TrackedHeadDeltaRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
        parent_rows: Option<Vec<MaterializedTrackedStateRow>>,
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
        coverage: &mut WorkingDiffIndexCoverage,
    ) -> Result<CommitId, LixError> {
        let typed_rows = deltas
            .iter()
            .map(test_snapshot)
            .collect::<Result<Vec<_>, _>>()?;
        let deltas = deltas
            .iter()
            .zip(&typed_rows)
            .map(|(delta, snapshot)| test_current_delta(delta, snapshot.as_deref()))
            .collect::<Vec<_>>();
        self.stage_current_state_with_working_diff(
            branch_id,
            parent_generation,
            new_head,
            &deltas,
            absence_guards,
            parent_rows,
            working_diff_capture_checkpoint_commit_id,
            coverage,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn stage_current_state_with_working_diff(
        &mut self,
        branch_id: &str,
        parent_generation: Option<CommitId>,
        new_head: CommitId,
        deltas: &[CurrentStateDeltaRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
        parent_rows: Option<Vec<MaterializedTrackedStateRow>>,
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
        coverage: &mut WorkingDiffIndexCoverage,
    ) -> Result<CommitId, LixError> {
        self.stage_current_state_with_working_diff_inner(
            branch_id,
            parent_generation,
            new_head,
            deltas,
            &[],
            absence_guards,
            parent_rows,
            working_diff_capture_checkpoint_commit_id,
            coverage,
            false,
            None,
            None,
            WorkingDiffBaselineAction::Continue,
            &BTreeMap::new(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn stage_current_state_with_certified_predecessors(
        &mut self,
        branch_id: &str,
        parent_generation: Option<CommitId>,
        new_head: CommitId,
        deltas: &[CurrentStateDeltaRef<'_>],
        durable_predecessors: &[CertifiedCurrentStatePredecessorRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
        parent_rows: Option<Vec<MaterializedTrackedStateRow>>,
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
        coverage: &mut WorkingDiffIndexCoverage,
    ) -> Result<CommitId, LixError> {
        self.stage_current_state_with_working_diff_inner(
            branch_id,
            parent_generation,
            new_head,
            deltas,
            durable_predecessors,
            absence_guards,
            parent_rows,
            working_diff_capture_checkpoint_commit_id,
            coverage,
            false,
            None,
            None,
            WorkingDiffBaselineAction::Continue,
            &BTreeMap::new(),
        )
        .await
    }

    /// Publishes a checkpoint into the already-visible generation.
    ///
    /// The checkpoint selected refs are the complete dirty set for the
    /// interval. Rewriting only those rows to `Clean` starts the next epoch
    /// without copying every unchanged HOT row into a fresh generation.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn stage_checkpoint_current_state(
        &mut self,
        branch_id: &str,
        generation: CommitId,
        new_head: CommitId,
        deltas: &[CurrentStateDeltaRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
        checkpoint_commit_id: CommitId,
        coverage: &mut WorkingDiffIndexCoverage,
    ) -> Result<CommitId, LixError> {
        let generation = self
            .stage_current_state_with_working_diff_inner(
                branch_id,
                Some(generation),
                new_head,
                deltas,
                &[],
                absence_guards,
                None,
                Some(checkpoint_commit_id),
                coverage,
                false,
                None,
                None,
                WorkingDiffBaselineAction::Reset,
                &BTreeMap::new(),
            )
            .await?;
        // The checkpoint has now materialized the complete dirty set as HOT
        // rows. Keeping the immutable packed inputs active would make every
        // later read revisit checkpointed history within the same generation.
        stage_retire_packed_current_bases(self.store, self.writes, branch_id, generation).await?;
        Ok(generation)
    }

    /// Carries the unselected part of a partial checkpoint into its new
    /// working-diff epoch without reconstructing the branch snapshot.
    ///
    /// Every delta is already dirty in the visible generation. Its immutable
    /// before-image remains the checkpoint baseline; only the checkpoint id,
    /// current commit ownership, and sparse dirty index advance.
    pub(crate) async fn stage_partial_checkpoint_current_state(
        &mut self,
        branch_id: &str,
        generation: CommitId,
        new_head: CommitId,
        deltas: &[CurrentStateDeltaRef<'_>],
        previous_checkpoint_commit_id: CommitId,
        checkpoint_commit_id: CommitId,
        coverage: &mut WorkingDiffIndexCoverage,
    ) -> Result<CommitId, LixError> {
        self.stage_current_state_with_working_diff_inner(
            branch_id,
            Some(generation),
            new_head,
            deltas,
            &[],
            &BTreeSet::new(),
            None,
            Some(checkpoint_commit_id),
            coverage,
            false,
            None,
            None,
            WorkingDiffBaselineAction::Rebase {
                previous_checkpoint_commit_id,
            },
            &BTreeMap::new(),
        )
        .await
    }

    /// Stages deltas whose absence was already validated against the coherent
    /// transaction snapshot. The caller must publish the corresponding branch
    /// control with a compare-and-swap precondition from that same snapshot.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn stage_validated_insert_current_state_with_working_diff(
        &mut self,
        branch_id: &str,
        parent_generation: Option<CommitId>,
        new_head: CommitId,
        deltas: &[CurrentStateDeltaRef<'_>],
        absence_guards: &[TrackedStateKeyRef<'_>],
        parent_rows: Option<Vec<MaterializedTrackedStateRow>>,
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
        coverage: &mut WorkingDiffIndexCoverage,
        validated_absent_file_id: Option<&str>,
    ) -> Result<CommitId, LixError> {
        if parent_generation.is_none() {
            let owned_guards = absence_guards
                .iter()
                .map(|guard| TrackedStateKey {
                    schema_key: guard.schema_key.to_string(),
                    file_id: guard.file_id.map(str::to_string),
                    row_pk: guard.row_pk.clone(),
                })
                .collect::<BTreeSet<_>>();
            return self
                .stage_current_state_with_working_diff_inner(
                    branch_id,
                    parent_generation,
                    new_head,
                    deltas,
                    &[],
                    &owned_guards,
                    parent_rows,
                    working_diff_capture_checkpoint_commit_id,
                    coverage,
                    true,
                    validated_absent_file_id,
                    None,
                    WorkingDiffBaselineAction::Continue,
                    &BTreeMap::new(),
                )
                .await;
        }
        let no_owned_guards = BTreeSet::new();
        self.stage_current_state_with_working_diff_inner(
            branch_id,
            parent_generation,
            new_head,
            deltas,
            &[],
            &no_owned_guards,
            parent_rows,
            working_diff_capture_checkpoint_commit_id,
            coverage,
            true,
            validated_absent_file_id,
            Some(absence_guards),
            WorkingDiffBaselineAction::Continue,
            &BTreeMap::new(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn stage_current_state_with_working_diff_inner(
        &mut self,
        branch_id: &str,
        parent_generation: Option<CommitId>,
        new_head: CommitId,
        deltas: &[CurrentStateDeltaRef<'_>],
        durable_predecessors: &[CertifiedCurrentStatePredecessorRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
        parent_rows: Option<Vec<MaterializedTrackedStateRow>>,
        working_diff_capture_checkpoint_commit_id: Option<CommitId>,
        coverage: &mut WorkingDiffIndexCoverage,
        absence_guards_validated: bool,
        validated_absent_file_id: Option<&str>,
        borrowed_absence_guards: Option<&[TrackedStateKeyRef<'_>]>,
        working_diff_baseline_action: WorkingDiffBaselineAction,
        certified_live_increments: &BTreeMap<(String, Option<String>), u64>,
    ) -> Result<CommitId, LixError> {
        if matches!(
            working_diff_baseline_action,
            WorkingDiffBaselineAction::Rebase { .. }
        )
            && working_diff_capture_checkpoint_commit_id.is_none()
        {
            return Err(head_value_error(
                "partial-checkpoint rebase requires an active checkpoint",
            ));
        }
        let reset_working_diff_baselines =
            working_diff_baseline_action == WorkingDiffBaselineAction::Reset;
        let rebase_working_diff_baselines = matches!(
            working_diff_baseline_action,
            WorkingDiffBaselineAction::Rebase { .. }
        );
        let predecessor_checkpoint_commit_id = match working_diff_baseline_action {
            WorkingDiffBaselineAction::Rebase {
                previous_checkpoint_commit_id,
            } => Some(previous_checkpoint_commit_id),
            WorkingDiffBaselineAction::Continue | WorkingDiffBaselineAction::Reset => {
                working_diff_capture_checkpoint_commit_id
            }
        };
        let generation = parent_generation.unwrap_or(new_head);
        let sorted = {
            let _span = tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.materialization.hot.sort"
            )
            .entered();
            let mut sorted = deltas.iter().collect::<Vec<_>>();
            for delta in &sorted {
                delta.validate()?;
            }
            let mut already_strictly_sorted = true;
            for pair in sorted.windows(2) {
                match compare_hot_deltas(pair[0], pair[1]) {
                    Ordering::Less => {}
                    Ordering::Equal => {
                        return Err(current_state_duplicate_delta_error(pair[1]));
                    }
                    Ordering::Greater => {
                        already_strictly_sorted = false;
                        break;
                    }
                }
            }
            if !already_strictly_sorted {
                sorted.sort_unstable_by(|left, right| compare_hot_deltas(left, right));
                for pair in sorted.windows(2) {
                    if compare_hot_deltas(pair[0], pair[1]).is_eq() {
                        return Err(current_state_duplicate_delta_error(pair[1]));
                    }
                }
            }
            sorted
        };

        let durable_previous_values = {
            let mut predecessor_index = 0usize;
            let mut aligned = Vec::with_capacity(sorted.len());
            for delta in &sorted {
                let predecessor = durable_predecessors.get(predecessor_index);
                match predecessor
                    .map(|predecessor| compare_certified_predecessor_to_delta(predecessor, delta))
                {
                    Some(Ordering::Less) => {
                        return Err(head_value_error(
                            "certified predecessor does not belong to a staged delta",
                        ));
                    }
                    Some(Ordering::Equal) => {
                        let value = durable_predecessors[predecessor_index].value.view()?;
                        if value.untracked || value.deleted {
                            return Err(head_value_error(
                                "certified predecessor must be a live tracked row",
                            ));
                        }
                        aligned.push(Some(durable_predecessors[predecessor_index].value.clone()));
                        predecessor_index += 1;
                    }
                    Some(Ordering::Greater) | None => aligned.push(None),
                }
            }
            if predecessor_index != durable_predecessors.len() {
                return Err(head_value_error(
                    "certified predecessor does not belong to a staged delta",
                ));
            }
            aligned
        };

        if parent_generation.is_none() {
            if !durable_predecessors.is_empty() {
                return Err(head_value_error(
                    "bootstrap publication cannot carry durable predecessors",
                ));
            }
            stage_hot_bootstrap(
                self.writes,
                branch_id,
                generation,
                parent_rows.unwrap_or_default(),
                &sorted,
                absence_guards,
                working_diff_capture_checkpoint_commit_id,
                coverage,
            )?;
            return Ok(generation);
        }

        let identities = {
            let _span = tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.materialization.hot.identities"
            )
            .entered();
            encode_hot_mutation_identities(branch_id, generation, &sorted)
        };
        // Mutation validation must use primary rows rather than the file-id
        // projection. The projection is an equally-valued read accelerator,
        // not an ownership record.
        let loaded_previous_values = hot_load_primary_mutation_identity_refs(
            self.store,
            &identities,
            &sorted,
            &durable_previous_values,
            absence_guards_validated,
            validated_absent_file_id,
        )
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.materialization.hot.previous"
        ))
        .await?;
        let mut loaded_previous_values = loaded_previous_values.into_iter();
        let mut previous_values = sorted
            .iter()
            .zip(durable_previous_values.iter())
            .map(|(delta, durable_predecessor)| {
                if hot_delta_is_guarded_by_absent_file(
                    delta,
                    absence_guards_validated,
                    validated_absent_file_id,
                ) {
                    None
                } else if let Some(durable_predecessor) = durable_predecessor {
                    Some(durable_predecessor.clone())
                } else {
                    loaded_previous_values
                        .next()
                        .expect("every unguarded hot delta has one loaded previous value")
                }
            })
            .collect::<Vec<_>>();
        let mut previous_from_packed = vec![false; previous_values.len()];
        debug_assert_eq!(loaded_previous_values.len(), 0);
        let packed_previous_indices = durable_previous_values
            .iter()
            .enumerate()
            .filter_map(|(index, predecessor)| predecessor.is_none().then_some(index))
            .collect::<Vec<_>>();
        let packed_previous_keys = packed_previous_indices
            .iter()
            .map(|&index| {
                let delta = sorted[index];
                TrackedStateKeyRef {
                    schema_key: delta.schema_key,
                    row_pk: delta.row_pk,
                    file_id: delta.file_id,
                }
            })
            .collect::<Vec<_>>();
        // The HOT overlay predecessor loaded just above already shadows the
        // packed current base whenever it is at least as new as the newest
        // base ref, so hand it over and let the base loader skip those keys.
        let packed_previous_shadow = packed_previous_indices
            .iter()
            .map(|&index| {
                Ok(previous_values[index]
                    .as_ref()
                    .map(CertifiedCurrentStatePredecessor::view)
                    .transpose()?
                    .and_then(|previous| previous.commit_id))
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        let packed_previous = Box::pin(load_packed_current_base_exact_entries(
            self.store,
            branch_id,
            generation,
            &packed_previous_keys,
            &packed_previous_shadow,
            None,
        ))
        .await?;
        for (index, packed_previous) in packed_previous_indices.iter().copied().zip(packed_previous)
        {
            let previous = &mut previous_values[index];
            let Some((packed_value, _, base_coordinate, base_checkpoint_commit_id)) =
                &packed_previous
            else {
                continue;
            };
            let packed_is_newer = match (
                previous
                    .as_ref()
                    .map(CertifiedCurrentStatePredecessor::view)
                    .transpose()?,
                Some(packed_value.commit_id),
            ) {
                (None, Some(_)) => true,
                (Some(previous), Some(packed)) => {
                    previous.commit_id.is_some_and(|previous| packed > previous)
                }
                _ => false,
            };
            if !packed_is_newer {
                continue;
            }
            previous_from_packed[index] = true;
            *previous = Some(CertifiedCurrentStatePredecessor::Packed(PackedHeadValue {
                change_id: packed_value.change_id,
                commit_id: packed_value.commit_id,
                deleted: packed_value.deleted,
                created_at: packed_value.created_at,
                updated_at: packed_value.updated_at,
                working_diff_baseline: packed_current_base_working_diff_baseline(
                    predecessor_checkpoint_commit_id,
                    *base_checkpoint_commit_id,
                ),
                columnar_base_coordinate: base_coordinate.map(|coordinate| {
                    ColumnarBaseCoordinate {
                        base_commit_id: coordinate.base_commit_id,
                        group_index: coordinate.group_index,
                        row_index: coordinate.row_index,
                    }
                }),
            }));
        }
        let root_previous = if load_root_current_base_commit(self.store, branch_id, generation)
            .await?
            .is_some()
        {
            Box::pin(load_root_current_base_exact(
                self.store,
                branch_id,
                generation,
                predecessor_checkpoint_commit_id,
                &packed_previous_keys,
                ChangeRecordProjection::identity_only(),
            ))
            .await?
        } else {
            MaterializedHotStateExactBatch::new(
                MaterializedHotStateBatch::default(),
                vec![None; packed_previous_keys.len()],
            )?
        };
        for (index, candidate) in packed_previous_indices
            .iter()
            .copied()
            .zip((0..packed_previous_keys.len()).map(|index| root_previous.row(index)))
        {
            let Some(candidate) = candidate else {
                continue;
            };
            let candidate_is_newer = previous_values[index]
                .as_ref()
                .map(CertifiedCurrentStatePredecessor::view)
                .transpose()?
                .is_none_or(|previous| {
                    candidate.commit_id().is_some_and(|candidate| {
                        previous
                            .commit_id
                            .is_none_or(|previous| candidate > previous)
                    })
                });
            if candidate_is_newer {
                previous_from_packed[index] = true;
                previous_values[index] = candidate.durable_predecessor().cloned();
            }
        }
        let mut collection_controls =
            load_incremental_collection_controls(self.store, branch_id, generation, &sorted)
                .await?;
        let missing_certified_scopes = certified_live_increments
            .keys()
            .filter(|scope| !collection_controls.contains_key(*scope))
            .map(
                |(schema_key, file_id)| crate::collection_generation::CollectionScopeRef {
                    schema_key,
                    file_id: file_id.as_deref(),
                },
            )
            .collect::<Vec<_>>();
        let missing_certified_controls = load_hot_collection_controls(
            self.store,
            branch_id,
            generation,
            &missing_certified_scopes,
        )
        .await?;
        collection_controls.extend(
            missing_certified_scopes
                .into_iter()
                .zip(missing_certified_controls)
                .map(|(scope, control)| {
                    (
                        (
                            scope.schema_key.to_owned(),
                            scope.file_id.map(str::to_owned),
                        ),
                        control,
                    )
                }),
        );
        // Collection-generation markers retire every older member in their
        // scope atomically. Apply them before interpreting previous row values
        // so checkpoint-expanded tombstones do not decrement the freshly reset
        // live count.
        apply_incremental_collection_generation_deltas(&mut collection_controls, &sorted)?;
        // A predecessor nulled here is absent for a reason that has nothing to
        // do with a missing serving row: its generation was retired. Canonical
        // state still holds that identity, so a canonical `created_at` lookup
        // would happily resolve it and silently change the timestamp a
        // re-insert after a generation retirement reports today. Remember which
        // slots were nulled for that reason so the lookup below can exclude
        // them. This is transaction-local and durably stores nothing.
        let mut retired_predecessor = vec![false; previous_values.len()];
        for (index, (delta, previous)) in sorted.iter().zip(&mut previous_values).enumerate() {
            if delta.schema_key == crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY {
                continue;
            }
            let belongs_to_retired_generation = previous
                .as_ref()
                .map(CertifiedCurrentStatePredecessor::view)
                .transpose()?
                .is_some_and(|value| {
                    !row_belongs_to_active_collection_generation(
                        &collection_controls,
                        generation,
                        delta.schema_key,
                        delta.file_id,
                        value.untracked,
                        value.commit_id,
                    )
                });
            if belongs_to_retired_generation {
                *previous = None;
                retired_predecessor[index] = true;
            }
        }
        // Narrowed retention fence.
        //
        // `reject_retention_change` below only runs when a predecessor
        // resolves — from a HOT row, or in a sparse generation from the root
        // base. An untracked write whose identity has neither skips the check
        // entirely, so the fence is today only as durable as the tombstone
        // that carries it. `hot_row_tombstone_probe`'s `tombstone_dropped`
        // route measures the consequence: the untracked insert that four
        // supported routes refuse gets in once the tombstone is gone, and
        // `undo` is refused afterwards instead. The throw moves off the
        // operation that caused it and onto an innocent one.
        //
        // Consulting canonical state at the branch head restores the fence
        // where the serving view cannot carry it, and keeps the refusal on the
        // insert.
        //
        // This block's key list used to be untracked-only, which left it empty
        // on every ordinary commit. It no longer is — see the note below on
        // the second question the same batch now answers, and the cost that
        // widening carries.
        // The same batch answers a second question. A tracked insert whose
        // predecessor is absent is either a genuinely new identity — canonical
        // holds nothing, the lookup misses, and `delta.created_at` stands — or
        // an identity whose serving-view predecessor is gone while canonical
        // still carries its `created_at`. Serving-layer tombstone compaction
        // creates exactly the second case, and nothing else in the engine
        // rejects the fresh timestamp that would otherwise be minted, so the
        // recovery has to happen here.
        //
        // This rides the retention fence's existing read: same commit, same
        // key list, same `identity_only` projection. `created_at` is a
        // descriptor column rather than a change-record payload, so it is
        // already materialized and the projection does not widen. What does
        // change is how often the block runs: the untracked-only filter left
        // it empty on ordinary tracked inserts, and it now executes whenever a
        // commit introduces a new identity.
        let mut canonical_created_ats: Vec<Option<LixTimestamp>> = Vec::new();
        {
            let mut unresolved = Vec::new();
            let mut unresolved_slots = Vec::new();
            for (index, (delta, previous)) in sorted.iter().zip(&previous_values).enumerate() {
                // The fence's key set must be preserved exactly: it always
                // included retired-generation slots, and dropping them here
                // would quietly weaken it. Only the *tracked* retired slots
                // are excluded, and only because they must not inherit — they
                // never reached the fence in the first place.
                if delta.deleted
                    || previous.is_some()
                    || (!delta.untracked && retired_predecessor[index])
                {
                    continue;
                }
                unresolved.push(TrackedStateKeyRef {
                    schema_key: delta.schema_key,
                    file_id: delta.file_id,
                    row_pk: delta.row_pk,
                });
                unresolved_slots.push((index, delta.untracked));
            }
            #[cfg(any(test, feature = "storage-benches"))]
            if !unresolved.is_empty() {
                BROAD_CANONICAL_CREATED_AT_LOOKUPS
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                BROAD_CANONICAL_CREATED_AT_KEYS.fetch_add(
                    unresolved.len() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            if !unresolved.is_empty()
                && let Some(control) = BranchHeadControlContext::new()
                    .reader(self.store)
                    .load(branch_id)
                    .await?
                // A branch control can legitimately name a head with no
                // canonical commit state. Replaying against it would turn a
                // legal write into an internal error, which is worse than the
                // hole this closes. No canonical authority means no canonical
                // predecessor, so there is nothing to fence against. This is
                // the same authority check `validate_diff_row_created_at`
                // already uses before it trusts an ancestry lookup.
                && crate::tracked_state::load_commit_state_authority_ids(
                    self.store,
                    std::slice::from_ref(&control.head_commit_id),
                )
                .await?
                .into_iter()
                .next()
                .flatten()
                .is_some()
            {
                let mut reader =
                    crate::tracked_state::TrackedStateContext::new().reader(self.store);
                let canonical = reader
                    .load_projected_batch_at_commit_refs(
                        &control.head_commit_id.to_string(),
                        &unresolved,
                        &ChangeRecordProjection::identity_only(),
                    )
                    .await?;
                canonical_created_ats = vec![None; sorted.len()];
                for (slot, (index, untracked)) in unresolved_slots.iter().copied().enumerate() {
                    let Some(row) = canonical.row(slot) else {
                        continue;
                    };
                    if untracked {
                        // Any canonical row for the identity — live or
                        // tombstoned — means it has been tracked, which is
                        // exactly what `reject_retention_change` refuses to
                        // flip.
                        let key = &unresolved[slot];
                        return Err(LixError::new(
                            LixError::CODE_UNIQUE,
                            format!(
                                "cannot insert untracked row in schema '{}' row_pk {:?}: a tracked row with this identity exists in canonical history; retention is immutable for an identity",
                                key.schema_key, key.row_pk,
                            ),
                        ));
                    }
                    // A tracked insert over an identity canonical still knows.
                    // Inherit its first `created_at` rather than minting a new
                    // one, which is what the serving view's predecessor would
                    // have supplied had it still been there.
                    canonical_created_ats[index] = Some(row.created_at());
                    #[cfg(any(test, feature = "storage-benches"))]
                    BROAD_CANONICAL_CREATED_AT_HITS
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }
        let mut created_ats = Vec::with_capacity(sorted.len());
        for (index, (delta, previous)) in sorted.iter().zip(&previous_values).enumerate() {
            let Some(previous) = previous else {
                // Canonical supplies the timestamp when it still knows the
                // identity; otherwise this is a genuinely new row and mints
                // its own.
                created_ats.push(
                    canonical_created_ats
                        .get(index)
                        .copied()
                        .flatten()
                        .unwrap_or(delta.created_at),
                );
                continue;
            };
            let existing = previous.view()?;
            if let Some(borrowed_absence_guards) = borrowed_absence_guards {
                reject_borrowed_guarded_live_member(borrowed_absence_guards, delta, existing)?;
            } else {
                reject_guarded_live_member(absence_guards, delta, existing)?;
            }
            reject_retention_change(delta, existing)?;
            created_ats.push(if reset_working_diff_baselines && !delta.untracked {
                // Checkpoint selection canonicalizes newly added rows to the
                // changelog timestamp and preserves the original timestamp
                // for modified/removed rows.
                delta.created_at
            } else {
                effective_hot_created_at(existing, working_diff_capture_checkpoint_commit_id)
            });
        }
        // A checkpoint often selects immutable change records whose HOT row
        // is already the exact authoritative value. Re-publishing those rows
        // only changes the row-local dirty marker and commit ownership, even
        // though the checkpoint delta already records historical membership.
        //
        // Retain only a provably identical row whose baseline is already
        // clean. A dirty identical row still needs one compacting rewrite to
        // discard its stale before-image; a squashed selection has a new
        // change ID and must likewise be written so canonical timestamps and
        // payload ownership match historical rebuilds.
        let (sorted, previous_values, created_ats) = if reset_working_diff_baselines {
            let mut retained_deltas = Vec::with_capacity(sorted.len());
            let mut retained_previous = Vec::with_capacity(previous_values.len());
            let mut retained_created_ats = Vec::with_capacity(created_ats.len());
            for (((delta, previous), created_at), previous_from_packed) in sorted
                .into_iter()
                .zip(previous_values)
                .zip(created_ats)
                .zip(previous_from_packed)
            {
                let identical_immutable_change = !previous_from_packed
                    && !delta.untracked
                    && !delta.deleted
                    && delta.schema_key
                        != crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
                    && previous
                        .as_ref()
                        .map(CertifiedCurrentStatePredecessor::view)
                        .transpose()?
                        .is_some_and(|value| {
                            value.change_id == delta.change_id
                                && value.deleted == delta.deleted
                                && value.created_at == delta.created_at
                                && value.updated_at == delta.updated_at
                                && value.working_diff_baseline == WorkingDiffBaseline::Clean
                        });
                if identical_immutable_change {
                    continue;
                }
                retained_deltas.push(delta);
                retained_previous.push(previous);
                retained_created_ats.push(created_at);
            }
            (retained_deltas, retained_previous, retained_created_ats)
        } else {
            (sorted, previous_values, created_ats)
        };
        // Serving-view tombstone compaction. Computed here rather than earlier
        // because the retain filter above rebinds `sorted`, and a mask built
        // against the pre-filter slice would address the wrong rows.
        //
        // `Box::pin` is load-bearing, not style: this is the universal commit
        // write path, guarded by the
        // `slatedb_history_blob_survives_until_final_root_release` stack
        // canary, and the future's size is paid whether or not the branch is
        // taken. The file already carries nine of these for the same reason.
        let compacted = if reset_working_diff_baselines {
            Box::pin(hot_compaction_mask(
                self.store,
                branch_id,
                generation,
                &sorted,
                None,
                HotTombstoneMaskKind::Checkpoint,
                self.transaction_global_schema_keys,
            ))
            .await?
        } else {
            Vec::new()
        };
        // Interval-local tombstone elision, the ordinary-commit sibling of the
        // compaction above.
        //
        // This is NOT "run the compaction mask on every commit". That would
        // trip gate (c) below and fail the publication, because an ordinary
        // commit's rows are not `(Clean, false)`. It is a second, narrower
        // admission rule sharing gates (a) and (b): only a delete whose
        // pre-image already carries `BeforeAbsent` for the active checkpoint,
        // whose owed before-image is therefore absence, and whose tombstone
        // consequently shadows nothing.
        //
        // Phase 11 of `hot_row_tombstone_probe` is why this exists: with
        // deletes confined to one checkpoint interval the compaction route runs
        // and is offered nothing (`routes=1 offered=0`), so the tombstones
        // accumulate linearly and forever. The cheapest fix is not to create
        // them.
        let interval_local = match (
            reset_working_diff_baselines,
            working_diff_capture_checkpoint_commit_id,
        ) {
            (false, Some(active_checkpoint_commit_id)) => {
                let preconditions = hot_interval_local_preconditions(
                    &sorted,
                    &previous_values,
                    active_checkpoint_commit_id,
                )?;
                if preconditions.iter().any(|admitted| *admitted) {
                    Box::pin(hot_compaction_mask(
                        self.store,
                        branch_id,
                        generation,
                        &sorted,
                        Some(&preconditions),
                        HotTombstoneMaskKind::IntervalLocal,
                        self.transaction_global_schema_keys,
                    ))
                    .await?
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        };
        let identities = encode_hot_mutation_identities(branch_id, generation, &sorted);
        let unmatched_guards = if absence_guards_validated || absence_guards.is_empty() {
            BTreeSet::new()
        } else {
            let validated_delta_keys = sorted
                .iter()
                .map(|delta| TrackedStateKey {
                    schema_key: delta.schema_key.to_string(),
                    row_pk: delta.row_pk.clone(),
                    file_id: delta.file_id.map(str::to_string),
                })
                .collect::<BTreeSet<_>>();
            absence_guards
                .iter()
                .filter(|key| !validated_delta_keys.contains(*key))
                .cloned()
                .collect::<BTreeSet<_>>()
        };
        reject_hot_absence_guards(self.store, branch_id, generation, &unmatched_guards).await?;

        // Build every fallible output before mutating the write set. The
        // required primary-row batch above now supplies both mutation
        // validation and the checkpoint first-before decision.  `HOT_DIFF`
        // is an empty dirty-key index, so there is deliberately no second
        // point-read batch against it here.
        let mut next_coverage = *coverage;
        let diff_scope = working_diff_capture_checkpoint_commit_id.map(|checkpoint_commit_id| {
            encode_working_diff_scope_prefix(branch_id, checkpoint_commit_id, generation)
        });
        let diff_key_capacity = diff_scope.as_deref().map_or(0, |scope| {
            sorted
                .iter()
                .try_fold(0_usize, |total, delta| {
                    total.checked_add(encoded_hot_identity_key_len(
                        scope.len(),
                        delta.schema_key,
                        delta.row_pk,
                        delta.file_id,
                    )?)
                })
                .unwrap_or(0)
        });
        let mut diff_key_bytes = Vec::with_capacity(diff_key_capacity);
        let mut diff_puts = Vec::with_capacity(diff_scope.as_ref().map_or(0, |_| sorted.len()));
        let next_value_capacity = sorted
            .iter()
            .zip(&previous_values)
            .try_fold(0_usize, |total, (delta, previous)| {
                let inherited_coordinate = previous.as_ref().is_some_and(|previous| {
                    previous
                        .view()
                        .expect("HOT predecessor was validated before capacity planning")
                        .columnar_base_coordinate
                        .is_some()
                });
                checked_add_hot_next_value_capacity(
                    total,
                    delta,
                    working_diff_capture_checkpoint_commit_id.is_some(),
                    inherited_coordinate,
                )
            })
            // Preserve the encoder's fallible behavior for impossible input:
            // overflow must not turn into an attempted `usize::MAX`
            // allocation. The row encoder will report its normal length
            // error while this arena falls back to ordinary growth.
            .unwrap_or(0);
        let mut next_value_ranges = Vec::with_capacity(sorted.len());
        let mut next_value_bytes = Vec::with_capacity(next_value_capacity);
        {
            let _span = tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.materialization.hot.values"
            )
            .entered();
            for (index, (delta, (created_at, previous))) in sorted
                .iter()
                .zip(created_ats.iter().zip(&previous_values))
                .enumerate()
            {
                // Ordinary commits have no active checkpoint, so their baseline
                // is always disabled. Do not decode the row a second time merely
                // to rediscover that fact; the first decode above already handled
                // retention validation and `created_at` preservation.
                let (working_diff_baseline, newly_dirty) = if reset_working_diff_baselines
                    && !delta.untracked
                {
                    (WorkingDiffBaseline::Clean, false)
                } else if rebase_working_diff_baselines && !delta.untracked {
                    let previous = previous
                        .as_ref()
                        .map(CertifiedCurrentStatePredecessor::view)
                        .transpose()?
                        .ok_or_else(|| {
                            head_value_error(
                                "partial-checkpoint remainder has no HOT before-image",
                            )
                        })?;
                    rebase_hot_working_diff_baseline(
                        working_diff_capture_checkpoint_commit_id
                            .expect("partial-checkpoint rebase requires a checkpoint"),
                        previous,
                    )?
                } else if working_diff_capture_checkpoint_commit_id.is_some() && !delta.untracked {
                    let previous = previous
                        .as_ref()
                        .map(CertifiedCurrentStatePredecessor::view)
                        .transpose()?;
                    next_hot_working_diff_baseline(
                        working_diff_capture_checkpoint_commit_id,
                        delta,
                        previous,
                    )?
                } else {
                    (WorkingDiffBaseline::Disabled, false)
                };
                if newly_dirty {
                    let key = append_hot_diff_key_parts(
                        &mut diff_key_bytes,
                        diff_scope
                            .as_deref()
                            .expect("a newly dirty hot row requires an active checkpoint"),
                        delta.schema_key,
                        delta.row_pk,
                        delta.file_id,
                    );
                    next_coverage
                        .add_encoded_group_key(&diff_key_bytes[key.clone()])
                        .ok_or_else(|| {
                            head_value_error("hot working-diff index count exceeds u64")
                        })?;
                    diff_puts.push(EncodedPut {
                        key: buffer_range(&key),
                        value: BufferRange::default(),
                    });
                }
                let interval_local_row = interval_local.get(index).copied().unwrap_or(false);
                if interval_local_row
                    && (newly_dirty
                        || !matches!(
                            working_diff_baseline,
                            WorkingDiffBaseline::BeforeAbsent { .. }
                        ))
                {
                    // Gate (c) for the elision route, and the reason the
                    // precondition is allowed to be computed from the
                    // pre-image alone: this is where the cheap predicate is
                    // reconciled against the baseline the loop actually
                    // derived. `BeforeAbsent` with `newly_dirty == false` is
                    // the one state whose owed before-image is absence, so it
                    // is the one state in which no row need survive. Any other
                    // pairing means the two disagree, and a publication that
                    // drops a row whose before-image is still owed is worse
                    // than a publication that fails.
                    return Err(head_value_error(
                        "interval-local hot tombstone is not provably net-absent at elision",
                    ));
                }
                let compacted_row = compacted.get(index).copied().unwrap_or(false);
                if compacted_row
                    && (working_diff_baseline != WorkingDiffBaseline::Clean || newly_dirty)
                {
                    // Gate (c), asserted rather than assumed. Compaction runs
                    // only under `reset_working_diff_baselines`, which forces
                    // `(Clean, false)` for every tracked delta, so a compacted
                    // row is provably clean at the moment it is removed and
                    // "a dirty row cannot vanish" survives. If that stops
                    // holding, fail the publication rather than drop a row
                    // whose before-image is still owed.
                    return Err(head_value_error(
                        "compacted hot tombstone is not provably clean at removal",
                    ));
                }
                next_value_ranges.push(
                    if delta.physically_deletes() || compacted_row || interval_local_row {
                        None
                    } else {
                        let mut value = delta.value_ref(*created_at, working_diff_baseline);
                        value.columnar_base_coordinate = next_columnar_base_coordinate(
                            reset_working_diff_baselines,
                            delta,
                            previous.as_ref(),
                        )?;
                        Some(append_head_value(&mut next_value_bytes, &value)?)
                    },
                );
            }
        }
        let next_value_bytes = Bytes::from(next_value_bytes);
        stage_incremental_collection_controls(
            self.writes,
            branch_id,
            generation,
            &sorted,
            &previous_values,
            collection_controls,
            certified_live_increments,
        )?;
        // The incremental path above drops the ordered identity digest, which
        // an exact-closure scope needs in order to prove absence. Recompute
        // the one affected control from its pre-image plus the values staged
        // here. Bounded by that scope, not by the branch's untracked
        // population.
        if sorted
            .iter()
            .any(|delta| scope_requires_exact_closure(branch_id, delta.schema_key, delta.file_id))
        {
            let staged = sorted
                .iter()
                .zip(&next_value_ranges)
                .filter(|(delta, _)| {
                    scope_requires_exact_closure(branch_id, delta.schema_key, delta.file_id)
                })
                .map(|(delta, range)| {
                    (
                        HeadRowIdentity {
                            schema_key: delta.schema_key.to_owned(),
                            row_pk: delta.row_pk.clone(),
                            file_id: delta.file_id.map(str::to_owned),
                        },
                        range
                            .as_ref()
                            .map(|range| next_value_bytes.slice(range.clone())),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            Box::pin(restage_exact_closure_collection_control(
                self.store,
                self.writes,
                branch_id,
                generation,
                &staged,
            ))
            .await?;
        }

        async {
            stage_hot_diff_batch(
                self.writes,
                diff_scope.as_deref().unwrap_or_default(),
                diff_key_bytes,
                diff_puts,
            )?;
            stage_hot_mutation_batch(self.writes, identities, next_value_bytes, next_value_ranges);
            stage_incremental_file_delete_cascades(
                self.store,
                self.writes,
                branch_id,
                generation,
                &sorted,
                working_diff_capture_checkpoint_commit_id,
                reset_working_diff_baselines,
                &mut next_coverage,
            )
            .await
        }
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.materialization.hot.stage"
        ))
        .await?;
        *coverage = next_coverage;
        Ok(generation)
    }

    /// Publishes a complete replacement generation for a lifecycle event.
    ///
    /// The supplied snapshot is the target commit's tracked portion.  Any
    /// branch-local untracked rows are copied from the previous generation,
    /// then this transaction's untracked mutations are applied before its
    /// tracked mutations.  That order admits the one legitimate mixed case:
    /// deleting an untracked row and selecting a tracked row with the same
    /// identity in the same atomic publication.  Every other retention
    /// collision fails before the new control can become visible.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn stage_complete_current_state_with_working_diff(
        &mut self,
        branch_id: &str,
        generation: CommitId,
        parent_tracked: HotTrackedSnapshot,
        preserved_untracked_generation: Option<CommitId>,
        tracked_deltas: &[CurrentStateDeltaRef<'_>],
        untracked_deltas: &[CurrentStateDeltaRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
        working_diff_mode: CompleteWorkingDiffMode,
        coverage: &mut WorkingDiffIndexCoverage,
    ) -> Result<(HotTrackedSnapshot, BTreeSet<String>), LixError> {
        let mut rows = parent_tracked.rows;
        let mut untracked_rows = match preserved_untracked_generation {
            Some(previous_generation) => {
                load_hot_untracked_generation(self.store, branch_id, previous_generation).await?
            }
            None => BTreeMap::new(),
        };

        let sorted_untracked = sorted_lifecycle_hot_deltas(untracked_deltas, true)?;
        let sorted_tracked = sorted_lifecycle_hot_deltas(tracked_deltas, false)?;
        reject_lifecycle_retention_collisions(&sorted_untracked, &sorted_tracked)?;

        for delta in &sorted_untracked {
            apply_complete_hot_snapshot_delta(&mut untracked_rows, delta, absence_guards)?;
        }
        merge_final_untracked_rows(&mut rows, untracked_rows)?;
        for delta in &sorted_tracked {
            apply_complete_hot_snapshot_delta(&mut rows, delta, absence_guards)?;
        }

        match working_diff_mode {
            CompleteWorkingDiffMode::Disabled => {
                normalize_complete_hot_snapshot_baselines(&mut rows, WorkingDiffBaseline::Disabled)?
            }
            CompleteWorkingDiffMode::ResetClean => {
                normalize_complete_hot_snapshot_baselines(&mut rows, WorkingDiffBaseline::Clean)?
            }
            CompleteWorkingDiffMode::Rebase {
                checkpoint_commit_id,
                checkpoint,
            } => rebase_complete_hot_snapshot_working_diff(
                self.writes,
                branch_id,
                generation,
                checkpoint_commit_id,
                &mut rows,
                checkpoint.rows,
                coverage,
            )?,
        }

        let mut final_tracked = BTreeMap::new();
        let mut schema_keys = BTreeSet::new();
        for (identity, bytes) in &rows {
            schema_keys.insert(identity.schema_key.clone());
            if !decode_head_value(bytes.as_ref())?.untracked {
                final_tracked.insert(identity.clone(), bytes.clone());
            }
        }

        stage_complete_collection_controls(self.writes, branch_id, generation, &rows)?;
        stage_complete_hot_rows(self.writes, branch_id, generation, rows);
        Ok((
            HotTrackedSnapshot {
                rows: final_tracked,
            },
            schema_keys,
        ))
    }
}

#[allow(clippy::too_many_arguments)]
fn rebase_complete_hot_snapshot_working_diff(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    generation: CommitId,
    checkpoint_commit_id: CommitId,
    rows: &mut HotRowMap,
    checkpoint_rows: HotRowMap,
    coverage: &mut WorkingDiffIndexCoverage,
) -> Result<(), LixError> {
    *coverage = WorkingDiffIndexCoverage::default();
    let scope = encode_working_diff_scope_prefix(branch_id, checkpoint_commit_id, generation);
    let mut diff_key_bytes = Vec::new();
    let mut diff_puts = Vec::new();

    for (identity, bytes) in rows.iter_mut() {
        let after = decode_head_value(bytes.as_ref())?;
        if after.untracked {
            continue;
        }
        let before = checkpoint_rows
            .get(identity)
            .map(|bytes| decode_head_value(bytes.as_ref()))
            .transpose()?;
        let before_version = before.and_then(HeadValueView::working_diff_version);
        let after_version = after
            .working_diff_version()
            .ok_or_else(|| head_value_error("complete tracked row has no working-diff version"))?;
        let (baseline, dirty) = match before_version {
            Some(before) if before.change_id == after_version.change_id => {
                (WorkingDiffBaseline::Clean, false)
            }
            Some(before) => (
                WorkingDiffBaseline::BeforePresent {
                    checkpoint_commit_id,
                    version: before,
                },
                true,
            ),
            None => (
                WorkingDiffBaseline::BeforeAbsent {
                    checkpoint_commit_id,
                },
                !after.deleted,
            ),
        };
        *bytes = Bytes::from(reencode_head_value_with_baseline(after, baseline)?);
        if dirty {
            let key = append_hot_diff_key_parts(
                &mut diff_key_bytes,
                &scope,
                &identity.schema_key,
                &identity.row_pk,
                identity.file_id.as_deref(),
            );
            coverage
                .add_encoded_group_key(&diff_key_bytes[key.clone()])
                .ok_or_else(|| head_value_error("hot working-diff index count exceeds u64"))?;
            diff_puts.push(EncodedPut {
                key: buffer_range(&key),
                value: BufferRange::default(),
            });
        }
    }
    stage_hot_diff_batch(writes, &scope, diff_key_bytes, diff_puts)
}

#[allow(clippy::too_many_arguments)]
async fn stage_incremental_file_delete_cascades(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    branch_id: &str,
    generation: CommitId,
    deltas: &[&CurrentStateDeltaRef<'_>],
    working_diff_capture_checkpoint_commit_id: Option<CommitId>,
    reset_working_diff_baselines: bool,
    coverage: &mut WorkingDiffIndexCoverage,
) -> Result<(), LixError> {
    let mut cascades = BTreeMap::<String, &CurrentStateDeltaRef<'_>>::new();
    for cascade in deltas {
        let Some(file_id) = file_delete_cascade_id(cascade)? else {
            continue;
        };
        cascades.insert(file_id, cascade);
    }
    if cascades.is_empty() {
        return Ok(());
    }
    #[cfg(test)]
    INCREMENTAL_CASCADE_EXPLICIT_INDEX_BUILDS.with(|builds| {
        builds.set(builds.get().saturating_add(1));
    });
    let explicit = deltas
        .iter()
        .map(|delta| HeadRowIdentity {
            schema_key: delta.schema_key.to_string(),
            row_pk: delta.row_pk.clone(),
            file_id: delta.file_id.map(str::to_string),
        })
        .collect::<BTreeSet<_>>();
    let identities =
        hot_load_file_scope_identities(store, branch_id, generation, &cascades).await?;
    let values = hot_load_primary_identity_bytes(store, &identities).await?;
    let scope = hot_scope_prefix(branch_id, generation);
    let key_capacity = identities
        .iter()
        .try_fold(0_usize, |total, identity| {
            let key_len = encoded_hot_identity_key_len(
                scope.len(),
                &identity.schema_key,
                &identity.row_pk,
                identity.file_id.as_deref(),
            )?;
            total.checked_add(key_len)
        })
        .unwrap_or(0);
    let mut mutations = HotCascadeMutationBuffers::with_capacity(
        identities.len(),
        key_capacity,
        working_diff_capture_checkpoint_commit_id.is_some(),
    );
    let diff_scope = working_diff_capture_checkpoint_commit_id.map(|checkpoint_commit_id| {
        encode_working_diff_scope_prefix(branch_id, checkpoint_commit_id, generation)
    });
    let diff_key_capacity = diff_scope.as_deref().map_or(0, |scope| {
        identities
            .iter()
            .try_fold(0_usize, |total, identity| {
                total.checked_add(encoded_hot_identity_key_len(
                    scope.len(),
                    &identity.schema_key,
                    &identity.row_pk,
                    identity.file_id.as_deref(),
                )?)
            })
            .unwrap_or(0)
    });
    let mut diff_key_bytes = Vec::with_capacity(diff_key_capacity);
    let mut diff_puts = Vec::with_capacity(diff_scope.as_ref().map_or(0, |_| identities.len()));
    for (identity, previous) in identities.into_iter().zip(values) {
        let row_identity = identity.clone().into_row_identity();
        if explicit.contains(&row_identity) {
            continue;
        }
        let cascade = cascades
            .get(
                identity
                    .file_id
                    .as_deref()
                    .expect("file-backed identity requires file id"),
            )
            .expect("file scan only returns requested cascade ids");
        let Some(previous) = previous else {
            return Err(head_value_error(
                "hot file-backed identity has no authoritative primary row",
            ));
        };
        let existing = decode_head_value(&previous)?;
        // A file delete cascades only within its own lane. Since PR D a row and
        // its owning file are validated into the same lane, so the cross-lane
        // combination should never arrive here; skipping it is defence in
        // depth, not live behaviour.
        //
        // The `existing.untracked` branch below must NOT be deleted as dead
        // code. Both lanes share this one path, keyed on the row's own flag,
        // and it is the only mechanism that removes an untracked file's own
        // rows when that file is deleted. What the invariant removes is the
        // cross-lane pairing, which is this condition, not that branch.
        if cascade.untracked != existing.untracked || existing.deleted {
            continue;
        }
        let row_start = mutations.key_bytes.len();
        mutations.key_bytes.extend_from_slice(&scope);
        write_key_string(
            &mut mutations.key_bytes,
            &identity.schema_key,
            KEY_PART_FINAL,
        );
        write_file_id(&mut mutations.key_bytes, identity.file_id.as_deref());
        write_row_pk(&mut mutations.key_bytes, &identity.row_pk);
        let row_key = BufferRange::new(row_start, mutations.key_bytes.len() - row_start);
        if existing.untracked {
            mutations.row_deletes.push(row_key);
            continue;
        }
        let (baseline, newly_dirty) = if reset_working_diff_baselines {
            (WorkingDiffBaseline::Clean, false)
        } else {
            next_cascade_working_diff_baseline(working_diff_capture_checkpoint_commit_id, existing)?
        };
        if newly_dirty {
            let key = append_hot_diff_key_parts(
                &mut diff_key_bytes,
                diff_scope
                    .as_deref()
                    .expect("new cascade dirty row requires active checkpoint"),
                &identity.schema_key,
                &identity.row_pk,
                identity.file_id.as_deref(),
            );
            coverage
                .add_encoded_group_key(&diff_key_bytes[key.clone()])
                .ok_or_else(|| head_value_error("hot working-diff index count exceeds u64"))?;
            diff_puts.push(EncodedPut {
                key: buffer_range(&key),
                value: BufferRange::default(),
            });
        }
        let value = append_head_value(
            &mut mutations.value_bytes,
            &HeadValueRef {
                change_id: cascade.change_id,
                commit_id: cascade.commit_id,
                untracked: false,
                deleted: true,
                created_at: existing.created_at,
                updated_at: cascade.updated_at,
                snapshot: None,
                metadata: None,
                columnar_base_coordinate: existing.columnar_base_coordinate,
                working_diff_baseline: baseline,
            },
        )?;
        let value = buffer_range(&value);
        mutations.row_puts.push(EncodedPut {
            key: row_key,
            value,
        });
    }
    stage_hot_diff_batch(
        writes,
        diff_scope.as_deref().unwrap_or_default(),
        diff_key_bytes,
        diff_puts,
    )?;
    if !mutations.row_puts.is_empty() || !mutations.row_deletes.is_empty() {
        stage_hot_encoded_mutation_ranges(
            writes,
            Bytes::from(mutations.key_bytes),
            Bytes::from(mutations.value_bytes),
            mutations.row_puts,
            mutations.row_deletes,
            Vec::new(),
        );
    }
    Ok(())
}

struct HotCascadeMutationBuffers {
    key_bytes: Vec<u8>,
    value_bytes: Vec<u8>,
    row_puts: Vec<EncodedPut>,
    row_deletes: Vec<BufferRange>,
}

impl HotCascadeMutationBuffers {
    fn with_capacity(row_capacity: usize, key_capacity: usize, active_checkpoint: bool) -> Self {
        let checkpoint_bytes = if active_checkpoint {
            WORKING_DIFF_CHECKPOINT_BYTES + WORKING_DIFF_VERSION_BYTES
        } else {
            0
        };
        let value_bytes_per_row = HEAD_VALUE_TYPED_HEADER_BYTES
            .checked_add(checkpoint_bytes)
            .and_then(|bytes| bytes.checked_add(COLUMNAR_BASE_COORDINATE_BYTES));
        let value_capacity = value_bytes_per_row
            .and_then(|value_bytes| row_capacity.checked_mul(value_bytes))
            .unwrap_or(0);
        Self {
            key_bytes: Vec::with_capacity(key_capacity),
            value_bytes: Vec::with_capacity(value_capacity),
            row_puts: Vec::with_capacity(row_capacity),
            row_deletes: Vec::with_capacity(row_capacity),
        }
    }
}

#[cfg(test)]
std::thread_local! {
    static INCREMENTAL_CASCADE_EXPLICIT_INDEX_BUILDS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn incremental_cascade_explicit_index_builds() -> usize {
    INCREMENTAL_CASCADE_EXPLICIT_INDEX_BUILDS.with(std::cell::Cell::get)
}

fn next_cascade_working_diff_baseline(
    active_checkpoint_commit_id: Option<CommitId>,
    previous: HeadValueView<'_>,
) -> Result<(WorkingDiffBaseline, bool), LixError> {
    let Some(active_checkpoint_commit_id) = active_checkpoint_commit_id else {
        return Ok((WorkingDiffBaseline::Disabled, false));
    };
    match previous.working_diff_baseline {
        WorkingDiffBaseline::Clean => {
            let before = previous
                .working_diff_version()
                .ok_or_else(|| head_value_error("tracked cascade member has no version"))?;
            Ok((
                WorkingDiffBaseline::BeforePresent {
                    checkpoint_commit_id: active_checkpoint_commit_id,
                    version: before,
                },
                true,
            ))
        }
        WorkingDiffBaseline::BeforeAbsent {
            checkpoint_commit_id,
        } if checkpoint_commit_id == active_checkpoint_commit_id => Ok((
            WorkingDiffBaseline::BeforeAbsent {
                checkpoint_commit_id,
            },
            false,
        )),
        WorkingDiffBaseline::BeforePresent {
            checkpoint_commit_id,
            version,
        } if checkpoint_commit_id == active_checkpoint_commit_id => Ok((
            WorkingDiffBaseline::BeforePresent {
                checkpoint_commit_id,
                version,
            },
            false,
        )),
        WorkingDiffBaseline::BeforeAbsent { .. } | WorkingDiffBaseline::BeforePresent { .. } => {
            let mut before = previous
                .working_diff_version()
                .ok_or_else(|| head_value_error("tracked cascade member has no version"))?;
            before.created_at =
                effective_hot_created_at(previous, Some(active_checkpoint_commit_id));
            before.commit_id = active_checkpoint_commit_id;
            Ok((
                WorkingDiffBaseline::BeforePresent {
                    checkpoint_commit_id: active_checkpoint_commit_id,
                    version: before,
                },
                true,
            ))
        }
        WorkingDiffBaseline::Disabled => Err(head_value_error(
            "active checkpoint generation contains a cascade member without a baseline",
        )),
    }
}

/// Selects the baseline for one primary-row mutation and whether the sparse
/// dirty-key index needs its first entry. The current primary row is already
/// loaded for retention and insert validation, so this must remain purely
/// in-memory: adding a `HOT_DIFF` lookup here would recreate the avoidable
/// second read batch this layout removes.
fn next_hot_working_diff_baseline(
    active_checkpoint_commit_id: Option<CommitId>,
    delta: &CurrentStateDeltaRef<'_>,
    previous: Option<HeadValueView<'_>>,
) -> Result<(WorkingDiffBaseline, bool), LixError> {
    let Some(active_checkpoint_commit_id) = active_checkpoint_commit_id else {
        return Ok((WorkingDiffBaseline::Disabled, false));
    };
    if delta.untracked {
        return Ok((WorkingDiffBaseline::Disabled, false));
    }
    let Some(previous) = previous else {
        return Ok((
            WorkingDiffBaseline::BeforeAbsent {
                checkpoint_commit_id: active_checkpoint_commit_id,
            },
            true,
        ));
    };
    if previous.untracked {
        return Err(head_value_error(
            "tracked mutation has an untracked primary before image",
        ));
    }
    match previous.working_diff_baseline {
        WorkingDiffBaseline::Clean => {
            let before = previous
                .working_diff_version()
                .ok_or_else(|| head_value_error("tracked mutation has no working-diff version"))?;
            Ok((
                WorkingDiffBaseline::BeforePresent {
                    checkpoint_commit_id: active_checkpoint_commit_id,
                    version: before,
                },
                true,
            ))
        }
        WorkingDiffBaseline::BeforeAbsent {
            checkpoint_commit_id,
        } if checkpoint_commit_id == active_checkpoint_commit_id => Ok((
            WorkingDiffBaseline::BeforeAbsent {
                checkpoint_commit_id,
            },
            false,
        )),
        WorkingDiffBaseline::BeforePresent {
            checkpoint_commit_id,
            version,
        } if checkpoint_commit_id == active_checkpoint_commit_id => Ok((
            WorkingDiffBaseline::BeforePresent {
                checkpoint_commit_id,
                version,
            },
            false,
        )),
        WorkingDiffBaseline::BeforeAbsent { .. } | WorkingDiffBaseline::BeforePresent { .. } => {
            let mut before = previous
                .working_diff_version()
                .ok_or_else(|| head_value_error("tracked mutation has no working-diff version"))?;
            before.created_at =
                effective_hot_created_at(previous, Some(active_checkpoint_commit_id));
            before.commit_id = active_checkpoint_commit_id;
            Ok((
                WorkingDiffBaseline::BeforePresent {
                    checkpoint_commit_id: active_checkpoint_commit_id,
                    version: before,
                },
                true,
            ))
        }
        WorkingDiffBaseline::Disabled => Err(head_value_error(
            "active checkpoint generation contains a tracked row without a baseline",
        )),
    }
}

fn rebase_hot_working_diff_baseline(
    checkpoint_commit_id: CommitId,
    previous: HeadValueView<'_>,
) -> Result<(WorkingDiffBaseline, bool), LixError> {
    match previous.working_diff_baseline {
        WorkingDiffBaseline::BeforeAbsent { .. } => Ok((
            WorkingDiffBaseline::BeforeAbsent {
                checkpoint_commit_id,
            },
            true,
        )),
        WorkingDiffBaseline::BeforePresent { version, .. } => Ok((
            WorkingDiffBaseline::BeforePresent {
                checkpoint_commit_id,
                version,
            },
            true,
        )),
        WorkingDiffBaseline::Clean | WorkingDiffBaseline::Disabled => Err(head_value_error(
            "partial-checkpoint remainder is not dirty in the previous epoch",
        )),
    }
}

fn next_columnar_base_coordinate(
    reset_working_diff_baselines: bool,
    delta: &CurrentStateDeltaRef<'_>,
    previous: Option<&CertifiedCurrentStatePredecessor>,
) -> Result<Option<ColumnarBaseCoordinate>, LixError> {
    if reset_working_diff_baselines {
        return Ok(None);
    }
    Ok(delta.columnar_base_coordinate.or(previous
        .map(CertifiedCurrentStatePredecessor::view)
        .transpose()?
        .and_then(|value| value.columnar_base_coordinate)))
}

async fn load_hot_untracked_generation(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
) -> Result<HotRowMap, LixError> {
    let filter = TrackedStateFilter {
        include_tombstones: true,
        ..TrackedStateFilter::default()
    };
    let HotScanEntries::Decoded(entries) =
        hot_scan_entries(store, branch_id, generation, &filter, None, None)
            .await?
            .expect("unbounded HOT scan cannot exhaust a byte budget")
    else {
        unreachable!("an unconstrained HOT scan cannot select the finite point-read route");
    };
    let mut rows = BTreeMap::new();
    for (identity, bytes) in entries {
        let value = decode_head_value(bytes.as_ref())?;
        if !value.untracked {
            continue;
        }
        if value.deleted {
            return Err(head_value_error(
                "untracked hot row must be physically removed rather than tombstoned",
            ));
        }
        match rows.entry(identity.into_row_identity()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(bytes);
            }
            std::collections::btree_map::Entry::Occupied(entry) => {
                let identity = entry.key();
                return Err(LixError::new(
                    LixError::CODE_UNIQUE,
                    format!(
                        "hot generation contains duplicate untracked identity in schema '{}' row_pk {:?}",
                        identity.schema_key, identity.row_pk
                    ),
                ));
            }
        }
    }
    Ok(rows)
}

fn merge_final_untracked_rows(
    rows: &mut HotRowMap,
    untracked_rows: HotRowMap,
) -> Result<(), LixError> {
    for (identity, bytes) in untracked_rows {
        match rows.entry(identity) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(bytes);
            }
            std::collections::btree_map::Entry::Occupied(entry) => {
                let identity = entry.key();
                return Err(LixError::new(
                    LixError::CODE_UNIQUE,
                    format!(
                        "cannot materialize tracked and untracked hot rows with the same identity in schema '{}' row_pk {:?}",
                        identity.schema_key, identity.row_pk
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn sorted_lifecycle_hot_deltas<'a>(
    deltas: &'a [CurrentStateDeltaRef<'a>],
    expect_untracked: bool,
) -> Result<Vec<&'a CurrentStateDeltaRef<'a>>, LixError> {
    let mut sorted = Vec::with_capacity(deltas.len());
    for delta in deltas {
        delta.validate()?;
        if delta.untracked != expect_untracked {
            return Err(head_value_error(if expect_untracked {
                "untracked lifecycle delta was marked tracked"
            } else {
                "tracked lifecycle delta was marked untracked"
            }));
        }
        sorted.push(delta);
    }
    sorted.sort_unstable_by(|left, right| compare_hot_deltas(left, right));
    for pair in sorted.windows(2) {
        if compare_hot_deltas(pair[0], pair[1]).is_eq() {
            return Err(current_state_duplicate_delta_error(pair[1]));
        }
    }
    Ok(sorted)
}

fn reject_lifecycle_retention_collisions(
    untracked: &[&CurrentStateDeltaRef<'_>],
    tracked: &[&CurrentStateDeltaRef<'_>],
) -> Result<(), LixError> {
    let mut untracked_index = 0;
    let mut tracked_index = 0;
    while untracked_index < untracked.len() && tracked_index < tracked.len() {
        match compare_hot_deltas(untracked[untracked_index], tracked[tracked_index]) {
            Ordering::Less => untracked_index += 1,
            Ordering::Greater => tracked_index += 1,
            Ordering::Equal => {
                if !untracked[untracked_index].physically_deletes() {
                    return Err(current_state_duplicate_delta_error(tracked[tracked_index]));
                }
                untracked_index += 1;
                tracked_index += 1;
            }
        }
    }
    Ok(())
}

fn apply_complete_hot_snapshot_delta(
    rows: &mut HotRowMap,
    delta: &CurrentStateDeltaRef<'_>,
    absence_guards: &BTreeSet<TrackedStateKey>,
) -> Result<(), LixError> {
    apply_complete_file_delete_cascade(rows, delta)?;
    let identity = HeadRowIdentity {
        schema_key: delta.schema_key.to_string(),
        row_pk: delta.row_pk.clone(),
        file_id: delta.file_id.map(str::to_string),
    };
    let previous = rows.get(&identity).map(|bytes| bytes.as_ref());
    if let Some(previous) = previous {
        let existing = decode_head_value(previous)?;
        reject_guarded_live_member(absence_guards, delta, existing)?;
        reject_retention_change(delta, existing)?;
    }
    if delta.physically_deletes() {
        rows.remove(&identity);
    } else {
        let created_at = previous
            .map(decode_head_value)
            .transpose()?
            .map_or(delta.created_at, |value| value.created_at);
        rows.insert(
            identity,
            Bytes::from(encode_head_value(&{
                let mut value = delta.value_ref(created_at, WorkingDiffBaseline::Disabled);
                value.columnar_base_coordinate = None;
                value
            })?),
        );
    }
    Ok(())
}

fn apply_complete_file_delete_cascade(
    rows: &mut HotRowMap,
    delta: &CurrentStateDeltaRef<'_>,
) -> Result<(), LixError> {
    let Some(file_id) = file_delete_cascade_id(delta)? else {
        return Ok(());
    };
    let identities = rows
        .keys()
        .filter(|identity| identity.file_id.as_deref() == Some(file_id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    for identity in identities {
        let Some(previous) = rows.get(&identity) else {
            continue;
        };
        let existing = decode_head_value(previous.as_ref())?;
        // Same-lane cascade only; see the note on the incremental cascade. The
        // `existing.untracked` branch below is the untracked lane's own removal
        // mechanism and must not be deleted as dead code.
        if delta.untracked != existing.untracked || existing.deleted {
            continue;
        }
        if existing.untracked {
            rows.remove(&identity);
            continue;
        }
        rows.insert(
            identity,
            Bytes::from(encode_head_value(&HeadValueRef {
                change_id: delta.change_id,
                commit_id: delta.commit_id,
                untracked: false,
                deleted: true,
                created_at: existing.created_at,
                updated_at: delta.updated_at,
                snapshot: None,
                metadata: None,
                columnar_base_coordinate: existing.columnar_base_coordinate,
                working_diff_baseline: WorkingDiffBaseline::Disabled,
            })?),
        );
    }
    Ok(())
}

fn file_delete_cascade_id(delta: &CurrentStateDeltaRef<'_>) -> Result<Option<String>, LixError> {
    if delta.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY || !delta.deleted {
        return Ok(None);
    }
    delta
        .row_pk
        .as_single_string_owned()
        .map(Some)
        .map_err(|error| {
            head_value_error(&format!(
                "file descriptor tombstone has invalid identity: {error}"
            ))
        })
}

fn normalize_complete_hot_snapshot_baselines(
    rows: &mut HotRowMap,
    tracked_baseline: WorkingDiffBaseline,
) -> Result<(), LixError> {
    for bytes in rows.values_mut() {
        let value = decode_head_value(bytes.as_ref())?;
        if value.untracked {
            continue;
        }
        *bytes = Bytes::from(reencode_head_value_with_baseline(value, tracked_baseline)?);
    }
    Ok(())
}

fn compare_hot_deltas(
    left: &CurrentStateDeltaRef<'_>,
    right: &CurrentStateDeltaRef<'_>,
) -> Ordering {
    left.schema_key
        .cmp(right.schema_key)
        .then_with(|| left.row_pk.cmp(right.row_pk))
        .then_with(|| left.file_id.cmp(&right.file_id))
}

fn compare_certified_predecessor_to_delta(
    predecessor: &CertifiedCurrentStatePredecessorRef<'_>,
    delta: &CurrentStateDeltaRef<'_>,
) -> Ordering {
    predecessor
        .schema_key
        .cmp(delta.schema_key)
        .then_with(|| predecessor.row_pk.cmp(delta.row_pk))
        .then_with(|| predecessor.file_id.cmp(&delta.file_id))
}

fn hot_identity(
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
    row_pk: &RowPk,
    file_id: Option<&str>,
) -> HeadIdentity {
    HeadIdentity {
        branch_id: branch_id.to_string(),
        generation,
        schema_key: schema_key.to_string(),
        row_pk: row_pk.clone(),
        file_id: file_id.map(str::to_string),
    }
}

struct EncodedHotMutationIdentities {
    key_bytes: Bytes,
    key_ranges: Vec<EncodedHotMutationIdentityRanges>,
}

#[derive(Clone, Copy)]
struct EncodedHotMutationIdentityRanges {
    row_key: BufferRange,
    file_schema_key: Option<BufferRange>,
}

fn encode_hot_mutation_identities(
    branch_id: &str,
    generation: CommitId,
    deltas: &[&CurrentStateDeltaRef<'_>],
) -> EncodedHotMutationIdentities {
    let scope = hot_scope_prefix(branch_id, generation);
    let encoded_capacity = encoded_hot_mutation_identity_capacity(scope.len(), deltas).unwrap_or(0);
    let mut encoded = Vec::with_capacity(encoded_capacity);
    let mut key_ranges = Vec::with_capacity(deltas.len());
    for delta in deltas {
        key_ranges.push(append_hot_mutation_identity(&mut encoded, &scope, delta));
    }
    EncodedHotMutationIdentities {
        key_bytes: Bytes::from(encoded),
        key_ranges,
    }
}

fn encoded_hot_mutation_identity_capacity(
    scope_len: usize,
    deltas: &[&CurrentStateDeltaRef<'_>],
) -> Option<usize> {
    deltas.iter().try_fold(0_usize, |total, delta| {
        let key_len =
            encoded_hot_identity_key_len(scope_len, delta.schema_key, delta.row_pk, delta.file_id)?;
        let marker_len = if delta.file_id.is_some() {
            scope_len.checked_add(encoded_key_bytes_len(delta.schema_key.as_bytes())?)?
        } else {
            0
        };
        total.checked_add(key_len)?.checked_add(marker_len)
    })
}

fn append_hot_mutation_identity(
    encoded: &mut Vec<u8>,
    scope: &[u8],
    delta: &CurrentStateDeltaRef<'_>,
) -> EncodedHotMutationIdentityRanges {
    let row_start = encoded.len();
    encoded.extend_from_slice(scope);
    write_key_string(encoded, delta.schema_key, KEY_PART_FINAL);
    write_file_id(encoded, delta.file_id);
    write_row_pk(encoded, delta.row_pk);
    let row_key = BufferRange::new(row_start, encoded.len() - row_start);

    let file_schema_key = delta.file_id.map(|_| {
        let marker_start = encoded.len();
        encoded.extend_from_slice(scope);
        write_key_string(encoded, delta.schema_key, KEY_PART_FINAL);
        BufferRange::new(marker_start, encoded.len() - marker_start)
    });
    EncodedHotMutationIdentityRanges {
        row_key,
        file_schema_key,
    }
}

async fn hot_load_primary_mutation_identity_refs(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: &EncodedHotMutationIdentities,
    deltas: &[&CurrentStateDeltaRef<'_>],
    durable_predecessors: &[Option<CertifiedCurrentStatePredecessor>],
    absence_guards_validated: bool,
    validated_absent_file_id: Option<&str>,
) -> Result<Vec<Option<CertifiedCurrentStatePredecessor>>, LixError> {
    assert_eq!(
        identities.key_ranges.len(),
        deltas.len(),
        "every hot mutation identity must have one source delta"
    );
    assert_eq!(
        durable_predecessors.len(),
        deltas.len(),
        "every hot mutation identity must have one predecessor slot"
    );
    let read_count = deltas
        .iter()
        .zip(durable_predecessors)
        .filter(|(delta, durable_predecessor)| {
            durable_predecessor.is_none()
                && !hot_delta_is_guarded_by_absent_file(
                    delta,
                    absence_guards_validated,
                    validated_absent_file_id,
                )
        })
        .count();
    if read_count == 0 {
        return Ok(Vec::new());
    }
    if read_count == deltas.len()
        && let Some(values) = hot_scan_dense_mutation_identity_range(store, identities).await?
    {
        return Ok(values
            .into_iter()
            .map(|value| value.map(CertifiedCurrentStatePredecessor::Encoded))
            .collect());
    }
    let mut keys = Vec::with_capacity(read_count);
    for ((identity, delta), durable_predecessor) in identities
        .key_ranges
        .iter()
        .zip(deltas)
        .zip(durable_predecessors)
    {
        if durable_predecessor.is_some()
            || hot_delta_is_guarded_by_absent_file(
                delta,
                absence_guards_validated,
                validated_absent_file_id,
            )
        {
            continue;
        }
        let start = identity.row_key.offset();
        keys.push(StorageKey(
            identities
                .key_bytes
                .slice(start..start + identity.row_key.len()),
        ));
    }
    PointReadPlan::new(ROW_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .map(|value| {
            value
                .map(full_value_bytes)
                .transpose()
                .map(|value| value.map(CertifiedCurrentStatePredecessor::Encoded))
        })
        .collect()
}

async fn hot_scan_dense_mutation_identity_range(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: &EncodedHotMutationIdentities,
) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
    hot_scan_dense_encoded_key_range(store, identities.key_ranges.len(), |index| {
        let range = identities.key_ranges[index].row_key;
        let start = range.offset();
        &identities.key_bytes[start..start.saturating_add(range.len())]
    })
    .await
}

fn hot_delta_is_guarded_by_absent_file(
    delta: &CurrentStateDeltaRef<'_>,
    absence_guards_validated: bool,
    validated_absent_file_id: Option<&str>,
) -> bool {
    absence_guards_validated
        && validated_absent_file_id.is_some_and(|file_id| delta.file_id == Some(file_id))
}

/// Adds one encoded current-state value to the shared arena's capacity plan.
///
/// Ordinary commits are exact. During an active checkpoint, a tracked row
/// may carry a fixed-size first-before image, so the plan reserves that upper
/// bound without decoding every predecessor a third time. Physical untracked
/// deletes produce no value. Every operation is checked so an impossible
/// batch can safely use a zero-capacity growth fallback and reach the normal
/// fallible encoder instead of attempting an overflowing allocation.
fn checked_add_hot_next_value_capacity(
    total: usize,
    delta: &CurrentStateDeltaRef<'_>,
    active_checkpoint: bool,
    inherited_coordinate: bool,
) -> Option<usize> {
    if delta.physically_deletes() {
        return Some(total);
    }
    let (snapshot_len, metadata_len) = if delta.deleted {
        (0, 0)
    } else {
        (
            delta.snapshot.map_or(0, <[u8]>::len),
            delta
                .metadata
                .map(lix_schema::Jsonb::binary_len)
                .transpose()
                .ok()?
                .unwrap_or(0),
        )
    };
    // Keep the plan bounded by the same on-disk u32 fields the encoder checks.
    u32::try_from(snapshot_len).ok()?;
    u32::try_from(metadata_len).ok()?;
    let baseline_len = if active_checkpoint && !delta.untracked {
        WORKING_DIFF_CHECKPOINT_BYTES + WORKING_DIFF_VERSION_BYTES
    } else {
        0
    };
    let encoded_len = HEAD_VALUE_TYPED_HEADER_BYTES
        .checked_add(snapshot_len)?
        .checked_add(metadata_len)?
        .checked_add(baseline_len)?
        .checked_add(
            (delta.columnar_base_coordinate.is_some() || inherited_coordinate)
                .then_some(COLUMNAR_BASE_COORDINATE_BYTES)
                .unwrap_or(0),
        )?;
    total.checked_add(encoded_len)
}

/// Whether the generation still has any immutable base beneath its HOT rows.
///
/// `ROW_SPACE` is a sparse overlay. A tombstone is what shadows a base row for
/// the same identity, so every plane that can serve a row for this generation
/// has to be proven empty before a tombstone can be treated as dead weight.
/// The planes are the same three `has_schema_rows` consults, minus `ROW_SPACE`
/// itself: the packed current base, its exclusive-schema variant, and the
/// sparse root base.
///
/// This is one existence probe per plane per publication, not per identity.
async fn hot_generation_has_any_base(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
) -> Result<bool, LixError> {
    if !packed_current_base_refs(store, branch_id, generation)
        .await?
        .is_empty()
    {
        return Ok(true);
    }
    if load_root_current_base_commit(store, branch_id, generation)
        .await?
        .is_some()
    {
        return Ok(true);
    }
    if hot_space_prefix_has_entry(
        store,
        PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE,
        Bytes::from(hot_scope_prefix(branch_id, generation)),
    )
    .await?
    {
        return Ok(true);
    }
    Ok(false)
}

/// Key-only existence probe for one prefix in one space.
async fn hot_space_prefix_has_entry(
    store: &(impl StorageAdapterRead + ?Sized),
    space: StorageSpace,
    prefix: Bytes,
) -> Result<bool, LixError> {
    let range = StoragePrefix { bytes: prefix }.to_range()?;
    let mut cursor = store
        .begin_scan(
            space,
            range,
            StorageBeginScanOptions {
                projection: StorageCoreProjection::KeyOnly,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    let (page, _has_more) = cursor.next_page(1).await?.into_parts();
    Ok(!page.is_empty())
}

/// Which of `deltas` may have their serving-view tombstone removed outright
/// rather than republished as a tombstone.
///
/// `ROW_SPACE` is a derived serving view, so nothing here is canonical: a
/// removed tombstone stays recoverable from history, and `undo` replays from
/// canonical state rather than from the serving row. The only question is
/// whether removing it changes an *answer*, which happens exactly when
/// something below the tombstone would resurface.
///
/// Two things can sit below one:
///
/// - **(a) a base for the same generation.** Ruled out wholesale by
///   [`hot_generation_has_any_base`]. A checkpoint publishes no base of its
///   own, so this is the ordinary state at a checkpoint rather than a rare one.
/// - **(b) a global-branch row for the same identity.** The global branch sits
///   beneath every other branch, and a branch tombstone is what hides a global
///   row from that branch — the behaviour
///   `main_tombstone_hides_global_row` and its four siblings pin. Ruled out
///   per *schema key*, against both what is already durable
///   (`has_schema_rows`, which is base-complete) and what this transaction is
///   publishing on global (`transaction_global_schema_keys`). Per-schema
///   granularity is not an optimization: a checkpoint always stages its own
///   global `lix_checkpoint` row, so a transaction-wide global test would
///   block every checkpoint and make this pass silently inert.
///
/// A third condition — the delete still being observable through its
/// working-diff baseline — does not exist. `DIFF_SPACE`, not `ROW_SPACE`, is
/// the working diff's authority; `hot_row_tombstone_probe`'s `near_miss` arm
/// measures that and keeps an inverted assertion so a change that reintroduces
/// the dependency fails loudly.
///
/// Callers must pass the deltas *after* the checkpoint retain filter, since
/// that filter rebinds the slice and an earlier mask would misalign.
/// Which of the two tombstone-removal routes a mask is being built for.
///
/// The two share every gate and differ only in *when* they run and which
/// deltas they admit, so they share one implementation and separate counters.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum HotTombstoneMaskKind {
    /// A checkpoint publication reclaiming tombstones it has just discharged.
    Checkpoint,
    /// An ordinary commit declining to create a tombstone for an identity that
    /// was created and deleted inside the open checkpoint interval.
    IntervalLocal,
}

/// Rows an interval-local elision may consider, decided from the *pre-image*
/// alone and therefore without any store read.
///
/// A tracked delete is interval-local exactly when the row it replaces already
/// carries `BeforeAbsent` for the currently active checkpoint: that baseline
/// means "the first mutation after the checkpoint created this identity", so a
/// delete of it is net-absent against the interval baseline. Phase 12 measured
/// the consequence directly - `lix_working_diff` reports **nothing** for such
/// an identity while its tombstone exists - which is what makes the tombstone
/// owed to nobody.
///
/// This is deliberately the same predicate `next_hot_working_diff_baseline`
/// uses to return `(BeforeAbsent { .. }, false)`, evaluated here from the same
/// pre-image, and the value loop asserts the two agree before it drops a row.
fn hot_interval_local_preconditions(
    deltas: &[&CurrentStateDeltaRef<'_>],
    previous_values: &[Option<CertifiedCurrentStatePredecessor>],
    active_checkpoint_commit_id: CommitId,
) -> Result<Vec<bool>, LixError> {
    let mut preconditions = vec![false; deltas.len()];
    for (index, (delta, previous)) in deltas.iter().zip(previous_values).enumerate() {
        if !delta.deleted || delta.untracked {
            continue;
        }
        let Some(previous) = previous else {
            continue;
        };
        let previous = previous.view()?;
        if previous.untracked {
            continue;
        }
        preconditions[index] = matches!(
            previous.working_diff_baseline,
            WorkingDiffBaseline::BeforeAbsent {
                checkpoint_commit_id,
            } if checkpoint_commit_id == active_checkpoint_commit_id
        );
    }
    Ok(preconditions)
}

async fn hot_compaction_mask(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    deltas: &[&CurrentStateDeltaRef<'_>],
    preconditions: Option<&[bool]>,
    kind: HotTombstoneMaskKind,
    transaction_global_schema_keys: Option<&BTreeSet<String>>,
) -> Result<Vec<bool>, LixError> {
    let mask = vec![false; deltas.len()];
    #[cfg(any(test, feature = "storage-benches"))]
    {
        let (routes, offered) = match kind {
            HotTombstoneMaskKind::Checkpoint => {
                (&COMPACTED_TOMBSTONE_ROUTES, &COMPACTED_TOMBSTONE_OFFERED)
            }
            HotTombstoneMaskKind::IntervalLocal => (
                &INTERVAL_LOCAL_TOMBSTONE_ROUTES,
                &INTERVAL_LOCAL_TOMBSTONE_OFFERED,
            ),
        };
        routes.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        offered.fetch_add(deltas.len() as u64, std::sync::atomic::Ordering::Relaxed);
    }
    let is_candidate = |index: usize, delta: &CurrentStateDeltaRef<'_>| {
        delta.deleted
            && !delta.untracked
            && delta.schema_key != crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
            // An exact-closure scope proves its membership from a recomputed
            // identity digest. Leaving its tombstones alone keeps that proof
            // out of this change's blast radius for no measurable loss.
            && !scope_requires_exact_closure(branch_id, delta.schema_key, delta.file_id)
            // Empty means "no extra precondition"; the checkpoint route passes
            // `None`. A short slice must read as `false`, never as admitted.
            && preconditions.is_none_or(|pre| pre.get(index).copied().unwrap_or(false))
    };
    let candidates = deltas
        .iter()
        .enumerate()
        .filter(|(index, delta)| is_candidate(*index, delta))
        .count();
    if candidates == 0 {
        return Ok(mask);
    }
    #[cfg(any(test, feature = "storage-benches"))]
    match kind {
        HotTombstoneMaskKind::Checkpoint => &COMPACTED_TOMBSTONE_CANDIDATES,
        HotTombstoneMaskKind::IntervalLocal => &INTERVAL_LOCAL_TOMBSTONE_CANDIDATES,
    }
    .fetch_add(candidates as u64, std::sync::atomic::Ordering::Relaxed);

    // Gate (a).
    if hot_generation_has_any_base(store, branch_id, generation).await? {
        return Ok(mask);
    }

    // Gate (b). Nothing sits below the global branch, so it is vacuous there
    // and neither half of it is consulted.
    let global_is_below = branch_id != crate::GLOBAL_BRANCH_ID;
    if global_is_below && transaction_global_schema_keys.is_none() {
        // The caller cannot enumerate what this transaction stages on global.
        // That is the disabling verdict wherever global sits below.
        return Ok(mask);
    }
    let global_control = if global_is_below {
        BranchHeadControlContext::new()
            .reader(store)
            .load(crate::GLOBAL_BRANCH_ID)
            .await?
    } else {
        None
    };
    let reader = HotStateStoreReader {
        store,
        transaction_cache: None,
        root_base_cache: None,
    };

    let mut mask = mask;
    let mut compacted = 0_u64;
    // One verdict per distinct schema key, not per identity: a churned
    // collection offers thousands of tombstones in one schema.
    let mut verdicts: BTreeMap<&str, bool> = BTreeMap::new();
    for (index, delta) in deltas.iter().enumerate() {
        if !is_candidate(index, delta) {
            continue;
        }
        let blocked = if !global_is_below {
            false
        } else if let Some(&verdict) = verdicts.get(delta.schema_key) {
            verdict
        } else {
            let verdict = transaction_global_schema_keys
                .is_some_and(|schema_keys| schema_keys.contains(delta.schema_key))
                || match global_control {
                    Some(control) => {
                        reader
                            .has_schema_rows(crate::GLOBAL_BRANCH_ID, control, delta.schema_key)
                            .await?
                    }
                    // No global branch control means no published global
                    // generation, so no global row can be hidden by anything.
                    None => false,
                };
            verdicts.insert(delta.schema_key, verdict);
            verdict
        };
        if !blocked {
            mask[index] = true;
            compacted += 1;
        }
    }
    #[cfg(any(test, feature = "storage-benches"))]
    match kind {
        HotTombstoneMaskKind::Checkpoint => &COMPACTED_TOMBSTONE_COMPACTED,
        HotTombstoneMaskKind::IntervalLocal => &INTERVAL_LOCAL_TOMBSTONE_ELIDED,
    }
    .fetch_add(compacted, std::sync::atomic::Ordering::Relaxed);
    let _ = compacted;
    let _ = kind;
    Ok(mask)
}

fn stage_hot_mutation_batch(
    writes: &mut StorageWriteSet,
    identities: EncodedHotMutationIdentities,
    value_bytes: Bytes,
    value_ranges: Vec<Option<Range<usize>>>,
) {
    assert_eq!(
        identities.key_ranges.len(),
        value_ranges.len(),
        "every hot mutation identity must have one staged value"
    );
    let put_count = value_ranges.iter().flatten().count();
    let delete_count = value_ranges.len() - put_count;
    let file_count = identities
        .key_ranges
        .iter()
        .filter(|identity| identity.file_schema_key.is_some())
        .count();
    let mut row_puts = Vec::with_capacity(put_count);
    let mut row_deletes = Vec::with_capacity(delete_count);
    let mut file_schema_puts = Vec::with_capacity(file_count);
    for (identity, value) in identities.key_ranges.iter().zip(&value_ranges) {
        if let Some(value) = value {
            let value = buffer_range(value);
            row_puts.push(EncodedPut {
                key: identity.row_key,
                value,
            });
        } else {
            row_deletes.push(identity.row_key);
        }
        if let Some(key) = identity.file_schema_key {
            file_schema_puts.push(key);
        }
    }
    file_schema_puts.sort_unstable_by(|left, right| {
        let left = &identities.key_bytes[left.offset()..left.offset().saturating_add(left.len())];
        let right =
            &identities.key_bytes[right.offset()..right.offset().saturating_add(right.len())];
        left.cmp(right)
    });
    file_schema_puts.dedup_by(|left, right| {
        identities.key_bytes[left.offset()..left.offset().saturating_add(left.len())]
            == identities.key_bytes[right.offset()..right.offset().saturating_add(right.len())]
    });

    stage_hot_encoded_mutation_ranges(
        writes,
        identities.key_bytes,
        value_bytes,
        row_puts,
        row_deletes,
        file_schema_puts,
    );
}

fn stage_hot_encoded_mutation_ranges(
    writes: &mut StorageWriteSet,
    key_bytes: Bytes,
    value_bytes: Bytes,
    row_puts: Vec<EncodedPut>,
    row_deletes: Vec<BufferRange>,
    mut file_schema_puts: Vec<BufferRange>,
) {
    let row_batch = EncodedMutationBatch::try_new(
        key_bytes.clone(),
        value_bytes.clone(),
        row_puts,
        row_deletes,
    )
    .expect("hot row ranges originate in the supplied encoded buffers");
    writes.stage_encoded_batch(ROW_SPACE, row_batch);
    file_schema_puts.retain(|key| {
        !writes.contains_put(
            FILE_SPACE,
            &key_bytes[key.offset()..key.offset().saturating_add(key.len())],
        )
    });
    if !file_schema_puts.is_empty() {
        let file_puts = file_schema_puts
            .into_iter()
            .map(|key| EncodedPut {
                key,
                value: BufferRange::new(0, 0),
            })
            .collect();
        let file_batch =
            EncodedMutationBatch::try_new(key_bytes, Bytes::new(), file_puts, Vec::new())
                .expect("hot file schema ranges originate in the supplied encoded buffers");
        writes.stage_encoded_batch(FILE_SPACE, file_batch);
    }
}

fn stage_hot_diff_batch(
    writes: &mut StorageWriteSet,
    scope: &[u8],
    identity_key_bytes: Vec<u8>,
    identity_puts: Vec<EncodedPut>,
) -> Result<(), LixError> {
    if identity_puts.is_empty() {
        return Ok(());
    }
    if scope.is_empty() {
        return Err(head_value_error(
            "hot diff identities require a checkpoint scope",
        ));
    }
    if identity_puts.len() < HOT_DIFF_PACK_MIN_IDENTITIES {
        let batch = EncodedMutationBatch::try_new(
            Bytes::from(identity_key_bytes),
            Bytes::new(),
            identity_puts,
            Vec::new(),
        )
        .expect("direct hot diff ranges originate in the supplied encoded buffer");
        writes.stage_encoded_batch(DIFF_SPACE, batch);
        return Ok(());
    }

    // HOT_DIFF is a set of eagerly persisted identities. Store bounded
    // segments instead of repeating the checkpoint scope in one LSM entry per
    // identity. The content digest makes each segment key deterministic while
    // the value retains every identity suffix in the same atomic write set.
    let value_capacity = identity_key_bytes
        .len()
        .saturating_sub(scope.len().saturating_mul(identity_puts.len()))
        .saturating_add(identity_puts.len().saturating_mul(4))
        .saturating_add(
            identity_puts
                .len()
                .div_ceil(HOT_DIFF_SEGMENT_MAX_IDENTITIES as usize)
                .saturating_mul(HOT_DIFF_SEGMENT_HEADER_BYTES),
        );
    let mut value_bytes = Vec::with_capacity(value_capacity);
    let mut value_ranges = Vec::<Range<usize>>::new();
    let mut segment_start = value_bytes.len();
    value_bytes.push(HOT_DIFF_SEGMENT_VERSION);
    value_bytes.extend_from_slice(&0_u32.to_le_bytes());
    let mut segment_count = 0_u32;

    for put in identity_puts {
        let key_start = put.key.offset();
        let key_end = key_start
            .checked_add(put.key.len())
            .ok_or_else(|| head_value_error("hot diff key range overflow"))?;
        let full_key = identity_key_bytes
            .get(key_start..key_end)
            .ok_or_else(|| head_value_error("hot diff key range escapes its arena"))?;
        let suffix = full_key
            .strip_prefix(scope)
            .ok_or_else(|| head_value_error("hot diff identity key is outside its scope"))?;
        let suffix_len = u32::try_from(suffix.len())
            .map_err(|_| head_value_error("hot diff identity suffix exceeds u32"))?;
        let encoded_len = 4_usize.saturating_add(suffix.len());
        if HOT_DIFF_SEGMENT_HEADER_BYTES.saturating_add(encoded_len) > HOT_DIFF_SEGMENT_MAX_BYTES {
            return Err(head_value_error(
                "hot diff identity exceeds the segment size limit",
            ));
        }
        let current_len = value_bytes.len() - segment_start;
        if segment_count > 0
            && (segment_count == HOT_DIFF_SEGMENT_MAX_IDENTITIES
                || current_len.saturating_add(encoded_len) > HOT_DIFF_SEGMENT_MAX_BYTES)
        {
            value_bytes[segment_start + 1..segment_start + HOT_DIFF_SEGMENT_HEADER_BYTES]
                .copy_from_slice(&segment_count.to_le_bytes());
            value_ranges.push(segment_start..value_bytes.len());
            segment_start = value_bytes.len();
            value_bytes.push(HOT_DIFF_SEGMENT_VERSION);
            value_bytes.extend_from_slice(&0_u32.to_le_bytes());
            segment_count = 0;
        }
        value_bytes.extend_from_slice(&suffix_len.to_le_bytes());
        value_bytes.extend_from_slice(suffix);
        segment_count += 1;
    }
    value_bytes[segment_start + 1..segment_start + HOT_DIFF_SEGMENT_HEADER_BYTES]
        .copy_from_slice(&segment_count.to_le_bytes());
    value_ranges.push(segment_start..value_bytes.len());

    let mut key_bytes = Vec::with_capacity(value_ranges.len().saturating_mul(scope.len() + 32));
    let mut puts = Vec::with_capacity(value_ranges.len());
    for value in value_ranges {
        let key_start = key_bytes.len();
        key_bytes.extend_from_slice(scope);
        key_bytes.extend_from_slice(blake3::hash(&value_bytes[value.clone()]).as_bytes());
        puts.push(EncodedPut {
            key: BufferRange::new(key_start, key_bytes.len() - key_start),
            value: buffer_range(&value),
        });
    }
    let batch = EncodedMutationBatch::try_new(
        Bytes::from(key_bytes),
        Bytes::from(value_bytes),
        puts,
        Vec::new(),
    )
    .expect("hot diff segment ranges originate in the supplied encoded buffers");
    writes.stage_encoded_batch(DIFF_SPACE, batch);
    Ok(())
}

fn buffer_range(range: &Range<usize>) -> BufferRange {
    BufferRange::new(range.start, range.end - range.start)
}

fn stage_complete_hot_rows(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    generation: CommitId,
    rows: HotRowMap,
) {
    if rows.is_empty() {
        return;
    }
    let scope = hot_scope_prefix(branch_id, generation);
    let file_schema_keys = rows
        .keys()
        .filter(|identity| identity.file_id.is_some())
        .map(|identity| identity.schema_key.clone())
        .collect::<BTreeSet<_>>();
    let value_capacity = rows.values().map(Bytes::len).sum();
    let marker_key_capacity = file_schema_keys
        .iter()
        .map(|schema_key| {
            scope
                .len()
                .saturating_add(encoded_key_bytes_len(schema_key.as_bytes()).unwrap_or(0))
        })
        .sum::<usize>();
    let key_capacity = rows
        .len()
        .saturating_mul(scope.len() + 32)
        .saturating_add(
            rows.keys()
                .map(|identity| {
                    identity
                        .schema_key
                        .len()
                        .saturating_add(identity.row_pk.estimated_heap_bytes())
                        .saturating_add(
                            identity
                                .file_id
                                .as_ref()
                                .map_or(0, |file_id| file_id.len().saturating_mul(2)),
                        )
                })
                .sum(),
        )
        .saturating_add(marker_key_capacity);
    let mut key_bytes = Vec::with_capacity(key_capacity);
    let mut value_bytes = Vec::with_capacity(value_capacity);
    let mut row_puts = Vec::with_capacity(rows.len());
    let mut file_puts = Vec::with_capacity(file_schema_keys.len());
    for (identity, value) in rows {
        let value_start = value_bytes.len();
        value_bytes.extend_from_slice(value.as_ref());
        let value = BufferRange::new(value_start, value_bytes.len() - value_start);

        let row_start = key_bytes.len();
        key_bytes.extend_from_slice(&scope);
        write_key_string(&mut key_bytes, &identity.schema_key, KEY_PART_FINAL);
        write_file_id(&mut key_bytes, identity.file_id.as_deref());
        write_row_pk(&mut key_bytes, &identity.row_pk);
        row_puts.push(EncodedPut {
            key: BufferRange::new(row_start, key_bytes.len() - row_start),
            value,
        });
    }
    for schema_key in file_schema_keys {
        let file_start = key_bytes.len();
        key_bytes.extend_from_slice(&scope);
        write_key_string(&mut key_bytes, &schema_key, KEY_PART_FINAL);
        file_puts.push(EncodedPut {
            key: BufferRange::new(file_start, key_bytes.len() - file_start),
            value: BufferRange::new(0, 0),
        });
    }
    let key_bytes = Bytes::from(key_bytes);
    let value_bytes = Bytes::from(value_bytes);
    let row_batch =
        EncodedMutationBatch::try_new(key_bytes.clone(), value_bytes.clone(), row_puts, Vec::new())
            .expect("complete hot row ranges originate in the supplied encoded buffers");
    writes.stage_encoded_batch(ROW_SPACE, row_batch);
    file_puts.retain(|put| {
        !writes.contains_put(
            FILE_SPACE,
            &key_bytes[put.key.offset()..put.key.offset().saturating_add(put.key.len())],
        )
    });
    if !file_puts.is_empty() {
        let file_batch =
            EncodedMutationBatch::try_new(key_bytes, Bytes::new(), file_puts, Vec::new())
                .expect("complete hot file ranges originate in the supplied encoded buffers");
        writes.stage_encoded_batch(FILE_SPACE, file_batch);
    }
}

#[cfg(test)]
pub(super) fn stage_test_hot_value(
    writes: &mut StorageWriteSet,
    identity: &HeadIdentity,
    value: &HeadValue,
) -> Result<(), LixError> {
    let rows = BTreeMap::from([(
        HeadRowIdentity {
            schema_key: identity.schema_key.clone(),
            row_pk: identity.row_pk.clone(),
            file_id: identity.file_id.clone(),
        },
        Bytes::from(encode_head_value(&value.as_ref())?),
    )]);
    stage_complete_hot_rows(writes, &identity.branch_id, identity.generation, rows);
    Ok(())
}

fn stage_hot_bootstrap(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    generation: CommitId,
    parent_rows: Vec<MaterializedTrackedStateRow>,
    deltas: &[&CurrentStateDeltaRef<'_>],
    absence_guards: &BTreeSet<TrackedStateKey>,
    working_diff_capture_checkpoint_commit_id: Option<CommitId>,
    coverage: &mut WorkingDiffIndexCoverage,
) -> Result<(), LixError> {
    let mut rows = HotRowMap::new();
    let tracked_baseline = if working_diff_capture_checkpoint_commit_id.is_some() {
        WorkingDiffBaseline::Clean
    } else {
        WorkingDiffBaseline::Disabled
    };
    for row in parent_rows {
        let snapshot = if row.deleted {
            None
        } else {
            let typed = row.decoded_snapshot.as_ref().ok_or_else(|| {
                head_value_error("live bootstrap row is missing its native typed payload")
            })?;
            Some(
                typed
                    .durable_payload()
                    .map_err(|error| {
                        head_value_error(format!(
                            "cannot retain bootstrap typed payload: {error:?}"
                        ))
                    })?
                    .to_vec(),
            )
        };
        let key = TrackedStateKey {
            schema_key: row.schema_key.clone(),
            row_pk: row.row_pk.clone(),
            file_id: row.file_id.clone(),
        };
        if absence_guards.contains(&key) && !row.deleted {
            return Err(tracked_head_duplicate_insert_error(&key));
        }
        let identity = HeadRowIdentity {
            schema_key: row.schema_key,
            row_pk: row.row_pk,
            file_id: row.file_id,
        };
        let metadata = row
            .metadata
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(|error| {
                head_value_error(&format!("bootstrap metadata is invalid JSON: {error}"))
            })?
            .map(lix_schema::Jsonb::from_value);
        let value = HeadValueRef {
            change_id: Some(row.change_id),
            commit_id: Some(row.commit_id),
            untracked: false,
            deleted: row.deleted,
            created_at: LixTimestamp::expect_parse("hot bootstrap created_at", &row.created_at),
            updated_at: LixTimestamp::expect_parse("hot bootstrap updated_at", &row.updated_at),
            snapshot: snapshot.as_deref(),
            metadata: metadata.as_ref(),
            columnar_base_coordinate: None,
            working_diff_baseline: tracked_baseline,
        };
        if rows
            .insert(identity, Bytes::from(encode_head_value(&value)?))
            .is_some()
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "hot bootstrap contains duplicate tracked row identity",
            ));
        }
    }
    for delta in deltas {
        apply_complete_file_delete_cascade(&mut rows, delta)?;
        let identity = HeadRowIdentity {
            schema_key: delta.schema_key.to_string(),
            row_pk: delta.row_pk.clone(),
            file_id: delta.file_id.map(str::to_string),
        };
        let previous = rows.get(&identity).map(|bytes| bytes.as_ref());
        if let Some(previous) = previous {
            let existing = decode_head_value(previous)?;
            reject_guarded_live_member(absence_guards, delta, existing)?;
            reject_retention_change(delta, existing)?;
        }
        if delta.physically_deletes() {
            rows.remove(&identity);
        } else {
            let created_at = previous
                .map(decode_head_value)
                .transpose()?
                .map_or(delta.created_at, |value| value.created_at);
            rows.insert(
                identity,
                Bytes::from(encode_head_value(&{
                    let mut value = delta.value_ref(
                        created_at,
                        if delta.untracked {
                            WorkingDiffBaseline::Disabled
                        } else {
                            tracked_baseline
                        },
                    );
                    value.columnar_base_coordinate = None;
                    value
                })?),
            );
        }
    }
    stage_complete_collection_controls(writes, branch_id, generation, &rows)?;
    stage_complete_hot_rows(writes, branch_id, generation, rows);
    *coverage = WorkingDiffIndexCoverage::default();
    Ok(())
}

async fn reject_hot_absence_guards(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    guards: &BTreeSet<TrackedStateKey>,
) -> Result<(), LixError> {
    if guards.is_empty() {
        return Ok(());
    }
    let identities = guards
        .iter()
        .map(|key| {
            hot_identity(
                branch_id,
                generation,
                &key.schema_key,
                &key.row_pk,
                key.file_id.as_deref(),
            )
        })
        .collect::<Vec<_>>();
    for (identity, value) in identities
        .iter()
        .zip(hot_load_primary_identity_bytes(store, &identities).await?)
    {
        let Some(value) = value else {
            continue;
        };
        let value = decode_head_value(&value)?;
        if !value.deleted {
            return Err(tracked_head_duplicate_insert_error(&TrackedStateKey {
                schema_key: identity.schema_key.clone(),
                row_pk: identity.row_pk.clone(),
                file_id: identity.file_id.clone(),
            }));
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct EncodedHotPointKeyRanges {
    primary: BufferRange,
}

struct EncodedHotPointKeys {
    bytes: Bytes,
    ranges: Vec<EncodedHotPointKeyRanges>,
}

impl EncodedHotPointKeys {
    fn primary_key(&self, index: usize) -> StorageKey {
        self.key_for_range(self.ranges[index].primary)
    }

    fn primary_key_bytes(&self, index: usize) -> &[u8] {
        let range = self.ranges[index].primary;
        &self.bytes[range.offset()..range.offset() + range.len()]
    }

    fn key_for_range(&self, range: BufferRange) -> StorageKey {
        let start = range.offset();
        StorageKey(self.bytes.slice(start..start + range.len()))
    }
}

fn encode_hot_point_keys(
    branch_id: &str,
    generation: CommitId,
    keys: &[TrackedStateKeyRef<'_>],
) -> EncodedHotPointKeys {
    encode_hot_point_keys_with(branch_id, generation, keys.len(), |index| keys[index])
}

fn encode_hot_point_keys_with<'a>(
    branch_id: &str,
    generation: CommitId,
    key_count: usize,
    mut key_at: impl FnMut(usize) -> TrackedStateKeyRef<'a>,
) -> EncodedHotPointKeys {
    let scope = hot_scope_prefix(branch_id, generation);
    let planned_capacity = (0..key_count).try_fold(0_usize, |total, index| {
        let key = key_at(index);
        let primary_len =
            encoded_hot_identity_key_len(scope.len(), key.schema_key, key.row_pk, key.file_id)?;
        total.checked_add(primary_len)
    });
    let capacity = planned_capacity.unwrap_or(0);
    let mut bytes = Vec::with_capacity(capacity);
    let mut ranges = Vec::with_capacity(key_count);
    for index in 0..key_count {
        let key = key_at(index);
        let primary_start = bytes.len();
        bytes.extend_from_slice(&scope);
        write_key_string(&mut bytes, key.schema_key, KEY_PART_FINAL);
        write_file_id(&mut bytes, key.file_id);
        write_row_pk(&mut bytes, key.row_pk);
        let primary = BufferRange::new(primary_start, bytes.len() - primary_start);

        ranges.push(EncodedHotPointKeyRanges { primary });
    }
    debug_assert!(planned_capacity.is_none() || bytes.len() == capacity);
    EncodedHotPointKeys {
        bytes: Bytes::from(bytes),
        ranges,
    }
}

#[derive(Clone, Copy)]
struct FiniteHotIdentityRef<'a> {
    row_pk: &'a RowPk,
    file_id: Option<&'a str>,
}

/// One exact schema partition whose invariant identity components are retained
/// once for the entire point-read batch.
///
/// Row and file descriptors borrow the caller's filter. Physical primary
/// keys share one immutable arena, so a dense-range probe can reuse the exact
/// same ranges before falling back to MultiGet.
struct FiniteHotIdentityBatchRef<'a> {
    branch_id: &'a str,
    generation: CommitId,
    schema_key: &'a str,
    identities: Vec<FiniteHotIdentityRef<'a>>,
    encoded: EncodedHotPointKeys,
}

impl<'a> FiniteHotIdentityBatchRef<'a> {
    fn new(
        branch_id: &'a str,
        generation: CommitId,
        schema_key: &'a str,
        mut row_pks: Vec<&'a RowPk>,
        mut file_ids: Vec<Option<&'a str>>,
    ) -> Option<Self> {
        row_pks.sort_unstable();
        row_pks.dedup();
        file_ids.sort_unstable();
        file_ids.dedup();
        let identity_count = row_pks.len().checked_mul(file_ids.len())?;
        let mut identities = Vec::with_capacity(identity_count);
        for row_pk in row_pks {
            for &file_id in &file_ids {
                identities.push(FiniteHotIdentityRef { row_pk, file_id });
            }
        }
        let encoded =
            encode_hot_point_keys_with(branch_id, generation, identities.len(), |index| {
                TrackedStateKeyRef {
                    schema_key,
                    row_pk: identities[index].row_pk,
                    file_id: identities[index].file_id,
                }
            });
        Some(Self {
            branch_id,
            generation,
            schema_key,
            identities,
            encoded,
        })
    }

    fn len(&self) -> usize {
        self.identities.len()
    }

    fn key_ref(&self, index: usize) -> TrackedStateKeyRef<'a> {
        let identity = self.identities[index];
        TrackedStateKeyRef {
            schema_key: self.schema_key,
            row_pk: identity.row_pk,
            file_id: identity.file_id,
        }
    }
}

struct FiniteHotEntryBatchRef<'a> {
    identities: FiniteHotIdentityBatchRef<'a>,
    values: Vec<Option<Bytes>>,
}

/// Identity decoded from a storage scan without constructing row-owned
/// `String` buffers for key metadata.
///
/// The immutable storage key is retained once. Schema and file ids normally
/// remain compact ranges into that buffer; only an escaped-NUL key part uses
/// the owned fallback. String and byte row-PK components retain `Bytes`
/// slices of the same key allocation.
#[derive(Debug)]
struct HotScanIdentity {
    key: Bytes,
    schema_key: HotScanString,
    row_pk: RowPk,
    file_id: Option<HotScanString>,
}

#[derive(Debug)]
enum HotScanString {
    Borrowed(Range<u32>),
    Owned(String),
}

impl HotScanString {
    fn as_str<'a>(&'a self, key: &'a Bytes) -> &'a str {
        match self {
            Self::Borrowed(range) => {
                let range = range.start as usize..range.end as usize;
                // SAFETY: `read_hot_scan_key_string` validates this exact
                // range as UTF-8 before constructing the descriptor.
                unsafe { std::str::from_utf8_unchecked(&key[range]) }
            }
            Self::Owned(value) => value,
        }
    }

    fn into_shared_str(self, key: &Bytes) -> SharedStr {
        match self {
            Self::Borrowed(range) => {
                let range = range.start as usize..range.end as usize;
                let value = {
                    // SAFETY: the decoder validated this exact range.
                    unsafe { std::str::from_utf8_unchecked(&key[range]) }
                };
                #[cfg(feature = "storage-benches")]
                {
                    crate::storage_bench::record_hot_scan_key_handle_clone();
                }
                SharedStr::from_utf8_slice(key.clone(), value)
                    .expect("decoded key string remains inside its retained key")
            }
            Self::Owned(value) => SharedStr::from(value),
        }
    }

    fn into_string(self, key: &Bytes) -> String {
        match self {
            Self::Borrowed(range) => {
                let range = range.start as usize..range.end as usize;
                // SAFETY: the decoder validated this exact range.
                unsafe { std::str::from_utf8_unchecked(&key[range]) }.to_owned()
            }
            Self::Owned(value) => value,
        }
    }

    #[cfg(test)]
    fn owns_fallback_buffer(&self) -> bool {
        matches!(self, Self::Owned(_))
    }
}

impl HotScanIdentity {
    fn schema_key(&self) -> &str {
        self.schema_key.as_str(&self.key)
    }

    fn file_id(&self) -> Option<&str> {
        self.file_id
            .as_ref()
            .map(|file_id| file_id.as_str(&self.key))
    }

    fn matches_filter(&self, filter: &TrackedStateFilter) -> bool {
        (filter.schema_keys.is_empty()
            || filter
                .schema_keys
                .iter()
                .any(|schema_key| schema_key == self.schema_key()))
            && filter.matches_row_pk(&self.row_pk)
            && (filter.file_ids.is_empty()
                || filter.file_ids.iter().any(|filter| match filter {
                    NullableKeyFilter::Any => true,
                    NullableKeyFilter::Null => self.file_id().is_none(),
                    NullableKeyFilter::Value(value) => self.file_id() == Some(value.as_str()),
                }))
    }

    fn into_row_identity(self) -> HeadRowIdentity {
        let Self {
            key,
            schema_key,
            row_pk,
            file_id,
        } = self;
        HeadRowIdentity {
            schema_key: schema_key.into_string(&key),
            row_pk,
            file_id: file_id.map(|file_id| file_id.into_string(&key)),
        }
    }

    #[cfg(test)]
    fn owned_metadata_buffer_count(&self) -> usize {
        usize::from(self.schema_key.owns_fallback_buffer())
            + usize::from(
                self.file_id
                    .as_ref()
                    .is_some_and(HotScanString::owns_fallback_buffer),
            )
    }
}

impl PartialEq for HotScanIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.schema_key() == other.schema_key()
            && self.row_pk == other.row_pk
            && self.file_id() == other.file_id()
    }
}

impl Eq for HotScanIdentity {}

impl PartialOrd for HotScanIdentity {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HotScanIdentity {
    fn cmp(&self, other: &Self) -> Ordering {
        self.schema_key()
            .cmp(other.schema_key())
            .then_with(|| self.row_pk.cmp(&other.row_pk))
            .then_with(|| self.file_id().cmp(&other.file_id()))
    }
}

impl LiveMaterializationIdentity for HotScanIdentity {
    fn schema_key(&self) -> &str {
        HotScanIdentity::schema_key(self)
    }

    fn row_pk(&self) -> &RowPk {
        &self.row_pk
    }

    fn push_materialized(
        self,
        rows: &mut MaterializedHotStateBatchBuilder,
        snapshot_content: Option<SharedStr>,
        metadata: Option<SharedStr>,
        deleted: bool,
        created_at: LixTimestamp,
        updated_at: LixTimestamp,
        global: bool,
        change_id: Option<ChangeId>,
        commit_id: Option<CommitId>,
        untracked: bool,
        branch_id: &str,
    ) {
        // `self` is consumed here, so the primary key moves into the column
        // instead of being cloned out of a value about to be dropped. The
        // identity strings stay borrowed: they are slices of the retained key
        // and the builder interns them into its shared dictionary.
        let Self {
            key,
            schema_key,
            row_pk,
            file_id,
        } = self;
        let schema_key = schema_key.as_str(&key);
        let file_id = file_id.as_ref().map(|file_id| file_id.as_str(&key));
        rows.push_materialized_interned(
            row_pk,
            schema_key,
            file_id,
            snapshot_content,
            metadata,
            deleted,
            created_at,
            updated_at,
            global,
            change_id,
            commit_id,
            untracked,
            branch_id,
        );
    }
}

enum HotScanEntries<'a> {
    Finite(Vec<FiniteHotEntryBatchRef<'a>>),
    Decoded(Vec<(HotScanIdentity, Bytes)>),
}

fn filter_hot_scan_entries_by_collection_generation(
    entries: &mut HotScanEntries<'_>,
    control: HotCollectionControl,
) -> Result<(), LixError> {
    let visible = |bytes: &Bytes| -> Result<bool, LixError> {
        let value = decode_head_value(bytes)?;
        Ok(survives_collection_generation_fence(
            value.untracked,
            value.commit_id,
            control.active_generation,
            false,
        ))
    };
    match entries {
        HotScanEntries::Decoded(rows) => {
            let mut retained = Vec::with_capacity(rows.len());
            for (identity, bytes) in rows.drain(..) {
                if visible(&bytes)? {
                    retained.push((identity, bytes));
                }
            }
            *rows = retained;
        }
        HotScanEntries::Finite(batches) => {
            for batch in batches {
                for value in &mut batch.values {
                    if value
                        .as_ref()
                        .map(&visible)
                        .transpose()?
                        .is_some_and(|visible| !visible)
                    {
                        *value = None;
                    }
                }
            }
        }
    }
    Ok(())
}

fn hot_exact_identity_batches<'a>(
    branch_id: &'a str,
    generation: CommitId,
    filter: &'a TrackedStateFilter,
) -> Option<Vec<FiniteHotIdentityBatchRef<'a>>> {
    if filter.schema_keys.is_empty() || filter.row_pks.is_empty() {
        return None;
    }
    let mut schema_keys = filter
        .schema_keys
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    schema_keys.sort_unstable();
    schema_keys.dedup();
    let row_pks = filter
        .row_pks
        .iter()
        .filter(|row_pk| filter.matches_row_pk(row_pk))
        .collect::<Vec<_>>();
    let file_ids = if filter.file_ids.is_empty() {
        vec![None]
    } else {
        filter
            .file_ids
            .iter()
            .map(|file_id| match file_id {
                NullableKeyFilter::Null => Some(None),
                NullableKeyFilter::Value(value) => Some(Some(value.as_str())),
                NullableKeyFilter::Any => None,
            })
            .collect::<Option<Vec<_>>>()?
    };
    schema_keys
        .into_iter()
        .map(|schema_key| {
            FiniteHotIdentityBatchRef::new(
                branch_id,
                generation,
                schema_key,
                row_pks.clone(),
                file_ids.clone(),
            )
        })
        .collect()
}

async fn hot_load_finite_identity_bytes(
    store: &(impl StorageAdapterRead + ?Sized),
    batch: &FiniteHotIdentityBatchRef<'_>,
) -> Result<Vec<Option<Bytes>>, LixError> {
    if batch.identities.is_empty() {
        return Ok(Vec::new());
    }
    let keys = (0..batch.len())
        .map(|index| batch.encoded.primary_key(index))
        .collect::<Vec<_>>();
    PointReadPlan::new(ROW_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .map(|value| value.map(full_value_bytes).transpose())
        .collect()
}

async fn hot_scan_finite_identity_batches<'a>(
    store: &(impl StorageAdapterRead + ?Sized),
    batches: Vec<FiniteHotIdentityBatchRef<'a>>,
    limit: Option<usize>,
) -> Result<Vec<FiniteHotEntryBatchRef<'a>>, LixError> {
    let expected_generation = batches.first().map(|batch| batch.generation);
    let mut remaining = limit.unwrap_or(usize::MAX);
    let mut entries = Vec::with_capacity(batches.len());
    for identities in batches {
        debug_assert_eq!(Some(identities.generation), expected_generation);
        if remaining == 0 {
            break;
        }
        let mut values = if limit.is_none()
            && let Some(values) = hot_scan_dense_identity_range(store, &identities).await?
        {
            values
        } else {
            hot_load_finite_identity_bytes(store, &identities).await?
        };
        if limit.is_some() {
            for value in &mut values {
                if value.is_none() {
                    continue;
                }
                if remaining == 0 {
                    *value = None;
                } else {
                    remaining -= 1;
                }
            }
        }
        entries.push(FiniteHotEntryBatchRef { identities, values });
    }
    Ok(entries)
}

async fn materialize_hot_scan_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    entries: HotScanEntries<'_>,
    projection: ChangeRecordProjection,
    branch_id: &str,
    active_checkpoint_commit_id: Option<CommitId>,
) -> Result<MaterializedHotStateBatch, LixError> {
    match entries {
        HotScanEntries::Decoded(entries) => {
            materialize_live_entries(
                store,
                entries,
                projection,
                branch_id,
                active_checkpoint_commit_id,
            )
            .await
        }
        HotScanEntries::Finite(batches) => {
            let row_count = batches
                .iter()
                .map(|batch| batch.values.iter().flatten().count())
                .sum();
            let mut entries = Vec::with_capacity(row_count);
            for batch in batches {
                debug_assert_eq!(batch.identities.branch_id, branch_id);
                for (index, value) in batch.values.into_iter().enumerate() {
                    let Some(value) = value else {
                        continue;
                    };
                    entries.push((batch.identities.key_ref(index), value));
                }
            }
            materialize_live_entries(
                store,
                entries,
                projection,
                branch_id,
                active_checkpoint_commit_id,
            )
            .await
        }
    }
}

async fn hot_load_identity_ref_bytes(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    identities: &[TrackedStateKeyRef<'_>],
) -> Result<Vec<Option<Bytes>>, LixError> {
    if identities.is_empty() {
        return Ok(Vec::new());
    }
    let encoded = encode_hot_point_keys(branch_id, generation, identities);
    let keys = (0..identities.len())
        .map(|index| encoded.primary_key(index))
        .collect::<Vec<_>>();
    PointReadPlan::new(ROW_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .map(|value| value.map(full_value_bytes).transpose())
        .collect()
}

/// Loads the authoritative primary row for every identity.
async fn hot_load_primary_identity_bytes(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: &[HeadIdentity],
) -> Result<Vec<Option<Bytes>>, LixError> {
    if identities.is_empty() {
        return Ok(Vec::new());
    }
    let scope = &identities[0];
    debug_assert!(identities.iter().all(|identity| {
        identity.branch_id == scope.branch_id && identity.generation == scope.generation
    }));
    let identities = identities
        .iter()
        .map(|identity| TrackedStateKeyRef {
            schema_key: identity.schema_key.as_str(),
            file_id: identity.file_id.as_deref(),
            row_pk: &identity.row_pk,
        })
        .collect::<Vec<_>>();
    let encoded = encode_hot_point_keys(scope.branch_id.as_str(), scope.generation, &identities);
    let keys = (0..identities.len())
        .map(|index| encoded.primary_key(index))
        .collect::<Vec<_>>();
    PointReadPlan::new(ROW_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .map(|value| value.map(full_value_bytes).transpose())
        .collect()
}

async fn hot_load_file_scope_identities(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    cascades: &BTreeMap<String, &CurrentStateDeltaRef<'_>>,
) -> Result<Vec<HeadIdentity>, LixError> {
    let scope = hot_scope_prefix(branch_id, generation);
    let range = StoragePrefix {
        bytes: Bytes::from(scope.clone()),
    }
    .to_range()?;
    let mut identities = Vec::new();
    let mut cursor = store
        .begin_scan(
            ROW_SPACE,
            range,
            StorageBeginScanOptions {
                projection: StorageCoreProjection::KeyOnly,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    loop {
        let (page, page_has_more) = cursor
            .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
            .await?
            .into_parts();
        for entry in page {
            let row = decode_hot_row_key_in_scope(entry.key.0.as_ref(), &scope)?;
            if !row
                .file_id
                .as_ref()
                .is_some_and(|file_id| cascades.contains_key(file_id))
            {
                continue;
            }
            identities.push(HeadIdentity {
                branch_id: branch_id.to_string(),
                generation,
                schema_key: row.schema_key,
                row_pk: row.row_pk,
                file_id: row.file_id,
            });
        }
        if !page_has_more {
            break;
        }
    }
    Ok(identities)
}

fn working_diff_baseline_before(
    baseline: WorkingDiffBaseline,
    checkpoint_commit_id: CommitId,
) -> Option<Option<WorkingDiffVersion>> {
    match baseline {
        WorkingDiffBaseline::BeforeAbsent {
            checkpoint_commit_id: owner,
        } if owner == checkpoint_commit_id => Some(None),
        WorkingDiffBaseline::BeforePresent {
            checkpoint_commit_id: owner,
            version,
        } if owner == checkpoint_commit_id => Some(Some(version)),
        WorkingDiffBaseline::BeforeAbsent { .. } | WorkingDiffBaseline::BeforePresent { .. } => {
            None
        }
        WorkingDiffBaseline::Disabled | WorkingDiffBaseline::Clean => None,
    }
}

fn packed_working_diff_slot(slot: &Option<lix_schema::Jsonb>) -> WorkingDiffSlotFingerprint {
    match slot {
        None => WorkingDiffSlotFingerprint {
            kind: WORKING_DIFF_SLOT_NONE,
            hash: [0; JSON_REF_BYTES],
        },
        Some(metadata) => WorkingDiffSlotFingerprint {
            kind: WORKING_DIFF_SLOT_INLINE,
            hash: *blake3::hash(
                metadata
                    .binary()
                    .expect("validated native JSONB metadata must encode")
                    .as_ref(),
            )
            .as_bytes(),
        },
    }
}

fn packed_working_diff_snapshot(payload: Option<&[u8]>) -> WorkingDiffSlotFingerprint {
    payload.map_or(
        WorkingDiffSlotFingerprint {
            kind: WORKING_DIFF_SLOT_NONE,
            hash: [0; JSON_REF_BYTES],
        },
        |payload| WorkingDiffSlotFingerprint {
            // Working-diff equality only distinguishes absent from present
            // and compares the hash. Reuse the inline-present discriminator
            // for the single native payload representation.
            kind: WORKING_DIFF_SLOT_INLINE,
            hash: *blake3::hash(payload).as_bytes(),
        },
    )
}

/// Builds a packed-base after candidate from its compact authenticated index.
///
/// The dominant fresh-row case has no before image and therefore needs only
/// identity and provenance to emit `Added`. The comparison resolver hydrates
/// only the rare live/live candidates whose payloads must be compared. Keeping
/// every other slot unresolved avoids loading cold owners merely to answer the
/// common case.
fn packed_compact_working_diff_version(
    value: &crate::tracked_state::TrackedStateIndexValue,
) -> WorkingDiffVersion {
    WorkingDiffVersion {
        change_id: value.change_id,
        commit_id: value.commit_id,
        deleted: value.deleted,
        created_at: value.created_at,
        updated_at: value.updated_at,
        snapshot: WorkingDiffSlotFingerprint::unresolved(),
        metadata: WorkingDiffSlotFingerprint::unresolved(),
    }
}

/// Counts which of the two working-diff read paths served a request, so the
/// public-surface equivalence test can prove it exercised both instead of
/// silently comparing one path against itself.
#[cfg(test)]
pub(crate) static WORKING_DIFF_PATH_HITS: WorkingDiffPathHits = WorkingDiffPathHits {
    index_scan: std::sync::atomic::AtomicUsize::new(0),
    primary_scan: std::sync::atomic::AtomicUsize::new(0),
};

#[cfg(test)]
pub(crate) struct WorkingDiffPathHits {
    pub(crate) index_scan: std::sync::atomic::AtomicUsize,
    pub(crate) primary_scan: std::sync::atomic::AtomicUsize,
}

/// Decides whether a working-diff read may skip the `HOT_DIFF` scope scan and
/// enumerate primary `HOT_ROW` rows instead, and under which filter.
///
/// `None` means "no bounded primary route exists — take the index scan".
///
/// # Why this is sound without the coverage proof
///
/// The `HOT_DIFF` coverage proof is scope-global and cannot be partitioned (see
/// [`append_hot_diff_key_parts`]), so a *filtered* index scan still has to
/// enumerate the whole `(branch, checkpoint, generation)` scope to reconstruct
/// it. The proof exists to certify that the sparse index names **every** dirty
/// identity. A reader that never consults the index does not need it certified:
/// it needs the primary rows it enumerates to be the complete authority for the
/// identities it claims to cover.
///
/// That is exactly the invariant the finite bypass already rests on, and it is
/// a property of `HOT_ROW`, not of the filter shape:
///
/// * `HOT_DIFF` keys are only written for `!delta.untracked` deltas.
/// * A primary row is physically removed only by
///   `CurrentStateDelta::physically_deletes` (`untracked && deleted`); a tracked
///   delete leaves a tombstone that keeps its baseline.
/// * `reject_retention_change` forbids flipping retention while a physical
///   member exists.
///
/// So every dirty tracked identity has a branch-local `HOT_ROW` row carrying its
/// own `working_diff_baseline` — **except** identities whose current authority
/// is a packed current base published inside this checkpoint window, which own
/// no `HOT_ROW` row at all. Those are precisely what the caller's
/// `packed_refs.is_empty()` guard excludes before calling this, and that guard
/// is unchanged.
///
/// The only thing this function adds is *which* bounded routes count. The
/// caller previously admitted one (a finite `schema_key + row_pk` identity
/// batch); a file-scoped read is equally bounded, because `HOT_ROW` is keyed
/// `scope ++ schema_key ++ file_id ++ row_pk` and `hot_scan_entries` already
/// owns the file-first prefix route over it. Admitting it trades
/// O(dirty rows in the branch) for O(live rows in the file).
async fn hot_working_diff_bypass_filter<'a>(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    filter: &'a TrackedStateFilter,
) -> Result<Option<Cow<'a, TrackedStateFilter>>, LixError> {
    // A finite identity batch resolves as point reads and needs no file bound.
    if !filter.row_pks.is_empty() {
        return Ok((!filter.schema_keys.is_empty()).then_some(Cow::Borrowed(filter)));
    }
    // Every remaining bounded route is a file-first `HOT_ROW` prefix seek, so
    // every requested file id must be an exact value. `Any` and `Null` name no
    // prefix.
    if filter.file_ids.is_empty()
        || filter
            .file_ids
            .iter()
            .any(|file_id| !matches!(file_id, NullableKeyFilter::Value(_)))
    {
        return Ok(None);
    }
    if !filter.schema_keys.is_empty() {
        return Ok(Some(Cow::Borrowed(filter)));
    }
    // No schema predicate. `HOT_ROW` is schema-major, so a file bound alone
    // names no prefix. `FILE_SPACE` holds one marker per
    // `(branch, generation, schema)` that has ever written a file-backed row,
    // which is the schema domain a file-scoped read must cover: a row with a
    // non-null `file_id` cannot exist without its schema's marker. Markers are
    // conservative in the safe direction — never removed within a generation,
    // so a stale one costs one empty seek and cannot hide a live row.
    let schema_keys = hot_file_backed_schema_keys(store, branch_id, generation).await?;
    if schema_keys.is_empty() {
        // Nothing in this generation is file-backed. Fall back rather than
        // inventing an empty-prefix scan; the index path answers this
        // (necessarily empty) case correctly and it is not hot.
        return Ok(None);
    }
    let mut bypass = filter.clone();
    bypass.schema_keys = schema_keys;
    Ok(Some(Cow::Owned(bypass)))
}

/// Names every schema with a file-backed row in this `(branch, generation)`,
/// read from the conservative `FILE_SPACE` markers.
///
/// The markers carry no value, so this is a key-only scan bounded by the number
/// of schemas, not by the number of rows.
async fn hot_file_backed_schema_keys(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
) -> Result<Vec<String>, LixError> {
    let scope = hot_scope_prefix(branch_id, generation);
    let range = StoragePrefix {
        bytes: Bytes::from(scope.clone()),
    }
    .to_range()?;
    let mut schema_keys = Vec::new();
    let mut cursor = store
        .begin_scan(
            FILE_SPACE,
            range,
            StorageBeginScanOptions {
                projection: StorageCoreProjection::KeyOnly,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    while let Some(entries) = cursor.next_chunk().await? {
        for entry in entries {
            schema_keys.push(decode_hot_file_schema_key_in_scope(entry.key.0, &scope)?);
        }
    }
    schema_keys.sort();
    schema_keys.dedup();
    Ok(schema_keys)
}

fn decode_hot_file_schema_key_in_scope(key: Bytes, scope: &[u8]) -> Result<String, LixError> {
    if !key.starts_with(scope) {
        return Err(key_codec_error(
            "hot file marker does not begin with its scanned scope",
        ));
    }
    let mut offset = scope.len();
    let (schema_key, terminator) = read_hot_scan_key_string(&key, &mut offset, "schema key")?;
    if terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot file marker schema key has an invalid terminator",
        ));
    }
    if offset != key.len() {
        return Err(key_codec_error("hot file marker key has trailing bytes"));
    }
    Ok(schema_key.as_str(&key).to_string())
}

/// Resolves a checkpoint diff from row-local first-before images. Broad diffs
/// normally enumerate the sparse dirty-key index; finite PK queries and broad
/// reads with an uncertified derived index scan authoritative primary rows.
async fn hot_working_diff_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    checkpoint_commit_id: CommitId,
    generation: CommitId,
    expected_coverage: WorkingDiffIndexCoverage,
    filter: &TrackedStateFilter,
) -> Result<Option<Vec<TrackedStateDiffEntry>>, LixError> {
    let packed_refs = packed_current_base_refs(store, branch_id, generation).await?;
    let packed_refs = packed_refs
        .into_iter()
        .filter(|base| base.checkpoint_commit_id == Some(checkpoint_commit_id))
        .collect::<Vec<_>>();
    let has_matching_packed_ref = packed_refs
        .iter()
        .any(|base| packed_base_matches_file_filter(base, &filter.file_ids));
    if !has_matching_packed_ref {
        if let Some(bypass) =
            hot_working_diff_bypass_filter(store, branch_id, generation, filter).await?
        {
            return hot_working_diff_entries_from_primary(
                store,
                branch_id,
                checkpoint_commit_id,
                generation,
                bypass.as_ref(),
            )
            .await;
        }
    }

    #[cfg(test)]
    WORKING_DIFF_PATH_HITS
        .index_scan
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let scope = encode_working_diff_scope_prefix(branch_id, checkpoint_commit_id, generation);
    let range = StoragePrefix {
        bytes: Bytes::from(scope.clone()),
    }
    .to_range()?;
    let mut actual_coverage = WorkingDiffIndexCoverage::default();
    let mut selected = BTreeMap::<HeadIdentity, Option<WorkingDiffVersion>>::new();
    let mut cursor = store
        .begin_scan(DIFF_SPACE, range, StorageBeginScanOptions::default())
        .await?;
    loop {
        let (page, page_has_more) = cursor
            .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
            .await?
            .into_parts();
        for entry in page {
            let Ok(bytes) = full_value_bytes(entry.value) else {
                return Ok(None);
            };
            if bytes.is_empty() {
                if actual_coverage
                    .add_encoded_group_key(entry.key.0.as_ref())
                    .is_none()
                {
                    return Ok(None);
                }
                let Ok(identity) = decode_hot_diff_key_in_scope(entry.key.0.as_ref(), &scope)
                else {
                    return Ok(None);
                };
                if matches_filter(&identity, filter) {
                    selected.insert(
                        HeadIdentity {
                            branch_id: branch_id.to_string(),
                            generation,
                            schema_key: identity.schema_key,
                            row_pk: identity.row_pk,
                            file_id: identity.file_id,
                        },
                        None,
                    );
                }
                continue;
            }
            let Ok(segment_scope) = decode_hot_diff_segment_key(entry.key.0.as_ref()) else {
                return Ok(None);
            };
            if segment_scope.digest != *blake3::hash(&bytes).as_bytes() {
                return Ok(None);
            }
            let decoded =
                visit_hot_diff_segment(&bytes, &scope, &mut actual_coverage, |identity| {
                    if matches_filter(&identity, filter) {
                        selected.insert(
                            HeadIdentity {
                                branch_id: branch_id.to_string(),
                                generation,
                                schema_key: identity.schema_key,
                                row_pk: identity.row_pk,
                                file_id: identity.file_id,
                            },
                            None,
                        );
                    }
                });
            if decoded.is_err() {
                return Ok(None);
            }
        }
        if !page_has_more {
            break;
        }
    }
    for base_ref in &packed_refs {
        if actual_coverage
            .add_encoded_group_key(&base_ref.coverage_key)
            .is_none()
        {
            return Ok(None);
        }
        if !packed_base_matches_file_filter(base_ref, &filter.file_ids) {
            continue;
        }
        let Ok(members) =
            crate::tracked_state::scan_commit_delta_members(store, base_ref.commit_id).await
        else {
            return Ok(None);
        };
        for (key, value) in members {
            if !packed_identity_matches_filter(
                &key.schema_key,
                &key.row_pk,
                key.file_id.as_deref(),
                filter,
            ) {
                continue;
            }
            let identity = HeadIdentity {
                branch_id: branch_id.to_string(),
                generation,
                schema_key: key.schema_key,
                row_pk: key.row_pk,
                file_id: key.file_id,
            };
            let version = packed_compact_working_diff_version(&value);
            match selected.entry(identity) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(Some(version));
                }
                std::collections::btree_map::Entry::Occupied(mut entry)
                    if entry
                        .get()
                        .is_none_or(|previous| previous.commit_id < version.commit_id) =>
                {
                    entry.insert(Some(version));
                }
                std::collections::btree_map::Entry::Occupied(_) => {}
            }
        }
    }
    if actual_coverage != expected_coverage {
        if !has_matching_packed_ref {
            // The sparse dirty-key index is a derived accelerator. Complete
            // HOT primary rows carry the authoritative checkpoint baseline,
            // so an invalid index certificate must degrade to one local
            // current-state scan before canonical commit replay is considered.
            // Snapshot replicas intentionally keep historical commit bodies
            // cold; replaying them here turns one local read into a request per
            // checkpoint even though the answer is already in HOT state.
            return hot_working_diff_entries_from_primary(
                store,
                branch_id,
                checkpoint_commit_id,
                generation,
                filter,
            )
            .await;
        }
        return Ok(None);
    }
    let (selected, base_versions): (Vec<_>, Vec<_>) = selected.into_iter().unzip();
    let after_values = hot_load_primary_identity_bytes(store, &selected).await?;
    let mut candidates = Vec::with_capacity(selected.len());
    for ((identity, after), base_after) in selected.into_iter().zip(after_values).zip(base_versions)
    {
        let hot_after = if let Some(after) = after {
            let Ok(after) = decode_head_value(&after) else {
                return Ok(None);
            };
            // Not a classification, an inconsistency guard — and deliberately
            // stricter than the finite bypass, which merely skips an untracked
            // or absent primary row (`finite_working_diff_versions`).
            //
            // The two are equivalent because the populations differ. This loop
            // only visits identities the sparse `HOT_DIFF` index already
            // asserts are dirty against this checkpoint, and a dirty identity
            // always has a tracked primary row in this generation:
            //
            // * `HOT_DIFF` keys are only written for `!delta.untracked`
            //   deltas, both incrementally and from the file cascade.
            // * A primary row is physically removed only by
            //   `CurrentStateDelta::physically_deletes` — `untracked &&
            //   deleted`. A tracked delete writes a tombstone that keeps its
            //   baseline, so a dirty row cannot vanish.
            // * `reject_retention_change` forbids flipping retention while any
            //   physical member exists, so a dirty tracked row cannot be
            //   overwritten by an untracked one.
            // * The scope prefix contains the checkpoint and generation, so a
            //   foreign checkpoint owner cannot appear. A matching packed base
            //   can add identities beyond the sparse dirty index; its primary
            //   HOT row may validly be `Clean`, in which case that row is the
            //   checkpoint before-image for the newer packed value.
            //
            // The finite bypass instead reads *all* primary rows matching a
            // finite identity filter, where clean, untracked, and absent rows
            // are the normal case and skipping them is the classification. It
            // never sees this population. Keep the strict guard here apart
            // from the explicit clean-HOT/packed-overlay case below.
            if after.untracked {
                return Ok(None);
            }
            let baseline = after.working_diff_baseline;
            let Some(after) = after.working_diff_version() else {
                return Ok(None);
            };
            let before = if baseline == WorkingDiffBaseline::Clean
                && base_after.is_some_and(|packed| after.commit_id < packed.commit_id)
            {
                // A clean HOT row is the checkpoint state itself. When a
                // newer packed current base overlays it, that row is the exact
                // before image for comparing the packed replacement.
                Some(after)
            } else {
                let Some(before) = working_diff_baseline_before(baseline, checkpoint_commit_id)
                else {
                    return Ok(None);
                };
                before
            };
            Some((before, after))
        } else {
            None
        };
        let Some((before, after)) = choose_hot_or_packed_working_diff(hot_after, base_after) else {
            return Ok(None);
        };
        let identity = identity.into_row_identity();
        candidates.push((
            TrackedStateKey {
                schema_key: identity.schema_key,
                row_pk: identity.row_pk,
                file_id: identity.file_id,
            },
            before,
            after,
        ));
    }
    Ok(Some(
        classify_hot_working_diff_entries(store, candidates).await?,
    ))
}

fn choose_hot_or_packed_working_diff(
    hot: Option<(Option<WorkingDiffVersion>, WorkingDiffVersion)>,
    packed: Option<WorkingDiffVersion>,
) -> Option<(Option<WorkingDiffVersion>, WorkingDiffVersion)> {
    match (hot, packed) {
        (Some(hot), Some(packed)) if hot.1.commit_id < packed.commit_id => Some((hot.0, packed)),
        (Some(hot), _) => Some(hot),
        (None, Some(packed)) => Some((None, packed)),
        (None, None) => None,
    }
}

async fn hot_working_diff_entries_from_primary(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    checkpoint_commit_id: CommitId,
    generation: CommitId,
    filter: &TrackedStateFilter,
) -> Result<Option<Vec<TrackedStateDiffEntry>>, LixError> {
    #[cfg(test)]
    WORKING_DIFF_PATH_HITS
        .primary_scan
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let rows = hot_scan_entries(store, branch_id, generation, filter, None, None)
        .await?
        .expect("unbounded HOT scan cannot exhaust a byte budget");
    match rows {
        HotScanEntries::Decoded(rows) => {
            let mut candidates = Vec::with_capacity(rows.len());
            for (identity, bytes) in rows {
                let Some(versions) = finite_working_diff_versions(&bytes, checkpoint_commit_id)
                else {
                    return Ok(None);
                };
                let Some((before, after)) = versions else {
                    continue;
                };
                candidates.push((identity, before, after));
            }
            Ok(Some(
                classify_hot_working_diff_scan_entries(store, candidates).await?,
            ))
        }
        HotScanEntries::Finite(batches) => {
            let row_count = batches
                .iter()
                .map(|batch| batch.values.iter().flatten().count())
                .sum();
            let mut candidates = Vec::with_capacity(row_count);
            for batch in batches {
                for (index, bytes) in batch.values.into_iter().enumerate() {
                    let Some(bytes) = bytes else {
                        continue;
                    };
                    let Some(versions) = finite_working_diff_versions(&bytes, checkpoint_commit_id)
                    else {
                        return Ok(None);
                    };
                    let Some((before, after)) = versions else {
                        continue;
                    };
                    candidates.push((batch.identities.key_ref(index), before, after));
                }
            }
            Ok(Some(
                classify_hot_working_diff_entry_refs(store, candidates).await?,
            ))
        }
    }
}

/// Classifies one primary `HOT_ROW` value for the finite bypass.
///
/// `None` means "this scope cannot answer, replay canonically"; `Some(None)`
/// means "this row contributes no diff entry".
///
/// Skipping an untracked, clean, or foreign-checkpoint row here is not the
/// same decision the index-driven path makes for the same predicates — that
/// path fails closed. Both are correct because they classify different
/// populations: this one sees every primary row matching a finite identity
/// filter, where untracked/clean/absent rows are ordinary and not dirty, while
/// the index-driven path only ever sees identities `HOT_DIFF` already asserts
/// are dirty, where those same states would be corruption. See the equivalence
/// argument in `hot_working_diff_entries`; the reachable-state proof is
/// exercised end to end by
/// `working_diff_primary_scan_and_index_scan_agree_on_every_row_state`.
fn finite_working_diff_versions(
    bytes: &Bytes,
    checkpoint_commit_id: CommitId,
) -> Option<Option<(Option<WorkingDiffVersion>, WorkingDiffVersion)>> {
    let after = decode_head_value(bytes).ok()?;
    if after.untracked || after.working_diff_baseline == WorkingDiffBaseline::Clean {
        return Some(None);
    }
    if working_diff_checkpoint_owner(after.working_diff_baseline)
        .is_some_and(|owner| owner != checkpoint_commit_id)
    {
        return Some(None);
    }
    let before = working_diff_baseline_before(after.working_diff_baseline, checkpoint_commit_id)?;
    let after = after.working_diff_version()?;
    Some(Some((before, after)))
}

async fn classify_hot_working_diff_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    mut candidates: Vec<(
        TrackedStateKey,
        Option<WorkingDiffVersion>,
        WorkingDiffVersion,
    )>,
) -> Result<Vec<TrackedStateDiffEntry>, LixError> {
    resolve_working_diff_comparison_payloads(
        store,
        &mut candidates,
        |(key, _, _)| key.clone(),
        |(_, before, _)| before,
        |(_, _, after)| after,
    )
    .await?;
    let row_count = candidates.len();
    let mut keys = Vec::with_capacity(row_count);
    let mut versions = Vec::with_capacity(row_count);
    for (key, before, after) in candidates {
        keys.push(key);
        versions.push((before, after));
    }
    let identities = TrackedStateDiffIdentity::from_key_batch(keys)?;
    let mut entries = Vec::with_capacity(row_count);
    for (identity, (before, after)) in identities.into_iter().zip(versions) {
        if let Some(entry) = classify_hot_working_diff_entry(identity, before, after)? {
            entries.push(entry);
        }
    }
    Ok(entries)
}

async fn classify_hot_working_diff_entry_refs(
    store: &(impl StorageAdapterRead + ?Sized),
    mut candidates: Vec<(
        TrackedStateKeyRef<'_>,
        Option<WorkingDiffVersion>,
        WorkingDiffVersion,
    )>,
) -> Result<Vec<TrackedStateDiffEntry>, LixError> {
    resolve_working_diff_comparison_payloads(
        store,
        &mut candidates,
        |(key, _, _)| TrackedStateKey {
            schema_key: key.schema_key.to_owned(),
            file_id: key.file_id.map(str::to_owned),
            row_pk: key.row_pk.clone(),
        },
        |(_, before, _)| before,
        |(_, _, after)| after,
    )
    .await?;
    let row_count = candidates.len();
    let identities =
        TrackedStateDiffIdentity::from_key_refs(row_count, |index| candidates[index].0)?;
    let mut entries = Vec::with_capacity(row_count);
    for (identity, (_, before, after)) in identities.into_iter().zip(candidates) {
        if let Some(entry) = classify_hot_working_diff_entry(identity, before, after)? {
            entries.push(entry);
        }
    }
    Ok(entries)
}

async fn classify_hot_working_diff_scan_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    mut candidates: Vec<(
        HotScanIdentity,
        Option<WorkingDiffVersion>,
        WorkingDiffVersion,
    )>,
) -> Result<Vec<TrackedStateDiffEntry>, LixError> {
    resolve_working_diff_comparison_payloads(
        store,
        &mut candidates,
        |(identity, _, _)| TrackedStateKey {
            schema_key: identity.schema_key().to_owned(),
            file_id: identity.file_id().map(str::to_owned),
            row_pk: identity.row_pk.clone(),
        },
        |(_, before, _)| before,
        |(_, _, after)| after,
    )
    .await?;
    let row_count = candidates.len();
    let identities = TrackedStateDiffIdentity::from_key_refs(row_count, |index| {
        let identity = &candidates[index].0;
        TrackedStateKeyRef {
            schema_key: identity.schema_key(),
            file_id: identity.file_id(),
            row_pk: &identity.row_pk,
        }
    })?;
    let mut entries = Vec::with_capacity(row_count);
    for (identity, (_, before, after)) in identities.into_iter().zip(candidates) {
        if let Some(entry) = classify_hot_working_diff_entry(identity, before, after)? {
            entries.push(entry);
        }
    }
    Ok(entries)
}

#[derive(Debug, Clone, Copy)]
enum WorkingDiffPayloadSide {
    Before,
    After,
}

struct PendingWorkingDiffPayload {
    candidate_index: usize,
    side: WorkingDiffPayloadSide,
    key: TrackedStateKey,
}

/// Hydrates only the payload slots needed to compare two distinct live changes.
///
/// Root-backed before images and compact packed after images retain identity
/// and provenance without materializing payloads. Added, removed, and
/// same-change rows need no payload comparison. For the remaining ambiguous
/// live/live pairs, resolve every missing side from the local changelog in one
/// batch, then co-load all exact physical owners that were not locally present.
/// This keeps one ambiguous packed replacement from degrading the entire
/// working diff to canonical ancestry replay.
async fn resolve_working_diff_comparison_payloads<T>(
    store: &(impl StorageAdapterRead + ?Sized),
    candidates: &mut [T],
    key_of: impl Fn(&T) -> TrackedStateKey,
    before_of: impl Fn(&mut T) -> &mut Option<WorkingDiffVersion>,
    after_of: impl Fn(&mut T) -> &mut WorkingDiffVersion,
) -> Result<(), LixError> {
    let mut pending = Vec::new();
    for index in 0..candidates.len() {
        let before = *before_of(&mut candidates[index]);
        let after = *after_of(&mut candidates[index]);
        let Some(before) = before else {
            continue;
        };
        if before.deleted || after.deleted || before.change_id == after.change_id {
            continue;
        }
        let key = key_of(&candidates[index]);
        if before.payload_is_unresolved() {
            pending.push(PendingWorkingDiffPayload {
                candidate_index: index,
                side: WorkingDiffPayloadSide::Before,
                key: key.clone(),
            });
        }
        if after.payload_is_unresolved() {
            pending.push(PendingWorkingDiffPayload {
                candidate_index: index,
                side: WorkingDiffPayloadSide::After,
                key,
            });
        }
    }
    if pending.is_empty() {
        return Ok(());
    }
    let requests = pending
        .iter()
        .map(|pending| {
            let version = match pending.side {
                WorkingDiffPayloadSide::Before => before_of(
                    &mut candidates[pending.candidate_index],
                )
                .as_ref()
                .expect("pending before image is present"),
                WorkingDiffPayloadSide::After => {
                    after_of(&mut candidates[pending.candidate_index])
                }
            };
            crate::tracked_state::AuthoritativeLiveChangeRequest {
                change_id: version.change_id,
                source_commit_id: version.commit_id,
                key: pending.key.clone(),
                updated_at: version.updated_at,
            }
        })
        .collect::<Vec<_>>();
    let records = crate::tracked_state::load_authoritative_live_change_records(store, &requests)
        .await?;
    for (pending, record) in pending.into_iter().zip(records) {
        let version = match pending.side {
            WorkingDiffPayloadSide::Before => before_of(
                &mut candidates[pending.candidate_index],
            )
            .as_mut()
            .expect("pending before image is present"),
            WorkingDiffPayloadSide::After => {
                after_of(&mut candidates[pending.candidate_index])
            }
        };
        version.resolve_payload_slots(
            packed_working_diff_snapshot(record.snapshot.as_deref()),
            packed_working_diff_slot(&record.metadata),
        );
    }
    Ok(())
}

fn classify_hot_working_diff_entry(
    diff_identity: TrackedStateDiffIdentity,
    before: Option<WorkingDiffVersion>,
    after: WorkingDiffVersion,
) -> Result<Option<TrackedStateDiffEntry>, LixError> {
    let before_row = before.map(|version| version.into_diff_row(diff_identity.clone()));
    let after_row = after.into_diff_row(diff_identity.clone());
    match (
        before_row.as_ref().filter(|row| !row.deleted),
        (!after_row.deleted).then_some(&after_row),
    ) {
        (None, None) => Ok(None),
        (None, Some(_)) => Ok(Some(TrackedStateDiffEntry {
            identity: diff_identity,
            kind: TrackedStateDiffKind::Added,
            before: before_row,
            after: Some(after_row),
        })),
        (Some(_), None) => Ok(Some(TrackedStateDiffEntry {
            identity: diff_identity,
            kind: TrackedStateDiffKind::Removed,
            before: before_row,
            after: Some(after_row),
        })),
        (Some(_), Some(_)) => {
            let before = before.expect("a present before row implies a before version");
            match before.payload_equality(after) {
                WorkingDiffPayloadEquality::Equal => Ok(None),
                WorkingDiffPayloadEquality::Different => Ok(Some(TrackedStateDiffEntry {
                    identity: diff_identity,
                    kind: TrackedStateDiffKind::Modified,
                    before: before_row,
                    after: Some(after_row),
                })),
                // Never guess. Callers must resolve ambiguous payloads or
                // decline the accelerator before classification.
                WorkingDiffPayloadEquality::Unresolved => Err(head_value_error(
                    "working-diff classification reached an unresolved payload",
                )),
            }
        }
    }
}

async fn hot_scan_entries<'a>(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &'a str,
    generation: CommitId,
    filter: &'a TrackedStateFilter,
    limit: Option<usize>,
    retained_byte_budget: Option<usize>,
) -> Result<Option<HotScanEntries<'a>>, LixError> {
    // The null-file member is a true point key. A logical-PK scan can use a
    // single MultiGet only when this schema has no file-backed members; if it
    // does, fall through to the complete primary-prefix route so UPDATE and
    // DELETE still see every candidate member.
    #[cfg(feature = "storage-benches")]
    let is_blob_ref_probe = filter.schema_keys.len() == 1
        && filter.schema_keys[0] == "lix_binary_blob_ref"
        && !filter.row_pks.is_empty();
    // Site A: the `scan_lix_file_live_batch` request shape. Keyed on the schema
    // pair alone so the gate is arm-invariant.
    #[cfg(feature = "storage-benches")]
    let is_file_live_batch = filter.schema_keys.len() == 2
        && filter
            .schema_keys
            .iter()
            .any(|key| key == "lix_file_descriptor")
        && filter
            .schema_keys
            .iter()
            .any(|key| key == "lix_binary_blob_ref");
    #[cfg(feature = "storage-benches")]
    if is_file_live_batch {
        crate::storage_bench::record_file_live_scan_call();
    }
    #[cfg(feature = "storage-benches")]
    if is_blob_ref_probe {
        crate::storage_bench::record_hot_blob_ref_scan_call();
    }
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_hot_scan_call();

    if let Some(identities) = hot_exact_identity_batches(branch_id, generation, filter) {
        let may_use_null_point_batch = !filter.file_ids.is_empty()
            || !hot_schema_has_file_members(store, branch_id, generation, &filter.schema_keys)
                .await?;
        if may_use_null_point_batch {
            #[cfg(feature = "storage-benches")]
            if is_blob_ref_probe {
                crate::storage_bench::record_hot_blob_ref_scan_point_batch();
            }
            #[cfg(feature = "storage-benches")]
            {
                crate::storage_bench::record_hot_scan_point_batch();
                if is_file_live_batch {
                    crate::storage_bench::record_file_live_scan_point_batch();
                }
            }
            let entries = HotScanEntries::Finite(
                hot_scan_finite_identity_batches(store, identities, limit).await?,
            );
            return Ok(hot_scan_entries_fit_budget(entries, retained_byte_budget));
        }
    }

    // The authoritative hot index is file-first, so filesystem queries such as
    // `WHERE file_id = $1` read one contiguous hydrated range without a second
    // value projection or random point-read hydration.
    if let Some(prefixes) = hot_file_scan_prefixes(branch_id, generation, filter) {
        #[cfg(feature = "storage-benches")]
        if is_blob_ref_probe {
            crate::storage_bench::record_hot_blob_ref_scan_file_prefix();
        }
        #[cfg(feature = "storage-benches")]
        {
            crate::storage_bench::record_hot_scan_file_prefix();
            if is_file_live_batch {
                crate::storage_bench::record_file_live_scan_file_prefix();
            }
        }
        let entries = HotScanEntries::Decoded(
            scan_hot_file_entries(store, branch_id, generation, prefixes, filter, limit).await?,
        );
        return Ok(hot_scan_entries_fit_budget(entries, retained_byte_budget));
    }

    #[cfg(feature = "storage-benches")]
    if is_blob_ref_probe {
        crate::storage_bench::record_hot_blob_ref_scan_fallback();
    }
    #[cfg(feature = "storage-benches")]
    {
        crate::storage_bench::record_hot_scan_fallback(!filter.row_pks.is_empty());
        if is_file_live_batch {
            crate::storage_bench::record_file_live_scan_fallback();
        }
    }
    let scope = hot_scope_prefix(branch_id, generation);
    let mut prefixes = hot_row_scan_prefixes(&scope, filter);
    prefixes.sort();
    prefixes.dedup();
    let mut rows = Vec::new();
    let mut saw_file_backed_row = false;
    let mut retained_bytes = 0_usize;
    // A fixed file bucket has the same physical and logical order. Every
    // broader file domain must defer LIMIT until file-first storage order has
    // been restored to canonical `(schema, row_pk, file_id)` order.
    let physical_limit = limit.filter(|_| hot_filter_has_one_fixed_file_bucket(filter));
    for prefix in prefixes {
        let range = StoragePrefix {
            bytes: Bytes::from(prefix),
        }
        .to_range()?;
        let mut cursor = store
            .begin_scan(ROW_SPACE, range, StorageBeginScanOptions::default())
            .await?;
        loop {
            let remaining = physical_limit.map(|limit| limit.saturating_sub(rows.len()));
            if matches!(remaining, Some(0)) {
                let rows = if saw_file_backed_row {
                    canonicalize_hot_scan_rows(rows, limit)?
                } else {
                    rows
                };
                return Ok(Some(HotScanEntries::Decoded(rows)));
            }
            // Deliberately bounded: this reader stops at the caller's LIMIT.
            let (page, page_has_more) = cursor
                .next_page(remaining.unwrap_or(crate::storage_adapter::MAX_SCAN_PAGE_ROWS))
                .await?
                .into_parts();
            for entry in page {
                let encoded_key_bytes = entry.key.0.len();
                let identity = decode_hot_scan_row_key_in_scope(entry.key.0, &scope)?;
                let entry_matches_filter = identity.matches_filter(filter);
                #[cfg(any(test, feature = "storage-benches"))]
                {
                    HOT_SCAN_DECODED_ENTRIES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                #[cfg(feature = "storage-benches")]
                {
                    if is_blob_ref_probe {
                        crate::storage_bench::record_hot_blob_ref_scan_entry(entry_matches_filter);
                    }
                    crate::storage_bench::record_hot_scan_fallback_entry(entry_matches_filter);
                }
                #[cfg(feature = "storage-benches")]
                if is_file_live_batch {
                    crate::storage_bench::record_file_live_scan_entry(entry_matches_filter);
                }
                if entry_matches_filter {
                    saw_file_backed_row |= identity.file_id().is_some();
                    let value = full_value_bytes(entry.value)?;
                    #[cfg(any(test, feature = "storage-benches"))]
                    {
                        HOT_SCAN_MATCHED_ENTRIES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if value.len() > 1 && value[1] & 0b0000_0001 != 0 {
                            HOT_SCAN_TOMBSTONE_ENTRIES
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                    retained_bytes = retained_bytes
                        .checked_add(encoded_key_bytes)
                        .and_then(|bytes| bytes.checked_add(value.len()))
                        .and_then(|bytes| bytes.checked_add(size_of::<(HotScanIdentity, Bytes)>()))
                        .ok_or_else(|| head_value_error("HOT scan resident byte size overflow"))?;
                    if retained_byte_budget.is_some_and(|budget| retained_bytes > budget) {
                        return Ok(None);
                    }
                    rows.push((identity, value));
                    if physical_limit.is_some_and(|limit| rows.len() >= limit) {
                        let rows = if saw_file_backed_row {
                            canonicalize_hot_scan_rows(rows, limit)?
                        } else {
                            rows
                        };
                        return Ok(Some(HotScanEntries::Decoded(rows)));
                    }
                }
            }
            if !page_has_more {
                break;
            }
        }
    }
    if saw_file_backed_row {
        rows = canonicalize_hot_scan_rows(rows, limit)?;
    } else if let Some(limit) = limit {
        rows.truncate(limit);
    }
    Ok(Some(HotScanEntries::Decoded(rows)))
}

fn hot_filter_has_one_fixed_file_bucket(filter: &TrackedStateFilter) -> bool {
    let Some(first) = filter.file_ids.first() else {
        return false;
    };
    !matches!(first, NullableKeyFilter::Any)
        && filter.file_ids.iter().all(|file_id| file_id == first)
}

/// Restores the logical live-state identity order before any caller observes
/// rows or applies LIMIT.
///
/// One physical HOT primary key is the sole authority for its logical
/// identity. Repeated scans may therefore collapse only byte-identical copies
/// of that same key. Distinct keys or values for one logical identity are an
/// invalid authority state and fail closed instead of selecting a second
/// winner.
fn canonicalize_hot_scan_rows(
    mut rows: Vec<(HotScanIdentity, Bytes)>,
    limit: Option<usize>,
) -> Result<Vec<(HotScanIdentity, Bytes)>, LixError> {
    let already_strictly_ordered = rows
        .windows(2)
        .all(|pair| pair[0].0.cmp(&pair[1].0).is_lt());
    if !already_strictly_ordered {
        rows.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        for pair in rows.windows(2) {
            if pair[0].0 != pair[1].0 {
                continue;
            }
            if pair[0].0.key != pair[1].0.key || pair[0].1 != pair[1].1 {
                return Err(head_value_error(format!(
                    "duplicate HOT authority for schema '{}' row_pk {:?} file_id {:?} has different physical bytes",
                    pair[0].0.schema_key(),
                    pair[0].0.row_pk,
                    pair[0].0.file_id(),
                )));
            }
        }
        rows.dedup_by(|left, right| left.0 == right.0);
    }
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    Ok(rows)
}

fn hot_scan_entries_fit_budget<'a>(
    entries: HotScanEntries<'a>,
    retained_byte_budget: Option<usize>,
) -> Option<HotScanEntries<'a>> {
    let Some(budget) = retained_byte_budget else {
        return Some(entries);
    };
    let retained_bytes = match &entries {
        HotScanEntries::Decoded(rows) => rows
            .capacity()
            .saturating_mul(size_of::<(HotScanIdentity, Bytes)>())
            .saturating_add(rows.iter().fold(0_usize, |bytes, (identity, value)| {
                bytes
                    .saturating_add(identity.key.len())
                    .saturating_add(value.len())
                    .saturating_add(match &identity.schema_key {
                        HotScanString::Borrowed(_) => 0,
                        HotScanString::Owned(value) => value.capacity(),
                    })
                    .saturating_add(match &identity.file_id {
                        None | Some(HotScanString::Borrowed(_)) => 0,
                        Some(HotScanString::Owned(value)) => value.capacity(),
                    })
            })),
        HotScanEntries::Finite(batches) => batches
            .capacity()
            .saturating_mul(size_of::<FiniteHotEntryBatchRef<'_>>())
            .saturating_add(batches.iter().fold(0_usize, |bytes, batch| {
                bytes
                    .saturating_add(
                        batch
                            .identities
                            .identities
                            .capacity()
                            .saturating_mul(size_of::<FiniteHotIdentityRef<'_>>()),
                    )
                    .saturating_add(batch.identities.encoded.bytes.len())
                    .saturating_add(
                        batch
                            .identities
                            .encoded
                            .ranges
                            .capacity()
                            .saturating_mul(size_of::<EncodedHotPointKeyRanges>()),
                    )
                    .saturating_add(
                        batch
                            .values
                            .capacity()
                            .saturating_mul(size_of::<Option<Bytes>>()),
                    )
                    .saturating_add(
                        batch
                            .values
                            .iter()
                            .flatten()
                            .fold(0_usize, |bytes, value| bytes.saturating_add(value.len())),
                    )
            })),
    };
    (retained_bytes <= budget).then_some(entries)
}

async fn hot_scan_dense_identity_range(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: &FiniteHotIdentityBatchRef<'_>,
) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
    hot_scan_dense_encoded_key_range(store, identities.len(), |index| {
        identities.encoded.primary_key_bytes(index)
    })
    .await
}

async fn hot_scan_dense_encoded_key_range<'a>(
    store: &(impl StorageAdapterRead + ?Sized),
    key_count: usize,
    key_at: impl Fn(usize) -> &'a [u8],
) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
    if key_count < HOT_DENSE_SCAN_MIN_IDENTITIES {
        return Ok(None);
    }
    if key_count == 0 {
        return Ok(Some(Vec::new()));
    }
    if (1..key_count).any(|index| key_at(index - 1) > key_at(index)) {
        return Ok(None);
    }
    let first_key = StorageKey(Bytes::copy_from_slice(key_at(0)));
    let last_key = StorageKey(Bytes::copy_from_slice(key_at(key_count - 1)));
    let range = crate::storage_adapter::StorageKeyRange {
        lower: std::ops::Bound::Included(first_key),
        upper: std::ops::Bound::Included(last_key),
    };
    let scan_budget = key_count.saturating_mul(HOT_DENSE_SCAN_MAX_OVERREAD);
    let mut scanned = 0;
    let mut requested_index = 0;
    let mut values = vec![None; key_count];
    let mut cursor = store
        .begin_scan(ROW_SPACE, range, StorageBeginScanOptions::default())
        .await?;
    loop {
        let remaining_budget = scan_budget.saturating_sub(scanned);
        if remaining_budget == 0 {
            return Ok(None);
        }
        // Deliberately bounded: this reader gives up once it has burned its
        // scan budget rather than reading an unbounded range.
        let (page, page_has_more) = cursor
            .next_page(remaining_budget.min(crate::storage_adapter::MAX_SCAN_PAGE_ROWS))
            .await?
            .into_parts();
        scanned += page.len();
        for entry in page {
            while requested_index < key_count && key_at(requested_index) < entry.key.0.as_ref() {
                requested_index += 1;
            }
            if requested_index < key_count && key_at(requested_index) == entry.key.0.as_ref() {
                values[requested_index] = Some(full_value_bytes(entry.value)?);
                requested_index += 1;
            }
        }
        if requested_index == key_count || !page_has_more {
            return Ok(Some(values));
        }
    }
}

fn hot_row_scan_prefixes(scope: &[u8], filter: &TrackedStateFilter) -> Vec<Vec<u8>> {
    if filter.schema_keys.is_empty() {
        return vec![scope.to_vec()];
    }
    filter
        .schema_keys
        .iter()
        .map(|schema_key| {
            let mut prefix = scope.to_vec();
            write_key_string(&mut prefix, schema_key, KEY_PART_FINAL);
            prefix
        })
        .collect()
}

fn hot_file_scan_prefixes(
    branch_id: &str,
    generation: CommitId,
    filter: &TrackedStateFilter,
) -> Option<Vec<Vec<u8>>> {
    if filter.schema_keys.is_empty()
        || filter.file_ids.is_empty()
        || !filter.row_pks.is_empty()
        || filter
            .file_ids
            .iter()
            .any(|file_id| matches!(file_id, NullableKeyFilter::Any))
    {
        return None;
    }
    let mut prefixes = Vec::with_capacity(filter.schema_keys.len() * filter.file_ids.len());
    for schema_key in &filter.schema_keys {
        for file_id in &filter.file_ids {
            let mut prefix = hot_scope_prefix(branch_id, generation);
            write_key_string(&mut prefix, schema_key, KEY_PART_FINAL);
            match file_id {
                NullableKeyFilter::Null => write_file_id(&mut prefix, None),
                NullableKeyFilter::Value(file_id) => write_file_id(&mut prefix, Some(file_id)),
                NullableKeyFilter::Any => {
                    unreachable!("file-id projection predicate was checked above")
                }
            }
            prefixes.push(prefix);
        }
    }
    prefixes.sort();
    prefixes.dedup();
    Some(prefixes)
}

async fn scan_hot_file_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    prefixes: Vec<Vec<u8>>,
    filter: &TrackedStateFilter,
    limit: Option<usize>,
) -> Result<Vec<(HotScanIdentity, Bytes)>, LixError> {
    let scope = hot_scope_prefix(branch_id, generation);
    let mut rows = Vec::new();
    for prefix in prefixes {
        let Some(range) = hot_file_row_pk_range(prefix, filter)? else {
            continue;
        };
        let mut cursor = store
            .begin_scan(ROW_SPACE, range, StorageBeginScanOptions::default())
            .await?;
        loop {
            let (page, page_has_more) = cursor
                .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
                .await?
                .into_parts();
            for entry in page {
                let identity = decode_hot_scan_row_key_in_scope(entry.key.0, &scope)?;
                #[cfg(feature = "storage-benches")]
                {
                    if filter.schema_keys.len() == 2
                        && filter
                            .schema_keys
                            .iter()
                            .any(|key| key == "lix_file_descriptor")
                        && filter
                            .schema_keys
                            .iter()
                            .any(|key| key == "lix_binary_blob_ref")
                    {
                        crate::storage_bench::record_file_live_scan_entry(
                            identity.matches_filter(filter),
                        );
                    }
                }
                #[cfg(any(test, feature = "storage-benches"))]
                {
                    HOT_SCAN_DECODED_ENTRIES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                if identity.matches_filter(filter) {
                    let value = full_value_bytes(entry.value)?;
                    #[cfg(any(test, feature = "storage-benches"))]
                    {
                        HOT_SCAN_MATCHED_ENTRIES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if value.len() > 1 && value[1] & 0b0000_0001 != 0 {
                            HOT_SCAN_TOMBSTONE_ENTRIES
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                    rows.push((identity, value));
                }
            }
            if !page_has_more {
                break;
            }
        }
    }
    // Physical rows are ordered `(schema, file_id, row_pk)`, while SQL rows
    // are ordered `(schema, row_pk, file_id)`.
    canonicalize_hot_scan_rows(rows, limit)
}

/// Narrows one canonical `(branch, generation, schema, file)` partition to
/// the requested typed primary-key interval. The row-key codec is the same
/// sole-writer codec used for point keys, so these are storage bounds over the
/// authority itself rather than a secondary locator.
fn hot_file_row_pk_range(
    prefix: Vec<u8>,
    filter: &TrackedStateFilter,
) -> Result<Option<crate::storage_adapter::StorageKeyRange>, LixError> {
    let mut range = StoragePrefix {
        bytes: Bytes::copy_from_slice(&prefix),
    }
    .to_range()?;
    if let Some(lower) = filter.row_pk_lower.as_ref() {
        let mut key = prefix.clone();
        write_row_pk(&mut key, &lower.row_pk);
        if !lower.inclusive {
            let Some(successor) = hot_index_key_successor(&key) else {
                return Ok(None);
            };
            key = successor;
        }
        range.lower = std::ops::Bound::Included(StorageKey(Bytes::from(key)));
    }
    if let Some(upper) = filter.row_pk_upper.as_ref() {
        let mut key = prefix;
        write_row_pk(&mut key, &upper.row_pk);
        if upper.inclusive {
            let Some(successor) = hot_index_key_successor(&key) else {
                return Ok(None);
            };
            key = successor;
        }
        range.upper = std::ops::Bound::Excluded(StorageKey(Bytes::from(key)));
    }
    if let (std::ops::Bound::Included(lower), std::ops::Bound::Excluded(upper)) =
        (&range.lower, &range.upper)
        && lower >= upper
    {
        return Ok(None);
    }
    Ok(Some(range))
}

async fn hot_schema_has_file_members(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    schema_keys: &[String],
) -> Result<bool, LixError> {
    // Exact identity batches always name their schema. Broad scans deliberately
    // take the primary-prefix route, which already sees all file members.
    if schema_keys.is_empty() {
        return Ok(true);
    }
    for schema_key in schema_keys {
        if hot_schema_has_file_member(store, branch_id, generation, schema_key).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn hot_schema_has_file_member(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
) -> Result<bool, LixError> {
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_hot_scan_file_member_guard_read();
    let scope = hot_scope_prefix(branch_id, generation);
    let key = StorageKey(Bytes::from(encode_hot_file_schema_key(&scope, schema_key)));
    let values = PointReadPlan::new(FILE_SPACE, &[key])
        .materialize(
            store,
            StorageGetOptions {
                projection: StorageCoreProjection::KeyOnly,
            },
        )
        .await?;
    Ok(values.value.into_iter().next().flatten().is_some())
}

fn hot_scope_prefix(branch_id: &str, generation: CommitId) -> Vec<u8> {
    encode_scope_prefix(branch_id, generation)
}

#[cfg(test)]
pub(super) fn encode_hot_row_key(identity: &HeadIdentity) -> Vec<u8> {
    encode_hot_row_key_parts(
        &identity.branch_id,
        identity.generation,
        &identity.schema_key,
        &identity.row_pk,
        identity.file_id.as_deref(),
    )
}

fn encode_hot_row_key_parts(
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
    row_pk: &RowPk,
    file_id: Option<&str>,
) -> Vec<u8> {
    let mut key = hot_scope_prefix(branch_id, generation);
    write_key_string(&mut key, schema_key, KEY_PART_FINAL);
    write_file_id(&mut key, file_id);
    write_row_pk(&mut key, row_pk);
    key
}

#[cfg(test)]
pub(crate) fn encode_hot_row_key_for_test(
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
    row_pk: &RowPk,
    file_id: Option<&str>,
) -> Vec<u8> {
    encode_hot_row_key_parts(branch_id, generation, schema_key, row_pk, file_id)
}

fn validate_exact_collection_member(
    branch_id: &str,
    branch_generation: CommitId,
    scope_prefix: &[u8],
    scope: crate::collection_generation::CollectionScopeRef<'_>,
    required_identity: TrackedStateKeyRef<'_>,
    expected_untracked: bool,
    raw_key: &[u8],
    raw_value: &[u8],
) -> Result<Option<HeadRowIdentity>, LixError> {
    let identity = decode_hot_row_key_in_scope(raw_key, scope_prefix)?;
    if identity.schema_key != scope.schema_key
        || scope
            .file_id
            .is_some_and(|file_id| identity.file_id.as_deref() != Some(file_id))
    {
        return Err(head_value_error(
            "selected collection scan escaped its exact scope",
        ));
    }
    let canonical = encode_hot_row_key_parts(
        branch_id,
        branch_generation,
        &identity.schema_key,
        &identity.row_pk,
        identity.file_id.as_deref(),
    );
    validate_canonical_exact_collection_key(raw_key, &canonical)?;
    let value = decode_head_value(raw_value)?;
    let is_required_identity = identity.schema_key == required_identity.schema_key
        && identity.row_pk == *required_identity.row_pk
        && identity.file_id.as_deref() == required_identity.file_id;
    if value.deleted {
        if !is_required_identity {
            return Ok(None);
        }
        return Err(head_value_error(
            "required collection identity is a tombstone instead of a live member",
        ));
    }
    if is_required_identity && value.untracked != expected_untracked {
        return Err(head_value_error(
            "required collection identity belongs to the wrong state domain",
        ));
    }
    if is_required_identity {
        return Err(head_value_error(
            "required point miss omitted a live collection authority member",
        ));
    }
    Ok(Some(identity))
}

fn validate_canonical_exact_collection_key(
    raw_key: &[u8],
    canonical_key: &[u8],
) -> Result<(), LixError> {
    if canonical_key != raw_key {
        return Err(head_value_error(
            "selected collection contains a non-canonical identity key",
        ));
    }
    Ok(())
}

#[cfg(test)]
fn encode_hot_diff_key(checkpoint_commit_id: CommitId, identity: &HeadIdentity) -> Vec<u8> {
    encode_hot_diff_key_parts(
        &identity.branch_id,
        checkpoint_commit_id,
        identity.generation,
        &identity.schema_key,
        &identity.row_pk,
        identity.file_id.as_deref(),
    )
}

#[cfg(test)]
fn encode_hot_diff_key_parts(
    branch_id: &str,
    checkpoint_commit_id: CommitId,
    generation: CommitId,
    schema_key: &str,
    row_pk: &RowPk,
    file_id: Option<&str>,
) -> Vec<u8> {
    let scope = encode_working_diff_scope_prefix(branch_id, checkpoint_commit_id, generation);
    let mut key = Vec::with_capacity(
        encoded_hot_identity_key_len(scope.len(), schema_key, row_pk, file_id).unwrap_or(0),
    );
    append_hot_diff_key_parts(&mut key, &scope, schema_key, row_pk, file_id);
    key
}

/// Appends one dirty-row identity as a `HOT_DIFF` key (also used verbatim as
/// the identity's coverage group key).
///
/// The component order after the scope — `schema_key ++ row_pk ++ file_id`
/// — deliberately does not mirror `HOT_ROW`'s `schema_key ++ file_id ++
/// row_pk`, and aligning them would buy nothing: no reader ever
/// prefix-seeks `HOT_DIFF` below its `(branch, checkpoint, generation)`
/// scope.
///
/// - Any reader that *consults* this index must enumerate the entire scope to
///   reconstruct the [`WorkingDiffIndexCoverage`] count/XOR proof before the
///   sparse index may be trusted, so a schema-filtered diff query that takes
///   the index path visits every entry and filters in memory. A sub-scope seek
///   can never satisfy the proof.
/// - Finite (schema + row) and file-scoped diff queries bypass this space
///   entirely and read the `working_diff_baseline` inline on primary `HOT_ROW`
///   rows, so they never pay the proof at all. See
///   [`hot_working_diff_bypass_filter`].
/// - Batches of `HOT_DIFF_PACK_MIN_IDENTITIES` or more identities are packed
///   into segments keyed by `scope ++ digest`; the identity components leave
///   the key for the segment value altogether.
///
/// The order is not arbitrary in one respect: it is exactly
/// `compare_hot_deltas`, the order a publication's deltas are already sorted
/// in, so the segment packer can append identity suffixes without a second
/// sort. It is otherwise a load-bearing input to the coverage hash: every
/// writer, the segment visitor, and the stored epoch coverage must produce
/// these bytes identically. Change it only for a reason, and only everywhere
/// at once.
///
/// # Why the proof is not partitioned
///
/// [`WorkingDiffIndexCoverage`] is a monoid — `group_count` is additive and
/// `group_key_xor` is commutative — so it looks like it could be maintained
/// per `file_id`, composed back into the scope proof, and a
/// `scope ++ file_id` seek made legal. Two independent facts block that:
///
/// - **Not every coverage group is an identity.** A packed current base
///   contributes exactly one group key naming a *commit*
///   (`hot_scope_prefix ++ new_head`, see the collection-replacement writer),
///   standing for a whole schema collection whose members span every file and
///   are recovered from the compact commit-delta member index at read time.
///   Attributing it to file partitions means expanding it per
///   member on the write path — reinstating the per-identity coverage cost
///   the manifest exists to remove.
/// - **A partition-keyed segment is not a packed segment.** A segment batches
///   whatever identities one publication produced, in `compare_hot_deltas`
///   order — which is this key's `schema_key ++ row_pk ++ file_id`, so
///   `file_id` is the *last* discriminator and identities of one file are
///   scattered through the batch. Moving the partition ahead of the digest
///   forces the packer to re-sort and split each publication by `file_id`, so
///   a bulk edit spread over many files emits one segment per file — each
///   re-paying the scope and a 32-byte digest — and most publications stop
///   reaching `HOT_DIFF_PACK_MIN_IDENTITIES` per partition at all.
///
/// Measured consequence of routing a file-scoped read *through* this proof
/// (rocksdb, hetzner-cpx62-II, `working_diff_file_scope`, 9 reps, four dirty
/// rows in the probed file): `WHERE file_id = $1` cost 0.61 ms at 1k total
/// dirty rows and 51.6 ms at 100k — linear in the dirty set, flat in the
/// answer — while the finite `schema_key + row_pk + file_id` shape that
/// bypasses this space stayed at ~0.5 ms across the same range.
///
/// That is why file-scoped reads no longer take this path. The route that
/// removes the scan does not need this space at all: `HOT_ROW` is keyed
/// `schema_key ++ file_id ++ row_pk` and `hot_scan_entries` owns a
/// file-first prefix route, so a file-scoped working-diff read enumerates
/// primary rows the way the finite bypass does, trading O(dirty rows in the
/// branch) for O(live rows in the file). [`hot_working_diff_bypass_filter`]
/// implements it and carries the soundness argument.
///
/// The one obligation that route cannot discharge is a packed current base
/// published inside this checkpoint window, whose members own no `HOT_ROW` row
/// and contribute exactly the whole-commit coverage groups described above.
/// That case is excluded by the caller's pre-existing `packed_refs.is_empty()`
/// guard and still reads the index, proof and all — which is why the proof
/// stays scope-global and this key order stays as it is.
///
/// The ordinary **row** surface has no such obligation and does take that
/// route: `lixcol_file_id` is an exact provider constraint that lands in
/// `HotStateFilter::file_ids`, and every authority the live-state merge reads
/// — `HOT_ROW`, the packed current base, and the root current base — filters
/// on it. Rows that never had a branch-local `HOT_ROW` are therefore still
/// returned by the other two legs.
fn append_hot_diff_key_parts(
    key_bytes: &mut Vec<u8>,
    scope: &[u8],
    schema_key: &str,
    row_pk: &RowPk,
    file_id: Option<&str>,
) -> Range<usize> {
    let start = key_bytes.len();
    key_bytes.extend_from_slice(scope);
    write_key_string(key_bytes, schema_key, KEY_PART_FINAL);
    write_row_pk(key_bytes, row_pk);
    write_file_id(key_bytes, file_id);
    start..key_bytes.len()
}

fn encoded_hot_identity_key_len(
    scope_len: usize,
    schema_key: &str,
    row_pk: &RowPk,
    file_id: Option<&str>,
) -> Option<usize> {
    let file_id_len = match file_id {
        Some(file_id) => encoded_key_bytes_len(file_id.as_bytes())?,
        None => 0,
    };
    scope_len
        .checked_add(encoded_key_bytes_len(schema_key.as_bytes())?)?
        .checked_add(encoded_row_pk_len(row_pk)?)?
        .checked_add(1)?
        .checked_add(file_id_len)
}

fn encoded_row_pk_len(row_pk: &RowPk) -> Option<usize> {
    row_pk
        .components
        .iter()
        .try_fold(1_usize, |total, component| {
            let payload_len = match component {
                crate::row_pk::RowPkComponent::Uuid(_) => Some(16 + 1),
                crate::row_pk::RowPkComponent::Integer(_) => Some(8 + 1),
                crate::row_pk::RowPkComponent::String(value) => {
                    encoded_key_bytes_len(value.as_bytes())
                }
                crate::row_pk::RowPkComponent::Bytes(value) => {
                    encoded_key_bytes_len(value.as_ref())
                }
            }?;
            total.checked_add(1)?.checked_add(payload_len)
        })
}

fn encoded_key_bytes_len(value: &[u8]) -> Option<usize> {
    value
        .len()
        .checked_add(memchr::memchr_iter(KEY_PART_FINAL, value).count())?
        .checked_add(2)
}

fn decode_hot_scan_row_key_in_scope(key: Bytes, scope: &[u8]) -> Result<HotScanIdentity, LixError> {
    if !key.starts_with(scope) {
        return Err(key_codec_error(
            "hot row does not begin with its scanned scope",
        ));
    }
    let mut offset = scope.len();
    let (schema_key, schema_terminator) =
        read_hot_scan_key_string(&key, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot row schema key has an invalid terminator",
        ));
    }
    let file_id = read_hot_scan_file_id(&key, &mut offset)?;
    let row_pk = read_hot_scan_row_pk(&key, &mut offset)?;
    if offset != key.len() {
        return Err(key_codec_error("hot row key has trailing bytes"));
    }
    #[cfg(feature = "storage-benches")]
    {
        crate::storage_bench::record_hot_scan_row_decoded();
    }
    Ok(HotScanIdentity {
        key,
        schema_key,
        row_pk,
        file_id,
    })
}

/// Test-only shim; see `crate::order_preserving_key::tests`.
#[cfg(test)]
pub(crate) fn hot_decode_row_pk_probe(bytes: &[u8]) -> Option<(RowPk, usize)> {
    let mut offset = 0usize;
    read_hot_scan_row_pk(&Bytes::copy_from_slice(bytes), &mut offset)
        .ok()
        .map(|row_pk| (row_pk, offset))
}

fn read_hot_scan_row_pk(bytes: &Bytes, offset: &mut usize) -> Result<RowPk, LixError> {
    let version = bytes
        .get(*offset)
        .copied()
        .ok_or_else(|| key_codec_error("is truncated before row primary key version"))?;
    *offset += 1;
    if version != ROW_PK_CODEC_V1 {
        return Err(key_codec_error(&format!(
            "has unsupported row primary key codec version {version}"
        )));
    }
    let mut components = SmallVec::new();
    loop {
        let (part, terminator) = read_hot_scan_row_pk_part(bytes, offset)?;
        components.push(part);
        match terminator {
            KEY_PART_FINAL => break,
            KEY_PART_MORE => {}
            _ => {
                return Err(key_codec_error("row primary key has an invalid terminator"));
            }
        }
    }
    RowPk::from_components(components)
        .map_err(|error| key_codec_error(&format!("contains an invalid row primary key: {error}")))
}

fn read_hot_scan_row_pk_part(
    bytes: &Bytes,
    offset: &mut usize,
) -> Result<(crate::row_pk::RowPkComponent, u8), LixError> {
    let tag = bytes
        .get(*offset)
        .copied()
        .ok_or_else(|| key_codec_error("is truncated before row primary key part tag"))?;
    *offset += 1;
    match tag {
        ROW_PK_STRING => {
            let (value, terminator) = read_hot_scan_key_string(bytes, offset, "row primary key")?;
            Ok((
                crate::row_pk::RowPkComponent::String(value.into_shared_str(bytes)),
                terminator,
            ))
        }
        ROW_PK_BYTES => {
            let (value, terminator) =
                read_hot_scan_shared_bytes(bytes, offset, "row primary key bytes")?;
            Ok((crate::row_pk::RowPkComponent::Bytes(value), terminator))
        }
        ROW_PK_UUID => {
            let uuid_end = offset
                .checked_add(ROW_PK_UUID_BYTES)
                .ok_or_else(|| key_codec_error("UUIDv7 row primary key offset overflow"))?;
            let uuid_bytes: [u8; 16] = bytes
                .get(*offset..uuid_end)
                .ok_or_else(|| key_codec_error("is truncated in UUIDv7 row primary key"))?
                .try_into()
                .expect("UUIDv7 slice has fixed length");
            let terminator = bytes
                .get(uuid_end)
                .copied()
                .ok_or_else(|| key_codec_error("is truncated after UUIDv7 row primary key"))?;
            if !is_key_part_terminator(terminator) {
                return Err(key_codec_error(
                    "UUIDv7 row primary key has an invalid terminator",
                ));
            }
            *offset = uuid_end + 1;
            Ok((crate::row_pk::RowPkComponent::Uuid(uuid_bytes), terminator))
        }
        ROW_PK_INTEGER => {
            let integer_end = offset
                .checked_add(ROW_PK_INTEGER_BYTES)
                .ok_or_else(|| key_codec_error("integer row primary key offset overflow"))?;
            let ordered = u64::from_be_bytes(
                bytes
                    .get(*offset..integer_end)
                    .ok_or_else(|| key_codec_error("is truncated in integer row primary key"))?
                    .try_into()
                    .expect("integer slice has fixed length"),
            );
            let terminator = bytes
                .get(integer_end)
                .copied()
                .ok_or_else(|| key_codec_error("is truncated after integer row primary key"))?;
            if !is_key_part_terminator(terminator) {
                return Err(key_codec_error(
                    "integer row primary key has an invalid terminator",
                ));
            }
            *offset = integer_end + 1;
            Ok((
                crate::row_pk::RowPkComponent::Integer(i64_from_ordered_integer(ordered)),
                terminator,
            ))
        }
        _ => Err(key_codec_error("has an unknown row primary key part tag")),
    }
}

fn read_hot_scan_file_id(
    bytes: &Bytes,
    offset: &mut usize,
) -> Result<Option<HotScanString>, LixError> {
    let tag = *bytes
        .get(*offset)
        .ok_or_else(|| key_codec_error("is truncated before file id"))?;
    *offset += 1;
    match tag {
        FILE_ID_NONE => Ok(None),
        FILE_ID_SOME => {
            let (file_id, terminator) = read_hot_scan_key_string(bytes, offset, "file id")?;
            if terminator != KEY_PART_FINAL {
                return Err(key_codec_error("file id has an invalid terminator"));
            }
            Ok(Some(file_id))
        }
        _ => Err(key_codec_error("has an invalid file id tag")),
    }
}

fn read_hot_scan_key_string(
    bytes: &Bytes,
    offset: &mut usize,
    field: &str,
) -> Result<(HotScanString, u8), LixError> {
    let part = scan_key_part(bytes.as_ref(), *offset)
        .map_err(|error| head_key_part_error(error, field))?;
    *offset = part.end;
    match part.value {
        // No escape: the part is a span of the key, so it stays shared rather
        // than becoming an owned buffer for generated schema and file ids.
        ScannedKeyValue::Verbatim(range) => {
            std::str::from_utf8(&bytes[range.clone()])
                .map_err(|error| key_codec_error(&format!("{field} is not UTF-8: {error}")))?;
            let start = u32::try_from(range.start)
                .map_err(|_| key_codec_error(&format!("{field} offset exceeds u32")))?;
            let end = u32::try_from(range.end)
                .map_err(|_| key_codec_error(&format!("{field} offset exceeds u32")))?;
            Ok((HotScanString::Borrowed(start..end), part.terminator))
        }
        // Embedded NULs require unescaping; that uncommon case owns its buffer.
        ScannedKeyValue::Unescaped(value) => {
            let value = String::from_utf8(value).map_err(|error| {
                key_codec_error(&format!("{field} is not UTF-8: {}", error.utf8_error()))
            })?;
            Ok((HotScanString::Owned(value), part.terminator))
        }
    }
}

fn read_hot_scan_shared_bytes(
    bytes: &Bytes,
    offset: &mut usize,
    field: &str,
) -> Result<(Bytes, u8), LixError> {
    let part = scan_key_part(bytes.as_ref(), *offset)
        .map_err(|error| head_key_part_error(error, field))?;
    *offset = part.end;
    let value = match part.value {
        // No escape: hand back a refcounted slice of the same allocation.
        ScannedKeyValue::Verbatim(range) => {
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_hot_scan_key_handle_clone();
            bytes.slice(range)
        }
        // Embedded NULs had to be unescaped, so this case owns its buffer.
        ScannedKeyValue::Unescaped(value) => Bytes::from(value),
    };
    Ok((value, part.terminator))
}

fn decode_hot_row_key_in_scope(bytes: &[u8], scope: &[u8]) -> Result<HeadRowIdentity, LixError> {
    if !bytes.starts_with(scope) {
        return Err(key_codec_error(
            "hot row does not begin with its scanned scope",
        ));
    }
    let mut offset = scope.len();
    let (schema_key, schema_terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot row schema key has an invalid terminator",
        ));
    }
    let file_id = read_file_id(bytes, &mut offset)?;
    let row_pk = read_row_pk(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("hot row key has trailing bytes"));
    }
    Ok(HeadRowIdentity {
        schema_key,
        row_pk,
        file_id,
    })
}

/// Distinguishes the two record kinds sharing [`INDEX_SPACE`]: entries and
/// the per-collection completeness witness. Entries sort after the witness for
/// a given schema, so a witness probe is a point read and never scans entries.
const HOT_INDEX_WITNESS_TAG: u8 = 0x00;
const HOT_INDEX_ENTRY_TAG: u8 = 0x01;
const HOT_INDEX_CANDIDATE_PAGE: usize = 256;
/// Distinct values one indexed-column probe may resolve.
///
/// Each value costs its own range scan, so a very wide `IN` list — or a very
/// wide join build side — is cheaper to answer with the ordinary collection
/// scan. Declining above the limit keeps the probe route weakly better than
/// the scan route it replaces.
const HOT_INDEX_PROBE_VALUE_LIMIT: usize = 64;

/// One index entry to publish: the row's indexed value and its identity.
#[derive(Debug, Clone)]
pub(crate) struct HotIndexEntry {
    pub(crate) schema_key: String,
    pub(crate) ordinal: u16,
    pub(crate) value: HotIndexValue,
    pub(crate) row_pk: RowPk,
}

/// Stages index entries and, optionally, the collection witnesses that make
/// them selectable.
///
/// Put-only by construction: there is no delete path, which is what keeps this
/// O(changed rows) with no reads. Duplicate `(space, key)` mutations are
/// rejected by the write set, so identical entries staged twice in one commit
/// are collapsed here rather than at lowering time.
pub(crate) async fn stage_hot_index_entries(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    branch_id: &str,
    generation: CommitId,
    entries: &[HotIndexEntry],
    witnessed_collections: &BTreeSet<(String, u16)>,
) -> Result<(), LixError> {
    let mut staged = BTreeSet::new();
    let mut published_by_collection: BTreeMap<(String, u16), u64> = BTreeMap::new();
    for entry in entries {
        let key = encode_hot_index_entry_key(
            branch_id,
            generation,
            &entry.schema_key,
            entry.ordinal,
            &entry.value,
            &entry.row_pk,
        );
        if !staged.insert(key.clone()) {
            continue;
        }
        let identity = entry.row_pk.as_json_array_text().map_err(|error| {
            head_value_error(format!("hot index row pk is not encodable: {error}"))
        })?;
        writes.put(
            INDEX_SPACE,
            StorageKey(Bytes::from(key)),
            StorageValue {
                bytes: Bytes::from(identity.into_bytes()),
            },
        );
        *published_by_collection
            .entry((entry.schema_key.clone(), entry.ordinal))
            .or_default() += 1;
    }
    for collection in witnessed_collections {
        published_by_collection
            .entry(collection.clone())
            .or_default();
    }
    if published_by_collection.is_empty() {
        return Ok(());
    }
    // One multi-get and one put per *collection* touched by this commit, not
    // per row, so maintaining the count is O(collections) and independent of
    // how many rows the commit carries.
    let witness_keys = published_by_collection
        .keys()
        .map(|(schema_key, ordinal)| {
            StorageKey(Bytes::from(encode_hot_index_witness_key(
                branch_id, generation, schema_key, *ordinal,
            )))
        })
        .collect::<Vec<_>>();
    let previous = PointReadPlan::new(INDEX_SPACE, &witness_keys)
        .materialize(read, StorageGetOptions::default())
        .await?
        .value;
    for ((collection, published), (key, previous)) in published_by_collection
        .into_iter()
        .zip(witness_keys.into_iter().zip(previous.into_iter()))
    {
        let existing = match previous {
            Some(StorageProjectedValue::FullValue(bytes)) => Some(decode_hot_index_witness(&bytes)),
            Some(StorageProjectedValue::KeyOnly) | None => None,
        };
        // Never *create* a witness for a collection that has not asserted
        // completeness. A witness is the claim that this plane holds every row
        // of the collection, and inventing one here would turn a partial index
        // into an authoritative-looking one.
        if existing.is_none() && !witnessed_collections.contains(&collection) {
            continue;
        }
        // A witness whose previous value could not be decoded restarts the
        // count from this commit. That undercounts history, which shrinks the
        // budget and makes the guard fire *earlier* — the safe direction.
        let total = existing.flatten().unwrap_or(0).saturating_add(published);
        if !staged.insert(key.0.to_vec()) {
            continue;
        }
        writes.put(
            INDEX_SPACE,
            key,
            StorageValue {
                bytes: Bytes::from(encode_hot_index_witness(total).to_vec()),
            },
        );
    }
    Ok(())
}

/// Entries published into one `(collection, column)` plane since the
/// generation began, as carried by the witness record.
///
/// This is a monotone upper bound on the plane's size, not an exact count: it
/// counts staged puts, and a commit that rewrites an unchanged value restages
/// an identical key. Over-counting only widens the budget, so the bound is
/// safe in the direction that matters.
fn encode_hot_index_witness(entries_published: u64) -> [u8; 8] {
    entries_published.to_be_bytes()
}

fn decode_hot_index_witness(value: &[u8]) -> Option<u64> {
    value.try_into().ok().map(u64::from_be_bytes)
}

/// How many candidates one value lookup may collect before the collection scan
/// becomes the cheaper route.
///
/// **This is a graceful-degradation bound, not a repair.** Entries still
/// accumulate; past the budget the plane simply stops being consulted, so the
/// index can never cost more than not having it. Nothing is pruned and no
/// bytes are reclaimed.
///
/// The constant is measured, not chosen. On `ryzen-9950x-I` at base
/// `a0ccd0dd6`, resolving one candidate costs ≈0.73 µs (point read plus
/// snapshot parse) while the collection scan costs ≈0.30–0.49 µs per row, so
/// the index route loses once a bucket exceeds roughly two thirds of the rows
/// the scan would walk. `entries_published` is an upper bound on the plane's
/// size and grows with the same row churn that grows the scan's row count
/// (deleted rows leave tombstones the scan still walks), which makes it the
/// available proxy for that row count. Halving it keeps the budget below the
/// measured crossover.
///
/// Checked against every point of the measurement that motivated it: the aged
/// 100/1000/10000 buckets, the moved 100/1000/10000 buckets and the 100/1000/5000
/// write-path buckets all exceed their budget, and all were slower than the
/// scan; the fresh one-candidate arm and the 10-candidate write-path arm stay
/// under it, and both were faster.
///
/// At the boundary the route flips: a bucket of exactly `budget` is served by
/// the index; one entry more abandons the route after reading that one extra
/// entry and pays the scan, so the worst case is the scan plus a bounded
/// key-only prefix read.
///
/// The floor exists because below it the absolute cost is smaller than the
/// measurement's own noise floor — a 64-candidate bucket resolves in ≈47 µs —
/// so flipping tiny collections to a scan would trade a real access path for
/// no measurable gain.
fn hot_index_candidate_budget(entries_published: u64) -> usize {
    const MIN_CANDIDATE_BUDGET: u64 = 64;
    usize::try_from((entries_published / 2).max(MIN_CANDIDATE_BUDGET)).unwrap_or(usize::MAX)
}

/// One indexed value, encoded so that equality is a key prefix.
///
/// Integers use the same order-preserving flip as row-pk components so a
/// future range predicate can reuse this encoding unchanged.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum HotIndexValue {
    String(String),
    Integer(i64),
}

impl HotIndexValue {
    fn write(&self, out: &mut Vec<u8>) {
        match self {
            Self::String(value) => {
                out.push(ROW_PK_STRING);
                write_key_string(out, value, KEY_PART_FINAL);
            }
            Self::Integer(value) => {
                out.push(ROW_PK_INTEGER);
                out.extend_from_slice(&ordered_integer_from_i64(*value).to_be_bytes());
                out.push(KEY_PART_FINAL);
            }
        }
    }
}

/// `scope | schema | ENTRY | ordinal | value | row_pk`.
pub(crate) fn encode_hot_index_entry_key(
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
    ordinal: u16,
    value: &HotIndexValue,
    row_pk: &RowPk,
) -> Vec<u8> {
    let mut key = hot_index_value_prefix(branch_id, generation, schema_key, ordinal, value);
    write_row_pk(&mut key, row_pk);
    key
}

/// Every entry for one `(collection, column, value)`: the equality access path.
pub(crate) fn hot_index_value_prefix(
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
    ordinal: u16,
    value: &HotIndexValue,
) -> Vec<u8> {
    let mut key = hot_scope_prefix(branch_id, generation);
    write_key_string(&mut key, schema_key, KEY_PART_FINAL);
    key.push(HOT_INDEX_ENTRY_TAG);
    key.extend_from_slice(&ordinal.to_be_bytes());
    value.write(&mut key);
    key
}

/// Every entry for one `(collection, column)`, whatever the value: the range
/// access path's outer bound.
pub(crate) fn hot_index_column_prefix(
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
    ordinal: u16,
) -> Vec<u8> {
    let mut key = hot_scope_prefix(branch_id, generation);
    write_key_string(&mut key, schema_key, KEY_PART_FINAL);
    key.push(HOT_INDEX_ENTRY_TAG);
    key.extend_from_slice(&ordinal.to_be_bytes());
    key
}

/// The least key strictly greater than every key having `prefix` as a prefix.
///
/// This is the whole of inclusive-upper-bound correctness. Entries for value
/// `v` are `prefix_v ++ row_pk`, so they all sort **after** `prefix_v`
/// itself. An upper bound of `Excluded(prefix_v)` therefore excludes every row
/// equal to `v` — right for `< v`, and silently wrong for `<= v`, which is the
/// one place a range seek loses rows while still returning a plausible answer.
/// `<= v` must bound with `Excluded(successor(prefix_v))`.
///
/// `None` means the prefix is all `0xff` and no successor exists, in which case
/// the range runs to the end of the space.
fn hot_index_key_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    while let Some(last) = successor.last_mut() {
        if *last == u8::MAX {
            successor.pop();
        } else {
            *last += 1;
            return Some(successor);
        }
    }
    None
}

/// Asserts that this plane holds every row of one `(collection, column)` for
/// one generation.
///
/// **This is not a compatibility shim and must not be deleted as one.** It
/// gates *access-path selection* over a scan path that is permanently present:
/// columns a schema does not declare are never indexed, so the collection scan
/// can never be retired. Without the witness a generation whose rows predate
/// this plane would look like an empty index and silently return no rows,
/// which is the one failure mode this design must not have. A repository
/// upgrades by publishing its next generation, not by rebuilding at open.
pub(crate) fn encode_hot_index_witness_key(
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
    ordinal: u16,
) -> Vec<u8> {
    let mut key = hot_scope_prefix(branch_id, generation);
    write_key_string(&mut key, schema_key, KEY_PART_FINAL);
    key.push(HOT_INDEX_WITNESS_TAG);
    key.extend_from_slice(&ordinal.to_be_bytes());
    key
}

fn encode_hot_file_schema_key(scope: &[u8], schema_key: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(
        scope
            .len()
            .saturating_add(encoded_key_bytes_len(schema_key.as_bytes()).unwrap_or(0)),
    );
    key.extend_from_slice(scope);
    write_key_string(&mut key, schema_key, KEY_PART_FINAL);
    key
}

struct HotDiffSegmentScope {
    branch_id: String,
    checkpoint_commit_id: CommitId,
    generation: CommitId,
    digest: [u8; 32],
}

fn decode_hot_diff_key_in_scope(bytes: &[u8], scope: &[u8]) -> Result<HeadRowIdentity, LixError> {
    if !bytes.starts_with(scope) {
        return Err(key_codec_error(
            "hot diff row does not begin with its scanned scope",
        ));
    }
    let mut offset = scope.len();
    let (schema_key, schema_terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot diff row schema key has an invalid terminator",
        ));
    }
    let row_pk = read_row_pk(bytes, &mut offset)?;
    let file_id = read_file_id(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("hot diff row key has trailing bytes"));
    }
    Ok(HeadRowIdentity {
        schema_key,
        row_pk,
        file_id,
    })
}

fn decode_hot_diff_segment_key(bytes: &[u8]) -> Result<HotDiffSegmentScope, LixError> {
    let mut offset = 0;
    let (branch_id, branch_terminator) = read_key_string(bytes, &mut offset, "branch id")?;
    if branch_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot diff branch id has an invalid terminator",
        ));
    }
    let checkpoint_commit_id = read_generation(bytes, &mut offset)?;
    let generation = read_generation(bytes, &mut offset)?;
    let digest_bytes = bytes
        .get(offset..)
        .ok_or_else(|| key_codec_error("hot diff segment key is truncated before its digest"))?;
    let digest = <[u8; 32]>::try_from(digest_bytes)
        .map_err(|_| key_codec_error("hot diff segment key has an invalid digest length"))?;
    Ok(HotDiffSegmentScope {
        branch_id,
        checkpoint_commit_id,
        generation,
        digest,
    })
}

fn visit_hot_diff_segment(
    bytes: &[u8],
    scope: &[u8],
    coverage: &mut WorkingDiffIndexCoverage,
    mut visit: impl FnMut(HeadRowIdentity),
) -> Result<(), LixError> {
    if bytes.len() < HOT_DIFF_SEGMENT_HEADER_BYTES {
        return Err(key_codec_error("hot diff segment is truncated"));
    }
    if bytes[0] != HOT_DIFF_SEGMENT_VERSION {
        return Err(key_codec_error("hot diff segment has an unknown version"));
    }
    let count = u32::from_le_bytes(
        bytes[1..HOT_DIFF_SEGMENT_HEADER_BYTES]
            .try_into()
            .expect("hot diff segment header has a fixed count width"),
    );
    if count == 0 || count > HOT_DIFF_SEGMENT_MAX_IDENTITIES {
        return Err(key_codec_error(
            "hot diff segment has an invalid identity count",
        ));
    }
    let mut offset = HOT_DIFF_SEGMENT_HEADER_BYTES;
    let mut full_key = Vec::with_capacity(scope.len() + 128);
    full_key.extend_from_slice(scope);
    for _ in 0..count {
        let length_end = offset
            .checked_add(4)
            .ok_or_else(|| key_codec_error("hot diff segment length offset overflow"))?;
        let encoded_length = bytes.get(offset..length_end).ok_or_else(|| {
            key_codec_error("hot diff segment is truncated before identity length")
        })?;
        let suffix_len = u32::from_le_bytes(
            encoded_length
                .try_into()
                .expect("hot diff identity length has a fixed width"),
        ) as usize;
        if suffix_len == 0 {
            return Err(key_codec_error("hot diff segment has an empty identity"));
        }
        let suffix_end = length_end
            .checked_add(suffix_len)
            .ok_or_else(|| key_codec_error("hot diff segment identity offset overflow"))?;
        let suffix = bytes
            .get(length_end..suffix_end)
            .ok_or_else(|| key_codec_error("hot diff segment is truncated in an identity"))?;
        full_key.truncate(scope.len());
        full_key.extend_from_slice(suffix);
        coverage
            .add_encoded_group_key(&full_key)
            .ok_or_else(|| head_value_error("hot working-diff index count exceeds u64"))?;
        visit(decode_hot_diff_key_in_scope(&full_key, scope)?);
        offset = suffix_end;
    }
    if offset != bytes.len() {
        return Err(key_codec_error("hot diff segment has trailing bytes"));
    }
    Ok(())
}

fn decode_hot_diff_key(bytes: &[u8]) -> Result<(CommitId, HeadIdentity), LixError> {
    let mut offset = 0;
    let (branch_id, branch_terminator) = read_key_string(bytes, &mut offset, "branch id")?;
    if branch_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot diff branch id has an invalid terminator",
        ));
    }
    let checkpoint_commit_id = read_generation(bytes, &mut offset)?;
    let generation = read_generation(bytes, &mut offset)?;
    let (schema_key, schema_terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot diff schema key has an invalid terminator",
        ));
    }
    let row_pk = read_row_pk(bytes, &mut offset)?;
    let file_id = read_file_id(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("hot diff key has trailing bytes"));
    }
    Ok((
        checkpoint_commit_id,
        HeadIdentity {
            branch_id,
            generation,
            schema_key,
            row_pk,
            file_id,
        },
    ))
}

/// Every serving plane whose key begins with `(branch_id, generation)`.
///
/// These are derived caches of one branch generation, so a generation that no
/// live branch control selects is exactly one contiguous key range per space.
const GENERATION_SCOPED_SPACES: &[StorageSpace] = &[
    INDEX_SPACE,
    ROW_SPACE,
    FILE_SPACE,
    COLLECTION_CONTROL_SPACE,
    PACKED_CURRENT_BASE_SPACE,
    PACKED_CURRENT_BASE_CONTROL_SPACE,
    PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE,
    ROOT_CURRENT_BASE_SPACE,
];

/// The `(branch_id, generation)` key prefix that scopes every derived serving
/// plane. Exposed for GC census assertions.
#[cfg(test)]
pub(crate) fn hot_generation_scope_prefix(branch_id: &str, generation: CommitId) -> Vec<u8> {
    encode_scope_prefix(branch_id, generation)
}

/// Retires every derived serving row of one branch generation.
///
/// The caller proves the generation is unreachable from the live branch
/// controls; this stages the deletes. Work is bounded by the rows actually
/// reclaimed — each space is entered at the generation's key prefix, never
/// scanned whole — so a repository sweep costs what its garbage costs.
pub(crate) async fn stage_retire_hot_generation<S>(
    store: &S,
    writes: &mut StorageWriteSet,
    branch_id: &str,
    generation: CommitId,
) -> Result<u64, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let prefix = StoragePrefix {
        bytes: Bytes::from(encode_scope_prefix(branch_id, generation)),
    };
    // Counted at the top of the real function, so a commit lane that never
    // rotates its generation reads as zero calls rather than as "the symbol
    // exists, therefore the branch ran".
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_hot_retire_call();
    let mut deleted = 0_u64;
    for space in GENERATION_SCOPED_SPACES {
        // A publication that supersedes this generation stages its own
        // lifecycle mutations first. Restating one of those keys here is a
        // duplicate mutation, not an idempotent delete.
        #[cfg(feature = "storage-benches")]
        let space_start = std::time::Instant::now();
        let declared = writes.declared_keys(*space);
        let mut cursor = store
            .begin_scan(
                *space,
                prefix.to_range()?,
                StorageBeginScanOptions {
                    projection: StorageCoreProjection::KeyOnly,
                    ..StorageBeginScanOptions::default()
                },
            )
            .await?;
        #[cfg(feature = "storage-benches")]
        let open_nanos = u64::try_from(space_start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        #[cfg(feature = "storage-benches")]
        let mut space_rows = 0_u64;
        #[cfg(feature = "storage-benches")]
        let mut space_pages = 0_u64;
        loop {
            let (page, page_has_more) = cursor
                .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
                .await?
                .into_parts();
            #[cfg(feature = "storage-benches")]
            {
                space_pages = space_pages.saturating_add(1);
            }
            for entry in page {
                // Counted here, inside the per-entry decode loop that does the
                // work, not at the layer that returns the answer: a post-filter
                // count cannot distinguish a seek from a full-prefix walk.
                #[cfg(feature = "storage-benches")]
                {
                    space_rows = space_rows.saturating_add(1);
                }
                if declared.contains(entry.key.0.as_ref()) {
                    continue;
                }
                writes.delete(*space, entry.key);
                deleted = deleted.saturating_add(1);
            }
            if !page_has_more {
                break;
            }
        }
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_hot_retire_space(
            space.id.0,
            space_rows,
            space_pages,
            open_nanos,
            u64::try_from(space_start.elapsed().as_nanos()).unwrap_or(u64::MAX),
        );
    }
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_hot_retire_deleted(deleted);
    Ok(deleted)
}

pub(crate) async fn stage_collect_stale_hot_diff_records<S>(
    store: &S,
    writes: &mut StorageWriteSet,
    active: &BTreeMap<String, ActiveWorkingDiffScope>,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let range = StoragePrefix {
        bytes: Bytes::new(),
    }
    .to_range()?;
    let mut cursor = store
        .begin_scan(DIFF_SPACE, range, StorageBeginScanOptions::default())
        .await?;
    loop {
        let (page, page_has_more) = cursor
            .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
            .await?
            .into_parts();
        for entry in page {
            let keep = match full_value_bytes(entry.value) {
                Ok(bytes) if bytes.is_empty() => decode_hot_diff_key(entry.key.0.as_ref())
                    .is_ok_and(|(checkpoint_commit_id, identity)| {
                        active.get(&identity.branch_id).is_some_and(|scope| {
                            scope.checkpoint_commit_id == checkpoint_commit_id
                                && scope.generation == identity.generation
                        })
                    }),
                Ok(bytes) => {
                    decode_hot_diff_segment_key(entry.key.0.as_ref()).is_ok_and(|segment_scope| {
                        if !active.get(&segment_scope.branch_id).is_some_and(|scope| {
                            scope.checkpoint_commit_id == segment_scope.checkpoint_commit_id
                                && scope.generation == segment_scope.generation
                        }) {
                            return false;
                        }
                        let scope = encode_working_diff_scope_prefix(
                            &segment_scope.branch_id,
                            segment_scope.checkpoint_commit_id,
                            segment_scope.generation,
                        );
                        let mut coverage = WorkingDiffIndexCoverage::default();
                        segment_scope.digest == *blake3::hash(&bytes).as_bytes()
                            && visit_hot_diff_segment(&bytes, &scope, &mut coverage, |_| {}).is_ok()
                    })
                }
                _ => false,
            };
            if !keep {
                writes.delete(DIFF_SPACE, entry.key);
            }
        }
        if !page_has_more {
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;

    use super::*;
    use crate::branch::{BranchHeadControl, stage_branch_head_control};
    use crate::storage_adapter::{
        Memory, StorageAdapter, StorageBeginScanOptions, StorageGetManyRequest,
        StorageGetManyResult, StorageKeyRange, StorageReadOptions, StorageScanCursor,
        StorageWriteOptions,
    };

    /// `HotCollectionControl` is `#[musli(packed)]`: its fields are positional
    /// and the encoding carries no field tags or length prefix. Appending a
    /// fourth field therefore appends bytes that a three-field reader cannot
    /// account for, and `storage_codec::decode` rejects trailing bytes rather
    /// than ignoring them.
    ///
    /// This pins the cost of carrying a per-collection flag (for example a
    /// "this collection has been compacted" bit) on the control record: it is
    /// an on-disk format break that requires a repository protocol bump, the
    /// same way the `ordered_identity_digest` field did. It is not an additive
    /// change.
    #[test]
    fn adding_a_field_to_the_packed_collection_control_breaks_older_readers() {
        #[derive(musli::Encode)]
        #[musli(packed)]
        struct WidenedHotCollectionControl {
            active_generation: CommitId,
            live_count: u64,
            ordered_identity_digest: Option<[u8; 32]>,
            compacted: bool,
        }

        let generation = CommitId::for_test_label("compaction-arity");

        // Both digest states matter. A `None` option and a `Some` option lay
        // out differently in packed mode, so each has to be checked
        // separately: the danger to rule out is not only a loud failure but a
        // decode that silently succeeds with wrong field values.
        for digest in [None, Some([0xA5u8; 32])] {
            let current = HotCollectionControl {
                active_generation: generation,
                live_count: 7,
                ordered_identity_digest: digest,
            };
            let current_bytes = storage_codec::encode("hot collection control", &current)
                .expect("current control should encode");

            // Sanity: the three-field control round-trips today, so any
            // failure below is caused by the added field and nothing else.
            let round_tripped: HotCollectionControl =
                storage_codec::decode("hot collection control", &current_bytes)
                    .expect("current control should round-trip");
            assert_eq!(round_tripped, current);

            let widened = WidenedHotCollectionControl {
                active_generation: generation,
                live_count: 7,
                ordered_identity_digest: digest,
                compacted: true,
            };
            let widened_bytes = storage_codec::encode("widened hot collection control", &widened)
                .expect("widened control should encode");
            assert!(
                widened_bytes.len() > current_bytes.len(),
                "the fourth positional field must actually widen the encoding"
            );

            let error = storage_codec::decode::<HotCollectionControl>(
                "hot collection control",
                &widened_bytes,
            )
            .expect_err(
                "a three-field reader must reject a four-field control rather than \
                 silently decoding it",
            );
            let message = error.to_string();
            assert!(
                message.contains("failed to decode hot collection control"),
                "expected a decode rejection, got: {message}"
            );
        }
    }

    /// The root-base cache has no invalidation rule — it relies entirely on the
    /// key being exact. So the key must actually discriminate: a different base
    /// commit, a different filter, or a different projection must all miss.
    /// A false hit here would serve one collection's rows for another's query.
    #[test]
    fn root_base_batch_cache_key_is_exact() {
        let cache = RootBaseBatchCache::default();
        let first = CommitId::for_test_label("root-base-cache-first");
        let second = CommitId::for_test_label("root-base-cache-second");
        let request =
            |schema_key: &str, columns: &[&str], limit: Option<usize>| TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    schema_keys: vec![schema_key.to_owned()],
                    ..TrackedStateFilter::default()
                },
                read_columns: TrackedStateReadColumns {
                    columns: columns.iter().map(|column| (*column).to_owned()).collect(),
                },
                limit,
            };
        let batch = Arc::new(crate::tracked_state::MaterializedTrackedStateBatch::default());

        let stored = request("s", &["change_id"], None);
        cache.insert(first, stored.clone(), Arc::clone(&batch));

        assert!(cache.get(first, &stored).is_some(), "exact key must hit");
        assert!(
            cache.get(second, &stored).is_none(),
            "a different base commit must miss"
        );
        assert!(
            cache
                .get(first, &request("other", &["change_id"], None))
                .is_none(),
            "a different schema filter must miss"
        );
        assert!(
            cache
                .get(first, &request("s", &["snapshot_content"], None))
                .is_none(),
            "a different projection must miss"
        );
        assert!(
            cache
                .get(first, &request("s", &["change_id"], Some(1)))
                .is_none(),
            "a different limit must miss"
        );
    }

    /// The cache is bounded by entry count, so a rotated generation that is
    /// scanned under many request shapes cannot grow it without limit.
    #[test]
    fn root_base_batch_cache_evicts_least_recently_used() {
        let cache = RootBaseBatchCache::default();
        let commit_id = CommitId::for_test_label("root-base-cache-evict");
        let request = |index: usize| TrackedStateScanRequest {
            filter: TrackedStateFilter {
                schema_keys: vec![format!("s{index}")],
                ..TrackedStateFilter::default()
            },
            ..TrackedStateScanRequest::default()
        };
        let batch = Arc::new(crate::tracked_state::MaterializedTrackedStateBatch::default());
        for index in 0..=ROOT_BASE_BATCH_CACHE_MAX_ENTRIES {
            cache.insert(commit_id, request(index), Arc::clone(&batch));
        }
        assert_eq!(
            cache.entries().resident.len(),
            ROOT_BASE_BATCH_CACHE_MAX_ENTRIES
        );
        assert!(
            cache.get(commit_id, &request(0)).is_none(),
            "the oldest entry must have been evicted"
        );
        assert!(
            cache
                .get(commit_id, &request(ROOT_BASE_BATCH_CACHE_MAX_ENTRIES))
                .is_some(),
            "the newest entry must be resident"
        );
    }

    /// The memo replaced a per-row recomputation with a per-scope one, and it
    /// also replaced `all(|scope| created_at >= scope.floor)` with a single
    /// comparison against the largest floor. Both rewrites are only sound if
    /// the verdict is a pure function of the scope, so check it directly
    /// against the rule it encodes, including the cases that used to be
    /// separate scopes: a filed row is governed by its unfiled scope *and* its
    /// filed scope, and either can disqualify it or raise its floor.
    #[test]
    fn root_scope_memo_reproduces_the_per_scope_rule() {
        let branch_generation = CommitId::for_test_label("memo-branch-generation");
        let root_generation = CommitId::for_test_label("memo-root-generation");
        let unfiled = ("s".to_owned(), None);
        let filed = ("s".to_owned(), Some("f".to_owned()));

        // Reference implementation: the pre-memo predicate, verbatim.
        let reference =
            |created_at: LixTimestamp,
             file_id: Option<&str>,
             active: &BTreeMap<(String, Option<String>), RootCollectionGeneration>,
             stored: &BTreeMap<(String, Option<String>), HotCollectionControl>| {
                [
                    Some(("s".to_owned(), None)),
                    file_id.map(|file_id| ("s".to_owned(), Some(file_id.to_owned()))),
                ]
                .into_iter()
                .flatten()
                .all(|scope| {
                    let root = active
                        .get(&scope)
                        .map_or(branch_generation, |generation| generation.commit_id);
                    if stored
                        .get(&scope)
                        .is_some_and(|control| control.active_generation != root)
                    {
                        return false;
                    }
                    active
                        .get(&scope)
                        .is_none_or(|generation| created_at >= generation.created_at)
                })
            };

        let stamp = |text: &str| LixTimestamp::expect_parse("memo test timestamp", text);
        let low = stamp("2026-01-01T00:00:00Z");
        let high = stamp("2026-03-01T00:00:00Z");
        let control = |generation: CommitId| HotCollectionControl {
            active_generation: generation,
            live_count: 0,
            ordered_identity_digest: None,
        };

        let generation = |created_at: LixTimestamp| RootCollectionGeneration {
            commit_id: root_generation,
            created_at,
        };
        // Floor on the unfiled scope only.
        let unfiled_floor = BTreeMap::from([(unfiled.clone(), generation(high))]);
        // Two floors, the larger on the filed scope — the `max` case.
        let both_floors = BTreeMap::from([
            (unfiled.clone(), generation(low)),
            (filed.clone(), generation(high)),
        ]);
        let cases: Vec<(
            BTreeMap<(String, Option<String>), RootCollectionGeneration>,
            BTreeMap<(String, Option<String>), HotCollectionControl>,
        )> = vec![
            (BTreeMap::new(), BTreeMap::new()),
            (unfiled_floor, BTreeMap::new()),
            (both_floors, BTreeMap::new()),
            // Disqualifying control on the filed scope only.
            (
                BTreeMap::new(),
                BTreeMap::from([(filed.clone(), control(root_generation))]),
            ),
            // Agreeing control: must not disqualify.
            (
                BTreeMap::new(),
                BTreeMap::from([(unfiled.clone(), control(branch_generation))]),
            ),
        ];

        for (index, (active, stored)) in cases.iter().enumerate() {
            for file_id in [None, Some("f")] {
                let mut memo = RootScopeMemo::default();
                for created_at in [
                    stamp("2025-01-01T00:00:00Z"),
                    stamp("2026-01-01T00:00:00Z"),
                    stamp("2026-03-01T00:00:00Z"),
                    stamp("2027-01-01T00:00:00Z"),
                ] {
                    let verdict = memo.verdict("s", file_id, branch_generation, active, stored);
                    let actual = !verdict.disqualified
                        && verdict.floor.is_none_or(|floor| created_at >= floor);
                    assert_eq!(
                        actual,
                        reference(created_at, file_id, active, stored),
                        "case {index}, file_id {file_id:?}, created_at {created_at:?}"
                    );
                }
            }
        }

        // A primed memo must not answer for a different scope.
        let active = BTreeMap::from([(
            filed.clone(),
            RootCollectionGeneration {
                commit_id: root_generation,
                created_at: high,
            },
        )]);
        let stored = BTreeMap::new();
        let mut memo = RootScopeMemo::default();
        let filed_verdict = memo.verdict("s", Some("f"), branch_generation, &active, &stored);
        let unfiled_verdict = memo.verdict("s", None, branch_generation, &active, &stored);
        assert_eq!(filed_verdict.floor, Some(high));
        assert_eq!(
            unfiled_verdict.floor, None,
            "the unfiled scope has no collection generation and must not inherit the filed floor"
        );
    }

    #[test]
    fn transaction_hot_state_cache_is_bounded_per_metadata_lane() {
        let cache = HotStateTransactionCache::default();
        for index in 0..=TRANSACTION_HOT_STATE_CACHE_MAX_ENTRIES {
            let generation = CommitId::for_test_label(&format!("cache-generation-{index}"));
            cache
                .remember_collection_control(
                    HotCollectionCacheKey {
                        branch_id: "cache-branch".to_owned(),
                        generation,
                        schema_key: format!("schema-{index}"),
                        file_id: None,
                    },
                    HotCollectionControl {
                        active_generation: generation,
                        live_count: index as u64,
                        ordered_identity_digest: None,
                    },
                )
                .expect("remember collection control");
            cache
                .remember_packed_current_base_refs(
                    &format!("packed-branch-{index}"),
                    generation,
                    &[PackedCurrentBaseRef {
                        commit_id: generation,
                        checkpoint_commit_id: None,
                        file_id: None,
                        coverage_key: Bytes::new(),
                    }],
                )
                .expect("remember packed current-base refs");
            assert!(
                !cache
                    .should_reuse_packed_points(generation)
                    .expect("observe first packed point scope")
            );
            for observation in 2..=TRANSACTION_PACKED_POINT_CACHE_MIN_OBSERVATIONS {
                assert_eq!(
                    cache
                        .should_reuse_packed_points(generation)
                        .expect("observe packed point scope"),
                    index < TRANSACTION_HOT_STATE_CACHE_MAX_ENTRIES
                        && observation == TRANSACTION_PACKED_POINT_CACHE_MIN_OBSERVATIONS,
                );
            }
        }
        assert_eq!(
            cache.collection_controls.lock().unwrap().len(),
            TRANSACTION_HOT_STATE_CACHE_MAX_ENTRIES
        );
        assert_eq!(
            cache.packed_current_base_refs.lock().unwrap().len(),
            TRANSACTION_HOT_STATE_CACHE_MAX_ENTRIES
        );
        assert_eq!(
            cache
                .packed_point_generation_observations
                .lock()
                .unwrap()
                .len(),
            TRANSACTION_HOT_STATE_CACHE_MAX_ENTRIES
        );
    }

    #[test]
    fn packed_current_base_file_scope_roundtrips_and_filters_exactly() {
        let checkpoint = CommitId::for_test_label("packed-file-checkpoint");
        let encoded = packed_current_base_value(Some(checkpoint), Some("file-a"))
            .expect("encode packed file scope");
        let (decoded_checkpoint, decoded_file) =
            decode_packed_current_base_value(&encoded).expect("decode packed file scope");
        assert_eq!(decoded_checkpoint, Some(checkpoint));
        assert_eq!(decoded_file.as_deref(), Some("file-a"));

        let base_ref = PackedCurrentBaseRef {
            commit_id: CommitId::for_test_label("packed-file-owner"),
            checkpoint_commit_id: decoded_checkpoint,
            file_id: decoded_file,
            coverage_key: Bytes::new(),
        };
        assert!(packed_base_matches_file_filter(
            &base_ref,
            &[NullableKeyFilter::Value("file-a".to_owned())]
        ));
        assert!(!packed_base_matches_file_filter(
            &base_ref,
            &[NullableKeyFilter::Value("file-b".to_owned())]
        ));
        assert!(!packed_base_matches_file_filter(
            &base_ref,
            &[NullableKeyFilter::Null]
        ));
        assert!(packed_base_matches_file_filter(
            &base_ref,
            &[NullableKeyFilter::Any]
        ));

        let unscoped = packed_current_base_value(None, None).expect("encode unscoped base");
        assert_eq!(unscoped.len(), 16);
        assert_eq!(
            decode_packed_current_base_value(&unscoped).expect("decode unscoped base"),
            (None, None)
        );
    }

    #[tokio::test]
    async fn transaction_reader_reuses_collection_control_point_read() {
        const BRANCH_ID: &str = "collection-control-cache-branch";
        const SCHEMA_KEY: &str = "collection_control_cache_schema";
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label("collection-control-cache-generation");
        let mut writes = StorageWriteSet::new();
        stage_hot_collection_control(
            &mut writes,
            BRANCH_ID,
            generation,
            crate::collection_generation::CollectionScopeRef {
                schema_key: SCHEMA_KEY,
                file_id: None,
            },
            HotCollectionControl {
                active_generation: generation,
                live_count: 7,
                ordered_identity_digest: Some([3; 32]),
            },
        )
        .expect("stage collection control fixture");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("publish collection control fixture");

        let get_many_calls = Arc::new(AtomicUsize::new(0));
        let reader = HotStateStoreReader {
            store: CountingRead {
                inner: storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("open collection control fixture read"),
                get_many_calls: Arc::clone(&get_many_calls),
                scan_calls: None,
            },
            transaction_cache: Some(Arc::new(HotStateTransactionCache::default())),
            root_base_cache: None,
        };
        for _ in 0..2 {
            let control = reader
                .collection_generation(
                    BRANCH_ID,
                    generation,
                    crate::collection_generation::CollectionScopeRef {
                        schema_key: SCHEMA_KEY,
                        file_id: None,
                    },
                )
                .await
                .expect("load cached collection control");
            assert_eq!(control.live_count, 7);
            assert_eq!(control.ordered_identity_digest, Some([3; 32]));
        }
        assert_eq!(
            get_many_calls.load(Ordering::Relaxed),
            1,
            "the immutable transaction snapshot should point-read a control once"
        );
    }

    struct CountingRead<R> {
        inner: R,
        get_many_calls: Arc<AtomicUsize>,
        scan_calls: Option<Arc<AtomicUsize>>,
    }

    impl<R: StorageAdapterRead> StorageAdapterRead for CountingRead<R> {
        fn snapshot_cache_key(&self) -> Option<u128> {
            self.inner.snapshot_cache_key()
        }

        async fn get_many(
            &self,
            requests: &[StorageGetManyRequest<'_>],
        ) -> Result<StorageGetManyResult, crate::storage_adapter::StorageError> {
            self.get_many_calls.fetch_add(1, Ordering::Relaxed);
            self.inner.get_many(requests).await
        }

        async fn begin_scan(
            &self,
            space: StorageSpace,
            range: StorageKeyRange,
            opts: StorageBeginScanOptions,
        ) -> Result<StorageScanCursor<'_>, crate::storage_adapter::StorageError> {
            if let Some(scan_calls) = &self.scan_calls {
                scan_calls.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.begin_scan(space, range, opts).await
        }
    }

    struct JsonCountingRead<R> {
        inner: R,
        json_get_many_calls: Arc<AtomicUsize>,
    }

    impl<R: StorageAdapterRead> StorageAdapterRead for JsonCountingRead<R> {
        fn snapshot_cache_key(&self) -> Option<u128> {
            self.inner.snapshot_cache_key()
        }

        async fn get_many(
            &self,
            requests: &[StorageGetManyRequest<'_>],
        ) -> Result<StorageGetManyResult, crate::storage_adapter::StorageError> {
            if requests
                .iter()
                .any(|request| request.space == crate::json_store::store::JSON_SPACE)
            {
                self.json_get_many_calls.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.get_many(requests).await
        }

        async fn begin_scan(
            &self,
            space: StorageSpace,
            range: StorageKeyRange,
            opts: StorageBeginScanOptions,
        ) -> Result<StorageScanCursor<'_>, crate::storage_adapter::StorageError> {
            self.inner.begin_scan(space, range, opts).await
        }
    }

    fn timestamp() -> LixTimestamp {
        LixTimestamp::expect_parse("hot working-diff test timestamp", "2026-01-01T00:00:00Z")
    }

    fn native_snapshot_payload(row_pk: &RowPk, value: serde_json::Value) -> Vec<u8> {
        let row =
            WasmTypedRow::from_test_json_unchecked(row_pk, &value).expect("test row should build");
        row.durable_payload()
            .expect("test typed snapshot should encode")
            .to_vec()
    }

    fn encoded_test_hot_value(generation: CommitId, untracked: bool, deleted: bool) -> Bytes {
        let row_pk = RowPk::single("closure-row");
        let snapshot = (!deleted)
            .then(|| native_snapshot_payload(&row_pk, serde_json::json!({"value": "closure"})));
        Bytes::from(
            encode_head_value(&HeadValueRef {
                // Both lanes carry a change id; only the tracked lane carries a
                // commit id. That asymmetry is the whole untracked model.
                change_id: Some(ChangeId::for_test_label("closure-change")),
                commit_id: (!untracked).then_some(generation),
                untracked,
                deleted,
                created_at: timestamp(),
                updated_at: timestamp(),
                snapshot: snapshot.as_deref(),
                metadata: None,
                columnar_base_coordinate: None,
                working_diff_baseline: WorkingDiffBaseline::Disabled,
            })
            .expect("closure fixture HOT value should encode"),
        )
    }

    #[test]
    fn exact_collection_member_rejects_noncanonical_domain_tombstone_and_order() {
        const BRANCH_ID: &str = "closure-member-branch";
        const SCHEMA_KEY: &str = "closure_member_schema";
        let generation = CommitId::for_test_label("closure-member-generation");
        let scope = crate::collection_generation::CollectionScopeRef {
            schema_key: SCHEMA_KEY,
            file_id: None,
        };
        let scope_prefix = hot_scope_prefix(BRANCH_ID, generation);
        let identity = HeadRowIdentity {
            schema_key: SCHEMA_KEY.to_owned(),
            row_pk: RowPk::single("member-a"),
            file_id: None,
        };
        let missing_row_pk = RowPk::single("missing-member");
        let missing_identity = TrackedStateKeyRef {
            schema_key: SCHEMA_KEY,
            row_pk: &missing_row_pk,
            file_id: None,
        };
        let required_identity = TrackedStateKeyRef {
            schema_key: SCHEMA_KEY,
            row_pk: &identity.row_pk,
            file_id: None,
        };
        let key =
            encode_hot_row_key_parts(BRANCH_ID, generation, SCHEMA_KEY, &identity.row_pk, None);
        let untracked = encoded_test_hot_value(generation, true, false);
        validate_exact_collection_member(
            BRANCH_ID,
            generation,
            &scope_prefix,
            scope,
            missing_identity,
            true,
            &key,
            &untracked,
        )
        .expect("live untracked member should validate");

        let tracked = encoded_test_hot_value(generation, false, false);
        let wrong_domain = validate_exact_collection_member(
            BRANCH_ID,
            generation,
            &scope_prefix,
            scope,
            required_identity,
            true,
            &key,
            &tracked,
        )
        .expect_err("tracked member must not satisfy an untracked closure");
        assert!(wrong_domain.message.contains("wrong state domain"));

        let tombstone = encoded_test_hot_value(generation, false, true);
        let tombstone_error = validate_exact_collection_member(
            BRANCH_ID,
            generation,
            &scope_prefix,
            scope,
            required_identity,
            false,
            &key,
            &tombstone,
        )
        .expect_err("tombstone must not satisfy a live closure");
        assert!(tombstone_error.message.contains("tombstone"));

        let mut malformed = key.clone();
        malformed.pop();
        assert!(
            validate_exact_collection_member(
                BRANCH_ID,
                generation,
                &scope_prefix,
                scope,
                missing_identity,
                true,
                &malformed,
                &untracked,
            )
            .is_err()
        );
        let mut noncanonical = key.clone();
        noncanonical.push(0);
        let noncanonical_error = validate_canonical_exact_collection_key(&noncanonical, &key)
            .expect_err("raw and canonical encodings must match byte-for-byte");
        assert!(noncanonical_error.message.contains("non-canonical"));

        let mut digest = CompleteHotCollectionDigest::new(BRANCH_ID, generation, scope);
        digest
            .push(&identity, &key)
            .expect("first canonical identity should hash");
        let duplicate = digest
            .push(&identity, &key)
            .expect_err("duplicate canonical identity must fail");
        assert!(duplicate.message.contains("duplicate canonical identity"));

        let high_identity = HeadRowIdentity {
            schema_key: SCHEMA_KEY.to_owned(),
            row_pk: RowPk::single("member-z"),
            file_id: None,
        };
        let high_key = encode_hot_row_key_parts(
            BRANCH_ID,
            generation,
            SCHEMA_KEY,
            &high_identity.row_pk,
            None,
        );
        let mut out_of_order = CompleteHotCollectionDigest::new(BRANCH_ID, generation, scope);
        out_of_order
            .push(&high_identity, &high_key)
            .expect("first high identity should hash");
        let ordering = out_of_order
            .push(&identity, &key)
            .expect_err("descending identity must fail");
        assert!(ordering.message.contains("not in canonical order"));
    }

    #[tokio::test]
    async fn complete_collection_digest_closes_typed_file_members_and_authenticated_empty() {
        const BRANCH_ID: &str = "closure-file-branch";
        const SCHEMA_KEY: &str = "closure_file_schema";
        let generation = CommitId::for_test_label("closure-file-generation");
        let mut rows = HotRowMap::new();
        let typed_pk = RowPk::from_components(smallvec::smallvec![
            crate::row_pk::RowPkComponent::Integer(-7),
            crate::row_pk::RowPkComponent::Bytes(Bytes::from_static(b"typed")),
        ])
        .expect("typed composite primary key");
        for (row_pk, file_id) in [
            (RowPk::single("unfiled"), None),
            (typed_pk, Some("a.lix".to_owned())),
            (RowPk::single("file-string"), Some("b.lix".to_owned())),
        ] {
            rows.insert(
                HeadRowIdentity {
                    schema_key: SCHEMA_KEY.to_owned(),
                    row_pk,
                    file_id,
                },
                encoded_test_hot_value(generation, false, false),
            );
        }
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = StorageWriteSet::new();
        stage_complete_collection_controls(&mut writes, BRANCH_ID, generation, &rows)
            .expect("complete controls should stage");
        stage_complete_hot_rows(&mut writes, BRANCH_ID, generation, rows);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("typed file fixture should publish");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("typed file fixture should read");
        let reader = HotStateStoreReader {
            store: &read,
            transaction_cache: None,
            root_base_cache: None,
        };
        let missing_row_pk = RowPk::single("missing-member");
        let missing_identity = TrackedStateKeyRef {
            schema_key: SCHEMA_KEY,
            row_pk: &missing_row_pk,
            file_id: None,
        };
        reader
            .validate_exact_collection_closure(
                BRANCH_ID,
                generation,
                crate::collection_generation::CollectionScopeRef {
                    schema_key: SCHEMA_KEY,
                    file_id: None,
                },
                missing_identity,
                HotStateReadDomain::Tracked,
                false,
            )
            .await
            .expect("schema scope should close in canonical file/member order");
        reader
            .validate_exact_collection_closure(
                BRANCH_ID,
                generation,
                crate::collection_generation::CollectionScopeRef {
                    schema_key: SCHEMA_KEY,
                    file_id: Some("a.lix"),
                },
                TrackedStateKeyRef {
                    schema_key: SCHEMA_KEY,
                    row_pk: &missing_row_pk,
                    file_id: Some("a.lix"),
                },
                HotStateReadDomain::Tracked,
                false,
            )
            .await
            .expect("typed file scope should close with complete PK encoding");

        const EMPTY_SCHEMA_KEY: &str = "authenticated_empty_schema";
        let empty_scope = crate::collection_generation::CollectionScopeRef {
            schema_key: EMPTY_SCHEMA_KEY,
            file_id: None,
        };
        let marker_identity = HeadRowIdentity {
            schema_key: crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY.to_owned(),
            row_pk: RowPk::single(crate::collection_generation::collection_scope_key(
                empty_scope,
            )),
            file_id: None,
        };
        let marker_rows = HotRowMap::from([(
            marker_identity,
            encoded_test_hot_value(generation, false, false),
        )]);
        let mut writes = StorageWriteSet::new();
        stage_complete_collection_controls(&mut writes, BRANCH_ID, generation, &marker_rows)
            .expect("authenticated empty control should stage");
        stage_complete_hot_rows(&mut writes, BRANCH_ID, generation, marker_rows);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("authenticated empty fixture should publish");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("authenticated empty fixture should read");
        HotStateStoreReader {
            store: &read,
            transaction_cache: None,
            root_base_cache: None,
        }
        .validate_exact_collection_closure(
            BRANCH_ID,
            generation,
            empty_scope,
            TrackedStateKeyRef {
                schema_key: EMPTY_SCHEMA_KEY,
                row_pk: &missing_row_pk,
                file_id: None,
            },
            HotStateReadDomain::Tracked,
            false,
        )
        .await
        .expect("explicit empty control should authenticate an empty scope");
    }

    /// A collection-generation fence retires every tracked member older than
    /// the marker. Untracked rows carry no `commit_id`, so the commit-ordered
    /// comparison that implements the fence is false for them; once tracked
    /// and untracked rows share one serving generation, a tracked collection
    /// replacement would silently delete the branch's history-free rows in the
    /// same schema scope unless they are exempt.
    #[tokio::test]
    async fn collection_generation_fence_retires_stale_tracked_rows_but_not_untracked_rows() {
        const BRANCH_ID: &str = "fence-branch";
        const SCHEMA_KEY: &str = "fence_schema";
        // The fence is a commit-ordered comparison, so the stale row's commit
        // must sort strictly below the marker's. Sorting two labels makes that
        // true regardless of how the labels hash.
        let mut ordered = [
            CommitId::for_test_label("fence-commit-a"),
            CommitId::for_test_label("fence-commit-b"),
        ];
        ordered.sort();
        let [old_generation, fence_commit] = ordered;
        let generation = CommitId::for_test_label("fence-serving-generation");
        assert_ne!(
            generation, fence_commit,
            "an active fence requires the control to name a different generation"
        );
        let scope = crate::collection_generation::CollectionScopeRef {
            schema_key: SCHEMA_KEY,
            file_id: None,
        };

        let stale_tracked = HeadRowIdentity {
            schema_key: SCHEMA_KEY.to_owned(),
            row_pk: RowPk::single("stale-tracked"),
            file_id: None,
        };
        let untracked = HeadRowIdentity {
            schema_key: SCHEMA_KEY.to_owned(),
            row_pk: RowPk::single("live-untracked"),
            file_id: None,
        };
        let marker = HeadRowIdentity {
            schema_key: crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY.to_owned(),
            row_pk: RowPk::single(crate::collection_generation::collection_scope_key(scope)),
            file_id: None,
        };

        let rows = HotRowMap::from([
            // Older than the fence: must be retired.
            (
                stale_tracked,
                encoded_test_hot_value(old_generation, false, false),
            ),
            // History-free: the fence cannot speak about it.
            (
                untracked.clone(),
                encoded_test_hot_value(generation, true, false),
            ),
            (marker, encoded_test_hot_value(fence_commit, false, false)),
        ]);

        let storage = StorageAdapter::new(Memory::new());
        let mut writes = StorageWriteSet::new();
        stage_complete_collection_controls(&mut writes, BRANCH_ID, generation, &rows)
            .expect("fence controls should stage");
        stage_complete_hot_rows(&mut writes, BRANCH_ID, generation, rows);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("fence fixture should publish");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("fence fixture should read");
        let reader = HotStateStoreReader {
            store: &read,
            transaction_cache: None,
            root_base_cache: None,
        };
        let batch = reader
            .scan_live_batch_for_generation(
                BRANCH_ID,
                generation,
                None,
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec![SCHEMA_KEY.to_owned()],
                        ..TrackedStateFilter::default()
                    },
                    read_columns: TrackedStateReadColumns::default(),
                    limit: None,
                },
            )
            .await
            .expect("fenced scope should scan");

        let surviving = batch
            .iter()
            .map(|row| (row.row_pk().clone(), row.untracked()))
            .collect::<Vec<_>>();
        assert_eq!(
            surviving,
            vec![(RowPk::single("live-untracked"), true)],
            "the fence must retire the stale tracked row and keep the untracked row"
        );
    }

    #[tokio::test]
    async fn exact_collection_closure_distinguishes_bootstrap_from_published_missing_digest() {
        const BRANCH_ID: &str = "closure-bootstrap-branch";
        const SCHEMA_KEY: &str = "closure_bootstrap_schema";
        let generation = CommitId::for_test_label("closure-bootstrap-generation");
        let scope = crate::collection_generation::CollectionScopeRef {
            schema_key: SCHEMA_KEY,
            file_id: None,
        };
        let missing_row_pk = RowPk::single("missing-member");
        let required_identity = TrackedStateKeyRef {
            schema_key: SCHEMA_KEY,
            row_pk: &missing_row_pk,
            file_id: None,
        };
        let storage = StorageAdapter::new(Memory::new());

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("bootstrap read should open");
        let reader = HotStateStoreReader {
            store: &read,
            transaction_cache: None,
            root_base_cache: None,
        };
        reader
            .validate_exact_collection_closure(
                BRANCH_ID,
                generation,
                scope,
                required_identity,
                HotStateReadDomain::Untracked,
                true,
            )
            .await
            .expect("an explicitly allowed empty bootstrap may omit its control");
        let error = reader
            .validate_exact_collection_closure(
                BRANCH_ID,
                generation,
                scope,
                required_identity,
                HotStateReadDomain::Untracked,
                false,
            )
            .await
            .expect_err("a published empty scope must carry its exact control");
        assert!(error.message.contains("missing its exact control"));
        drop(read);

        let mut writes = StorageWriteSet::new();
        stage_hot_collection_control(
            &mut writes,
            BRANCH_ID,
            generation,
            scope,
            HotCollectionControl {
                active_generation: generation,
                live_count: 0,
                ordered_identity_digest: None,
            },
        )
        .expect("digestless published control should encode as a corruption fixture");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("digestless corruption fixture should publish");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("published corruption read should open");
        let error = HotStateStoreReader {
            store: &read,
            transaction_cache: None,
            root_base_cache: None,
        }
        .validate_exact_collection_closure(
            BRANCH_ID,
            generation,
            scope,
            required_identity,
            HotStateReadDomain::Untracked,
            true,
        )
        .await
        .expect_err("bootstrap allowance must not accept a published digestless control");
        assert!(error.message.contains("no exact identity digest"));
    }

    #[tokio::test]
    async fn exact_collection_closure_rejects_missing_malformed_stale_and_forged_controls() {
        const BRANCH_ID: &str = "closure-control-branch";
        const SCHEMA_KEY: &str = "closure_control_schema";
        let generation = CommitId::for_test_label("closure-control-generation");
        let scope = crate::collection_generation::CollectionScopeRef {
            schema_key: SCHEMA_KEY,
            file_id: None,
        };
        let rows = HotRowMap::from([(
            HeadRowIdentity {
                schema_key: SCHEMA_KEY.to_owned(),
                row_pk: RowPk::single("member"),
                file_id: None,
            },
            encoded_test_hot_value(generation, false, false),
        )]);
        let memory = Memory::new();
        let storage = StorageAdapter::new(memory.clone());
        let mut writes = StorageWriteSet::new();
        stage_complete_collection_controls(&mut writes, BRANCH_ID, generation, &rows)
            .expect("base control should stage");
        stage_complete_hot_rows(&mut writes, BRANCH_ID, generation, rows);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("base closure fixture should publish");
        let base = memory.fork().expect("base fixture fork");
        drop(storage);
        drop(memory);
        let missing_row_pk = RowPk::single("missing-member");

        for (label, expected) in [
            ("missing", "missing its exact control"),
            ("malformed", "hot collection control"),
            ("stale", "stale generation"),
            ("no-digest", "no exact identity digest"),
            ("forged", "identity digest"),
        ] {
            let storage = StorageAdapter::new(
                base.fork().expect("fork base closure fixture"),
            );
            let control_key = StorageKey(Bytes::from(hot_collection_control_key(
                BRANCH_ID, generation, scope,
            )));
            let mut writes = StorageWriteSet::new();
            match label {
                "missing" => writes.delete(COLLECTION_CONTROL_SPACE, control_key),
                "malformed" => writes.put(
                    COLLECTION_CONTROL_SPACE,
                    control_key,
                    StorageValue {
                        bytes: Bytes::from_static(b"\0"),
                    },
                ),
                "stale" => stage_hot_collection_control(
                    &mut writes,
                    BRANCH_ID,
                    generation,
                    scope,
                    HotCollectionControl {
                        active_generation: CommitId::for_test_label("stale-generation"),
                        live_count: 1,
                        ordered_identity_digest: Some([0; 32]),
                    },
                )
                .expect("stale control should encode"),
                "no-digest" => stage_hot_collection_control(
                    &mut writes,
                    BRANCH_ID,
                    generation,
                    scope,
                    HotCollectionControl {
                        active_generation: generation,
                        live_count: 1,
                        ordered_identity_digest: None,
                    },
                )
                .expect("digest-free control should encode"),
                "forged" => stage_hot_collection_control(
                    &mut writes,
                    BRANCH_ID,
                    generation,
                    scope,
                    HotCollectionControl {
                        active_generation: generation,
                        live_count: 1,
                        ordered_identity_digest: Some([0; 32]),
                    },
                )
                .expect("forged control should encode"),
                _ => unreachable!("closed corruption fixture set"),
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("control corruption should publish below the reader");
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("corrupt control fixture should read");
            let error = HotStateStoreReader {
                store: &read,
                transaction_cache: None,
                root_base_cache: None,
            }
            .validate_exact_collection_closure(
                BRANCH_ID,
                generation,
                scope,
                TrackedStateKeyRef {
                    schema_key: SCHEMA_KEY,
                    row_pk: &missing_row_pk,
                    file_id: None,
                },
                HotStateReadDomain::Tracked,
                false,
            )
            .await
            .expect_err("corrupt exact control must fail closed");
            assert!(
                error.message.contains(expected),
                "unexpected {label} control error: {error:?}"
            );
        }
    }

    fn live_row(row_pk: &str, commit_label: &str) -> MaterializedHotStateRow {
        MaterializedHotStateRow {
            row_pk: RowPk::single(row_pk),
            schema_key: "schema".to_owned(),
            file_id: None,
            snapshot_content: None,
            metadata: None,
            deleted: false,
            created_at: timestamp(),
            updated_at: timestamp(),
            global: false,
            change_id: Some(ChangeId::for_test_label(&format!("change-{commit_label}"))),
            commit_id: Some(CommitId::for_test_label(commit_label)),
            untracked: false,
            branch_id: Arc::from("branch"),
        }
    }

    #[test]
    fn sparse_hot_overlay_merges_with_packed_rows_in_identity_order() {
        let hot = vec![live_row("c", "hot-c")];
        let packed = vec![live_row("a", "packed-a"), live_row("b", "packed-b")];

        let merged = merge_ordered_live_rows(hot, packed);

        assert_eq!(
            merged
                .iter()
                .map(|row| row.row_pk.as_single_string_owned().expect("single key"))
                .collect::<Vec<_>>(),
            ["a", "b", "c"]
        );
    }

    #[test]
    fn ordered_authority_exclusion_removes_only_identity_collisions() {
        let rows = MaterializedHotStateBatch::from_rows(vec![
            live_row("a", "candidate-a"),
            live_row("b", "candidate-b"),
            live_row("c", "candidate-c"),
            live_row("d", "candidate-d"),
        ]);
        let authority = MaterializedHotStateBatch::from_rows(vec![
            live_row("a", "authority-a"),
            live_row("c", "authority-c"),
        ]);

        let filtered = exclude_ordered_live_batch_identities(rows, &authority);

        assert_eq!(
            filtered
                .iter()
                .map(|row| { row.row_pk().as_single_string_owned().expect("single key") })
                .collect::<Vec<_>>(),
            ["b", "d"]
        );
    }

    #[tokio::test]
    async fn packed_mutation_lookup_retains_large_refs_and_finds_system_schemas() {
        const COMMIT_LABEL: &str = "packed-system-schema-base";
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label(COMMIT_LABEL);
        let row_pk = RowPk::single("packed-system-row");
        let snapshot = serde_json::json!({
            "key": "packed-system-row",
            "value": "x".repeat(crate::json_store::JSON_INLINE_MAX_BYTES + 1),
        })
        .to_string();
        crate::test_support::seed_branch_head_with_rows(
            storage.clone(),
            crate::GLOBAL_BRANCH_ID,
            COMMIT_LABEL,
            &[MaterializedTrackedStateRow {
                row_pk: row_pk.clone(),
                schema_key: "lix_key_value".to_owned(),
                file_id: None,
                snapshot_content: Some(snapshot.into()),
                decoded_snapshot: None,
                metadata: None,
                deleted: false,
                created_at: timestamp().to_string(),
                updated_at: timestamp().to_string(),
                change_id: ChangeId::for_test_label("packed-system-change"),
                commit_id: generation,
            }],
        )
        .await;

        let mut manifest_key = hot_scope_prefix(crate::GLOBAL_BRANCH_ID, generation);
        manifest_key.extend_from_slice(generation.as_uuid().as_bytes());
        let mut writes = StorageWriteSet::new();
        writes.delete(
            ROW_SPACE,
            StorageKey(Bytes::from(encode_hot_row_key(&HeadIdentity {
                branch_id: crate::GLOBAL_BRANCH_ID.to_owned(),
                generation,
                schema_key: "lix_key_value".to_owned(),
                row_pk: row_pk.clone(),
                file_id: None,
            }))),
        );
        writes.put(
            PACKED_CURRENT_BASE_SPACE,
            StorageKey(Bytes::from(manifest_key)),
            StorageValue {
                bytes: Bytes::from_static(&[0; 16]),
            },
        );
        writes.put(
            PACKED_CURRENT_BASE_CONTROL_SPACE,
            StorageKey(Bytes::from(hot_scope_prefix(
                crate::GLOBAL_BRANCH_ID,
                generation,
            ))),
            StorageValue {
                bytes: Bytes::from_static(&[1]),
            },
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("publish packed system-schema fixture");

        let json_get_many_calls = Arc::new(AtomicUsize::new(0));
        let get_many_calls = Arc::new(AtomicUsize::new(0));
        let scan_calls = Arc::new(AtomicUsize::new(0));
        let read = JsonCountingRead {
            inner: CountingRead {
                inner: storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("open packed fixture read"),
                get_many_calls: Arc::clone(&get_many_calls),
                scan_calls: Some(Arc::clone(&scan_calls)),
            },
            json_get_many_calls: Arc::clone(&json_get_many_calls),
        };
        let transaction_cache = Arc::new(HotStateTransactionCache::default());
        let request_keys = [TrackedStateKeyRef {
            schema_key: "lix_key_value",
            row_pk: &row_pk,
            file_id: None,
        }];
        let entries = load_packed_current_base_exact_entries(
            &read,
            crate::GLOBAL_BRANCH_ID,
            generation,
            &request_keys,
            &[],
            Some(transaction_cache.as_ref()),
        )
        .await
        .expect("load packed mutation predecessor");
        for _ in 2..=TRANSACTION_PACKED_POINT_CACHE_MIN_OBSERVATIONS {
            let observed = load_packed_current_base_exact_entries(
                &read,
                crate::GLOBAL_BRANCH_ID,
                generation,
                &request_keys,
                &[],
                Some(transaction_cache.as_ref()),
            )
            .await
            .expect("observe repeated packed mutation predecessor");
            assert!(observed[0].is_some());
        }
        let admitted = load_packed_current_base_exact_entries(
            &read,
            crate::GLOBAL_BRANCH_ID,
            generation,
            &request_keys,
            &[],
            Some(transaction_cache.as_ref()),
        )
        .await
        .expect("admit repeated packed mutation predecessor");
        assert!(admitted[0].is_some());
        let admitted_read_counts = (
            get_many_calls.load(Ordering::Relaxed),
            scan_calls.load(Ordering::Relaxed),
        );
        let reused = load_packed_current_base_exact_entries(
            &read,
            crate::GLOBAL_BRANCH_ID,
            generation,
            &request_keys,
            &[],
            Some(transaction_cache.as_ref()),
        )
        .await
        .expect("reuse admitted packed mutation predecessor");
        assert!(reused[0].is_some());
        assert_eq!(
            (
                get_many_calls.load(Ordering::Relaxed),
                scan_calls.load(Ordering::Relaxed),
            ),
            admitted_read_counts,
            "a transaction snapshot must reuse an admitted packed segment by immutable address"
        );
        let (_, change, _, _) = entries[0].as_ref().expect("packed predecessor exists");
        assert!(
            change.snapshot.is_some(),
            "mutation lookup must retain the native typed payload"
        );
        assert_eq!(
            json_get_many_calls.load(Ordering::Relaxed),
            0,
            "mutation predecessor lookup must not read large JSON payloads"
        );

        let reader = HotStateStoreReader {
            store: read,
            transaction_cache: Some(transaction_cache),
            root_base_cache: None,
        };
        let control = BranchHeadControl {
            head_commit_id: generation,
            tracked_generation: generation,
            current_state_revision: 0,
            schema_presence_bloom: [u64::MAX; 4],
            working_diff_checkpoint_commit_id: None,
            created_at: timestamp(),
            updated_at: timestamp(),
            ref_change_id: ChangeId::for_test_label("packed-system-ref"),
        };
        assert!(
            reader
                .has_schema_rows(crate::GLOBAL_BRANCH_ID, control, "lix_key_value",)
                .await
                .expect("probe packed system schema"),
            "engine-owned schemas must probe packed bases before skipping plugin segments"
        );
        let exact = reader
            .load_projected_live_batch_refs(
                crate::GLOBAL_BRANCH_ID,
                control,
                &[TrackedStateKeyRef {
                    schema_key: "lix_key_value",
                    row_pk: &row_pk,
                    file_id: None,
                }],
                &ChangeRecordProjection::full(),
            )
            .await
            .expect("load packed system row through exact live-state API");
        assert!(
            exact.row(0).is_some(),
            "engine-owned exact lookups must resolve packed bases before skipping plugin segments"
        );
    }

    #[tokio::test]
    async fn complete_replacement_retires_only_exclusive_schema_bases() {
        const BRANCH_ID: &str = "exclusive-schema-replacement";
        const SCHEMA_KEY: &str = "target_schema";
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label("exclusive-generation");
        let shared_head = CommitId::for_test_label("shared-base");
        let exclusive_head = CommitId::for_test_label("exclusive-base");
        let replacement_head = CommitId::for_test_label("replacement-base");
        let mut fixture_writes = StorageWriteSet::new();
        stage_hot_collection_control(
            &mut fixture_writes,
            BRANCH_ID,
            generation,
            crate::collection_generation::CollectionScopeRef {
                schema_key: SCHEMA_KEY,
                file_id: None,
            },
            HotCollectionControl {
                active_generation: generation,
                live_count: 1_024,
                ordered_identity_digest: None,
            },
        )
        .expect("stage replacement collection control");
        for head in [shared_head, exclusive_head] {
            let mut key = hot_scope_prefix(BRANCH_ID, generation);
            key.extend_from_slice(head.as_uuid().as_bytes());
            fixture_writes.put(
                PACKED_CURRENT_BASE_SPACE,
                StorageKey(Bytes::from(key)),
                StorageValue {
                    bytes: Bytes::from_static(&[0; 16]),
                },
            );
        }
        fixture_writes.put(
            PACKED_CURRENT_BASE_CONTROL_SPACE,
            StorageKey(Bytes::from(hot_scope_prefix(BRANCH_ID, generation))),
            StorageValue {
                bytes: Bytes::from_static(&[1]),
            },
        );
        stage_packed_exclusive_schema_base_ref(
            &mut fixture_writes,
            BRANCH_ID,
            generation,
            SCHEMA_KEY,
            exclusive_head,
        );
        storage
            .commit_write_set(fixture_writes, StorageWriteOptions::default())
            .await
            .expect("commit packed replacement fixture");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open packed replacement read");
        let mut writes = StorageWriteSet::new();
        let mut coverage = WorkingDiffIndexCoverage::default();
        let (_, retired) = HotStateWriter {
            store: &read,
            writes: &mut writes,
            transaction_global_schema_keys: None,
        }
        .stage_complete_collection_replacement_current_base(
            BRANCH_ID,
            generation,
            replacement_head,
            SCHEMA_KEY,
            1_024,
            &crate::hot_state::RowColumnarWriteSets::new(),
            None,
            &mut coverage,
        )
        .await
        .expect("stage complete packed replacement");
        assert!(retired);
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit complete packed replacement");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("verify complete packed replacement");
        let base_keys = [shared_head, exclusive_head, replacement_head]
            .into_iter()
            .map(|head| {
                let mut key = hot_scope_prefix(BRANCH_ID, generation);
                key.extend_from_slice(head.as_uuid().as_bytes());
                StorageKey(Bytes::from(key))
            })
            .collect::<Vec<_>>();
        let bases = PointReadPlan::new(PACKED_CURRENT_BASE_SPACE, &base_keys)
            .materialize(&read, StorageGetOptions::default())
            .await
            .expect("read replacement bases")
            .value;
        assert!(
            bases[0].is_some(),
            "a shared-schema base must remain visible"
        );
        assert!(bases[1].is_none(), "the exclusive predecessor must retire");
        assert!(bases[2].is_some(), "the replacement base must publish");
    }

    #[tokio::test]
    async fn corrupt_collection_control_cannot_certify_omitted_authoritative_members() {
        const BRANCH_ID: &str = "01920000-0000-7000-8000-0000000000ca";
        const SCHEMA_KEY: &str = "corrupt_control_schema";
        let storage = StorageAdapter::new(Memory::new());
        let parent_commit_id = CommitId::for_test_label("corrupt-control-parent");
        let row_pks = [
            RowPk::single("row-a"),
            RowPk::single("row-b"),
            RowPk::single("row-c"),
        ];
        let created_at = timestamp();
        let rows = row_pks
            .iter()
            .enumerate()
            .map(|(index, row_pk)| MaterializedTrackedStateRow {
                row_pk: row_pk.clone(),
                schema_key: SCHEMA_KEY.to_owned(),
                file_id: None,
                snapshot_content: Some(format!(r#"{{"index":{index}}}"#).into()),
                decoded_snapshot: None,
                metadata: None,
                deleted: false,
                created_at: created_at.to_string(),
                updated_at: created_at.to_string(),
                change_id: ChangeId::for_test_label(&format!("corrupt-control-{index}")),
                commit_id: parent_commit_id,
            })
            .collect::<Vec<_>>();
        crate::test_support::seed_branch_head_with_rows(
            storage.clone(),
            BRANCH_ID,
            "corrupt-control-parent",
            &rows,
        )
        .await;

        let forged_members = [&row_pks[0], &row_pks[1]];
        let forged_digest =
            crate::collection_generation::ordered_single_string_identity_digest(forged_members)
                .expect("single-string fixture identities should hash");
        let mut corrupt_writes = StorageWriteSet::new();
        stage_hot_collection_control(
            &mut corrupt_writes,
            BRANCH_ID,
            parent_commit_id,
            crate::collection_generation::CollectionScopeRef {
                schema_key: SCHEMA_KEY,
                file_id: None,
            },
            HotCollectionControl {
                active_generation: parent_commit_id,
                live_count: 2,
                ordered_identity_digest: Some(forged_digest),
            },
        )
        .expect("forged derived control should encode");
        storage
            .commit_write_set(corrupt_writes, StorageWriteOptions::default())
            .await
            .expect("forged derived control should commit");

        let new_head = CommitId::for_test_label("corrupt-control-child");
        let change_ids = [
            ChangeId::for_test_label("corrupt-control-child-a"),
            ChangeId::for_test_label("corrupt-control-child-b"),
        ];
        let replacement_snapshots = forged_members
            .iter()
            .map(|row_pk| {
                let row = WasmTypedRow::from_test_json_unchecked(
                    row_pk,
                    &serde_json::json!({"replacement": true}),
                )
                .expect("replacement snapshot should type");
                encode_snapshot(&row).expect("replacement snapshot should encode")
            })
            .collect::<Vec<_>>();
        let replacement_deltas = forged_members
            .iter()
            .zip(change_ids)
            .zip(&replacement_snapshots)
            .map(|((row_pk, change_id), snapshot)| CurrentStateDeltaRef {
                schema_key: SCHEMA_KEY,
                file_id: None,
                row_pk,
                change_id: Some(change_id),
                commit_id: Some(new_head),
                untracked: false,
                deleted: false,
                created_at,
                updated_at: created_at,
                snapshot: Some(snapshot),
                metadata: None,
                columnar_base_coordinate: None,
            })
            .collect::<Vec<_>>();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("corrupt-control read should open");
        let mut replacement_writes = StorageWriteSet::new();
        let replacement = HotStateWriter {
            store: &read,
            writes: &mut replacement_writes,
            transaction_global_schema_keys: None,
        }
        .try_stage_exact_collection_replacement_current_base(
            BRANCH_ID,
            parent_commit_id,
            parent_commit_id,
            false,
            new_head,
            &replacement_deltas,
            &crate::hot_state::RowColumnarWriteSets::new(),
            None,
            &mut WorkingDiffIndexCoverage::default(),
        )
        .await
        .expect("authority mismatch should fail closed");
        assert_eq!(replacement, None);
        assert_eq!(replacement_writes.stats().staged_puts, 0);
        assert_eq!(replacement_writes.stats().staged_deletes, 0);

        let deletion_deltas = replacement_deltas
            .iter()
            .map(|delta| CurrentStateDeltaRef {
                deleted: true,
                snapshot: None,
                ..*delta
            })
            .collect::<Vec<_>>();
        let mut deletion_writes = StorageWriteSet::new();
        let deletion = HotStateWriter {
            store: &read,
            writes: &mut deletion_writes,
            transaction_global_schema_keys: None,
        }
        .try_stage_exact_collection_delete_current_base(
            BRANCH_ID,
            parent_commit_id,
            parent_commit_id,
            new_head,
            &deletion_deltas,
            None,
        )
        .await
        .expect("delete authority mismatch should fail closed");
        assert_eq!(deletion, None);
        assert_eq!(deletion_writes.stats().staged_puts, 0);
        assert_eq!(deletion_writes.stats().staged_deletes, 0);
    }

    #[tokio::test]
    async fn checkpoint_retires_materialized_packed_bases_in_active_generation() {
        const BRANCH_ID: &str = "01920000-0000-7000-8000-0000000000c9";
        const COMMIT_LABEL: &str = "checkpoint-packed-base";
        const SCHEMA_KEY: &str = "checkpoint_schema";
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label(COMMIT_LABEL);
        let row_pk = RowPk::single("packed-row");
        let created_at = timestamp();
        crate::test_support::seed_branch_head_with_rows(
            storage.clone(),
            BRANCH_ID,
            COMMIT_LABEL,
            &[MaterializedTrackedStateRow {
                row_pk: row_pk.clone(),
                schema_key: SCHEMA_KEY.to_owned(),
                file_id: None,
                snapshot_content: Some(r#"{"key":"packed-row"}"#.into()),
                decoded_snapshot: None,
                metadata: None,
                deleted: false,
                created_at: created_at.to_string(),
                updated_at: created_at.to_string(),
                change_id: ChangeId::for_test_label("checkpoint-packed-base-change"),
                commit_id: generation,
            }],
        )
        .await;

        let mut manifest_key = hot_scope_prefix(BRANCH_ID, generation);
        manifest_key.extend_from_slice(generation.as_uuid().as_bytes());
        let control_key = hot_scope_prefix(BRANCH_ID, generation);
        let index_key =
            packed_exclusive_schema_base_key(BRANCH_ID, generation, SCHEMA_KEY, generation);
        let mut fixture_writes = StorageWriteSet::new();
        fixture_writes.delete(
            ROW_SPACE,
            StorageKey(Bytes::from(encode_hot_row_key(&HeadIdentity {
                branch_id: BRANCH_ID.to_owned(),
                generation,
                schema_key: SCHEMA_KEY.to_owned(),
                row_pk: row_pk.clone(),
                file_id: None,
            }))),
        );
        fixture_writes.put(
            PACKED_CURRENT_BASE_SPACE,
            StorageKey(Bytes::from(manifest_key.clone())),
            StorageValue {
                bytes: Bytes::from_static(&[0; 16]),
            },
        );
        fixture_writes.put(
            PACKED_CURRENT_BASE_CONTROL_SPACE,
            StorageKey(Bytes::from(control_key.clone())),
            StorageValue {
                bytes: Bytes::from_static(&[1]),
            },
        );
        fixture_writes.put(
            PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE,
            StorageKey(Bytes::from(index_key.clone())),
            StorageValue {
                bytes: Bytes::from_static(&[1]),
            },
        );
        storage
            .commit_write_set(fixture_writes, StorageWriteOptions::default())
            .await
            .expect("publish packed checkpoint fixture");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open packed checkpoint read");
        let checkpoint_head = CommitId::for_test_label("checkpoint-packed-head");
        let checkpoint_change = ChangeId::for_test_label("checkpoint-packed-base-change");
        let typed = WasmTypedRow::from_test_json_unchecked(
            &row_pk,
            &serde_json::json!({"key": "packed-row"}),
        )
        .expect("packed checkpoint fixture should type");
        let snapshot = typed.durable_payload().expect("typed payload");
        let delta = CurrentStateDeltaRef {
            schema_key: SCHEMA_KEY,
            file_id: None,
            row_pk: &row_pk,
            change_id: Some(checkpoint_change),
            commit_id: Some(generation),
            untracked: false,
            deleted: false,
            created_at,
            updated_at: created_at,
            snapshot: Some(snapshot.as_ref()),
            metadata: None,
            columnar_base_coordinate: None,
        };
        let mut checkpoint_writes = StorageWriteSet::new();
        let mut coverage = WorkingDiffIndexCoverage::default();
        HotStateWriter {
            store: &read,
            writes: &mut checkpoint_writes,
            transaction_global_schema_keys: None,
        }
        .stage_checkpoint_current_state(
            BRANCH_ID,
            generation,
            checkpoint_head,
            &[delta],
            &BTreeSet::new(),
            generation,
            &mut coverage,
        )
        .await
        .expect("stage checkpoint over packed base");
        drop(read);
        storage
            .commit_write_set(checkpoint_writes, StorageWriteOptions::default())
            .await
            .expect("commit checkpoint over packed base");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("verify packed checkpoint retirement");
        let packed = PointReadPlan::new(
            PACKED_CURRENT_BASE_SPACE,
            &[StorageKey(Bytes::from(manifest_key))],
        )
        .materialize(&read, StorageGetOptions::default())
        .await
        .expect("read retired packed manifest")
        .value;
        let control = PointReadPlan::new(
            PACKED_CURRENT_BASE_CONTROL_SPACE,
            &[StorageKey(Bytes::from(control_key))],
        )
        .materialize(&read, StorageGetOptions::default())
        .await
        .expect("read retired packed control")
        .value;
        assert!(packed[0].is_none());
        assert!(control[0].is_none());
        let index = PointReadPlan::new(
            PACKED_CURRENT_EXCLUSIVE_SCHEMA_BASE_SPACE,
            &[StorageKey(Bytes::from(index_key))],
        )
        .materialize(&read, StorageGetOptions::default())
        .await
        .expect("read retired packed exclusive-schema index")
        .value;
        assert!(index[0].is_none());
        let hot = PointReadPlan::new(
            ROW_SPACE,
            &[StorageKey(Bytes::from(encode_hot_row_key(&HeadIdentity {
                branch_id: BRANCH_ID.to_owned(),
                generation,
                schema_key: SCHEMA_KEY.to_owned(),
                row_pk,
                file_id: None,
            })))],
        )
        .materialize(&read, StorageGetOptions::default())
        .await
        .expect("read materialized checkpoint row")
        .value;
        assert!(
            hot[0].is_some(),
            "checkpoint must materialize a packed-only row before retiring its base"
        );
    }

    fn diff_identity(branch_id: &str, generation: CommitId, row: &str) -> HeadIdentity {
        HeadIdentity {
            branch_id: branch_id.to_string(),
            generation,
            schema_key: "schema".to_string(),
            row_pk: RowPk::single(row),
            file_id: None,
        }
    }

    fn working_diff_version(label: &str) -> WorkingDiffVersion {
        WorkingDiffVersion {
            change_id: ChangeId::for_test_label(&format!("{label}-change")),
            commit_id: CommitId::for_test_label(&format!("{label}-commit")),
            deleted: false,
            created_at: timestamp(),
            updated_at: timestamp(),
            snapshot: WorkingDiffSlotFingerprint {
                kind: WORKING_DIFF_SLOT_NONE,
                hash: [0; JSON_REF_BYTES],
            },
            metadata: WorkingDiffSlotFingerprint {
                kind: WORKING_DIFF_SLOT_NONE,
                hash: [0; JSON_REF_BYTES],
            },
        }
    }

    #[test]
    fn packed_recreate_wins_over_older_hot_working_diff_tombstone() {
        let baseline = working_diff_version("checkpoint-baseline");
        let mut hot = working_diff_version("hot-tombstone");
        hot.commit_id = CommitId::new(uuid::Uuid::from_u128(1));
        hot.deleted = true;
        let mut packed = working_diff_version("packed-recreate");
        packed.commit_id = CommitId::new(uuid::Uuid::from_u128(2));

        let (before, after) =
            choose_hot_or_packed_working_diff(Some((Some(baseline), hot)), Some(packed))
                .expect("one current version must win");

        assert_eq!(before, Some(baseline));
        assert_eq!(after, packed);
        assert!(!after.deleted);
    }

    #[test]
    fn packed_exact_keys_are_ordered_and_deduplicated() {
        let filter = TrackedStateFilter {
            schema_keys: vec![
                "schema-b".to_owned(),
                "schema-a".to_owned(),
                "schema-a".to_owned(),
            ],
            row_pks: vec![
                RowPk::single("second"),
                RowPk::single("first"),
                RowPk::single("first"),
            ],
            file_ids: vec![NullableKeyFilter::Null],
            ..TrackedStateFilter::default()
        };

        let keys = packed_exact_keys_for_filter(&filter).expect("filter is finite");
        assert_eq!(
            keys,
            vec![
                TrackedStateKey {
                    schema_key: "schema-a".to_owned(),
                    file_id: None,
                    row_pk: RowPk::single("first"),
                },
                TrackedStateKey {
                    schema_key: "schema-a".to_owned(),
                    file_id: None,
                    row_pk: RowPk::single("second"),
                },
                TrackedStateKey {
                    schema_key: "schema-b".to_owned(),
                    file_id: None,
                    row_pk: RowPk::single("first"),
                },
                TrackedStateKey {
                    schema_key: "schema-b".to_owned(),
                    file_id: None,
                    row_pk: RowPk::single("second"),
                },
            ]
        );

        let bounded = TrackedStateFilter {
            schema_keys: vec!["schema".to_owned()],
            row_pks: vec![
                RowPk::single("first"),
                RowPk::single("second"),
                RowPk::single("second"),
                RowPk::single("third"),
            ],
            row_pk_lower: Some(crate::tracked_state::RowPkRangeBound {
                row_pk: RowPk::single("second"),
                inclusive: true,
            }),
            row_pk_upper: Some(crate::tracked_state::RowPkRangeBound {
                row_pk: RowPk::single("third"),
                inclusive: false,
            }),
            file_ids: vec![NullableKeyFilter::Null],
            ..TrackedStateFilter::default()
        };
        assert_eq!(
            packed_exact_keys_for_filter(&bounded).expect("bounded filter is finite"),
            vec![TrackedStateKey {
                schema_key: "schema".to_owned(),
                file_id: None,
                row_pk: RowPk::single("second"),
            }]
        );

        let filed = TrackedStateFilter {
            schema_keys: vec!["schema".to_owned()],
            row_pks: vec![RowPk::single("owner")],
            file_ids: vec![
                NullableKeyFilter::Value("file-b".to_owned()),
                NullableKeyFilter::Null,
                NullableKeyFilter::Value("file-a".to_owned()),
            ],
            ..TrackedStateFilter::default()
        };
        assert_eq!(
            packed_exact_keys_for_filter(&filed).expect("explicit file filters are finite"),
            vec![
                TrackedStateKey {
                    schema_key: "schema".to_owned(),
                    file_id: None,
                    row_pk: RowPk::single("owner"),
                },
                TrackedStateKey {
                    schema_key: "schema".to_owned(),
                    file_id: Some("file-a".to_owned()),
                    row_pk: RowPk::single("owner"),
                },
                TrackedStateKey {
                    schema_key: "schema".to_owned(),
                    file_id: Some("file-b".to_owned()),
                    row_pk: RowPk::single("owner"),
                },
            ]
        );
        assert!(
            packed_exact_keys_for_filter(&TrackedStateFilter {
                schema_keys: vec!["schema".to_owned()],
                row_pks: vec![RowPk::single("owner")],
                file_ids: vec![NullableKeyFilter::Any],
                ..TrackedStateFilter::default()
            })
            .is_none()
        );
        assert!(
            packed_exact_keys_for_filter(&TrackedStateFilter {
                schema_keys: vec!["schema".to_owned()],
                row_pks: vec![RowPk::single("owner")],
                ..TrackedStateFilter::default()
            })
            .is_none()
        );
    }

    fn single_hot_diff_segment(
        checkpoint_commit_id: CommitId,
        identity: &HeadIdentity,
    ) -> (Vec<u8>, Vec<u8>) {
        let scope = encode_working_diff_scope_prefix(
            &identity.branch_id,
            checkpoint_commit_id,
            identity.generation,
        );
        let full_key = encode_hot_diff_key(checkpoint_commit_id, identity);
        let suffix = full_key
            .strip_prefix(scope.as_slice())
            .expect("encoded hot diff identity starts with its scope");
        let mut value = Vec::with_capacity(HOT_DIFF_SEGMENT_HEADER_BYTES + 4 + suffix.len());
        value.push(HOT_DIFF_SEGMENT_VERSION);
        value.extend_from_slice(&1_u32.to_le_bytes());
        value.extend_from_slice(
            &u32::try_from(suffix.len())
                .expect("test identity suffix fits u32")
                .to_le_bytes(),
        );
        value.extend_from_slice(suffix);
        let mut key = scope;
        key.extend_from_slice(blake3::hash(&value).as_bytes());
        (key, value)
    }

    #[tokio::test]
    async fn hot_working_diff_entries_share_one_identity_batch() {
        let storage = StorageAdapter::new(Memory::new());
        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("classification read should open");
        let candidates = ["first", "second"]
            .into_iter()
            .map(|row| {
                (
                    TrackedStateKey {
                        schema_key: "schema".to_owned(),
                        row_pk: RowPk::single(row),
                        file_id: Some("file".to_owned()),
                    },
                    None,
                    working_diff_version(row),
                )
            })
            .collect();

        let entries = classify_hot_working_diff_entries(&store, candidates)
            .await
            .expect("valid working diff batch");

        assert_eq!(entries.len(), 2);
        assert!(entries[0].identity.shares_batch_with(&entries[1].identity));
        for entry in &entries {
            assert!(
                entry
                    .identity
                    .shares_key_with(&entry.after.as_ref().expect("after row").identity)
            );
        }
    }

    #[tokio::test]
    async fn finite_hot_working_diff_borrows_keys_into_one_identity_batch() {
        let storage = StorageAdapter::new(Memory::new());
        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("classification read should open");
        let schema_key = String::from("schema");
        let file_id = String::from("file");
        let row_pks = [RowPk::single("first"), RowPk::single("second")];
        let candidates = row_pks
            .iter()
            .enumerate()
            .map(|(index, row_pk)| {
                (
                    TrackedStateKeyRef {
                        schema_key: &schema_key,
                        row_pk,
                        file_id: Some(&file_id),
                    },
                    None,
                    working_diff_version(if index == 0 { "first" } else { "second" }),
                )
            })
            .collect();

        let entries = classify_hot_working_diff_entry_refs(&store, candidates)
            .await
            .expect("valid borrowed diff batch");

        assert_eq!(entries.len(), 2);
        assert!(entries[0].identity.shares_batch_with(&entries[1].identity));
        assert_eq!(entries[0].identity.schema_key(), schema_key);
        assert_eq!(entries[1].identity.file_id(), Some(file_id.as_str()));
        for entry in &entries {
            assert!(
                entry
                    .identity
                    .shares_key_with(&entry.after.as_ref().expect("after row").identity)
            );
        }
    }

    #[test]
    fn ten_thousand_finite_hot_identities_share_one_primary_key_arena() {
        let generation = CommitId::for_test_label("point-key-generation");
        let row_pks = (0..5_000)
            .map(|index| RowPk::single(format!("row-{index:05}")))
            .collect::<Vec<_>>();
        let branch_id = String::from("branch");
        let schema_key = String::from("schema");
        let batch = FiniteHotIdentityBatchRef::new(
            &branch_id,
            generation,
            &schema_key,
            row_pks.iter().collect(),
            vec![None, Some("file")],
        )
        .expect("test identity count is representable");

        assert_eq!(batch.len(), 10_000);
        assert_eq!(batch.encoded.ranges.len(), batch.len());
        assert_eq!(batch.encoded.ranges.capacity(), batch.len());
        assert_eq!(
            batch
                .encoded
                .ranges
                .last()
                .map(|ranges| ranges.primary.offset() + ranges.primary.len()),
            Some(batch.encoded.bytes.len())
        );
        for index in [0, 1, 9_999] {
            let identity = batch.key_ref(index);
            assert_eq!(batch.branch_id.as_ptr(), branch_id.as_ptr());
            assert_eq!(identity.schema_key.as_ptr(), schema_key.as_ptr());
            let primary = batch.encoded.ranges[index].primary;
            let key = batch.encoded.primary_key(index);
            assert_eq!(
                key.0.as_ptr(),
                batch.encoded.bytes[primary.offset()..].as_ptr(),
                "primary point key {index} must remain a slice of the batch arena"
            );
        }
    }

    #[test]
    fn ten_thousand_hot_scan_identities_borrow_repeated_metadata() {
        const ROW_COUNT: usize = 10_000;
        let generation = CommitId::for_test_label("borrowed-scan-generation");
        let scope = hot_scope_prefix("branch", generation);
        let schema_key = "shared_schema";
        let file_id = "shared_file";
        let row_pks = (0..ROW_COUNT)
            .map(|index| RowPk::single(format!("row-{index:05}")))
            .collect::<Vec<_>>();
        let capacity = row_pks
            .iter()
            .try_fold(0_usize, |total, row_pk| {
                total.checked_add(
                    encoded_hot_identity_key_len(scope.len(), schema_key, row_pk, Some(file_id))
                        .expect("test key size is representable"),
                )
            })
            .expect("test key arena size is representable");
        let mut key_bytes = Vec::with_capacity(capacity);
        let ranges = row_pks
            .iter()
            .map(|row_pk| {
                let start = key_bytes.len();
                key_bytes.extend_from_slice(&scope);
                write_key_string(&mut key_bytes, schema_key, KEY_PART_FINAL);
                write_file_id(&mut key_bytes, Some(file_id));
                write_row_pk(&mut key_bytes, row_pk);
                start..key_bytes.len()
            })
            .collect::<Vec<_>>();
        assert_eq!(key_bytes.len(), capacity);
        let key_bytes = Bytes::from(key_bytes);
        let identities = ranges
            .into_iter()
            .map(|range| {
                decode_hot_scan_row_key_in_scope(key_bytes.slice(range), &scope)
                    .expect("decode borrowed hot scan key")
            })
            .collect::<Vec<_>>();

        assert_eq!(identities.len(), ROW_COUNT);
        assert_eq!(
            identities
                .iter()
                .map(HotScanIdentity::owned_metadata_buffer_count)
                .sum::<usize>(),
            0,
            "normal schema and file ids must remain ranges over storage keys"
        );
        assert!(identities.iter().all(|identity| {
            identity.schema_key() == schema_key && identity.file_id() == Some(file_id)
        }));

        let mut rows = MaterializedHotStateBatchBuilder::with_capacity(ROW_COUNT);
        for identity in identities {
            identity.push_materialized(
                &mut rows,
                None,
                None,
                false,
                timestamp(),
                timestamp(),
                false,
                None,
                None,
                true,
                "branch",
            );
        }
        let rows = rows.finish();

        assert_eq!(rows.len(), ROW_COUNT);
        assert_eq!(rows.dictionary_entry_count(), 3);
        assert_eq!(rows.dictionary_arena_buffer_count(), 1);
        assert_eq!(
            rows.dictionary_arena_allocation_count(),
            1,
            "materialization should allocate one small identity arena, not per-row buffers"
        );
        assert_eq!(rows.dictionary_arena_large_allocation_count(), 0);
        assert_eq!(
            rows.row(0).schema_key().as_ptr(),
            rows.row(ROW_COUNT - 1).schema_key().as_ptr()
        );
        assert_eq!(
            rows.row(0).file_id().expect("file").as_ptr(),
            rows.row(ROW_COUNT - 1).file_id().expect("file").as_ptr()
        );
    }

    fn adversarial_hot_scan_entry(
        generation: CommitId,
        row_pk: &str,
        file_id: &str,
        value: &'static [u8],
    ) -> (HotScanIdentity, Bytes) {
        let scope = hot_scope_prefix("branch", generation);
        let key = Bytes::from(encode_hot_row_key_parts(
            "branch",
            generation,
            "schema",
            &RowPk::single(row_pk),
            Some(file_id),
        ));
        let identity = decode_hot_scan_row_key_in_scope(key, &scope)
            .expect("decode adversarial HOT scan identity");
        (identity, Bytes::from_static(value))
    }

    #[test]
    fn hot_scan_canonicalizes_before_limit_and_collapses_only_identical_duplicates() {
        let generation = CommitId::for_test_label("adversarial-hot-canonical-order");
        let physical_rows = || {
            vec![
                adversarial_hot_scan_entry(generation, "row-z", "file-a", b"z"),
                adversarial_hot_scan_entry(generation, "row-z", "file-a", b"z"),
                adversarial_hot_scan_entry(generation, "row-a", "file-b", b"a"),
            ]
        };

        let canonical = canonicalize_hot_scan_rows(physical_rows(), None)
            .expect("identical repeated HOT observations should canonicalize");
        assert_eq!(canonical.len(), 2);
        assert_eq!(
            canonical
                .iter()
                .map(|(identity, _)| (identity.row_pk.clone(), identity.file_id()))
                .collect::<Vec<_>>(),
            [
                (RowPk::single("row-a"), Some("file-b")),
                (RowPk::single("row-z"), Some("file-a")),
            ]
        );

        let limited = canonicalize_hot_scan_rows(physical_rows(), Some(1))
            .expect("LIMIT should apply after HOT canonicalization");
        assert_eq!(limited.len(), 1);
        assert_eq!(limited[0].0.row_pk, RowPk::single("row-a"));
        assert_eq!(limited[0].0.file_id(), Some("file-b"));
    }

    #[test]
    fn hot_scan_rejects_conflicting_duplicate_authority() {
        let generation = CommitId::for_test_label("conflicting-hot-authority");
        let error = canonicalize_hot_scan_rows(
            vec![
                adversarial_hot_scan_entry(generation, "row", "file", b"older"),
                adversarial_hot_scan_entry(generation, "row", "file", b"newer"),
            ],
            None,
        )
        .expect_err("one HOT identity cannot have two authoritative byte values");

        assert!(
            error
                .message
                .contains("duplicate HOT authority for schema 'schema'"),
            "unexpected duplicate-authority error: {error:?}"
        );
    }

    #[test]
    fn hot_scan_admission_is_bounded_by_retained_bytes_not_row_count() {
        const TINY_ROW_COUNT: usize = 5_000;
        const BUDGET: usize = 4 * 1024 * 1024;
        let generation = CommitId::for_test_label("hot-scan-byte-budget");
        let scope = hot_scope_prefix("branch", generation);
        let tiny_rows = (0..TINY_ROW_COUNT)
            .map(|index| {
                let row_pk = RowPk::single(format!("row-{index:05}"));
                let key = Bytes::from(encode_hot_row_key_parts(
                    "branch", generation, "schema", &row_pk, None,
                ));
                let identity = decode_hot_scan_row_key_in_scope(key, &scope)
                    .expect("decode tiny HOT scan identity");
                (identity, Bytes::from_static(b"{}"))
            })
            .collect::<Vec<_>>();
        assert!(
            hot_scan_entries_fit_budget(HotScanEntries::Decoded(tiny_rows), Some(BUDGET),)
                .is_some(),
            "thousands of narrows must not trip a cardinality policy"
        );

        let row_pk = RowPk::single("large");
        let key = Bytes::from(encode_hot_row_key_parts(
            "branch", generation, "schema", &row_pk, None,
        ));
        let identity =
            decode_hot_scan_row_key_in_scope(key, &scope).expect("decode large HOT scan identity");
        let wide_rows = vec![(identity, Bytes::from(vec![0_u8; BUDGET]))];
        assert!(
            hot_scan_entries_fit_budget(HotScanEntries::Decoded(wide_rows), Some(BUDGET),)
                .is_none(),
            "retained payload bytes must govern fallback even for one row"
        );
    }

    #[test]
    fn hot_diff_keys_append_into_one_exact_arena() {
        let checkpoint = CommitId::for_test_label("shared-hot-diff-checkpoint");
        let generation = CommitId::for_test_label("shared-hot-diff-generation");
        let scope = encode_working_diff_scope_prefix("branch", checkpoint, generation);
        let identities = [
            HeadRowIdentity {
                schema_key: "schema".to_string(),
                row_pk: RowPk::single("first"),
                file_id: None,
            },
            HeadRowIdentity {
                schema_key: "schema\0escaped".to_string(),
                row_pk: RowPk::single("second-escaped"),
                file_id: Some("file\0id".to_string()),
            },
        ];
        let capacity = identities
            .iter()
            .try_fold(0_usize, |total, identity| {
                total.checked_add(encoded_hot_identity_key_len(
                    scope.len(),
                    &identity.schema_key,
                    &identity.row_pk,
                    identity.file_id.as_deref(),
                )?)
            })
            .expect("test identities have a representable encoded size");
        let mut key_bytes = Vec::with_capacity(capacity);
        let allocation = key_bytes.as_ptr();
        let ranges = identities
            .iter()
            .map(|identity| {
                append_hot_diff_key_parts(
                    &mut key_bytes,
                    &scope,
                    &identity.schema_key,
                    &identity.row_pk,
                    identity.file_id.as_deref(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(key_bytes.len(), capacity);
        assert_eq!(key_bytes.capacity(), capacity);
        assert_eq!(key_bytes.as_ptr(), allocation);
        assert_eq!(ranges[0].end, ranges[1].start);
        for (range, expected) in ranges.into_iter().zip(identities) {
            let (decoded_checkpoint, decoded_identity) =
                decode_hot_diff_key(&key_bytes[range]).expect("decode appended hot-diff key");
            assert_eq!(decoded_checkpoint, checkpoint);
            assert_eq!(decoded_identity.branch_id, "branch");
            assert_eq!(decoded_identity.generation, generation);
            assert_eq!(decoded_identity.schema_key, expected.schema_key);
            assert_eq!(decoded_identity.row_pk, expected.row_pk);
            assert_eq!(decoded_identity.file_id, expected.file_id);
        }
    }

    #[test]
    fn columnar_base_coordinate_survives_repeated_hot_updates_and_tombstones() {
        let row_pk = RowPk::single("coordinated-row");
        let coordinate = ColumnarBaseCoordinate {
            base_commit_id: CommitId::for_test_label("coordinate-base"),
            group_index: 7,
            row_index: 31,
        };
        let previous_typed =
            WasmTypedRow::from_test_json_unchecked(&row_pk, &serde_json::json!({}))
                .expect("coordinated predecessor should type");
        let updated_typed =
            WasmTypedRow::from_test_json_unchecked(&row_pk, &serde_json::json!({"updated": true}))
                .expect("coordinated update should type");
        let previous_snapshot = previous_typed.durable_payload().expect("typed payload");
        let updated_snapshot = updated_typed.durable_payload().expect("typed payload");
        let previous = HeadValueRef {
            change_id: Some(ChangeId::for_test_label("coordinate-before-change")),
            commit_id: Some(CommitId::for_test_label("coordinate-before-commit")),
            untracked: false,
            deleted: false,
            created_at: timestamp(),
            updated_at: timestamp(),
            snapshot: Some(previous_snapshot.as_ref()),
            metadata: None,
            columnar_base_coordinate: Some(coordinate),
            working_diff_baseline: WorkingDiffBaseline::Disabled,
        };
        let mut predecessor = CertifiedCurrentStatePredecessor::Encoded(Bytes::from(
            encode_head_value(&previous).expect("encode coordinated predecessor"),
        ));
        for deleted in [false, true] {
            let delta = CurrentStateDeltaRef {
                schema_key: "schema",
                file_id: None,
                row_pk: &row_pk,
                change_id: Some(ChangeId::for_test_label(if deleted {
                    "coordinate-delete-change"
                } else {
                    "coordinate-update-change"
                })),
                commit_id: Some(CommitId::for_test_label(if deleted {
                    "coordinate-delete-commit"
                } else {
                    "coordinate-update-commit"
                })),
                untracked: false,
                deleted,
                created_at: timestamp(),
                updated_at: timestamp(),
                snapshot: (!deleted).then_some(updated_snapshot.as_ref()),
                metadata: None,
                columnar_base_coordinate: None,
            };
            let inherited = next_columnar_base_coordinate(false, &delta, Some(&predecessor))
                .expect("inherit coordinate");
            assert_eq!(inherited, Some(coordinate));
            assert_eq!(
                next_columnar_base_coordinate(true, &delta, Some(&predecessor))
                    .expect("clear coordinate for new base"),
                None
            );
            let mut next = delta.value_ref(timestamp(), WorkingDiffBaseline::Disabled);
            next.columnar_base_coordinate = inherited;
            predecessor = CertifiedCurrentStatePredecessor::Encoded(Bytes::from(
                encode_head_value(&next).expect("encode repeated coordinated mutation"),
            ));
        }
        assert!(predecessor.view().expect("decode tombstone").deleted);
        assert_eq!(
            predecessor
                .view()
                .expect("decode tombstone coordinate")
                .columnar_base_coordinate,
            Some(coordinate)
        );
    }

    #[tokio::test]
    async fn hot_diff_segments_preserve_identity_coverage_with_bounded_puts() {
        const IDENTITY_COUNT: usize = 10_000;

        let checkpoint = CommitId::for_test_label("segmented-hot-diff-checkpoint");
        let generation = CommitId::for_test_label("segmented-hot-diff-generation");
        let scope = encode_working_diff_scope_prefix("branch", checkpoint, generation);
        let mut identity_key_bytes = Vec::new();
        let mut identity_puts = Vec::with_capacity(IDENTITY_COUNT);
        let mut expected_coverage = WorkingDiffIndexCoverage::default();
        for index in 0..IDENTITY_COUNT {
            let row_pk = RowPk::single(format!("row-{index:05}"));
            let key = append_hot_diff_key_parts(
                &mut identity_key_bytes,
                &scope,
                "schema",
                &row_pk,
                Some("file.md"),
            );
            expected_coverage
                .add_encoded_group_key(&identity_key_bytes[key.clone()])
                .expect("test coverage count fits u64");
            identity_puts.push(EncodedPut {
                key: buffer_range(&key),
                value: BufferRange::default(),
            });
        }

        let mut writes = StorageWriteSet::new();
        stage_hot_diff_batch(&mut writes, &scope, identity_key_bytes, identity_puts)
            .expect("stage segmented hot diff");
        assert!(
            writes.stats().staged_puts <= 3,
            "ten thousand short identities should require at most three bounded segments"
        );

        let storage = StorageAdapter::new(Memory::new());
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit segmented hot diff");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open segmented hot diff read");
        let range = StoragePrefix {
            bytes: Bytes::from(scope.clone()),
        }
        .to_range()
        .expect("valid prefix range");
        let mut cursor = read
            .begin_scan(DIFF_SPACE, range, StorageBeginScanOptions::default())
            .await
            .expect("begin segmented hot diff scan");
        let (page, page_has_more) = cursor
            .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
            .await
            .expect("scan segmented hot diff")
            .into_parts();
        assert!(!page_has_more);

        let mut actual_coverage = WorkingDiffIndexCoverage::default();
        let mut decoded = 0_usize;
        for entry in page {
            let bytes = full_value_bytes(entry.value).expect("full segment value");
            let segment_scope =
                decode_hot_diff_segment_key(entry.key.0.as_ref()).expect("segment key");
            assert_eq!(segment_scope.digest, *blake3::hash(&bytes).as_bytes());
            visit_hot_diff_segment(&bytes, &scope, &mut actual_coverage, |_| decoded += 1)
                .expect("decode hot diff segment");
        }
        assert_eq!(decoded, IDENTITY_COUNT);
        assert_eq!(actual_coverage, expected_coverage);
    }

    #[test]
    fn hot_mutation_keys_append_into_one_exact_arena() {
        let generation = CommitId::for_test_label("shared-hot-mutation-generation");
        let scope = hot_scope_prefix("branch", generation);
        let first_pk = RowPk::single("first-row");
        let second_pk = RowPk::single("second");
        let first_snapshot =
            native_snapshot_payload(&first_pk, serde_json::json!({"value": "first"}));
        let second_snapshot =
            native_snapshot_payload(&second_pk, serde_json::json!({"value": "second"}));
        let first = CurrentStateDeltaRef {
            schema_key: "schema\0escaped",
            file_id: Some("file\0id"),
            row_pk: &first_pk,
            change_id: Some(ChangeId::for_test_label("hot-mutation-first")),
            commit_id: None,
            untracked: true,
            deleted: false,
            created_at: timestamp(),
            updated_at: timestamp(),
            snapshot: Some(&first_snapshot),
            metadata: None,
            columnar_base_coordinate: None,
        };
        let second = CurrentStateDeltaRef {
            schema_key: "schema_without_file",
            file_id: None,
            row_pk: &second_pk,
            change_id: Some(ChangeId::for_test_label("hot-mutation-second")),
            commit_id: None,
            untracked: true,
            deleted: false,
            created_at: timestamp(),
            updated_at: timestamp(),
            snapshot: Some(&second_snapshot),
            metadata: None,
            columnar_base_coordinate: None,
        };
        let deltas = [&first, &second];
        let capacity = encoded_hot_mutation_identity_capacity(scope.len(), &deltas)
            .expect("test identities have a representable encoded size");
        let mut key_bytes = Vec::with_capacity(capacity);
        let allocation = key_bytes.as_ptr();
        let ranges = deltas
            .iter()
            .map(|delta| append_hot_mutation_identity(&mut key_bytes, &scope, delta))
            .collect::<Vec<_>>();

        assert_eq!(key_bytes.len(), capacity);
        assert_eq!(key_bytes.capacity(), capacity);
        assert_eq!(key_bytes.as_ptr(), allocation);
        assert_eq!(ranges[0].row_key.offset(), 0);
        assert_eq!(
            ranges[0]
                .file_schema_key
                .expect("first identity has a file schema marker")
                .offset()
                + ranges[0]
                    .file_schema_key
                    .expect("first identity has a file schema marker")
                    .len(),
            ranges[1].row_key.offset()
        );
        for (range, delta) in ranges.iter().zip(deltas) {
            let row_start = range.row_key.offset();
            let row = decode_hot_row_key_in_scope(
                &key_bytes[row_start..row_start + range.row_key.len()],
                &scope,
            )
            .expect("decode shared row key");
            assert_eq!(row.schema_key, delta.schema_key);
            assert_eq!(row.row_pk, *delta.row_pk);
            assert_eq!(row.file_id.as_deref(), delta.file_id);

            let scan_row = decode_hot_scan_row_key_in_scope(
                Bytes::copy_from_slice(&key_bytes[row_start..row_start + range.row_key.len()]),
                &scope,
            )
            .expect("decode shared row key for direct scan");
            assert_eq!(scan_row.schema_key(), delta.schema_key);
            assert_eq!(scan_row.row_pk, *delta.row_pk);
            assert_eq!(scan_row.file_id(), delta.file_id);
            assert_eq!(
                scan_row.owned_metadata_buffer_count(),
                usize::from(delta.schema_key.contains('\0'))
                    + usize::from(delta.file_id.is_some_and(|file_id| file_id.contains('\0'))),
                "only escaped metadata should take an owned fallback"
            );

            if let Some(marker) = range.file_schema_key {
                let marker_start = marker.offset();
                assert_eq!(
                    &key_bytes[marker_start..marker_start + marker.len()],
                    encode_hot_file_schema_key(&scope, delta.schema_key)
                );
            }
        }

        let encoded = encode_hot_mutation_identities("branch", generation, &deltas);
        assert_eq!(encoded.key_bytes.as_ref(), key_bytes);
        assert_eq!(encoded.key_ranges.len(), ranges.len());
        for (encoded, expected) in encoded.key_ranges.iter().zip(ranges) {
            assert_eq!(encoded.row_key, expected.row_key);
            assert_eq!(encoded.file_schema_key, expected.file_schema_key);
        }
    }

    #[test]
    fn hot_next_values_append_into_one_planned_arena() {
        let tracked_pk = RowPk::single("tracked");
        let tombstone_pk = RowPk::single("tombstone");
        let untracked_pk = RowPk::single("untracked");
        let removed_pk = RowPk::single("removed");
        let tracked_typed = WasmTypedRow::from_test_json_unchecked(
            &tracked_pk,
            &serde_json::json!({"tracked": true}),
        )
        .expect("tracked fixture should type");
        let untracked_typed = WasmTypedRow::from_test_json_unchecked(
            &untracked_pk,
            &serde_json::json!({"large": "snapshot"}),
        )
        .expect("untracked fixture should type");
        let tracked_snapshot = tracked_typed.durable_payload().expect("typed payload");
        let untracked_snapshot = untracked_typed.durable_payload().expect("typed payload");
        let tracked_metadata = lix_schema::Jsonb::from_value(serde_json::json!({"source": "test"}));
        let tracked = CurrentStateDeltaRef {
            schema_key: "tracked_schema",
            file_id: Some("tracked.json"),
            row_pk: &tracked_pk,
            change_id: Some(ChangeId::for_test_label("planned-value-change")),
            commit_id: Some(CommitId::for_test_label("planned-value-commit")),
            untracked: false,
            deleted: false,
            created_at: timestamp(),
            updated_at: timestamp(),
            snapshot: Some(tracked_snapshot.as_ref()),
            metadata: Some(&tracked_metadata),
            columnar_base_coordinate: None,
        };
        let tombstone = CurrentStateDeltaRef {
            schema_key: "tracked_schema",
            file_id: None,
            row_pk: &tombstone_pk,
            change_id: Some(ChangeId::for_test_label("planned-tombstone-change")),
            commit_id: Some(CommitId::for_test_label("planned-tombstone-commit")),
            untracked: false,
            deleted: true,
            created_at: timestamp(),
            updated_at: timestamp(),
            snapshot: None,
            metadata: None,
            columnar_base_coordinate: None,
        };
        let untracked = CurrentStateDeltaRef {
            schema_key: "untracked_schema",
            file_id: Some("untracked.json"),
            row_pk: &untracked_pk,
            change_id: Some(ChangeId::for_test_label("hot-untracked-member")),
            commit_id: None,
            untracked: true,
            deleted: false,
            created_at: timestamp(),
            updated_at: timestamp(),
            snapshot: Some(untracked_snapshot.as_ref()),
            metadata: None,
            columnar_base_coordinate: None,
        };
        let removed = CurrentStateDeltaRef {
            schema_key: "untracked_schema",
            file_id: Some("removed.json"),
            row_pk: &removed_pk,
            change_id: Some(ChangeId::for_test_label("hot-untracked-removed")),
            commit_id: None,
            untracked: true,
            deleted: true,
            created_at: timestamp(),
            updated_at: timestamp(),
            snapshot: None,
            metadata: None,
            columnar_base_coordinate: None,
        };
        let deltas = [&tracked, &tombstone, &untracked, &removed];

        let ordinary_capacity = deltas
            .iter()
            .try_fold(0_usize, |total, delta| {
                checked_add_hot_next_value_capacity(total, delta, false, false)
            })
            .expect("ordinary test values have a representable encoded size");
        let mut ordinary = Vec::with_capacity(ordinary_capacity);
        let ordinary_allocation = ordinary.as_ptr();
        let mut ordinary_expected = Vec::new();
        let mut ordinary_ranges = Vec::new();
        for delta in deltas {
            if delta.physically_deletes() {
                continue;
            }
            let value = delta.value_ref(delta.created_at, WorkingDiffBaseline::Disabled);
            ordinary_expected.extend_from_slice(
                &encode_head_value(&value).expect("encode ordinary expected value"),
            );
            ordinary_ranges.push(
                append_head_value(&mut ordinary, &value).expect("append ordinary planned value"),
            );
        }
        assert_eq!(ordinary, ordinary_expected);
        assert_eq!(ordinary.len(), ordinary_capacity);
        assert_eq!(ordinary.capacity(), ordinary_capacity);
        assert_eq!(ordinary.as_ptr(), ordinary_allocation);
        for range in ordinary_ranges {
            assert_eq!(
                decode_head_value(&ordinary[range])
                    .expect("decode ordinary planned value")
                    .working_diff_baseline,
                WorkingDiffBaseline::Disabled
            );
        }

        let before = WorkingDiffVersion {
            change_id: ChangeId::for_test_label("planned-before-change"),
            commit_id: CommitId::for_test_label("planned-before-commit"),
            deleted: false,
            created_at: timestamp(),
            updated_at: timestamp(),
            snapshot: WorkingDiffSlotFingerprint {
                kind: WORKING_DIFF_SLOT_NONE,
                hash: [0; JSON_REF_BYTES],
            },
            metadata: WorkingDiffSlotFingerprint {
                kind: WORKING_DIFF_SLOT_NONE,
                hash: [0; JSON_REF_BYTES],
            },
        };
        let checkpoint_capacity = [&tracked, &tombstone, &untracked, &removed]
            .iter()
            .try_fold(0_usize, |total, delta| {
                checked_add_hot_next_value_capacity(total, delta, true, false)
            })
            .expect("checkpoint test values have a representable encoded size");
        let checkpoint_baselines = [
            WorkingDiffBaseline::BeforePresent {
                checkpoint_commit_id: CommitId::for_test_label("checkpoint"),
                version: before,
            },
            WorkingDiffBaseline::BeforePresent {
                checkpoint_commit_id: CommitId::for_test_label("checkpoint"),
                version: before,
            },
            WorkingDiffBaseline::Disabled,
            WorkingDiffBaseline::Disabled,
        ];
        let mut checkpoint = Vec::with_capacity(checkpoint_capacity);
        let checkpoint_allocation = checkpoint.as_ptr();
        let mut checkpoint_expected = Vec::new();
        for (delta, baseline) in [&tracked, &tombstone, &untracked, &removed]
            .into_iter()
            .zip(checkpoint_baselines)
        {
            if delta.physically_deletes() {
                continue;
            }
            let value = delta.value_ref(delta.created_at, baseline);
            checkpoint_expected.extend_from_slice(
                &encode_head_value(&value).expect("encode checkpoint expected value"),
            );
            append_head_value(&mut checkpoint, &value).expect("append checkpoint planned value");
        }
        assert_eq!(checkpoint, checkpoint_expected);
        assert_eq!(checkpoint.len(), checkpoint_capacity);
        assert_eq!(checkpoint.capacity(), checkpoint_capacity);
        assert_eq!(checkpoint.as_ptr(), checkpoint_allocation);

        let before_absent = tracked.value_ref(
            tracked.created_at,
            WorkingDiffBaseline::BeforeAbsent {
                checkpoint_commit_id: CommitId::for_test_label("checkpoint"),
            },
        );
        let before_absent_bytes =
            encode_head_value(&before_absent).expect("encode before-absent checkpoint value");
        let tracked_checkpoint_capacity =
            checked_add_hot_next_value_capacity(0, &tracked, true, false)
                .expect("tracked checkpoint value has a representable size");
        assert_eq!(
            tracked_checkpoint_capacity,
            before_absent_bytes.len() + WORKING_DIFF_VERSION_BYTES,
            "new checkpoint rows use the same safe fixed-size upper bound"
        );

        assert!(
            checked_add_hot_next_value_capacity(usize::MAX, &tracked, false, false).is_none(),
            "overflow must select the caller's zero-capacity fallback"
        );
        assert_eq!(
            checked_add_hot_next_value_capacity(usize::MAX, &tracked, false, false).unwrap_or(0),
            0
        );
    }

    #[test]
    fn hot_tracked_snapshot_clones_share_encoded_row_values() {
        let decoded_snapshot = Arc::new(WasmTypedRow {
            schema_fingerprint: [9; 32],
            row_pk: vec![lix_schema::Value::Text("row".to_owned())].into(),
            row: lix_schema::Row::from([(
                "id".to_owned(),
                lix_schema::Value::Text("row".to_owned()),
            )]),
            native_payload: std::sync::OnceLock::new(),
            boundary_create_validation: std::sync::OnceLock::new(),
        });
        let snapshot =
            HotTrackedSnapshot::from_materialized_rows(vec![MaterializedTrackedStateRow {
                row_pk: RowPk::single("row"),
                schema_key: "schema".to_string(),
                file_id: None,
                snapshot_content: None,
                decoded_snapshot: Some(Arc::clone(&decoded_snapshot)),
                metadata: None,
                deleted: false,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                updated_at: "2026-01-01T00:00:00Z".to_string(),
                change_id: ChangeId::for_test_label("hot-shared-value-change"),
                commit_id: CommitId::for_test_label("hot-shared-value-commit"),
            }])
            .expect("encode tracked snapshot");
        let cloned = snapshot.clone();
        let source = snapshot.rows.values().next().expect("source encoded row");
        let retained = cloned.rows.values().next().expect("cloned encoded row");

        assert_eq!(source.as_ptr(), retained.as_ptr());
        assert_eq!(source.len(), retained.len());
        let decoded = decode_head_value(source).expect("decode typed tracked snapshot");
        assert_eq!(
            decoded.snapshot.expect("tracked snapshot retains payload")[0],
            crate::plugin::wire::typed::STORAGE_ROW_PAYLOAD_VERSION,
            "HOT lifecycle persistence must omit the envelope-owned primary key"
        );
        assert_eq!(
            WasmTypedRow::decode_durable_payload(
                Arc::from(
                    decoded
                        .snapshot
                        .expect("tracked snapshot retained typed payload"),
                ),
                "schema",
                &RowPk::single("row"),
            )
            .expect("decode native typed payload against its HOT envelope"),
            *decoded_snapshot
        );
    }

    #[test]
    fn hot_batch_staging_retains_encoded_key_and_value_arenas() {
        let key_bytes = Bytes::from_static(b"row-keymarker");
        let value_bytes = Bytes::from_static(b"value");
        let identities = EncodedHotMutationIdentities {
            key_bytes: key_bytes.clone(),
            key_ranges: vec![EncodedHotMutationIdentityRanges {
                row_key: BufferRange::new(0, 7),
                file_schema_key: Some(BufferRange::new(7, 6)),
            }],
        };
        let mut writes = StorageWriteSet::new();

        stage_hot_mutation_batch(&mut writes, identities, value_bytes, vec![Some(0..5)]);

        let stats = writes.arena_stats();
        assert_eq!(stats.spaces, 2);
        assert_eq!(stats.put_descriptors, 2);
        assert_eq!(stats.key_inline_bytes, 0);
        assert_eq!(stats.value_inline_bytes, 0);
        assert_eq!(stats.key_shared_buffers, 2);
        assert_eq!(stats.value_shared_buffers, 2);
    }

    #[test]
    fn ten_thousand_file_cascade_mutations_reserve_shared_buffers_once() {
        const ROW_COUNT: usize = 10_000;

        let tombstone = HeadValueRef {
            change_id: Some(ChangeId::for_test_label("cascade-reserve-change")),
            commit_id: Some(CommitId::for_test_label("cascade-reserve-commit")),
            untracked: false,
            deleted: true,
            created_at: timestamp(),
            updated_at: timestamp(),
            snapshot: None,
            metadata: None,
            columnar_base_coordinate: None,
            working_diff_baseline: WorkingDiffBaseline::BeforePresent {
                checkpoint_commit_id: CommitId::for_test_label("cascade-reserve-checkpoint"),
                version: working_diff_version("cascade-reserve-before"),
            },
        };
        let encoded_tombstone =
            encode_head_value(&tombstone).expect("encode maximum cascade tombstone");
        assert_eq!(
            encoded_tombstone.len(),
            HEAD_VALUE_TYPED_HEADER_BYTES
                + WORKING_DIFF_CHECKPOINT_BYTES
                + WORKING_DIFF_VERSION_BYTES,
            "the cascade value reservation must cover the largest checkpoint tombstone"
        );

        let mut buffers = HotCascadeMutationBuffers::with_capacity(ROW_COUNT, 0, true);
        let value_allocation = buffers.value_bytes.as_ptr();
        let row_put_allocation = buffers.row_puts.as_ptr();
        let row_delete_allocation = buffers.row_deletes.as_ptr();
        let descriptor = EncodedPut {
            key: BufferRange::default(),
            value: BufferRange::default(),
        };
        for _ in 0..ROW_COUNT {
            append_head_value(&mut buffers.value_bytes, &tombstone)
                .expect("append planned cascade tombstone");
            buffers.row_puts.push(descriptor);
            buffers.row_deletes.push(BufferRange::default());
        }

        assert_eq!(
            buffers.value_bytes.len(),
            ROW_COUNT
                * (HEAD_VALUE_TYPED_HEADER_BYTES
                    + WORKING_DIFF_CHECKPOINT_BYTES
                    + WORKING_DIFF_VERSION_BYTES)
        );
        assert_eq!(buffers.value_bytes.as_ptr(), value_allocation);
        assert_eq!(buffers.row_puts.as_ptr(), row_put_allocation);
        assert_eq!(buffers.row_deletes.as_ptr(), row_delete_allocation);
        assert!(buffers.row_puts.capacity() >= ROW_COUNT);
        assert!(buffers.row_deletes.capacity() >= ROW_COUNT);
    }

    /// The budget is the whole guard, so its shape is pinned here rather than
    /// inferred from a timing test. Every constant is justified in
    /// [`hot_index_candidate_budget`]'s own documentation against the
    /// measurement that produced it.
    #[test]
    fn the_candidate_budget_floors_small_planes_and_halves_large_ones() {
        // Below the floor the absolute cost of resolving a bucket is smaller
        // than the measurement's noise floor, so tiny planes keep the index.
        assert_eq!(hot_index_candidate_budget(0), 64);
        assert_eq!(hot_index_candidate_budget(1), 64);
        assert_eq!(hot_index_candidate_budget(128), 64);
        // Above it the budget is half the plane, which sits below the measured
        // crossover of roughly two thirds.
        assert_eq!(hot_index_candidate_budget(129), 64);
        assert_eq!(hot_index_candidate_budget(200), 100);
        assert_eq!(hot_index_candidate_budget(20_000), 10_000);
        // The guard must not overflow into a permissive budget on a plane
        // whose count is nonsense.
        assert_eq!(
            hot_index_candidate_budget(u64::MAX),
            (u64::MAX / 2) as usize
        );
    }

    /// Every arm of the measurement that motivated the guard, classified by the
    /// budget. The aged, moved and large write-path buckets were all slower
    /// than the collection scan and must be refused; the fresh and small
    /// write-path buckets were faster and must be kept.
    #[test]
    fn the_budget_refuses_exactly_the_buckets_that_lost_to_the_scan() {
        for (entries_published, bucket, expected_served) in [
            // arm            plane    bucket  index route beat the scan?
            /* fresh      */
            (1_u64, 1_usize, true),
            /* write 10   */ (11, 10, true),
            /* write 100  */ (101, 100, false),
            /* write 1000 */ (1_001, 1_000, false),
            /* write 5000 */ (5_001, 5_000, false),
            /* aged 100   */ (100, 100, false),
            /* aged 1000  */ (1_000, 1_000, false),
            /* aged 10000 */ (10_000, 10_000, false),
            /* moved 100  */ (199, 100, false),
            /* moved 1000 */ (1_999, 1_000, false),
            /* moved 10k  */ (19_999, 10_000, false),
        ] {
            let served = bucket <= hot_index_candidate_budget(entries_published);
            assert_eq!(
                served, expected_served,
                "plane of {entries_published} entries, bucket of {bucket}",
            );
        }
    }

    #[test]
    fn a_witness_round_trips_its_published_count() {
        assert_eq!(
            decode_hot_index_witness(&encode_hot_index_witness(0)),
            Some(0)
        );
        assert_eq!(
            decode_hot_index_witness(&encode_hot_index_witness(7_919)),
            Some(7_919)
        );
        // An unreadable witness cannot size the plane, and the lookup refuses
        // the index rather than guessing a budget.
        assert_eq!(decode_hot_index_witness(&[]), None);
        assert_eq!(decode_hot_index_witness(&[0, 1, 2]), None);
    }

    #[tokio::test]
    async fn ordinary_incremental_import_skips_file_cascade_identity_index() {
        const DELTAS: usize = 4096;

        let storage = StorageAdapter::new(Memory::new());
        let read = crate::storage_adapter::SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("open ordinary incremental import read"),
        );
        let row_pk = RowPk::single("ordinary");
        let timestamp = timestamp();
        let snapshot = native_snapshot_payload(&row_pk, serde_json::json!({"value": "ordinary"}));
        let delta = CurrentStateDeltaRef {
            schema_key: "ordinary_schema",
            file_id: Some("ordinary.json"),
            row_pk: &row_pk,
            change_id: Some(ChangeId::for_test_label("hot-ordinary-incremental")),
            commit_id: None,
            untracked: true,
            deleted: false,
            created_at: timestamp,
            updated_at: timestamp,
            snapshot: Some(&snapshot),
            metadata: None,
            columnar_base_coordinate: None,
        };
        let deltas = vec![&delta; DELTAS];
        let generation = CommitId::for_test_label("ordinary-import-generation");
        let mut writes = StorageWriteSet::new();
        let mut coverage = WorkingDiffIndexCoverage::default();
        let explicit_index_builds = incremental_cascade_explicit_index_builds();

        stage_incremental_file_delete_cascades(
            &read,
            &mut writes,
            "ordinary-import",
            generation,
            &deltas,
            None,
            false,
            &mut coverage,
        )
        .await
        .expect("ordinary imports do not need file-delete cascade staging");

        assert_eq!(
            incremental_cascade_explicit_index_builds(),
            explicit_index_builds,
            "ordinary imports must return before allocating the batch-sized explicit identity index"
        );
        assert!(writes.is_empty());
    }

    #[tokio::test]
    async fn dense_mutation_identity_range_scan_matches_point_reads() {
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label("dense-mutation-generation");
        let row_pks = (0..HOT_DENSE_SCAN_MIN_IDENTITIES)
            .map(|index| RowPk::single(format!("{index:04}")))
            .collect::<Vec<_>>();
        let timestamp = timestamp();
        let snapshots = row_pks
            .iter()
            .map(|row_pk| native_snapshot_payload(row_pk, serde_json::json!({"value": "dense"})))
            .collect::<Vec<_>>();
        let deltas = row_pks
            .iter()
            .zip(&snapshots)
            .map(|(row_pk, snapshot)| CurrentStateDeltaRef {
                schema_key: "schema",
                file_id: None,
                row_pk,
                change_id: Some(ChangeId::for_test_label("hot-planned-arena")),
                commit_id: None,
                untracked: true,
                deleted: false,
                created_at: timestamp,
                updated_at: timestamp,
                snapshot: Some(snapshot),
                metadata: None,
                columnar_base_coordinate: None,
            })
            .collect::<Vec<_>>();
        let delta_refs = deltas.iter().collect::<Vec<_>>();
        let encoded = encode_hot_mutation_identities("branch", generation, &delta_refs);
        let keys = encoded
            .key_ranges
            .iter()
            .map(|ranges| {
                let start = ranges.row_key.offset();
                StorageKey(
                    encoded
                        .key_bytes
                        .slice(start..start.saturating_add(ranges.row_key.len())),
                )
            })
            .collect::<Vec<_>>();
        let mut writes = StorageWriteSet::new();
        for key in &keys {
            writes.put(
                ROW_SPACE,
                key.clone(),
                StorageValue {
                    bytes: Bytes::from_static(b"row"),
                },
            );
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit dense mutation fixture");
        let read = crate::storage_adapter::SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("open dense mutation read"),
        );

        let dense = hot_scan_dense_mutation_identity_range(&read, &encoded)
            .await
            .expect("scan dense mutation range")
            .expect("dense mutation range should stay on the scan path");
        let point = PointReadPlan::new(ROW_SPACE, &keys)
            .materialize(&read, StorageGetOptions::default())
            .await
            .expect("point-read dense mutation identities")
            .value
            .into_iter()
            .map(|value| value.map(full_value_bytes).transpose())
            .collect::<Result<Vec<_>, _>>()
            .expect("decode dense mutation point reads");

        assert_eq!(dense, point);
        assert_eq!(
            dense.iter().flatten().count(),
            HOT_DENSE_SCAN_MIN_IDENTITIES
        );
    }

    #[tokio::test]
    async fn dense_identity_range_scan_returns_requested_rows() {
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label("dense-range-generation");
        let all_identities = (0..HOT_DENSE_SCAN_MIN_IDENTITIES * 2)
            .map(|index| diff_identity("branch", generation, &format!("{index:04}")))
            .collect::<Vec<_>>();
        let requested = all_identities
            .iter()
            .step_by(2)
            .cloned()
            .collect::<Vec<_>>();
        let requested_batch = FiniteHotIdentityBatchRef::new(
            "branch",
            generation,
            "schema",
            requested.iter().map(|identity| &identity.row_pk).collect(),
            vec![None],
        )
        .expect("dense identity count is representable");
        let mut writes = StorageWriteSet::new();
        for identity in &all_identities {
            writes.put(
                ROW_SPACE,
                StorageKey(Bytes::from(encode_hot_row_key(identity))),
                StorageValue {
                    bytes: Bytes::from_static(b"row"),
                },
            );
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit dense-range fixture");
        let read = crate::storage_adapter::SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("open dense-range read"),
        );

        let dense = hot_scan_dense_identity_range(&read, &requested_batch)
            .await
            .expect("scan dense identity range")
            .expect("dense range should stay on the scan path");
        let point = hot_load_finite_identity_bytes(&read, &requested_batch)
            .await
            .expect("point-read the same dense identities");

        assert_eq!(dense, point);
        assert_eq!(dense.iter().flatten().count(), requested.len());
    }

    #[tokio::test]
    async fn sparse_identity_range_scan_returns_to_point_reads() {
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label("sparse-range-generation");
        let all_identities = (0..HOT_DENSE_SCAN_MIN_IDENTITIES * 4)
            .map(|index| diff_identity("branch", generation, &format!("{index:04}")))
            .collect::<Vec<_>>();
        let requested = all_identities
            .iter()
            .step_by(4)
            .cloned()
            .collect::<Vec<_>>();
        let requested_batch = FiniteHotIdentityBatchRef::new(
            "branch",
            generation,
            "schema",
            requested.iter().map(|identity| &identity.row_pk).collect(),
            vec![None],
        )
        .expect("sparse identity count is representable");
        let mut writes = StorageWriteSet::new();
        for identity in &all_identities {
            writes.put(
                ROW_SPACE,
                StorageKey(Bytes::from(encode_hot_row_key(identity))),
                StorageValue {
                    bytes: Bytes::from_static(b"row"),
                },
            );
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit sparse-range fixture");
        let read = crate::storage_adapter::SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("open sparse-range read"),
        );

        let rows = hot_scan_dense_identity_range(&read, &requested_batch)
            .await
            .expect("probe sparse identity range");

        assert!(
            rows.is_none(),
            "sparse ranges must return to the exact point-read path"
        );
        let point = hot_load_finite_identity_bytes(&read, &requested_batch)
            .await
            .expect("point-read sparse identities after dense fallback");
        assert_eq!(point.iter().flatten().count(), requested.len());
    }

    #[tokio::test]
    async fn working_diff_gc_keeps_only_control_bound_hot_records() {
        let storage = StorageAdapter::new(Memory::new());
        let active_generation = CommitId::for_test_label("active-generation");
        let active_checkpoint = CommitId::for_test_label("active-checkpoint");
        let stale_generation = CommitId::for_test_label("stale-generation");
        let stale_checkpoint = CommitId::for_test_label("stale-checkpoint");
        let orphan_generation = CommitId::for_test_label("orphan-generation");
        let orphan_checkpoint = CommitId::for_test_label("orphan-checkpoint");

        let active_identity = diff_identity("active", active_generation, "active-row");
        let stale_identity = diff_identity("stale", stale_generation, "stale-row");
        let orphan_identity = diff_identity("deleted", orphan_generation, "orphan-row");
        let (active_key, active_value) =
            single_hot_diff_segment(active_checkpoint, &active_identity);
        let (stale_key, stale_value) = single_hot_diff_segment(stale_checkpoint, &stale_identity);
        let (orphan_key, orphan_value) =
            single_hot_diff_segment(orphan_checkpoint, &orphan_identity);

        let mut writes = StorageWriteSet::new();
        stage_tracked_working_diff_epoch(
            &mut writes,
            "active",
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: active_checkpoint,
                generation: active_generation,
                coverage: WorkingDiffIndexCoverage::default(),
            },
        )
        .expect("stage active epoch");
        stage_branch_head_control(
            &mut writes,
            "active",
            BranchHeadControl {
                head_commit_id: active_generation,
                tracked_generation: active_generation,
                current_state_revision: 0,
                schema_presence_bloom: [u64::MAX; 4],
                working_diff_checkpoint_commit_id: Some(active_checkpoint),
                created_at: timestamp(),
                updated_at: timestamp(),
                ref_change_id: ChangeId::for_test_label("active-ref"),
            },
        )
        .expect("stage active control");

        stage_tracked_working_diff_epoch(
            &mut writes,
            "stale",
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: stale_checkpoint,
                generation: stale_generation,
                coverage: WorkingDiffIndexCoverage::default(),
            },
        )
        .expect("stage stale epoch");
        stage_branch_head_control(
            &mut writes,
            "stale",
            BranchHeadControl {
                head_commit_id: stale_generation,
                tracked_generation: stale_generation,
                current_state_revision: 0,
                schema_presence_bloom: [u64::MAX; 4],
                working_diff_checkpoint_commit_id: None,
                created_at: timestamp(),
                updated_at: timestamp(),
                ref_change_id: ChangeId::for_test_label("stale-ref"),
            },
        )
        .expect("stage stale control");

        stage_tracked_working_diff_epoch(
            &mut writes,
            "deleted",
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id: orphan_checkpoint,
                generation: orphan_generation,
                coverage: WorkingDiffIndexCoverage::default(),
            },
        )
        .expect("stage orphan epoch");

        for (key, value) in [
            (&active_key, active_value),
            (&stale_key, stale_value),
            (&orphan_key, orphan_value),
        ] {
            writes.put(
                DIFF_SPACE,
                StorageKey(Bytes::copy_from_slice(key)),
                StorageValue {
                    bytes: Bytes::from(value),
                },
            );
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit hot working-diff GC fixture");

        let read = crate::storage_adapter::SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("open hot working-diff GC read"),
        );
        let mut gc_writes = StorageWriteSet::new();
        stage_collect_stale_working_diff_indexes(&read, &mut gc_writes)
            .await
            .expect("stage hot working-diff GC");
        drop(read);
        storage
            .commit_write_set(gc_writes, StorageWriteOptions::default())
            .await
            .expect("commit hot working-diff GC");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open hot working-diff GC verification read");
        let active_epoch = PointReadPlan::new(
            TRACKED_WORKING_DIFF_MARKER_SPACE,
            &[StorageKey(Bytes::from(
                working_diff_marker_key("active").expect("active working-diff marker key"),
            ))],
        )
        .materialize(&read, StorageGetOptions::default())
        .await
        .expect("read active epoch")
        .value
        .into_iter()
        .next()
        .flatten();
        assert!(active_epoch.is_some(), "active epoch must survive GC");

        for (branch_id, key) in [("stale", stale_key), ("deleted", orphan_key)] {
            let epoch = PointReadPlan::new(
                TRACKED_WORKING_DIFF_MARKER_SPACE,
                &[StorageKey(Bytes::from(
                    working_diff_marker_key(branch_id).expect("working-diff marker key"),
                ))],
            )
            .materialize(&read, StorageGetOptions::default())
            .await
            .expect("read stale epoch")
            .value
            .into_iter()
            .next()
            .flatten();
            assert!(epoch.is_none(), "inactive epoch must be reclaimed");
            let record = PointReadPlan::new(DIFF_SPACE, &[StorageKey(Bytes::from(key))])
                .materialize(&read, StorageGetOptions::default())
                .await
                .expect("read stale hot record")
                .value
                .into_iter()
                .next()
                .flatten();
            assert!(record.is_none(), "inactive hot record must be reclaimed");
        }

        let active_record = PointReadPlan::new(DIFF_SPACE, &[StorageKey(Bytes::from(active_key))])
            .materialize(&read, StorageGetOptions::default())
            .await
            .expect("read active hot record")
            .value
            .into_iter()
            .next()
            .flatten();
        assert!(active_record.is_some(), "active hot record must survive GC");
    }

    /// Gate (b) of `hot_compaction_mask`, both verdicts, in one publication.
    ///
    /// A branch tombstone is what hides a same-identity global row from that
    /// branch — the contract `main_tombstone_hides_global_row` and its
    /// siblings pin — so compaction must keep it. A tombstone in a schema the
    /// global branch has no rows in shadows nothing and must go. Both are
    /// asserted here rather than only through a checkpoint's row count,
    /// because a wholly inert pass and a correctly conservative one produce
    /// the same count.
    #[tokio::test]
    async fn checkpoint_compaction_keeps_only_globally_shadowed_tombstones() {
        const BRANCH_ID: &str = "01920000-0000-7000-8000-0000000000e5";
        const BRANCH_LABEL: &str = "e45e-compaction-branch";
        const GLOBAL_LABEL: &str = "e45e-compaction-global";
        const SHADOWED_SCHEMA: &str = "e45e_shadowed_schema";
        const PRIVATE_SCHEMA: &str = "e45e_private_schema";

        let storage = StorageAdapter::new(Memory::new());
        let created_at = timestamp();
        let shadowed_pk = RowPk::single("shadowed-row");
        let private_pk = RowPk::single("private-row");
        let global_commit = CommitId::for_test_label(GLOBAL_LABEL);
        let generation = CommitId::for_test_label(BRANCH_LABEL);

        crate::test_support::seed_branch_head_with_rows(
            storage.clone(),
            crate::GLOBAL_BRANCH_ID,
            GLOBAL_LABEL,
            &[MaterializedTrackedStateRow {
                row_pk: shadowed_pk.clone(),
                schema_key: SHADOWED_SCHEMA.to_owned(),
                file_id: None,
                snapshot_content: Some(r#"{"key":"global-row"}"#.into()),
                decoded_snapshot: None,
                metadata: None,
                deleted: false,
                created_at: created_at.to_string(),
                updated_at: created_at.to_string(),
                change_id: ChangeId::for_test_label("e45e-global-change"),
                commit_id: global_commit,
            }],
        )
        .await;
        crate::test_support::seed_branch_head_with_rows(
            storage.clone(),
            BRANCH_ID,
            BRANCH_LABEL,
            &[
                MaterializedTrackedStateRow {
                    row_pk: private_pk.clone(),
                    schema_key: PRIVATE_SCHEMA.to_owned(),
                    file_id: None,
                    snapshot_content: Some(r#"{"key":"private-row"}"#.into()),
                    decoded_snapshot: None,
                    metadata: None,
                    deleted: false,
                    created_at: created_at.to_string(),
                    updated_at: created_at.to_string(),
                    change_id: ChangeId::for_test_label("e45e-branch-private"),
                    commit_id: generation,
                },
                MaterializedTrackedStateRow {
                    row_pk: shadowed_pk.clone(),
                    schema_key: SHADOWED_SCHEMA.to_owned(),
                    file_id: None,
                    snapshot_content: Some(r#"{"key":"branch-row"}"#.into()),
                    decoded_snapshot: None,
                    metadata: None,
                    deleted: false,
                    created_at: created_at.to_string(),
                    updated_at: created_at.to_string(),
                    change_id: ChangeId::for_test_label("e45e-branch-shadowed"),
                    commit_id: generation,
                },
            ],
        )
        .await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open compaction gate read");
        let checkpoint_head = CommitId::for_test_label("e45e-compaction-checkpoint");
        let deltas = [
            CurrentStateDeltaRef {
                schema_key: PRIVATE_SCHEMA,
                file_id: None,
                row_pk: &private_pk,
                change_id: Some(ChangeId::for_test_label("e45e-delete-private")),
                commit_id: Some(checkpoint_head),
                untracked: false,
                deleted: true,
                created_at,
                updated_at: created_at,
                snapshot: None,
                metadata: None,
                columnar_base_coordinate: None,
            },
            CurrentStateDeltaRef {
                schema_key: SHADOWED_SCHEMA,
                file_id: None,
                row_pk: &shadowed_pk,
                change_id: Some(ChangeId::for_test_label("e45e-delete-shadowed")),
                commit_id: Some(checkpoint_head),
                untracked: false,
                deleted: true,
                created_at,
                updated_at: created_at,
                snapshot: None,
                metadata: None,
                columnar_base_coordinate: None,
            },
        ];
        // This transaction publishes nothing on global, so the only global
        // authority is what the seed already made durable.
        let staged_on_global = BTreeSet::new();
        let mut checkpoint_writes = StorageWriteSet::new();
        let mut coverage = WorkingDiffIndexCoverage::default();
        HotStateWriter {
            store: &read,
            writes: &mut checkpoint_writes,
            transaction_global_schema_keys: Some(&staged_on_global),
        }
        .stage_checkpoint_current_state(
            BRANCH_ID,
            generation,
            checkpoint_head,
            &deltas,
            &BTreeSet::new(),
            checkpoint_head,
            &mut coverage,
        )
        .await
        .expect("checkpoint should stage");
        drop(read);
        storage
            .commit_write_set(checkpoint_writes, StorageWriteOptions::default())
            .await
            .expect("commit compaction checkpoint");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("verify compaction gate");
        let key = |schema_key: &str, row_pk: &RowPk| {
            StorageKey(Bytes::from(encode_hot_row_key(&HeadIdentity {
                branch_id: BRANCH_ID.to_owned(),
                generation,
                schema_key: schema_key.to_owned(),
                row_pk: row_pk.clone(),
                file_id: None,
            })))
        };
        let rows = PointReadPlan::new(
            ROW_SPACE,
            &[
                key(SHADOWED_SCHEMA, &shadowed_pk),
                key(PRIVATE_SCHEMA, &private_pk),
            ],
        )
        .materialize(&read, StorageGetOptions::default())
        .await
        .expect("read compacted generation")
        .value;
        assert!(
            rows[0].is_some(),
            "a tombstone shadowing a global row must survive the checkpoint"
        );
        assert!(
            rows[1].is_none(),
            "a tombstone shadowing nothing must be reclaimed by the checkpoint"
        );
    }
}
