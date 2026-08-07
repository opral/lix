use crate::LixError;
use crate::common::LixTimestamp;
use crate::common::{ExactBatch, ExactValue};
use crate::entity_pk::EntityPk;
use crate::json_store::{JsonRef, JsonSlot};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct CommitId {
    uuid: Uuid,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ChangeId {
    uuid: Uuid,
}

const UUID_HYPHENATED_LEN: usize = uuid::fmt::Hyphenated::LENGTH;

impl CommitId {
    pub(crate) fn new(value: Uuid) -> Self {
        Self { uuid: value }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, uuid::Error> {
        value.parse()
    }

    pub(crate) fn parse_lix(value: &str, context: &str) -> Result<Self, LixError> {
        #[cfg_attr(not(test), expect(clippy::bind_instead_of_map))]
        Self::parse(value).or_else(|error| {
            #[cfg(test)]
            {
                if !value.is_empty() {
                    return Ok(Self::for_test_label(value));
                }
            }
            Err(LixError::new(
                LixError::CODE_UNKNOWN,
                format!("{context} must be a UUID commit id: {error}"),
            ))
        })
    }

    pub(crate) fn as_uuid(&self) -> &Uuid {
        &self.uuid
    }

    /// Creates a commit id whose low 32 bits are reserved for directly
    /// addressable packed change ordinals.
    ///
    /// Fold those bits into the remaining random field before clearing them
    /// so deterministic UUID providers still produce distinct commit ids.
    pub(crate) fn with_change_address_space(value: Uuid) -> Self {
        let mut bytes = *value.as_bytes();
        bytes[8] = (bytes[8] & 0xc0) | ((bytes[8] ^ bytes[12]) & 0x3f);
        bytes[9] ^= bytes[13];
        bytes[10] ^= bytes[14];
        bytes[11] ^= bytes[15];
        bytes[12..].fill(0);
        Self::new(Uuid::from_bytes(bytes))
    }

    #[cfg(any(test, feature = "storage-benches"))]
    pub(crate) fn for_test_label(value: &str) -> Self {
        Uuid::parse_str(value)
            .map(Self::new)
            .unwrap_or_else(|_| Self::new(test_uuid_from_label(0x43, value)))
    }
}

impl ChangeId {
    pub(crate) fn new(value: Uuid) -> Self {
        Self { uuid: value }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, uuid::Error> {
        value.parse()
    }

    pub(crate) fn parse_lix(value: &str, context: &str) -> Result<Self, LixError> {
        #[cfg_attr(not(test), expect(clippy::bind_instead_of_map))]
        Self::parse(value).or_else(|error| {
            #[cfg(test)]
            {
                if !value.is_empty() {
                    return Ok(Self::for_test_label(value));
                }
            }
            Err(LixError::new(
                LixError::CODE_UNKNOWN,
                format!("{context} must be a UUID change id: {error}"),
            ))
        })
    }

    pub(crate) fn as_uuid(&self) -> &Uuid {
        &self.uuid
    }

    #[cfg(any(test, feature = "storage-benches"))]
    pub(crate) fn for_test_label(value: &str) -> Self {
        Uuid::parse_str(value)
            .map(Self::new)
            .unwrap_or_else(|_| Self::new(test_uuid_from_label(0x68, value)))
    }
}

fn uuid_text(value: Uuid) -> [u8; UUID_HYPHENATED_LEN] {
    let mut text = [0; UUID_HYPHENATED_LEN];
    value.hyphenated().encode_lower(&mut text);
    text
}

fn uuid_text_str(text: &[u8; UUID_HYPHENATED_LEN]) -> &str {
    std::str::from_utf8(text).expect("UUID text cache should contain valid UTF-8")
}

#[cfg(any(test, feature = "storage-benches"))]
fn test_uuid_from_label(kind: u8, value: &str) -> Uuid {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0100_0000_01b3;

    fn hash(seed: u64, bytes: impl Iterator<Item = u8>) -> u64 {
        bytes.fold(seed, |hash, byte| {
            let hash = hash ^ u64::from(byte);
            hash.wrapping_mul(FNV_PRIME)
        })
    }

    let high = hash(FNV_OFFSET ^ u64::from(kind), value.bytes());
    let low = hash(FNV_OFFSET ^ !u64::from(kind), value.bytes().rev());
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&high.to_be_bytes());
    bytes[8..].copy_from_slice(&low.to_be_bytes());
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

macro_rules! impl_uuid_id {
    ($id:ident, $name:literal) => {
        impl fmt::Display for $id {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let text = uuid_text(self.uuid);
                f.write_str(uuid_text_str(&text))
            }
        }

        impl Default for $id {
            fn default() -> Self {
                Self::new(Uuid::nil())
            }
        }

        impl From<Uuid> for $id {
            fn from(value: Uuid) -> Self {
                Self::new(value)
            }
        }

        impl From<$id> for Uuid {
            fn from(value: $id) -> Self {
                value.uuid
            }
        }

        impl FromStr for $id {
            type Err = uuid::Error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(value).map(Self::new)
            }
        }

        impl TryFrom<&str> for $id {
            type Error = uuid::Error;

            fn try_from(value: &str) -> Result<Self, Self::Error> {
                value.parse()
            }
        }

        impl TryFrom<String> for $id {
            type Error = uuid::Error;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                value.parse()
            }
        }

        impl From<$id> for String {
            fn from(value: $id) -> Self {
                value.to_string()
            }
        }

        impl From<&$id> for String {
            fn from(value: &$id) -> Self {
                value.to_string()
            }
        }

        impl PartialEq<str> for $id {
            fn eq(&self, other: &str) -> bool {
                let text = uuid_text(self.uuid);
                if uuid_text_str(&text) == other {
                    return true;
                }
                #[cfg(test)]
                {
                    if !other.is_empty() && Self::for_test_label(other) == *self {
                        return true;
                    }
                }
                false
            }
        }

        impl PartialEq<&str> for $id {
            fn eq(&self, other: &&str) -> bool {
                self == *other
            }
        }

        impl PartialEq<String> for $id {
            fn eq(&self, other: &String) -> bool {
                self == other.as_str()
            }
        }

        impl PartialEq<$id> for str {
            fn eq(&self, other: &$id) -> bool {
                other == self
            }
        }

        impl PartialEq<$id> for &str {
            fn eq(&self, other: &$id) -> bool {
                other == *self
            }
        }

        impl PartialEq<$id> for String {
            fn eq(&self, other: &$id) -> bool {
                other == self
            }
        }

        impl<M> musli::Encode<M> for $id {
            type Encode = uuid::Bytes;

            fn encode<E>(&self, encoder: E) -> Result<(), E::Error>
            where
                E: musli::Encoder<Mode = M>,
            {
                encoder.encode_array(self.uuid.as_bytes())
            }

            fn size_hint(&self) -> Option<usize> {
                Some(std::mem::size_of::<uuid::Bytes>())
            }

            fn as_encode(&self) -> &Self::Encode {
                self.uuid.as_bytes()
            }
        }

        impl<'de, M, A> musli::Decode<'de, M, A> for $id
        where
            A: musli::Allocator,
        {
            fn decode<D>(decoder: D) -> Result<Self, D::Error>
            where
                D: musli::Decoder<'de, Mode = M, Allocator = A>,
            {
                Ok(Self::new(Uuid::from_bytes(decoder.decode_array()?)))
            }
        }

        impl serde::Serialize for $id {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(&self.to_string())
            }
        }

        impl<'de> serde::Deserialize<'de> for $id {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let value = <String as serde::Deserialize>::deserialize(deserializer)?;
                value.parse().map_err(serde::de::Error::custom)
            }
        }
    };
}

impl_uuid_id!(CommitId, "commit id");
impl_uuid_id!(ChangeId, "change id");

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ChangelogAppend {
    pub(crate) commits: Vec<CommitRecord>,
    pub(crate) changes: Vec<ChangeRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitRecord {
    /// Version 3 adds the bounded linear topology segment certified below.
    pub(crate) format_version: u32,
    pub(crate) commit_id: CommitId,
    /// Longest-path distance from a graph root. Every parent has a strictly
    /// smaller generation, enabling bounded priority graph walks.
    pub(crate) generation: u64,
    pub(crate) parent_commit_ids: Vec<CommitId>,
    /// Number of single-parent edges from this commit to the current bounded
    /// routing segment's base. Parent ids and generations remain the sole
    /// chronology authority; this field only decides when a reader may attempt
    /// a fully authenticated batched walk.
    pub(crate) linear_segment_depth: u8,
    /// Immediate-parent-to-base commit ids for a full-width segment endpoint.
    /// Non-endpoints store an empty list. A reader must load and validate every
    /// listed commit before it can skip the corresponding adapter round trips.
    pub(crate) linear_segment_ancestor_commit_ids: Vec<CommitId>,
    pub(crate) change_id: ChangeId,
    pub(crate) account_id: String,
    pub(crate) created_at: LixTimestamp,
}

// Three edges per authenticated batch keeps the persisted route below the
// five-percent write/disk ceiling while still eliminating half of adapter
// calls on a deep linear walk. Wider routes approach the same sixteen bytes
// of duplicated ids per commit and exceeded the SlateDB physical-disk gate.
pub(crate) const LINEAR_SEGMENT_MAX_DEPTH: u8 = 3;

/// Derives the bounded linear segment owned by a new commit record.
///
/// Merge commits and roots start a segment. A single-parent commit extends its
/// parent's segment until the fixed width is full, then starts the next one.
/// Branches may share a base; merge-base readers detect that case and refine
/// within the segment instead of skipping over the fork.
pub(crate) fn next_linear_segment_depth(
    commit_id: CommitId,
    parent_commit_ids: &[CommitId],
    parent_depth: Option<u8>,
) -> Result<u8, LixError> {
    match parent_commit_ids {
        [_] => {
            let depth = parent_depth.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("commit '{commit_id}' has no parent segment metadata"),
                )
            })?;
            if depth < LINEAR_SEGMENT_MAX_DEPTH {
                Ok(depth + 1)
            } else {
                Ok(0)
            }
        }
        _ => Ok(0),
    }
}

/// Derives an in-memory parent route while constructing a linear fixture or
/// publication batch. Callers persist the returned route only at full-width
/// endpoints; shorter routes are construction state, not storage authority.
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) fn next_linear_segment_path(
    commit_id: CommitId,
    parent_commit_ids: &[CommitId],
    parent_segment: Option<(u8, &[CommitId])>,
) -> Result<(u8, Vec<CommitId>), LixError> {
    let parent_depth = parent_segment.map(|(depth, _)| depth);
    let depth = next_linear_segment_depth(commit_id, parent_commit_ids, parent_depth)?;
    if depth == 0 {
        return Ok((0, Vec::new()));
    }
    let [parent_commit_id] = parent_commit_ids else {
        unreachable!("positive linear segment depth requires one parent");
    };
    let (_, parent_path) = parent_segment.expect("positive depth has parent segment metadata");
    if parent_path.len() != usize::from(depth - 1) {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("commit '{commit_id}' has incomplete parent routing metadata"),
        ));
    }
    let mut path = Vec::with_capacity(usize::from(depth));
    path.push(*parent_commit_id);
    path.extend_from_slice(parent_path);
    Ok((depth, path))
}

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) fn persisted_linear_segment_path(depth: u8, path: &[CommitId]) -> Vec<CommitId> {
    if depth == LINEAR_SEGMENT_MAX_DEPTH {
        path.to_vec()
    } else {
        Vec::new()
    }
}

pub(crate) fn validate_linear_segment_hint_shape(
    commit_id: CommitId,
    parent_commit_ids: &[CommitId],
    depth: u8,
    ancestor_commit_ids: &[CommitId],
) -> Result<(), LixError> {
    if depth > LINEAR_SEGMENT_MAX_DEPTH {
        return Err(LixError::unknown(format!(
            "commit '{commit_id}' linear segment depth exceeds the bounded width"
        )));
    }
    if depth > 0 && parent_commit_ids.len() != 1 {
        return Err(LixError::unknown(format!(
            "commit '{commit_id}' has a nonlinear parent set inside a linear segment"
        )));
    }
    if depth == LINEAR_SEGMENT_MAX_DEPTH {
        if ancestor_commit_ids.len() != usize::from(LINEAR_SEGMENT_MAX_DEPTH) {
            return Err(LixError::unknown(format!(
                "commit '{commit_id}' full linear segment has an invalid routing width"
            )));
        }
        if ancestor_commit_ids.first() != parent_commit_ids.first() {
            return Err(LixError::unknown(format!(
                "commit '{commit_id}' linear segment routing does not start at its parent"
            )));
        }
        let mut unique = std::collections::BTreeSet::new();
        unique.insert(commit_id);
        if ancestor_commit_ids
            .iter()
            .any(|ancestor_commit_id| !unique.insert(*ancestor_commit_id))
        {
            return Err(LixError::unknown(format!(
                "commit '{commit_id}' linear segment routing contains a cycle"
            )));
        }
    } else if !ancestor_commit_ids.is_empty() {
        return Err(LixError::unknown(format!(
            "commit '{commit_id}' non-endpoint linear segment has routing entries"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod topology_tests {
    use super::{CommitId, LINEAR_SEGMENT_MAX_DEPTH, next_linear_segment_depth};

    #[test]
    fn linear_segments_are_bounded_and_merges_reset_them() {
        let root = CommitId::for_test_label("segment-root");
        let mut parent = root;
        let mut depth =
            next_linear_segment_depth(root, &[], None).expect("root should start segment");
        assert_eq!(depth, 0);

        for expected_depth in 1..=LINEAR_SEGMENT_MAX_DEPTH {
            let commit = CommitId::for_test_label(&format!("segment-{expected_depth}"));
            depth = next_linear_segment_depth(commit, &[parent], Some(depth))
                .expect("linear child should extend segment");
            assert_eq!(depth, expected_depth);
            parent = commit;
        }

        let next = CommitId::for_test_label("next-segment");
        assert_eq!(
            next_linear_segment_depth(next, &[parent], Some(depth))
                .expect("full segment should roll over"),
            0
        );

        let merge = CommitId::for_test_label("segment-merge");
        assert_eq!(
            next_linear_segment_depth(merge, &[root, next], None)
                .expect("merge should reset segment"),
            0
        );
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CommitLoadRequest<'a> {
    pub(crate) commit_ids: &'a [CommitId],
}

pub(crate) type CommitLoadBatch<'a> = ExactBatch<'a, CommitId, CommitRecord>;

impl ExactValue<CommitId> for CommitRecord {
    fn matches_exact_key(&self, key: &CommitId) -> bool {
        self.commit_id == *key
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CommitScanRequest<'a> {
    pub(crate) start_after: Option<&'a str>,
    pub(crate) limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitScanBatch {
    pub(crate) entries: Vec<CommitRecord>,
    pub(crate) next_start_after: Option<CommitId>,
}

/// In-memory change record. The stored form (`ChangeRecordRef` /
/// `ChangeRecordView`) omits `change_id`: it is the storage key and gets
/// reconstructed on decode.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ChangeRecord {
    pub(crate) format_version: u32,
    pub(crate) change_id: ChangeId,
    pub(crate) account_id: String,
    pub(crate) schema_key: String,
    pub(crate) entity_pk: EntityPk,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot: JsonSlot,
    pub(crate) metadata: JsonSlot,
    pub(crate) created_at: LixTimestamp,
    pub(crate) origin_key: Option<String>,
}

#[derive(musli::Encode)]
#[musli(packed)]
pub(crate) struct ChangeRecordRef<'a> {
    pub(crate) format_version: u32,
    pub(crate) account_id: &'a str,
    pub(crate) schema_key: &'a str,
    pub(crate) entity_pk: &'a EntityPk,
    #[musli(with = crate::storage_codec::option_id_string)]
    pub(crate) file_id: Option<&'a str>,
    #[musli(with = crate::json_store::json_slot_storage_ref)]
    pub(crate) snapshot: crate::json_store::JsonSlotRef<'a>,
    #[musli(with = crate::json_store::json_slot_storage_ref)]
    pub(crate) metadata: crate::json_store::JsonSlotRef<'a>,
    pub(crate) created_at: LixTimestamp,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) origin_key: Option<&'a str>,
}

/// Borrowed, already-prepared change record for the terminal transaction
/// append lane.
///
/// Unlike [`ChangeRecord`], this form never owns a second copy of row JSON,
/// primary-key parts, or schema strings. Transaction materialization has
/// already assigned identities and validated the facts, so the changelog can
/// encode these references directly into the final write set.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TransactionChangeRecordRef<'a> {
    pub(crate) change_id: ChangeId,
    pub(crate) format_version: u32,
    pub(crate) account_id: &'a str,
    pub(crate) schema_key: &'a str,
    pub(crate) entity_pk: &'a EntityPk,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) snapshot: crate::json_store::JsonSlotRef<'a>,
    pub(crate) metadata: crate::json_store::JsonSlotRef<'a>,
    pub(crate) created_at: LixTimestamp,
    pub(crate) origin_key: Option<&'a str>,
}

impl<'a> From<&'a ChangeRecord> for TransactionChangeRecordRef<'a> {
    fn from(record: &'a ChangeRecord) -> Self {
        Self {
            change_id: record.change_id,
            format_version: record.format_version,
            account_id: &record.account_id,
            schema_key: &record.schema_key,
            entity_pk: &record.entity_pk,
            file_id: record.file_id.as_deref(),
            snapshot: record.snapshot.as_ref_slot(),
            metadata: record.metadata.as_ref_slot(),
            created_at: record.created_at,
            origin_key: record.origin_key.as_deref(),
        }
    }
}

/// Trusted changelog facts assembled at the transaction commit boundary.
///
/// This is deliberately separate from [`ChangelogAppend`]: the generic
/// writer supports validation and read-your-writes overlays, while this lane
/// is terminal and encodes prepared transaction facts directly into storage.
#[derive(Debug)]
pub(crate) struct TransactionChangelogAppend<'a> {
    pub(crate) commits: Vec<CommitRecord>,
    pub(crate) changes: Vec<TransactionChangeRecordRef<'a>>,
}

#[derive(Clone, Debug, Eq, PartialEq, musli::Decode)]
#[musli(packed)]
pub(crate) struct ChangeRecordView<'a> {
    pub(crate) format_version: u32,
    pub(crate) account_id: &'a str,
    pub(crate) schema_key: &'a str,
    pub(crate) entity_pk: EntityPk,
    #[musli(with = crate::storage_codec::option_id_string)]
    pub(crate) file_id: Option<String>,
    #[musli(with = crate::json_store::json_slot_storage)]
    pub(crate) snapshot: JsonSlot,
    #[musli(with = crate::json_store::json_slot_storage)]
    pub(crate) metadata: JsonSlot,
    pub(crate) created_at: LixTimestamp,
    #[musli(with = crate::storage_codec::option)]
    pub(crate) origin_key: Option<String>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ChangeLoadRequest<'a> {
    pub(crate) change_ids: &'a [ChangeId],
}

pub(crate) type ChangeLoadBatch<'a> = ExactBatch<'a, ChangeId, ChangeRecord>;

impl ExactValue<ChangeId> for ChangeRecord {
    fn matches_exact_key(&self, key: &ChangeId) -> bool {
        self.change_id == *key
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ChangeScanRequest<'a> {
    pub(crate) start_after: Option<&'a str>,
    pub(crate) limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ChangeScanBatch {
    pub(crate) entries: Vec<ChangeRecord>,
    pub(crate) next_start_after: Option<ChangeId>,
}

#[cfg(feature = "storage-benches")]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RebuildIndexStats {
    pub(crate) expected: usize,
    pub(crate) put: usize,
    pub(crate) deleted: usize,
    pub(crate) unchanged: usize,
}

#[allow(dead_code)] // Activated by the checkpoint GC integration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GcRoot {
    BranchHead(CommitId),
    StandaloneChange(ChangeId),
    /// A history-free untracked current-state member owns this payload
    /// directly, without a changelog record to retain it.
    CurrentPayload(JsonRef),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GcLiveSet {
    pub(crate) commits: Vec<CommitId>,
    pub(crate) changes: Vec<ChangeId>,
    pub(crate) payloads: Vec<JsonRef>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GcSweepSet {
    pub(crate) commits: Vec<CommitId>,
    pub(crate) commit_change_ids: Vec<ChangeId>,
    pub(crate) changes: Vec<ChangeId>,
    pub(crate) json_payloads: Vec<JsonRef>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GcRepairSet {}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GcPlan {
    pub(crate) roots: Vec<GcRoot>,
    pub(crate) live: GcLiveSet,
    pub(crate) sweep: GcSweepSet,
    pub(crate) repair: GcRepairSet,
}

/// Canonical derived `lix_commit` row snapshot.
///
/// Commit graph, live-state, and SQL change surfaces must produce the same
/// representation from the canonical `changelog.commit` record.
pub(crate) fn commit_row_snapshot_json(commit_id: &str) -> Result<String, LixError> {
    serde_json::to_string(&serde_json::json!({ "id": commit_id })).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("commit row snapshot serialization failed: {error}"),
        )
    })
}
