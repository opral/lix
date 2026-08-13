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

    /// The change id of the commit itself, at ordinal zero of its own change
    /// address space.
    ///
    /// [`Self::with_change_address_space`] reserves the low 32 bits for packed
    /// change ordinals, and the packed encoder biases every ordinal by one
    /// (`tracked_state::storage::addressable_change_id`), so the all-zero
    /// address is permanently unreachable as a member change —
    /// `direct_change_locator` rejects it. That makes it the natural, and only
    /// safe, slot for the synthetic `lix_commit` change that stands for the
    /// commit itself.
    ///
    /// Deriving it is what lets the commit record be the single authority for
    /// this identity. It was previously a freshly generated UUIDv7 stored on
    /// the commit record and mirrored into a dedicated reverse-index space so
    /// `lix_change` point reads could invert it; both are now unnecessary.
    pub(crate) fn commit_change_id(&self) -> ChangeId {
        ChangeId::new(self.uuid)
    }

    /// Test-only commit ids must satisfy the same invariant as real ones: the
    /// low 32 bits are reserved, because the commit's own change id is that
    /// address at ordinal zero.
    #[cfg(any(test, feature = "storage-benches"))]
    pub(crate) fn for_test_label(value: &str) -> Self {
        let uuid = Uuid::parse_str(value).unwrap_or_else(|_| test_uuid_from_label(0x43, value));
        Self::with_change_address_space(uuid)
    }
}

impl ChangeId {
    /// Recovers the commit whose synthetic `lix_commit` change this id is, if
    /// it is one.
    ///
    /// The inverse of [`CommitId::commit_change_id`]. Returns `None` unless the
    /// low 32 bits are zero, which is exactly the condition that excludes every
    /// packed member change.
    pub(crate) fn as_commit_change(&self) -> Option<CommitId> {
        (self.uuid.as_bytes()[12..] == [0; 4]).then(|| CommitId::new(self.uuid))
    }

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

/// Current on-disk shape of [`CommitRecord`].
pub(crate) const COMMIT_RECORD_FORMAT_VERSION: u32 = 5;

#[derive(Clone, Debug, Eq, PartialEq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitRecord {
    /// Version 3 adds the authenticated first-parent jump certified below.
    /// Version 4 drops the stored `change_id`: it is now derived from
    /// `commit_id` by [`CommitRecord::change_id`].
    /// Version 5 adds `touched_scope_digest`, the per-commit membership test
    /// that lets history traversal skip loading a commit's replay-state
    /// authority. This is a breaking on-disk change: the record codec is
    /// packed, so v4 bytes do not decode as v5.
    pub(crate) format_version: u32,
    pub(crate) commit_id: CommitId,
    /// Longest-path distance from a graph root. Every parent has a strictly
    /// smaller generation, enabling bounded priority graph walks.
    pub(crate) generation: u64,
    pub(crate) parent_commit_ids: Vec<CommitId>,
    /// Myers applicative-random-access-stack jump. It is derived from this
    /// record's first parent and lives in the same immutable authority.
    pub(crate) first_parent_jump_commit_id: CommitId,
    /// Number of first-parent edges covered by the jump. Roots and merge
    /// commits reset the linear lane with a self jump of span zero.
    pub(crate) first_parent_jump_span: u64,
    pub(crate) account_id: String,
    pub(crate) created_at: LixTimestamp,
    /// Collection scopes this commit's delta has members in.
    ///
    /// Published here rather than on the commit-state manifest precisely
    /// because graph traversal already loads this record to find parents: the
    /// history membership test then costs no extra point read. See
    /// [`crate::changelog::CommitTouchedScopeDigest`].
    pub(crate) touched_scope_digest: super::CommitTouchedScopeDigest,
}

impl CommitRecord {
    /// The public `lix_change.id` of this commit's synthetic `lix_commit` row.
    ///
    /// Derived, never stored: see [`CommitId::commit_change_id`].
    pub(crate) fn change_id(&self) -> ChangeId {
        self.commit_id.commit_change_id()
    }
}

/// Derives the one-pointer Myers jump owned by a new immutable commit.
///
/// If the parent's jump and its jump cover equal spans, the child composes
/// them; otherwise it points to its parent. The resulting random-access stack
/// answers level-ancestor and linear LCA queries in at most logarithmic hops
/// with one pointer per node.
///
/// See Eugene W. Myers, “An Applicative Random-Access Stack,” Information
/// Processing Letters 17(5), 1983, doi:10.1016/0020-0190(83)90106-0.
pub(crate) fn next_first_parent_jump(
    commit_id: CommitId,
    parent_commit_ids: &[CommitId],
    parent: Option<&CommitRecord>,
    parent_jump: Option<&CommitRecord>,
) -> Result<(CommitId, u64), LixError> {
    match parent_commit_ids {
        [parent_commit_id] => {
            let parent = parent.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("commit '{commit_id}' has no parent jump metadata"),
                )
            })?;
            if parent.commit_id != *parent_commit_id {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("commit '{commit_id}' resolved the wrong parent jump record"),
                ));
            }
            let jump = parent_jump.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("commit '{commit_id}' has no parent jump target"),
                )
            })?;
            if jump.commit_id != parent.first_parent_jump_commit_id
                || parent.generation.checked_sub(jump.generation)
                    != Some(parent.first_parent_jump_span)
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "commit '{}' has invalid first-parent jump metadata",
                        parent.commit_id
                    ),
                ));
            }
            if parent.first_parent_jump_span == jump.first_parent_jump_span {
                let span = 1u64
                    .checked_add(parent.first_parent_jump_span)
                    .and_then(|value| value.checked_add(jump.first_parent_jump_span))
                    .ok_or_else(|| LixError::unknown("first-parent jump span exceeds u64"))?;
                Ok((jump.first_parent_jump_commit_id, span))
            } else {
                Ok((parent.commit_id, 1))
            }
        }
        _ => Ok((commit_id, 0)),
    }
}

#[cfg(test)]
mod topology_tests {
    use super::{CommitId, CommitRecord, next_first_parent_jump};
    use crate::common::LixTimestamp;

    #[test]
    fn myers_jumps_are_logarithmic_for_every_level_ancestor_through_4096() {
        let mut records = vec![record(0, None, None)];
        for depth in 1..=4_096u64 {
            let parent = records.last().expect("linear parent");
            let jump = &records[usize::try_from(parent.generation - parent.first_parent_jump_span)
                .expect("jump depth fits usize")];
            let commit_id = id(depth);
            let (jump_id, jump_span) =
                next_first_parent_jump(commit_id, &[parent.commit_id], Some(parent), Some(jump))
                    .expect("derive Myers jump");
            records.push(record(
                depth,
                Some(parent.commit_id),
                Some((jump_id, jump_span)),
            ));
        }

        for start in 1..records.len() {
            for target in 0..start {
                let mut cursor = start;
                let mut hops = 0usize;
                while cursor > target {
                    let node = &records[cursor];
                    let jump = cursor
                        - usize::try_from(node.first_parent_jump_span)
                            .expect("jump span fits usize");
                    cursor = if jump >= target { jump } else { cursor - 1 };
                    hops += 1;
                }
                // Myers' bound is expressed with the ceiling logarithm of
                // the stack length; `start` is one less than that length.
                let ceil_log =
                    usize::try_from(usize::BITS - start.leading_zeros()).expect("log fits usize");
                let bound = 3 * ceil_log - 2;
                assert!(
                    hops <= bound,
                    "start={start} target={target} hops={hops} bound={bound}"
                );
            }
        }

        let merge = id(5_000);
        assert_eq!(
            next_first_parent_jump(merge, &[id(1), id(2)], None, None).expect("merge resets jump"),
            (merge, 0)
        );
    }

    #[test]
    fn myers_lca_matches_parent_walker_for_exhaustive_branch_splits() {
        #[derive(Clone, Copy)]
        struct Node {
            parent: Option<usize>,
            jump: usize,
            span: usize,
            depth: usize,
        }

        fn push(nodes: &mut Vec<Node>, parent: Option<usize>) -> usize {
            let index = nodes.len();
            let Some(parent_index) = parent else {
                nodes.push(Node {
                    parent: None,
                    jump: index,
                    span: 0,
                    depth: 0,
                });
                return index;
            };
            let parent = nodes[parent_index];
            let parent_jump = nodes[parent.jump];
            let (jump, span) = if parent.span == parent_jump.span {
                (parent_jump.jump, 1 + parent.span + parent_jump.span)
            } else {
                (parent_index, 1)
            };
            nodes.push(Node {
                parent: Some(parent_index),
                jump,
                span,
                depth: parent.depth + 1,
            });
            index
        }

        fn general_lca(nodes: &[Node], mut left: usize, mut right: usize) -> usize {
            while nodes[left].depth > nodes[right].depth {
                left = nodes[left].parent.expect("non-root has parent");
            }
            while nodes[right].depth > nodes[left].depth {
                right = nodes[right].parent.expect("non-root has parent");
            }
            while left != right {
                left = nodes[left].parent.expect("fork side has parent");
                right = nodes[right].parent.expect("fork side has parent");
            }
            left
        }

        fn myers_lca(nodes: &[Node], mut left: usize, mut right: usize) -> (usize, usize) {
            let mut hops = 0;
            while nodes[left].depth != nodes[right].depth {
                let (deeper, target) = if nodes[left].depth > nodes[right].depth {
                    (&mut left, nodes[right].depth)
                } else {
                    (&mut right, nodes[left].depth)
                };
                let jump = nodes[*deeper].jump;
                *deeper = if nodes[jump].depth >= target {
                    jump
                } else {
                    nodes[*deeper].parent.expect("non-root has parent")
                };
                hops += 1;
            }
            while left != right {
                if nodes[left].jump == nodes[right].jump {
                    left = nodes[left].parent.expect("fork side has parent");
                    right = nodes[right].parent.expect("fork side has parent");
                } else {
                    left = nodes[left].jump;
                    right = nodes[right].jump;
                }
                hops += 1;
            }
            (left, hops)
        }

        let mut nodes = Vec::new();
        let root = push(&mut nodes, None);
        let mut trunk = vec![root];
        for _ in 0..64 {
            trunk.push(push(&mut nodes, trunk.last().copied()));
        }
        for (split_depth, fork) in trunk.iter().copied().enumerate() {
            for left_len in 1..=32 {
                let mut left = fork;
                for _ in 0..left_len {
                    left = push(&mut nodes, Some(left));
                }
                for right_len in 1..=32 {
                    let mut right = fork;
                    for _ in 0..right_len {
                        right = push(&mut nodes, Some(right));
                    }
                    let expected = general_lca(&nodes, left, right);
                    let (actual, hops) = myers_lca(&nodes, left, right);
                    let max_depth = nodes[left].depth.max(nodes[right].depth);
                    let bound =
                        6 * usize::try_from((max_depth + 1).ilog2()).expect("log fits usize") + 2;
                    assert_eq!(
                        actual, expected,
                        "split={split_depth} left={left_len} right={right_len}"
                    );
                    assert!(
                        hops <= bound,
                        "split={split_depth} left={left_len} right={right_len} hops={hops} bound={bound}"
                    );
                }
            }
        }
    }

    fn id(depth: u64) -> CommitId {
        CommitId::for_test_label(&format!("myers-{depth}"))
    }

    fn record(depth: u64, parent: Option<CommitId>, jump: Option<(CommitId, u64)>) -> CommitRecord {
        let commit_id = id(depth);
        CommitRecord {
            touched_scope_digest: crate::changelog::CommitTouchedScopeDigest::absent(),
            format_version: 4,
            commit_id,
            generation: depth,
            parent_commit_ids: parent.into_iter().collect(),
            first_parent_jump_commit_id: jump.map_or(commit_id, |jump| jump.0),
            first_parent_jump_span: jump.map_or(0, |jump| jump.1),
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            created_at: LixTimestamp::expect_parse("Myers test timestamp", "2026-08-11T00:00:00Z"),
        }
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
