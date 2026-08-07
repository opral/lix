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
pub(crate) const COMMIT_RECORD_FORMAT_VERSION: u32 = 3;

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

    /// Returns the public change identity of the synthetic commit envelope.
    ///
    /// Commit ids reserve their low 32 bits for directly addressable tracked
    /// changes. A fixed domain-tag xor in the remaining UUIDv7 random field
    /// preserves timestamp, version, variant, and the zero packed address.
    /// Applying the same xor again is the inverse, so exact `lix_change`
    /// lookup needs no durable reverse map. The zero packed address remains
    /// disjoint from directly addressed tracked changes, whose ordinals start
    /// at one; explicit identities are protected by collision probes.
    pub(crate) fn envelope_change_id(self) -> Result<ChangeId, LixError> {
        if self.uuid.as_bytes()[12..] != [0; 4] {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("commit '{}' has no reserved change address space", self),
            ));
        }
        let mut bytes = *self.uuid.as_bytes();
        xor_commit_envelope_domain(&mut bytes);
        Ok(ChangeId::new(Uuid::from_bytes(bytes)))
    }

    #[cfg(any(test, feature = "storage-benches"))]
    pub(crate) fn for_test_label(value: &str) -> Self {
        let uuid = Uuid::parse_str(value).unwrap_or_else(|_| test_uuid_from_label(0x43, value));
        Self::with_change_address_space(uuid)
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

    /// Inverts [`CommitId::envelope_change_id`] for the only identities that
    /// can belong to the generated commit-envelope domain.
    pub(crate) fn envelope_commit_id(self) -> Option<CommitId> {
        (self.uuid.as_bytes()[12..] == [0; 4]).then(|| {
            let mut bytes = *self.uuid.as_bytes();
            xor_commit_envelope_domain(&mut bytes);
            CommitId::new(Uuid::from_bytes(bytes))
        })
    }

    #[cfg(any(test, feature = "storage-benches"))]
    pub(crate) fn for_test_label(value: &str) -> Self {
        Uuid::parse_str(value)
            .map(Self::new)
            .unwrap_or_else(|_| Self::new(test_uuid_from_label(0x68, value)))
    }
}

fn xor_commit_envelope_domain(bytes: &mut [u8; 16]) {
    // Keep UUID timestamp/version/variant and the packed tracked-change
    // address untouched. The nonzero mask is an involutive namespace tag.
    bytes[6] ^= 0x0d;
    bytes[7] ^= 0x4c;
    bytes[8] ^= 0x21;
    bytes[9] ^= 0x49;
    bytes[10] ^= 0x58;
    bytes[11] ^= 0xa5;
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
    /// Version 3 removes the independently persisted commit-envelope change id.
    pub(crate) format_version: u32,
    pub(crate) commit_id: CommitId,
    /// Longest-path distance from a graph root. Every parent has a strictly
    /// smaller generation, enabling bounded priority graph walks.
    pub(crate) generation: u64,
    pub(crate) parent_commit_ids: Vec<CommitId>,
    pub(crate) account_id: String,
    pub(crate) created_at: LixTimestamp,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commit_envelope_identity_is_domain_separated_deterministic_bijection() {
        let commit_id = CommitId::with_change_address_space(uuid::Uuid::from_u128(
            0x0192_0000_0000_7000_8123_4567_89ab_cdef,
        ));
        let first = commit_id.envelope_change_id().unwrap();
        let second = commit_id.envelope_change_id().unwrap();

        assert_eq!(first, second);
        assert_ne!(first.as_uuid(), commit_id.as_uuid());
        assert_eq!(first.as_uuid().get_version_num(), 7);
        assert_eq!(
            first.as_uuid().get_variant(),
            commit_id.as_uuid().get_variant()
        );
        assert_eq!(
            &first.as_uuid().as_bytes()[..6],
            &commit_id.as_uuid().as_bytes()[..6]
        );
        assert_eq!(&first.as_uuid().as_bytes()[12..], &[0; 4]);
        assert_eq!(first.envelope_commit_id(), Some(commit_id));
    }

    #[test]
    fn ordinary_change_identity_is_not_misclassified_as_commit_envelope() {
        let change_id = ChangeId::new(uuid::Uuid::from_u128(
            0x0192_0000_0000_7000_8123_4567_0000_0001,
        ));
        assert_eq!(change_id.envelope_commit_id(), None);
    }

    #[test]
    fn commit_without_reserved_address_space_cannot_derive_envelope_identity() {
        let malformed = CommitId::new(uuid::Uuid::from_u128(
            0x0192_0000_0000_7000_8123_4567_0000_0001,
        ));
        let error = malformed
            .envelope_change_id()
            .expect_err("noncanonical commit id must fail closed");
        assert!(error.message.contains("no reserved change address space"));
    }
}
