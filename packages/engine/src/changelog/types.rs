use crate::LixError;
use crate::common::LixTimestamp;
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
    pub(crate) commit_change_refs: Vec<CommitChangeRefSet>,
}

#[derive(Clone, Debug, Eq, PartialEq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitRecord {
    pub(crate) format_version: u32,
    pub(crate) commit_id: CommitId,
    pub(crate) parent_commit_ids: Vec<CommitId>,
    pub(crate) change_id: ChangeId,
    #[musli(with = crate::storage_codec::id_string_seq)]
    pub(crate) author_account_ids: Vec<String>,
    pub(crate) created_at: LixTimestamp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitChangeRefSet {
    pub(crate) commit_id: CommitId,
    pub(crate) entries: Vec<ChangeId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitChangeRefChunk {
    pub(crate) format_version: u32,
    pub(crate) commit_id: CommitId,
    pub(crate) entries: Vec<ChangeId>,
}

/// Stored ref chunk: the commit id lives in the storage key, the entries are
/// the referenced change ids sorted ascending. Everything else about a change
/// (schema key, file id, entity pk, payloads) lives in its change record and
/// is point-read by change id when needed.
#[derive(musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitChangeRefChunkWire {
    pub(crate) format_version: u32,
    pub(crate) entries: Vec<ChangeId>,
}

/// Encode-only borrowed twin of [`CommitChangeRefChunkWire`].
#[derive(musli::Encode)]
#[musli(packed)]
pub(crate) struct CommitChangeRefChunkWireRef<'a> {
    pub(crate) format_version: u32,
    pub(crate) entries: &'a [ChangeId],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommitProjection {
    Record,
    Full,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CommitLoadRequest<'a> {
    pub(crate) commit_ids: &'a [CommitId],
    pub(crate) projection: CommitProjection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitLoadBatch {
    pub(crate) entries: Vec<Option<CommitLoadEntry>>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CommitScanRequest<'a> {
    pub(crate) start_after: Option<&'a str>,
    pub(crate) limit: Option<usize>,
    pub(crate) projection: CommitProjection,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommitScanBatch {
    pub(crate) entries: Vec<CommitLoadEntry>,
    pub(crate) next_start_after: Option<CommitId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CommitLoadEntry {
    Record(CommitRecord),
    Full {
        record: CommitRecord,
        change_ref_chunks: Vec<CommitChangeRefChunk>,
    },
}

/// In-memory change record. The stored form (`ChangeRecordRef` /
/// `ChangeRecordView`) omits `change_id`: it is the storage key and gets
/// reconstructed on decode.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ChangeRecord {
    pub(crate) format_version: u32,
    pub(crate) change_id: ChangeId,
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
    pub(crate) schema_key: &'a str,
    #[musli(with = crate::storage_codec::id_string_seq)]
    pub(crate) entity_pk: &'a [String],
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

#[derive(Clone, Debug, Eq, PartialEq, musli::Decode)]
#[musli(packed)]
pub(crate) struct ChangeRecordView<'a> {
    pub(crate) format_version: u32,
    pub(crate) schema_key: &'a str,
    #[musli(with = crate::storage_codec::id_string_seq)]
    pub(crate) entity_pk: Vec<String>,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ChangeLoadBatch {
    pub(crate) entries: Vec<Option<ChangeRecord>>,
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

#[cfg(feature = "storage-benches")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GcRoot {
    BranchHead(CommitId),
}

#[cfg(feature = "storage-benches")]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GcLiveSet {
    pub(crate) commits: Vec<CommitId>,
    pub(crate) changes: Vec<ChangeId>,
    pub(crate) payloads: Vec<JsonRef>,
}

#[cfg(feature = "storage-benches")]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GcSweepSet {
    pub(crate) commits: Vec<CommitId>,
    pub(crate) changes: Vec<ChangeId>,
    pub(crate) commit_change_ref_chunks: Vec<(CommitId, u32)>,
    pub(crate) json_payloads: Vec<JsonRef>,
}

#[cfg(feature = "storage-benches")]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct GcRepairSet {}

#[cfg(feature = "storage-benches")]
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
