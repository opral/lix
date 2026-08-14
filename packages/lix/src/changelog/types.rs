use crate::LixError;
use crate::common::LixTimestamp;
#[cfg(test)]
use crate::common::{ExactBatch, ExactValue};
use crate::row_pk::RowPk;
use crate::json_store::JsonSlot;
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

    #[cfg(test)]
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

    /// Returns the directly addressable identity of one packed change in a
    /// commit whose low 32 bits were reserved by
    /// [`CommitId::with_change_address_space`].
    pub(crate) fn for_commit_ordinal(commit_id: CommitId, ordinal: u32) -> Option<Self> {
        if ordinal == 0 {
            return None;
        }
        let mut bytes = *commit_id.as_uuid().as_bytes();
        if bytes[12..] != [0; 4] {
            return None;
        }
        bytes[12..].copy_from_slice(&ordinal.to_be_bytes());
        Some(Self::new(Uuid::from_bytes(bytes)))
    }

    #[cfg(test)]
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

#[cfg(test)]
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

#[derive(Clone, Debug, Eq, PartialEq, musli::Encode, musli::Decode)]
#[musli(packed)]
pub(crate) struct CommitRecord {
    /// Version 2 removes physical replay policy from the semantic commit row.
    pub(crate) format_version: u32,
    pub(crate) commit_id: CommitId,
    /// Longest-path distance from a graph root. Every parent has a strictly
    /// smaller generation, enabling bounded priority graph walks.
    pub(crate) generation: u64,
    pub(crate) parent_commit_ids: Vec<CommitId>,
    pub(crate) change_id: ChangeId,
    pub(crate) account_id: String,
    pub(crate) created_at: LixTimestamp,
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
    pub(crate) row_pk: RowPk,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot: JsonSlot,
    pub(crate) metadata: JsonSlot,
    pub(crate) created_at: LixTimestamp,
    pub(crate) origin_key: Option<String>,
}

#[cfg(test)]
pub(crate) type ChangeLoadBatch<'a> = ExactBatch<'a, ChangeId, ChangeRecord>;

#[cfg(test)]
impl ExactValue<ChangeId> for ChangeRecord {
    fn matches_exact_key(&self, key: &ChangeId) -> bool {
        self.change_id == *key
    }
}

#[derive(musli::Encode)]
#[musli(packed)]
pub(crate) struct ChangeRecordRef<'a> {
    pub(crate) format_version: u32,
    pub(crate) account_id: &'a str,
    pub(crate) schema_key: &'a str,
    pub(crate) row_pk: &'a RowPk,
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
    pub(crate) account_id: &'a str,
    pub(crate) schema_key: &'a str,
    pub(crate) row_pk: RowPk,
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

/// Canonical semantic payload stored inside a ForkTree Commit object.
///
/// The immutable object envelope owns identity, graph, generation, and state
/// edges. This payload preserves the remaining public `lix_commit` fields;
/// readers validate every duplicated semantic fact against the envelope.
pub(crate) fn encode_forktree_commit_payload(record: &CommitRecord) -> Result<Vec<u8>, LixError> {
    crate::storage_codec::encode("ForkTree commit semantic payload", record)
}

pub(crate) fn decode_forktree_commit_payload(bytes: &[u8]) -> Result<CommitRecord, LixError> {
    crate::storage_codec::decode("ForkTree commit semantic payload", bytes)
}

/// Canonical semantic payload stored inside either semantic Change object or
/// standalone RefChange object. ChangeId remains in the authenticated object
/// envelope and is reconstructed only after the envelope has authenticated.
pub(crate) fn encode_forktree_change_payload(record: &ChangeRecord) -> Result<Vec<u8>, LixError> {
    crate::storage_codec::encode(
        "ForkTree change semantic payload",
        &ChangeRecordRef {
            format_version: record.format_version,
            account_id: &record.account_id,
            schema_key: &record.schema_key,
            row_pk: &record.row_pk,
            file_id: record.file_id.as_deref(),
            snapshot: record.snapshot.as_ref_slot(),
            metadata: record.metadata.as_ref_slot(),
            created_at: record.created_at,
            origin_key: record.origin_key.as_deref(),
        },
    )
}

pub(crate) fn decode_forktree_change_payload(
    bytes: &[u8],
    change_id: ChangeId,
) -> Result<ChangeRecord, LixError> {
    let view: ChangeRecordView<'_> =
        crate::storage_codec::decode("ForkTree change semantic payload", bytes)?;
    let record = ChangeRecord {
        format_version: view.format_version,
        change_id,
        account_id: view.account_id.to_string(),
        schema_key: view.schema_key.to_string(),
        row_pk: view.row_pk,
        file_id: view.file_id,
        snapshot: view.snapshot,
        metadata: view.metadata,
        created_at: view.created_at,
        origin_key: view.origin_key,
    };
    Ok(record)
}

pub(crate) fn forktree_change_json_payload_ids(record: &ChangeRecord) -> Vec<[u8; 32]> {
    [record.snapshot.as_ref_slot(), record.metadata.as_ref_slot()]
        .into_iter()
        .filter_map(|slot| match slot {
            crate::json_store::JsonSlotRef::ForkTreeObject(object_id) => Some(*object_id),
            _ => None,
        })
        .collect()
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
