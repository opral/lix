use bytes::Bytes;

use crate::storage::{SpaceId, StorageError, StorageSpace};

use super::codec::{Decoder, Encoder, corruption, keyed_hash};

const OBJECT_MAGIC: &[u8; 8] = b"LIXFTO\0\x01";
const OBJECT_HASH_DOMAIN: &str = "lix forktree immutable object id v1";

pub(crate) const OBJECT_SPACE: StorageSpace =
    StorageSpace::immutable(SpaceId(0x0009_0001), "forktree.object.v1");

#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct ObjectId([u8; 32]);

impl ObjectId {
    pub(crate) const ZERO: Self = Self([0; 32]);

    pub(crate) const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(crate) const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub(crate) const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl std::fmt::Display for ObjectId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub(super) enum ObjectDomain {
    RepositoryRoot = 1,
    BranchSnapshot = 2,
    Commit = 3,
    SemanticChange = 4,
    BranchRefChange = 5,
    OrderedTreeNode = 6,
    UploadPart = 7,
    UploadProgress = 8,
    BlobChunk = 9,
}

impl ObjectDomain {
    fn decode(value: u16) -> Result<Self, StorageError> {
        match value {
            1 => Ok(Self::RepositoryRoot),
            2 => Ok(Self::BranchSnapshot),
            3 => Ok(Self::Commit),
            4 => Ok(Self::SemanticChange),
            5 => Ok(Self::BranchRefChange),
            6 => Ok(Self::OrderedTreeNode),
            7 => Ok(Self::UploadPart),
            8 => Ok(Self::UploadProgress),
            9 => Ok(Self::BlobChunk),
            _ => Err(corruption(format!("unknown object domain {value}"))),
        }
    }
}

pub(super) fn encode_object(
    domain: ObjectDomain,
    encode_payload: impl FnOnce(&mut Encoder) -> Result<(), StorageError>,
) -> Result<(ObjectId, Bytes), StorageError> {
    let mut encoder = Encoder::with_prefix(OBJECT_MAGIC);
    encoder.u32(domain as u32);
    encode_payload(&mut encoder)?;
    let bytes = Bytes::from(encoder.into_vec());
    let id = ObjectId(keyed_hash(OBJECT_HASH_DOMAIN, &bytes));
    Ok((id, bytes))
}

pub(super) fn decode_object<'a>(
    expected_id: ObjectId,
    expected_domain: ObjectDomain,
    bytes: &'a [u8],
) -> Result<Decoder<'a>, StorageError> {
    let actual = ObjectId(keyed_hash(OBJECT_HASH_DOMAIN, bytes));
    if actual != expected_id {
        return Err(corruption(format!(
            "object {expected_id} failed content authentication"
        )));
    }
    let mut decoder = Decoder::after_prefix(bytes, OBJECT_MAGIC)?;
    let domain =
        u16::try_from(decoder.u32()?).map_err(|_| corruption("object domain exceeds u16"))?;
    let actual_domain = ObjectDomain::decode(domain)?;
    if actual_domain != expected_domain {
        return Err(corruption(format!(
            "object {expected_id} has domain {actual_domain:?}, expected {expected_domain:?}"
        )));
    }
    Ok(decoder)
}

pub(super) fn encode_id(encoder: &mut Encoder, id: ObjectId) {
    encoder.fixed(id.as_bytes());
}

pub(super) fn decode_id(decoder: &mut Decoder<'_>) -> Result<ObjectId, StorageError> {
    decoder.fixed().map(ObjectId)
}

pub(super) fn encode_optional_id(encoder: &mut Encoder, id: Option<ObjectId>) {
    encoder.optional_fixed(
        id.as_ref()
            .map(ObjectId::as_bytes)
            .map(<[u8; 32]>::as_slice),
    );
}

pub(super) fn decode_optional_id(
    decoder: &mut Decoder<'_>,
    label: &str,
) -> Result<Option<ObjectId>, StorageError> {
    decoder.optional_fixed(label).map(|id| id.map(ObjectId))
}
