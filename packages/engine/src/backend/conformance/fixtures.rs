use bytes::Bytes;

use crate::backend::{Key, PutBatch, PutEntry, SpaceId, StoredValue};

pub fn space(id: u32) -> SpaceId {
    SpaceId(id)
}

pub fn key(bytes: impl AsRef<[u8]>) -> Key {
    Key(Bytes::copy_from_slice(bytes.as_ref()))
}

pub fn full_put(key: Key, value: impl Into<Bytes>) -> PutEntry {
    PutEntry {
        key,
        value: StoredValue {
            bytes: value.into(),
        },
    }
}

pub fn put_batch(entries: impl IntoIterator<Item = PutEntry>) -> PutBatch {
    PutBatch {
        entries: entries.into_iter().collect(),
    }
}
