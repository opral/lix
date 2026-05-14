use bytes::Bytes;

use crate::backend_v2::{Key, PutBatch, PutEntry, SpaceId, StoredValue};

pub fn space(id: u32) -> SpaceId {
    SpaceId(id)
}

pub fn key(bytes: impl Into<Bytes>) -> Key {
    Key(bytes.into())
}

pub fn full_put(key: Key, value: impl Into<Bytes>) -> PutEntry {
    PutEntry {
        key,
        value: StoredValue::FullValue(value.into()),
    }
}

pub fn put_batch(entries: impl IntoIterator<Item = PutEntry>) -> PutBatch {
    PutBatch {
        entries: entries.into_iter().collect(),
    }
}
