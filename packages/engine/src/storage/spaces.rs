use std::ops::Bound;

use bytes::{BufMut, Bytes, BytesMut};

use crate::backend::{BackendError, Key, KeyRange, KeyRef, SpaceId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StorageSpace {
    pub id: SpaceId,
    pub name: &'static str,
}

impl StorageSpace {
    pub const fn new(id: SpaceId, name: &'static str) -> Self {
        Self { id, name }
    }

    pub fn encode_key(&self, key: &Key) -> Key {
        encode_physical_key(self.id, key)
    }

    pub fn encode_range(&self, range: KeyRange, resume_after: Option<&Key>) -> KeyRange {
        encode_physical_range(self.id, range, resume_after)
    }
}

impl std::fmt::Display for StorageSpace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({:?})", self.name, self.id)
    }
}

pub(crate) fn encode_physical_key(space: SpaceId, key: &Key) -> Key {
    let mut bytes = BytesMut::with_capacity(4 + key.0.len());
    bytes.put_u32(space.0);
    bytes.extend_from_slice(key.0.as_ref());
    Key(bytes.freeze())
}

pub(crate) fn decode_logical_key_ref(key: KeyRef<'_>) -> Result<KeyRef<'_>, BackendError> {
    if key.0.len() < 4 {
        return Err(BackendError::Corruption(
            "storage physical key shorter than space prefix".into(),
        ));
    }
    Ok(KeyRef(&key.0[4..]))
}

pub(crate) fn decode_logical_key(key: &Key) -> Result<Key, BackendError> {
    if key.0.len() < 4 {
        return Err(BackendError::Corruption(
            "storage physical key shorter than space prefix".into(),
        ));
    }
    Ok(Key(key.0.slice(4..)))
}

pub(crate) fn encode_physical_range(
    space: SpaceId,
    range: KeyRange,
    resume_after: Option<&Key>,
) -> KeyRange {
    let lower = match (range.lower, resume_after) {
        (_, Some(resume_after)) => Bound::Excluded(encode_physical_key(space, resume_after)),
        (Bound::Included(key), None) => Bound::Included(encode_physical_key(space, &key)),
        (Bound::Excluded(key), None) => Bound::Excluded(encode_physical_key(space, &key)),
        (Bound::Unbounded, None) => Bound::Included(space_lower_bound(space)),
    };

    let upper = match range.upper {
        Bound::Included(key) => Bound::Included(encode_physical_key(space, &key)),
        Bound::Excluded(key) => Bound::Excluded(encode_physical_key(space, &key)),
        Bound::Unbounded => Bound::Excluded(space_upper_bound(space)),
    };

    KeyRange { lower, upper }
}

fn space_lower_bound(space: SpaceId) -> Key {
    Key(Bytes::copy_from_slice(&space.0.to_be_bytes()))
}

fn space_upper_bound(space: SpaceId) -> Key {
    if space.0 == u32::MAX {
        Key(Bytes::from_static(b"\xff\xff\xff\xff\xff"))
    } else {
        Key(Bytes::copy_from_slice(&(space.0 + 1).to_be_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use crate::backend::{Key, SpaceId};
    use crate::storage::StorageSpace;

    #[test]
    fn storage_space_preserves_id_and_name() {
        let space = StorageSpace::new(SpaceId(7), "test.space");

        assert_eq!(space.id, SpaceId(7));
        assert_eq!(space.name, "test.space");
        assert_eq!(space.to_string(), "test.space(SpaceId(7))");
    }

    #[test]
    fn physical_keys_are_prefixed_by_space_id() {
        let space = StorageSpace::new(SpaceId(7), "test.space");
        let physical = space.encode_key(&Key(bytes::Bytes::from_static(b"abc")));

        assert_eq!(physical.0.as_ref(), b"\0\0\0\x07abc");
        assert_eq!(
            super::decode_logical_key(&physical).expect("decode key"),
            Key(bytes::Bytes::from_static(b"abc"))
        );
    }
}
