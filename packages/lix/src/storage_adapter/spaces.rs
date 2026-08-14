use std::ops::Bound;

use bytes::{BufMut, Bytes, BytesMut};

#[cfg(test)]
use crate::storage::StorageError;
use crate::storage::{Key, KeyRange, StorageSpace, ValueSemantics};

pub(crate) const MUTATION_REVISION_SPACE: StorageSpace = StorageSpace::engine_declared(
    0x0007_0001,
    "observe.mutation_revision",
    ValueSemantics::Mutable,
);

impl StorageSpace {
    pub const fn physical_prefix(&self) -> [u8; 4] {
        self.id().to_be_bytes()
    }

    pub fn encode_key(&self, key: &Key) -> Key {
        encode_physical_key(self.id(), key)
    }

    pub fn encode_range(&self, range: KeyRange) -> KeyRange {
        encode_physical_range(self.id(), range)
    }
}

pub(crate) fn encode_physical_key(space: u32, key: &Key) -> Key {
    let mut bytes = BytesMut::with_capacity(4 + key.0.len());
    bytes.put_u32(space);
    bytes.extend_from_slice(key.0.as_ref());
    Key(bytes.freeze())
}

#[cfg(test)]
pub(crate) fn decode_logical_key(key: &Key) -> Result<Key, StorageError> {
    if key.0.len() < 4 {
        return Err(StorageError::Corruption(
            "storage physical key shorter than space prefix".into(),
        ));
    }
    Ok(Key(key.0.slice(4..)))
}

pub(crate) fn encode_physical_range(space: u32, range: KeyRange) -> KeyRange {
    let lower = match range.lower {
        Bound::Included(key) => Bound::Included(encode_physical_key(space, &key)),
        Bound::Excluded(key) => Bound::Excluded(encode_physical_key(space, &key)),
        Bound::Unbounded => Bound::Included(space_lower_bound(space)),
    };

    let upper = match range.upper {
        Bound::Included(key) => Bound::Included(encode_physical_key(space, &key)),
        Bound::Excluded(key) => Bound::Excluded(encode_physical_key(space, &key)),
        Bound::Unbounded => space_upper_bound(space),
    };

    KeyRange { lower, upper }
}

fn space_lower_bound(space: u32) -> Key {
    Key(Bytes::copy_from_slice(&space.to_be_bytes()))
}

fn space_upper_bound(space: u32) -> Bound<Key> {
    if space == u32::MAX {
        Bound::Unbounded
    } else {
        Bound::Excluded(Key(Bytes::copy_from_slice(&(space + 1).to_be_bytes())))
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::{Key, ValueSemantics};
    use crate::storage_adapter::StorageSpace;

    #[test]
    fn storage_space_preserves_id_and_name() {
        let space = StorageSpace::engine_declared(7, "test.space", ValueSemantics::Mutable);

        assert_eq!(space.id(), 7);
        assert_eq!(space.name(), "test.space");
        assert_eq!(space.to_string(), "test.space(SpaceId(7), Mutable)");
    }

    #[test]
    fn physical_keys_are_prefixed_by_space_id() {
        let space = StorageSpace::engine_declared(7, "test.space", ValueSemantics::Mutable);
        let physical = space.encode_key(&Key(bytes::Bytes::from_static(b"abc")));

        assert_eq!(physical.0.as_ref(), b"\0\0\0\x07abc");
        assert_eq!(
            super::decode_logical_key(&physical).expect("decode key"),
            Key(bytes::Bytes::from_static(b"abc"))
        );
    }

    #[test]
    fn included_logical_lower_bound_is_encoded() {
        use std::ops::Bound;

        let range = crate::storage::KeyRange {
            lower: Bound::Included(Key(bytes::Bytes::from_static(b"m"))),
            upper: Bound::Unbounded,
        };
        let encoded = super::encode_physical_range(7, range);

        assert_eq!(
            encoded.lower,
            Bound::Included(super::encode_physical_key(
                7,
                &Key(bytes::Bytes::from_static(b"m"))
            ))
        );
    }

    #[test]
    fn exclusive_logical_lower_bound_is_encoded() {
        use std::ops::Bound;

        let range = crate::storage::KeyRange {
            lower: Bound::Excluded(Key(bytes::Bytes::from_static(b"r"))),
            upper: Bound::Unbounded,
        };
        let encoded = super::encode_physical_range(7, range);

        assert_eq!(
            encoded.lower,
            Bound::Excluded(super::encode_physical_key(
                7,
                &Key(bytes::Bytes::from_static(b"r"))
            ))
        );
    }

    #[test]
    fn max_space_unbounded_range_has_unbounded_physical_upper_bound() {
        use std::ops::Bound;

        let range = crate::storage::KeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        };
        let encoded = super::encode_physical_range(u32::MAX, range);

        assert_eq!(encoded.upper, Bound::Unbounded);
    }

    #[test]
    fn non_max_space_unbounded_range_uses_next_space_exclusive_upper_bound() {
        use std::ops::Bound;

        let range = crate::storage::KeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        };
        let encoded = super::encode_physical_range(7, range);

        assert_eq!(
            encoded.upper,
            Bound::Excluded(Key(bytes::Bytes::from_static(b"\0\0\0\x08")))
        );
    }
}
