use std::ops::Bound;

use bytes::{BufMut, Bytes, BytesMut};

use crate::storage::{Key, KeyRange, SpaceId, StorageError};

pub(crate) const MUTATION_REVISION_SPACE: StorageSpace =
    StorageSpace::new(SpaceId(0x0007_0001), "observe.mutation_revision");

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StorageSpace {
    pub id: SpaceId,
    pub name: &'static str,
}

impl StorageSpace {
    pub const fn new(id: SpaceId, name: &'static str) -> Self {
        Self { id, name }
    }

    pub const fn physical_prefix(&self) -> [u8; 4] {
        self.id.0.to_be_bytes()
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

#[cfg(test)]
pub(crate) fn decode_logical_key(key: &Key) -> Result<Key, StorageError> {
    if key.0.len() < 4 {
        return Err(StorageError::Corruption(
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
    let range_lower = match range.lower {
        Bound::Included(key) => Bound::Included(encode_physical_key(space, &key)),
        Bound::Excluded(key) => Bound::Excluded(encode_physical_key(space, &key)),
        Bound::Unbounded => Bound::Included(space_lower_bound(space)),
    };

    let lower = match resume_after {
        Some(resume_after) => max_lower_bound(
            range_lower,
            Bound::Excluded(encode_physical_key(space, resume_after)),
        ),
        None => range_lower,
    };

    let upper = match range.upper {
        Bound::Included(key) => Bound::Included(encode_physical_key(space, &key)),
        Bound::Excluded(key) => Bound::Excluded(encode_physical_key(space, &key)),
        Bound::Unbounded => space_upper_bound(space),
    };

    KeyRange { lower, upper }
}

fn max_lower_bound(left: Bound<Key>, right: Bound<Key>) -> Bound<Key> {
    match (left, right) {
        (Bound::Unbounded, bound) | (bound, Bound::Unbounded) => bound,
        (Bound::Included(left), Bound::Included(right)) => {
            if left >= right {
                Bound::Included(left)
            } else {
                Bound::Included(right)
            }
        }
        (Bound::Included(left), Bound::Excluded(right)) => {
            if left > right {
                Bound::Included(left)
            } else {
                Bound::Excluded(right)
            }
        }
        (Bound::Excluded(left), Bound::Included(right)) => {
            if left >= right {
                Bound::Excluded(left)
            } else {
                Bound::Included(right)
            }
        }
        (Bound::Excluded(left), Bound::Excluded(right)) => {
            if left >= right {
                Bound::Excluded(left)
            } else {
                Bound::Excluded(right)
            }
        }
    }
}

fn space_lower_bound(space: SpaceId) -> Key {
    Key(Bytes::copy_from_slice(&space.0.to_be_bytes()))
}

fn space_upper_bound(space: SpaceId) -> Bound<Key> {
    if space.0 == u32::MAX {
        Bound::Unbounded
    } else {
        Bound::Excluded(Key(Bytes::copy_from_slice(&(space.0 + 1).to_be_bytes())))
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::{Key, SpaceId};
    use crate::storage_adapter::StorageSpace;

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

    #[test]
    fn resume_after_before_lower_keeps_lower_bound() {
        use std::ops::Bound;

        let range = crate::storage::KeyRange {
            lower: Bound::Included(Key(bytes::Bytes::from_static(b"m"))),
            upper: Bound::Unbounded,
        };
        let encoded = super::encode_physical_range(
            SpaceId(7),
            range,
            Some(&Key(bytes::Bytes::from_static(b"a"))),
        );

        assert_eq!(
            encoded.lower,
            Bound::Included(super::encode_physical_key(
                SpaceId(7),
                &Key(bytes::Bytes::from_static(b"m"))
            ))
        );
    }

    #[test]
    fn resume_after_inside_range_becomes_exclusive_lower_bound() {
        use std::ops::Bound;

        let range = crate::storage::KeyRange {
            lower: Bound::Included(Key(bytes::Bytes::from_static(b"m"))),
            upper: Bound::Unbounded,
        };
        let encoded = super::encode_physical_range(
            SpaceId(7),
            range,
            Some(&Key(bytes::Bytes::from_static(b"r"))),
        );

        assert_eq!(
            encoded.lower,
            Bound::Excluded(super::encode_physical_key(
                SpaceId(7),
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
        let encoded = super::encode_physical_range(SpaceId(u32::MAX), range, None);

        assert_eq!(encoded.upper, Bound::Unbounded);
    }

    #[test]
    fn non_max_space_unbounded_range_uses_next_space_exclusive_upper_bound() {
        use std::ops::Bound;

        let range = crate::storage::KeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        };
        let encoded = super::encode_physical_range(SpaceId(7), range, None);

        assert_eq!(
            encoded.upper,
            Bound::Excluded(Key(bytes::Bytes::from_static(b"\0\0\0\x08")))
        );
    }
}
