use std::ops::Bound;

use bytes::{BufMut, Bytes, BytesMut};

use crate::storage::{
    CoreProjection, GetManyRequest, GetOptions, Key, KeyRange, ProjectedValue, SpaceId,
    StorageError, StorageSpace, ValueSemantics,
};
use crate::storage_adapter::{StorageAdapterRead, exact_get_many};

/// The single storage space holding every "something changed here" revision
/// singleton.
///
/// Physical keys are `4-byte-BE space id ++ logical key`, so one space with
/// one-byte logical keys puts all revision singletons in adjacent physical
/// keys. That is one SST block, one hot-key write region, and one batched
/// point read instead of one read per singleton.
///
/// Every key holds an opaque uuid-v7 token whose only meaningful operation is
/// equality: "did this fact change since I read it".
pub(crate) const REVISION_SPACE: StorageSpace = StorageSpace::declare(
    SpaceId(0x0007_0000),
    "lix.revision.v1",
    ValueSemantics::Mutable,
);

/// Active-account visibility token. Rotated only by a commit that writes an
/// account row or moves a branch ref, so an ordinary CRUD commit leaves it
/// alone and a disabled account rotates it.
pub(crate) const REVISION_KEY_ACCOUNT: &[u8] = b"a";
/// Binary-CAS reclamation token. Rotated only by an authenticated CAS sweep,
/// and asserted unchanged by every CAS publisher, so a publisher that planned
/// against payload rows a sweep then deleted cannot commit.
pub(crate) const REVISION_KEY_BINARY_CAS_RECLAMATION: &[u8] = b"b";
/// Binary-CAS publication token. Rewritten by every CAS publisher without a
/// precondition on itself — publishers are mutually independent — and asserted
/// unchanged by every sweep, so a sweep whose reachability plan predates a
/// concurrent publication cannot commit.
pub(crate) const REVISION_KEY_BINARY_CAS_PUBLICATION: &[u8] = b"p";
/// Registered-schema catalog visibility token.
pub(crate) const REVISION_KEY_CATALOG: &[u8] = b"c";
/// Filesystem path-index cache-freshness token.
pub(crate) const REVISION_KEY_FILESYSTEM_PATH: &[u8] = b"f";
/// Physical any-storage-mutation token retained for storage-generation checks.
/// Observer invalidation compares `REVISION_KEY_OBSERVABLE` instead.
pub(crate) const REVISION_KEY_MUTATION: &[u8] = b"m";
/// Observable repository-state token. Unlike the physical mutation token, it
/// is not rotated by private operation scratch or read-interest journal writes.
pub(crate) const REVISION_KEY_OBSERVABLE: &[u8] = b"o";
/// Tracked-state mutation token used as the transaction snapshot fence.
pub(crate) const REVISION_KEY_TRACKED_MUTATION: &[u8] = b"t";

/// These adapter-owned tokens describe physical/observer bookkeeping rather
/// than repository content, so migration's portable content witnesses omit
/// them while continuing to cover every other revision-space key.
pub(crate) fn is_non_content_revision_key(space_id: u32, key: &[u8]) -> bool {
    space_id == REVISION_SPACE.id.0
        && (key == REVISION_KEY_MUTATION || key == REVISION_KEY_OBSERVABLE)
}

pub(crate) fn revision_key(key: &'static [u8]) -> Key {
    Key(Bytes::from_static(key))
}

/// Loads `N` revision singletons with one batched point read against one space.
///
/// Callers that need more than one revision in the same read scope must use
/// this instead of issuing separate point reads: the whole reason the
/// singletons share a space is that the backend can serve them from one
/// lookup over one contiguous key region.
pub(crate) async fn load_revisions<R, const N: usize>(
    read: &R,
    keys: [&'static [u8]; N],
) -> Result<[Option<Bytes>; N], StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let keys: [Key; N] = keys.map(revision_key);
    let result = exact_get_many(
        read,
        &[GetManyRequest {
            space: REVISION_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }],
    )
    .await?;
    let mut values = result.values.into_iter();
    Ok(std::array::from_fn(|_| {
        values.next().flatten().and_then(|value| match value {
            ProjectedValue::FullValue(bytes) => Some(bytes),
            ProjectedValue::KeyOnly => None,
        })
    }))
}

/// Loads exactly one revision singleton.
pub(crate) async fn load_revision<R>(
    read: &R,
    key: &'static [u8],
) -> Result<Option<Bytes>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let [value] = load_revisions(read, [key]).await?;
    Ok(value)
}

impl StorageSpace {
    pub const fn physical_prefix(&self) -> [u8; 4] {
        self.id.0.to_be_bytes()
    }

    pub fn encode_key(&self, key: &Key) -> Key {
        encode_physical_key(self.id, key)
    }

    pub fn encode_range(&self, range: KeyRange) -> KeyRange {
        encode_physical_range(self.id, range)
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

pub(crate) fn encode_physical_range(space: SpaceId, range: KeyRange) -> KeyRange {
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
        let space = StorageSpace::mutable(SpaceId(7), "test.space");

        assert_eq!(space.id, SpaceId(7));
        assert_eq!(space.name, "test.space");
        assert_eq!(space.to_string(), "test.space(SpaceId(7), Mutable)");
        assert_eq!(
            space.visibility,
            crate::storage::StorageSpaceVisibility::Observable
        );
    }

    #[test]
    fn portable_content_ignores_only_physical_and_observable_revision_tokens() {
        assert!(super::is_non_content_revision_key(
            super::REVISION_SPACE.id.0,
            super::REVISION_KEY_MUTATION
        ));
        assert!(super::is_non_content_revision_key(
            super::REVISION_SPACE.id.0,
            super::REVISION_KEY_OBSERVABLE
        ));
        for key in [
            super::REVISION_KEY_ACCOUNT,
            super::REVISION_KEY_BINARY_CAS_PUBLICATION,
            super::REVISION_KEY_BINARY_CAS_RECLAMATION,
            super::REVISION_KEY_CATALOG,
            super::REVISION_KEY_FILESYSTEM_PATH,
            super::REVISION_KEY_TRACKED_MUTATION,
        ] {
            assert!(
                !super::is_non_content_revision_key(super::REVISION_SPACE.id.0, key),
                "semantic revision key {:?} must remain witnessed",
                key
            );
        }
        assert!(!super::is_non_content_revision_key(SpaceId(7).0, b"m"));
    }

    #[test]
    fn checked_redeclarations_preserve_private_registry_visibility() {
        let declared = crate::sync::PARTIAL_READ_INTEREST_SPACE;
        let checked = StorageSpace::mutable(declared.id, declared.name);
        assert_eq!(
            checked.visibility,
            crate::storage::StorageSpaceVisibility::Private
        );
    }

    #[test]
    fn physical_keys_are_prefixed_by_space_id() {
        let space = StorageSpace::mutable(SpaceId(7), "test.space");
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
        let encoded = super::encode_physical_range(SpaceId(7), range);

        assert_eq!(
            encoded.lower,
            Bound::Included(super::encode_physical_key(
                SpaceId(7),
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
        let encoded = super::encode_physical_range(SpaceId(7), range);

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
        let encoded = super::encode_physical_range(SpaceId(u32::MAX), range);

        assert_eq!(encoded.upper, Bound::Unbounded);
    }

    #[test]
    fn non_max_space_unbounded_range_uses_next_space_exclusive_upper_bound() {
        use std::ops::Bound;

        let range = crate::storage::KeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        };
        let encoded = super::encode_physical_range(SpaceId(7), range);

        assert_eq!(
            encoded.upper,
            Bound::Excluded(Key(bytes::Bytes::from_static(b"\0\0\0\x08")))
        );
    }
}
