use crate::backend_v2::{
    BackendError, BackendRead, Key, Prefix, ReadBatch, ScanOptions, ScanPage, SpaceId,
};
use crate::storage_v2::{StorageReadResult, StorageReadStats};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScanResumeKey {
    pub last_key: Option<Key>,
}

impl ScanResumeKey {
    pub fn start() -> Self {
        Self { last_key: None }
    }

    pub fn from_last_key(last_key: Key) -> Self {
        Self {
            last_key: Some(last_key),
        }
    }
}

pub(crate) fn scan_prefix<R>(
    read: &R,
    space: SpaceId,
    prefix: Prefix,
    opts: ScanOptions<'_>,
) -> Result<ScanPage, BackendError>
where
    R: BackendRead,
{
    Ok(scan_prefix_with_stats(read, space, prefix, opts)?.value)
}

pub(crate) fn scan_prefix_with_stats<R>(
    read: &R,
    space: SpaceId,
    prefix: Prefix,
    opts: ScanOptions<'_>,
) -> Result<StorageReadResult<ScanPage>, BackendError>
where
    R: BackendRead,
{
    if opts.limit_rows == 0 {
        return Ok(StorageReadResult::new(
            ScanPage {
                entries: ReadBatch::default(),
                has_more: false,
            },
            StorageReadStats {
                requested_keys: 0,
                unique_backend_keys: 0,
                backend_calls: 0,
                prefix_lowered: 1,
            },
        ));
    }
    let page = read.scan_range(space, prefix.to_range()?, opts)?;
    Ok(StorageReadResult::new(
        page,
        StorageReadStats {
            requested_keys: 0,
            unique_backend_keys: 0,
            backend_calls: 1,
            prefix_lowered: 1,
        },
    ))
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::ops::Bound;

    use bytes::Bytes;

    use crate::backend_v2::{
        BackendError, BackendRead, ConformanceBackend, GetManyResult, GetOptions, Key, KeyRange,
        Prefix, ReadBatch, ReadOptions, ScanOptions, ScanPage, SpaceId, StoredValue, WriteOptions,
    };
    use crate::storage_v2::{scan_prefix, StorageContext, StorageReader, StorageSpace};

    fn key(bytes: &'static str) -> Key {
        Key(Bytes::from_static(bytes.as_bytes()))
    }

    fn key_bytes(bytes: &'static [u8]) -> Key {
        Key(Bytes::from_static(bytes))
    }

    fn value(bytes: &'static str) -> StoredValue {
        StoredValue {
            bytes: Bytes::from_static(bytes.as_bytes()),
        }
    }

    fn space(id: u32) -> StorageSpace {
        match id {
            1 => StorageSpace::new(SpaceId(1), "test.space.one"),
            _ => StorageSpace::new(SpaceId(id), "test.space.other"),
        }
    }

    #[test]
    fn prefix_scan_limit_zero_returns_empty_page() {
        let storage = StorageContext::new(ConformanceBackend::new());
        let mut writes = storage.new_write_set();
        writes.stage_put(space(1), key("aa"), value("AA"));
        storage
            .commit_write_set(writes, WriteOptions::default())
            .expect("seed");

        let read = storage
            .begin_read(ReadOptions::default())
            .expect("begin read");
        let page = read
            .scan_prefix(
                space(1),
                Prefix {
                    bytes: Bytes::from_static(b"a"),
                },
                ScanOptions {
                    limit_rows: 0,
                    ..ScanOptions::default()
                },
            )
            .expect("prefix scan");

        assert!(page.entries.entries.is_empty());
        assert!(!page.has_more);
    }

    #[test]
    fn prefix_scan_lowers_empty_prefix_to_unbounded_upper_range() {
        let read = CapturingRead::default();

        scan_prefix(
            &read,
            SpaceId(1),
            Prefix {
                bytes: Bytes::new(),
            },
            ScanOptions::default(),
        )
        .expect("scan prefix");

        assert_eq!(
            read.take_range(),
            KeyRange {
                lower: Bound::Included(Key(Bytes::new())),
                upper: Bound::Unbounded,
            }
        );
    }

    #[test]
    fn prefix_scan_lowers_ff_prefix_to_unbounded_upper_range() {
        let read = CapturingRead::default();

        scan_prefix(
            &read,
            SpaceId(1),
            Prefix {
                bytes: Bytes::from_static(&[0xff]),
            },
            ScanOptions::default(),
        )
        .expect("scan prefix");

        assert_eq!(
            read.take_range(),
            KeyRange {
                lower: Bound::Included(key_bytes(&[0xff])),
                upper: Bound::Unbounded,
            }
        );
    }

    #[test]
    fn prefix_scan_lowers_trailing_ff_prefix_to_next_lexicographic_bound() {
        let read = CapturingRead::default();

        scan_prefix(
            &read,
            SpaceId(1),
            Prefix {
                bytes: Bytes::from_static(&[0x00, 0xff]),
            },
            ScanOptions::default(),
        )
        .expect("scan prefix");

        assert_eq!(
            read.take_range(),
            KeyRange {
                lower: Bound::Included(key_bytes(&[0x00, 0xff])),
                upper: Bound::Excluded(key_bytes(&[0x01])),
            }
        );
    }

    #[derive(Default)]
    struct CapturingRead {
        range: RefCell<Option<KeyRange>>,
    }

    impl CapturingRead {
        fn take_range(&self) -> KeyRange {
            self.range
                .borrow_mut()
                .take()
                .expect("scan_range should have been called")
        }
    }

    impl BackendRead for CapturingRead {
        fn get_many(
            &self,
            _space: SpaceId,
            _keys: &[Key],
            _opts: GetOptions<'_>,
        ) -> Result<GetManyResult, BackendError> {
            unimplemented!("not used by prefix lowering tests")
        }

        fn scan_range(
            &self,
            _space: SpaceId,
            range: KeyRange,
            _opts: ScanOptions<'_>,
        ) -> Result<ScanPage, BackendError> {
            self.range.replace(Some(range));
            Ok(ScanPage {
                entries: ReadBatch::default(),
                has_more: false,
            })
        }
    }
}
