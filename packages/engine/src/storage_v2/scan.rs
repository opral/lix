use crate::backend_v2::{
    BackendError, BackendRead, Key, KeyRange, KeyRef, Prefix, ProjectedValueRef, ReadBatch,
    ReadEntry, ScanOptions, ScanPage, ScanResult, ScanVisitor, SpaceId,
};
use crate::storage_v2::{StorageReadResult, StorageReadStats};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StorageScanBuffer {
    entries: Vec<ReadEntry>,
}

impl StorageScanBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
        }
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn entries(&self) -> &[ReadEntry] {
        &self.entries
    }

    pub fn capacity(&self) -> usize {
        self.entries.capacity()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BorrowedScanPage<'a> {
    pub entries: &'a [ReadEntry],
    pub has_more: bool,
}

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

pub(crate) fn scan_range<R>(
    read: &R,
    space: SpaceId,
    range: KeyRange,
    opts: ScanOptions<'_>,
) -> Result<ScanPage, BackendError>
where
    R: BackendRead,
{
    let mut buffer = StorageScanBuffer::with_capacity(opts.limit_rows);
    let has_more = {
        let page = scan_range_into(read, space, range, opts, &mut buffer)?;
        page.has_more
    };

    Ok(ScanPage {
        entries: ReadBatch {
            entries: buffer.entries,
        },
        has_more,
    })
}

pub(crate) fn scan_range_into<'a, R>(
    read: &R,
    space: SpaceId,
    range: KeyRange,
    opts: ScanOptions<'_>,
    buffer: &'a mut StorageScanBuffer,
) -> Result<BorrowedScanPage<'a>, BackendError>
where
    R: BackendRead,
{
    buffer.clear();

    if opts.limit_rows == 0 {
        return Ok(BorrowedScanPage {
            entries: buffer.entries(),
            has_more: false,
        });
    }

    if buffer.entries.capacity() < opts.limit_rows {
        buffer
            .entries
            .reserve(opts.limit_rows - buffer.entries.capacity());
    }

    let result = read.visit_range(
        space,
        range,
        opts,
        &mut |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
            buffer.entries.push(ReadEntry {
                key: key.to_owned_key(),
                value: value.to_owned(),
            });
            Ok(())
        },
    )?;

    Ok(BorrowedScanPage {
        entries: buffer.entries(),
        has_more: result.has_more,
    })
}

pub(crate) fn visit_scan_range<R, V>(
    read: &R,
    space: SpaceId,
    range: KeyRange,
    opts: ScanOptions<'_>,
    visitor: &mut V,
) -> Result<ScanResult, BackendError>
where
    R: BackendRead,
    V: ScanVisitor + ?Sized,
{
    if opts.limit_rows == 0 {
        return Ok(ScanResult::default());
    }

    read.visit_range(space, range, opts, visitor)
}

pub(crate) fn scan_range_with_stats<R>(
    read: &R,
    space: SpaceId,
    range: KeyRange,
    opts: ScanOptions<'_>,
) -> Result<StorageReadResult<ScanPage>, BackendError>
where
    R: BackendRead,
{
    let backend_calls = u64::from(opts.limit_rows != 0);
    let page = scan_range(read, space, range, opts)?;
    Ok(StorageReadResult::new(
        page,
        StorageReadStats {
            requested_keys: 0,
            unique_backend_keys: 0,
            backend_calls,
            prefix_lowered: 0,
        },
    ))
}

pub(crate) fn scan_prefix_into<'a, R>(
    read: &R,
    space: SpaceId,
    prefix: Prefix,
    opts: ScanOptions<'_>,
    buffer: &'a mut StorageScanBuffer,
) -> Result<BorrowedScanPage<'a>, BackendError>
where
    R: BackendRead,
{
    scan_range_into(read, space, prefix.to_range()?, opts, buffer)
}

pub(crate) fn visit_scan_prefix<R, V>(
    read: &R,
    space: SpaceId,
    prefix: Prefix,
    opts: ScanOptions<'_>,
    visitor: &mut V,
) -> Result<ScanResult, BackendError>
where
    R: BackendRead,
    V: ScanVisitor + ?Sized,
{
    visit_scan_range(read, space, prefix.to_range()?, opts, visitor)
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
    let page = scan_range(read, space, prefix.to_range()?, opts)?;
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
        BackendError, BackendRead, ConformanceBackend, GetOptions, Key, KeyRange, PointVisitor,
        Prefix, ProjectedValueRef, ReadOptions, ScanOptions, ScanResult, ScanVisitor, SpaceId,
        StoredValue, WriteOptions,
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
        fn visit_many<V>(
            &self,
            _space: SpaceId,
            _keys: &[Key],
            _opts: GetOptions<'_>,
            _visitor: &mut V,
        ) -> Result<(), BackendError>
        where
            V: PointVisitor + ?Sized,
        {
            unimplemented!("not used by prefix lowering tests")
        }

        fn visit_range<V>(
            &self,
            _space: SpaceId,
            range: KeyRange,
            _opts: ScanOptions<'_>,
            _visitor: &mut V,
        ) -> Result<ScanResult, BackendError>
        where
            V: ScanVisitor + ?Sized,
        {
            self.range.replace(Some(range));
            Ok(ScanResult::default())
        }
    }
}
