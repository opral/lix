use crate::backend_v2::{
    BackendError, BackendRead, CoreProjection, Key, KeyRange, KeyRef, Prefix, ProjectedValueRef,
    ReadBatch, ReadEntry, ScanChunk, ScanOptions, ScanResult, ScanVisitor, SpaceId,
};
use crate::storage_v2::{
    decode_logical_key_ref, StorageReadResult, StorageReadStats, StorageSpace,
};

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
pub struct BorrowedScanChunk<'a> {
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
) -> Result<ScanChunk, BackendError>
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
) -> Result<ScanChunk, BackendError>
where
    R: BackendRead,
{
    let mut buffer = StorageScanBuffer::with_capacity(opts.limit_rows);
    let has_more = {
        let chunk = scan_range_into(read, space, range, opts, &mut buffer)?;
        chunk.has_more
    };

    Ok(ScanChunk {
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
) -> Result<BorrowedScanChunk<'a>, BackendError>
where
    R: BackendRead,
{
    buffer.clear();

    if opts.limit_rows == 0 {
        return Ok(BorrowedScanChunk {
            entries: buffer.entries(),
            has_more: false,
        });
    }

    if buffer.entries.capacity() < opts.limit_rows {
        buffer
            .entries
            .reserve(opts.limit_rows - buffer.entries.capacity());
    }

    let storage_space = StorageSpace::new(space, "storage_v2.scan");
    let resume_after = opts.resume_after;
    let physical_range = storage_space.encode_range(range, resume_after);
    let physical_opts = ScanOptions {
        resume_after: None,
        ..opts
    };

    let result = read.visit_range(
        physical_range,
        physical_opts,
        &mut |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
            let key = decode_logical_key_ref(key)?;
            buffer.entries.push(ReadEntry {
                key: key.to_owned_key(),
                value: value.to_owned(),
            });
            Ok(())
        },
    )?;

    Ok(BorrowedScanChunk {
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
    Ok(visit_scan_range_with_stats(read, space, range, opts, visitor)?.value)
}

pub(crate) fn visit_scan_range_with_stats<R, V>(
    read: &R,
    space: SpaceId,
    range: KeyRange,
    opts: ScanOptions<'_>,
    visitor: &mut V,
) -> Result<StorageReadResult<ScanResult>, BackendError>
where
    R: BackendRead,
    V: ScanVisitor + ?Sized,
{
    if opts.limit_rows == 0 {
        return Ok(StorageReadResult::new(
            ScanResult::default(),
            scan_trace_stats(ScanKind::Range, opts, 0, false, 0),
        ));
    }

    let storage_space = StorageSpace::new(space, "storage_v2.scan");
    let resume_after = opts.resume_after;
    let physical_range = storage_space.encode_range(range, resume_after);
    let physical_opts = ScanOptions {
        resume_after: None,
        ..opts
    };

    struct LogicalScanVisitor<'a, V: ?Sized> {
        inner: &'a mut V,
    }

    impl<V> ScanVisitor for LogicalScanVisitor<'_, V>
    where
        V: ScanVisitor + ?Sized,
    {
        fn visit(
            &mut self,
            key: KeyRef<'_>,
            value: ProjectedValueRef<'_>,
        ) -> Result<(), BackendError> {
            self.inner.visit(decode_logical_key_ref(key)?, value)
        }
    }

    let result = read.visit_range(
        physical_range,
        physical_opts,
        &mut LogicalScanVisitor { inner: visitor },
    )?;
    let stats = scan_trace_stats(
        ScanKind::Range,
        opts,
        result.emitted as u64,
        result.has_more,
        1,
    );
    Ok(StorageReadResult::new(result, stats))
}

pub(crate) fn scan_range_with_stats<R>(
    read: &R,
    space: SpaceId,
    range: KeyRange,
    opts: ScanOptions<'_>,
) -> Result<StorageReadResult<ScanChunk>, BackendError>
where
    R: BackendRead,
{
    let backend_calls = u64::from(opts.limit_rows != 0);
    let chunk = scan_range(read, space, range, opts)?;
    let mut stats = scan_trace_stats(
        ScanKind::Range,
        opts,
        chunk.entries.entries.len() as u64,
        chunk.has_more,
        backend_calls,
    );
    stats.prefix_lowered = 0;
    Ok(StorageReadResult::new(chunk, stats))
}

pub(crate) fn scan_prefix_into<'a, R>(
    read: &R,
    space: SpaceId,
    prefix: Prefix,
    opts: ScanOptions<'_>,
    buffer: &'a mut StorageScanBuffer,
) -> Result<BorrowedScanChunk<'a>, BackendError>
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
    Ok(visit_scan_prefix_with_stats(read, space, prefix, opts, visitor)?.value)
}

pub(crate) fn visit_scan_prefix_with_stats<R, V>(
    read: &R,
    space: SpaceId,
    prefix: Prefix,
    opts: ScanOptions<'_>,
    visitor: &mut V,
) -> Result<StorageReadResult<ScanResult>, BackendError>
where
    R: BackendRead,
    V: ScanVisitor + ?Sized,
{
    let mut result = visit_scan_range_with_stats(read, space, prefix.to_range()?, opts, visitor)?;
    result.stats.range_scan_chunks = 0;
    result.stats.prefix_scan_chunks = 1;
    result.stats.prefix_lowered = 1;
    Ok(result)
}

pub(crate) fn scan_prefix_with_stats<R>(
    read: &R,
    space: SpaceId,
    prefix: Prefix,
    opts: ScanOptions<'_>,
) -> Result<StorageReadResult<ScanChunk>, BackendError>
where
    R: BackendRead,
{
    if opts.limit_rows == 0 {
        let mut stats = scan_trace_stats(ScanKind::Prefix, opts, 0, false, 0);
        stats.prefix_lowered = 1;
        return Ok(StorageReadResult::new(
            ScanChunk {
                entries: ReadBatch::default(),
                has_more: false,
            },
            stats,
        ));
    }
    let chunk = scan_range(read, space, prefix.to_range()?, opts)?;
    let mut stats = scan_trace_stats(
        ScanKind::Prefix,
        opts,
        chunk.entries.entries.len() as u64,
        chunk.has_more,
        1,
    );
    stats.prefix_lowered = 1;
    Ok(StorageReadResult::new(chunk, stats))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanKind {
    Range,
    Prefix,
}

fn scan_trace_stats(
    kind: ScanKind,
    opts: ScanOptions<'_>,
    emitted_rows: u64,
    has_more: bool,
    backend_calls: u64,
) -> StorageReadStats {
    let (range_scan_chunks, prefix_scan_chunks) = match kind {
        ScanKind::Range => (1, 0),
        ScanKind::Prefix => (0, 1),
    };
    let (scan_key_only_chunks, scan_full_value_chunks) = match opts.projection {
        CoreProjection::KeyOnly => (1, 0),
        CoreProjection::FullValue => (0, 1),
    };
    StorageReadStats {
        requested_keys: 0,
        unique_backend_keys: 0,
        backend_calls,
        prefix_lowered: 0,
        range_scan_chunks,
        prefix_scan_chunks,
        scan_key_only_chunks,
        scan_full_value_chunks,
        scan_rows: emitted_rows,
        scan_has_more: u64::from(has_more),
        scan_resume_after: u64::from(opts.resume_after.is_some()),
        scan_limit_rows_total: opts.limit_rows as u64,
        scan_limit_rows_max: opts.limit_rows as u64,
    }
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
        let chunk = read
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

        assert!(chunk.entries.entries.is_empty());
        assert!(!chunk.has_more);
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
                lower: Bound::Included(space(1).encode_key(&Key(Bytes::new()))),
                upper: Bound::Excluded(space(2).encode_key(&Key(Bytes::new()))),
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
                lower: Bound::Included(space(1).encode_key(&key_bytes(&[0xff]))),
                upper: Bound::Excluded(space(2).encode_key(&Key(Bytes::new()))),
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
                lower: Bound::Included(space(1).encode_key(&key_bytes(&[0x00, 0xff]))),
                upper: Bound::Excluded(space(1).encode_key(&key_bytes(&[0x01]))),
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
