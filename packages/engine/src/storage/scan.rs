use crate::backend::{
    BackendError, BackendRead, CoreProjection, Key, KeyRange, KeyRef, Prefix, ProjectedValueRef,
    ReadEntry, ScanChunk, ScanOptions, ScanResult, ScanVisitor, SpaceId,
    visit_range as backend_visit_range,
};
use crate::storage::{
    StorageRead, StorageReadResult, StorageReadStats, StorageSpace, decode_logical_key_ref,
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScanBuffer {
    entries: Vec<ReadEntry>,
    last_physical_key: Vec<u8>,
}

impl ScanBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            last_physical_key: Vec::new(),
        }
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.last_physical_key.clear();
    }

    pub fn entries(&self) -> &[ReadEntry] {
        &self.entries
    }

    pub fn capacity(&self) -> usize {
        self.entries.capacity()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanChunkRef<'a> {
    pub entries: &'a [ReadEntry],
    pub has_more: bool,
}

pub struct ScanCursor<'a, C> {
    inner: &'a mut C,
    kind: ScanKind,
    physical_range: KeyRange,
    projection: CoreProjection,
    chunks_seen: u64,
    last_physical_key: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanPlan {
    space: StorageSpace,
    kind: ScanPlanKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ScanPlanKind {
    Range(KeyRange),
    Prefix(Prefix),
}

impl ScanPlan {
    pub fn range(space: StorageSpace, range: KeyRange) -> Self {
        Self {
            space,
            kind: ScanPlanKind::Range(range),
        }
    }

    pub fn prefix(space: StorageSpace, prefix: Prefix) -> Self {
        Self {
            space,
            kind: ScanPlanKind::Prefix(prefix),
        }
    }

    pub fn collect<R>(
        &self,
        read: &R,
        opts: ScanOptions<'_>,
    ) -> Result<StorageReadResult<ScanChunk>, BackendError>
    where
        R: StorageRead + ?Sized,
    {
        match &self.kind {
            ScanPlanKind::Range(range) => {
                scan_range_with_stats(read.backend_read(), self.space.id, range.clone(), opts)
            }
            ScanPlanKind::Prefix(prefix) => {
                scan_prefix_with_stats(read.backend_read(), self.space.id, prefix.clone(), opts)
            }
        }
    }

    pub fn collect_into<'a, R>(
        &self,
        read: &R,
        opts: ScanOptions<'_>,
        buffer: &'a mut ScanBuffer,
    ) -> Result<StorageReadResult<ScanChunkRef<'a>>, BackendError>
    where
        R: StorageRead + ?Sized,
    {
        let chunk = match &self.kind {
            ScanPlanKind::Range(range) => scan_range_into(
                read.backend_read(),
                self.space.id,
                range.clone(),
                opts,
                buffer,
            )?,
            ScanPlanKind::Prefix(prefix) => scan_prefix_into(
                read.backend_read(),
                self.space.id,
                prefix.clone(),
                opts,
                buffer,
            )?,
        };
        let backend_calls = u64::from(opts.limit_rows != 0);
        let kind = match self.kind {
            ScanPlanKind::Range(_) => ScanKind::Range,
            ScanPlanKind::Prefix(_) => ScanKind::Prefix,
        };
        let mut stats = scan_trace_stats(
            kind,
            opts,
            chunk.entries.len() as u64,
            chunk.has_more,
            backend_calls,
        );
        if matches!(kind, ScanKind::Prefix) {
            stats.prefix_lowered = 1;
        }
        Ok(StorageReadResult::new(chunk, stats))
    }

    pub fn visit<R, V>(
        &self,
        read: &R,
        opts: ScanOptions<'_>,
        visitor: &mut V,
    ) -> Result<StorageReadResult<ScanResult>, BackendError>
    where
        R: StorageRead + ?Sized,
        V: ScanVisitor + ?Sized,
    {
        match &self.kind {
            ScanPlanKind::Range(range) => visit_scan_range_with_stats(
                read.backend_read(),
                self.space.id,
                range.clone(),
                opts,
                visitor,
            ),
            ScanPlanKind::Prefix(prefix) => visit_scan_prefix_with_stats(
                read.backend_read(),
                self.space.id,
                prefix.clone(),
                opts,
                visitor,
            ),
        }
    }

    pub fn cursor<R, T, F>(&self, read: &R, opts: ScanOptions<'_>, f: F) -> Result<T, BackendError>
    where
        R: StorageRead + ?Sized,
        F: FnOnce(
            &mut ScanCursor<'_, <R::BackendRead as BackendRead>::RangeScan<'_>>,
        ) -> Result<T, BackendError>,
    {
        match &self.kind {
            ScanPlanKind::Range(range) => {
                with_range_scan(read.backend_read(), self.space.id, range.clone(), opts, f)
            }
            ScanPlanKind::Prefix(prefix) => {
                with_prefix_scan(read.backend_read(), self.space.id, prefix.clone(), opts, f)
            }
        }
    }
}

impl<C> ScanCursor<'_, C>
where
    C: crate::backend::BackendRangeScan,
{
    pub fn visit_next(
        &mut self,
        limit_rows: usize,
        visitor: &mut dyn ScanVisitor,
    ) -> Result<ScanResult, BackendError> {
        Ok(self.visit_next_with_stats(limit_rows, visitor)?.value)
    }

    pub fn visit_next_with_stats<V>(
        &mut self,
        limit_rows: usize,
        visitor: &mut V,
    ) -> Result<StorageReadResult<ScanResult>, BackendError>
    where
        V: ScanVisitor + ?Sized,
    {
        struct LogicalScanVisitor<'a, V: ?Sized> {
            physical_range: &'a KeyRange,
            last_physical_key: &'a mut Vec<u8>,
            emitted: usize,
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
                validate_scan_physical_key(self.physical_range, self.last_physical_key, key)?;
                self.emitted += 1;
                self.inner.visit(decode_logical_key_ref(key)?, value)
            }
        }

        let mut logical_visitor = LogicalScanVisitor {
            physical_range: &self.physical_range,
            last_physical_key: &mut self.last_physical_key,
            emitted: 0,
            inner: visitor,
        };
        let result = self.inner.visit_next(limit_rows, &mut logical_visitor)?;
        validate_scan_result_count(result, logical_visitor.emitted, limit_rows)?;
        let mut stats = scan_trace_stats(
            self.kind,
            ScanOptions {
                projection: self.projection,
                limit_rows,
                resume_after: None,
            },
            result.emitted as u64,
            result.has_more,
            u64::from(limit_rows != 0),
        );
        stats.scan_resume_after = u64::from(self.chunks_seen > 0);
        if matches!(self.kind, ScanKind::Prefix) {
            stats.prefix_lowered = u64::from(self.chunks_seen == 0);
        }
        self.chunks_seen += u64::from(result.emitted > 0 || result.has_more);
        Ok(StorageReadResult::new(result, stats))
    }
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

pub(crate) fn with_range_scan<R, T, F>(
    read: &R,
    space: SpaceId,
    range: KeyRange,
    opts: ScanOptions<'_>,
    f: F,
) -> Result<T, BackendError>
where
    R: BackendRead,
    F: FnOnce(&mut ScanCursor<'_, R::RangeScan<'_>>) -> Result<T, BackendError>,
{
    let storage_space = StorageSpace::new(space, "storage.scan");
    let resume_after = opts.resume_after;
    let physical_range = storage_space.encode_range(range, resume_after);
    let physical_opts = ScanOptions {
        resume_after: None,
        ..opts
    };
    let backend_range = physical_range.clone();
    read.with_range_scan(backend_range, physical_opts, |backend_cursor| {
        let mut cursor = ScanCursor {
            inner: backend_cursor,
            kind: ScanKind::Range,
            physical_range,
            projection: opts.projection,
            chunks_seen: 0,
            last_physical_key: resume_after
                .map(|key| storage_space.encode_key(key).0.to_vec())
                .unwrap_or_default(),
        };
        f(&mut cursor)
    })
}

pub(crate) fn with_prefix_scan<R, T, F>(
    read: &R,
    space: SpaceId,
    prefix: Prefix,
    opts: ScanOptions<'_>,
    f: F,
) -> Result<T, BackendError>
where
    R: BackendRead,
    F: FnOnce(&mut ScanCursor<'_, R::RangeScan<'_>>) -> Result<T, BackendError>,
{
    with_range_scan(read, space, prefix.to_range()?, opts, |cursor| {
        cursor.kind = ScanKind::Prefix;
        f(cursor)
    })
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
    let mut buffer = ScanBuffer::with_capacity(opts.limit_rows);
    let has_more = {
        let chunk = scan_range_into(read, space, range, opts, &mut buffer)?;
        chunk.has_more
    };

    Ok(ScanChunk {
        entries: buffer.entries,
        has_more,
    })
}

pub(crate) fn scan_range_into<'a, R>(
    read: &R,
    space: SpaceId,
    range: KeyRange,
    opts: ScanOptions<'_>,
    buffer: &'a mut ScanBuffer,
) -> Result<ScanChunkRef<'a>, BackendError>
where
    R: BackendRead,
{
    buffer.clear();

    if opts.limit_rows == 0 {
        return Ok(ScanChunkRef {
            entries: buffer.entries(),
            has_more: false,
        });
    }

    if buffer.entries.capacity() < opts.limit_rows {
        buffer
            .entries
            .reserve(opts.limit_rows - buffer.entries.capacity());
    }

    let storage_space = StorageSpace::new(space, "storage.scan");
    let resume_after = opts.resume_after;
    let physical_range = storage_space.encode_range(range, resume_after);
    let physical_opts = ScanOptions {
        resume_after: None,
        ..opts
    };
    let mut emitted = 0usize;

    let result = backend_visit_range(
        read,
        physical_range.clone(),
        physical_opts,
        &mut |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
            validate_scan_physical_key(&physical_range, &mut buffer.last_physical_key, key)?;
            emitted += 1;
            let key = decode_logical_key_ref(key)?;
            buffer.entries.push(ReadEntry {
                key: key.to_owned_key(),
                value: value.to_owned(),
            });
            Ok(())
        },
    )?;
    validate_scan_result_count(result, emitted, opts.limit_rows)?;

    Ok(ScanChunkRef {
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

    let storage_space = StorageSpace::new(space, "storage.scan");
    let resume_after = opts.resume_after;
    let physical_range = storage_space.encode_range(range, resume_after);
    let physical_opts = ScanOptions {
        resume_after: None,
        ..opts
    };

    struct LogicalScanVisitor<'a, V: ?Sized> {
        physical_range: &'a KeyRange,
        last_physical_key: &'a mut Vec<u8>,
        emitted: usize,
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
            validate_scan_physical_key(self.physical_range, self.last_physical_key, key)?;
            self.emitted += 1;
            self.inner.visit(decode_logical_key_ref(key)?, value)
        }
    }

    let mut last_physical_key = resume_after
        .map(|key| storage_space.encode_key(key).0.to_vec())
        .unwrap_or_default();
    let mut logical_visitor = LogicalScanVisitor {
        physical_range: &physical_range,
        last_physical_key: &mut last_physical_key,
        emitted: 0,
        inner: visitor,
    };
    let result = backend_visit_range(
        read,
        physical_range.clone(),
        physical_opts,
        &mut logical_visitor,
    )?;
    validate_scan_result_count(result, logical_visitor.emitted, opts.limit_rows)?;
    let stats = scan_trace_stats(
        ScanKind::Range,
        opts,
        result.emitted as u64,
        result.has_more,
        1,
    );
    Ok(StorageReadResult::new(result, stats))
}

fn validate_scan_physical_key(
    physical_range: &KeyRange,
    last_physical_key: &mut Vec<u8>,
    key: KeyRef<'_>,
) -> Result<(), BackendError> {
    if !range_contains_ref(physical_range, key) {
        return Err(BackendError::Corruption(
            "scan backend emitted key outside requested range".to_string(),
        ));
    }
    if !last_physical_key.is_empty() && last_physical_key.as_slice() >= key.0 {
        return Err(BackendError::Corruption(
            "scan backend emitted keys out of strict ascending order".to_string(),
        ));
    }

    last_physical_key.clear();
    last_physical_key.extend_from_slice(key.0);
    Ok(())
}

fn validate_scan_result_count(
    result: ScanResult,
    visited: usize,
    limit_rows: usize,
) -> Result<(), BackendError> {
    if result.emitted != visited {
        return Err(BackendError::Corruption(format!(
            "scan backend reported {} emitted rows after visiting {visited} rows",
            result.emitted
        )));
    }
    if result.emitted > limit_rows {
        return Err(BackendError::Corruption(format!(
            "scan backend emitted {} rows for limit {limit_rows}",
            result.emitted
        )));
    }
    if limit_rows != 0 && result.emitted == 0 && result.has_more {
        return Err(BackendError::Corruption(
            "scan backend reported more rows after emitting no rows".to_string(),
        ));
    }
    Ok(())
}

fn range_contains_ref(range: &KeyRange, key: KeyRef<'_>) -> bool {
    let lower_ok = match &range.lower {
        std::ops::Bound::Included(lower) => key >= lower.as_ref(),
        std::ops::Bound::Excluded(lower) => key > lower.as_ref(),
        std::ops::Bound::Unbounded => true,
    };
    let upper_ok = match &range.upper {
        std::ops::Bound::Included(upper) => key <= upper.as_ref(),
        std::ops::Bound::Excluded(upper) => key < upper.as_ref(),
        std::ops::Bound::Unbounded => true,
    };
    lower_ok && upper_ok
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
        chunk.entries.len() as u64,
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
    buffer: &'a mut ScanBuffer,
) -> Result<ScanChunkRef<'a>, BackendError>
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
                entries: Vec::new(),
                has_more: false,
            },
            stats,
        ));
    }
    let chunk = scan_range(read, space, prefix.to_range()?, opts)?;
    let mut stats = scan_trace_stats(
        ScanKind::Prefix,
        opts,
        chunk.entries.len() as u64,
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

    use super::scan_prefix;
    use crate::backend::{
        BackendError, BackendRangeScan, BackendRead, BufferedRangeScan, GetOptions,
        InMemoryBackend, Key, KeyRange, KeyRef, PointVisitor, Prefix, ProjectedValue,
        ProjectedValueRef, ReadEntry, ReadOptions, ScanOptions, ScanResult, ScanVisitor, SpaceId,
        StoredValue, WriteOptions,
    };
    use crate::storage::{ScanBuffer, ScanPlan, StorageContext, StorageReadScope, StorageSpace};

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

    fn projected_value(bytes: &'static str) -> ProjectedValue {
        ProjectedValue::FullValue(Bytes::from_static(bytes.as_bytes()))
    }

    fn space(id: u32) -> StorageSpace {
        match id {
            1 => StorageSpace::new(SpaceId(1), "test.space.one"),
            _ => StorageSpace::new(SpaceId(id), "test.space.other"),
        }
    }

    #[test]
    fn prefix_scan_limit_zero_returns_empty_page() {
        let storage = StorageContext::new(InMemoryBackend::new());
        let mut writes = storage.new_write_set();
        writes.put(space(1), key("aa"), value("AA"));
        storage
            .commit_write_set(writes, WriteOptions::default())
            .expect("seed");

        let read = storage
            .begin_read(ReadOptions::default())
            .expect("begin read");
        let chunk = ScanPlan::prefix(
            space(1),
            Prefix {
                bytes: Bytes::from_static(b"a"),
            },
        )
        .collect(
            &read,
            ScanOptions {
                limit_rows: 0,
                ..ScanOptions::default()
            },
        )
        .expect("prefix scan");

        assert!(chunk.value.entries.is_empty());
        assert!(!chunk.value.has_more);
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

    #[test]
    fn scan_collect_rejects_backend_row_outside_requested_range() {
        let read = StorageReadScope::new(BrokenScanRead {
            rows: vec![ReadEntry {
                key: space(2).encode_key(&key("b")),
                value: projected_value("B"),
            }],
            emitted_override: None,
            has_more_override: None,
        });

        let error = ScanPlan::range(
            space(1),
            KeyRange {
                lower: Bound::Included(key("a")),
                upper: Bound::Excluded(key("z")),
            },
        )
        .collect(&read, ScanOptions::default())
        .expect_err("out-of-range backend scan row should be rejected");

        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("emitted key outside requested range")
        ));
    }

    #[test]
    fn scan_collect_rejects_unordered_backend_rows() {
        let read = StorageReadScope::new(BrokenScanRead {
            rows: vec![
                ReadEntry {
                    key: space(1).encode_key(&key("b")),
                    value: projected_value("B"),
                },
                ReadEntry {
                    key: space(1).encode_key(&key("a")),
                    value: projected_value("A"),
                },
            ],
            emitted_override: None,
            has_more_override: None,
        });

        let error = ScanPlan::range(
            space(1),
            KeyRange {
                lower: Bound::Included(key("a")),
                upper: Bound::Excluded(key("z")),
            },
        )
        .collect(&read, ScanOptions::default())
        .expect_err("unordered backend scan rows should be rejected");

        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("out of strict ascending order")
        ));
    }

    #[test]
    fn scan_collect_rejects_duplicate_backend_rows() {
        let duplicate_key = space(1).encode_key(&key("a"));
        let read = StorageReadScope::new(BrokenScanRead {
            rows: vec![
                ReadEntry {
                    key: duplicate_key.clone(),
                    value: projected_value("A1"),
                },
                ReadEntry {
                    key: duplicate_key,
                    value: projected_value("A2"),
                },
            ],
            emitted_override: None,
            has_more_override: None,
        });

        let error = ScanPlan::range(
            space(1),
            KeyRange {
                lower: Bound::Included(key("a")),
                upper: Bound::Excluded(key("z")),
            },
        )
        .collect(&read, ScanOptions::default())
        .expect_err("duplicate backend scan rows should be rejected");

        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("out of strict ascending order")
        ));
    }

    #[test]
    fn scan_visit_rejects_backend_emitted_count_mismatch() {
        let read = StorageReadScope::new(BrokenScanRead {
            rows: vec![ReadEntry {
                key: space(1).encode_key(&key("a")),
                value: projected_value("A"),
            }],
            emitted_override: Some(0),
            has_more_override: None,
        });

        let error = ScanPlan::range(
            space(1),
            KeyRange {
                lower: Bound::Included(key("a")),
                upper: Bound::Excluded(key("z")),
            },
        )
        .visit(
            &read,
            ScanOptions::default(),
            &mut |_key: KeyRef<'_>, _value: ProjectedValueRef<'_>| Ok(()),
        )
        .expect_err("lying backend scan emitted count should be rejected");

        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("reported 0 emitted rows after visiting 1 rows")
        ));
    }

    #[test]
    fn scan_cursor_rejects_backend_row_outside_requested_range() {
        let read = StorageReadScope::new(BrokenScanRead {
            rows: vec![ReadEntry {
                key: space(2).encode_key(&key("b")),
                value: projected_value("B"),
            }],
            emitted_override: None,
            has_more_override: None,
        });

        let error = ScanPlan::range(
            space(1),
            KeyRange {
                lower: Bound::Included(key("a")),
                upper: Bound::Excluded(key("z")),
            },
        )
        .cursor(&read, ScanOptions::default(), |cursor| {
            cursor.visit_next(
                10,
                &mut |_key: KeyRef<'_>, _value: ProjectedValueRef<'_>| Ok(()),
            )
        })
        .expect_err("cursor scan should reject out-of-range backend row");

        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("emitted key outside requested range")
        ));
    }

    #[test]
    fn scan_collect_into_rejects_unordered_backend_rows() {
        let read = StorageReadScope::new(BrokenScanRead {
            rows: vec![
                ReadEntry {
                    key: space(1).encode_key(&key("b")),
                    value: projected_value("B"),
                },
                ReadEntry {
                    key: space(1).encode_key(&key("a")),
                    value: projected_value("A"),
                },
            ],
            emitted_override: None,
            has_more_override: None,
        });
        let mut buffer = ScanBuffer::new();

        let error = ScanPlan::range(
            space(1),
            KeyRange {
                lower: Bound::Included(key("a")),
                upper: Bound::Excluded(key("z")),
            },
        )
        .collect_into(&read, ScanOptions::default(), &mut buffer)
        .expect_err("collect_into should reject unordered backend rows");

        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("out of strict ascending order")
        ));
    }

    #[test]
    fn scan_visit_rejects_duplicate_backend_rows() {
        let duplicate_key = space(1).encode_key(&key("a"));
        let read = StorageReadScope::new(BrokenScanRead {
            rows: vec![
                ReadEntry {
                    key: duplicate_key.clone(),
                    value: projected_value("A1"),
                },
                ReadEntry {
                    key: duplicate_key,
                    value: projected_value("A2"),
                },
            ],
            emitted_override: None,
            has_more_override: None,
        });

        let error = ScanPlan::range(
            space(1),
            KeyRange {
                lower: Bound::Included(key("a")),
                upper: Bound::Excluded(key("z")),
            },
        )
        .visit(
            &read,
            ScanOptions::default(),
            &mut |_key: KeyRef<'_>, _value: ProjectedValueRef<'_>| Ok(()),
        )
        .expect_err("visit should reject duplicate backend rows");

        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("out of strict ascending order")
        ));
    }

    #[test]
    fn scan_cursor_rejects_duplicate_backend_rows() {
        let duplicate_key = space(1).encode_key(&key("a"));
        let read = StorageReadScope::new(BrokenScanRead {
            rows: vec![
                ReadEntry {
                    key: duplicate_key.clone(),
                    value: projected_value("A1"),
                },
                ReadEntry {
                    key: duplicate_key,
                    value: projected_value("A2"),
                },
            ],
            emitted_override: None,
            has_more_override: None,
        });

        let error = ScanPlan::range(
            space(1),
            KeyRange {
                lower: Bound::Included(key("a")),
                upper: Bound::Excluded(key("z")),
            },
        )
        .cursor(&read, ScanOptions::default(), |cursor| {
            cursor.visit_next(
                10,
                &mut |_key: KeyRef<'_>, _value: ProjectedValueRef<'_>| Ok(()),
            )
        })
        .expect_err("cursor should reject duplicate backend rows");

        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("out of strict ascending order")
        ));
    }

    #[test]
    fn scan_cursor_rejects_backend_emitted_count_mismatch() {
        let read = StorageReadScope::new(BrokenScanRead {
            rows: vec![ReadEntry {
                key: space(1).encode_key(&key("a")),
                value: projected_value("A"),
            }],
            emitted_override: Some(0),
            has_more_override: None,
        });

        let error = ScanPlan::range(
            space(1),
            KeyRange {
                lower: Bound::Included(key("a")),
                upper: Bound::Excluded(key("z")),
            },
        )
        .cursor(&read, ScanOptions::default(), |cursor| {
            cursor.visit_next(
                10,
                &mut |_key: KeyRef<'_>, _value: ProjectedValueRef<'_>| Ok(()),
            )
        })
        .expect_err("cursor should reject lying emitted count");

        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("reported 0 emitted rows after visiting 1 rows")
        ));
    }

    #[test]
    fn scan_rejects_backend_row_at_resume_key() {
        let read = StorageReadScope::new(BrokenScanRead {
            rows: vec![ReadEntry {
                key: space(1).encode_key(&key("m")),
                value: projected_value("M"),
            }],
            emitted_override: None,
            has_more_override: None,
        });
        let resume_after = key("m");

        let error = ScanPlan::range(
            space(1),
            KeyRange {
                lower: Bound::Included(key("a")),
                upper: Bound::Excluded(key("z")),
            },
        )
        .collect(
            &read,
            ScanOptions {
                resume_after: Some(&resume_after),
                ..ScanOptions::default()
            },
        )
        .expect_err("backend row at resume key should be rejected");

        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("emitted key outside requested range")
        ));
    }

    #[test]
    fn scan_collect_rejects_backend_has_more_after_empty_page() {
        let read = StorageReadScope::new(BrokenScanRead {
            rows: Vec::new(),
            emitted_override: Some(0),
            has_more_override: Some(true),
        });

        let error = ScanPlan::range(
            space(1),
            KeyRange {
                lower: Bound::Included(key("a")),
                upper: Bound::Excluded(key("z")),
            },
        )
        .collect(&read, ScanOptions::default())
        .expect_err("empty page with has_more should be rejected");

        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("reported more rows after emitting no rows")
        ));
    }

    #[test]
    fn scan_collect_into_rejects_backend_emitted_count_mismatch() {
        let read = StorageReadScope::new(BrokenScanRead {
            rows: vec![ReadEntry {
                key: space(1).encode_key(&key("a")),
                value: projected_value("A"),
            }],
            emitted_override: Some(0),
            has_more_override: None,
        });
        let mut buffer = ScanBuffer::new();

        let error = ScanPlan::range(
            space(1),
            KeyRange {
                lower: Bound::Included(key("a")),
                upper: Bound::Excluded(key("z")),
            },
        )
        .collect_into(&read, ScanOptions::default(), &mut buffer)
        .expect_err("collect_into should reject lying emitted count");

        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("reported 0 emitted rows after visiting 1 rows")
        ));
    }

    #[test]
    fn scan_cursor_rejects_duplicate_across_visit_next_calls() {
        let duplicate_key = space(1).encode_key(&key("a"));
        let read = StorageReadScope::new(BrokenScanRead {
            rows: vec![
                ReadEntry {
                    key: duplicate_key.clone(),
                    value: projected_value("A1"),
                },
                ReadEntry {
                    key: duplicate_key,
                    value: projected_value("A2"),
                },
            ],
            emitted_override: None,
            has_more_override: None,
        });

        let error = ScanPlan::range(
            space(1),
            KeyRange {
                lower: Bound::Included(key("a")),
                upper: Bound::Excluded(key("z")),
            },
        )
        .cursor(&read, ScanOptions::default(), |cursor| {
            cursor.visit_next(1, &mut |_key: KeyRef<'_>, _value: ProjectedValueRef<'_>| {
                Ok(())
            })?;
            cursor.visit_next(1, &mut |_key: KeyRef<'_>, _value: ProjectedValueRef<'_>| {
                Ok(())
            })
        })
        .expect_err("cursor should reject duplicate rows across chunks");

        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("out of strict ascending order")
        ));
    }

    #[test]
    fn scan_visit_rejects_row_between_resume_and_lower_bound() {
        let read = StorageReadScope::new(BrokenScanRead {
            rows: vec![ReadEntry {
                key: space(1).encode_key(&key("b")),
                value: projected_value("B"),
            }],
            emitted_override: None,
            has_more_override: None,
        });
        let resume_after = key("a");

        let error = ScanPlan::range(
            space(1),
            KeyRange {
                lower: Bound::Included(key("m")),
                upper: Bound::Excluded(key("z")),
            },
        )
        .visit(
            &read,
            ScanOptions {
                resume_after: Some(&resume_after),
                ..ScanOptions::default()
            },
            &mut |_key: KeyRef<'_>, _value: ProjectedValueRef<'_>| Ok(()),
        )
        .expect_err("visit should reject row below lower bound after earlier resume key");

        assert!(matches!(
            error,
            BackendError::Corruption(message)
                if message.contains("emitted key outside requested range")
        ));
    }

    struct BrokenScanRead {
        rows: Vec<ReadEntry>,
        emitted_override: Option<usize>,
        has_more_override: Option<bool>,
    }

    struct BrokenRangeScan {
        rows: Vec<ReadEntry>,
        position: usize,
        emitted_override: Option<usize>,
        has_more_override: Option<bool>,
    }

    impl BackendRangeScan for BrokenRangeScan {
        fn visit_next<V>(
            &mut self,
            limit_rows: usize,
            visitor: &mut V,
        ) -> Result<ScanResult, BackendError>
        where
            V: ScanVisitor + ?Sized,
        {
            let mut emitted = 0usize;
            while emitted < limit_rows {
                let Some(entry) = self.rows.get(self.position) else {
                    break;
                };
                visitor.visit(entry.key.as_ref(), entry.value.as_ref())?;
                self.position += 1;
                emitted += 1;
            }

            Ok(ScanResult {
                emitted: self.emitted_override.unwrap_or(emitted),
                has_more: self
                    .has_more_override
                    .unwrap_or(self.position < self.rows.len()),
            })
        }
    }

    impl BackendRead for BrokenScanRead {
        type RangeScan<'a> = BrokenRangeScan;

        fn visit_keys<V>(
            &self,
            _keys: &[Key],
            _opts: GetOptions<'_>,
            _visitor: &mut V,
        ) -> Result<(), BackendError>
        where
            V: PointVisitor + ?Sized,
        {
            unimplemented!("not used by scan corruption tests")
        }

        fn with_range_scan<T, F>(
            &self,
            _range: KeyRange,
            _opts: ScanOptions<'_>,
            f: F,
        ) -> Result<T, BackendError>
        where
            F: FnOnce(&mut Self::RangeScan<'_>) -> Result<T, BackendError>,
        {
            let mut cursor = BrokenRangeScan {
                rows: self.rows.clone(),
                position: 0,
                emitted_override: self.emitted_override,
                has_more_override: self.has_more_override,
            };
            f(&mut cursor)
        }
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
        type RangeScan<'a> = BufferedRangeScan;

        fn visit_keys<V>(
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

        fn with_range_scan<T, F>(
            &self,
            range: KeyRange,
            _opts: ScanOptions<'_>,
            f: F,
        ) -> Result<T, BackendError>
        where
            F: FnOnce(&mut Self::RangeScan<'_>) -> Result<T, BackendError>,
        {
            self.range.replace(Some(range));
            let mut cursor = BufferedRangeScan::default();
            f(&mut cursor)
        }
    }
}
