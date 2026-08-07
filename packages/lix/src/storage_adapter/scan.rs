use crate::storage::{
    BeginScanOptions, CoreProjection, KeyRange, Prefix, ScanChunk, ScanCursor, StorageError,
};
use crate::storage_adapter::{
    StorageAdapterRead, StorageReadResult, StorageReadStats, StorageSpace,
};

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

    pub async fn begin<'a, R>(
        &self,
        read: &'a R,
        opts: BeginScanOptions,
    ) -> Result<ScanPlanCursor<'a>, StorageError>
    where
        R: StorageAdapterRead + ?Sized,
    {
        let kind = match self.kind {
            ScanPlanKind::Range(_) => ScanKind::Range,
            ScanPlanKind::Prefix(_) => ScanKind::Prefix,
        };
        let range = match &self.kind {
            ScanPlanKind::Range(range) => range.clone(),
            ScanPlanKind::Prefix(prefix) => prefix.to_range()?,
        };
        let cursor = read.begin_scan(self.space, range, opts).await?;
        Ok(ScanPlanCursor { cursor, kind, opts })
    }

    pub async fn first_page<R>(
        &self,
        read: &R,
        opts: BeginScanOptions,
    ) -> Result<StorageReadResult<ScanChunk>, StorageError>
    where
        R: StorageAdapterRead + ?Sized,
    {
        self.page(read, opts, crate::storage::MAX_SCAN_PAGE_ROWS)
            .await
    }

    pub async fn page<R>(
        &self,
        read: &R,
        opts: BeginScanOptions,
        limit_rows: usize,
    ) -> Result<StorageReadResult<ScanChunk>, StorageError>
    where
        R: StorageAdapterRead + ?Sized,
    {
        self.begin(read, opts).await?.next_page(limit_rows).await
    }
}

#[expect(missing_debug_implementations)]
pub struct ScanPlanCursor<'a> {
    cursor: ScanCursor<'a>,
    kind: ScanKind,
    opts: BeginScanOptions,
}

impl ScanPlanCursor<'_> {
    pub async fn next_page(
        &mut self,
        limit_rows: usize,
    ) -> Result<StorageReadResult<ScanChunk>, StorageError> {
        let chunk = self.cursor.next_page(limit_rows).await?;
        let storage_calls = u64::from(limit_rows != 0);
        let mut stats = scan_trace_stats(
            self.kind,
            self.opts,
            limit_rows,
            chunk.entries.len() as u64,
            chunk.has_more,
            storage_calls,
        );
        if matches!(self.kind, ScanKind::Prefix) {
            stats.prefix_lowered = 1;
        }
        Ok(StorageReadResult::new(chunk, stats))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanKind {
    Range,
    Prefix,
}

fn scan_trace_stats(
    kind: ScanKind,
    opts: BeginScanOptions,
    limit_rows: usize,
    emitted_rows: u64,
    has_more: bool,
    storage_calls: u64,
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
        unique_storage_keys: 0,
        storage_calls,
        prefix_lowered: 0,
        range_scan_chunks,
        prefix_scan_chunks,
        scan_key_only_chunks,
        scan_full_value_chunks,
        scan_rows: emitted_rows,
        scan_has_more: u64::from(has_more),
        scan_limit_rows_total: limit_rows as u64,
        scan_limit_rows_max: limit_rows as u64,
    }
}
