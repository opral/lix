use crate::storage::{CoreProjection, KeyRange, Prefix, ScanChunk, ScanOptions, StorageError};
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

    pub async fn collect<R>(
        &self,
        read: &R,
        opts: ScanOptions,
    ) -> Result<StorageReadResult<ScanChunk>, StorageError>
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
        let storage_calls = u64::from(opts.limit_rows != 0);
        let chunk = if opts.limit_rows == 0 {
            ScanChunk {
                entries: Vec::new(),
                has_more: false,
            }
        } else {
            read.scan(self.space.id, range, opts.clone()).await?
        };
        let mut stats = scan_trace_stats(
            kind,
            &opts,
            chunk.entries.len() as u64,
            chunk.has_more,
            storage_calls,
        );
        if matches!(kind, ScanKind::Prefix) {
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
    opts: &ScanOptions,
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
        scan_resume_after: u64::from(opts.resume_after.is_some()),
        scan_limit_rows_total: opts.limit_rows as u64,
        scan_limit_rows_max: opts.limit_rows as u64,
    }
}
