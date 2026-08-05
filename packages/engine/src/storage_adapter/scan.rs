use crate::storage::{
    CoreProjection, Key, KeyRange, Prefix, ProjectedValue, ScanChunk, ScanOptions, StorageError,
};
use crate::storage_adapter::{
    StorageAdapterRead, StorageReadResult, StorageReadStats, StorageSpace,
};
#[cfg(feature = "storage-benches")]
use std::time::Instant;

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
        #[cfg(feature = "storage-benches")]
        let started = crate::sql_profile::is_active().then(Instant::now);
        let chunk = if opts.limit_rows == 0 {
            ScanChunk {
                entries: Vec::new(),
                has_more: false,
            }
        } else {
            read.scan(self.space, range, opts.clone()).await?
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
        #[cfg(feature = "storage-benches")]
        if let Some(started) = started {
            crate::sql_profile::record_phase(
                crate::sql_profile::Phase::StorageRead,
                started.elapsed(),
            );
        }
        #[cfg(feature = "storage-benches")]
        crate::sql_profile::record_storage_stats(stats);
        Ok(StorageReadResult::new(chunk, stats))
    }
}

/// Collects disjoint ranges through one coherent adapter read. The adapter
/// owns pagination and may lower the request to one backend-native multi-range
/// operation; callers receive one bucket per input range.
pub(crate) async fn collect_many<R>(
    read: &R,
    space: StorageSpace,
    ranges: &[KeyRange],
    projection: CoreProjection,
) -> Result<Vec<Vec<crate::storage::ReadEntry>>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let buckets = read.scan_many(space, ranges, projection).await?;
    if buckets.len() != ranges.len() {
        return Err(StorageError::Corruption(format!(
            "storage multi-range read returned {} buckets for {} ranges",
            buckets.len(),
            ranges.len()
        )));
    }
    for (range, bucket) in ranges.iter().zip(&buckets) {
        let mut previous = None;
        for entry in bucket {
            if previous
                .as_ref()
                .is_some_and(|previous: &Key| previous >= &entry.key)
                || !key_in_range(range, &entry.key)
            {
                return Err(StorageError::Corruption(
                    "storage multi-range bucket is not strictly ordered or escaped its range"
                        .to_string(),
                ));
            }
            if matches!(
                (projection, &entry.value),
                (CoreProjection::KeyOnly, ProjectedValue::FullValue(_))
                    | (CoreProjection::FullValue, ProjectedValue::KeyOnly)
            ) {
                return Err(StorageError::Corruption(
                    "storage multi-range bucket returned the wrong projection".to_string(),
                ));
            }
            previous = Some(entry.key.clone());
        }
    }
    #[cfg(feature = "storage-benches")]
    {
        let rows = buckets
            .iter()
            .map(|bucket| bucket.len() as u64)
            .sum::<u64>();
        crate::sql_profile::record_storage_stats(StorageReadStats {
            storage_calls: 1,
            prefix_lowered: ranges.len() as u64,
            prefix_scan_chunks: ranges.len() as u64,
            scan_full_value_chunks: (projection == CoreProjection::FullValue)
                .then_some(ranges.len() as u64)
                .unwrap_or_default(),
            scan_key_only_chunks: (projection == CoreProjection::KeyOnly)
                .then_some(ranges.len() as u64)
                .unwrap_or_default(),
            scan_rows: rows,
            ..StorageReadStats::default()
        });
    }
    Ok(buckets)
}

fn key_in_range(range: &KeyRange, key: &Key) -> bool {
    let lower = match &range.lower {
        std::ops::Bound::Included(bound) => key >= bound,
        std::ops::Bound::Excluded(bound) => key > bound,
        std::ops::Bound::Unbounded => true,
    };
    let upper = match &range.upper {
        std::ops::Bound::Included(bound) => key <= bound,
        std::ops::Bound::Excluded(bound) => key < bound,
        std::ops::Bound::Unbounded => true,
    };
    lower && upper
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{GetManyRequest, GetManyResult, Key, ReadEntry, StorageRead};
    use bytes::Bytes;
    use std::future::Future;
    use std::ops::Bound;

    struct WrongBucketCount;

    impl StorageAdapterRead for WrongBucketCount {
        fn get_many(
            &self,
            _requests: &[GetManyRequest<'_>],
        ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
            async { Ok(GetManyResult::new(Vec::new())) }
        }

        fn scan(
            &self,
            _space: StorageSpace,
            _range: KeyRange,
            _opts: ScanOptions,
        ) -> impl Future<Output = Result<ScanChunk, StorageError>> + Send {
            async {
                Ok(ScanChunk {
                    entries: Vec::new(),
                    has_more: false,
                })
            }
        }

        fn scan_many(
            &self,
            _space: StorageSpace,
            _ranges: &[KeyRange],
            _projection: CoreProjection,
        ) -> impl Future<Output = Result<Vec<Vec<ReadEntry>>, StorageError>> + Send {
            async { Ok(vec![Vec::new()]) }
        }
    }

    #[tokio::test]
    async fn collect_many_rejects_wrong_bucket_cardinality() {
        let ranges = vec![
            KeyRange {
                lower: Bound::Included(Key(Bytes::from_static(b"a"))),
                upper: Bound::Excluded(Key(Bytes::from_static(b"b"))),
            },
            KeyRange {
                lower: Bound::Included(Key(Bytes::from_static(b"c"))),
                upper: Bound::Excluded(Key(Bytes::from_static(b"d"))),
            },
        ];
        let error = collect_many(
            &WrongBucketCount,
            StorageSpace::mutable(crate::storage::SpaceId(1), "test"),
            &ranges,
            CoreProjection::KeyOnly,
        )
        .await
        .expect_err("wrong bucket count must fail closed");
        assert!(matches!(error, StorageError::Corruption(message) if message.contains("buckets")));
    }
}
