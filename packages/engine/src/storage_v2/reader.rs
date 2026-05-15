use crate::backend_v2::{
    BackendError, BackendRead, GetOptions, Key, KeyRange, Prefix, ProjectedValue, ScanOptions,
    ScanPage,
};
use crate::storage_v2::{
    get_many_caller_order, get_many_caller_order_with_stats, get_many_values_caller_order,
    get_many_values_caller_order_with_stats, scan_prefix, scan_prefix_with_stats, PointSlot,
    StorageReadResult, StorageReadScope, StorageReadStats, StorageSpace,
};

pub trait StorageReader {
    fn get_many_caller_order(
        &self,
        space: StorageSpace,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<Vec<PointSlot>, BackendError>;

    fn get_many_caller_order_with_stats(
        &self,
        space: StorageSpace,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<StorageReadResult<Vec<PointSlot>>, BackendError>;

    fn get_many_values_caller_order(
        &self,
        space: StorageSpace,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<Vec<Option<ProjectedValue>>, BackendError>;

    fn get_many_values_caller_order_with_stats(
        &self,
        space: StorageSpace,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<StorageReadResult<Vec<Option<ProjectedValue>>>, BackendError>;

    fn scan_range(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: ScanOptions<'_>,
    ) -> Result<ScanPage, BackendError>;

    fn scan_prefix(
        &self,
        space: StorageSpace,
        prefix: Prefix,
        opts: ScanOptions<'_>,
    ) -> Result<ScanPage, BackendError>;

    fn scan_range_with_stats(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: ScanOptions<'_>,
    ) -> Result<StorageReadResult<ScanPage>, BackendError>;

    fn scan_prefix_with_stats(
        &self,
        space: StorageSpace,
        prefix: Prefix,
        opts: ScanOptions<'_>,
    ) -> Result<StorageReadResult<ScanPage>, BackendError>;
}

impl<R> StorageReader for StorageReadScope<R>
where
    R: BackendRead,
{
    fn get_many_caller_order(
        &self,
        space: StorageSpace,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<Vec<PointSlot>, BackendError> {
        get_many_caller_order(self.backend_read(), space.id, keys, opts)
    }

    fn get_many_caller_order_with_stats(
        &self,
        space: StorageSpace,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<StorageReadResult<Vec<PointSlot>>, BackendError> {
        get_many_caller_order_with_stats(self.backend_read(), space.id, keys, opts)
    }

    fn get_many_values_caller_order(
        &self,
        space: StorageSpace,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<Vec<Option<ProjectedValue>>, BackendError> {
        get_many_values_caller_order(self.backend_read(), space.id, keys, opts)
    }

    fn get_many_values_caller_order_with_stats(
        &self,
        space: StorageSpace,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<StorageReadResult<Vec<Option<ProjectedValue>>>, BackendError> {
        get_many_values_caller_order_with_stats(self.backend_read(), space.id, keys, opts)
    }

    fn scan_range(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: ScanOptions<'_>,
    ) -> Result<ScanPage, BackendError> {
        self.backend_read().scan_range(space.id, range, opts)
    }

    fn scan_prefix(
        &self,
        space: StorageSpace,
        prefix: Prefix,
        opts: ScanOptions<'_>,
    ) -> Result<ScanPage, BackendError> {
        scan_prefix(self.backend_read(), space.id, prefix, opts)
    }

    fn scan_range_with_stats(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: ScanOptions<'_>,
    ) -> Result<StorageReadResult<ScanPage>, BackendError> {
        let page = self.backend_read().scan_range(space.id, range, opts)?;
        Ok(StorageReadResult::new(
            page,
            StorageReadStats {
                requested_keys: 0,
                unique_backend_keys: 0,
                backend_calls: 1,
                prefix_lowered: 0,
            },
        ))
    }

    fn scan_prefix_with_stats(
        &self,
        space: StorageSpace,
        prefix: Prefix,
        opts: ScanOptions<'_>,
    ) -> Result<StorageReadResult<ScanPage>, BackendError> {
        scan_prefix_with_stats(self.backend_read(), space.id, prefix, opts)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::ops::Bound;

    use bytes::Bytes;

    use crate::backend_v2::{
        BackendError, BackendRead, ConformanceBackend, CoreProjection, GetManyResult, GetOptions,
        Key, KeyRange, Prefix, ProjectedValue, ReadBatch, ReadEntry, ReadOptions, ScanOptions,
        ScanPage, SpaceId, StoredValue, WriteOptions,
    };
    use crate::storage_v2::{StorageContext, StorageReader, StorageSpace};

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

    #[derive(Default)]
    struct SpyRead {
        get_many_keys: RefCell<Vec<Key>>,
        scan_range: RefCell<Option<KeyRange>>,
        scan_range_calls: RefCell<u64>,
    }

    impl BackendRead for SpyRead {
        fn get_many(
            &self,
            _space: SpaceId,
            keys: &[Key],
            opts: GetOptions<'_>,
        ) -> Result<GetManyResult, BackendError> {
            self.get_many_keys.replace(keys.to_vec());
            Ok(GetManyResult {
                entries: ReadBatch {
                    entries: keys
                        .iter()
                        .map(|key| ReadEntry {
                            key: key.clone(),
                            value: match opts.projection {
                                CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
                                CoreProjection::FullValue => {
                                    ProjectedValue::FullValue(key.0.clone())
                                }
                            },
                        })
                        .collect(),
                },
            })
        }

        fn scan_range(
            &self,
            _space: SpaceId,
            range: KeyRange,
            _opts: ScanOptions<'_>,
        ) -> Result<ScanPage, BackendError> {
            *self.scan_range_calls.borrow_mut() += 1;
            self.scan_range.replace(Some(range));
            Ok(ScanPage {
                entries: ReadBatch::default(),
                has_more: false,
            })
        }
    }

    #[test]
    fn point_reads_reconstruct_caller_order_duplicates_and_missing() {
        let storage = StorageContext::new(ConformanceBackend::new());
        let mut writes = storage.new_write_set();
        writes.stage_put(space(1), key("a"), value("A"));
        writes.stage_put(space(1), key("b"), value("B"));
        storage
            .commit_write_set(writes, WriteOptions::default())
            .expect("seed");
        let read = storage
            .begin_read(ReadOptions::default())
            .expect("begin read");

        let slots = read
            .get_many_caller_order(
                space(1),
                &[key("b"), key("missing"), key("a"), key("b")],
                GetOptions::default(),
            )
            .expect("caller order");

        assert_eq!(slots[0].key, key("b"));
        assert_eq!(
            slots[0].value,
            Some(ProjectedValue::FullValue(Bytes::from_static(b"B")))
        );
        assert_eq!(slots[1].key, key("missing"));
        assert_eq!(slots[1].value, None);
        assert_eq!(slots[2].key, key("a"));
        assert_eq!(
            slots[2].value,
            Some(ProjectedValue::FullValue(Bytes::from_static(b"A")))
        );
        assert_eq!(slots[3].key, key("b"));
        assert_eq!(
            slots[3].value,
            Some(ProjectedValue::FullValue(Bytes::from_static(b"B")))
        );
    }

    #[test]
    fn point_reads_dedupe_before_backend_call() {
        let read = crate::storage_v2::StorageReadScope::new(SpyRead::default());
        let slots = read
            .get_many_caller_order(
                space(1),
                &[key("b"), key("a"), key("b"), key("missing"), key("missing")],
                GetOptions::default(),
            )
            .expect("caller order");

        assert_eq!(
            read.backend_read().get_many_keys.borrow().as_slice(),
            &[key("b"), key("a"), key("missing")]
        );
        assert_eq!(
            slots.into_iter().map(|slot| slot.key).collect::<Vec<_>>(),
            vec![key("b"), key("a"), key("b"), key("missing"), key("missing")]
        );
    }

    #[test]
    fn point_reads_can_return_values_without_echoing_keys() {
        let storage = StorageContext::new(ConformanceBackend::new());
        let mut writes = storage.new_write_set();
        writes.stage_put(space(1), key("a"), value("A"));
        writes.stage_put(space(1), key("b"), value("B"));
        storage
            .commit_write_set(writes, WriteOptions::default())
            .expect("seed");
        let read = storage
            .begin_read(ReadOptions::default())
            .expect("begin read");

        let values = read
            .get_many_values_caller_order(
                space(1),
                &[key("b"), key("missing"), key("a"), key("b")],
                GetOptions::default(),
            )
            .expect("caller order values");

        assert_eq!(
            values,
            vec![
                Some(ProjectedValue::FullValue(Bytes::from_static(b"B"))),
                None,
                Some(ProjectedValue::FullValue(Bytes::from_static(b"A"))),
                Some(ProjectedValue::FullValue(Bytes::from_static(b"B"))),
            ]
        );
    }

    #[test]
    fn point_reads_report_shape_stats() {
        let read = crate::storage_v2::StorageReadScope::new(SpyRead::default());
        let result = read
            .get_many_values_caller_order_with_stats(
                space(1),
                &[key("b"), key("a"), key("b"), key("missing")],
                GetOptions::default(),
            )
            .expect("caller order");

        assert_eq!(result.value.len(), 4);
        assert_eq!(result.stats.requested_keys, 4);
        assert_eq!(result.stats.unique_backend_keys, 3);
        assert_eq!(result.stats.backend_calls, 1);
        assert_eq!(result.stats.prefix_lowered, 0);
    }

    #[test]
    fn prefix_scan_lowers_to_range_and_respects_key_only_projection() {
        let storage = StorageContext::new(ConformanceBackend::new());
        let mut writes = storage.new_write_set();
        writes.stage_put(space(1), key("aa"), value("AA"));
        writes.stage_put(space(1), key("ab"), value("AB"));
        writes.stage_put(space(1), key("b"), value("B"));
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
                    projection: CoreProjection::KeyOnly,
                    limit_rows: 10,
                    resume_after: None,
                },
            )
            .expect("prefix scan");

        assert_eq!(
            page.entries
                .entries
                .into_iter()
                .map(|entry| (entry.key, entry.value))
                .collect::<Vec<_>>(),
            vec![
                (key("aa"), ProjectedValue::KeyOnly),
                (key("ab"), ProjectedValue::KeyOnly),
            ]
        );
        assert!(!page.has_more);
    }

    #[test]
    fn prefix_scan_lowers_expected_range() {
        let read = crate::storage_v2::StorageReadScope::new(SpyRead::default());
        read.scan_prefix(
            space(1),
            Prefix {
                bytes: Bytes::from_static(b"a\xff"),
            },
            ScanOptions::default(),
        )
        .expect("prefix scan");

        let range = read
            .backend_read()
            .scan_range
            .borrow()
            .clone()
            .expect("range captured");
        assert_eq!(range.lower, Bound::Included(key_bytes(b"a\xff")));
        assert_eq!(range.upper, Bound::Excluded(key("b")));
    }

    #[test]
    fn scan_range_reports_shape_stats() {
        let read = crate::storage_v2::StorageReadScope::new(SpyRead::default());
        let result = read
            .scan_range_with_stats(
                space(1),
                KeyRange {
                    lower: Bound::Included(key("a")),
                    upper: Bound::Excluded(key("z")),
                },
                ScanOptions::default(),
            )
            .expect("scan range");

        assert_eq!(result.stats.requested_keys, 0);
        assert_eq!(result.stats.unique_backend_keys, 0);
        assert_eq!(result.stats.backend_calls, 1);
        assert_eq!(result.stats.prefix_lowered, 0);
    }

    #[test]
    fn prefix_scan_reports_shape_stats() {
        let read = crate::storage_v2::StorageReadScope::new(SpyRead::default());
        let result = read
            .scan_prefix_with_stats(
                space(1),
                Prefix {
                    bytes: Bytes::from_static(b"a"),
                },
                ScanOptions::default(),
            )
            .expect("prefix scan");

        assert_eq!(result.stats.requested_keys, 0);
        assert_eq!(result.stats.unique_backend_keys, 0);
        assert_eq!(result.stats.backend_calls, 1);
        assert_eq!(result.stats.prefix_lowered, 1);
        assert_eq!(*read.backend_read().scan_range_calls.borrow(), 1);
    }

    #[test]
    fn prefix_scan_limit_zero_reports_no_backend_call() {
        let read = crate::storage_v2::StorageReadScope::new(SpyRead::default());
        let result = read
            .scan_prefix_with_stats(
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

        assert!(result.value.entries.entries.is_empty());
        assert_eq!(result.stats.backend_calls, 0);
        assert_eq!(result.stats.prefix_lowered, 1);
        assert_eq!(*read.backend_read().scan_range_calls.borrow(), 0);
    }
}
