use crate::backend_v2::{
    BackendError, BackendRead, GetOptions, Key, KeyRange, Prefix, ScanOptions, ScanPage,
};
use crate::storage_v2::{
    get_many_caller_order, scan_prefix, PointSlot, StorageReadScope, StorageSpace,
};

pub trait StorageReader {
    fn get_many_caller_order(
        &self,
        space: StorageSpace,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<Vec<PointSlot>, BackendError>;

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
        StorageSpace::new(SpaceId(id))
    }

    #[derive(Default)]
    struct SpyRead {
        get_many_keys: RefCell<Vec<Key>>,
        scan_range: RefCell<Option<KeyRange>>,
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
            &[key("a"), key("b"), key("missing")]
        );
        assert_eq!(
            slots.into_iter().map(|slot| slot.key).collect::<Vec<_>>(),
            vec![key("b"), key("a"), key("b"), key("missing"), key("missing")]
        );
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
}
