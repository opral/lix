#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::ops::Bound;

    use bytes::Bytes;

    use crate::backend::{
        BackendError, BackendRead, CoreProjection, GetOptions, InMemoryBackend, Key, KeyRange,
        KeyRef, PointVisitor, Prefix, ProjectedValue, ProjectedValueRef, ReadOptions, ScanOptions,
        ScanResult, ScanVisitor, SpaceId, StoredValue, WriteOptions,
    };
    use crate::storage::{
        PointReadBuffer, PointReadPlan, ScanBuffer, ScanPlan, StorageContext, StorageRead,
        StorageSpace,
    };

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
        fn visit_keys<V>(
            &self,
            _space: SpaceId,
            keys: &[Key],
            opts: GetOptions<'_>,
            visitor: &mut V,
        ) -> Result<(), BackendError>
        where
            V: PointVisitor + ?Sized,
        {
            self.get_many_keys.replace(keys.to_vec());
            for (index, key) in keys.iter().enumerate() {
                let value = match opts.projection {
                    CoreProjection::KeyOnly => ProjectedValueRef::KeyOnly,
                    CoreProjection::FullValue => ProjectedValueRef::FullValue(key.0.as_ref()),
                };
                visitor.visit(index, key, Some(value))?;
            }
            Ok(())
        }

        fn scan<V>(
            &self,
            _space: SpaceId,
            range: KeyRange,
            _opts: ScanOptions<'_>,
            _visitor: &mut V,
        ) -> Result<ScanResult, BackendError>
        where
            V: ScanVisitor + ?Sized,
        {
            *self.scan_range_calls.borrow_mut() += 1;
            self.scan_range.replace(Some(range));
            Ok(ScanResult::default())
        }
    }

    #[derive(Default)]
    struct RequestedOrderRead {
        get_many_keys: RefCell<Vec<Key>>,
    }

    impl BackendRead for RequestedOrderRead {
        fn visit_keys<V>(
            &self,
            _space: SpaceId,
            keys: &[Key],
            _opts: GetOptions<'_>,
            visitor: &mut V,
        ) -> Result<(), BackendError>
        where
            V: PointVisitor + ?Sized,
        {
            self.get_many_keys.replace(keys.to_vec());
            for (index, key) in keys.iter().enumerate() {
                let value = (!key.0.ends_with(b"missing"))
                    .then_some(ProjectedValueRef::FullValue(key.0.as_ref()));
                visitor.visit(index, key, value)?;
            }
            Ok(())
        }

        fn scan<V>(
            &self,
            _space: SpaceId,
            _range: KeyRange,
            _opts: ScanOptions<'_>,
            _visitor: &mut V,
        ) -> Result<ScanResult, BackendError>
        where
            V: ScanVisitor + ?Sized,
        {
            unreachable!("requested-order point-read test does not scan")
        }
    }

    #[test]
    fn point_reads_reconstruct_caller_order_duplicates_and_missing() {
        let storage = StorageContext::new(InMemoryBackend::new());
        let mut writes = storage.new_write_set();
        writes.put(space(1), key("a"), value("A"));
        writes.put(space(1), key("b"), value("B"));
        storage
            .commit_write_set(writes, WriteOptions::default())
            .expect("seed");
        let read = storage
            .begin_read(ReadOptions::default())
            .expect("begin read");

        let result = PointReadPlan::new(space(1), &[key("b"), key("missing"), key("a"), key("b")])
            .materialize(&read, GetOptions::default())
            .expect("caller order");

        assert_eq!(
            result.value[0],
            Some(ProjectedValue::FullValue(Bytes::from_static(b"B")))
        );
        assert_eq!(result.value[1], None);
        assert_eq!(
            result.value[2],
            Some(ProjectedValue::FullValue(Bytes::from_static(b"A")))
        );
        assert_eq!(
            result.value[3],
            Some(ProjectedValue::FullValue(Bytes::from_static(b"B")))
        );
    }

    #[test]
    fn point_reads_dedupe_before_backend_call() {
        let read = crate::storage::StorageReadScope::new(SpyRead::default());
        let result = PointReadPlan::new(
            space(1),
            &[key("b"), key("a"), key("b"), key("missing"), key("missing")],
        )
        .materialize(&read, GetOptions::default())
        .expect("caller order");

        read.with_backend(|backend_read| {
            assert_eq!(
                backend_read.get_many_keys.borrow().as_slice(),
                [key("b"), key("a"), key("missing")]
            );
            Ok(())
        })
        .expect("backend keys");
        assert_eq!(
            result.value,
            vec![
                Some(ProjectedValue::FullValue(key("b").0)),
                Some(ProjectedValue::FullValue(key("a").0)),
                Some(ProjectedValue::FullValue(key("b").0)),
                Some(ProjectedValue::FullValue(key("missing").0)),
                Some(ProjectedValue::FullValue(key("missing").0)),
            ]
        );
    }

    #[test]
    fn point_reads_can_return_values_without_echoing_keys() {
        let storage = StorageContext::new(InMemoryBackend::new());
        let mut writes = storage.new_write_set();
        writes.put(space(1), key("a"), value("A"));
        writes.put(space(1), key("b"), value("B"));
        storage
            .commit_write_set(writes, WriteOptions::default())
            .expect("seed");
        let read = storage
            .begin_read(ReadOptions::default())
            .expect("begin read");

        let values = PointReadPlan::new(space(1), &[key("b"), key("missing"), key("a"), key("b")])
            .materialize(&read, GetOptions::default())
            .expect("caller order values");

        assert_eq!(
            values.value,
            vec![
                Some(ProjectedValue::FullValue(Bytes::from_static(b"B"))),
                None,
                Some(ProjectedValue::FullValue(Bytes::from_static(b"A"))),
                Some(ProjectedValue::FullValue(Bytes::from_static(b"B"))),
            ]
        );
    }

    #[test]
    fn point_reads_can_return_indexed_values_without_duplicate_value_clones() {
        let storage = StorageContext::new(InMemoryBackend::new());
        let mut writes = storage.new_write_set();
        writes.put(space(1), key("a"), value("A"));
        writes.put(space(1), key("b"), value("B"));
        storage
            .commit_write_set(writes, WriteOptions::default())
            .expect("seed");
        let read = storage
            .begin_read(ReadOptions::default())
            .expect("begin read");

        let plan = PointReadPlan::new(space(1), &[key("b"), key("missing"), key("a"), key("b")]);
        let indexed = plan
            .collect(&read, GetOptions::default())
            .expect("indexed caller order values")
            .value;

        assert_eq!(indexed.len(), 4);
        assert_eq!(indexed.unique_values.len(), 3);
        assert_eq!(indexed.requested_to_unique.to_vec(), vec![0, 1, 2, 0]);
        assert_eq!(
            indexed.value_at(0),
            Some(&ProjectedValue::FullValue(Bytes::from_static(b"B")))
        );
        assert_eq!(indexed.value_at(1), None);
        assert_eq!(
            indexed.value_at(2),
            Some(&ProjectedValue::FullValue(Bytes::from_static(b"A")))
        );
        assert_eq!(
            indexed.materialize_caller_order(),
            vec![
                Some(ProjectedValue::FullValue(Bytes::from_static(b"B"))),
                None,
                Some(ProjectedValue::FullValue(Bytes::from_static(b"A"))),
                Some(ProjectedValue::FullValue(Bytes::from_static(b"B"))),
            ]
        );
    }

    #[test]
    fn point_request_plan_can_be_reused_for_indexed_reads() {
        let storage = StorageContext::new(InMemoryBackend::new());
        let mut writes = storage.new_write_set();
        writes.put(space(1), key("a"), value("A"));
        writes.put(space(1), key("b"), value("B"));
        storage
            .commit_write_set(writes, WriteOptions::default())
            .expect("seed");
        let read = storage
            .begin_read(ReadOptions::default())
            .expect("begin read");
        let plan = PointReadPlan::new(space(1), &[key("b"), key("missing"), key("a"), key("b")]);

        assert_eq!(plan.len(), 4);
        assert_eq!(
            plan.logical_unique_keys,
            vec![key("b"), key("missing"), key("a")]
        );
        assert_eq!(plan.requested_to_unique().to_vec(), vec![0, 1, 2, 0]);

        let result = plan
            .collect(&read, GetOptions::default())
            .expect("planned indexed read");

        assert_eq!(result.stats.requested_keys, 4);
        assert_eq!(result.stats.unique_backend_keys, 3);
        assert_eq!(result.stats.backend_calls, 1);
        assert_eq!(result.value.requested_to_unique.to_vec(), vec![0, 1, 2, 0]);
        assert_eq!(
            result.value.value_at(0),
            Some(&ProjectedValue::FullValue(Bytes::from_static(b"B")))
        );
        assert_eq!(result.value.value_at(1), None);
        assert_eq!(
            result.value.value_at(2),
            Some(&ProjectedValue::FullValue(Bytes::from_static(b"A")))
        );

        let borrowed = plan
            .collect(&read, GetOptions::default())
            .expect("borrowed planned indexed read");

        assert_eq!(borrowed.stats.requested_keys, 4);
        assert_eq!(
            borrowed.value.requested_to_unique,
            plan.requested_to_unique()
        );
        assert_eq!(
            borrowed.value.value_at(0),
            Some(&ProjectedValue::FullValue(Bytes::from_static(b"B")))
        );
        assert_eq!(borrowed.value.value_at(1), None);
    }

    #[test]
    #[expect(
        clippy::drop_non_drop,
        reason = "the explicit drops end borrows before reusing the shared buffer"
    )]
    fn planned_point_reads_can_reuse_value_buffer() {
        let storage = StorageContext::new(InMemoryBackend::new());
        let mut writes = storage.new_write_set();
        writes.put(space(1), key("a"), value("A"));
        writes.put(space(1), key("b"), value("B"));
        writes.put(space(1), key("c"), value("C"));
        storage
            .commit_write_set(writes, WriteOptions::default())
            .expect("seed");
        let read = storage
            .begin_read(ReadOptions::default())
            .expect("begin read");
        let first_plan =
            PointReadPlan::new(space(1), &[key("b"), key("missing"), key("a"), key("b")]);
        let second_plan = PointReadPlan::new(space(1), &[key("c")]);
        let mut buffer = PointReadBuffer::new();

        let first = first_plan
            .collect_into(&read, GetOptions::default(), &mut buffer)
            .expect("first buffered planned indexed read");

        assert_eq!(first.stats.requested_keys, 4);
        assert_eq!(first.stats.unique_backend_keys, 3);
        assert_eq!(first.value.len(), 4);
        assert_eq!(first.value.unique_values.len(), 3);
        assert_eq!(
            first.value.value_at(0),
            Some(&ProjectedValue::FullValue(Bytes::from_static(b"B")))
        );
        assert_eq!(first.value.value_at(1), None);
        assert_eq!(
            first.value.value_at(2),
            Some(&ProjectedValue::FullValue(Bytes::from_static(b"A")))
        );
        drop(first);

        let capacity_after_first = buffer.capacity();
        let second = second_plan
            .collect_into(&read, GetOptions::default(), &mut buffer)
            .expect("second buffered planned indexed read");

        assert_eq!(second.stats.requested_keys, 1);
        assert_eq!(second.stats.unique_backend_keys, 1);
        assert_eq!(second.value.unique_values.len(), 1);
        assert_eq!(
            second.value.value_at(0),
            Some(&ProjectedValue::FullValue(Bytes::from_static(b"C")))
        );
        drop(second);
        assert!(
            buffer.capacity() >= capacity_after_first,
            "buffer allocation should be retained for reuse"
        );
    }

    #[test]
    fn point_request_plan_can_be_built_from_known_unique_keys() {
        let plan = PointReadPlan::from_unique_keys(space(1), vec![key("a"), key("b"), key("c")]);

        assert_eq!(plan.len(), 3);
        assert_eq!(plan.logical_unique_keys, vec![key("a"), key("b"), key("c")]);
        assert_eq!(plan.requested_to_unique().to_vec(), vec![0, 1, 2]);
    }

    #[test]
    fn planned_point_reads_use_backend_requested_order_slots() {
        let read = crate::storage::StorageReadScope::new(RequestedOrderRead::default());
        let plan = PointReadPlan::new(space(1), &[key("b"), key("missing"), key("a"), key("b")]);

        let result = plan
            .collect(&read, GetOptions::default())
            .expect("borrowed planned indexed read");

        read.with_backend(|backend_read| {
            assert_eq!(
                backend_read.get_many_keys.borrow().as_slice(),
                [key("b"), key("missing"), key("a")]
            );
            Ok(())
        })
        .expect("backend keys");
        assert_eq!(result.stats.requested_keys, 4);
        assert_eq!(result.stats.unique_backend_keys, 3);
        assert_eq!(result.stats.backend_calls, 1);
        assert_eq!(
            result.value.value_at(0),
            Some(&ProjectedValue::FullValue(Bytes::from_static(b"b")))
        );
        assert_eq!(result.value.value_at(1), None);
        assert_eq!(
            result.value.value_at(2),
            Some(&ProjectedValue::FullValue(Bytes::from_static(b"a")))
        );
    }

    #[test]
    fn physical_point_request_plan_reuses_encoded_backend_keys() {
        let read = crate::storage::StorageReadScope::new(RequestedOrderRead::default());
        let plan = PointReadPlan::new(space(1), &[key("b"), key("missing"), key("a"), key("b")]);

        assert_eq!(
            plan.logical_unique_keys,
            vec![key("b"), key("missing"), key("a")]
        );

        let result = plan
            .collect(&read, GetOptions::default())
            .expect("borrowed physical planned indexed read");

        read.with_backend(|backend_read| {
            assert_eq!(
                backend_read.get_many_keys.borrow().as_slice(),
                plan.logical_unique_keys.as_slice()
            );
            Ok(())
        })
        .expect("backend keys");
        assert_eq!(result.stats.requested_keys, 4);
        assert_eq!(result.stats.unique_backend_keys, 3);
        assert_eq!(result.stats.backend_calls, 1);
        assert_eq!(
            result.value.value_at(0),
            Some(&ProjectedValue::FullValue(Bytes::from_static(b"b")))
        );
        assert_eq!(result.value.value_at(1), None);
    }

    #[test]
    fn planned_point_reads_can_visit_unique_values_without_materializing_indexed_result() {
        let storage = StorageContext::new(InMemoryBackend::new());
        let mut writes = storage.new_write_set();
        writes.put(space(1), key("a"), value("A"));
        writes.put(space(1), key("b"), value("B"));
        storage
            .commit_write_set(writes, WriteOptions::default())
            .expect("seed");
        let read = storage
            .begin_read(ReadOptions::default())
            .expect("begin read");
        let plan = PointReadPlan::new(space(1), &[key("b"), key("missing"), key("a"), key("b")]);

        let mut visited = Vec::new();
        let stats = plan
            .visit(&read, GetOptions::default(), &mut |unique_index: usize,
                                                       key: &Key,
                                                       value: Option<
                ProjectedValueRef<'_>,
            >| {
                visited.push((
                    unique_index,
                    key.clone(),
                    value.map(ProjectedValueRef::to_owned),
                ));
                Ok(())
            })
            .expect("visit unique point values");

        assert_eq!(stats.requested_keys, 4);
        assert_eq!(stats.unique_backend_keys, 3);
        assert_eq!(stats.backend_calls, 1);
        assert_eq!(
            visited,
            vec![
                (
                    0,
                    key("b"),
                    Some(ProjectedValue::FullValue(Bytes::from_static(b"B")))
                ),
                (1, key("missing"), None),
                (
                    2,
                    key("a"),
                    Some(ProjectedValue::FullValue(Bytes::from_static(b"A")))
                ),
            ]
        );
    }

    #[test]
    fn point_reads_report_shape_stats() {
        let read = crate::storage::StorageReadScope::new(SpyRead::default());
        let result = PointReadPlan::new(space(1), &[key("b"), key("a"), key("b"), key("missing")])
            .materialize(&read, GetOptions::default())
            .expect("caller order");

        assert_eq!(result.value.len(), 4);
        assert_eq!(result.stats.requested_keys, 4);
        assert_eq!(result.stats.unique_backend_keys, 3);
        assert_eq!(result.stats.backend_calls, 1);
        assert_eq!(result.stats.prefix_lowered, 0);
        assert_eq!(result.stats.range_scan_chunks, 0);
        assert_eq!(result.stats.prefix_scan_chunks, 0);
        assert_eq!(result.stats.scan_key_only_chunks, 0);
        assert_eq!(result.stats.scan_full_value_chunks, 0);
        assert_eq!(result.stats.scan_rows, 0);
        assert_eq!(result.stats.scan_has_more, 0);
        assert_eq!(result.stats.scan_resume_after, 0);
        assert_eq!(result.stats.scan_limit_rows_total, 0);
        assert_eq!(result.stats.scan_limit_rows_max, 0);
    }

    #[test]
    fn prefix_scan_lowers_to_range_and_respects_key_only_projection() {
        let storage = StorageContext::new(InMemoryBackend::new());
        let mut writes = storage.new_write_set();
        writes.put(space(1), key("aa"), value("AA"));
        writes.put(space(1), key("ab"), value("AB"));
        writes.put(space(1), key("b"), value("B"));
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
                projection: CoreProjection::KeyOnly,
                limit_rows: 10,
                resume_after: None,
            },
        )
        .expect("prefix scan");

        assert_eq!(
            chunk
                .value
                .entries
                .into_iter()
                .map(|entry| (entry.key, entry.value))
                .collect::<Vec<_>>(),
            vec![
                (key("aa"), ProjectedValue::KeyOnly),
                (key("ab"), ProjectedValue::KeyOnly),
            ]
        );
        assert!(!chunk.value.has_more);
    }

    #[test]
    fn scan_range_into_reuses_storage_buffer() {
        let storage = StorageContext::new(InMemoryBackend::new());
        let mut writes = storage.new_write_set();
        writes.put(space(1), key("aa"), value("AA"));
        writes.put(space(1), key("ab"), value("AB"));
        writes.put(space(1), key("b"), value("B"));
        storage
            .commit_write_set(writes, WriteOptions::default())
            .expect("seed");

        let read = storage
            .begin_read(ReadOptions::default())
            .expect("begin read");
        let mut buffer = ScanBuffer::with_capacity(8);

        {
            let chunk = ScanPlan::range(
                space(1),
                KeyRange {
                    lower: Bound::Included(key("a")),
                    upper: Bound::Excluded(key("b")),
                },
            )
            .collect_into(
                &read,
                ScanOptions {
                    projection: CoreProjection::KeyOnly,
                    limit_rows: 10,
                    resume_after: None,
                },
                &mut buffer,
            )
            .expect("scan range into");

            assert_eq!(
                chunk
                    .value
                    .entries
                    .iter()
                    .map(|entry| (&entry.key, &entry.value))
                    .collect::<Vec<_>>(),
                vec![
                    (&key("aa"), &ProjectedValue::KeyOnly),
                    (&key("ab"), &ProjectedValue::KeyOnly),
                ]
            );
            assert!(!chunk.value.has_more);
        }

        let capacity_after_first_scan = buffer.capacity();
        assert!(capacity_after_first_scan >= 8);

        {
            let chunk = ScanPlan::prefix(
                space(1),
                Prefix {
                    bytes: Bytes::from_static(b"a"),
                },
            )
            .collect_into(
                &read,
                ScanOptions {
                    projection: CoreProjection::FullValue,
                    limit_rows: 10,
                    resume_after: None,
                },
                &mut buffer,
            )
            .expect("scan prefix into");

            assert_eq!(
                chunk
                    .value
                    .entries
                    .iter()
                    .map(|entry| (&entry.key, &entry.value))
                    .collect::<Vec<_>>(),
                vec![
                    (
                        &key("aa"),
                        &ProjectedValue::FullValue(Bytes::from_static(b"AA"))
                    ),
                    (
                        &key("ab"),
                        &ProjectedValue::FullValue(Bytes::from_static(b"AB"))
                    ),
                ]
            );
            assert!(!chunk.value.has_more);
        }

        assert_eq!(buffer.capacity(), capacity_after_first_scan);
    }

    #[test]
    fn visit_scan_prefix_lowers_without_materializing_entries() {
        let storage = StorageContext::new(InMemoryBackend::new());
        let mut writes = storage.new_write_set();
        writes.put(space(1), key("aa"), value("AA"));
        writes.put(space(1), key("ab"), value("AB"));
        writes.put(space(1), key("b"), value("B"));
        storage
            .commit_write_set(writes, WriteOptions::default())
            .expect("seed");

        let read = storage
            .begin_read(ReadOptions::default())
            .expect("begin read");
        let mut visited = Vec::new();
        let result = ScanPlan::prefix(
            space(1),
            Prefix {
                bytes: Bytes::from_static(b"a"),
            },
        )
        .visit(
            &read,
            ScanOptions {
                projection: CoreProjection::FullValue,
                limit_rows: 10,
                resume_after: None,
            },
            &mut |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
                visited.push((key.to_owned_key(), value.to_owned()));
                Ok(())
            },
        )
        .expect("visit scan prefix");

        assert_eq!(result.value.emitted, 2);
        assert!(!result.value.has_more);
        assert_eq!(
            visited,
            vec![
                (
                    key("aa"),
                    ProjectedValue::FullValue(Bytes::from_static(b"AA"))
                ),
                (
                    key("ab"),
                    ProjectedValue::FullValue(Bytes::from_static(b"AB"))
                ),
            ]
        );
    }

    #[test]
    fn prefix_scan_lowers_expected_range() {
        let read = crate::storage::StorageReadScope::new(SpyRead::default());
        ScanPlan::prefix(
            space(1),
            Prefix {
                bytes: Bytes::from_static(b"a\xff"),
            },
        )
        .collect(&read, ScanOptions::default())
        .expect("prefix scan");

        let range = read
            .with_backend(|backend_read| Ok(backend_read.scan_range.borrow().clone()))
            .expect("backend range")
            .expect("range captured");
        assert_eq!(range.lower, Bound::Included(key_bytes(b"a\xff")));
        assert_eq!(range.upper, Bound::Excluded(key("b")));
    }

    #[test]
    fn scan_range_reports_shape_stats() {
        let read = crate::storage::StorageReadScope::new(SpyRead::default());
        let result = ScanPlan::range(
            space(1),
            KeyRange {
                lower: Bound::Included(key("a")),
                upper: Bound::Excluded(key("z")),
            },
        )
        .collect(&read, ScanOptions::default())
        .expect("scan range");

        assert_eq!(result.stats.requested_keys, 0);
        assert_eq!(result.stats.unique_backend_keys, 0);
        assert_eq!(result.stats.backend_calls, 1);
        assert_eq!(result.stats.prefix_lowered, 0);
        assert_eq!(result.stats.range_scan_chunks, 1);
        assert_eq!(result.stats.prefix_scan_chunks, 0);
        assert_eq!(result.stats.scan_key_only_chunks, 0);
        assert_eq!(result.stats.scan_full_value_chunks, 1);
        assert_eq!(result.stats.scan_rows, 0);
        assert_eq!(result.stats.scan_has_more, 0);
        assert_eq!(result.stats.scan_resume_after, 0);
        assert_eq!(result.stats.scan_limit_rows_total, 1024);
        assert_eq!(result.stats.scan_limit_rows_max, 1024);
    }

    #[test]
    fn prefix_scan_reports_shape_stats() {
        let read = crate::storage::StorageReadScope::new(SpyRead::default());
        let result = ScanPlan::prefix(
            space(1),
            Prefix {
                bytes: Bytes::from_static(b"a"),
            },
        )
        .collect(&read, ScanOptions::default())
        .expect("prefix scan");

        assert_eq!(result.stats.requested_keys, 0);
        assert_eq!(result.stats.unique_backend_keys, 0);
        assert_eq!(result.stats.backend_calls, 1);
        assert_eq!(result.stats.prefix_lowered, 1);
        assert_eq!(result.stats.range_scan_chunks, 0);
        assert_eq!(result.stats.prefix_scan_chunks, 1);
        assert_eq!(result.stats.scan_full_value_chunks, 1);
        read.with_backend(|backend_read| {
            assert_eq!(*backend_read.scan_range_calls.borrow(), 1);
            Ok(())
        })
        .expect("scan range calls");
    }

    #[test]
    fn visit_scan_reports_trace_stats() {
        let storage = StorageContext::new(InMemoryBackend::new());
        let mut writes = storage.new_write_set();
        writes.put(space(1), key("aa"), value("AA"));
        writes.put(space(1), key("ab"), value("AB"));
        writes.put(space(1), key("ac"), value("AC"));
        storage
            .commit_write_set(writes, WriteOptions::default())
            .expect("seed");
        let read = storage
            .begin_read(ReadOptions::default())
            .expect("begin read");

        let result = ScanPlan::prefix(
            space(1),
            Prefix {
                bytes: Bytes::from_static(b"a"),
            },
        )
        .visit(
            &read,
            ScanOptions {
                projection: CoreProjection::KeyOnly,
                limit_rows: 2,
                resume_after: Some(&key("aa")),
            },
            &mut |_key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
                assert!(matches!(value, ProjectedValueRef::KeyOnly));
                Ok(())
            },
        )
        .expect("visit scan prefix with stats");

        assert_eq!(result.value.emitted, 2);
        assert!(!result.value.has_more);
        assert_eq!(result.stats.backend_calls, 1);
        assert_eq!(result.stats.prefix_lowered, 1);
        assert_eq!(result.stats.range_scan_chunks, 0);
        assert_eq!(result.stats.prefix_scan_chunks, 1);
        assert_eq!(result.stats.scan_key_only_chunks, 1);
        assert_eq!(result.stats.scan_full_value_chunks, 0);
        assert_eq!(result.stats.scan_rows, 2);
        assert_eq!(result.stats.scan_has_more, 0);
        assert_eq!(result.stats.scan_resume_after, 1);
        assert_eq!(result.stats.scan_limit_rows_total, 2);
        assert_eq!(result.stats.scan_limit_rows_max, 2);
    }

    #[test]
    fn prefix_scan_limit_zero_reports_no_backend_call() {
        let read = crate::storage::StorageReadScope::new(SpyRead::default());
        let result = ScanPlan::prefix(
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

        assert!(result.value.entries.is_empty());
        assert_eq!(result.stats.backend_calls, 0);
        assert_eq!(result.stats.prefix_lowered, 1);
        read.with_backend(|backend_read| {
            assert_eq!(*backend_read.scan_range_calls.borrow(), 0);
            Ok(())
        })
        .expect("scan range calls");
    }
}
