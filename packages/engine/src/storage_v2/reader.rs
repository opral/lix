use crate::backend_v2::{
    BackendError, BackendRead, GetOptions, Key, KeyRange, PointVisitor, Prefix, ProjectedValue,
    ProjectedValueRef, ScanChunk, ScanOptions, ScanResult, ScanVisitor,
};
use crate::storage_v2::{
    get_many_borrowed_indexed_values_for_physical_plan,
    get_many_borrowed_indexed_values_for_physical_plan_with_stats,
    get_many_borrowed_indexed_values_for_plan,
    get_many_borrowed_indexed_values_for_plan_with_stats, get_many_caller_order,
    get_many_caller_order_with_stats, get_many_indexed_values_caller_order,
    get_many_indexed_values_caller_order_with_stats, get_many_indexed_values_for_physical_plan,
    get_many_indexed_values_for_physical_plan_into,
    get_many_indexed_values_for_physical_plan_into_with_stats,
    get_many_indexed_values_for_physical_plan_with_stats, get_many_indexed_values_for_plan,
    get_many_indexed_values_for_plan_into, get_many_indexed_values_for_plan_into_with_stats,
    get_many_indexed_values_for_plan_with_stats, get_many_values_caller_order,
    get_many_values_caller_order_with_stats, scan_prefix, scan_prefix_into, scan_prefix_with_stats,
    scan_range, scan_range_into, scan_range_with_stats, visit_scan_prefix,
    visit_scan_prefix_with_stats, visit_scan_range, visit_scan_range_with_stats,
    visit_unique_point_values_for_physical_plan, visit_unique_point_values_for_plan,
    BorrowedIndexedPointValues, BorrowedScanChunk, BufferedIndexedPointValues, IndexedPointValues,
    PhysicalPointRequestPlan, PointRequestPlan, PointSlot, PointValueBuffer, StorageReadResult,
    StorageReadScope, StorageReadStats, StorageScanBuffer, StorageSpace,
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

    fn get_many_indexed_values_caller_order(
        &self,
        space: StorageSpace,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<IndexedPointValues, BackendError>;

    fn get_many_indexed_values_caller_order_with_stats(
        &self,
        space: StorageSpace,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<StorageReadResult<IndexedPointValues>, BackendError>;

    fn get_many_indexed_values_for_plan(
        &self,
        space: StorageSpace,
        plan: &PointRequestPlan,
        opts: GetOptions<'_>,
    ) -> Result<IndexedPointValues, BackendError>;

    fn get_many_indexed_values_for_plan_with_stats(
        &self,
        space: StorageSpace,
        plan: &PointRequestPlan,
        opts: GetOptions<'_>,
    ) -> Result<StorageReadResult<IndexedPointValues>, BackendError>;

    fn get_many_indexed_values_for_physical_plan(
        &self,
        plan: &PhysicalPointRequestPlan,
        opts: GetOptions<'_>,
    ) -> Result<IndexedPointValues, BackendError>;

    fn get_many_indexed_values_for_physical_plan_with_stats(
        &self,
        plan: &PhysicalPointRequestPlan,
        opts: GetOptions<'_>,
    ) -> Result<StorageReadResult<IndexedPointValues>, BackendError>;

    fn get_many_borrowed_indexed_values_for_plan<'a>(
        &self,
        space: StorageSpace,
        plan: &'a PointRequestPlan,
        opts: GetOptions<'_>,
    ) -> Result<BorrowedIndexedPointValues<'a>, BackendError>;

    fn get_many_borrowed_indexed_values_for_plan_with_stats<'a>(
        &self,
        space: StorageSpace,
        plan: &'a PointRequestPlan,
        opts: GetOptions<'_>,
    ) -> Result<StorageReadResult<BorrowedIndexedPointValues<'a>>, BackendError>;

    fn get_many_borrowed_indexed_values_for_physical_plan<'a>(
        &self,
        plan: &'a PhysicalPointRequestPlan,
        opts: GetOptions<'_>,
    ) -> Result<BorrowedIndexedPointValues<'a>, BackendError>;

    fn get_many_borrowed_indexed_values_for_physical_plan_with_stats<'a>(
        &self,
        plan: &'a PhysicalPointRequestPlan,
        opts: GetOptions<'_>,
    ) -> Result<StorageReadResult<BorrowedIndexedPointValues<'a>>, BackendError>;

    fn get_many_indexed_values_for_plan_into<'plan, 'buf>(
        &self,
        space: StorageSpace,
        plan: &'plan PointRequestPlan,
        opts: GetOptions<'_>,
        buffer: &'buf mut PointValueBuffer,
    ) -> Result<BufferedIndexedPointValues<'plan, 'buf>, BackendError>;

    fn get_many_indexed_values_for_plan_into_with_stats<'plan, 'buf>(
        &self,
        space: StorageSpace,
        plan: &'plan PointRequestPlan,
        opts: GetOptions<'_>,
        buffer: &'buf mut PointValueBuffer,
    ) -> Result<StorageReadResult<BufferedIndexedPointValues<'plan, 'buf>>, BackendError>;

    fn get_many_indexed_values_for_physical_plan_into<'plan, 'buf>(
        &self,
        plan: &'plan PhysicalPointRequestPlan,
        opts: GetOptions<'_>,
        buffer: &'buf mut PointValueBuffer,
    ) -> Result<BufferedIndexedPointValues<'plan, 'buf>, BackendError>;

    fn get_many_indexed_values_for_physical_plan_into_with_stats<'plan, 'buf>(
        &self,
        plan: &'plan PhysicalPointRequestPlan,
        opts: GetOptions<'_>,
        buffer: &'buf mut PointValueBuffer,
    ) -> Result<StorageReadResult<BufferedIndexedPointValues<'plan, 'buf>>, BackendError>;

    fn visit_unique_point_values_for_plan<V>(
        &self,
        space: StorageSpace,
        plan: &PointRequestPlan,
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<StorageReadStats, BackendError>
    where
        V: PointVisitor + ?Sized;

    fn visit_unique_point_values_for_physical_plan<V>(
        &self,
        plan: &PhysicalPointRequestPlan,
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<StorageReadStats, BackendError>
    where
        V: PointVisitor + ?Sized;

    fn scan_range(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: ScanOptions<'_>,
    ) -> Result<ScanChunk, BackendError>;

    fn scan_prefix(
        &self,
        space: StorageSpace,
        prefix: Prefix,
        opts: ScanOptions<'_>,
    ) -> Result<ScanChunk, BackendError>;

    fn scan_range_into<'a>(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: ScanOptions<'_>,
        buffer: &'a mut StorageScanBuffer,
    ) -> Result<BorrowedScanChunk<'a>, BackendError>;

    fn scan_prefix_into<'a>(
        &self,
        space: StorageSpace,
        prefix: Prefix,
        opts: ScanOptions<'_>,
        buffer: &'a mut StorageScanBuffer,
    ) -> Result<BorrowedScanChunk<'a>, BackendError>;

    fn visit_scan_range<V>(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: ScanOptions<'_>,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized;

    fn visit_scan_prefix<V>(
        &self,
        space: StorageSpace,
        prefix: Prefix,
        opts: ScanOptions<'_>,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized;

    fn visit_scan_range_with_stats<V>(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: ScanOptions<'_>,
        visitor: &mut V,
    ) -> Result<StorageReadResult<ScanResult>, BackendError>
    where
        V: ScanVisitor + ?Sized;

    fn visit_scan_prefix_with_stats<V>(
        &self,
        space: StorageSpace,
        prefix: Prefix,
        opts: ScanOptions<'_>,
        visitor: &mut V,
    ) -> Result<StorageReadResult<ScanResult>, BackendError>
    where
        V: ScanVisitor + ?Sized;

    fn scan_range_with_stats(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: ScanOptions<'_>,
    ) -> Result<StorageReadResult<ScanChunk>, BackendError>;

    fn scan_prefix_with_stats(
        &self,
        space: StorageSpace,
        prefix: Prefix,
        opts: ScanOptions<'_>,
    ) -> Result<StorageReadResult<ScanChunk>, BackendError>;
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

    fn get_many_indexed_values_caller_order(
        &self,
        space: StorageSpace,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<IndexedPointValues, BackendError> {
        get_many_indexed_values_caller_order(self.backend_read(), space.id, keys, opts)
    }

    fn get_many_indexed_values_caller_order_with_stats(
        &self,
        space: StorageSpace,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<StorageReadResult<IndexedPointValues>, BackendError> {
        get_many_indexed_values_caller_order_with_stats(self.backend_read(), space.id, keys, opts)
    }

    fn get_many_indexed_values_for_plan(
        &self,
        space: StorageSpace,
        plan: &PointRequestPlan,
        opts: GetOptions<'_>,
    ) -> Result<IndexedPointValues, BackendError> {
        get_many_indexed_values_for_plan(self.backend_read(), space.id, plan, opts)
    }

    fn get_many_indexed_values_for_plan_with_stats(
        &self,
        space: StorageSpace,
        plan: &PointRequestPlan,
        opts: GetOptions<'_>,
    ) -> Result<StorageReadResult<IndexedPointValues>, BackendError> {
        get_many_indexed_values_for_plan_with_stats(self.backend_read(), space.id, plan, opts)
    }

    fn get_many_indexed_values_for_physical_plan(
        &self,
        plan: &PhysicalPointRequestPlan,
        opts: GetOptions<'_>,
    ) -> Result<IndexedPointValues, BackendError> {
        get_many_indexed_values_for_physical_plan(self.backend_read(), plan, opts)
    }

    fn get_many_indexed_values_for_physical_plan_with_stats(
        &self,
        plan: &PhysicalPointRequestPlan,
        opts: GetOptions<'_>,
    ) -> Result<StorageReadResult<IndexedPointValues>, BackendError> {
        get_many_indexed_values_for_physical_plan_with_stats(self.backend_read(), plan, opts)
    }

    fn get_many_borrowed_indexed_values_for_plan<'a>(
        &self,
        space: StorageSpace,
        plan: &'a PointRequestPlan,
        opts: GetOptions<'_>,
    ) -> Result<BorrowedIndexedPointValues<'a>, BackendError> {
        get_many_borrowed_indexed_values_for_plan(self.backend_read(), space.id, plan, opts)
    }

    fn get_many_borrowed_indexed_values_for_plan_with_stats<'a>(
        &self,
        space: StorageSpace,
        plan: &'a PointRequestPlan,
        opts: GetOptions<'_>,
    ) -> Result<StorageReadResult<BorrowedIndexedPointValues<'a>>, BackendError> {
        get_many_borrowed_indexed_values_for_plan_with_stats(
            self.backend_read(),
            space.id,
            plan,
            opts,
        )
    }

    fn get_many_borrowed_indexed_values_for_physical_plan<'a>(
        &self,
        plan: &'a PhysicalPointRequestPlan,
        opts: GetOptions<'_>,
    ) -> Result<BorrowedIndexedPointValues<'a>, BackendError> {
        get_many_borrowed_indexed_values_for_physical_plan(self.backend_read(), plan, opts)
    }

    fn get_many_borrowed_indexed_values_for_physical_plan_with_stats<'a>(
        &self,
        plan: &'a PhysicalPointRequestPlan,
        opts: GetOptions<'_>,
    ) -> Result<StorageReadResult<BorrowedIndexedPointValues<'a>>, BackendError> {
        get_many_borrowed_indexed_values_for_physical_plan_with_stats(
            self.backend_read(),
            plan,
            opts,
        )
    }

    fn get_many_indexed_values_for_plan_into<'plan, 'buf>(
        &self,
        space: StorageSpace,
        plan: &'plan PointRequestPlan,
        opts: GetOptions<'_>,
        buffer: &'buf mut PointValueBuffer,
    ) -> Result<BufferedIndexedPointValues<'plan, 'buf>, BackendError> {
        get_many_indexed_values_for_plan_into(self.backend_read(), space.id, plan, opts, buffer)
    }

    fn get_many_indexed_values_for_plan_into_with_stats<'plan, 'buf>(
        &self,
        space: StorageSpace,
        plan: &'plan PointRequestPlan,
        opts: GetOptions<'_>,
        buffer: &'buf mut PointValueBuffer,
    ) -> Result<StorageReadResult<BufferedIndexedPointValues<'plan, 'buf>>, BackendError> {
        get_many_indexed_values_for_plan_into_with_stats(
            self.backend_read(),
            space.id,
            plan,
            opts,
            buffer,
        )
    }

    fn get_many_indexed_values_for_physical_plan_into<'plan, 'buf>(
        &self,
        plan: &'plan PhysicalPointRequestPlan,
        opts: GetOptions<'_>,
        buffer: &'buf mut PointValueBuffer,
    ) -> Result<BufferedIndexedPointValues<'plan, 'buf>, BackendError> {
        get_many_indexed_values_for_physical_plan_into(self.backend_read(), plan, opts, buffer)
    }

    fn get_many_indexed_values_for_physical_plan_into_with_stats<'plan, 'buf>(
        &self,
        plan: &'plan PhysicalPointRequestPlan,
        opts: GetOptions<'_>,
        buffer: &'buf mut PointValueBuffer,
    ) -> Result<StorageReadResult<BufferedIndexedPointValues<'plan, 'buf>>, BackendError> {
        get_many_indexed_values_for_physical_plan_into_with_stats(
            self.backend_read(),
            plan,
            opts,
            buffer,
        )
    }

    fn visit_unique_point_values_for_plan<V>(
        &self,
        space: StorageSpace,
        plan: &PointRequestPlan,
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<StorageReadStats, BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        visit_unique_point_values_for_plan(self.backend_read(), space.id, plan, opts, visitor)
    }

    fn visit_unique_point_values_for_physical_plan<V>(
        &self,
        plan: &PhysicalPointRequestPlan,
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<StorageReadStats, BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        visit_unique_point_values_for_physical_plan(self.backend_read(), plan, opts, visitor)
    }

    fn scan_range(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: ScanOptions<'_>,
    ) -> Result<ScanChunk, BackendError> {
        scan_range(self.backend_read(), space.id, range, opts)
    }

    fn scan_prefix(
        &self,
        space: StorageSpace,
        prefix: Prefix,
        opts: ScanOptions<'_>,
    ) -> Result<ScanChunk, BackendError> {
        scan_prefix(self.backend_read(), space.id, prefix, opts)
    }

    fn scan_range_into<'a>(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: ScanOptions<'_>,
        buffer: &'a mut StorageScanBuffer,
    ) -> Result<BorrowedScanChunk<'a>, BackendError> {
        scan_range_into(self.backend_read(), space.id, range, opts, buffer)
    }

    fn scan_prefix_into<'a>(
        &self,
        space: StorageSpace,
        prefix: Prefix,
        opts: ScanOptions<'_>,
        buffer: &'a mut StorageScanBuffer,
    ) -> Result<BorrowedScanChunk<'a>, BackendError> {
        scan_prefix_into(self.backend_read(), space.id, prefix, opts, buffer)
    }

    fn visit_scan_range<V>(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: ScanOptions<'_>,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized,
    {
        visit_scan_range(self.backend_read(), space.id, range, opts, visitor)
    }

    fn visit_scan_prefix<V>(
        &self,
        space: StorageSpace,
        prefix: Prefix,
        opts: ScanOptions<'_>,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized,
    {
        visit_scan_prefix(self.backend_read(), space.id, prefix, opts, visitor)
    }

    fn visit_scan_range_with_stats<V>(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: ScanOptions<'_>,
        visitor: &mut V,
    ) -> Result<StorageReadResult<ScanResult>, BackendError>
    where
        V: ScanVisitor + ?Sized,
    {
        visit_scan_range_with_stats(self.backend_read(), space.id, range, opts, visitor)
    }

    fn visit_scan_prefix_with_stats<V>(
        &self,
        space: StorageSpace,
        prefix: Prefix,
        opts: ScanOptions<'_>,
        visitor: &mut V,
    ) -> Result<StorageReadResult<ScanResult>, BackendError>
    where
        V: ScanVisitor + ?Sized,
    {
        visit_scan_prefix_with_stats(self.backend_read(), space.id, prefix, opts, visitor)
    }

    fn scan_range_with_stats(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: ScanOptions<'_>,
    ) -> Result<StorageReadResult<ScanChunk>, BackendError> {
        scan_range_with_stats(self.backend_read(), space.id, range, opts)
    }

    fn scan_prefix_with_stats(
        &self,
        space: StorageSpace,
        prefix: Prefix,
        opts: ScanOptions<'_>,
    ) -> Result<StorageReadResult<ScanChunk>, BackendError> {
        scan_prefix_with_stats(self.backend_read(), space.id, prefix, opts)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::ops::Bound;

    use bytes::Bytes;

    use crate::backend_v2::{
        BackendError, BackendRead, ConformanceBackend, CoreProjection, GetOptions, Key, KeyRange,
        KeyRef, PointVisitor, Prefix, ProjectedValue, ProjectedValueRef, ReadOptions, ScanOptions,
        ScanResult, ScanVisitor, SpaceId, StoredValue, WriteOptions,
    };
    use crate::storage_v2::{
        PhysicalPointRequestPlan, PointRequestPlan, PointValueBuffer, StorageContext,
        StorageReader, StorageScanBuffer, StorageSpace,
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
        fn visit_many<V>(
            &self,
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

        fn visit_range<V>(
            &self,
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
        fn visit_many<V>(
            &self,
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

        fn visit_range<V>(
            &self,
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
            [
                space(1).encode_key(&key("b")),
                space(1).encode_key(&key("a")),
                space(1).encode_key(&key("missing"))
            ]
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
    fn point_reads_can_return_indexed_values_without_duplicate_value_clones() {
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

        let indexed = read
            .get_many_indexed_values_caller_order(
                space(1),
                &[key("b"), key("missing"), key("a"), key("b")],
                GetOptions::default(),
            )
            .expect("indexed caller order values");

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
        let plan = PointRequestPlan::new(&[key("b"), key("missing"), key("a"), key("b")]);

        assert_eq!(plan.len(), 4);
        assert_eq!(plan.unique_keys, vec![key("b"), key("missing"), key("a")]);
        assert_eq!(plan.requested_to_unique().to_vec(), vec![0, 1, 2, 0]);

        let result = read
            .get_many_indexed_values_for_plan_with_stats(space(1), &plan, GetOptions::default())
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

        let borrowed = read
            .get_many_borrowed_indexed_values_for_plan_with_stats(
                space(1),
                &plan,
                GetOptions::default(),
            )
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
    fn planned_point_reads_can_reuse_value_buffer() {
        let storage = StorageContext::new(ConformanceBackend::new());
        let mut writes = storage.new_write_set();
        writes.stage_put(space(1), key("a"), value("A"));
        writes.stage_put(space(1), key("b"), value("B"));
        writes.stage_put(space(1), key("c"), value("C"));
        storage
            .commit_write_set(writes, WriteOptions::default())
            .expect("seed");
        let read = storage
            .begin_read(ReadOptions::default())
            .expect("begin read");
        let first_plan = PointRequestPlan::new(&[key("b"), key("missing"), key("a"), key("b")]);
        let second_plan = PointRequestPlan::new(&[key("c")]);
        let mut buffer = PointValueBuffer::new();

        let first = read
            .get_many_indexed_values_for_plan_into_with_stats(
                space(1),
                &first_plan,
                GetOptions::default(),
                &mut buffer,
            )
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
        let second = read
            .get_many_indexed_values_for_plan_into_with_stats(
                space(1),
                &second_plan,
                GetOptions::default(),
                &mut buffer,
            )
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
        let plan = PointRequestPlan::from_unique_keys(vec![key("a"), key("b"), key("c")]);

        assert_eq!(plan.len(), 3);
        assert_eq!(plan.unique_keys, vec![key("a"), key("b"), key("c")]);
        assert_eq!(plan.requested_to_unique().to_vec(), vec![0, 1, 2]);
    }

    #[test]
    fn planned_point_reads_use_backend_requested_order_slots() {
        let read = crate::storage_v2::StorageReadScope::new(RequestedOrderRead::default());
        let plan = PointRequestPlan::new(&[key("b"), key("missing"), key("a"), key("b")]);

        let result = read
            .get_many_borrowed_indexed_values_for_plan_with_stats(
                space(1),
                &plan,
                GetOptions::default(),
            )
            .expect("borrowed planned indexed read");

        assert_eq!(
            read.backend_read().get_many_keys.borrow().as_slice(),
            [
                space(1).encode_key(&key("b")),
                space(1).encode_key(&key("missing")),
                space(1).encode_key(&key("a"))
            ]
        );
        assert_eq!(result.stats.requested_keys, 4);
        assert_eq!(result.stats.unique_backend_keys, 3);
        assert_eq!(result.stats.backend_calls, 1);
        assert_eq!(
            result.value.value_at(0),
            Some(&ProjectedValue::FullValue(Bytes::from_static(
                b"\0\0\0\x01b"
            )))
        );
        assert_eq!(result.value.value_at(1), None);
        assert_eq!(
            result.value.value_at(2),
            Some(&ProjectedValue::FullValue(Bytes::from_static(
                b"\0\0\0\x01a"
            )))
        );
    }

    #[test]
    fn physical_point_request_plan_reuses_encoded_backend_keys() {
        let read = crate::storage_v2::StorageReadScope::new(RequestedOrderRead::default());
        let plan = PhysicalPointRequestPlan::new(
            space(1),
            &[key("b"), key("missing"), key("a"), key("b")],
        );

        assert_eq!(
            plan.logical_unique_keys,
            vec![key("b"), key("missing"), key("a")]
        );
        assert_eq!(
            plan.physical_unique_keys,
            vec![
                space(1).encode_key(&key("b")),
                space(1).encode_key(&key("missing")),
                space(1).encode_key(&key("a")),
            ]
        );

        let result = read
            .get_many_borrowed_indexed_values_for_physical_plan_with_stats(
                &plan,
                GetOptions::default(),
            )
            .expect("borrowed physical planned indexed read");

        assert_eq!(
            read.backend_read().get_many_keys.borrow().as_slice(),
            plan.physical_unique_keys.as_slice()
        );
        assert_eq!(result.stats.requested_keys, 4);
        assert_eq!(result.stats.unique_backend_keys, 3);
        assert_eq!(result.stats.backend_calls, 1);
        assert_eq!(
            result.value.value_at(0),
            Some(&ProjectedValue::FullValue(Bytes::from_static(
                b"\0\0\0\x01b"
            )))
        );
        assert_eq!(result.value.value_at(1), None);
    }

    #[test]
    fn planned_point_reads_can_visit_unique_values_without_materializing_indexed_result() {
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
        let plan = PointRequestPlan::new(&[key("b"), key("missing"), key("a"), key("b")]);

        let mut visited = Vec::new();
        let stats = read
            .visit_unique_point_values_for_plan(
                space(1),
                &plan,
                GetOptions::default(),
                &mut |unique_index: usize, key: &Key, value: Option<ProjectedValueRef<'_>>| {
                    visited.push((
                        unique_index,
                        key.clone(),
                        value.map(|value| value.to_owned()),
                    ));
                    Ok(())
                },
            )
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
        let chunk = read
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
            chunk
                .entries
                .entries
                .into_iter()
                .map(|entry| (entry.key, entry.value))
                .collect::<Vec<_>>(),
            vec![
                (key("aa"), ProjectedValue::KeyOnly),
                (key("ab"), ProjectedValue::KeyOnly),
            ]
        );
        assert!(!chunk.has_more);
    }

    #[test]
    fn scan_range_into_reuses_storage_buffer() {
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
        let mut buffer = StorageScanBuffer::with_capacity(8);

        {
            let chunk = read
                .scan_range_into(
                    space(1),
                    KeyRange {
                        lower: Bound::Included(key("a")),
                        upper: Bound::Excluded(key("b")),
                    },
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
                    .entries
                    .iter()
                    .map(|entry| (&entry.key, &entry.value))
                    .collect::<Vec<_>>(),
                vec![
                    (&key("aa"), &ProjectedValue::KeyOnly),
                    (&key("ab"), &ProjectedValue::KeyOnly),
                ]
            );
            assert!(!chunk.has_more);
        }

        let capacity_after_first_scan = buffer.capacity();
        assert!(capacity_after_first_scan >= 8);

        {
            let chunk = read
                .scan_prefix_into(
                    space(1),
                    Prefix {
                        bytes: Bytes::from_static(b"a"),
                    },
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
            assert!(!chunk.has_more);
        }

        assert_eq!(buffer.capacity(), capacity_after_first_scan);
    }

    #[test]
    fn visit_scan_prefix_lowers_without_materializing_entries() {
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
        let mut visited = Vec::new();
        let result = read
            .visit_scan_prefix(
                space(1),
                Prefix {
                    bytes: Bytes::from_static(b"a"),
                },
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

        assert_eq!(result.emitted, 2);
        assert!(!result.has_more);
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
        assert_eq!(
            range.lower,
            Bound::Included(space(1).encode_key(&key_bytes(b"a\xff")))
        );
        assert_eq!(range.upper, Bound::Excluded(space(1).encode_key(&key("b"))));
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
        assert_eq!(result.stats.range_scan_chunks, 0);
        assert_eq!(result.stats.prefix_scan_chunks, 1);
        assert_eq!(result.stats.scan_full_value_chunks, 1);
        assert_eq!(*read.backend_read().scan_range_calls.borrow(), 1);
    }

    #[test]
    fn visit_scan_reports_trace_stats() {
        let storage = StorageContext::new(ConformanceBackend::new());
        let mut writes = storage.new_write_set();
        writes.stage_put(space(1), key("aa"), value("AA"));
        writes.stage_put(space(1), key("ab"), value("AB"));
        writes.stage_put(space(1), key("ac"), value("AC"));
        storage
            .commit_write_set(writes, WriteOptions::default())
            .expect("seed");
        let read = storage
            .begin_read(ReadOptions::default())
            .expect("begin read");

        let result = read
            .visit_scan_prefix_with_stats(
                space(1),
                Prefix {
                    bytes: Bytes::from_static(b"a"),
                },
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
