use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use lix_engine::{
    project_backend_read4_value_part, Backend, BackendKvAccessSegment, BackendKvEntryPage,
    BackendKvExistsBatch, BackendKvExistsGroup, BackendKvGetRequest, BackendKvKeyPage,
    BackendKvRead4Order, BackendKvRead4Page, BackendKvRead4Projection, BackendKvReadV3Presence,
    BackendKvScanRange, BackendKvScanRequest, BackendKvTableReadRequest, BackendKvValueBatch,
    BackendKvValueGroup, BackendKvValuePage, BackendKvWriteBatch, BackendKvWriteOp,
    BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction, BytePageBuilder,
    LixError,
};
use rocksdb::{Direction, IteratorMode, Options, WriteBatch, DB};
use tempfile::TempDir;

#[derive(Clone)]
pub(crate) struct RocksDbBenchBackend {
    inner: Arc<RocksDbBenchInner>,
}

struct RocksDbBenchInner {
    db: DB,
    _dir: TempDir,
}

pub(crate) struct RocksDbBenchTransaction {
    inner: Arc<RocksDbBenchInner>,
    pending: BTreeMap<Vec<u8>, PendingWrite>,
    pending_range_deletes: Vec<EncodedRange>,
    commit_ops: Vec<EncodedWriteOp>,
}

enum PendingWrite {
    Put(usize),
    Delete,
}

enum EncodedWriteOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    DeleteRange { range: EncodedRange },
}

fn commit_op_value(ops: &[EncodedWriteOp], index: usize) -> Option<&[u8]> {
    match ops.get(index)? {
        EncodedWriteOp::Put { value, .. } => Some(value),
        EncodedWriteOp::Delete { .. } | EncodedWriteOp::DeleteRange { .. } => None,
    }
}

#[derive(Clone)]
struct EncodedRange {
    start: Vec<u8>,
    end: Vec<u8>,
}

impl EncodedRange {
    fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && key < self.end.as_slice()
    }
}

impl RocksDbBenchBackend {
    pub(crate) fn new() -> Result<Self, LixError> {
        let dir = TempDir::new().map_err(io_error)?;
        let db = open_rocksdb(dir.path())?;
        Ok(Self {
            inner: Arc::new(RocksDbBenchInner { db, _dir: dir }),
        })
    }

    #[allow(dead_code)]
    pub(crate) fn path(&self) -> &Path {
        self.inner._dir.path()
    }
}

#[async_trait]
impl Backend for RocksDbBenchBackend {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        Ok(Box::new(RocksDbBenchTransaction {
            inner: Arc::clone(&self.inner),
            pending: BTreeMap::new(),
            pending_range_deletes: Vec::new(),
            commit_ops: Vec::new(),
        }))
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        Ok(Box::new(RocksDbBenchTransaction {
            inner: Arc::clone(&self.inner),
            pending: BTreeMap::new(),
            pending_range_deletes: Vec::new(),
            commit_ops: Vec::new(),
        }))
    }
}

#[async_trait]
impl BackendReadTransaction for RocksDbBenchTransaction {
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let namespace = group.namespace.clone();
            let mut resolved_values = vec![None; group.keys.len()];
            let mut committed_keys = Vec::new();
            let mut committed_positions = Vec::new();
            for (position, key) in group.keys.into_iter().enumerate() {
                let encoded_key = encode_key(namespace.as_str(), &key);
                match self.pending.get(&encoded_key) {
                    Some(PendingWrite::Put(op_index)) => {
                        resolved_values[position] = Some(
                            commit_op_value(&self.commit_ops, *op_index)
                                .expect("pending put should point at commit put")
                                .to_vec(),
                        )
                    }
                    Some(PendingWrite::Delete) => {}
                    None if encoded_in_ranges(&encoded_key, &self.pending_range_deletes) => {}
                    None => {
                        committed_positions.push(position);
                        committed_keys.push(encoded_key);
                    }
                }
            }
            let committed_values = self.inner.db.multi_get(committed_keys);
            for (position, value) in committed_positions.into_iter().zip(committed_values) {
                match value.map_err(rocksdb_error)? {
                    Some(value) => resolved_values[position] = Some(value),
                    None => {}
                }
            }
            let mut values = BytePageBuilder::with_capacity(resolved_values.len(), 0);
            let mut present = Vec::with_capacity(resolved_values.len());
            for value in resolved_values {
                if let Some(value) = value {
                    values.push(value);
                    present.push(true);
                } else {
                    values.push([]);
                    present.push(false);
                }
            }
            groups.push(BackendKvValueGroup::new(
                namespace,
                values.finish(),
                present,
            ));
        }
        Ok(BackendKvValueBatch { groups })
    }

    async fn exists_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvExistsBatch, LixError> {
        rocksdb_get_exists_many(
            &self.inner.db,
            &self.pending,
            &self.pending_range_deletes,
            request,
        )
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        rocksdb_scan_keys(
            &self.inner.db,
            &self.pending,
            &self.pending_range_deletes,
            request,
        )
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        rocksdb_scan_values(
            &self.inner.db,
            &self.pending,
            &self.pending_range_deletes,
            &self.commit_ops,
            request,
        )
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        rocksdb_scan_entries(
            &self.inner.db,
            &self.pending,
            &self.pending_range_deletes,
            &self.commit_ops,
            request,
        )
    }

    async fn read4(
        &mut self,
        request: BackendKvTableReadRequest,
    ) -> Result<BackendKvRead4Page, LixError> {
        rocksdb_read4(
            &self.inner.db,
            &self.pending,
            &self.pending_range_deletes,
            &self.commit_ops,
            request,
        )
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        Ok(())
    }
}

#[async_trait]
impl BackendWriteTransaction for RocksDbBenchTransaction {
    async fn write_kv_batch(
        &mut self,
        batch: BackendKvWriteBatch,
    ) -> Result<BackendKvWriteStats, LixError> {
        let mut stats = BackendKvWriteStats::default();
        for group in batch.groups {
            let (namespace, ops) = group.into_ops();
            for op in ops {
                match op {
                    BackendKvWriteOp::Put { key, value } => {
                        stats.puts += 1;
                        stats.bytes_written += key.len() + value.len();
                        let encoded_key = encode_key(namespace.as_str(), &key);
                        let op_index = self.commit_ops.len();
                        self.pending
                            .insert(encoded_key.clone(), PendingWrite::Put(op_index));
                        self.commit_ops.push(EncodedWriteOp::Put {
                            key: encoded_key,
                            value,
                        });
                    }
                    BackendKvWriteOp::Delete { key } => {
                        stats.deletes += 1;
                        stats.bytes_written += key.len();
                        let encoded_key = encode_key(namespace.as_str(), &key);
                        self.pending
                            .insert(encoded_key.clone(), PendingWrite::Delete);
                        self.commit_ops
                            .push(EncodedWriteOp::Delete { key: encoded_key });
                    }
                    BackendKvWriteOp::DeleteRange { range } => {
                        let encoded_range = encoded_range(namespace.as_str(), &range);
                        stats.delete_ranges += 1;
                        stats.bytes_written += delete_range_bytes(&range);
                        self.pending.retain(|key, _| !encoded_range.contains(key));
                        self.pending_range_deletes.push(encoded_range.clone());
                        self.commit_ops.push(EncodedWriteOp::DeleteRange {
                            range: encoded_range,
                        });
                    }
                }
            }
        }
        Ok(stats)
    }

    async fn commit(self: Box<Self>) -> Result<(), LixError> {
        let mut write_batch = WriteBatch::default();
        for op in self.commit_ops {
            match op {
                EncodedWriteOp::Put { key, value } => write_batch.put(key, value),
                EncodedWriteOp::Delete { key } => write_batch.delete(key),
                EncodedWriteOp::DeleteRange { range } => {
                    write_batch.delete_range(range.start, range.end)
                }
            }
        }
        self.inner.db.write(write_batch).map_err(rocksdb_error)?;
        Ok(())
    }
}

fn open_rocksdb(path: &Path) -> Result<DB, LixError> {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_use_fsync(false);
    options.set_write_buffer_size(64 * 1024 * 1024);
    DB::open(&options, path).map_err(rocksdb_error)
}

fn rocksdb_get_exists_many(
    db: &DB,
    pending: &BTreeMap<Vec<u8>, PendingWrite>,
    pending_range_deletes: &[EncodedRange],
    request: BackendKvGetRequest,
) -> Result<BackendKvExistsBatch, LixError> {
    let mut groups = Vec::with_capacity(request.groups.len());
    for group in request.groups {
        let namespace = group.namespace.clone();
        let mut exists = vec![false; group.keys.len()];
        let mut committed = Vec::new();

        for (position, key) in group.keys.into_iter().enumerate() {
            let encoded_key = encode_key(namespace.as_str(), &key);
            match pending.get(&encoded_key) {
                Some(PendingWrite::Put(_)) => exists[position] = true,
                Some(PendingWrite::Delete) => {}
                None if encoded_in_ranges(&encoded_key, pending_range_deletes) => {}
                None => {
                    committed.push((encoded_key, position));
                }
            }
        }

        fill_committed_exists(db, &mut exists, committed)?;
        groups.push(BackendKvExistsGroup { namespace, exists });
    }

    Ok(BackendKvExistsBatch { groups })
}

fn fill_committed_exists(
    db: &DB,
    exists: &mut [bool],
    mut committed: Vec<(Vec<u8>, usize)>,
) -> Result<(), LixError> {
    if committed.is_empty() {
        return Ok(());
    }

    committed.sort_by(|left, right| left.0.cmp(&right.0));
    let mut iter = db.raw_iterator();
    iter.seek(&committed[0].0);

    for (target_key, position) in committed {
        while iter.valid() {
            let Some(current_key) = iter.key() else {
                break;
            };
            if current_key >= target_key.as_slice() {
                break;
            }
            iter.next();
        }

        if !iter.valid() {
            iter.status().map_err(rocksdb_error)?;
            break;
        }

        if iter
            .key()
            .is_some_and(|current_key| current_key == target_key.as_slice())
        {
            exists[position] = true;
        }
    }

    iter.status().map_err(rocksdb_error)?;
    Ok(())
}

fn rocksdb_scan_keys(
    db: &DB,
    pending: &BTreeMap<Vec<u8>, PendingWrite>,
    pending_range_deletes: &[EncodedRange],
    request: BackendKvScanRequest,
) -> Result<BackendKvKeyPage, LixError> {
    let bounds = ScanBounds::new(&request);
    if pending.is_empty() && pending_range_deletes.is_empty() {
        return rocksdb_scan_committed_keys(db, request, bounds);
    }

    let mut merged = BTreeSet::new();
    let mut iter = db.raw_iterator();
    iter.seek(&bounds.start_encoded);
    while iter.valid() {
        let Some(encoded_key) = iter.key() else {
            break;
        };
        if !bounds.contains_encoded(encoded_key) {
            break;
        }
        if encoded_in_ranges(encoded_key, pending_range_deletes) {
            iter.next();
            continue;
        }
        let logical_key = decode_key(&request.namespace, encoded_key)?;
        if !key_after_cursor(&request, &logical_key) {
            iter.next();
            continue;
        }
        merged.insert(logical_key);
        iter.next();
    }
    iter.status().map_err(rocksdb_error)?;

    for (encoded_key, write) in
        pending.range(bounds.start_encoded.clone()..bounds.end_encoded.clone())
    {
        if !bounds.contains_encoded(encoded_key) {
            continue;
        }
        let logical_key = decode_key(&request.namespace, encoded_key)?;
        if !key_in_range(&logical_key, &request.range) || !key_after_cursor(&request, &logical_key)
        {
            continue;
        }
        match write {
            PendingWrite::Put(_) => {
                merged.insert(logical_key);
            }
            PendingWrite::Delete => {
                merged.remove(&logical_key);
            }
        }
    }
    Ok(key_page_from_iter(merged, request.limit))
}

fn rocksdb_scan_values(
    db: &DB,
    pending: &BTreeMap<Vec<u8>, PendingWrite>,
    pending_range_deletes: &[EncodedRange],
    commit_ops: &[EncodedWriteOp],
    request: BackendKvScanRequest,
) -> Result<BackendKvValuePage, LixError> {
    let bounds = ScanBounds::new(&request);
    if pending.is_empty() && pending_range_deletes.is_empty() {
        return rocksdb_scan_committed_values(db, request, bounds);
    }

    let mut merged = BTreeMap::new();
    for item in db.iterator(IteratorMode::From(
        &bounds.start_encoded,
        Direction::Forward,
    )) {
        let (encoded_key, value) = item.map_err(rocksdb_error)?;
        let encoded_key = encoded_key.as_ref();
        if !bounds.contains_encoded(encoded_key) {
            break;
        }
        if encoded_in_ranges(encoded_key, pending_range_deletes) {
            continue;
        }
        let logical_key = decode_key(&request.namespace, encoded_key)?;
        if !key_after_cursor(&request, &logical_key) {
            continue;
        }
        merged.insert(logical_key, value.to_vec());
    }
    overlay_pending_values(&mut merged, pending, commit_ops, &request, &bounds)?;
    Ok(value_page_from_iter(merged, request.limit))
}

fn rocksdb_scan_entries(
    db: &DB,
    pending: &BTreeMap<Vec<u8>, PendingWrite>,
    pending_range_deletes: &[EncodedRange],
    commit_ops: &[EncodedWriteOp],
    request: BackendKvScanRequest,
) -> Result<BackendKvEntryPage, LixError> {
    let bounds = ScanBounds::new(&request);
    if pending.is_empty() && pending_range_deletes.is_empty() {
        return rocksdb_scan_committed_entries(db, request, bounds);
    }
    let mut merged = BTreeMap::new();
    for item in db.iterator(IteratorMode::From(
        &bounds.start_encoded,
        Direction::Forward,
    )) {
        let (key, value) = item.map_err(rocksdb_error)?;
        let key = key.as_ref();
        if !bounds.contains_encoded(key) {
            break;
        }
        if encoded_in_ranges(key, pending_range_deletes) {
            continue;
        }
        let logical_key = decode_key(&request.namespace, key)?;
        if !key_after_cursor(&request, &logical_key) {
            continue;
        }
        merged.insert(logical_key, value.to_vec());
    }
    overlay_pending_values(&mut merged, pending, commit_ops, &request, &bounds)?;
    Ok(entry_page_from_iter(merged, request.limit))
}

fn rocksdb_read4(
    db: &DB,
    pending: &BTreeMap<Vec<u8>, PendingWrite>,
    pending_range_deletes: &[EncodedRange],
    commit_ops: &[EncodedWriteOp],
    request: BackendKvTableReadRequest,
) -> Result<BackendKvRead4Page, LixError> {
    if request.residual_filter.is_some() {
        return Err(LixError::unknown(
            "rocksdb bench read4 cannot apply residual filters",
        ));
    }
    if request.session.is_some() {
        return Err(LixError::unknown(
            "rocksdb bench read4 does not support read sessions",
        ));
    }

    let namespace = request.table.namespace;
    let mut keyed = Vec::new();
    let mut run_spans = Vec::new();
    let mut spans = Vec::new();
    for segment in request.access {
        match segment {
            BackendKvAccessSegment::Points {
                keys,
                request_indexes,
            } => rocksdb_read4_push_indexed_keys(&mut keyed, keys, request_indexes)?,
            BackendKvAccessSegment::Run {
                lower,
                upper,
                keys,
                request_indexes,
            } => {
                run_spans.push((lower, upper));
                rocksdb_read4_push_indexed_keys(&mut keyed, keys, request_indexes)?;
            }
            BackendKvAccessSegment::Span { lower, upper } => spans.push((lower, upper)),
        }
    }
    if !keyed.is_empty() && !spans.is_empty() {
        return Err(LixError::unknown(
            "rocksdb bench read4 cannot mix point/run and span access",
        ));
    }
    if !keyed.is_empty() || spans.is_empty() {
        if request.after.is_some() {
            return Err(LixError::unknown(
                "rocksdb bench read4 point/run access does not support after cursors",
            ));
        }
        if run_spans.is_empty() {
            return rocksdb_read4_points(
                db,
                pending,
                pending_range_deletes,
                commit_ops,
                namespace,
                keyed,
                request.projection,
                request.output_order,
            );
        }
        return rocksdb_read4_runs(
            db,
            pending,
            pending_range_deletes,
            commit_ops,
            namespace,
            keyed,
            run_spans,
            request.projection,
            request.output_order,
        );
    }
    if request.output_order != BackendKvRead4Order::KeyOrder {
        return Err(LixError::unknown(
            "rocksdb bench read4 span access requires key order output",
        ));
    }
    rocksdb_read4_spans(
        db,
        pending,
        pending_range_deletes,
        commit_ops,
        namespace,
        spans,
        request.after,
        request.limit.unwrap_or(usize::MAX),
        request.projection,
    )
}

fn rocksdb_read4_push_indexed_keys(
    output: &mut Vec<(u32, Vec<u8>)>,
    keys: Vec<Vec<u8>>,
    request_indexes: Vec<u32>,
) -> Result<(), LixError> {
    if keys.len() != request_indexes.len() {
        return Err(LixError::unknown("rocksdb bench read4 key/index mismatch"));
    }
    output.extend(request_indexes.into_iter().zip(keys));
    Ok(())
}

fn rocksdb_read4_points(
    db: &DB,
    pending: &BTreeMap<Vec<u8>, PendingWrite>,
    pending_range_deletes: &[EncodedRange],
    commit_ops: &[EncodedWriteOp],
    namespace: String,
    mut keyed: Vec<(u32, Vec<u8>)>,
    projection: BackendKvRead4Projection,
    order: BackendKvRead4Order,
) -> Result<BackendKvRead4Page, LixError> {
    match order {
        BackendKvRead4Order::RequestOrder => keyed.sort_by_key(|(index, _)| *index),
        BackendKvRead4Order::KeyOrder => keyed.sort_by(|left, right| left.1.cmp(&right.1)),
    }
    let resolved = rocksdb_read4_point_values(
        db,
        pending,
        pending_range_deletes,
        commit_ops,
        &namespace,
        &keyed,
    )?;
    rocksdb_read4_keyed_page(keyed, resolved, projection, order)
}

fn rocksdb_read4_runs(
    db: &DB,
    pending: &BTreeMap<Vec<u8>, PendingWrite>,
    pending_range_deletes: &[EncodedRange],
    commit_ops: &[EncodedWriteOp],
    namespace: String,
    mut keyed: Vec<(u32, Vec<u8>)>,
    run_spans: Vec<(Vec<u8>, Vec<u8>)>,
    projection: BackendKvRead4Projection,
    order: BackendKvRead4Order,
) -> Result<BackendKvRead4Page, LixError> {
    match order {
        BackendKvRead4Order::RequestOrder => keyed.sort_by_key(|(index, _)| *index),
        BackendKvRead4Order::KeyOrder => keyed.sort_by(|left, right| left.1.cmp(&right.1)),
    }
    let values_by_key = rocksdb_read4_collect_spans(
        db,
        pending,
        pending_range_deletes,
        commit_ops,
        &namespace,
        run_spans,
        None,
    )?;
    let resolved = keyed
        .iter()
        .map(|(_, key)| values_by_key.get(key).cloned())
        .collect();
    rocksdb_read4_keyed_page(keyed, resolved, projection, order)
}

fn rocksdb_read4_keyed_page(
    keyed: Vec<(u32, Vec<u8>)>,
    resolved: Vec<Option<Vec<u8>>>,
    projection: BackendKvRead4Projection,
    order: BackendKvRead4Order,
) -> Result<BackendKvRead4Page, LixError> {
    let request_indexes = match order {
        BackendKvRead4Order::RequestOrder => None,
        BackendKvRead4Order::KeyOrder => Some(keyed.iter().map(|(index, _)| *index).collect()),
    };
    let mut keys = BytePageBuilder::with_capacity(keyed.len(), 0);
    let mut present = Vec::with_capacity(keyed.len());
    let mut value_builders = rocksdb_read4_value_builders(&projection);
    for ((_, key), value) in keyed.into_iter().zip(resolved) {
        keys.push(&key);
        present.push(value.is_some());
        if let Some(value) = value {
            rocksdb_read4_push_projected(&mut value_builders, &projection, &value)?;
        } else {
            for builder in &mut value_builders {
                builder.push([]);
            }
        }
    }
    Ok(BackendKvRead4Page {
        keys: keys.finish(),
        presence: BackendKvReadV3Presence::bitmap(present),
        values: value_builders
            .into_iter()
            .map(BytePageBuilder::finish)
            .collect(),
        request_indexes,
        resume_after: None,
    })
}

fn rocksdb_read4_point_values(
    db: &DB,
    pending: &BTreeMap<Vec<u8>, PendingWrite>,
    pending_range_deletes: &[EncodedRange],
    commit_ops: &[EncodedWriteOp],
    namespace: &str,
    keyed: &[(u32, Vec<u8>)],
) -> Result<Vec<Option<Vec<u8>>>, LixError> {
    let mut resolved_values = vec![None; keyed.len()];
    let mut committed_keys = Vec::new();
    let mut committed_positions = Vec::new();
    for (position, (_, key)) in keyed.iter().enumerate() {
        let encoded_key = encode_key(namespace, key);
        match pending.get(&encoded_key) {
            Some(PendingWrite::Put(op_index)) => {
                resolved_values[position] = Some(
                    commit_op_value(commit_ops, *op_index)
                        .expect("pending put should point at commit put")
                        .to_vec(),
                )
            }
            Some(PendingWrite::Delete) => {}
            None if encoded_in_ranges(&encoded_key, pending_range_deletes) => {}
            None => {
                committed_positions.push(position);
                committed_keys.push(encoded_key);
            }
        }
    }
    for (position, value) in committed_positions
        .into_iter()
        .zip(db.multi_get(committed_keys))
    {
        if let Some(value) = value.map_err(rocksdb_error)? {
            resolved_values[position] = Some(value);
        }
    }
    Ok(resolved_values)
}

fn rocksdb_read4_spans(
    db: &DB,
    pending: &BTreeMap<Vec<u8>, PendingWrite>,
    pending_range_deletes: &[EncodedRange],
    commit_ops: &[EncodedWriteOp],
    namespace: String,
    spans: Vec<(Vec<u8>, Vec<u8>)>,
    after: Option<Vec<u8>>,
    limit: usize,
    projection: BackendKvRead4Projection,
) -> Result<BackendKvRead4Page, LixError> {
    if pending.is_empty() && pending_range_deletes.is_empty() {
        return rocksdb_read4_committed_spans(db, namespace, spans, after, limit, projection);
    }
    let values_by_key = rocksdb_read4_collect_spans(
        db,
        pending,
        pending_range_deletes,
        commit_ops,
        &namespace,
        spans,
        after.as_deref(),
    )?;
    let mut keys = BytePageBuilder::new();
    let mut value_builders = rocksdb_read4_value_builders(&projection);
    let mut count = 0;
    let mut resume_after_candidate = None;
    for (key, value) in values_by_key {
        if count < limit {
            resume_after_candidate = Some(key.clone());
            keys.push(&key);
            rocksdb_read4_push_projected(&mut value_builders, &projection, &value)?;
        }
        count += 1;
        if count > limit {
            break;
        }
    }
    let resume_after = (count > limit).then_some(resume_after_candidate).flatten();
    Ok(BackendKvRead4Page {
        keys: keys.finish(),
        presence: BackendKvReadV3Presence::All,
        values: value_builders
            .into_iter()
            .map(BytePageBuilder::finish)
            .collect(),
        request_indexes: None,
        resume_after,
    })
}

fn rocksdb_read4_committed_spans(
    db: &DB,
    namespace: String,
    mut spans: Vec<(Vec<u8>, Vec<u8>)>,
    after: Option<Vec<u8>>,
    limit: usize,
    projection: BackendKvRead4Projection,
) -> Result<BackendKvRead4Page, LixError> {
    spans.retain(|(lower, upper)| upper.is_empty() || lower < upper);
    spans.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

    let mut keys = BytePageBuilder::new();
    let mut value_builders = rocksdb_read4_value_builders(&projection);
    let mut count = 0;
    let mut resume_after_candidate = None;
    let mut seen = BTreeSet::new();
    for (span_index, (lower, upper)) in spans.iter().enumerate() {
        let bounds = Read4Bounds::new(&namespace, lower, upper);
        match &projection {
            BackendKvRead4Projection::KeysOnly => {
                let mut iter = db.raw_iterator();
                iter.seek(&bounds.start_encoded);
                while iter.valid() {
                    let Some(encoded_key) = iter.key() else {
                        break;
                    };
                    if !bounds.contains_encoded(encoded_key) {
                        break;
                    }
                    let logical_key = decode_key(&namespace, encoded_key)?;
                    if after
                        .as_deref()
                        .is_some_and(|after| logical_key.as_slice() <= after)
                        || !seen.insert(logical_key.clone())
                    {
                        iter.next();
                        continue;
                    }
                    if count < limit {
                        resume_after_candidate = Some(logical_key.clone());
                        keys.push(&logical_key);
                    }
                    count += 1;
                    if count > limit {
                        break;
                    }
                    iter.next();
                }
                iter.status().map_err(rocksdb_error)?;
            }
            BackendKvRead4Projection::Parts(_) => {
                for item in db.iterator(IteratorMode::From(
                    &bounds.start_encoded,
                    Direction::Forward,
                )) {
                    let (encoded_key, value) = item.map_err(rocksdb_error)?;
                    let encoded_key = encoded_key.as_ref();
                    if !bounds.contains_encoded(encoded_key) {
                        break;
                    }
                    let logical_key = decode_key(&namespace, encoded_key)?;
                    if after
                        .as_deref()
                        .is_some_and(|after| logical_key.as_slice() <= after)
                        || !seen.insert(logical_key.clone())
                    {
                        continue;
                    }
                    if count < limit {
                        resume_after_candidate = Some(logical_key.clone());
                        keys.push(&logical_key);
                        rocksdb_read4_push_projected(
                            &mut value_builders,
                            &projection,
                            value.as_ref(),
                        )?;
                    }
                    count += 1;
                    if count > limit {
                        break;
                    }
                }
            }
        }
        if count > limit {
            break;
        }
        if count == limit && span_index + 1 < spans.len() {
            resume_after_candidate = keys
                .len()
                .checked_sub(1)
                .and_then(|index| keys.get(index))
                .map(<[u8]>::to_vec);
            count += 1;
            break;
        }
    }
    let resume_after = (count > limit).then_some(resume_after_candidate).flatten();
    Ok(BackendKvRead4Page {
        keys: keys.finish(),
        presence: BackendKvReadV3Presence::All,
        values: value_builders
            .into_iter()
            .map(BytePageBuilder::finish)
            .collect(),
        request_indexes: None,
        resume_after,
    })
}

fn rocksdb_read4_collect_spans(
    db: &DB,
    pending: &BTreeMap<Vec<u8>, PendingWrite>,
    pending_range_deletes: &[EncodedRange],
    commit_ops: &[EncodedWriteOp],
    namespace: &str,
    mut spans: Vec<(Vec<u8>, Vec<u8>)>,
    after: Option<&[u8]>,
) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, LixError> {
    spans.retain(|(lower, upper)| upper.is_empty() || lower < upper);
    spans.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    let mut merged = BTreeMap::new();
    for (lower, upper) in spans {
        let bounds = Read4Bounds::new(namespace, &lower, &upper);
        for item in db.iterator(IteratorMode::From(
            &bounds.start_encoded,
            Direction::Forward,
        )) {
            let (encoded_key, value) = item.map_err(rocksdb_error)?;
            let encoded_key = encoded_key.as_ref();
            if !bounds.contains_encoded(encoded_key) {
                break;
            }
            if encoded_in_ranges(encoded_key, pending_range_deletes) {
                continue;
            }
            let logical_key = decode_key(namespace, encoded_key)?;
            if after.is_some_and(|after| logical_key.as_slice() <= after) {
                continue;
            }
            merged.insert(logical_key, value.to_vec());
        }
        for (encoded_key, write) in
            pending.range(bounds.start_encoded.clone()..bounds.end_encoded.clone())
        {
            if !bounds.contains_encoded(encoded_key) {
                continue;
            }
            let logical_key = decode_key(namespace, encoded_key)?;
            if after.is_some_and(|after| logical_key.as_slice() <= after) {
                continue;
            }
            match write {
                PendingWrite::Put(op_index) => {
                    let value = commit_op_value(commit_ops, *op_index)
                        .expect("pending put should point at commit put");
                    merged.insert(logical_key, value.to_vec());
                }
                PendingWrite::Delete => {
                    merged.remove(&logical_key);
                }
            }
        }
    }
    Ok(merged)
}

fn rocksdb_read4_value_builders(projection: &BackendKvRead4Projection) -> Vec<BytePageBuilder> {
    match projection {
        BackendKvRead4Projection::KeysOnly => Vec::new(),
        BackendKvRead4Projection::Parts(parts) => {
            parts.iter().map(|_| BytePageBuilder::new()).collect()
        }
    }
}

fn rocksdb_read4_push_projected(
    builders: &mut [BytePageBuilder],
    projection: &BackendKvRead4Projection,
    value: &[u8],
) -> Result<(), LixError> {
    if let BackendKvRead4Projection::Parts(parts) = projection {
        for (part, builder) in parts.iter().zip(builders.iter_mut()) {
            builder.push(project_backend_read4_value_part(value, *part)?);
        }
    }
    Ok(())
}

struct Read4Bounds {
    start_encoded: Vec<u8>,
    end_encoded: Vec<u8>,
    namespace_prefix: Vec<u8>,
}

impl Read4Bounds {
    fn new(namespace: &str, lower: &[u8], upper: &[u8]) -> Self {
        let start_encoded = encode_key(namespace, lower);
        let end_encoded = if upper.is_empty() {
            namespace_end_key(namespace)
        } else {
            encode_key(namespace, upper)
        };
        Self {
            start_encoded,
            end_encoded,
            namespace_prefix: namespace_prefix(namespace),
        }
    }

    fn contains_encoded(&self, encoded_key: &[u8]) -> bool {
        encoded_key < self.end_encoded.as_slice()
            && encoded_key.starts_with(self.namespace_prefix.as_slice())
    }
}

struct ScanBounds {
    start_encoded: Vec<u8>,
    end_encoded: Vec<u8>,
    namespace_prefix: Vec<u8>,
}

impl ScanBounds {
    fn new(request: &BackendKvScanRequest) -> Self {
        let start = scan_start_key(request);
        let start_encoded = encode_key(&request.namespace, &start);
        let end = scan_end_key(&request.range);
        let end_encoded = end
            .as_ref()
            .map(|end| encode_key(&request.namespace, end))
            .unwrap_or_else(|| namespace_end_key(&request.namespace));
        let namespace_prefix = namespace_prefix(&request.namespace);
        Self {
            start_encoded,
            end_encoded,
            namespace_prefix,
        }
    }

    fn contains_encoded(&self, encoded_key: &[u8]) -> bool {
        encoded_key < self.end_encoded.as_slice()
            && encoded_key.starts_with(self.namespace_prefix.as_slice())
    }
}

fn rocksdb_scan_committed_keys(
    db: &DB,
    request: BackendKvScanRequest,
    bounds: ScanBounds,
) -> Result<BackendKvKeyPage, LixError> {
    let mut keys = BytePageBuilder::new();
    let mut count = 0;
    let mut resume_after_candidate = None;
    let mut iter = db.raw_iterator();
    iter.seek(&bounds.start_encoded);
    while iter.valid() {
        let Some(encoded_key) = iter.key() else {
            break;
        };
        if !bounds.contains_encoded(encoded_key) {
            break;
        }
        let logical_key = decode_key(&request.namespace, encoded_key)?;
        if !key_after_cursor(&request, &logical_key) {
            iter.next();
            continue;
        }
        if count < request.limit {
            resume_after_candidate = Some(logical_key.clone());
            keys.push(&logical_key);
        }
        count += 1;
        if count > request.limit {
            break;
        }
        iter.next();
    }
    iter.status().map_err(rocksdb_error)?;
    let resume_after = (count > request.limit)
        .then_some(resume_after_candidate)
        .flatten();
    Ok(BackendKvKeyPage {
        keys: keys.finish(),
        resume_after,
    })
}

fn rocksdb_scan_committed_values(
    db: &DB,
    request: BackendKvScanRequest,
    bounds: ScanBounds,
) -> Result<BackendKvValuePage, LixError> {
    let mut values = BytePageBuilder::new();
    let mut count = 0;
    let mut resume_after_candidate = None;
    for item in db.iterator(IteratorMode::From(
        &bounds.start_encoded,
        Direction::Forward,
    )) {
        let (encoded_key, value) = item.map_err(rocksdb_error)?;
        let encoded_key = encoded_key.as_ref();
        if !bounds.contains_encoded(encoded_key) {
            break;
        }
        let logical_key = decode_key(&request.namespace, encoded_key)?;
        if !key_after_cursor(&request, &logical_key) {
            continue;
        }
        if count < request.limit {
            resume_after_candidate = Some(logical_key);
            values.push(value.as_ref());
        }
        count += 1;
        if count > request.limit {
            break;
        }
    }
    let resume_after = (count > request.limit)
        .then_some(resume_after_candidate)
        .flatten();
    Ok(BackendKvValuePage {
        values: values.finish(),
        resume_after,
    })
}

fn rocksdb_scan_committed_entries(
    db: &DB,
    request: BackendKvScanRequest,
    bounds: ScanBounds,
) -> Result<BackendKvEntryPage, LixError> {
    let mut keys = BytePageBuilder::new();
    let mut values = BytePageBuilder::new();
    let mut count = 0;
    let mut resume_after_candidate = None;
    for item in db.iterator(IteratorMode::From(
        &bounds.start_encoded,
        Direction::Forward,
    )) {
        let (key, value) = item.map_err(rocksdb_error)?;
        let key = key.as_ref();
        if !bounds.contains_encoded(key) {
            break;
        }
        let logical_key = decode_key(&request.namespace, key)?;
        if !key_after_cursor(&request, &logical_key) {
            continue;
        }
        if count < request.limit {
            resume_after_candidate = Some(logical_key.clone());
            keys.push(&logical_key);
            values.push(value.as_ref());
        }
        count += 1;
        if count > request.limit {
            break;
        }
    }
    let resume_after = (count > request.limit)
        .then_some(resume_after_candidate)
        .flatten();
    Ok(BackendKvEntryPage {
        keys: keys.finish(),
        values: values.finish(),
        resume_after,
    })
}

fn overlay_pending_values(
    merged: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    pending: &BTreeMap<Vec<u8>, PendingWrite>,
    commit_ops: &[EncodedWriteOp],
    request: &BackendKvScanRequest,
    bounds: &ScanBounds,
) -> Result<(), LixError> {
    for (encoded_key, write) in
        pending.range(bounds.start_encoded.clone()..bounds.end_encoded.clone())
    {
        if !bounds.contains_encoded(encoded_key) {
            continue;
        }
        let logical_key = decode_key(&request.namespace, encoded_key)?;
        if !key_in_range(&logical_key, &request.range) || !key_after_cursor(request, &logical_key) {
            continue;
        }
        match write {
            PendingWrite::Put(op_index) => {
                let value = commit_op_value(commit_ops, *op_index)
                    .expect("pending put should point at commit put");
                merged.insert(logical_key, value.to_vec());
            }
            PendingWrite::Delete => {
                merged.remove(&logical_key);
            }
        }
    }
    Ok(())
}

fn key_page_from_iter(
    keys_iter: impl IntoIterator<Item = Vec<u8>>,
    limit: usize,
) -> BackendKvKeyPage {
    let mut keys = BytePageBuilder::new();
    let mut count = 0;
    let mut resume_after_candidate = None;
    for key in keys_iter {
        if count < limit {
            resume_after_candidate = Some(key.clone());
            keys.push(&key);
        }
        count += 1;
        if count > limit {
            break;
        }
    }
    let resume_after = (count > limit).then_some(resume_after_candidate).flatten();
    BackendKvKeyPage {
        keys: keys.finish(),
        resume_after,
    }
}

fn value_page_from_iter(
    values_iter: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
    limit: usize,
) -> BackendKvValuePage {
    let mut values = BytePageBuilder::new();
    let mut count = 0;
    let mut resume_after_candidate = None;
    for (key, value) in values_iter {
        if count < limit {
            resume_after_candidate = Some(key);
            values.push(&value);
        }
        count += 1;
        if count > limit {
            break;
        }
    }
    let resume_after = (count > limit).then_some(resume_after_candidate).flatten();
    BackendKvValuePage {
        values: values.finish(),
        resume_after,
    }
}

fn entry_page_from_iter(
    entries_iter: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
    limit: usize,
) -> BackendKvEntryPage {
    let mut keys = BytePageBuilder::new();
    let mut values = BytePageBuilder::new();
    let mut count = 0;
    let mut resume_after_candidate = None;
    for (key, value) in entries_iter {
        if count < limit {
            resume_after_candidate = Some(key.clone());
            keys.push(&key);
            values.push(&value);
        }
        count += 1;
        if count > limit {
            break;
        }
    }
    let resume_after = (count > limit).then_some(resume_after_candidate).flatten();
    BackendKvEntryPage {
        keys: keys.finish(),
        values: values.finish(),
        resume_after,
    }
}

fn scan_start_key(request: &BackendKvScanRequest) -> Vec<u8> {
    let range_start = match &request.range {
        BackendKvScanRange::Prefix(prefix) => prefix.as_slice(),
        BackendKvScanRange::Range { start, .. } => start.as_slice(),
    };
    match request.after.as_deref() {
        Some(after) if after > range_start => after.to_vec(),
        _ => range_start.to_vec(),
    }
}

fn scan_end_key(range: &BackendKvScanRange) -> Option<Vec<u8>> {
    match range {
        BackendKvScanRange::Prefix(prefix) => prefix_end(prefix),
        BackendKvScanRange::Range { end, .. } => Some(end.clone()),
    }
}

fn key_in_range(key: &[u8], range: &BackendKvScanRange) -> bool {
    match range {
        BackendKvScanRange::Prefix(prefix) => key.starts_with(prefix),
        BackendKvScanRange::Range { start, end } => key >= start.as_slice() && key < end.as_slice(),
    }
}

fn key_after_cursor(request: &BackendKvScanRequest, key: &[u8]) -> bool {
    request.after.as_deref().is_none_or(|after| key > after)
}

fn encode_key(namespace: &str, key: &[u8]) -> Vec<u8> {
    let namespace = namespace.as_bytes();
    let len = u32::try_from(namespace.len()).expect("bench namespace fits u32");
    let mut encoded = Vec::with_capacity(4 + namespace.len() + key.len());
    encoded.extend_from_slice(&len.to_be_bytes());
    encoded.extend_from_slice(namespace);
    encoded.extend_from_slice(key);
    encoded
}

fn namespace_prefix(namespace: &str) -> Vec<u8> {
    encode_key(namespace, &[])
}

fn namespace_end_key(namespace: &str) -> Vec<u8> {
    prefix_end(&namespace_prefix(namespace)).expect("encoded namespace prefix has an upper bound")
}

fn encoded_range(namespace: &str, range: &BackendKvScanRange) -> EncodedRange {
    let start = match range {
        BackendKvScanRange::Prefix(prefix) => prefix.as_slice(),
        BackendKvScanRange::Range { start, .. } => start.as_slice(),
    };
    let end = scan_end_key(range)
        .as_ref()
        .map(|end| encode_key(namespace, end))
        .unwrap_or_else(|| namespace_end_key(namespace));
    EncodedRange {
        start: encode_key(namespace, start),
        end,
    }
}

fn encoded_in_ranges(key: &[u8], ranges: &[EncodedRange]) -> bool {
    ranges.iter().any(|range| range.contains(key))
}

fn delete_range_bytes(range: &BackendKvScanRange) -> usize {
    match range {
        BackendKvScanRange::Prefix(prefix) => prefix.len(),
        BackendKvScanRange::Range { start, end } => start.len() + end.len(),
    }
}

fn decode_key(namespace: &str, encoded: &[u8]) -> Result<Vec<u8>, LixError> {
    let prefix = namespace_prefix(namespace);
    encoded
        .strip_prefix(prefix.as_slice())
        .map(|key| key.to_vec())
        .ok_or_else(|| LixError::new("LIX_ERROR_UNKNOWN", "rocksdb bench key prefix mismatch"))
}

fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] != u8::MAX {
            end[index] += 1;
            end.truncate(index + 1);
            return Some(end);
        }
    }
    None
}

fn rocksdb_error(error: rocksdb::Error) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("rocksdb bench backend: {error}"),
    )
}

fn io_error(error: std::io::Error) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("rocksdb bench backend: {error}"),
    )
}
