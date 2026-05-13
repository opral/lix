use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use lix_engine::{
    Backend, BackendKvEntryPage, BackendKvExistsBatch, BackendKvExistsGroup, BackendKvGetRequest,
    BackendKvKeyPage, BackendKvScanRange, BackendKvScanRequest, BackendKvValueBatch,
    BackendKvValueGroup, BackendKvValuePage, BackendKvWriteBatch, BackendKvWriteStats,
    BackendReadTransaction, BackendWriteTransaction, BytePageBuilder, LixError,
};
use rocksdb::{Direction, IteratorMode, Options, WriteBatch, DB};
use tempfile::TempDir;

#[derive(Clone)]
pub(crate) struct RocksDbChangelogBenchBackend {
    inner: Arc<RocksDbChangelogBenchInner>,
}

struct RocksDbChangelogBenchInner {
    db: DB,
    _dir: TempDir,
}

pub(crate) struct RocksDbChangelogBenchTransaction {
    inner: Arc<RocksDbChangelogBenchInner>,
    pending: BTreeMap<Vec<u8>, PendingWrite>,
}

enum PendingWrite {
    Put(Vec<u8>),
    Delete,
}

impl RocksDbChangelogBenchBackend {
    pub(crate) fn new() -> Result<Self, LixError> {
        let dir = TempDir::new().map_err(io_error)?;
        let db = open_rocksdb(dir.path())?;
        Ok(Self {
            inner: Arc::new(RocksDbChangelogBenchInner { db, _dir: dir }),
        })
    }

    #[allow(dead_code)]
    pub(crate) fn path(&self) -> &Path {
        self.inner._dir.path()
    }
}

#[async_trait]
impl Backend for RocksDbChangelogBenchBackend {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        Ok(Box::new(RocksDbChangelogBenchTransaction {
            inner: Arc::clone(&self.inner),
            pending: BTreeMap::new(),
        }))
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        Ok(Box::new(RocksDbChangelogBenchTransaction {
            inner: Arc::clone(&self.inner),
            pending: BTreeMap::new(),
        }))
    }
}

#[async_trait]
impl BackendReadTransaction for RocksDbChangelogBenchTransaction {
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
                    Some(PendingWrite::Put(value)) => {
                        resolved_values[position] = Some(value.clone())
                    }
                    Some(PendingWrite::Delete) => {}
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
        rocksdb_get_exists_many(&self.inner.db, &self.pending, request)
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        rocksdb_scan_keys(&self.inner.db, &self.pending, request)
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        rocksdb_scan_values(&self.inner.db, &self.pending, request)
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        rocksdb_scan_entries(&self.inner.db, &self.pending, request)
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        Ok(())
    }
}

#[async_trait]
impl BackendWriteTransaction for RocksDbChangelogBenchTransaction {
    async fn write_kv_batch(
        &mut self,
        batch: BackendKvWriteBatch,
    ) -> Result<BackendKvWriteStats, LixError> {
        let mut stats = BackendKvWriteStats::default();
        for group in batch.groups {
            let namespace = group.namespace().to_string();
            for index in 0..group.put_count() {
                let key = group.put_key(index).ok_or_else(|| {
                    LixError::new("LIX_ERROR_UNKNOWN", "backend write batch missing put key")
                })?;
                let value = group.put_value(index).ok_or_else(|| {
                    LixError::new("LIX_ERROR_UNKNOWN", "backend write batch missing put value")
                })?;
                stats.puts += 1;
                stats.bytes_written += key.len() + value.len();
                self.pending.insert(
                    encode_key(namespace.as_str(), key),
                    PendingWrite::Put(value.to_vec()),
                );
            }
            for index in 0..group.delete_count() {
                let key = group.delete_key(index).ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "backend write batch missing delete key",
                    )
                })?;
                stats.deletes += 1;
                stats.bytes_written += key.len();
                self.pending
                    .insert(encode_key(namespace.as_str(), key), PendingWrite::Delete);
            }
        }
        Ok(stats)
    }

    async fn commit(self: Box<Self>) -> Result<(), LixError> {
        let mut write_batch = WriteBatch::default();
        for (key, write) in self.pending {
            match write {
                PendingWrite::Put(value) => write_batch.put(key, value),
                PendingWrite::Delete => write_batch.delete(key),
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
    request: BackendKvScanRequest,
) -> Result<BackendKvKeyPage, LixError> {
    let bounds = ScanBounds::new(&request);
    if pending.is_empty() {
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
    request: BackendKvScanRequest,
) -> Result<BackendKvValuePage, LixError> {
    let bounds = ScanBounds::new(&request);
    if pending.is_empty() {
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
        let logical_key = decode_key(&request.namespace, encoded_key)?;
        if !key_after_cursor(&request, &logical_key) {
            continue;
        }
        merged.insert(logical_key, value.to_vec());
    }
    overlay_pending_values(&mut merged, pending, &request, &bounds)?;
    Ok(value_page_from_iter(merged, request.limit))
}

fn rocksdb_scan_entries(
    db: &DB,
    pending: &BTreeMap<Vec<u8>, PendingWrite>,
    request: BackendKvScanRequest,
) -> Result<BackendKvEntryPage, LixError> {
    let bounds = ScanBounds::new(&request);
    if pending.is_empty() {
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
        let logical_key = decode_key(&request.namespace, key)?;
        if !key_after_cursor(&request, &logical_key) {
            continue;
        }
        merged.insert(logical_key, value.to_vec());
    }
    overlay_pending_values(&mut merged, pending, &request, &bounds)?;
    Ok(entry_page_from_iter(merged, request.limit))
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
            PendingWrite::Put(value) => {
                merged.insert(logical_key, value.clone());
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
    let mut end = namespace_prefix(namespace);
    end.push(0xFF);
    end
}

fn decode_key(namespace: &str, encoded: &[u8]) -> Result<Vec<u8>, LixError> {
    let prefix = namespace_prefix(namespace);
    encoded
        .strip_prefix(prefix.as_slice())
        .map(|key| key.to_vec())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "rocksdb changelog bench key prefix mismatch",
            )
        })
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
        format!("rocksdb changelog bench backend: {error}"),
    )
}

fn io_error(error: std::io::Error) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("rocksdb changelog bench backend: {error}"),
    )
}
