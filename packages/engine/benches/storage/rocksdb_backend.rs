use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use lix_engine::{
    Backend, BackendKvEntry, BackendKvEntryPage, BackendKvExistsBatch, BackendKvExistsGroup,
    BackendKvGetRequest, BackendKvKeyPage, BackendKvScanRange, BackendKvScanRequest,
    BackendKvValueBatch, BackendKvValueGroup, BackendKvValuePage, BackendKvWriteBatch,
    BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction, LixError,
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
}

enum PendingWrite {
    Put(Vec<u8>),
    Delete,
}

impl RocksDbBenchBackend {
    pub(crate) fn new() -> Result<Self, LixError> {
        let dir = TempDir::new().map_err(io_error)?;
        let db = open_rocksdb(dir.path())?;
        Ok(Self {
            inner: Arc::new(RocksDbBenchInner { db, _dir: dir }),
        })
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
        }))
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        Ok(Box::new(RocksDbBenchTransaction {
            inner: Arc::clone(&self.inner),
            pending: BTreeMap::new(),
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
            let mut values = vec![None; group.keys.len()];
            let mut committed_keys = Vec::new();
            let mut committed_positions = Vec::new();
            for (position, key) in group.keys.into_iter().enumerate() {
                let encoded_key = encode_key(&group.namespace, &key);
                match self.pending.get(&encoded_key) {
                    Some(PendingWrite::Put(value)) => values[position] = Some(value.clone()),
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
                    Some(value) => values[position] = Some(value),
                    None => {}
                }
            }
            groups.push(BackendKvValueGroup {
                namespace: group.namespace,
                values,
            });
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
        let entries = rocksdb_scan_entries(&self.inner.db, &self.pending, request)?;
        Ok(BackendKvKeyPage {
            keys: entries.entries.into_iter().map(|entry| entry.key).collect(),
            resume_after: entries.resume_after,
        })
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        let entries = rocksdb_scan_entries(&self.inner.db, &self.pending, request)?;
        Ok(BackendKvValuePage {
            values: entries
                .entries
                .into_iter()
                .map(|entry| entry.value)
                .collect(),
            resume_after: entries.resume_after,
        })
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
impl BackendWriteTransaction for RocksDbBenchTransaction {
    async fn write_kv_batch(
        &mut self,
        batch: BackendKvWriteBatch,
    ) -> Result<BackendKvWriteStats, LixError> {
        let mut stats = BackendKvWriteStats::default();
        for group in batch.groups {
            for put in group.puts {
                stats.puts += 1;
                stats.bytes_written += put.key.len() + put.value.len();
                self.pending.insert(
                    encode_key(&group.namespace, &put.key),
                    PendingWrite::Put(put.value),
                );
            }
            for key in group.deletes {
                stats.deletes += 1;
                stats.bytes_written += key.len();
                self.pending
                    .insert(encode_key(&group.namespace, &key), PendingWrite::Delete);
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
        let mut exists = vec![false; group.keys.len()];
        let mut committed = Vec::new();

        for (position, key) in group.keys.into_iter().enumerate() {
            let encoded_key = encode_key(&group.namespace, &key);
            match pending.get(&encoded_key) {
                Some(PendingWrite::Put(_)) => exists[position] = true,
                Some(PendingWrite::Delete) => {}
                None => {
                    committed.push((encoded_key, position));
                }
            }
        }

        fill_committed_exists(db, &mut exists, committed)?;
        groups.push(BackendKvExistsGroup {
            namespace: group.namespace,
            exists,
        });
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

fn rocksdb_scan_entries(
    db: &DB,
    pending: &BTreeMap<Vec<u8>, PendingWrite>,
    request: BackendKvScanRequest,
) -> Result<BackendKvEntryPage, LixError> {
    let start = scan_start_key(&request);
    let start_encoded = encode_key(&request.namespace, &start);
    let end = scan_end_key(&request.range);
    let end_encoded = end
        .as_ref()
        .map(|end| encode_key(&request.namespace, end))
        .unwrap_or_else(|| namespace_end_key(&request.namespace));
    let namespace_prefix = namespace_prefix(&request.namespace);
    if pending.is_empty() {
        return rocksdb_scan_committed_entries(
            db,
            request,
            start_encoded,
            end_encoded,
            namespace_prefix,
        );
    }
    let mut merged = BTreeMap::new();
    for item in db.iterator(IteratorMode::From(&start_encoded, Direction::Forward)) {
        let (key, value) = item.map_err(rocksdb_error)?;
        let key = key.as_ref();
        if key >= end_encoded.as_slice() || !key.starts_with(&namespace_prefix) {
            break;
        }
        let logical_key = decode_key(&request.namespace, key)?;
        if let Some(after) = request.after.as_deref() {
            if logical_key.as_slice() <= after {
                continue;
            }
        }
        merged.insert(logical_key, value.to_vec());
    }
    for (encoded_key, write) in pending.range(start_encoded..end_encoded) {
        if !encoded_key.starts_with(&namespace_prefix) {
            continue;
        }
        let logical_key = decode_key(&request.namespace, encoded_key)?;
        if !key_in_range(&logical_key, &request.range) {
            continue;
        }
        if let Some(after) = request.after.as_deref() {
            if logical_key.as_slice() <= after {
                continue;
            }
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
    let mut entries = Vec::new();
    for (key, value) in merged {
        if entries.len() > request.limit {
            break;
        }
        entries.push(BackendKvEntry { key, value });
    }
    let has_more = entries.len() > request.limit;
    entries.truncate(request.limit);
    let resume_after = has_more
        .then(|| entries.last().map(|entry| entry.key.clone()))
        .flatten();
    Ok(BackendKvEntryPage {
        entries,
        resume_after,
    })
}

fn rocksdb_scan_committed_entries(
    db: &DB,
    request: BackendKvScanRequest,
    start_encoded: Vec<u8>,
    end_encoded: Vec<u8>,
    namespace_prefix: Vec<u8>,
) -> Result<BackendKvEntryPage, LixError> {
    let mut entries = Vec::new();
    for item in db.iterator(IteratorMode::From(&start_encoded, Direction::Forward)) {
        let (key, value) = item.map_err(rocksdb_error)?;
        let key = key.as_ref();
        if key >= end_encoded.as_slice() || !key.starts_with(&namespace_prefix) {
            break;
        }
        let logical_key = decode_key(&request.namespace, key)?;
        if let Some(after) = request.after.as_deref() {
            if logical_key.as_slice() <= after {
                continue;
            }
        }
        entries.push(BackendKvEntry {
            key: logical_key,
            value: value.to_vec(),
        });
        if entries.len() > request.limit {
            break;
        }
    }
    let has_more = entries.len() > request.limit;
    entries.truncate(request.limit);
    let resume_after = has_more
        .then(|| entries.last().map(|entry| entry.key.clone()))
        .flatten();
    Ok(BackendKvEntryPage {
        entries,
        resume_after,
    })
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
