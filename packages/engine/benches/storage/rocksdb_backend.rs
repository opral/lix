use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use lix_engine::{
    Backend, BackendKvGetRequest, BackendKvGetResult, BackendKvGetResultGroup, BackendKvPair,
    BackendKvScanRange, BackendKvScanRequest, BackendKvScanResult, BackendKvWriteBatch,
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
    async fn get_kv_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvGetResult, LixError> {
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let mut values = Vec::with_capacity(group.keys.len());
            let mut committed_positions = Vec::new();
            let mut committed_keys = Vec::new();
            for key in group.keys {
                let encoded_key = encode_key(&group.namespace, &key);
                match self.pending.get(&encoded_key) {
                    Some(PendingWrite::Put(value)) => values.push(Some(value.clone())),
                    Some(PendingWrite::Delete) => values.push(None),
                    None => {
                        committed_positions.push(values.len());
                        committed_keys.push(encoded_key);
                        values.push(None);
                    }
                }
            }
            let committed_values = self.inner.db.multi_get(committed_keys);
            for (position, value) in committed_positions.into_iter().zip(committed_values) {
                values[position] = value.map_err(rocksdb_error)?;
            }
            groups.push(BackendKvGetResultGroup {
                namespace: group.namespace,
                values,
            });
        }
        Ok(BackendKvGetResult { groups })
    }

    async fn scan_kv(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvScanResult, LixError> {
        rocksdb_scan(&self.inner.db, &self.pending, request)
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

fn rocksdb_scan(
    db: &DB,
    pending: &BTreeMap<Vec<u8>, PendingWrite>,
    request: BackendKvScanRequest,
) -> Result<BackendKvScanResult, LixError> {
    let start = scan_start_key(&request);
    let start_encoded = encode_key(&request.namespace, &start);
    let end = scan_end_key(&request.range);
    let end_encoded = end
        .as_ref()
        .map(|end| encode_key(&request.namespace, end))
        .unwrap_or_else(|| namespace_end_key(&request.namespace));
    let namespace_prefix = namespace_prefix(&request.namespace);
    if pending.is_empty() {
        return rocksdb_scan_committed(db, request, start_encoded, end_encoded, namespace_prefix);
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
    let mut rows = Vec::new();
    for (key, value) in merged {
        if rows.len() > request.limit {
            break;
        }
        rows.push(BackendKvPair::new(key, value));
    }
    let has_more = rows.len() > request.limit;
    rows.truncate(request.limit);
    let resume_after = has_more
        .then(|| rows.last().map(|row| row.key.clone()))
        .flatten();
    Ok(BackendKvScanResult { rows, resume_after })
}

fn rocksdb_scan_committed(
    db: &DB,
    request: BackendKvScanRequest,
    start_encoded: Vec<u8>,
    end_encoded: Vec<u8>,
    namespace_prefix: Vec<u8>,
) -> Result<BackendKvScanResult, LixError> {
    let mut rows = Vec::new();
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
        rows.push(BackendKvPair::new(logical_key, value.to_vec()));
        if rows.len() > request.limit {
            break;
        }
    }
    let has_more = rows.len() > request.limit;
    rows.truncate(request.limit);
    let resume_after = has_more
        .then(|| rows.last().map(|row| row.key.clone()))
        .flatten();
    Ok(BackendKvScanResult { rows, resume_after })
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
