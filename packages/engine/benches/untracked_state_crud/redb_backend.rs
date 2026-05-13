use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use lix_engine::{
    Backend, BackendKvEntryPage, BackendKvExistsBatch, BackendKvExistsGroup, BackendKvGetRequest,
    BackendKvKeyPage, BackendKvScanRange, BackendKvScanRequest, BackendKvValueBatch,
    BackendKvValueGroup, BackendKvValuePage, BackendKvWriteBatch, BackendKvWriteOp,
    BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction, BytePageBuilder,
    LixError,
};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use tempfile::TempDir;

const KV_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");

#[derive(Clone)]
pub(crate) struct RedbBenchBackend {
    inner: Arc<RedbBenchInner>,
}

struct RedbBenchInner {
    db: Database,
    _dir: TempDir,
}

pub(crate) struct RedbBenchTransaction {
    inner: Arc<RedbBenchInner>,
    commit_ops: Vec<EncodedWriteOp>,
}

enum EncodedWriteOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    DeleteRange { range: EncodedRange },
}

#[derive(Clone)]
struct EncodedRange {
    start: Vec<u8>,
    end: Vec<u8>,
}

impl RedbBenchBackend {
    pub(crate) fn new() -> Result<Self, LixError> {
        let dir = TempDir::new().map_err(io_error)?;
        let path = dir.path().join("bench.redb");
        let db = Database::create(path).map_err(redb_error)?;
        {
            let mut tx = db.begin_write().map_err(redb_error)?;
            tx.set_durability(Durability::None).map_err(redb_error)?;
            tx.open_table(KV_TABLE).map_err(redb_error)?;
            tx.commit().map_err(redb_error)?;
        }
        Ok(Self {
            inner: Arc::new(RedbBenchInner { db, _dir: dir }),
        })
    }

    #[allow(dead_code)]
    pub(crate) fn path(&self) -> &Path {
        self.inner._dir.path()
    }
}

#[async_trait]
impl Backend for RedbBenchBackend {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        Ok(Box::new(RedbBenchTransaction {
            inner: Arc::clone(&self.inner),
            commit_ops: Vec::new(),
        }))
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        Ok(Box::new(RedbBenchTransaction {
            inner: Arc::clone(&self.inner),
            commit_ops: Vec::new(),
        }))
    }
}

#[async_trait]
impl BackendReadTransaction for RedbBenchTransaction {
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        let tx = self.inner.db.begin_read().map_err(redb_error)?;
        let table = tx.open_table(KV_TABLE).map_err(redb_error)?;
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let namespace = group.namespace.clone();
            let mut values = BytePageBuilder::with_capacity(group.keys.len(), 0);
            let mut present = Vec::with_capacity(group.keys.len());
            for key in group.keys {
                let encoded_key = encode_key(&namespace, &key);
                if let Some(value) = table.get(encoded_key.as_slice()).map_err(redb_error)? {
                    values.push(value.value());
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
        let tx = self.inner.db.begin_read().map_err(redb_error)?;
        let table = tx.open_table(KV_TABLE).map_err(redb_error)?;
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let namespace = group.namespace.clone();
            let mut exists = Vec::with_capacity(group.keys.len());
            for key in group.keys {
                let encoded_key = encode_key(&namespace, &key);
                exists.push(
                    table
                        .get(encoded_key.as_slice())
                        .map_err(redb_error)?
                        .is_some(),
                );
            }
            groups.push(BackendKvExistsGroup { namespace, exists });
        }
        Ok(BackendKvExistsBatch { groups })
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        let tx = self.inner.db.begin_read().map_err(redb_error)?;
        let table = tx.open_table(KV_TABLE).map_err(redb_error)?;
        redb_scan_keys(&table, request)
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        let tx = self.inner.db.begin_read().map_err(redb_error)?;
        let table = tx.open_table(KV_TABLE).map_err(redb_error)?;
        redb_scan_values(&table, request)
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        let tx = self.inner.db.begin_read().map_err(redb_error)?;
        let table = tx.open_table(KV_TABLE).map_err(redb_error)?;
        redb_scan_entries(&table, request)
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        Ok(())
    }
}

#[async_trait]
impl BackendWriteTransaction for RedbBenchTransaction {
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
                        self.commit_ops.push(EncodedWriteOp::Put {
                            key: encode_key(&namespace, &key),
                            value,
                        });
                    }
                    BackendKvWriteOp::Delete { key } => {
                        stats.deletes += 1;
                        stats.bytes_written += key.len();
                        self.commit_ops.push(EncodedWriteOp::Delete {
                            key: encode_key(&namespace, &key),
                        });
                    }
                    BackendKvWriteOp::DeleteRange { range } => {
                        stats.delete_ranges += 1;
                        stats.bytes_written += delete_range_bytes(&range);
                        self.commit_ops.push(EncodedWriteOp::DeleteRange {
                            range: encoded_range(&namespace, &range),
                        });
                    }
                }
            }
        }
        Ok(stats)
    }

    async fn commit(self: Box<Self>) -> Result<(), LixError> {
        let mut tx = self.inner.db.begin_write().map_err(redb_error)?;
        tx.set_durability(Durability::None).map_err(redb_error)?;
        {
            let mut table = tx.open_table(KV_TABLE).map_err(redb_error)?;
            for op in self.commit_ops {
                match op {
                    EncodedWriteOp::Put { key, value } => {
                        table
                            .insert(key.as_slice(), value.as_slice())
                            .map_err(redb_error)?;
                    }
                    EncodedWriteOp::Delete { key } => {
                        table.remove(key.as_slice()).map_err(redb_error)?;
                    }
                    EncodedWriteOp::DeleteRange { range } => {
                        let keys = table
                            .range(range.start.as_slice()..range.end.as_slice())
                            .map_err(redb_error)?
                            .map(|item| {
                                item.map(|(key, _value)| key.value().to_vec())
                                    .map_err(redb_error)
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        for key in keys {
                            table.remove(key.as_slice()).map_err(redb_error)?;
                        }
                    }
                }
            }
        }
        tx.commit().map_err(redb_error)?;
        Ok(())
    }
}

fn redb_scan_keys(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    request: BackendKvScanRequest,
) -> Result<BackendKvKeyPage, LixError> {
    let bounds = ScanBounds::new(&request);
    let mut keys = BytePageBuilder::new();
    let mut count = 0;
    let mut resume_after_candidate = None;
    for item in table
        .range(bounds.start_encoded.as_slice()..bounds.end_encoded.as_slice())
        .map_err(redb_error)?
    {
        let (encoded_key, _value) = item.map_err(redb_error)?;
        let logical_key = decode_key(&request.namespace, encoded_key.value())?;
        if !key_after_cursor(&request, &logical_key) {
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
    }
    let resume_after = (count > request.limit)
        .then_some(resume_after_candidate)
        .flatten();
    Ok(BackendKvKeyPage {
        keys: keys.finish(),
        resume_after,
    })
}

fn redb_scan_values(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    request: BackendKvScanRequest,
) -> Result<BackendKvValuePage, LixError> {
    let bounds = ScanBounds::new(&request);
    let mut values = BytePageBuilder::new();
    let mut count = 0;
    let mut resume_after_candidate = None;
    for item in table
        .range(bounds.start_encoded.as_slice()..bounds.end_encoded.as_slice())
        .map_err(redb_error)?
    {
        let (encoded_key, value) = item.map_err(redb_error)?;
        let logical_key = decode_key(&request.namespace, encoded_key.value())?;
        if !key_after_cursor(&request, &logical_key) {
            continue;
        }
        if count < request.limit {
            resume_after_candidate = Some(logical_key);
            values.push(value.value());
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

fn redb_scan_entries(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    request: BackendKvScanRequest,
) -> Result<BackendKvEntryPage, LixError> {
    let bounds = ScanBounds::new(&request);
    let mut keys = BytePageBuilder::new();
    let mut values = BytePageBuilder::new();
    let mut count = 0;
    let mut resume_after_candidate = None;
    for item in table
        .range(bounds.start_encoded.as_slice()..bounds.end_encoded.as_slice())
        .map_err(redb_error)?
    {
        let (encoded_key, value) = item.map_err(redb_error)?;
        let logical_key = decode_key(&request.namespace, encoded_key.value())?;
        if !key_after_cursor(&request, &logical_key) {
            continue;
        }
        if count < request.limit {
            resume_after_candidate = Some(logical_key.clone());
            keys.push(&logical_key);
            values.push(value.value());
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

struct ScanBounds {
    start_encoded: Vec<u8>,
    end_encoded: Vec<u8>,
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
        Self {
            start_encoded,
            end_encoded,
        }
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
        .ok_or_else(|| LixError::new("LIX_ERROR_UNKNOWN", "redb bench key prefix mismatch"))
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

fn redb_error(error: impl std::fmt::Display) -> LixError {
    LixError::new("LIX_ERROR_UNKNOWN", format!("redb bench backend: {error}"))
}

fn io_error(error: std::io::Error) -> LixError {
    LixError::new("LIX_ERROR_UNKNOWN", format!("redb bench backend: {error}"))
}
