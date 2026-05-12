use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_engine::{
    Backend, BackendKvEntryPage, BackendKvExistsBatch, BackendKvExistsGroup, BackendKvGetRequest,
    BackendKvKeyPage, BackendKvScanRange, BackendKvScanRequest, BackendKvValueBatch,
    BackendKvValueGroup, BackendKvValuePage, BackendKvWriteBatch, BackendKvWriteOp,
    BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction, BytePageBuilder,
    LixError,
};

type KvKey = (String, Vec<u8>);
type KvMap = BTreeMap<KvKey, Vec<u8>>;

#[derive(Debug, Clone, Default)]
pub(crate) struct InMemoryBackend {
    kv: Arc<Mutex<KvMap>>,
}

impl InMemoryBackend {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Backend for InMemoryBackend {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        let snapshot = self
            .kv
            .lock()
            .map_err(|_| lock_error("rs-sdk in-memory backend kv"))?
            .clone();
        Ok(Box::new(InMemoryReadTransaction { kv: snapshot }))
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        let snapshot = self
            .kv
            .lock()
            .map_err(|_| lock_error("rs-sdk in-memory backend kv"))?
            .clone();
        Ok(Box::new(InMemoryWriteTransaction {
            parent: Arc::clone(&self.kv),
            kv: snapshot,
        }))
    }
}

struct InMemoryReadTransaction {
    kv: KvMap,
}

#[async_trait]
impl BackendReadTransaction for InMemoryReadTransaction {
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        Ok(get_values_from_map(&self.kv, request))
    }

    async fn exists_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvExistsBatch, LixError> {
        Ok(exists_many_from_map(&self.kv, request))
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        Ok(scan_map_keys(&self.kv, request))
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        Ok(scan_map_values(&self.kv, request))
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        Ok(scan_map_entries(&self.kv, request))
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        Ok(())
    }
}

struct InMemoryWriteTransaction {
    parent: Arc<Mutex<KvMap>>,
    kv: KvMap,
}

#[async_trait]
impl BackendReadTransaction for InMemoryWriteTransaction {
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        Ok(get_values_from_map(&self.kv, request))
    }

    async fn exists_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvExistsBatch, LixError> {
        Ok(exists_many_from_map(&self.kv, request))
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        Ok(scan_map_keys(&self.kv, request))
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        Ok(scan_map_values(&self.kv, request))
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        Ok(scan_map_entries(&self.kv, request))
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        Ok(())
    }
}

#[async_trait]
impl BackendWriteTransaction for InMemoryWriteTransaction {
    async fn write_kv_batch(
        &mut self,
        batch: BackendKvWriteBatch,
    ) -> Result<BackendKvWriteStats, LixError> {
        let mut stats = BackendKvWriteStats::default();
        for group in batch.groups {
            let namespace = group.namespace().to_string();
            for op in group.ops() {
                match op {
                    BackendKvWriteOp::Put { key, value } => {
                        stats.puts += 1;
                        stats.bytes_written += key.len() + value.len();
                        self.kv
                            .insert((namespace.clone(), key.clone()), value.clone());
                    }
                    BackendKvWriteOp::Delete { key } => {
                        stats.deletes += 1;
                        stats.bytes_written += key.len();
                        self.kv.remove(&(namespace.clone(), key.clone()));
                    }
                    BackendKvWriteOp::DeleteRange { range } => {
                        stats.delete_ranges += 1;
                        stats.bytes_written += delete_range_bytes(range);
                        self.kv.retain(|(candidate_namespace, key), _| {
                            candidate_namespace != &namespace || !key_matches_range(key, range)
                        });
                    }
                }
            }
        }
        Ok(stats)
    }

    async fn commit(self: Box<Self>) -> Result<(), LixError> {
        *self
            .parent
            .lock()
            .map_err(|_| lock_error("rs-sdk in-memory backend kv"))? = self.kv;
        Ok(())
    }
}

fn get_values_from_map(kv: &KvMap, request: BackendKvGetRequest) -> BackendKvValueBatch {
    let mut groups = Vec::with_capacity(request.groups.len());
    for group in request.groups {
        let namespace = group.namespace.clone();
        let mut values = BytePageBuilder::with_capacity(group.keys.len(), 0);
        let mut present = Vec::with_capacity(group.keys.len());
        for key in group.keys {
            if let Some(value) = kv.get(&(namespace.clone(), key)) {
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
    BackendKvValueBatch { groups }
}

fn exists_many_from_map(kv: &KvMap, request: BackendKvGetRequest) -> BackendKvExistsBatch {
    let mut groups = Vec::with_capacity(request.groups.len());
    for group in request.groups {
        let namespace = group.namespace.clone();
        let exists = group
            .keys
            .into_iter()
            .map(|key| kv.contains_key(&(namespace.clone(), key)))
            .collect();
        groups.push(BackendKvExistsGroup { namespace, exists });
    }
    BackendKvExistsBatch { groups }
}

fn scan_map_keys(kv: &KvMap, request: BackendKvScanRequest) -> BackendKvKeyPage {
    let pairs = scan_filtered_pairs(kv, &request);
    let has_more = pairs.len() > request.limit;
    let mut keys = BytePageBuilder::with_capacity(request.limit.min(pairs.len()), 0);
    let mut resume_after = None;
    for (index, (key, _)) in pairs.into_iter().enumerate() {
        if index >= request.limit {
            break;
        }
        resume_after = Some(key.clone());
        keys.push(key);
    }
    let resume_after = has_more.then_some(resume_after).flatten();
    BackendKvKeyPage {
        keys: keys.finish(),
        resume_after,
    }
}

fn scan_map_values(kv: &KvMap, request: BackendKvScanRequest) -> BackendKvValuePage {
    let pairs = scan_filtered_pairs(kv, &request);
    let has_more = pairs.len() > request.limit;
    let mut values = BytePageBuilder::with_capacity(request.limit.min(pairs.len()), 0);
    let mut resume_after = None;
    for (index, (key, value)) in pairs.into_iter().enumerate() {
        if index >= request.limit {
            break;
        }
        resume_after = Some(key.clone());
        values.push(value);
    }
    let resume_after = has_more.then_some(resume_after).flatten();
    BackendKvValuePage {
        values: values.finish(),
        resume_after,
    }
}

fn scan_map_entries(kv: &KvMap, request: BackendKvScanRequest) -> BackendKvEntryPage {
    let pairs = scan_filtered_pairs(kv, &request);
    let has_more = pairs.len() > request.limit;
    let mut keys = BytePageBuilder::with_capacity(request.limit.min(pairs.len()), 0);
    let mut values = BytePageBuilder::with_capacity(request.limit.min(pairs.len()), 0);
    let mut resume_after = None;
    for (index, (key, value)) in pairs.into_iter().enumerate() {
        if index >= request.limit {
            break;
        }
        resume_after = Some(key.clone());
        keys.push(key);
        values.push(value);
    }
    let resume_after = has_more.then_some(resume_after).flatten();
    BackendKvEntryPage {
        keys: keys.finish(),
        values: values.finish(),
        resume_after,
    }
}

fn scan_filtered_pairs<'a>(
    kv: &'a KvMap,
    request: &BackendKvScanRequest,
) -> Vec<(&'a Vec<u8>, &'a Vec<u8>)> {
    let scan_limit = request
        .limit
        .checked_add(1 + usize::from(request.after.is_some()))
        .unwrap_or(request.limit);
    let mut pairs = kv
        .iter()
        .filter(|((candidate_namespace, key), _)| {
            candidate_namespace == &request.namespace && key_matches_range(key, &request.range)
        })
        .filter(|((_, key), _)| {
            request
                .after
                .as_deref()
                .is_none_or(|after| key.as_slice() > after)
        })
        .collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0 .1.cmp(&right.0 .1));
    pairs.truncate(scan_limit);
    pairs
        .into_iter()
        .filter(|((_, key), _)| {
            request
                .after
                .as_deref()
                .is_none_or(|after| key.as_slice() > after)
        })
        .map(|((_, key), value)| (key, value))
        .collect()
}

fn key_matches_range(key: &[u8], range: &BackendKvScanRange) -> bool {
    match range {
        BackendKvScanRange::Prefix(prefix) => key.starts_with(prefix),
        BackendKvScanRange::Range { start, end } => start.as_slice() <= key && key < end.as_slice(),
    }
}

fn delete_range_bytes(range: &BackendKvScanRange) -> usize {
    match range {
        BackendKvScanRange::Prefix(prefix) => prefix.len(),
        BackendKvScanRange::Range { start, end } => start.len() + end.len(),
    }
}

fn lock_error(name: &str) -> LixError {
    LixError::new("LIX_ERROR_UNKNOWN", format!("{name} mutex was poisoned"))
}
