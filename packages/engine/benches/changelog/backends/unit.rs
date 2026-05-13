use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_engine::{
    Backend, BackendKvEntryPage, BackendKvExistsBatch, BackendKvExistsGroup, BackendKvGetRequest,
    BackendKvKeyPage, BackendKvScanRange, BackendKvScanRequest, BackendKvValueBatch,
    BackendKvValueGroup, BackendKvValuePage, BackendKvWriteBatch, BackendKvWriteStats,
    BackendReadTransaction, BackendWriteTransaction, BytePageBuilder, LixError,
};

type KvMap = BTreeMap<(String, Vec<u8>), Vec<u8>>;

#[derive(Clone, Debug, Default)]
pub(crate) struct UnitChangelogBenchBackend {
    kv: Arc<Mutex<KvMap>>,
}

impl UnitChangelogBenchBackend {
    pub(crate) fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Backend for UnitChangelogBenchBackend {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        Ok(Box::new(UnitChangelogBenchReadTransaction {
            kv: Arc::clone(&self.kv),
        }))
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        let snapshot = self
            .kv
            .lock()
            .map_err(|_| lock_error("changelog bench kv"))?
            .clone();
        Ok(Box::new(UnitChangelogBenchWriteTransaction {
            parent: Arc::clone(&self.kv),
            kv: snapshot,
        }))
    }
}

struct UnitChangelogBenchReadTransaction {
    kv: Arc<Mutex<KvMap>>,
}

struct UnitChangelogBenchWriteTransaction {
    parent: Arc<Mutex<KvMap>>,
    kv: KvMap,
}

#[async_trait]
impl BackendReadTransaction for UnitChangelogBenchReadTransaction {
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        let kv = self
            .kv
            .lock()
            .map_err(|_| lock_error("changelog bench kv"))?;
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
        Ok(BackendKvValueBatch { groups })
    }

    async fn exists_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvExistsBatch, LixError> {
        let kv = self
            .kv
            .lock()
            .map_err(|_| lock_error("changelog bench kv"))?;
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
        Ok(BackendKvExistsBatch { groups })
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        let kv = self
            .kv
            .lock()
            .map_err(|_| lock_error("changelog bench kv"))?;
        let (pairs, resume_after) = scan_pairs(&kv, &request);
        let mut keys = BytePageBuilder::with_capacity(pairs.len(), 0);
        for (key, _) in pairs {
            keys.push(key);
        }
        Ok(BackendKvKeyPage {
            keys: keys.finish(),
            resume_after,
        })
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        let kv = self
            .kv
            .lock()
            .map_err(|_| lock_error("changelog bench kv"))?;
        let (pairs, resume_after) = scan_pairs(&kv, &request);
        let mut values = BytePageBuilder::with_capacity(pairs.len(), 0);
        for (_, value) in pairs {
            values.push(value);
        }
        Ok(BackendKvValuePage {
            values: values.finish(),
            resume_after,
        })
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        let kv = self
            .kv
            .lock()
            .map_err(|_| lock_error("changelog bench kv"))?;
        let (pairs, resume_after) = scan_pairs(&kv, &request);
        let mut keys = BytePageBuilder::with_capacity(pairs.len(), 0);
        let mut values = BytePageBuilder::with_capacity(pairs.len(), 0);
        for (key, value) in pairs {
            keys.push(key);
            values.push(value);
        }
        Ok(BackendKvEntryPage {
            keys: keys.finish(),
            values: values.finish(),
            resume_after,
        })
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        Ok(())
    }
}

#[async_trait]
impl BackendReadTransaction for UnitChangelogBenchWriteTransaction {
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        read_get_values(&self.kv, request)
    }

    async fn exists_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvExistsBatch, LixError> {
        read_exists_many(&self.kv, request)
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        read_scan_keys(&self.kv, request)
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        read_scan_values(&self.kv, request)
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        read_scan_entries(&self.kv, request)
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        Ok(())
    }
}

#[async_trait]
impl BackendWriteTransaction for UnitChangelogBenchWriteTransaction {
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
                self.kv
                    .insert((namespace.clone(), key.to_vec()), value.to_vec());
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
                self.kv.remove(&(namespace.clone(), key.to_vec()));
            }
        }
        Ok(stats)
    }

    async fn commit(self: Box<Self>) -> Result<(), LixError> {
        *self
            .parent
            .lock()
            .map_err(|_| lock_error("changelog bench kv"))? = self.kv;
        Ok(())
    }
}

fn read_get_values(
    kv: &KvMap,
    request: BackendKvGetRequest,
) -> Result<BackendKvValueBatch, LixError> {
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
    Ok(BackendKvValueBatch { groups })
}

fn read_exists_many(
    kv: &KvMap,
    request: BackendKvGetRequest,
) -> Result<BackendKvExistsBatch, LixError> {
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
    Ok(BackendKvExistsBatch { groups })
}

fn read_scan_keys(kv: &KvMap, request: BackendKvScanRequest) -> Result<BackendKvKeyPage, LixError> {
    let (pairs, resume_after) = scan_pairs(kv, &request);
    let mut keys = BytePageBuilder::with_capacity(pairs.len(), 0);
    for (key, _) in pairs {
        keys.push(key);
    }
    Ok(BackendKvKeyPage {
        keys: keys.finish(),
        resume_after,
    })
}

fn read_scan_values(
    kv: &KvMap,
    request: BackendKvScanRequest,
) -> Result<BackendKvValuePage, LixError> {
    let (pairs, resume_after) = scan_pairs(kv, &request);
    let mut values = BytePageBuilder::with_capacity(pairs.len(), 0);
    for (_, value) in pairs {
        values.push(value);
    }
    Ok(BackendKvValuePage {
        values: values.finish(),
        resume_after,
    })
}

fn read_scan_entries(
    kv: &KvMap,
    request: BackendKvScanRequest,
) -> Result<BackendKvEntryPage, LixError> {
    let (pairs, resume_after) = scan_pairs(kv, &request);
    let mut keys = BytePageBuilder::with_capacity(pairs.len(), 0);
    let mut values = BytePageBuilder::with_capacity(pairs.len(), 0);
    for (key, value) in pairs {
        keys.push(key);
        values.push(value);
    }
    Ok(BackendKvEntryPage {
        keys: keys.finish(),
        values: values.finish(),
        resume_after,
    })
}

fn scan_pairs<'a>(
    kv: &'a KvMap,
    request: &BackendKvScanRequest,
) -> (Vec<(&'a Vec<u8>, &'a Vec<u8>)>, Option<Vec<u8>>) {
    let mut pairs = kv
        .iter()
        .filter(|((namespace, key), _)| {
            namespace == &request.namespace && key_matches_range(key, &request.range)
        })
        .filter(|((_, key), _)| {
            request
                .after
                .as_ref()
                .map(|after| key > after)
                .unwrap_or(true)
        })
        .map(|((_, key), value)| (key, value))
        .collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    let resume_after = if pairs.len() > request.limit {
        pairs
            .get(request.limit.saturating_sub(1))
            .map(|(key, _)| (*key).clone())
    } else {
        None
    };
    pairs.truncate(request.limit);
    (pairs, resume_after)
}

fn key_matches_range(key: &[u8], range: &BackendKvScanRange) -> bool {
    match range {
        BackendKvScanRange::Prefix(prefix) => key.starts_with(prefix),
        BackendKvScanRange::Range { start, end } => key >= start.as_slice() && key < end.as_slice(),
    }
}

fn lock_error(name: &str) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("failed to acquire {name} lock"),
    )
}
