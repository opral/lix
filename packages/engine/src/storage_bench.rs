use crate::binary_cas::{BinaryBlobWrite, BinaryCasContext};
use crate::changelog::{
    canonicalize_materialized_change, CanonicalChange, ChangelogContext, ChangelogScanRequest,
    MaterializedCanonicalChange,
};
use crate::entity_identity::EntityIdentity;
use crate::entity_identity::EntityIdentityPart;
use crate::json_store::context::JsonStoreContext;
use crate::json_store::types::{JsonProjectionPath, JsonRef};
use crate::storage::KvScanRange;
use crate::storage::{KvGetGroup, KvGetRequest, KvScanRequest, KvWriteBatch, StorageContext};
use crate::tracked_state::{
    TrackedStateContext, TrackedStateDiffRequest, TrackedStateFilter, TrackedStateProjection,
    TrackedStateRow, TrackedStateRowRequest, TrackedStateScanRequest,
};
use crate::untracked_state::{
    UntrackedStateContext, UntrackedStateFilter, UntrackedStateRow, UntrackedStateRowRequest,
    UntrackedStateScanRequest,
};
use crate::{Backend, LixError, NullableKeyFilter};
use std::sync::Arc;
use std::time::{Duration, Instant};

const DEFAULT_MAX_INLINE_ENCODED_VALUE_BYTES: usize = 1024;

#[derive(Debug, Clone, Copy)]
pub struct StorageBenchConfig {
    pub rows: usize,
    pub blob_bytes: usize,
    pub state_payload_bytes: usize,
    pub key_pattern: StorageBenchKeyPattern,
    pub selectivity: StorageBenchSelectivity,
    pub update_fraction: StorageBenchUpdateFraction,
}

impl StorageBenchConfig {
    pub fn with_rows(mut self, rows: usize) -> Self {
        self.rows = rows;
        self
    }

    pub fn with_blob_bytes(mut self, blob_bytes: usize) -> Self {
        self.blob_bytes = blob_bytes;
        self
    }

    pub fn with_state_payload_bytes(mut self, state_payload_bytes: usize) -> Self {
        self.state_payload_bytes = state_payload_bytes;
        self
    }

    pub fn with_key_pattern(mut self, key_pattern: StorageBenchKeyPattern) -> Self {
        self.key_pattern = key_pattern;
        self
    }

    pub fn with_selectivity(mut self, selectivity: StorageBenchSelectivity) -> Self {
        self.selectivity = selectivity;
        self
    }

    pub fn with_update_fraction(mut self, update_fraction: StorageBenchUpdateFraction) -> Self {
        self.update_fraction = update_fraction;
        self
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StorageBenchKeyPattern {
    Sequential,
    Random,
}

#[derive(Debug, Clone, Copy)]
pub enum StorageBenchSelectivity {
    Percent1,
    Percent10,
    Percent100,
}

impl StorageBenchSelectivity {
    fn matches(self, index: usize) -> bool {
        match self {
            Self::Percent1 => index % 100 == 0,
            Self::Percent10 => index % 10 == 0,
            Self::Percent100 => true,
        }
    }

    fn expected_rows(self, rows: usize) -> usize {
        (0..rows).filter(|index| self.matches(*index)).count()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum StorageBenchUpdateFraction {
    Percent10,
    Percent100,
}

impl StorageBenchUpdateFraction {
    fn rows(self, total_rows: usize) -> usize {
        match self {
            Self::Percent10 => total_rows.div_ceil(10),
            Self::Percent100 => total_rows,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StorageBenchReport {
    pub measured_rows: usize,
    pub verified_rows: usize,
    pub elapsed: Duration,
}

pub struct StorageApiFixture {
    storage: StorageContext,
    rows: usize,
}

const STORAGE_API_NAMESPACE: &str = "bench.storage_api";
const STORAGE_API_ALT_NAMESPACE: &str = "bench.storage_api.alt";

pub async fn storage_api_write_kv_batch_puts(
    backend: Arc<dyn Backend + Send + Sync>,
    rows: usize,
) -> Result<StorageBenchReport, LixError> {
    let storage = StorageContext::new(backend);
    let mut transaction = storage.begin_write_transaction().await?;
    let mut batch = KvWriteBatch::new();
    for index in 0..rows {
        batch.put(
            STORAGE_API_NAMESPACE,
            storage_api_key(index),
            storage_api_value(index),
        );
    }
    let started_at = Instant::now();
    let stats = transaction.write_kv_batch(batch).await?;
    transaction.commit().await?;
    Ok(StorageBenchReport {
        measured_rows: stats.puts,
        verified_rows: rows,
        elapsed: started_at.elapsed(),
    })
}

pub async fn storage_api_write_kv_batch_mixed_put_delete(
    backend: Arc<dyn Backend + Send + Sync>,
    rows: usize,
) -> Result<StorageBenchReport, LixError> {
    let fixture = prepare_storage_api_read(backend, rows).await?;
    let mut transaction = fixture.storage.begin_write_transaction().await?;
    let mut batch = KvWriteBatch::new();
    for index in 0..rows {
        if index % 2 == 0 {
            batch.put(
                STORAGE_API_NAMESPACE,
                storage_api_key(index),
                storage_api_updated_value(index),
            );
        } else {
            batch.delete(STORAGE_API_NAMESPACE, storage_api_key(index));
        }
    }
    let started_at = Instant::now();
    let stats = transaction.write_kv_batch(batch).await?;
    transaction.commit().await?;
    Ok(StorageBenchReport {
        measured_rows: stats.puts + stats.deletes,
        verified_rows: rows,
        elapsed: started_at.elapsed(),
    })
}

pub async fn storage_api_write_kv_batch_multi_namespace(
    backend: Arc<dyn Backend + Send + Sync>,
    rows: usize,
) -> Result<StorageBenchReport, LixError> {
    let storage = StorageContext::new(backend);
    let mut transaction = storage.begin_write_transaction().await?;
    let mut batch = KvWriteBatch::new();
    for index in 0..rows {
        let namespace = if index % 2 == 0 {
            STORAGE_API_NAMESPACE
        } else {
            STORAGE_API_ALT_NAMESPACE
        };
        batch.put(namespace, storage_api_key(index), storage_api_value(index));
    }
    let started_at = Instant::now();
    let stats = transaction.write_kv_batch(batch).await?;
    transaction.commit().await?;
    Ok(StorageBenchReport {
        measured_rows: stats.puts,
        verified_rows: rows,
        elapsed: started_at.elapsed(),
    })
}

pub async fn storage_api_write_kv_batch_duplicate_keys(
    backend: Arc<dyn Backend + Send + Sync>,
    rows: usize,
) -> Result<StorageBenchReport, LixError> {
    let storage = StorageContext::new(backend);
    let mut transaction = storage.begin_write_transaction().await?;
    let mut batch = KvWriteBatch::new();
    for index in 0..rows {
        batch.put(
            STORAGE_API_NAMESPACE,
            storage_api_key(index % 100),
            storage_api_value(index),
        );
    }
    let started_at = Instant::now();
    let stats = transaction.write_kv_batch(batch).await?;
    transaction.commit().await?;
    Ok(StorageBenchReport {
        measured_rows: stats.puts,
        verified_rows: rows,
        elapsed: started_at.elapsed(),
    })
}

pub async fn storage_api_write_kv_batch_value_size(
    backend: Arc<dyn Backend + Send + Sync>,
    rows: usize,
    value_bytes: usize,
) -> Result<StorageBenchReport, LixError> {
    let storage = StorageContext::new(backend);
    let mut transaction = storage.begin_write_transaction().await?;
    let mut batch = KvWriteBatch::new();
    for index in 0..rows {
        batch.put(
            STORAGE_API_NAMESPACE,
            storage_api_key(index),
            storage_api_value_with_bytes(index, value_bytes),
        );
    }
    let started_at = Instant::now();
    let stats = transaction.write_kv_batch(batch).await?;
    transaction.commit().await?;
    Ok(StorageBenchReport {
        measured_rows: stats.puts,
        verified_rows: rows,
        elapsed: started_at.elapsed(),
    })
}

pub async fn storage_api_write_and_commit(
    backend: Arc<dyn Backend + Send + Sync>,
    rows: usize,
) -> Result<StorageBenchReport, LixError> {
    let storage = StorageContext::new(backend);
    let started_at = Instant::now();
    let mut transaction = storage.begin_write_transaction().await?;
    let mut batch = KvWriteBatch::new();
    for index in 0..rows {
        batch.put(
            STORAGE_API_NAMESPACE,
            storage_api_key(index),
            storage_api_value(index),
        );
    }
    let stats = transaction.write_kv_batch(batch).await?;
    transaction.commit().await?;
    Ok(StorageBenchReport {
        measured_rows: stats.puts,
        verified_rows: rows,
        elapsed: started_at.elapsed(),
    })
}

pub async fn storage_api_rollback_after_write(
    backend: Arc<dyn Backend + Send + Sync>,
    rows: usize,
) -> Result<StorageBenchReport, LixError> {
    let storage = StorageContext::new(backend);
    let started_at = Instant::now();
    let mut transaction = storage.begin_write_transaction().await?;
    let mut batch = KvWriteBatch::new();
    for index in 0..rows {
        batch.put(
            STORAGE_API_NAMESPACE,
            storage_api_key(index),
            storage_api_value(index),
        );
    }
    let stats = transaction.write_kv_batch(batch).await?;
    transaction.rollback().await?;
    Ok(StorageBenchReport {
        measured_rows: stats.puts,
        verified_rows: rows,
        elapsed: started_at.elapsed(),
    })
}

pub async fn prepare_storage_api_read(
    backend: Arc<dyn Backend + Send + Sync>,
    rows: usize,
) -> Result<StorageApiFixture, LixError> {
    let storage = StorageContext::new(backend);
    let mut transaction = storage.begin_write_transaction().await?;
    let mut batch = KvWriteBatch::new();
    for index in 0..rows {
        batch.put(
            STORAGE_API_NAMESPACE,
            storage_api_key(index),
            storage_api_value(index),
        );
    }
    transaction.write_kv_batch(batch).await?;
    transaction.commit().await?;
    Ok(StorageApiFixture { storage, rows })
}

pub async fn storage_api_get_kv_many_hits_prepared(
    fixture: &StorageApiFixture,
    reads: usize,
) -> Result<StorageBenchReport, LixError> {
    let mut transaction = fixture.storage.begin_read_transaction().await?;
    let keys = (0..reads)
        .map(|index| storage_api_key(index % fixture.rows))
        .collect::<Vec<_>>();
    let started_at = Instant::now();
    let result = transaction
        .get_kv_many(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: STORAGE_API_NAMESPACE.to_string(),
                keys,
            }],
        })
        .await?;
    transaction.rollback().await?;
    let verified_rows = result.groups[0]
        .values
        .iter()
        .filter(|value| value.is_some())
        .count();
    Ok(StorageBenchReport {
        measured_rows: reads,
        verified_rows,
        elapsed: started_at.elapsed(),
    })
}

pub async fn storage_api_get_kv_many_misses_prepared(
    fixture: &StorageApiFixture,
    reads: usize,
) -> Result<StorageBenchReport, LixError> {
    let mut transaction = fixture.storage.begin_read_transaction().await?;
    let keys = (0..reads)
        .map(|index| storage_api_missing_key(index))
        .collect::<Vec<_>>();
    let started_at = Instant::now();
    let result = transaction
        .get_kv_many(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: STORAGE_API_NAMESPACE.to_string(),
                keys,
            }],
        })
        .await?;
    transaction.rollback().await?;
    let verified_rows = result.groups[0]
        .values
        .iter()
        .filter(|value| value.is_none())
        .count();
    Ok(StorageBenchReport {
        measured_rows: reads,
        verified_rows,
        elapsed: started_at.elapsed(),
    })
}

pub async fn storage_api_get_kv_many_mixed_hit_miss_prepared(
    fixture: &StorageApiFixture,
    reads: usize,
) -> Result<StorageBenchReport, LixError> {
    let mut transaction = fixture.storage.begin_read_transaction().await?;
    let keys = (0..reads)
        .map(|index| {
            if index % 2 == 0 {
                storage_api_key(index % fixture.rows)
            } else {
                storage_api_missing_key(index)
            }
        })
        .collect::<Vec<_>>();
    let started_at = Instant::now();
    let result = transaction
        .get_kv_many(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: STORAGE_API_NAMESPACE.to_string(),
                keys,
            }],
        })
        .await?;
    transaction.rollback().await?;
    let verified_rows = result.groups[0]
        .values
        .iter()
        .filter(|value| value.is_some())
        .count();
    Ok(StorageBenchReport {
        measured_rows: reads,
        verified_rows,
        elapsed: started_at.elapsed(),
    })
}

pub async fn storage_api_get_kv_many_multi_namespace(
    backend: Arc<dyn Backend + Send + Sync>,
    reads: usize,
) -> Result<StorageBenchReport, LixError> {
    let storage = StorageContext::new(backend);
    let mut transaction = storage.begin_write_transaction().await?;
    let mut batch = KvWriteBatch::new();
    for index in 0..reads {
        let namespace = if index % 2 == 0 {
            STORAGE_API_NAMESPACE
        } else {
            STORAGE_API_ALT_NAMESPACE
        };
        batch.put(namespace, storage_api_key(index), storage_api_value(index));
    }
    transaction.write_kv_batch(batch).await?;
    transaction.commit().await?;

    let mut transaction = storage.begin_read_transaction().await?;
    let even_keys = (0..reads)
        .step_by(2)
        .map(storage_api_key)
        .collect::<Vec<_>>();
    let odd_keys = (1..reads)
        .step_by(2)
        .map(storage_api_key)
        .collect::<Vec<_>>();
    let started_at = Instant::now();
    let result = transaction
        .get_kv_many(KvGetRequest {
            groups: vec![
                KvGetGroup {
                    namespace: STORAGE_API_NAMESPACE.to_string(),
                    keys: even_keys,
                },
                KvGetGroup {
                    namespace: STORAGE_API_ALT_NAMESPACE.to_string(),
                    keys: odd_keys,
                },
            ],
        })
        .await?;
    transaction.rollback().await?;
    let verified_rows = result
        .groups
        .iter()
        .flat_map(|group| group.values.iter())
        .filter(|value| value.is_some())
        .count();
    Ok(StorageBenchReport {
        measured_rows: reads,
        verified_rows,
        elapsed: started_at.elapsed(),
    })
}

pub async fn storage_api_get_kv_many_duplicate_keys_prepared(
    fixture: &StorageApiFixture,
    reads: usize,
) -> Result<StorageBenchReport, LixError> {
    let mut transaction = fixture.storage.begin_read_transaction().await?;
    let keys = (0..reads)
        .map(|index| storage_api_key(index % 100))
        .collect::<Vec<_>>();
    let started_at = Instant::now();
    let result = transaction
        .get_kv_many(KvGetRequest {
            groups: vec![KvGetGroup {
                namespace: STORAGE_API_NAMESPACE.to_string(),
                keys,
            }],
        })
        .await?;
    transaction.rollback().await?;
    let verified_rows = result.groups[0]
        .values
        .iter()
        .filter(|value| value.is_some())
        .count();
    Ok(StorageBenchReport {
        measured_rows: reads,
        verified_rows,
        elapsed: started_at.elapsed(),
    })
}

pub async fn storage_api_scan_kv_prefix_prepared(
    fixture: &StorageApiFixture,
    limit: usize,
) -> Result<StorageBenchReport, LixError> {
    let mut transaction = fixture.storage.begin_read_transaction().await?;
    let started_at = Instant::now();
    let result = transaction
        .scan_kv(KvScanRequest {
            namespace: STORAGE_API_NAMESPACE.to_string(),
            range: KvScanRange::prefix(b"key/".to_vec()),
            after: None,
            limit,
        })
        .await?;
    transaction.rollback().await?;
    Ok(StorageBenchReport {
        measured_rows: result.rows.len(),
        verified_rows: limit.min(fixture.rows),
        elapsed: started_at.elapsed(),
    })
}

pub async fn storage_api_scan_kv_after_pages_prepared(
    fixture: &StorageApiFixture,
    page_size: usize,
) -> Result<StorageBenchReport, LixError> {
    let mut transaction = fixture.storage.begin_read_transaction().await?;
    let started_at = Instant::now();
    let mut after = None;
    let mut measured_rows = 0usize;
    loop {
        let result = transaction
            .scan_kv(KvScanRequest {
                namespace: STORAGE_API_NAMESPACE.to_string(),
                range: KvScanRange::prefix(b"key/".to_vec()),
                after,
                limit: page_size,
            })
            .await?;
        if result.rows.is_empty() {
            break;
        }
        measured_rows += result.rows.len();
        let Some(resume_after) = result.resume_after else {
            break;
        };
        after = Some(resume_after);
    }
    transaction.rollback().await?;
    Ok(StorageBenchReport {
        measured_rows,
        verified_rows: fixture.rows,
        elapsed: started_at.elapsed(),
    })
}

pub async fn storage_api_scan_kv_empty_range_prepared(
    fixture: &StorageApiFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut transaction = fixture.storage.begin_read_transaction().await?;
    let started_at = Instant::now();
    let result = transaction
        .scan_kv(KvScanRequest {
            namespace: STORAGE_API_NAMESPACE.to_string(),
            range: KvScanRange::prefix(b"absent/".to_vec()),
            after: None,
            limit: fixture.rows,
        })
        .await?;
    transaction.rollback().await?;
    Ok(StorageBenchReport {
        measured_rows: result.rows.len(),
        verified_rows: 0,
        elapsed: started_at.elapsed(),
    })
}

pub async fn prepare_storage_api_selective_scan(
    backend: Arc<dyn Backend + Send + Sync>,
    rows: usize,
    selectivity: StorageBenchSelectivity,
) -> Result<StorageApiFixture, LixError> {
    let storage = StorageContext::new(backend);
    let mut transaction = storage.begin_write_transaction().await?;
    let mut batch = KvWriteBatch::new();
    for index in 0..rows {
        let key = if selectivity.matches(index) {
            storage_api_selective_key(index)
        } else {
            storage_api_key(index)
        };
        batch.put(STORAGE_API_NAMESPACE, key, storage_api_value(index));
    }
    transaction.write_kv_batch(batch).await?;
    transaction.commit().await?;
    Ok(StorageApiFixture { storage, rows })
}

pub async fn storage_api_scan_kv_selective_prefix_prepared(
    fixture: &StorageApiFixture,
    selectivity: StorageBenchSelectivity,
) -> Result<StorageBenchReport, LixError> {
    let mut transaction = fixture.storage.begin_read_transaction().await?;
    let started_at = Instant::now();
    let result = transaction
        .scan_kv(KvScanRequest {
            namespace: STORAGE_API_NAMESPACE.to_string(),
            range: KvScanRange::prefix(b"selective/".to_vec()),
            after: None,
            limit: fixture.rows,
        })
        .await?;
    transaction.rollback().await?;
    Ok(StorageBenchReport {
        measured_rows: result.rows.len(),
        verified_rows: selectivity.expected_rows(fixture.rows),
        elapsed: started_at.elapsed(),
    })
}

pub async fn storage_api_transaction_commit_empty(
    backend: Arc<dyn Backend + Send + Sync>,
) -> Result<StorageBenchReport, LixError> {
    let storage = StorageContext::new(backend);
    let started_at = Instant::now();
    let transaction = storage.begin_write_transaction().await?;
    transaction.commit().await?;
    Ok(StorageBenchReport {
        measured_rows: 0,
        verified_rows: 0,
        elapsed: started_at.elapsed(),
    })
}

fn storage_api_key(index: usize) -> Vec<u8> {
    format!("key/{index:08}").into_bytes()
}

fn storage_api_selective_key(index: usize) -> Vec<u8> {
    format!("selective/{index:08}").into_bytes()
}

fn storage_api_missing_key(index: usize) -> Vec<u8> {
    format!("missing/{index:08}").into_bytes()
}

fn storage_api_value(index: usize) -> Vec<u8> {
    format!("value/{index:08}/{}", "x".repeat(64)).into_bytes()
}

fn storage_api_value_with_bytes(index: usize, value_bytes: usize) -> Vec<u8> {
    let prefix = format!("value/{index:08}/");
    if value_bytes <= prefix.len() {
        return prefix.into_bytes();
    }
    let mut value = prefix.into_bytes();
    value.extend(std::iter::repeat_n(b'x', value_bytes - value.len()));
    value
}

fn storage_api_updated_value(index: usize) -> Vec<u8> {
    format!("updated/{index:08}/{}", "y".repeat(64)).into_bytes()
}

pub struct TrackedStateWriteRootFixture {
    context: TrackedStateContext,
    rows: Vec<TrackedStateRow>,
}

pub struct TrackedStateReadFixture {
    context: TrackedStateContext,
    rows: usize,
    commit_id: String,
    key_pattern: StorageBenchKeyPattern,
    selectivity: StorageBenchSelectivity,
}

pub struct TrackedStateUpdateFixture {
    context: TrackedStateContext,
    rows: Vec<TrackedStateRow>,
}

pub struct TrackedStateDiffFixture {
    context: TrackedStateContext,
    left_commit_id: String,
    right_commit_id: String,
    expected_entries: usize,
}

pub struct UntrackedStateWriteFixture {
    context: UntrackedStateContext,
    rows: Vec<UntrackedStateRow>,
}

pub struct UntrackedStateReadFixture {
    context: UntrackedStateContext,
    rows: usize,
    key_pattern: StorageBenchKeyPattern,
    selectivity: StorageBenchSelectivity,
}

pub struct ChangelogAppendFixture {
    context: ChangelogContext,
    changes: Vec<MaterializedCanonicalChange>,
}

pub struct ChangelogReadFixture {
    context: ChangelogContext,
    rows: usize,
}

pub struct ChangelogCodecFixture {
    changes: Vec<CanonicalChange>,
    encoded_changes: Vec<Vec<u8>>,
}

pub struct BinaryCasWriteFixture {
    context: BinaryCasContext,
    file_ids: Vec<String>,
    payloads: Vec<Vec<u8>>,
}

pub struct BinaryCasReadFixture {
    context: BinaryCasContext,
    rows: usize,
    hashes: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum JsonStorePayloadShape {
    SmallRaw1k,
    MediumStructured16k,
    LargeStructured128k,
    LargeArray128k,
}

#[derive(Debug, Clone, Copy)]
pub enum JsonStoreProjectionShape {
    TopLevelTarget,
    TopLevelTenProps,
    NestedTarget,
    ArrayItem999,
    Status,
}

pub struct JsonStoreWriteFixture {
    context: JsonStoreContext,
    documents: Vec<Vec<u8>>,
}

pub struct JsonStoreReadFixture {
    context: JsonStoreContext,
    refs: Vec<JsonRef>,
    paths: Vec<JsonProjectionPath>,
}

pub async fn prepare_tracked_state_write_root(
    config: StorageBenchConfig,
) -> Result<TrackedStateWriteRootFixture, LixError> {
    prepare_tracked_state_write_root_with_max_inline_encoded_value_bytes(
        config,
        DEFAULT_MAX_INLINE_ENCODED_VALUE_BYTES,
    )
    .await
}

pub async fn prepare_tracked_state_write_root_with_max_inline_encoded_value_bytes(
    config: StorageBenchConfig,
    max_inline_encoded_value_bytes: usize,
) -> Result<TrackedStateWriteRootFixture, LixError> {
    Ok(TrackedStateWriteRootFixture {
        context: TrackedStateContext::with_max_inline_encoded_value_bytes_for_bench(
            max_inline_encoded_value_bytes,
        ),
        rows: tracked_rows(config, "bench-tracked-commit"),
    })
}

pub async fn tracked_state_write_root_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &TrackedStateWriteRootFixture,
) -> Result<StorageBenchReport, LixError> {
    write_tracked_root(
        backend,
        &fixture.context,
        "bench-tracked-commit",
        None,
        &fixture.rows,
    )
    .await?;
    Ok(report(
        fixture.rows.len(),
        fixture.rows.len(),
        Duration::ZERO,
    ))
}

pub async fn prepare_tracked_state_read(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<TrackedStateReadFixture, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-commit");
    write_tracked_root(backend, &context, "bench-tracked-commit", None, &rows).await?;
    Ok(TrackedStateReadFixture {
        context,
        rows: config.rows,
        commit_id: "bench-tracked-commit".to_string(),
        key_pattern: config.key_pattern,
        selectivity: config.selectivity,
    })
}

pub async fn prepare_tracked_state_read_file_selective(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<TrackedStateReadFixture, LixError> {
    prepare_tracked_state_read_file_selective_with_max_inline_encoded_value_bytes(
        backend,
        config,
        DEFAULT_MAX_INLINE_ENCODED_VALUE_BYTES,
    )
    .await
}

pub async fn prepare_tracked_state_read_file_selective_with_max_inline_encoded_value_bytes(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
    max_inline_encoded_value_bytes: usize,
) -> Result<TrackedStateReadFixture, LixError> {
    let context = TrackedStateContext::with_max_inline_encoded_value_bytes_for_bench(
        max_inline_encoded_value_bytes,
    );
    let rows = tracked_rows_file_selective(config, "bench-tracked-commit");
    write_tracked_root(backend, &context, "bench-tracked-commit", None, &rows).await?;
    Ok(TrackedStateReadFixture {
        context,
        rows: config.rows,
        commit_id: "bench-tracked-commit".to_string(),
        key_pattern: config.key_pattern,
        selectivity: config.selectivity,
    })
}

pub async fn tracked_state_read_point_hit_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &TrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut verified_rows = 0;
    let mut reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    for index in 0..fixture.rows {
        if reader
            .load_row_at_commit(
                &fixture.commit_id,
                &TrackedStateRowRequest {
                    schema_key: tracked_schema_key(index, StorageBenchSelectivity::Percent100),
                    entity_id: EntityIdentity::single(entity_id(
                        "tracked",
                        index,
                        fixture.key_pattern,
                    )),
                    file_id: NullableKeyFilter::Value("bench.json".to_string()),
                },
            )
            .await?
            .is_some()
        {
            verified_rows += 1;
        }
    }
    Ok(report(fixture.rows, verified_rows, Duration::ZERO))
}

pub async fn tracked_state_read_point_hit_constant_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &TrackedStateReadFixture,
    measured_reads: usize,
) -> Result<StorageBenchReport, LixError> {
    let mut verified_rows = 0;
    let mut reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    for index in 0..measured_reads.min(fixture.rows) {
        if reader
            .load_row_at_commit(
                &fixture.commit_id,
                &TrackedStateRowRequest {
                    schema_key: tracked_schema_key(index, StorageBenchSelectivity::Percent100),
                    entity_id: EntityIdentity::single(entity_id(
                        "tracked",
                        index,
                        fixture.key_pattern,
                    )),
                    file_id: NullableKeyFilter::Value("bench.json".to_string()),
                },
            )
            .await?
            .is_some()
        {
            verified_rows += 1;
        }
    }
    Ok(report(
        measured_reads.min(fixture.rows),
        verified_rows,
        Duration::ZERO,
    ))
}

pub async fn tracked_state_read_point_miss_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &TrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut misses = 0;
    let mut reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    for index in 0..fixture.rows {
        if reader
            .load_row_at_commit(
                &fixture.commit_id,
                &TrackedStateRowRequest {
                    schema_key: "bench_tracked_entity".to_string(),
                    entity_id: EntityIdentity::single(format!("missing-{index}")),
                    file_id: NullableKeyFilter::Value("bench.json".to_string()),
                },
            )
            .await?
            .is_none()
        {
            misses += 1;
        }
    }
    Ok(report(fixture.rows, misses, Duration::ZERO))
}

pub async fn tracked_state_scan_all_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &TrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let verified_rows = scan_tracked(backend, &fixture.context, &fixture.commit_id)
        .await?
        .len();
    Ok(report(fixture.rows, verified_rows, Duration::ZERO))
}

pub async fn tracked_state_scan_schema_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &TrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    let verified_rows = reader
        .scan_rows_at_commit(
            &fixture.commit_id,
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    schema_keys: vec![tracked_schema_key(0, StorageBenchSelectivity::Percent100)],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await?
        .len();
    Ok(report(fixture.rows, verified_rows, Duration::ZERO))
}

pub async fn tracked_state_scan_schema_selective_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &TrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    let verified_rows = reader
        .scan_rows_at_commit(
            &fixture.commit_id,
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    schema_keys: vec![TRACKED_MATCH_SCHEMA_KEY.to_string()],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await?
        .len();
    Ok(report(
        fixture.selectivity.expected_rows(fixture.rows),
        verified_rows,
        Duration::ZERO,
    ))
}

pub async fn tracked_state_scan_file_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &TrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    let verified_rows = reader
        .scan_rows_at_commit(
            &fixture.commit_id,
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    file_ids: vec![NullableKeyFilter::Value("bench.json".to_string())],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await?
        .len();
    Ok(report(fixture.rows, verified_rows, Duration::ZERO))
}

pub async fn tracked_state_scan_file_selective_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &TrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    let verified_rows = reader
        .scan_rows_at_commit(
            &fixture.commit_id,
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    file_ids: vec![NullableKeyFilter::Value("bench-match.json".to_string())],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await?
        .len();
    Ok(report(
        fixture.selectivity.expected_rows(fixture.rows),
        verified_rows,
        Duration::ZERO,
    ))
}

pub async fn tracked_state_scan_file_header_selective_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &TrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    let verified_rows = reader
        .scan_rows_at_commit(
            &fixture.commit_id,
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    file_ids: vec![NullableKeyFilter::Value("bench-match.json".to_string())],
                    ..Default::default()
                },
                projection: TrackedStateProjection {
                    columns: vec![
                        "entity_id".to_string(),
                        "schema_key".to_string(),
                        "schema_version".to_string(),
                        "file_id".to_string(),
                        "metadata".to_string(),
                        "created_at".to_string(),
                        "updated_at".to_string(),
                        "change_id".to_string(),
                        "commit_id".to_string(),
                    ],
                },
                ..Default::default()
            },
        )
        .await?
        .len();
    Ok(report(
        fixture.selectivity.expected_rows(fixture.rows),
        verified_rows,
        Duration::ZERO,
    ))
}

pub async fn prepare_tracked_state_update(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<TrackedStateUpdateFixture, LixError> {
    prepare_tracked_state_update_rows(backend, config, config.update_fraction.rows(config.rows))
        .await
}

pub async fn prepare_tracked_state_update_rows(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
    updated_rows: usize,
) -> Result<TrackedStateUpdateFixture, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-parent");
    write_tracked_root(backend, &context, "bench-tracked-parent", None, &rows).await?;
    let mut updated_rows = tracked_rows(
        config.with_rows(updated_rows.min(config.rows)),
        "bench-tracked-child",
    );
    for (index, row) in updated_rows.iter_mut().enumerate() {
        row.snapshot_content = Some(updated_snapshot_content(index, config.state_payload_bytes));
    }
    Ok(TrackedStateUpdateFixture {
        context,
        rows: updated_rows,
    })
}

pub async fn prepare_tracked_state_partial_snapshot_update_rows(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
    updated_rows: usize,
) -> Result<TrackedStateUpdateFixture, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-parent");
    write_tracked_root(backend, &context, "bench-tracked-parent", None, &rows).await?;
    let mut updated_rows = tracked_rows(
        config.with_rows(updated_rows.min(config.rows)),
        "bench-tracked-child",
    );
    for (index, row) in updated_rows.iter_mut().enumerate() {
        row.snapshot_content = Some(partial_updated_snapshot_content(
            index,
            config.state_payload_bytes,
        ));
    }
    Ok(TrackedStateUpdateFixture {
        context,
        rows: updated_rows,
    })
}

pub async fn prepare_tracked_state_append_child(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<TrackedStateUpdateFixture, LixError> {
    prepare_tracked_state_append_child_rows(backend, config, config.rows).await
}

pub async fn prepare_tracked_state_append_child_rows(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
    appended_rows: usize,
) -> Result<TrackedStateUpdateFixture, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-parent");
    write_tracked_root(backend, &context, "bench-tracked-parent", None, &rows).await?;
    let mut appended_rows = tracked_rows(
        config.with_rows(appended_rows.min(config.rows)),
        "bench-tracked-child",
    );
    for (index, row) in appended_rows.iter_mut().enumerate() {
        row.entity_id = EntityIdentity::single(entity_id("tracked-new", index, config.key_pattern));
        row.change_id = format!("tracked-new-change-{index}");
    }
    Ok(TrackedStateUpdateFixture {
        context,
        rows: appended_rows,
    })
}

pub async fn prepare_tracked_state_tombstone_rows(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
    tombstone_rows: usize,
) -> Result<TrackedStateUpdateFixture, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-parent");
    write_tracked_root(backend, &context, "bench-tracked-parent", None, &rows).await?;
    let mut tombstones = tracked_rows(
        config.with_rows(tombstone_rows.min(config.rows)),
        "bench-tracked-child",
    );
    for row in &mut tombstones {
        row.snapshot_content = None;
    }
    Ok(TrackedStateUpdateFixture {
        context,
        rows: tombstones,
    })
}

pub async fn tracked_state_update_existing_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &TrackedStateUpdateFixture,
) -> Result<StorageBenchReport, LixError> {
    write_tracked_root(
        backend,
        &fixture.context,
        "bench-tracked-child",
        Some("bench-tracked-parent"),
        &fixture.rows,
    )
    .await?;
    Ok(report(
        fixture.rows.len(),
        fixture.rows.len(),
        Duration::ZERO,
    ))
}

pub async fn prepare_tracked_state_diff_update_rows(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
    updated_rows: usize,
) -> Result<TrackedStateDiffFixture, LixError> {
    let fixture = prepare_tracked_state_update_rows(backend, config, updated_rows).await?;
    tracked_state_update_existing_prepared(backend, &fixture).await?;
    Ok(TrackedStateDiffFixture {
        context: fixture.context,
        left_commit_id: "bench-tracked-parent".to_string(),
        right_commit_id: "bench-tracked-child".to_string(),
        expected_entries: fixture.rows.len(),
    })
}

pub async fn prepare_tracked_state_diff_tombstone_rows(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
    tombstone_rows: usize,
) -> Result<TrackedStateDiffFixture, LixError> {
    let fixture = prepare_tracked_state_tombstone_rows(backend, config, tombstone_rows).await?;
    tracked_state_update_existing_prepared(backend, &fixture).await?;
    Ok(TrackedStateDiffFixture {
        context: fixture.context,
        left_commit_id: "bench-tracked-parent".to_string(),
        right_commit_id: "bench-tracked-child".to_string(),
        expected_entries: fixture.rows.len(),
    })
}

pub async fn prepare_tracked_state_diff_equal(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<TrackedStateDiffFixture, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-parent");
    write_tracked_root(backend, &context, "bench-tracked-parent", None, &rows).await?;
    Ok(TrackedStateDiffFixture {
        context,
        left_commit_id: "bench-tracked-parent".to_string(),
        right_commit_id: "bench-tracked-parent".to_string(),
        expected_entries: 0,
    })
}

pub async fn tracked_state_diff_commits_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &TrackedStateDiffFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    let diff = reader
        .diff_commits(
            &fixture.left_commit_id,
            &fixture.right_commit_id,
            &TrackedStateDiffRequest::default(),
        )
        .await?;
    Ok(report(
        fixture.expected_entries,
        diff.entries.len(),
        Duration::ZERO,
    ))
}

pub async fn prepare_untracked_state_write_rows(
    config: StorageBenchConfig,
) -> Result<UntrackedStateWriteFixture, LixError> {
    Ok(UntrackedStateWriteFixture {
        context: UntrackedStateContext::new(),
        rows: untracked_rows(config),
    })
}

pub async fn untracked_state_write_rows_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &UntrackedStateWriteFixture,
) -> Result<StorageBenchReport, LixError> {
    write_untracked_rows(backend, &fixture.context, &fixture.rows).await?;
    let verified_rows = scan_untracked(
        backend,
        &fixture.context,
        UntrackedStateScanRequest::default(),
    )
    .await?
    .len();
    Ok(report(fixture.rows.len(), verified_rows, Duration::ZERO))
}

pub async fn prepare_untracked_state_read(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<UntrackedStateReadFixture, LixError> {
    let context = UntrackedStateContext::new();
    let rows = untracked_rows(config);
    write_untracked_rows(backend, &context, &rows).await?;
    Ok(UntrackedStateReadFixture {
        context,
        rows: config.rows,
        key_pattern: config.key_pattern,
        selectivity: config.selectivity,
    })
}

pub async fn untracked_state_read_point_hit_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &UntrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut verified_rows = 0;
    let mut reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    for index in 0..fixture.rows {
        if reader
            .load_row(&UntrackedStateRowRequest {
                schema_key: untracked_schema_key(index, StorageBenchSelectivity::Percent100),
                version_id: "bench-version".to_string(),
                entity_id: EntityIdentity::single(entity_id(
                    "untracked",
                    index,
                    fixture.key_pattern,
                )),
                file_id: NullableKeyFilter::Value("bench.json".to_string()),
            })
            .await?
            .is_some()
        {
            verified_rows += 1;
        }
    }
    Ok(report(fixture.rows, verified_rows, Duration::ZERO))
}

pub async fn untracked_state_read_point_hit_constant_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &UntrackedStateReadFixture,
    measured_reads: usize,
) -> Result<StorageBenchReport, LixError> {
    let mut verified_rows = 0;
    let mut reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    for index in 0..measured_reads.min(fixture.rows) {
        if reader
            .load_row(&UntrackedStateRowRequest {
                schema_key: untracked_schema_key(index, StorageBenchSelectivity::Percent100),
                version_id: "bench-version".to_string(),
                entity_id: EntityIdentity::single(entity_id(
                    "untracked",
                    index,
                    fixture.key_pattern,
                )),
                file_id: NullableKeyFilter::Value("bench.json".to_string()),
            })
            .await?
            .is_some()
        {
            verified_rows += 1;
        }
    }
    Ok(report(
        measured_reads.min(fixture.rows),
        verified_rows,
        Duration::ZERO,
    ))
}

pub async fn untracked_state_read_point_miss_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &UntrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut misses = 0;
    let mut reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    for index in 0..fixture.rows {
        if reader
            .load_row(&UntrackedStateRowRequest {
                schema_key: "bench_untracked_entity".to_string(),
                version_id: "bench-version".to_string(),
                entity_id: EntityIdentity::single(format!("missing-{index}")),
                file_id: NullableKeyFilter::Value("bench.json".to_string()),
            })
            .await?
            .is_none()
        {
            misses += 1;
        }
    }
    Ok(report(fixture.rows, misses, Duration::ZERO))
}

pub async fn untracked_state_scan_all_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &UntrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let verified_rows = scan_untracked(
        backend,
        &fixture.context,
        UntrackedStateScanRequest::default(),
    )
    .await?
    .len();
    Ok(report(fixture.rows, verified_rows, Duration::ZERO))
}

pub async fn untracked_state_scan_version_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &UntrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let verified_rows = scan_untracked(
        backend,
        &fixture.context,
        UntrackedStateScanRequest {
            filter: UntrackedStateFilter {
                version_ids: vec!["bench-version".to_string()],
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await?
    .len();
    Ok(report(fixture.rows, verified_rows, Duration::ZERO))
}

pub async fn untracked_state_scan_schema_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &UntrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let verified_rows = scan_untracked(
        backend,
        &fixture.context,
        UntrackedStateScanRequest {
            filter: UntrackedStateFilter {
                schema_keys: vec![untracked_schema_key(0, StorageBenchSelectivity::Percent100)],
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await?
    .len();
    Ok(report(fixture.rows, verified_rows, Duration::ZERO))
}

pub async fn untracked_state_scan_schema_selective_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &UntrackedStateReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let verified_rows = scan_untracked(
        backend,
        &fixture.context,
        UntrackedStateScanRequest {
            filter: UntrackedStateFilter {
                schema_keys: vec![UNTRACKED_MATCH_SCHEMA_KEY.to_string()],
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await?
    .len();
    Ok(report(
        fixture.selectivity.expected_rows(fixture.rows),
        verified_rows,
        Duration::ZERO,
    ))
}

pub async fn prepare_untracked_state_overwrite(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<UntrackedStateWriteFixture, LixError> {
    let context = UntrackedStateContext::new();
    let rows = untracked_rows(config);
    write_untracked_rows(backend, &context, &rows).await?;
    let mut updated_rows =
        untracked_rows(config.with_rows(config.update_fraction.rows(config.rows)));
    for (index, row) in updated_rows.iter_mut().enumerate() {
        row.snapshot_content = Some(updated_snapshot_content(index, config.state_payload_bytes));
    }
    Ok(UntrackedStateWriteFixture {
        context,
        rows: updated_rows,
    })
}

pub async fn prepare_untracked_state_insert_new_keys(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<UntrackedStateWriteFixture, LixError> {
    let context = UntrackedStateContext::new();
    let rows = untracked_rows(config);
    write_untracked_rows(backend, &context, &rows).await?;
    let mut new_rows = untracked_rows(config);
    for (index, row) in new_rows.iter_mut().enumerate() {
        row.entity_id =
            EntityIdentity::single(entity_id("untracked-new", index, config.key_pattern));
    }
    Ok(UntrackedStateWriteFixture {
        context,
        rows: new_rows,
    })
}

pub async fn untracked_state_overwrite_existing_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &UntrackedStateWriteFixture,
) -> Result<StorageBenchReport, LixError> {
    write_untracked_rows(backend, &fixture.context, &fixture.rows).await?;
    let verified_rows = scan_untracked(
        backend,
        &fixture.context,
        UntrackedStateScanRequest::default(),
    )
    .await?
    .len();
    Ok(report(fixture.rows.len(), verified_rows, Duration::ZERO))
}

pub async fn prepare_changelog_append_changes(
    config: StorageBenchConfig,
) -> Result<ChangelogAppendFixture, LixError> {
    Ok(ChangelogAppendFixture {
        context: ChangelogContext::new(),
        changes: changelog_materialized_changes(config),
    })
}

pub async fn prepare_changelog_append_tombstones(
    config: StorageBenchConfig,
) -> Result<ChangelogAppendFixture, LixError> {
    Ok(ChangelogAppendFixture {
        context: ChangelogContext::new(),
        changes: changelog_tombstone_changes(config),
    })
}

pub async fn prepare_changelog_append_metadata(
    config: StorageBenchConfig,
) -> Result<ChangelogAppendFixture, LixError> {
    Ok(ChangelogAppendFixture {
        context: ChangelogContext::new(),
        changes: changelog_metadata_changes(config),
    })
}

pub async fn prepare_changelog_append_shared_payload(
    config: StorageBenchConfig,
) -> Result<ChangelogAppendFixture, LixError> {
    Ok(ChangelogAppendFixture {
        context: ChangelogContext::new(),
        changes: changelog_shared_payload_changes(config),
    })
}

pub async fn prepare_changelog_append_shared_metadata(
    config: StorageBenchConfig,
) -> Result<ChangelogAppendFixture, LixError> {
    Ok(ChangelogAppendFixture {
        context: ChangelogContext::new(),
        changes: changelog_shared_metadata_changes(config),
    })
}

pub async fn prepare_changelog_append_shared_payload_and_metadata(
    config: StorageBenchConfig,
) -> Result<ChangelogAppendFixture, LixError> {
    Ok(ChangelogAppendFixture {
        context: ChangelogContext::new(),
        changes: changelog_shared_payload_and_metadata_changes(config),
    })
}

pub async fn prepare_changelog_append_composite_entity_ids(
    config: StorageBenchConfig,
) -> Result<ChangelogAppendFixture, LixError> {
    Ok(ChangelogAppendFixture {
        context: ChangelogContext::new(),
        changes: changelog_composite_entity_id_changes(config),
    })
}

pub async fn prepare_changelog_codec(
    config: StorageBenchConfig,
) -> Result<ChangelogCodecFixture, LixError> {
    let changes = changelog_changes(config);
    let encoded_changes = changes
        .iter()
        .map(crate::changelog::codec::encode_change)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ChangelogCodecFixture {
        changes,
        encoded_changes,
    })
}

pub async fn changelog_append_changes_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &ChangelogAppendFixture,
) -> Result<StorageBenchReport, LixError> {
    append_changelog_changes(backend, &fixture.context, &fixture.changes).await?;
    let reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    let verified_rows = reader
        .scan_changes(&ChangelogScanRequest::default())
        .await?
        .len();
    Ok(report(fixture.changes.len(), verified_rows, Duration::ZERO))
}

pub async fn prepare_changelog_read(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<ChangelogReadFixture, LixError> {
    let context = ChangelogContext::new();
    let changes = changelog_materialized_changes(config);
    append_changelog_changes(backend, &context, &changes).await?;
    Ok(ChangelogReadFixture {
        context,
        rows: config.rows,
    })
}

pub async fn prepare_changelog_read_with_selectivity(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<ChangelogReadFixture, LixError> {
    let context = ChangelogContext::new();
    let changes = changelog_selective_changes(config);
    append_changelog_changes(backend, &context, &changes).await?;
    Ok(ChangelogReadFixture {
        context,
        rows: config.rows,
    })
}

pub async fn prepare_changelog_read_entity_history(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<ChangelogReadFixture, LixError> {
    let context = ChangelogContext::new();
    let changes = changelog_entity_history_changes(config);
    append_changelog_changes(backend, &context, &changes).await?;
    Ok(ChangelogReadFixture {
        context,
        rows: config.rows,
    })
}

pub async fn prepare_changelog_read_commit_facts(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<ChangelogReadFixture, LixError> {
    let context = ChangelogContext::new();
    let changes = changelog_commit_fact_changes(config);
    append_changelog_changes(backend, &context, &changes).await?;
    Ok(ChangelogReadFixture {
        context,
        rows: config.rows,
    })
}

pub async fn changelog_encode_only_prepared(
    fixture: &ChangelogCodecFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut verified_rows = 0;
    let mut encoded_bytes = 0;
    for change in &fixture.changes {
        encoded_bytes += crate::changelog::codec::encode_change(change)?.len();
        verified_rows += 1;
    }
    Ok(report(
        fixture.changes.len(),
        verified_rows + usize::from(encoded_bytes == 0),
        Duration::ZERO,
    ))
}

pub async fn changelog_decode_only_prepared(
    fixture: &ChangelogCodecFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut verified_rows = 0;
    let mut decoded_bytes = 0;
    for bytes in &fixture.encoded_changes {
        let change = crate::changelog::codec::decode_change(bytes)?;
        decoded_bytes += change.schema_key.len();
        verified_rows += 1;
    }
    Ok(report(
        fixture.encoded_changes.len(),
        verified_rows + usize::from(decoded_bytes == 0),
        Duration::ZERO,
    ))
}

pub async fn changelog_load_change_hit_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &ChangelogReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    let mut verified_rows = 0;
    for index in 0..fixture.rows {
        if reader
            .load_change(&format!("bench-change-{index}"))
            .await?
            .is_some()
        {
            verified_rows += 1;
        }
    }
    Ok(report(fixture.rows, verified_rows, Duration::ZERO))
}

pub async fn changelog_load_change_miss_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &ChangelogReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    let mut misses = 0;
    for index in 0..fixture.rows {
        if reader
            .load_change(&format!("missing-change-{index}"))
            .await?
            .is_none()
        {
            misses += 1;
        }
    }
    Ok(report(fixture.rows, misses, Duration::ZERO))
}

pub async fn changelog_scan_all_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &ChangelogReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    let verified_rows = reader
        .scan_changes(&ChangelogScanRequest::default())
        .await?
        .len();
    Ok(report(fixture.rows, verified_rows, Duration::ZERO))
}

pub async fn changelog_scan_limit_100_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &ChangelogReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    let expected = fixture.rows.min(100);
    let verified_rows = reader
        .scan_changes(&ChangelogScanRequest {
            limit: Some(expected),
        })
        .await?
        .len();
    Ok(report(expected, verified_rows, Duration::ZERO))
}

pub async fn changelog_scan_schema_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &ChangelogReadFixture,
    selectivity: StorageBenchSelectivity,
) -> Result<StorageBenchReport, LixError> {
    let reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    let changes = reader
        .scan_changes(&ChangelogScanRequest::default())
        .await?;
    let verified_rows = changes
        .iter()
        .filter(|change| change.schema_key == CHANGELOG_MATCH_SCHEMA_KEY)
        .count();
    Ok(report(
        selectivity.expected_rows(fixture.rows),
        verified_rows,
        Duration::ZERO,
    ))
}

pub async fn changelog_scan_entity_history_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &ChangelogReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    let changes = reader
        .scan_changes(&ChangelogScanRequest::default())
        .await?;
    let target = EntityIdentity::single(CHANGELOG_HISTORY_ENTITY_ID);
    let verified_rows = changes
        .iter()
        .filter(|change| change.entity_id == target)
        .count();
    Ok(report(
        fixture.rows.div_ceil(10),
        verified_rows,
        Duration::ZERO,
    ))
}

pub async fn changelog_scan_commit_facts_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &ChangelogReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    let changes = reader
        .scan_changes(&ChangelogScanRequest::default())
        .await?;
    let verified_rows = changes
        .iter()
        .filter(|change| change.schema_key == "lix_commit")
        .count();
    Ok(report(
        fixture.rows.div_ceil(10),
        verified_rows,
        Duration::ZERO,
    ))
}

pub async fn prepare_binary_cas_write_blobs(
    config: StorageBenchConfig,
) -> Result<BinaryCasWriteFixture, LixError> {
    Ok(BinaryCasWriteFixture {
        context: BinaryCasContext::new(),
        file_ids: binary_file_ids(config.rows),
        payloads: binary_payloads(config.rows, config.blob_bytes),
    })
}

pub async fn prepare_binary_cas_write_duplicate_payload(
    config: StorageBenchConfig,
) -> Result<BinaryCasWriteFixture, LixError> {
    let payload = binary_payload(0, config.blob_bytes);
    Ok(BinaryCasWriteFixture {
        context: BinaryCasContext::new(),
        file_ids: binary_file_ids(config.rows),
        payloads: (0..config.rows).map(|_| payload.clone()).collect(),
    })
}

pub async fn prepare_binary_cas_write_half_duplicate_payload(
    config: StorageBenchConfig,
) -> Result<BinaryCasWriteFixture, LixError> {
    Ok(BinaryCasWriteFixture {
        context: BinaryCasContext::new(),
        file_ids: binary_file_ids(config.rows),
        payloads: binary_half_duplicate_payloads(config.rows, config.blob_bytes),
    })
}

pub async fn binary_cas_write_blobs_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &BinaryCasWriteFixture,
) -> Result<StorageBenchReport, LixError> {
    let writes = binary_blob_writes(&fixture.file_ids, &fixture.payloads);
    write_binary_blob_writes(backend, &fixture.context, &writes).await?;
    let verified_rows = count_binary_cas_manifests(backend).await?;
    Ok(report(writes.len(), verified_rows, Duration::ZERO))
}

pub async fn prepare_binary_cas_read(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<BinaryCasReadFixture, LixError> {
    let context = BinaryCasContext::new();
    let payloads = binary_payloads(config.rows, config.blob_bytes);
    let file_ids = binary_file_ids(config.rows);
    let writes = binary_blob_writes(&file_ids, &payloads);
    write_binary_blob_writes(backend, &context, &writes).await?;
    let hashes = payloads
        .iter()
        .map(|payload| crate::binary_cas::binary_blob_hash_hex(payload))
        .collect::<Vec<_>>();
    Ok(BinaryCasReadFixture {
        context,
        rows: config.rows,
        hashes,
    })
}

pub async fn binary_cas_read_blob_hit_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &BinaryCasReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut verified_rows = 0;
    let mut reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    for hash in &fixture.hashes {
        if reader.load_blob_data_by_hash(hash).await?.is_some() {
            verified_rows += 1;
        }
    }
    Ok(report(fixture.hashes.len(), verified_rows, Duration::ZERO))
}

pub async fn binary_cas_read_blob_miss_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &BinaryCasReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut misses = 0;
    let mut reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    for index in 0..fixture.rows {
        let missing_hash = format!("{index:064x}");
        if reader
            .load_blob_data_by_hash(&missing_hash)
            .await?
            .is_none()
        {
            misses += 1;
        }
    }
    Ok(report(fixture.rows, misses, Duration::ZERO))
}

pub async fn prepare_json_store_write(
    shape: JsonStorePayloadShape,
    rows: usize,
) -> Result<JsonStoreWriteFixture, LixError> {
    Ok(JsonStoreWriteFixture {
        context: JsonStoreContext::new(),
        documents: json_documents(shape, rows),
    })
}

pub async fn prepare_json_store_write_dedupe(
    shape: JsonStorePayloadShape,
    rows: usize,
) -> Result<JsonStoreWriteFixture, LixError> {
    let document = json_document(shape, 0);
    Ok(JsonStoreWriteFixture {
        context: JsonStoreContext::new(),
        documents: (0..rows).map(|_| document.clone()).collect(),
    })
}

pub async fn json_store_write_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &JsonStoreWriteFixture,
) -> Result<StorageBenchReport, LixError> {
    let storage = StorageContext::new(Arc::clone(backend));
    let mut transaction = storage.begin_write_transaction().await?;
    {
        let mut writer = fixture.context.writer();
        for document in &fixture.documents {
            writer.stage_bytes(document)?;
        }
        let mut store = transaction.as_mut();
        writer.flush(&mut store).await?;
    }
    transaction.commit().await?;
    Ok(report(
        fixture.documents.len(),
        fixture.documents.len(),
        Duration::ZERO,
    ))
}

pub async fn prepare_json_store_read(
    backend: &Arc<dyn Backend + Send + Sync>,
    shape: JsonStorePayloadShape,
    rows: usize,
) -> Result<JsonStoreReadFixture, LixError> {
    prepare_json_store_projection_read(
        backend,
        shape,
        rows,
        JsonStoreProjectionShape::TopLevelTarget,
    )
    .await
}

pub async fn prepare_json_store_projection_read(
    backend: &Arc<dyn Backend + Send + Sync>,
    shape: JsonStorePayloadShape,
    rows: usize,
    projection: JsonStoreProjectionShape,
) -> Result<JsonStoreReadFixture, LixError> {
    let context = JsonStoreContext::new();
    let documents = json_documents(shape, rows);
    let mut refs = Vec::with_capacity(documents.len());
    let storage = StorageContext::new(Arc::clone(backend));
    let mut transaction = storage.begin_write_transaction().await?;
    {
        let mut writer = context.writer();
        for document in documents {
            refs.push(writer.stage_bytes(&document)?);
        }
        let mut store = transaction.as_mut();
        writer.flush(&mut store).await?;
    }
    transaction.commit().await?;
    Ok(JsonStoreReadFixture {
        context,
        refs,
        paths: json_projection_paths(projection),
    })
}

pub async fn json_store_read_bytes_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &JsonStoreReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut verified_rows = 0;
    let mut reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    for json_ref in &fixture.refs {
        if reader.load_bytes(json_ref).await?.is_some() {
            verified_rows += 1;
        }
    }
    Ok(report(fixture.refs.len(), verified_rows, Duration::ZERO))
}

pub async fn json_store_read_value_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &JsonStoreReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut verified_rows = 0;
    let mut reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    for json_ref in &fixture.refs {
        if reader.load_json_value(json_ref).await?.is_some() {
            verified_rows += 1;
        }
    }
    Ok(report(fixture.refs.len(), verified_rows, Duration::ZERO))
}

pub async fn json_store_read_projection_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &JsonStoreReadFixture,
) -> Result<StorageBenchReport, LixError> {
    let mut verified_rows = 0;
    let mut reader = fixture
        .context
        .reader(StorageContext::new(Arc::clone(backend)));
    for json_ref in &fixture.refs {
        if reader
            .load_json_projection(json_ref, &fixture.paths)
            .await?
            .is_some()
        {
            verified_rows += 1;
        }
    }
    Ok(report(fixture.refs.len(), verified_rows, Duration::ZERO))
}

pub async fn prepare_json_store_base_update_object(
    backend: &Arc<dyn Backend + Send + Sync>,
    rows: usize,
) -> Result<JsonStoreReadFixture, LixError> {
    prepare_json_store_base_update(backend, JsonStorePayloadShape::LargeStructured128k, rows).await
}

pub async fn prepare_json_store_base_update_array(
    backend: &Arc<dyn Backend + Send + Sync>,
    rows: usize,
) -> Result<JsonStoreReadFixture, LixError> {
    prepare_json_store_base_update(backend, JsonStorePayloadShape::LargeArray128k, rows).await
}

async fn prepare_json_store_base_update(
    backend: &Arc<dyn Backend + Send + Sync>,
    shape: JsonStorePayloadShape,
    rows: usize,
) -> Result<JsonStoreReadFixture, LixError> {
    let context = JsonStoreContext::new();
    let documents = json_documents(shape, rows);
    let mut refs = Vec::with_capacity(documents.len());
    let storage = StorageContext::new(Arc::clone(backend));
    let mut transaction = storage.begin_write_transaction().await?;
    {
        let mut writer = context.writer();
        for document in documents {
            refs.push(writer.stage_bytes(&document)?);
        }
        let mut store = transaction.as_mut();
        writer.flush(&mut store).await?;
    }
    transaction.commit().await?;
    Ok(JsonStoreReadFixture {
        context,
        refs,
        paths: json_projection_paths(JsonStoreProjectionShape::TopLevelTarget),
    })
}

pub async fn json_store_write_against_base_object_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &JsonStoreReadFixture,
) -> Result<StorageBenchReport, LixError> {
    json_store_write_against_base_prepared(
        backend,
        fixture,
        JsonStorePayloadShape::LargeStructured128k,
    )
    .await
}

pub async fn json_store_write_against_base_array_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &JsonStoreReadFixture,
) -> Result<StorageBenchReport, LixError> {
    json_store_write_against_base_prepared(backend, fixture, JsonStorePayloadShape::LargeArray128k)
        .await
}

async fn json_store_write_against_base_prepared(
    backend: &Arc<dyn Backend + Send + Sync>,
    fixture: &JsonStoreReadFixture,
    shape: JsonStorePayloadShape,
) -> Result<StorageBenchReport, LixError> {
    let storage = StorageContext::new(Arc::clone(backend));
    let mut transaction = storage.begin_write_transaction().await?;
    {
        let mut writer = fixture.context.writer();
        for (index, _json_ref) in fixture.refs.iter().enumerate() {
            let updated = updated_json_document(shape, index);
            writer.stage_bytes(&updated)?;
        }
        let mut store = transaction.as_mut();
        writer.flush(&mut store).await?;
    }
    transaction.commit().await?;
    Ok(report(
        fixture.refs.len(),
        fixture.refs.len(),
        Duration::ZERO,
    ))
}

pub async fn tracked_state_write_root(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let rows = tracked_rows(config, "bench-tracked-commit");
    let context = TrackedStateContext::new();
    let started = Instant::now();
    write_tracked_root(backend, &context, "bench-tracked-commit", None, &rows).await?;
    let elapsed = started.elapsed();
    let verified_rows = scan_tracked(backend, &context, "bench-tracked-commit")
        .await?
        .len();
    Ok(report(rows.len(), verified_rows, elapsed))
}

pub async fn tracked_state_read_point_hit(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-commit");
    write_tracked_root(backend, &context, "bench-tracked-commit", None, &rows).await?;

    let started = Instant::now();
    let mut verified_rows = 0;
    let mut reader = context.reader(StorageContext::new(Arc::clone(backend)));
    for index in 0..config.rows {
        if reader
            .load_row_at_commit(
                "bench-tracked-commit",
                &TrackedStateRowRequest {
                    schema_key: tracked_schema_key(index, StorageBenchSelectivity::Percent100),
                    entity_id: EntityIdentity::single(entity_id(
                        "tracked",
                        index,
                        config.key_pattern,
                    )),
                    file_id: NullableKeyFilter::Value("bench.json".to_string()),
                },
            )
            .await?
            .is_some()
        {
            verified_rows += 1;
        }
    }
    Ok(report(config.rows, verified_rows, started.elapsed()))
}

pub async fn tracked_state_read_point_miss(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-commit");
    write_tracked_root(backend, &context, "bench-tracked-commit", None, &rows).await?;

    let started = Instant::now();
    let mut misses = 0;
    let mut reader = context.reader(StorageContext::new(Arc::clone(backend)));
    for index in 0..config.rows {
        if reader
            .load_row_at_commit(
                "bench-tracked-commit",
                &TrackedStateRowRequest {
                    schema_key: tracked_schema_key(index, StorageBenchSelectivity::Percent100),
                    entity_id: EntityIdentity::single(format!("missing-{index}")),
                    file_id: NullableKeyFilter::Value("bench.json".to_string()),
                },
            )
            .await?
            .is_none()
        {
            misses += 1;
        }
    }
    Ok(report(config.rows, misses, started.elapsed()))
}

pub async fn tracked_state_scan_all(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-commit");
    write_tracked_root(backend, &context, "bench-tracked-commit", None, &rows).await?;

    let started = Instant::now();
    let verified_rows = scan_tracked(backend, &context, "bench-tracked-commit")
        .await?
        .len();
    Ok(report(config.rows, verified_rows, started.elapsed()))
}

pub async fn tracked_state_scan_schema(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-commit");
    write_tracked_root(backend, &context, "bench-tracked-commit", None, &rows).await?;

    let started = Instant::now();
    let mut reader = context.reader(StorageContext::new(Arc::clone(backend)));
    let verified_rows = reader
        .scan_rows_at_commit(
            "bench-tracked-commit",
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    schema_keys: vec![tracked_schema_key(0, StorageBenchSelectivity::Percent100)],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await?
        .len();
    Ok(report(config.rows, verified_rows, started.elapsed()))
}

pub async fn tracked_state_scan_file(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-commit");
    write_tracked_root(backend, &context, "bench-tracked-commit", None, &rows).await?;

    let started = Instant::now();
    let mut reader = context.reader(StorageContext::new(Arc::clone(backend)));
    let verified_rows = reader
        .scan_rows_at_commit(
            "bench-tracked-commit",
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    file_ids: vec![NullableKeyFilter::Value("bench.json".to_string())],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await?
        .len();
    Ok(report(config.rows, verified_rows, started.elapsed()))
}

pub async fn tracked_state_update_existing(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = TrackedStateContext::new();
    let rows = tracked_rows(config, "bench-tracked-parent");
    write_tracked_root(backend, &context, "bench-tracked-parent", None, &rows).await?;
    let mut updated_rows = tracked_rows(config, "bench-tracked-child");
    for (index, row) in updated_rows.iter_mut().enumerate() {
        row.snapshot_content = Some(updated_snapshot_content(index, config.state_payload_bytes));
    }

    let started = Instant::now();
    write_tracked_root(
        backend,
        &context,
        "bench-tracked-child",
        Some("bench-tracked-parent"),
        &updated_rows,
    )
    .await?;
    let elapsed = started.elapsed();
    let verified_rows = scan_tracked(backend, &context, "bench-tracked-child")
        .await?
        .len();
    Ok(report(updated_rows.len(), verified_rows, elapsed))
}

pub async fn untracked_state_write_rows(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let rows = untracked_rows(config);
    let context = UntrackedStateContext::new();
    let started = Instant::now();
    write_untracked_rows(backend, &context, &rows).await?;
    let elapsed = started.elapsed();
    let verified_rows = scan_untracked(backend, &context, UntrackedStateScanRequest::default())
        .await?
        .len();
    Ok(report(rows.len(), verified_rows, elapsed))
}

pub async fn untracked_state_read_point_hit(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = UntrackedStateContext::new();
    let rows = untracked_rows(config);
    write_untracked_rows(backend, &context, &rows).await?;

    let started = Instant::now();
    let mut verified_rows = 0;
    let mut reader = context.reader(StorageContext::new(Arc::clone(backend)));
    for index in 0..config.rows {
        if reader
            .load_row(&UntrackedStateRowRequest {
                schema_key: untracked_schema_key(index, StorageBenchSelectivity::Percent100),
                version_id: "bench-version".to_string(),
                entity_id: EntityIdentity::single(entity_id(
                    "untracked",
                    index,
                    config.key_pattern,
                )),
                file_id: NullableKeyFilter::Value("bench.json".to_string()),
            })
            .await?
            .is_some()
        {
            verified_rows += 1;
        }
    }
    Ok(report(config.rows, verified_rows, started.elapsed()))
}

pub async fn untracked_state_read_point_miss(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = UntrackedStateContext::new();
    let rows = untracked_rows(config);
    write_untracked_rows(backend, &context, &rows).await?;

    let started = Instant::now();
    let mut misses = 0;
    let mut reader = context.reader(StorageContext::new(Arc::clone(backend)));
    for index in 0..config.rows {
        if reader
            .load_row(&UntrackedStateRowRequest {
                schema_key: "bench_untracked_entity".to_string(),
                version_id: "bench-version".to_string(),
                entity_id: EntityIdentity::single(format!("missing-{index}")),
                file_id: NullableKeyFilter::Value("bench.json".to_string()),
            })
            .await?
            .is_none()
        {
            misses += 1;
        }
    }
    Ok(report(config.rows, misses, started.elapsed()))
}

pub async fn untracked_state_scan_all(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = UntrackedStateContext::new();
    let rows = untracked_rows(config);
    write_untracked_rows(backend, &context, &rows).await?;

    let started = Instant::now();
    let verified_rows = scan_untracked(backend, &context, UntrackedStateScanRequest::default())
        .await?
        .len();
    Ok(report(config.rows, verified_rows, started.elapsed()))
}

pub async fn untracked_state_scan_version(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = UntrackedStateContext::new();
    let rows = untracked_rows(config);
    write_untracked_rows(backend, &context, &rows).await?;

    let started = Instant::now();
    let verified_rows = scan_untracked(
        backend,
        &context,
        UntrackedStateScanRequest {
            filter: UntrackedStateFilter {
                version_ids: vec!["bench-version".to_string()],
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await?
    .len();
    Ok(report(config.rows, verified_rows, started.elapsed()))
}

pub async fn untracked_state_scan_schema(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = UntrackedStateContext::new();
    let rows = untracked_rows(config);
    write_untracked_rows(backend, &context, &rows).await?;

    let started = Instant::now();
    let verified_rows = scan_untracked(
        backend,
        &context,
        UntrackedStateScanRequest {
            filter: UntrackedStateFilter {
                schema_keys: vec![untracked_schema_key(0, StorageBenchSelectivity::Percent100)],
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await?
    .len();
    Ok(report(config.rows, verified_rows, started.elapsed()))
}

pub async fn untracked_state_overwrite_existing(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = UntrackedStateContext::new();
    let rows = untracked_rows(config);
    write_untracked_rows(backend, &context, &rows).await?;
    let mut updated_rows = untracked_rows(config);
    for (index, row) in updated_rows.iter_mut().enumerate() {
        row.snapshot_content = Some(updated_snapshot_content(index, config.state_payload_bytes));
    }

    let started = Instant::now();
    write_untracked_rows(backend, &context, &updated_rows).await?;
    let elapsed = started.elapsed();
    let verified_rows = scan_untracked(backend, &context, UntrackedStateScanRequest::default())
        .await?
        .len();
    Ok(report(updated_rows.len(), verified_rows, elapsed))
}

pub async fn changelog_append_changes(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let changes = changelog_materialized_changes(config);
    let context = ChangelogContext::new();
    let started = Instant::now();
    append_changelog_changes(backend, &context, &changes).await?;
    let elapsed = started.elapsed();
    let reader = context.reader(StorageContext::new(Arc::clone(backend)));
    let verified_rows = reader
        .scan_changes(&ChangelogScanRequest::default())
        .await?
        .len();
    Ok(report(changes.len(), verified_rows, elapsed))
}

pub async fn changelog_load_change_hit(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = ChangelogContext::new();
    let changes = changelog_materialized_changes(config);
    append_changelog_changes(backend, &context, &changes).await?;
    let reader = context.reader(StorageContext::new(Arc::clone(backend)));

    let started = Instant::now();
    let mut verified_rows = 0;
    for index in 0..config.rows {
        if reader
            .load_change(&format!("bench-change-{index}"))
            .await?
            .is_some()
        {
            verified_rows += 1;
        }
    }
    Ok(report(config.rows, verified_rows, started.elapsed()))
}

pub async fn changelog_load_change_miss(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = ChangelogContext::new();
    let changes = changelog_materialized_changes(config);
    append_changelog_changes(backend, &context, &changes).await?;
    let reader = context.reader(StorageContext::new(Arc::clone(backend)));

    let started = Instant::now();
    let mut misses = 0;
    for index in 0..config.rows {
        if reader
            .load_change(&format!("missing-change-{index}"))
            .await?
            .is_none()
        {
            misses += 1;
        }
    }
    Ok(report(config.rows, misses, started.elapsed()))
}

pub async fn changelog_scan_all(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = ChangelogContext::new();
    let changes = changelog_materialized_changes(config);
    append_changelog_changes(backend, &context, &changes).await?;
    let reader = context.reader(StorageContext::new(Arc::clone(backend)));

    let started = Instant::now();
    let verified_rows = reader
        .scan_changes(&ChangelogScanRequest::default())
        .await?
        .len();
    Ok(report(config.rows, verified_rows, started.elapsed()))
}

pub async fn changelog_scan_limit_100(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = ChangelogContext::new();
    let changes = changelog_materialized_changes(config);
    append_changelog_changes(backend, &context, &changes).await?;
    let reader = context.reader(StorageContext::new(Arc::clone(backend)));
    let expected = config.rows.min(100);

    let started = Instant::now();
    let verified_rows = reader
        .scan_changes(&ChangelogScanRequest {
            limit: Some(expected),
        })
        .await?
        .len();
    Ok(report(expected, verified_rows, started.elapsed()))
}

pub async fn binary_cas_write_blobs(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let payloads = binary_payloads(config.rows, config.blob_bytes);
    let file_ids = binary_file_ids(config.rows);
    let writes = binary_blob_writes(&file_ids, &payloads);
    let context = BinaryCasContext::new();

    let started = Instant::now();
    write_binary_blob_writes(backend, &context, &writes).await?;
    let elapsed = started.elapsed();
    let verified_rows = count_binary_cas_manifests(backend).await?;
    Ok(report(writes.len(), verified_rows, elapsed))
}

pub async fn binary_cas_read_blob_hit(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = BinaryCasContext::new();
    let payloads = binary_payloads(config.rows, config.blob_bytes);
    let file_ids = binary_file_ids(config.rows);
    let writes = binary_blob_writes(&file_ids, &payloads);
    write_binary_blob_writes(backend, &context, &writes).await?;
    let hashes = payloads
        .iter()
        .map(|payload| crate::binary_cas::binary_blob_hash_hex(payload))
        .collect::<Vec<_>>();

    let started = Instant::now();
    let mut verified_rows = 0;
    let mut reader = context.reader(StorageContext::new(Arc::clone(backend)));
    for hash in &hashes {
        if reader.load_blob_data_by_hash(hash).await?.is_some() {
            verified_rows += 1;
        }
    }
    Ok(report(hashes.len(), verified_rows, started.elapsed()))
}

pub async fn binary_cas_read_blob_miss(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let context = BinaryCasContext::new();
    let payloads = binary_payloads(config.rows, config.blob_bytes);
    let file_ids = binary_file_ids(config.rows);
    let writes = binary_blob_writes(&file_ids, &payloads);
    write_binary_blob_writes(backend, &context, &writes).await?;

    let started = Instant::now();
    let mut misses = 0;
    let mut reader = context.reader(StorageContext::new(Arc::clone(backend)));
    for index in 0..config.rows {
        let missing_hash = format!("{index:064x}");
        if reader
            .load_blob_data_by_hash(&missing_hash)
            .await?
            .is_none()
        {
            misses += 1;
        }
    }
    Ok(report(config.rows, misses, started.elapsed()))
}

pub async fn binary_cas_write_duplicate_payload(
    backend: &Arc<dyn Backend + Send + Sync>,
    config: StorageBenchConfig,
) -> Result<StorageBenchReport, LixError> {
    let payload = binary_payload(0, config.blob_bytes);
    let payloads = (0..config.rows)
        .map(|_| payload.clone())
        .collect::<Vec<_>>();
    let file_ids = binary_file_ids(config.rows);
    let writes = binary_blob_writes(&file_ids, &payloads);
    let context = BinaryCasContext::new();

    let started = Instant::now();
    write_binary_blob_writes(backend, &context, &writes).await?;
    let elapsed = started.elapsed();
    let verified_rows = count_binary_cas_manifests(backend).await?;
    Ok(report(writes.len(), verified_rows, elapsed))
}

async fn write_tracked_root(
    backend: &Arc<dyn Backend + Send + Sync>,
    context: &TrackedStateContext,
    commit_id: &str,
    parent_commit_id: Option<&str>,
    rows: &[TrackedStateRow],
) -> Result<(), LixError> {
    let storage = StorageContext::new(Arc::clone(backend));
    let mut transaction = storage.begin_write_transaction().await?;
    {
        let mut writer = context.writer(transaction.as_mut());
        writer.write_root(commit_id, parent_commit_id, rows).await?;
    }
    transaction.commit().await
}

async fn scan_tracked(
    backend: &Arc<dyn Backend + Send + Sync>,
    context: &TrackedStateContext,
    commit_id: &str,
) -> Result<Vec<TrackedStateRow>, LixError> {
    let mut reader = context.reader(StorageContext::new(Arc::clone(backend)));
    reader
        .scan_rows_at_commit(commit_id, &TrackedStateScanRequest::default())
        .await
}

async fn write_untracked_rows(
    backend: &Arc<dyn Backend + Send + Sync>,
    context: &UntrackedStateContext,
    rows: &[UntrackedStateRow],
) -> Result<(), LixError> {
    let storage = StorageContext::new(Arc::clone(backend));
    let mut transaction = storage.begin_write_transaction().await?;
    {
        let mut writer = context.writer(transaction.as_mut());
        writer.write_rows(rows).await?;
    }
    transaction.commit().await
}

async fn scan_untracked(
    backend: &Arc<dyn Backend + Send + Sync>,
    context: &UntrackedStateContext,
    request: UntrackedStateScanRequest,
) -> Result<Vec<UntrackedStateRow>, LixError> {
    let mut reader = context.reader(StorageContext::new(Arc::clone(backend)));
    reader.scan_rows(&request).await
}

async fn append_changelog_changes(
    backend: &Arc<dyn Backend + Send + Sync>,
    context: &ChangelogContext,
    changes: &[MaterializedCanonicalChange],
) -> Result<(), LixError> {
    let storage = StorageContext::new(Arc::clone(backend));
    let mut transaction = storage.begin_write_transaction().await?;
    {
        let mut json_writer = JsonStoreContext::new().writer();
        let canonical_changes = changes
            .iter()
            .map(|change| canonicalize_materialized_change(&mut json_writer, change))
            .collect::<Result<Vec<_>, _>>()?;
        json_writer.flush(&mut transaction.as_mut()).await?;
        let mut writer = context.writer(transaction.as_mut());
        writer.append_changes(&canonical_changes).await?;
    }
    transaction.commit().await
}

async fn write_binary_blob_writes(
    backend: &Arc<dyn Backend + Send + Sync>,
    context: &BinaryCasContext,
    writes: &[BinaryBlobWrite<'_>],
) -> Result<(), LixError> {
    let storage = StorageContext::new(Arc::clone(backend));
    let mut transaction = storage.begin_write_transaction().await?;
    {
        let mut writer = context.writer(transaction.as_mut());
        writer.put_blob_writes(writes).await?;
    }
    transaction.commit().await
}

async fn count_binary_cas_manifests(
    backend: &Arc<dyn Backend + Send + Sync>,
) -> Result<usize, LixError> {
    let context = BinaryCasContext::new();
    let mut reader = context.reader(StorageContext::new(Arc::clone(backend)));
    reader.count_blob_manifests().await
}

fn report(measured_rows: usize, verified_rows: usize, elapsed: Duration) -> StorageBenchReport {
    StorageBenchReport {
        measured_rows,
        verified_rows,
        elapsed,
    }
}

const TRACKED_MATCH_SCHEMA_KEY: &str = "bench_tracked_entity";
const TRACKED_OTHER_SCHEMA_KEY: &str = "bench_tracked_other_entity";
const UNTRACKED_MATCH_SCHEMA_KEY: &str = "bench_untracked_entity";
const UNTRACKED_OTHER_SCHEMA_KEY: &str = "bench_untracked_other_entity";
const CHANGELOG_MATCH_SCHEMA_KEY: &str = "bench_changelog_entity";
const CHANGELOG_OTHER_SCHEMA_KEY: &str = "bench_changelog_other_entity";
const CHANGELOG_HISTORY_ENTITY_ID: &str = "change-entity-history-target";

fn tracked_rows(config: StorageBenchConfig, commit_id: &str) -> Vec<TrackedStateRow> {
    (0..config.rows)
        .map(|index| TrackedStateRow {
            entity_id: EntityIdentity::single(entity_id("tracked", index, config.key_pattern)),
            schema_key: tracked_schema_key(index, config.selectivity),
            file_id: Some("bench.json".to_string()),
            snapshot_content: Some(snapshot_content(index, config.state_payload_bytes)),
            metadata: None,
            origin: None,
            schema_version: "1".to_string(),
            created_at: timestamp(index),
            updated_at: timestamp(index),
            change_id: format!("tracked-change-{index}"),
            commit_id: commit_id.to_string(),
        })
        .collect()
}

fn tracked_rows_file_selective(
    config: StorageBenchConfig,
    commit_id: &str,
) -> Vec<TrackedStateRow> {
    (0..config.rows)
        .map(|index| TrackedStateRow {
            entity_id: EntityIdentity::single(entity_id("tracked", index, config.key_pattern)),
            schema_key: TRACKED_MATCH_SCHEMA_KEY.to_string(),
            file_id: Some(
                if config.selectivity.matches(index) {
                    "bench-match.json"
                } else {
                    "bench-other.json"
                }
                .to_string(),
            ),
            snapshot_content: Some(snapshot_content(index, config.state_payload_bytes)),
            metadata: None,
            origin: None,
            schema_version: "1".to_string(),
            created_at: timestamp(index),
            updated_at: timestamp(index),
            change_id: format!("tracked-change-{index}"),
            commit_id: commit_id.to_string(),
        })
        .collect()
}

fn untracked_rows(config: StorageBenchConfig) -> Vec<UntrackedStateRow> {
    (0..config.rows)
        .map(|index| UntrackedStateRow {
            entity_id: EntityIdentity::single(entity_id("untracked", index, config.key_pattern)),
            schema_key: untracked_schema_key(index, config.selectivity),
            file_id: Some("bench.json".to_string()),
            snapshot_content: Some(snapshot_content(index, config.state_payload_bytes)),
            metadata: None,
            origin: None,
            schema_version: "1".to_string(),
            created_at: timestamp(index),
            updated_at: timestamp(index),
            global: false,
            version_id: "bench-version".to_string(),
        })
        .collect()
}

fn changelog_changes(config: StorageBenchConfig) -> Vec<CanonicalChange> {
    changelog_materialized_changes(config)
        .into_iter()
        .map(canonical_changelog_bench_change)
        .collect()
}

fn changelog_materialized_changes(config: StorageBenchConfig) -> Vec<MaterializedCanonicalChange> {
    (0..config.rows)
        .map(|index| MaterializedCanonicalChange {
            id: format!("bench-change-{index}"),
            entity_id: EntityIdentity::single(entity_id(
                "change-entity",
                index,
                config.key_pattern,
            )),
            schema_key: "bench_changelog_entity".to_string(),
            schema_version: "1".to_string(),
            file_id: Some("bench.json".to_string()),
            snapshot_content: Some(snapshot_content(index, config.state_payload_bytes)),
            metadata: None,
            created_at: timestamp(index),
        })
        .collect()
}

fn canonical_changelog_bench_change(change: MaterializedCanonicalChange) -> CanonicalChange {
    let snapshot_ref = change
        .snapshot_content
        .as_ref()
        .map(|value| JsonRef::from_hash(blake3::hash(value.as_bytes())));
    let metadata_ref = change
        .metadata
        .as_ref()
        .map(|value| JsonRef::from_hash(blake3::hash(value.as_bytes())));
    CanonicalChange {
        id: change.id,
        entity_id: change.entity_id,
        schema_key: change.schema_key,
        schema_version: change.schema_version,
        file_id: change.file_id,
        snapshot_ref,
        metadata_ref,
        created_at: change.created_at,
    }
}

fn changelog_tombstone_changes(config: StorageBenchConfig) -> Vec<MaterializedCanonicalChange> {
    changelog_materialized_changes(config)
        .into_iter()
        .map(|mut change| {
            change.snapshot_content = None;
            change.metadata = None;
            change
        })
        .collect()
}

fn changelog_metadata_changes(config: StorageBenchConfig) -> Vec<MaterializedCanonicalChange> {
    changelog_materialized_changes(config)
        .into_iter()
        .enumerate()
        .map(|(index, mut change)| {
            change.metadata = Some(snapshot_content(index, config.state_payload_bytes));
            change
        })
        .collect()
}

fn changelog_shared_payload_changes(
    config: StorageBenchConfig,
) -> Vec<MaterializedCanonicalChange> {
    let shared_snapshot_content = snapshot_content(0, config.state_payload_bytes);
    changelog_materialized_changes(config)
        .into_iter()
        .map(|mut change| {
            change.snapshot_content = Some(shared_snapshot_content.clone());
            change
        })
        .collect()
}

fn changelog_shared_metadata_changes(
    config: StorageBenchConfig,
) -> Vec<MaterializedCanonicalChange> {
    let shared_metadata = snapshot_content(0, config.state_payload_bytes);
    changelog_materialized_changes(config)
        .into_iter()
        .map(|mut change| {
            change.snapshot_content = None;
            change.metadata = Some(shared_metadata.clone());
            change
        })
        .collect()
}

fn changelog_shared_payload_and_metadata_changes(
    config: StorageBenchConfig,
) -> Vec<MaterializedCanonicalChange> {
    let shared_snapshot_content = snapshot_content(0, config.state_payload_bytes);
    let shared_metadata = snapshot_content(1, config.state_payload_bytes);
    changelog_materialized_changes(config)
        .into_iter()
        .map(|mut change| {
            change.snapshot_content = Some(shared_snapshot_content.clone());
            change.metadata = Some(shared_metadata.clone());
            change
        })
        .collect()
}

fn changelog_composite_entity_id_changes(
    config: StorageBenchConfig,
) -> Vec<MaterializedCanonicalChange> {
    changelog_materialized_changes(config)
        .into_iter()
        .enumerate()
        .map(|(index, mut change)| {
            change.entity_id = EntityIdentity {
                parts: vec![
                    EntityIdentityPart::String(entity_id(
                        "change-composite",
                        index,
                        config.key_pattern,
                    )),
                    EntityIdentityPart::Number(index.to_string()),
                    EntityIdentityPart::Bool(index % 2 == 0),
                ],
            };
            change
        })
        .collect()
}

fn changelog_selective_changes(config: StorageBenchConfig) -> Vec<MaterializedCanonicalChange> {
    changelog_materialized_changes(config)
        .into_iter()
        .enumerate()
        .map(|(index, mut change)| {
            change.schema_key = changelog_schema_key(index, config.selectivity);
            change
        })
        .collect()
}

fn changelog_entity_history_changes(
    config: StorageBenchConfig,
) -> Vec<MaterializedCanonicalChange> {
    changelog_materialized_changes(config)
        .into_iter()
        .enumerate()
        .map(|(index, mut change)| {
            if index % 10 == 0 {
                change.entity_id = EntityIdentity::single(CHANGELOG_HISTORY_ENTITY_ID);
            }
            change
        })
        .collect()
}

fn changelog_commit_fact_changes(config: StorageBenchConfig) -> Vec<MaterializedCanonicalChange> {
    changelog_materialized_changes(config)
        .into_iter()
        .enumerate()
        .map(|(index, mut change)| {
            if index % 10 == 0 {
                change.schema_key = "lix_commit".to_string();
                change.entity_id = EntityIdentity::single(format!("bench-commit-{index}"));
                change.snapshot_content = Some(format!(
                    "{{\"id\":\"bench-commit-{index}\",\"parent_ids\":[]}}"
                ));
            }
            change
        })
        .collect()
}

fn tracked_schema_key(index: usize, selectivity: StorageBenchSelectivity) -> String {
    if selectivity.matches(index) {
        TRACKED_MATCH_SCHEMA_KEY
    } else {
        TRACKED_OTHER_SCHEMA_KEY
    }
    .to_string()
}

fn untracked_schema_key(index: usize, selectivity: StorageBenchSelectivity) -> String {
    if selectivity.matches(index) {
        UNTRACKED_MATCH_SCHEMA_KEY
    } else {
        UNTRACKED_OTHER_SCHEMA_KEY
    }
    .to_string()
}

fn changelog_schema_key(index: usize, selectivity: StorageBenchSelectivity) -> String {
    if selectivity.matches(index) {
        CHANGELOG_MATCH_SCHEMA_KEY
    } else {
        CHANGELOG_OTHER_SCHEMA_KEY
    }
    .to_string()
}

fn entity_id(prefix: &str, index: usize, key_pattern: StorageBenchKeyPattern) -> String {
    match key_pattern {
        StorageBenchKeyPattern::Sequential => format!("{prefix}-{index}"),
        StorageBenchKeyPattern::Random => format!("{prefix}-{:016x}", randomish_index(index)),
    }
}

fn randomish_index(index: usize) -> u64 {
    let mut value = index as u64;
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn binary_file_ids(rows: usize) -> Vec<String> {
    (0..rows)
        .map(|index| format!("bench-file-{index}"))
        .collect()
}

fn binary_payloads(rows: usize, blob_bytes: usize) -> Vec<Vec<u8>> {
    (0..rows)
        .map(|index| binary_payload(index, blob_bytes))
        .collect()
}

fn binary_half_duplicate_payloads(rows: usize, blob_bytes: usize) -> Vec<Vec<u8>> {
    (0..rows)
        .map(|index| {
            if index % 2 == 0 {
                binary_payload(0, blob_bytes)
            } else {
                binary_payload(index, blob_bytes)
            }
        })
        .collect()
}

fn binary_blob_writes<'a>(
    file_ids: &'a [String],
    payloads: &'a [Vec<u8>],
) -> Vec<BinaryBlobWrite<'a>> {
    payloads
        .iter()
        .enumerate()
        .map(|(index, payload)| BinaryBlobWrite {
            file_id: file_ids[index].as_str(),
            version_id: "bench-version",
            data: payload.as_slice(),
        })
        .collect()
}

fn snapshot_content(index: usize, target_bytes: usize) -> String {
    let mut value = serde_json::json!({
        "id": format!("entity-{index}"),
        "value": format!("value-{index}"),
        "index": index
    });
    pad_snapshot_content(&mut value, target_bytes);
    value.to_string()
}

fn updated_snapshot_content(index: usize, target_bytes: usize) -> String {
    let mut value = serde_json::json!({
        "id": format!("entity-{index}"),
        "value": format!("updated-{index}"),
        "index": index
    });
    pad_snapshot_content(&mut value, target_bytes);
    value.to_string()
}

fn partial_updated_snapshot_content(index: usize, target_bytes: usize) -> String {
    let mut value = serde_json::json!({
        "id": format!("entity-{index}"),
        "value": format!("value-{index}"),
        "index": index,
        "done": true
    });
    pad_snapshot_content(&mut value, target_bytes);
    value.to_string()
}

fn pad_snapshot_content(value: &mut serde_json::Value, target_bytes: usize) {
    let current = value.to_string().len();
    if target_bytes <= current {
        return;
    }
    value["padding"] = serde_json::Value::String("x".repeat(target_bytes - current));
}

fn timestamp(index: usize) -> String {
    format!(
        "2026-05-01T00:{:02}:{:02}.000Z",
        (index / 60) % 60,
        index % 60
    )
}

fn binary_payload(index: usize, len: usize) -> Vec<u8> {
    let mut payload = (0..len)
        .map(|offset| {
            ((index as u64)
                .wrapping_mul(31)
                .wrapping_add((offset as u64).wrapping_mul(17))
                & 0xff) as u8
        })
        .collect::<Vec<_>>();
    for (offset, byte) in (index as u64).to_le_bytes().into_iter().enumerate() {
        if offset < payload.len() {
            payload[offset] = byte;
        }
    }
    payload
}

fn json_documents(shape: JsonStorePayloadShape, rows: usize) -> Vec<Vec<u8>> {
    (0..rows).map(|index| json_document(shape, index)).collect()
}

fn json_document(shape: JsonStorePayloadShape, index: usize) -> Vec<u8> {
    match shape {
        JsonStorePayloadShape::SmallRaw1k => json_object_document(index, 1_024, 8),
        JsonStorePayloadShape::MediumStructured16k => json_object_document(index, 16 * 1024, 128),
        JsonStorePayloadShape::LargeStructured128k => {
            json_object_document(index, 128 * 1024, 1_000)
        }
        JsonStorePayloadShape::LargeArray128k => json_array_document(index, 128 * 1024, 1_000),
    }
}

fn updated_json_document(shape: JsonStorePayloadShape, index: usize) -> Vec<u8> {
    let bytes = json_document(shape, index);
    let mut value: serde_json::Value =
        serde_json::from_slice(&bytes).expect("storage bench JSON document should parse");
    match shape {
        JsonStorePayloadShape::LargeArray128k => {
            value["items"][999]["value"] =
                serde_json::Value::String(format!("updated-array-value-{index}"));
        }
        JsonStorePayloadShape::SmallRaw1k
        | JsonStorePayloadShape::MediumStructured16k
        | JsonStorePayloadShape::LargeStructured128k => {
            value["field_999"] = serde_json::Value::String(format!("updated-object-value-{index}"));
        }
    }
    serde_json::to_vec(&value).expect("storage bench updated JSON should serialize")
}

fn json_object_document(index: usize, target_bytes: usize, fields: usize) -> Vec<u8> {
    let mut object = serde_json::Map::new();
    object.insert(
        "id".to_string(),
        serde_json::Value::String(format!("json-{index}")),
    );
    object.insert(
        "target".to_string(),
        serde_json::Value::String(format!("target-{index}")),
    );
    object.insert(
        "status".to_string(),
        serde_json::Value::String(if index % 2 == 0 { "open" } else { "closed" }.to_string()),
    );
    object.insert(
        "nested".to_string(),
        serde_json::json!({
            "target": format!("nested-target-{index}"),
            "revision": index,
        }),
    );
    for field_index in 0..fields {
        object.insert(
            format!("field_{field_index}"),
            serde_json::Value::String(format!("value-{index}-{field_index}")),
        );
    }
    pad_json_object(&mut object, target_bytes);
    serde_json::to_vec(&serde_json::Value::Object(object))
        .expect("storage bench object JSON should serialize")
}

fn json_array_document(index: usize, target_bytes: usize, items: usize) -> Vec<u8> {
    let mut object = serde_json::Map::new();
    object.insert(
        "id".to_string(),
        serde_json::Value::String(format!("json-array-{index}")),
    );
    object.insert(
        "target".to_string(),
        serde_json::Value::String(format!("target-{index}")),
    );
    object.insert(
        "status".to_string(),
        serde_json::Value::String(if index % 2 == 0 { "open" } else { "closed" }.to_string()),
    );
    object.insert(
        "items".to_string(),
        serde_json::Value::Array(
            (0..items)
                .map(|item_index| {
                    serde_json::json!({
                        "index": item_index,
                        "status": if item_index % 2 == 0 { "ready" } else { "blocked" },
                        "value": format!("item-{index}-{item_index}"),
                    })
                })
                .collect(),
        ),
    );
    pad_json_object(&mut object, target_bytes);
    serde_json::to_vec(&serde_json::Value::Object(object))
        .expect("storage bench array JSON should serialize")
}

fn pad_json_object(object: &mut serde_json::Map<String, serde_json::Value>, target_bytes: usize) {
    let current = serde_json::to_vec(&serde_json::Value::Object(object.clone()))
        .expect("storage bench JSON should serialize")
        .len();
    if target_bytes <= current {
        return;
    }
    object.insert(
        "padding".to_string(),
        serde_json::Value::String("x".repeat(target_bytes - current)),
    );
}

fn json_projection_paths(projection: JsonStoreProjectionShape) -> Vec<JsonProjectionPath> {
    match projection {
        JsonStoreProjectionShape::TopLevelTarget => vec![JsonProjectionPath::new("/target")],
        JsonStoreProjectionShape::TopLevelTenProps => (0..10)
            .map(|index| JsonProjectionPath::new(format!("/field_{index}")))
            .collect(),
        JsonStoreProjectionShape::NestedTarget => vec![JsonProjectionPath::new("/nested/target")],
        JsonStoreProjectionShape::ArrayItem999 => {
            vec![JsonProjectionPath::new("/items/999/value")]
        }
        JsonStoreProjectionShape::Status => vec![JsonProjectionPath::new("/status")],
    }
}
