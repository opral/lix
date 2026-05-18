use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_rs_sdk::{
    open_lix, Backend, BackendKvEntryPage, BackendKvExistsBatch, BackendKvExistsGroup,
    BackendKvGetRequest, BackendKvKeyPage, BackendKvScanRange, BackendKvScanRequest,
    BackendKvValueBatch, BackendKvValueGroup, BackendKvValuePage, BackendKvWriteBatch,
    BackendKvWriteOp, BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction,
    BytePageBuilder, CreateVersionOptions, LixError, MergeVersionOptions, MergeVersionOutcome,
    OpenLixOptions, SwitchVersionOptions, Value,
};

#[tokio::test]
async fn rs_sdk_open_register_write_query_version_and_merge_flow() {
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    let main_version_id = lix.active_version_id().await.unwrap();

    register_crm_task_schema(&lix).await;

    lix.execute(
        "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
        &[
            Value::Text("task-1".to_string()),
            Value::Text("Draft RS SDK flow".to_string()),
            Value::Boolean(false),
            Value::Text(r#"{"priority":"high","tags":["sdk","json"]}"#.to_string()),
        ],
    )
    .await
    .unwrap();

    let projected = lix
        .execute(
            "SELECT title, done, meta, lixcol_snapshot_content FROM crm_task WHERE id = $1",
            &[Value::Text("task-1".to_string())],
        )
        .await
        .unwrap();
    assert_crm_task_projection(&projected);

    assert_eq!(task_done(&lix, "task-1").await, false);

    let draft = lix
        .create_version(CreateVersionOptions {
            id: Some("draft-version".to_string()),
            name: "Draft".to_string(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    assert_eq!(draft.id, "draft-version");
    assert_eq!(draft.name, "Draft");
    assert!(!draft.hidden);

    lix.switch_version(SwitchVersionOptions {
        version_id: draft.id.clone(),
    })
    .await
    .unwrap();

    lix.execute(
        "UPDATE crm_task SET done = $1 WHERE id = $2",
        &[Value::Boolean(true), Value::Text("task-1".to_string())],
    )
    .await
    .unwrap();

    assert_eq!(task_done(&lix, "task-1").await, true);

    lix.switch_version(SwitchVersionOptions {
        version_id: main_version_id.clone(),
    })
    .await
    .unwrap();

    assert_eq!(task_done(&lix, "task-1").await, false);

    let merge = lix
        .merge_version(MergeVersionOptions {
            source_version_id: draft.id,
        })
        .await
        .unwrap();

    assert_eq!(merge.outcome, MergeVersionOutcome::FastForward);
    assert_eq!(merge.target_version_id, main_version_id);
    assert_eq!(merge.change_stats.total, 1);
    assert_eq!(merge.change_stats.modified, 1);
    assert_eq!(merge.created_merge_commit_id, None);
    assert_eq!(task_done(&lix, "task-1").await, true);

    lix.close().await.unwrap();
}

#[tokio::test]
async fn rs_sdk_close_is_idempotent_and_rejects_later_operations() {
    let backend = SharedTestBackend::new();
    let close_count = backend.close_count();
    let lix = open_lix(OpenLixOptions {
        backend: Some(Box::new(backend)),
    })
    .await
    .unwrap();

    lix.close().await.unwrap();
    lix.close().await.unwrap();
    assert_eq!(
        close_count
            .lock()
            .map(|count| *count)
            .expect("close count lock should be available"),
        1
    );

    let error = lix
        .execute("SELECT value FROM lix_key_value WHERE key = 'lix_id'", &[])
        .await
        .expect_err("execute after close should fail");
    assert_closed(error);

    let error = lix
        .active_version_id()
        .await
        .expect_err("active_version_id after close should fail");
    assert_closed(error);
}

#[tokio::test]
async fn rs_sdk_close_does_not_destroy_committed_data() {
    let backend = SharedTestBackend::new();
    let first = open_lix(OpenLixOptions {
        backend: Some(Box::new(backend.clone())),
    })
    .await
    .unwrap();

    first
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('close-key', 'close-value')",
            &[],
        )
        .await
        .unwrap();
    first.close().await.unwrap();

    let error = first
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'close-key'",
            &[],
        )
        .await
        .expect_err("closed handle should not be usable");
    assert_closed(error);

    let second = open_lix(OpenLixOptions {
        backend: Some(Box::new(backend)),
    })
    .await
    .unwrap();
    let result = second
        .execute(
            "SELECT key FROM lix_key_value WHERE key = 'close-key' AND value = lix_json('\"close-value\"')",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(
        result.rows()[0].values(),
        &[Value::Text("close-key".to_string())]
    );
    second.close().await.unwrap();
}

#[tokio::test]
async fn failed_write_validation_does_not_poison_backend_transaction() {
    let backend = SharedTestBackend::rejecting_nested_transactions();
    let rollback_count = backend.rollback_count();
    let lix = open_lix(OpenLixOptions {
        backend: Some(Box::new(backend)),
    })
    .await
    .unwrap();

    register_poison_task_schema(&lix).await;

    let error = lix
        .execute(
            "INSERT INTO poison_task (id, title) VALUES ($1, $2)",
            &[
                Value::Text("bad-task".to_string()),
                Value::Text("missing meta".to_string()),
            ],
        )
        .await
        .expect_err("schema validation should reject missing required field");
    assert_eq!(error.code, "LIX_ERROR_SCHEMA_VALIDATION");

    let result = lix.execute("SELECT 1 AS ok", &[]).await.unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].values(), &[Value::Integer(1)]);
    assert!(
        *rollback_count
            .lock()
            .expect("rollback count lock should be available")
            > 0,
        "failed commit validation should rollback the backend transaction"
    );

    lix.execute(
        "INSERT INTO poison_task (id, title, meta) VALUES ($1, $2, lix_json($3))",
        &[
            Value::Text("good-task".to_string()),
            Value::Text("valid".to_string()),
            Value::Text(r#"{"priority":"high"}"#.to_string()),
        ],
    )
    .await
    .expect("valid write after failed write should succeed");

    lix.close().await.unwrap();
}

#[tokio::test]
async fn transaction_commits_multiple_statements_together() {
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    register_crm_task_schema(&lix).await;

    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
        &[
            Value::Text("tx-task-1".to_string()),
            Value::Text("First".to_string()),
            Value::Boolean(false),
            Value::Text(r#"{"batch":1}"#.to_string()),
        ],
    )
    .await
    .unwrap();
    tx.execute(
        "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
        &[
            Value::Text("tx-task-2".to_string()),
            Value::Text("Second".to_string()),
            Value::Boolean(true),
            Value::Text(r#"{"batch":1}"#.to_string()),
        ],
    )
    .await
    .unwrap();

    let staged = tx
        .execute(
            "SELECT id FROM crm_task WHERE id IN ($1, $2) ORDER BY id",
            &[
                Value::Text("tx-task-1".to_string()),
                Value::Text("tx-task-2".to_string()),
            ],
        )
        .await
        .unwrap();
    assert_eq!(staged.len(), 2);

    tx.commit().await.unwrap();

    let committed = lix
        .execute(
            "SELECT id FROM crm_task WHERE id IN ($1, $2) ORDER BY id",
            &[
                Value::Text("tx-task-1".to_string()),
                Value::Text("tx-task-2".to_string()),
            ],
        )
        .await
        .unwrap();
    assert_eq!(committed.len(), 2);
    lix.close().await.unwrap();
}

#[tokio::test]
async fn transaction_rollback_discards_staged_writes() {
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    register_crm_task_schema(&lix).await;

    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
        &[
            Value::Text("rolled-back-task".to_string()),
            Value::Text("Rollback".to_string()),
            Value::Boolean(false),
            Value::Text(r#"{"batch":1}"#.to_string()),
        ],
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();

    let result = lix
        .execute(
            "SELECT id FROM crm_task WHERE id = $1",
            &[Value::Text("rolled-back-task".to_string())],
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 0);
    lix.close().await.unwrap();
}

#[tokio::test]
async fn transaction_blocks_session_execute_on_same_handle() {
    let lix = open_lix(OpenLixOptions::default()).await.unwrap();
    register_crm_task_schema(&lix).await;

    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
        &[
            Value::Text("tx-only-task".to_string()),
            Value::Text("Inside tx".to_string()),
            Value::Boolean(false),
            Value::Text(r#"{"batch":1}"#.to_string()),
        ],
    )
    .await
    .unwrap();

    let error = lix
        .execute(
            "INSERT INTO crm_task (id, title, done, meta) VALUES ($1, $2, $3, lix_json($4))",
            &[
                Value::Text("outside-task".to_string()),
                Value::Text("Outside tx".to_string()),
                Value::Boolean(false),
                Value::Text(r#"{"batch":1}"#.to_string()),
            ],
        )
        .await
        .expect_err("session writes should be blocked while explicit transaction is active");
    assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE");

    let error = lix
        .execute("SELECT 1 AS ok", &[])
        .await
        .expect_err("session reads should be blocked while explicit transaction is active");
    assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE");

    let tx_read = tx
        .execute("SELECT 1 AS ok", &[])
        .await
        .expect("transaction reads should remain available");
    assert_eq!(tx_read.rows()[0].get::<i64>("ok").unwrap(), 1);

    tx.commit().await.unwrap();

    let committed = lix
        .execute(
            "SELECT id FROM crm_task WHERE id IN ($1, $2) ORDER BY id",
            &[
                Value::Text("outside-task".to_string()),
                Value::Text("tx-only-task".to_string()),
            ],
        )
        .await
        .unwrap();
    assert_eq!(committed.len(), 1);
    assert_eq!(
        committed.rows()[0].values(),
        &[Value::Text("tx-only-task".to_string())]
    );
    lix.close().await.unwrap();
}

async fn register_crm_task_schema(lix: &lix_rs_sdk::Lix) {
    let schema = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "x-lix-key": "crm_task",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "required": ["id", "title", "done", "meta"],
        "properties": {
            "id": { "type": "string" },
            "title": { "type": "string" },
            "done": { "type": "boolean" },
            "meta": { "type": "object" }
        },
        "additionalProperties": false
    }"#;

    lix.execute(
        "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
        &[Value::Text(schema.to_string())],
    )
    .await
    .unwrap();
}

fn assert_crm_task_projection(result: &lix_rs_sdk::ExecuteResult) {
    assert_eq!(result.len(), 1);
    let row = &result.rows()[0];
    assert_eq!(
        row.get::<String>("title").unwrap(),
        "Draft RS SDK flow".to_string()
    );
    assert_eq!(row.get::<bool>("done").unwrap(), false);

    let meta = row.get::<Value>("meta").unwrap();
    let Value::Json(meta) = meta else {
        panic!("expected meta JSON value, got {meta:?}");
    };
    assert_eq!(
        meta.get("priority").and_then(|value| value.as_str()),
        Some("high")
    );
    assert_eq!(
        meta.get("tags")
            .and_then(|value| value.as_array())
            .map(|tags| tags.len()),
        Some(2)
    );

    let snapshot = row.get::<Value>("lixcol_snapshot_content").unwrap();
    let Value::Json(snapshot) = snapshot else {
        panic!("expected snapshot JSON value, got {snapshot:?}");
    };
    assert_eq!(
        snapshot.get("id").and_then(|value| value.as_str()),
        Some("task-1")
    );
    assert_eq!(
        snapshot
            .get("meta")
            .and_then(|value| value.get("priority"))
            .and_then(|value| value.as_str()),
        Some("high")
    );

    let missing = row
        .value("missing")
        .expect_err("missing column should return a structured error");
    assert_eq!(missing.code, "LIX_COLUMN_NOT_FOUND");
}

async fn register_poison_task_schema(lix: &lix_rs_sdk::Lix) {
    let schema = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "x-lix-key": "poison_task",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "required": ["id", "title", "meta"],
        "properties": {
            "id": { "type": "string" },
            "title": { "type": "string" },
            "meta": { "type": "object" }
        },
        "additionalProperties": false
    }"#;

    lix.execute(
        "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
        &[Value::Text(schema.to_string())],
    )
    .await
    .unwrap();
}

async fn task_done(lix: &lix_rs_sdk::Lix, task_id: &str) -> bool {
    let result = lix
        .execute(
            "SELECT done FROM crm_task WHERE id = $1",
            &[Value::Text(task_id.to_string())],
        )
        .await
        .unwrap();

    let rows = result;
    assert_eq!(rows.len(), 1);

    match rows.rows()[0].values().first() {
        Some(Value::Boolean(done)) => *done,
        value => panic!("expected boolean done value, got {value:?}"),
    }
}

fn assert_closed(error: LixError) {
    assert_eq!(error.code, LixError::CODE_CLOSED);
}

type KvMap = BTreeMap<(String, Vec<u8>), Vec<u8>>;

#[derive(Clone, Default)]
struct SharedTestBackend {
    kv: Arc<Mutex<KvMap>>,
    close_count: Arc<Mutex<usize>>,
    rollback_count: Arc<Mutex<usize>>,
    active_transaction: Arc<Mutex<bool>>,
    reject_nested_transactions: bool,
}

impl SharedTestBackend {
    fn new() -> Self {
        Self::default()
    }

    fn rejecting_nested_transactions() -> Self {
        Self {
            reject_nested_transactions: true,
            ..Self::default()
        }
    }

    fn close_count(&self) -> Arc<Mutex<usize>> {
        Arc::clone(&self.close_count)
    }

    fn rollback_count(&self) -> Arc<Mutex<usize>> {
        Arc::clone(&self.rollback_count)
    }

    fn begin_test_transaction(&self) -> Result<SharedTestTransaction, LixError> {
        let mut active_transaction = self
            .active_transaction
            .lock()
            .map_err(|_| LixError::unknown("test backend active transaction lock poisoned"))?;
        if *active_transaction && self.reject_nested_transactions {
            return Err(LixError::unknown(
                "cannot open nested Lix backend transaction",
            ));
        }
        *active_transaction = true;
        drop(active_transaction);

        let snapshot = self
            .kv
            .lock()
            .map_err(|_| LixError::unknown("test backend lock poisoned"))?
            .clone();
        Ok(SharedTestTransaction {
            parent: Arc::clone(&self.kv),
            kv: snapshot,
            active_transaction: Arc::clone(&self.active_transaction),
            rollback_count: Arc::clone(&self.rollback_count),
        })
    }
}

#[async_trait]
impl Backend for SharedTestBackend {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        Ok(Box::new(self.begin_test_transaction()?))
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        Ok(Box::new(self.begin_test_transaction()?))
    }

    async fn close(&self) -> Result<(), LixError> {
        *self
            .close_count
            .lock()
            .map_err(|_| LixError::unknown("test backend close count lock poisoned"))? += 1;
        Ok(())
    }
}

struct SharedTestTransaction {
    parent: Arc<Mutex<KvMap>>,
    kv: KvMap,
    active_transaction: Arc<Mutex<bool>>,
    rollback_count: Arc<Mutex<usize>>,
}

#[async_trait]
impl BackendReadTransaction for SharedTestTransaction {
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
        *self
            .rollback_count
            .lock()
            .map_err(|_| LixError::unknown("test backend rollback count lock poisoned"))? += 1;
        *self
            .active_transaction
            .lock()
            .map_err(|_| LixError::unknown("test backend active transaction lock poisoned"))? =
            false;
        Ok(())
    }
}

#[async_trait]
impl BackendWriteTransaction for SharedTestTransaction {
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
            .map_err(|_| LixError::unknown("test backend lock poisoned"))? = self.kv;
        *self
            .active_transaction
            .lock()
            .map_err(|_| LixError::unknown("test backend active transaction lock poisoned"))? =
            false;
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
