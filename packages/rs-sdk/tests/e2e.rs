use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_rs_sdk::{
    open_lix, CreateVersionOptions, KvPair, KvScanRange, LixBackend, LixBackendTransaction,
    LixError, MergeVersionOptions, MergeVersionOutcome, OpenLixOptions, SwitchVersionOptions,
    TransactionBeginMode, Value,
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
    assert_eq!(merge.applied_change_count, 0);
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

async fn register_crm_task_schema(lix: &lix_rs_sdk::Lix) {
    let schema = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "x-lix-key": "crm_task",
        "x-lix-version": "1",
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
        "x-lix-version": "1",
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
}

#[async_trait]
impl LixBackend for SharedTestBackend {
    async fn begin_transaction(
        &self,
        mode: TransactionBeginMode,
    ) -> Result<Box<dyn LixBackendTransaction + Send + Sync + 'static>, LixError> {
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
        Ok(Box::new(SharedTestTransaction {
            mode,
            parent: Arc::clone(&self.kv),
            kv: snapshot,
            active_transaction: Arc::clone(&self.active_transaction),
            rollback_count: Arc::clone(&self.rollback_count),
        }))
    }

    async fn kv_get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        Ok(self
            .kv
            .lock()
            .map_err(|_| LixError::unknown("test backend lock poisoned"))?
            .get(&(namespace.to_string(), key.to_vec()))
            .cloned())
    }

    async fn kv_scan(
        &self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        let guard = self
            .kv
            .lock()
            .map_err(|_| LixError::unknown("test backend lock poisoned"))?;
        Ok(scan_map(&guard, namespace, &range, limit))
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
    mode: TransactionBeginMode,
    parent: Arc<Mutex<KvMap>>,
    kv: KvMap,
    active_transaction: Arc<Mutex<bool>>,
    rollback_count: Arc<Mutex<usize>>,
}

#[async_trait]
impl LixBackendTransaction for SharedTestTransaction {
    fn mode(&self) -> TransactionBeginMode {
        self.mode
    }

    async fn kv_get(&mut self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        Ok(self.kv.get(&(namespace.to_string(), key.to_vec())).cloned())
    }

    async fn kv_scan(
        &mut self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        Ok(scan_map(&self.kv, namespace, &range, limit))
    }

    async fn kv_put(&mut self, namespace: &str, key: &[u8], value: &[u8]) -> Result<(), LixError> {
        self.kv
            .insert((namespace.to_string(), key.to_vec()), value.to_vec());
        Ok(())
    }

    async fn kv_delete(&mut self, namespace: &str, key: &[u8]) -> Result<(), LixError> {
        self.kv.remove(&(namespace.to_string(), key.to_vec()));
        Ok(())
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

fn scan_map(kv: &KvMap, namespace: &str, range: &KvScanRange, limit: Option<usize>) -> Vec<KvPair> {
    let mut pairs = kv
        .iter()
        .filter(|((candidate_namespace, key), _)| {
            candidate_namespace == namespace && key_matches_range(key, range)
        })
        .map(|((_, key), value)| KvPair::new(key.clone(), value.clone()))
        .collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.key.cmp(&right.key));
    if let Some(limit) = limit {
        pairs.truncate(limit);
    }
    pairs
}

fn key_matches_range(key: &[u8], range: &KvScanRange) -> bool {
    match range {
        KvScanRange::Prefix(prefix) => key.starts_with(prefix),
        KvScanRange::Range { start, end } => start.as_slice() <= key && key < end.as_slice(),
    }
}
