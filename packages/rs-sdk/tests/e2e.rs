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
        "INSERT INTO crm_task (id, title, done) VALUES ($1, $2, $3)",
        &[
            Value::Text("task-1".to_string()),
            Value::Text("Draft RS SDK flow".to_string()),
            Value::Boolean(false),
        ],
    )
    .await
    .unwrap();

    assert_eq!(task_done(&lix, "task-1").await, false);

    let draft = lix
        .create_version(CreateVersionOptions {
            id: Some("draft-version".to_string()),
            name: "Draft".to_string(),
        })
        .await
        .unwrap();

    lix.switch_version(SwitchVersionOptions {
        version_id: draft.version_id.clone(),
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
            source_version_id: draft.version_id,
        })
        .await
        .unwrap();

    assert_eq!(merge.outcome, MergeVersionOutcome::MergeCommitted);
    assert_eq!(merge.target_version_id, main_version_id);
    assert!(merge.applied_change_count > 0);
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

async fn register_crm_task_schema(lix: &lix_rs_sdk::Lix) {
    let schema = r#"{
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "x-lix-key": "crm_task",
        "x-lix-version": "1",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "required": ["id", "title", "done"],
        "properties": {
            "id": { "type": "string" },
            "title": { "type": "string" },
            "done": { "type": "boolean" }
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
    assert_eq!(error.description, "Lix handle is closed");
    assert_eq!(
        error.hint.as_deref(),
        Some("Open a new Lix handle before calling this method.")
    );
}

type KvMap = BTreeMap<(String, Vec<u8>), Vec<u8>>;

#[derive(Clone, Default)]
struct SharedTestBackend {
    kv: Arc<Mutex<KvMap>>,
    close_count: Arc<Mutex<usize>>,
}

impl SharedTestBackend {
    fn new() -> Self {
        Self::default()
    }

    fn close_count(&self) -> Arc<Mutex<usize>> {
        Arc::clone(&self.close_count)
    }
}

#[async_trait]
impl LixBackend for SharedTestBackend {
    async fn begin_transaction(
        &self,
        mode: TransactionBeginMode,
    ) -> Result<Box<dyn LixBackendTransaction + Send + Sync + 'static>, LixError> {
        let snapshot = self
            .kv
            .lock()
            .map_err(|_| LixError::unknown("test backend lock poisoned"))?
            .clone();
        Ok(Box::new(SharedTestTransaction {
            mode,
            parent: Arc::clone(&self.kv),
            kv: snapshot,
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
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
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
