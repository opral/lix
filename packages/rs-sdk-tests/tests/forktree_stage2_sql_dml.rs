//! Public SQL acceptance oracle for the ForkTree Stage2 physical-owner cut.
//!
//! This file intentionally uses only the public/session SQL owners. On the
//! frozen `a12b76c8` baseline it is compile-red at the two SPI symbols below;
//! Stage2 makes it runnable by wiring the closed, cfg-only physical owner.

use async_trait::async_trait;
use lix::integration::AcceptancePhysicalLayout;
use lix::storage::Storage;
use lix::{ExecuteBatchStatement, ExecuteResult, Lix, LixError, Value, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use serde_json::{Value as JsonValue, json};
use sha2::{Digest as _, Sha256};
use std::path::Path;

const EXPECTED_RESULT_DIGEST: &str =
    "8ab75635b3ab498f7d77b1552fb0ec923dd661fdf655cd24cc66e0405f0ea6e1";
const EXPECTED_FINAL_DIGEST: &str =
    "3ad9161a21a253c6985b16628d482c016e2f786a19babd9211a1d1a790e8f4b1";

#[derive(Debug, Clone, PartialEq, Eq)]
struct OracleArtifact {
    result_digest: String,
    final_digest: String,
}

fn result_record(result: &ExecuteResult) -> JsonValue {
    json!({
        "statementIndex": result.statement_index(),
        "label": result.label(),
        "rowsAffected": result.rows_affected(),
        "columns": result.columns(),
        "rows": result
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>(),
    })
}

fn sha256_json(value: &JsonValue) -> String {
    let encoded = serde_json::to_vec(value).expect("oracle evidence must serialize");
    format!("{:x}", Sha256::digest(encoded))
}

fn error_statement_index(error: &LixError) -> Option<u64> {
    error
        .details
        .as_ref()
        .and_then(JsonValue::as_object)
        .and_then(|details| details.get("statementIndex"))
        .and_then(JsonValue::as_u64)
}

#[async_trait]
trait AcceptanceBackend {
    type Storage: Storage + Clone + Send + Sync + 'static;

    fn open(path: &Path) -> Self::Storage;
    async fn flush(storage: &Self::Storage);
}

struct RocksBackend;

#[async_trait]
impl AcceptanceBackend for RocksBackend {
    type Storage = RocksDB;

    fn open(path: &Path) -> Self::Storage {
        RocksDB::open(path.join(".lix")).expect("open Stage2 SQL RocksDB")
    }

    async fn flush(storage: &Self::Storage) {
        storage.flush().expect("flush Stage2 SQL RocksDB");
    }
}

struct SlateBackend;

#[async_trait]
impl AcceptanceBackend for SlateBackend {
    type Storage = SlateDB;

    fn open(path: &Path) -> Self::Storage {
        SlateDB::open(path.join(".lix")).expect("open Stage2 SQL SlateDB")
    }

    async fn flush(storage: &Self::Storage) {
        storage.flush().await.expect("flush Stage2 SQL SlateDB");
    }
}

async fn open_with_layout<B: AcceptanceBackend>(
    path: &Path,
    layout: AcceptancePhysicalLayout,
) -> (Lix<B::Storage>, B::Storage) {
    let storage = B::open(path);
    let lix = open_lix()
        .with_storage(storage.clone())
        .with_acceptance_physical_layout(layout)
        .await
        .expect("open fresh repository with selected physical owner");
    (lix, storage)
}

async fn register_fixture<S>(lix: &Lix<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let parent = r#"{"x-lix-key":"stage2_parent","x-lix-primary-key":["/id"],"type":"object","properties":{"id":{"type":"string"},"name":{"type":"string"}},"required":["id","name"],"additionalProperties":false}"#;
    let item = r#"{"x-lix-key":"stage2_item","x-lix-primary-key":["/tenant","/id"],"x-lix-foreign-keys":[{"properties":["/parent_id"],"references":{"schemaKey":"stage2_parent","properties":["/id"]}}],"type":"object","properties":{"tenant":{"type":"string"},"id":{"type":"string"},"parent_id":{"type":"string"},"title":{"type":"string"},"slug":{"type":"string","x-lix-default":"title + '-slug'"},"rank":{"type":"integer","x-lix-default":"7"},"note":{"type":["string","null"]}},"required":["tenant","id","parent_id","title","slug","rank"],"additionalProperties":false}"#;

    lix.execute(
        "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1)), (lix_json($2))",
        &[Value::Text(parent.to_owned()), Value::Text(item.to_owned())],
    )
    .await
    .expect("register Stage2 SQL schemas");
}

fn batch_statement(label: &str, sql: &str, params: Vec<Value>) -> ExecuteBatchStatement {
    ExecuteBatchStatement {
        label: Some(label.to_owned()),
        sql: sql.to_owned(),
        params,
    }
}

fn acceptance_batch() -> Vec<ExecuteBatchStatement> {
    vec![
        batch_statement(
            "parent",
            "INSERT INTO stage2_parent (id, name) VALUES ('p1', 'Parent One') RETURNING id, name",
            vec![],
        ),
        batch_statement(
            "parent",
            "INSERT INTO stage2_parent (id, name) VALUES ('p2', 'Parent Two') RETURNING id, name",
            vec![],
        ),
        batch_statement(
            "insert",
            "INSERT INTO stage2_item (tenant, id, parent_id, title, note) VALUES ('t1', 'i1', 'p1', 'alpha', NULL) RETURNING tenant, id, parent_id, title, slug, rank, note",
            vec![],
        ),
        batch_statement(
            "insert",
            "INSERT INTO stage2_item (tenant, id, parent_id, title, note) VALUES ('t1', 'i2', 'p1', 'beta', 'memo') RETURNING tenant, id, title, slug, rank, note",
            vec![],
        ),
        batch_statement(
            "insert",
            "INSERT INTO stage2_item (tenant, id, parent_id, title) VALUES ('t2', 'i1', 'p2', 'gamma') RETURNING tenant, id, title, slug, rank, note",
            vec![],
        ),
        batch_statement(
            "read",
            "SELECT tenant, id, title, slug, rank, note FROM stage2_item ORDER BY tenant, id",
            vec![],
        ),
        batch_statement(
            "mutate",
            "UPDATE stage2_item SET title = 'alpha-two' WHERE tenant = 't1' AND id = 'i1' RETURNING tenant, id, title, slug, rank, note",
            vec![],
        ),
        batch_statement(
            "mutate",
            "INSERT INTO stage2_item (tenant, id, parent_id, title, note) VALUES ('t1', 'i1', 'p1', 'alpha-three', NULL) ON CONFLICT (tenant, id) DO UPDATE SET title = excluded.title, note = excluded.note RETURNING tenant, id, title, slug, rank, note",
            vec![],
        ),
        batch_statement(
            "mutate",
            "INSERT INTO stage2_item (tenant, id, parent_id, title) VALUES ('t1', 'i1', 'p1', 'ignored') ON CONFLICT (tenant, id) DO NOTHING RETURNING tenant, id, title",
            vec![],
        ),
        batch_statement(
            "kv",
            "INSERT INTO lix_key_value (key, value) VALUES ('stage2-batch', 'one') RETURNING key, value",
            vec![],
        ),
        batch_statement(
            "kv",
            "INSERT INTO lix_key_value (key, value) VALUES ('stage2-batch', 'two') ON CONFLICT (key) DO UPDATE SET value = excluded.value RETURNING key, value",
            vec![],
        ),
        batch_statement(
            "read",
            "SELECT key, value FROM lix_key_value WHERE key = 'stage2-batch'",
            vec![],
        ),
        batch_statement(
            "insert",
            "INSERT INTO stage2_item (tenant, id, parent_id, title) VALUES ('t2', 'i2', 'p2', 'delta') RETURNING tenant, id, title, slug, rank, note",
            vec![],
        ),
        batch_statement(
            "mutate",
            "UPDATE stage2_item SET note = 'updated-note' WHERE tenant = 't2' AND id = 'i2' RETURNING tenant, id, note",
            vec![],
        ),
        batch_statement(
            "mutate",
            "DELETE FROM stage2_item WHERE tenant = 't1' AND id = 'i2' RETURNING tenant, id, title",
            vec![],
        ),
        batch_statement(
            "read",
            "SELECT tenant, id, title FROM stage2_item WHERE tenant = 't1' ORDER BY id",
            vec![],
        ),
        batch_statement(
            "kv",
            "INSERT INTO lix_key_value (key, value) VALUES ('stage2-extra', lix_json('{\"n\":3}')) RETURNING key, value",
            vec![],
        ),
        batch_statement(
            "read",
            "SELECT tenant, COUNT(*) AS count FROM stage2_item GROUP BY tenant ORDER BY tenant",
            vec![],
        ),
    ]
}

async fn exact_query<S>(lix: &Lix<S>, sql: &str) -> JsonValue
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = lix.execute(sql, &[]).await.expect("execute exact query");
    result_record(&result)
}

async fn run_trace<B: AcceptanceBackend>(layout: AcceptancePhysicalLayout) -> OracleArtifact {
    let directory = tempfile::tempdir().expect("create Stage2 SQL oracle directory");
    let (lix, storage) = open_with_layout::<B>(directory.path(), layout).await;
    register_fixture(&lix).await;

    let statements = acceptance_batch();
    assert_eq!(statements.len(), 18);
    let labels = statements
        .iter()
        .map(|statement| statement.label.clone())
        .collect::<Vec<_>>();
    let batch = lix
        .execute_batch(&statements)
        .await
        .expect("execute 18-statement acceptance batch");
    assert_eq!(batch.len(), statements.len());
    for (index, result) in batch.iter().enumerate() {
        assert_eq!(result.statement_index(), Some(index));
        assert_eq!(result.label(), labels[index].as_deref());
    }
    assert_eq!(
        batch[8].rows_affected(),
        0,
        "DO NOTHING must omit RETURNING"
    );
    assert!(batch[8].is_empty());
    assert_eq!(
        batch[2].rows()[0].get::<String>("slug").unwrap(),
        "alpha-slug"
    );
    assert_eq!(batch[2].rows()[0].get::<i64>("rank").unwrap(), 7);
    assert_eq!(batch[2].rows()[0].value("note").unwrap(), &Value::Null);

    let duplicate_error = lix
        .execute(
            "INSERT INTO stage2_item (tenant, id, parent_id, title) VALUES ('t1', 'i1', 'p1', 'duplicate')",
            &[],
        )
        .await
        .expect_err("composite primary-key duplicate must fail");
    assert_eq!(duplicate_error.code, LixError::CODE_UNIQUE);

    let fk_error = lix
        .execute(
            "INSERT INTO stage2_item (tenant, id, parent_id, title) VALUES ('t9', 'missing', 'no-parent', 'invalid')",
            &[],
        )
        .await
        .expect_err("missing foreign key must fail");
    assert_eq!(fk_error.code, LixError::CODE_FOREIGN_KEY);

    let batch_error = lix
        .execute_batch(&[
            batch_statement(
                "rollback-0",
                "INSERT INTO lix_key_value (key, value) VALUES ('stage2-batch-rollback', 'first')",
                vec![],
            ),
            batch_statement(
                "rollback-1",
                "INSERT INTO lix_key_value (key, value) VALUES ('stage2-batch-rollback', 'duplicate')",
                vec![],
            ),
            batch_statement(
                "rollback-2",
                "INSERT INTO lix_key_value (key, value) VALUES ('stage2-batch-unreached', 'never')",
                vec![],
            ),
        ])
        .await
        .expect_err("middle statement must roll back the whole batch");
    assert_eq!(batch_error.code, LixError::CODE_UNIQUE);
    assert_eq!(error_statement_index(&batch_error), Some(1));
    assert!(
        lix.execute(
            "SELECT key FROM lix_key_value WHERE key IN ('stage2-batch-rollback', 'stage2-batch-unreached')",
            &[],
        )
        .await
        .unwrap()
        .is_empty(),
        "automatic batch failure must publish none of its statements",
    );

    let mut savepoint = lix
        .begin_transaction()
        .await
        .expect("begin savepoint transaction");
    savepoint
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('stage2-savepoint-before', 'before') RETURNING key, value",
            &[],
        )
        .await
        .expect("stage pre-error statement");
    let savepoint_error = savepoint
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('stage2-savepoint-bad', 'not-an-integer') RETURNING CAST(value AS BIGINT)",
            &[],
        )
        .await
        .expect_err("post-stage RETURNING conversion must fail");
    savepoint
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('stage2-savepoint-after', 'after') RETURNING key, value",
            &[],
        )
        .await
        .expect("transaction must remain usable after statement rollback");
    savepoint
        .commit()
        .await
        .expect("commit surviving savepoint statements");
    let savepoint_rows = lix
        .execute(
            "SELECT key FROM lix_key_value WHERE key LIKE 'stage2-savepoint-%' ORDER BY key",
            &[],
        )
        .await
        .expect("read statement-savepoint survivors");
    assert_eq!(savepoint_rows.len(), 2);
    assert_eq!(
        savepoint_rows
            .rows()
            .iter()
            .map(|row| row.get::<String>("key").unwrap())
            .collect::<Vec<_>>(),
        ["stage2-savepoint-after", "stage2-savepoint-before"],
    );

    let mut rollback = lix
        .begin_transaction()
        .await
        .expect("begin explicit rollback");
    rollback
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('stage2-explicit-rollback', 'no') RETURNING key",
            &[],
        )
        .await
        .expect("stage explicit rollback row");
    rollback
        .rollback()
        .await
        .expect("roll back explicit transaction");
    assert!(
        lix.execute(
            "SELECT key FROM lix_key_value WHERE key = 'stage2-explicit-rollback'",
            &[],
        )
        .await
        .unwrap()
        .is_empty(),
    );

    let winner = lix
        .open_workspace_session()
        .await
        .expect("open same-owner winner session");
    let mut stale = lix
        .begin_transaction()
        .await
        .expect("begin stale transaction");
    stale
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('stage2-stale', 'stale')",
            &[],
        )
        .await
        .expect("stage stale same-owner write");
    winner
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('stage2-stale', 'winner')",
            &[],
        )
        .await
        .expect("commit same-owner winner");
    let stale_error = stale
        .commit()
        .await
        .expect_err("same-owner stale transaction must reject");
    assert_eq!(stale_error.code, LixError::CODE_UNIQUE);
    winner.close().await.expect("close winner session");

    let left_session = lix
        .open_workspace_session()
        .await
        .expect("open unrelated left session");
    let right_session = lix
        .open_workspace_session()
        .await
        .expect("open unrelated right session");
    let mut left = left_session.begin_transaction().await.unwrap();
    let mut right = right_session.begin_transaction().await.unwrap();
    left.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('stage2-unrelated-left', 'left')",
        &[],
    )
    .await
    .expect("stage unrelated left owner");
    right
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('stage2-unrelated-right', 'right')",
            &[],
        )
        .await
        .expect("stage unrelated right owner");
    left.commit().await.expect("commit first unrelated owner");
    right
        .commit()
        .await
        .expect("commit stale but unrelated owner");
    left_session.close().await.unwrap();
    right_session.close().await.unwrap();

    let result_evidence = json!({
        "batch": batch.iter().map(result_record).collect::<Vec<_>>(),
        "errors": {
            "compositeDuplicate": duplicate_error.code,
            "foreignKey": fk_error.code,
            "batch": batch_error.code,
            "batchStatementIndex": error_statement_index(&batch_error),
            "statementSavepoint": savepoint_error.code,
            "staleSameOwner": stale_error.code,
        },
        "savepointRows": result_record(&savepoint_rows),
    });
    let result_digest = sha256_json(&result_evidence);

    B::flush(&storage).await;
    lix.close().await.expect("close Stage2 SQL workspace");
    drop(lix);
    drop(storage);

    let (reopened, reopened_storage) = open_with_layout::<B>(directory.path(), layout).await;
    let final_items = exact_query(
        &reopened,
        "SELECT tenant, id, parent_id, title, slug, rank, note FROM stage2_item ORDER BY tenant, id",
    )
    .await;
    let final_kv = exact_query(
        &reopened,
        "SELECT key, value FROM lix_key_value WHERE key LIKE 'stage2-%' ORDER BY key",
    )
    .await;
    let final_digest = sha256_json(&json!({"items": final_items, "keyValue": final_kv}));

    B::flush(&reopened_storage).await;
    reopened
        .close()
        .await
        .expect("close cold-reopened workspace");
    drop(reopened);
    drop(reopened_storage);

    OracleArtifact {
        result_digest,
        final_digest,
    }
}

async fn qualify_backend<B: AcceptanceBackend>() {
    let current = run_trace::<B>(AcceptancePhysicalLayout::Current).await;
    let forktree = run_trace::<B>(AcceptancePhysicalLayout::ForkTree).await;
    assert_eq!(
        forktree, current,
        "physical layouts changed public SQL semantics"
    );
    assert_eq!(current.result_digest, EXPECTED_RESULT_DIGEST);
    assert_eq!(current.final_digest, EXPECTED_FINAL_DIGEST);
}

#[tokio::test]
async fn forktree_stage2_sql_dml_rocksdb() {
    qualify_backend::<RocksBackend>().await;
}

#[tokio::test]
async fn forktree_stage2_sql_dml_slatedb() {
    qualify_backend::<SlateBackend>().await;
}
