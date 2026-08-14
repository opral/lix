use std::path::{Path, PathBuf};

use async_trait::async_trait;
use lix::storage::Storage;
use lix::storage_adapter::StorageAdapter;
use lix::storage_bench::{NativeRowDurableCounters, native_row_durable_counters};
use lix::transaction::bench::BenchTransactionFixture;
use lix::{Value, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

const ID: &str = "018f8b7c-2d91-7b4d-8c41-5dbacb2fca21";
const WRONG_ID: &str = "018f8b7c-2d91-7b4d-8c41-5dbacb2fca22";

#[async_trait]
trait DurableFixture: Storage + Clone + Send + Sync + 'static {
    fn open_at(path: &Path) -> Self;
    async fn flush_all(&self);
    fn label() -> &'static str;
}

#[async_trait]
impl DurableFixture for RocksDB {
    fn open_at(path: &Path) -> Self {
        Self::open(path).expect("open RocksDB native-row fixture")
    }

    async fn flush_all(&self) {
        self.flush().expect("flush RocksDB native-row fixture");
    }

    fn label() -> &'static str {
        "rocksdb"
    }
}

#[async_trait]
impl DurableFixture for SlateDB {
    fn open_at(path: &Path) -> Self {
        Self::open(path).expect("open SlateDB native-row fixture")
    }

    async fn flush_all(&self) {
        self.flush()
            .await
            .expect("flush SlateDB native-row fixture");
        self.flush_memtable_for_diagnostics()
            .await
            .expect("flush SlateDB native-row memtable");
    }

    fn label() -> &'static str {
        "slatedb"
    }
}

fn delta(
    after: NativeRowDurableCounters,
    before: NativeRowDurableCounters,
) -> NativeRowDurableCounters {
    NativeRowDurableCounters {
        native_writes: after.native_writes - before.native_writes,
        native_reads: after.native_reads - before.native_reads,
        native_write_bytes: after.native_write_bytes - before.native_write_bytes,
        native_read_bytes: after.native_read_bytes - before.native_read_bytes,
        whole_row_json_writes: after.whole_row_json_writes - before.whole_row_json_writes,
        whole_row_json_reads: after.whole_row_json_reads - before.whole_row_json_reads,
        whole_row_json_write_bytes: after.whole_row_json_write_bytes
            - before.whole_row_json_write_bytes,
        whole_row_json_read_bytes: after.whole_row_json_read_bytes
            - before.whole_row_json_read_bytes,
    }
}

async fn run_backend<S: DurableFixture>(path: PathBuf) {
    let storage = S::open_at(&path);
    let schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "native_carrier_probe",
        "columns": [
            {"name": "id", "type": "uuid", "nullable": false, "default_expression": "uuidv7()"},
            {"name": "label", "type": "text", "nullable": false},
            {"name": "count", "type": "int8", "nullable": false},
            {"name": "ratio", "type": "float8", "nullable": false},
            {"name": "active", "type": "boolean", "nullable": false},
            {"name": "created_at", "type": "timestamptz", "nullable": false, "default_expression": "CURRENT_TIMESTAMP"}
        ],
        "primary_key": ["id"]
    });
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("initialize public native-row repository");
    let registration = lix
        .open_another_session()
        .await
        .expect("open public registration session");
    registration
        .execute(
            "INSERT INTO lix_registered_schema (schema_key, value) VALUES ($1, CAST($2 AS JSONB))",
            &[
                Value::Text("native_carrier_probe".into()),
                Value::Text(schema.to_string()),
            ],
        )
        .await
        .expect("register public native-row schema");
    drop(registration);
    let session = lix
        .open_another_session()
        .await
        .expect("open public native-row session");
    let branch_id = session.active_branch_id().await.expect("active branch");
    let before = native_row_durable_counters();
    session
        .execute(
            "INSERT INTO native_carrier_probe (id, label, count, ratio, active) VALUES ($1, 'ready', 42, 1.5, true)",
            &[Value::Text(ID.into())],
        )
        .await
        .expect("insert public native row");
    let after_insert = native_row_durable_counters();
    let selected = session
        .execute(
            "SELECT id, label, count, ratio, active, created_at FROM native_carrier_probe WHERE id = $1",
            &[Value::Text(ID.into())],
        )
        .await
        .expect("select public native row");
    assert_eq!(selected.len(), 1);
    assert!(
        BenchTransactionFixture::<S>::reopen_native_row_on_branch(
            StorageAdapter::new(storage.clone()),
            &branch_id,
            "native_carrier_probe",
            ID,
        )
        .await
        .expect("read retained native scalar tuple"),
        "retained read did not expose the native scalar tuple"
    );
    let native_value = BenchTransactionFixture::<S>::native_uuid_physical_value_on_branch(
        StorageAdapter::new(storage.clone()),
        &branch_id,
        "native_carrier_probe",
        ID,
    )
    .await;
    let magic_offset = native_value
        .windows(b"LIXROW01".len())
        .position(|window| window == b"LIXROW01")
        .expect("exact StateKey value has no native-row envelope");
    let body = &native_value[magic_offset + b"LIXROW01".len() + 32 + 32..];
    let pk_bytes = uuid::Uuid::parse_str(ID).expect("canonical probe UUID");
    assert!(
        body.windows(pk_bytes.as_bytes().len())
            .all(|window| window != pk_bytes.as_bytes()),
        "typed primary key was duplicated in the non-PK tuple body"
    );
    let native_value_bytes = native_value.len();
    let after_retained_read = native_row_durable_counters();
    let insert = delta(after_insert, before);
    let retained_read = delta(after_retained_read, after_insert);
    let live = delta(after_retained_read, before);
    eprintln!(
        "backend={} insert={:?} retained_read={:?}",
        S::label(),
        insert,
        retained_read
    );
    assert!(
        live.native_writes > 0,
        "native row did not reach durable encoder"
    );
    assert!(
        live.native_reads > 0,
        "native row did not reach retained-read decoder"
    );
    assert_eq!(insert.whole_row_json_writes, 0);
    assert_eq!(insert.whole_row_json_write_bytes, 0);
    assert_eq!(retained_read.whole_row_json_reads, 0);
    assert_eq!(retained_read.whole_row_json_read_bytes, 0);

    drop(session);
    drop(lix);
    storage.flush_all().await;
    drop(storage);

    let reopened_storage = S::open_at(&path);
    let before_reopen = native_row_durable_counters();
    assert!(
        BenchTransactionFixture::<S>::reopen_native_row_on_branch(
            StorageAdapter::new(reopened_storage.clone()),
            &branch_id,
            "native_carrier_probe",
            ID,
        )
        .await
        .expect("read cold native scalar tuple"),
        "cold reopen did not expose the native scalar tuple"
    );
    let cold = delta(native_row_durable_counters(), before_reopen);
    assert!(
        cold.native_reads > 0,
        "cold reopen bypassed native row decoder"
    );
    assert_eq!(cold.whole_row_json_reads, 0);
    assert_eq!(cold.whole_row_json_read_bytes, 0);
    BenchTransactionFixture::<S>::substitute_native_uuid_owner_on_branch(
        StorageAdapter::new(reopened_storage.clone()),
        &branch_id,
        "native_carrier_probe",
        ID,
        WRONG_ID,
    )
    .await;
    let substitution = BenchTransactionFixture::<S>::reopen_native_row_on_branch(
        StorageAdapter::new(reopened_storage.clone()),
        &branch_id,
        "native_carrier_probe",
        ID,
    )
    .await
    .expect_err("same-size wrong-owner tuple must fail closed");
    assert!(
        substitution.message.contains("authenticated state key"),
        "unexpected substitution diagnostic: {substitution:?}"
    );
    reopened_storage.flush_all().await;

    println!(
        "backend={} live={live:?} cold={cold:?} native_values=1 native_value_bytes={} duplicated_pk_bytes=0 substitution={}",
        S::label(),
        native_value_bytes,
        substitution.message
    );
}

fn main() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build native-row runtime")
        .block_on(async {
            let rocks = tempfile::tempdir().expect("create RocksDB directory");
            run_backend::<RocksDB>(rocks.path().join("db")).await;
            let slate = tempfile::tempdir().expect("create SlateDB directory");
            run_backend::<SlateDB>(slate.path().join("db")).await;
        });
}
