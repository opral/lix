use std::time::Instant;

use lix::{Value, open_lix, storage_adapter::Storage};
use lix::storage_adapter::{StorageAdapter, StorageReadOptions};
use lix::storage_bench::layout_accounting;
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    for backend in ["rocksdb", "slatedb"] {
        let dir = tempfile::tempdir().expect("tempdir");
        match backend {
            "rocksdb" => run(RocksDB::open(dir.path()).expect("rocks"), backend).await,
            "slatedb" => run(SlateDB::open(dir.path()).expect("slate"), backend).await,
            _ => unreachable!(),
        }
    }
}

async fn run<S>(storage: S, backend: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let accounting_storage = storage.clone();
    let lix = open_lix().with_storage(storage).await.expect("open");
    let parent = serde_json::json!({
        "$schema":"https://lix.dev/schema-v1.json", "key":"cover_parent",
        "columns":[{"name":"id","type":"text","nullable":false}], "primary_key":["id"]
    });
    let child = serde_json::json!({
        "$schema":"https://lix.dev/schema-v1.json", "key":"cover_child",
        "columns":[
            {"name":"id","type":"text","nullable":false},
            {"name":"parent_id","type":"text","nullable":false},
            {"name":"score","type":"int8","nullable":false}
        ],
        "primary_key":["id"],
        "foreign_keys":[{"columns":["parent_id"],"references":{"schema_key":"cover_parent","columns":["id"]}}]
    });
    for schema in [parent, child] {
        lix.execute("INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))", &[Value::Text(schema.to_string())]).await.expect("schema");
    }
    lix.execute("INSERT INTO cover_parent (id) VALUES ('p0'),('p1')", &[]).await.expect("parents");
    for start in (0..1000).step_by(100) {
        let values = (start..start + 100).map(|i| format!("('c{i}','p{}',{i})", usize::from(i % 10 != 0))).collect::<Vec<_>>().join(",");
        lix.execute(&format!("INSERT INTO cover_child (id,parent_id,score) VALUES {values}"), &[]).await.expect("children");
    }
    macro_rules! measure_query {
        ($operation:literal, $sql:expr, $expected_rows:expr) => {{
            for _ in 0..3 {
                assert_eq!(
                    lix.execute($sql, &[]).await.expect("warm query").len(),
                    $expected_rows
                );
            }
            let mut wall = Vec::with_capacity(20);
            for _ in 0..20 {
                let started = Instant::now();
                let rows = lix.execute($sql, &[]).await.expect("measured query");
                wall.push(started.elapsed().as_nanos() as u64);
                assert_eq!(rows.len(), $expected_rows);
            }
            wall.sort_unstable();
            println!(
                "covering_score backend={backend} op={} wall_p50_ns={} wall_p95_ns={}",
                $operation, wall[10], wall[19]
            );
        }};
    }

    measure_query!(
        "filter",
        "SELECT id, parent_id, score FROM cover_child WHERE parent_id = 'p0' ORDER BY id",
        100
    );
    measure_query!(
        "point",
        "SELECT id, parent_id, score FROM cover_child WHERE id = 'c0'",
        1
    );
    measure_query!(
        "full",
        "SELECT id, parent_id, score FROM cover_child ORDER BY id",
        1000
    );

    for _ in 0..3 {
        lix.execute("UPDATE cover_child SET score = score + 1 WHERE id = 'c0'", &[])
            .await
            .expect("warm update");
    }
    let mut update_wall = Vec::with_capacity(20);
    for _ in 0..20 {
        let started = Instant::now();
        lix.execute("UPDATE cover_child SET score = score + 1 WHERE id = 'c0'", &[])
            .await
            .expect("measured update");
        update_wall.push(started.elapsed().as_nanos() as u64);
    }
    update_wall.sort_unstable();
    println!(
        "covering_score backend={backend} op=update wall_p50_ns={} wall_p95_ns={}",
        update_wall[10], update_wall[19]
    );

    let adapter = StorageAdapter::new(accounting_storage);
    let read = adapter
        .begin_read(StorageReadOptions::default())
        .await
        .expect("layout read");
    let accounting = layout_accounting(&read).await;
    let rows = accounting.iter().map(|entry| entry.rows).sum::<u64>();
    let bytes = accounting
        .iter()
        .map(|entry| entry.key_bytes + entry.value_bytes)
        .sum::<u64>();
    println!("covering_storage backend={backend} rows={rows} bytes={bytes}");
}
