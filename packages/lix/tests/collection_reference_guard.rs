#![cfg(feature = "storage-benches")]
use lix::{LixError, Value, open_lix, storage_bench::measure_checkpoint_foreground};
use std::time::Instant;
async fn collection_census(
    mode: &str,
    n: usize,
) -> Result<lix::storage_bench::CheckpointForegroundAccounting, LixError> {
    assert!(matches!(mode, "plain" | "empty_row_ref"));
    let db = open_lix().await?;
    let schema = serde_json::json!({
        "$schema":"https://lix.dev/schema-v1.json", "key":"audit_plain",
        "columns":[{"name":"id","type":"int8","nullable":false}],
        "primary_key":["id"]
    });
    db.execute(
        "INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)",
        &[Value::Text(schema.to_string())],
    )
    .await?;
    if mode == "empty_row_ref" {
        let schema = serde_json::json!({
            "$schema":"https://lix.dev/schema-v1.json", "key":"audit_empty_refs",
            "columns":[{"name":"id","type":"int8","nullable":false},
                {"name":"target","type":"text","nullable":true}],
            "primary_key":["id"], "row_refs":[{"column":"target"}]
        });
        db.execute(
            "INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)",
            &[Value::Text(schema.to_string())],
        )
        .await?;
    }
    for start in (0..n).step_by(1000) {
        let values = (start..(start + 1000).min(n))
            .map(|i| format!("({i})"))
            .collect::<Vec<_>>()
            .join(",");
        db.execute(&format!("INSERT INTO audit_plain(id) VALUES {values}"), &[])
            .await?;
    }
    let before = db
        .execute("SELECT count(*) AS n FROM audit_plain", &[])
        .await?
        .rows()[0]
        .get::<i64>("n")?;
    assert_eq!(before, n as i64);
    let started = Instant::now();
    let (result, reads) =
        measure_checkpoint_foreground(async { db.execute("DELETE FROM audit_plain", &[]).await })
            .await;
    let result = result?;
    let elapsed = started.elapsed();
    assert_eq!(result.rows_affected(), n as u64);
    let after = db
        .execute("SELECT count(*) AS n FROM audit_plain", &[])
        .await?
        .rows()[0]
        .get::<i64>("n")?;
    assert_eq!(after, 0);
    println!(
        "mode={mode} rows={n} affected={} elapsed_ms={:.3} point_keys={} scan_starts={} scan_pages={} scan_rows={} written_bytes={}",
        result.rows_affected(),
        elapsed.as_secs_f64() * 1000.,
        reads.point_keys,
        reads.scan_starts,
        reads.scan_pages,
        reads.scan_rows,
        reads.written_bytes
    );
    db.close().await?;
    Ok(reads)
}

#[tokio::test]
async fn empty_reference_declarations_preserve_collection_marker_delete() -> Result<(), LixError> {
    for n in [100, 2000] {
        let plain = collection_census("plain", n).await?;
        let empty = collection_census("empty_row_ref", n).await?;
        assert!(
            empty.written_bytes <= plain.written_bytes + 2048,
            "empty source caused row tombstones: plain={plain:?}, empty={empty:?}"
        );
        assert!(
            empty.scan_rows <= plain.scan_rows + 16,
            "empty source caused row reads: plain={plain:?}, empty={empty:?}"
        );
    }
    Ok(())
}
