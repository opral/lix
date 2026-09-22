#![cfg(feature = "storage-benches")]
use lix::{LixError, Value, open_lix, storage_bench::measure_checkpoint_foreground};

#[tokio::test]
async fn indexed_in_above_sixty_four_values_keeps_storage_reads_bounded() -> Result<(), LixError> {
    let mut counts = Vec::new();
    for n in [100, 2000] {
        let db = open_lix().await?;
        let schema = serde_json::json!({
            "$schema":"https://lix.dev/schema-v1.json", "key":"wide_index",
            "columns":[{"name":"id","type":"int8","nullable":false},
                {"name":"value","type":"text","nullable":false}],
            "primary_key":["id"], "unique":[["value"]]
        });
        db.execute(
            "INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)",
            &[Value::Text(schema.to_string())],
        )
        .await?;
        let rows = (0..n)
            .map(|id| format!("({id},'value{id}')"))
            .collect::<Vec<_>>()
            .join(",");
        db.execute(
            &format!("INSERT INTO wide_index(id,value) VALUES {rows}"),
            &[],
        )
        .await?;
        let values = (0..70)
            .map(|id| format!("'absent{id}'"))
            .collect::<Vec<_>>()
            .join(",");
        let (result, census) = measure_checkpoint_foreground(async {
            db.execute(
                &format!("SELECT id FROM wide_index WHERE value IN ({values})"),
                &[],
            )
            .await
        })
        .await;
        assert!(result?.rows().is_empty());
        counts.push(census);
        db.close().await?;
    }
    assert!(
        counts[1].scan_rows <= counts[0].scan_rows + 16,
        "wide IN scans collection: {counts:?}"
    );
    assert!(
        counts[1].point_keys <= counts[0].point_keys + 32,
        "wide IN hydrates collection: {counts:?}"
    );
    Ok(())
}

#[tokio::test]
async fn many_absent_index_values_fall_back_before_opening_range_per_key() -> Result<(), LixError> {
    let db = open_lix().await?;
    let schema = serde_json::json!({
        "$schema":"https://lix.dev/schema-v1.json", "key":"absent_index",
        "columns":[{"name":"id","type":"int8","nullable":false},
            {"name":"value","type":"text","nullable":false}],
        "primary_key":["id"], "unique":[["value"]]
    });
    db.execute(
        "INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)",
        &[Value::Text(schema.to_string())],
    )
    .await?;
    let rows = (0..100)
        .map(|id| format!("({id},'present{id}')"))
        .collect::<Vec<_>>()
        .join(",");
    db.execute(
        &format!("INSERT INTO absent_index(id,value) VALUES {rows}"),
        &[],
    )
    .await?;
    let mut counts = Vec::new();
    for count in [256, 4096] {
        let values = (0..count)
            .map(|id| format!("'absent{id}'"))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("SELECT id FROM absent_index WHERE value IN ({values})");
        let (result, census) =
            measure_checkpoint_foreground(async { db.execute(&sql, &[]).await }).await;
        assert!(result?.rows().is_empty());
        assert!(
            census.scan_starts < 64,
            "empty buckets opened a range per key: {census:?}"
        );
        counts.push(census);
    }
    assert!(
        counts[1].scan_starts <= counts[0].scan_starts + 4,
        "range seeks scale with absent keys: {counts:?}"
    );
    db.close().await?;
    Ok(())
}
