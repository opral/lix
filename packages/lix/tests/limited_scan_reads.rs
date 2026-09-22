#![cfg(feature = "storage-benches")]
use lix::{LixError, Value, open_lix, storage_bench::measure_checkpoint_foreground};

#[tokio::test]
async fn limit_bounds_storage_reads_and_skips_deleted_rows() -> Result<(), LixError> {
    let mut counts = Vec::new();
    for n in [100, 2000] {
        let db = open_lix().await?;
        let schema = serde_json::json!({
            "$schema":"https://lix.dev/schema-v1.json", "key":"limited_rows",
            "columns":[{"name":"id","type":"int8","nullable":false}],
            "primary_key":["id"]
        });
        db.execute(
            "INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)",
            &[Value::Text(schema.to_string())],
        )
        .await?;
        let rows = (0..n)
            .map(|id| format!("({id})"))
            .collect::<Vec<_>>()
            .join(",");
        db.execute(&format!("INSERT INTO limited_rows(id) VALUES {rows}"), &[])
            .await?;
        db.execute("DELETE FROM limited_rows WHERE id IN (0,1,2)", &[])
            .await?;
        let (result, census) = measure_checkpoint_foreground(async {
            db.execute("SELECT id FROM limited_rows LIMIT 1", &[]).await
        })
        .await;
        let result = result?;
        assert_eq!(result.rows().len(), 1);
        assert!(result.rows()[0].get::<i64>("id")? >= 3);
        println!("LIMIT input_rows={n} storage={census:?}");
        counts.push(census);
        assert!(db.execute(
            "SELECT id FROM limited_rows WHERE lixcol_file_id IN ('01920000-0000-7000-8000-0000000000a1', '01920000-0000-7000-8000-0000000000a2') LIMIT 1", &[]
        ).await?.rows().is_empty());
        db.execute(
            &format!("INSERT INTO limited_rows(id,lixcol_untracked) VALUES ({n},true)"),
            &[],
        )
        .await?;
        let retained = db
            .execute(
                "SELECT id FROM limited_rows WHERE lixcol_untracked = true LIMIT 1",
                &[],
            )
            .await?;
        assert_eq!(retained.rows().len(), 1);
        assert_eq!(retained.rows()[0].get::<i64>("id")?, n);
        db.close().await?;
    }
    assert!(
        counts[1].scan_rows <= counts[0].scan_rows + 8,
        "LIMIT reads grow with unrelated rows: {counts:?}"
    );
    assert!(
        counts[1].scan_rows < 32,
        "LIMIT must bound storage reads: {counts:?}"
    );
    Ok(())
}

#[tokio::test]
async fn join_above_sixty_four_keys_keeps_indexed_probe() -> Result<(), LixError> {
    let mut counts = Vec::new();
    for n in [100, 2000] {
        let db = open_lix().await?;
        for key in ["wide_build", "wide_probe"] {
            let schema = serde_json::json!({
                "$schema":"https://lix.dev/schema-v1.json", "key":key,
                "columns":[{"name":"id","type":"int8","nullable":false},
                    {"name":"value","type":"text","nullable":false}],
                "primary_key":["id"], "unique":[["value"]]
            });
            db.execute(
                "INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)",
                &[Value::Text(schema.to_string())],
            )
            .await?;
        }
        for (table, count) in [("wide_build", 70), ("wide_probe", n)] {
            let rows = (0..count)
                .map(|id| format!("({id},'value{id}')"))
                .collect::<Vec<_>>()
                .join(",");
            db.execute(&format!("INSERT INTO {table}(id,value) VALUES {rows}"), &[])
                .await?;
        }
        let (result, census) = measure_checkpoint_foreground(async {
            db.execute(
                "SELECT b.id FROM wide_build b JOIN wide_probe p ON b.value = p.value",
                &[],
            )
            .await
        })
        .await;
        assert_eq!(result?.rows().len(), 70);
        println!("JOIN build_keys=70 probe_rows={n} storage={census:?}");
        counts.push(census);
        db.close().await?;
    }
    assert!(
        counts[1].scan_rows <= counts[0].scan_rows + 32,
        "wide join scans probe collection: {counts:?}"
    );
    Ok(())
}
