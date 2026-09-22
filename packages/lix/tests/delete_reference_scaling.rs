#![cfg(feature = "storage-benches")]
//! DELETE-side reference-source storage-read census.
//!
//! The measured DELETE affects one parent (100 in bulk mode) and no children.
//! Source size varies independently of the answer. The churn fixture first
//! updates every reference away from the deleted target.
use lix::{LixError, Value, open_lix, storage_bench::measure_checkpoint_foreground};
use std::time::Instant;

const FILE_A: &str = "01920000-0000-7000-8000-0000000000a1";
const FILE_B: &str = "01920000-0000-7000-8000-0000000000a2";

async fn delete_census(
    mode: &str,
    total: usize,
) -> Result<lix::storage_bench::CheckpointForegroundAccounting, LixError> {
    assert!(matches!(
        mode,
        "row_ref" | "row_ref_churn" | "composite_fk" | "composite_fk_bulk"
    ));
    let bulk = mode == "composite_fk_bulk";
    let affected_parents = if bulk { 100 } else { 1 };
    let db = open_lix().await?;
    let (parent, child) = if matches!(mode, "row_ref" | "row_ref_churn") {
        (
            serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"audit_ref_parent","columns":[{"name":"id","type":"int8","nullable":false}],"primary_key":["id"]}),
            serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"audit_ref_child","columns":[{"name":"id","type":"int8","nullable":false},{"name":"target","type":"text","nullable":false}],"primary_key":["id"],"row_refs":[{"column":"target"}]}),
        )
    } else {
        (
            serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"audit_fk_parent","columns":[{"name":"a","type":"int8","nullable":false},{"name":"b","type":"int8","nullable":false}],"primary_key":["a","b"]}),
            serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"audit_fk_child","columns":[{"name":"id","type":"int8","nullable":false},{"name":"pa","type":"int8","nullable":false},{"name":"pb","type":"int8","nullable":false}],"primary_key":["id"],"foreign_keys":[{"columns":["pa","pb"],"references":{"schema_key":"audit_fk_parent","columns":["a","b"]}}]}),
        )
    };
    for schema in [parent, child] {
        db.execute(
            "INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)",
            &[Value::Text(schema.to_string())],
        )
        .await?;
    }
    db.execute(
        "INSERT INTO lix_file(id,path) VALUES ($1,'/a'),($2,'/b')",
        &[Value::Text(FILE_A.into()), Value::Text(FILE_B.into())],
    )
    .await?;
    let delete_sql = if matches!(mode, "row_ref" | "row_ref_churn") {
        db.execute(
            "INSERT INTO audit_ref_parent(id,lixcol_file_id) VALUES (1,$1),(1,$2)",
            &[Value::Text(FILE_A.into()), Value::Text(FILE_B.into())],
        )
        .await?;
        let result = db
            .execute(
                "SELECT lix_row_ref('audit_ref_parent',$1,1) AS reference",
                &[Value::Text(FILE_B.into())],
            )
            .await?;
        let reference = result.rows()[0].get::<lix::RowRef>("reference")?;
        let target = Value::Text(reference.as_str().to_owned());
        let initial_target = if mode == "row_ref_churn" {
            let result = db
                .execute(
                    "SELECT lix_row_ref('audit_ref_parent',$1,1) AS reference",
                    &[Value::Text(FILE_A.into())],
                )
                .await?;
            let reference = result.rows()[0].get::<lix::RowRef>("reference")?;
            Value::Text(reference.as_str().to_owned())
        } else {
            target.clone()
        };
        for start in (0..total).step_by(1000) {
            let values = (start..(start + 1000).min(total))
                .map(|id| format!("({id},$1,'{FILE_B}')"))
                .collect::<Vec<_>>()
                .join(",");
            db.execute(
                &format!("INSERT INTO audit_ref_child(id,target,lixcol_file_id) VALUES {values}"),
                std::slice::from_ref(&initial_target),
            )
            .await?;
        }
        if mode == "row_ref_churn" {
            db.execute("UPDATE audit_ref_child SET target=$1", &[target])
                .await?;
        }
        "DELETE FROM audit_ref_parent WHERE id=1 AND lixcol_file_id='01920000-0000-7000-8000-0000000000a1'".to_owned()
    } else {
        let parent_values = if bulk {
            (1..=100)
                .map(|id| format!("({id},{id},'{FILE_A}')"))
                .chain(std::iter::once(format!("(1001,1001,'{FILE_A}')")))
                .collect::<Vec<_>>()
                .join(",")
        } else {
            format!("(1,1,'{FILE_A}'),(2,2,'{FILE_A}')")
        };
        db.execute(
            &format!("INSERT INTO audit_fk_parent(a,b,lixcol_file_id) VALUES {parent_values}"),
            &[],
        )
        .await?;
        let referenced_id = if bulk { 1001 } else { 2 };
        for start in (0..total).step_by(1000) {
            let values = (start..(start + 1000).min(total))
                .map(|id| format!("({id},{referenced_id},{referenced_id},'{FILE_A}')"))
                .collect::<Vec<_>>()
                .join(",");
            db.execute(
                &format!("INSERT INTO audit_fk_child(id,pa,pb,lixcol_file_id) VALUES {values}"),
                &[],
            )
            .await?;
        }
        if bulk {
            let ids = (1..=100)
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(",");
            format!("DELETE FROM audit_fk_parent WHERE a IN ({ids})")
        } else {
            "DELETE FROM audit_fk_parent WHERE a=1 AND b=1 AND lixcol_file_id='01920000-0000-7000-8000-0000000000a1'".to_owned()
        }
    };

    let start = Instant::now();
    let (result, reads) =
        measure_checkpoint_foreground(async { db.execute(&delete_sql, &[]).await }).await;
    let result = result?;
    assert_eq!(result.rows_affected(), affected_parents);
    let elapsed = start.elapsed();
    let child = if matches!(mode, "row_ref" | "row_ref_churn") {
        "audit_ref_child"
    } else {
        "audit_fk_child"
    };
    let remaining = db
        .execute(&format!("SELECT count(*) AS n FROM {child}"), &[])
        .await?
        .rows()[0]
        .get::<i64>("n")?;
    assert_eq!(remaining, total as i64);
    println!(
        "mode={mode} children={total} affected_parents={affected_parents} affected_children=0 remaining={remaining} delete_ms={:.3} read_views={} point_keys={} scan_starts={} scan_pages={} scan_rows={} ",
        elapsed.as_secs_f64() * 1000.0,
        reads.read_views,
        reads.point_keys,
        reads.scan_starts,
        reads.scan_pages,
        reads.scan_rows
    );
    db.close().await?;
    Ok(reads)
}

#[tokio::test]
async fn unrelated_reference_sources_and_history_do_not_scale_delete_reads() -> Result<(), LixError>
{
    for mode in [
        "row_ref",
        "row_ref_churn",
        "composite_fk",
        "composite_fk_bulk",
    ] {
        let small = delete_census(mode, 100).await?;
        let large = delete_census(mode, 2000).await?;
        assert!(
            large.scan_rows <= small.scan_rows + 32,
            "{mode}: scans scale with unrelated rows: small={small:?}, large={large:?}"
        );
        assert!(
            large.point_keys <= small.point_keys + 64,
            "{mode}: point hydration scales with unrelated rows: small={small:?}, large={large:?}"
        );
    }
    Ok(())
}
