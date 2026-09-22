//! Reproducible cross-file row-reference cascade fan-out workload. Run with `cargo run -p lix
//! --release --example profile_row_ref_cascade -- 10000 100` (rows, matching rows).
use lix::{LixError, Value, open_lix};
use std::{io::Write, time::Instant};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), LixError> {
    if std::env::var_os("LIX_CASCADE_TRACE").is_some() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_target(true)
            .init();
    }
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let total = args
        .first()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);
    let fanout = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(total);
    assert!(fanout <= total);
    let db = open_lix().await?;
    for schema in [
        serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"bench_ref_parent","columns":[{"name":"id","type":"int8","nullable":false}],"primary_key":["id"]}),
        serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"bench_ref_child","columns":[{"name":"id","type":"int8","nullable":false},{"name":"target","type":"text","nullable":false}],"primary_key":["id"],"row_refs":[{"column":"target","on_delete":"cascade"}]}),
    ] {
        db.execute(
            "INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)",
            &[Value::Text(schema.to_string())],
        )
        .await?;
    }
    let files = [
        "01920000-0000-7000-8000-0000000000a1",
        "01920000-0000-7000-8000-0000000000a2",
    ];
    db.execute(
        "INSERT INTO lix_file(id,path) VALUES ($1,'/a'),($2,'/b')",
        &[Value::Text(files[0].into()), Value::Text(files[1].into())],
    )
    .await?;
    db.execute(
        "INSERT INTO bench_ref_parent(id,lixcol_file_id) VALUES (1,$1),(1,$2)",
        &[Value::Text(files[0].into()), Value::Text(files[1].into())],
    )
    .await?;
    let mut targets = Vec::new();
    for file in files {
        let result = db
            .execute(
                "SELECT lix_row_ref('bench_ref_parent',$1,1) AS reference",
                &[Value::Text(file.into())],
            )
            .await?;
        let reference = result.rows()[0].get::<lix::RowRef>("reference")?;
        targets.push(Value::Text(reference.as_str().to_owned()));
    }
    for (first, end, target) in [(0, fanout, &targets[0]), (fanout, total, &targets[1])] {
        for start in (first..end).step_by(1000) {
            let values = (start..(start + 1000).min(end))
                .map(|id| format!("({id},$1,'{}')", files[1]))
                .collect::<Vec<_>>()
                .join(",");
            db.execute(
                &format!("INSERT INTO bench_ref_child(id,target,lixcol_file_id) VALUES {values}"),
                std::slice::from_ref(target),
            )
            .await?;
        }
    }
    let mut perf_control = std::env::var_os("LIX_CASCADE_PERF_CONTROL").map(|path| {
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open perf control FIFO")
    });
    if let Some(control) = &mut perf_control {
        writeln!(control, "enable").unwrap();
    }
    let started = Instant::now();
    db.execute(
        "DELETE FROM bench_ref_parent WHERE id=1 AND lixcol_file_id=$1",
        &[Value::Text(files[0].into())],
    )
    .await?;
    let elapsed = started.elapsed();
    if let Some(control) = &mut perf_control {
        writeln!(control, "disable").unwrap();
    }
    let remaining = db
        .execute("SELECT count(*) AS n FROM bench_ref_child", &[])
        .await?
        .rows()[0]
        .get::<i64>("n")?;
    assert_eq!(remaining, (total - fanout) as i64);
    println!(
        "rows={total} fanout={fanout} delete_ms={:.3}",
        elapsed.as_secs_f64() * 1000.0
    );
    db.close().await?;
    Ok(())
}
