//! Reproducible in-memory fan-out workload. Run with `cargo run -p lix
//! --release --example profile_cascade -- 10000 100` (rows, matching rows).
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
        serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"bench_parent","columns":[{"name":"id","type":"int8","nullable":false}],"primary_key":["id"]}),
        serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"bench_child","columns":[{"name":"id","type":"int8","nullable":false},{"name":"parent_id","type":"int8","nullable":false}],"primary_key":["id"],"foreign_keys":[{"columns":["parent_id"],"references":{"schema_key":"bench_parent","columns":["id"]},"on_delete":"cascade"}]}),
    ] {
        db.execute(
            "INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)",
            &[Value::Text(schema.to_string())],
        )
        .await?;
    }
    db.execute("INSERT INTO bench_parent(id) VALUES (1),(2)", &[])
        .await?;
    for start in (0..total).step_by(1000) {
        let values = (start..(start + 1000).min(total))
            .map(|id| format!("({id},{})", if id < fanout { 1 } else { 2 }))
            .collect::<Vec<_>>()
            .join(",");
        db.execute(
            &format!("INSERT INTO bench_child(id,parent_id) VALUES {values}"),
            &[],
        )
        .await?;
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
    db.execute("DELETE FROM bench_parent WHERE id=1", &[])
        .await?;
    let elapsed = started.elapsed();
    if let Some(control) = &mut perf_control {
        writeln!(control, "disable").unwrap();
    }
    let remaining = db
        .execute("SELECT count(*) AS n FROM bench_child", &[])
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
