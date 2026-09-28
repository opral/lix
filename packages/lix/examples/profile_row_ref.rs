//! Run `cargo run -p lix --release --example profile_row_ref -- 10000 100`.
//! Arguments: keys per file and point-query repetitions. Setup is not timed.
use lix::{LixError, Value, open_lix};
use std::{io::Write, time::Instant};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), LixError> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let count = args
        .first()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000);
    let repeats = args
        .get(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(100);
    assert!(count >= 2 && repeats > 0);
    let db = open_lix().await?;
    let schema = serde_json::json!({"$schema":"https://lix.dev/schema-v1.json","key":"bench_ref","columns":[{"name":"id","type":"int8","nullable":false}],"primary_key":["id"]});
    db.execute(
        "INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)",
        &[Value::Text(schema.to_string())],
    )
    .await?;
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
        "SELECT commit_id FROM lix_create_checkpoint(NULL, NULL)",
        &[],
    )
    .await?;
    for file in files {
        for start in (0..count).step_by(1000) {
            let rows = (start..(start + 1000).min(count))
                .map(|id| format!("({id},'{file}')"))
                .collect::<Vec<_>>()
                .join(",");
            db.execute(
                &format!("INSERT INTO bench_ref(id,lixcol_file_id) VALUES {rows}"),
                &[],
            )
            .await?;
        }
    }
    let start = Instant::now();
    let refs = db
        .execute(
            "SELECT lix_row_ref('bench_ref',lixcol_file_id,id) AS row_ref FROM bench_ref",
            &[],
        )
        .await?;
    assert_eq!(refs.rows().len(), count * 2);
    println!(
        "rows={} construct_ms={:.3}",
        count * 2,
        start.elapsed().as_secs_f64() * 1000.0
    );
    let params = [Value::Text(files[0].into()), Value::Text(files[1].into())];
    let sql = "SELECT row_ref FROM lix_diff('bench_ref') WHERE row_ref IN (lix_row_ref('bench_ref',$1,0),lix_row_ref('bench_ref',$2,1))";
    assert_eq!(db.execute(sql, &params).await?.rows().len(), 2);
    let mut control = std::env::var_os("LIX_ROW_REF_PERF_CONTROL").map(|path| {
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("perf control FIFO")
    });
    if let Some(control) = &mut control {
        writeln!(control, "enable").unwrap();
    }
    let start = Instant::now();
    for _ in 0..repeats {
        assert_eq!(db.execute(sql, &params).await?.rows().len(), 2);
    }
    let elapsed = start.elapsed();
    if let Some(control) = &mut control {
        writeln!(control, "disable").unwrap();
    }
    println!(
        "rows={} matched=2 repeats={} pair_lookup_ms={:.3}",
        count * 2,
        repeats,
        elapsed.as_secs_f64() * 1000.0 / repeats as f64
    );
    db.close().await?;
    Ok(())
}
