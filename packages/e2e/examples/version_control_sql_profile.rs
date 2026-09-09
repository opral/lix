//! Fixed-page SQL and checkpoint scaling profile using public APIs.
//!
//! cargo run --manifest-path tooling/Cargo.toml -p lix_e2e --release \
//!   --example version_control_sql_profile -- 100 1000 5000
//! Emits JSON lines; setup is excluded from query timings. The default backend
//! is RocksDB; pass LIX_PROFILE_MEMORY=1 for an in-memory control run.
use lix::storage::Storage;
use lix::{Lix, Value, open_lix};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sizes = std::env::args()
        .skip(1)
        .map(|arg| arg.parse::<usize>())
        .collect::<Result<Vec<_>, _>>()?;
    let sizes = if sizes.is_empty() {
        vec![100, 1000, 5000]
    } else {
        sizes
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let directory = tempfile::tempdir()?;
        if std::env::var_os("LIX_PROFILE_MEMORY").is_some() {
            profile(open_lix().await?, "memory", sizes).await
        } else {
            profile(
                open_lix()
                    .with_storage(lix_storage_rocksdb::RocksDB::open(directory.path())?)
                    .await?,
                "rocksdb",
                sizes,
            )
            .await
        }
    })
}

async fn profile<S>(
    lix: Lix<S>,
    backend: &str,
    sizes: Vec<usize>,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut retained = 0;
    for size in sizes {
        while retained < size {
            lix.execute("INSERT INTO lix_key_value (key, value) VALUES ('history', $1) ON CONFLICT (key) DO UPDATE SET value = excluded.value", &[Value::Text(retained.to_string())]).await?;
            lix.execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
                .await?;
            retained += 1;
        }
        let anchor = lix
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await?
            .rows()[0]
            .get::<String>("id")?;
        let parameters = [Value::Text(anchor)];
        for (name, query) in [
            (
                "log_page",
                "SELECT commit_id FROM lix_log($1) WHERE is_checkpoint ORDER BY position LIMIT 20",
            ),
            (
                "history_page",
                "WITH page AS (SELECT commit_id, position FROM lix_log($1) WHERE is_checkpoint ORDER BY position LIMIT 20) SELECT p.commit_id, h.key, h.diff_type FROM page p LEFT JOIN lix_history('lix_key_value', $1) h ON h.lixcol_to_commit_id = p.commit_id ORDER BY p.position",
            ),
        ] {
            let (median, p95, rows) = measure(&lix, query, &parameters).await?;
            assert_eq!(rows, size.min(20));
            println!(
                "{}",
                serde_json::json!({"backend":backend,"query":name,"retained":size,"page":20,"median_us":median,"p95_us":p95,"rows":rows})
            );
        }
        // Grow distinct working rows without checkpointing, then time the
        // actual full capture once. Empty follow-ups are not substitutes.
        for index in 0..size {
            lix.execute("INSERT INTO lix_key_value (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = excluded.value", &[Value::Text(format!("working-{index}")), Value::Text(size.to_string())]).await?;
        }
        let started = Instant::now();
        lix.execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
            .await?;
        println!(
            "{}",
            serde_json::json!({"backend":backend,"operation":"full_checkpoint","working_rows":size,"elapsed_us":started.elapsed().as_micros()})
        );
        let working = lix
            .execute("SELECT count(*) AS n FROM lix_diff('lix_key_value')", &[])
            .await?;
        assert_eq!(working.rows()[0].get::<i64>("n")?, 0);
        retained += 1;
    }
    lix.close().await?;
    Ok(())
}

async fn measure<S: Storage + Clone + Send + Sync + 'static>(
    lix: &Lix<S>,
    sql: &str,
    parameters: &[Value],
) -> Result<(u128, u128, usize), lix::LixError> {
    let mut samples = Vec::new();
    let mut rows = 0;
    for iteration in 0..24 {
        let started = Instant::now();
        let result = lix.execute(sql, parameters).await?;
        let elapsed = started.elapsed().as_micros();
        rows = result.len();
        if iteration >= 3 {
            samples.push(elapsed);
        }
    }
    samples.sort_unstable();
    Ok((
        samples[samples.len() / 2],
        samples[samples.len() * 95 / 100],
        rows,
    ))
}
