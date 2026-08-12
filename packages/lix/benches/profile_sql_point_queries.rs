//! Reproducible SlateDB SQL point-query profile.
//!
//! ```text
//! cargo bench -p lix --bench profile_sql_point_queries --no-default-features \
//!   --features slatedb_benches -- \
//!   setup /tmp/lix-sql-point-100k 100000
//! cargo bench -p lix --bench profile_sql_point_queries --no-default-features \
//!   --features slatedb_benches -- \
//!   run /tmp/lix-sql-point-100k 100000 1000
//! ```

use std::hint::black_box;
use std::path::Path;
use std::time::{Duration, Instant};

use lix::integration::{Engine, SessionContext};
use lix::storage::Storage;
use lix::Value;
use lix_storage_slatedb::SlateDB;

const SEED_BATCH_ROWS: usize = 1_000;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = std::env::args()
        .skip(1)
        .filter(|argument| argument != "--bench")
        .collect::<Vec<_>>();
    match args.as_slice() {
        [command, directory, rows] if command == "setup" => {
            setup(Path::new(directory), parse(rows, "rows")).await;
        }
        [command, directory, rows, rounds] if command == "run" => {
            run(
                Path::new(directory),
                parse(rows, "rows"),
                parse(rounds, "rounds"),
            )
            .await;
        }
        _ => panic!(
            "usage:\n  profile_sql_point_queries setup <storage-dir> <rows>\n  \
             profile_sql_point_queries run <storage-dir> <rows> <rounds>"
        ),
    }
}

fn parse(value: &str, label: &str) -> usize {
    value
        .parse::<usize>()
        .unwrap_or_else(|_| panic!("{label} must be a positive integer"))
}

async fn open_initialized(path: &Path) -> (SlateDB, SessionContext<SlateDB>) {
    let storage = SlateDB::open(path).expect("open point-query SlateDB");
    let engine = Engine::new(storage.clone())
        .await
        .expect("open point-query engine");
    let session = engine
        .open_session()
        .await
        .expect("open point-query session");
    (storage, session)
}

async fn setup(path: &Path, rows: usize) {
    assert!(rows > 0);
    assert!(!path.exists(), "refusing to overwrite {}", path.display());
    let storage = SlateDB::open(path).expect("create point-query SlateDB");
    Engine::initialize(storage.clone())
        .await
        .expect("initialize point-query fixture");
    let engine = Engine::new(storage.clone())
        .await
        .expect("open initialized point-query fixture");
    let session = engine
        .open_session()
        .await
        .expect("open point-query setup session");
    let schema = serde_json::json!({
        "x-lix-key": "point_query_record",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "properties": {
            "id": { "type": "string" },
            "revision": { "type": "integer" },
            "payload": { "type": "string" }
        },
        "required": ["id", "revision", "payload"],
        "additionalProperties": false
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register point-query schema");
    let started = Instant::now();
    for start in (0..rows).step_by(SEED_BATCH_ROWS) {
        let end = (start + SEED_BATCH_ROWS).min(rows);
        let mut sql =
            String::from("INSERT INTO point_query_record (id, revision, payload) VALUES ");
        let mut params = Vec::with_capacity((end - start) * 3);
        for (offset, index) in (start..end).enumerate() {
            if offset > 0 {
                sql.push(',');
            }
            let parameter = offset * 3;
            sql.push_str(&format!(
                "(${}, ${}, ${})",
                parameter + 1,
                parameter + 2,
                parameter + 3
            ));
            params.push(Value::Text(key(index)));
            params.push(Value::Integer(index as i64));
            params.push(Value::Text(format!("payload-{index:08}")));
        }
        session
            .execute(&sql, &params)
            .await
            .expect("seed point-query rows");
    }
    session
        .create_checkpoint()
        .await
        .expect("checkpoint point-query fixture");
    storage.flush().await.expect("flush point-query SlateDB");
    println!(
        "setup rows={rows} elapsed_ms={:.3}",
        millis(started.elapsed())
    );
}

async fn run(path: &Path, rows: usize, rounds: usize) {
    assert!(rows > 0 && rounds > 0);
    let (_storage, session) = open_initialized(path).await;
    let sql =
        "SELECT id, revision, payload FROM point_query_record WHERE id = $1 AND revision >= 0";
    let cold_started = Instant::now();
    let (_, cold_profile) = session
        .execute_profiled(sql, &[Value::Text(key(rows / 2))])
        .await
        .expect("cold point query");
    let cold = cold_started.elapsed();

    for index in 0..32 {
        let query_index = permute(index, rows);
        black_box(
            session
                .execute(sql, &[Value::Text(key(query_index))])
                .await
                .expect("warm point query"),
        );
    }

    let warm_profile_started = Instant::now();
    let (_, warm_profile) = session
        .execute_profiled(sql, &[Value::Text(key(rows / 2))])
        .await
        .expect("profiled warm point query");
    let warm_profile_total = warm_profile_started.elapsed();

    let random = measure(&session, sql, rows, rounds, false).await;
    let repeated = measure(&session, sql, rows, rounds, true).await;
    println!(
        "point_query rows={rows} rounds={rounds} cold_us={:.3} cold_logical_us={:.3} \
         cold_physical_us={:.3} cold_execution_us={:.3} cold_other_us={:.3} \
         warm_profile_total_us={:.3} warm_logical_us={:.3} warm_physical_us={:.3} \
         warm_execution_us={:.3} warm_other_us={:.3} \
         random_p50_us={:.3} random_p95_us={:.3} repeated_p50_us={:.3} repeated_p95_us={:.3}",
        micros(cold),
        micros(cold_profile.logical_planning),
        micros(cold_profile.physical_planning),
        micros(cold_profile.arrow_execution),
        micros(cold_profile.unattributed_overhead()),
        micros(warm_profile_total),
        micros(warm_profile.logical_planning),
        micros(warm_profile.physical_planning),
        micros(warm_profile.arrow_execution),
        micros(warm_profile.unattributed_overhead()),
        micros(percentile(&random, 50)),
        micros(percentile(&random, 95)),
        micros(percentile(&repeated, 50)),
        micros(percentile(&repeated, 95)),
    );
}

async fn measure<S: Storage + Clone + Send + Sync + 'static>(
    session: &SessionContext<S>,
    sql: &str,
    rows: usize,
    rounds: usize,
    repeated: bool,
) -> Vec<Duration> {
    let mut durations = Vec::with_capacity(rounds);
    for index in 0..rounds {
        let query_index = if repeated {
            rows / 2
        } else {
            permute(index + 97, rows)
        };
        let started = Instant::now();
        let result = session
            .execute(sql, &[Value::Text(key(query_index))])
            .await
            .expect("profile point query");
        black_box(result);
        durations.push(started.elapsed());
    }
    durations.sort_unstable();
    durations
}

fn permute(index: usize, rows: usize) -> usize {
    index.wrapping_mul(0x9e37_79b1).wrapping_add(0x85eb_ca6b) % rows
}

fn key(index: usize) -> String {
    format!("record-{index:08}")
}

fn percentile(samples: &[Duration], percentile: usize) -> Duration {
    samples[((samples.len() - 1) * percentile) / 100]
}

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
