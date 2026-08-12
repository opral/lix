//! Profile internal eager, collected-batch, and live-batch result paths.
//!
//! This keeps SQL execution and result consumption in the same profile scope.
//! `full` is the current materialized public result path; `stream` converts
//! one row at a time after DataFusion collection; `live` consumes the
//! benchmark-only physical batch stream before collection; `count_only` skips
//! scalar conversion altogether. The public eager `ExecuteResult` API is not
//! changed by this benchmark. A numeric `LIX_SQL_PROFILE_ROW_LIMIT` models an
//! early stop by an eventual explicit read-stream API.
//!
//! ```text
//! LIX_SQL_PROFILE_RESULT_MODE=full \
//!   cargo bench -p lix --no-default-features --features storage-benches \
//!   --bench profile_sql_result_streaming
//! LIX_SQL_PROFILE_RESULT_MODE=stream LIX_SQL_PROFILE_ROW_LIMIT=100 \
//!   cargo bench -p lix --no-default-features --features storage-benches \
//!   --bench profile_sql_result_streaming
//! LIX_SQL_PROFILE_RESULT_MODE=live LIX_SQL_PROFILE_ROW_LIMIT=100 \
//!   cargo bench -p lix --no-default-features --features storage-benches \
//!   --bench profile_sql_result_streaming
//! ```

use std::hint::black_box;
use std::time::{Duration, Instant};

use lix::Value;
use lix::integration::{Engine, SessionContext};
use lix::storage::Memory;

const DEFAULT_ROWS: usize = 131_072;
const DEFAULT_ROUNDS: usize = 9;
const DEFAULT_WARMUPS: usize = 2;
const BATCH_ROWS: usize = 500;
const PARTITIONS: usize = 8;
const TABLE_PREFIX: &str = "profile_result_row";
const REGISTER_SCHEMA_SQL: &str = "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))";
// Keep a pullable scan at the root. A global ORDER BY would make DataFusion's
// sort operator consume the complete input before emitting its first batch,
// masking whether dropping the live stream cancels the scan. The fixture uses
// distinct registered tables below so Lix's identical-scan cache cannot merge
// the children back into one eager scan.

#[derive(Clone, Copy, Debug)]
struct Config {
    rows: usize,
    rounds: usize,
    warmups: usize,
    limit: Option<usize>,
}

#[derive(Clone, Copy, Debug)]
struct Sample {
    wall: Duration,
    profile: lix::SqlReadProfile,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mode = std::env::var("LIX_SQL_PROFILE_RESULT_MODE").unwrap_or_default();
    if !matches!(
        mode.as_str(),
        "full" | "stream" | "live" | "count_only" | "fixture_only"
    ) {
        eprintln!(
            "set LIX_SQL_PROFILE_RESULT_MODE to full, stream, live, count_only, or fixture_only; \
             set LIX_SQL_PROFILE_ROW_LIMIT=100 to model early stop"
        );
        return;
    }
    let config = Config::from_env();
    let session = seed_fixture(config.rows).await;
    if mode == "fixture_only" {
        println!(
            "execute_result_streaming_profile mode=fixture_only rows={} limit=all rounds=0 \
             wall_median_us=0 profile_total_us=0 logical_us=0 physical_us=0 \
             arrow_execution_us=0 public_result_materialization_us=0 other_us=0 \
             scan_rows=0 scan_batches=0 scan_arrow_bytes=0 result_rows_consumed=0 \
             result_rows_materialized=0 result_rows_retained=0 result_checksum=0",
            config.rows
        );
        black_box(session);
        return;
    }
    let select_sql = select_sql(config.rows);
    let expected_rows = if mode == "count_only" {
        config.rows
    } else {
        config
            .limit
            .map_or(config.rows, |limit| limit.min(config.rows))
    };

    for _ in 0..config.warmups {
        let profile = session
            .execute_result_streaming_profiled(&select_sql, &[], &mode, config.limit)
            .await
            .expect("warmup profile query");
        black_box(profile);
    }

    let mut samples = Vec::with_capacity(config.rounds);
    for _ in 0..config.rounds {
        let started = Instant::now();
        let profile = session
            .execute_result_streaming_profiled(&select_sql, &[], &mode, config.limit)
            .await
            .expect("profile query");
        black_box(profile);
        samples.push(Sample {
            wall: started.elapsed(),
            profile,
        });
    }
    samples.sort_by_key(|sample| sample.wall);

    let median = samples[samples.len() / 2];
    assert_eq!(median.profile.result_rows_consumed as usize, expected_rows);
    if mode != "count_only" {
        assert_eq!(
            median.profile.result_checksum,
            expected_checksum(expected_rows),
            "result checksum must match the deterministic fixture"
        );
    }
    let expected_materialized = if mode == "count_only" {
        0
    } else if mode == "full" {
        config.rows
    } else {
        expected_rows
    };
    assert_eq!(
        median.profile.result_rows_materialized as usize,
        expected_materialized
    );
    assert_eq!(
        median.profile.result_rows_retained as usize,
        usize::from(mode == "full") * config.rows
    );
    println!(
        "execute_result_streaming_profile mode={mode} rows={} limit={} rounds={} \
         wall_median_us={:.3} profile_total_us={:.3} logical_us={:.3} physical_us={:.3} \
         arrow_execution_us={:.3} public_result_materialization_us={:.3} \
         other_us={:.3} scan_rows={} scan_batches={} scan_arrow_bytes={} \
         result_rows_consumed={} result_rows_materialized={} result_rows_retained={} \
         result_checksum={}",
        config.rows,
        config
            .limit
            .map_or_else(|| "all".to_string(), |limit| limit.to_string()),
        config.rounds,
        micros(median.wall),
        micros(median.profile.total),
        micros(median.profile.logical_planning),
        micros(median.profile.physical_planning),
        micros(median.profile.arrow_execution),
        micros(median.profile.public_result_materialization),
        micros(median.profile.unattributed_overhead()),
        median.profile.scan_rows,
        median.profile.scan_batches,
        median.profile.scan_arrow_bytes,
        median.profile.result_rows_consumed,
        median.profile.result_rows_materialized,
        median.profile.result_rows_retained,
        median.profile.result_checksum,
    );
}

fn expected_checksum(rows: usize) -> u64 {
    (0..rows).fold(0u64, |checksum, ordinal| {
        fixture_row_checksum(
            checksum,
            &format!("row-{ordinal:08}"),
            ordinal as i64,
            &format!("payload-{ordinal:08}"),
        )
    })
}

fn fixture_row_checksum(checksum: u64, id: &str, ordinal: i64, payload: &str) -> u64 {
    let mut checksum = if checksum == 0 {
        0xcbf2_9ce4_8422_2325
    } else {
        checksum
    };
    checksum = checksum_bytes(checksum, &[0xff]);
    checksum = checksum_sized_bytes(checksum, 4, id.as_bytes());
    checksum = checksum_bytes(checksum, &[2]);
    checksum = checksum_bytes(checksum, &ordinal.to_le_bytes());
    checksum_sized_bytes(checksum, 4, payload.as_bytes())
}

fn checksum_sized_bytes(checksum: u64, tag: u8, bytes: &[u8]) -> u64 {
    let checksum = checksum_bytes(checksum, &[tag]);
    let checksum = checksum_bytes(checksum, &(bytes.len() as u64).to_le_bytes());
    checksum_bytes(checksum, bytes)
}

fn checksum_bytes(mut checksum: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        checksum ^= u64::from(*byte);
        checksum = checksum.wrapping_mul(0x0000_0100_0000_01b3);
    }
    checksum
}

fn select_sql(rows: usize) -> String {
    let rows_per_table = rows.div_ceil(PARTITIONS);
    (0..PARTITIONS)
        .filter_map(|partition| {
            let start = partition * rows_per_table;
            (start < rows).then(|| {
                let table = format!("{TABLE_PREFIX}_{partition}");
                format!("SELECT id, ordinal, payload FROM {table}")
            })
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

impl Config {
    fn from_env() -> Self {
        Self {
            rows: positive_env_usize("LIX_SQL_PROFILE_ROWS", DEFAULT_ROWS),
            rounds: positive_env_usize("LIX_SQL_PROFILE_ROUNDS", DEFAULT_ROUNDS),
            warmups: positive_env_usize("LIX_SQL_PROFILE_WARMUPS", DEFAULT_WARMUPS),
            limit: match std::env::var("LIX_SQL_PROFILE_ROW_LIMIT") {
                Ok(value) if value == "all" => None,
                Ok(value) => {
                    Some(value.parse::<usize>().expect(
                        "LIX_SQL_PROFILE_ROW_LIMIT must be a non-negative integer or 'all'",
                    ))
                }
                Err(std::env::VarError::NotPresent) => None,
                Err(std::env::VarError::NotUnicode(_)) => {
                    panic!("LIX_SQL_PROFILE_ROW_LIMIT must be valid UTF-8")
                }
            },
        }
    }
}

async fn seed_fixture(rows: usize) -> SessionContext<Memory> {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("initialize result streaming profile storage");
    let engine = Engine::new(storage)
        .await
        .expect("open result streaming profile engine");
    let session = engine
        .open_session()
        .await
        .expect("open result streaming profile session");
    let rows_per_table = rows.div_ceil(PARTITIONS);
    for partition in 0..PARTITIONS {
        let table = format!("{TABLE_PREFIX}_{partition}");
        let schema = serde_json::json!({
            "x-lix-key": table,
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "ordinal": { "type": "integer" },
                "payload": { "type": "string" }
            },
            "required": ["id", "ordinal", "payload"],
            "additionalProperties": false
        });
        session
            .execute(REGISTER_SCHEMA_SQL, &[Value::Text(schema.to_string())])
            .await
            .expect("register result streaming profile schema");

        let start = partition * rows_per_table;
        let end = (start + rows_per_table).min(rows);
        for batch_start in (start..end).step_by(BATCH_ROWS) {
            let batch_end = (batch_start + BATCH_ROWS).min(end);
            let mut sql = format!("INSERT INTO {table} (id, ordinal, payload) VALUES ");
            let mut params = Vec::with_capacity((batch_end - batch_start) * 3);
            for (offset, ordinal) in (batch_start..batch_end).enumerate() {
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
                params.push(Value::Text(format!("row-{ordinal:08}")));
                params.push(Value::Integer(ordinal as i64));
                params.push(Value::Text(format!("payload-{ordinal:08}")));
            }
            session
                .execute(&sql, &params)
                .await
                .expect("seed result streaming profile rows");
        }
    }
    session
}

fn positive_env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|value| {
            let parsed = value
                .parse::<usize>()
                .expect("profile value must be an integer");
            assert!(parsed > 0, "profile value must be positive");
            parsed
        })
        .unwrap_or(default)
}

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}
