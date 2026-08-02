mod config;
mod data;
mod duckdb;
mod lix;
mod queries;
mod result;

use std::hint::black_box;
use std::time::{Duration, Instant};

use config::Config;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let config = Config::from_env();
    eprintln!(
        "seeding deterministic TPC-H-derived data at scale factor {}",
        config.scale_factor
    );
    let duckdb = duckdb::seeded(config.scale_factor);
    #[allow(unused_mut)]
    let mut lix_fixtures = vec![lix::Fixture::rocksdb(config.scale_factor).await];
    #[cfg(feature = "slatedb")]
    lix_fixtures.push(lix::Fixture::slatedb(config.scale_factor).await);

    validate_loaded_keys(&duckdb, &lix_fixtures).await;

    let selected_query = std::env::var("LIX_TPCH_QUERY")
        .ok()
        .map(|value| value.parse::<u8>().expect("LIX_TPCH_QUERY must be 1..=22"));
    assert!(
        selected_query.is_none_or(|number| (1..=22).contains(&number)),
        "LIX_TPCH_QUERY must be 1..=22"
    );
    for query in queries::queries(config.scale_factor)
        .into_iter()
        .filter(|query| selected_query.is_none_or(|number| number == query.number))
    {
        let duckdb_rows = result::from_arrow(&duckdb::query(&duckdb, &query.sql));
        for fixture in &lix_fixtures {
            let lix_rows = result::from_lix(&fixture.query(&query.sql).await);
            if std::env::var_os("LIX_TPCH_EXPLAIN").is_some() {
                eprintln!(
                    "Lix {} Q{} EXPLAIN ANALYZE:\n{}",
                    fixture.name(),
                    query.number,
                    fixture.explain_analyze(&query.sql).await
                );
            }
            result::assert_equivalent(&format!("TPC-H Q{}", query.number), &lix_rows, &duckdb_rows);
        }

        // Independent warmups keep the validation work out of timed samples.
        black_box(result::from_arrow(&duckdb::query(&duckdb, &query.sql)));
        for fixture in &lix_fixtures {
            black_box(result::from_lix(&fixture.query(&query.sql).await));
        }

        let mut duckdb_samples = Vec::with_capacity(config.samples);
        let mut lix_samples = (0..lix_fixtures.len())
            .map(|_| Vec::with_capacity(config.samples))
            .collect::<Vec<_>>();
        for sample in 0..config.samples {
            if sample % 2 == 0 {
                let started = Instant::now();
                black_box(result::from_arrow(&duckdb::query(&duckdb, &query.sql)));
                duckdb_samples.push(started.elapsed());
            }
            for fixture_index in if sample % 2 == 0 {
                (0..lix_fixtures.len()).collect::<Vec<_>>()
            } else {
                (0..lix_fixtures.len()).rev().collect::<Vec<_>>()
            } {
                let started = Instant::now();
                black_box(result::from_lix(
                    &lix_fixtures[fixture_index].query(&query.sql).await,
                ));
                lix_samples[fixture_index].push(started.elapsed());
            }
            if sample % 2 != 0 {
                let started = Instant::now();
                black_box(result::from_arrow(&duckdb::query(&duckdb, &query.sql)));
                duckdb_samples.push(started.elapsed());
            }
        }
        let duckdb_median = config::median(&duckdb_samples);

        for (fixture, lix_samples) in lix_fixtures.iter().zip(lix_samples) {
            let lix_median = config::median(&lix_samples);
            println!(
                "{}",
                serde_json::json!({
                    "suite": "tpch-derived-common-types",
                    "scale_factor": config.scale_factor,
                    "query": query.number,
                    "backend": fixture.name(),
                    "threads": 1,
                    "boundary": "rust_owned_rows_end_to_end",
                    "samples": config.samples,
                    "duckdb_samples_ms": duckdb_samples.iter().copied().map(millis).collect::<Vec<_>>(),
                    "lix_samples_ms": lix_samples.iter().copied().map(millis).collect::<Vec<_>>(),
                    "duckdb_median_ms": millis(duckdb_median),
                    "duckdb_p90_ms": millis(config::p90(&duckdb_samples)),
                    "duckdb_mad_ms": millis(config::median_absolute_deviation(&duckdb_samples)),
                    "lix_median_ms": millis(lix_median),
                    "lix_p90_ms": millis(config::p90(&lix_samples)),
                    "lix_mad_ms": millis(config::median_absolute_deviation(&lix_samples)),
                    "lix_over_duckdb": lix_median.as_secs_f64() / duckdb_median.as_secs_f64(),
                })
            );
        }
    }
}

async fn validate_loaded_keys(duckdb: &::duckdb::Connection, fixtures: &[lix::Fixture]) {
    const TABLE_KEYS: [(&str, &str); 8] = [
        ("region", "SUM(r_regionkey)"),
        ("nation", "SUM(n_nationkey), SUM(n_regionkey)"),
        ("supplier", "SUM(s_suppkey), SUM(s_nationkey)"),
        ("part", "SUM(p_partkey)"),
        ("partsupp", "SUM(ps_partkey), SUM(ps_suppkey)"),
        ("customer", "SUM(c_custkey), SUM(c_nationkey)"),
        ("orders", "SUM(o_orderkey), SUM(o_custkey)"),
        ("lineitem", "SUM(l_orderkey), SUM(l_linenumber)"),
    ];
    for (table, key_sums) in TABLE_KEYS {
        let sql = format!("SELECT COUNT(*), {key_sums} FROM {table}");
        let duckdb_rows = result::from_arrow(&duckdb::query(duckdb, &sql));
        for fixture in fixtures {
            let lix_rows = result::from_lix(&fixture.query(&sql).await);
            result::assert_equivalent(
                &format!("TPC-H {table} load checksum"),
                &lix_rows,
                &duckdb_rows,
            );
        }
    }
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
