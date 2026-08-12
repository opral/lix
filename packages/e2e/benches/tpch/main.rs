mod config;
mod data;
mod duckdb;
mod lix;
mod overlay;
mod queries;
mod result;

use std::hint::black_box;
use std::time::{Duration, Instant};

use config::Config;

#[derive(Clone, Copy)]
struct DuckDbPhaseSample {
    prepare: Duration,
    query_arrow: Duration,
    arrow_collection: Duration,
    owned_row_materialization: Duration,
}

#[derive(Clone, Copy)]
struct LixPhaseSample {
    profile: ::lix::SqlReadProfile,
    common_owned_row_normalization: Duration,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let config = Config::from_env();
    eprintln!(
        "seeding deterministic TPC-H-derived data at scale factor {} with {} overlay",
        config.scale_factor,
        config.overlay.name()
    );
    let overlay = config.overlay.lineitem_divisor().map(|divisor| {
        overlay::select_every_nth(
            data::lineitems(config.scale_factor).map(|row| row.rowkey),
            divisor,
        )
    });
    if let Some(overlay) = &overlay {
        assert!(
            !overlay.items.is_empty(),
            "TPC-H overlay must update at least one row"
        );
    }
    let overlay_rowkeys = overlay
        .as_ref()
        .map_or([].as_slice(), |overlay| overlay.items.as_slice());
    let duckdb = duckdb::seeded(config.scale_factor, overlay_rowkeys);
    #[allow(unused_mut)]
    let mut lix_fixtures = vec![lix::Fixture::rocksdb(config.scale_factor, overlay_rowkeys).await];
    #[cfg(feature = "slatedb")]
    lix_fixtures.push(lix::Fixture::slatedb(config.scale_factor, overlay_rowkeys).await);

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
        let mut duckdb_phase_samples = Vec::with_capacity(config.samples);
        let mut lix_samples = (0..lix_fixtures.len())
            .map(|_| Vec::with_capacity(config.samples))
            .collect::<Vec<_>>();
        let mut lix_phase_samples = (0..lix_fixtures.len())
            .map(|_| Vec::with_capacity(config.samples))
            .collect::<Vec<_>>();
        for sample in 0..config.samples {
            if sample % 2 == 0 {
                let started = Instant::now();
                let profiled = duckdb::query_profiled(&duckdb, &query.sql);
                let materialization_started = Instant::now();
                black_box(result::from_arrow(&profiled.batches));
                let owned_row_materialization = materialization_started.elapsed();
                duckdb_samples.push(started.elapsed());
                duckdb_phase_samples.push(DuckDbPhaseSample {
                    prepare: profiled.prepare,
                    query_arrow: profiled.query_arrow,
                    arrow_collection: profiled.arrow_collection,
                    owned_row_materialization,
                });
            }
            for fixture_index in if sample % 2 == 0 {
                (0..lix_fixtures.len()).collect::<Vec<_>>()
            } else {
                (0..lix_fixtures.len()).rev().collect::<Vec<_>>()
            } {
                let started = Instant::now();
                let (result, profile) =
                    lix_fixtures[fixture_index].query_profiled(&query.sql).await;
                let normalization_started = Instant::now();
                black_box(result::from_lix(&result));
                let common_owned_row_normalization = normalization_started.elapsed();
                lix_samples[fixture_index].push(started.elapsed());
                lix_phase_samples[fixture_index].push(LixPhaseSample {
                    profile,
                    common_owned_row_normalization,
                });
            }
            if sample % 2 != 0 {
                let started = Instant::now();
                let profiled = duckdb::query_profiled(&duckdb, &query.sql);
                let materialization_started = Instant::now();
                black_box(result::from_arrow(&profiled.batches));
                let owned_row_materialization = materialization_started.elapsed();
                duckdb_samples.push(started.elapsed());
                duckdb_phase_samples.push(DuckDbPhaseSample {
                    prepare: profiled.prepare,
                    query_arrow: profiled.query_arrow,
                    arrow_collection: profiled.arrow_collection,
                    owned_row_materialization,
                });
            }
        }
        let duckdb_median = config::median(&duckdb_samples);

        for ((fixture, lix_samples), lix_phase_samples) in
            lix_fixtures.iter().zip(lix_samples).zip(lix_phase_samples)
        {
            let lix_median = config::median(&lix_samples);
            println!(
                "{}",
                serde_json::json!({
                    "suite": "tpch-derived-common-types",
                    "scale_factor": config.scale_factor,
                    "overlay": config.overlay.name(),
                    "overlay_rows": overlay.as_ref().map_or(0, |overlay| overlay.items.len()),
                    "overlay_fraction": overlay.as_ref().map_or(0.0, |overlay| {
                        overlay.items.len() as f64 / overlay.total_rows as f64
                    }),
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
                    "duckdb_phase_contract": "prepare; query_arrow=execution_plus_internal_result; arrow_collection; owned_rows",
                    "duckdb_phase_samples_ms": duckdb_phase_samples.iter().map(|sample| serde_json::json!({
                        "prepare": millis(sample.prepare),
                        "query_arrow_execution_plus_internal_result": millis(sample.query_arrow),
                        "arrow_collection": millis(sample.arrow_collection),
                        "owned_row_materialization": millis(sample.owned_row_materialization),
                    })).collect::<Vec<_>>(),
                    "duckdb_prepare_median_ms": median_millis(duckdb_phase_samples.iter().map(|sample| sample.prepare)),
                    "duckdb_query_arrow_median_ms": median_millis(duckdb_phase_samples.iter().map(|sample| sample.query_arrow)),
                    "duckdb_arrow_collection_median_ms": median_millis(duckdb_phase_samples.iter().map(|sample| sample.arrow_collection)),
                    "duckdb_owned_row_materialization_median_ms": median_millis(duckdb_phase_samples.iter().map(|sample| sample.owned_row_materialization)),
                    "lix_median_ms": millis(lix_median),
                    "lix_p90_ms": millis(config::p90(&lix_samples)),
                    "lix_mad_ms": millis(config::median_absolute_deviation(&lix_samples)),
                    "lix_phase_contract": "four disjoint wall phases; scan_elapsed is overlapping summed operator poll time",
                    "lix_phase_samples": lix_phase_samples.iter().map(|sample| serde_json::json!({
                        "logical_planning_ms": millis(sample.profile.logical_planning),
                        "physical_planning_ms": millis(sample.profile.physical_planning),
                        "arrow_execution_ms": millis(sample.profile.arrow_execution),
                        "public_result_materialization_ms": millis(sample.profile.public_result_materialization),
                        "unattributed_overhead_ms": millis(sample.profile.unattributed_overhead()),
                        "common_owned_row_normalization_ms": millis(sample.common_owned_row_normalization),
                        "scan_elapsed_operator_sum_ms": millis(sample.profile.scan_elapsed),
                        "scan_rows": sample.profile.scan_rows,
                        "scan_batches": sample.profile.scan_batches,
                        "scan_arrow_bytes": sample.profile.scan_arrow_bytes,
                    })).collect::<Vec<_>>(),
                    "lix_logical_planning_median_ms": median_millis(lix_phase_samples.iter().map(|sample| sample.profile.logical_planning)),
                    "lix_physical_planning_median_ms": median_millis(lix_phase_samples.iter().map(|sample| sample.profile.physical_planning)),
                    "lix_arrow_execution_median_ms": median_millis(lix_phase_samples.iter().map(|sample| sample.profile.arrow_execution)),
                    "lix_public_result_materialization_median_ms": median_millis(lix_phase_samples.iter().map(|sample| sample.profile.public_result_materialization)),
                    "lix_unattributed_overhead_median_ms": median_millis(lix_phase_samples.iter().map(|sample| sample.profile.unattributed_overhead())),
                    "lix_common_owned_row_normalization_median_ms": median_millis(lix_phase_samples.iter().map(|sample| sample.common_owned_row_normalization)),
                    "lix_scan_elapsed_operator_sum_median_ms": median_millis(lix_phase_samples.iter().map(|sample| sample.profile.scan_elapsed)),
                    "lix_scan_rows_median": median_u64(lix_phase_samples.iter().map(|sample| sample.profile.scan_rows)),
                    "lix_scan_batches_median": median_u64(lix_phase_samples.iter().map(|sample| sample.profile.scan_batches)),
                    "lix_scan_arrow_bytes_median": median_u64(lix_phase_samples.iter().map(|sample| sample.profile.scan_arrow_bytes)),
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
        (
            "lineitem",
            "SUM(l_orderkey), SUM(l_linenumber), SUM(l_quantity)",
        ),
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

fn median_millis(samples: impl Iterator<Item = Duration>) -> f64 {
    millis(config::median(&samples.collect::<Vec<_>>()))
}

fn median_u64(samples: impl Iterator<Item = u64>) -> u64 {
    let mut samples = samples.collect::<Vec<_>>();
    samples.sort_unstable();
    samples[samples.len() / 2]
}
