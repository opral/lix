//! Does an insert's cost depend on how many rows the collection already holds?
//!
//! `validate_committed_unique_constraints` scans committed rows with an empty
//! `row_pks` list and parses every snapshot it reads, so an insert into a
//! schema that declares `x-lix-unique` may already pay a full collection scan.
//! Foreign keys that reference a parent primary key resolve to a row-pk
//! point lookup instead and should be flat.
//!
//! That difference decides whether a declared-column index is a read win paid
//! for on the write path, or a win on both. It is measured here rather than
//! argued, before any index exists.
//!
//! Each lane seeds N rows and then times individual inserts past that point, so
//! the reported number is the marginal cost of one insert into a collection of
//! that size — not an amortized average over the seed.
//!
//! `pk_only` is the **null control**: its schema declares no unique group and
//! no foreign key, so it has no indexed column and executes no changed code in
//! either arm. Whatever spread it shows between arms is this harness's noise
//! floor, and a delta on another lane that is smaller than that spread is not
//! a result.
//!
//! Usage: `exppq_write_scaling [sizes_csv] [measured_inserts] [backend]`
//! (defaults: `10,100,500,2000`, 40, and `rocksdb`).

use std::time::Instant;

use lix::Value;
use lix::storage::Storage;
use lix::{Lix, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

#[derive(Clone, Copy, PartialEq)]
enum Lane {
    /// Primary key only: no inter-row constraint to validate.
    PkOnly,
    /// Declares `x-lix-unique` on a non-primary-key column.
    Unique,
    /// Declares a foreign key onto a parent's primary key.
    ForeignKey,
}

impl Lane {
    fn label(self) -> &'static str {
        match self {
            Self::PkOnly => "pk_only",
            Self::Unique => "unique_declared",
            Self::ForeignKey => "fk_declared",
        }
    }
}

const LANES: [Lane; 3] = [Lane::PkOnly, Lane::Unique, Lane::ForeignKey];

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let sizes = args
        .next()
        .unwrap_or_else(|| "10,100,500,2000".to_string())
        .split(',')
        .map(|value| value.trim().parse::<usize>().expect("size"))
        .collect::<Vec<_>>();
    let measured: usize = args
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(40);
    let backend = args.next().unwrap_or_else(|| "rocksdb".to_owned());

    println!(
        "# exppq write scaling | measured_inserts={measured} | sizes={sizes:?} | storage={backend}"
    );
    for lane in LANES {
        for &size in &sizes {
            match backend.as_str() {
                "rocksdb" => {
                    let directory = tempfile::tempdir().expect("create RocksDB directory");
                    let storage = RocksDB::open(directory.path()).expect("open RocksDB");
                    run_lane(storage, lane, size, measured).await;
                }
                "memory" => {
                    run_lane(lix::Memory::default(), lane, size, measured).await;
                }
                "slatedb" => {
                    let directory = tempfile::tempdir().expect("create SlateDB directory");
                    let storage = SlateDB::open(directory.path()).expect("open SlateDB");
                    run_lane(storage, lane, size, measured).await;
                }
                other => panic!("unknown backend '{other}', expected rocksdb, slatedb or memory"),
            }
        }
    }
}

async fn run_lane<S>(storage: S, lane: Lane, seeded: usize, measured: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    open_lix()
        .with_storage(storage.clone())
        .await
        .expect("initialize fixture");
    let lix = open_lix().with_storage(storage).await.expect("open lix");
    let session = lix.open_another_session().await.expect("open session");

    for schema in schemas() {
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("register schema");
    }

    // A declared non-PK scalar forces the committed constraint-value bridge.
    if lane == Lane::ForeignKey {
        for index in 0..2 {
            session
                .execute(
                    "INSERT INTO w_fk_parent (id, code) VALUES ($1, $2)",
                    &[
                        Value::Text(format!("parent-id-{index}")),
                        Value::Text(format!("parent-{index}")),
                    ],
                )
                .await
                .expect("insert parent");
        }
    }

    for index in 0..seeded {
        insert_row(&session, lane, index).await;
    }

    let mut samples = Vec::with_capacity(measured);
    let mut phase_ratios = Vec::with_capacity(measured);
    let mut accounting = lix::storage_bench::ConstraintValidationAccounting::default();
    for step in 0..measured {
        let index = seeded + step;
        let _ = lix::storage_bench::take_constraint_validation_accounting();
        let started = Instant::now();
        insert_row(&session, lane, index).await;
        let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
        let sample = lix::storage_bench::take_constraint_validation_accounting();
        phase_ratios.push(validation_phase_ratios(sample, wall_us));
        add_validation_accounting(&mut accounting, sample);
        samples.push(wall_us);
    }
    samples.sort_by(|left, right| left.partial_cmp(right).expect("no NaN timings"));
    phase_ratios.sort_by(|left, right| left.0.partial_cmp(&right.0).expect("no NaN ratios"));
    println!(
        "op=insert lane={} seeded_rows={seeded} measured={measured} p50_us={:.1} p95_us={:.1} min_us={:.1} constraint_scan_calls={} constraint_scan_rows={} constraint_scan_us={:.1} select_rows={} select_us={:.1} json_parse_calls={} json_parse_bytes={} json_parse_us={:.1} median_scan_pct={:.3} median_select_pct={:.3} median_json_pct={:.3} median_combined_pct={:.3}",
        lane.label(),
        percentile(&samples, 0.50),
        percentile(&samples, 0.95),
        samples[0],
        accounting.committed_scan_calls,
        accounting.committed_scan_rows,
        accounting.committed_scan_ns as f64 / 1_000.0,
        accounting.materialized_select_rows,
        accounting.materialized_select_ns as f64 / 1_000.0,
        accounting.json_parse_calls,
        accounting.json_parse_bytes,
        accounting.json_parse_ns as f64 / 1_000.0,
        median_ratio(&phase_ratios, 0),
        median_ratio(&phase_ratios, 1),
        median_ratio(&phase_ratios, 2),
        median_combined_ratio(&phase_ratios),
    );

    if lane == Lane::ForeignKey {
        let mut updates = Vec::with_capacity(measured);
        let mut phase_ratios = Vec::with_capacity(measured);
        let mut accounting = lix::storage_bench::ConstraintValidationAccounting::default();
        for step in 0..measured {
            let _ = lix::storage_bench::take_constraint_validation_accounting();
            let started = Instant::now();
            session
                .execute(
                    "UPDATE w_fk_child SET parent_code = $1 WHERE id = $2",
                    &[
                        Value::Text("parent-1".into()),
                        Value::Text(format!("row-{}", seeded + step)),
                    ],
                )
                .await
                .expect("update fk row");
            let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
            let sample = lix::storage_bench::take_constraint_validation_accounting();
            phase_ratios.push(validation_phase_ratios(sample, wall_us));
            add_validation_accounting(&mut accounting, sample);
            updates.push(wall_us);
        }
        updates.sort_by(|left, right| left.partial_cmp(right).expect("no NaN timings"));
        phase_ratios.sort_by(|left, right| left.0.partial_cmp(&right.0).expect("no NaN ratios"));
        println!(
            "op=update lane={} seeded_rows={seeded} measured={measured} p50_us={:.1} p95_us={:.1} min_us={:.1} constraint_scan_calls={} constraint_scan_rows={} constraint_scan_us={:.1} select_rows={} select_us={:.1} json_parse_calls={} json_parse_bytes={} json_parse_us={:.1} median_scan_pct={:.3} median_select_pct={:.3} median_json_pct={:.3} median_combined_pct={:.3}",
            lane.label(),
            percentile(&updates, 0.50),
            percentile(&updates, 0.95),
            updates[0],
            accounting.committed_scan_calls,
            accounting.committed_scan_rows,
            accounting.committed_scan_ns as f64 / 1_000.0,
            accounting.materialized_select_rows,
            accounting.materialized_select_ns as f64 / 1_000.0,
            accounting.json_parse_calls,
            accounting.json_parse_bytes,
            accounting.json_parse_ns as f64 / 1_000.0,
            median_ratio(&phase_ratios, 0),
            median_ratio(&phase_ratios, 1),
            median_ratio(&phase_ratios, 2),
            median_combined_ratio(&phase_ratios),
        );
        let error = session
            .execute(
                "INSERT INTO w_fk_child (id, parent_code) VALUES ($1, $2)",
                &[
                    Value::Text("invalid-child".into()),
                    Value::Text("missing-parent".into()),
                ],
            )
            .await
            .expect_err("missing FK target must fail");
        assert_eq!(error.code, lix::LixError::CODE_FOREIGN_KEY);
    }
}

fn validation_phase_ratios(
    sample: lix::storage_bench::ConstraintValidationAccounting,
    wall_us: f64,
) -> (f64, f64, f64) {
    let wall_ns = wall_us * 1_000.0;
    (
        sample.committed_scan_ns as f64 * 100.0 / wall_ns,
        sample.materialized_select_ns as f64 * 100.0 / wall_ns,
        sample.json_parse_ns as f64 * 100.0 / wall_ns,
    )
}

fn median_ratio(samples: &[(f64, f64, f64)], field: usize) -> f64 {
    let mut values = samples
        .iter()
        .map(|sample| match field {
            0 => sample.0,
            1 => sample.1,
            _ => sample.2,
        })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| left.partial_cmp(right).expect("no NaN ratios"));
    percentile(&values, 0.50)
}

fn median_combined_ratio(samples: &[(f64, f64, f64)]) -> f64 {
    let mut values = samples
        .iter()
        .map(|sample| sample.0 + sample.1 + sample.2)
        .collect::<Vec<_>>();
    values.sort_by(|left, right| left.partial_cmp(right).expect("no NaN ratios"));
    percentile(&values, 0.50)
}

fn add_validation_accounting(
    total: &mut lix::storage_bench::ConstraintValidationAccounting,
    sample: lix::storage_bench::ConstraintValidationAccounting,
) {
    total.committed_scan_calls += sample.committed_scan_calls;
    total.committed_scan_rows += sample.committed_scan_rows;
    total.committed_scan_ns += sample.committed_scan_ns;
    total.materialized_select_rows += sample.materialized_select_rows;
    total.materialized_select_ns += sample.materialized_select_ns;
    total.json_parse_calls += sample.json_parse_calls;
    total.json_parse_bytes += sample.json_parse_bytes;
    total.json_parse_ns += sample.json_parse_ns;
}

async fn insert_row<S>(session: &Lix<S>, lane: Lane, index: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    match lane {
        Lane::PkOnly => {
            session
                .execute(
                    "INSERT INTO w_pk_only (id, payload) VALUES ($1, $2)",
                    &[
                        Value::Text(format!("row-{index}")),
                        Value::Text(format!("payload-{index}")),
                    ],
                )
                .await
                .expect("insert pk_only row");
        }
        Lane::Unique => {
            session
                .execute(
                    "INSERT INTO w_unique (id, slug) VALUES ($1, $2)",
                    &[
                        Value::Text(format!("row-{index}")),
                        Value::Text(format!("slug-{index}")),
                    ],
                )
                .await
                .expect("insert unique row");
        }
        Lane::ForeignKey => {
            session
                .execute(
                    "INSERT INTO w_fk_child (id, parent_code) VALUES ($1, $2)",
                    &[
                        Value::Text(format!("row-{index}")),
                        Value::Text("parent-0".into()),
                    ],
                )
                .await
                .expect("insert fk row");
        }
    }
}

fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let position = ((sorted.len() - 1) as f64 * quantile).round() as usize;
    sorted[position]
}

fn schemas() -> [serde_json::Value; 4] {
    [
        serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "w_pk_only",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "payload", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        }),
        serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "w_unique",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "slug", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
            "unique": [["slug"]],
        }),
        serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "w_fk_parent",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "code", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
            "unique": [["code"]],
        }),
        serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "w_fk_child",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "parent_code", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
            "foreign_keys": [{
                "columns": ["parent_code"],
                "references": { "schema_key": "w_fk_parent", "columns": ["code"] }
            }],
        }),
    ]
}
