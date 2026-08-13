//! Does an insert's cost depend on how many rows the collection already holds?
//!
//! `validate_committed_unique_constraints` scans committed rows with an empty
//! `entity_pks` list and parses every snapshot it reads, so an insert into a
//! schema that declares `x-lix-unique` may already pay a full collection scan.
//! Foreign keys that reference a parent primary key resolve to an entity-pk
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
                other => panic!("unknown backend '{other}', expected rocksdb or memory"),
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
                "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("register schema");
    }

    // The foreign-key lane needs exactly one parent to point at. Its cost must
    // not depend on how many parents exist, only on the child collection size.
    if lane == Lane::ForeignKey {
        session
            .execute(
                "INSERT INTO w_fk_parent (id) VALUES ($1)",
                &[Value::Text("parent-0".into())],
            )
            .await
            .expect("insert parent");
    }

    for index in 0..seeded {
        insert_row(&session, lane, index).await;
    }

    let mut samples = Vec::with_capacity(measured);
    for step in 0..measured {
        let index = seeded + step;
        let started = Instant::now();
        insert_row(&session, lane, index).await;
        samples.push(started.elapsed().as_secs_f64() * 1_000_000.0);
    }
    samples.sort_by(|left, right| left.partial_cmp(right).expect("no NaN timings"));

    println!(
        "lane={} seeded_rows={seeded} measured={measured} p50_us={:.1} p95_us={:.1} min_us={:.1}",
        lane.label(),
        percentile(&samples, 0.50),
        percentile(&samples, 0.95),
        samples[0],
    );
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
                    r#"INSERT INTO w_fk_child (id, "parentId") VALUES ($1, $2)"#,
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
            "x-lix-key": "w_pk_only",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "payload": { "type": "string" }
            },
            "required": ["id", "payload"],
            "additionalProperties": false
        }),
        serde_json::json!({
            "x-lix-key": "w_unique",
            "x-lix-primary-key": ["/id"],
            "x-lix-unique": [["/slug"]],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "slug": { "type": "string" }
            },
            "required": ["id", "slug"],
            "additionalProperties": false
        }),
        serde_json::json!({
            "x-lix-key": "w_fk_parent",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": { "id": { "type": "string" } },
            "required": ["id"],
            "additionalProperties": false
        }),
        serde_json::json!({
            "x-lix-key": "w_fk_child",
            "x-lix-primary-key": ["/id"],
            "x-lix-foreign-keys": [{
                "properties": ["/parentId"],
                "references": { "schemaKey": "w_fk_parent", "properties": ["/id"] }
            }],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "parentId": { "type": "string" }
            },
            "required": ["id", "parentId"],
            "additionalProperties": false
        }),
    ]
}
