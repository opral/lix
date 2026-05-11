use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use lix_engine::{storage_bench, Backend};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value as JsonValue;
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[path = "../storage/rocksdb_backend.rs"]
mod rocksdb_backend;
#[path = "../storage/sqlite_backend.rs"]
mod sqlite_backend;

use rocksdb_backend::RocksDbBenchBackend;
use sqlite_backend::SqliteBenchBackend;

const PNPM_LOCK_JSON: &str = include_str!("../fixtures/pnpm-lock.fixture.json");
const BASELINE_ROWS: usize = 100;
const SMOKE_ROWS: usize = 1_000;
const SCALE_ROWS: usize = 10_000;
const CHANGE_ROW_DENOMINATOR: usize = 10;

#[derive(Clone)]
struct PointerRow {
    path: String,
    value_json: String,
    updated_value_json: String,
}

struct RawSqliteFixture {
    conn: Connection,
    _dir: TempDir,
}

#[derive(Clone, Copy)]
enum BackendProfile {
    Sqlite,
    RocksDb,
}

impl BackendProfile {
    fn label(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::RocksDb => "rocksdb",
        }
    }
}

fn json_pointer_physical_benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime for json_pointer physical benchmarks");
    let rows = fixture_rows();

    bench_raw_sqlite(c, &rows, BASELINE_ROWS, "baseline");
    bench_physical(c, &runtime, &rows, BASELINE_ROWS, "baseline");
    bench_raw_sqlite(c, &rows, SMOKE_ROWS, "smoke");
    bench_physical(c, &runtime, &rows, SMOKE_ROWS, "smoke");
    bench_raw_sqlite(c, &rows, SCALE_ROWS, "scale");
    bench_physical(c, &runtime, &rows, SCALE_ROWS, "scale");
}

fn bench_raw_sqlite(c: &mut Criterion, all_rows: &[PointerRow], row_count: usize, label: &str) {
    let rows = all_rows[..row_count].to_vec();
    let change_rows = changed_row_count(row_count);
    let mut group = c.benchmark_group(format!("json_pointer_physical/raw_sqlite/{label}"));
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(250));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function(
        format!("write_root_all_rows/{}", row_label(row_count)),
        |b| {
            b.iter_batched(
                prepare_raw_sqlite_empty,
                |fixture| black_box(raw_sqlite_insert_all(fixture, &rows)),
                BatchSize::LargeInput,
            )
        },
    );

    group.bench_function(
        format!("get_many_exact_keys/{}", row_label(row_count)),
        |b| {
            b.iter_batched(
                || prepare_raw_sqlite_seeded(&rows),
                |fixture| black_box(raw_sqlite_get_many_exact(fixture, &rows)),
                BatchSize::LargeInput,
            )
        },
    );

    group.bench_function(
        format!("get_many_missing_keys/{}", row_label(row_count)),
        |b| {
            b.iter_batched(
                || prepare_raw_sqlite_seeded(&rows),
                |fixture| black_box(raw_sqlite_get_many_missing(fixture, row_count)),
                BatchSize::LargeInput,
            )
        },
    );

    group.bench_function(
        format!("exists_many_exact_keys/{}", row_label(row_count)),
        |b| {
            b.iter_batched(
                || prepare_raw_sqlite_seeded(&rows),
                |fixture| black_box(raw_sqlite_exists_many(fixture, &rows)),
                BatchSize::LargeInput,
            )
        },
    );

    group.bench_function(format!("scan_keys_only/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_scan_keys_only(fixture, row_count)),
            BatchSize::LargeInput,
        )
    });

    group.bench_function(format!("scan_headers_only/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_scan_keys_only(fixture, row_count)),
            BatchSize::LargeInput,
        )
    });

    group.bench_function(format!("scan_full_rows/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_scan_full_rows(fixture, row_count)),
            BatchSize::LargeInput,
        )
    });

    group.bench_function(
        format!("prefix_scan_schema/{}", row_label(row_count)),
        |b| {
            b.iter_batched(
                || prepare_raw_sqlite_seeded(&rows),
                |fixture| black_box(raw_sqlite_scan_full_rows(fixture, row_count)),
                BatchSize::LargeInput,
            )
        },
    );

    group.bench_function(
        format!("prefix_scan_schema_file_null/{}", row_label(row_count)),
        |b| {
            b.iter_batched(
                || prepare_raw_sqlite_seeded(&rows),
                |fixture| black_box(raw_sqlite_scan_full_rows(fixture, row_count)),
                BatchSize::LargeInput,
            )
        },
    );

    group.bench_function(
        format!("write_delta_10pct_updates/{}", row_label(row_count)),
        |b| {
            b.iter_batched(
                || prepare_raw_sqlite_seeded(&rows),
                |fixture| black_box(raw_sqlite_update_first_rows(fixture, &rows, change_rows)),
                BatchSize::LargeInput,
            )
        },
    );

    group.bench_function(
        format!("write_tombstone_10pct_deletes/{}", row_label(row_count)),
        |b| {
            b.iter_batched(
                || prepare_raw_sqlite_seeded(&rows),
                |fixture| black_box(raw_sqlite_delete_first_rows(fixture, &rows, change_rows)),
                BatchSize::LargeInput,
            )
        },
    );

    group.finish();
}

fn bench_physical(
    c: &mut Criterion,
    runtime: &Runtime,
    all_rows: &[PointerRow],
    row_count: usize,
    label: &str,
) {
    let rows = all_rows[..row_count].to_vec();
    let storage_rows = storage_rows(&rows);
    let change_rows = changed_row_count(row_count);

    for profile in [BackendProfile::Sqlite, BackendProfile::RocksDb] {
        let mut group =
            c.benchmark_group(format!("json_pointer_physical/{}/{label}", profile.label()));
        group.sample_size(10);
        group.warm_up_time(Duration::from_millis(250));
        group.measurement_time(Duration::from_secs(1));

        group.bench_function(
            format!("write_root_all_rows/{}", row_label(row_count)),
            |b| {
                b.iter_batched(
                    || {
                        runtime
                            .block_on(
                                storage_bench::prepare_json_pointer_tracked_state_write_root(
                                    &storage_rows,
                                ),
                            )
                            .expect("prepare json_pointer physical write root")
                    },
                    |fixture| {
                        let backend = physical_backend(profile);
                        black_box(
                            runtime
                                .block_on(storage_bench::tracked_state_write_root_prepared(
                                    &backend, &fixture,
                                ))
                                .expect("json_pointer physical write root"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        group.bench_function(
            format!("get_many_exact_keys/{}", row_label(row_count)),
            |b| {
                b.iter_batched(
                    || prepare_physical_read(runtime, profile, &storage_rows),
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(
                                    storage_bench::json_pointer_tracked_state_get_many_prepared(
                                        &backend, &fixture,
                                    ),
                                )
                                .expect("json_pointer physical get_many"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        group.bench_function(
            format!("get_many_missing_keys/{}", row_label(row_count)),
            |b| {
                b.iter_batched(
                    || prepare_physical_read(runtime, profile, &storage_rows),
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(
                                    storage_bench::json_pointer_tracked_state_get_many_missing_prepared(
                                        &backend, &fixture,
                                    ),
                                )
                                .expect("json_pointer physical get_many missing"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        group.bench_function(format!("scan_keys_only/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || prepare_physical_read(runtime, profile, &storage_rows),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(
                                storage_bench::json_pointer_tracked_state_scan_keys_only_prepared(
                                    &backend, &fixture,
                                ),
                            )
                            .expect("json_pointer physical scan keys"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(
            format!("scan_headers_only/{}", row_label(row_count)),
            |b| {
                b.iter_batched(
                    || prepare_physical_read(runtime, profile, &storage_rows),
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(
                                    storage_bench::json_pointer_tracked_state_scan_headers_only_prepared(
                                        &backend, &fixture,
                                    ),
                                )
                                .expect("json_pointer physical scan headers"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        group.bench_function(format!("scan_full_rows/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || prepare_physical_read(runtime, profile, &storage_rows),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(
                                storage_bench::json_pointer_tracked_state_scan_full_rows_prepared(
                                    &backend, &fixture,
                                ),
                            )
                            .expect("json_pointer physical scan full rows"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("prefix_scan_schema/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || prepare_physical_read(runtime, profile, &storage_rows),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(
                                storage_bench::json_pointer_tracked_state_prefix_scan_schema_prepared(
                                    &backend, &fixture,
                                ),
                            )
                            .expect("json_pointer physical prefix schema scan"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(
            format!("prefix_scan_schema_file_null/{}", row_label(row_count)),
            |b| {
                b.iter_batched(
                    || prepare_physical_read(runtime, profile, &storage_rows),
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(
                                    storage_bench::json_pointer_tracked_state_prefix_scan_schema_file_null_prepared(
                                        &backend, &fixture,
                                    ),
                                )
                                .expect("json_pointer physical prefix schema file null scan"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        group.bench_function(
            format!("write_delta_10pct_updates/{}", row_label(row_count)),
            |b| {
                b.iter_batched(
                    || {
                        let backend = physical_backend(profile);
                        let fixture = runtime
                            .block_on(
                                storage_bench::prepare_json_pointer_tracked_state_update_rows(
                                    &backend,
                                    &storage_rows,
                                    change_rows,
                                ),
                            )
                            .expect("prepare json_pointer physical delta update");
                        (backend, fixture)
                    },
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(storage_bench::tracked_state_update_existing_prepared(
                                    &backend, &fixture,
                                ))
                                .expect("json_pointer physical delta update"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        group.bench_function(
            format!("write_tombstone_10pct_deletes/{}", row_label(row_count)),
            |b| {
                b.iter_batched(
                    || {
                        let backend = physical_backend(profile);
                        let fixture = runtime
                            .block_on(
                                storage_bench::prepare_json_pointer_tracked_state_tombstone_rows(
                                    &backend,
                                    &storage_rows,
                                    change_rows,
                                ),
                            )
                            .expect("prepare json_pointer physical tombstones");
                        (backend, fixture)
                    },
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(storage_bench::tracked_state_update_existing_prepared(
                                    &backend, &fixture,
                                ))
                                .expect("json_pointer physical tombstones"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        group.bench_function(
            format!("changed_keys_update_10pct/{}", row_label(row_count)),
            |b| {
                b.iter_batched(
                    || {
                        let backend = physical_backend(profile);
                        let fixture = runtime
                            .block_on(
                                storage_bench::prepare_json_pointer_tracked_state_diff_update_rows(
                                    &backend,
                                    &storage_rows,
                                    change_rows,
                                ),
                            )
                            .expect("prepare json_pointer physical changed keys");
                        (backend, fixture)
                    },
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(
                                    storage_bench::json_pointer_tracked_state_changed_keys_prepared(
                                        &backend, &fixture,
                                    ),
                                )
                                .expect("json_pointer physical changed keys"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        group.bench_function(
            format!("changed_keys_delta_chain_10x1pct/{}", row_label(row_count)),
            |b| {
                b.iter_batched(
                    || {
                        let backend = physical_backend(profile);
                        let fixture = runtime
                            .block_on(
                                storage_bench::prepare_json_pointer_tracked_state_diff_delta_chain(
                                    &backend,
                                    &storage_rows,
                                    10,
                                    (row_count / 100).max(1),
                                ),
                            )
                            .expect("prepare json_pointer physical delta-chain changed keys");
                        (backend, fixture)
                    },
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(
                                    storage_bench::json_pointer_tracked_state_changed_keys_prepared(
                                        &backend, &fixture,
                                    ),
                                )
                                .expect("json_pointer physical delta-chain changed keys"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        group.bench_function(
            format!("materialize_delta_chain_10x1pct/{}", row_label(row_count)),
            |b| {
                b.iter_batched(
                    || {
                        let backend = physical_backend(profile);
                        let fixture = runtime
                            .block_on(
                                storage_bench::prepare_json_pointer_tracked_state_materialize_delta_chain(
                                    &backend,
                                    &storage_rows,
                                    10,
                                    (row_count / 100).max(1),
                                ),
                            )
                            .expect("prepare json_pointer physical materialize delta chain");
                        (backend, fixture)
                    },
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(storage_bench::tracked_state_materialize_root_prepared(
                                    &backend, &fixture,
                                ))
                                .expect("json_pointer physical materialize delta chain"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        group.finish();
    }
}

fn fixture_rows() -> Vec<PointerRow> {
    let root: JsonValue = serde_json::from_str(PNPM_LOCK_JSON).expect("pnpm lock JSON fixture");
    let mut rows = Vec::new();
    flatten_json("", &root, &mut rows);
    assert!(
        rows.len() >= SCALE_ROWS,
        "pnpm lock fixture should have at least {SCALE_ROWS} pointer rows, got {}",
        rows.len()
    );
    rows
}

fn prepare_raw_sqlite_empty() -> RawSqliteFixture {
    let dir = TempDir::new().expect("create raw sqlite tempdir");
    let conn = Connection::open(dir.path().join("json-pointer-physical.sqlite"))
        .expect("open raw sqlite json_pointer physical db");
    conn.execute_batch(
        "
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
        PRAGMA temp_store = MEMORY;
        PRAGMA foreign_keys = ON;
        CREATE TABLE json_pointer (
            path TEXT NOT NULL PRIMARY KEY,
            value TEXT NOT NULL
        ) WITHOUT ROWID;
        ",
    )
    .expect("configure raw sqlite json_pointer physical db");
    RawSqliteFixture { conn, _dir: dir }
}

fn prepare_raw_sqlite_seeded(rows: &[PointerRow]) -> RawSqliteFixture {
    let fixture = prepare_raw_sqlite_empty();
    raw_sqlite_seed(&fixture.conn, rows);
    fixture
}

fn raw_sqlite_seed(conn: &Connection, rows: &[PointerRow]) {
    conn.execute_batch("BEGIN IMMEDIATE")
        .expect("begin raw sqlite seed");
    {
        let mut statement = conn
            .prepare_cached(
                "INSERT INTO json_pointer (path, value) VALUES (?1, ?2)
                 ON CONFLICT(path) DO UPDATE SET value = excluded.value",
            )
            .expect("prepare raw sqlite seed insert");
        for row in rows {
            statement
                .execute(params![row.path.as_str(), row.value_json.as_str()])
                .expect("insert raw sqlite seed row");
        }
    }
    conn.execute_batch("COMMIT")
        .expect("commit raw sqlite seed");
}

fn raw_sqlite_insert_all(fixture: RawSqliteFixture, rows: &[PointerRow]) -> usize {
    raw_sqlite_seed(&fixture.conn, rows);
    rows.len()
}

fn raw_sqlite_get_many_exact(fixture: RawSqliteFixture, rows: &[PointerRow]) -> usize {
    let mut statement = fixture
        .conn
        .prepare_cached("SELECT value FROM json_pointer WHERE path = ?1")
        .expect("prepare raw sqlite exact get");
    let mut found = 0;
    for row in rows {
        if statement
            .query_row(params![row.path.as_str()], |_| Ok(()))
            .optional()
            .expect("raw sqlite exact get")
            .is_some()
        {
            found += 1;
        }
    }
    assert_eq!(found, rows.len());
    found
}

fn raw_sqlite_get_many_missing(fixture: RawSqliteFixture, row_count: usize) -> usize {
    let mut statement = fixture
        .conn
        .prepare_cached("SELECT value FROM json_pointer WHERE path = ?1")
        .expect("prepare raw sqlite missing get");
    let mut found = 0;
    for index in 0..row_count {
        let missing_path = format!("/__missing/{index}");
        if statement
            .query_row(params![missing_path.as_str()], |_| Ok(()))
            .optional()
            .expect("raw sqlite missing get")
            .is_some()
        {
            found += 1;
        }
    }
    assert_eq!(found, 0);
    found
}

fn raw_sqlite_exists_many(fixture: RawSqliteFixture, rows: &[PointerRow]) -> usize {
    let mut statement = fixture
        .conn
        .prepare_cached("SELECT 1 FROM json_pointer WHERE path = ?1")
        .expect("prepare raw sqlite exists");
    let mut found = 0;
    for row in rows {
        if statement
            .query_row(params![row.path.as_str()], |_| Ok(()))
            .optional()
            .expect("raw sqlite exists")
            .is_some()
        {
            found += 1;
        }
    }
    assert_eq!(found, rows.len());
    found
}

fn raw_sqlite_scan_keys_only(fixture: RawSqliteFixture, expected_rows: usize) -> usize {
    let mut statement = fixture
        .conn
        .prepare_cached("SELECT path FROM json_pointer ORDER BY path")
        .expect("prepare raw sqlite keys scan");
    let count = statement
        .query_map([], |_| Ok(()))
        .expect("raw sqlite keys scan")
        .count();
    assert_eq!(count, expected_rows);
    count
}

fn raw_sqlite_scan_full_rows(fixture: RawSqliteFixture, expected_rows: usize) -> usize {
    let mut statement = fixture
        .conn
        .prepare_cached("SELECT path, value FROM json_pointer ORDER BY path")
        .expect("prepare raw sqlite full scan");
    let count = statement
        .query_map([], |_| Ok(()))
        .expect("raw sqlite full scan")
        .count();
    assert_eq!(count, expected_rows);
    count
}

fn raw_sqlite_update_first_rows(
    fixture: RawSqliteFixture,
    rows: &[PointerRow],
    change_rows: usize,
) -> usize {
    fixture
        .conn
        .execute_batch("BEGIN IMMEDIATE")
        .expect("begin raw sqlite update");
    let mut affected = 0;
    {
        let mut statement = fixture
            .conn
            .prepare_cached("UPDATE json_pointer SET value = ?1 WHERE path = ?2")
            .expect("prepare raw sqlite update");
        for row in &rows[..change_rows] {
            affected += statement
                .execute(params![row.updated_value_json.as_str(), row.path.as_str()])
                .expect("raw sqlite update");
        }
    }
    fixture
        .conn
        .execute_batch("COMMIT")
        .expect("commit raw sqlite update");
    assert_eq!(affected, change_rows);
    affected
}

fn raw_sqlite_delete_first_rows(
    fixture: RawSqliteFixture,
    rows: &[PointerRow],
    change_rows: usize,
) -> usize {
    fixture
        .conn
        .execute_batch("BEGIN IMMEDIATE")
        .expect("begin raw sqlite delete");
    let mut affected = 0;
    {
        let mut statement = fixture
            .conn
            .prepare_cached("DELETE FROM json_pointer WHERE path = ?1")
            .expect("prepare raw sqlite delete");
        for row in &rows[..change_rows] {
            affected += statement
                .execute(params![row.path.as_str()])
                .expect("raw sqlite delete");
        }
    }
    fixture
        .conn
        .execute_batch("COMMIT")
        .expect("commit raw sqlite delete");
    assert_eq!(affected, change_rows);
    affected
}

fn storage_rows(rows: &[PointerRow]) -> Vec<storage_bench::JsonPointerStorageRow> {
    rows.iter()
        .map(|row| storage_bench::JsonPointerStorageRow {
            path: row.path.clone(),
            value_json: row.value_json.clone(),
            updated_value_json: row.updated_value_json.clone(),
        })
        .collect()
}

fn physical_backend(profile: BackendProfile) -> Arc<dyn Backend + Send + Sync> {
    match profile {
        BackendProfile::Sqlite => {
            Arc::new(SqliteBenchBackend::tempfile().expect("create sqlite physical backend"))
        }
        BackendProfile::RocksDb => {
            Arc::new(RocksDbBenchBackend::new().expect("create rocksdb physical backend"))
        }
    }
}

fn prepare_physical_read(
    runtime: &Runtime,
    profile: BackendProfile,
    rows: &[storage_bench::JsonPointerStorageRow],
) -> (
    Arc<dyn Backend + Send + Sync>,
    storage_bench::JsonPointerTrackedStateReadFixture,
) {
    let backend = physical_backend(profile);
    let fixture = runtime
        .block_on(storage_bench::prepare_json_pointer_tracked_state_read(
            &backend, rows,
        ))
        .expect("prepare json_pointer physical read");
    (backend, fixture)
}

fn flatten_json(path: &str, value: &JsonValue, rows: &mut Vec<PointerRow>) {
    rows.push(PointerRow {
        path: path.to_string(),
        value_json: value.to_string(),
        updated_value_json: updated_value_for(path),
    });

    match value {
        JsonValue::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                let child_path = format!("{path}/{}", index);
                flatten_json(&child_path, item, rows);
            }
        }
        JsonValue::Object(map) => {
            for (key, child) in map {
                let child_path = format!("{path}/{}", escape_pointer_token(key));
                flatten_json(&child_path, child, rows);
            }
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {}
    }
}

fn updated_value_for(path: &str) -> String {
    serde_json::json!({
        "updated": true,
        "path": path,
    })
    .to_string()
}

fn escape_pointer_token(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}

fn row_label(rows: usize) -> String {
    if rows >= 1_000 {
        format!("{}k", rows / 1_000)
    } else {
        rows.to_string()
    }
}

fn changed_row_count(rows: usize) -> usize {
    (rows / CHANGE_ROW_DENOMINATOR).max(1)
}

criterion_group!(benches, json_pointer_physical_benches);
criterion_main!(benches);
