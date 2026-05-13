use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use lix_engine::{
    storage_bench, Backend, CreateVersionOptions, Engine, MergeVersionOptions, MergeVersionOutcome,
    SessionContext, SwitchVersionOptions,
};
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

const JSON_POINTER_SCHEMA_JSON: &str =
    include_str!("../../../plugin-json-v2/schema/json_pointer.json");
const PNPM_LOCK_JSON: &str = include_str!("../fixtures/pnpm-lock.fixture.json");
const BASELINE_ROWS: usize = 100;
const SMOKE_ROWS: usize = 1_000;
const SCALE_ROWS: usize = 10_000;
const CHUNK_SIZE: usize = 500;
const CHANGE_ROW_DENOMINATOR: usize = 10;

#[derive(Clone)]
struct PointerRow {
    path: String,
    value_json: String,
    updated_value_json: String,
}

#[derive(Clone, Copy)]
enum LixBackendProfile {
    Sqlite,
    RocksDb,
}

impl LixBackendProfile {
    fn name(self) -> &'static str {
        match self {
            Self::Sqlite => "lix_sqlite",
            Self::RocksDb => "lix_rocksdb",
        }
    }

    fn backend_label(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::RocksDb => "rocksdb",
        }
    }
}

struct RawSqliteFixture {
    conn: Connection,
    _dir: TempDir,
}

struct LixFixture {
    session: SessionContext,
}

fn log12_physical_layout_benches(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create tokio runtime for log12 physical layout benchmarks");
    let rows = fixture_rows();

    bench_raw_sqlite(c, &rows, BASELINE_ROWS, "baseline");
    bench_raw_storage(c, &runtime, &rows, BASELINE_ROWS, "baseline");
    bench_lix(c, &runtime, &rows, BASELINE_ROWS, "baseline");
    bench_raw_sqlite(c, &rows, SMOKE_ROWS, "smoke");
    bench_raw_storage(c, &runtime, &rows, SMOKE_ROWS, "smoke");
    bench_lix(c, &runtime, &rows, SMOKE_ROWS, "smoke");
    bench_raw_sqlite(c, &rows, SCALE_ROWS, "scale");
    bench_raw_storage(c, &runtime, &rows, SCALE_ROWS, "scale");
    bench_lix(c, &runtime, &rows, SCALE_ROWS, "scale");
}

fn bench_raw_sqlite(c: &mut Criterion, all_rows: &[PointerRow], row_count: usize, label: &str) {
    let rows = all_rows[..row_count].to_vec();
    let mut group = c.benchmark_group(format!("log12_physical_layout/raw_sqlite/{label}"));
    group.sample_size(if row_count <= SMOKE_ROWS { 20 } else { 11 });
    group.warm_up_time(Duration::from_millis(250));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function(format!("insert_all_rows/{}", row_label(row_count)), |b| {
        b.iter_batched(
            prepare_raw_sqlite_empty,
            |fixture| black_box(raw_sqlite_insert_all(fixture, &rows)),
            BatchSize::LargeInput,
        )
    });

    group.bench_function(
        format!("select_all_path_value/{}", row_label(row_count)),
        |b| {
            b.iter_batched(
                || prepare_raw_sqlite_seeded(&rows),
                |fixture| black_box(raw_sqlite_select_all(fixture, row_count)),
                BatchSize::LargeInput,
            )
        },
    );

    group.bench_function(format!("select_one_by_pk/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_select_one_by_pk(fixture, pick_pk_row(&rows))),
            BatchSize::LargeInput,
        )
    });

    group.bench_function(format!("update_all_values/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_update_all(fixture, row_count)),
            BatchSize::LargeInput,
        )
    });

    group.bench_function(format!("update_one_by_pk/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_update_one_by_pk(fixture, pick_pk_row(&rows))),
            BatchSize::LargeInput,
        )
    });

    group.bench_function(format!("delete_all_rows/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_delete_all(fixture, row_count)),
            BatchSize::LargeInput,
        )
    });

    group.bench_function(format!("delete_one_by_pk/{}", row_label(row_count)), |b| {
        b.iter_batched(
            || prepare_raw_sqlite_seeded(&rows),
            |fixture| black_box(raw_sqlite_delete_one_by_pk(fixture, pick_pk_row(&rows))),
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

fn bench_raw_storage(
    c: &mut Criterion,
    runtime: &Runtime,
    all_rows: &[PointerRow],
    row_count: usize,
    label: &str,
) {
    let rows = all_rows[..row_count].to_vec();
    let storage_rows = storage_rows(&rows);
    let change_rows = changed_row_count(row_count);
    for profile in [LixBackendProfile::Sqlite, LixBackendProfile::RocksDb] {
        let mut group = c.benchmark_group(format!(
            "log12_physical_layout/raw_storage_{}/{label}",
            profile.backend_label()
        ));
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
                            .expect("prepare json_pointer raw storage write root")
                    },
                    |fixture| {
                        let backend = raw_storage_backend(profile);
                        black_box(
                            runtime
                                .block_on(storage_bench::tracked_state_write_root_prepared(
                                    &backend, &fixture,
                                ))
                                .expect("json_pointer raw storage write root"),
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
                    || {
                        let backend = raw_storage_backend(profile);
                        let fixture = runtime
                            .block_on(storage_bench::prepare_json_pointer_tracked_state_read(
                                &backend,
                                &storage_rows,
                            ))
                            .expect("prepare json_pointer raw storage get_many");
                        (backend, fixture)
                    },
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(
                                    storage_bench::json_pointer_tracked_state_get_many_prepared(
                                        &backend, &fixture,
                                    ),
                                )
                                .expect("json_pointer raw storage get_many"),
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
                    || prepare_raw_storage_read(runtime, profile, &storage_rows),
                    |(backend, fixture)| {
                        black_box(
                        runtime
                            .block_on(
                                storage_bench::json_pointer_tracked_state_get_many_missing_prepared(
                                    &backend, &fixture,
                                ),
                            )
                            .expect("json_pointer raw storage get_many missing"),
                    )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        group.bench_function(format!("scan_keys_only/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || prepare_raw_storage_read(runtime, profile, &storage_rows),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(
                                storage_bench::json_pointer_tracked_state_scan_keys_only_prepared(
                                    &backend, &fixture,
                                ),
                            )
                            .expect("json_pointer raw storage scan keys"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("scan_headers_only/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || prepare_raw_storage_read(runtime, profile, &storage_rows),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(
                                storage_bench::json_pointer_tracked_state_scan_headers_only_prepared(
                                    &backend, &fixture,
                                ),
                            )
                            .expect("json_pointer raw storage scan headers"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("scan_full_rows/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || prepare_raw_storage_read(runtime, profile, &storage_rows),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(
                                storage_bench::json_pointer_tracked_state_scan_full_rows_prepared(
                                    &backend, &fixture,
                                ),
                            )
                            .expect("json_pointer raw storage scan"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("prefix_scan_schema/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || prepare_raw_storage_read(runtime, profile, &storage_rows),
                |(backend, fixture)| {
                    black_box(
                        runtime
                            .block_on(
                                storage_bench::json_pointer_tracked_state_prefix_scan_schema_prepared(
                                    &backend, &fixture,
                                ),
                            )
                            .expect("json_pointer raw storage prefix schema scan"),
                    )
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(
            format!("prefix_scan_schema_file_null/{}", row_label(row_count)),
            |b| {
                b.iter_batched(
                    || prepare_raw_storage_read(runtime, profile, &storage_rows),
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(
                                    storage_bench::json_pointer_tracked_state_prefix_scan_schema_file_null_prepared(
                                        &backend, &fixture,
                                    ),
                                )
                                .expect("json_pointer raw storage prefix schema file null scan"),
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
                        let backend = raw_storage_backend(profile);
                        let fixture = runtime
                            .block_on(
                                storage_bench::prepare_json_pointer_tracked_state_update_rows(
                                    &backend,
                                    &storage_rows,
                                    change_rows,
                                ),
                            )
                            .expect("prepare json_pointer raw storage delta update");
                        (backend, fixture)
                    },
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(storage_bench::tracked_state_update_existing_prepared(
                                    &backend, &fixture,
                                ))
                                .expect("json_pointer raw storage delta update"),
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
                        let backend = raw_storage_backend(profile);
                        let fixture = runtime
                            .block_on(
                                storage_bench::prepare_json_pointer_tracked_state_tombstone_rows(
                                    &backend,
                                    &storage_rows,
                                    change_rows,
                                ),
                            )
                            .expect("prepare json_pointer raw storage tombstones");
                        (backend, fixture)
                    },
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(storage_bench::tracked_state_update_existing_prepared(
                                    &backend, &fixture,
                                ))
                                .expect("json_pointer raw storage tombstones"),
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
                        let backend = raw_storage_backend(profile);
                        let fixture = runtime
                            .block_on(
                                storage_bench::prepare_json_pointer_tracked_state_diff_update_rows(
                                    &backend,
                                    &storage_rows,
                                    change_rows,
                                ),
                            )
                            .expect("prepare json_pointer raw storage changed keys");
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
                                .expect("json_pointer raw storage changed keys"),
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
                        let backend = raw_storage_backend(profile);
                        let fixture = runtime
                            .block_on(
                                storage_bench::prepare_json_pointer_tracked_state_diff_delta_chain(
                                    &backend,
                                    &storage_rows,
                                    10,
                                    (row_count / 100).max(1),
                                ),
                            )
                            .expect("prepare json_pointer raw storage delta-chain changed keys");
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
                                .expect("json_pointer raw storage delta-chain changed keys"),
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
                        let backend = raw_storage_backend(profile);
                        let fixture = runtime
                            .block_on(
                                storage_bench::prepare_json_pointer_tracked_state_materialize_delta_chain(
                                    &backend,
                                    &storage_rows,
                                    10,
                                    (row_count / 100).max(1),
                                ),
                            )
                            .expect("prepare json_pointer raw storage materialize delta chain");
                        (backend, fixture)
                    },
                    |(backend, fixture)| {
                        black_box(
                            runtime
                                .block_on(storage_bench::tracked_state_materialize_root_prepared(
                                    &backend, &fixture,
                                ))
                                .expect("json_pointer raw storage materialize delta chain"),
                        )
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        group.finish();
    }
}

fn bench_lix(
    c: &mut Criterion,
    runtime: &Runtime,
    all_rows: &[PointerRow],
    row_count: usize,
    label: &str,
) {
    let rows = all_rows[..row_count].to_vec();
    let change_rows = changed_row_count(row_count);
    for profile in [LixBackendProfile::Sqlite, LixBackendProfile::RocksDb] {
        let mut group =
            c.benchmark_group(format!("log12_physical_layout/{}/{label}", profile.name()));
        group.sample_size(if row_count <= SMOKE_ROWS { 11 } else { 11 });
        group.warm_up_time(Duration::from_millis(250));
        group.measurement_time(Duration::from_secs(1));

        group.bench_function(format!("insert_all_rows/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || runtime.block_on(prepare_lix_empty(profile)),
                |fixture| black_box(runtime.block_on(lix_insert_all(fixture, &rows))),
                BatchSize::LargeInput,
            )
        });

        group.bench_function(
            format!("select_all_path_value/{}", row_label(row_count)),
            |b| {
                b.iter_batched(
                    || runtime.block_on(prepare_lix_seeded(profile, &rows)),
                    |fixture| black_box(runtime.block_on(lix_select_all(fixture, row_count))),
                    BatchSize::LargeInput,
                )
            },
        );

        group.bench_function(format!("select_one_by_pk/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || runtime.block_on(prepare_lix_seeded(profile, &rows)),
                |fixture| {
                    black_box(runtime.block_on(lix_select_one_by_pk(fixture, pick_pk_row(&rows))))
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("update_all_values/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || runtime.block_on(prepare_lix_seeded(profile, &rows)),
                |fixture| black_box(runtime.block_on(lix_update_all(fixture, row_count))),
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("update_one_by_pk/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || runtime.block_on(prepare_lix_seeded(profile, &rows)),
                |fixture| {
                    black_box(runtime.block_on(lix_update_one_by_pk(fixture, pick_pk_row(&rows))))
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("delete_all_rows/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || runtime.block_on(prepare_lix_seeded(profile, &rows)),
                |fixture| black_box(runtime.block_on(lix_delete_all(fixture, row_count))),
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("delete_one_by_pk/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || runtime.block_on(prepare_lix_seeded(profile, &rows)),
                |fixture| {
                    black_box(runtime.block_on(lix_delete_one_by_pk(fixture, pick_pk_row(&rows))))
                },
                BatchSize::LargeInput,
            )
        });

        group.bench_function(format!("create_version/{}", row_label(row_count)), |b| {
            b.iter_batched(
                || runtime.block_on(prepare_lix_seeded(profile, &rows)),
                |fixture| black_box(runtime.block_on(lix_create_version(fixture))),
                BatchSize::LargeInput,
            )
        });

        group.bench_function(
            format!(
                "merge_version_fast_forward_10pct_updates/{}",
                row_label(row_count)
            ),
            |b| {
                b.iter_batched(
                    || runtime.block_on(prepare_lix_seeded(profile, &rows)),
                    |fixture| {
                        black_box(runtime.block_on(lix_merge_version_fast_forward(
                            fixture,
                            &rows,
                            change_rows,
                        )))
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        group.bench_function(
            format!(
                "merge_version_divergent_10pct_updates/{}",
                row_label(row_count)
            ),
            |b| {
                b.iter_batched(
                    || runtime.block_on(prepare_lix_seeded(profile, &rows)),
                    |fixture| {
                        black_box(runtime.block_on(lix_merge_version_divergent(
                            fixture,
                            &rows,
                            change_rows,
                        )))
                    },
                    BatchSize::LargeInput,
                )
            },
        );

        group.finish();
    }
}

fn prepare_raw_sqlite_empty() -> RawSqliteFixture {
    let dir = TempDir::new().expect("create raw sqlite tempdir");
    let conn = Connection::open(dir.path().join("log12-physical-layout.sqlite"))
        .expect("open raw sqlite log12 physical layout db");
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
    .expect("configure raw sqlite log12 physical layout db");
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

fn raw_sqlite_select_all(fixture: RawSqliteFixture, expected_rows: usize) -> usize {
    let mut statement = fixture
        .conn
        .prepare_cached("SELECT path, value FROM json_pointer ORDER BY path")
        .expect("prepare raw sqlite select all");
    let count = statement
        .query_map([], |_| Ok(()))
        .expect("raw sqlite select all")
        .count();
    assert_eq!(count, expected_rows);
    count
}

fn raw_sqlite_select_one_by_pk(fixture: RawSqliteFixture, row: &PointerRow) -> usize {
    let mut statement = fixture
        .conn
        .prepare_cached("SELECT path, value FROM json_pointer WHERE path = ?1")
        .expect("prepare raw sqlite select by pk");
    let found = statement
        .query_row(params![row.path.as_str()], |_| Ok(()))
        .optional()
        .expect("raw sqlite select by pk")
        .is_some();
    assert!(found);
    usize::from(found)
}

fn raw_sqlite_update_all(fixture: RawSqliteFixture, expected_rows: usize) -> usize {
    let affected = fixture
        .conn
        .execute(
            "UPDATE json_pointer SET value = ?1",
            params![r#"{"updated":true}"#],
        )
        .expect("raw sqlite update all");
    assert_eq!(affected, expected_rows);
    affected
}

fn raw_sqlite_update_one_by_pk(fixture: RawSqliteFixture, row: &PointerRow) -> usize {
    let affected = fixture
        .conn
        .execute(
            "UPDATE json_pointer SET value = ?1 WHERE path = ?2",
            params![row.updated_value_json.as_str(), row.path.as_str()],
        )
        .expect("raw sqlite update by pk");
    assert_eq!(affected, 1);
    affected
}

fn raw_sqlite_delete_all(fixture: RawSqliteFixture, expected_rows: usize) -> usize {
    let affected = fixture
        .conn
        .execute("DELETE FROM json_pointer", [])
        .expect("raw sqlite delete all");
    assert_eq!(affected, expected_rows);
    affected
}

fn raw_sqlite_delete_one_by_pk(fixture: RawSqliteFixture, row: &PointerRow) -> usize {
    let affected = fixture
        .conn
        .execute(
            "DELETE FROM json_pointer WHERE path = ?1",
            params![row.path.as_str()],
        )
        .expect("raw sqlite delete by pk");
    assert_eq!(affected, 1);
    affected
}

async fn prepare_lix_empty(profile: LixBackendProfile) -> LixFixture {
    let engine = match profile {
        LixBackendProfile::Sqlite => {
            let backend = SqliteBenchBackend::tempfile()
                .expect("create sqlite log12 physical layout backend");
            Engine::initialize(Box::new(backend.clone()))
                .await
                .expect("initialize sqlite log12 physical layout Lix backend");
            Engine::new(Box::new(backend))
                .await
                .expect("open sqlite log12 physical layout Lix engine")
        }
        LixBackendProfile::RocksDb => {
            let backend =
                RocksDbBenchBackend::new().expect("create rocksdb log12 physical layout backend");
            Engine::initialize(Box::new(backend.clone()))
                .await
                .expect("initialize rocksdb log12 physical layout Lix backend");
            Engine::new(Box::new(backend))
                .await
                .expect("open rocksdb log12 physical layout Lix engine")
        }
    };
    let setup_session = engine
        .open_workspace_session()
        .await
        .expect("open log12 physical layout Lix setup workspace session");
    register_json_pointer_schema(&setup_session).await;
    let session = engine
        .open_workspace_session()
        .await
        .expect("open log12 physical layout Lix benchmark workspace session");
    LixFixture { session }
}

async fn prepare_lix_seeded(profile: LixBackendProfile, rows: &[PointerRow]) -> LixFixture {
    let fixture = prepare_lix_empty(profile).await;
    insert_lix_rows(&fixture.session, rows).await;
    fixture
}

async fn register_json_pointer_schema(session: &SessionContext) {
    let sql = format!(
        "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked)
         VALUES (lix_json('{}'), false, false)",
        sql_string(JSON_POINTER_SCHEMA_JSON)
    );
    let affected = session
        .execute(&sql, &[])
        .await
        .expect("register json_pointer schema")
        .rows_affected();
    assert_eq!(affected, 1);
}

async fn lix_insert_all(fixture: LixFixture, rows: &[PointerRow]) -> usize {
    insert_lix_rows(&fixture.session, rows).await;
    rows.len()
}

async fn insert_lix_rows(session: &SessionContext, rows: &[PointerRow]) {
    for chunk in rows.chunks(CHUNK_SIZE) {
        let mut sql = String::from("INSERT INTO json_pointer (path, value) VALUES ");
        for (index, row) in chunk.iter().enumerate() {
            if index > 0 {
                sql.push(',');
            }
            sql.push_str(&format!(
                "('{}', lix_json('{}'))",
                sql_string(row.path.as_str()),
                sql_string(row.value_json.as_str())
            ));
        }
        let affected = session
            .execute(&sql, &[])
            .await
            .expect("insert json_pointer rows")
            .rows_affected();
        assert_eq!(affected as usize, chunk.len());
    }
}

async fn lix_select_all(fixture: LixFixture, expected_rows: usize) -> usize {
    let result = fixture
        .session
        .execute("SELECT path, value FROM json_pointer ORDER BY path", &[])
        .await
        .expect("select all json_pointer rows");
    assert_eq!(result.len(), expected_rows);
    result.len()
}

async fn lix_select_one_by_pk(fixture: LixFixture, row: &PointerRow) -> usize {
    let sql = format!(
        "SELECT path, value FROM json_pointer WHERE path = '{}'",
        sql_string(row.path.as_str())
    );
    let result = fixture
        .session
        .execute(&sql, &[])
        .await
        .expect("select json_pointer row by path");
    assert_eq!(result.len(), 1);
    result.len()
}

async fn lix_update_all(fixture: LixFixture, expected_rows: usize) -> usize {
    let affected = fixture
        .session
        .execute(
            r#"UPDATE json_pointer SET value = lix_json('{"updated":true}')"#,
            &[],
        )
        .await
        .expect("update all json_pointer rows")
        .rows_affected() as usize;
    assert_eq!(affected, expected_rows);
    affected
}

async fn lix_update_one_by_pk(fixture: LixFixture, row: &PointerRow) -> usize {
    let sql = format!(
        "UPDATE json_pointer SET value = lix_json('{}') WHERE path = '{}'",
        sql_string(row.updated_value_json.as_str()),
        sql_string(row.path.as_str())
    );
    let affected = fixture
        .session
        .execute(&sql, &[])
        .await
        .expect("update json_pointer row by path")
        .rows_affected() as usize;
    assert_eq!(affected, 1);
    affected
}

async fn lix_delete_all(fixture: LixFixture, expected_rows: usize) -> usize {
    let affected = fixture
        .session
        .execute("DELETE FROM json_pointer", &[])
        .await
        .expect("delete all json_pointer rows")
        .rows_affected() as usize;
    assert_eq!(affected, expected_rows);
    affected
}

async fn lix_delete_one_by_pk(fixture: LixFixture, row: &PointerRow) -> usize {
    let sql = format!(
        "DELETE FROM json_pointer WHERE path = '{}'",
        sql_string(row.path.as_str())
    );
    let affected = fixture
        .session
        .execute(&sql, &[])
        .await
        .expect("delete json_pointer row by path")
        .rows_affected() as usize;
    assert_eq!(affected, 1);
    affected
}

async fn lix_create_version(fixture: LixFixture) -> String {
    create_lix_version(&fixture.session).await
}

async fn create_lix_version(session: &SessionContext) -> String {
    let receipt = session
        .create_version(CreateVersionOptions {
            id: Some("bench-draft".to_string()),
            name: "bench draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("create json_pointer benchmark version");
    receipt.id
}

async fn lix_merge_version_fast_forward(
    fixture: LixFixture,
    rows: &[PointerRow],
    change_rows: usize,
) -> usize {
    let main_id = fixture
        .session
        .active_version_id()
        .await
        .expect("load active json_pointer main version id");
    let draft_id = create_lix_version(&fixture.session).await;
    let (draft_session, _) = fixture
        .session
        .switch_version(SwitchVersionOptions {
            version_id: draft_id.clone(),
        })
        .await
        .expect("switch to json_pointer draft version");
    update_lix_rows_by_pk(&draft_session, &rows[..change_rows], "source").await;
    let (main_session, _) = draft_session
        .switch_version(SwitchVersionOptions {
            version_id: main_id,
        })
        .await
        .expect("switch back to main version");
    let receipt = main_session
        .merge_version(MergeVersionOptions {
            source_version_id: draft_id,
        })
        .await
        .expect("merge fast-forward json_pointer draft");
    assert_eq!(receipt.outcome, MergeVersionOutcome::FastForward);
    assert_eq!(receipt.change_stats.total, change_rows);
    receipt.change_stats.total
}

async fn lix_merge_version_divergent(
    fixture: LixFixture,
    rows: &[PointerRow],
    change_rows: usize,
) -> usize {
    let main_id = fixture
        .session
        .active_version_id()
        .await
        .expect("load active json_pointer main version id");
    let draft_id = create_lix_version(&fixture.session).await;
    let (draft_session, _) = fixture
        .session
        .switch_version(SwitchVersionOptions {
            version_id: draft_id.clone(),
        })
        .await
        .expect("switch to json_pointer draft version");
    update_lix_rows_by_pk(&draft_session, &rows[..change_rows], "source").await;
    let (main_session, _) = draft_session
        .switch_version(SwitchVersionOptions {
            version_id: main_id,
        })
        .await
        .expect("switch back to main version");
    update_lix_rows_by_pk(&main_session, &rows[change_rows..change_rows * 2], "target").await;
    let receipt = main_session
        .merge_version(MergeVersionOptions {
            source_version_id: draft_id,
        })
        .await
        .expect("merge divergent json_pointer draft");
    assert_eq!(receipt.outcome, MergeVersionOutcome::MergeCommitted);
    assert_eq!(receipt.change_stats.total, change_rows);
    receipt.change_stats.total
}

async fn update_lix_rows_by_pk(session: &SessionContext, rows: &[PointerRow], side: &str) {
    for row in rows {
        let value = serde_json::json!({
            "updated": true,
            "side": side,
            "path": row.path,
        })
        .to_string();
        let sql = format!(
            "UPDATE json_pointer SET value = lix_json('{}') WHERE path = '{}'",
            sql_string(value.as_str()),
            sql_string(row.path.as_str())
        );
        let affected = session
            .execute(&sql, &[])
            .await
            .expect("update json_pointer row by path")
            .rows_affected();
        assert_eq!(affected, 1);
    }
}

fn fixture_rows() -> Vec<PointerRow> {
    let root: JsonValue = serde_json::from_str(PNPM_LOCK_JSON).expect("pnpm lock JSON fixture");
    let mut rows = Vec::new();
    flatten_json("", &root, &mut rows);
    rows.retain(|row| !row.path.is_empty());
    assert!(
        rows.len() >= SCALE_ROWS,
        "pnpm lock fixture should have at least {SCALE_ROWS} pointer rows, got {}",
        rows.len()
    );
    rows
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

fn pick_pk_row(rows: &[PointerRow]) -> &PointerRow {
    &rows[rows.len() / 2]
}

fn raw_storage_backend(profile: LixBackendProfile) -> Arc<dyn Backend + Send + Sync> {
    match profile {
        LixBackendProfile::Sqlite => {
            Arc::new(SqliteBenchBackend::tempfile().expect("create sqlite raw storage backend"))
        }
        LixBackendProfile::RocksDb => {
            Arc::new(RocksDbBenchBackend::new().expect("create rocksdb raw storage backend"))
        }
    }
}

fn prepare_raw_storage_read(
    runtime: &Runtime,
    profile: LixBackendProfile,
    rows: &[storage_bench::JsonPointerStorageRow],
) -> (
    Arc<dyn Backend + Send + Sync>,
    storage_bench::JsonPointerTrackedStateReadFixture,
) {
    let backend = raw_storage_backend(profile);
    let fixture = runtime
        .block_on(storage_bench::prepare_json_pointer_tracked_state_read(
            &backend, rows,
        ))
        .expect("prepare json_pointer raw storage read");
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

fn sql_string(value: &str) -> String {
    value.replace('\'', "''")
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

criterion_group!(benches, log12_physical_layout_benches);
criterion_main!(benches);
