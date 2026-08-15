// This parity test includes the benchmark-only fixture modules directly.
// Most of their CRUD helpers are intentionally unused by this one focused
// assertion.
#![recursion_limit = "256"]
#![allow(dead_code)]

const READ_MANY_PK_COUNT: usize = 4;

#[path = "../benches/tracked_state_crud/sql_session.rs"]
mod sql_session;
#[path = "../benches/tracked_state_crud/storage.rs"]
mod storage;
#[path = "../benches/tracked_state_crud/workload.rs"]
mod workload;

use workload::WorkloadRow;

static PUBLIC_RESULT_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn serialized_public_result_test() -> tokio::sync::MutexGuard<'static, ()> {
    PUBLIC_RESULT_TEST_LOCK.lock().await
}

/// Stack budget for the OLAP parity bodies below.
///
/// These tests build and optimize real DataFusion plans over a seeded fixture.
/// Plan construction and the optimizer recurse per plan node, and an
/// unoptimized build needs several times the stack that an optimized one does,
/// so the bodies overflow libtest's 2 MiB worker-stack default in the `test`
/// profile while passing in `--release`. Running them on an explicitly sized
/// thread makes the suite independent of the build profile and of whether
/// `RUST_MIN_STACK` happens to be exported by the caller.
const PUBLIC_RESULT_TEST_STACK_SIZE: usize = 32 * 1024 * 1024;

/// Runs an async test body on a thread with [`PUBLIC_RESULT_TEST_STACK_SIZE`]
/// of stack, driven by a current-thread runtime so the concurrency semantics
/// match the `#[tokio::test]` default these bodies were written against.
///
/// Panics (including assertion failures) propagate to the libtest worker
/// through the join handle, so a failing assertion still fails the test.
fn run_on_sized_stack<Body, Fut>(name: &str, body: Body)
where
    Body: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()>,
{
    std::thread::Builder::new()
        .name(name.to_string())
        .stack_size(PUBLIC_RESULT_TEST_STACK_SIZE)
        .spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build public-result test runtime")
                .block_on(body());
        })
        .expect("spawn public-result test thread")
        .join()
        .expect("public-result test body panicked");
}

#[test]
fn typed_olap_queries_are_plain_datafusion_selects() {
    use datafusion::sql::parser::DFParser;
    use datafusion::sql::sqlparser::ast::{SetExpr, Statement as SqlStatement};
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;

    for shape in sql_session::OlapReadShape::ALL {
        let mut statements = DFParser::parse_sql_with_dialect(shape.sql(), &PostgreSqlDialect {})
            .expect("typed OLAP benchmark SQL should parse");
        let statement = statements.pop_front().expect("one OLAP statement");
        let datafusion::sql::parser::Statement::Statement(statement) = statement else {
            panic!("{} must remain a SELECT query", shape.label());
        };
        let SqlStatement::Query(query) = statement.as_ref() else {
            panic!("{} must remain a SELECT query", shape.label());
        };
        assert!(
            query.with.is_none(),
            "{} must not need a CTE to evade a native query recognizer",
            shape.label()
        );
        assert!(
            matches!(query.body.as_ref(), SetExpr::Select(_)),
            "{} must remain a general DataFusion SELECT",
            shape.label()
        );
    }
}

#[test]
fn typed_olap_shapes_validate_exact_results_on_every_adapter() {
    run_on_sized_stack("olap-exact-results", || async {
        validate_exact_olap_shapes().await;
    });
}

async fn validate_exact_olap_shapes() {
    let _test_guard = serialized_public_result_test().await;
    let rows = workload::fixture_rows(128);
    for &profile in storage::STORAGE_PROFILES {
        let fixture = sql_session::seeded_olap_fixture(profile, &rows).await;
        for shape in sql_session::OlapReadShape::ALL {
            let expected_rows = match shape {
                sql_session::OlapReadShape::Scan => rows.len(),
                sql_session::OlapReadShape::Filter => 6,
                sql_session::OlapReadShape::Sort => 85,
                sql_session::OlapReadShape::Group => 32,
                sql_session::OlapReadShape::Aggregate => 1,
                sql_session::OlapReadShape::Join => 85,
            };
            assert_eq!(
                fixture.read_olap(shape).await,
                expected_rows,
                "{}",
                shape.label()
            );
        }
    }
}

#[test]
fn typed_olap_shapes_validate_above_columnar_publication_threshold() {
    run_on_sized_stack("olap-above-columnar-threshold", || async {
        validate_above_columnar_publication_threshold().await;
    });
}

async fn validate_above_columnar_publication_threshold() {
    let _test_guard = serialized_public_result_test().await;
    let rows = workload::fixture_rows(2_048);
    for &profile in storage::STORAGE_PROFILES {
        let fixture = sql_session::seeded_olap_fixture(profile, &rows).await;
        for shape in sql_session::OlapReadShape::ALL {
            let expected_rows = match shape {
                sql_session::OlapReadShape::Scan => rows.len(),
                sql_session::OlapReadShape::Filter => 86,
                sql_session::OlapReadShape::Sort => 1_365,
                sql_session::OlapReadShape::Group => 32,
                sql_session::OlapReadShape::Aggregate => 1,
                sql_session::OlapReadShape::Join => 1_365,
            };
            assert_eq!(
                fixture.read_olap(shape).await,
                expected_rows,
                "{}",
                shape.label()
            );
        }
    }
}

#[test]
fn typed_olap_shapes_validate_exact_results_after_sparse_and_moderate_mutations() {
    run_on_sized_stack("post-update-olap-validation", || async {
        let _test_guard = serialized_public_result_test().await;
        validate_post_update_olap_shapes().await;
    });
}

async fn validate_post_update_olap_shapes() {
    let rows = workload::fixture_rows(2_048);
    for &profile in storage::STORAGE_PROFILES {
        for mutation_profile in [
            sql_session::OlapMutationProfile::Sparse,
            sql_session::OlapMutationProfile::Moderate,
        ] {
            let fixture =
                sql_session::seeded_olap_fixture_with_mutations(profile, &rows, mutation_profile)
                    .await;
            for shape in sql_session::OlapReadShape::ALL {
                fixture.read_olap(shape).await;
            }
        }
    }
}

#[test]
fn insert_benchmark_uses_public_batch_on_every_adapter() {
    run_on_sized_stack("public-parameter-batch", || async {
        validate_public_parameter_batch().await;
    });
}

async fn validate_public_parameter_batch() {
    let _test_guard = serialized_public_result_test().await;
    let rows = [
        WorkloadRow {
            path: "/alpha".to_string(),
            value_json: r#"{"enabled":true}"#.to_string(),
            updated_value_json: r#"{"enabled":false}"#.to_string(),
        },
        WorkloadRow {
            path: "/beta".to_string(),
            value_json: r#"[1,2,3]"#.to_string(),
            updated_value_json: r#"[4,5,6]"#.to_string(),
        },
    ];

    for &profile in storage::STORAGE_PROFILES {
        let fixture =
            sql_session::empty_fixture_with_read_many_pk_count(profile, &rows, rows.len()).await;
        assert_eq!(fixture.insert_all().await, rows.len());
    }
}

#[test]
fn public_insert_batches_work_across_sequential_fixtures() {
    run_on_sized_stack("sequential-public-batches", || async {
        validate_sequential_public_batches().await;
    });
}

async fn validate_sequential_public_batches() {
    let _test_guard = serialized_public_result_test().await;
    let rows = [WorkloadRow {
        path: "/sequential".to_string(),
        value_json: r#"{"enabled":true}"#.to_string(),
        updated_value_json: r#"{"enabled":false}"#.to_string(),
    }];

    let rocksdb = sql_session::empty_fixture_with_read_many_pk_count(
        storage::StorageProfile::RocksDB,
        &rows,
        1,
    )
    .await;
    assert_eq!(rocksdb.insert_all().await, 1);

    let slatedb = sql_session::empty_fixture_with_read_many_pk_count(
        storage::StorageProfile::SlateDB,
        &rows,
        1,
    )
    .await;
    assert_eq!(slatedb.insert_all().await, 1);
}

#[test]
fn public_insert_batches_work_for_concurrent_fixtures() {
    run_on_sized_stack("concurrent-public-batches", || async {
        validate_concurrent_public_batches().await;
    });
}

async fn validate_concurrent_public_batches() {
    let rows = [WorkloadRow {
        path: "/concurrent".to_string(),
        value_json: r#"{"enabled":true}"#.to_string(),
        updated_value_json: r#"{"enabled":false}"#.to_string(),
    }];

    let (rocksdb, slatedb) = tokio::join!(
        insert_one_counter_fixture(storage::StorageProfile::RocksDB, &rows),
        insert_one_counter_fixture(storage::StorageProfile::SlateDB, &rows),
    );
    assert_eq!(rocksdb, 1);
    assert_eq!(slatedb, 1);
}

async fn insert_one_counter_fixture(
    profile: storage::StorageProfile,
    rows: &[WorkloadRow],
) -> usize {
    let _test_guard = serialized_public_result_test().await;
    let fixture =
        sql_session::empty_fixture_with_read_many_pk_count(profile, rows, rows.len()).await;
    fixture.insert_all().await
}

/// Regression for the automatic-write future retaining SlateDB's large
/// adapter-specific open and commit futures on Tokio's default test stack.
///
/// Deliberately NOT wrapped in [`run_on_sized_stack`]: asserting that this
/// path still fits the default worker stack is the entire point of the test.
/// It must keep running on whatever stack libtest hands it.
#[cfg(feature = "slatedb")]
#[tokio::test]
async fn slatedb_automatic_write_runs_on_default_stack() {
    let _test_guard = serialized_public_result_test().await;
    let rows = [WorkloadRow {
        path: "/default-stack".to_string(),
        value_json: r#"{"enabled":true}"#.to_string(),
        updated_value_json: r#"{"enabled":false}"#.to_string(),
    }];
    let fixture = sql_session::empty_fixture_with_read_many_pk_count(
        storage::StorageProfile::SlateDB,
        &rows,
        rows.len(),
    )
    .await;
    assert_eq!(fixture.insert_all().await, rows.len());
}

#[test]
fn scaling_fixture_extends_the_real_workload_in_physical_key_order() {
    let rows = workload::fixture_rows(20_000);

    assert_eq!(rows.len(), 20_000);
    assert!(
        rows.windows(2).all(|pair| pair[0].path < pair[1].path),
        "scaling identities must remain strictly ordered for comparable dense writes"
    );
    assert!(
        rows.iter().any(|row| row.path.starts_with("/~lix-scale/")),
        "rows beyond the embedded real workload must use deterministic synthetic identities"
    );
}

#[test]
fn slatedb_is_not_routed_through_the_unsupported_kv_layer() {
    assert_eq!(
        storage::KV_STORAGE_PROFILES
            .iter()
            .map(|profile| profile.name())
            .collect::<Vec<_>>(),
        vec!["lix_rocksdb"]
    );
    #[cfg(feature = "slatedb")]
    assert!(
        storage::STORAGE_PROFILES
            .iter()
            .any(|profile| profile.name() == "lix_slatedb"),
        "SlateDB must remain covered by transaction and SQL-session profiles"
    );
}
