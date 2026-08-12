// This parity test includes the benchmark-only fixture modules directly.
// Most of their CRUD helpers are intentionally unused by this one focused
// assertion.
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

#[test]
fn typed_olap_queries_are_plain_datafusion_selects() {
    use datafusion::sql::parser::DFParser;
    use datafusion::sql::sqlparser::ast::{SetExpr, Statement as SqlStatement};
    use datafusion::sql::sqlparser::dialect::GenericDialect;

    for shape in sql_session::OlapReadShape::ALL {
        let mut statements = DFParser::parse_sql_with_dialect(shape.sql(), &GenericDialect {})
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

/// Explicitly sized thread: see
/// [`typed_olap_shapes_validate_above_columnar_publication_threshold`]. Every
/// OLAP-shape test in this file builds the same deep DataFusion plans and none
/// of them fit the default test-thread stack with any margin.
#[test]
fn typed_olap_shapes_validate_exact_results_on_every_adapter() {
    std::thread::Builder::new()
        .name("every-adapter-olap-validation".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build every-adapter OLAP runtime")
                .block_on(validate_every_adapter_olap_shapes());
        })
        .expect("spawn every-adapter OLAP validation thread")
        .join()
        .expect("join every-adapter OLAP validation thread");
}

async fn validate_every_adapter_olap_shapes() {
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

/// Runs on an explicitly sized thread for the same reason
/// `typed_olap_shapes_validate_exact_results_after_sparse_and_moderate_mutations`
/// below does: the OLAP shapes over a 2,048-row fixture build deep DataFusion
/// plans whose unoptimized frames do not fit the default test-thread stack.
///
/// This test had been running on the default stack with no measurable margin —
/// adding one `u64` field to `SqlReadProfile`, with no other change, was enough
/// to abort it. Nothing about the assertions changes here; it just gets the
/// stack its sibling already has.
#[test]
fn typed_olap_shapes_validate_above_columnar_publication_threshold() {
    std::thread::Builder::new()
        .name("columnar-threshold-olap-validation".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build columnar-threshold OLAP runtime")
                .block_on(validate_columnar_threshold_olap_shapes());
        })
        .expect("spawn columnar-threshold OLAP validation thread")
        .join()
        .expect("join columnar-threshold OLAP validation thread");
}

async fn validate_columnar_threshold_olap_shapes() {
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
    std::thread::Builder::new()
        .name("post-update-olap-validation".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build post-update OLAP runtime")
                .block_on(async {
                    let _test_guard = serialized_public_result_test().await;
                    validate_post_update_olap_shapes().await;
                });
        })
        .expect("spawn post-update OLAP validation thread")
        .join()
        .expect("join post-update OLAP validation thread");
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

#[tokio::test]
async fn insert_benchmark_hits_certified_parameter_batch_on_every_adapter() {
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

#[tokio::test]
async fn certified_insert_counter_deltas_are_isolated_across_sequential_fixtures() {
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

#[tokio::test]
async fn certified_insert_counter_deltas_are_isolated_for_concurrent_fixtures() {
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
    // The futures are intentionally joined concurrently. Each fixture owns
    // the process-global counter snapshot through the entire operation, so
    // their exact certification=1/execution=1 deltas cannot overlap.
    let _counter_guard = serialized_public_result_test().await;
    let fixture =
        sql_session::empty_fixture_with_read_many_pk_count(profile, rows, rows.len()).await;
    fixture.insert_all().await
}

/// Regression for the automatic-write future retaining SlateDB's large
/// adapter-specific open and commit futures on Tokio's default test stack.
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
