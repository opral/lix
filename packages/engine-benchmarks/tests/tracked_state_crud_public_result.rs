// This parity test includes the benchmark-only fixture modules directly.
// Most of their CRUD helpers are intentionally unused by this one focused
// assertion.
#![allow(dead_code)]

const READ_MANY_PK_COUNT: usize = 4;

#[path = "../benches/tracked_state_crud/raw_sqlite.rs"]
mod raw_sqlite;
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

#[tokio::test]
async fn typed_olap_shapes_validate_exact_results_on_every_adapter() {
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

#[tokio::test]
async fn typed_olap_shapes_validate_above_columnar_publication_threshold() {
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
    for &profile in storage::STORAGE_PROFILES
        .iter()
        .filter(|profile| profile.name() != "lix_sqlite")
    {
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
fn standalone_sqlite_public_results_match_every_lix_adapter() {
    std::thread::Builder::new()
        .name("tracked-state-public-result".to_string())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build parity-test runtime")
                .block_on(async {
                    let _test_guard = serialized_public_result_test().await;
                    standalone_sqlite_public_results_match_every_lix_adapter_async().await;
                });
        })
        .expect("spawn parity-test thread")
        .join()
        .expect("join parity-test thread");
}

async fn standalone_sqlite_public_results_match_every_lix_adapter_async() {
    let rows = [
        WorkloadRow {
            path: "/alpha".to_string(),
            value_json: r#"{"enabled":true}"#.to_string(),
            updated_value_json: r#"{"enabled":false}"#.to_string(),
        },
        WorkloadRow {
            path: "/beta".to_string(),
            value_json: r"[1,2,3]".to_string(),
            updated_value_json: r"[4,5,6]".to_string(),
        },
        WorkloadRow {
            path: "/null".to_string(),
            value_json: "null".to_string(),
            updated_value_json: r#"{"updated":true}"#.to_string(),
        },
        WorkloadRow {
            path: "/text".to_string(),
            value_json: r#""hello""#.to_string(),
            updated_value_json: r#""updated""#.to_string(),
        },
    ];
    for &profile in storage::STORAGE_PROFILES {
        let mut raw = raw_sqlite::seeded_fixture(&rows);
        let lix = sql_session::seeded_fixture(profile, &rows).await;

        assert_eq!(raw.read_all_public_result(), lix.read_all_result().await);
        assert_eq!(
            raw.read_one_by_pk_public_result(),
            lix.read_one_by_pk_result().await
        );
        assert_eq!(
            raw.read_many_by_pk_public_result(READ_MANY_PK_COUNT),
            lix.read_many_by_pk_result().await
        );

        assert_eq!(raw.update_all_literal(), rows.len());
        assert_eq!(lix.update_all_bound().await, rows.len());
        assert_eq!(raw.read_all_public_result(), lix.read_all_result().await);
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

    let sqlite = sql_session::empty_fixture_with_read_many_pk_count(
        storage::StorageProfile::SQLite,
        &rows,
        1,
    )
    .await;
    assert_eq!(sqlite.insert_all().await, 1);

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

    let (sqlite, slatedb) = tokio::join!(
        insert_one_counter_fixture(storage::StorageProfile::SQLite, &rows),
        insert_one_counter_fixture(storage::StorageProfile::SlateDB, &rows),
    );
    assert_eq!(sqlite, 1);
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
        vec!["lix_sqlite", "lix_rocksdb"]
    );
    #[cfg(feature = "slatedb")]
    assert!(
        storage::STORAGE_PROFILES
            .iter()
            .any(|profile| profile.name() == "lix_slatedb"),
        "SlateDB must remain covered by transaction and SQL-session profiles"
    );
}
