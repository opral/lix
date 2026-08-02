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

#[test]
fn typed_olap_queries_are_structurally_ineligible_for_strict_native_reads() {
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
            query.with.is_some(),
            "{} must retain a top-level CTE: strict_single_table_select rejects query.with before any native entity recognizer can match",
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
                .block_on(validate_post_update_olap_shapes());
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
                .block_on(standalone_sqlite_public_results_match_every_lix_adapter_async());
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
