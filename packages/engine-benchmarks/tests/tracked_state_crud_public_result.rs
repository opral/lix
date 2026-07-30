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

#[tokio::test]
async fn standalone_sqlite_public_results_match_every_lix_adapter() {
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
