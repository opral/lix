// This parity test includes the benchmark-only fixture modules directly.
// Most of their CRUD helpers are intentionally unused by this one focused
// assertion.
#![allow(dead_code)]

const READ_MANY_PK_COUNT: usize = 10;

#[path = "../benches/tracked_state_crud/raw_sqlite.rs"]
mod raw_sqlite;
#[path = "../benches/tracked_state_crud/sql_session.rs"]
mod sql_session;
#[path = "../benches/tracked_state_crud/storage.rs"]
mod storage;
#[path = "../benches/tracked_state_crud/workload.rs"]
mod workload;

use storage::StorageProfile;
use workload::WorkloadRow;

#[tokio::test]
async fn standalone_sqlite_public_results_match_lix_sql_results() {
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
    let mut raw = raw_sqlite::seeded_fixture(&rows);
    let lix = sql_session::seeded_fixture(StorageProfile::RocksDB, &rows).await;

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
