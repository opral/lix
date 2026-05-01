#[macro_use]
#[path = "support/mod.rs"]
mod support;

#[path = "sql/entity_history.rs"]
mod entity_history;
#[path = "sql/lix_change.rs"]
mod lix_change;
#[path = "sql/lix_commit.rs"]
mod lix_commit;
#[path = "sql/lix_directory.rs"]
mod lix_directory;
#[path = "sql/lix_directory_history.rs"]
mod lix_directory_history;
#[path = "sql/lix_file.rs"]
mod lix_file;
#[path = "sql/lix_file_history.rs"]
mod lix_file_history;
#[path = "sql/lix_key_value.rs"]
mod lix_key_value;
#[path = "sql/lix_registered_schema.rs"]
mod lix_registered_schema;
#[path = "sql/lix_state.rs"]
mod lix_state;
#[path = "sql/lix_state_history.rs"]
mod lix_state_history;
#[path = "sql/lix_version.rs"]
mod lix_version;

use lix_engine::ExecuteResult;
use lix_engine::Value;

async fn select_rows(
    session: &crate::support::simulation_test::engine::SimSession,
    sql: &str,
) -> Vec<Vec<Value>> {
    let result = session
        .execute(sql, &[])
        .await
        .expect("SELECT should succeed");
    rows_from_result(result)
}

fn assert_rows_eq(result: ExecuteResult, expected: Vec<Vec<Value>>) {
    assert_eq!(rows_from_result(result), expected);
}

fn rows_from_result(result: ExecuteResult) -> Vec<Vec<Value>> {
    let ExecuteResult::Rows(row_set) = result else {
        panic!("SELECT should return rows");
    };
    row_set
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}
