mod entity_history;
mod lix_change;
mod lix_commit;
mod lix_directory;
mod lix_directory_history;
mod lix_file;
mod lix_file_history;
mod lix_key_value;
mod lix_registered_schema;
mod lix_state;
mod lix_state_history;
mod lix_version;

use lix_engine::engine2::ExecuteResult;
use lix_engine::Value;

async fn select_rows(
    session: &crate::support::simulation_test::engine2::SimSession,
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
