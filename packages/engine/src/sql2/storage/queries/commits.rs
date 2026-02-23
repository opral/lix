use super::super::tables;

pub(crate) fn select_commit_exists_sql() -> String {
    format!(
        "SELECT 1 FROM {} WHERE id = $1 LIMIT 1",
        tables::commits::COMMIT,
    )
}
