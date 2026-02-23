use std::collections::BTreeSet;

use crate::sql::bind_sql;
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackend, LixError, Value};

pub(crate) async fn resolve_requested_root_commits_from_predicates(
    backend: &dyn LixBackend,
    requested_predicates: &[String],
    params: &[Value],
) -> Result<Vec<String>, LixError> {
    let mut all_predicates = vec![format!("c.lixcol_version_id = '{GLOBAL_VERSION_ID}'")];
    all_predicates.extend(requested_predicates.iter().cloned());
    let requested_where_sql = render_where_clause(&all_predicates);
    let sql = format!(
        "WITH commit_by_version AS ( \
           SELECT \
             entity_id AS id, \
             version_id AS lixcol_version_id \
           FROM lix_internal_state_materialized_v1_lix_commit \
           WHERE schema_key = 'lix_commit' \
             AND version_id = '{global_version}' \
             AND is_tombstone = 0 \
             AND snapshot_content IS NOT NULL \
         ) \
         SELECT DISTINCT c.id \
         FROM commit_by_version c \
         {requested_where}",
        global_version = GLOBAL_VERSION_ID,
        requested_where = requested_where_sql,
    );
    let bound = bind_sql(&sql, params, backend.dialect())?;
    let rows = backend.execute(&bound.sql, &bound.params).await?;
    let mut roots = BTreeSet::new();
    for row in &rows.rows {
        if let Some(id) = text_value_at(row, 0) {
            roots.insert(id.to_string());
        }
    }
    Ok(roots.into_iter().collect())
}

fn render_where_clause(predicates: &[String]) -> String {
    if predicates.is_empty() {
        return String::new();
    }
    format!("WHERE {}", predicates.join(" AND "))
}

fn text_value_at(row: &[Value], index: usize) -> Option<&str> {
    match row.get(index) {
        Some(Value::Text(value)) => Some(value.as_str()),
        _ => None,
    }
}
