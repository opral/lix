use crate::backend::QueryExecutor;
use crate::errors::classification::is_missing_relation_error;
use crate::version::{
    parse_version_descriptor_snapshot, version_descriptor_file_id, version_descriptor_plugin_key,
    version_descriptor_schema_key, version_descriptor_schema_version,
};
use crate::{LixBackend, LixError, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionDescriptorRow {
    pub(crate) version_id: String,
    pub(crate) name: String,
    pub(crate) hidden: bool,
    pub(crate) change_id: Option<String>,
}

pub(crate) async fn load_version_descriptor_with_backend(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<Option<VersionDescriptorRow>, LixError> {
    let mut executor = backend;
    load_version_descriptor_with_executor(&mut executor, version_id).await
}

pub(crate) async fn load_version_descriptor_with_executor(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<Option<VersionDescriptorRow>, LixError> {
    let sql = format!(
        "SELECT c.id AS change_id, s.content AS snapshot_content \
         FROM lix_internal_change c \
         LEFT JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
         WHERE c.schema_key = '{schema_key}' \
           AND c.schema_version = '{schema_version}' \
           AND c.entity_id = '{entity_id}' \
           AND c.file_id = '{file_id}' \
           AND c.plugin_key = '{plugin_key}' \
         ORDER BY c.created_at DESC, c.id DESC \
         LIMIT 1",
        schema_key = escape_sql_string(version_descriptor_schema_key()),
        schema_version = escape_sql_string(version_descriptor_schema_version()),
        entity_id = escape_sql_string(version_id),
        file_id = escape_sql_string(version_descriptor_file_id()),
        plugin_key = escape_sql_string(version_descriptor_plugin_key()),
    );
    let result = match executor.execute(&sql, &[]).await {
        Ok(result) => result,
        Err(err) if is_missing_relation_error(&err) => return Ok(None),
        Err(err) => return Err(err),
    };
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    let Some(Value::Text(snapshot_content)) = row.get(1) else {
        return Ok(None);
    };
    let change_id = optional_text_value(row.first())?;
    Ok(Some(parse_descriptor_row(snapshot_content, change_id)?))
}

pub(crate) async fn load_all_version_descriptors_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Vec<VersionDescriptorRow>, LixError> {
    let sql = format!(
        "WITH ranked AS (\
             SELECT c.entity_id, \
                    c.id AS change_id, \
                    s.content AS snapshot_content, \
                    ROW_NUMBER() OVER (PARTITION BY c.entity_id ORDER BY c.created_at DESC, c.id DESC) AS rn \
             FROM lix_internal_change c \
             LEFT JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
             WHERE c.schema_key = '{schema_key}' \
               AND c.schema_version = '{schema_version}' \
               AND c.file_id = '{file_id}' \
               AND c.plugin_key = '{plugin_key}'\
         ) \
         SELECT entity_id, change_id, snapshot_content \
         FROM ranked \
         WHERE rn = 1 \
           AND snapshot_content IS NOT NULL \
         ORDER BY entity_id ASC",
        schema_key = escape_sql_string(version_descriptor_schema_key()),
        schema_version = escape_sql_string(version_descriptor_schema_version()),
        file_id = escape_sql_string(version_descriptor_file_id()),
        plugin_key = escape_sql_string(version_descriptor_plugin_key()),
    );
    let result = match executor.execute(&sql, &[]).await {
        Ok(result) => result,
        Err(err) if is_missing_relation_error(&err) => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };

    let mut descriptors = Vec::new();
    for row in &result.rows {
        let Some(Value::Text(snapshot_content)) = row.get(2) else {
            continue;
        };
        let change_id = optional_text_value(row.get(1))?;
        descriptors.push(parse_descriptor_row(snapshot_content, change_id)?);
    }
    descriptors.sort_by(|left, right| left.version_id.cmp(&right.version_id));
    Ok(descriptors)
}

pub(crate) async fn find_version_id_by_name_with_backend(
    backend: &dyn LixBackend,
    name: &str,
) -> Result<Option<String>, LixError> {
    let mut executor = backend;
    find_version_id_by_name_with_executor(&mut executor, name).await
}

pub(crate) async fn find_version_id_by_name_with_executor(
    executor: &mut dyn QueryExecutor,
    name: &str,
) -> Result<Option<String>, LixError> {
    for descriptor in load_all_version_descriptors_with_executor(executor).await? {
        if descriptor.name == name {
            return Ok(Some(descriptor.version_id));
        }
    }
    Ok(None)
}

pub(crate) async fn version_exists_with_backend(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<bool, LixError> {
    Ok(load_version_descriptor_with_backend(backend, version_id)
        .await?
        .is_some())
}

pub(crate) async fn version_exists_with_executor(
    executor: &mut dyn QueryExecutor,
    version_id: &str,
) -> Result<bool, LixError> {
    Ok(load_version_descriptor_with_executor(executor, version_id)
        .await?
        .is_some())
}

pub(crate) fn build_admin_version_source_sql() -> String {
    format!(
        "WITH descriptor_ranked AS (\
             SELECT c.entity_id, \
                    s.content AS snapshot_content, \
                    ROW_NUMBER() OVER (PARTITION BY c.entity_id ORDER BY c.created_at DESC, c.id DESC) AS rn \
             FROM lix_internal_change c \
             LEFT JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
             WHERE c.schema_key = '{descriptor_schema_key}' \
               AND c.schema_version = '{descriptor_schema_version}' \
               AND c.file_id = '{descriptor_file_id}' \
               AND c.plugin_key = '{descriptor_plugin_key}'\
         ), \
         ref_ranked AS (\
             SELECT c.entity_id, \
                    s.content AS snapshot_content, \
                    ROW_NUMBER() OVER (PARTITION BY c.entity_id ORDER BY c.created_at DESC, c.id DESC) AS rn \
             FROM lix_internal_change c \
             LEFT JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
             WHERE c.schema_key = '{ref_schema_key}' \
               AND c.schema_version = '{ref_schema_version}' \
               AND c.file_id = '{ref_file_id}' \
               AND c.plugin_key = '{ref_plugin_key}'\
         ) \
         SELECT \
             d.entity_id AS id, \
             COALESCE(lix_json_extract(d.snapshot_content, 'name'), '') AS name, \
             COALESCE(lix_json_extract_boolean(d.snapshot_content, 'hidden'), false) AS hidden, \
             COALESCE(lix_json_extract(r.snapshot_content, 'commit_id'), '') AS commit_id \
         FROM descriptor_ranked d \
         LEFT JOIN ref_ranked r \
           ON r.entity_id = d.entity_id \
          AND r.rn = 1 \
          AND r.snapshot_content IS NOT NULL \
         WHERE d.rn = 1 \
           AND d.snapshot_content IS NOT NULL \
         ORDER BY d.entity_id ASC",
        descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
        descriptor_schema_version = escape_sql_string(version_descriptor_schema_version()),
        descriptor_file_id = escape_sql_string(version_descriptor_file_id()),
        descriptor_plugin_key = escape_sql_string(version_descriptor_plugin_key()),
        ref_schema_key = escape_sql_string(crate::version::version_ref_schema_key()),
        ref_schema_version = escape_sql_string(crate::version::version_ref_schema_version()),
        ref_file_id = escape_sql_string(crate::version::version_ref_file_id()),
        ref_plugin_key = escape_sql_string(crate::version::version_ref_plugin_key()),
    )
}

fn parse_descriptor_row(
    snapshot_content: &str,
    change_id: Option<String>,
) -> Result<VersionDescriptorRow, LixError> {
    let snapshot = parse_version_descriptor_snapshot(snapshot_content)?;
    Ok(VersionDescriptorRow {
        version_id: snapshot.id,
        name: snapshot.name.unwrap_or_default(),
        hidden: snapshot.hidden,
        change_id,
    })
}

fn optional_text_value(value: Option<&Value>) -> Result<Option<String>, LixError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Text(text)) => Ok(Some(text.clone())),
        Some(Value::Integer(number)) => Ok(Some(number.to_string())),
        Some(other) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!("expected nullable text-like version descriptor field, got {other:?}"),
        )),
    }
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}
