use crate::backend::QueryExecutor;
use crate::{LixBackend, LixError, SqlDialect, Value};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveReadColumn {
    pub(crate) property_name: String,
}

#[derive(Debug, Clone)]
pub(crate) struct LiveReadContract {
    access: super::storage::LiveRowAccess,
    columns: Vec<LiveReadColumn>,
}

impl LiveReadContract {
    fn raw_access(&self) -> &super::storage::LiveRowAccess {
        &self.access
    }

    pub(crate) fn columns(&self) -> &[LiveReadColumn] {
        &self.columns
    }

    pub(crate) fn normalized_projection_sql(&self, table_alias: Option<&str>) -> String {
        self.access.normalized_projection_sql(table_alias)
    }

    pub(crate) fn normalized_values(
        &self,
        snapshot_content: Option<&str>,
    ) -> Result<std::collections::BTreeMap<String, Value>, LixError> {
        super::storage::normalized_live_column_values(self.access.layout(), snapshot_content)
    }

    pub(crate) fn snapshot_json_from_values(
        &self,
        schema_key: &str,
        values: &std::collections::BTreeMap<String, Value>,
    ) -> Result<JsonValue, LixError> {
        super::snapshot_json_from_values(&self.access, schema_key, values)
    }

    pub(crate) fn snapshot_text_from_values(
        &self,
        schema_key: &str,
        values: &std::collections::BTreeMap<String, Value>,
    ) -> Result<String, LixError> {
        super::snapshot_text_from_values(&self.access, schema_key, values)
    }
}

pub(crate) async fn load_schema_read_contract_with_backend(
    backend: &dyn LixBackend,
    schema_key: &str,
) -> Result<LiveReadContract, LixError> {
    super::storage::load_live_row_access_with_backend(backend, schema_key)
        .await
        .map(read_contract_from_storage)
}

pub(crate) async fn load_schema_read_contract_for_table_name(
    backend: &dyn LixBackend,
    table_name: &str,
) -> Result<Option<LiveReadContract>, LixError> {
    super::storage::load_live_row_access_for_table_name(backend, table_name)
        .await
        .map(|access| access.map(read_contract_from_storage))
}

pub(super) fn live_read_contract_from_layout(
    layout: super::storage::LiveTableLayout,
) -> LiveReadContract {
    read_contract_from_storage(super::storage::LiveRowAccess::new(layout))
}

pub(crate) async fn live_storage_relation_exists_with_backend(
    backend: &dyn LixBackend,
    schema_key: &str,
) -> Result<bool, LixError> {
    let relation_name = tracked_relation_name(schema_key);
    match backend.dialect() {
        SqlDialect::Sqlite => {
            let result = backend
                .execute(
                    "SELECT 1 \
                     FROM sqlite_master \
                     WHERE name = $1 \
                       AND type IN ('table', 'view') \
                     LIMIT 1",
                    &[Value::Text(relation_name)],
                )
                .await?;
            Ok(!result.rows.is_empty())
        }
        SqlDialect::Postgres => {
            let result = backend
                .execute(
                    "SELECT 1 \
                     FROM information_schema.tables \
                     WHERE table_name = $1 \
                     LIMIT 1",
                    &[Value::Text(relation_name)],
                )
                .await?;
            Ok(!result.rows.is_empty())
        }
    }
}

pub(crate) async fn live_storage_relation_exists_with_executor(
    executor: &mut dyn QueryExecutor,
    schema_key: &str,
) -> Result<bool, LixError> {
    let relation_name = tracked_relation_name(schema_key);
    match executor.dialect() {
        SqlDialect::Sqlite => {
            let result = executor
                .execute(
                    "SELECT 1 \
                     FROM sqlite_master \
                     WHERE name = $1 \
                       AND type IN ('table', 'view') \
                     LIMIT 1",
                    &[Value::Text(relation_name)],
                )
                .await?;
            Ok(!result.rows.is_empty())
        }
        SqlDialect::Postgres => {
            let result = executor
                .execute(
                    "SELECT 1 \
                     FROM information_schema.tables \
                     WHERE table_name = $1 \
                     LIMIT 1",
                    &[Value::Text(relation_name)],
                )
                .await?;
            Ok(!result.rows.is_empty())
        }
    }
}

pub(crate) fn tracked_relation_name(schema_key: &str) -> String {
    super::storage::tracked_live_table_name(schema_key)
}

pub(crate) fn payload_column_name_for_schema(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
    property_name: &str,
) -> Result<String, LixError> {
    let layout = schema_layout(schema_key, schema_definition)?;
    super::storage::live_column_name_for_property(&layout, property_name)
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "live schema '{}' does not include property '{}'",
                    schema_key, property_name
                ),
            )
        })
}

pub(crate) fn normalized_projection_sql_for_schema(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
    table_alias: Option<&str>,
) -> Result<String, LixError> {
    Ok(
        super::storage::LiveRowAccess::new(schema_layout(schema_key, schema_definition)?)
            .normalized_projection_sql(table_alias),
    )
}

pub(crate) fn snapshot_select_expr_for_schema(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
    dialect: SqlDialect,
    table_alias: Option<&str>,
) -> Result<String, LixError> {
    Ok(super::shared::snapshot_sql::live_snapshot_select_expr(
        &schema_layout(schema_key, schema_definition)?,
        dialect,
        table_alias,
    ))
}

#[cfg(test)]
pub(crate) fn normalized_values_for_schema(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
    snapshot_content: Option<&str>,
) -> Result<std::collections::BTreeMap<String, Value>, LixError> {
    super::storage::normalized_live_column_values(
        &schema_layout(schema_key, schema_definition)?,
        snapshot_content,
    )
}

pub(crate) fn logical_snapshot_from_projected_row_with_contract(
    access: Option<&LiveReadContract>,
    schema_key: &str,
    row: &[Value],
    snapshot_index: usize,
    normalized_start_index: usize,
) -> Result<Option<JsonValue>, LixError> {
    super::storage::logical_snapshot_from_projected_row(
        access.map(|access| access.raw_access()),
        schema_key,
        row,
        snapshot_index,
        normalized_start_index,
    )
}

#[cfg(test)]
pub(crate) fn schema_column_names(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
) -> Result<Vec<String>, LixError> {
    Ok(schema_layout(schema_key, schema_definition)?
        .columns
        .into_iter()
        .map(|column| column.column_name)
        .collect())
}

fn schema_layout(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
) -> Result<super::storage::LiveTableLayout, LixError> {
    if let Some(schema_definition) = schema_definition {
        return super::storage::live_table_layout_from_schema(schema_definition);
    }
    super::storage::builtin_live_table_layout(schema_key)?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("missing live schema definition for '{}'", schema_key),
        )
    })
}

fn read_contract_from_storage(access: super::storage::LiveRowAccess) -> LiveReadContract {
    LiveReadContract {
        columns: access
            .columns()
            .iter()
            .map(|column| LiveReadColumn {
                property_name: column.property_name.clone(),
            })
            .collect(),
        access,
    }
}
