use crate::live_state::store::LiveStateBackendRef;
use crate::{LixError, SqlDialect, Value};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveReadColumn {
    pub(crate) property_name: String,
}

#[derive(Debug, Clone)]
pub(crate) struct LiveRowShape {
    access: super::storage::LiveRowAccess,
    columns: Vec<LiveReadColumn>,
}

impl LiveRowShape {
    fn raw_access(&self) -> &super::storage::LiveRowAccess {
        &self.access
    }

    pub(crate) fn columns(&self) -> &[LiveReadColumn] {
        &self.columns
    }

    pub(crate) fn normalized_projection_sql(&self, table_alias: Option<&str>) -> String {
        self.access.normalized_projection_sql(table_alias)
    }

    pub(crate) fn payload_column_name(&self, property_name: &str) -> Option<&str> {
        self.access.payload_column_name(property_name)
    }

    pub(crate) fn snapshot_select_expr(
        &self,
        dialect: SqlDialect,
        table_alias: Option<&str>,
    ) -> String {
        super::shared::snapshot_sql::live_snapshot_select_expr(
            self.access.layout(),
            dialect,
            table_alias,
        )
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
        super::stored_rows::snapshot_json_from_values(&self.access, schema_key, values)
    }

    pub(crate) fn snapshot_text_from_values(
        &self,
        schema_key: &str,
        values: &std::collections::BTreeMap<String, Value>,
    ) -> Result<String, LixError> {
        serde_json::to_string(&self.snapshot_json_from_values(schema_key, values)?).map_err(
            |error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    &format!(
                        "failed to serialize live snapshot for schema '{}': {error}",
                        schema_key
                    ),
                )
            },
        )
    }
}

pub(crate) async fn load_live_row_shape_with_backend(
    backend: LiveStateBackendRef<'_>,
    schema_key: &str,
) -> Result<LiveRowShape, LixError> {
    super::storage::load_live_row_access_with_backend(backend, schema_key)
        .await
        .map(live_row_shape_from_storage)
}

pub(crate) async fn load_live_row_shape_for_table_name(
    backend: LiveStateBackendRef<'_>,
    table_name: &str,
) -> Result<Option<LiveRowShape>, LixError> {
    super::storage::load_live_row_access_for_table_name(backend, table_name)
        .await
        .map(|access| access.map(live_row_shape_from_storage))
}

pub(crate) fn live_row_shape_from_definition(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
) -> Result<LiveRowShape, LixError> {
    schema_layout(schema_key, schema_definition)
        .map(|layout| live_row_shape_from_storage(super::storage::LiveRowAccess::new(layout)))
}

pub(crate) fn live_row_shape_from_layout(layout: super::storage::LiveTableLayout) -> LiveRowShape {
    live_row_shape_from_storage(super::storage::LiveRowAccess::new(layout))
}

pub(crate) async fn live_storage_relation_exists_with_backend(
    backend: LiveStateBackendRef<'_>,
    schema_key: &str,
) -> Result<bool, LixError> {
    let relation_name = tracked_relation_name(schema_key);
    crate::live_state::storage::live_storage_relation_exists(backend, &relation_name).await
}

pub(crate) fn tracked_relation_name(schema_key: &str) -> String {
    super::storage::tracked_live_table_name(schema_key)
}

pub(crate) fn snapshot_select_expr_for_schema(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
    dialect: SqlDialect,
    table_alias: Option<&str>,
) -> Result<String, LixError> {
    Ok(
        live_row_shape_from_definition(schema_key, schema_definition)?
            .snapshot_select_expr(dialect, table_alias),
    )
}

pub(crate) fn logical_snapshot_from_projected_row_with_shape(
    access: Option<&LiveRowShape>,
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

fn live_row_shape_from_storage(access: super::storage::LiveRowAccess) -> LiveRowShape {
    LiveRowShape {
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
