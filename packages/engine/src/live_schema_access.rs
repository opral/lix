use crate::live_state::shared::snapshot_sql::live_snapshot_select_expr;
use crate::live_state::storage::{
    builtin_live_table_layout, live_column_name_for_property, live_table_layout_from_schema,
    tracked_live_table_name, LiveRowAccess, LiveTableLayout,
};
use crate::{LixError, SqlDialect};
use serde_json::Value as JsonValue;

pub(crate) fn tracked_relation_name(schema_key: &str) -> String {
    tracked_live_table_name(schema_key)
}

pub(crate) fn payload_column_name_for_schema(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
    property_name: &str,
) -> Result<String, LixError> {
    let layout = schema_layout(schema_key, schema_definition)?;
    live_column_name_for_property(&layout, property_name)
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
        LiveRowAccess::new(schema_layout(schema_key, schema_definition)?)
            .normalized_projection_sql(table_alias),
    )
}

pub(crate) fn snapshot_select_expr_for_schema(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
    dialect: SqlDialect,
    table_alias: Option<&str>,
) -> Result<String, LixError> {
    Ok(live_snapshot_select_expr(
        &schema_layout(schema_key, schema_definition)?,
        dialect,
        table_alias,
    ))
}

fn schema_layout(
    schema_key: &str,
    schema_definition: Option<&JsonValue>,
) -> Result<LiveTableLayout, LixError> {
    if let Some(schema_definition) = schema_definition {
        return live_table_layout_from_schema(schema_definition);
    }
    builtin_live_table_layout(schema_key)?.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("missing live schema definition for '{}'", schema_key),
        )
    })
}
