use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::TableProvider;
use datafusion::datasource::ViewTable;
use datafusion::logical_expr::expr_fn::{col, try_cast};
use datafusion::logical_expr::{lit, Expr, LogicalPlan};
use datafusion::prelude::SessionContext;
use serde_json::Value as JsonValue;

use crate::LixError;

use super::udf::{
    lix_json_extract_boolean_expr, lix_json_extract_json_expr, lix_json_extract_text_expr,
};

pub(crate) async fn register_entity_views(
    ctx: &SessionContext,
    schema_definitions: &[JsonValue],
) -> Result<(), LixError> {
    let lix_state = ctx
        .table_provider("lix_state")
        .await
        .map_err(datafusion_error_to_lix_error)?;
    let lix_state_by_version = ctx
        .table_provider("lix_state_by_version")
        .await
        .map_err(datafusion_error_to_lix_error)?;

    for schema in schema_definitions {
        let spec = match derive_entity_view_spec_from_schema(schema) {
            Ok(spec) => spec,
            Err(_) => continue,
        };

        if !schema_exposed_as_entity_view(&spec.schema_key) {
            continue;
        }

        register_entity_view(
            ctx,
            &spec.schema_key,
            &spec.schema_key,
            &spec.schema_key,
            EntityViewVariant::Default,
            Arc::clone(&lix_state),
            &spec.column_types,
            &spec.visible_columns,
        )?;

        let by_version_name = format!("{}_by_version", spec.schema_key);
        register_entity_view(
            ctx,
            &by_version_name,
            &spec.schema_key,
            &spec.schema_key,
            EntityViewVariant::ByVersion,
            Arc::clone(&lix_state_by_version),
            &spec.column_types,
            &spec.visible_columns,
        )?;
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EntityViewVariant {
    Default,
    ByVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EntityColumnType {
    String,
    Json,
    Integer,
    Number,
    Boolean,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EntityViewSpec {
    schema_key: String,
    visible_columns: Vec<String>,
    column_types: BTreeMap<String, EntityColumnType>,
}

fn schema_exposed_as_entity_view(schema_key: &str) -> bool {
    !matches!(schema_key, "lix_active_version" | "lix_active_account")
}

fn register_entity_view(
    ctx: &SessionContext,
    relation_name: &str,
    public_name: &str,
    schema_key: &str,
    variant: EntityViewVariant,
    provider: Arc<dyn TableProvider>,
    schema_column_types: &BTreeMap<String, EntityColumnType>,
    visible_columns: &[String],
) -> Result<(), LixError> {
    let logical_plan = compiled_entity_view_logical_plan(
        ctx,
        schema_key,
        variant,
        provider,
        schema_column_types,
        visible_columns,
    )?;
    ctx.register_table(relation_name, Arc::new(ViewTable::new(logical_plan, None)))
        .map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "sql2 failed to register entity view '{public_name}' as '{relation_name}': {error}"
                ),
            )
        })?;
    Ok(())
}

fn compiled_entity_view_logical_plan(
    ctx: &SessionContext,
    schema_key: &str,
    variant: EntityViewVariant,
    provider: Arc<dyn TableProvider>,
    schema_column_types: &BTreeMap<String, EntityColumnType>,
    visible_columns: &[String],
) -> Result<LogicalPlan, LixError> {
    let projection_exprs =
        entity_view_projection_exprs(variant, schema_column_types, visible_columns);
    let dataframe = ctx
        .read_table(provider)
        .map_err(datafusion_error_to_lix_error)?
        .filter(col("schema_key").eq(lit(schema_key.to_string())))
        .map_err(datafusion_error_to_lix_error)?
        .select(projection_exprs)
        .map_err(datafusion_error_to_lix_error)?;

    Ok(dataframe.into_unoptimized_plan())
}

fn entity_view_projection_exprs(
    variant: EntityViewVariant,
    schema_column_types: &BTreeMap<String, EntityColumnType>,
    visible_columns: &[String],
) -> Vec<Expr> {
    let mut exprs = visible_columns
        .iter()
        .filter_map(|column_name| {
            let column_type = schema_column_types.get(column_name)?;
            Some(
                entity_payload_projection_expr(column_name, *column_type)
                    .alias(column_name.clone()),
            )
        })
        .collect::<Vec<_>>();

    exprs.extend(
        entity_base_relation_columns(variant)
            .into_iter()
            .map(|column_name| col(column_name.clone()).alias(format!("lixcol_{column_name}"))),
    );
    exprs
}

fn entity_payload_projection_expr(property_name: &str, column_type: EntityColumnType) -> Expr {
    let snapshot_content = col("snapshot_content");
    match column_type {
        EntityColumnType::String => lix_json_extract_text_expr(snapshot_content, property_name),
        EntityColumnType::Json => lix_json_extract_json_expr(snapshot_content, property_name),
        EntityColumnType::Boolean => lix_json_extract_boolean_expr(snapshot_content, property_name),
        EntityColumnType::Integer => try_cast(
            lix_json_extract_text_expr(snapshot_content, property_name),
            DataType::Int64,
        ),
        EntityColumnType::Number => try_cast(
            lix_json_extract_text_expr(snapshot_content, property_name),
            DataType::Float64,
        ),
    }
}

#[cfg(test)]
pub(crate) fn entity_view_column_types(
    variant: EntityViewVariant,
    schema_column_types: &BTreeMap<String, EntityColumnType>,
) -> BTreeMap<String, EntityColumnType> {
    let mut column_types = schema_column_types.clone();
    column_types.extend(
        entity_base_relation_column_types(variant)
            .into_iter()
            .map(|(column_name, column_type)| (format!("lixcol_{column_name}"), column_type)),
    );
    column_types
}

fn derive_entity_view_spec_from_schema(schema: &JsonValue) -> Result<EntityViewSpec, LixError> {
    let schema_key = schema
        .get("x-lix-key")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "schema is missing string x-lix-key".to_string(),
            )
        })?;

    let mut visible_columns = schema
        .get("properties")
        .and_then(JsonValue::as_object)
        .map(|properties| {
            let mut columns = properties
                .keys()
                .filter(|key| !key.starts_with("lixcol_"))
                .cloned()
                .collect::<Vec<_>>();
            columns.sort();
            columns
        })
        .unwrap_or_default();
    visible_columns.dedup();

    let column_types = schema
        .get("properties")
        .and_then(JsonValue::as_object)
        .map(|properties| {
            properties
                .iter()
                .filter(|(key, _)| !key.starts_with("lixcol_"))
                .filter_map(|(key, property_schema)| {
                    entity_column_type_from_schema(property_schema).map(|kind| (key.clone(), kind))
                })
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();

    Ok(EntityViewSpec {
        schema_key: schema_key.to_string(),
        visible_columns,
        column_types,
    })
}

fn entity_column_type_from_schema(schema: &JsonValue) -> Option<EntityColumnType> {
    let mut kinds = BTreeSet::new();
    collect_entity_type_kinds(schema, &mut kinds);
    kinds.remove("null");

    if kinds.is_empty() {
        return None;
    }

    if kinds.len() == 1 {
        return match kinds.into_iter().next() {
            Some("boolean") => Some(EntityColumnType::Boolean),
            Some("integer") => Some(EntityColumnType::Integer),
            Some("number") => Some(EntityColumnType::Number),
            Some("string") => Some(EntityColumnType::String),
            Some("object" | "array") => Some(EntityColumnType::Json),
            _ => None,
        };
    }

    Some(EntityColumnType::Json)
}

fn collect_entity_type_kinds<'a>(schema: &'a JsonValue, out: &mut BTreeSet<&'a str>) {
    match schema.get("type") {
        Some(JsonValue::String(kind)) => {
            out.insert(kind.as_str());
        }
        Some(JsonValue::Array(kinds)) => {
            for kind in kinds.iter().filter_map(JsonValue::as_str) {
                out.insert(kind);
            }
        }
        _ => {}
    }

    for keyword in ["anyOf", "oneOf", "allOf"] {
        if let Some(JsonValue::Array(branches)) = schema.get(keyword) {
            for branch in branches {
                collect_entity_type_kinds(branch, out);
            }
        }
    }
}

fn entity_base_relation_columns(variant: EntityViewVariant) -> Vec<String> {
    let mut columns = vec![
        "entity_id".to_string(),
        "schema_key".to_string(),
        "file_id".to_string(),
        "plugin_key".to_string(),
        "snapshot_content".to_string(),
        "metadata".to_string(),
        "schema_version".to_string(),
        "created_at".to_string(),
        "updated_at".to_string(),
        "global".to_string(),
        "change_id".to_string(),
        "commit_id".to_string(),
        "untracked".to_string(),
    ];
    if matches!(variant, EntityViewVariant::ByVersion) {
        columns.push("version_id".to_string());
    }
    columns
}

#[cfg(test)]
fn entity_base_relation_column_types(
    variant: EntityViewVariant,
) -> BTreeMap<String, EntityColumnType> {
    let mut column_types = BTreeMap::from([
        ("entity_id".to_string(), EntityColumnType::String),
        ("schema_key".to_string(), EntityColumnType::String),
        ("file_id".to_string(), EntityColumnType::String),
        ("plugin_key".to_string(), EntityColumnType::String),
        ("snapshot_content".to_string(), EntityColumnType::Json),
        ("metadata".to_string(), EntityColumnType::Json),
        ("schema_version".to_string(), EntityColumnType::String),
        ("created_at".to_string(), EntityColumnType::String),
        ("updated_at".to_string(), EntityColumnType::String),
        ("global".to_string(), EntityColumnType::Boolean),
        ("change_id".to_string(), EntityColumnType::String),
        ("commit_id".to_string(), EntityColumnType::String),
        ("untracked".to_string(), EntityColumnType::Boolean),
    ]);
    if matches!(variant, EntityViewVariant::ByVersion) {
        column_types.insert("version_id".to_string(), EntityColumnType::String);
    }
    column_types
}

fn datafusion_error_to_lix_error(error: datafusion::error::DataFusionError) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("sql2 DataFusion error: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        derive_entity_view_spec_from_schema, entity_view_column_types,
        schema_exposed_as_entity_view, EntityColumnType, EntityViewVariant,
    };
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn excludes_non_entity_builtin_session_surfaces() {
        assert!(!schema_exposed_as_entity_view("lix_active_version"));
        assert!(!schema_exposed_as_entity_view("lix_active_account"));
        assert!(schema_exposed_as_entity_view("project_message"));
    }

    #[test]
    fn includes_hidden_state_columns_for_by_version_views() {
        let column_types = entity_view_column_types(
            EntityViewVariant::ByVersion,
            &BTreeMap::from([("value".to_string(), EntityColumnType::String)]),
        );

        assert_eq!(column_types.get("value"), Some(&EntityColumnType::String));
        assert_eq!(
            column_types.get("lixcol_version_id"),
            Some(&EntityColumnType::String)
        );
        assert_eq!(
            column_types.get("lixcol_entity_id"),
            Some(&EntityColumnType::String)
        );
    }

    #[test]
    fn derives_entity_view_spec_from_schema_definition() {
        let spec = derive_entity_view_spec_from_schema(&json!({
            "x-lix-key": "project_message",
            "type": "object",
            "properties": {
                "body": { "type": "string" },
                "rating": { "type": "number" },
                "meta": { "type": "object" },
                "lixcol_entity_id": { "type": "string" }
            }
        }))
        .expect("schema should derive entity view spec");

        assert_eq!(spec.schema_key, "project_message");
        assert_eq!(
            spec.visible_columns,
            vec!["body".to_string(), "meta".to_string(), "rating".to_string()]
        );
        assert_eq!(
            spec.column_types.get("body"),
            Some(&EntityColumnType::String)
        );
        assert_eq!(
            spec.column_types.get("rating"),
            Some(&EntityColumnType::Number)
        );
        assert_eq!(spec.column_types.get("meta"), Some(&EntityColumnType::Json));
        assert!(!spec.column_types.contains_key("lixcol_entity_id"));
    }
}
