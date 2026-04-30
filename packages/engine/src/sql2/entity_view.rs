use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
#[cfg(test)]
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::datasource::ViewTable;
use datafusion::logical_expr::expr::FieldMetadata;
use datafusion::logical_expr::expr_fn::{col, try_cast};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::prelude::SessionContext;

use super::udfs::{lix_json_extract_expr, lix_json_extract_text_expr, lix_text_encode_expr};
use crate::catalog::{
    state_relation_column_is_nullable_for_variant, state_relation_columns_for_variant,
    SurfaceColumnType, SurfaceFamily, SurfaceRegistry, SurfaceVariant,
};
#[cfg(test)]
use crate::live_state::StateSurfaceColumn;
use crate::LixError;

pub(crate) const VARIANT_FIELD_METADATA_KEY: &str = "lix.surface_type";
pub(crate) const VARIANT_FIELD_METADATA_VALUE: &str = "variant";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Sql2EntityViewBaseRelation {
    LixState,
    LixStateByVersion,
    LixStateHistory,
}

impl Sql2EntityViewBaseRelation {
    #[cfg(test)]
    pub(crate) fn relation_name(self) -> &'static str {
        match self {
            Self::LixState => "lix_state",
            Self::LixStateByVersion => "lix_state_by_version",
            Self::LixStateHistory => "lix_state_history",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PreparedSql2EntityViewExpr {
    BaseRelationColumn { column_name: String },
    JsonPayloadProperty { property_name: String },
}

impl PreparedSql2EntityViewExpr {
    #[cfg(test)]
    fn extend_required_state_projection(&self, projection: &mut Vec<StateSurfaceColumn>) {
        let mut ensure = |column| {
            if !projection.contains(&column) {
                projection.push(column);
            }
        };

        match self {
            Self::BaseRelationColumn { column_name } => {
                if let Some(column) = state_surface_column_from_source_column_name(column_name) {
                    ensure(column);
                }
            }
            Self::JsonPayloadProperty { .. } => ensure(StateSurfaceColumn::SnapshotContent),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedSql2EntityViewColumn {
    pub(crate) public_name: String,
    pub(crate) column_type: SurfaceColumnType,
    pub(crate) nullable: bool,
    pub(crate) expression: PreparedSql2EntityViewExpr,
}

impl PreparedSql2EntityViewColumn {
    pub(crate) fn projection_expr(&self) -> Expr {
        let expr = entity_view_projection_expr(self);
        // Only explicit engine-owned Variant columns carry variant metadata.
        // Schema-derived JSON fields should flow through the Json branch below.
        if self.column_type == SurfaceColumnType::Variant {
            expr.alias_with_metadata(self.public_name.clone(), Some(variant_field_metadata()))
        } else {
            expr.alias(self.public_name.clone())
        }
    }

    #[cfg(test)]
    pub(crate) fn output_field(&self) -> Field {
        Field::new(
            self.public_name.clone(),
            arrow_data_type_for_surface_column_type(self.column_type),
            self.nullable,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedSql2EntityViewPlan {
    pub(crate) public_name: String,
    pub(crate) schema_key: String,
    pub(crate) surface_variant: SurfaceVariant,
    pub(crate) base_relation: Sql2EntityViewBaseRelation,
    pub(crate) column_order: Vec<String>,
    pub(crate) column_types: BTreeMap<String, SurfaceColumnType>,
    pub(crate) column_plans: BTreeMap<String, PreparedSql2EntityViewColumn>,
}

impl PreparedSql2EntityViewPlan {
    pub(crate) fn column_plan(&self, column_name: &str) -> Option<&PreparedSql2EntityViewColumn> {
        self.column_plans.get(column_name)
    }

    #[cfg(test)]
    pub(crate) fn required_state_projection(
        &self,
        projected_columns: &[String],
    ) -> Vec<StateSurfaceColumn> {
        let mut projection = Vec::<StateSurfaceColumn>::new();
        for column_name in projected_columns {
            if let Some(column) = self.column_plan(column_name) {
                column
                    .expression
                    .extend_required_state_projection(&mut projection);
            }
        }

        if projection.is_empty() {
            projection.push(StateSurfaceColumn::SnapshotContent);
        }

        projection
    }

    #[cfg(test)]
    pub(crate) fn projected_schema(
        &self,
        projected_columns: &[String],
    ) -> Result<SchemaRef, LixError> {
        let fields = projected_columns
            .iter()
            .map(|column_name| {
                self.column_plan(column_name)
                    .map(PreparedSql2EntityViewColumn::output_field)
                    .ok_or_else(|| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!(
                                "sql2 entity view '{}' is missing output field metadata for '{}'",
                                self.public_name, column_name
                            ),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Arc::new(Schema::new(fields)))
    }

    pub(crate) fn projection_exprs(
        &self,
        projected_columns: &[String],
    ) -> Result<Vec<Expr>, LixError> {
        projected_columns
            .iter()
            .map(|column_name| {
                self.column_plan(column_name)
                    .map(PreparedSql2EntityViewColumn::projection_expr)
                    .ok_or_else(|| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!(
                                "sql2 entity view '{}' is missing projection expr metadata for '{}'",
                                self.public_name, column_name
                            ),
                        )
                    })
            })
            .collect()
    }

    pub(crate) fn compiled_logical_plan(
        &self,
        ctx: &SessionContext,
        provider: Arc<dyn TableProvider>,
    ) -> Result<LogicalPlan, LixError> {
        let projection_exprs = self.projection_exprs(&self.column_order)?;
        let dataframe = ctx
            .read_table(provider)
            .map_err(datafusion_error_to_lix_error)?
            .filter(col("schema_key").eq(datafusion::logical_expr::lit(self.schema_key.clone())))
            .map_err(datafusion_error_to_lix_error)?
            .select(projection_exprs)
            .map_err(datafusion_error_to_lix_error)?;
        Ok(dataframe.into_unoptimized_plan())
    }

    pub(crate) fn compiled_view_provider(
        &self,
        ctx: &SessionContext,
        provider: Arc<dyn TableProvider>,
    ) -> Result<Arc<dyn TableProvider>, LixError> {
        Ok(Arc::new(ViewTable::new(
            self.compiled_logical_plan(ctx, provider)?,
            None,
        )))
    }
}

#[cfg(test)]
pub(crate) fn arrow_data_type_for_surface_column_type(column_type: SurfaceColumnType) -> DataType {
    match column_type {
        SurfaceColumnType::String | SurfaceColumnType::Json => DataType::Utf8,
        SurfaceColumnType::Variant => DataType::Binary,
        SurfaceColumnType::Integer => DataType::Int64,
        SurfaceColumnType::Number => DataType::Float64,
        SurfaceColumnType::Boolean => DataType::Boolean,
    }
}

pub(crate) fn prepared_entity_view_plans_for_registry(
    registry: &SurfaceRegistry,
    surface_names: &[String],
) -> BTreeMap<String, PreparedSql2EntityViewPlan> {
    surface_names
        .iter()
        .filter_map(|surface_name| {
            let resolved = registry.bind_relation_name(surface_name)?;
            let schema_key = resolved.implicit_overrides.fixed_schema_key.clone()?;
            (resolved.descriptor.surface_family == SurfaceFamily::Entity).then(|| {
                let column_order = resolved
                    .descriptor
                    .visible_columns
                    .iter()
                    .chain(resolved.descriptor.hidden_columns.iter())
                    .cloned()
                    .collect::<Vec<_>>();
                let column_types = resolved.column_types.clone();
                let column_plans = column_order
                    .iter()
                    .map(|column_name| {
                        let expression = prepared_entity_view_expr_for_column(
                            resolved.descriptor.surface_variant,
                            column_name,
                        );
                        (
                            column_name.clone(),
                            PreparedSql2EntityViewColumn {
                                public_name: column_name.clone(),
                                column_type: *column_types
                                    .get(column_name)
                                    .expect("entity-view column should have a type"),
                                nullable: entity_view_column_is_nullable(
                                    resolved.descriptor.surface_variant,
                                    &expression,
                                ),
                                expression,
                            },
                        )
                    })
                    .collect::<BTreeMap<_, _>>();

                (
                    surface_name.clone(),
                    PreparedSql2EntityViewPlan {
                        public_name: resolved.descriptor.public_name.clone(),
                        schema_key,
                        surface_variant: resolved.descriptor.surface_variant,
                        base_relation: entity_view_base_relation(
                            resolved.descriptor.surface_variant,
                        ),
                        column_order,
                        column_types,
                        column_plans,
                    },
                )
            })
        })
        .collect()
}

fn entity_view_base_relation(surface_variant: SurfaceVariant) -> Sql2EntityViewBaseRelation {
    match surface_variant {
        SurfaceVariant::Default | SurfaceVariant::WorkingChanges => {
            Sql2EntityViewBaseRelation::LixState
        }
        SurfaceVariant::ByVersion => Sql2EntityViewBaseRelation::LixStateByVersion,
        SurfaceVariant::History => Sql2EntityViewBaseRelation::LixStateHistory,
    }
}

fn entity_base_relation_variant(surface_variant: SurfaceVariant) -> SurfaceVariant {
    match surface_variant {
        SurfaceVariant::Default | SurfaceVariant::WorkingChanges => SurfaceVariant::Default,
        SurfaceVariant::ByVersion => SurfaceVariant::ByVersion,
        SurfaceVariant::History => SurfaceVariant::History,
    }
}

fn prepared_entity_view_expr_for_column(
    surface_variant: SurfaceVariant,
    column_name: &str,
) -> PreparedSql2EntityViewExpr {
    if let Some(base_column_name) = column_name.strip_prefix("lixcol_") {
        if state_relation_columns_for_variant(entity_base_relation_variant(surface_variant))
            .iter()
            .any(|candidate| candidate == base_column_name)
        {
            return PreparedSql2EntityViewExpr::BaseRelationColumn {
                column_name: base_column_name.to_string(),
            };
        }
    }

    PreparedSql2EntityViewExpr::JsonPayloadProperty {
        property_name: column_name.to_string(),
    }
}

fn entity_view_projection_expr(column: &PreparedSql2EntityViewColumn) -> Expr {
    match &column.expression {
        PreparedSql2EntityViewExpr::BaseRelationColumn { column_name } => col(column_name.clone()),
        PreparedSql2EntityViewExpr::JsonPayloadProperty { property_name } => {
            json_payload_projection_expr(property_name, column.column_type)
        }
    }
}

#[cfg(test)]
fn state_surface_column_from_source_column_name(column_name: &str) -> Option<StateSurfaceColumn> {
    match column_name {
        "entity_id" => Some(StateSurfaceColumn::EntityId),
        "schema_key" => Some(StateSurfaceColumn::SchemaKey),
        "file_id" => Some(StateSurfaceColumn::FileId),
        "snapshot_content" => Some(StateSurfaceColumn::SnapshotContent),
        "metadata" => Some(StateSurfaceColumn::Metadata),
        "schema_version" => Some(StateSurfaceColumn::SchemaVersion),
        "created_at" => Some(StateSurfaceColumn::CreatedAt),
        "updated_at" => Some(StateSurfaceColumn::UpdatedAt),
        "global" => Some(StateSurfaceColumn::Global),
        "change_id" => Some(StateSurfaceColumn::ChangeId),
        "commit_id" => Some(StateSurfaceColumn::CommitId),
        "untracked" => Some(StateSurfaceColumn::Untracked),
        "version_id" => Some(StateSurfaceColumn::VersionId),
        _ => None,
    }
}

fn entity_view_column_is_nullable(
    surface_variant: SurfaceVariant,
    expression: &PreparedSql2EntityViewExpr,
) -> bool {
    match expression {
        // Payload columns are nullable by construction: missing properties,
        // explicit JSON nulls, and failed numeric coercions all surface as NULL.
        PreparedSql2EntityViewExpr::JsonPayloadProperty { .. } => true,
        PreparedSql2EntityViewExpr::BaseRelationColumn { column_name } => {
            state_relation_column_is_nullable_for_variant(
                entity_base_relation_variant(surface_variant),
                column_name,
            )
            .unwrap_or(true)
        }
    }
}

fn json_payload_projection_expr(property_name: &str, column_type: SurfaceColumnType) -> Expr {
    let snapshot_content = col("snapshot_content");
    match column_type {
        SurfaceColumnType::String => lix_json_extract_text_expr(snapshot_content, property_name),
        SurfaceColumnType::Json => lix_json_extract_expr(snapshot_content, property_name),
        // Variant remains available for future explicit owner-chosen polymorphic
        // payloads, but schema-derived JSON fields must not route through it.
        SurfaceColumnType::Variant => {
            lix_text_encode_expr(lix_json_extract_expr(snapshot_content, property_name))
        }
        SurfaceColumnType::Boolean => try_cast(
            lix_json_extract_text_expr(snapshot_content, property_name),
            DataType::Boolean,
        ),
        SurfaceColumnType::Integer => try_cast(
            lix_json_extract_text_expr(snapshot_content, property_name),
            DataType::Int64,
        ),
        SurfaceColumnType::Number => try_cast(
            lix_json_extract_text_expr(snapshot_content, property_name),
            DataType::Float64,
        ),
    }
}

fn datafusion_error_to_lix_error(error: datafusion::common::DataFusionError) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("sql2 DataFusion error: {error}"),
    )
}

fn variant_field_metadata() -> FieldMetadata {
    std::collections::HashMap::from([(
        VARIANT_FIELD_METADATA_KEY.to_string(),
        VARIANT_FIELD_METADATA_VALUE.to_string(),
    )])
    .into()
}

#[cfg(test)]
mod tests {
    use super::{
        prepared_entity_view_plans_for_registry, variant_field_metadata,
        PreparedSql2EntityViewColumn, PreparedSql2EntityViewExpr, Sql2EntityViewBaseRelation,
    };
    use crate::catalog::{
        build_builtin_surface_registry, dynamic_entity_surface_spec_from_schema,
        register_dynamic_entity_surface_spec, SurfaceColumnType,
    };
    use crate::live_state::StateSurfaceColumn;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::MemTable;
    use datafusion::logical_expr::expr::{Alias, ScalarFunction, TryCast};
    use datafusion::logical_expr::Expr;
    use datafusion::prelude::SessionContext;
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn default_entity_surface_compiles_to_lix_state_view() {
        let mut registry = build_builtin_surface_registry();
        register_dynamic_entity_surface_spec(
            &mut registry,
            dynamic_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "project_message",
                "type": "object",
                "properties": {
                    "id": { "type": "string" },
                    "message": { "type": "string" }
                }
            }))
            .expect("schema should compile"),
        );

        let plans =
            prepared_entity_view_plans_for_registry(&registry, &["project_message".to_string()]);
        let plan = plans
            .get("project_message")
            .expect("default entity surface should compile");

        assert_eq!(plan.base_relation, Sql2EntityViewBaseRelation::LixState);
        assert_eq!(plan.base_relation.relation_name(), "lix_state");
    }

    #[test]
    fn by_version_entity_surface_compiles_to_lix_state_by_version_view() {
        let registry = build_builtin_surface_registry();
        let plans = prepared_entity_view_plans_for_registry(
            &registry,
            &["lix_registered_schema_by_version".to_string()],
        );
        let plan = plans
            .get("lix_registered_schema_by_version")
            .expect("by-version entity surface should compile");

        assert_eq!(
            plan.base_relation,
            Sql2EntityViewBaseRelation::LixStateByVersion
        );
        assert_eq!(plan.base_relation.relation_name(), "lix_state_by_version");
    }

    #[test]
    fn history_entity_surface_compiles_to_lix_state_history_view() {
        let mut registry = build_builtin_surface_registry();
        register_dynamic_entity_surface_spec(
            &mut registry,
            dynamic_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "project_event",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            }))
            .expect("schema should compile"),
        );

        let plans = prepared_entity_view_plans_for_registry(
            &registry,
            &["project_event_history".to_string()],
        );
        let plan = plans
            .get("project_event_history")
            .expect("history entity surface should compile");

        assert_eq!(
            plan.base_relation,
            Sql2EntityViewBaseRelation::LixStateHistory
        );
        assert_eq!(plan.base_relation.relation_name(), "lix_state_history");
        assert_eq!(
            plan.column_plan("lixcol_start_commit_id")
                .expect("history root column should compile")
                .expression,
            PreparedSql2EntityViewExpr::BaseRelationColumn {
                column_name: "start_commit_id".to_string()
            }
        );
        assert_eq!(
            plan.column_plan("lixcol_depth")
                .expect("history depth column should compile")
                .expression,
            PreparedSql2EntityViewExpr::BaseRelationColumn {
                column_name: "depth".to_string()
            }
        );
        assert!(plan.column_plan("lixcol_version_id").is_none());
    }

    #[test]
    fn entity_view_plan_tracks_payload_and_hidden_column_sources() {
        let registry = build_builtin_surface_registry();
        let plans = prepared_entity_view_plans_for_registry(
            &registry,
            &["lix_registered_schema".to_string()],
        );
        let plan = plans
            .get("lix_registered_schema")
            .expect("registered schema surface should compile");

        assert_eq!(
            plan.column_plan("value")
                .expect("payload column should compile")
                .expression,
            PreparedSql2EntityViewExpr::JsonPayloadProperty {
                property_name: "value".to_string()
            }
        );
        assert_eq!(
            plan.column_plan("lixcol_entity_id")
                .expect("hidden column should compile")
                .expression,
            PreparedSql2EntityViewExpr::BaseRelationColumn {
                column_name: "entity_id".to_string()
            }
        );
        assert_eq!(
            plan.column_plan("lixcol_snapshot_content")
                .expect("derived base column should compile")
                .expression,
            PreparedSql2EntityViewExpr::BaseRelationColumn {
                column_name: "snapshot_content".to_string()
            }
        );
        assert!(plan.column_plan("lixcol_version_id").is_none());
        assert_eq!(
            plan.required_state_projection(&["value".to_string(), "lixcol_entity_id".to_string()]),
            vec![
                StateSurfaceColumn::SnapshotContent,
                StateSurfaceColumn::EntityId
            ]
        );
    }

    #[test]
    fn integer_payload_projection_uses_json_extract_then_try_cast() {
        let mut registry = build_builtin_surface_registry();
        register_dynamic_entity_surface_spec(
            &mut registry,
            dynamic_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "metrics",
                "type": "object",
                "properties": {
                    "count": { "type": "integer" }
                }
            }))
            .expect("schema should compile"),
        );

        let plans = prepared_entity_view_plans_for_registry(&registry, &["metrics".to_string()]);
        let plan = plans
            .get("metrics")
            .expect("metrics surface should compile");
        let exprs = plan
            .projection_exprs(&["count".to_string()])
            .expect("projection exprs should build");

        let Expr::Alias(Alias { expr, name, .. }) = &exprs[0] else {
            panic!("projection should alias the payload expr");
        };
        assert_eq!(name, "count");

        let Expr::TryCast(TryCast {
            expr: inner,
            data_type,
        }) = expr.as_ref()
        else {
            panic!("integer payloads should compile to try_cast");
        };
        assert_eq!(data_type, &DataType::Int64);

        let Expr::ScalarFunction(ScalarFunction { func, args }) = inner.as_ref() else {
            panic!("integer payloads should extract from JSON first");
        };
        assert_eq!(func.name(), "lix_json_extract_text");
        assert_eq!(args.len(), 2);
    }

    #[test]
    fn boolean_payload_projection_casts_text_json_extract() {
        let mut registry = build_builtin_surface_registry();
        register_dynamic_entity_surface_spec(
            &mut registry,
            dynamic_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "flags",
                "type": "object",
                "properties": {
                    "enabled": { "type": "boolean" }
                }
            }))
            .expect("schema should compile"),
        );

        let plans = prepared_entity_view_plans_for_registry(&registry, &["flags".to_string()]);
        let plan = plans.get("flags").expect("flags surface should compile");
        let exprs = plan
            .projection_exprs(&["enabled".to_string()])
            .expect("projection exprs should build");

        let Expr::Alias(Alias { expr, .. }) = &exprs[0] else {
            panic!("projection should alias the payload expr");
        };
        let Expr::TryCast(TryCast {
            expr: inner,
            data_type,
        }) = expr.as_ref()
        else {
            panic!("boolean payloads should compile to try_cast");
        };
        assert_eq!(data_type, &DataType::Boolean);
        let Expr::ScalarFunction(ScalarFunction { func, .. }) = inner.as_ref() else {
            panic!("boolean payload should extract from JSON first");
        };
        assert_eq!(func.name(), "lix_json_extract_text");
    }

    #[test]
    fn json_payload_projection_uses_json_extract_for_lix_key_value() {
        let registry = build_builtin_surface_registry();
        let plans =
            prepared_entity_view_plans_for_registry(&registry, &["lix_key_value".to_string()]);
        let plan = plans
            .get("lix_key_value")
            .expect("lix_key_value surface should compile");

        assert_eq!(
            plan.column_types.get("value"),
            Some(&SurfaceColumnType::Json)
        );

        let exprs = plan
            .projection_exprs(&["value".to_string()])
            .expect("projection exprs should build");

        let Expr::Alias(Alias { expr, .. }) = &exprs[0] else {
            panic!("projection should alias the payload expr");
        };
        let Expr::ScalarFunction(ScalarFunction { func, .. }) = expr.as_ref() else {
            panic!("json payload should compile to a scalar function");
        };
        assert_eq!(func.name(), "lix_json_extract");
    }

    #[test]
    fn lix_registered_schema_value_stays_json_in_sql2() {
        let registry = build_builtin_surface_registry();
        let plans = prepared_entity_view_plans_for_registry(
            &registry,
            &["lix_registered_schema".to_string()],
        );
        let plan = plans
            .get("lix_registered_schema")
            .expect("registered schema surface should compile");

        assert_eq!(
            plan.column_types.get("value"),
            Some(&SurfaceColumnType::Json)
        );

        let exprs = plan
            .projection_exprs(&["value".to_string()])
            .expect("projection exprs should build");

        let Expr::Alias(Alias { expr, .. }) = &exprs[0] else {
            panic!("projection should alias the payload expr");
        };
        let Expr::ScalarFunction(ScalarFunction { func, .. }) = expr.as_ref() else {
            panic!("registered schema JSON payload should compile to a scalar function");
        };
        assert_eq!(func.name(), "lix_json_extract");

        let schema = plan
            .projected_schema(&["value".to_string()])
            .expect("schema should build");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert!(schema.field(0).is_nullable());
    }

    #[test]
    fn schema_derived_multi_kind_json_payload_stays_utf8_json_in_sql2() {
        let mut registry = build_builtin_surface_registry();
        register_dynamic_entity_surface_spec(
            &mut registry,
            dynamic_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "flex_value",
                "type": "object",
                "properties": {
                    "value": {
                        "anyOf": [
                            { "type": "string" },
                            { "type": "object" }
                        ]
                    }
                }
            }))
            .expect("schema should compile"),
        );

        let plans = prepared_entity_view_plans_for_registry(&registry, &["flex_value".to_string()]);
        let plan = plans
            .get("flex_value")
            .expect("flex_value surface should compile");

        assert_eq!(
            plan.column_types.get("value"),
            Some(&SurfaceColumnType::Json)
        );

        let exprs = plan
            .projection_exprs(&["value".to_string()])
            .expect("projection exprs should build");

        let Expr::Alias(Alias { expr, .. }) = &exprs[0] else {
            panic!("projection should alias the payload expr");
        };
        let Expr::ScalarFunction(ScalarFunction { func, .. }) = expr.as_ref() else {
            panic!("json payload should compile to a scalar function");
        };
        assert_eq!(func.name(), "lix_json_extract");

        let schema = plan
            .projected_schema(&["value".to_string()])
            .expect("schema should build");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert!(schema.field(0).is_nullable());
    }

    #[test]
    fn explicit_variant_column_keeps_variant_sql2_behavior() {
        let column = PreparedSql2EntityViewColumn {
            public_name: "value".to_string(),
            column_type: SurfaceColumnType::Variant,
            nullable: true,
            expression: PreparedSql2EntityViewExpr::JsonPayloadProperty {
                property_name: "value".to_string(),
            },
        };

        let expr = column.projection_expr();
        let Expr::Alias(Alias {
            expr,
            metadata,
            name,
            ..
        }) = expr
        else {
            panic!("projection should alias the variant payload expr");
        };
        assert_eq!(name, "value");
        assert_eq!(metadata.as_ref(), Some(&variant_field_metadata()));

        let Expr::ScalarFunction(ScalarFunction { func, args }) = expr.as_ref() else {
            panic!("variant payload should compile to a scalar function");
        };
        assert_eq!(func.name(), "lix_text_encode");
        let Expr::ScalarFunction(ScalarFunction { func, .. }) = &args[0] else {
            panic!("variant payload should encode JSON extraction");
        };
        assert_eq!(func.name(), "lix_json_extract");

        let field = column.output_field();
        assert_eq!(field.data_type(), &DataType::Binary);
        assert!(field.is_nullable());
    }

    #[test]
    fn projected_schema_centralizes_field_types_and_nullability() {
        let registry = build_builtin_surface_registry();
        let plans = prepared_entity_view_plans_for_registry(
            &registry,
            &["lix_registered_schema".to_string()],
        );
        let plan = plans
            .get("lix_registered_schema")
            .expect("registered schema surface should compile");

        let schema = plan
            .projected_schema(&[
                "value".to_string(),
                "lixcol_entity_id".to_string(),
                "lixcol_global".to_string(),
                "lixcol_metadata".to_string(),
            ])
            .expect("schema should build");

        assert_eq!(schema.field(0).name(), "value");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert!(schema.field(0).is_nullable());

        assert_eq!(schema.field(1).name(), "lixcol_entity_id");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert!(!schema.field(1).is_nullable());

        assert_eq!(schema.field(2).name(), "lixcol_global");
        assert_eq!(schema.field(2).data_type(), &DataType::Boolean);
        assert!(!schema.field(2).is_nullable());

        assert_eq!(schema.field(3).name(), "lixcol_metadata");
        assert_eq!(schema.field(3).data_type(), &DataType::Utf8);
        assert!(schema.field(3).is_nullable());
    }

    #[test]
    fn history_projected_schema_uses_history_base_relation_nullability() {
        let mut registry = build_builtin_surface_registry();
        register_dynamic_entity_surface_spec(
            &mut registry,
            dynamic_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "project_event",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            }))
            .expect("schema should compile"),
        );

        let plans = prepared_entity_view_plans_for_registry(
            &registry,
            &["project_event_history".to_string()],
        );
        let plan = plans
            .get("project_event_history")
            .expect("history entity surface should compile");

        let schema = plan
            .projected_schema(&[
                "lixcol_commit_id".to_string(),
                "lixcol_commit_created_at".to_string(),
                "lixcol_start_commit_id".to_string(),
                "lixcol_depth".to_string(),
                "lixcol_metadata".to_string(),
            ])
            .expect("history schema should build");

        assert_eq!(schema.field(0).name(), "lixcol_commit_id");
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "lixcol_commit_created_at");
        assert!(!schema.field(1).is_nullable());
        assert_eq!(schema.field(2).name(), "lixcol_start_commit_id");
        assert!(!schema.field(2).is_nullable());
        assert_eq!(schema.field(3).name(), "lixcol_depth");
        assert!(!schema.field(3).is_nullable());
        assert_eq!(schema.field(4).name(), "lixcol_metadata");
        assert!(schema.field(4).is_nullable());
    }

    #[test]
    fn compiled_logical_plan_is_state_filter_plus_projection() {
        let registry = build_builtin_surface_registry();
        let plans = prepared_entity_view_plans_for_registry(
            &registry,
            &["lix_registered_schema".to_string()],
        );
        let plan = plans
            .get("lix_registered_schema")
            .expect("registered schema surface should compile");
        let ctx = SessionContext::new();
        let provider = Arc::new(
            MemTable::try_new(default_state_relation_schema(), vec![vec![]])
                .expect("memtable should build"),
        );

        let logical_plan = plan
            .compiled_logical_plan(&ctx, provider)
            .expect("logical plan should compile");
        let display = format!("{}", logical_plan.display_indent());

        assert!(display.contains("Filter:"));
        assert!(display.contains("schema_key = Utf8(\"lix_registered_schema\")"));
        assert!(display.contains("lixcol_entity_id"));
        assert!(display.contains("lixcol_snapshot_content"));
        assert!(display.contains("value"));
    }

    #[test]
    fn compiled_history_logical_plan_is_state_history_filter_plus_projection() {
        let mut registry = build_builtin_surface_registry();
        register_dynamic_entity_surface_spec(
            &mut registry,
            dynamic_entity_surface_spec_from_schema(&json!({
                "x-lix-key": "project_event",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                }
            }))
            .expect("schema should compile"),
        );

        let plans = prepared_entity_view_plans_for_registry(
            &registry,
            &["project_event_history".to_string()],
        );
        let plan = plans
            .get("project_event_history")
            .expect("history entity surface should compile");
        let ctx = SessionContext::new();
        let provider = Arc::new(
            MemTable::try_new(history_state_relation_schema(), vec![vec![]])
                .expect("history memtable should build"),
        );

        let logical_plan = plan
            .compiled_logical_plan(&ctx, provider)
            .expect("history logical plan should compile");
        let display = format!("{}", logical_plan.display_indent());

        assert!(display.contains("Filter:"));
        assert!(display.contains("schema_key = Utf8(\"project_event\")"));
        assert!(display.contains("value"));
        assert!(display.contains("lixcol_entity_id"));
        assert!(display.contains("lixcol_start_commit_id"));
        assert!(display.contains("lixcol_depth"));
    }

    fn default_state_relation_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("entity_id", DataType::Utf8, false),
            Field::new("schema_key", DataType::Utf8, false),
            Field::new("file_id", DataType::Utf8, true),
            Field::new("snapshot_content", DataType::Utf8, true),
            Field::new("metadata", DataType::Utf8, true),
            Field::new("schema_version", DataType::Utf8, true),
            Field::new("created_at", DataType::Utf8, true),
            Field::new("updated_at", DataType::Utf8, true),
            Field::new("global", DataType::Boolean, false),
            Field::new("change_id", DataType::Utf8, true),
            Field::new("commit_id", DataType::Utf8, true),
            Field::new("untracked", DataType::Boolean, false),
        ]))
    }

    fn history_state_relation_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("entity_id", DataType::Utf8, false),
            Field::new("schema_key", DataType::Utf8, false),
            Field::new("file_id", DataType::Utf8, true),
            Field::new("snapshot_content", DataType::Utf8, true),
            Field::new("metadata", DataType::Utf8, true),
            Field::new("schema_version", DataType::Utf8, false),
            Field::new("change_id", DataType::Utf8, false),
            Field::new("commit_id", DataType::Utf8, false),
            Field::new("commit_created_at", DataType::Utf8, false),
            Field::new("start_commit_id", DataType::Utf8, false),
            Field::new("depth", DataType::Int64, false),
        ]))
    }
}
