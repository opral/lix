use serde_json::Value as JsonValue;
use sqlparser::ast::{
    BinaryOperator, Expr, Ident, ObjectName, ObjectNamePart, Query, Select, SelectItem,
    TableFactor, Value as AstValue,
};

use crate::sql::read_views::query_builder::{
    column_eq_text, lix_json_text_expr, select_query_from_table,
};

use crate::sql::{
    default_alias, rewrite_query_selects, rewrite_table_factors_in_select_decision,
    visit_query_selects, visit_table_factors_in_select, RewriteDecision,
};
use crate::{LixBackend, LixError};

use super::target::{
    resolve_targets_with_backend, EntityViewTarget, EntityViewVariant,
};

pub(crate) async fn rewrite_query_with_backend(
    backend: &dyn LixBackend,
    query: Query,
) -> Result<Option<Query>, LixError> {
    let view_names = collect_table_view_names(&query);
    if view_names.is_empty() {
        return Ok(None);
    }
    let resolved = resolve_targets_with_backend(backend, &view_names).await?;
    rewrite_query_with_resolver(query, &mut |name| {
        let Some(view_name) = object_name_terminal(name) else {
            return Ok(None);
        };
        let key = view_name.to_ascii_lowercase();
        Ok(resolved.get(&key).cloned())
    })
}

fn rewrite_query_with_resolver(
    query: Query,
    resolver: &mut dyn FnMut(&ObjectName) -> Result<Option<EntityViewTarget>, LixError>,
) -> Result<Option<Query>, LixError> {
    let mut rewrite_select_with_resolver = |select: &mut Select| rewrite_select(select, resolver);
    rewrite_query_selects(query, &mut rewrite_select_with_resolver)
}

fn rewrite_select(
    select: &mut Select,
    resolver: &mut dyn FnMut(&ObjectName) -> Result<Option<EntityViewTarget>, LixError>,
) -> Result<RewriteDecision, LixError> {
    let mut rewrite_factor = |relation: &mut TableFactor| rewrite_table_factor(relation, resolver);
    rewrite_table_factors_in_select_decision(select, &mut rewrite_factor)
}

fn rewrite_table_factor(
    relation: &mut TableFactor,
    resolver: &mut dyn FnMut(&ObjectName) -> Result<Option<EntityViewTarget>, LixError>,
) -> Result<RewriteDecision, LixError> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let Some(target) = resolver(name)? else {
                return Ok(RewriteDecision::Unchanged);
            };
            let derived_query = build_entity_view_query(&target)?;
            let derived_alias = alias
                .clone()
                .or_else(|| Some(default_alias(&target.view_name)));
            *relation = TableFactor::Derived {
                lateral: false,
                subquery: Box::new(derived_query),
                alias: derived_alias,
            };
            Ok(RewriteDecision::Changed)
        }
        _ => Ok(RewriteDecision::Unchanged),
    }
}

fn build_entity_view_query(target: &EntityViewTarget) -> Result<Query, LixError> {
    let (source_table, extra_predicates) = match target.variant {
        EntityViewVariant::Base => {
            base_effective_state_source(target.version_id_override.as_deref())
        }
        EntityViewVariant::ByVersion => ("lix_state_by_version".to_string(), Vec::new()),
        EntityViewVariant::History => ("lix_state_history".to_string(), Vec::new()),
    };

    let mut projection = property_projection_items(&target.properties);
    projection.extend(
        lixcol_aliases_for_variant(target.variant)
            .iter()
            .map(|(column, alias)| SelectItem::ExprWithAlias {
                expr: column_expr(column),
                alias: Ident::new(*alias),
            }),
    );

    let mut predicates = vec![column_eq_text("schema_key", &target.schema_key)];
    predicates.extend(extra_predicates);
    predicates.extend(override_predicates(target));

    Ok(select_query_from_table(
        projection,
        &source_table,
        conjunction(predicates),
    ))
}

fn property_projection_items(properties: &[String]) -> Vec<SelectItem> {
    properties
        .iter()
        .map(|property| SelectItem::ExprWithAlias {
            expr: lix_json_text_expr("snapshot_content", property),
            alias: Ident::with_quote('"', property),
        })
        .collect()
}

fn column_expr(name: &str) -> Expr {
    Expr::Identifier(Ident::new(name))
}

fn conjunction(mut predicates: Vec<Expr>) -> Expr {
    if predicates.is_empty() {
        return Expr::BinaryOp {
            left: Box::new(Expr::Value(AstValue::Number("1".to_string(), false).into())),
            op: BinaryOperator::Eq,
            right: Box::new(Expr::Value(AstValue::Number("1".to_string(), false).into())),
        };
    }
    let mut current = predicates.remove(0);
    for predicate in predicates {
        current = Expr::BinaryOp {
            left: Box::new(current),
            op: BinaryOperator::And,
            right: Box::new(predicate),
        };
    }
    current
}

fn base_effective_state_source(version_id_override: Option<&str>) -> (String, Vec<Expr>) {
    match version_id_override {
        // Base views represent effective state. With an explicit version override, the
        // effective-state source is still `lix_state_by_version`, scoped to that version.
        Some(version_id) => (
            "lix_state_by_version".to_string(),
            vec![column_eq_text("version_id", version_id)],
        ),
        None => ("lix_state".to_string(), Vec::new()),
    }
}

fn override_predicates(target: &EntityViewTarget) -> Vec<Expr> {
    target
        .override_predicates
        .iter()
        .map(|predicate| match &predicate.value {
            JsonValue::Null => Expr::IsNull(Box::new(column_expr(&predicate.column))),
            value => Expr::BinaryOp {
                left: Box::new(column_expr(&predicate.column)),
                op: BinaryOperator::Eq,
                right: Box::new(render_literal(value)),
            },
        })
        .collect()
}

fn render_literal(value: &JsonValue) -> Expr {
    match value {
        JsonValue::Null => Expr::Value(AstValue::Null.into()),
        JsonValue::Bool(value) => Expr::Value(AstValue::Boolean(*value).into()),
        JsonValue::Number(value) => Expr::Value(AstValue::Number(value.to_string(), false).into()),
        JsonValue::String(value) => Expr::Value(AstValue::SingleQuotedString(value.clone()).into()),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            Expr::Value(AstValue::SingleQuotedString(value.to_string()).into())
        }
    }
}

fn lixcol_aliases_for_variant(
    variant: EntityViewVariant,
) -> &'static [(&'static str, &'static str)] {
    match variant {
        EntityViewVariant::Base => &[
            ("entity_id", "lixcol_entity_id"),
            ("schema_key", "lixcol_schema_key"),
            ("file_id", "lixcol_file_id"),
            ("plugin_key", "lixcol_plugin_key"),
            ("schema_version", "lixcol_schema_version"),
            ("created_at", "lixcol_created_at"),
            ("updated_at", "lixcol_updated_at"),
            (
                "inherited_from_version_id",
                "lixcol_inherited_from_version_id",
            ),
            ("change_id", "lixcol_change_id"),
            ("untracked", "lixcol_untracked"),
            ("metadata", "lixcol_metadata"),
        ],
        EntityViewVariant::ByVersion => &[
            ("entity_id", "lixcol_entity_id"),
            ("schema_key", "lixcol_schema_key"),
            ("file_id", "lixcol_file_id"),
            ("version_id", "lixcol_version_id"),
            ("plugin_key", "lixcol_plugin_key"),
            ("schema_version", "lixcol_schema_version"),
            ("created_at", "lixcol_created_at"),
            ("updated_at", "lixcol_updated_at"),
            (
                "inherited_from_version_id",
                "lixcol_inherited_from_version_id",
            ),
            ("change_id", "lixcol_change_id"),
            ("untracked", "lixcol_untracked"),
            ("metadata", "lixcol_metadata"),
        ],
        EntityViewVariant::History => &[
            ("entity_id", "lixcol_entity_id"),
            ("schema_key", "lixcol_schema_key"),
            ("file_id", "lixcol_file_id"),
            ("version_id", "lixcol_version_id"),
            ("plugin_key", "lixcol_plugin_key"),
            ("schema_version", "lixcol_schema_version"),
            ("change_id", "lixcol_change_id"),
            ("metadata", "lixcol_metadata"),
            ("commit_id", "lixcol_commit_id"),
            ("root_commit_id", "lixcol_root_commit_id"),
            ("depth", "lixcol_depth"),
        ],
    }
}

fn collect_table_view_names(query: &Query) -> Vec<String> {
    let mut view_names = Vec::new();
    let _ = visit_query_selects(query, &mut |select| {
        visit_table_factors_in_select(select, &mut |relation| {
            let TableFactor::Table { name, .. } = relation else {
                return Ok(());
            };
            if let Some(view_name) = object_name_terminal(name) {
                view_names.push(view_name);
            }
            Ok(())
        })
    });
    view_names
}

fn object_name_terminal(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.clone())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{build_entity_view_query, EntityViewVariant};
    use crate::sql::entity_views::target::{EntityViewOverridePredicate, EntityViewTarget};

    fn sample_target(variant: EntityViewVariant) -> EntityViewTarget {
        EntityViewTarget {
            view_name: "lix_demo".to_string(),
            schema_key: "demo".to_string(),
            variant,
            schema: json!({"type":"object"}),
            properties: vec!["name".to_string()],
            primary_key_properties: Vec::new(),
            schema_version: "1".to_string(),
            file_id_override: None,
            plugin_key_override: None,
            version_id_override: None,
            override_predicates: Vec::new(),
        }
    }

    #[test]
    fn base_view_with_version_override_uses_state_by_version_source() {
        let mut target = sample_target(EntityViewVariant::Base);
        target.version_id_override = Some("v42".to_string());

        let query = build_entity_view_query(&target).expect("build entity view query");
        let sql = query.to_string();

        assert!(sql.contains("FROM lix_state_by_version"));
        assert!(sql.contains("version_id = 'v42'"));
    }

    #[test]
    fn projection_and_override_predicates_are_rendered_from_ast() {
        let mut target = sample_target(EntityViewVariant::Base);
        target.properties = vec!["my-prop".to_string()];
        target.override_predicates = vec![
            EntityViewOverridePredicate {
                column: "plugin_key".to_string(),
                value: serde_json::Value::Null,
            },
            EntityViewOverridePredicate {
                column: "untracked".to_string(),
                value: serde_json::Value::Bool(true),
            },
        ];

        let query = build_entity_view_query(&target).expect("build entity view query");
        let sql = query.to_string();

        assert!(sql.contains("lix_json_text(snapshot_content, 'my-prop') AS \"my-prop\""));
        assert!(sql.contains("plugin_key IS NULL"));
        assert!(sql.contains("untracked = true"));
    }
}
