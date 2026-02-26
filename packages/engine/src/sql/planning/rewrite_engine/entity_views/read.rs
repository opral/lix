use serde_json::Value as JsonValue;
use sqlparser::ast::{ObjectName, ObjectNamePart, Query, Select, TableFactor};

use crate::engine::sql::planning::rewrite_engine::{
    default_alias, escape_sql_string, parse_single_query, quote_ident, rewrite_query_selects,
    rewrite_table_factors_in_select_decision, visit_query_selects, visit_table_factors_in_select,
    RewriteDecision,
};
use crate::{LixBackend, LixError};

use super::target::{
    resolve_target_from_object_name, resolve_targets_with_backend, EntityViewTarget,
    EntityViewVariant,
};

pub(crate) fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    rewrite_query_with_resolver(query, &mut |name| resolve_target_from_object_name(name))
}

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
    let (source_sql, extra_predicates) = match target.variant {
        EntityViewVariant::Base => {
            base_effective_state_source(target.version_id_override.as_deref())
        }
        EntityViewVariant::ByVersion => {
            ("lix_state_by_version".to_string(), vec!["1=1".to_string()])
        }
        EntityViewVariant::History => ("lix_state_history".to_string(), vec!["1=1".to_string()]),
    };
    let mut select_parts = Vec::new();
    for property in &target.properties {
        select_parts.push(format!(
            "lix_json_extract(snapshot_content, '{property}') AS {alias}",
            property = escape_sql_string(property),
            alias = quote_ident(property),
        ));
    }
    for (column, alias) in lixcol_aliases_for_variant(target.variant) {
        select_parts.push(format!("{column} AS {alias}"));
    }
    let mut predicates = vec![format!(
        "schema_key = '{schema_key}'",
        schema_key = escape_sql_string(&target.schema_key)
    )];
    predicates.extend(extra_predicates);
    predicates.extend(override_predicates(target));

    let sql = format!(
        "SELECT {projection} \
         FROM {source} \
         WHERE {predicate}",
        projection = select_parts.join(", "),
        source = source_sql,
        predicate = predicates.join(" AND "),
    );
    parse_single_query(&sql)
}

fn base_effective_state_source(version_id_override: Option<&str>) -> (String, Vec<String>) {
    match version_id_override {
        // Base views represent effective state. With an explicit version override, the
        // effective-state source is still `lix_state_by_version`, scoped to that version.
        Some(version_id) => (
            "lix_state_by_version".to_string(),
            vec![format!("version_id = '{}'", escape_sql_string(version_id))],
        ),
        None => ("lix_state".to_string(), vec!["1=1".to_string()]),
    }
}

fn override_predicates(target: &EntityViewTarget) -> Vec<String> {
    target
        .override_predicates
        .iter()
        .map(|predicate| match &predicate.value {
            JsonValue::Null => format!("{column} IS NULL", column = predicate.column),
            value => format!(
                "{column} = {literal}",
                column = predicate.column,
                literal = render_literal(value)
            ),
        })
        .collect()
}

fn render_literal(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "NULL".to_string(),
        JsonValue::Bool(value) => {
            if *value {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        JsonValue::Number(value) => value.to_string(),
        JsonValue::String(value) => format!("'{}'", escape_sql_string(value)),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            format!("'{}'", escape_sql_string(&value.to_string()))
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
