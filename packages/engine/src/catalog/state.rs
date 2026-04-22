use std::collections::BTreeMap;

use serde_json::Value as JsonValue;

use crate::catalog::{
    CatalogSource, DefaultScopeSemantics, SurfaceCapability, SurfaceColumnType, SurfaceDescriptor,
    SurfaceFamily, SurfaceImplicitOverrides, SurfaceReadFreshness, SurfaceReadSemantics,
    SurfaceResolutionCapabilities, SurfaceTraits, SurfaceVariant,
};
use crate::schema::lix_state_surface_schema_definition;
use crate::LixError;

pub(crate) fn state_by_version_relation_name() -> &'static str {
    "lix_state_by_version"
}

pub(crate) fn state_surface_validation_schema(schema_key: &str) -> Option<JsonValue> {
    (schema_key == "lix_state").then(|| lix_state_surface_schema_definition().clone())
}

pub(crate) fn state_surface_effective_foreign_key_target_schema_key(
    referenced_schema_key: &str,
    referenced_properties: &[String],
    local_values: &[JsonValue],
    source_schema_key: &str,
    index: usize,
) -> Result<Option<String>, LixError> {
    if referenced_schema_key != "lix_state" {
        return Ok(None);
    }

    let Some(schema_key_position) = referenced_properties
        .iter()
        .position(|pointer| pointer == "/schema_key")
    else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "foreign key at index {index} in schema '{}' references lix_state and must include '/schema_key' in references.properties",
                source_schema_key
            ),
        ));
    };

    match &local_values[schema_key_position] {
        JsonValue::String(schema_key) => Ok(Some(schema_key.clone())),
        other => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("foreign key target schema_key must be a string, got {other}"),
        )),
    }
}

pub(crate) fn state_surface_descriptor(name: &str, variant: SurfaceVariant) -> SurfaceDescriptor {
    let (default_scope, capability, exposes_version_column, exposes_history_columns) = match variant
    {
        SurfaceVariant::Default => (
            DefaultScopeSemantics::ActiveVersion,
            SurfaceCapability::ReadWrite,
            false,
            false,
        ),
        SurfaceVariant::ByVersion => (
            DefaultScopeSemantics::ExplicitVersion,
            SurfaceCapability::ReadWrite,
            true,
            false,
        ),
        SurfaceVariant::History => (
            DefaultScopeSemantics::History,
            SurfaceCapability::ReadOnly,
            true,
            true,
        ),
        SurfaceVariant::WorkingChanges => (
            DefaultScopeSemantics::ActiveVersion,
            SurfaceCapability::ReadOnly,
            false,
            false,
        ),
    };

    let visible_columns = state_relation_columns_for_variant(variant);

    let hidden_columns = match variant {
        SurfaceVariant::Default => vec!["version_id".to_string()],
        _ => Vec::new(),
    };

    SurfaceDescriptor {
        public_name: name.to_string(),
        surface_family: SurfaceFamily::State,
        surface_variant: variant,
        read_freshness: match variant {
            SurfaceVariant::History => SurfaceReadFreshness::AllowsStaleProjection,
            SurfaceVariant::Default
            | SurfaceVariant::ByVersion
            | SurfaceVariant::WorkingChanges => SurfaceReadFreshness::RequiresFreshProjection,
        },
        read_semantics: match variant {
            SurfaceVariant::History => SurfaceReadSemantics::CanonicalHistory,
            SurfaceVariant::Default
            | SurfaceVariant::ByVersion
            | SurfaceVariant::WorkingChanges => SurfaceReadSemantics::WorkspaceEffective,
        },
        visible_columns,
        hidden_columns,
        column_types: state_column_types(),
        capability,
        default_scope,
        surface_traits: SurfaceTraits {
            state_backed: true,
            exposes_version_column,
            exposes_history_columns,
            ..SurfaceTraits::default()
        },
        resolution_capabilities: SurfaceResolutionCapabilities {
            canonical_state_scan: true,
            semantic_write: capability == SurfaceCapability::ReadWrite,
            ..SurfaceResolutionCapabilities::default()
        },
        implicit_overrides: SurfaceImplicitOverrides {
            expose_version_id: exposes_version_column,
            ..SurfaceImplicitOverrides::default()
        },
        catalog_source: CatalogSource::Builtin,
    }
}

pub(crate) fn state_relation_columns_for_variant(variant: SurfaceVariant) -> Vec<String> {
    match variant {
        SurfaceVariant::History => state_history_columns(),
        SurfaceVariant::ByVersion => state_by_version_columns(),
        SurfaceVariant::Default | SurfaceVariant::WorkingChanges => state_columns(),
    }
}

pub(crate) fn state_relation_column_types_for_variant(
    variant: SurfaceVariant,
) -> BTreeMap<String, SurfaceColumnType> {
    let all_types = state_column_types();
    state_relation_columns_for_variant(variant)
        .into_iter()
        .filter_map(|column_name| {
            all_types
                .get(&column_name)
                .copied()
                .map(|column_type| (column_name, column_type))
        })
        .collect()
}

pub(crate) fn state_relation_column_is_nullable_for_variant(
    variant: SurfaceVariant,
    column_name: &str,
) -> Option<bool> {
    match entity_base_relation_variant(variant) {
        SurfaceVariant::Default => match column_name {
            "entity_id" | "schema_key" | "global" | "untracked" => Some(false),
            "file_id" | "plugin_key" | "snapshot_content" | "metadata" | "schema_version"
            | "created_at" | "updated_at" | "change_id" | "commit_id" => Some(true),
            _ => None,
        },
        SurfaceVariant::ByVersion => match column_name {
            "entity_id" | "schema_key" | "global" | "untracked" | "version_id" => Some(false),
            "file_id" | "plugin_key" | "snapshot_content" | "metadata" | "schema_version"
            | "created_at" | "updated_at" | "change_id" | "commit_id" => Some(true),
            _ => None,
        },
        SurfaceVariant::History => match column_name {
            "entity_id" | "schema_key" | "schema_version" | "change_id" | "commit_id"
            | "commit_created_at" | "root_commit_id" | "depth" | "version_id" => Some(false),
            "file_id" | "plugin_key" | "snapshot_content" | "metadata" => Some(true),
            _ => None,
        },
        SurfaceVariant::WorkingChanges => None,
    }
}

fn entity_base_relation_variant(variant: SurfaceVariant) -> SurfaceVariant {
    match variant {
        SurfaceVariant::Default | SurfaceVariant::WorkingChanges => SurfaceVariant::Default,
        SurfaceVariant::ByVersion => SurfaceVariant::ByVersion,
        SurfaceVariant::History => SurfaceVariant::History,
    }
}

fn state_columns() -> Vec<String> {
    vec![
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
    ]
}

fn state_by_version_columns() -> Vec<String> {
    let mut columns = state_columns();
    columns.push("version_id".to_string());
    columns
}

fn state_history_columns() -> Vec<String> {
    vec![
        "entity_id".to_string(),
        "schema_key".to_string(),
        "file_id".to_string(),
        "plugin_key".to_string(),
        "snapshot_content".to_string(),
        "metadata".to_string(),
        "schema_version".to_string(),
        "change_id".to_string(),
        "commit_id".to_string(),
        "commit_created_at".to_string(),
        "root_commit_id".to_string(),
        "depth".to_string(),
        "version_id".to_string(),
    ]
}

fn state_column_types() -> BTreeMap<String, SurfaceColumnType> {
    BTreeMap::from([
        ("entity_id".to_string(), SurfaceColumnType::String),
        ("schema_key".to_string(), SurfaceColumnType::String),
        ("file_id".to_string(), SurfaceColumnType::String),
        ("plugin_key".to_string(), SurfaceColumnType::String),
        ("snapshot_content".to_string(), SurfaceColumnType::Json),
        ("metadata".to_string(), SurfaceColumnType::Json),
        ("schema_version".to_string(), SurfaceColumnType::String),
        ("created_at".to_string(), SurfaceColumnType::String),
        ("updated_at".to_string(), SurfaceColumnType::String),
        ("global".to_string(), SurfaceColumnType::Boolean),
        ("change_id".to_string(), SurfaceColumnType::String),
        ("commit_id".to_string(), SurfaceColumnType::String),
        ("untracked".to_string(), SurfaceColumnType::Boolean),
        ("version_id".to_string(), SurfaceColumnType::String),
        ("commit_created_at".to_string(), SurfaceColumnType::String),
        ("root_commit_id".to_string(), SurfaceColumnType::String),
        ("depth".to_string(), SurfaceColumnType::Integer),
    ])
}

#[cfg(test)]
mod tests {
    use super::{
        state_surface_descriptor, state_surface_effective_foreign_key_target_schema_key,
        state_surface_validation_schema,
    };
    use crate::catalog::{SurfaceCapability, SurfaceFamily, SurfaceVariant};
    use serde_json::json;

    #[test]
    fn default_state_surface_hides_version_id_but_remains_state_backed() {
        let descriptor = state_surface_descriptor("lix_state", SurfaceVariant::Default);

        assert_eq!(descriptor.surface_family, SurfaceFamily::State);
        assert_eq!(descriptor.capability, SurfaceCapability::ReadWrite);
        assert!(descriptor.surface_traits.state_backed);
        assert!(descriptor
            .hidden_columns
            .iter()
            .any(|column| column == "version_id"));
    }

    #[test]
    fn history_state_surface_exposes_history_columns() {
        let descriptor = state_surface_descriptor("lix_state_history", SurfaceVariant::History);

        assert_eq!(descriptor.surface_variant, SurfaceVariant::History);
        assert!(descriptor.surface_traits.exposes_history_columns);
        assert!(descriptor
            .visible_columns
            .iter()
            .any(|column| column == "commit_id"));
        assert!(descriptor
            .visible_columns
            .iter()
            .any(|column| column == "depth"));
    }

    #[test]
    fn validation_schema_is_only_exposed_for_default_state_surface() {
        assert!(state_surface_validation_schema("lix_state").is_some());
        assert!(state_surface_validation_schema("lix_state_by_version").is_none());
    }

    #[test]
    fn default_state_foreign_keys_resolve_target_schema_key_from_local_values() {
        let schema_key = state_surface_effective_foreign_key_target_schema_key(
            "lix_state",
            &["/schema_key".to_string()],
            &[json!("app_user")],
            "app_membership",
            0,
        )
        .expect("foreign-key schema-key lookup should succeed");

        assert_eq!(schema_key, Some("app_user".to_string()));
    }
}
