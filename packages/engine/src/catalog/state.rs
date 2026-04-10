use std::collections::BTreeMap;

use crate::catalog::{
    CatalogSource, DefaultScopeSemantics, SurfaceCapability, SurfaceColumnType, SurfaceDescriptor,
    SurfaceFamily, SurfaceImplicitOverrides, SurfaceReadFreshness, SurfaceReadSemantics,
    SurfaceResolutionCapabilities, SurfaceTraits, SurfaceVariant,
};

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

    let visible_columns = match variant {
        SurfaceVariant::History => state_history_columns(),
        SurfaceVariant::ByVersion => state_by_version_columns(),
        _ => state_columns(),
    };

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
        "writer_key".to_string(),
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
        ("writer_key".to_string(), SurfaceColumnType::String),
        ("version_id".to_string(), SurfaceColumnType::String),
        ("commit_created_at".to_string(), SurfaceColumnType::String),
        ("root_commit_id".to_string(), SurfaceColumnType::String),
        ("depth".to_string(), SurfaceColumnType::Integer),
    ])
}

#[cfg(test)]
mod tests {
    use super::state_surface_descriptor;
    use crate::catalog::{SurfaceCapability, SurfaceFamily, SurfaceVariant};

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
}
