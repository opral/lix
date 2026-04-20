//! Centralized relation classification for SQL-facing names.

use crate::catalog::{
    build_builtin_surface_registry, builtin_public_surface_names, CatalogSource, SurfaceRegistry,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RelationPolicy {
    InternalRelation,
    ProtectedBuiltinPublicSurface,
    UserDefinedPublicSurface,
    UserDefinedPrivateRelation,
}

pub(crate) fn classify_relation_name(
    relation_name: &str,
    surface_registry: Option<&SurfaceRegistry>,
) -> RelationPolicy {
    let normalized = normalize_relation_name(relation_name);
    if is_internal_relation_name(&normalized) {
        return RelationPolicy::InternalRelation;
    }

    if let Some(registry) = surface_registry {
        if let Some(policy) = classify_registry_surface_name(&normalized, registry) {
            return policy;
        }
    }

    classify_registry_surface_name(&normalized, builtin_surface_registry())
        .unwrap_or(RelationPolicy::UserDefinedPrivateRelation)
}

pub(crate) fn classify_builtin_relation_name(relation_name: &str) -> RelationPolicy {
    classify_relation_name(relation_name, None)
}

pub(crate) fn builtin_internal_exact_relation_names() -> Vec<&'static str> {
    let mut relations = Vec::new();
    relations.extend_from_slice(crate::canonical::internal_exact_relation_names());
    relations.extend_from_slice(crate::live_state::internal_exact_relation_names());
    relations.extend_from_slice(crate::binary_cas::internal_exact_relation_names());
    relations
}

pub(crate) fn protected_builtin_public_surface_names() -> Vec<String> {
    builtin_public_surface_names()
}

fn builtin_surface_registry() -> &'static SurfaceRegistry {
    static BUILTIN_SURFACE_REGISTRY: std::sync::OnceLock<SurfaceRegistry> =
        std::sync::OnceLock::new();
    BUILTIN_SURFACE_REGISTRY.get_or_init(build_builtin_surface_registry)
}

fn classify_registry_surface_name(
    normalized_relation_name: &str,
    registry: &SurfaceRegistry,
) -> Option<RelationPolicy> {
    let binding = registry.bind_relation_name(normalized_relation_name)?;
    Some(match binding.descriptor.catalog_source {
        CatalogSource::Builtin => RelationPolicy::ProtectedBuiltinPublicSurface,
        CatalogSource::Dynamic => RelationPolicy::UserDefinedPublicSurface,
    })
}

fn is_internal_relation_name(normalized_relation_name: &str) -> bool {
    normalized_relation_name.starts_with(crate::live_state::INTERNAL_RELATION_PREFIX)
        || builtin_internal_exact_relation_names()
            .iter()
            .any(|name| *name == normalized_relation_name)
}

fn normalize_relation_name(relation_name: &str) -> String {
    relation_name.trim().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::catalog::{
        entity_surface_descriptors, CatalogSource, DynamicEntitySurfaceSpec, SurfaceRegistry,
    };
    use crate::live_state::tracked_relation_name;

    use super::{
        builtin_internal_exact_relation_names, classify_builtin_relation_name,
        classify_relation_name, protected_builtin_public_surface_names, RelationPolicy,
    };

    #[test]
    fn classifies_exact_internal_relations() {
        assert_eq!(
            classify_builtin_relation_name("lix_internal_snapshot"),
            RelationPolicy::InternalRelation
        );
        assert_eq!(
            classify_builtin_relation_name("LIX_INTERNAL_REGISTERED_SCHEMA_BOOTSTRAP"),
            RelationPolicy::InternalRelation
        );
    }

    #[test]
    fn classifies_live_storage_family_through_internal_relation_prefix() {
        assert_eq!(
            classify_builtin_relation_name(&tracked_relation_name("lix_key_value")),
            RelationPolicy::InternalRelation
        );
    }

    #[test]
    fn classifies_builtin_public_surfaces() {
        assert_eq!(
            classify_builtin_relation_name("lix_state"),
            RelationPolicy::ProtectedBuiltinPublicSurface
        );
        assert_eq!(
            classify_builtin_relation_name("LIX_STATE_BY_VERSION"),
            RelationPolicy::ProtectedBuiltinPublicSurface
        );
    }

    #[test]
    fn classifies_dynamic_public_surfaces_when_registry_is_available() {
        let mut registry = SurfaceRegistry::new();
        registry.insert_descriptors(entity_surface_descriptors(
            &DynamicEntitySurfaceSpec {
                schema_key: "project_message".to_string(),
                visible_columns: vec!["id".to_string(), "body".to_string()],
                column_types: BTreeMap::new(),
            },
            CatalogSource::Dynamic,
        ));

        assert_eq!(
            classify_relation_name("project_message", Some(&registry)),
            RelationPolicy::UserDefinedPublicSurface
        );
        assert_eq!(
            classify_relation_name("project_message_history", Some(&registry)),
            RelationPolicy::UserDefinedPublicSurface
        );
    }

    #[test]
    fn classifies_unknown_names_as_user_private_relations() {
        assert_eq!(
            classify_builtin_relation_name("user_table"),
            RelationPolicy::UserDefinedPrivateRelation
        );
    }

    #[test]
    fn inventory_lists_exact_internal_relations_once() {
        let exact_relations = builtin_internal_exact_relation_names();
        assert!(exact_relations.contains(&"lix_internal_snapshot"));
        assert!(exact_relations.contains(&crate::live_state::REGISTERED_SCHEMA_BOOTSTRAP_TABLE));

        let unique = exact_relations
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique.len(), exact_relations.len());
    }

    #[test]
    fn lists_protected_builtin_public_surfaces() {
        let surfaces = protected_builtin_public_surface_names();
        assert!(surfaces.iter().any(|name| name == "lix_state"));
        assert!(surfaces.iter().any(|name| name == "lix_version"));
    }
}
