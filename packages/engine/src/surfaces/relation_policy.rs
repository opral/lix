//! Centralized relation protection policy.
//!
//! Policy choice:
//! - Lix uses a semantic internal-object model.
//! - `lix_internal_*` names are implementation storage details behind this
//!   classifier, not a public reserved SQL namespace contract.
//! - built-in public surfaces such as `lix_state` and `lix_version` are the
//!   protected user-facing system relations.

use sqlparser::ast::{ObjectName, ObjectNamePart};

use crate::contracts::surface::{CatalogSource, SurfaceRegistry};

use super::build_builtin_surface_registry;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RelationPolicyModel {
    SemanticInternalObjects,
}

pub(crate) const RELATION_POLICY_MODEL: RelationPolicyModel =
    RelationPolicyModel::SemanticInternalObjects;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RelationPolicy {
    InternalStorage,
    ProtectedBuiltinPublicSurface,
    UserDefinedPublicSurface,
    UserDefinedPrivateRelation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InternalRelationFamily {
    pub(crate) owner: &'static str,
    pub(crate) prefix: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BuiltinRelationInventory {
    pub(crate) exact_internal_relations: Vec<&'static str>,
    pub(crate) internal_relation_families: Vec<InternalRelationFamily>,
    pub(crate) protected_builtin_public_surfaces: Vec<String>,
}

pub(crate) fn classify_builtin_object_name(name: &ObjectName) -> Option<RelationPolicy> {
    object_name_to_relation_name(name).map(|relation| classify_builtin_relation_name(&relation))
}

pub(crate) fn classify_relation_name(
    relation_name: &str,
    surface_registry: Option<&SurfaceRegistry>,
) -> RelationPolicy {
    let normalized = normalize_relation_name(relation_name);
    if is_internal_storage_relation_name(&normalized) {
        return RelationPolicy::InternalStorage;
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

pub(crate) fn object_name_is_internal_storage_relation(name: &ObjectName) -> bool {
    matches!(
        classify_builtin_object_name(name),
        Some(RelationPolicy::InternalStorage)
    )
}

pub(crate) fn object_name_is_protected_builtin_ddl_target(name: &ObjectName) -> bool {
    matches!(
        classify_builtin_object_name(name),
        Some(RelationPolicy::InternalStorage | RelationPolicy::ProtectedBuiltinPublicSurface)
    )
}

pub(crate) fn builtin_relation_inventory() -> BuiltinRelationInventory {
    BuiltinRelationInventory {
        exact_internal_relations: builtin_internal_exact_relation_names().to_vec(),
        internal_relation_families: builtin_internal_relation_families().to_vec(),
        protected_builtin_public_surfaces: protected_builtin_public_surface_names(),
    }
}

pub(crate) fn relation_policy_choice_summary() -> &'static str {
    match RELATION_POLICY_MODEL {
        RelationPolicyModel::SemanticInternalObjects => {
            "Lix treats `lix_internal_*` names as implementation storage behind semantic classification, not as a public reserved SQL namespace."
        }
    }
}

pub(crate) fn builtin_internal_exact_relation_names() -> &'static [&'static str] {
    &[
        crate::binary_cas::schema::INTERNAL_BINARY_BLOB_MANIFEST,
        crate::binary_cas::schema::INTERNAL_BINARY_BLOB_MANIFEST_CHUNK,
        crate::binary_cas::schema::INTERNAL_BINARY_BLOB_STORE,
        crate::binary_cas::schema::INTERNAL_BINARY_CHUNK_STORE,
        crate::binary_cas::schema::INTERNAL_BINARY_FILE_VERSION_REF,
        crate::canonical::journal::CHANGE_TABLE,
        crate::canonical::graph::COMMIT_GRAPH_NODE_TABLE,
        crate::session::version_ops::commit::COMMIT_IDEMPOTENCY_TABLE,
        crate::canonical::ENTITY_STATE_TIMELINE_BREAKPOINT_TABLE,
        crate::live_state::FILE_DATA_CACHE_TABLE,
        crate::live_state::FILE_LIXCOL_CACHE_TABLE,
        crate::live_state::FILE_PATH_CACHE_TABLE,
        crate::version_state::checkpoints::cache::LAST_CHECKPOINT_TABLE,
        crate::live_state::LIVE_STATE_STATUS_TABLE,
        crate::session::observe::OBSERVE_TICK_TABLE,
        crate::live_state::REGISTERED_SCHEMA_BOOTSTRAP_TABLE,
        crate::canonical::journal::SNAPSHOT_TABLE,
        crate::canonical::TIMELINE_STATUS_TABLE,
        crate::session::version_ops::undo_redo::UNDO_REDO_OPERATION_TABLE,
        crate::session::workspace::WORKSPACE_METADATA_TABLE,
        crate::live_state::writer_key::WRITER_KEY_TABLE,
    ]
}

pub(crate) fn builtin_internal_relation_families() -> &'static [InternalRelationFamily] {
    &[InternalRelationFamily {
        owner: "live_state",
        prefix: crate::live_state::TRACKED_RELATION_PREFIX,
    }]
}

pub(crate) fn protected_builtin_public_surface_names() -> Vec<String> {
    builtin_surface_registry().public_surface_names()
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

fn is_internal_storage_relation_name(normalized_relation_name: &str) -> bool {
    builtin_internal_exact_relation_names()
        .iter()
        .any(|name| *name == normalized_relation_name)
        || crate::live_state::is_internal_relation_name(normalized_relation_name)
}

fn normalize_relation_name(relation_name: &str) -> String {
    relation_name.trim().to_ascii_lowercase()
}

fn object_name_to_relation_name(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| normalize_relation_name(&ident.value))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::contracts::surface::{
        entity_surface_descriptors, CatalogSource, DynamicEntitySurfaceSpec, SurfaceRegistry,
    };
    use crate::live_state::tracked_relation_name;

    use super::{
        builtin_internal_exact_relation_names, builtin_internal_relation_families,
        builtin_relation_inventory, classify_builtin_relation_name, classify_relation_name,
        protected_builtin_public_surface_names, relation_policy_choice_summary, RelationPolicy,
        RelationPolicyModel, RELATION_POLICY_MODEL,
    };

    #[test]
    fn classifies_exact_internal_relations() {
        assert_eq!(
            classify_builtin_relation_name("lix_internal_snapshot"),
            RelationPolicy::InternalStorage
        );
        assert_eq!(
            classify_builtin_relation_name("LIX_INTERNAL_REGISTERED_SCHEMA_BOOTSTRAP"),
            RelationPolicy::InternalStorage
        );
    }

    #[test]
    fn classifies_live_storage_family_through_live_state_owner() {
        assert_eq!(
            classify_builtin_relation_name(&tracked_relation_name("lix_key_value")),
            RelationPolicy::InternalStorage
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
                predicate_overrides: Vec::new(),
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
        assert!(exact_relations.contains(&crate::canonical::journal::SNAPSHOT_TABLE));
        assert!(exact_relations.contains(&crate::live_state::REGISTERED_SCHEMA_BOOTSTRAP_TABLE));

        let unique = exact_relations
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique.len(), exact_relations.len());
    }

    #[test]
    fn inventory_lists_internal_relation_families_explicitly() {
        let families = builtin_internal_relation_families();
        assert_eq!(families.len(), 1);
        assert_eq!(families[0].owner, "live_state");
        assert_eq!(
            families[0].prefix,
            crate::live_state::TRACKED_RELATION_PREFIX
        );
    }

    #[test]
    fn inventory_lists_protected_builtin_public_surfaces() {
        let surfaces = protected_builtin_public_surface_names();
        assert!(surfaces.iter().any(|name| name == "lix_state"));
        assert!(surfaces.iter().any(|name| name == "lix_version"));

        let inventory = builtin_relation_inventory();
        assert_eq!(inventory.protected_builtin_public_surfaces, surfaces);
    }

    #[test]
    fn documents_semantic_internal_object_policy_model() {
        assert_eq!(
            RELATION_POLICY_MODEL,
            RelationPolicyModel::SemanticInternalObjects
        );
        assert!(
            relation_policy_choice_summary().contains("not as a public reserved SQL namespace"),
            "relation policy owner should document the semantic internal-object model"
        );
    }
}
