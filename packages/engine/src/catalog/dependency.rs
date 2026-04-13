use std::collections::BTreeSet;

use crate::catalog::{
    bind_surface_relation, CatalogHistoryReadSemantics, CatalogProjectionInputVersionScope,
    CatalogProjectionRegistry, FilesystemRelationBinding, FilesystemRelationKind,
    RelationBindContext, RelationBinding, ResolvedSurface, SchemaRelationBinding, SurfaceFamily,
    SurfaceRegistry, SurfaceVariant, VersionRelationBinding,
};
use crate::contracts::SessionDependency;
use crate::LixError;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct CatalogSurfaceDependencyMetadata {
    pub(crate) relation_names: BTreeSet<String>,
    pub(crate) compiled_schema_keys: BTreeSet<String>,
    pub(crate) session_dependencies: BTreeSet<SessionDependency>,
    pub(crate) uses_dynamic_state_relations: bool,
    pub(crate) depends_on_active_version: bool,
    pub(crate) depends_on_public_surface_registry: bool,
}

impl CatalogSurfaceDependencyMetadata {
    fn note_active_version(&mut self) {
        self.depends_on_active_version = true;
        self.session_dependencies
            .insert(SessionDependency::ActiveVersion);
    }

    fn note_public_surface_registry(&mut self) {
        self.depends_on_public_surface_registry = true;
        self.session_dependencies
            .insert(SessionDependency::PublicSurfaceRegistryGeneration);
    }

    fn merge_schema_key(&mut self, schema_key: impl Into<String>) {
        self.compiled_schema_keys.insert(schema_key.into());
    }

    fn merge_relation_name(&mut self, relation_name: impl Into<String>) {
        self.relation_names.insert(relation_name.into());
    }

    fn merge_projection_inputs(
        &mut self,
        public_name: &str,
        declarations: &CatalogProjectionRegistry,
    ) -> bool {
        let Some(registration) = declarations.registration_for_surface(public_name) else {
            return false;
        };

        for input in registration.projection().inputs() {
            self.merge_schema_key(input.schema_key);
            if input.version_scope == CatalogProjectionInputVersionScope::RequestedVersion {
                self.note_active_version();
            }
        }
        true
    }

    fn merge_schema_relation(&mut self, binding: &SchemaRelationBinding) {
        self.merge_schema_key(binding.schema_key.clone());
        if matches!(
            binding.default_scope,
            crate::catalog::DefaultScopeSemantics::ActiveVersion
        ) {
            self.note_active_version();
        }
    }

    fn merge_version_relation(&mut self, binding: &VersionRelationBinding) {
        self.merge_schema_key(binding.descriptor_source.schema_key.clone());
        match &binding.head_source {
            crate::catalog::VersionHeadSourceBinding::StoredRefs(stored) => {
                self.merge_schema_key(stored.schema_key.clone());
            }
            crate::catalog::VersionHeadSourceBinding::InlineCurrentHeads(_) => {}
        }
    }

    fn merge_filesystem_relation(&mut self, binding: &FilesystemRelationBinding) {
        match binding.kind {
            FilesystemRelationKind::File => {
                self.merge_schema_key(binding.file_descriptor_schema_key.clone());
                self.merge_schema_key(binding.directory_descriptor_schema_key.clone());
                self.merge_schema_key(binding.binary_blob_ref_schema_key.clone());
            }
            FilesystemRelationKind::Directory => {
                self.merge_schema_key(binding.directory_descriptor_schema_key.clone());
            }
        }
        if matches!(
            binding.scope,
            crate::catalog::FilesystemProjectionScope::ActiveVersion
        ) {
            self.note_active_version();
        }
    }
}

pub(crate) fn dependency_metadata_for_surface_name(
    surfaces: &SurfaceRegistry,
    declarations: &CatalogProjectionRegistry,
    relation_name: &str,
) -> Result<Option<CatalogSurfaceDependencyMetadata>, LixError> {
    let Some(binding) = surfaces.bind_relation_name(relation_name) else {
        return Ok(None);
    };
    dependency_metadata_for_surface_binding(declarations, &binding)
}

pub(crate) fn dependency_metadata_for_surface_binding(
    declarations: &CatalogProjectionRegistry,
    surface_binding: &ResolvedSurface,
) -> Result<Option<CatalogSurfaceDependencyMetadata>, LixError> {
    let mut metadata = CatalogSurfaceDependencyMetadata::default();
    metadata.note_public_surface_registry();
    metadata.merge_relation_name(surface_binding.descriptor.public_name.clone());

    if metadata.merge_projection_inputs(&surface_binding.descriptor.public_name, declarations) {
        return Ok(Some(metadata));
    }

    let bound_relation = bind_surface_relation(surface_binding, RelationBindContext::default())?;
    match bound_relation {
        Some(RelationBinding::SchemaRelation(binding)) => {
            metadata.merge_schema_relation(&binding);
            if matches!(
                (
                    surface_binding.descriptor.surface_family,
                    surface_binding.descriptor.surface_variant,
                ),
                (
                    SurfaceFamily::State | SurfaceFamily::Entity,
                    SurfaceVariant::History
                )
            ) {
                metadata.note_active_version();
            }
            Ok(Some(metadata))
        }
        Some(RelationBinding::VersionRelation(binding)) => {
            metadata.merge_version_relation(&binding);
            Ok(Some(metadata))
        }
        Some(RelationBinding::FilesystemRelation(binding)) => {
            metadata.merge_filesystem_relation(&binding);
            Ok(Some(metadata))
        }
        None => match (
            surface_binding.descriptor.surface_family,
            surface_binding.descriptor.surface_variant,
        ) {
            (SurfaceFamily::Filesystem, SurfaceVariant::History) => {
                match crate::catalog::history_read_semantics(surface_binding) {
                    Some(CatalogHistoryReadSemantics::FileHistory {
                        active_version_lineage,
                    }) => {
                        metadata.merge_schema_key("lix_file_descriptor");
                        metadata.merge_schema_key("lix_directory_descriptor");
                        metadata.merge_schema_key("lix_binary_blob_ref");
                        if active_version_lineage {
                            metadata.note_active_version();
                        }
                        Ok(Some(metadata))
                    }
                    Some(CatalogHistoryReadSemantics::DirectoryHistoryActiveVersion) => {
                        metadata.merge_schema_key("lix_directory_descriptor");
                        metadata.note_active_version();
                        Ok(Some(metadata))
                    }
                    _ => Ok(Some(metadata)),
                }
            }
            (SurfaceFamily::State, SurfaceVariant::Default)
            | (SurfaceFamily::Change, SurfaceVariant::WorkingChanges) => {
                metadata.uses_dynamic_state_relations = true;
                metadata.note_active_version();
                Ok(Some(metadata))
            }
            (SurfaceFamily::State, _) => {
                metadata.uses_dynamic_state_relations = true;
                Ok(Some(metadata))
            }
            (SurfaceFamily::Change, _)
                if surface_binding
                    .descriptor
                    .resolution_capabilities
                    .canonical_change_scan =>
            {
                metadata.merge_schema_key("lix_change");
                Ok(Some(metadata))
            }
            _ => Ok(Some(metadata)),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{dependency_metadata_for_surface_name, CatalogSurfaceDependencyMetadata};
    use crate::catalog::{build_builtin_surface_registry, builtin_catalog_projection_registry};
    use crate::contracts::SessionDependency;

    #[test]
    fn lix_version_dependency_metadata_comes_from_catalog_declaration() {
        let metadata = dependency_metadata_for_surface_name(
            &build_builtin_surface_registry(),
            builtin_catalog_projection_registry(),
            "lix_version",
        )
        .expect("catalog dependency metadata lookup should succeed")
        .expect("lix_version should resolve");

        assert_eq!(
            metadata.relation_names,
            BTreeSet::from(["lix_version".to_string()])
        );
        assert_eq!(
            metadata.compiled_schema_keys,
            BTreeSet::from([
                "lix_version_descriptor".to_string(),
                "lix_version_ref".to_string(),
            ])
        );
        assert!(!metadata.depends_on_active_version);
        assert!(metadata
            .session_dependencies
            .contains(&SessionDependency::PublicSurfaceRegistryGeneration));
    }

    #[test]
    fn lix_file_dependency_metadata_tracks_catalog_declared_inputs_and_active_version() {
        let metadata = dependency_metadata_for_surface_name(
            &build_builtin_surface_registry(),
            builtin_catalog_projection_registry(),
            "lix_file",
        )
        .expect("catalog dependency metadata lookup should succeed")
        .expect("lix_file should resolve");

        assert_eq!(
            metadata.relation_names,
            BTreeSet::from(["lix_file".to_string()])
        );
        assert!(metadata.depends_on_active_version);
        assert_eq!(
            metadata.compiled_schema_keys,
            BTreeSet::from([
                "lix_binary_blob_ref".to_string(),
                "lix_directory_descriptor".to_string(),
                "lix_file_descriptor".to_string(),
            ])
        );
    }

    #[test]
    fn lix_state_dependency_metadata_tracks_dynamic_state_and_active_version() {
        let metadata = dependency_metadata_for_surface_name(
            &build_builtin_surface_registry(),
            builtin_catalog_projection_registry(),
            "lix_state",
        )
        .expect("catalog dependency metadata lookup should succeed")
        .expect("lix_state should resolve");

        assert_eq!(
            metadata.relation_names,
            BTreeSet::from(["lix_state".to_string()])
        );
        assert!(metadata.uses_dynamic_state_relations);
        assert!(metadata.depends_on_active_version);
        assert!(metadata.compiled_schema_keys.is_empty());
    }

    #[test]
    fn lix_working_changes_dependency_metadata_is_catalog_owned() {
        let metadata = dependency_metadata_for_surface_name(
            &build_builtin_surface_registry(),
            builtin_catalog_projection_registry(),
            "lix_working_changes",
        )
        .expect("catalog dependency metadata lookup should succeed")
        .expect("lix_working_changes should resolve");

        assert_eq!(
            metadata.relation_names,
            BTreeSet::from(["lix_working_changes".to_string()])
        );
        assert!(metadata.uses_dynamic_state_relations);
        assert!(metadata.depends_on_active_version);
        assert!(metadata
            .session_dependencies
            .contains(&SessionDependency::PublicSurfaceRegistryGeneration));
    }

    #[test]
    fn missing_surface_returns_none() {
        let metadata = dependency_metadata_for_surface_name(
            &build_builtin_surface_registry(),
            builtin_catalog_projection_registry(),
            "missing_surface",
        )
        .expect("catalog dependency metadata lookup should succeed");

        assert_eq!(metadata, None::<CatalogSurfaceDependencyMetadata>);
    }
}
