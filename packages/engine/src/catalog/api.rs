use sqlparser::ast::ObjectName;

use crate::catalog::{
    admin_scan_kind, bind_filesystem_relation, bind_named_relation, bind_surface_relation,
    builtin_catalog_projection_registry, dependency_metadata_for_surface_binding,
    dependency_metadata_for_surface_name, explicit_version_counterpart_surface_name,
    filesystem_scan_semantics, history_read_semantics, is_working_changes_surface,
    read_preparation_semantics, state_surface_effective_foreign_key_target_schema_key,
    state_surface_validation_schema, transaction_insert_semantics, write_surface_semantics,
    CatalogAdminScanKind, CatalogFilesystemScanSemantics, CatalogHistoryReadSemantics,
    CatalogProjectionDefinition, CatalogProjectionRegistry, CatalogReadPreparationSemantics,
    CatalogSurfaceDependencyMetadata, CatalogTransactionInsertSemantics,
    CatalogWriteSurfaceSemantics, FilesystemProjectionScope, FilesystemRelationKind,
    RegisteredCatalogProjection, RelationBindContext, RelationBinding, ResolvedRelation,
    SurfaceDescriptor, SurfaceRegistry,
};
use crate::LixError;
use serde_json::Value as JsonValue;

/// Compiler-facing semantic catalog contract.
///
/// This is the root API `sql/*` should target instead of importing catalog
/// implementation files directly.
#[allow(dead_code)]
pub(crate) trait CatalogCompilerApi {
    fn resolve_surface(&self, relation_name: &str) -> Option<ResolvedRelation>;

    fn resolve_surface_descriptor(&self, relation_name: &str) -> Option<SurfaceDescriptor>;

    fn resolve_object_name(&self, name: &ObjectName) -> Option<ResolvedRelation>;

    fn visible_columns(&self, relation_name: &str) -> Option<Vec<String>>;

    fn public_surface_names(&self) -> Vec<String>;

    fn bind_relation(
        &self,
        relation_name: &str,
        context: RelationBindContext<'_>,
    ) -> Result<Option<RelationBinding>, LixError>;

    fn bind_surface_runtime_relation(
        &self,
        resolved_relation: &ResolvedRelation,
        context: RelationBindContext<'_>,
    ) -> Result<Option<RelationBinding>, LixError>;

    fn bind_filesystem_runtime_relation(
        &self,
        kind: FilesystemRelationKind,
        scope: FilesystemProjectionScope,
        active_version_id: Option<&str>,
    ) -> Result<RelationBinding, LixError>;

    fn derived_surface_registration(
        &self,
        public_name: &str,
    ) -> Option<&RegisteredCatalogProjection>;

    fn dependency_metadata(
        &self,
        public_name: &str,
    ) -> Result<Option<CatalogSurfaceDependencyMetadata>, LixError>;

    fn dependency_metadata_for_binding(
        &self,
        resolved_relation: &ResolvedRelation,
    ) -> Result<Option<CatalogSurfaceDependencyMetadata>, LixError>;

    fn history_read_semantics(
        &self,
        resolved_relation: &ResolvedRelation,
    ) -> Option<CatalogHistoryReadSemantics>;

    fn explicit_version_counterpart_surface_name(
        &self,
        resolved_relation: &ResolvedRelation,
        missing_columns: &[String],
    ) -> Option<String>;

    fn read_preparation_semantics(
        &self,
        resolved_relation: &ResolvedRelation,
    ) -> CatalogReadPreparationSemantics;

    fn validation_schema(&self, schema_key: &str) -> Option<JsonValue>;

    fn effective_foreign_key_target_schema_key(
        &self,
        referenced_schema_key: &str,
        referenced_properties: &[String],
        local_values: &[JsonValue],
        source_schema_key: &str,
        index: usize,
    ) -> Result<Option<String>, LixError>;

    fn filesystem_scan_semantics(
        &self,
        resolved_relation: &ResolvedRelation,
    ) -> Result<Option<CatalogFilesystemScanSemantics>, LixError>;

    fn is_working_changes_surface(&self, resolved_relation: &ResolvedRelation) -> bool;

    fn admin_scan_kind(&self, resolved_relation: &ResolvedRelation)
        -> Option<CatalogAdminScanKind>;

    fn transaction_insert_semantics(
        &self,
        resolved_relation: &ResolvedRelation,
    ) -> Option<CatalogTransactionInsertSemantics>;

    fn write_surface_semantics(
        &self,
        resolved_relation: &ResolvedRelation,
    ) -> Result<Option<CatalogWriteSurfaceSemantics>, LixError>;

    fn transaction_insert_semantics_for_object_name(
        &self,
        name: &ObjectName,
    ) -> Option<CatalogTransactionInsertSemantics> {
        self.resolve_object_name(name)
            .and_then(|binding| self.transaction_insert_semantics(&binding))
    }

    fn write_surface_semantics_for_object_name(
        &self,
        name: &ObjectName,
    ) -> Result<Option<CatalogWriteSurfaceSemantics>, LixError> {
        let Some(binding) = self.resolve_object_name(name) else {
            return Ok(None);
        };
        self.write_surface_semantics(&binding)
    }

    fn derived_surface_definition(
        &self,
        public_name: &str,
    ) -> Option<&dyn CatalogProjectionDefinition> {
        self.derived_surface_registration(public_name)
            .map(RegisteredCatalogProjection::projection)
    }
}

#[allow(dead_code)]
#[derive(Clone, Copy)]
pub(crate) struct CatalogCompilerFacade<'a> {
    surfaces: &'a SurfaceRegistry,
    declarations: &'a CatalogProjectionRegistry,
}

impl<'a> CatalogCompilerFacade<'a> {
    pub(crate) fn new(
        surfaces: &'a SurfaceRegistry,
        declarations: &'a CatalogProjectionRegistry,
    ) -> Self {
        Self {
            surfaces,
            declarations,
        }
    }
}

impl CatalogCompilerApi for CatalogCompilerFacade<'_> {
    fn resolve_surface(&self, relation_name: &str) -> Option<ResolvedRelation> {
        self.surfaces.bind_relation_name(relation_name)
    }

    fn resolve_surface_descriptor(&self, relation_name: &str) -> Option<SurfaceDescriptor> {
        self.resolve_surface(relation_name)
            .map(|binding| binding.descriptor)
    }

    fn resolve_object_name(&self, name: &ObjectName) -> Option<ResolvedRelation> {
        self.surfaces.bind_object_name(name)
    }

    fn visible_columns(&self, relation_name: &str) -> Option<Vec<String>> {
        self.surfaces.public_surface_columns(relation_name)
    }

    fn public_surface_names(&self) -> Vec<String> {
        self.surfaces.public_surface_names()
    }

    fn bind_relation(
        &self,
        relation_name: &str,
        context: RelationBindContext<'_>,
    ) -> Result<Option<RelationBinding>, LixError> {
        bind_named_relation(relation_name, context)
    }

    fn bind_surface_runtime_relation(
        &self,
        resolved_relation: &ResolvedRelation,
        context: RelationBindContext<'_>,
    ) -> Result<Option<RelationBinding>, LixError> {
        bind_surface_relation(resolved_relation, context)
    }

    fn bind_filesystem_runtime_relation(
        &self,
        kind: FilesystemRelationKind,
        scope: FilesystemProjectionScope,
        active_version_id: Option<&str>,
    ) -> Result<RelationBinding, LixError> {
        bind_filesystem_relation(kind, scope, active_version_id)
    }

    fn derived_surface_registration(
        &self,
        public_name: &str,
    ) -> Option<&RegisteredCatalogProjection> {
        self.declarations.registration_for_surface(public_name)
    }

    fn dependency_metadata(
        &self,
        public_name: &str,
    ) -> Result<Option<CatalogSurfaceDependencyMetadata>, LixError> {
        dependency_metadata_for_surface_name(self.surfaces, self.declarations, public_name)
    }

    fn dependency_metadata_for_binding(
        &self,
        resolved_relation: &ResolvedRelation,
    ) -> Result<Option<CatalogSurfaceDependencyMetadata>, LixError> {
        dependency_metadata_for_surface_binding(self.declarations, resolved_relation)
    }

    fn history_read_semantics(
        &self,
        resolved_relation: &ResolvedRelation,
    ) -> Option<CatalogHistoryReadSemantics> {
        history_read_semantics(resolved_relation)
    }

    fn explicit_version_counterpart_surface_name(
        &self,
        resolved_relation: &ResolvedRelation,
        missing_columns: &[String],
    ) -> Option<String> {
        explicit_version_counterpart_surface_name(resolved_relation, missing_columns)
    }

    fn read_preparation_semantics(
        &self,
        resolved_relation: &ResolvedRelation,
    ) -> CatalogReadPreparationSemantics {
        read_preparation_semantics(resolved_relation)
    }

    fn validation_schema(&self, schema_key: &str) -> Option<JsonValue> {
        state_surface_validation_schema(schema_key)
    }

    fn effective_foreign_key_target_schema_key(
        &self,
        referenced_schema_key: &str,
        referenced_properties: &[String],
        local_values: &[JsonValue],
        source_schema_key: &str,
        index: usize,
    ) -> Result<Option<String>, LixError> {
        state_surface_effective_foreign_key_target_schema_key(
            referenced_schema_key,
            referenced_properties,
            local_values,
            source_schema_key,
            index,
        )
    }

    fn filesystem_scan_semantics(
        &self,
        resolved_relation: &ResolvedRelation,
    ) -> Result<Option<CatalogFilesystemScanSemantics>, LixError> {
        filesystem_scan_semantics(resolved_relation)
    }

    fn is_working_changes_surface(&self, resolved_relation: &ResolvedRelation) -> bool {
        is_working_changes_surface(resolved_relation)
    }

    fn admin_scan_kind(
        &self,
        resolved_relation: &ResolvedRelation,
    ) -> Option<CatalogAdminScanKind> {
        admin_scan_kind(resolved_relation)
    }

    fn transaction_insert_semantics(
        &self,
        resolved_relation: &ResolvedRelation,
    ) -> Option<CatalogTransactionInsertSemantics> {
        transaction_insert_semantics(resolved_relation)
    }

    fn write_surface_semantics(
        &self,
        resolved_relation: &ResolvedRelation,
    ) -> Result<Option<CatalogWriteSurfaceSemantics>, LixError> {
        write_surface_semantics(resolved_relation)
    }
}

pub(crate) fn builtin_catalog_compiler_facade() -> CatalogCompilerFacade<'static> {
    CatalogCompilerFacade::new(
        super::builtin_surface_registry(),
        builtin_catalog_projection_registry(),
    )
}

pub(crate) fn catalog_compiler_facade_for_registry(
    surfaces: &SurfaceRegistry,
) -> CatalogCompilerFacade<'_> {
    CatalogCompilerFacade::new(surfaces, builtin_catalog_projection_registry())
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

    use super::{builtin_catalog_compiler_facade, CatalogCompilerApi};
    use crate::catalog::{
        CatalogAdminScanKind, CatalogAdminWriteBehavior, CatalogFilesystemScanSemantics,
        CatalogHistoryReadSemantics, CatalogScanVersionScope, CatalogTransactionInsertSemantics,
        CatalogWriteSurfaceSemantics, CatalogWriteTargetKind, CatalogWriteVersionSemantics,
        FilesystemProjectionScope, FilesystemRelationKind, RelationBindContext, RelationBinding,
        SurfaceFamily,
    };
    use crate::session::SessionDependency;

    #[test]
    fn builtin_facade_resolves_surface_descriptors_and_columns() {
        let facade = builtin_catalog_compiler_facade();

        let descriptor = facade
            .resolve_surface_descriptor("lix_version")
            .expect("lix_version surface should resolve");
        assert_eq!(descriptor.public_name, "lix_version");
        assert_eq!(descriptor.surface_family, SurfaceFamily::Admin);

        let columns = facade
            .visible_columns("lix_version")
            .expect("lix_version columns should resolve");
        assert!(columns.iter().any(|column| column == "id"));
        assert!(columns.iter().any(|column| column == "commit_id"));

        let object_name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new("lix_version"))]);
        let binding = facade
            .resolve_object_name(&object_name)
            .expect("object-name resolution should succeed");
        assert_eq!(binding.descriptor.public_name, "lix_version");

        let public_names = facade.public_surface_names();
        assert!(public_names.iter().any(|name| name == "lix_version"));

        let relation_binding = facade
            .bind_relation("lix_version", RelationBindContext::default())
            .expect("binding lookup should succeed")
            .expect("lix_version relation binding should exist");
        match relation_binding {
            RelationBinding::VersionRelation(_) => {}
            other => panic!("expected version relation binding, got {other:?}"),
        }
    }

    #[test]
    fn builtin_facade_exposes_derived_surface_declarations() {
        let facade = builtin_catalog_compiler_facade();

        let definition = facade
            .derived_surface_definition("lix_version")
            .expect("lix_version declaration should resolve");
        assert_eq!(definition.name(), "lix_version");

        let surfaces = definition.surfaces();
        assert_eq!(surfaces.len(), 1);
        assert_eq!(surfaces[0].public_name, "lix_version");
    }

    #[test]
    fn builtin_facade_exposes_catalog_owned_dependency_metadata() {
        let facade = builtin_catalog_compiler_facade();

        let version = facade
            .dependency_metadata("lix_version")
            .expect("dependency metadata lookup should succeed")
            .expect("lix_version dependency metadata should resolve");
        assert!(version.relation_names.contains("lix_version"));
        assert!(version
            .compiled_schema_keys
            .contains("lix_version_descriptor"));
        assert!(version.compiled_schema_keys.contains("lix_version_ref"));
        assert!(version
            .session_dependencies
            .contains(&SessionDependency::PublicSurfaceRegistryGeneration));

        let state = facade
            .dependency_metadata("lix_state")
            .expect("dependency metadata lookup should succeed")
            .expect("lix_state dependency metadata should resolve");
        assert!(state.relation_names.contains("lix_state"));
        assert!(state.uses_dynamic_state_relations);
        assert!(state
            .session_dependencies
            .contains(&SessionDependency::ActiveVersion));

        let working_changes = facade
            .dependency_metadata("lix_working_changes")
            .expect("dependency metadata lookup should succeed")
            .expect("lix_working_changes dependency metadata should resolve");
        assert!(working_changes
            .relation_names
            .contains("lix_working_changes"));
        assert!(working_changes.uses_dynamic_state_relations);
    }

    #[test]
    fn builtin_facade_exposes_catalog_owned_direct_read_semantics() {
        let facade = builtin_catalog_compiler_facade();
        let binding = facade
            .resolve_surface("lix_file_history")
            .expect("lix_file_history should resolve");

        assert_eq!(
            facade.history_read_semantics(&binding),
            Some(CatalogHistoryReadSemantics::FileHistory {
                active_version_lineage: true,
            })
        );
    }

    #[test]
    fn builtin_facade_exposes_catalog_owned_transaction_insert_semantics() {
        let facade = builtin_catalog_compiler_facade();
        let binding = facade
            .resolve_surface("lix_file")
            .expect("lix_file should resolve");

        assert_eq!(
            facade.transaction_insert_semantics(&binding),
            Some(CatalogTransactionInsertSemantics {
                coalescable: true,
                transaction_sensitive_columns: std::collections::BTreeSet::from([
                    "global".to_string(),
                    "untracked".to_string(),
                    "version_id".to_string(),
                ]),
            })
        );
    }

    #[test]
    fn builtin_facade_exposes_catalog_owned_write_surface_semantics() {
        let facade = builtin_catalog_compiler_facade();

        let file = facade
            .resolve_surface("lix_file")
            .expect("lix_file should resolve");
        assert_eq!(
            facade
                .write_surface_semantics(&file)
                .expect("write semantics lookup should succeed"),
            Some(CatalogWriteSurfaceSemantics {
                target_kind: CatalogWriteTargetKind::Filesystem,
                version_semantics: CatalogWriteVersionSemantics::ActiveVersionDefault,
                filesystem_kind: Some(FilesystemRelationKind::File),
                filesystem_scope: Some(FilesystemProjectionScope::ActiveVersion),
                supports_untracked_writes: true,
                admin_behavior: None,
            })
        );

        let version = facade
            .resolve_surface("lix_version")
            .expect("lix_version should resolve");
        assert_eq!(
            facade
                .write_surface_semantics(&version)
                .expect("write semantics lookup should succeed"),
            Some(CatalogWriteSurfaceSemantics {
                target_kind: CatalogWriteTargetKind::Admin,
                version_semantics: CatalogWriteVersionSemantics::GlobalAdmin,
                filesystem_kind: None,
                filesystem_scope: None,
                supports_untracked_writes: true,
                admin_behavior: Some(CatalogAdminWriteBehavior::Version),
            })
        );
    }

    #[test]
    fn builtin_facade_exposes_catalog_owned_scan_semantics() {
        let facade = builtin_catalog_compiler_facade();

        let file_history = facade
            .resolve_surface("lix_file_history")
            .expect("lix_file_history should resolve");
        assert_eq!(
            facade
                .filesystem_scan_semantics(&file_history)
                .expect("filesystem scan semantics lookup should succeed"),
            Some(CatalogFilesystemScanSemantics {
                kind: FilesystemRelationKind::File,
                version_scope: CatalogScanVersionScope::History,
            })
        );

        let working_changes = facade
            .resolve_surface("lix_working_changes")
            .expect("lix_working_changes should resolve");
        assert!(facade.is_working_changes_surface(&working_changes));

        let version = facade
            .resolve_surface("lix_version")
            .expect("lix_version should resolve");
        assert_eq!(
            facade.admin_scan_kind(&version),
            Some(CatalogAdminScanKind::Version)
        );
    }
}
