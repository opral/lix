use sqlparser::ast::ObjectName;

use crate::catalog::{
    bind_named_relation, bind_surface_relation, builtin_catalog_projection_registry,
    CatalogProjectionDefinition, CatalogProjectionRegistry, RegisteredCatalogProjection,
    RelationBindContext, RelationBinding, SurfaceBinding, SurfaceDescriptor, SurfaceRegistry,
};
use crate::LixError;

/// Compiler-facing semantic catalog contract.
///
/// This is the root API `sql/*` should target instead of importing catalog
/// implementation files directly.
#[allow(dead_code)]
pub(crate) trait CatalogCompilerApi {
    fn resolve_surface(&self, relation_name: &str) -> Option<SurfaceBinding>;

    fn resolve_surface_descriptor(&self, relation_name: &str) -> Option<SurfaceDescriptor>;

    fn resolve_object_name(&self, name: &ObjectName) -> Option<SurfaceBinding>;

    fn visible_columns(&self, relation_name: &str) -> Option<Vec<String>>;

    fn public_surface_names(&self) -> Vec<String>;

    fn bind_relation(
        &self,
        relation_name: &str,
        context: RelationBindContext<'_>,
    ) -> Result<Option<RelationBinding>, LixError>;

    fn bind_surface_runtime_relation(
        &self,
        surface_binding: &SurfaceBinding,
        context: RelationBindContext<'_>,
    ) -> Result<Option<RelationBinding>, LixError>;

    fn derived_surface_registration(
        &self,
        public_name: &str,
    ) -> Option<&RegisteredCatalogProjection>;

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
    fn resolve_surface(&self, relation_name: &str) -> Option<SurfaceBinding> {
        self.surfaces.bind_relation_name(relation_name)
    }

    fn resolve_surface_descriptor(&self, relation_name: &str) -> Option<SurfaceDescriptor> {
        self.resolve_surface(relation_name)
            .map(|binding| binding.descriptor)
    }

    fn resolve_object_name(&self, name: &ObjectName) -> Option<SurfaceBinding> {
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
        surface_binding: &SurfaceBinding,
        context: RelationBindContext<'_>,
    ) -> Result<Option<RelationBinding>, LixError> {
        bind_surface_relation(surface_binding, context)
    }

    fn derived_surface_registration(
        &self,
        public_name: &str,
    ) -> Option<&RegisteredCatalogProjection> {
        self.declarations.registration_for_surface(public_name)
    }
}

pub(crate) fn builtin_catalog_compiler_facade() -> CatalogCompilerFacade<'static> {
    CatalogCompilerFacade::new(
        super::builtin_surface_registry(),
        builtin_catalog_projection_registry(),
    )
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};

    use super::{builtin_catalog_compiler_facade, CatalogCompilerApi};
    use crate::catalog::{RelationBindContext, RelationBinding, SurfaceFamily};

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
}
