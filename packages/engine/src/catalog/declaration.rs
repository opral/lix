use std::sync::{Arc, OnceLock};

use crate::catalog::{SurfaceFamily, SurfaceVariant};
use crate::common::error::LixError;
use crate::common::types::Value;
use crate::contracts::artifacts::RowIdentity;

/// Catalog-owned declaration contracts for derived public surfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum CatalogProjectionLifecycle {
    ReadTime,
    WriteTime,
    Async,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct CatalogProjectionRegistration<P> {
    projection: P,
    lifecycle: CatalogProjectionLifecycle,
}

#[allow(dead_code)]
impl<P> CatalogProjectionRegistration<P> {
    pub(crate) fn new(projection: P, lifecycle: CatalogProjectionLifecycle) -> Self {
        Self {
            projection,
            lifecycle,
        }
    }

    pub(crate) fn projection(&self) -> &P {
        &self.projection
    }

    pub(crate) fn lifecycle(&self) -> CatalogProjectionLifecycle {
        self.lifecycle
    }

    pub(crate) fn into_parts(self) -> (P, CatalogProjectionLifecycle) {
        (self.projection, self.lifecycle)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum CatalogProjectionStorageKind {
    Tracked,
    Untracked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum CatalogProjectionInputVersionScope {
    SchemaDefault,
    Global,
    CurrentCommittedFrontier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct CatalogProjectionInputSpec {
    pub(crate) schema_key: String,
    pub(crate) storage: CatalogProjectionStorageKind,
    pub(crate) version_scope: CatalogProjectionInputVersionScope,
}

#[allow(dead_code)]
impl CatalogProjectionInputSpec {
    pub(crate) fn tracked(schema_key: impl Into<String>) -> Self {
        Self {
            schema_key: schema_key.into(),
            storage: CatalogProjectionStorageKind::Tracked,
            version_scope: CatalogProjectionInputVersionScope::SchemaDefault,
        }
    }

    pub(crate) fn untracked(schema_key: impl Into<String>) -> Self {
        Self {
            schema_key: schema_key.into(),
            storage: CatalogProjectionStorageKind::Untracked,
            version_scope: CatalogProjectionInputVersionScope::SchemaDefault,
        }
    }

    pub(crate) fn with_version_scope(
        mut self,
        version_scope: CatalogProjectionInputVersionScope,
    ) -> Self {
        self.version_scope = version_scope;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct CatalogProjectionSurfaceSpec {
    pub(crate) public_name: String,
    pub(crate) surface_family: SurfaceFamily,
    pub(crate) surface_variant: SurfaceVariant,
}

#[allow(dead_code)]
impl CatalogProjectionSurfaceSpec {
    pub(crate) fn new(
        public_name: impl Into<String>,
        surface_family: SurfaceFamily,
        surface_variant: SurfaceVariant,
    ) -> Self {
        Self {
            public_name: public_name.into(),
            surface_family,
            surface_variant,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CatalogProjectionSourceRow {
    pub(crate) storage: CatalogProjectionStorageKind,
    pub(crate) identity: RowIdentity,
    pub(crate) schema_key: String,
    pub(crate) version_id: String,
    pub(crate) values: std::collections::BTreeMap<String, Value>,
}

impl CatalogProjectionSourceRow {
    pub(crate) fn new(
        storage: CatalogProjectionStorageKind,
        identity: RowIdentity,
        schema_key: impl Into<String>,
        version_id: impl Into<String>,
        values: std::collections::BTreeMap<String, Value>,
    ) -> Self {
        Self {
            storage,
            identity,
            schema_key: schema_key.into(),
            version_id: version_id.into(),
            values,
        }
    }

    pub(crate) fn storage(&self) -> CatalogProjectionStorageKind {
        self.storage
    }

    #[allow(dead_code)]
    pub(crate) fn identity(&self) -> &RowIdentity {
        &self.identity
    }

    #[allow(dead_code)]
    pub(crate) fn values(&self) -> &std::collections::BTreeMap<String, Value> {
        &self.values
    }

    pub(crate) fn entity_id(&self) -> &str {
        self.identity.entity_id.as_str()
    }

    pub(crate) fn property_text(&self, property_name: &str) -> Option<String> {
        self.values
            .get(property_name)
            .and_then(|value| match value {
                Value::Text(text) => Some(text.clone()),
                _ => None,
            })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CatalogProjectionInputRows {
    pub(crate) spec: CatalogProjectionInputSpec,
    pub(crate) rows: Vec<CatalogProjectionSourceRow>,
}

impl CatalogProjectionInputRows {
    pub(crate) fn new(
        spec: CatalogProjectionInputSpec,
        rows: Vec<CatalogProjectionSourceRow>,
    ) -> Self {
        Self { spec, rows }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct CatalogProjectionInput {
    pub(crate) inputs: Vec<CatalogProjectionInputRows>,
}

impl CatalogProjectionInput {
    pub(crate) fn new(inputs: Vec<CatalogProjectionInputRows>) -> Self {
        Self { inputs }
    }

    pub(crate) fn rows_for(
        &self,
        spec: &CatalogProjectionInputSpec,
    ) -> Option<&[CatalogProjectionSourceRow]> {
        self.inputs
            .iter()
            .find(|input| &input.spec == spec)
            .map(|input| input.rows.as_slice())
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct CatalogDerivedRow {
    pub(crate) surface_name: String,
    pub(crate) identity: Option<RowIdentity>,
    pub(crate) values: std::collections::BTreeMap<String, Value>,
}

impl CatalogDerivedRow {
    pub(crate) fn new(
        surface_name: impl Into<String>,
        values: std::collections::BTreeMap<String, Value>,
    ) -> Self {
        Self {
            surface_name: surface_name.into(),
            identity: None,
            values,
        }
    }

    pub(crate) fn with_identity(mut self, identity: RowIdentity) -> Self {
        self.identity = Some(identity);
        self
    }
}

/// Catalog-owned declaration boundary.
#[allow(dead_code)]
pub(crate) trait CatalogProjectionDefinition: Send + Sync {
    fn name(&self) -> &'static str;

    fn inputs(&self) -> Vec<CatalogProjectionInputSpec>;

    fn surfaces(&self) -> Vec<CatalogProjectionSurfaceSpec>;

    fn derive(&self, input: &CatalogProjectionInput) -> Result<Vec<CatalogDerivedRow>, LixError>;
}

#[derive(Clone)]
pub(crate) struct RegisteredCatalogProjection {
    projection: Arc<dyn CatalogProjectionDefinition>,
    lifecycle: CatalogProjectionLifecycle,
}

impl RegisteredCatalogProjection {
    pub(crate) fn new<P>(registration: CatalogProjectionRegistration<P>) -> Self
    where
        P: CatalogProjectionDefinition + 'static,
    {
        let (projection, lifecycle) = registration.into_parts();
        Self {
            projection: Arc::new(projection),
            lifecycle,
        }
    }

    pub(crate) fn projection(&self) -> &dyn CatalogProjectionDefinition {
        self.projection.as_ref()
    }

    pub(crate) fn lifecycle(&self) -> CatalogProjectionLifecycle {
        self.lifecycle
    }
}

#[derive(Clone)]
pub(crate) struct CatalogProjectionRegistry {
    registrations: Vec<RegisteredCatalogProjection>,
}

impl CatalogProjectionRegistry {
    pub(crate) fn new(registrations: Vec<RegisteredCatalogProjection>) -> Self {
        Self { registrations }
    }

    pub(crate) fn registrations(&self) -> &[RegisteredCatalogProjection] {
        &self.registrations
    }
}

static BUILTIN_CATALOG_PROJECTION_REGISTRY: OnceLock<CatalogProjectionRegistry> = OnceLock::new();

pub(crate) fn builtin_catalog_projection_registry() -> &'static CatalogProjectionRegistry {
    BUILTIN_CATALOG_PROJECTION_REGISTRY.get_or_init(|| {
        CatalogProjectionRegistry::new(vec![RegisteredCatalogProjection::new(
            crate::catalog::builtin_lix_version_catalog_registration(),
        )])
    })
}

#[cfg(test)]
mod tests {
    use super::{
        builtin_catalog_projection_registry, CatalogDerivedRow, CatalogProjectionDefinition,
        CatalogProjectionInput, CatalogProjectionInputRows, CatalogProjectionInputSpec,
        CatalogProjectionInputVersionScope, CatalogProjectionLifecycle,
        CatalogProjectionRegistration, CatalogProjectionRegistry, CatalogProjectionSourceRow,
        CatalogProjectionSurfaceSpec, RegisteredCatalogProjection,
    };
    use crate::catalog::{SurfaceFamily, SurfaceVariant};
    use crate::contracts::artifacts::RowIdentity;
    use crate::Value;

    #[test]
    fn catalog_registration_keeps_definition_separate_from_lifecycle() {
        let registration =
            CatalogProjectionRegistration::new("demo", CatalogProjectionLifecycle::WriteTime);

        assert_eq!(registration.projection(), &"demo");
        assert_eq!(
            registration.lifecycle(),
            CatalogProjectionLifecycle::WriteTime
        );
    }

    #[test]
    fn catalog_input_spec_preserves_version_scope_override() {
        let spec = CatalogProjectionInputSpec::tracked("lix_version_descriptor")
            .with_version_scope(CatalogProjectionInputVersionScope::CurrentCommittedFrontier);

        assert_eq!(
            spec,
            CatalogProjectionInputSpec {
                schema_key: "lix_version_descriptor".to_string(),
                storage: crate::catalog::CatalogProjectionStorageKind::Tracked,
                version_scope: CatalogProjectionInputVersionScope::CurrentCommittedFrontier,
            }
        );
    }

    #[test]
    fn catalog_projection_definition_stays_declarative_over_inputs_and_surfaces() {
        let definition = DemoDefinition;

        assert_eq!(
            definition.inputs(),
            vec![CatalogProjectionInputSpec::untracked("lix_version_ref")]
        );
        assert_eq!(
            definition.surfaces(),
            vec![CatalogProjectionSurfaceSpec::new(
                "lix_version",
                SurfaceFamily::Admin,
                SurfaceVariant::Default,
            )]
        );
    }

    #[test]
    fn registered_catalog_projection_keeps_definition_and_lifecycle() {
        let registration = RegisteredCatalogProjection::new(CatalogProjectionRegistration::new(
            DemoDefinition,
            CatalogProjectionLifecycle::Async,
        ));

        assert_eq!(registration.projection().name(), "demo");
        assert_eq!(registration.lifecycle(), CatalogProjectionLifecycle::Async);
    }

    #[test]
    fn builtin_catalog_projection_registry_exposes_lix_version() {
        let registrations = builtin_catalog_projection_registry().registrations();

        assert_eq!(registrations.len(), 1);
        assert_eq!(registrations[0].projection().name(), "lix_version");
        assert_eq!(
            registrations[0].lifecycle(),
            CatalogProjectionLifecycle::ReadTime
        );
    }

    #[test]
    fn catalog_projection_registry_holds_registered_catalog_projections() {
        let registry = CatalogProjectionRegistry::new(vec![RegisteredCatalogProjection::new(
            CatalogProjectionRegistration::new(
                DemoDefinition,
                CatalogProjectionLifecycle::ReadTime,
            ),
        )]);

        assert_eq!(registry.registrations().len(), 1);
        assert_eq!(registry.registrations()[0].projection().name(), "demo");
    }

    #[test]
    fn catalog_projection_input_groups_rows_by_declared_spec() {
        let tracked_spec = CatalogProjectionInputSpec::tracked("lix_version_descriptor");
        let untracked_spec = CatalogProjectionInputSpec::untracked("lix_version_ref");

        let input = CatalogProjectionInput::new(vec![
            CatalogProjectionInputRows::new(tracked_spec.clone(), vec![sample_tracked_row()]),
            CatalogProjectionInputRows::new(untracked_spec.clone(), vec![sample_untracked_row()]),
        ]);

        assert_eq!(
            input.rows_for(&tracked_spec).expect("tracked rows").len(),
            1
        );
        assert_eq!(
            input
                .rows_for(&untracked_spec)
                .expect("untracked rows")
                .len(),
            1
        );
    }

    #[test]
    fn catalog_derived_row_keeps_surface_name_separate_from_identity() {
        let derived = CatalogDerivedRow::new(
            "lix_version",
            std::collections::BTreeMap::from([
                ("id".to_string(), Value::Text("version-1".into())),
                ("name".to_string(), Value::Text("main".into())),
            ]),
        )
        .with_identity(RowIdentity {
            schema_key: "lix_version_descriptor".into(),
            version_id: "v1".into(),
            entity_id: "version-1".into(),
            file_id: "file-1".into(),
        });

        assert_eq!(derived.surface_name, "lix_version");
        assert_eq!(
            derived.identity,
            Some(RowIdentity {
                schema_key: "lix_version_descriptor".into(),
                version_id: "v1".into(),
                entity_id: "version-1".into(),
                file_id: "file-1".into(),
            })
        );
    }

    #[derive(Clone, Copy)]
    struct DemoDefinition;

    impl CatalogProjectionDefinition for DemoDefinition {
        fn name(&self) -> &'static str {
            "demo"
        }

        fn inputs(&self) -> Vec<CatalogProjectionInputSpec> {
            vec![CatalogProjectionInputSpec::untracked("lix_version_ref")]
        }

        fn surfaces(&self) -> Vec<CatalogProjectionSurfaceSpec> {
            vec![CatalogProjectionSurfaceSpec::new(
                "lix_version",
                SurfaceFamily::Admin,
                SurfaceVariant::Default,
            )]
        }

        fn derive(
            &self,
            _input: &CatalogProjectionInput,
        ) -> Result<Vec<CatalogDerivedRow>, crate::LixError> {
            Ok(Vec::new())
        }
    }

    fn sample_tracked_row() -> CatalogProjectionSourceRow {
        CatalogProjectionSourceRow::new(
            crate::catalog::CatalogProjectionStorageKind::Tracked,
            RowIdentity {
                schema_key: "demo_schema".into(),
                version_id: "v1".into(),
                entity_id: "entity-1".into(),
                file_id: "file-1".into(),
            },
            "demo_schema",
            "v1",
            std::collections::BTreeMap::from([("name".into(), Value::Text("tracked".into()))]),
        )
    }

    fn sample_untracked_row() -> CatalogProjectionSourceRow {
        CatalogProjectionSourceRow::new(
            crate::catalog::CatalogProjectionStorageKind::Untracked,
            RowIdentity {
                schema_key: "demo_schema".into(),
                version_id: "v2".into(),
                entity_id: "entity-2".into(),
                file_id: "file-2".into(),
            },
            "demo_schema",
            "v2",
            std::collections::BTreeMap::from([("name".into(), Value::Text("untracked".into()))]),
        )
    }
}
