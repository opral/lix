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
    RequestedVersion,
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
    pub(crate) tombstone: bool,
    pub(crate) identity: RowIdentity,
    pub(crate) schema_key: String,
    pub(crate) version_id: String,
    pub(crate) schema_version: Option<String>,
    pub(crate) plugin_key: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) change_id: Option<String>,
    pub(crate) writer_key: Option<String>,
    pub(crate) global: Option<bool>,
    pub(crate) created_at: Option<String>,
    pub(crate) updated_at: Option<String>,
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
            tombstone: false,
            identity,
            schema_key: schema_key.into(),
            version_id: version_id.into(),
            schema_version: None,
            plugin_key: None,
            metadata: None,
            change_id: None,
            writer_key: None,
            global: None,
            created_at: None,
            updated_at: None,
            values,
        }
    }

    pub(crate) fn with_live_metadata(
        mut self,
        schema_version: impl Into<String>,
        plugin_key: impl Into<String>,
        metadata: Option<String>,
        change_id: Option<String>,
        writer_key: Option<String>,
        global: bool,
        created_at: Option<String>,
        updated_at: Option<String>,
    ) -> Self {
        self.schema_version = Some(schema_version.into());
        self.plugin_key = Some(plugin_key.into());
        self.metadata = metadata;
        self.change_id = change_id;
        self.writer_key = writer_key;
        self.global = Some(global);
        self.created_at = created_at;
        self.updated_at = updated_at;
        self
    }

    pub(crate) fn with_tombstone(mut self, tombstone: bool) -> Self {
        self.tombstone = tombstone;
        self
    }

    pub(crate) fn set_writer_key(&mut self, writer_key: Option<String>) {
        self.writer_key = writer_key;
    }

    pub(crate) fn storage(&self) -> CatalogProjectionStorageKind {
        self.storage
    }

    pub(crate) fn is_tombstone(&self) -> bool {
        self.tombstone
    }

    #[allow(dead_code)]
    pub(crate) fn identity(&self) -> &RowIdentity {
        &self.identity
    }

    #[allow(dead_code)]
    pub(crate) fn values(&self) -> &std::collections::BTreeMap<String, Value> {
        &self.values
    }

    pub(crate) fn file_id(&self) -> &str {
        self.identity.file_id.as_str()
    }

    pub(crate) fn schema_version(&self) -> Option<&str> {
        self.schema_version.as_deref()
    }

    pub(crate) fn plugin_key(&self) -> Option<&str> {
        self.plugin_key.as_deref()
    }

    pub(crate) fn metadata_text(&self) -> Option<&str> {
        self.metadata.as_deref()
    }

    pub(crate) fn change_id(&self) -> Option<&str> {
        self.change_id.as_deref()
    }

    pub(crate) fn writer_key(&self) -> Option<&str> {
        self.writer_key.as_deref()
    }

    pub(crate) fn global(&self) -> Option<bool> {
        self.global
    }

    pub(crate) fn created_at(&self) -> Option<&str> {
        self.created_at.as_deref()
    }

    pub(crate) fn updated_at(&self) -> Option<&str> {
        self.updated_at.as_deref()
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
pub(crate) struct CatalogProjectionContext {
    pub(crate) requested_version_id: Option<String>,
    pub(crate) current_committed_version_ids: Vec<String>,
    pub(crate) current_version_heads: std::collections::BTreeMap<String, String>,
    pub(crate) change_commit_ids: std::collections::BTreeMap<String, String>,
    pub(crate) blob_data_by_hash: std::collections::BTreeMap<String, Option<Vec<u8>>>,
}

impl CatalogProjectionContext {
    pub(crate) fn requested_version_id(&self) -> Option<&str> {
        self.requested_version_id.as_deref()
    }

    pub(crate) fn current_committed_version_ids(&self) -> &[String] {
        self.current_committed_version_ids.as_slice()
    }

    pub(crate) fn current_head_commit_id(&self, version_id: &str) -> Option<&str> {
        self.current_version_heads
            .get(version_id)
            .map(String::as_str)
    }

    pub(crate) fn commit_id_for_change(&self, change_id: &str) -> Option<&str> {
        self.change_commit_ids.get(change_id).map(String::as_str)
    }

    pub(crate) fn blob_data(&self, blob_hash: &str) -> Option<&[u8]> {
        self.blob_data_by_hash
            .get(blob_hash)
            .and_then(|value| value.as_deref())
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct CatalogProjectionInput {
    pub(crate) inputs: Vec<CatalogProjectionInputRows>,
    pub(crate) context: CatalogProjectionContext,
}

impl CatalogProjectionInput {
    #[allow(dead_code)]
    pub(crate) fn new(inputs: Vec<CatalogProjectionInputRows>) -> Self {
        Self {
            inputs,
            context: CatalogProjectionContext::default(),
        }
    }

    pub(crate) fn with_context(
        inputs: Vec<CatalogProjectionInputRows>,
        context: CatalogProjectionContext,
    ) -> Self {
        Self { inputs, context }
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

    pub(crate) fn context(&self) -> &CatalogProjectionContext {
        &self.context
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
        CatalogProjectionRegistry::new(vec![
            RegisteredCatalogProjection::new(
                crate::catalog::builtin_lix_version_catalog_registration(),
            ),
            RegisteredCatalogProjection::new(
                crate::catalog::builtin_lix_file_catalog_registration(),
            ),
            RegisteredCatalogProjection::new(
                crate::catalog::builtin_lix_file_by_version_catalog_registration(),
            ),
            RegisteredCatalogProjection::new(
                crate::catalog::builtin_lix_directory_catalog_registration(),
            ),
            RegisteredCatalogProjection::new(
                crate::catalog::builtin_lix_directory_by_version_catalog_registration(),
            ),
        ])
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
    fn builtin_catalog_projection_registry_exposes_builtin_surfaces() {
        let registrations = builtin_catalog_projection_registry().registrations();

        let names = registrations
            .iter()
            .map(|registration| registration.projection().name())
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec![
                "lix_version",
                "lix_file",
                "lix_file_by_version",
                "lix_directory",
                "lix_directory_by_version",
            ]
        );
        assert!(registrations
            .iter()
            .all(|registration| registration.lifecycle() == CatalogProjectionLifecycle::ReadTime));
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
