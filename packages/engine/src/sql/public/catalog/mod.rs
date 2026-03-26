use crate::cel::shared_runtime;
use crate::schema::annotations::overrides::{collect_lixcol_overrides, LixcolOverrideValue};
use crate::schema::builtin::{builtin_schema_definition, builtin_schema_keys};
use crate::schema::schema_from_registered_snapshot;
use crate::schema::SqlRegisteredSchemaProvider;
use crate::{LixBackend, LixError};
use serde_json::Value as JsonValue;
use sqlparser::ast::{ObjectName, ObjectNamePart};
use std::collections::BTreeMap;
use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub(crate) struct CatalogEpoch(u64);

impl CatalogEpoch {
    pub(crate) fn value(self) -> u64 {
        self.0
    }

    fn bump(&mut self) {
        self.0 += 1;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogSource {
    Builtin,
    Dynamic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SurfaceFamily {
    State,
    Entity,
    Filesystem,
    Admin,
    Change,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SurfaceVariant {
    Default,
    ByVersion,
    History,
    Active,
    WorkingChanges,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SurfaceCapability {
    ReadOnly,
    ReadWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DefaultScopeSemantics {
    ActiveVersion,
    ExplicitVersion,
    History,
    GlobalAdmin,
    WorkingChanges,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct SurfaceTraits {
    pub(crate) state_backed: bool,
    pub(crate) schema_driven_projection: bool,
    pub(crate) exposes_version_column: bool,
    pub(crate) exposes_history_columns: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct SurfaceResolutionCapabilities {
    pub(crate) canonical_state_scan: bool,
    pub(crate) canonical_filesystem_scan: bool,
    pub(crate) canonical_admin_scan: bool,
    pub(crate) canonical_change_scan: bool,
    pub(crate) canonical_working_changes_scan: bool,
    pub(crate) entity_projection: bool,
    pub(crate) semantic_write: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct SurfaceImplicitOverrides {
    pub(crate) fixed_schema_key: Option<String>,
    pub(crate) expose_version_id: bool,
    pub(crate) predicate_overrides: Vec<SurfaceOverridePredicate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SurfaceOverrideValue {
    Null,
    Boolean(bool),
    Number(String),
    String(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SurfaceColumnType {
    String,
    Integer,
    Number,
    Boolean,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SurfaceOverridePredicate {
    pub(crate) column: String,
    pub(crate) value: SurfaceOverrideValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SurfaceDescriptor {
    pub(crate) public_name: String,
    pub(crate) surface_family: SurfaceFamily,
    pub(crate) surface_variant: SurfaceVariant,
    pub(crate) visible_columns: Vec<String>,
    pub(crate) hidden_columns: Vec<String>,
    pub(crate) column_types: BTreeMap<String, SurfaceColumnType>,
    pub(crate) capability: SurfaceCapability,
    pub(crate) default_scope: DefaultScopeSemantics,
    pub(crate) surface_traits: SurfaceTraits,
    pub(crate) resolution_capabilities: SurfaceResolutionCapabilities,
    pub(crate) implicit_overrides: SurfaceImplicitOverrides,
    pub(crate) catalog_source: CatalogSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SurfaceBinding {
    pub(crate) descriptor: SurfaceDescriptor,
    pub(crate) catalog_epoch: Option<CatalogEpoch>,
    pub(crate) exposed_columns: Vec<String>,
    pub(crate) column_types: BTreeMap<String, SurfaceColumnType>,
    pub(crate) capability: SurfaceCapability,
    pub(crate) default_scope: DefaultScopeSemantics,
    pub(crate) implicit_overrides: SurfaceImplicitOverrides,
    pub(crate) resolution_capabilities: SurfaceResolutionCapabilities,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DynamicEntitySurfaceSpec {
    pub(crate) schema_key: String,
    pub(crate) visible_columns: Vec<String>,
    pub(crate) column_types: BTreeMap<String, SurfaceColumnType>,
    pub(crate) predicate_overrides: Vec<SurfaceOverridePredicate>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SurfaceRegistry {
    epoch: CatalogEpoch,
    descriptors: BTreeMap<String, SurfaceDescriptor>,
}

impl SurfaceRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_builtin_surfaces() -> Self {
        let mut registry = Self::new();
        for descriptor in builtin_surface_descriptors() {
            registry.insert_descriptor(descriptor);
        }
        registry.register_builtin_entity_surfaces();
        registry
    }

    pub(crate) async fn bootstrap_with_backend(backend: &dyn LixBackend) -> Result<Self, LixError> {
        let mut registry = Self::with_builtin_surfaces();
        let mut provider = SqlRegisteredSchemaProvider::new(backend);
        for (_, schema) in provider.load_latest_schema_entries().await? {
            let spec = entity_surface_spec_from_schema(&schema)?;
            registry.register_dynamic_entity_surfaces(spec);
        }
        Ok(registry)
    }

    pub(crate) fn epoch(&self) -> CatalogEpoch {
        self.epoch
    }

    pub(crate) fn bind_relation_name(&self, relation_name: &str) -> Option<SurfaceBinding> {
        let key = normalize_surface_name(relation_name);
        let descriptor = self.descriptors.get(&key)?.clone();
        Some(SurfaceBinding {
            catalog_epoch: match descriptor.catalog_source {
                CatalogSource::Builtin => None,
                CatalogSource::Dynamic => Some(self.epoch),
            },
            exposed_columns: descriptor.visible_columns.clone(),
            column_types: descriptor.column_types.clone(),
            capability: descriptor.capability,
            default_scope: descriptor.default_scope,
            implicit_overrides: descriptor.implicit_overrides.clone(),
            resolution_capabilities: descriptor.resolution_capabilities.clone(),
            descriptor,
        })
    }

    pub(crate) fn bind_object_name(&self, name: &ObjectName) -> Option<SurfaceBinding> {
        let relation_name = object_name_to_relation_name(name)?;
        self.bind_relation_name(&relation_name)
    }

    pub(crate) fn public_surface_names(&self) -> Vec<String> {
        self.descriptors
            .values()
            .map(|descriptor| descriptor.public_name.clone())
            .collect()
    }

    pub(crate) fn public_surface_columns(&self, relation_name: &str) -> Option<Vec<String>> {
        self.bind_relation_name(relation_name).map(|binding| {
            let mut columns = binding.descriptor.visible_columns.clone();
            columns.extend(binding.descriptor.hidden_columns.clone());
            columns
        })
    }

    pub(crate) fn registered_schema_keys(&self) -> Vec<String> {
        let mut schema_keys = self
            .descriptors
            .values()
            .filter_map(|descriptor| descriptor.implicit_overrides.fixed_schema_key.clone())
            .collect::<Vec<_>>();
        schema_keys.sort();
        schema_keys.dedup();
        schema_keys
    }

    pub(crate) fn registered_state_backed_schema_keys(&self) -> Vec<String> {
        let mut schema_keys = self
            .descriptors
            .values()
            .filter(|descriptor| {
                matches!(
                    descriptor.surface_family,
                    SurfaceFamily::State | SurfaceFamily::Entity | SurfaceFamily::Filesystem
                )
            })
            .filter_map(|descriptor| descriptor.implicit_overrides.fixed_schema_key.clone())
            .collect::<Vec<_>>();
        schema_keys.sort();
        schema_keys.dedup();
        schema_keys
    }

    pub(crate) fn registered_state_surface_schema_keys(&self) -> Vec<String> {
        self.registered_state_backed_schema_keys()
    }

    pub(crate) fn register_dynamic_entity_surfaces(
        &mut self,
        spec: DynamicEntitySurfaceSpec,
    ) -> CatalogEpoch {
        let descriptors = entity_descriptors_from_spec(&spec, CatalogSource::Dynamic)
            .into_iter()
            .filter(|descriptor| self.entity_descriptor_name_available(&descriptor.public_name))
            .collect::<Vec<_>>();
        if descriptors.is_empty() {
            return self.epoch;
        }
        self.epoch.bump();
        for descriptor in descriptors {
            self.insert_descriptor(descriptor);
        }
        self.epoch
    }

    pub(crate) fn remove_dynamic_entity_surfaces_for_schema_key(&mut self, schema_key: &str) {
        let dynamic_descriptor_names = self
            .descriptors
            .iter()
            .filter_map(|(name, descriptor)| {
                (descriptor.catalog_source == CatalogSource::Dynamic
                    && descriptor.implicit_overrides.fixed_schema_key.as_deref()
                        == Some(schema_key))
                .then_some(name.clone())
            })
            .collect::<Vec<_>>();
        if dynamic_descriptor_names.is_empty() {
            return;
        }
        self.epoch.bump();
        for name in dynamic_descriptor_names {
            self.descriptors.remove(&name);
        }
    }

    pub(crate) fn replace_dynamic_entity_surfaces_from_stored_snapshot(
        &mut self,
        snapshot: &JsonValue,
    ) -> Result<(), LixError> {
        let (key, schema) = schema_from_registered_snapshot(snapshot)?;
        self.remove_dynamic_entity_surfaces_for_schema_key(&key.schema_key);
        let spec = entity_surface_spec_from_schema(&schema)?;
        self.register_dynamic_entity_surfaces(spec);
        Ok(())
    }

    fn insert_descriptor(&mut self, descriptor: SurfaceDescriptor) {
        self.descriptors
            .insert(normalize_surface_name(&descriptor.public_name), descriptor);
    }

    fn register_builtin_entity_surfaces(&mut self) {
        for schema_key in builtin_schema_keys() {
            if !builtin_schema_exposed_as_entity_surface(schema_key) {
                continue;
            }
            let Some(schema) = builtin_schema_definition(schema_key) else {
                continue;
            };
            let Ok(spec) = entity_surface_spec_from_schema(schema) else {
                continue;
            };
            for descriptor in entity_descriptors_from_spec(&spec, CatalogSource::Builtin) {
                if !self.entity_descriptor_name_available(&descriptor.public_name) {
                    continue;
                }
                self.insert_descriptor(descriptor);
            }
        }
    }

    fn entity_descriptor_name_available(&self, public_name: &str) -> bool {
        self.descriptors
            .get(&normalize_surface_name(public_name))
            .is_none_or(|descriptor| descriptor.surface_family == SurfaceFamily::Entity)
    }
}

fn builtin_schema_exposed_as_entity_surface(schema_key: &str) -> bool {
    !matches!(schema_key, "lix_active_version" | "lix_active_account")
}

fn normalize_surface_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

fn builtin_surface_registry() -> &'static SurfaceRegistry {
    static BUILTIN_SURFACE_REGISTRY: OnceLock<SurfaceRegistry> = OnceLock::new();
    BUILTIN_SURFACE_REGISTRY.get_or_init(SurfaceRegistry::with_builtin_surfaces)
}

pub(crate) fn builtin_public_surface_names() -> Vec<String> {
    builtin_surface_registry().public_surface_names()
}

pub(crate) fn builtin_public_surface_columns(relation_name: &str) -> Option<Vec<String>> {
    builtin_surface_registry().public_surface_columns(relation_name)
}

fn object_name_to_relation_name(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .map(|ident| ident.value.clone())
}

fn builtin_surface_descriptors() -> Vec<SurfaceDescriptor> {
    vec![
        state_surface_descriptor("lix_state", SurfaceVariant::Default),
        state_surface_descriptor("lix_state_by_version", SurfaceVariant::ByVersion),
        state_surface_descriptor("lix_state_history", SurfaceVariant::History),
        SurfaceDescriptor {
            public_name: "lix_change".to_string(),
            surface_family: SurfaceFamily::Change,
            surface_variant: SurfaceVariant::History,
            visible_columns: change_columns(),
            hidden_columns: Vec::new(),
            column_types: change_column_types(),
            capability: SurfaceCapability::ReadOnly,
            default_scope: DefaultScopeSemantics::History,
            surface_traits: SurfaceTraits {
                exposes_history_columns: true,
                ..SurfaceTraits::default()
            },
            resolution_capabilities: SurfaceResolutionCapabilities {
                canonical_change_scan: true,
                ..SurfaceResolutionCapabilities::default()
            },
            implicit_overrides: SurfaceImplicitOverrides::default(),
            catalog_source: CatalogSource::Builtin,
        },
        SurfaceDescriptor {
            public_name: "lix_working_changes".to_string(),
            surface_family: SurfaceFamily::Change,
            surface_variant: SurfaceVariant::WorkingChanges,
            visible_columns: working_changes_columns(),
            hidden_columns: Vec::new(),
            column_types: working_changes_column_types(),
            capability: SurfaceCapability::ReadOnly,
            default_scope: DefaultScopeSemantics::WorkingChanges,
            surface_traits: SurfaceTraits {
                exposes_history_columns: true,
                ..SurfaceTraits::default()
            },
            resolution_capabilities: SurfaceResolutionCapabilities {
                canonical_working_changes_scan: true,
                ..SurfaceResolutionCapabilities::default()
            },
            implicit_overrides: SurfaceImplicitOverrides::default(),
            catalog_source: CatalogSource::Builtin,
        },
        filesystem_surface_descriptor("lix_file", SurfaceVariant::Default),
        filesystem_surface_descriptor("lix_file_by_version", SurfaceVariant::ByVersion),
        filesystem_surface_descriptor("lix_file_history", SurfaceVariant::History),
        filesystem_surface_descriptor("lix_file_history_by_version", SurfaceVariant::History),
        filesystem_surface_descriptor("lix_directory", SurfaceVariant::Default),
        filesystem_surface_descriptor("lix_directory_by_version", SurfaceVariant::ByVersion),
        filesystem_surface_descriptor("lix_directory_history", SurfaceVariant::History),
        admin_surface_descriptor("lix_version", SurfaceVariant::Default),
    ]
}

fn state_surface_descriptor(name: &str, variant: SurfaceVariant) -> SurfaceDescriptor {
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
        SurfaceVariant::Active | SurfaceVariant::WorkingChanges => (
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

fn filesystem_surface_descriptor(name: &str, variant: SurfaceVariant) -> SurfaceDescriptor {
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
        SurfaceVariant::Active | SurfaceVariant::WorkingChanges => (
            DefaultScopeSemantics::ActiveVersion,
            SurfaceCapability::ReadOnly,
            false,
            false,
        ),
    };

    let visible_columns = match name {
        "lix_file" => filesystem_file_columns(),
        "lix_file_by_version" => filesystem_file_by_version_columns(),
        "lix_file_history" | "lix_file_history_by_version" => filesystem_file_history_columns(),
        "lix_directory" => filesystem_directory_columns(),
        "lix_directory_by_version" => filesystem_directory_by_version_columns(),
        "lix_directory_history" => filesystem_directory_history_columns(),
        _ => filesystem_file_columns(),
    };

    SurfaceDescriptor {
        public_name: name.to_string(),
        surface_family: SurfaceFamily::Filesystem,
        surface_variant: variant,
        visible_columns,
        hidden_columns: Vec::new(),
        column_types: filesystem_column_types(name),
        capability,
        default_scope,
        surface_traits: SurfaceTraits {
            exposes_version_column,
            exposes_history_columns,
            ..SurfaceTraits::default()
        },
        resolution_capabilities: SurfaceResolutionCapabilities {
            canonical_filesystem_scan: true,
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

fn admin_surface_descriptor(name: &str, variant: SurfaceVariant) -> SurfaceDescriptor {
    let capability = match name {
        "lix_version" => SurfaceCapability::ReadWrite,
        _ => SurfaceCapability::ReadOnly,
    };

    SurfaceDescriptor {
        public_name: name.to_string(),
        surface_family: SurfaceFamily::Admin,
        surface_variant: variant,
        visible_columns: admin_columns(name),
        hidden_columns: Vec::new(),
        column_types: admin_column_types(name),
        capability,
        default_scope: DefaultScopeSemantics::GlobalAdmin,
        surface_traits: SurfaceTraits::default(),
        resolution_capabilities: SurfaceResolutionCapabilities {
            canonical_admin_scan: true,
            semantic_write: capability == SurfaceCapability::ReadWrite,
            ..SurfaceResolutionCapabilities::default()
        },
        implicit_overrides: SurfaceImplicitOverrides {
            fixed_schema_key: Some(name.to_string()),
            ..SurfaceImplicitOverrides::default()
        },
        catalog_source: CatalogSource::Builtin,
    }
}

fn entity_descriptors_from_spec(
    spec: &DynamicEntitySurfaceSpec,
    catalog_source: CatalogSource,
) -> Vec<SurfaceDescriptor> {
    let history_name = format!("{}_history", spec.schema_key);
    let by_version_name = format!("{}_by_version", spec.schema_key);
    let default_visible = entity_visible_columns(&spec.visible_columns, false, false);
    let by_version_visible = entity_visible_columns(&spec.visible_columns, true, false);
    let history_visible = entity_visible_columns(&spec.visible_columns, true, true);
    let hidden_columns = entity_hidden_columns();
    let column_types = entity_column_types(&spec.column_types);
    let default_capability = entity_surface_capability(&spec.schema_key, SurfaceVariant::Default);
    let by_version_capability =
        entity_surface_capability(&spec.schema_key, SurfaceVariant::ByVersion);
    let history_capability = entity_surface_capability(&spec.schema_key, SurfaceVariant::History);

    vec![
        SurfaceDescriptor {
            public_name: spec.schema_key.clone(),
            surface_family: SurfaceFamily::Entity,
            surface_variant: SurfaceVariant::Default,
            visible_columns: default_visible,
            hidden_columns: hidden_columns.clone(),
            column_types: column_types.clone(),
            capability: default_capability,
            default_scope: DefaultScopeSemantics::ActiveVersion,
            surface_traits: SurfaceTraits {
                state_backed: true,
                schema_driven_projection: true,
                ..SurfaceTraits::default()
            },
            resolution_capabilities: SurfaceResolutionCapabilities {
                canonical_state_scan: true,
                entity_projection: true,
                semantic_write: default_capability == SurfaceCapability::ReadWrite,
                ..SurfaceResolutionCapabilities::default()
            },
            implicit_overrides: SurfaceImplicitOverrides {
                fixed_schema_key: Some(spec.schema_key.clone()),
                expose_version_id: false,
                predicate_overrides: entity_override_predicates_for_variant(
                    &spec.predicate_overrides,
                    SurfaceVariant::Default,
                ),
            },
            catalog_source,
        },
        SurfaceDescriptor {
            public_name: by_version_name,
            surface_family: SurfaceFamily::Entity,
            surface_variant: SurfaceVariant::ByVersion,
            visible_columns: by_version_visible,
            hidden_columns: hidden_columns.clone(),
            column_types: column_types.clone(),
            capability: by_version_capability,
            default_scope: DefaultScopeSemantics::ExplicitVersion,
            surface_traits: SurfaceTraits {
                state_backed: true,
                schema_driven_projection: true,
                exposes_version_column: true,
                ..SurfaceTraits::default()
            },
            resolution_capabilities: SurfaceResolutionCapabilities {
                canonical_state_scan: true,
                entity_projection: true,
                semantic_write: by_version_capability == SurfaceCapability::ReadWrite,
                ..SurfaceResolutionCapabilities::default()
            },
            implicit_overrides: SurfaceImplicitOverrides {
                fixed_schema_key: Some(spec.schema_key.clone()),
                expose_version_id: true,
                predicate_overrides: entity_override_predicates_for_variant(
                    &spec.predicate_overrides,
                    SurfaceVariant::ByVersion,
                ),
            },
            catalog_source,
        },
        SurfaceDescriptor {
            public_name: history_name,
            surface_family: SurfaceFamily::Entity,
            surface_variant: SurfaceVariant::History,
            visible_columns: history_visible,
            hidden_columns,
            column_types,
            capability: history_capability,
            default_scope: DefaultScopeSemantics::History,
            surface_traits: SurfaceTraits {
                state_backed: true,
                schema_driven_projection: true,
                exposes_version_column: true,
                exposes_history_columns: true,
            },
            resolution_capabilities: SurfaceResolutionCapabilities {
                canonical_state_scan: true,
                entity_projection: true,
                semantic_write: history_capability == SurfaceCapability::ReadWrite,
                ..SurfaceResolutionCapabilities::default()
            },
            implicit_overrides: SurfaceImplicitOverrides {
                fixed_schema_key: Some(spec.schema_key.clone()),
                expose_version_id: true,
                predicate_overrides: entity_override_predicates_for_variant(
                    &spec.predicate_overrides,
                    SurfaceVariant::History,
                ),
            },
            catalog_source,
        },
    ]
}

fn entity_surface_capability(schema_key: &str, variant: SurfaceVariant) -> SurfaceCapability {
    if matches!(variant, SurfaceVariant::History) {
        return SurfaceCapability::ReadOnly;
    }

    match schema_key {
        "lix_commit"
        | "lix_change_set"
        | "lix_commit_edge"
        | "lix_change_set_element"
        | "lix_change_author" => SurfaceCapability::ReadOnly,
        _ => SurfaceCapability::ReadWrite,
    }
}

fn entity_surface_spec_from_schema(
    schema: &JsonValue,
) -> Result<DynamicEntitySurfaceSpec, LixError> {
    let schema_key = schema
        .get("x-lix-key")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "schema is missing string x-lix-key".to_string(),
        })?;

    let mut visible_columns = schema
        .get("properties")
        .and_then(JsonValue::as_object)
        .map(|properties| {
            let mut columns = properties
                .keys()
                .filter(|key| !key.starts_with("lixcol_"))
                .cloned()
                .collect::<Vec<_>>();
            columns.sort();
            columns
        })
        .unwrap_or_default();
    visible_columns.dedup();
    let column_types = schema
        .get("properties")
        .and_then(JsonValue::as_object)
        .map(|properties| {
            properties
                .iter()
                .filter(|(key, _)| !key.starts_with("lixcol_"))
                .filter_map(|(key, property_schema)| {
                    surface_column_type_from_schema(property_schema).map(|kind| (key.clone(), kind))
                })
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();

    let predicate_overrides = collect_override_predicates(schema, schema_key)?;

    Ok(DynamicEntitySurfaceSpec {
        schema_key: schema_key.to_string(),
        visible_columns,
        column_types,
        predicate_overrides,
    })
}

fn entity_override_predicates_for_variant(
    predicates: &[SurfaceOverridePredicate],
    variant: SurfaceVariant,
) -> Vec<SurfaceOverridePredicate> {
    predicates
        .iter()
        .filter(|predicate| match predicate.column.as_str() {
            "global" | "untracked" => !matches!(variant, SurfaceVariant::History),
            _ => true,
        })
        .cloned()
        .collect()
}

fn collect_override_predicates(
    schema: &JsonValue,
    schema_key: &str,
) -> Result<Vec<SurfaceOverridePredicate>, LixError> {
    let mut predicates = Vec::new();
    for override_entry in collect_lixcol_overrides(schema, schema_key, shared_runtime())? {
        let Some(column) = (match override_entry.key.as_str() {
            "lixcol_entity_id" => Some("entity_id"),
            "lixcol_file_id" => Some("file_id"),
            "lixcol_plugin_key" => Some("plugin_key"),
            "lixcol_global" => Some("global"),
            "lixcol_metadata" => Some("metadata"),
            "lixcol_untracked" => Some("untracked"),
            _ => None,
        }) else {
            continue;
        };
        let value = match override_entry.value {
            LixcolOverrideValue::Null => SurfaceOverrideValue::Null,
            LixcolOverrideValue::Boolean(value) => SurfaceOverrideValue::Boolean(value),
            LixcolOverrideValue::Number(value) => SurfaceOverrideValue::Number(value),
            LixcolOverrideValue::String(value) => SurfaceOverrideValue::String(value),
        };
        predicates.push(SurfaceOverridePredicate {
            column: column.to_string(),
            value,
        });
    }
    Ok(predicates)
}

fn state_columns() -> Vec<String> {
    [
        "entity_id",
        "schema_key",
        "file_id",
        "plugin_key",
        "snapshot_content",
        "metadata",
        "schema_version",
        "created_at",
        "updated_at",
        "global",
        "change_id",
        "commit_id",
        "untracked",
        "writer_key",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn state_by_version_columns() -> Vec<String> {
    let mut columns = state_columns();
    columns.push("version_id".to_string());
    columns
}

fn state_column_types() -> BTreeMap<String, SurfaceColumnType> {
    BTreeMap::from([
        ("global".to_string(), SurfaceColumnType::Boolean),
        ("untracked".to_string(), SurfaceColumnType::Boolean),
    ])
}

fn state_history_columns() -> Vec<String> {
    [
        "entity_id",
        "schema_key",
        "file_id",
        "plugin_key",
        "snapshot_content",
        "metadata",
        "schema_version",
        "change_id",
        "commit_id",
        "commit_created_at",
        "root_commit_id",
        "depth",
        "version_id",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn change_columns() -> Vec<String> {
    [
        "id",
        "entity_id",
        "schema_key",
        "schema_version",
        "file_id",
        "plugin_key",
        "metadata",
        "created_at",
        "snapshot_content",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn change_column_types() -> BTreeMap<String, SurfaceColumnType> {
    BTreeMap::new()
}

fn working_changes_columns() -> Vec<String> {
    [
        "entity_id",
        "schema_key",
        "file_id",
        "lixcol_global",
        "before_change_id",
        "after_change_id",
        "before_commit_id",
        "after_commit_id",
        "status",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn working_changes_column_types() -> BTreeMap<String, SurfaceColumnType> {
    BTreeMap::from([("lixcol_global".to_string(), SurfaceColumnType::Boolean)])
}

fn filesystem_file_columns() -> Vec<String> {
    [
        "id",
        "directory_id",
        "name",
        "extension",
        "path",
        "data",
        "metadata",
        "hidden",
        "lixcol_entity_id",
        "lixcol_schema_key",
        "lixcol_file_id",
        "lixcol_plugin_key",
        "lixcol_schema_version",
        "lixcol_global",
        "lixcol_change_id",
        "lixcol_created_at",
        "lixcol_updated_at",
        "lixcol_commit_id",
        "lixcol_writer_key",
        "lixcol_untracked",
        "lixcol_metadata",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn filesystem_file_by_version_columns() -> Vec<String> {
    let mut columns = filesystem_file_columns();
    columns.insert(11, "lixcol_version_id".to_string());
    columns
}

fn filesystem_file_history_columns() -> Vec<String> {
    [
        "id",
        "path",
        "data",
        "metadata",
        "hidden",
        "lixcol_entity_id",
        "lixcol_schema_key",
        "lixcol_file_id",
        "lixcol_version_id",
        "lixcol_plugin_key",
        "lixcol_schema_version",
        "lixcol_change_id",
        "lixcol_metadata",
        "lixcol_commit_id",
        "lixcol_commit_created_at",
        "lixcol_root_commit_id",
        "lixcol_depth",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn filesystem_directory_columns() -> Vec<String> {
    [
        "id",
        "parent_id",
        "name",
        "path",
        "hidden",
        "lixcol_entity_id",
        "lixcol_schema_key",
        "lixcol_schema_version",
        "lixcol_global",
        "lixcol_change_id",
        "lixcol_created_at",
        "lixcol_updated_at",
        "lixcol_commit_id",
        "lixcol_untracked",
        "lixcol_metadata",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn filesystem_directory_by_version_columns() -> Vec<String> {
    let mut columns = filesystem_directory_columns();
    columns.insert(8, "lixcol_version_id".to_string());
    columns
}

fn filesystem_directory_history_columns() -> Vec<String> {
    [
        "id",
        "parent_id",
        "name",
        "path",
        "hidden",
        "lixcol_entity_id",
        "lixcol_schema_key",
        "lixcol_file_id",
        "lixcol_version_id",
        "lixcol_plugin_key",
        "lixcol_schema_version",
        "lixcol_change_id",
        "lixcol_metadata",
        "lixcol_commit_id",
        "lixcol_commit_created_at",
        "lixcol_root_commit_id",
        "lixcol_depth",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn filesystem_column_types(name: &str) -> BTreeMap<String, SurfaceColumnType> {
    match name {
        "lix_file" | "lix_file_by_version" | "lix_file_history" | "lix_file_history_by_version" => {
            BTreeMap::from([
                ("hidden".to_string(), SurfaceColumnType::Boolean),
                ("lixcol_global".to_string(), SurfaceColumnType::Boolean),
                ("lixcol_untracked".to_string(), SurfaceColumnType::Boolean),
            ])
        }
        "lix_directory" | "lix_directory_by_version" | "lix_directory_history" => BTreeMap::from([
            ("hidden".to_string(), SurfaceColumnType::Boolean),
            ("lixcol_global".to_string(), SurfaceColumnType::Boolean),
            ("lixcol_untracked".to_string(), SurfaceColumnType::Boolean),
        ]),
        _ => BTreeMap::new(),
    }
}

fn admin_columns(name: &str) -> Vec<String> {
    match name {
        "lix_version" => vec![
            "id".to_string(),
            "name".to_string(),
            "hidden".to_string(),
            "commit_id".to_string(),
        ],
        _ => vec!["id".to_string()],
    }
}

fn admin_column_types(name: &str) -> BTreeMap<String, SurfaceColumnType> {
    match name {
        "lix_version" => BTreeMap::from([("hidden".to_string(), SurfaceColumnType::Boolean)]),
        _ => BTreeMap::new(),
    }
}

fn entity_visible_columns(
    visible_columns: &[String],
    include_version_id: bool,
    include_history_columns: bool,
) -> Vec<String> {
    let mut columns = visible_columns.to_vec();
    if include_version_id {
        columns.push("lixcol_version_id".to_string());
    }
    if include_history_columns {
        columns.extend([
            "lixcol_change_id".to_string(),
            "lixcol_commit_id".to_string(),
            "lixcol_root_commit_id".to_string(),
            "lixcol_depth".to_string(),
        ]);
    }
    columns
}

fn entity_hidden_columns() -> Vec<String> {
    [
        "lixcol_entity_id",
        "lixcol_schema_key",
        "lixcol_file_id",
        "lixcol_plugin_key",
        "lixcol_schema_version",
        "lixcol_global",
        "lixcol_writer_key",
        "lixcol_untracked",
        "lixcol_metadata",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn entity_column_types(
    property_column_types: &BTreeMap<String, SurfaceColumnType>,
) -> BTreeMap<String, SurfaceColumnType> {
    let mut column_types = property_column_types.clone();
    column_types.insert("lixcol_global".to_string(), SurfaceColumnType::Boolean);
    column_types.insert("lixcol_untracked".to_string(), SurfaceColumnType::Boolean);
    column_types
}

fn surface_column_type_from_schema(schema: &JsonValue) -> Option<SurfaceColumnType> {
    let types = match schema.get("type") {
        Some(JsonValue::String(kind)) => vec![kind.as_str()],
        Some(JsonValue::Array(kinds)) => kinds
            .iter()
            .filter_map(JsonValue::as_str)
            .collect::<Vec<_>>(),
        _ => return None,
    };

    if types.iter().any(|kind| *kind == "boolean") {
        return Some(SurfaceColumnType::Boolean);
    }
    if types.iter().any(|kind| *kind == "integer") {
        return Some(SurfaceColumnType::Integer);
    }
    if types.iter().any(|kind| *kind == "number") {
        return Some(SurfaceColumnType::Number);
    }
    if types.iter().any(|kind| *kind == "string") {
        return Some(SurfaceColumnType::String);
    }
    if types.iter().any(|kind| matches!(*kind, "object" | "array")) {
        return Some(SurfaceColumnType::Json);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{
        builtin_public_surface_columns, builtin_public_surface_names,
        entity_surface_spec_from_schema, CatalogEpoch, DefaultScopeSemantics,
        DynamicEntitySurfaceSpec, SurfaceCapability, SurfaceFamily, SurfaceOverrideValue,
        SurfaceRegistry, SurfaceVariant,
    };
    use crate::{LixBackend, LixError, QueryResult, SqlDialect, Value};
    use async_trait::async_trait;
    use serde_json::json;
    use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};
    use std::collections::{BTreeMap, HashMap, HashSet};

    #[test]
    fn binds_builtin_state_surfaces() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let binding = registry
            .bind_relation_name("lix_state_by_version")
            .expect("builtin surface should bind");

        assert_eq!(binding.descriptor.surface_family, SurfaceFamily::State);
        assert_eq!(
            binding.descriptor.surface_variant,
            SurfaceVariant::ByVersion
        );
        assert_eq!(
            binding.default_scope,
            DefaultScopeSemantics::ExplicitVersion
        );
        assert!(binding.implicit_overrides.expose_version_id);
    }

    #[test]
    fn dynamic_entity_registration_bumps_catalog_epoch_and_tracks_binding_epoch() {
        let mut registry = SurfaceRegistry::with_builtin_surfaces();
        assert_eq!(registry.epoch(), CatalogEpoch::default());

        let epoch = registry.register_dynamic_entity_surfaces(DynamicEntitySurfaceSpec {
            schema_key: "lix_key_value".to_string(),
            visible_columns: vec!["key".to_string(), "value".to_string()],
            column_types: BTreeMap::new(),
            predicate_overrides: Vec::new(),
        });

        assert_eq!(epoch.value(), 1);
        let binding = registry
            .bind_relation_name("lix_key_value")
            .expect("dynamic surface should bind");
        assert_eq!(binding.catalog_epoch, Some(epoch));
        assert_eq!(
            binding.implicit_overrides.fixed_schema_key.as_deref(),
            Some("lix_key_value")
        );
    }

    #[test]
    fn builtin_registry_bootstraps_builtin_entity_surfaces() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let binding = registry
            .bind_relation_name("lix_key_value")
            .expect("builtin schema-derived entity surface should bind");

        assert_eq!(binding.descriptor.surface_family, SurfaceFamily::Entity);
        assert_eq!(
            binding.implicit_overrides.fixed_schema_key.as_deref(),
            Some("lix_key_value")
        );
    }

    #[test]
    fn builtin_registry_exposes_registered_schema_by_version_entity_surface() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let binding = registry
            .bind_relation_name("lix_registered_schema_by_version")
            .expect("registered schema by-version surface should bind");

        assert_eq!(binding.descriptor.surface_family, SurfaceFamily::Entity);
        assert_eq!(
            binding.descriptor.surface_variant,
            SurfaceVariant::ByVersion
        );
        assert_eq!(
            binding.implicit_overrides.fixed_schema_key.as_deref(),
            Some("lix_registered_schema")
        );
    }

    #[test]
    fn builtin_registry_exposes_registered_schema_default_entity_surface() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let binding = registry
            .bind_relation_name("lix_registered_schema")
            .expect("registered schema default surface should bind");

        assert_eq!(binding.descriptor.surface_family, SurfaceFamily::Entity);
        assert_eq!(binding.descriptor.surface_variant, SurfaceVariant::Default);
        assert_eq!(
            binding.implicit_overrides.fixed_schema_key.as_deref(),
            Some("lix_registered_schema")
        );
    }

    #[test]
    fn derived_builtin_entity_surfaces_are_read_only() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        for surface in [
            "lix_commit",
            "lix_commit_by_version",
            "lix_change_set",
            "lix_change_set_by_version",
            "lix_change_author",
            "lix_change_author_by_version",
            "lix_change_set_element",
            "lix_change_set_element_by_version",
            "lix_commit_edge",
            "lix_commit_edge_by_version",
        ] {
            let binding = registry
                .bind_relation_name(surface)
                .expect("derived builtin surface should bind");
            assert_eq!(binding.capability, SurfaceCapability::ReadOnly);
            assert!(
                !binding.resolution_capabilities.semantic_write,
                "derived builtin surface should not permit semantic writes: {surface}"
            );
        }
    }

    #[test]
    fn builtin_public_surface_names_are_unique() {
        let names = builtin_public_surface_names();
        let mut seen = HashSet::new();
        for name in names {
            assert!(
                seen.insert(name.clone()),
                "duplicate public surface: {name}"
            );
        }
    }

    #[test]
    fn filesystem_surface_columns_match_public_contracts() {
        assert_eq!(
            builtin_public_surface_columns("lix_file").expect("lix_file columns"),
            vec![
                "id",
                "directory_id",
                "name",
                "extension",
                "path",
                "data",
                "metadata",
                "hidden",
                "lixcol_entity_id",
                "lixcol_schema_key",
                "lixcol_file_id",
                "lixcol_plugin_key",
                "lixcol_schema_version",
                "lixcol_global",
                "lixcol_change_id",
                "lixcol_created_at",
                "lixcol_updated_at",
                "lixcol_commit_id",
                "lixcol_writer_key",
                "lixcol_untracked",
                "lixcol_metadata",
            ]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
        );
        assert_eq!(
            builtin_public_surface_columns("lix_file_history_by_version")
                .expect("lix_file_history_by_version columns"),
            vec![
                "id",
                "path",
                "data",
                "metadata",
                "hidden",
                "lixcol_entity_id",
                "lixcol_schema_key",
                "lixcol_file_id",
                "lixcol_version_id",
                "lixcol_plugin_key",
                "lixcol_schema_version",
                "lixcol_change_id",
                "lixcol_metadata",
                "lixcol_commit_id",
                "lixcol_commit_created_at",
                "lixcol_root_commit_id",
                "lixcol_depth",
            ]
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>()
        );
    }

    #[test]
    fn entity_surface_spec_is_derived_from_schema_properties() {
        let spec = entity_surface_spec_from_schema(&json!({
            "x-lix-key": "project_message",
            "properties": {
                "message": { "type": "string" },
                "id": { "type": "string" }
            }
        }))
        .expect("schema spec should derive");

        assert_eq!(spec.schema_key, "project_message");
        assert_eq!(
            spec.visible_columns,
            vec!["id".to_string(), "message".to_string()]
        );
    }

    #[test]
    fn entity_surface_spec_evaluates_override_metadata() {
        let spec = entity_surface_spec_from_schema(&json!({
            "x-lix-key": "message",
            "x-lix-version": "1",
            "x-lix-override-lixcols": {
                "lixcol_file_id": "\"lix\"",
                "lixcol_plugin_key": "\"lix\"",
                "lixcol_global": "true"
            },
            "properties": {
                "body": { "type": "string" },
                "id": { "type": "string" }
            }
        }))
        .expect("schema spec should derive");

        assert_eq!(spec.predicate_overrides.len(), 3);
        assert!(spec.predicate_overrides.iter().any(|predicate| {
            predicate.column == "global" && predicate.value == SurfaceOverrideValue::Boolean(true)
        }));
    }

    #[test]
    fn entity_surface_spec_rejects_removed_lixcol_version_override() {
        let err = entity_surface_spec_from_schema(&json!({
            "x-lix-key": "message",
            "x-lix-version": "1",
            "x-lix-override-lixcols": {
                "lixcol_version_id": "\"global\""
            },
            "properties": {
                "id": { "type": "string" }
            }
        }))
        .expect_err("removed lixcol_version_id override should be rejected");

        assert!(
            err.description
                .contains("x-lix-override-lixcols.lixcol_version_id"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn binds_object_names_using_last_relation_segment() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let binding = registry
            .bind_object_name(&ObjectName(vec![
                ObjectNamePart::Identifier(Ident::new("main")),
                ObjectNamePart::Identifier(Ident::new("lix_state")),
            ]))
            .expect("object name should bind");

        assert_eq!(binding.descriptor.public_name, "lix_state");
    }

    #[derive(Default)]
    struct FakeBackend {
        schema_rows: HashMap<String, String>,
    }

    #[async_trait(?Send)]
    impl LixBackend for FakeBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            if sql.contains("FROM lix_internal_registered_schema_bootstrap") {
                let rows = self
                    .schema_rows
                    .values()
                    .cloned()
                    .map(|snapshot| vec![Value::Text(snapshot)])
                    .collect::<Vec<_>>();
                return Ok(QueryResult {
                    rows,
                    columns: vec!["snapshot_content".to_string()],
                });
            }

            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(
            &self,
            _mode: crate::TransactionMode,
        ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
            Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "transactions are not needed in this test backend".to_string(),
            })
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn crate::LixBackendTransaction + '_>, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "begin_savepoint not supported in test backend",
            ))
        }
    }

    #[tokio::test]
    async fn bootstrap_with_backend_loads_dynamic_schema_surfaces() {
        let mut backend = FakeBackend::default();
        backend.schema_rows.insert(
            "message".to_string(),
            r#"{"value":{"x-lix-key":"message","x-lix-version":"1","type":"object","properties":{"id":{"type":"string"},"body":{"type":"string"}}}}"#.to_string(),
        );

        let registry = SurfaceRegistry::bootstrap_with_backend(&backend)
            .await
            .expect("registry should bootstrap");
        let binding = registry
            .bind_relation_name("message")
            .expect("dynamic registered schema surface should bind");

        assert_eq!(binding.descriptor.surface_family, SurfaceFamily::Entity);
        assert!(binding.catalog_epoch.is_some());
        assert_eq!(
            binding.exposed_columns,
            vec!["body".to_string(), "id".to_string()]
        );
    }
}
