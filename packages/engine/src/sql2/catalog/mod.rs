use crate::builtin_schema::{builtin_schema_definition, builtin_schema_keys};
use crate::cel::CelEvaluator;
use crate::schema::SqlStoredSchemaProvider;
use crate::{LixBackend, LixError};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sqlparser::ast::{ObjectName, ObjectNamePart};
use std::collections::BTreeMap;

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
    pub(crate) canonical_admin_scan: bool,
    pub(crate) canonical_change_scan: bool,
    pub(crate) entity_projection: bool,
    pub(crate) semantic_write: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct SurfaceImplicitOverrides {
    pub(crate) fixed_schema_key: Option<String>,
    pub(crate) expose_version_id: bool,
    pub(crate) fixed_version_id: Option<String>,
    pub(crate) predicate_overrides: Vec<SurfaceOverridePredicate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SurfaceOverrideValue {
    Null,
    Boolean(bool),
    Number(String),
    String(String),
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
    pub(crate) capability: SurfaceCapability,
    pub(crate) default_scope: DefaultScopeSemantics,
    pub(crate) implicit_overrides: SurfaceImplicitOverrides,
    pub(crate) resolution_capabilities: SurfaceResolutionCapabilities,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DynamicEntitySurfaceSpec {
    pub(crate) schema_key: String,
    pub(crate) visible_columns: Vec<String>,
    pub(crate) fixed_version_id: Option<String>,
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
        let mut provider = SqlStoredSchemaProvider::new(backend);
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

    pub(crate) fn register_dynamic_entity_surfaces(
        &mut self,
        spec: DynamicEntitySurfaceSpec,
    ) -> CatalogEpoch {
        if self.entity_surface_name_conflicts(&spec.schema_key) {
            return self.epoch;
        }
        self.epoch.bump();
        for descriptor in entity_descriptors_from_spec(&spec, CatalogSource::Dynamic) {
            self.insert_descriptor(descriptor);
        }
        self.epoch
    }

    fn insert_descriptor(&mut self, descriptor: SurfaceDescriptor) {
        self.descriptors
            .insert(normalize_surface_name(&descriptor.public_name), descriptor);
    }

    fn register_builtin_entity_surfaces(&mut self) {
        for schema_key in builtin_schema_keys() {
            let Some(schema) = builtin_schema_definition(schema_key) else {
                continue;
            };
            let Ok(spec) = entity_surface_spec_from_schema(schema) else {
                continue;
            };
            if self.entity_surface_name_conflicts(&spec.schema_key) {
                continue;
            }
            for descriptor in entity_descriptors_from_spec(&spec, CatalogSource::Builtin) {
                self.insert_descriptor(descriptor);
            }
        }
    }

    fn entity_surface_name_conflicts(&self, schema_key: &str) -> bool {
        [
            schema_key.to_string(),
            format!("{schema_key}_by_version"),
            format!("{schema_key}_history"),
        ]
        .into_iter()
        .any(|name| {
            self.descriptors
                .get(&normalize_surface_name(&name))
                .is_some_and(|descriptor| descriptor.surface_family != SurfaceFamily::Entity)
        })
    }
}

fn normalize_surface_name(name: &str) -> String {
    name.trim().to_ascii_lowercase()
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
            capability: SurfaceCapability::ReadOnly,
            default_scope: DefaultScopeSemantics::WorkingChanges,
            surface_traits: SurfaceTraits {
                exposes_history_columns: true,
                ..SurfaceTraits::default()
            },
            resolution_capabilities: SurfaceResolutionCapabilities::default(),
            implicit_overrides: SurfaceImplicitOverrides::default(),
            catalog_source: CatalogSource::Builtin,
        },
        filesystem_surface_descriptor("lix_file", SurfaceVariant::Default),
        filesystem_surface_descriptor("lix_file_by_version", SurfaceVariant::ByVersion),
        filesystem_surface_descriptor("lix_file_history", SurfaceVariant::History),
        admin_surface_descriptor("lix_version", SurfaceVariant::Default),
        admin_surface_descriptor("lix_active_version", SurfaceVariant::Active),
        admin_surface_descriptor("lix_stored_schema", SurfaceVariant::Default),
        admin_surface_descriptor("lix_active_account", SurfaceVariant::Active),
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

    SurfaceDescriptor {
        public_name: name.to_string(),
        surface_family: SurfaceFamily::Filesystem,
        surface_variant: variant,
        visible_columns: filesystem_columns(),
        hidden_columns: Vec::new(),
        capability,
        default_scope,
        surface_traits: SurfaceTraits {
            exposes_version_column,
            exposes_history_columns,
            ..SurfaceTraits::default()
        },
        resolution_capabilities: SurfaceResolutionCapabilities {
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
        "lix_version" | "lix_active_version" | "lix_active_account" => SurfaceCapability::ReadWrite,
        "lix_stored_schema" => SurfaceCapability::ReadOnly,
        _ => SurfaceCapability::ReadOnly,
    };

    SurfaceDescriptor {
        public_name: name.to_string(),
        surface_family: SurfaceFamily::Admin,
        surface_variant: variant,
        visible_columns: admin_columns(name),
        hidden_columns: Vec::new(),
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

    vec![
        SurfaceDescriptor {
            public_name: spec.schema_key.clone(),
            surface_family: SurfaceFamily::Entity,
            surface_variant: SurfaceVariant::Default,
            visible_columns: default_visible,
            hidden_columns: hidden_columns.clone(),
            capability: SurfaceCapability::ReadWrite,
            default_scope: DefaultScopeSemantics::ActiveVersion,
            surface_traits: SurfaceTraits {
                state_backed: true,
                schema_driven_projection: true,
                ..SurfaceTraits::default()
            },
            resolution_capabilities: SurfaceResolutionCapabilities {
                canonical_state_scan: true,
                entity_projection: true,
                semantic_write: true,
                ..SurfaceResolutionCapabilities::default()
            },
            implicit_overrides: SurfaceImplicitOverrides {
                fixed_schema_key: Some(spec.schema_key.clone()),
                expose_version_id: false,
                fixed_version_id: spec.fixed_version_id.clone(),
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
            capability: SurfaceCapability::ReadWrite,
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
                semantic_write: true,
                ..SurfaceResolutionCapabilities::default()
            },
            implicit_overrides: SurfaceImplicitOverrides {
                fixed_schema_key: Some(spec.schema_key.clone()),
                expose_version_id: true,
                fixed_version_id: spec.fixed_version_id.clone(),
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
            capability: SurfaceCapability::ReadOnly,
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
                semantic_write: false,
                ..SurfaceResolutionCapabilities::default()
            },
            implicit_overrides: SurfaceImplicitOverrides {
                fixed_schema_key: Some(spec.schema_key.clone()),
                expose_version_id: true,
                fixed_version_id: spec.fixed_version_id.clone(),
                predicate_overrides: entity_override_predicates_for_variant(
                    &spec.predicate_overrides,
                    SurfaceVariant::History,
                ),
            },
            catalog_source,
        },
    ]
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

    let evaluator = CelEvaluator::new();
    let fixed_version_id =
        extract_lixcol_string_override(schema, schema_key, "lixcol_version_id", &evaluator)?;
    let predicate_overrides = collect_override_predicates(schema, schema_key, &evaluator)?;

    Ok(DynamicEntitySurfaceSpec {
        schema_key: schema_key.to_string(),
        visible_columns,
        fixed_version_id,
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

fn raw_lixcol_override_expression<'a>(schema: &'a JsonValue, key: &str) -> Option<&'a str> {
    schema
        .get("x-lix-override-lixcols")
        .and_then(JsonValue::as_object)
        .and_then(|overrides| overrides.get(key))
        .and_then(JsonValue::as_str)
}

fn evaluate_lixcol_override(
    schema: &JsonValue,
    schema_key: &str,
    key: &str,
    evaluator: &CelEvaluator,
) -> Result<Option<JsonValue>, LixError> {
    let Some(raw_expression) = raw_lixcol_override_expression(schema, key) else {
        return Ok(None);
    };
    let expression = raw_expression.trim();
    if expression.is_empty() {
        return Ok(None);
    }
    evaluator
        .evaluate(expression, &JsonMap::new())
        .map(Some)
        .map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "invalid x-lix-override-lixcols expression for '{}.{}': {}",
                schema_key, key, error.description
            ),
        })
}

fn extract_lixcol_string_override(
    schema: &JsonValue,
    schema_key: &str,
    key: &str,
    evaluator: &CelEvaluator,
) -> Result<Option<String>, LixError> {
    let Some(value) = evaluate_lixcol_override(schema, schema_key, key, evaluator)? else {
        return Ok(None);
    };
    match value {
        JsonValue::String(text) => Ok(Some(text)),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "x-lix-override-lixcols '{}.{}' must evaluate to a string",
                schema_key, key
            ),
        }),
    }
}

fn extract_lixcol_scalar_override(
    schema: &JsonValue,
    schema_key: &str,
    key: &str,
    evaluator: &CelEvaluator,
) -> Result<Option<SurfaceOverrideValue>, LixError> {
    let Some(value) = evaluate_lixcol_override(schema, schema_key, key, evaluator)? else {
        return Ok(None);
    };
    match value {
        JsonValue::Null => Ok(Some(SurfaceOverrideValue::Null)),
        JsonValue::Bool(value) => Ok(Some(SurfaceOverrideValue::Boolean(value))),
        JsonValue::Number(value) => Ok(Some(SurfaceOverrideValue::Number(value.to_string()))),
        JsonValue::String(value) => Ok(Some(SurfaceOverrideValue::String(value))),
        JsonValue::Array(_) | JsonValue::Object(_) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "x-lix-override-lixcols '{}.{}' must evaluate to a scalar or null",
                schema_key, key
            ),
        }),
    }
}

fn collect_override_predicates(
    schema: &JsonValue,
    schema_key: &str,
    evaluator: &CelEvaluator,
) -> Result<Vec<SurfaceOverridePredicate>, LixError> {
    let mut predicates = Vec::new();
    for (override_key, column) in [
        ("lixcol_entity_id", "entity_id"),
        ("lixcol_file_id", "file_id"),
        ("lixcol_plugin_key", "plugin_key"),
        ("lixcol_global", "global"),
        ("lixcol_metadata", "metadata"),
        ("lixcol_untracked", "untracked"),
    ] {
        let Some(value) =
            extract_lixcol_scalar_override(schema, schema_key, override_key, evaluator)?
        else {
            continue;
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

fn filesystem_columns() -> Vec<String> {
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
        "lixcol_commit_id",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

fn admin_columns(name: &str) -> Vec<String> {
    match name {
        "lix_active_version" => vec!["id".to_string(), "version_id".to_string()],
        "lix_active_account" => vec!["id".to_string(), "account_id".to_string()],
        "lix_stored_schema" => vec![
            "value".to_string(),
            "lixcol_schema_key".to_string(),
            "lixcol_schema_version".to_string(),
        ],
        "lix_version" => vec![
            "id".to_string(),
            "name".to_string(),
            "hidden".to_string(),
            "commit_id".to_string(),
        ],
        _ => vec!["id".to_string()],
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

#[cfg(test)]
mod tests {
    use super::{
        entity_surface_spec_from_schema, CatalogEpoch, DefaultScopeSemantics,
        DynamicEntitySurfaceSpec, SurfaceFamily, SurfaceOverrideValue, SurfaceRegistry,
        SurfaceVariant,
    };
    use crate::{LixBackend, LixError, QueryResult, SqlDialect, Value};
    use async_trait::async_trait;
    use serde_json::json;
    use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};
    use std::collections::HashMap;

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
            fixed_version_id: None,
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
        assert_eq!(spec.fixed_version_id, None);
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

        assert_eq!(spec.fixed_version_id, None);
        assert_eq!(spec.predicate_overrides.len(), 3);
        assert!(spec.predicate_overrides.iter().any(|predicate| {
            predicate.column == "global" && predicate.value == SurfaceOverrideValue::Boolean(true)
        }));
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
            if sql.contains("FROM lix_internal_stored_schema_bootstrap") {
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

        async fn begin_transaction(&self) -> Result<Box<dyn crate::LixTransaction + '_>, LixError> {
            Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "transactions are not needed in this test backend".to_string(),
            })
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
            .expect("dynamic stored schema surface should bind");

        assert_eq!(binding.descriptor.surface_family, SurfaceFamily::Entity);
        assert!(binding.catalog_epoch.is_some());
        assert_eq!(
            binding.exposed_columns,
            vec!["body".to_string(), "id".to_string()]
        );
    }
}
