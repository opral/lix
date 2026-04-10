//! Catalog-owned public relation registry contracts.

use sqlparser::ast::{ObjectName, ObjectNamePart};
use std::collections::BTreeMap;

use super::state::state_surface_descriptor;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub(crate) struct CatalogEpoch(u64);

impl CatalogEpoch {
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
    WorkingChanges,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SurfaceCapability {
    ReadOnly,
    ReadWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SurfaceReadFreshness {
    RequiresFreshProjection,
    AllowsStaleProjection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SurfaceReadSemantics {
    /// Reads committed graph/ref-selected state directly, without workspace overlays.
    CommittedGraph,
    /// Reads effective state selected by workspace version scope and may overlay
    /// workspace-owned annotation or untracked rows.
    WorkspaceEffective,
    /// Reads canonical history/change facts rather than current selected state.
    CanonicalHistory,
    /// Reads workspace-local working or pending changes rather than committed state.
    WorkspaceChanges,
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
    pub(crate) read_freshness: SurfaceReadFreshness,
    pub(crate) read_semantics: SurfaceReadSemantics,
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
    pub(crate) read_freshness: SurfaceReadFreshness,
    pub(crate) read_semantics: SurfaceReadSemantics,
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

    pub(crate) fn catalog_epoch(&self) -> CatalogEpoch {
        self.epoch
    }

    pub(crate) fn insert_descriptors(
        &mut self,
        descriptors: impl IntoIterator<Item = SurfaceDescriptor>,
    ) -> bool {
        let mut changed = false;
        for descriptor in descriptors {
            if !self.descriptor_name_available(&descriptor) {
                continue;
            }
            self.insert_descriptor(descriptor);
            changed = true;
        }
        changed
    }

    pub(crate) fn remove_descriptors_matching(
        &mut self,
        mut predicate: impl FnMut(&SurfaceDescriptor) -> bool,
    ) -> bool {
        let descriptor_names = self
            .descriptors
            .iter()
            .filter_map(|(name, descriptor)| predicate(descriptor).then_some(name.clone()))
            .collect::<Vec<_>>();
        if descriptor_names.is_empty() {
            return false;
        }

        for name in descriptor_names {
            self.descriptors.remove(&name);
        }

        true
    }

    pub(crate) fn advance_catalog_epoch(&mut self) -> CatalogEpoch {
        self.epoch.bump();
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
            read_freshness: descriptor.read_freshness,
            read_semantics: descriptor.read_semantics,
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

    fn insert_descriptor(&mut self, descriptor: SurfaceDescriptor) {
        self.descriptors
            .insert(normalize_surface_name(&descriptor.public_name), descriptor);
    }

    fn descriptor_name_available(&self, descriptor: &SurfaceDescriptor) -> bool {
        self.descriptors
            .get(&normalize_surface_name(&descriptor.public_name))
            .is_none_or(|existing| {
                descriptor.surface_family == SurfaceFamily::Entity
                    && existing.surface_family == SurfaceFamily::Entity
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

pub(crate) fn builtin_surface_descriptors() -> Vec<SurfaceDescriptor> {
    vec![
        state_surface_descriptor("lix_state", SurfaceVariant::Default),
        state_surface_descriptor("lix_state_by_version", SurfaceVariant::ByVersion),
        state_surface_descriptor("lix_state_history", SurfaceVariant::History),
        SurfaceDescriptor {
            public_name: "lix_change".to_string(),
            surface_family: SurfaceFamily::Change,
            surface_variant: SurfaceVariant::History,
            read_freshness: SurfaceReadFreshness::AllowsStaleProjection,
            read_semantics: SurfaceReadSemantics::CanonicalHistory,
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
            read_freshness: SurfaceReadFreshness::AllowsStaleProjection,
            read_semantics: SurfaceReadSemantics::WorkspaceChanges,
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
        SurfaceVariant::WorkingChanges => (
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
        read_freshness: SurfaceReadFreshness::AllowsStaleProjection,
        read_semantics: SurfaceReadSemantics::CommittedGraph,
        visible_columns: version_columns(),
        hidden_columns: Vec::new(),
        column_types: version_column_types(),
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

pub(crate) fn dynamic_entity_surface_descriptor(
    public_name: &str,
    spec: &DynamicEntitySurfaceSpec,
    variant: SurfaceVariant,
    catalog_source: CatalogSource,
) -> SurfaceDescriptor {
    let (read_freshness, read_semantics, default_scope) = match variant {
        SurfaceVariant::Default => (
            SurfaceReadFreshness::RequiresFreshProjection,
            SurfaceReadSemantics::WorkspaceEffective,
            DefaultScopeSemantics::ActiveVersion,
        ),
        SurfaceVariant::ByVersion => (
            SurfaceReadFreshness::RequiresFreshProjection,
            SurfaceReadSemantics::WorkspaceEffective,
            DefaultScopeSemantics::ExplicitVersion,
        ),
        SurfaceVariant::History => (
            SurfaceReadFreshness::AllowsStaleProjection,
            SurfaceReadSemantics::CanonicalHistory,
            DefaultScopeSemantics::History,
        ),
        SurfaceVariant::WorkingChanges => (
            SurfaceReadFreshness::AllowsStaleProjection,
            SurfaceReadSemantics::WorkspaceChanges,
            DefaultScopeSemantics::WorkingChanges,
        ),
    };
    let capability = entity_surface_capability(&spec.schema_key, variant);

    let visible_columns = entity_visible_columns(spec, variant);
    let hidden_columns = entity_hidden_columns(variant);

    SurfaceDescriptor {
        public_name: public_name.to_string(),
        surface_family: SurfaceFamily::Entity,
        surface_variant: variant,
        read_freshness,
        read_semantics,
        visible_columns,
        hidden_columns,
        column_types: entity_column_types(spec, variant),
        capability,
        default_scope,
        surface_traits: SurfaceTraits {
            state_backed: true,
            schema_driven_projection: true,
            exposes_version_column: matches!(
                variant,
                SurfaceVariant::ByVersion | SurfaceVariant::History
            ),
            exposes_history_columns: variant == SurfaceVariant::History,
        },
        resolution_capabilities: SurfaceResolutionCapabilities {
            canonical_state_scan: true,
            entity_projection: true,
            semantic_write: capability == SurfaceCapability::ReadWrite,
            ..SurfaceResolutionCapabilities::default()
        },
        implicit_overrides: SurfaceImplicitOverrides {
            fixed_schema_key: Some(spec.schema_key.clone()),
            expose_version_id: matches!(
                variant,
                SurfaceVariant::ByVersion | SurfaceVariant::History
            ),
            predicate_overrides: entity_override_predicates_for_variant(
                &spec.predicate_overrides,
                variant,
            ),
        },
        catalog_source,
    }
}

pub(crate) fn entity_surface_descriptors(
    spec: &DynamicEntitySurfaceSpec,
    catalog_source: CatalogSource,
) -> Vec<SurfaceDescriptor> {
    vec![
        dynamic_entity_surface_descriptor(
            &spec.schema_key,
            spec,
            SurfaceVariant::Default,
            catalog_source,
        ),
        dynamic_entity_surface_descriptor(
            &format!("{}_by_version", spec.schema_key),
            spec,
            SurfaceVariant::ByVersion,
            catalog_source,
        ),
        dynamic_entity_surface_descriptor(
            &format!("{}_history", spec.schema_key),
            spec,
            SurfaceVariant::History,
            catalog_source,
        ),
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

fn entity_visible_columns(spec: &DynamicEntitySurfaceSpec, variant: SurfaceVariant) -> Vec<String> {
    let mut columns = spec.visible_columns.clone();
    match variant {
        SurfaceVariant::Default => {}
        SurfaceVariant::ByVersion => columns.push("lixcol_version_id".to_string()),
        SurfaceVariant::History => columns.extend([
            "lixcol_entity_id".to_string(),
            "lixcol_schema_key".to_string(),
            "lixcol_file_id".to_string(),
            "lixcol_plugin_key".to_string(),
            "lixcol_schema_version".to_string(),
            "lixcol_version_id".to_string(),
            "lixcol_change_id".to_string(),
            "lixcol_commit_id".to_string(),
            "lixcol_commit_created_at".to_string(),
            "lixcol_root_commit_id".to_string(),
            "lixcol_depth".to_string(),
            "lixcol_metadata".to_string(),
            "lixcol_global".to_string(),
            "lixcol_untracked".to_string(),
            "lixcol_writer_key".to_string(),
        ]),
        SurfaceVariant::WorkingChanges => {}
    }
    columns
}

fn entity_hidden_columns(variant: SurfaceVariant) -> Vec<String> {
    match variant {
        SurfaceVariant::Default => vec![
            "lixcol_entity_id".to_string(),
            "lixcol_schema_key".to_string(),
            "lixcol_file_id".to_string(),
            "lixcol_plugin_key".to_string(),
            "lixcol_schema_version".to_string(),
            "lixcol_version_id".to_string(),
            "lixcol_metadata".to_string(),
            "lixcol_global".to_string(),
            "lixcol_untracked".to_string(),
            "lixcol_writer_key".to_string(),
        ],
        SurfaceVariant::ByVersion => vec![
            "lixcol_entity_id".to_string(),
            "lixcol_schema_key".to_string(),
            "lixcol_file_id".to_string(),
            "lixcol_plugin_key".to_string(),
            "lixcol_schema_version".to_string(),
            "lixcol_metadata".to_string(),
            "lixcol_global".to_string(),
            "lixcol_untracked".to_string(),
            "lixcol_writer_key".to_string(),
        ],
        SurfaceVariant::History => Vec::new(),
        SurfaceVariant::WorkingChanges => Vec::new(),
    }
}

fn entity_column_types(
    spec: &DynamicEntitySurfaceSpec,
    variant: SurfaceVariant,
) -> BTreeMap<String, SurfaceColumnType> {
    let mut column_types = spec.column_types.clone();
    column_types.extend(BTreeMap::from([
        ("lixcol_entity_id".to_string(), SurfaceColumnType::String),
        ("lixcol_schema_key".to_string(), SurfaceColumnType::String),
        ("lixcol_file_id".to_string(), SurfaceColumnType::String),
        ("lixcol_plugin_key".to_string(), SurfaceColumnType::String),
        (
            "lixcol_schema_version".to_string(),
            SurfaceColumnType::String,
        ),
        ("lixcol_version_id".to_string(), SurfaceColumnType::String),
        ("lixcol_metadata".to_string(), SurfaceColumnType::Json),
        ("lixcol_global".to_string(), SurfaceColumnType::Boolean),
        ("lixcol_untracked".to_string(), SurfaceColumnType::Boolean),
        ("lixcol_writer_key".to_string(), SurfaceColumnType::String),
    ]));

    if variant == SurfaceVariant::History {
        column_types.extend(BTreeMap::from([
            ("lixcol_change_id".to_string(), SurfaceColumnType::String),
            ("lixcol_commit_id".to_string(), SurfaceColumnType::String),
            (
                "lixcol_commit_created_at".to_string(),
                SurfaceColumnType::String,
            ),
            (
                "lixcol_root_commit_id".to_string(),
                SurfaceColumnType::String,
            ),
            ("lixcol_depth".to_string(), SurfaceColumnType::Integer),
        ]));
    }

    column_types
}

fn change_columns() -> Vec<String> {
    vec![
        "id".to_string(),
        "entity_id".to_string(),
        "schema_key".to_string(),
        "schema_version".to_string(),
        "file_id".to_string(),
        "plugin_key".to_string(),
        "metadata".to_string(),
        "created_at".to_string(),
        "snapshot_content".to_string(),
    ]
}

fn change_column_types() -> BTreeMap<String, SurfaceColumnType> {
    BTreeMap::new()
}

fn working_changes_columns() -> Vec<String> {
    vec![
        "entity_id".to_string(),
        "schema_key".to_string(),
        "schema_version".to_string(),
        "file_id".to_string(),
        "plugin_key".to_string(),
        "snapshot_content".to_string(),
        "change_id".to_string(),
        "is_deleted".to_string(),
    ]
}

fn working_changes_column_types() -> BTreeMap<String, SurfaceColumnType> {
    BTreeMap::from([
        ("entity_id".to_string(), SurfaceColumnType::String),
        ("schema_key".to_string(), SurfaceColumnType::String),
        ("schema_version".to_string(), SurfaceColumnType::String),
        ("file_id".to_string(), SurfaceColumnType::String),
        ("plugin_key".to_string(), SurfaceColumnType::String),
        ("snapshot_content".to_string(), SurfaceColumnType::Json),
        ("change_id".to_string(), SurfaceColumnType::String),
        ("is_deleted".to_string(), SurfaceColumnType::Boolean),
    ])
}

fn filesystem_file_columns() -> Vec<String> {
    vec![
        "id".to_string(),
        "directory_id".to_string(),
        "name".to_string(),
        "extension".to_string(),
        "path".to_string(),
        "data".to_string(),
        "metadata".to_string(),
        "hidden".to_string(),
        "lixcol_entity_id".to_string(),
        "lixcol_schema_key".to_string(),
        "lixcol_file_id".to_string(),
        "lixcol_plugin_key".to_string(),
        "lixcol_schema_version".to_string(),
        "lixcol_global".to_string(),
        "lixcol_change_id".to_string(),
        "lixcol_created_at".to_string(),
        "lixcol_updated_at".to_string(),
        "lixcol_commit_id".to_string(),
        "lixcol_writer_key".to_string(),
        "lixcol_untracked".to_string(),
        "lixcol_metadata".to_string(),
    ]
}

fn filesystem_file_by_version_columns() -> Vec<String> {
    let mut columns = filesystem_file_columns();
    columns.insert(11, "lixcol_version_id".to_string());
    columns
}

fn filesystem_file_history_columns() -> Vec<String> {
    vec![
        "id".to_string(),
        "path".to_string(),
        "data".to_string(),
        "metadata".to_string(),
        "hidden".to_string(),
        "lixcol_entity_id".to_string(),
        "lixcol_schema_key".to_string(),
        "lixcol_file_id".to_string(),
        "lixcol_version_id".to_string(),
        "lixcol_plugin_key".to_string(),
        "lixcol_schema_version".to_string(),
        "lixcol_change_id".to_string(),
        "lixcol_metadata".to_string(),
        "lixcol_commit_id".to_string(),
        "lixcol_commit_created_at".to_string(),
        "lixcol_root_commit_id".to_string(),
        "lixcol_depth".to_string(),
    ]
}

fn filesystem_directory_columns() -> Vec<String> {
    vec![
        "id".to_string(),
        "parent_id".to_string(),
        "name".to_string(),
        "path".to_string(),
        "hidden".to_string(),
        "lixcol_entity_id".to_string(),
        "lixcol_schema_key".to_string(),
        "lixcol_schema_version".to_string(),
        "lixcol_global".to_string(),
        "lixcol_change_id".to_string(),
        "lixcol_created_at".to_string(),
        "lixcol_updated_at".to_string(),
        "lixcol_commit_id".to_string(),
        "lixcol_untracked".to_string(),
        "lixcol_metadata".to_string(),
    ]
}

fn filesystem_directory_by_version_columns() -> Vec<String> {
    let mut columns = filesystem_directory_columns();
    columns.insert(8, "lixcol_version_id".to_string());
    columns
}

fn filesystem_directory_history_columns() -> Vec<String> {
    vec![
        "id".to_string(),
        "parent_id".to_string(),
        "name".to_string(),
        "path".to_string(),
        "hidden".to_string(),
        "lixcol_entity_id".to_string(),
        "lixcol_schema_key".to_string(),
        "lixcol_file_id".to_string(),
        "lixcol_version_id".to_string(),
        "lixcol_plugin_key".to_string(),
        "lixcol_schema_version".to_string(),
        "lixcol_change_id".to_string(),
        "lixcol_metadata".to_string(),
        "lixcol_commit_id".to_string(),
        "lixcol_commit_created_at".to_string(),
        "lixcol_root_commit_id".to_string(),
        "lixcol_depth".to_string(),
    ]
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

fn version_columns() -> Vec<String> {
    vec![
        "id".to_string(),
        "name".to_string(),
        "hidden".to_string(),
        "commit_id".to_string(),
    ]
}

fn version_column_types() -> BTreeMap<String, SurfaceColumnType> {
    BTreeMap::from([
        ("id".to_string(), SurfaceColumnType::String),
        ("name".to_string(), SurfaceColumnType::String),
        ("hidden".to_string(), SurfaceColumnType::Boolean),
        ("commit_id".to_string(), SurfaceColumnType::String),
    ])
}
