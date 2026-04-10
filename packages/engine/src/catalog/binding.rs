#![allow(dead_code)]

use std::collections::BTreeMap;

use crate::catalog::{
    DefaultScopeSemantics, SurfaceBinding, SurfaceColumnType, SurfaceFamily,
    SurfaceOverridePredicate, SurfaceRegistry, SurfaceVariant,
};
use crate::contracts::version_artifacts::{
    version_descriptor_file_id, version_descriptor_plugin_key, version_descriptor_schema_key,
    version_descriptor_schema_version, version_ref_file_id, version_ref_plugin_key,
    version_ref_schema_key, version_ref_schema_version, version_ref_storage_version_id,
};
use crate::contracts::GLOBAL_VERSION_ID;
use crate::{LixError, VersionId};

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilesystemProjectionScope {
    ActiveVersion,
    ExplicitVersion,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RelationBinding {
    SchemaRelation(SchemaRelationBinding),
    VersionRelation(VersionRelationBinding),
    FilesystemRelation(FilesystemRelationBinding),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaRelationBinding {
    pub(crate) public_name: String,
    pub(crate) schema_key: String,
    pub(crate) surface_family: SurfaceFamily,
    pub(crate) surface_variant: SurfaceVariant,
    pub(crate) default_scope: DefaultScopeSemantics,
    pub(crate) expose_version_id: bool,
    pub(crate) visible_columns: Vec<String>,
    pub(crate) column_types: BTreeMap<String, SurfaceColumnType>,
    pub(crate) predicate_overrides: Vec<SurfaceOverridePredicate>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionRelationBinding {
    pub(crate) global_version_id: String,
    pub(crate) descriptor_source: VersionDescriptorSourceBinding,
    pub(crate) head_source: VersionHeadSourceBinding,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionDescriptorSourceBinding {
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: String,
    pub(crate) plugin_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredVersionHeadSourceBinding {
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: String,
    pub(crate) plugin_key: String,
    pub(crate) storage_version_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VersionHeadSourceBinding {
    StoredRefs(StoredVersionHeadSourceBinding),
    InlineCurrentHeads(BTreeMap<String, String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilesystemRelationKind {
    File,
    Directory,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilesystemRelationBinding {
    pub(crate) kind: FilesystemRelationKind,
    pub(crate) scope: FilesystemProjectionScope,
    pub(crate) active_version_id: Option<VersionId>,
    pub(crate) global_version_id: String,
    pub(crate) version_descriptor_schema_key: String,
    pub(crate) version_ref_schema_key: String,
    pub(crate) file_descriptor_schema_key: String,
    pub(crate) directory_descriptor_schema_key: String,
    pub(crate) binary_blob_ref_schema_key: String,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RelationBindContext<'a> {
    pub(crate) active_version_id: Option<&'a str>,
    pub(crate) current_heads: Option<&'a BTreeMap<String, String>>,
}

pub(crate) fn bind_schema_relation(surface_binding: &SurfaceBinding) -> Option<RelationBinding> {
    let schema_key = surface_binding
        .implicit_overrides
        .fixed_schema_key
        .clone()?;
    Some(RelationBinding::SchemaRelation(SchemaRelationBinding {
        public_name: surface_binding.descriptor.public_name.clone(),
        schema_key,
        surface_family: surface_binding.descriptor.surface_family,
        surface_variant: surface_binding.descriptor.surface_variant,
        default_scope: surface_binding.default_scope,
        expose_version_id: surface_binding.implicit_overrides.expose_version_id,
        visible_columns: surface_binding.exposed_columns.clone(),
        column_types: surface_binding.column_types.clone(),
        predicate_overrides: surface_binding
            .implicit_overrides
            .predicate_overrides
            .clone(),
    }))
}

pub(crate) fn bind_version_relation(
    current_heads: Option<&BTreeMap<String, String>>,
) -> RelationBinding {
    RelationBinding::VersionRelation(VersionRelationBinding {
        global_version_id: GLOBAL_VERSION_ID.to_string(),
        descriptor_source: VersionDescriptorSourceBinding {
            schema_key: version_descriptor_schema_key().to_string(),
            schema_version: version_descriptor_schema_version().to_string(),
            file_id: version_descriptor_file_id().to_string(),
            plugin_key: version_descriptor_plugin_key().to_string(),
        },
        head_source: match current_heads {
            Some(current_heads) => {
                VersionHeadSourceBinding::InlineCurrentHeads(current_heads.clone())
            }
            None => VersionHeadSourceBinding::StoredRefs(StoredVersionHeadSourceBinding {
                schema_key: version_ref_schema_key().to_string(),
                schema_version: version_ref_schema_version().to_string(),
                file_id: version_ref_file_id().to_string(),
                plugin_key: version_ref_plugin_key().to_string(),
                storage_version_id: version_ref_storage_version_id().to_string(),
            }),
        },
    })
}

pub(crate) fn bind_filesystem_relation(
    kind: FilesystemRelationKind,
    scope: FilesystemProjectionScope,
    active_version_id: Option<&str>,
) -> Result<RelationBinding, LixError> {
    let active_version_id = active_version_id.map(VersionId::new).transpose()?;
    Ok(RelationBinding::FilesystemRelation(
        FilesystemRelationBinding {
            kind,
            scope,
            active_version_id,
            global_version_id: GLOBAL_VERSION_ID.to_string(),
            version_descriptor_schema_key: version_descriptor_schema_key().to_string(),
            version_ref_schema_key: version_ref_schema_key().to_string(),
            file_descriptor_schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
            directory_descriptor_schema_key: DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
            binary_blob_ref_schema_key: BINARY_BLOB_REF_SCHEMA_KEY.to_string(),
        },
    ))
}

pub(crate) fn bind_named_relation(
    relation_name: &str,
    context: RelationBindContext<'_>,
) -> Result<Option<RelationBinding>, LixError> {
    let binding = match relation_name {
        "lix_version" => Some(bind_version_relation(context.current_heads)),
        "lix_file" => Some(bind_filesystem_relation(
            FilesystemRelationKind::File,
            FilesystemProjectionScope::ActiveVersion,
            context.active_version_id,
        )?),
        "lix_file_by_version" => Some(bind_filesystem_relation(
            FilesystemRelationKind::File,
            FilesystemProjectionScope::ExplicitVersion,
            None,
        )?),
        "lix_directory" => Some(bind_filesystem_relation(
            FilesystemRelationKind::Directory,
            FilesystemProjectionScope::ActiveVersion,
            context.active_version_id,
        )?),
        "lix_directory_by_version" => Some(bind_filesystem_relation(
            FilesystemRelationKind::Directory,
            FilesystemProjectionScope::ExplicitVersion,
            None,
        )?),
        _ => None,
    };

    Ok(binding)
}

pub(crate) fn bind_surface_relation(
    surface_binding: &SurfaceBinding,
    context: RelationBindContext<'_>,
) -> Result<Option<RelationBinding>, LixError> {
    if let Some(binding) = bind_named_relation(
        &surface_binding.descriptor.public_name,
        RelationBindContext {
            active_version_id: context.active_version_id,
            current_heads: context.current_heads,
        },
    )? {
        return Ok(Some(binding));
    }

    Ok(bind_schema_relation(surface_binding))
}

pub(crate) fn bind_registry_relation(
    registry: &SurfaceRegistry,
    relation_name: &str,
    context: RelationBindContext<'_>,
) -> Result<Option<RelationBinding>, LixError> {
    let Some(surface_binding) = registry.bind_relation_name(relation_name) else {
        return Ok(None);
    };
    bind_surface_relation(&surface_binding, context)
}
