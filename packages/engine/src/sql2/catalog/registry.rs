use std::collections::BTreeMap;

use serde_json::Value as JsonValue;

use crate::LixError;

use super::{PublicColumn, PublicSurfaceContract, PublicSurfaceKind, SurfaceCapabilities};
use crate::sql2::entity_provider::{
    derive_entity_surface_spec_from_schema, schema_exposed_as_entity_history_surface,
    schema_exposed_as_entity_surface, EntityProviderVariant,
};
use crate::sql2::history_route::{
    HISTORY_COL_CHANGE_ID, HISTORY_COL_COMMIT_CREATED_AT, HISTORY_COL_DEPTH, HISTORY_COL_ENTITY_ID,
    HISTORY_COL_FILE_ID, HISTORY_COL_METADATA, HISTORY_COL_OBSERVED_COMMIT_ID,
    HISTORY_COL_SCHEMA_KEY, HISTORY_COL_SNAPSHOT_CONTENT, HISTORY_COL_START_COMMIT_ID,
};

#[derive(Clone, Debug, Default)]
pub(crate) struct PublicCatalog {
    surfaces: BTreeMap<String, PublicSurfaceContract>,
}

impl PublicCatalog {
    pub(crate) fn from_visible_schemas(schema_definitions: &[JsonValue]) -> Result<Self, LixError> {
        let mut catalog = Self::default();
        catalog.insert_system_surfaces()?;
        for schema in schema_definitions {
            catalog.insert_entity_surfaces_from_schema(schema)?;
        }
        Ok(catalog)
    }

    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    pub(crate) fn insert(&mut self, surface: PublicSurfaceContract) -> Result<(), LixError> {
        if self.surfaces.contains_key(&surface.name) {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("duplicate public SQL surface '{}'", surface.name),
            ));
        }
        self.surfaces.insert(surface.name.clone(), surface);
        Ok(())
    }

    pub(crate) fn surface(&self, table_name: &str) -> Option<&PublicSurfaceContract> {
        self.surfaces.get(table_name)
    }

    pub(crate) fn require_surface(
        &self,
        table_name: &str,
    ) -> Result<&PublicSurfaceContract, LixError> {
        self.surface(table_name).ok_or_else(|| {
            LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                format!("unknown SQL table '{table_name}'"),
            )
        })
    }

    fn insert_system_surfaces(&mut self) -> Result<(), LixError> {
        self.insert(surface(
            "lix_state",
            PublicSurfaceKind::LixState,
            lix_state_columns(false),
            SurfaceCapabilities::read_write(),
        ))?;
        self.insert(surface(
            "lix_state_by_version",
            PublicSurfaceKind::LixStateByVersion,
            lix_state_columns(true),
            SurfaceCapabilities::read_write(),
        ))?;
        self.insert(surface(
            "lix_file",
            PublicSurfaceKind::File,
            filesystem_columns(false),
            SurfaceCapabilities::read_write(),
        ))?;
        self.insert(surface(
            "lix_file_by_version",
            PublicSurfaceKind::FileByVersion,
            filesystem_columns(true),
            SurfaceCapabilities::read_write(),
        ))?;
        self.insert(surface(
            "lix_directory",
            PublicSurfaceKind::Directory,
            directory_columns(false),
            SurfaceCapabilities::read_write(),
        ))?;
        self.insert(surface(
            "lix_directory_by_version",
            PublicSurfaceKind::DirectoryByVersion,
            directory_columns(true),
            SurfaceCapabilities::read_write(),
        ))?;
        self.insert(surface(
            "lix_version",
            PublicSurfaceKind::Version,
            vec![
                PublicColumn::public_insert_only("id"),
                PublicColumn::public("name"),
                PublicColumn::public("hidden"),
                PublicColumn::public("commit_id"),
            ],
            SurfaceCapabilities::read_write(),
        ))?;
        self.insert(surface(
            "lix_change",
            PublicSurfaceKind::Change,
            public_columns([
                "id",
                "entity_id",
                "schema_key",
                "file_id",
                "metadata",
                "created_at",
                "snapshot_content",
            ]),
            SurfaceCapabilities::read_only(),
        ))?;
        self.insert(surface(
            "lix_state_history",
            PublicSurfaceKind::History,
            state_history_columns(),
            SurfaceCapabilities::read_only(),
        ))?;
        self.insert(surface(
            "lix_file_history",
            PublicSurfaceKind::FileHistory,
            file_history_columns(),
            SurfaceCapabilities::read_only(),
        ))?;
        self.insert(surface(
            "lix_directory_history",
            PublicSurfaceKind::DirectoryHistory,
            directory_history_columns(),
            SurfaceCapabilities::read_only(),
        ))?;
        Ok(())
    }

    fn insert_entity_surfaces_from_schema(&mut self, schema: &JsonValue) -> Result<(), LixError> {
        let spec = match derive_entity_surface_spec_from_schema(schema) {
            Ok(spec) => spec,
            Err(_) => return Ok(()),
        };

        if !schema_exposed_as_entity_surface(&spec.schema_key) {
            return Ok(());
        };

        let mut columns = entity_columns(&spec);
        columns.extend(entity_hidden_columns(false));

        self.insert(surface(
            &spec.schema_key,
            PublicSurfaceKind::EntityBase {
                schema_key: spec.schema_key.clone(),
            },
            columns,
            SurfaceCapabilities::read_write(),
        ))?;

        let mut by_version_columns = entity_columns(&spec);
        by_version_columns.extend(entity_hidden_columns(true));

        self.insert(surface(
            format!("{}_by_version", spec.schema_key),
            PublicSurfaceKind::EntityByVersion {
                schema_key: spec.schema_key.clone(),
            },
            by_version_columns,
            SurfaceCapabilities::read_write(),
        ))?;

        if schema_exposed_as_entity_history_surface(&spec.schema_key) {
            let mut history_columns = spec
                .columns
                .iter()
                .map(|column| PublicColumn::public(column.name.as_str()))
                .collect::<Vec<_>>();
            history_columns.extend(entity_system_columns(EntityProviderVariant::History));

            self.insert(surface(
                format!("{}_history", spec.schema_key),
                PublicSurfaceKind::EntityHistory {
                    schema_key: spec.schema_key,
                },
                history_columns,
                SurfaceCapabilities::read_only(),
            ))?;
        }

        Ok(())
    }
}

fn surface(
    name: impl Into<String>,
    kind: PublicSurfaceKind,
    columns: Vec<PublicColumn>,
    capabilities: SurfaceCapabilities,
) -> PublicSurfaceContract {
    let columns = columns
        .into_iter()
        .enumerate()
        .map(|(id, column)| column.with_id(id))
        .collect();
    PublicSurfaceContract {
        name: name.into(),
        kind,
        columns,
        capabilities,
    }
}

fn public_columns<const N: usize>(names: [&str; N]) -> Vec<PublicColumn> {
    names.into_iter().map(PublicColumn::public).collect()
}

fn entity_columns(spec: &crate::sql2::entity_provider::EntitySurfaceSpec) -> Vec<PublicColumn> {
    let primary_key_roots = spec
        .primary_key_paths
        .iter()
        .filter_map(|path| path.first())
        .collect::<std::collections::BTreeSet<_>>();
    spec.columns
        .iter()
        .map(|column| {
            if primary_key_roots.contains(&column.name) {
                PublicColumn::public_insert_only(column.name.as_str())
            } else {
                PublicColumn::public(column.name.as_str())
            }
        })
        .collect()
}

fn lix_state_columns(by_version: bool) -> Vec<PublicColumn> {
    let mut columns = vec![
        PublicColumn::public_insert_only("entity_id"),
        PublicColumn::public_insert_only("schema_key"),
        PublicColumn::public_insert_only("file_id"),
        PublicColumn::public("snapshot_content"),
        PublicColumn::public("metadata"),
        PublicColumn::public_insert_only("created_at"),
        PublicColumn::public_insert_only("updated_at"),
        PublicColumn::public_insert_only("global"),
        PublicColumn::public_insert_only("change_id"),
        PublicColumn::public_insert_only("commit_id"),
        PublicColumn::public_insert_only("untracked"),
    ];
    if by_version {
        columns.push(PublicColumn::public_insert_only("version_id"));
    }
    columns
}

fn filesystem_columns(by_version: bool) -> Vec<PublicColumn> {
    let mut columns = vec![
        PublicColumn::public_insert_only("id"),
        PublicColumn::public("path"),
        PublicColumn::public("directory_id"),
        PublicColumn::public("name"),
        PublicColumn::public("hidden"),
        PublicColumn::public("data"),
    ];
    columns.extend(filesystem_hidden_columns(by_version));
    columns
}

fn directory_columns(by_version: bool) -> Vec<PublicColumn> {
    let mut columns = vec![
        PublicColumn::public_insert_only("id"),
        PublicColumn::public_insert_only("path"),
        PublicColumn::public("parent_id"),
        PublicColumn::public("name"),
        PublicColumn::public("hidden"),
    ];
    columns.extend(filesystem_hidden_columns(by_version));
    columns
}

fn entity_hidden_columns(by_version: bool) -> Vec<PublicColumn> {
    let mut columns = entity_system_columns(if by_version {
        EntityProviderVariant::ByVersion
    } else {
        EntityProviderVariant::Active
    });
    if by_version {
        columns.push(PublicColumn::public_insert_only("lixcol_version_id"));
    }
    columns
}

fn filesystem_hidden_columns(by_version: bool) -> Vec<PublicColumn> {
    let mut columns = vec![
        PublicColumn::hidden("lixcol_entity_id"),
        PublicColumn::hidden("lixcol_schema_key"),
        PublicColumn::hidden("lixcol_file_id"),
        PublicColumn::hidden("lixcol_global"),
        PublicColumn::hidden("lixcol_change_id"),
        PublicColumn::hidden("lixcol_created_at"),
        PublicColumn::hidden("lixcol_updated_at"),
        PublicColumn::hidden("lixcol_commit_id"),
        PublicColumn::hidden("lixcol_untracked"),
        PublicColumn::hidden("lixcol_metadata"),
    ];
    if by_version {
        columns.push(PublicColumn::public_insert_only("lixcol_version_id"));
    }
    columns
}

fn entity_system_columns(variant: EntityProviderVariant) -> Vec<PublicColumn> {
    if variant == EntityProviderVariant::History {
        return public_columns([
            HISTORY_COL_ENTITY_ID,
            HISTORY_COL_SCHEMA_KEY,
            HISTORY_COL_FILE_ID,
            HISTORY_COL_SNAPSHOT_CONTENT,
            HISTORY_COL_METADATA,
            HISTORY_COL_CHANGE_ID,
            HISTORY_COL_OBSERVED_COMMIT_ID,
            HISTORY_COL_COMMIT_CREATED_AT,
            HISTORY_COL_START_COMMIT_ID,
            HISTORY_COL_DEPTH,
        ]);
    }

    vec![
        PublicColumn::hidden("lixcol_entity_id"),
        PublicColumn::hidden("lixcol_schema_key"),
        PublicColumn::hidden("lixcol_file_id"),
        PublicColumn::hidden("lixcol_snapshot_content"),
        PublicColumn::hidden("lixcol_global"),
        PublicColumn::hidden("lixcol_change_id"),
        PublicColumn::hidden("lixcol_created_at"),
        PublicColumn::hidden("lixcol_updated_at"),
        PublicColumn::hidden("lixcol_commit_id"),
        PublicColumn::hidden("lixcol_untracked"),
        PublicColumn::hidden("lixcol_metadata"),
    ]
}

fn state_history_columns() -> Vec<PublicColumn> {
    public_columns([
        "entity_id",
        "schema_key",
        "file_id",
        "snapshot_content",
        "metadata",
        "change_id",
        "observed_commit_id",
        "commit_created_at",
        "start_commit_id",
        "depth",
    ])
}

fn file_history_columns() -> Vec<PublicColumn> {
    public_columns([
        "id",
        "path",
        "directory_id",
        "name",
        "hidden",
        "data",
        HISTORY_COL_ENTITY_ID,
        HISTORY_COL_SCHEMA_KEY,
        HISTORY_COL_FILE_ID,
        HISTORY_COL_SNAPSHOT_CONTENT,
        HISTORY_COL_METADATA,
        HISTORY_COL_CHANGE_ID,
        HISTORY_COL_OBSERVED_COMMIT_ID,
        HISTORY_COL_COMMIT_CREATED_AT,
        HISTORY_COL_START_COMMIT_ID,
        HISTORY_COL_DEPTH,
    ])
}

fn directory_history_columns() -> Vec<PublicColumn> {
    public_columns([
        "id",
        "path",
        "parent_id",
        "name",
        "hidden",
        HISTORY_COL_ENTITY_ID,
        HISTORY_COL_SCHEMA_KEY,
        HISTORY_COL_FILE_ID,
        HISTORY_COL_SNAPSHOT_CONTENT,
        HISTORY_COL_METADATA,
        HISTORY_COL_CHANGE_ID,
        HISTORY_COL_OBSERVED_COMMIT_ID,
        HISTORY_COL_COMMIT_CREATED_AT,
        HISTORY_COL_START_COMMIT_ID,
        HISTORY_COL_DEPTH,
    ])
}
