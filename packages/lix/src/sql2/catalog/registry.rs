use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::OnceLock;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use serde_json::Value as JsonValue;

use crate::LixError;

use super::{PublicColumn, PublicSurfaceContract, PublicSurfaceKind, SurfaceCapabilities};
use crate::sql2::catalog::schema_surface_schema;
use crate::sql2::catalog::{
    SchemaSurfaceShape, SchemaSurfaceSpec, derive_schema_surface_spec_from_schema,
    schema_exposed_as_history_surface, schema_exposed_as_schema_surface,
};
use crate::sql2::history_route::{
    HISTORY_COL_AS_OF_COMMIT_ID, HISTORY_COL_CHANGE_CREATED_AT, HISTORY_COL_CHANGE_ID,
    HISTORY_COL_COMMIT_CREATED_AT, HISTORY_COL_DEPTH, HISTORY_COL_ENTITY_PK, HISTORY_COL_FILE_ID,
    HISTORY_COL_IS_DELETED, HISTORY_COL_METADATA, HISTORY_COL_OBSERVED_COMMIT_ID,
    HISTORY_COL_ORIGIN_KEY, HISTORY_COL_SCHEMA_KEY, HISTORY_COL_SOURCE_CHANGES,
};
#[cfg(test)]
use crate::sql2::providers::filesystem_working_diff_schema;
use crate::sql2::result_metadata::json_field;

#[derive(Clone, Debug, Default)]
pub(crate) struct PublicCatalog {
    surfaces: BTreeMap<String, PublicSurfaceContract>,
    schema_specs: BTreeMap<String, SchemaSurfaceSpec>,
}

impl PublicCatalog {
    pub(crate) fn from_visible_schemas(schema_definitions: &[JsonValue]) -> Result<Self, LixError> {
        let mut catalog = Self::default();
        catalog.insert_system_surfaces()?;
        for schema in schema_definitions {
            catalog.insert_schema_surfaces_from_schema(schema)?;
        }
        Ok(catalog)
    }

    /// Compile-time SQL surfaces whose shape cannot be changed at runtime.
    ///
    /// Alongside the hand-written filesystem surfaces, Lix seeds a fixed set
    /// of system row schemas. Public runtime registration reserves the
    /// complete `lix_*` namespace, so only trusted bootstrap schemas can add
    /// Lix-owned surfaces to this catalog.
    pub(crate) fn fixed_system() -> &'static Self {
        Self::fixed_system_shared()
    }

    /// The same immutable catalog behind an `Arc` so per-statement provider
    /// registration can share it instead of deep-copying two `BTreeMap`s.
    pub(crate) fn fixed_system_shared() -> &'static Arc<Self> {
        static FIXED_SYSTEM_CATALOG: OnceLock<Arc<PublicCatalog>> = OnceLock::new();
        FIXED_SYSTEM_CATALOG.get_or_init(|| {
            let schemas = crate::schema::seed_schema_definitions()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            Arc::new(
                Self::from_visible_schemas(&schemas)
                    .expect("compile-time Lix schemas must form a valid SQL catalog"),
            )
        })
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

    /// Whether a runtime schema key uses the SQL-normalized namespace owned by
    /// Lix. Schema definitions are lowercase snake_case today, but normalize
    /// here as well so this policy cannot diverge from unquoted SQL names.
    pub(crate) fn runtime_schema_key_uses_reserved_namespace(schema_key: &str) -> bool {
        let schema_key = schema_key.to_ascii_lowercase();
        schema_key == "lix" || schema_key.starts_with("lix_")
    }

    pub(crate) fn surfaces(&self) -> impl Iterator<Item = &PublicSurfaceContract> {
        self.surfaces.values()
    }

    pub(crate) fn schema_spec(&self, schema_key: &str) -> Option<&SchemaSurfaceSpec> {
        self.schema_specs.get(schema_key)
    }

    #[cfg(test)]
    pub(crate) fn surface_schema(&self, table_name: &str) -> Option<SchemaRef> {
        let surface = self.surface(table_name)?;
        Some(match &surface.kind {
            PublicSurfaceKind::File => filesystem_schema(false, true),
            PublicSurfaceKind::FileByBranch => filesystem_schema(true, true),
            PublicSurfaceKind::Directory => filesystem_schema(false, false),
            PublicSurfaceKind::DirectoryByBranch => filesystem_schema(true, false),
            PublicSurfaceKind::Branch => Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("hidden", DataType::Boolean, false),
                Field::new("commit_id", DataType::Utf8, false),
            ])),
            PublicSurfaceKind::WorkingDiff => working_diff_schema(false),
            PublicSurfaceKind::WorkingDiffByBranch => working_diff_schema(true),
            PublicSurfaceKind::Revert
            | PublicSurfaceKind::Apply
            | PublicSurfaceKind::CreateCheckpoint => Arc::new(Schema::new(vec![Field::new(
                "diff_id",
                DataType::Utf8,
                false,
            )])),
            PublicSurfaceKind::FileWorkingDiff | PublicSurfaceKind::DirectoryWorkingDiff => {
                filesystem_working_diff_schema(false)
            }
            PublicSurfaceKind::FileWorkingDiffByBranch
            | PublicSurfaceKind::DirectoryWorkingDiffByBranch => {
                filesystem_working_diff_schema(true)
            }
            PublicSurfaceKind::Change => Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("account_id", DataType::Utf8, false),
                json_field("row_pk", false),
                Field::new("schema_key", DataType::Utf8, false),
                Field::new("file_id", DataType::Utf8, true),
                json_field("metadata", true),
                Field::new("created_at", DataType::Utf8, false),
                Field::new("origin_key", DataType::Utf8, true),
                json_field("snapshot_content", true),
            ])),
            PublicSurfaceKind::FileHistory => history_filesystem_schema(true),
            PublicSurfaceKind::DirectoryHistory => history_filesystem_schema(false),
            PublicSurfaceKind::SchemaBase { schema_key } => {
                schema_surface_schema(self.schema_spec(schema_key)?, SchemaSurfaceShape::Active)
            }
            PublicSurfaceKind::SchemaByBranch { schema_key } => {
                schema_surface_schema(self.schema_spec(schema_key)?, SchemaSurfaceShape::ByBranch)
            }
            PublicSurfaceKind::SchemaHistory { schema_key } => {
                schema_surface_schema(self.schema_spec(schema_key)?, SchemaSurfaceShape::History)
            }
        })
    }

    pub(crate) fn history_surface_schema(&self, table_name: &str) -> Option<SchemaRef> {
        let surface = self.surface(table_name)?;
        match &surface.kind {
            PublicSurfaceKind::FileHistory => Some(history_filesystem_schema(true)),
            PublicSurfaceKind::DirectoryHistory => Some(history_filesystem_schema(false)),
            PublicSurfaceKind::SchemaHistory { schema_key } => Some(schema_surface_schema(
                self.schema_spec(schema_key)?,
                SchemaSurfaceShape::History,
            )),
            _ => None,
        }
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
            "lix_file",
            PublicSurfaceKind::File,
            filesystem_columns(false),
            SurfaceCapabilities::read_write(),
        ))?;
        self.insert(surface(
            "lix_file_by_branch",
            PublicSurfaceKind::FileByBranch,
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
            "lix_directory_by_branch",
            PublicSurfaceKind::DirectoryByBranch,
            directory_columns(true),
            SurfaceCapabilities::read_write(),
        ))?;
        self.insert(surface(
            "lix_branch",
            PublicSurfaceKind::Branch,
            vec![
                PublicColumn::public_insert_only("id", false),
                PublicColumn::public("name", false),
                PublicColumn::public("hidden", false).with_default("FALSE"),
                PublicColumn::public("commit_id", false)
                    .with_default("lix_active_branch_commit_id()"),
            ],
            SurfaceCapabilities::read_write(),
        ))?;
        self.insert(surface(
            "lix_change",
            PublicSurfaceKind::Change,
            public_columns([
                ("id", false),
                ("account_id", false),
                ("row_pk", false),
                ("schema_key", false),
                ("file_id", true),
                ("metadata", true),
                ("created_at", false),
                ("origin_key", true),
                ("snapshot_content", true),
            ]),
            SurfaceCapabilities::read_only(),
        ))?;
        self.insert(surface(
            "lix_working_diff",
            PublicSurfaceKind::WorkingDiff,
            public_columns([
                ("diff_id", false),
                ("row_pk", false),
                ("schema_key", false),
                ("file_id", true),
                ("diff_type", false),
                ("before_change_id", true),
                ("after_change_id", true),
            ]),
            SurfaceCapabilities::read_only(),
        ))?;
        self.insert(surface(
            "lix_working_diff_by_branch",
            PublicSurfaceKind::WorkingDiffByBranch,
            public_columns([
                ("diff_id", false),
                ("row_pk", false),
                ("schema_key", false),
                ("file_id", true),
                ("diff_type", false),
                ("before_change_id", true),
                ("after_change_id", true),
                ("lixcol_branch_id", false),
            ]),
            SurfaceCapabilities::read_only(),
        ))?;
        for (name, kind) in [
            ("lix_revert", PublicSurfaceKind::Revert),
            ("lix_apply", PublicSurfaceKind::Apply),
            ("lix_create_checkpoint", PublicSurfaceKind::CreateCheckpoint),
        ] {
            self.insert(surface(
                name,
                kind,
                vec![
                    PublicColumn::public_insert_only("diff_id", false),
                    PublicColumn::public_read_only("commit_id", false),
                ],
                SurfaceCapabilities {
                    insert: true,
                    update: false,
                    delete: false,
                },
            ))?;
        }
        for (name, kind, by_branch) in [
            (
                "lix_file_working_diff",
                PublicSurfaceKind::FileWorkingDiff,
                false,
            ),
            (
                "lix_file_working_diff_by_branch",
                PublicSurfaceKind::FileWorkingDiffByBranch,
                true,
            ),
            (
                "lix_directory_working_diff",
                PublicSurfaceKind::DirectoryWorkingDiff,
                false,
            ),
            (
                "lix_directory_working_diff_by_branch",
                PublicSurfaceKind::DirectoryWorkingDiffByBranch,
                true,
            ),
        ] {
            let mut columns = vec![
                ("id", false),
                ("path", true),
                ("previous_path", true),
                ("change_kind", false),
            ];
            if by_branch {
                columns.push(("lixcol_branch_id", false));
            }
            self.insert(surface(
                name,
                kind,
                columns
                    .into_iter()
                    .map(|(name, nullable)| PublicColumn::public(name, nullable))
                    .collect(),
                SurfaceCapabilities::read_only(),
            ))?;
        }
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

    fn insert_schema_surfaces_from_schema(&mut self, schema: &JsonValue) -> Result<(), LixError> {
        let parsed = crate::schema::parse_lix_schema(schema)?;
        if Self::runtime_schema_key_uses_reserved_namespace(&parsed.key)
            && !crate::schema::is_seed_schema_key(&parsed.key)
        {
            return Err(LixError::new(
                LixError::CODE_RESERVED_SCHEMA_NAMESPACE,
                format!(
                    "registered schema '{}' uses the reserved Lix schema namespace but is not a Lix bootstrap schema",
                    parsed.key
                ),
            )
            .with_hint(
                "Custom `lix` and `lix_*` schema keys are incompatible with this Lix version. Migrate the workspace with application-specific tooling before upgrading.",
            ));
        }

        let spec = derive_schema_surface_spec_from_schema(schema)?;

        if !schema_exposed_as_schema_surface(&spec.schema_key) {
            return Ok(());
        }

        let mut columns = row_columns(&spec);
        columns.extend(row_hidden_columns(&spec, false));
        let capabilities = if crate::sql2::read_only::is_read_only_schema_surface(&spec.schema_key)
        {
            SurfaceCapabilities::read_only()
        } else {
            SurfaceCapabilities::read_write()
        };

        self.insert(surface(
            &spec.schema_key,
            PublicSurfaceKind::SchemaBase {
                schema_key: spec.schema_key.clone(),
            },
            columns,
            capabilities.clone(),
        ))?;

        if spec.schema_key != crate::schema::LIX_CHECKPOINT_SCHEMA_KEY {
            let mut by_branch_columns = row_columns(&spec);
            by_branch_columns.extend(row_hidden_columns(&spec, true));

            self.insert(surface(
                format!("{}_by_branch", spec.schema_key),
                PublicSurfaceKind::SchemaByBranch {
                    schema_key: spec.schema_key.clone(),
                },
                by_branch_columns,
                capabilities,
            ))?;
        }

        if schema_exposed_as_history_surface(&spec.schema_key) {
            let history_identity_roots = primary_key_roots(&spec);
            let mut history_columns = spec
                .columns
                .iter()
                .map(|column| {
                    PublicColumn::public(
                        column.name.as_str(),
                        !history_identity_roots.contains(&column.name),
                    )
                })
                .collect::<Vec<_>>();
            history_columns.extend(row_history_system_columns());

            self.insert(surface(
                format!("{}_history", spec.schema_key),
                PublicSurfaceKind::SchemaHistory {
                    schema_key: spec.schema_key.clone(),
                },
                history_columns,
                SurfaceCapabilities::read_only(),
            ))?;
        }

        self.schema_specs.insert(spec.schema_key.clone(), spec);
        Ok(())
    }
}

#[cfg(test)]
fn working_diff_schema(by_branch: bool) -> SchemaRef {
    let mut fields = vec![
        Field::new("diff_id", DataType::Utf8, false),
        json_field("row_pk", false),
        Field::new("schema_key", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, true),
        Field::new("diff_type", DataType::Utf8, false),
        Field::new("before_change_id", DataType::Utf8, true),
        Field::new("after_change_id", DataType::Utf8, true),
    ];
    if by_branch {
        fields.push(Field::new("lixcol_branch_id", DataType::Utf8, false));
    }
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
fn filesystem_schema(by_branch: bool, include_data: bool) -> SchemaRef {
    let mut fields = if include_data {
        vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("path", DataType::Utf8, false),
            Field::new("directory_id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, false),
            Field::new("content", DataType::LargeBinary, false),
        ]
    } else {
        vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("path", DataType::Utf8, true),
            Field::new("parent_id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, false),
        ]
    };
    fields.extend([
        json_field("lixcol_row_pk", false),
        Field::new("lixcol_schema_key", DataType::Utf8, false),
        Field::new("lixcol_file_id", DataType::Utf8, true),
        Field::new("lixcol_global", DataType::Boolean, true),
        Field::new("lixcol_change_id", DataType::Utf8, true),
        Field::new("lixcol_created_at", DataType::Utf8, true),
        Field::new("lixcol_updated_at", DataType::Utf8, true),
        Field::new("lixcol_commit_id", DataType::Utf8, true),
        json_field("lixcol_metadata", true),
    ]);
    if by_branch {
        fields.push(Field::new("lixcol_branch_id", DataType::Utf8, false));
    }
    Arc::new(Schema::new(fields))
}

fn history_filesystem_schema(include_data: bool) -> SchemaRef {
    let mut fields = if include_data {
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("path", DataType::Utf8, true),
            Field::new("directory_id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("content", DataType::LargeBinary, true),
        ]
    } else {
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("path", DataType::Utf8, true),
            Field::new("parent_id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
        ]
    };
    fields.extend([
        json_field(HISTORY_COL_ENTITY_PK, false),
        json_field(HISTORY_COL_SOURCE_CHANGES, false),
        Field::new(HISTORY_COL_OBSERVED_COMMIT_ID, DataType::Utf8, false),
        Field::new(HISTORY_COL_COMMIT_CREATED_AT, DataType::Utf8, false),
        Field::new(HISTORY_COL_AS_OF_COMMIT_ID, DataType::Utf8, false),
        Field::new(HISTORY_COL_DEPTH, DataType::Int64, false),
        Field::new(HISTORY_COL_IS_DELETED, DataType::Boolean, false),
    ]);
    Arc::new(Schema::new(fields))
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

fn public_columns<const N: usize>(columns: [(&str, bool); N]) -> Vec<PublicColumn> {
    columns
        .into_iter()
        .map(|(name, read_nullable)| PublicColumn::public(name, read_nullable))
        .collect()
}

fn primary_key_roots(spec: &SchemaSurfaceSpec) -> std::collections::BTreeSet<&String> {
    spec.primary_key_paths
        .iter()
        .filter_map(|path| path.first())
        .collect()
}

fn row_columns(spec: &SchemaSurfaceSpec) -> Vec<PublicColumn> {
    let primary_key_roots = primary_key_roots(spec);
    spec.columns
        .iter()
        .map(|column| {
            let public_column =
                if spec.schema_key == "lix_registered_schema" && column.name == "value" {
                    PublicColumn::public(column.name.as_str(), column.read_nullable)
                } else if primary_key_roots.contains(&column.name) {
                    PublicColumn::public_insert_only(column.name.as_str(), column.read_nullable)
                } else {
                    PublicColumn::public(column.name.as_str(), column.read_nullable)
                };
            if let Some(default) = column.default_expression.as_deref() {
                public_column.with_default(default)
            } else if !column.insert_required {
                public_column.optional_on_insert()
            } else {
                public_column
            }
        })
        .collect()
}

fn filesystem_columns(by_branch: bool) -> Vec<PublicColumn> {
    let mut columns = vec![
        PublicColumn::public_insert_only("id", false).with_default("uuidv7()"),
        PublicColumn::public("path", false).conditional_on_insert(),
        PublicColumn::public("directory_id", true).conditional_on_insert(),
        PublicColumn::public("name", false).conditional_on_insert(),
        PublicColumn::public("content", false).with_default("CAST('' AS BYTEA)"),
    ];
    columns.extend(filesystem_hidden_columns(by_branch));
    columns
}

fn directory_columns(by_branch: bool) -> Vec<PublicColumn> {
    let mut columns = vec![
        PublicColumn::public_insert_only("id", false).with_default("uuidv7()"),
        PublicColumn::public("path", true).conditional_on_insert(),
        PublicColumn::public("parent_id", true).conditional_on_insert(),
        PublicColumn::public("name", false).conditional_on_insert(),
    ];
    columns.extend(filesystem_hidden_columns(by_branch));
    columns
}

fn row_hidden_columns(spec: &SchemaSurfaceSpec, by_branch: bool) -> Vec<PublicColumn> {
    row_system_columns(
        spec,
        if by_branch {
            SchemaSurfaceShape::ByBranch
        } else {
            SchemaSurfaceShape::Active
        },
    )
}

fn filesystem_hidden_columns(by_branch: bool) -> Vec<PublicColumn> {
    let mut columns = vec![
        PublicColumn::hidden("lixcol_row_pk", false),
        PublicColumn::hidden("lixcol_schema_key", false),
        PublicColumn::hidden("lixcol_file_id", true),
        PublicColumn::public_insert_only("lixcol_global", false).with_default("FALSE"),
        PublicColumn::public_read_only("lixcol_change_id", true),
        PublicColumn::hidden("lixcol_created_at", false),
        PublicColumn::hidden("lixcol_updated_at", false),
        PublicColumn::hidden("lixcol_commit_id", true),
        PublicColumn::public("lixcol_metadata", true).optional_on_insert(),
    ];
    if by_branch {
        columns.push(PublicColumn::public_insert_only("lixcol_branch_id", false));
    }
    columns
}

fn row_system_columns(
    spec: &SchemaSurfaceSpec,
    variant: SchemaSurfaceShape,
) -> Vec<PublicColumn> {
    debug_assert_ne!(variant, SchemaSurfaceShape::History);
    let row_pk = PublicColumn::public_insert_only("lixcol_row_pk", false);
    let row_pk = if spec.primary_key_paths.is_empty() {
        row_pk
    } else {
        row_pk.conditional_on_insert()
    };
    let mut columns = vec![
        row_pk,
        PublicColumn::public_read_only("lixcol_schema_key", false),
        PublicColumn::public_insert_only("lixcol_file_id", true).optional_on_insert(),
        PublicColumn::public("lixcol_metadata", true).optional_on_insert(),
        PublicColumn::public_read_only("lixcol_created_at", false),
        PublicColumn::public_read_only("lixcol_updated_at", false),
        PublicColumn::public_insert_only("lixcol_global", false).with_default("FALSE"),
        PublicColumn::public_read_only("lixcol_change_id", true),
        PublicColumn::public_read_only("lixcol_commit_id", true),
    ];
    if variant == SchemaSurfaceShape::ByBranch {
        columns.push(PublicColumn::public_insert_only("lixcol_branch_id", false));
    }
    columns
}

fn row_history_system_columns() -> Vec<PublicColumn> {
    history_columns([
        (HISTORY_COL_ENTITY_PK, false),
        (HISTORY_COL_SCHEMA_KEY, false),
        (HISTORY_COL_FILE_ID, true),
        (HISTORY_COL_METADATA, true),
        (HISTORY_COL_CHANGE_ID, false),
        (HISTORY_COL_CHANGE_CREATED_AT, false),
        (HISTORY_COL_ORIGIN_KEY, true),
        (HISTORY_COL_OBSERVED_COMMIT_ID, false),
        (HISTORY_COL_COMMIT_CREATED_AT, false),
        (HISTORY_COL_AS_OF_COMMIT_ID, false),
        (HISTORY_COL_DEPTH, false),
        (HISTORY_COL_IS_DELETED, false),
    ])
}

fn file_history_columns() -> Vec<PublicColumn> {
    history_columns([
        ("id", false),
        ("path", true),
        ("directory_id", true),
        ("name", true),
        ("content", true),
        (HISTORY_COL_ENTITY_PK, false),
        (HISTORY_COL_SOURCE_CHANGES, false),
        (HISTORY_COL_OBSERVED_COMMIT_ID, false),
        (HISTORY_COL_COMMIT_CREATED_AT, false),
        (HISTORY_COL_AS_OF_COMMIT_ID, false),
        (HISTORY_COL_DEPTH, false),
        (HISTORY_COL_IS_DELETED, false),
    ])
}

fn directory_history_columns() -> Vec<PublicColumn> {
    history_columns([
        ("id", false),
        ("path", true),
        ("parent_id", true),
        ("name", true),
        (HISTORY_COL_ENTITY_PK, false),
        (HISTORY_COL_SOURCE_CHANGES, false),
        (HISTORY_COL_OBSERVED_COMMIT_ID, false),
        (HISTORY_COL_COMMIT_CREATED_AT, false),
        (HISTORY_COL_AS_OF_COMMIT_ID, false),
        (HISTORY_COL_DEPTH, false),
        (HISTORY_COL_IS_DELETED, false),
    ])
}

fn history_columns<const N: usize>(columns: [(&str, bool); N]) -> Vec<PublicColumn> {
    columns
        .into_iter()
        .map(|(name, nullable)| {
            if name == HISTORY_COL_AS_OF_COMMIT_ID {
                PublicColumn::hidden(name, nullable)
            } else {
                PublicColumn::public_read_only(name, nullable)
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::PublicCatalog;
    use crate::LixError;

    #[test]
    fn catalog_rejects_legacy_runtime_schema_in_reserved_lix_namespace() {
        for legacy_schema in [
            json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "lix_plugin_note",
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                ],
                "primary_key": ["id"],
            }),
            json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "lix_registry_only_legacy",
                "columns": [
                    { "name": "payload", "type": "text", "nullable": false },
                ],
                "primary_key": ["payload"],
            }),
        ] {
            let schema_key = legacy_schema["key"]
                .as_str()
                .expect("test schema key")
                .to_string();
            let error = PublicCatalog::from_visible_schemas(&[legacy_schema])
                .expect_err("every legacy runtime lix_* schema must be rejected");

            assert_eq!(error.code, LixError::CODE_RESERVED_SCHEMA_NAMESPACE);
            assert!(error.message.contains(&schema_key), "{error:?}");
            assert!(
                error
                    .hint
                    .as_deref()
                    .is_some_and(|hint| hint.contains("application-specific tooling")),
                "{error:?}"
            );
        }
    }

    #[test]
    fn catalog_accepts_lix_bootstrap_schemas_in_reserved_namespace() {
        PublicCatalog::from_visible_schemas(
            &crate::schema::seed_schema_definitions()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>(),
        )
        .expect("trusted bootstrap schemas own the reserved lix_* namespace");
    }
}
