use std::collections::BTreeMap;
#[cfg(test)]
use std::sync::Arc;
use std::sync::OnceLock;

#[cfg(test)]
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use serde_json::Value as JsonValue;

use crate::LixError;

use super::{PublicColumn, PublicSurfaceContract, PublicSurfaceKind, SurfaceCapabilities};
#[cfg(test)]
use crate::sql2::catalog::entity_surface_schema;
use crate::sql2::catalog::{
    EntitySurfaceShape, EntitySurfaceSpec, derive_entity_surface_spec_from_schema,
    schema_exposed_as_entity_history_surface, schema_exposed_as_entity_surface,
};
use crate::sql2::history_route::{
    HISTORY_COL_AS_OF_COMMIT_ID, HISTORY_COL_CHANGE_CREATED_AT, HISTORY_COL_CHANGE_ID,
    HISTORY_COL_COMMIT_CREATED_AT, HISTORY_COL_DEPTH, HISTORY_COL_ENTITY_PK, HISTORY_COL_FILE_ID,
    HISTORY_COL_IS_DELETED, HISTORY_COL_METADATA, HISTORY_COL_OBSERVED_COMMIT_ID,
    HISTORY_COL_ORIGIN_KEY, HISTORY_COL_SCHEMA_KEY, HISTORY_COL_SNAPSHOT_CONTENT,
    HISTORY_COL_SOURCE_CHANGES,
};
#[cfg(test)]
use crate::sql2::result_metadata::json_field;

#[derive(Clone, Debug, Default)]
pub(crate) struct PublicCatalog {
    surfaces: BTreeMap<String, PublicSurfaceContract>,
    entity_specs: BTreeMap<String, EntitySurfaceSpec>,
    schema_definitions: BTreeMap<String, JsonValue>,
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

    /// Builds a catalog that keeps the retired generic-state adapters reachable
    /// only from their focused unit tests. The public constructors deliberately
    /// never call this; a later adapter-deletion PR can remove this seam with
    /// the providers it protects.
    #[cfg(test)]
    pub(crate) fn from_visible_schemas_with_internal_state_adapters(
        schema_definitions: &[JsonValue],
    ) -> Result<Self, LixError> {
        let mut catalog = Self::from_visible_schemas(schema_definitions)?;
        catalog.insert(surface(
            "lix_state",
            PublicSurfaceKind::LixState,
            lix_state_columns(false),
            SurfaceCapabilities::read_write(),
        ))?;
        catalog.insert(surface(
            "lix_state_by_branch",
            PublicSurfaceKind::LixStateByBranch,
            lix_state_columns(true),
            SurfaceCapabilities::read_write(),
        ))?;
        catalog.insert(surface(
            "lix_state_history",
            PublicSurfaceKind::History,
            state_history_columns(),
            SurfaceCapabilities::read_only(),
        ))?;
        Ok(catalog)
    }

    /// Compile-time SQL surfaces whose shape cannot be changed at runtime.
    ///
    /// Alongside the hand-written filesystem surfaces, Lix seeds a fixed set
    /// of system entity schemas. Public runtime registration reserves the
    /// complete `lix_*` namespace, so only trusted bootstrap schemas can add
    /// Lix-owned surfaces to this catalog.
    pub(crate) fn fixed_system() -> &'static Self {
        static FIXED_SYSTEM_CATALOG: OnceLock<PublicCatalog> = OnceLock::new();
        FIXED_SYSTEM_CATALOG.get_or_init(|| {
            let schemas = crate::schema::seed_schema_definitions()
                .into_iter()
                .cloned()
                .collect::<Vec<_>>();
            Self::from_visible_schemas(&schemas)
                .expect("compile-time Lix schemas must form a valid SQL catalog")
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

    /// Returns the generated table name that would be claimed by both a
    /// candidate schema key and an already-visible schema key.
    pub(crate) fn generated_surface_collision_for_schema_key<'a>(
        schema_key: &str,
        visible_schema_keys: impl IntoIterator<Item = &'a str>,
    ) -> Option<String> {
        let candidate_names = generated_entity_surface_names(schema_key);
        visible_schema_keys
            .into_iter()
            .filter(|visible_key| *visible_key != schema_key)
            .flat_map(generated_entity_surface_names)
            .find(|surface_name| candidate_names.contains(surface_name))
    }

    pub(crate) fn surfaces(&self) -> impl Iterator<Item = &PublicSurfaceContract> {
        self.surfaces.values()
    }

    pub(crate) fn entity_spec(&self, schema_key: &str) -> Option<&EntitySurfaceSpec> {
        self.entity_specs.get(schema_key)
    }

    pub(crate) fn schema_definitions(&self) -> impl Iterator<Item = (&str, &JsonValue)> {
        self.schema_definitions
            .iter()
            .map(|(key, definition)| (key.as_str(), definition))
    }

    #[cfg(test)]
    pub(crate) fn surface_schema(&self, table_name: &str) -> Option<SchemaRef> {
        let surface = self.surface(table_name)?;
        Some(match &surface.kind {
            PublicSurfaceKind::Schema => Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("table_name", DataType::Utf8, true),
                Field::new("by_branch_table_name", DataType::Utf8, true),
                Field::new("history_table_name", DataType::Utf8, true),
                json_field("primary_key", false),
                json_field("columns", false),
                json_field("surfaces", false),
                json_field("definition", false),
            ])),
            PublicSurfaceKind::SchemaDefinition => Arc::new(Schema::new(vec![
                // Provider planning permits omission on INSERT even though
                // every row emitted by the read surface has a non-null key.
                Field::new("key", DataType::Utf8, true),
                json_field("definition", false),
            ])),
            PublicSurfaceKind::LixState => lix_state_schema(false),
            PublicSurfaceKind::LixStateByBranch => lix_state_schema(true),
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
            PublicSurfaceKind::Change => Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                json_field("entity_pk", false),
                Field::new("schema_key", DataType::Utf8, false),
                Field::new("file_id", DataType::Utf8, true),
                json_field("metadata", true),
                Field::new("created_at", DataType::Utf8, false),
                Field::new("origin_key", DataType::Utf8, true),
                json_field("snapshot_content", true),
            ])),
            PublicSurfaceKind::History => Arc::new(Schema::new(vec![
                json_field(HISTORY_COL_ENTITY_PK, false),
                Field::new(HISTORY_COL_SCHEMA_KEY, DataType::Utf8, false),
                Field::new(HISTORY_COL_FILE_ID, DataType::Utf8, true),
                json_field(HISTORY_COL_SNAPSHOT_CONTENT, true),
                json_field(HISTORY_COL_METADATA, true),
                Field::new(HISTORY_COL_CHANGE_ID, DataType::Utf8, false),
                Field::new(HISTORY_COL_CHANGE_CREATED_AT, DataType::Utf8, false),
                Field::new(HISTORY_COL_ORIGIN_KEY, DataType::Utf8, true),
                Field::new(HISTORY_COL_OBSERVED_COMMIT_ID, DataType::Utf8, false),
                Field::new(HISTORY_COL_COMMIT_CREATED_AT, DataType::Utf8, false),
                Field::new(HISTORY_COL_AS_OF_COMMIT_ID, DataType::Utf8, false),
                Field::new(HISTORY_COL_DEPTH, DataType::Int64, false),
                Field::new(HISTORY_COL_IS_DELETED, DataType::Boolean, false),
            ])),
            PublicSurfaceKind::FileHistory => history_filesystem_schema(true),
            PublicSurfaceKind::DirectoryHistory => history_filesystem_schema(false),
            PublicSurfaceKind::EntityBase { schema_key } => {
                entity_surface_schema(self.entity_spec(schema_key)?, EntitySurfaceShape::Active)
            }
            PublicSurfaceKind::EntityByBranch { schema_key } => {
                entity_surface_schema(self.entity_spec(schema_key)?, EntitySurfaceShape::ByBranch)
            }
            PublicSurfaceKind::EntityHistory { schema_key } => {
                entity_surface_schema(self.entity_spec(schema_key)?, EntitySurfaceShape::History)
            }
        })
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
            "lix_schema",
            PublicSurfaceKind::Schema,
            vec![
                PublicColumn::public_read_only("key", false),
                PublicColumn::public_read_only("table_name", true),
                PublicColumn::public_read_only("by_branch_table_name", true),
                PublicColumn::public_read_only("history_table_name", true),
                PublicColumn::public_read_only("primary_key", false),
                PublicColumn::public_read_only("columns", false),
                PublicColumn::public_read_only("surfaces", false),
                PublicColumn::public_read_only("definition", false),
            ],
            SurfaceCapabilities::read_only(),
        ))?;
        self.insert(surface(
            "lix_schema_definition",
            PublicSurfaceKind::SchemaDefinition,
            vec![
                PublicColumn::public_read_only("key", false),
                PublicColumn::public("definition", false),
            ],
            SurfaceCapabilities::insert_update(),
        ))?;
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
                ("entity_pk", false),
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
        let schema_key = crate::schema::schema_key_from_definition(schema)?.schema_key;
        if Self::runtime_schema_key_uses_reserved_namespace(&schema_key)
            && !crate::schema::is_seed_schema_key(&schema_key)
        {
            return Err(LixError::new(
                LixError::CODE_RESERVED_SCHEMA_NAMESPACE,
                format!(
                    "registered schema '{schema_key}' uses the reserved Lix schema namespace but is not a Lix bootstrap schema"
                ),
            )
            .with_hint(
                "Custom `lix` and `lix_*` schema keys are incompatible with this Lix version. Migrate the workspace with application-specific tooling before upgrading.",
            ));
        }

        if self
            .schema_definitions
            .insert(schema_key.clone(), schema.clone())
            .is_some()
        {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("duplicate visible schema definition '{schema_key}'"),
            ));
        }

        let Ok(spec) = derive_entity_surface_spec_from_schema(schema) else {
            return Ok(());
        };

        self.entity_specs
            .insert(spec.schema_key.clone(), spec.clone());

        if !schema_exposed_as_entity_surface(&spec.schema_key) {
            return Ok(());
        }

        let mut columns = entity_columns(&spec);
        columns.extend(entity_hidden_columns(&spec, false));
        let capabilities = if crate::sql2::read_only::is_read_only_entity_surface(&spec.schema_key)
        {
            SurfaceCapabilities::read_only()
        } else {
            SurfaceCapabilities::read_write()
        };

        self.insert(surface(
            &spec.schema_key,
            PublicSurfaceKind::EntityBase {
                schema_key: spec.schema_key.clone(),
            },
            columns,
            capabilities.clone(),
        ))?;

        let mut by_branch_columns = entity_columns(&spec);
        by_branch_columns.extend(entity_hidden_columns(&spec, true));

        self.insert(surface(
            format!("{}_by_branch", spec.schema_key),
            PublicSurfaceKind::EntityByBranch {
                schema_key: spec.schema_key.clone(),
            },
            by_branch_columns,
            capabilities,
        ))?;

        if schema_exposed_as_entity_history_surface(&spec.schema_key) {
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
            history_columns.extend(entity_history_system_columns());

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

fn generated_entity_surface_names(schema_key: &str) -> [String; 3] {
    [
        schema_key.to_string(),
        format!("{schema_key}_by_branch"),
        format!("{schema_key}_history"),
    ]
}

#[cfg(test)]
fn lix_state_schema(by_branch: bool) -> SchemaRef {
    let mut fields = vec![
        json_field("entity_pk", false),
        Field::new("schema_key", DataType::Utf8, false),
        Field::new("file_id", DataType::Utf8, true),
        json_field("snapshot_content", true),
        json_field("metadata", true),
        Field::new("created_at", DataType::Utf8, true),
        Field::new("updated_at", DataType::Utf8, true),
        Field::new("global", DataType::Boolean, true),
        Field::new("change_id", DataType::Utf8, true),
        Field::new("commit_id", DataType::Utf8, true),
        Field::new("untracked", DataType::Boolean, true),
    ];
    if by_branch {
        fields.push(Field::new("branch_id", DataType::Utf8, false));
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
            Field::new("data", DataType::LargeBinary, false),
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
        json_field("lixcol_entity_pk", false),
        Field::new("lixcol_schema_key", DataType::Utf8, false),
        Field::new("lixcol_file_id", DataType::Utf8, true),
        Field::new("lixcol_global", DataType::Boolean, true),
        Field::new("lixcol_change_id", DataType::Utf8, true),
        Field::new("lixcol_created_at", DataType::Utf8, true),
        Field::new("lixcol_updated_at", DataType::Utf8, true),
        Field::new("lixcol_commit_id", DataType::Utf8, true),
        Field::new("lixcol_untracked", DataType::Boolean, true),
        json_field("lixcol_metadata", true),
    ]);
    if by_branch {
        fields.push(Field::new("lixcol_branch_id", DataType::Utf8, false));
    }
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
fn history_filesystem_schema(include_data: bool) -> SchemaRef {
    let mut fields = if include_data {
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("path", DataType::Utf8, true),
            Field::new("directory_id", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("data", DataType::LargeBinary, true),
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

fn primary_key_roots(spec: &EntitySurfaceSpec) -> std::collections::BTreeSet<&String> {
    spec.primary_key_paths
        .iter()
        .filter_map(|path| path.first())
        .collect()
}

fn entity_columns(spec: &EntitySurfaceSpec) -> Vec<PublicColumn> {
    let primary_key_roots = primary_key_roots(spec);
    spec.columns
        .iter()
        .map(|column| {
            let public_column = if primary_key_roots.contains(&column.name) {
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

#[cfg(test)]
fn lix_state_columns(by_branch: bool) -> Vec<PublicColumn> {
    let global = if by_branch {
        PublicColumn::public("global", false).conditional_on_insert()
    } else {
        PublicColumn::public("global", false).with_default("FALSE")
    };
    let mut columns = vec![
        PublicColumn::public_insert_only("entity_pk", false),
        PublicColumn::public_insert_only("schema_key", false),
        PublicColumn::public_insert_only("file_id", true).optional_on_insert(),
        PublicColumn::public("snapshot_content", true).optional_on_insert(),
        PublicColumn::public("metadata", true).optional_on_insert(),
        PublicColumn::public_read_only("created_at", false),
        PublicColumn::public_read_only("updated_at", false),
        global,
        PublicColumn::public_read_only("change_id", true),
        PublicColumn::public_read_only("commit_id", true),
        PublicColumn::public("untracked", false).with_default("FALSE"),
    ];
    if by_branch {
        columns.push(PublicColumn::public_insert_only("branch_id", false).conditional_on_insert());
    }
    columns
}

#[cfg(test)]
fn state_history_columns() -> Vec<PublicColumn> {
    public_columns([
        (HISTORY_COL_ENTITY_PK, false),
        (HISTORY_COL_SCHEMA_KEY, false),
        (HISTORY_COL_FILE_ID, true),
        (HISTORY_COL_SNAPSHOT_CONTENT, true),
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

fn filesystem_columns(by_branch: bool) -> Vec<PublicColumn> {
    let mut columns = vec![
        PublicColumn::public_insert_only("id", false).with_default("lix_uuid_v7()"),
        PublicColumn::public("path", false).conditional_on_insert(),
        PublicColumn::public("directory_id", true).conditional_on_insert(),
        PublicColumn::public("name", false).conditional_on_insert(),
        PublicColumn::public("data", false).with_default("X''"),
    ];
    columns.extend(filesystem_hidden_columns(by_branch));
    columns
}

fn directory_columns(by_branch: bool) -> Vec<PublicColumn> {
    let mut columns = vec![
        PublicColumn::public_insert_only("id", false).with_default("lix_uuid_v7()"),
        PublicColumn::public("path", true).conditional_on_insert(),
        PublicColumn::public("parent_id", true).conditional_on_insert(),
        PublicColumn::public("name", false).conditional_on_insert(),
    ];
    columns.extend(filesystem_hidden_columns(by_branch));
    columns
}

fn entity_hidden_columns(spec: &EntitySurfaceSpec, by_branch: bool) -> Vec<PublicColumn> {
    entity_system_columns(
        spec,
        if by_branch {
            EntitySurfaceShape::ByBranch
        } else {
            EntitySurfaceShape::Active
        },
    )
}

fn filesystem_hidden_columns(by_branch: bool) -> Vec<PublicColumn> {
    let mut columns = vec![
        PublicColumn::hidden("lixcol_entity_pk", false),
        PublicColumn::hidden("lixcol_schema_key", false),
        PublicColumn::hidden("lixcol_file_id", true),
        PublicColumn::public_insert_only("lixcol_global", false).with_default("FALSE"),
        PublicColumn::public_read_only("lixcol_change_id", true),
        PublicColumn::hidden("lixcol_created_at", false),
        PublicColumn::hidden("lixcol_updated_at", false),
        PublicColumn::hidden("lixcol_commit_id", true),
        PublicColumn::public_insert_only("lixcol_untracked", false).with_default("FALSE"),
        PublicColumn::public("lixcol_metadata", true).optional_on_insert(),
    ];
    if by_branch {
        columns.push(PublicColumn::public_insert_only("lixcol_branch_id", false));
    }
    columns
}

fn entity_system_columns(
    spec: &EntitySurfaceSpec,
    variant: EntitySurfaceShape,
) -> Vec<PublicColumn> {
    debug_assert_ne!(variant, EntitySurfaceShape::History);
    let entity_pk = PublicColumn::public_insert_only("lixcol_entity_pk", false);
    let entity_pk = if spec.primary_key_paths.is_empty() {
        entity_pk
    } else {
        entity_pk.conditional_on_insert()
    };
    let mut columns = vec![
        entity_pk,
        PublicColumn::public_read_only("lixcol_schema_key", false),
        PublicColumn::public_insert_only("lixcol_file_id", true).optional_on_insert(),
        PublicColumn::hidden("lixcol_snapshot_content", true),
        PublicColumn::public("lixcol_metadata", true).optional_on_insert(),
        PublicColumn::public_read_only("lixcol_created_at", false),
        PublicColumn::public_read_only("lixcol_updated_at", false),
        PublicColumn::public_insert_only("lixcol_global", false).with_default("FALSE"),
        PublicColumn::public_read_only("lixcol_change_id", true),
        PublicColumn::public_read_only("lixcol_commit_id", true),
        PublicColumn::public_insert_only("lixcol_untracked", false).with_default("FALSE"),
    ];
    if variant == EntitySurfaceShape::ByBranch {
        columns.push(PublicColumn::public_insert_only("lixcol_branch_id", false));
    }
    columns
}

fn entity_history_system_columns() -> Vec<PublicColumn> {
    public_columns([
        (HISTORY_COL_ENTITY_PK, false),
        (HISTORY_COL_SCHEMA_KEY, false),
        (HISTORY_COL_FILE_ID, true),
        (HISTORY_COL_SNAPSHOT_CONTENT, true),
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
    public_columns([
        ("id", false),
        ("path", true),
        ("directory_id", true),
        ("name", true),
        ("data", true),
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
    public_columns([
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::PublicCatalog;
    use crate::LixError;

    #[test]
    fn catalog_rejects_legacy_runtime_schema_in_reserved_lix_namespace() {
        for legacy_schema in [
            json!({
                "x-lix-key": "lix_plugin_note",
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": { "id": { "type": "string" } },
                "required": ["id"],
                "additionalProperties": false,
            }),
            json!({
                "x-lix-key": "lix_registry_only_legacy",
                "type": "object",
                "properties": { "payload": {} },
                "additionalProperties": false,
            }),
        ] {
            let schema_key = legacy_schema["x-lix-key"]
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
