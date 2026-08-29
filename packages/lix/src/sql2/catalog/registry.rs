use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::OnceLock;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use serde_json::Value as JsonValue;

use crate::LixError;

use super::{
    PUBLIC_SCALAR_FUNCTION_NAMES, PublicColumn, PublicHistoryContract, PublicHistoryKind,
    PublicRelationKind, PublicScalarFunctionContract, PublicSurfaceClass, PublicSurfaceContract,
    PublicSurfaceKind, SurfaceCapabilities,
};
use crate::sql2::catalog::schema_surface_schema;
use crate::sql2::catalog::{
    SchemaSurfaceShape, SchemaSurfaceSpec, derive_schema_surface_spec_from_schema,
    schema_exposed_as_history_surface, schema_exposed_as_schema_surface,
};
use crate::sql2::history_route::{
    HISTORY_COL_AS_OF_COMMIT_ID, HISTORY_COL_CHANGE_CREATED_AT, HISTORY_COL_CHANGE_ID,
    HISTORY_COL_COMMIT_CREATED_AT, HISTORY_COL_DEPTH, HISTORY_COL_FILE_ID, HISTORY_COL_IS_DELETED,
    HISTORY_COL_METADATA, HISTORY_COL_OBSERVED_COMMIT_ID, HISTORY_COL_ORIGIN_KEY,
    HISTORY_COL_ROW_PK, HISTORY_COL_SCHEMA_KEY, HISTORY_COL_SOURCE_CHANGES,
};
use crate::sql2::result_metadata::{json_field, row_ref_field};

#[derive(Clone, Debug, Default)]
pub(crate) struct PublicCatalog {
    surfaces: BTreeMap<String, PublicSurfaceContract>,
    scalar_functions: BTreeMap<String, PublicScalarFunctionContract>,
    history: BTreeMap<String, PublicHistoryContract>,
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
        if !surface.kind.accepts_class(surface.class) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "public SQL surface '{}' has incompatible class {:?} and semantic kind {:?}",
                    surface.name, surface.class, surface.kind
                ),
            ));
        }
        if self.surfaces.contains_key(&surface.name)
            || self.scalar_functions.contains_key(&surface.name)
        {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("duplicate public SQL surface '{}'", surface.name),
            ));
        }
        self.surfaces.insert(surface.name.clone(), surface);
        Ok(())
    }

    fn insert_scalar_function(&mut self, name: &str) -> Result<(), LixError> {
        if self.surfaces.contains_key(name) || self.scalar_functions.contains_key(name) {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("duplicate public SQL surface '{name}'"),
            ));
        }
        self.scalar_functions.insert(
            name.to_string(),
            PublicScalarFunctionContract {
                name: name.to_string(),
                class: PublicSurfaceClass::ScalarFunction,
            },
        );
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

    pub(crate) fn scalar_functions(&self) -> impl Iterator<Item = &PublicScalarFunctionContract> {
        self.scalar_functions.values()
    }

    pub(crate) fn history_relations(&self) -> impl Iterator<Item = &PublicHistoryContract> {
        self.history.values()
    }

    pub(crate) fn history_relation(&self, relation_name: &str) -> Option<&PublicHistoryContract> {
        self.history.get(relation_name)
    }

    pub(crate) fn schema_spec(&self, schema_key: &str) -> Option<&SchemaSurfaceSpec> {
        self.schema_specs.get(schema_key)
    }

    pub(crate) fn surface_schema(&self, table_name: &str) -> Option<SchemaRef> {
        let surface = self.surface(table_name)?;
        Some(match &surface.kind {
            PublicSurfaceKind::File => filesystem_schema(true),
            PublicSurfaceKind::Directory => filesystem_schema(false),
            PublicSurfaceKind::Branch => Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("hidden", DataType::Boolean, false),
                Field::new("commit_id", DataType::Utf8, false),
            ])),
            PublicSurfaceKind::HistoryFunction
            | PublicSurfaceKind::DiffFunction
            | PublicSurfaceKind::CheckpointFunction
            | PublicSurfaceKind::StateAtFunction
            | PublicSurfaceKind::CommitAncestryFunction => {
                return None;
            }
            PublicSurfaceKind::Revert | PublicSurfaceKind::Apply => Arc::new(Schema::new(vec![
                row_ref_field("row_ref", false),
            ])),
            PublicSurfaceKind::Restore => Arc::new(Schema::new(vec![Field::new(
                "commit_id",
                DataType::Utf8,
                false,
            )])),
            PublicSurfaceKind::Change => Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("account_id", DataType::Utf8, false),
                row_ref_field("row_ref", true),
                Field::new("schema_key", DataType::Utf8, false),
                Field::new("file_id", DataType::Utf8, true),
                json_field("metadata", true),
                Field::new("created_at", DataType::Utf8, false),
                Field::new("origin_key", DataType::Utf8, true),
                json_field("snapshot_content", true),
            ])),
            PublicSurfaceKind::SchemaBase { schema_key } => {
                schema_surface_schema(self.schema_spec(schema_key)?, SchemaSurfaceShape::Active)
            }
        })
    }

    pub(crate) fn history_relation_schema(&self, relation_name: &str) -> Option<SchemaRef> {
        let history = self.history_relation(relation_name)?;
        match &history.kind {
            PublicHistoryKind::File => Some(history_filesystem_schema(true)),
            PublicHistoryKind::Directory => Some(history_filesystem_schema(false)),
            PublicHistoryKind::Schema { schema_key } => Some(schema_surface_schema(
                self.schema_spec(schema_key)?,
                SchemaSurfaceShape::History,
            )),
        }
    }

    fn insert_history(&mut self, mut history: PublicHistoryContract) -> Result<(), LixError> {
        if self.history.contains_key(&history.relation_name) {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!(
                    "duplicate public SQL history relation '{}'",
                    history.relation_name
                ),
            ));
        }
        history.columns = history
            .columns
            .into_iter()
            .enumerate()
            .map(|(id, column)| column.with_id(id))
            .collect();
        self.history.insert(history.relation_name.clone(), history);
        Ok(())
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
            PublicSurfaceClass::Relation(PublicRelationKind::View),
            PublicSurfaceKind::File,
            filesystem_columns(),
            SurfaceCapabilities::read_write(),
        ))?;
        self.insert(surface(
            "lix_directory",
            PublicSurfaceClass::Relation(PublicRelationKind::View),
            PublicSurfaceKind::Directory,
            directory_columns(),
            SurfaceCapabilities::read_write(),
        ))?;
        self.insert(surface(
            "lix_branch",
            PublicSurfaceClass::Relation(PublicRelationKind::View),
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
            PublicSurfaceClass::Relation(PublicRelationKind::View),
            PublicSurfaceKind::Change,
            public_columns([
                ("id", false),
                ("account_id", false),
                ("row_ref", true),
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
            "lix_history",
            PublicSurfaceClass::TableFunction,
            PublicSurfaceKind::HistoryFunction,
            Vec::new(),
            SurfaceCapabilities::read_only(),
        ))?;
        self.insert(surface(
            "lix_diff",
            PublicSurfaceClass::TableFunction,
            PublicSurfaceKind::DiffFunction,
            Vec::new(),
            SurfaceCapabilities::read_only(),
        ))?;
        self.insert(surface(
            "lix_create_checkpoint",
            PublicSurfaceClass::TableFunction,
            PublicSurfaceKind::CheckpointFunction,
            vec![PublicColumn::public_read_only("commit_id", false)],
            SurfaceCapabilities::read_only(),
        ))?;
        self.insert(surface(
            "lix_state_at",
            PublicSurfaceClass::TableFunction,
            PublicSurfaceKind::StateAtFunction,
            Vec::new(),
            SurfaceCapabilities::read_only(),
        ))?;
        self.insert(surface(
            "lix_commit_ancestry",
            PublicSurfaceClass::TableFunction,
            PublicSurfaceKind::CommitAncestryFunction,
            Vec::new(),
            SurfaceCapabilities::read_only(),
        ))?;
        for (name, kind) in [
            ("lix_revert", PublicSurfaceKind::Revert),
            ("lix_apply", PublicSurfaceKind::Apply),
        ] {
            self.insert(surface(
                name,
                PublicSurfaceClass::CommandSink,
                kind,
                vec![
                    PublicColumn::public_insert_only("row_ref", false),
                    PublicColumn::public_read_only("commit_id", false),
                ],
                SurfaceCapabilities {
                    insert: true,
                    update: false,
                    delete: false,
                },
            ))?;
        }
        self.insert(surface(
            "lix_restore",
            PublicSurfaceClass::CommandSink,
            PublicSurfaceKind::Restore,
            vec![PublicColumn::public_insert_only("commit_id", false)],
            SurfaceCapabilities {
                insert: true,
                update: false,
                delete: false,
            },
        ))?;
        for name in PUBLIC_SCALAR_FUNCTION_NAMES {
            self.insert_scalar_function(name)?;
        }
        self.insert_history(PublicHistoryContract {
            relation_name: "lix_file".to_string(),
            kind: PublicHistoryKind::File,
            columns: file_history_columns(),
        })?;
        self.insert_history(PublicHistoryContract {
            relation_name: "lix_directory".to_string(),
            kind: PublicHistoryKind::Directory,
            columns: directory_history_columns(),
        })?;
        Ok(())
    }

    fn insert_schema_surfaces_from_schema(&mut self, schema: &JsonValue) -> Result<(), LixError> {
        let parsed = crate::schema::parse_lix_schema(schema)?;
        // Repositories created before the relation-diff hard cut can still
        // contain this formerly seeded registration in their durable
        // catalog. Ignore that exact retired bootstrap schema while reading
        // old repositories; transaction normalization still rejects every
        // attempt to register a new reserved `lix_*` schema at runtime.
        if parsed.key == "lix_commit_edge" {
            return Ok(());
        }
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
                "The `lix` and `lix_*` schema namespaces are reserved for Lix. Register this schema under an owner-specific prefix such as `acme_task`.",
            ));
        }

        let spec = derive_schema_surface_spec_from_schema(schema)?;

        if !schema_exposed_as_schema_surface(&spec.schema_key) {
            return Ok(());
        }

        let mut columns = row_columns(&spec);
        columns.extend(row_hidden_columns(&spec));
        let capabilities = if crate::sql2::read_only::is_read_only_schema_surface(&spec.schema_key)
        {
            SurfaceCapabilities::read_only()
        } else {
            SurfaceCapabilities::read_write()
        };

        self.insert(surface(
            &spec.schema_key,
            PublicSurfaceClass::Relation(PublicRelationKind::Base),
            PublicSurfaceKind::SchemaBase {
                schema_key: spec.schema_key.clone(),
            },
            columns,
            capabilities.clone(),
        ))?;

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

            self.insert_history(PublicHistoryContract {
                relation_name: spec.schema_key.clone(),
                kind: PublicHistoryKind::Schema {
                    schema_key: spec.schema_key.clone(),
                },
                columns: history_columns,
            })?;
        }

        self.schema_specs.insert(spec.schema_key.clone(), spec);
        Ok(())
    }
}

fn filesystem_schema(include_data: bool) -> SchemaRef {
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
        row_ref_field(HISTORY_COL_ROW_PK, false),
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
    class: PublicSurfaceClass,
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
        class,
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

fn filesystem_columns() -> Vec<PublicColumn> {
    let mut columns = vec![
        PublicColumn::public_insert_only("id", false).with_default("uuidv7()"),
        PublicColumn::public("path", false).conditional_on_insert(),
        PublicColumn::public("directory_id", true).conditional_on_insert(),
        PublicColumn::public("name", false).conditional_on_insert(),
        PublicColumn::public("content", false).with_default("CAST('' AS BYTEA)"),
    ];
    columns.extend(filesystem_system_columns());
    columns
}

fn directory_columns() -> Vec<PublicColumn> {
    let mut columns = vec![
        PublicColumn::public_insert_only("id", false).with_default("uuidv7()"),
        PublicColumn::public("path", true).conditional_on_insert(),
        PublicColumn::public("parent_id", true).conditional_on_insert(),
        PublicColumn::public("name", false).conditional_on_insert(),
    ];
    columns.extend(filesystem_system_columns());
    columns
}

fn row_hidden_columns(spec: &SchemaSurfaceSpec) -> Vec<PublicColumn> {
    row_system_columns(spec, SchemaSurfaceShape::Active)
}

fn filesystem_system_columns() -> Vec<PublicColumn> {
    vec![
        PublicColumn::hidden("lixcol_schema_key", false),
        PublicColumn::hidden("lixcol_file_id", true),
        PublicColumn::public_insert_only("lixcol_global", false).with_default("FALSE"),
        PublicColumn::public_read_only("lixcol_change_id", true),
        PublicColumn::public_read_only("lixcol_created_at", false),
        PublicColumn::public_read_only("lixcol_updated_at", false),
        PublicColumn::hidden("lixcol_commit_id", true),
        PublicColumn::public_insert_only("lixcol_untracked", false).with_default("FALSE"),
        PublicColumn::public("lixcol_metadata", true).optional_on_insert(),
    ]
}

fn row_system_columns(_spec: &SchemaSurfaceSpec, variant: SchemaSurfaceShape) -> Vec<PublicColumn> {
    debug_assert_ne!(variant, SchemaSurfaceShape::History);
    vec![
        PublicColumn::public_read_only("lixcol_schema_key", false),
        PublicColumn::public_insert_only("lixcol_file_id", true).optional_on_insert(),
        PublicColumn::public("lixcol_metadata", true).optional_on_insert(),
        PublicColumn::public_read_only("lixcol_created_at", false),
        PublicColumn::public_read_only("lixcol_updated_at", false),
        PublicColumn::public_insert_only("lixcol_global", false).with_default("FALSE"),
        PublicColumn::public_read_only("lixcol_change_id", true),
        PublicColumn::public_read_only("lixcol_commit_id", true),
        PublicColumn::public_insert_only("lixcol_untracked", false).with_default("FALSE"),
    ]
}

fn row_history_system_columns() -> Vec<PublicColumn> {
    history_columns([
        (HISTORY_COL_ROW_PK, false),
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
        (HISTORY_COL_ROW_PK, false),
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
        (HISTORY_COL_ROW_PK, false),
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
    use std::collections::BTreeSet;

    use serde_json::json;

    use super::{PublicCatalog, PublicSurfaceKind};
    use crate::LixError;
    use crate::sql2::catalog::TRACKED_ROW_SYSTEM_COLUMN_NAMES;
    use crate::sql2::history_route::{
        HISTORY_COL_AS_OF_COMMIT_ID, HISTORY_COL_CHANGE_CREATED_AT,
        HISTORY_COL_CHANGE_ID, HISTORY_COL_COMMIT_CREATED_AT, HISTORY_COL_DEPTH,
        HISTORY_COL_FILE_ID, HISTORY_COL_IS_DELETED, HISTORY_COL_METADATA,
        HISTORY_COL_OBSERVED_COMMIT_ID, HISTORY_COL_ORIGIN_KEY, HISTORY_COL_ROW_PK,
        HISTORY_COL_SCHEMA_KEY, HISTORY_COL_SOURCE_CHANGES,
    };

    #[test]
    fn lixcol_names_are_reserved_for_system_metadata() {
        let catalog = PublicCatalog::fixed_system();
        let tracked = TRACKED_ROW_SYSTEM_COLUMN_NAMES
            .into_iter()
            .collect::<BTreeSet<_>>();

        for surface in catalog.surfaces() {
            let lixcol_columns = surface
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .filter(|name| name.contains("lixcol_"))
                .collect::<Vec<_>>();
            let lixcol_names = lixcol_columns.iter().copied().collect::<BTreeSet<_>>();
            assert_eq!(
                lixcol_columns.len(),
                lixcol_names.len(),
                "surface '{}' must not duplicate system columns",
                surface.name
            );
            match &surface.kind {
                PublicSurfaceKind::SchemaBase { .. }
                | PublicSurfaceKind::File
                | PublicSurfaceKind::Directory => assert_eq!(
                    lixcol_names, tracked,
                    "tracked relation '{}' must expose exactly the canonical bookkeeping set",
                    surface.name
                ),
                _ => assert!(
                    lixcol_names.is_empty(),
                    "non-state surface '{}' must not expose lixcol payload",
                    surface.name
                ),
            }
        }

        let history_system = [
            HISTORY_COL_ROW_PK,
            HISTORY_COL_SCHEMA_KEY,
            HISTORY_COL_FILE_ID,
            HISTORY_COL_METADATA,
            HISTORY_COL_CHANGE_ID,
            HISTORY_COL_CHANGE_CREATED_AT,
            HISTORY_COL_SOURCE_CHANGES,
            HISTORY_COL_ORIGIN_KEY,
            HISTORY_COL_OBSERVED_COMMIT_ID,
            HISTORY_COL_COMMIT_CREATED_AT,
            HISTORY_COL_AS_OF_COMMIT_ID,
            HISTORY_COL_DEPTH,
            HISTORY_COL_IS_DELETED,
        ];
        assert!(
            history_system
                .iter()
                .all(|name| name.starts_with("lixcol_")),
            "history metadata must retain the collision-safe system prefix"
        );
        let allowed_history = history_system.into_iter().collect::<BTreeSet<_>>();
        for history in catalog.history_relations() {
            let lixcol_columns = history
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .filter(|name| name.contains("lixcol_"))
                .collect::<Vec<_>>();
            let lixcol_names = lixcol_columns.iter().copied().collect::<BTreeSet<_>>();
            assert_eq!(
                lixcol_columns.len(),
                lixcol_names.len(),
                "history for '{}' must not duplicate system columns",
                history.relation_name
            );
            for name in lixcol_names {
                assert!(
                    allowed_history.contains(name),
                    "history for '{}' exposes non-system lixcol column '{name}'",
                    history.relation_name
                );
            }
        }
    }

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
                    .is_some_and(|hint| hint.contains("owner-specific prefix")),
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

    #[test]
    fn catalog_hides_retired_commit_edge_schema_in_existing_repositories() {
        let retired_schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "lix_commit_edge",
            "columns": [
                { "name": "commit_id", "type": "text", "nullable": false },
                { "name": "parent_commit_id", "type": "text", "nullable": false },
            ],
            "primary_key": ["commit_id", "parent_commit_id"],
        });
        let catalog = PublicCatalog::from_visible_schemas(&[retired_schema])
            .expect("repositories retaining a retired bootstrap registration should open");

        assert!(catalog.surface("lix_commit_edge").is_none());
        assert!(catalog.history_relation("lix_commit_edge").is_none());
        assert!(catalog.schema_spec("lix_commit_edge").is_none());
    }
}
