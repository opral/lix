use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use serde_json::Value as JsonValue;

use crate::LixError;
use crate::row_pk::RowPkComponentType;
use crate::sql2::history_route::{
    HISTORY_COL_AS_OF_COMMIT_ID, HISTORY_COL_CHANGE_CREATED_AT, HISTORY_COL_CHANGE_ID,
    HISTORY_COL_COMMIT_CREATED_AT, HISTORY_COL_DEPTH, HISTORY_COL_FILE_ID, HISTORY_COL_IS_DELETED,
    HISTORY_COL_METADATA, HISTORY_COL_OBSERVED_COMMIT_ID, HISTORY_COL_ORIGIN_KEY,
    HISTORY_COL_ROW_PK, HISTORY_COL_SCHEMA_KEY,
};
use crate::sql2::result_metadata::{json_field, mark_json_field};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SchemaSurfaceShape {
    Active,
    History,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SchemaColumnType {
    String,
    Jsonb,
    Integer,
    Number,
    Boolean,
    Timestamptz,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaSurfaceColumn {
    pub(crate) name: String,
    pub(crate) native_type: lix_schema::DataType,
    pub(crate) column_type: SchemaColumnType,
    pub(crate) read_nullable: bool,
    pub(crate) insert_required: bool,
    pub(crate) default_expression: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaSurfaceSpec {
    pub(crate) schema_key: String,
    /// Fingerprint used by the typed plugin wire and durable row payloads.
    /// SQL readers bind durable rows to this resolved schema before exposing
    /// any typed value.
    pub(crate) schema_fingerprint: [u8; 32],
    pub(crate) primary_key_paths: Vec<Vec<String>>,
    pub(crate) primary_key_component_types: Vec<RowPkComponentType>,
    pub(crate) columns: Vec<SchemaSurfaceColumn>,
    pub(crate) defaults: crate::catalog::DefaultPlan,
    /// Columns this schema already declares as a foreign key or as unique,
    /// which are therefore the columns the hot index plane can serve.
    ///
    /// Derived from `x-lix-foreign-keys` and `x-lix-unique` so indexing adds no
    /// user-facing concept. Order is stable and defines each column's ordinal
    /// in the index key, so it must not be reordered without retiring the
    /// index namespace.
    pub(crate) indexed_columns: Vec<SchemaIndexedColumn>,
    /// Whether changing one row can invalidate another row.
    ///
    /// Homogeneous point updates may be lowered into one physical write batch
    /// only when every row is independent. JSON Schema validation is row-local;
    /// uniqueness and foreign-key declarations are the inter-row exceptions.
    pub(crate) has_inter_row_constraints: bool,
    /// This exact schema shape proves that replacing `value` while preserving
    /// the already-valid string `path` produces complete valid row content.
    pub(crate) certifies_path_value_replacement: bool,
    /// Every accepted snapshot is exactly the declared, required top-level
    /// columns, so typed Arrow values can reconstruct canonical JSON without
    /// a second persisted snapshot payload.
    pub(crate) columnar_snapshot_bijective: bool,
}

impl SchemaSurfaceSpec {
    #[cfg(test)]
    pub(crate) fn visible_column_names(&self) -> impl Iterator<Item = &str> {
        self.columns.iter().map(|column| column.name.as_str())
    }

    pub(crate) fn visible_column(&self, column_name: &str) -> Option<&SchemaSurfaceColumn> {
        self.columns
            .iter()
            .find(|column| column.name == column_name)
    }

    /// Stable identity of the registered schema properties that determine an
    /// row columnar sidecar's physical meaning.
    ///
    /// In particular, String and Jsonb both use Arrow Utf8. A name/type-only
    /// comparison cannot distinguish scalar string bytes from canonical JSON
    /// text after a registered-schema amendment.
    pub(crate) fn columnar_layout_fingerprint(&self) -> String {
        fn update_part(hasher: &mut blake3::Hasher, bytes: &[u8]) {
            hasher.update(&(bytes.len() as u64).to_be_bytes());
            hasher.update(bytes);
        }

        let mut hasher = blake3::Hasher::new_derive_key("lix row columnar layout v1");
        update_part(&mut hasher, self.schema_key.as_bytes());
        hasher.update(&(self.columns.len() as u64).to_be_bytes());
        for column in &self.columns {
            update_part(&mut hasher, column.name.as_bytes());
            hasher.update(&[match column.column_type {
                SchemaColumnType::String => 1,
                SchemaColumnType::Jsonb => 2,
                SchemaColumnType::Integer => 3,
                SchemaColumnType::Number => 4,
                SchemaColumnType::Boolean => 5,
                SchemaColumnType::Timestamptz => 6,
            }]);
            hasher.update(&[u8::from(column.read_nullable)]);
        }
        hasher.update(&(self.primary_key_paths.len() as u64).to_be_bytes());
        for path in &self.primary_key_paths {
            hasher.update(&(path.len() as u64).to_be_bytes());
            for segment in path {
                update_part(&mut hasher, segment.as_bytes());
            }
        }
        hasher.update(&(self.primary_key_component_types.len() as u64).to_be_bytes());
        for component_type in &self.primary_key_component_types {
            hasher.update(&[match component_type {
                RowPkComponentType::Uuid => 1,
                RowPkComponentType::Integer => 2,
                RowPkComponentType::String => 3,
                RowPkComponentType::Bytes => 4,
            }]);
        }
        hasher.finalize().to_hex().to_string()
    }
}

pub(crate) fn derive_schema_surface_spec_from_schema(
    schema: &JsonValue,
) -> Result<SchemaSurfaceSpec, LixError> {
    let parsed = crate::schema::parse_lix_schema(schema)?;
    let schema_key = parsed.key.clone();
    let schema_fingerprint = *parsed
        .wire_fingerprint()
        .map_err(|error| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("failed to fingerprint resolved schema '{schema_key}': {error}"),
            )
        })?
        .as_bytes();
    let primary_key_paths = parsed
        .primary_key
        .iter()
        .cloned()
        .map(|column| vec![column])
        .collect::<Vec<_>>();
    let primary_key_component_types = parsed
        .primary_key
        .iter()
        .map(|name| {
            let column = parsed
                .columns
                .iter()
                .find(|column| &column.name == name)
                .expect("validated primary-key column must exist");
            match column.data_type {
                lix_schema::DataType::Uuid => RowPkComponentType::Uuid,
                lix_schema::DataType::Int8 => RowPkComponentType::Integer,
                lix_schema::DataType::Text => RowPkComponentType::String,
                _ => unreachable!("validated Schema v1 primary-key type"),
            }
        })
        .collect();
    let columns = parsed
        .columns
        .iter()
        .map(|column| SchemaSurfaceColumn {
            name: column.name.clone(),
            native_type: column.data_type,
            column_type: match column.data_type {
                lix_schema::DataType::Text | lix_schema::DataType::Uuid => SchemaColumnType::String,
                lix_schema::DataType::Int8 => SchemaColumnType::Integer,
                lix_schema::DataType::Float8 => SchemaColumnType::Number,
                lix_schema::DataType::Boolean => SchemaColumnType::Boolean,
                lix_schema::DataType::Jsonb => SchemaColumnType::Jsonb,
                lix_schema::DataType::Timestamptz => SchemaColumnType::Timestamptz,
            },
            read_nullable: column.nullable,
            insert_required: !column.nullable
                && column.default_value.is_none()
                && column.default_expression.is_none(),
            default_expression: column
                .default_expression
                .clone()
                .or_else(|| column.default_value.as_ref().map(postgres_literal)),
        })
        .collect::<Vec<_>>();
    let certifies_path_value_replacement = parsed.primary_key == ["path"]
        && parsed.columns.len() == 2
        && parsed.columns.iter().any(|column| {
            column.name == "path"
                && column.data_type == lix_schema::DataType::Text
                && !column.nullable
                && column.default_value.is_none()
                && column.default_expression.is_none()
        })
        && parsed.columns.iter().any(|column| {
            column.name == "value"
                && column.data_type == lix_schema::DataType::Jsonb
                && !column.nullable
                && column.default_value.is_none()
                && column.default_expression.is_none()
        });
    let indexed_columns = derive_indexed_columns(&parsed, &columns);
    let columnar_snapshot_bijective = columns.iter().all(|column| {
        !column.read_nullable
            && column.default_expression.is_none()
            && matches!(
                column.column_type,
                SchemaColumnType::String | SchemaColumnType::Integer | SchemaColumnType::Boolean
            )
    });
    Ok(SchemaSurfaceSpec {
        schema_key,
        schema_fingerprint,
        primary_key_paths,
        primary_key_component_types,
        indexed_columns,
        columns,
        defaults: crate::catalog::DefaultPlan::from_schema(schema),
        has_inter_row_constraints: !parsed.unique.is_empty() || !parsed.foreign_keys.is_empty(),
        certifies_path_value_replacement,
        columnar_snapshot_bijective,
    })
}

/// One column the hot index plane can serve, and its position in the index key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaIndexedColumn {
    pub(crate) name: String,
    pub(crate) ordinal: u16,
    pub(crate) column_type: SchemaColumnType,
}

/// Indexable columns, from declarations the schema already carries.
///
/// Deliberately narrow, and every exclusion is a case the collection scan still
/// serves correctly:
///
/// - single-column groups only — a composite group needs a composite key
///   encoding and no measured workload asks for one yet;
/// - `String` and `Integer` only — those are the types with an order-preserving
///   key encoding;
/// - primary-key columns are skipped — the hot row key already indexes them,
///   and a second access path to the same rows would be a second mechanism.
fn derive_indexed_columns(
    schema: &lix_schema::Schema,
    columns: &[SchemaSurfaceColumn],
) -> Vec<SchemaIndexedColumn> {
    let mut names = Vec::new();
    let mut push = |name: &str| {
        if !names.iter().any(|existing: &String| existing == name) {
            names.push(name.to_string());
        }
    };
    for group in &schema.unique {
        if let [name] = group.as_slice() {
            push(name);
        }
    }
    for foreign_key in &schema.foreign_keys {
        if let [name] = foreign_key.columns.as_slice() {
            push(name);
        }
    }
    names.sort();
    names
        .into_iter()
        .filter(|name| !schema.primary_key.contains(name))
        .filter_map(|name| {
            let column = columns.iter().find(|column| column.name == name)?;
            matches!(
                column.column_type,
                SchemaColumnType::String | SchemaColumnType::Integer
            )
            .then(|| SchemaIndexedColumn {
                name,
                ordinal: 0,
                column_type: column.column_type,
            })
        })
        .enumerate()
        .map(|(ordinal, column)| SchemaIndexedColumn {
            ordinal: u16::try_from(ordinal).unwrap_or(u16::MAX),
            ..column
        })
        .collect()
}

fn postgres_literal(value: &JsonValue) -> String {
    match value {
        JsonValue::Null => "NULL".to_string(),
        JsonValue::Bool(value) => value.to_string().to_ascii_uppercase(),
        JsonValue::Number(value) => value.to_string(),
        JsonValue::String(value) => format!("'{}'", value.replace('\'', "''")),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            format!("'{}'::jsonb", value.to_string().replace('\'', "''"))
        }
    }
}

pub(crate) fn schema_exposed_as_schema_surface(schema_key: &str) -> bool {
    !matches!(
        schema_key,
        "lix_binary_blob_ref"
            | "lix_branch_descriptor"
            | "lix_branch_ref"
            | "lix_change"
            | "lix_directory_descriptor"
            | "lix_file_descriptor"
            | "lix_undo_redo_marker"
            | "lix_collection_generation"
    )
}

pub(crate) fn schema_exposed_as_history_surface(schema_key: &str) -> bool {
    schema_exposed_as_schema_surface(schema_key)
        && schema_key != "lix_commit"
}

pub(crate) fn schema_surface_schema(
    spec: &SchemaSurfaceSpec,
    shape: SchemaSurfaceShape,
) -> SchemaRef {
    let history_identity_roots = if shape == SchemaSurfaceShape::History {
        spec.primary_key_paths
            .iter()
            .filter_map(|path| path.first())
            .cloned()
            .collect::<BTreeSet<_>>()
    } else {
        BTreeSet::new()
    };
    let mut fields = spec
        .columns
        .iter()
        .map(|column| {
            let read_nullable = if shape == SchemaSurfaceShape::History {
                !history_identity_roots.contains(&column.name)
            } else {
                column.read_nullable
            };
            let field = Field::new(
                &column.name,
                arrow_data_type_for_schema_column_type(column.column_type),
                read_nullable,
            );
            if column.column_type == SchemaColumnType::Jsonb {
                mark_json_field(field)
            } else {
                field
            }
        })
        .collect::<Vec<_>>();

    fields.extend(row_system_fields(shape));
    Arc::new(Schema::new(fields))
}

pub(crate) fn row_visible_fields(spec: &SchemaSurfaceSpec) -> Vec<Field> {
    let primary_key_ordinals = spec
        .primary_key_paths
        .iter()
        .enumerate()
        .filter_map(|(ordinal, path)| path.first().map(|name| (name.as_str(), ordinal)))
        .collect::<std::collections::HashMap<_, _>>();
    schema_surface_schema(spec, SchemaSurfaceShape::Active).fields()[..spec.columns.len()]
        .iter()
        .zip(&spec.columns)
        .map(|(field, column)| {
            let mut metadata = field.metadata().clone();
            metadata.insert(
                "lix.schema_v1.type".to_owned(),
                column.native_type.postgres_name().to_owned(),
            );
            if let Some(ordinal) = primary_key_ordinals.get(column.name.as_str()) {
                metadata.insert(
                    "lix.schema_v1.primary_key_ordinal".to_owned(),
                    ordinal.to_string(),
                );
            }
            field.as_ref().clone().with_metadata(metadata)
        })
        .collect()
}

pub(crate) fn row_system_fields(shape: SchemaSurfaceShape) -> Vec<Field> {
    if shape == SchemaSurfaceShape::History {
        return vec![
            json_field(HISTORY_COL_ROW_PK, false),
            Field::new(HISTORY_COL_SCHEMA_KEY, DataType::Utf8, false),
            Field::new(HISTORY_COL_FILE_ID, DataType::Utf8, true),
            json_field(HISTORY_COL_METADATA, true),
            Field::new(HISTORY_COL_CHANGE_ID, DataType::Utf8, false),
            Field::new(HISTORY_COL_CHANGE_CREATED_AT, DataType::Utf8, false),
            Field::new(HISTORY_COL_ORIGIN_KEY, DataType::Utf8, true),
            Field::new(HISTORY_COL_OBSERVED_COMMIT_ID, DataType::Utf8, false),
            Field::new(HISTORY_COL_COMMIT_CREATED_AT, DataType::Utf8, false),
            Field::new(HISTORY_COL_AS_OF_COMMIT_ID, DataType::Utf8, false),
            Field::new(HISTORY_COL_DEPTH, DataType::Int64, false),
            Field::new(HISTORY_COL_IS_DELETED, DataType::Boolean, false),
        ];
    }

    vec![
        json_field("lixcol_row_pk", true),
        Field::new("lixcol_schema_key", DataType::Utf8, false),
        Field::new("lixcol_file_id", DataType::Utf8, true),
        json_field("lixcol_metadata", true),
        Field::new("lixcol_created_at", DataType::Utf8, true),
        Field::new("lixcol_updated_at", DataType::Utf8, true),
        Field::new("lixcol_global", DataType::Boolean, true),
        Field::new("lixcol_change_id", DataType::Utf8, true),
        Field::new("lixcol_commit_id", DataType::Utf8, true),
        Field::new("lixcol_untracked", DataType::Boolean, true),
    ]
}

fn arrow_data_type_for_schema_column_type(column_type: SchemaColumnType) -> DataType {
    match column_type {
        SchemaColumnType::String | SchemaColumnType::Jsonb => DataType::Utf8,
        SchemaColumnType::Integer => DataType::Int64,
        SchemaColumnType::Number => DataType::Float64,
        SchemaColumnType::Boolean => DataType::Boolean,
        SchemaColumnType::Timestamptz => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        SchemaSurfaceShape, derive_schema_surface_spec_from_schema, schema_surface_schema,
    };

    fn path_value_schema(value_type: &str) -> serde_json::Value {
        json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "arbitrary_name",
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": value_type, "nullable": false }
            ],
            "primary_key": ["path"]
        })
    }

    #[test]
    fn certifies_complete_path_value_rows_when_value_accepts_all_json() {
        let spec = derive_schema_surface_spec_from_schema(&path_value_schema("jsonb"))
            .expect("schema should derive");

        assert!(spec.certifies_path_value_replacement);
    }

    #[test]
    fn columnar_snapshot_certificate_requires_exact_reversible_columns() {
        let strings = derive_schema_surface_spec_from_schema(&path_value_schema("text"))
            .expect("string schema should derive");
        assert!(strings.columnar_snapshot_bijective);

        let numbers = derive_schema_surface_spec_from_schema(&path_value_schema("float8"))
            .expect("number schema should derive");
        assert!(!numbers.columnar_snapshot_bijective);

        let mut reserved = path_value_schema("text");
        reserved["columns"]
            .as_array_mut()
            .expect("columns")
            .push(json!({ "name": "lixcol_user_value", "type": "text", "nullable": true }));
        let reserved = derive_schema_surface_spec_from_schema(&reserved)
            .expect("reserved-name schema should still derive");
        assert!(!reserved.columnar_snapshot_bijective);
    }

    #[test]
    fn does_not_certify_path_value_rows_with_value_constraints() {
        let schema = path_value_schema("text");
        let spec = derive_schema_surface_spec_from_schema(&schema).expect("schema should derive");
        assert!(!spec.certifies_path_value_replacement);
    }

    #[test]
    fn history_primary_key_columns_are_non_null() {
        let spec = derive_schema_surface_spec_from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "localized_document",
            "columns": [
                { "name": "tenant", "type": "text", "nullable": false },
                { "name": "id", "type": "text", "nullable": false },
                { "name": "locale", "type": "text", "nullable": false },
                { "name": "body", "type": "text", "nullable": false },
            ],
            "primary_key": ["tenant", "id", "locale"],
        }))
        .expect("schema should derive");

        let history = schema_surface_schema(&spec, SchemaSurfaceShape::History);
        assert!(
            !history
                .field_with_name("tenant")
                .expect("first identity column")
                .is_nullable()
        );
        assert!(
            !history
                .field_with_name("locale")
                .expect("top-level identity")
                .is_nullable()
        );
        assert!(
            history
                .field_with_name("body")
                .expect("payload")
                .is_nullable()
        );

        let active = schema_surface_schema(&spec, SchemaSurfaceShape::Active);
        assert!(
            !active
                .field_with_name("tenant")
                .expect("active identity input")
                .is_nullable(),
            "read nullability is independent from omission/default input semantics"
        );
    }

    #[test]
    fn columnar_layout_fingerprint_distinguishes_string_from_json_utf8() {
        let string_spec = derive_schema_surface_spec_from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "payload",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        }))
        .expect("string spec");
        let json_spec = derive_schema_surface_spec_from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "payload",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "value", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["id"],
        }))
        .expect("json spec");

        assert_ne!(
            string_spec.columnar_layout_fingerprint(),
            json_spec.columnar_layout_fingerprint()
        );
        assert_eq!(
            string_spec.columnar_layout_fingerprint(),
            string_spec.clone().columnar_layout_fingerprint()
        );
    }

    /// The load-bearing bridge under every certificate fast path: a column is
    /// only indexable because the schema declared it through `x-lix-unique` or
    /// `x-lix-foreign-keys`, and those are exactly the declarations that set
    /// `has_inter_row_constraints`.
    ///
    /// Four write certificates in `bound_public_write.rs` decline outright on
    /// `has_inter_row_constraints`. This implication is what turns those four
    /// bails into a proof that no row carrying an indexed column can reach
    /// commit without passing through transaction validation, where the hot
    /// index values are extracted.
    #[test]
    fn indexed_columns_imply_inter_row_constraints() {
        let cases = [
            json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "bypass_pk_only",
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                    { "name": "payload", "type": "text", "nullable": true },
                ],
                "primary_key": ["id"],
            }),
            json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "bypass_unique",
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                    { "name": "slug", "type": "text", "nullable": true },
                ],
                "primary_key": ["id"],
                "unique": [["slug"]],
            }),
            json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "bypass_fk",
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                    { "name": "parent_id", "type": "text", "nullable": true },
                ],
                "primary_key": ["id"],
                "foreign_keys": [{
                    "columns": ["parent_id"],
                    "references": { "schema_key": "bypass_pk_only", "columns": ["id"] }
                }],
            }),
            json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "bypass_composite_unique",
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                    { "name": "a", "type": "text", "nullable": true },
                    { "name": "b", "type": "text", "nullable": true },
                ],
                "primary_key": ["id"],
                "unique": [["a", "b"]],
            }),
        ];
        for schema in cases {
            let spec = derive_schema_surface_spec_from_schema(&schema).expect("spec");
            assert!(
                spec.indexed_columns.is_empty() || spec.has_inter_row_constraints,
                "{} declares indexed columns without inter-row constraints",
                spec.schema_key
            );
        }

        let unique = derive_schema_surface_spec_from_schema(&json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "bypass_unique",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "slug", "type": "text", "nullable": true },
            ],
            "primary_key": ["id"],
            "unique": [["slug"]],
        }))
        .expect("spec");
        assert_eq!(
            unique.indexed_columns.len(),
            1,
            "a single-column unique group is the indexable shape this relies on"
        );
    }
}
