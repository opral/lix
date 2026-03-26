use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const TRACKED_LIVE_TABLE_PREFIX: &str = "lix_internal_live_v1_";

use crate::backend::QueryExecutor;
use crate::schema::builtin::builtin_schema_definition;
use crate::schema::registry::compile_registered_live_layout;
use crate::{LixError, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveColumnKind {
    String,
    Integer,
    Number,
    Boolean,
    JsonText,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveColumnSpec {
    pub(crate) property_name: String,
    pub(crate) column_name: String,
    pub(crate) kind: LiveColumnKind,
    pub(crate) required: bool,
    pub(crate) nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveTableLayout {
    pub(crate) schema_key: String,
    pub(crate) columns: Vec<LiveColumnSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveRowAccess {
    layout: LiveTableLayout,
}

impl LiveColumnSpec {
    pub(crate) fn value_from_snapshot(
        &self,
        snapshot: Option<&JsonValue>,
    ) -> Result<Value, LixError> {
        let Some(snapshot) = snapshot else {
            return Ok(Value::Null);
        };
        let Some(value) = snapshot.get(&self.property_name) else {
            return Ok(Value::Null);
        };
        match self.kind {
            LiveColumnKind::String => Ok(value
                .as_str()
                .map(|text| Value::Text(text.to_string()))
                .unwrap_or(Value::Null)),
            LiveColumnKind::Integer => {
                Ok(value.as_i64().map(Value::Integer).unwrap_or(Value::Null))
            }
            LiveColumnKind::Number => Ok(value.as_f64().map(Value::Real).unwrap_or(Value::Null)),
            LiveColumnKind::Boolean => {
                Ok(value.as_bool().map(Value::Boolean).unwrap_or(Value::Null))
            }
            LiveColumnKind::JsonText => {
                serde_json::to_string(value)
                    .map(Value::Text)
                    .map_err(|error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            &format!(
                            "failed to serialize live JSON column '{}' for schema '{}': {error}",
                            self.column_name, self.property_name
                        ),
                        )
                    })
            }
        }
    }

    pub(crate) fn preserve_null_in_logical_snapshot(&self) -> bool {
        self.required && self.nullable
    }
}

pub(crate) fn builtin_live_table_layout(
    schema_key: &str,
) -> Result<Option<LiveTableLayout>, LixError> {
    let Some(schema) = builtin_schema_definition(schema_key) else {
        return Ok(None);
    };
    Ok(Some(live_table_layout_from_schema(schema)?))
}

pub(crate) fn live_table_layout_from_schema(
    schema: &JsonValue,
) -> Result<LiveTableLayout, LixError> {
    let schema_key = schema
        .get("x-lix-key")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "schema is missing string x-lix-key for live table layout",
            )
        })?;

    let mut columns = Vec::new();
    let mut seen_columns = BTreeSet::new();
    let required_properties = schema
        .get("required")
        .and_then(JsonValue::as_array)
        .map(|required| {
            required
                .iter()
                .filter_map(JsonValue::as_str)
                .map(str::to_string)
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    if let Some(properties) = schema.get("properties").and_then(JsonValue::as_object) {
        let mut ordered = properties.iter().collect::<Vec<_>>();
        ordered.sort_by(|(left, _), (right, _)| left.cmp(right));
        for (property_name, property_schema) in ordered {
            if property_name.starts_with("lixcol_") {
                continue;
            }
            let Some(kind) = live_column_kind_from_schema(property_schema) else {
                continue;
            };
            let column_name = live_column_name(property_name, kind);
            if !seen_columns.insert(column_name.clone()) {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    &format!(
                        "duplicate normalized live column '{}' for schema '{}'",
                        column_name, schema_key
                    ),
                ));
            }
            columns.push(LiveColumnSpec {
                property_name: property_name.clone(),
                column_name,
                kind,
                required: required_properties.contains(property_name.as_str()),
                nullable: schema_property_is_nullable(property_schema),
            });
        }
    }

    Ok(LiveTableLayout {
        schema_key: schema_key.to_string(),
        columns,
    })
}

pub(crate) fn merge_live_table_layouts<I>(
    schema_key: &str,
    layouts: I,
) -> Result<LiveTableLayout, LixError>
where
    I: IntoIterator<Item = LiveTableLayout>,
{
    let mut columns_by_name = BTreeMap::<String, LiveColumnSpec>::new();
    for layout in layouts {
        if layout.schema_key != schema_key {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                &format!(
                    "cannot merge live layouts for '{}' with foreign schema '{}'",
                    schema_key, layout.schema_key
                ),
            ));
        }
        for column in layout.columns {
            match columns_by_name.get(&column.column_name) {
                Some(existing)
                    if existing.kind != column.kind
                        || existing.property_name != column.property_name
                        || existing.required != column.required
                        || existing.nullable != column.nullable =>
                {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        &format!(
                            "schema '{}' has incompatible normalized live column '{}'",
                            schema_key, column.column_name
                        ),
                    ));
                }
                Some(_) => {}
                None => {
                    columns_by_name.insert(column.column_name.clone(), column);
                }
            }
        }
    }

    Ok(LiveTableLayout {
        schema_key: schema_key.to_string(),
        columns: columns_by_name.into_values().collect(),
    })
}

impl LiveRowAccess {
    pub(crate) fn new(layout: LiveTableLayout) -> Self {
        Self { layout }
    }

    pub(crate) fn normalized_projection_sql(&self, table_alias: Option<&str>) -> String {
        render_normalized_live_projection_sql(&self.layout, table_alias)
    }

    pub(crate) fn layout(&self) -> &LiveTableLayout {
        &self.layout
    }

    #[cfg(test)]
    pub(crate) fn logical_snapshot_from_row(
        &self,
        row: &[Value],
        normalized_start_index: usize,
    ) -> Result<Option<JsonValue>, LixError> {
        logical_live_snapshot_from_legacy_layout(
            &self.layout,
            row,
            normalized_start_index,
        )
    }

    #[cfg(test)]
    pub(crate) fn logical_snapshot_text_from_row(
        &self,
        row: &[Value],
        normalized_start_index: usize,
    ) -> Result<Option<String>, LixError> {
        let Some(snapshot) = self.logical_snapshot_from_row(row, normalized_start_index)? else {
            return Ok(None);
        };
        serde_json::to_string(&snapshot).map(Some).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                &format!(
                    "failed to serialize logical live snapshot for schema '{}': {error}",
                    self.layout.schema_key
                ),
            )
        })
    }
}

pub(crate) async fn load_live_table_layout_with_executor(
    executor: &mut dyn QueryExecutor,
    schema_key: &str,
) -> Result<LiveTableLayout, LixError> {
    if let Some(layout) = builtin_live_table_layout(schema_key)? {
        return Ok(layout);
    }
    let result = executor
        .execute(REGISTERED_SCHEMA_BOOTSTRAP_LAYOUT_SQL, &[])
        .await?;
    compile_registered_live_layout(schema_key, result.rows)
}

fn live_envelope_column_names() -> BTreeSet<&'static str> {
    BTreeSet::from([
        "entity_id",
        "schema_key",
        "schema_version",
        "file_id",
        "version_id",
        "global",
        "plugin_key",
        "snapshot_content",
        "change_id",
        "metadata",
        "writer_key",
        "is_tombstone",
        "created_at",
        "updated_at",
        "untracked",
    ])
}

pub(crate) fn parse_snapshot_object(
    snapshot_content: Option<&str>,
    schema_key: &str,
) -> Result<Option<JsonValue>, LixError> {
    let Some(snapshot_content) = snapshot_content else {
        return Ok(None);
    };
    let snapshot = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!(
                "live table normalization failed to parse snapshot_content for schema '{}': {error}",
                schema_key
            ),
        )
    })?;
    if !snapshot.is_object() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            &format!(
                "live table normalization requires object snapshot_content for schema '{}'",
                schema_key
            ),
        ));
    }
    Ok(Some(snapshot))
}

pub(crate) fn normalized_live_column_values(
    layout: &LiveTableLayout,
    snapshot_content: Option<&str>,
) -> Result<BTreeMap<String, Value>, LixError> {
    let snapshot = parse_snapshot_object(snapshot_content, &layout.schema_key)?;
    let mut values = BTreeMap::new();
    for column in &layout.columns {
        values.insert(
            column.column_name.clone(),
            column.value_from_snapshot(snapshot.as_ref())?,
        );
    }
    Ok(values)
}

pub(crate) fn render_normalized_live_projection_sql(
    layout: &LiveTableLayout,
    table_alias: Option<&str>,
) -> String {
    if layout.columns.is_empty() {
        return String::new();
    }

    format!(
        ", {}",
        layout
            .columns
            .iter()
            .map(|column| qualified_column_ref(table_alias, &column.column_name))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

pub(crate) fn tracked_live_table_name(schema_key: &str) -> String {
    format!("{TRACKED_LIVE_TABLE_PREFIX}{schema_key}")
}

pub(crate) fn untracked_live_table_name(schema_key: &str) -> String {
    tracked_live_table_name(schema_key)
}

pub(crate) fn is_untracked_live_table(_name: &str) -> bool {
    false
}

pub(crate) fn live_column_name_for_property<'a>(
    layout: &'a LiveTableLayout,
    property_name: &str,
) -> Option<&'a str> {
    layout
        .columns
        .iter()
        .find(|column| column.property_name == property_name)
        .map(|column| column.column_name.as_str())
}

fn live_column_kind_from_schema(schema: &JsonValue) -> Option<LiveColumnKind> {
    let types = match schema.get("type") {
        Some(JsonValue::String(kind)) => vec![kind.as_str()],
        Some(JsonValue::Array(kinds)) => kinds
            .iter()
            .filter_map(JsonValue::as_str)
            .collect::<Vec<_>>(),
        _ => return Some(LiveColumnKind::JsonText),
    };

    if types.iter().any(|kind| *kind == "boolean") {
        return Some(LiveColumnKind::Boolean);
    }
    if types.iter().any(|kind| *kind == "integer") {
        return Some(LiveColumnKind::Integer);
    }
    if types.iter().any(|kind| *kind == "number") {
        return Some(LiveColumnKind::Number);
    }
    if types.iter().any(|kind| *kind == "string") {
        return Some(LiveColumnKind::String);
    }
    if types.iter().any(|kind| matches!(*kind, "object" | "array")) {
        return Some(LiveColumnKind::JsonText);
    }
    None
}

fn schema_property_is_nullable(schema: &JsonValue) -> bool {
    if schema.is_null() {
        return true;
    }
    if schema
        .get("nullable")
        .and_then(JsonValue::as_bool)
        .unwrap_or(false)
    {
        return true;
    }
    match schema.get("type") {
        Some(JsonValue::String(kind)) if kind == "null" => return true,
        Some(JsonValue::Array(kinds)) => {
            if kinds.iter().any(|kind| kind.as_str() == Some("null")) {
                return true;
            }
        }
        _ => {}
    }
    if schema
        .get("const")
        .map(JsonValue::is_null)
        .unwrap_or(false)
    {
        return true;
    }
    ["anyOf", "oneOf"]
        .into_iter()
        .filter_map(|key| schema.get(key).and_then(JsonValue::as_array))
        .flatten()
        .any(schema_property_is_nullable)
}

fn live_column_name(property_name: &str, kind: LiveColumnKind) -> String {
    let base = match kind {
        LiveColumnKind::JsonText => format!("{property_name}_json"),
        _ => property_name.to_string(),
    };

    if live_envelope_column_names().contains(base.as_str()) {
        format!("{base}_value")
    } else {
        base
    }
}

fn qualified_column_ref(table_alias: Option<&str>, column_name: &str) -> String {
    match table_alias {
        Some(alias) => format!(
            "{}.{}",
            quote_ident_fragment(alias),
            quote_ident_fragment(column_name)
        ),
        None => quote_ident_fragment(column_name),
    }
}

fn quote_ident_fragment(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

#[cfg(test)]
fn logical_live_snapshot_from_legacy_layout(
    layout: &LiveTableLayout,
    row: &[Value],
    normalized_start_index: usize,
) -> Result<Option<JsonValue>, LixError> {
    let mut object = serde_json::Map::new();
    for (offset, column) in layout.columns.iter().enumerate() {
        let value = row.get(normalized_start_index + offset).ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                &format!(
                    "normalized live row for schema '{}' missing returning column '{}'",
                    layout.schema_key, column.column_name
                ),
            )
        })?;
        let json_value = match column.kind {
            LiveColumnKind::String => match value {
                Value::Null => JsonValue::Null,
                Value::Text(text) => JsonValue::String(text.clone()),
                other => {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        &format!(
                            "normalized live row for schema '{}' expected TEXT for column '{}', got {other:?}",
                            layout.schema_key, column.column_name
                        ),
                    ))
                }
            },
            LiveColumnKind::Integer => match value {
                Value::Null => JsonValue::Null,
                Value::Integer(number) => JsonValue::from(*number),
                other => {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        &format!(
                            "normalized live row for schema '{}' expected INTEGER for column '{}', got {other:?}",
                            layout.schema_key, column.column_name
                        ),
                    ))
                }
            },
            LiveColumnKind::Number => match value {
                Value::Null => JsonValue::Null,
                Value::Real(number) => JsonValue::from(*number),
                Value::Integer(number) => JsonValue::from(*number),
                other => {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        &format!(
                            "normalized live row for schema '{}' expected REAL for column '{}', got {other:?}",
                            layout.schema_key, column.column_name
                        ),
                    ))
                }
            },
            LiveColumnKind::Boolean => match value {
                Value::Null => JsonValue::Null,
                Value::Boolean(boolean) => JsonValue::from(*boolean),
                Value::Integer(number) if *number == 0 || *number == 1 => {
                    JsonValue::from(*number == 1)
                }
                other => {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        &format!(
                            "normalized live row for schema '{}' expected BOOLEAN for column '{}', got {other:?}",
                            layout.schema_key, column.column_name
                        ),
                    ))
                }
            },
            LiveColumnKind::JsonText => match value {
                Value::Null => JsonValue::Null,
                Value::Text(text) => serde_json::from_str(text).map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        &format!(
                            "normalized live row for schema '{}' returned invalid JSON text for column '{}': {error}",
                            layout.schema_key, column.column_name
                        ),
                    )
                })?,
                Value::Json(value) => value.clone(),
                other => {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        &format!(
                            "normalized live row for schema '{}' expected JSON text for column '{}', got {other:?}",
                            layout.schema_key, column.column_name
                        ),
                    ))
                }
            },
        };

        if json_value.is_null() {
            if column.preserve_null_in_logical_snapshot() {
                object.insert(column.property_name.clone(), JsonValue::Null);
            }
        } else {
            object.insert(column.property_name.clone(), json_value);
        }
    }

    Ok(Some(JsonValue::Object(object)))
}

const REGISTERED_SCHEMA_BOOTSTRAP_LAYOUT_SQL: &str = "SELECT snapshot_content \
     FROM lix_internal_registered_schema_bootstrap \
     WHERE schema_key = 'lix_registered_schema' \
       AND version_id = 'global' \
       AND is_tombstone = 0 \
       AND snapshot_content IS NOT NULL";

#[cfg(test)]
mod tests {
    use super::builtin_live_table_layout;

    #[test]
    fn registered_schema_layout_includes_value_json() {
        let layout = builtin_live_table_layout("lix_registered_schema")
            .expect("layout should compile")
            .expect("builtin schema should exist");
        assert!(
            layout
                .columns
                .iter()
                .any(|column| column.column_name == "value_json"),
            "expected value_json in layout, got {:?}",
            layout
                .columns
                .iter()
                .map(|column| column.column_name.as_str())
                .collect::<Vec<_>>()
        );
    }
}
