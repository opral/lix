use std::collections::BTreeMap;
use std::ops::Index;
use std::sync::Arc;

use serde_json::Value as JsonValue;

use crate::{DataType, Error, ErrorKind, Jsonb, Schema};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Text(String),
    Uuid(uuid::Uuid),
    Int8(i64),
    Float8(f64),
    Boolean(bool),
    /// JSON null is represented as `Jsonb(Value::Null)`, not SQL NULL.
    Jsonb(Jsonb),
    /// Signed UTC microseconds since the Unix epoch.
    Timestamptz(i64),
}

/// A complete Schema v1 row in canonical column-name order.
///
/// Column names are immutable shared strings so all records decoded from one
/// typed page can retain the page's single schema layout without allocating a
/// fresh name for every value. Lookups remain logarithmic and iteration is
/// deterministic in the same lexical order as a `BTreeMap`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Row {
    entries: Vec<(Arc<str>, Value)>,
}

impl Row {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[inline]
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.find(name).ok().map(|index| &self.entries[index].1)
    }

    pub fn get_mut(&mut self, name: &str) -> Option<&mut Value> {
        self.find(name).ok().map(|index| &mut self.entries[index].1)
    }

    pub fn contains_key(&self, name: &str) -> bool {
        self.find(name).is_ok()
    }

    pub fn insert(&mut self, name: impl Into<Arc<str>>, value: Value) -> Option<Value> {
        let name = name.into();
        match self.find(&name) {
            Ok(index) => Some(std::mem::replace(&mut self.entries[index].1, value)),
            Err(index) => {
                self.entries.insert(index, (name, value));
                None
            }
        }
    }

    pub fn remove(&mut self, name: &str) -> Option<Value> {
        self.find(name)
            .ok()
            .map(|index| self.entries.remove(index).1)
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&str, &Value)> + ExactSizeIterator {
        self.entries
            .iter()
            .map(|(name, value)| (name.as_ref(), value))
    }

    pub fn keys(&self) -> impl DoubleEndedIterator<Item = &str> + ExactSizeIterator {
        self.entries.iter().map(|(name, _)| name.as_ref())
    }

    /// Returns the immutable shared column layout used by typed page builders.
    #[doc(hidden)]
    pub fn shared_keys(&self) -> impl DoubleEndedIterator<Item = &Arc<str>> + ExactSizeIterator {
        self.entries.iter().map(|(name, _)| name)
    }

    pub fn values(&self) -> impl DoubleEndedIterator<Item = &Value> + ExactSizeIterator {
        self.entries.iter().map(|(_, value)| value)
    }

    /// Constructs a row from a page layout already proven to be strictly
    /// ordered and unique.
    #[doc(hidden)]
    pub fn from_sorted_entries(entries: Vec<(Arc<str>, Value)>) -> Result<Self, &'static str> {
        if entries
            .windows(2)
            .any(|pair| pair[0].0.as_ref() >= pair[1].0.as_ref())
        {
            return Err("typed row columns are not strictly ordered");
        }
        Ok(Self { entries })
    }

    #[inline]
    fn find(&self, name: &str) -> Result<usize, usize> {
        self.entries
            .binary_search_by(|(candidate, _)| candidate.as_ref().cmp(name))
    }
}

impl<K, const N: usize> From<[(K, Value); N]> for Row
where
    K: Into<Arc<str>>,
{
    fn from(entries: [(K, Value); N]) -> Self {
        entries.into_iter().collect()
    }
}

impl<K> FromIterator<(K, Value)> for Row
where
    K: Into<Arc<str>>,
{
    fn from_iter<T: IntoIterator<Item = (K, Value)>>(entries: T) -> Self {
        let entries = entries.into_iter();
        let mut row = Self::with_capacity(entries.size_hint().0);
        for (name, value) in entries {
            row.insert(name, value);
        }
        row
    }
}

impl<'a> IntoIterator for &'a Row {
    type Item = (&'a str, &'a Value);
    type IntoIter = std::iter::Map<
        std::slice::Iter<'a, (Arc<str>, Value)>,
        fn(&(Arc<str>, Value)) -> (&str, &Value),
    >;

    fn into_iter(self) -> Self::IntoIter {
        fn entry((name, value): &(Arc<str>, Value)) -> (&str, &Value) {
            (name.as_ref(), value)
        }
        self.entries.iter().map(entry)
    }
}

impl IntoIterator for Row {
    type Item = (Arc<str>, Value);
    type IntoIter = std::vec::IntoIter<(Arc<str>, Value)>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

impl Index<&str> for Row {
    type Output = Value;

    fn index(&self, name: &str) -> &Self::Output {
        self.get(name).expect("typed row column is missing")
    }
}

#[derive(Debug, Clone)]
pub struct CompiledSchema {
    columns: BTreeMap<String, CompiledColumn>,
    ordered_columns: Vec<(String, CompiledColumn)>,
    primary_key: Vec<String>,
}

#[derive(Debug, Clone)]
struct CompiledColumn {
    data_type: DataType,
    nullable: bool,
    has_default: bool,
    default_value: Option<JsonValue>,
    default_expression: Option<String>,
}

impl CompiledSchema {
    pub fn compile(schema: &Schema) -> Result<Self, Error> {
        schema.validate()?;
        Ok(Self {
            columns: schema
                .columns
                .iter()
                .map(|column| {
                    (
                        column.name.clone(),
                        CompiledColumn {
                            data_type: column.data_type,
                            nullable: column.nullable,
                            has_default: column.default_value.is_some()
                                || column.default_expression.is_some(),
                            default_value: column.default_value.clone(),
                            default_expression: column.default_expression.clone(),
                        },
                    )
                })
                .collect(),
            ordered_columns: schema
                .columns
                .iter()
                .map(|column| {
                    (
                        column.name.clone(),
                        CompiledColumn {
                            data_type: column.data_type,
                            nullable: column.nullable,
                            has_default: column.default_value.is_some()
                                || column.default_expression.is_some(),
                            default_value: column.default_value.clone(),
                            default_expression: column.default_expression.clone(),
                        },
                    )
                })
                .collect(),
            primary_key: schema.primary_key.clone(),
        })
    }

    /// Columns that identify a row, in schema declaration order.
    pub fn primary_key(&self) -> &[String] {
        &self.primary_key
    }

    /// Declared columns in the canonical Schema v1 storage order.
    pub fn canonical_columns(&self) -> impl ExactSizeIterator<Item = &str> {
        self.ordered_columns.iter().map(|(name, _)| name.as_str())
    }

    /// Declared native type for one row column.
    pub fn column_type(&self, name: &str) -> Option<DataType> {
        self.columns.get(name).map(|column| column.data_type)
    }

    /// Whether a declared column accepts SQL `NULL`.
    pub fn column_nullable(&self, name: &str) -> Option<bool> {
        self.columns.get(name).map(|column| column.nullable)
    }

    /// Materializes Schema v1 defaults directly into a native row.
    pub fn apply_defaults(
        &self,
        row: &mut Row,
        mut uuid_v7: impl FnMut() -> uuid::Uuid,
        mut current_timestamp_micros: impl FnMut() -> i64,
    ) -> Result<bool, Error> {
        let mut changed = false;
        for (name, column) in &self.ordered_columns {
            if row.contains_key(name) {
                continue;
            }
            let value = if let Some(value) = &column.default_value {
                json_value(column.data_type, value, name)?
            } else {
                match column.default_expression.as_deref() {
                    Some("uuidv7()") => Value::Uuid(uuid_v7()),
                    Some("CURRENT_TIMESTAMP") => Value::Timestamptz(current_timestamp_micros()),
                    None => continue,
                    Some(expression) => {
                        return row_error(
                            format!("/{name}"),
                            format!("unsupported default expression '{expression}'"),
                        );
                    }
                }
            };
            row.insert(name.clone(), value);
            changed = true;
        }
        Ok(changed)
    }

    /// Returns true when applying this schema would materialize at least one
    /// missing literal or expression default.
    pub fn defaults_would_apply(&self, row: &Row) -> bool {
        self.ordered_columns
            .iter()
            .any(|(name, column)| column.has_default && !row.contains_key(name))
    }

    /// Materializes omitted nullable columns as explicit SQL `NULL` values.
    ///
    /// Public SQL INSERT permits an omitted nullable column, while the native
    /// durable row format requires every declared column to be present. This
    /// closes that representation gap without inventing JSON snapshots or
    /// weakening complete-row validation at the plugin boundary.
    pub fn materialize_missing_nullable_columns(&self, row: &mut Row) -> bool {
        let mut changed = false;
        for (name, column) in &self.ordered_columns {
            if column.nullable && !row.contains_key(name) {
                row.insert(name.clone(), Value::Null);
                changed = true;
            }
        }
        changed
    }

    /// Converts one already-normalized JSON object into its native Schema v1
    /// row representation.
    ///
    /// This is the engine ingress bridge for callers that still construct
    /// public row values as JSON. The returned row is complete and ready for
    /// durable typed encoding; omitted nullable columns are represented as
    /// explicit SQL `NULL` values.
    pub fn row_from_json(&self, value: &JsonValue) -> Result<Row, Error> {
        let object = value.as_object().ok_or_else(|| {
            Error::new(
                ErrorKind::Row,
                "/",
                "typed row source must be a JSON object",
            )
        })?;
        let mut row = Row::with_capacity(self.columns.len());
        for (name, value) in object {
            let column = self
                .columns
                .get(name)
                .ok_or_else(|| Error::new(ErrorKind::Row, format!("/{name}"), "unknown column"))?;
            if !json_value_matches(column.data_type, column.nullable, value) {
                return row_error(
                    format!("/{name}"),
                    format!(
                        "expected PostgreSQL type '{}'",
                        column.data_type.postgres_name()
                    ),
                );
            }
            // The historical JSON snapshot ingress treated a JSON `null` in
            // a nullable SQL column as SQL NULL, including JSONB columns.
            // Preserve that public SQL contract at the one JSON-to-native
            // boundary; native callers can still represent JSONB null
            // explicitly as `Value::Jsonb(Jsonb::null())`.
            let value = if value.is_null() && column.nullable {
                Value::Null
            } else {
                json_value(column.data_type, value, name)?
            };
            row.insert(name.clone(), value);
        }
        self.validate_row(&row)?;
        self.materialize_missing_nullable_columns(&mut row);
        self.validate_complete_row(&row)?;
        Ok(row)
    }

    /// Encodes the non-primary-key values using the canonical Schema v1 body
    /// layout. The primary-key values travel in the page identity envelope and
    /// are therefore intentionally omitted from the body.
    pub fn encode_body(&self, row: &Row) -> Result<Vec<u8>, Error> {
        self.validate_complete_row(row)?;
        let mut plan = Vec::new();
        let mut values = Vec::new();
        for (name, column) in &self.ordered_columns {
            if self.primary_key.iter().any(|key| key == name) {
                continue;
            }
            let value = match row.get(name) {
                Some(value) => value,
                None => {
                    return row_error(
                        format!("/{name}"),
                        "typed row body requires every declared column",
                    );
                }
            };
            plan.push(body_column(column));
            values.push(body_value(column.data_type, value, name)?);
        }
        let mut output = Vec::new();
        crate::value_layout::encode_body(&plan, &values, &mut output).map_err(|error| {
            Error::new(
                ErrorKind::Row,
                "/",
                format!("failed to encode typed row body: {error}"),
            )
        })?;
        Ok(output)
    }

    /// Decodes a canonical Schema v1 body and combines it with the already
    /// decoded primary-key values. The result is a complete typed row and is
    /// validated before it is returned.
    pub fn decode_body(&self, key: &Row, body: &[u8]) -> Result<Row, Error> {
        for name in &self.primary_key {
            if !key.contains_key(name) {
                return row_error(
                    format!("/{name}"),
                    "typed row identity is missing a primary-key value",
                );
            }
        }
        let mut plan = Vec::new();
        let mut names = Vec::new();
        for (name, column) in &self.ordered_columns {
            if self.primary_key.iter().any(|key| key == name) {
                continue;
            }
            plan.push(body_column(column));
            names.push(name);
        }
        let values = crate::value_layout::decode_body(&plan, body).map_err(|error| {
            Error::new(
                ErrorKind::Row,
                "/",
                format!("failed to decode typed row body: {error}"),
            )
        })?;
        let mut row = key.clone();
        for (name, value) in names.into_iter().zip(values) {
            row.insert(name.clone(), row_value(value));
        }
        self.validate_row(&row)?;
        Ok(row)
    }

    pub fn validate_row(&self, row: &Row) -> Result<(), Error> {
        for name in row.keys() {
            if !self.columns.contains_key(name) {
                return row_error(format!("/{name}"), "unknown column");
            }
        }
        for (name, column) in &self.columns {
            let Some(value) = row.get(name) else {
                if column.nullable || column.has_default {
                    continue;
                }
                return row_error(
                    format!("/{name}"),
                    "missing NOT NULL column without a default",
                );
            };
            if matches!(value, Value::Null) {
                if column.nullable {
                    continue;
                }
                return row_error(format!("/{name}"), "must not be SQL NULL");
            }
            if !value_matches(column.data_type, value) {
                return row_error(
                    format!("/{name}"),
                    format!(
                        "expected PostgreSQL type '{}'",
                        column.data_type.postgres_name()
                    ),
                );
            }
        }
        Ok(())
    }

    /// Validates a row after host-side defaults and generated identities have
    /// been materialized. A complete native row carries every declared
    /// column; SQL NULL is represented explicitly by [`Value::Null`].
    pub fn validate_complete_row(&self, row: &Row) -> Result<(), Error> {
        if row.len() == self.columns.len() {
            for ((actual_name, value), (name, column)) in row.iter().zip(&self.columns) {
                if actual_name != name {
                    return row_error(format!("/{actual_name}"), "unknown column");
                }
                validate_typed_column(name, column, value)?;
            }
            return Ok(());
        }
        for (name, column) in &self.columns {
            let value = row.get(name).ok_or_else(|| {
                Error::new(
                    ErrorKind::Row,
                    format!("/{name}"),
                    format!(
                        "complete typed row is missing {} column",
                        if column.nullable {
                            "nullable"
                        } else {
                            "non-null"
                        }
                    ),
                )
            })?;
            validate_typed_column(name, column, value)?;
        }
        if row.len() != self.columns.len()
            && let Some(name) = row.keys().find(|name| !self.columns.contains_key(*name))
        {
            return row_error(format!("/{name}"), "unknown column");
        }
        Ok(())
    }

    /// Validates a component create before the host materializes its generated
    /// identity. Every declared non-key column must already be present and
    /// valid; the only omissions allowed are primary-key columns with Schema
    /// v1 defaults. This proof can therefore survive insertion of those exact
    /// generated key values without revalidating the unchanged row body.
    pub fn validate_create_row(&self, row: &Row) -> Result<(), Error> {
        if row.len() == self.columns.len() {
            for ((actual_name, value), (name, column)) in row.iter().zip(&self.columns) {
                if actual_name != name {
                    return row_error(format!("/{actual_name}"), "unknown column");
                }
                validate_typed_column(name, column, value)?;
            }
            return Ok(());
        }
        let mut found = 0usize;
        for (name, column) in &self.columns {
            let Some(value) = row.get(name) else {
                if column.has_default && self.primary_key.iter().any(|key| key == name) {
                    continue;
                }
                return row_error(
                    format!("/{name}"),
                    "create row may omit only a defaulted primary-key column",
                );
            };
            found += 1;
            validate_typed_column(name, column, value)?;
        }
        if found != row.len()
            && let Some(name) = row.keys().find(|name| !self.columns.contains_key(*name))
        {
            return row_error(format!("/{name}"), "unknown column");
        }
        Ok(())
    }

    /// Validate the canonical JSON object used for a row snapshot.
    ///
    /// A JSON `null` in a JSONB column is a JSONB value. In every other column
    /// it represents SQL `NULL` and therefore requires a nullable column.
    pub fn validate(&self, value: &JsonValue) -> Result<(), Error> {
        let object = value
            .as_object()
            .ok_or_else(|| Error::new(ErrorKind::Row, "/", "row snapshot must be an object"))?;
        for name in object.keys() {
            if !self.columns.contains_key(name) {
                return row_error(format!("/{name}"), "unknown column");
            }
        }
        for (name, column) in &self.columns {
            let Some(value) = object.get(name) else {
                if column.nullable || column.has_default {
                    continue;
                }
                return row_error(
                    format!("/{name}"),
                    "missing NOT NULL column without a default",
                );
            };
            if !json_value_matches(column.data_type, column.nullable, value) {
                return row_error(
                    format!("/{name}"),
                    format!(
                        "expected PostgreSQL type '{}'{}",
                        column.data_type.postgres_name(),
                        if column.nullable { " or SQL NULL" } else { "" }
                    ),
                );
            }
        }
        Ok(())
    }

    pub fn is_valid(&self, value: &JsonValue) -> bool {
        self.validate(value).is_ok()
    }
}

fn value_matches(data_type: DataType, value: &Value) -> bool {
    match (data_type, value) {
        (DataType::Text, Value::Text(value)) => !value.contains('\0'),
        (DataType::Uuid, Value::Uuid(_))
        | (DataType::Int8, Value::Int8(_))
        | (DataType::Boolean, Value::Boolean(_)) => true,
        (DataType::Jsonb, Value::Jsonb(value)) => jsonb_value_valid(value),
        (DataType::Timestamptz, Value::Timestamptz(value)) => {
            chrono::DateTime::from_timestamp_micros(*value).is_some()
        }
        (DataType::Float8, Value::Float8(value)) => value.is_finite(),
        _ => false,
    }
}

fn validate_typed_column(name: &str, column: &CompiledColumn, value: &Value) -> Result<(), Error> {
    if matches!(value, Value::Null) {
        if column.nullable {
            return Ok(());
        }
        return row_error(format!("/{name}"), "must not be SQL NULL");
    }
    if !value_matches(column.data_type, value) {
        return row_error(
            format!("/{name}"),
            format!(
                "expected PostgreSQL type '{}'",
                column.data_type.postgres_name()
            ),
        );
    }
    Ok(())
}

fn jsonb_value_valid(value: &Jsonb) -> bool {
    value.is_valid()
}

fn body_column(column: &CompiledColumn) -> crate::value_layout::BodyColumn {
    crate::value_layout::BodyColumn {
        kind: match column.data_type {
            DataType::Text => crate::value_layout::BodyKind::Text,
            DataType::Uuid => crate::value_layout::BodyKind::Uuid,
            DataType::Int8 => crate::value_layout::BodyKind::Int8,
            DataType::Float8 => crate::value_layout::BodyKind::Float8,
            DataType::Boolean => crate::value_layout::BodyKind::Boolean,
            DataType::Jsonb => crate::value_layout::BodyKind::Jsonb,
            DataType::Timestamptz => crate::value_layout::BodyKind::Timestamptz,
        },
        nullable: column.nullable,
    }
}

fn body_value(
    data_type: DataType,
    value: &Value,
    name: &str,
) -> Result<crate::value_layout::BodyValue, Error> {
    use crate::value_layout::BodyValue;
    let value = match (data_type, value) {
        (_, Value::Null) => BodyValue::Null,
        (DataType::Text, Value::Text(value)) => BodyValue::Text(value.clone()),
        (DataType::Uuid, Value::Uuid(value)) => BodyValue::Uuid(*value),
        (DataType::Int8, Value::Int8(value)) => BodyValue::Int8(*value),
        (DataType::Float8, Value::Float8(value)) => BodyValue::Float8(*value),
        (DataType::Boolean, Value::Boolean(value)) => BodyValue::Boolean(*value),
        (DataType::Jsonb, Value::Jsonb(value)) => BodyValue::Jsonb(value.as_value().clone()),
        (DataType::Timestamptz, Value::Timestamptz(value)) => BodyValue::Timestamptz(*value),
        _ => {
            return row_error(
                format!("/{name}"),
                "typed row value does not match its schema column",
            );
        }
    };
    Ok(value)
}

fn row_value(value: crate::value_layout::BodyValue) -> Value {
    use crate::value_layout::BodyValue;
    match value {
        BodyValue::Null => Value::Null,
        BodyValue::Text(value) => Value::Text(value),
        BodyValue::Uuid(value) => Value::Uuid(value),
        BodyValue::Int8(value) => Value::Int8(value),
        BodyValue::Float8(value) => Value::Float8(value),
        BodyValue::Boolean(value) => Value::Boolean(value),
        BodyValue::Jsonb(value) => Value::Jsonb(value.into()),
        BodyValue::Timestamptz(value) => Value::Timestamptz(value),
    }
}

fn json_value_matches(data_type: DataType, nullable: bool, value: &JsonValue) -> bool {
    if value.is_null() {
        return data_type == DataType::Jsonb || nullable;
    }
    match data_type {
        DataType::Text => value.is_string(),
        DataType::Uuid => value
            .as_str()
            .is_some_and(|value| uuid::Uuid::parse_str(value).is_ok()),
        DataType::Int8 => value.as_i64().is_some(),
        DataType::Float8 => value.as_f64().is_some_and(f64::is_finite),
        DataType::Boolean => value.is_boolean(),
        DataType::Jsonb => true,
        DataType::Timestamptz => value.as_str().is_some_and(is_rfc3339_timestamp),
    }
}

fn json_value(data_type: DataType, value: &JsonValue, name: &str) -> Result<Value, Error> {
    if value.is_null() {
        return Ok(match data_type {
            DataType::Jsonb => Value::Jsonb(JsonValue::Null.into()),
            _ => Value::Null,
        });
    }
    let value = match data_type {
        DataType::Text => Value::Text(value.as_str().expect("validated text value").to_owned()),
        DataType::Uuid => Value::Uuid(
            uuid::Uuid::parse_str(value.as_str().expect("validated UUID value")).map_err(
                |error| Error::new(ErrorKind::Row, format!("/{name}"), error.to_string()),
            )?,
        ),
        DataType::Int8 => Value::Int8(value.as_i64().expect("validated int8 value")),
        DataType::Float8 => Value::Float8(value.as_f64().expect("validated float8 value")),
        DataType::Boolean => Value::Boolean(value.as_bool().expect("validated boolean value")),
        DataType::Jsonb => Value::Jsonb(value.clone().into()),
        DataType::Timestamptz => Value::Timestamptz(
            chrono::DateTime::parse_from_rfc3339(
                value.as_str().expect("validated timestamptz value"),
            )
            .map_err(|error| Error::new(ErrorKind::Row, format!("/{name}"), error.to_string()))?
            .timestamp_micros(),
        ),
    };
    Ok(value)
}

pub(crate) fn is_rfc3339_timestamp(value: &str) -> bool {
    chrono::DateTime::parse_from_rfc3339(value).is_ok()
}

fn row_error<T>(path: impl Into<String>, message: impl Into<String>) -> Result<T, Error> {
    Err(Error::new(ErrorKind::Row, path, message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_iteration_is_canonical_and_duplicate_insert_replaces() {
        let mut row = Row::from([
            ("z", Value::Int8(3)),
            ("a", Value::Int8(1)),
            ("m", Value::Int8(2)),
        ]);

        assert_eq!(row.keys().collect::<Vec<_>>(), ["a", "m", "z"]);
        assert_eq!(row.insert("m", Value::Int8(7)), Some(Value::Int8(2)));
        assert_eq!(row.get("m"), Some(&Value::Int8(7)));
        assert_eq!(row.len(), 3);
    }

    #[test]
    fn sorted_page_entries_reject_duplicate_or_noncanonical_names() {
        assert!(
            Row::from_sorted_entries(vec![
                (Arc::<str>::from("a"), Value::Null),
                (Arc::<str>::from("a"), Value::Null),
            ])
            .is_err()
        );
        assert!(
            Row::from_sorted_entries(vec![
                (Arc::<str>::from("b"), Value::Null),
                (Arc::<str>::from("a"), Value::Null),
            ])
            .is_err()
        );
    }

    #[test]
    fn rows_can_share_one_page_column_layout() {
        let column = Arc::<str>::from("document");
        let first = Row::from_sorted_entries(vec![(column.clone(), Value::Null)]).unwrap();
        let second = Row::from_sorted_entries(vec![(column, Value::Null)]).unwrap();

        assert!(Arc::ptr_eq(&first.entries[0].0, &second.entries[0].0));
    }

    #[test]
    fn sql_completion_materializes_omitted_nullable_columns() {
        let schema: Schema = serde_json::from_value(serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "nullable_probe",
            "columns": [
                {"name": "id", "type": "text", "nullable": false},
                {"name": "layout", "type": "jsonb", "nullable": true}
            ],
            "primary_key": ["id"]
        }))
        .expect("probe schema should decode");
        let compiled = CompiledSchema::compile(&schema).expect("probe schema should compile");
        let mut row = Row::from([("id", Value::Text("row-1".to_owned()))]);

        assert!(compiled.materialize_missing_nullable_columns(&mut row));
        assert_eq!(row.get("layout"), Some(&Value::Null));
        assert!(!compiled.materialize_missing_nullable_columns(&mut row));
        compiled
            .validate_complete_row(&row)
            .expect("completed SQL row should be durable");
    }
}
