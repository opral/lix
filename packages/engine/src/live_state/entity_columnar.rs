//! Typed analytical sidecars for immutable entity generations.
//!
//! The transaction boundary calls this adapter while validated snapshots are
//! still decoded. It projects every scalar object property into bounded Arrow
//! batches and delegates the physical format to `columnar_row_group`. Complex
//! or type-unstable properties are omitted independently; a scan that needs an
//! omitted property simply retains the authoritative row-shaped fallback.

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::Value as JsonValue;

use crate::LixError;
use crate::changelog::CommitId;
use crate::columnar_row_group::{
    EncodedRowGroupSet, ROW_GROUP_MAX_ROWS, RowGroupSetId, encode_row_group_set_preserving_batches,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScalarKind {
    Unknown,
    String,
    Int64,
    Float64,
    Boolean,
    Unsupported,
}

impl ScalarKind {
    fn observe(self, value: &JsonValue) -> Self {
        let observed = match value {
            JsonValue::Null => return self,
            JsonValue::String(_) => Self::String,
            JsonValue::Bool(_) => Self::Boolean,
            JsonValue::Number(number) if number.as_i64().is_some() => Self::Int64,
            JsonValue::Number(number) if number.as_f64().is_some() => Self::Float64,
            JsonValue::Number(_) | JsonValue::Array(_) | JsonValue::Object(_) => Self::Unsupported,
        };
        match (self, observed) {
            (Self::Unknown, observed) => observed,
            (Self::Int64, Self::Float64) | (Self::Float64, Self::Int64) => Self::Float64,
            (current, observed) if current == observed => current,
            _ => Self::Unsupported,
        }
    }

    fn data_type(self) -> Option<DataType> {
        match self {
            Self::String => Some(DataType::Utf8),
            Self::Int64 => Some(DataType::Int64),
            Self::Float64 => Some(DataType::Float64),
            Self::Boolean => Some(DataType::Boolean),
            Self::Unknown | Self::Unsupported => None,
        }
    }
}

pub(crate) type EntityColumnarWriteSets = BTreeMap<(CommitId, String), EncodedRowGroupSet>;

pub(crate) fn entity_row_group_set_id(commit_id: CommitId, schema_key: &str) -> RowGroupSetId {
    let mut digest = blake3::Hasher::new();
    digest.update(b"lix.entity_columnar.v1");
    digest.update(commit_id.as_uuid().as_bytes());
    digest.update(&(schema_key.len() as u64).to_be_bytes());
    digest.update(schema_key.as_bytes());
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest.finalize().as_bytes()[..16]);
    RowGroupSetId::new(id)
}

pub(crate) fn encode_entity_scalar_row_groups<'a, I>(
    schema_key: &str,
    snapshots: I,
) -> Result<Option<EncodedRowGroupSet>, LixError>
where
    I: ExactSizeIterator<Item = &'a JsonValue> + Clone,
{
    if snapshots.len() == 0 {
        return Ok(None);
    }
    let mut kinds = BTreeMap::<String, ScalarKind>::new();
    for snapshot in snapshots.clone() {
        let Some(object) = snapshot.as_object() else {
            return Ok(None);
        };
        for (name, value) in object {
            let kind = kinds.entry(name.clone()).or_insert(ScalarKind::Unknown);
            *kind = kind.observe(value);
        }
    }
    kinds.retain(|_, kind| kind.data_type().is_some());
    if kinds.is_empty() {
        return Ok(None);
    }

    let fields = kinds
        .iter()
        .map(|(name, kind)| Field::new(name, kind.data_type().expect("retained scalar kind"), true))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let snapshots = snapshots.collect::<Vec<_>>();
    let boolean_fields = kinds
        .iter()
        .filter_map(|(name, kind)| (*kind == ScalarKind::Boolean).then_some(name.as_str()))
        .collect::<Vec<_>>();
    let partitions = if boolean_fields.is_empty() {
        vec![snapshots]
    } else {
        let mut partitions = BTreeMap::<Vec<Option<bool>>, Vec<&JsonValue>>::new();
        for snapshot in snapshots {
            let key = boolean_fields
                .iter()
                .map(|name| snapshot.get(*name).and_then(JsonValue::as_bool))
                .collect();
            partitions.entry(key).or_default().push(snapshot);
        }
        partitions.into_values().collect()
    };
    let mut batches = Vec::new();
    for partition in partitions {
        for rows in partition.chunks(ROW_GROUP_MAX_ROWS) {
            let columns = kinds
                .iter()
                .map(|(name, kind)| scalar_column(rows, name, *kind))
                .collect::<Result<Vec<_>, _>>()?;
            batches.push(
                RecordBatch::try_new(Arc::clone(&schema), columns)
                    .map_err(|error| entity_columnar_error(error.to_string()))?,
            );
        }
    }
    // This is a derived acceleration structure. Unsupported or oversized
    // physical columns must fall back to the authoritative row layout rather
    // than make an otherwise-valid transaction fail.
    Ok(optional_derived_row_group_set(
        encode_row_group_set_preserving_batches(schema_key, schema, &batches),
    ))
}

fn optional_derived_row_group_set(
    encoded: Result<EncodedRowGroupSet, LixError>,
) -> Option<EncodedRowGroupSet> {
    encoded.ok()
}

fn scalar_column(rows: &[&JsonValue], name: &str, kind: ScalarKind) -> Result<ArrayRef, LixError> {
    let values = rows
        .iter()
        .map(|snapshot| snapshot.get(name).unwrap_or(&JsonValue::Null));
    let array: ArrayRef = match kind {
        ScalarKind::String => Arc::new(StringArray::from_iter(values.map(JsonValue::as_str))),
        ScalarKind::Int64 => Arc::new(Int64Array::from_iter(values.map(JsonValue::as_i64))),
        ScalarKind::Float64 => Arc::new(Float64Array::from_iter(values.map(|value| {
            value
                .as_f64()
                .or_else(|| value.as_i64().map(|value| value as f64))
        }))),
        ScalarKind::Boolean => Arc::new(BooleanArray::from_iter(values.map(JsonValue::as_bool))),
        ScalarKind::Unknown | ScalarKind::Unsupported => {
            return Err(entity_columnar_error(
                "attempted to encode a non-scalar entity column",
            ));
        }
    };
    Ok(array)
}

fn entity_columnar_error(message: impl Into<String>) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("entity columnar layout: {}", message.into()),
    )
}

#[cfg(test)]
mod tests {
    use super::{entity_columnar_error, optional_derived_row_group_set};

    #[test]
    fn derived_encoding_failure_falls_back_without_rejecting_authoritative_rows() {
        assert!(
            optional_derived_row_group_set(Err(entity_columnar_error("physical limit"))).is_none()
        );
    }
}
