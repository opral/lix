//! Deterministic reconciliation for concurrent row snapshots.
//!
//! Row existence is resolved as one value. When `base`, `a`, and `b` are all
//! live JSON objects, their columns are reconciled independently. Callers
//! canonically rank the two successors before entering this module, so `b` is
//! always the host's last-writer-wins value.

use std::collections::BTreeSet;

use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::{LixError, catalog::SchemaPlan};

#[derive(Clone, Copy, Debug)]
pub(crate) struct RowVersionRef<'a> {
    pub(crate) snapshot: &'a JsonValue,
    pub(crate) metadata: Option<&'a JsonValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ReconciledRow {
    pub(crate) snapshot: JsonValue,
    pub(crate) metadata: Option<JsonValue>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ColumnMerge<'a> {
    pub(crate) column: &'a str,
    pub(crate) base: Option<&'a JsonValue>,
    pub(crate) a: Option<&'a JsonValue>,
    pub(crate) b: Option<&'a JsonValue>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ColumnMergeResult {
    /// Keep the canonically later (`b`) value, including its missing state.
    UseLww,
    /// Replace the column. `None` removes an optional column; it is distinct
    /// from `Some(JsonValue::Null)`.
    Replace(Option<JsonValue>),
}

pub(crate) fn primary_key_columns(schema: &SchemaPlan) -> Result<BTreeSet<String>, LixError> {
    let mut columns = BTreeSet::new();
    for pointer in schema.primary_key.as_deref().unwrap_or_default() {
        let [column] = pointer.as_slice() else {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!(
                    "column-based row merge requires top-level primary keys for schema '{}'",
                    schema.key.schema_key
                ),
            ));
        };
        columns.insert(column.clone());
    }
    Ok(columns)
}

/// Reconciles one row. `a` and `b` must already be ordered by `ConflictRank`.
///
/// The callback is invoked only when both successors changed the same column
/// differently. Returning `None` means that no plugin owns this column and
/// therefore selects host-native column-based LWW without a Wasm call.
pub(crate) fn reconcile_row<F>(
    base: Option<RowVersionRef<'_>>,
    a: Option<RowVersionRef<'_>>,
    b: Option<RowVersionRef<'_>>,
    primary_key_columns: &BTreeSet<String>,
    mut merge_column: F,
) -> Result<Option<ReconciledRow>, LixError>
where
    F: FnMut(ColumnMerge<'_>) -> Result<Option<ColumnMergeResult>, LixError>,
{
    if row_version_eq(a, b) {
        return clone_row(b);
    }
    if row_version_eq(a, base) {
        return clone_row(b);
    }
    if row_version_eq(b, base) {
        return clone_row(a);
    }

    // Creation/deletion races intentionally remain whole-row LWW. Column
    // merging only has meaning when all three row snapshots exist.
    let (Some(base), Some(a), Some(b)) = (base, a, b) else {
        return clone_row(b);
    };
    let base_object = require_object(base.snapshot, "base")?;
    let a_object = require_object(a.snapshot, "a")?;
    let b_object = require_object(b.snapshot, "b")?;

    let mut columns = BTreeSet::new();
    columns.extend(base_object.keys().cloned());
    columns.extend(a_object.keys().cloned());
    columns.extend(b_object.keys().cloned());

    let mut merged = JsonMap::new();
    for column in columns {
        let base_value = base_object.get(&column);
        let a_value = a_object.get(&column);
        let b_value = b_object.get(&column);

        if primary_key_columns.contains(&column) {
            if a_value != base_value || b_value != base_value {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!("row merge attempted to change primary-key column '{column}'"),
                ));
            }
            insert_optional(&mut merged, column, b_value.cloned());
            continue;
        }

        let selected = if a_value == b_value {
            a_value.cloned()
        } else if a_value == base_value {
            b_value.cloned()
        } else if b_value == base_value {
            a_value.cloned()
        } else {
            match merge_column(ColumnMerge {
                column: &column,
                base: base_value,
                a: a_value,
                b: b_value,
            })? {
                Some(ColumnMergeResult::Replace(value)) => value,
                Some(ColumnMergeResult::UseLww) | None => b_value.cloned(),
            }
        };
        insert_optional(&mut merged, column, selected);
    }

    Ok(Some(ReconciledRow {
        snapshot: JsonValue::Object(merged),
        // Metadata is not a schema column. Keep the same canonical LWW rule.
        metadata: b.metadata.cloned(),
    }))
}

fn require_object<'a>(
    value: &'a JsonValue,
    side: &str,
) -> Result<&'a JsonMap<String, JsonValue>, LixError> {
    value.as_object().ok_or_else(|| {
        LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            format!("row merge {side} snapshot must be a JSON object"),
        )
    })
}

fn insert_optional(map: &mut JsonMap<String, JsonValue>, key: String, value: Option<JsonValue>) {
    if let Some(value) = value {
        map.insert(key, value);
    }
}

fn row_version_eq(a: Option<RowVersionRef<'_>>, b: Option<RowVersionRef<'_>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => a.snapshot == b.snapshot && a.metadata == b.metadata,
        (None, Some(_)) | (Some(_), None) => false,
    }
}

fn clone_row(row: Option<RowVersionRef<'_>>) -> Result<Option<ReconciledRow>, LixError> {
    row.map(|row| {
        require_object(row.snapshot, "selected")?;
        Ok(ReconciledRow {
            snapshot: row.snapshot.clone(),
            metadata: row.metadata.cloned(),
        })
    })
    .transpose()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn row(value: &JsonValue) -> RowVersionRef<'_> {
        RowVersionRef {
            snapshot: value,
            metadata: None,
        }
    }

    fn no_primary_key() -> BTreeSet<String> {
        BTreeSet::new()
    }

    #[test]
    fn composes_changes_to_different_columns_without_calling_plugin() {
        let base = json!({"id":"1","title":"old","body":"old"});
        let a = json!({"id":"1","title":"a","body":"old"});
        let b = json!({"id":"1","title":"old","body":"b"});
        let mut calls = 0;
        let merged = reconcile_row(
            Some(row(&base)),
            Some(row(&a)),
            Some(row(&b)),
            &BTreeSet::from(["id".to_owned()]),
            |_| {
                calls += 1;
                Ok(None)
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(merged.snapshot, json!({"id":"1","title":"a","body":"b"}));
        assert_eq!(calls, 0);
    }

    #[test]
    fn overlapping_column_uses_b_by_default() {
        let base = json!({"body":"old"});
        let a = json!({"body":"alice"});
        let b = json!({"body":"bob"});
        let merged = reconcile_row(
            Some(row(&base)),
            Some(row(&a)),
            Some(row(&b)),
            &no_primary_key(),
            |_| Ok(None),
        )
        .unwrap()
        .unwrap();
        assert_eq!(merged.snapshot, b);
    }

    #[test]
    fn plugin_can_replace_only_the_overlapping_column() {
        let base = json!({"body":"old","rank":1});
        let a = json!({"body":"alice","rank":2});
        let b = json!({"body":"bob","rank":3});
        let merged = reconcile_row(
            Some(row(&base)),
            Some(row(&a)),
            Some(row(&b)),
            &no_primary_key(),
            |input| {
                Ok(Some(if input.column == "body" {
                    ColumnMergeResult::Replace(Some(JsonValue::String("alice + bob".to_owned())))
                } else {
                    ColumnMergeResult::UseLww
                }))
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(merged.snapshot, json!({"body":"alice + bob","rank":3}));
    }

    #[test]
    fn missing_and_null_are_distinct_column_values() {
        let base = json!({"value":"old"});
        let a = json!({"value":null});
        let b = json!({});
        let mut observed = None;
        let merged = reconcile_row(
            Some(row(&base)),
            Some(row(&a)),
            Some(row(&b)),
            &no_primary_key(),
            |input| {
                observed = Some((input.a.cloned(), input.b.cloned()));
                Ok(Some(ColumnMergeResult::UseLww))
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(observed, Some((Some(JsonValue::Null), None)));
        assert_eq!(merged.snapshot, json!({}));
    }

    #[test]
    fn delete_edit_race_uses_b_as_a_whole_row() {
        let base = json!({"body":"old"});
        let a = json!({"body":"edited"});
        let merged = reconcile_row(
            Some(row(&base)),
            Some(row(&a)),
            None,
            &no_primary_key(),
            |_| panic!("column merger must not run for row existence conflicts"),
        )
        .unwrap();
        assert_eq!(merged, None);
    }

    #[test]
    fn rejects_primary_key_changes() {
        let base = json!({"id":"1","body":"old"});
        let a = json!({"id":"2","body":"old"});
        let b = json!({"id":"1","body":"new"});
        let error = reconcile_row(
            Some(row(&base)),
            Some(row(&a)),
            Some(row(&b)),
            &BTreeSet::from(["id".to_owned()]),
            |_| Ok(None),
        )
        .unwrap_err();
        assert_eq!(error.code, LixError::CODE_SCHEMA_VALIDATION);
    }
}
