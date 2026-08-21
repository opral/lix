//! Deterministic reconciliation for concurrent row snapshots.
//!
//! Row existence is resolved as one value. When `base`, `a`, and `b` are all
//! live JSON objects, their columns are reconciled independently. Callers
//! canonically rank the two successors before entering this module, so `b` is
//! always the host's last-writer-wins value.

use std::collections::BTreeSet;

use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::{LixError, catalog::SchemaPlan, common::SharedStr, plugin::runtime::WasmTypedRow};

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

#[derive(Clone, Copy, Debug)]
pub(crate) struct TypedRowVersionRef<'a> {
    pub(crate) snapshot: &'a WasmTypedRow,
    pub(crate) metadata: Option<&'a SharedStr>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ReconciledTypedRow {
    pub(crate) snapshot: WasmTypedRow,
    pub(crate) metadata: Option<SharedStr>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct TypedColumnMerge<'a> {
    pub(crate) column: &'a str,
    pub(crate) a: Option<&'a lix_schema::Value>,
    pub(crate) b: Option<&'a lix_schema::Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TypedColumnMergeResult {
    UseLww,
    Replace(Option<lix_schema::Value>),
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

/// Reconciles plugin-owned rows without converting Schema v1 values through
/// an outer JSON row representation.
pub(crate) fn reconcile_typed_row<F>(
    base: Option<TypedRowVersionRef<'_>>,
    a: Option<TypedRowVersionRef<'_>>,
    b: Option<TypedRowVersionRef<'_>>,
    primary_key_columns: &BTreeSet<String>,
    mut merge_column: F,
) -> Result<Option<ReconciledTypedRow>, LixError>
where
    F: FnMut(TypedColumnMerge<'_>) -> Result<Option<TypedColumnMergeResult>, LixError>,
{
    if typed_row_version_eq(a, b) {
        return Ok(clone_typed_row(b));
    }
    if typed_row_version_eq(a, base) {
        return Ok(clone_typed_row(b));
    }
    if typed_row_version_eq(b, base) {
        return Ok(clone_typed_row(a));
    }

    let (Some(base), Some(a), Some(b)) = (base, a, b) else {
        return Ok(clone_typed_row(b));
    };
    require_compatible_typed_rows(base.snapshot, a.snapshot, b.snapshot)?;

    let mut columns = BTreeSet::new();
    columns.extend(base.snapshot.row.keys().map(str::to_owned));
    columns.extend(a.snapshot.row.keys().map(str::to_owned));
    columns.extend(b.snapshot.row.keys().map(str::to_owned));

    let mut merged = lix_schema::Row::new();
    for column in columns {
        let base_value = base.snapshot.row.get(&column);
        let a_value = a.snapshot.row.get(&column);
        let b_value = b.snapshot.row.get(&column);

        if primary_key_columns.contains(&column) {
            if a_value != base_value || b_value != base_value {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!("row merge attempted to change primary-key column '{column}'"),
                ));
            }
            insert_typed_optional(&mut merged, column, b_value.cloned());
            continue;
        }

        let selected = if a_value == b_value {
            a_value.cloned()
        } else if a_value == base_value {
            b_value.cloned()
        } else if b_value == base_value {
            a_value.cloned()
        } else {
            match merge_column(TypedColumnMerge {
                column: &column,
                a: a_value,
                b: b_value,
            })? {
                Some(TypedColumnMergeResult::Replace(value)) => value,
                Some(TypedColumnMergeResult::UseLww) | None => b_value.cloned(),
            }
        };
        insert_typed_optional(&mut merged, column, selected);
    }

    Ok(Some(ReconciledTypedRow {
        snapshot: WasmTypedRow {
            schema_fingerprint: b.snapshot.schema_fingerprint,
            row_pk: b.snapshot.row_pk.clone(),
            row: merged,
            native_payload: std::sync::OnceLock::new(),
            boundary_create_validation: std::sync::OnceLock::new(),
        },
        metadata: b.metadata.cloned(),
    }))
}

/// Visits only columns that require plugin arbitration, without constructing
/// the reconciled row. This is the discovery pass used to batch component
/// calls; the row is built once after those calls return.
pub(crate) fn visit_typed_row_overlaps<F>(
    base: Option<TypedRowVersionRef<'_>>,
    a: Option<TypedRowVersionRef<'_>>,
    b: Option<TypedRowVersionRef<'_>>,
    primary_key_columns: &BTreeSet<String>,
    mut visit: F,
) -> Result<(), LixError>
where
    F: FnMut(TypedColumnMerge<'_>) -> Result<(), LixError>,
{
    if typed_row_version_eq(a, b) || typed_row_version_eq(a, base) || typed_row_version_eq(b, base)
    {
        return Ok(());
    }
    let (Some(base), Some(a), Some(b)) = (base, a, b) else {
        return Ok(());
    };
    require_compatible_typed_rows(base.snapshot, a.snapshot, b.snapshot)?;

    let mut columns = BTreeSet::new();
    columns.extend(base.snapshot.row.keys());
    columns.extend(a.snapshot.row.keys());
    columns.extend(b.snapshot.row.keys());
    for column in columns {
        let base_value = base.snapshot.row.get(column);
        let a_value = a.snapshot.row.get(column);
        let b_value = b.snapshot.row.get(column);
        if primary_key_columns.contains(column) {
            if a_value != base_value || b_value != base_value {
                return Err(LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!("row merge attempted to change primary-key column '{column}'"),
                ));
            }
        } else if a_value != b_value && a_value != base_value && b_value != base_value {
            visit(TypedColumnMerge {
                column,
                a: a_value,
                b: b_value,
            })?;
        }
    }
    Ok(())
}

fn require_compatible_typed_rows(
    base: &WasmTypedRow,
    a: &WasmTypedRow,
    b: &WasmTypedRow,
) -> Result<(), LixError> {
    if base.schema_fingerprint != a.schema_fingerprint
        || base.schema_fingerprint != b.schema_fingerprint
    {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            "typed row merge requires one schema fingerprint",
        ));
    }
    if base.row_pk != a.row_pk || base.row_pk != b.row_pk {
        return Err(LixError::new(
            LixError::CODE_SCHEMA_VALIDATION,
            "typed row merge attempted to change the row identity",
        ));
    }
    Ok(())
}

fn insert_typed_optional(row: &mut lix_schema::Row, key: String, value: Option<lix_schema::Value>) {
    if let Some(value) = value {
        row.insert(key, value);
    }
}

fn typed_row_version_eq(
    a: Option<TypedRowVersionRef<'_>>,
    b: Option<TypedRowVersionRef<'_>>,
) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => a.snapshot == b.snapshot && a.metadata == b.metadata,
        (None, Some(_)) | (Some(_), None) => false,
    }
}

fn clone_typed_row(row: Option<TypedRowVersionRef<'_>>) -> Option<ReconciledTypedRow> {
    row.map(|row| ReconciledTypedRow {
        snapshot: row.snapshot.clone(),
        metadata: row.metadata.cloned(),
    })
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

    #[test]
    fn typed_merge_keeps_json_columns_as_jsonb_values() {
        fn typed_row(document: JsonValue) -> WasmTypedRow {
            WasmTypedRow {
                schema_fingerprint: [7; 32],
                row_pk: vec![lix_schema::Value::Text("row-1".to_owned())].into(),
                row: lix_schema::Row::from([
                    ("id".to_owned(), lix_schema::Value::Text("row-1".to_owned())),
                    (
                        "document".to_owned(),
                        lix_schema::Value::Jsonb(document.into()),
                    ),
                ]),
                native_payload: std::sync::OnceLock::new(),
                boundary_create_validation: std::sync::OnceLock::new(),
            }
        }

        let base = typed_row(json!({"base": true}));
        let a = typed_row(json!({"author": "a"}));
        let b = typed_row(json!({"author": "b"}));
        let merged = reconcile_typed_row(
            Some(TypedRowVersionRef {
                snapshot: &base,
                metadata: None,
            }),
            Some(TypedRowVersionRef {
                snapshot: &a,
                metadata: None,
            }),
            Some(TypedRowVersionRef {
                snapshot: &b,
                metadata: None,
            }),
            &BTreeSet::from(["id".to_owned()]),
            |input| {
                assert!(matches!(input.a, Some(lix_schema::Value::Jsonb(_))));
                assert!(matches!(input.b, Some(lix_schema::Value::Jsonb(_))));
                Ok(Some(TypedColumnMergeResult::Replace(Some(
                    lix_schema::Value::Jsonb(json!({"merged": true}).into()),
                ))))
            },
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            merged.snapshot.row.get("document"),
            Some(&lix_schema::Value::Jsonb(json!({"merged": true}).into()))
        );
    }
}
