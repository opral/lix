//! Declarative rows→Arrow batch building shared by the table specs.
//!
//! A spec describes its output columns once as a [`ColumnTable`] — a static
//! list of `(name, accessor)` pairs — and [`ColumnTable::build`] materializes
//! any projected schema from it, matching fields by name. This replaces the
//! per-provider hand-written `*_record_batch` match ladders while keeping
//! every error message under the caller's control via [`ColumnTableError`].

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, Int64Array, LargeBinaryArray, StringArray};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::LixError;
use crate::live_state::MaterializedLiveStateRow;
use crate::serialize_row_metadata;

/// How one output column is produced from a row of `R`.
///
/// Accessors are capture-free `fn` pointers so tables can live in statics;
/// formatting/serialization stays exactly where the old match arms had it.
pub(super) enum Col<R: 'static> {
    /// Borrowing text column (fast path, no per-cell allocation).
    Utf8(for<'a> fn(&'a R) -> Option<&'a str>),
    /// Owned text column (serialization, `to_string()` ids).
    Utf8Owned(fn(&R) -> Option<String>),
    /// Owned text column whose accessor can fail (e.g. entity-pk projection).
    Utf8Fallible(fn(&R) -> Result<Option<String>, LixError>),
    Bool(fn(&R) -> Option<bool>),
    I64(fn(&R) -> Option<i64>),
    Binary(fn(&R) -> Option<Vec<u8>>),
}

/// Build failure, kept structural so each table maps it to its own
/// byte-identical error message.
pub(super) enum ColumnTableError {
    /// The projected schema asked for a column the table does not define.
    UnsupportedColumn(String),
    /// Arrow rejected the assembled batch.
    Arrow(ArrowError),
    /// Arrow rejected the zero-column (row-count-only) batch.
    ArrowZeroColumn(ArrowError),
    /// A fallible cell accessor failed.
    Row(LixError),
}

pub(super) struct ColumnTable<R: 'static> {
    pub(super) columns: &'static [(&'static str, Col<R>)],
}

impl<R> ColumnTable<R> {
    /// Look up a column's accessor by name. Specs that mix table-driven
    /// system columns with bespoke ones (entity surfaces) resolve the shared
    /// accessor here and materialize it with [`build_array`].
    pub(super) fn col(&self, name: &str) -> Option<&Col<R>> {
        self.columns
            .iter()
            .find(|(column_name, _)| *column_name == name)
            .map(|(_, col)| col)
    }

    /// Build a batch matching `schema`'s fields by name. The zero-column
    /// projection (`SELECT count(*)`) is handled here once for every table.
    pub(super) fn build(
        &self,
        schema: SchemaRef,
        rows: &[R],
    ) -> Result<RecordBatch, ColumnTableError> {
        if schema.fields().is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
            return RecordBatch::try_new_with_options(schema, vec![], &options)
                .map_err(ColumnTableError::ArrowZeroColumn);
        }

        let arrays = schema
            .fields()
            .iter()
            .map(|field| {
                let name = field.name().as_str();
                let col = self
                    .col(name)
                    .ok_or_else(|| ColumnTableError::UnsupportedColumn(name.to_string()))?;
                build_array(col, rows)
            })
            .collect::<Result<Vec<_>, ColumnTableError>>()?;

        RecordBatch::try_new(schema, arrays).map_err(ColumnTableError::Arrow)
    }
}

/// Materialize one output column from `rows` via its accessor. The accessor
/// is an `fn` pointer evaluated once per row, exactly like the hand-written
/// match arms this replaces.
#[expect(trivial_casts)]
pub(super) fn build_array<R>(col: &Col<R>, rows: &[R]) -> Result<ArrayRef, ColumnTableError> {
    Ok(match col {
        Col::Utf8(get) => string_array(rows.iter().map(get)),
        Col::Utf8Owned(get) => {
            Arc::new(StringArray::from(rows.iter().map(get).collect::<Vec<_>>())) as ArrayRef
        }
        Col::Utf8Fallible(get) => Arc::new(StringArray::from(
            rows.iter()
                .map(get)
                .collect::<Result<Vec<_>, LixError>>()
                .map_err(ColumnTableError::Row)?,
        )) as ArrayRef,
        Col::Bool(get) => {
            Arc::new(BooleanArray::from(rows.iter().map(get).collect::<Vec<_>>())) as ArrayRef
        }
        Col::I64(get) => {
            Arc::new(Int64Array::from(rows.iter().map(get).collect::<Vec<_>>())) as ArrayRef
        }
        Col::Binary(get) => Arc::new(LargeBinaryArray::from(
            rows.iter()
                .map(get)
                .collect::<Vec<_>>()
                .iter()
                .map(|value| value.as_deref())
                .collect::<Vec<_>>(),
        )) as ArrayRef,
    })
}

/// Nullable Utf8 array from borrowed values; shared by the spec files.
#[expect(trivial_casts)]
pub(super) fn string_array<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from(values.collect::<Vec<_>>())) as ArrayRef
}

/// Column table over materialized live-state rows: the full `lix_state` /
/// `lix_state_by_branch` surface. Entity specs reuse the same accessors for
/// their `lixcol_*` system columns (they strip the prefix before lookup).
pub(super) static LIVE_STATE_COLS: ColumnTable<MaterializedLiveStateRow> = ColumnTable {
    columns: &[
        (
            "entity_pk",
            Col::Utf8Fallible(|row| row.entity_pk.as_json_array_text().map(Some)),
        ),
        ("schema_key", Col::Utf8(|row| Some(&row.schema_key))),
        ("file_id", Col::Utf8(|row| row.file_id.as_deref())),
        (
            "snapshot_content",
            Col::Utf8(|row| row.snapshot_content.as_deref()),
        ),
        (
            "metadata",
            Col::Utf8Owned(|row| row.metadata.as_deref().map(serialize_row_metadata)),
        ),
        ("created_at", Col::Utf8(|row| Some(&row.created_at))),
        ("updated_at", Col::Utf8(|row| Some(&row.updated_at))),
        ("global", Col::Bool(|row| Some(row.global))),
        (
            "change_id",
            Col::Utf8Owned(|row| row.change_id.map(|id| id.to_string())),
        ),
        (
            "commit_id",
            Col::Utf8Owned(|row| row.commit_id.map(|id| id.to_string())),
        ),
        ("untracked", Col::Bool(|row| Some(row.untracked))),
        ("branch_id", Col::Utf8(|row| Some(&row.branch_id))),
    ],
};
