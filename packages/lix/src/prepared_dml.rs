use std::sync::Arc;

use crate::{Blob, LixError, Value};

/// Compact, owned parameters for one prepared DML shape.
///
/// Rows are stored as fixed-size cell descriptors. Variable-width text,
/// JSON, and blob payloads share one byte arena; nulls and fixed-width scalar
/// values do not allocate per row. Cloning the batch only clones the three
/// backing arcs, so callers can retain a reusable batch without retaining one
/// allocation per parameter row.
#[derive(Clone, Debug)]
pub(crate) struct PreparedDmlParameterBatch {
    row_count: usize,
    column_count: usize,
    cells: Arc<[PreparedDmlCell]>,
    bytes: Arc<[u8]>,
}

#[derive(Clone, Copy, Debug)]
#[repr(u8)]
enum PreparedDmlValueKind {
    Null,
    Boolean,
    Integer,
    Real,
    Text,
    Jsonb,
    Timestamptz,
    Blob,
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
struct PreparedDmlCell {
    kind: PreparedDmlValueKind,
    start: u32,
    end: u32,
    scalar: [u8; 8],
}

/// A borrowed value view into a [`PreparedDmlParameterBatch`].
#[derive(Clone, Copy, Debug)]
pub(crate) enum PreparedDmlValueRef<'a> {
    Null,
    Boolean(bool),
    Integer(i64),
    Real(f64),
    Text(&'a str),
    Jsonb(&'a [u8]),
    Timestamptz(i64),
    Blob(&'a [u8]),
}

impl PreparedDmlParameterBatch {
    /// Packs owned parameter rows into one compact arena.
    pub(crate) fn from_rows(rows: impl IntoIterator<Item = Vec<Value>>) -> Result<Self, LixError> {
        let mut row_count = 0usize;
        let mut column_count = None;
        let mut cells = Vec::new();
        let mut bytes = Vec::new();
        for row in rows {
            let expected_columns = column_count.get_or_insert(row.len());
            if *expected_columns != row.len() {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "prepared DML parameters must be rectangular",
                ));
            }
            row_count = row_count.checked_add(1).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "prepared DML parameter row count overflowed",
                )
            })?;
            for value in row {
                cells.push(Self::pack_value(value, &mut bytes)?);
            }
        }
        Ok(Self {
            row_count,
            column_count: column_count.unwrap_or(0),
            cells: cells.into(),
            bytes: bytes.into(),
        })
    }

    pub(crate) fn row_count(&self) -> usize {
        self.row_count
    }

    pub(crate) fn column_count(&self) -> usize {
        self.column_count
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Returns a borrowed cell view with fail-closed bounds and encoding checks.
    pub(crate) fn get(
        &self,
        row: usize,
        column: usize,
    ) -> Result<PreparedDmlValueRef<'_>, LixError> {
        if row >= self.row_count || column >= self.column_count {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "prepared DML cell is out of bounds",
            ));
        }
        let cell = self.cells[row * self.column_count + column];
        let value = match cell.kind {
            PreparedDmlValueKind::Null => PreparedDmlValueRef::Null,
            PreparedDmlValueKind::Boolean => PreparedDmlValueRef::Boolean(cell.scalar[0] != 0),
            PreparedDmlValueKind::Integer => {
                PreparedDmlValueRef::Integer(i64::from_le_bytes(cell.scalar))
            }
            PreparedDmlValueKind::Real => {
                PreparedDmlValueRef::Real(f64::from_le_bytes(cell.scalar))
            }
            PreparedDmlValueKind::Text => {
                PreparedDmlValueRef::Text(std::str::from_utf8(self.slice(cell)).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "prepared DML text cell is not valid UTF-8",
                    )
                })?)
            }
            PreparedDmlValueKind::Jsonb => PreparedDmlValueRef::Jsonb(self.slice(cell)),
            PreparedDmlValueKind::Timestamptz => {
                PreparedDmlValueRef::Timestamptz(i64::from_le_bytes(cell.scalar))
            }
            PreparedDmlValueKind::Blob => PreparedDmlValueRef::Blob(self.slice(cell)),
        };
        Ok(value)
    }

    pub(crate) fn value(&self, row: usize, column: usize) -> PreparedDmlValueRef<'_> {
        debug_assert!(row < self.row_count && column < self.column_count);
        let cell = self.cells[row * self.column_count + column];
        match cell.kind {
            PreparedDmlValueKind::Null => PreparedDmlValueRef::Null,
            PreparedDmlValueKind::Boolean => PreparedDmlValueRef::Boolean(cell.scalar[0] != 0),
            PreparedDmlValueKind::Integer => {
                PreparedDmlValueRef::Integer(i64::from_le_bytes(cell.scalar))
            }
            PreparedDmlValueKind::Real => {
                PreparedDmlValueRef::Real(f64::from_le_bytes(cell.scalar))
            }
            PreparedDmlValueKind::Text => {
                // Text cells are validated and encoded by `pack_value`; this
                // internal hot route is only reachable from those cells.
                PreparedDmlValueRef::Text(unsafe {
                    std::str::from_utf8_unchecked(self.slice(cell))
                })
            }
            PreparedDmlValueKind::Jsonb => PreparedDmlValueRef::Jsonb(self.slice(cell)),
            PreparedDmlValueKind::Timestamptz => {
                PreparedDmlValueRef::Timestamptz(i64::from_le_bytes(cell.scalar))
            }
            PreparedDmlValueKind::Blob => PreparedDmlValueRef::Blob(self.slice(cell)),
        }
    }

    /// Materializes one row for certified generic write paths. This is
    /// bounded to one row and never recreates the public row-Arc ownership
    /// model.
    pub(crate) fn row_values(&self, row: usize) -> Result<Vec<Value>, LixError> {
        if row >= self.row_count {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "prepared DML row is out of bounds",
            ));
        }
        (0..self.column_count)
            .map(|column| match self.get(row, column)? {
                PreparedDmlValueRef::Null => Ok(Value::Null),
                PreparedDmlValueRef::Boolean(value) => Ok(Value::Boolean(value)),
                PreparedDmlValueRef::Integer(value) => Ok(Value::Integer(value)),
                PreparedDmlValueRef::Real(value) => Ok(Value::Real(value)),
                PreparedDmlValueRef::Text(value) => Ok(Value::Text(value.to_owned())),
                PreparedDmlValueRef::Jsonb(value) => serde_json::from_slice(value)
                    .map(Value::Jsonb)
                    .map_err(|error| {
                        LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            format!("prepared DML JSON parameter is invalid: {error}"),
                        )
                    }),
                PreparedDmlValueRef::Timestamptz(value) => Ok(Value::Timestamptz(value)),
                PreparedDmlValueRef::Blob(value) => Ok(Value::Blob(Blob::from(value.to_vec()))),
            })
            .collect()
    }

    fn pack_value(value: Value, bytes: &mut Vec<u8>) -> Result<PreparedDmlCell, LixError> {
        let mut cell = PreparedDmlCell {
            kind: PreparedDmlValueKind::Null,
            start: 0,
            end: 0,
            scalar: [0; 8],
        };
        match value {
            Value::Null => {}
            Value::Boolean(value) => {
                cell.kind = PreparedDmlValueKind::Boolean;
                cell.scalar[0] = u8::from(value);
            }
            Value::Integer(value) => {
                cell.kind = PreparedDmlValueKind::Integer;
                cell.scalar = value.to_le_bytes();
            }
            Value::Real(value) => {
                cell.kind = PreparedDmlValueKind::Real;
                cell.scalar = value.to_le_bytes();
            }
            Value::Text(value) => {
                cell.kind = PreparedDmlValueKind::Text;
                Self::set_bytes(&mut cell, bytes, value.as_bytes())?;
            }
            Value::Jsonb(value) => {
                cell.kind = PreparedDmlValueKind::Jsonb;
                let encoded = serde_json::to_vec(&value).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        format!("prepared DML JSON parameter cannot be encoded: {error}"),
                    )
                })?;
                Self::set_bytes(&mut cell, bytes, &encoded)?;
            }
            Value::Timestamptz(value) => {
                cell.kind = PreparedDmlValueKind::Timestamptz;
                cell.scalar = value.to_le_bytes();
            }
            Value::Blob(value) => {
                cell.kind = PreparedDmlValueKind::Blob;
                Self::set_bytes(&mut cell, bytes, &value)?;
            }
        }
        Ok(cell)
    }

    fn set_bytes(
        cell: &mut PreparedDmlCell,
        bytes: &mut Vec<u8>,
        value: &[u8],
    ) -> Result<(), LixError> {
        let start = u32::try_from(bytes.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "prepared DML parameter arena exceeds 4 GiB",
            )
        })?;
        bytes.extend_from_slice(value);
        let end = u32::try_from(bytes.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "prepared DML parameter arena exceeds 4 GiB",
            )
        })?;
        cell.start = start;
        cell.end = end;
        Ok(())
    }

    fn slice(&self, cell: PreparedDmlCell) -> &[u8] {
        &self.bytes[cell.start as usize..cell.end as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::{PreparedDmlParameterBatch, PreparedDmlValueRef};
    use crate::{Blob, Value};

    #[test]
    fn packs_scalar_variable_null_and_blob_cells_without_row_ownership() {
        let batch = PreparedDmlParameterBatch::from_rows([vec![
            Value::Null,
            Value::Boolean(true),
            Value::Integer(7),
            Value::Real(1.5),
            Value::Text("text".to_string()),
            Value::Jsonb(serde_json::json!({"ok": true}).into()),
            Value::Blob(Blob::from(vec![1_u8, 2, 3])),
        ]])
        .expect("rectangular parameter batch");
        assert!(matches!(batch.get(0, 0), Ok(PreparedDmlValueRef::Null)));
        assert!(matches!(
            batch.get(0, 1),
            Ok(PreparedDmlValueRef::Boolean(true))
        ));
        assert!(matches!(
            batch.get(0, 2),
            Ok(PreparedDmlValueRef::Integer(7))
        ));
        assert!(
            matches!(batch.get(0, 3), Ok(PreparedDmlValueRef::Real(value)) if (value - 1.5).abs() < f64::EPSILON)
        );
        assert!(matches!(
            batch.get(0, 4),
            Ok(PreparedDmlValueRef::Text("text"))
        ));
        assert!(matches!(batch.get(0, 5), Ok(PreparedDmlValueRef::Jsonb(_))));
        assert!(
            matches!(batch.get(0, 6), Ok(PreparedDmlValueRef::Blob(bytes)) if bytes == [1, 2, 3])
        );
        assert!(batch.get(1, 0).is_err());
        assert!(batch.get(0, 7).is_err());
    }

    #[test]
    fn rejects_non_rectangular_rows() {
        assert!(
            PreparedDmlParameterBatch::from_rows([
                vec![Value::Null],
                vec![Value::Null, Value::Null],
            ])
            .is_err()
        );
    }
}
