use std::sync::Arc;

use crate::{SqlQueryResult, Value};

/// Every cell of one result set, stored contiguously.
///
/// Result rows used to own a `Vec<Value>` each, so a scan allocated and freed
/// one vector per row and then walked the whole set again to repackage those
/// vectors into their public row form. The cells now live in a single buffer
/// and a row is only an index into it, which turns "one allocation per row"
/// into "one allocation per result".
#[derive(Debug)]
pub(crate) struct RowArena {
    columns: Arc<[String]>,
    values: Vec<Value>,
    layout: RowLayout,
}

/// Where each row starts and ends inside the arena.
#[derive(Debug)]
enum RowLayout {
    /// Every row is `width` cells wide. This is the shape every SQL result
    /// has, so row bounds are a multiplication rather than a lookup.
    Uniform { width: usize, row_count: usize },
    /// Row `index` ends at `ends[index]`. Only rows handed in through
    /// `ExecuteResult::from_rows`, which accepts arbitrary vectors, can be
    /// ragged; keeping them representable avoids padding a caller's row out
    /// with nulls it never supplied.
    Ragged { ends: Vec<usize> },
}

impl RowArena {
    /// Builds an arena whose rows are all `width` cells wide.
    ///
    /// `values` must be `width * row_count` cells in row-major order.
    pub(crate) fn uniform(columns: Arc<[String]>, values: Vec<Value>, width: usize) -> Self {
        let row_count = if width == 0 {
            0
        } else {
            debug_assert_eq!(values.len() % width, 0, "uniform arena must be rectangular");
            values.len() / width
        };
        Self {
            columns,
            values,
            layout: RowLayout::Uniform { width, row_count },
        }
    }

    /// Builds an arena of `row_count` zero-width rows.
    ///
    /// A statement can project no columns at all, in which case the row count
    /// carries the entire result.
    pub(crate) fn empty_rows(columns: Arc<[String]>, row_count: usize) -> Self {
        Self {
            columns,
            values: Vec::new(),
            layout: RowLayout::Uniform {
                width: 0,
                row_count,
            },
        }
    }

    /// Flattens owned per-row vectors into one arena.
    pub(crate) fn from_row_vectors(columns: Arc<[String]>, rows: Vec<Vec<Value>>) -> Self {
        let width = columns.len();
        if width == 0 {
            let row_count = rows.len();
            if rows.iter().all(Vec::is_empty) {
                return Self::empty_rows(columns, row_count);
            }
        }
        let total = rows.iter().map(Vec::len).sum::<usize>();
        let mut values = Vec::with_capacity(total);
        let mut uniform = true;
        let mut ends = Vec::with_capacity(rows.len());
        for mut row in rows {
            values.append(&mut row);
            uniform &= values.len() == (ends.len() + 1) * width;
            ends.push(values.len());
        }
        if uniform {
            return Self::uniform(columns, values, width);
        }
        Self {
            columns,
            values,
            layout: RowLayout::Ragged { ends },
        }
    }

    pub(crate) fn columns(&self) -> &Arc<[String]> {
        &self.columns
    }

    pub(crate) fn row_count(&self) -> usize {
        match &self.layout {
            RowLayout::Uniform { row_count, .. } => *row_count,
            RowLayout::Ragged { ends } => ends.len(),
        }
    }

    /// Returns the cells of row `index`, or an empty slice when the row does
    /// not exist.
    pub(crate) fn row(&self, index: usize) -> &[Value] {
        match &self.layout {
            RowLayout::Uniform { width, row_count } => {
                if index >= *row_count {
                    return &[];
                }
                let start = index * width;
                &self.values[start..start + width]
            }
            RowLayout::Ragged { ends } => {
                let Some(end) = ends.get(index).copied() else {
                    return &[];
                };
                let start = if index == 0 { 0 } else { ends[index - 1] };
                &self.values[start..end]
            }
        }
    }

    /// Rebuilds the owned per-row vectors the serialized result type carries.
    ///
    /// Only boundaries that must hand out `Vec<Vec<Value>>` — the wire format,
    /// idempotency receipts, provider results — pay for this; the session read
    /// path consumes the arena directly.
    pub(crate) fn into_sql_query_result(self) -> SqlQueryResult {
        let row_count = self.row_count();
        let Self {
            columns,
            values,
            layout,
        } = self;
        let mut rows = Vec::with_capacity(row_count);
        let mut cells = values.into_iter();
        match layout {
            RowLayout::Uniform { width, .. } => {
                for _ in 0..row_count {
                    rows.push(cells.by_ref().take(width).collect::<Vec<Value>>());
                }
            }
            RowLayout::Ragged { ends } => {
                let mut start = 0;
                for end in ends {
                    rows.push(cells.by_ref().take(end - start).collect::<Vec<Value>>());
                    start = end;
                }
            }
        }
        SqlQueryResult {
            rows,
            columns: columns.to_vec(),
            notices: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RowArena;
    use crate::Value;
    use std::sync::Arc;

    fn columns(names: &[&str]) -> Arc<[String]> {
        names
            .iter()
            .map(|name| (*name).to_string())
            .collect::<Vec<_>>()
            .into()
    }

    #[test]
    fn uniform_rows_slice_by_stride() {
        let arena = RowArena::uniform(
            columns(&["a", "b"]),
            vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
                Value::Integer(4),
            ],
            2,
        );
        assert_eq!(arena.row_count(), 2);
        assert_eq!(arena.row(0), [Value::Integer(1), Value::Integer(2)]);
        assert_eq!(arena.row(1), [Value::Integer(3), Value::Integer(4)]);
        assert_eq!(arena.row(2), []);
    }

    #[test]
    fn ragged_rows_keep_their_own_width() {
        let arena = RowArena::from_row_vectors(
            columns(&["a", "b"]),
            vec![
                vec![Value::Integer(1)],
                vec![Value::Integer(2), Value::Integer(3)],
            ],
        );
        assert_eq!(arena.row_count(), 2);
        assert_eq!(arena.row(0), [Value::Integer(1)]);
        assert_eq!(arena.row(1), [Value::Integer(2), Value::Integer(3)]);
    }

    #[test]
    fn zero_width_rows_survive_the_round_trip() {
        let arena = RowArena::from_row_vectors(columns(&[]), vec![Vec::new(), Vec::new()]);
        assert_eq!(arena.row_count(), 2);
        assert_eq!(arena.row(1), []);
        let result = arena.into_sql_query_result();
        assert_eq!(result.rows, vec![Vec::new(), Vec::new()]);
    }

    #[test]
    fn round_trip_restores_the_original_rows() {
        let rows = vec![
            vec![Value::Integer(1), Value::Text("x".into())],
            vec![Value::Null, Value::Text("y".into())],
        ];
        let arena = RowArena::from_row_vectors(columns(&["a", "b"]), rows.clone());
        let result = arena.into_sql_query_result();
        assert_eq!(result.rows, rows);
        assert_eq!(result.columns, vec!["a".to_string(), "b".to_string()]);
    }
}
