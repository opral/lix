//! Bounded pages over a live DataFusion result.
//!
//! A streamed read never collects its whole result. The pager receives each
//! DataFusion batch as it is produced and cuts it into pages whose row count
//! and public row bytes (see [`crate::common::public_row_bytes`]) stay within
//! the stream's [`PageLimits`]. Batches larger than a page are split by
//! zero-copy row slices; a single row larger than the byte limit becomes a
//! page of its own.

use std::mem::size_of;

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::arrow::record_batch::RecordBatch;

use crate::{LixError, LixNotice, Value};

use super::datafusion::{SessionReadResult, session_read_result_from_batches};

/// How a stream cuts its result into pages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PageLimits {
    /// Rows in the first page. Small, so opening a stream is cheap.
    pub(crate) first_rows: usize,
    /// Rows in every later page.
    pub(crate) rows: usize,
    /// Public value bytes per page; a single larger row gets its own page.
    pub(crate) bytes: usize,
}

impl Default for PageLimits {
    fn default() -> Self {
        Self {
            first_rows: 64,
            rows: 4096,
            bytes: 1024 * 1024,
        }
    }
}

impl PageLimits {
    pub(crate) fn rows_for_page(&self, page_index: usize) -> usize {
        if page_index == 0 {
            self.first_rows.max(1)
        } else {
            self.rows.max(1)
        }
    }
}

pub(crate) struct ResultPager {
    fields: Vec<Field>,
    limits: PageLimits,
    pages_emitted: usize,
    pending: Vec<RecordBatch>,
    pending_bytes: usize,
    pending_rows: usize,
}

impl ResultPager {
    pub(crate) fn new(fields: Vec<Field>, limits: PageLimits) -> Self {
        Self {
            fields,
            limits,
            pages_emitted: 0,
            pending: Vec::new(),
            pending_bytes: 0,
            pending_rows: 0,
        }
    }

    /// Accepts one result batch and returns every page it completed. The last
    /// partial page stays pending until a later batch fills it or [`finish`]
    /// flushes it.
    ///
    /// [`finish`]: Self::finish
    pub(crate) fn push(&mut self, batch: RecordBatch) -> Result<Vec<SessionReadResult>, LixError> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(Vec::new());
        }
        let row_bytes = batch_public_row_bytes(&batch);
        let mut pages = Vec::new();
        let mut slice_start = 0;
        for (row, bytes) in row_bytes.into_iter().enumerate() {
            if self.pending_rows > 0
                && (self.pending_bytes.saturating_add(bytes) > self.limits.bytes
                    || self.pending_rows >= self.limits.rows_for_page(self.pages_emitted))
            {
                if row > slice_start {
                    self.pending
                        .push(batch.slice(slice_start, row - slice_start));
                }
                pages.push(self.take_page()?);
                slice_start = row;
            }
            self.pending_bytes = self.pending_bytes.saturating_add(bytes);
            self.pending_rows += 1;
        }
        if slice_start < rows {
            self.pending
                .push(batch.slice(slice_start, rows - slice_start));
        }
        // Hand a full page out now rather than when the next row arrives, so
        // a small first page is not held back by a slow scan.
        if self.pending_rows >= self.limits.rows_for_page(self.pages_emitted) {
            pages.push(self.take_page()?);
        }
        Ok(pages)
    }

    /// Flushes the final partial page, if any rows remain.
    pub(crate) fn finish(&mut self) -> Result<Option<SessionReadResult>, LixError> {
        if self.pending_rows == 0 {
            return Ok(None);
        }
        self.take_page().map(Some)
    }

    fn take_page(&mut self) -> Result<SessionReadResult, LixError> {
        let batches = std::mem::take(&mut self.pending);
        self.pending_bytes = 0;
        self.pending_rows = 0;
        self.pages_emitted += 1;
        session_read_result_from_batches(self.fields.clone(), batches, Vec::<LixNotice>::new())
    }
}

/// Per-row public value bytes of one Arrow batch, computed from the arrays
/// without materializing values. Matches [`crate::common::public_row_bytes`]
/// for every type the public row conversion supports.
pub(crate) fn batch_public_row_bytes(batch: &RecordBatch) -> Vec<usize> {
    let slot_bytes = batch.num_columns().saturating_mul(size_of::<Value>());
    let mut bytes = vec![slot_bytes; batch.num_rows()];
    for array in batch.columns() {
        match array.data_type() {
            DataType::Utf8 => {
                add_offset_lengths(&mut bytes, array.as_string::<i32>().value_offsets())
            }
            DataType::LargeUtf8 => {
                add_offset_lengths(&mut bytes, array.as_string::<i64>().value_offsets());
            }
            DataType::Binary => {
                add_offset_lengths(&mut bytes, array.as_binary::<i32>().value_offsets())
            }
            DataType::LargeBinary => {
                add_offset_lengths(&mut bytes, array.as_binary::<i64>().value_offsets());
            }
            DataType::Utf8View => {
                let array = array.as_string_view();
                for (row, total) in bytes.iter_mut().enumerate() {
                    if array.is_valid(row) {
                        *total = total.saturating_add(array.value(row).len());
                    }
                }
            }
            DataType::BinaryView => {
                let array = array.as_binary_view();
                for (row, total) in bytes.iter_mut().enumerate() {
                    if array.is_valid(row) {
                        *total = total.saturating_add(array.value(row).len());
                    }
                }
            }
            _ => {}
        }
    }
    bytes
}

/// Public value bytes of a whole batch: the sum of [`batch_public_row_bytes`]
/// computed per column, without a per-row vector for offset-encoded columns.
pub(crate) fn batch_public_bytes(batch: &RecordBatch) -> usize {
    fn span<O: Copy + TryInto<usize>>(offsets: &[O]) -> usize {
        match (offsets.first(), offsets.last()) {
            (Some(first), Some(last)) => {
                let first = (*first).try_into().unwrap_or(0);
                (*last).try_into().unwrap_or(first).saturating_sub(first)
            }
            _ => 0,
        }
    }
    let mut total = batch
        .num_rows()
        .saturating_mul(batch.num_columns())
        .saturating_mul(size_of::<Value>());
    for array in batch.columns() {
        let payload = match array.data_type() {
            DataType::Utf8 => span(array.as_string::<i32>().value_offsets()),
            DataType::LargeUtf8 => span(array.as_string::<i64>().value_offsets()),
            DataType::Binary => span(array.as_binary::<i32>().value_offsets()),
            DataType::LargeBinary => span(array.as_binary::<i64>().value_offsets()),
            DataType::Utf8View => {
                let array = array.as_string_view();
                (0..array.len())
                    .filter(|row| array.is_valid(*row))
                    .map(|row| array.value(row).len())
                    .sum()
            }
            DataType::BinaryView => {
                let array = array.as_binary_view();
                (0..array.len())
                    .filter(|row| array.is_valid(*row))
                    .map(|row| array.value(row).len())
                    .sum()
            }
            _ => 0,
        };
        total = total.saturating_add(payload);
    }
    total
}

fn add_offset_lengths<O>(bytes: &mut [usize], offsets: &[O])
where
    O: Copy + TryInto<usize>,
{
    for (row, total) in bytes.iter_mut().enumerate() {
        let start = offsets[row].try_into().unwrap_or(0);
        let end = offsets[row + 1].try_into().unwrap_or(start);
        *total = total.saturating_add(end.saturating_sub(start));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::Schema;

    use super::*;
    use crate::common::public_row_bytes;

    fn batch(start: i64, rows: usize, text_len: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("text", DataType::Utf8, true),
        ]));
        let ids = (0..rows).map(|row| start + row as i64).collect::<Vec<_>>();
        let texts = (0..rows)
            .map(|row| (row % 3 != 0).then(|| "x".repeat(text_len + row % 5)))
            .collect::<Vec<_>>();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(texts)),
            ],
        )
        .unwrap()
    }

    fn fields() -> Vec<Field> {
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("text", DataType::Utf8, true),
        ]
    }

    fn page_rows(page: SessionReadResult) -> Vec<Vec<Value>> {
        page.into_sql_query_result().unwrap().rows
    }

    #[test]
    fn arrow_row_bytes_match_public_value_bytes() {
        let batch = batch(0, 17, 9).slice(3, 11);
        let expected = page_rows(
            session_read_result_from_batches(fields(), vec![batch.clone()], Vec::new()).unwrap(),
        )
        .iter()
        .map(|row| public_row_bytes(row))
        .collect::<Vec<_>>();
        assert_eq!(batch_public_row_bytes(&batch), expected);
    }

    #[test]
    fn pages_respect_the_byte_bound_and_preserve_row_order() {
        let page_bytes = 300;
        let mut pager = ResultPager::new(
            fields(),
            PageLimits {
                first_rows: usize::MAX,
                rows: usize::MAX,
                bytes: page_bytes,
            },
        );
        let mut pages = Vec::new();
        for (index, rows) in [5usize, 40, 1, 0, 23].into_iter().enumerate() {
            pages.extend(pager.push(batch(index as i64 * 100, rows, 20)).unwrap());
        }
        pages.extend(pager.finish().unwrap());
        assert!(pager.finish().unwrap().is_none());

        let mut ids = Vec::new();
        for page in pages {
            let rows = page_rows(page);
            assert!(!rows.is_empty(), "pages are never empty");
            let bytes = rows.iter().map(|row| public_row_bytes(row)).sum::<usize>();
            assert!(bytes <= page_bytes, "page holds {bytes} bytes");
            ids.extend(rows.into_iter().map(|row| row[0].clone()));
        }
        let expected = [(0, 5), (100, 40), (200, 1), (400, 23)]
            .into_iter()
            .flat_map(|(start, rows)| (0..rows).map(move |row| Value::Integer(start + row)))
            .collect::<Vec<_>>();
        assert_eq!(ids, expected);
    }

    #[test]
    fn a_row_larger_than_the_page_is_its_own_page() {
        let mut pager = ResultPager::new(
            fields(),
            PageLimits {
                first_rows: usize::MAX,
                rows: usize::MAX,
                bytes: 16,
            },
        );
        let pages = pager.push(batch(0, 4, 100)).unwrap();
        let last = pager.finish().unwrap();
        let sizes = pages
            .into_iter()
            .chain(last)
            .map(|page| page_rows(page).len())
            .collect::<Vec<_>>();
        assert_eq!(sizes, vec![1, 1, 1, 1]);
    }

    #[test]
    fn the_first_page_is_small_and_later_pages_are_row_bounded() {
        let mut pager = ResultPager::new(
            fields(),
            PageLimits {
                first_rows: 3,
                rows: 5,
                bytes: usize::MAX,
            },
        );
        let mut pages = pager.push(batch(0, 14, 1)).unwrap();
        pages.extend(pager.finish().unwrap());
        let sizes = pages
            .into_iter()
            .map(|page| page_rows(page).len())
            .collect::<Vec<_>>();
        assert_eq!(sizes, vec![3, 5, 5, 1]);
    }

    #[test]
    fn a_full_page_is_emitted_without_waiting_for_the_next_row() {
        let mut pager = ResultPager::new(
            fields(),
            PageLimits {
                first_rows: 4,
                rows: 8,
                bytes: usize::MAX,
            },
        );
        let pages = pager.push(batch(0, 4, 1)).unwrap();
        assert_eq!(pages.len(), 1, "the first page is complete at four rows");
        assert!(pager.finish().unwrap().is_none());
    }
}
