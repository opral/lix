//! The byte cap on buffered reads.
//!
//! A buffered read (`query().all()`, `execute()` RETURNING rows) collects its
//! whole result in memory. The cap turns an accidental huge result into a loud
//! `LIX_ERROR_RESULT_TOO_LARGE` error instead of an out-of-memory crash, and the
//! collection aborts as soon as the cap is crossed. Streams are not capped.
//!
//! The cap travels with the read as a task-local [`ReadLimits`] so every
//! buffered collection point charges against the caller's choice without
//! threading a parameter through the SQL runtime. There is no row budget and
//! no deadline: callers cancel a read by dropping it.
use crate::{LixError, Value};

/// Default cap on the memory one buffered result holds (its Arrow batches,
/// then its row values), not on any wire encoding.
pub(crate) const DEFAULT_MAX_RESULT_BYTES: usize = 64 * 1024 * 1024;

/// The limits one buffered read collects under.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ReadLimits {
    /// Result memory in bytes; `usize::MAX` opts out.
    pub(crate) max_bytes: usize,
    /// Rows the caller accepts (single-row reads); `None` is unlimited.
    pub(crate) max_rows: Option<usize>,
    /// The caller keys rows by column name: a write whose `RETURNING` repeats
    /// a name is rejected while binding, before it runs.
    pub(crate) unique_columns: bool,
}

impl Default for ReadLimits {
    fn default() -> Self {
        Self {
            max_bytes: DEFAULT_MAX_RESULT_BYTES,
            max_rows: None,
            unique_columns: false,
        }
    }
}

impl ReadLimits {
    pub(crate) fn bytes(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            max_rows: None,
            unique_columns: false,
        }
    }

    pub(crate) fn with_unique_columns(mut self, unique_columns: bool) -> Self {
        self.unique_columns = unique_columns;
        self
    }

    /// At most one row, of any size.
    pub(crate) fn single_row() -> Self {
        Self {
            max_bytes: usize::MAX,
            max_rows: Some(1),
            unique_columns: false,
        }
    }
}

tokio::task_local! {
    static READ_LIMITS: ReadLimits;
}

/// Runs `operation` with `limits` applied to every buffered collection inside it.
pub(crate) fn with_read_limits<F: Future>(
    limits: ReadLimits,
    operation: F,
) -> impl Future<Output = F::Output> {
    READ_LIMITS.scope(limits, operation)
}

/// The limits of the read being executed, or the defaults outside any scope.
pub(crate) fn current_read_limits() -> ReadLimits {
    READ_LIMITS.try_with(|limits| *limits).unwrap_or_default()
}

pub(crate) fn result_too_large(max_bytes: usize) -> LixError {
    LixError::new(
        LixError::CODE_RESULT_TOO_LARGE,
        format!(
            "query result exceeds the {max_bytes}-byte buffered read cap; nothing was returned"
        ),
    )
    .with_hint(
        "Raise or remove the cap with max_bytes (Rust: .max_bytes(n) or usize::MAX, JS: \
         { maxBytes: n } or Infinity). A read can stream() instead to stay bounded; a \
         write's RETURNING rows are always buffered, so return fewer rows or columns.",
    )
    .with_details(serde_json::json!({ "maxBytes": max_bytes }))
}

pub(crate) fn not_unique() -> LixError {
    LixError::new(
        LixError::CODE_NOT_UNIQUE,
        "query returned more than one row where at most one was expected",
    )
    .with_hint("Add a WHERE or LIMIT clause, or use query() / all() for several rows.")
}

/// Charges one buffered result against the current [`ReadLimits`].
pub(crate) struct ReadResultBudget {
    bytes: usize,
    rows: usize,
    limits: ReadLimits,
}

impl Default for ReadResultBudget {
    fn default() -> Self {
        Self::new(current_read_limits())
    }
}

impl ReadResultBudget {
    pub(crate) fn new(limits: ReadLimits) -> Self {
        Self {
            bytes: 0,
            rows: 0,
            limits,
        }
    }

    /// Charges `bytes` of public values plus the fixed cost of `rows` rows,
    /// so the cap tracks the memory the buffered `Row`s will take.
    pub(crate) fn charge(&mut self, bytes: usize, rows: usize) -> Result<(), LixError> {
        self.bytes = self
            .bytes
            .saturating_add(bytes)
            .saturating_add(rows.saturating_mul(size_of::<crate::Row>()));
        self.rows = self.rows.saturating_add(rows);
        if self.limits.max_rows.is_some_and(|max_rows| self.rows > max_rows) {
            return Err(not_unique());
        }
        if self.bytes > self.limits.max_bytes {
            return Err(result_too_large(self.limits.max_bytes));
        }
        Ok(())
    }

    pub(crate) fn charge_values(&mut self, values: &[Value]) -> Result<(), LixError> {
        self.charge(public_row_bytes(values), 1)
    }
}

/// Bytes one public result row occupies: one `Value` slot per cell plus its
/// variable-length payload. Buffered caps and streamed page sizes share
/// this measure so a byte limit means the same thing on every read path.
pub(crate) fn public_row_bytes(values: &[Value]) -> usize {
    values.iter().fold(0usize, |bytes, value| {
        bytes.saturating_add(
            size_of::<Value>()
                + match value {
                    Value::Text(value) => value.len(),
                    Value::Jsonb(value) => value.as_bytes().len(),
                    Value::Blob(value) => value.len(),
                    Value::RowRef(value) => value.as_str().len(),
                    _ => 0,
                },
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_budget_caps_bytes_including_per_row_cost() {
        let mut budget = ReadResultBudget::default();
        budget.charge(DEFAULT_MAX_RESULT_BYTES, 0).unwrap();
        let error = budget.charge(1, 0).unwrap_err();
        assert_eq!(error.code, LixError::CODE_RESULT_TOO_LARGE);
        assert!(error.hint.as_deref().unwrap_or_default().contains("stream()"));
        assert!(budget.charge(usize::MAX, usize::MAX).is_err());
        // Rows of empty values still cost their row and slot memory.
        let mut budget = ReadResultBudget::new(ReadLimits::bytes(1024));
        assert_eq!(
            budget.charge(0, 1024).unwrap_err().code,
            LixError::CODE_RESULT_TOO_LARGE
        );
    }

    #[tokio::test]
    async fn scoped_limits_reach_budgets_created_inside_the_read() {
        let opted_out = with_read_limits(ReadLimits::bytes(usize::MAX), async {
            ReadResultBudget::default().charge(DEFAULT_MAX_RESULT_BYTES * 4, 1)
        })
        .await;
        assert!(opted_out.is_ok());
        let tight = with_read_limits(ReadLimits::bytes(8), async {
            ReadResultBudget::default().charge(9, 1)
        })
        .await;
        assert_eq!(tight.unwrap_err().code, LixError::CODE_RESULT_TOO_LARGE);
        let single = with_read_limits(ReadLimits::single_row(), async {
            let mut budget = ReadResultBudget::default();
            budget.charge(usize::MAX / 4, 1)?;
            budget.charge(1, 1)
        })
        .await;
        assert_eq!(single.unwrap_err().code, LixError::CODE_NOT_UNIQUE);
        // Outside a scope the defaults apply again.
        assert_eq!(current_read_limits(), ReadLimits::default());
    }

    #[test]
    fn scoped_limits_work_without_a_tokio_runtime() {
        assert!(tokio::runtime::Handle::try_current().is_err());
        let result = futures_lite::future::block_on(with_read_limits(
            ReadLimits::bytes(1),
            async { ReadResultBudget::default().charge(2, 1) },
        ));
        assert_eq!(result.unwrap_err().code, LixError::CODE_RESULT_TOO_LARGE);
    }
}
