//! Bounds for buffered foreground reads. Durable execution never enters the
//! cancellable deadline; timeout must not obscure an accepted commit.
use crate::{LixError, Value};
use std::time::Duration;

pub(crate) const READ_DEADLINE: Duration = Duration::from_secs(30);
pub(crate) const MAX_READ_RESULT_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_READ_RESULT_ROWS: usize = 1_000_000;

pub(crate) async fn with_read_deadline<T>(
    operation: impl Future<Output = Result<T, LixError>>,
) -> Result<T, LixError> {
    read_deadline(operation, READ_DEADLINE).await
}

async fn read_deadline<T>(
    operation: impl Future<Output = Result<T, LixError>>,
    duration: Duration,
) -> Result<T, LixError> {
    use futures_util::{FutureExt, select_biased};
    // Native callers may use any executor, including futures_lite without a
    // Tokio reactor. A shared timer service preserves that public contract.
    #[cfg(not(target_family = "wasm"))]
    let deadline = futures_timer::Delay::new(duration).fuse();
    #[cfg(target_family = "wasm")]
    let deadline = crate::sync::sleep(duration).fuse();
    let operation = operation.fuse();
    futures_util::pin_mut!(deadline, operation);
    select_biased! {
        result = operation => result,
        _ = deadline => Err(LixError::new("LIX_READ_DEADLINE_EXCEEDED",
            "read preparation and execution exceeded the operation deadline")
            .with_details(serde_json::json!({"deadlineMs":duration.as_millis()}))),
    }
}

#[derive(Default)]
pub(crate) struct ReadResultBudget {
    bytes: usize,
    rows: usize,
}
impl ReadResultBudget {
    pub(crate) fn charge(&mut self, bytes: usize, rows: usize) -> Result<(), LixError> {
        self.bytes = self.bytes.saturating_add(bytes);
        self.rows = self.rows.saturating_add(rows);
        if self.bytes > MAX_READ_RESULT_BYTES || self.rows > MAX_READ_RESULT_ROWS {
            return Err(LixError::new("LIX_READ_RESOURCE_EXHAUSTED",
                "buffered read result exceeds its byte or row budget")
                .with_details(serde_json::json!({"maxBytes":MAX_READ_RESULT_BYTES,"maxRows":MAX_READ_RESULT_ROWS})));
        }
        Ok(())
    }
    pub(crate) fn charge_values(&mut self, values: &[Value]) -> Result<(), LixError> {
        let bytes = values.iter().fold(0usize, |bytes, value| {
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
        });
        self.charge(bytes, 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn read_deadline_cancels_pending_preparation() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        struct ReadGuard(Arc<AtomicBool>);
        impl Drop for ReadGuard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let released = Arc::new(AtomicBool::new(false));
        let observed = released.clone();
        let operation = async move {
            let _guard = ReadGuard(observed);
            futures_util::future::pending::<Result<(), LixError>>().await
        };
        let error = read_deadline(operation, Duration::from_millis(1))
            .await
            .unwrap_err();
        assert_eq!(error.code, "LIX_READ_DEADLINE_EXCEEDED");
        assert!(
            released.load(Ordering::SeqCst),
            "deadline must drop the pending read scope"
        );
    }
    #[test]
    fn read_deadline_works_without_a_tokio_reactor() {
        assert!(tokio::runtime::Handle::try_current().is_err());
        let result = futures_lite::future::block_on(read_deadline(
            futures_util::future::pending::<Result<(), LixError>>(),
            Duration::from_millis(1),
        ));
        assert_eq!(result.unwrap_err().code, "LIX_READ_DEADLINE_EXCEEDED");
    }

    #[test]
    fn result_budget_bounds_whole_operation_and_integer_overflow() {
        let mut budget = ReadResultBudget::default();
        budget
            .charge(MAX_READ_RESULT_BYTES, MAX_READ_RESULT_ROWS)
            .unwrap();
        assert_eq!(
            budget.charge(1, 0).unwrap_err().code,
            "LIX_READ_RESOURCE_EXHAUSTED"
        );
        assert!(budget.charge(usize::MAX, usize::MAX).is_err());
    }
}
