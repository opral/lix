use crate::binary_cas::BlobDataReader;
use crate::live_state::LiveStateContext;
use crate::{LixError, QueryResult, Value};

/// Single execution boundary for `sql2::execute_sql(...)`.
///
/// Session and transaction orchestration stay above `sql2`. They provide the
/// execution-scoped visible live-state context for each call.
///
/// Catalog lookup/registration will likely join this boundary later, but we
/// are intentionally not carrying it yet until the new DataFusion-owned path
/// actually needs it.
#[allow(dead_code)]
pub(crate) trait SqlExecutionContext {
    fn live_state(&self) -> &dyn LiveStateContext;
    fn blob_reader(&self) -> &dyn BlobDataReader;
}

/// Minimal top-level sql2 entrypoint.
///
/// The final implementation will build the DataFusion session from the
/// execution context and source rows from `live_state()`.
///
/// `catalog()` is intentionally omitted from the MVP boundary for now.
#[allow(dead_code)]
pub(crate) async fn execute_sql(
    _ctx: &dyn SqlExecutionContext,
    _sql: &str,
    _params: &[Value],
) -> Result<QueryResult, LixError> {
    Err(LixError::new(
        "LIX_ERROR_UNKNOWN",
        "sql2::execute_sql is not implemented yet",
    ))
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::{execute_sql, SqlExecutionContext};
    use crate::binary_cas::BlobDataReader;
    use crate::live_state::{ExactRowRequest, LiveRow, LiveStateContext, LiveStateScanRequest};
    use crate::LixError;

    struct DummyBlobReader;
    struct DummyLiveStateContext;

    struct DummySqlExecutionContext<'a> {
        blob_reader: &'a dyn BlobDataReader,
        live_state: &'a dyn LiveStateContext,
    }

    impl SqlExecutionContext for DummySqlExecutionContext<'_> {
        fn live_state(&self) -> &dyn LiveStateContext {
            self.live_state
        }

        fn blob_reader(&self) -> &dyn BlobDataReader {
            self.blob_reader
        }
    }

    #[async_trait(?Send)]
    impl LiveStateContext for DummyLiveStateContext {
        async fn scan(&self, _request: &LiveStateScanRequest) -> Result<Vec<LiveRow>, LixError> {
            Ok(vec![])
        }

        async fn load_exact(&self, _request: &ExactRowRequest) -> Result<Option<LiveRow>, LixError> {
            Ok(None)
        }
    }

    #[async_trait(?Send)]
    impl BlobDataReader for DummyBlobReader {
        async fn load_blob_data_by_hash(
            &self,
            _blob_hash: &str,
        ) -> Result<Option<Vec<u8>>, LixError> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn sql_execution_context_exposes_live_state_and_blob_reader() {
        let blob_reader = DummyBlobReader;
        let live_state = DummyLiveStateContext;
        let ctx = DummySqlExecutionContext {
            blob_reader: &blob_reader,
            live_state: &live_state,
        };

        assert!(std::ptr::eq(ctx.live_state(), &live_state as &dyn LiveStateContext));
        assert!(std::ptr::eq(
            ctx.blob_reader(),
            &blob_reader as &dyn BlobDataReader,
        ));
    }

    #[tokio::test]
    async fn execute_sql_uses_execution_context_boundary() {
        let blob_reader = DummyBlobReader;
        let live_state = DummyLiveStateContext;
        let ctx = DummySqlExecutionContext {
            blob_reader: &blob_reader,
            live_state: &live_state,
        };

        let error = execute_sql(&ctx, "SELECT 1", &[])
            .await
            .expect_err("sql2 execute scaffold should be explicit");
        assert!(error.description.contains("sql2::execute_sql"));
    }
}
