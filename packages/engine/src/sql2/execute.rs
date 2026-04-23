use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::prelude::SessionContext;

use crate::binary_cas::BlobDataReader;
use crate::live_state::LiveStateContext;
use crate::{LixError, QueryResult, Value};

use super::lix_state_provider::register_lix_state_providers;

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
    ctx: &dyn SqlExecutionContext,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    let session = SessionContext::new();
    register_lix_state_providers(&session, ctx.live_state()).await?;

    let mut dataframe = session
        .sql(sql)
        .await
        .map_err(datafusion_error_to_lix_error)?;
    if !params.is_empty() {
        dataframe = dataframe
            .with_param_values(params.iter().map(scalar_value_from_lix_value).collect::<Vec<_>>())
            .map_err(datafusion_error_to_lix_error)?;
    }

    let result_columns = dataframe
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect::<Vec<_>>();
    let batches = dataframe.collect().await.map_err(datafusion_error_to_lix_error)?;
    query_result_from_batches(&result_columns, &batches)
}

fn scalar_value_from_lix_value(value: &Value) -> ScalarValue {
    match value {
        Value::Null => ScalarValue::Null,
        Value::Boolean(value) => ScalarValue::Boolean(Some(*value)),
        Value::Integer(value) => ScalarValue::Int64(Some(*value)),
        Value::Real(value) => ScalarValue::Float64(Some(*value)),
        Value::Text(value) => ScalarValue::Utf8(Some(value.clone())),
        Value::Json(value) => ScalarValue::Utf8(Some(value.to_string())),
        Value::Blob(value) => ScalarValue::Binary(Some(value.clone())),
    }
}

fn datafusion_error_to_lix_error(error: datafusion::error::DataFusionError) -> LixError {
    LixError::new("LIX_ERROR_UNKNOWN", format!("sql2 DataFusion error: {error}"))
}

fn query_result_from_batches(
    result_columns: &[String],
    batches: &[RecordBatch],
) -> Result<QueryResult, LixError> {
    let mut rows = Vec::<Vec<Value>>::new();
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let mut row = Vec::<Value>::with_capacity(batch.num_columns());
            for array in batch.columns() {
                let scalar = ScalarValue::try_from_array(array.as_ref(), row_index)
                    .map_err(datafusion_error_to_lix_error)?;
                row.push(scalar_value_to_lix_value(&scalar));
            }
            rows.push(row);
        }
    }

    Ok(QueryResult {
        rows,
        columns: result_columns.to_vec(),
    })
}

fn scalar_value_to_lix_value(value: &ScalarValue) -> Value {
    match value {
        ScalarValue::Null => Value::Null,
        ScalarValue::Boolean(Some(value)) => Value::Boolean(*value),
        ScalarValue::Boolean(None) => Value::Null,
        ScalarValue::Int8(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::Int8(None) => Value::Null,
        ScalarValue::Int16(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::Int16(None) => Value::Null,
        ScalarValue::Int32(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::Int32(None) => Value::Null,
        ScalarValue::Int64(Some(value)) => Value::Integer(*value),
        ScalarValue::Int64(None) => Value::Null,
        ScalarValue::UInt8(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::UInt8(None) => Value::Null,
        ScalarValue::UInt16(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::UInt16(None) => Value::Null,
        ScalarValue::UInt32(Some(value)) => Value::Integer(i64::from(*value)),
        ScalarValue::UInt32(None) => Value::Null,
        ScalarValue::UInt64(Some(value)) => match i64::try_from(*value) {
            Ok(value) => Value::Integer(value),
            Err(_) => Value::Text(value.to_string()),
        },
        ScalarValue::UInt64(None) => Value::Null,
        ScalarValue::Float32(Some(value)) => Value::Real(f64::from(*value)),
        ScalarValue::Float32(None) => Value::Null,
        ScalarValue::Float64(Some(value)) => Value::Real(*value),
        ScalarValue::Float64(None) => Value::Null,
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => Value::Text(value.clone()),
        ScalarValue::Utf8(None) | ScalarValue::Utf8View(None) | ScalarValue::LargeUtf8(None) => {
            Value::Null
        }
        ScalarValue::Binary(Some(value)) | ScalarValue::LargeBinary(Some(value)) => {
            Value::Blob(value.clone())
        }
        ScalarValue::Binary(None) | ScalarValue::LargeBinary(None) => Value::Null,
        other => Value::Text(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use serde_json::json;

    use super::{execute_sql, SqlExecutionContext};
    use crate::binary_cas::BlobDataReader;
    use crate::live_state::{
        CommittedLiveStateContext, ExactRowRequest, LiveRow, LiveStateContext, LiveStateScanRequest,
    };
    use crate::test_support::boot_test_engine;
    use crate::{CreateVersionOptions, LixError, Value};

    struct DummyBlobReader;
    struct DummyLiveStateContext;
    struct BackendBlobReader<'a>(&'a dyn crate::LixBackend);

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

    #[async_trait(?Send)]
    impl BlobDataReader for BackendBlobReader<'_> {
        async fn load_blob_data_by_hash(
            &self,
            blob_hash: &str,
        ) -> Result<Option<Vec<u8>>, LixError> {
            self.0.load_blob_data_by_hash(blob_hash).await
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

        let result = execute_sql(&ctx, "SELECT 1", &[])
            .await
            .expect("sql2 execute should support literal-only queries");
        assert_eq!(result.rows, vec![vec![Value::Integer(1)]]);
    }

    struct BackendSqlExecutionContext<'a> {
        blob_reader: BackendBlobReader<'a>,
        live_state: CommittedLiveStateContext<'a>,
    }

    impl SqlExecutionContext for BackendSqlExecutionContext<'_> {
        fn live_state(&self) -> &dyn LiveStateContext {
            &self.live_state
        }

        fn blob_reader(&self) -> &dyn BlobDataReader {
            &self.blob_reader
        }
    }

    async fn setup_sql2_state_fixture(
    ) -> Result<(crate::test_support::TestSqliteBackend, crate::Session), crate::LixError> {
        let (backend, _lix, session) = boot_test_engine().await?;
        session
            .register_schema(&json!({
                "x-lix-key": "test_state_schema",
                "x-lix-version": "1",
                "type": "object",
                "properties": {
                    "value": { "type": "string" }
                },
                "required": ["value"],
                "additionalProperties": false
            }))
            .await?;
        session
            .create_version(CreateVersionOptions {
                id: Some("version-a".to_string()),
                name: Some("version-a".to_string()),
                ..CreateVersionOptions::default()
            })
            .await?;
        session
            .create_version(CreateVersionOptions {
                id: Some("version-b".to_string()),
                name: Some("version-b".to_string()),
                ..CreateVersionOptions::default()
            })
            .await?;
        session
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-a', 'test_state_schema', NULL, 'version-a', NULL, '{\"value\":\"A\"}', '1'\
                 )",
                &[],
            )
            .await?;
        session
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version\
                 ) VALUES (\
                 'entity-b', 'test_state_schema', NULL, 'version-b', NULL, '{\"value\":\"B\"}', '1'\
                 )",
                &[],
            )
            .await?;
        Ok((backend, session))
    }

    fn run_async_test_with_large_stack(
        test: impl FnOnce() -> futures_util::future::LocalBoxFuture<'static, ()> + Send + 'static,
    ) {
        std::thread::Builder::new()
            .name("sql2-execute-test".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("test runtime should build")
                    .block_on(test());
            })
            .expect("test thread should spawn")
            .join()
            .expect("test thread should join");
    }

    #[test]
    fn execute_sql_reads_lix_state_by_version() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let backend_ref: &dyn crate::LixBackend = &backend;
                let ctx = BackendSqlExecutionContext {
                    blob_reader: BackendBlobReader(backend_ref),
                    live_state: CommittedLiveStateContext::new(backend_ref),
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT entity_id, version_id, snapshot_content, commit_id \
                     FROM lix_state_by_version \
                     WHERE version_id = 'version-b' AND schema_key = 'test_state_schema'",
                    &[],
                )
                .await
                .expect("sql2 execute should read lix_state_by_version");

                assert_eq!(
                    result.columns,
                    vec!["entity_id", "version_id", "snapshot_content", "commit_id"]
                );
                assert_eq!(result.rows.len(), 1);
                assert_eq!(result.rows[0][0], Value::Text("entity-b".to_string()));
                assert_eq!(result.rows[0][1], Value::Text("version-b".to_string()));
                assert_eq!(
                    result.rows[0][2],
                    Value::Text("{\"value\":\"B\"}".to_string())
                );
                match &result.rows[0][3] {
                    Value::Text(commit_id) => assert!(!commit_id.is_empty()),
                    other => panic!("expected non-null commit_id text, got {other:?}"),
                }
            })
        });
    }

    #[test]
    fn execute_sql_supports_broad_lix_state_by_version_reads() {
        run_async_test_with_large_stack(|| {
            Box::pin(async move {
                let (backend, _session) = setup_sql2_state_fixture()
                    .await
                    .expect("fixture should initialize");
                let backend_ref: &dyn crate::LixBackend = &backend;
                let ctx = BackendSqlExecutionContext {
                    blob_reader: BackendBlobReader(backend_ref),
                    live_state: CommittedLiveStateContext::new(backend_ref),
                };

                let result = execute_sql(
                    &ctx,
                    "SELECT entity_id FROM lix_state_by_version WHERE schema_key = 'test_state_schema'",
                    &[],
                )
                .await
                .expect("broad by-version read should succeed");

                assert!(
                    result.rows.iter().any(|row| row[0] == Value::Text("entity-a".to_string()))
                        && result.rows.iter().any(|row| row[0] == Value::Text("entity-b".to_string())),
                    "expected broad by-version read to include rows from multiple visible versions: {:?}",
                    result.rows
                );
            })
        });
    }
}
