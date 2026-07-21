use std::sync::Arc;

use datafusion::arrow::datatypes::Field;
use datafusion::arrow::record_batch::RecordBatch;

use crate::entity_pk::EntityPk;
use crate::sql2::SqlExecutionContext;
use crate::sql2::bind::primary_key_read::{
    BoundPrimaryKeyProjection, BoundPrimaryKeyRead, BoundPrimaryKeyReadTarget,
};
use crate::sql2::session::PreparedReadSession;
use crate::{LixError, SqlQueryResult};

use super::datafusion::query_result_from_batches;

/// Execute a primary-key read after binding has committed to the native path.
///
/// This function has no unsupported/fallback result: once a provider loader is
/// entered, every error is returned to the caller. Transaction-overlay reads
/// cannot call this API because it accepts only the committed read context.
pub(crate) async fn execute_primary_key_read<C>(
    ctx: &C,
    prepared: &PreparedReadSession,
    read: &BoundPrimaryKeyRead,
) -> Result<SqlQueryResult, LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    let provider_projection = read
        .projection
        .iter()
        .map(|projection| projection.source_index)
        .collect::<Vec<_>>();

    let batch = match &read.target {
        BoundPrimaryKeyReadTarget::File { ids } => {
            crate::sql2::providers::load_active_lix_file_ids(
                ctx,
                Arc::clone(&prepared.branch_ref),
                &provider_projection,
                ids,
            )
            .await?
        }
        BoundPrimaryKeyReadTarget::Entity {
            schema_key, keys, ..
        } => {
            let spec = prepared
                .catalog
                .entity_spec(schema_key)
                .cloned()
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "native primary-key read lost entity schema '{schema_key}' after binding"
                        ),
                    )
                })?;
            let entity_pks = keys
                .iter()
                .cloned()
                .map(EntityPk::from_parts)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "native primary-key binder produced an invalid entity key for '{schema_key}': {error}"
                        ),
                    )
                })?;

            crate::sql2::providers::load_active_entity_pks(
                ctx,
                Arc::clone(&prepared.branch_ref),
                Arc::new(spec),
                &provider_projection,
                &entity_pks,
            )
            .await?
        }
    };

    let result_fields = aliased_result_fields(&batch, &read.projection)?;
    query_result_from_batches(&result_fields, std::slice::from_ref(&batch))
}

/// Rename the provider's projected fields without rebuilding their types or
/// metadata. The metadata is semantically significant: JSON-backed Utf8
/// columns must still become `Value::Json` after a SQL alias is applied.
fn aliased_result_fields(
    batch: &RecordBatch,
    projection: &[BoundPrimaryKeyProjection],
) -> Result<Vec<Field>, LixError> {
    if batch.num_columns() != projection.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "native primary-key provider returned {} columns for a {}-column projection",
                batch.num_columns(),
                projection.len()
            ),
        ));
    }

    batch
        .schema()
        .fields()
        .iter()
        .zip(projection)
        .map(|(field, projection)| {
            if field.name() != &projection.source_name {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "native primary-key provider returned column '{}' where '{}' was bound",
                        field.name(),
                        projection.source_name
                    ),
                ));
            }
            Ok(field
                .as_ref()
                .clone()
                .with_name(projection.output_name.clone()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use serde_json::json;

    use crate::Value;
    use crate::sql2::result_metadata::mark_json_field;

    use super::*;

    #[test]
    fn aliases_preserve_json_field_metadata_during_result_conversion() {
        let source_field = mark_json_field(Field::new("payload", DataType::Utf8, true));
        let source_metadata = source_field.metadata().clone();
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some(r#"{"ok":true}"#)]));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![source_field])), vec![values])
            .expect("test batch");
        let projection = vec![BoundPrimaryKeyProjection {
            source_index: 0,
            source_name: "payload".to_string(),
            output_name: "document".to_string(),
        }];

        let fields = aliased_result_fields(&batch, &projection).expect("alias fields");

        assert_eq!(fields[0].name(), "document");
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
        assert!(fields[0].is_nullable());
        assert_eq!(fields[0].metadata(), &source_metadata);

        let result = query_result_from_batches(&fields, &[batch]).expect("convert result");
        assert_eq!(result.columns, vec!["document"]);
        assert_eq!(result.rows, vec![vec![Value::Json(json!({ "ok": true }))]]);
    }

    #[test]
    fn aliased_fields_reject_provider_projection_drift() {
        let batch = RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
            "actual",
            DataType::Utf8,
            false,
        )])));
        let projection = vec![BoundPrimaryKeyProjection {
            source_index: 0,
            source_name: "expected".to_string(),
            output_name: "alias".to_string(),
        }];

        let error = aliased_result_fields(&batch, &projection).expect_err("schema must match");

        assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
        assert!(error.message.contains("actual"));
        assert!(error.message.contains("expected"));
    }
}
