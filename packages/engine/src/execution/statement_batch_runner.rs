use std::collections::BTreeMap;

use super::statement_batch::{
    CaptureShape, CaptureSlot, CaptureSlotId, PreparedParam, PreparedStatementBatch,
};
use crate::{LixBackendTransaction, LixError, QueryResult, Value};

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn execute_prepared_statement_batch_with_transaction(
    transaction: &mut dyn LixBackendTransaction,
    statement_batch: &PreparedStatementBatch,
) -> Result<QueryResult, LixError> {
    let slot_defs = statement_batch
        .slots
        .iter()
        .map(|slot| (slot.id.clone(), slot))
        .collect::<BTreeMap<_, _>>();
    let mut slot_values: BTreeMap<CaptureSlotId, QueryResult> = BTreeMap::new();
    let mut last_result = empty_query_result();

    for step in &statement_batch.steps {
        for relation in &step.relation_inputs {
            for setup_step in &relation.setup_steps {
                let params = resolve_params(&slot_defs, &slot_values, &setup_step.params)?;
                transaction.execute(&setup_step.sql, &params).await?;
            }
        }

        let params = resolve_params(&slot_defs, &slot_values, &step.params)?;
        let result = transaction.execute(&step.sql, &params).await?;

        if let Some(slot_id) = &step.capture {
            let slot = slot_defs.get(slot_id).ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "prepared statement batch referenced undefined capture slot '{}'",
                    slot_id.0
                ),
                hint: None,
            })?;
            validate_slot_capture(slot, &result)?;
            slot_values.insert(slot_id.clone(), result.clone());
        }

        for relation in &step.relation_inputs {
            for cleanup_step in &relation.cleanup_steps {
                let params = resolve_params(&slot_defs, &slot_values, &cleanup_step.params)?;
                transaction.execute(&cleanup_step.sql, &params).await?;
            }
        }

        last_result = result;
    }

    Ok(last_result)
}

#[cfg_attr(not(test), allow(dead_code))]
fn resolve_params(
    slot_defs: &BTreeMap<CaptureSlotId, &CaptureSlot>,
    slot_values: &BTreeMap<CaptureSlotId, QueryResult>,
    params: &[PreparedParam],
) -> Result<Vec<Value>, LixError> {
    params
        .iter()
        .map(|param| match param {
            PreparedParam::Literal(value) => Ok(value.clone()),
            PreparedParam::FromScalarSlot { slot } => {
                resolve_scalar_slot(slot_defs, slot_values, slot)
            }
            PreparedParam::FromRowColumn { slot, column } => {
                resolve_row_column(slot_defs, slot_values, slot, column)
            }
        })
        .collect()
}

#[cfg_attr(not(test), allow(dead_code))]
fn resolve_scalar_slot(
    slot_defs: &BTreeMap<CaptureSlotId, &CaptureSlot>,
    slot_values: &BTreeMap<CaptureSlotId, QueryResult>,
    slot_id: &CaptureSlotId,
) -> Result<Value, LixError> {
    let slot = slot_defs.get(slot_id).ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "prepared statement batch referenced undefined capture slot '{}'",
            slot_id.0
        ),
        hint: None,
    })?;
    if slot.shape != CaptureShape::Scalar {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "slot '{}' is not a scalar slot and cannot be used with FromScalarSlot",
                slot_id.0
            ),
            hint: None,
        });
    }
    let value = slot_values
        .get(slot_id)
        .and_then(|result| result.rows.first())
        .and_then(|row| row.first())
        .cloned()
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("scalar slot '{}' did not capture a value", slot_id.0),
            hint: None,
        })?;
    Ok(value)
}

#[cfg_attr(not(test), allow(dead_code))]
fn resolve_row_column(
    slot_defs: &BTreeMap<CaptureSlotId, &CaptureSlot>,
    slot_values: &BTreeMap<CaptureSlotId, QueryResult>,
    slot_id: &CaptureSlotId,
    column_name: &str,
) -> Result<Value, LixError> {
    let slot = slot_defs.get(slot_id).ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!(
            "prepared statement batch referenced undefined capture slot '{}'",
            slot_id.0
        ),
        hint: None,
    })?;
    match slot.shape {
        CaptureShape::OptionalRow | CaptureShape::ExactlyOneRow => {}
        _ => {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "slot '{}' cannot be used with FromRowColumn because it is not row-shaped",
                    slot_id.0
                ),
                hint: None,
            })
        }
    }

    let Some(result) = slot_values.get(slot_id) else {
        return if slot.shape == CaptureShape::OptionalRow {
            Ok(Value::Null)
        } else {
            Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("row slot '{}' did not capture a row", slot_id.0),
                hint: None,
            })
        };
    };

    let Some(row) = result.rows.first() else {
        return if slot.shape == CaptureShape::OptionalRow {
            Ok(Value::Null)
        } else {
            Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("row slot '{}' did not capture a row", slot_id.0),
                hint: None,
            })
        };
    };

    let index = slot
        .columns
        .iter()
        .position(|column| column.name == column_name)
        .or_else(|| {
            result
                .columns
                .iter()
                .position(|column| column == column_name)
        })
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "row slot '{}' is missing column '{}'",
                slot_id.0, column_name
            ),
            hint: None,
        })?;

    Ok(row.get(index).cloned().unwrap_or(Value::Null))
}

#[cfg_attr(not(test), allow(dead_code))]
fn validate_slot_capture(slot: &CaptureSlot, result: &QueryResult) -> Result<(), LixError> {
    match slot.shape {
        CaptureShape::Scalar => {
            if result.rows.len() != 1 || result.rows.first().map(|row| row.len()) != Some(1) {
                return Err(slot_shape_error(
                    slot,
                    "expected exactly one row and one column",
                ));
            }
        }
        CaptureShape::OptionalRow => {
            if result.rows.len() > 1 {
                return Err(slot_shape_error(slot, "expected zero or one row"));
            }
        }
        CaptureShape::ExactlyOneRow => {
            if result.rows.len() != 1 {
                return Err(slot_shape_error(slot, "expected exactly one row"));
            }
        }
        CaptureShape::RowSet => {}
    }

    if !slot.columns.is_empty() && !result.columns.is_empty() {
        let expected = slot
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        let actual = result
            .columns
            .iter()
            .map(|column| column.as_str())
            .collect::<Vec<_>>();
        if expected != actual {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "slot '{}' captured columns {:?} but expected {:?}",
                    slot.id.0, actual, expected
                ),
                hint: None,
            });
        }
    }

    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
fn slot_shape_error(slot: &CaptureSlot, message: &str) -> LixError {
    LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("slot '{}' shape violation: {}", slot.id.0, message),
        hint: None,
    }
}

fn empty_query_result() -> QueryResult {
    QueryResult {
        rows: Vec::new(),
        columns: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::super::statement_batch::{
        CaptureColumn, CaptureShape, CaptureSlot, CaptureSlotId, CaptureValueType, PreparedParam,
        PreparedStatementBatch, PreparedStep,
    };
    use super::execute_prepared_statement_batch_with_transaction;
    use crate::{LixBackendTransaction, LixError, QueryResult, SqlDialect, Value};

    struct FakeTransaction {
        log: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        fail_sql: Option<String>,
    }

    #[async_trait]
    impl LixBackendTransaction for FakeTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        fn mode(&self) -> crate::backend::TransactionBeginMode {
            crate::backend::TransactionBeginMode::Write
        }

        async fn execute(&mut self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            self.log.lock().unwrap().push(format!("tx:{sql}"));
            if self.fail_sql.as_deref() == Some(sql) {
                return Err(LixError::new("LIX_ERROR_UNKNOWN", "boom"));
            }
            if sql == "SELECT 1 AS one" {
                return Ok(QueryResult {
                    rows: vec![vec![Value::Integer(1)]],
                    columns: vec!["one".to_string()],
                });
            }
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            self.log.lock().unwrap().push("commit".to_string());
            Ok(())
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            self.log.lock().unwrap().push("rollback".to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn prepared_statement_batch_can_capture_and_reuse_slot_values() {
        let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut transaction = FakeTransaction {
            log,
            fail_sql: None,
        };
        let statement_batch = PreparedStatementBatch {
            slots: vec![CaptureSlot {
                id: CaptureSlotId("scalar".to_string()),
                shape: CaptureShape::Scalar,
                columns: vec![CaptureColumn {
                    name: "one".to_string(),
                    value_type: CaptureValueType::Integer,
                }],
            }],
            steps: vec![
                PreparedStep {
                    sql: "SELECT 1 AS one".to_string(),
                    params: Vec::new(),
                    capture: Some(CaptureSlotId("scalar".to_string())),
                    relation_inputs: Vec::new(),
                },
                PreparedStep {
                    sql: "INSERT INTO test VALUES ($1)".to_string(),
                    params: vec![PreparedParam::FromScalarSlot {
                        slot: CaptureSlotId("scalar".to_string()),
                    }],
                    capture: None,
                    relation_inputs: Vec::new(),
                },
            ],
        };

        execute_prepared_statement_batch_with_transaction(&mut transaction, &statement_batch)
            .await
            .expect("statement batch should succeed");
    }
}
