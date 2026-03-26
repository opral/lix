use std::collections::BTreeMap;

use crate::backend::prepared::{PreparedBatch, PreparedStatement};
use crate::backend::program::{
    PreparedParam, PreparedProgram, ProgramSlot, ProgramSlotId, SlotShape, WriteProgram, WriteStep,
};
use crate::{LixBackend, LixBackendTransaction, LixError, QueryResult, TransactionMode, Value};

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn execute_write_program_with_backend(
    backend: &dyn LixBackend,
    program: WriteProgram,
) -> Result<QueryResult, LixError> {
    let mut transaction = backend.begin_transaction(TransactionMode::Write).await?;
    let result = execute_write_program_steps_with_transaction(transaction.as_mut(), program).await;
    match result {
        Ok(result) => {
            transaction.commit().await?;
            Ok(result)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

pub(crate) async fn execute_write_program_with_transaction(
    transaction: &mut dyn LixBackendTransaction,
    program: WriteProgram,
) -> Result<QueryResult, LixError> {
    execute_write_program_steps_with_transaction(transaction, program).await
}

async fn execute_write_program_steps_with_transaction(
    transaction: &mut dyn LixBackendTransaction,
    program: WriteProgram,
) -> Result<QueryResult, LixError> {
    let mut batch = PreparedBatch { steps: Vec::new() };
    for step in program.steps {
        match step {
            WriteStep::PreparedBatch(other) => batch.extend(other),
            WriteStep::Statement { sql, params } => {
                batch.push_statement(PreparedStatement { sql, params });
            }
        }
    }
    transaction.execute_batch(&batch).await
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn execute_prepared_program_with_transaction(
    transaction: &mut dyn LixBackendTransaction,
    program: &PreparedProgram,
) -> Result<QueryResult, LixError> {
    let slot_defs = program
        .slots
        .iter()
        .map(|slot| (slot.id.clone(), slot))
        .collect::<BTreeMap<_, _>>();
    let mut slot_values: BTreeMap<ProgramSlotId, QueryResult> = BTreeMap::new();
    let mut last_result = empty_query_result();

    for step in &program.steps {
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
                description: format!("prepared program referenced undefined slot '{}'", slot_id.0),
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
    slot_defs: &BTreeMap<ProgramSlotId, &ProgramSlot>,
    slot_values: &BTreeMap<ProgramSlotId, QueryResult>,
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
    slot_defs: &BTreeMap<ProgramSlotId, &ProgramSlot>,
    slot_values: &BTreeMap<ProgramSlotId, QueryResult>,
    slot_id: &ProgramSlotId,
) -> Result<Value, LixError> {
    let slot = slot_defs.get(slot_id).ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("prepared program referenced undefined slot '{}'", slot_id.0),
    })?;
    if slot.shape != SlotShape::Scalar {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "slot '{}' is not a scalar slot and cannot be used with FromScalarSlot",
                slot_id.0
            ),
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
        })?;
    Ok(value)
}

#[cfg_attr(not(test), allow(dead_code))]
fn resolve_row_column(
    slot_defs: &BTreeMap<ProgramSlotId, &ProgramSlot>,
    slot_values: &BTreeMap<ProgramSlotId, QueryResult>,
    slot_id: &ProgramSlotId,
    column_name: &str,
) -> Result<Value, LixError> {
    let slot = slot_defs.get(slot_id).ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("prepared program referenced undefined slot '{}'", slot_id.0),
    })?;
    match slot.shape {
        SlotShape::OptionalRow | SlotShape::ExactlyOneRow => {}
        _ => {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "slot '{}' cannot be used with FromRowColumn because it is not row-shaped",
                    slot_id.0
                ),
            })
        }
    }

    let Some(result) = slot_values.get(slot_id) else {
        return if slot.shape == SlotShape::OptionalRow {
            Ok(Value::Null)
        } else {
            Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("row slot '{}' did not capture a row", slot_id.0),
            })
        };
    };

    let Some(row) = result.rows.first() else {
        return if slot.shape == SlotShape::OptionalRow {
            Ok(Value::Null)
        } else {
            Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!("row slot '{}' did not capture a row", slot_id.0),
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
        })?;

    Ok(row.get(index).cloned().unwrap_or(Value::Null))
}

#[cfg_attr(not(test), allow(dead_code))]
fn validate_slot_capture(slot: &ProgramSlot, result: &QueryResult) -> Result<(), LixError> {
    match slot.shape {
        SlotShape::Scalar => {
            if result.rows.len() != 1 || result.rows.first().map(|row| row.len()) != Some(1) {
                return Err(slot_shape_error(
                    slot,
                    "expected exactly one row and one column",
                ));
            }
        }
        SlotShape::OptionalRow => {
            if result.rows.len() > 1 {
                return Err(slot_shape_error(slot, "expected zero or one row"));
            }
        }
        SlotShape::ExactlyOneRow => {
            if result.rows.len() != 1 {
                return Err(slot_shape_error(slot, "expected exactly one row"));
            }
        }
        SlotShape::RowSet => {}
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
            });
        }
    }

    Ok(())
}

#[cfg_attr(not(test), allow(dead_code))]
fn slot_shape_error(slot: &ProgramSlot, message: &str) -> LixError {
    LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("slot '{}' shape violation: {}", slot.id.0, message),
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

    use super::{execute_prepared_program_with_transaction, execute_write_program_with_backend};
    use crate::backend::program::{
        PreparedParam, PreparedProgram, PreparedStep, ProgramSlot, ProgramSlotId, SlotColumn,
        SlotShape, SlotValueType, WriteProgram,
    };
    use crate::{LixBackend, LixBackendTransaction, LixError, QueryResult, SqlDialect, Value};

    #[derive(Default)]
    struct FakeBackend {
        log: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        fail_sql: Option<String>,
    }

    struct FakeTransaction {
        log: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        fail_sql: Option<String>,
    }

    #[async_trait(?Send)]
    impl LixBackend for FakeBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            self.log.lock().unwrap().push(format!("backend:{sql}"));
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(
            &self,
            _mode: crate::TransactionMode,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            self.log.lock().unwrap().push("begin".to_string());
            Ok(Box::new(FakeTransaction {
                log: std::sync::Arc::clone(&self.log),
                fail_sql: self.fail_sql.clone(),
            }))
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            self.begin_transaction(crate::TransactionMode::Write).await
        }
    }

    #[async_trait(?Send)]
    impl LixBackendTransaction for FakeTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        fn mode(&self) -> crate::TransactionMode {
            crate::TransactionMode::Write
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
    async fn owned_runner_commits_on_success() {
        let backend = FakeBackend::default();
        let mut program = WriteProgram::new();
        program.push_statement("INSERT INTO test VALUES (1)", Vec::new());

        execute_write_program_with_backend(&backend, program)
            .await
            .expect("program should succeed");

        let log = backend.log.lock().unwrap().clone();
        assert_eq!(
            log,
            vec![
                "begin".to_string(),
                "tx:INSERT INTO test VALUES (1)".to_string(),
                "commit".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn owned_runner_rolls_back_on_failure() {
        let backend = FakeBackend {
            log: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            fail_sql: Some("INSERT INTO fail VALUES (1)".to_string()),
        };
        let mut program = WriteProgram::new();
        program.push_statement("INSERT INTO fail VALUES (1)", Vec::new());

        let error = execute_write_program_with_backend(&backend, program)
            .await
            .expect_err("program should fail");
        assert!(error.description.contains("boom"));

        let log = backend.log.lock().unwrap().clone();
        assert_eq!(
            log,
            vec![
                "begin".to_string(),
                "tx:INSERT INTO fail VALUES (1)".to_string(),
                "rollback".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn prepared_program_can_capture_and_reuse_slot_values() {
        let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut transaction = FakeTransaction {
            log,
            fail_sql: None,
        };
        let program = PreparedProgram {
            slots: vec![ProgramSlot {
                id: ProgramSlotId("scalar".to_string()),
                shape: SlotShape::Scalar,
                columns: vec![SlotColumn {
                    name: "one".to_string(),
                    value_type: SlotValueType::Integer,
                }],
            }],
            steps: vec![
                PreparedStep {
                    sql: "SELECT 1 AS one".to_string(),
                    params: Vec::new(),
                    capture: Some(ProgramSlotId("scalar".to_string())),
                    relation_inputs: Vec::new(),
                },
                PreparedStep {
                    sql: "INSERT INTO test VALUES ($1)".to_string(),
                    params: vec![PreparedParam::FromScalarSlot {
                        slot: ProgramSlotId("scalar".to_string()),
                    }],
                    capture: None,
                    relation_inputs: Vec::new(),
                },
            ],
        };

        execute_prepared_program_with_transaction(&mut transaction, &program)
            .await
            .expect("program should succeed");
    }
}
