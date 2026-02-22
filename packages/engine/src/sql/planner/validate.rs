use crate::LixError;

pub(crate) fn ensure_single_statement_plan(statement_count: usize) -> Result<(), LixError> {
    if statement_count == 0 {
        return Err(LixError {
            message: "planner received empty statement block".to_string(),
        });
    }
    Ok(())
}
