use crate::cel::CelEvaluator;
use crate::{LixBackend, LixError, Value};

use super::sql2::contracts::planned_statement::PlannedStatementSet;

pub(crate) async fn preprocess_sql_to_plan(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    sql_text: &str,
    params: &[Value],
) -> Result<PlannedStatementSet, LixError> {
    super::sql2::planning::preprocess::preprocess_sql_to_plan(backend, evaluator, sql_text, params)
        .await
}
