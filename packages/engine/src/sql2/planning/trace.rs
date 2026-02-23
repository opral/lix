use super::super::contracts::planned_statement::PlannedStatementSet;
use super::super::type_bridge::to_sql_preprocess_output;

pub(crate) fn plan_fingerprint(output: &PlannedStatementSet) -> String {
    let sql_output = to_sql_preprocess_output(output);
    crate::sql::preprocess_plan_fingerprint(&sql_output)
}
