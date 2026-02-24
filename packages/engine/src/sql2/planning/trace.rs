use super::super::contracts::planned_statement::PlannedStatementSet;
use super::super::contracts::legacy_sql::preprocess_plan_fingerprint;

pub(crate) fn plan_fingerprint(output: &PlannedStatementSet) -> String {
    preprocess_plan_fingerprint(output)
}
