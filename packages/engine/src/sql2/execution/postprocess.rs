use crate::deterministic_mode::RuntimeFunctionProvider;
use crate::functions::SharedFunctionProvider;
use crate::{LixError, LixTransaction, Value};

use super::super::contracts::effects::DetectedFileDomainChange;
use super::super::contracts::postprocess_actions::{VtableDeletePlan, VtableUpdatePlan};
use super::super::contracts::prepared_statement::PreparedStatement;
use super::super::legacy_bridge::{
    build_delete_followup_statements_with_sql_bridge,
    build_update_followup_statements_with_sql_bridge,
};

pub(crate) async fn build_update_followup_statements(
    transaction: &mut dyn LixTransaction,
    plan: &VtableUpdatePlan,
    rows: &[Vec<Value>],
    detected_file_domain_changes: &[DetectedFileDomainChange],
    writer_key: Option<&str>,
    functions: &mut SharedFunctionProvider<RuntimeFunctionProvider>,
) -> Result<Vec<PreparedStatement>, LixError> {
    build_update_followup_statements_with_sql_bridge(
        transaction,
        plan,
        rows,
        detected_file_domain_changes,
        writer_key,
        functions,
    )
    .await
}

pub(crate) async fn build_delete_followup_statements(
    transaction: &mut dyn LixTransaction,
    plan: &VtableDeletePlan,
    rows: &[Vec<Value>],
    params: &[Value],
    detected_file_domain_changes: &[DetectedFileDomainChange],
    writer_key: Option<&str>,
    functions: &mut SharedFunctionProvider<RuntimeFunctionProvider>,
) -> Result<Vec<PreparedStatement>, LixError> {
    build_delete_followup_statements_with_sql_bridge(
        transaction,
        plan,
        rows,
        params,
        detected_file_domain_changes,
        writer_key,
        functions,
    )
    .await
}
