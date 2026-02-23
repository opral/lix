use crate::deterministic_mode::RuntimeFunctionProvider;
use crate::functions::SharedFunctionProvider;
use crate::sql;
use crate::{LixError, LixTransaction, Value};

use super::super::contracts::effects::DetectedFileDomainChange;
use super::super::contracts::postprocess_actions::{VtableDeletePlan, VtableUpdatePlan};
use super::super::contracts::prepared_statement::PreparedStatement;
use super::super::type_bridge::{
    from_sql_prepared_statements, to_sql_vtable_delete_plan, to_sql_vtable_update_plan,
};

pub(crate) async fn build_update_followup_statements(
    transaction: &mut dyn LixTransaction,
    plan: &VtableUpdatePlan,
    rows: &[Vec<Value>],
    detected_file_domain_changes: &[DetectedFileDomainChange],
    writer_key: Option<&str>,
    functions: &mut SharedFunctionProvider<RuntimeFunctionProvider>,
) -> Result<Vec<PreparedStatement>, LixError> {
    let sql_plan = to_sql_vtable_update_plan(plan);
    build_update_followup_statements_from_sql_plan(
        transaction,
        &sql_plan,
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
    let sql_plan = to_sql_vtable_delete_plan(plan);
    build_delete_followup_statements_from_sql_plan(
        transaction,
        &sql_plan,
        rows,
        params,
        detected_file_domain_changes,
        writer_key,
        functions,
    )
    .await
}

pub(crate) async fn build_update_followup_statements_from_sql_plan(
    transaction: &mut dyn LixTransaction,
    plan: &sql::VtableUpdatePlan,
    rows: &[Vec<Value>],
    detected_file_domain_changes: &[sql::DetectedFileDomainChange],
    writer_key: Option<&str>,
    functions: &mut SharedFunctionProvider<RuntimeFunctionProvider>,
) -> Result<Vec<PreparedStatement>, LixError> {
    let statements = sql::build_update_followup_sql(
        transaction,
        plan,
        rows,
        detected_file_domain_changes,
        writer_key,
        functions,
    )
    .await?;
    Ok(from_sql_prepared_statements(statements))
}

pub(crate) async fn build_delete_followup_statements_from_sql_plan(
    transaction: &mut dyn LixTransaction,
    plan: &sql::VtableDeletePlan,
    rows: &[Vec<Value>],
    params: &[Value],
    detected_file_domain_changes: &[sql::DetectedFileDomainChange],
    writer_key: Option<&str>,
    functions: &mut SharedFunctionProvider<RuntimeFunctionProvider>,
) -> Result<Vec<PreparedStatement>, LixError> {
    let statements = sql::build_delete_followup_sql(
        transaction,
        plan,
        rows,
        params,
        detected_file_domain_changes,
        writer_key,
        functions,
    )
    .await?;
    Ok(from_sql_prepared_statements(statements))
}
