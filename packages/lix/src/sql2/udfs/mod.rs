pub(crate) mod common;
mod current_timestamp;
pub(crate) mod execution_slots;
mod lix_active_account_id;
mod lix_active_branch_commit_id;
mod lix_active_branch_id;
mod lix_json_get;
mod lix_json_get_text;
mod lix_json_predicate;
mod lix_jsonb;
mod lix_latest_checkpoint_commit_id;
mod lix_octet_length;
mod lix_root_commit_id;
mod lix_row_ref;
mod uuidv7;

use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::ScalarUDF;

use crate::functions::FunctionProviderHandle;

pub(crate) use execution_slots::{ExecutionSlots, execution_slots};

pub(crate) fn register_row_ref_function(
    ctx: &SessionContext,
    catalog: Arc<crate::sql2::catalog::PublicCatalog>,
) {
    ctx.register_udf(ScalarUDF::from(lix_row_ref::LixRowRef::new(catalog)));
}

#[cfg(test)]
pub(crate) fn system_sql2_function_provider() -> FunctionProviderHandle {
    FunctionProviderHandle::system()
}

pub(crate) fn register_static_sql2_functions(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(lix_json_get::LixJsonGet::new(
        "__lix_json_get",
    )));
    ctx.register_udf(ScalarUDF::from(lix_json_get::LixJsonGet::new(
        "__lix_json_path_get",
    )));
    ctx.register_udf(ScalarUDF::from(lix_json_get_text::LixJsonGetText::new(
        "__lix_json_get_text",
    )));
    ctx.register_udf(ScalarUDF::from(lix_json_get_text::LixJsonGetText::new(
        "__lix_json_path_get_text",
    )));
    ctx.register_udf(ScalarUDF::from(
        lix_json_predicate::LixJsonPredicate::contains(),
    ));
    ctx.register_udf(ScalarUDF::from(
        lix_json_predicate::LixJsonPredicate::exists(),
    ));
    ctx.register_udf(ScalarUDF::from(lix_jsonb::LixJsonb::new()));
    ctx.register_udf(ScalarUDF::from(lix_octet_length::LixOctetLength::new()));
}

/// Installs the per-statement execution functions once, for the lifetime of
/// the session.
///
/// Each one holds only the session's [`ExecutionSlots`] and reads the current
/// statement's value at invocation time, so a session can be pooled and reused
/// without a function ever reporting an earlier statement's account, branch or
/// commit. Call [`bind_execution_sql2_functions`] before planning each
/// statement.
pub(crate) fn register_execution_sql2_functions(ctx: &SessionContext, slots: Arc<ExecutionSlots>) {
    ctx.register_udf(ScalarUDF::from(
        lix_active_account_id::LixActiveAccountId::new(Arc::clone(&slots)),
    ));
    ctx.register_udf(ScalarUDF::from(
        lix_active_branch_id::LixActiveBranchId::new(Arc::clone(&slots)),
    ));
    ctx.register_udf(ScalarUDF::from(
        lix_active_branch_commit_id::LixActiveBranchCommitId::new(Arc::clone(&slots)),
    ));
    ctx.register_udf(ScalarUDF::from(
        lix_latest_checkpoint_commit_id::LixLatestCheckpointCommitId::new(Arc::clone(&slots)),
    ));
    ctx.register_udf(ScalarUDF::from(lix_root_commit_id::LixRootCommitId::new(
        Arc::clone(&slots),
    )));
    ctx.register_udf(ScalarUDF::from(uuidv7::UuidV7 {
        slots: Arc::clone(&slots),
    }));
    ctx.register_udf(ScalarUDF::from(current_timestamp::CurrentTimestamp {
        slots,
    }));
}

/// Points the session's execution functions at this statement's facts.
pub(crate) fn bind_execution_sql2_functions(
    ctx: &SessionContext,
    functions: FunctionProviderHandle,
    active_account_id: &str,
    active_branch_id: Option<&str>,
    active_branch_commit_id: Option<&str>,
    latest_checkpoint_commit_id: Option<&str>,
    root_commit_id: Option<&str>,
) {
    execution_slots(ctx).bind(
        functions,
        active_account_id,
        active_branch_id,
        active_branch_commit_id,
        latest_checkpoint_commit_id,
        root_commit_id,
    );
}

#[cfg(test)]
pub(super) mod test_support {
    use datafusion::arrow::array::{Array, StringArray};

    use super::{bind_execution_sql2_functions, system_sql2_function_provider};

    pub(super) async fn single_text(sql: &str) -> Option<String> {
        let ctx = crate::sql2::session::new_sql_session_context();
        bind_execution_sql2_functions(
            &ctx,
            system_sql2_function_provider(),
            crate::ANONYMOUS_ACCOUNT_ID,
            None,
            None,
            None,
            None,
        );
        let batches = ctx
            .sql(sql)
            .await
            .expect("query should plan")
            .collect()
            .await
            .expect("query should execute");
        let array = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("first column should be utf8");
        (!array.is_null(0)).then(|| array.value(0).to_string())
    }
}
