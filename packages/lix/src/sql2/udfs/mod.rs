mod common;
mod lix_active_account_id;
mod lix_active_branch_commit_id;
mod lix_active_branch_id;
mod lix_json;
mod lix_json_get;
mod lix_json_get_text;
mod lix_octet_length;
mod lix_timestamp;
mod lix_uuid_v7;

use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::ScalarUDF;

use crate::functions::FunctionProviderHandle;

#[cfg(test)]
pub(crate) fn system_sql2_function_provider() -> FunctionProviderHandle {
    FunctionProviderHandle::system()
}

#[cfg(test)]
pub(crate) fn register_sql2_functions(
    ctx: &SessionContext,
    functions: FunctionProviderHandle,
    active_account_id: String,
    active_branch_id: Option<String>,
    active_branch_commit_id: Option<String>,
) {
    register_static_sql2_functions(ctx);
    register_execution_sql2_functions(
        ctx,
        functions,
        active_account_id,
        active_branch_id,
        active_branch_commit_id,
    );
}

pub(crate) fn register_static_sql2_functions(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(lix_json_get::LixJsonGet::new()));
    ctx.register_udf(ScalarUDF::from(lix_json_get_text::LixJsonGetText::new()));
    ctx.register_udf(ScalarUDF::from(lix_json::LixJson));
    ctx.register_udf(ScalarUDF::from(lix_octet_length::LixOctetLength::new()));
}

pub(crate) fn register_execution_sql2_functions(
    ctx: &SessionContext,
    functions: FunctionProviderHandle,
    active_account_id: String,
    active_branch_id: Option<String>,
    active_branch_commit_id: Option<String>,
) {
    ctx.register_udf(ScalarUDF::from(
        lix_active_account_id::LixActiveAccountId::new(active_account_id),
    ));
    ctx.register_udf(ScalarUDF::from(
        lix_active_branch_id::LixActiveBranchId::new(active_branch_id),
    ));
    ctx.register_udf(ScalarUDF::from(
        lix_active_branch_commit_id::LixActiveBranchCommitId::new(active_branch_commit_id),
    ));
    ctx.register_udf(ScalarUDF::from(lix_uuid_v7::LixUuidV7 {
        functions: functions.clone(),
    }));
    ctx.register_udf(ScalarUDF::from(lix_timestamp::LixTimestamp { functions }));
}

#[cfg(test)]
pub(super) mod test_support {
    use datafusion::arrow::array::{Array, StringArray};
    use datafusion::prelude::SessionContext;

    use super::{register_sql2_functions, system_sql2_function_provider};

    pub(super) async fn single_text(sql: &str) -> Option<String> {
        let ctx = SessionContext::new();
        register_sql2_functions(
            &ctx,
            system_sql2_function_provider(),
            crate::ANONYMOUS_ACCOUNT_ID.to_string(),
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
