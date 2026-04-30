mod common;
mod lix_empty_blob;
mod lix_json;
mod lix_json_extract;
mod lix_json_extract_text;
mod lix_text_decode;
mod lix_text_encode;
mod lix_uuid_v7;

use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::ScalarUDF;

pub(crate) use lix_json_extract::lix_json_extract_expr;
pub(crate) use lix_json_extract_text::lix_json_extract_text_expr;
pub(crate) use lix_text_encode::lix_text_encode_expr;

use crate::engine2::functions::{
    FunctionProvider, FunctionProviderHandle, SharedFunctionProvider, SystemFunctionProvider,
};

pub(crate) fn system_sql2_function_provider() -> FunctionProviderHandle {
    SharedFunctionProvider::new(Box::new(SystemFunctionProvider) as Box<dyn FunctionProvider + Send>)
}

pub(crate) fn register_sql2_functions(ctx: &SessionContext, functions: FunctionProviderHandle) {
    ctx.register_udf(ScalarUDF::from(lix_json_extract::LixJsonExtract::new()));
    ctx.register_udf(ScalarUDF::from(
        lix_json_extract_text::LixJsonExtractText::new(),
    ));
    ctx.register_udf(ScalarUDF::from(lix_text_decode::LixTextDecode::new()));
    ctx.register_udf(ScalarUDF::from(lix_text_encode::LixTextEncode::new()));
    ctx.register_udf(ScalarUDF::from(lix_json::LixJson));
    ctx.register_udf(ScalarUDF::from(lix_empty_blob::LixEmptyBlob));
    ctx.register_udf(ScalarUDF::from(lix_uuid_v7::LixUuidV7 { functions }));
}

#[cfg(test)]
pub(super) mod test_support {
    use datafusion::arrow::array::{Array, BinaryArray, StringArray};
    use datafusion::prelude::SessionContext;

    use super::{register_sql2_functions, system_sql2_function_provider};

    pub(super) async fn single_text(sql: &str) -> Option<String> {
        let ctx = SessionContext::new();
        register_sql2_functions(&ctx, system_sql2_function_provider());
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

    pub(super) async fn single_binary(sql: &str) -> Option<Vec<u8>> {
        let ctx = SessionContext::new();
        register_sql2_functions(&ctx, system_sql2_function_provider());
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
            .downcast_ref::<BinaryArray>()
            .expect("first column should be binary");
        (!array.is_null(0)).then(|| array.value(0).to_vec())
    }
}
