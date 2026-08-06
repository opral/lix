use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::functions::FunctionProviderHandle;

#[derive(Clone)]
pub(super) struct LixTimestamp {
    pub(super) functions: FunctionProviderHandle,
}

impl PartialEq for LixTimestamp {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for LixTimestamp {}

impl std::hash::Hash for LixTimestamp {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl std::fmt::Debug for LixTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixTimestamp").finish()
    }
}

impl ScalarUDFImpl for LixTimestamp {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "lix_timestamp"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::nullary(Volatility::Volatile));
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !args.args.is_empty() {
            return plan_err!("lix_timestamp requires no arguments");
        }
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            self.functions.call_timestamp().to_string(),
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::single_text;

    #[tokio::test]
    async fn returns_timestamp_text() {
        let value = single_text("SELECT lix_timestamp()")
            .await
            .expect("timestamp should not be null");
        assert!(!value.is_empty());
    }
}
