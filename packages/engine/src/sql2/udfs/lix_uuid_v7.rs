use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{plan_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::functions::FunctionProviderHandle;

#[derive(Clone)]
pub(super) struct LixUuidV7 {
    pub(super) functions: FunctionProviderHandle,
}

impl PartialEq for LixUuidV7 {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for LixUuidV7 {}

impl std::hash::Hash for LixUuidV7 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl std::fmt::Debug for LixUuidV7 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixUuidV7").finish()
    }
}

impl ScalarUDFImpl for LixUuidV7 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "lix_uuid_v7"
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
            return plan_err!("lix_uuid_v7 requires no arguments");
        }
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            self.functions.call_uuid_v7().to_string(),
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::single_text;

    #[tokio::test]
    async fn returns_uuid_text() {
        let value = single_text("SELECT lix_uuid_v7()")
            .await
            .expect("uuid should not be null");
        assert!(!value.is_empty());
    }
}
