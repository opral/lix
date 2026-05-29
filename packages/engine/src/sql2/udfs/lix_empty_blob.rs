use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct LixEmptyBlob;

impl ScalarUDFImpl for LixEmptyBlob {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "lix_empty_blob"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::nullary(Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(Vec::new()))))
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::single_binary;

    #[tokio::test]
    async fn returns_empty_binary_value() {
        assert_eq!(
            single_binary("SELECT lix_empty_blob()").await,
            Some(Vec::new())
        );
    }
}
