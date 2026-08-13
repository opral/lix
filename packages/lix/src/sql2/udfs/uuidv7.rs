use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use super::execution_slots::ExecutionSlots;

#[derive(Clone)]
pub(super) struct UuidV7 {
    pub(super) slots: Arc<ExecutionSlots>,
}

impl PartialEq for UuidV7 {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for UuidV7 {}

impl std::hash::Hash for UuidV7 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl std::fmt::Debug for UuidV7 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UuidV7").finish()
    }
}

impl ScalarUDFImpl for UuidV7 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "uuidv7"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::nullary(Volatility::Volatile));
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Lix exposes UUID values to SDKs as canonical strings while Schema
        // v1 retains the PostgreSQL logical type.
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !args.args.is_empty() {
            return plan_err!("uuidv7 requires no arguments");
        }
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            self.slots.functions()?.call_uuid_v7().to_string(),
        ))))
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::single_text;

    #[tokio::test]
    async fn returns_uuid_text() {
        let value = single_text("SELECT uuidv7()")
            .await
            .expect("uuid should not be null");
        assert!(!value.is_empty());
    }
}
