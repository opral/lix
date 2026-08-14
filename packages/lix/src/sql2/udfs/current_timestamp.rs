use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use super::execution_slots::ExecutionSlots;

#[derive(Clone)]
pub(super) struct CurrentTimestamp {
    pub(super) slots: Arc<ExecutionSlots>,
}

impl PartialEq for CurrentTimestamp {
    fn eq(&self, _other: &Self) -> bool { true }
}
impl Eq for CurrentTimestamp {}
impl std::hash::Hash for CurrentTimestamp {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) { self.name().hash(state); }
}
impl std::fmt::Debug for CurrentTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CurrentTimestamp").finish()
    }
}

impl ScalarUDFImpl for CurrentTimestamp {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &'static str { "__lix_current_timestamp" }
    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::nullary(Volatility::Stable));
        &SIGNATURE
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !args.args.is_empty() {
            return plan_err!("CURRENT_TIMESTAMP requires no arguments");
        }
        let timestamp = self.slots.current_timestamp()?;
        let micros = i64::try_from(timestamp.milliseconds_since_unix_epoch())
            .map_err(|_| datafusion::common::DataFusionError::Execution(
                "CURRENT_TIMESTAMP is outside the signed timestamp range".to_owned()
            ))?
            .checked_mul(1_000)
            .ok_or_else(|| datafusion::common::DataFusionError::Execution(
                "CURRENT_TIMESTAMP is outside the microsecond timestamp range".to_owned()
            ))?;
        Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
            Some(micros), Some("UTC".into()),
        )))
    }
}
