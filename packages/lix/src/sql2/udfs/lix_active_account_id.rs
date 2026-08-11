use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use super::execution_slots::ExecutionSlots;

#[derive(Clone)]
pub(super) struct LixActiveAccountId {
    slots: Arc<ExecutionSlots>,
}

impl LixActiveAccountId {
    pub(super) fn new(slots: Arc<ExecutionSlots>) -> Self {
        Self { slots }
    }
}

// Every session owns exactly one instance of this function, and plans that call
// it are never shared between sessions: `logical_plan_has_scalar_function`
// excludes any plan containing a scalar function from the read-plan and
// physical-plan caches. Identity therefore carries no information, exactly as
// for the volatile execution functions.
impl PartialEq for LixActiveAccountId {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for LixActiveAccountId {}

impl std::hash::Hash for LixActiveAccountId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl std::fmt::Debug for LixActiveAccountId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixActiveAccountId").finish()
    }
}

impl ScalarUDFImpl for LixActiveAccountId {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "lix_active_account_id"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::nullary(Volatility::Stable));
        &SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !args.args.is_empty() {
            return plan_err!("lix_active_account_id requires no arguments");
        }
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
            self.slots.active_account_id(),
        )))
    }
}
