use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use super::execution_slots::ExecutionSlots;

#[derive(Clone)]
pub(super) struct LixRootCommitId {
    slots: Arc<ExecutionSlots>,
}

impl LixRootCommitId {
    pub(super) fn new(slots: Arc<ExecutionSlots>) -> Self {
        Self { slots }
    }
}

impl PartialEq for LixRootCommitId {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for LixRootCommitId {}

impl std::hash::Hash for LixRootCommitId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl std::fmt::Debug for LixRootCommitId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("LixRootCommitId").finish()
    }
}

impl ScalarUDFImpl for LixRootCommitId {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "lix_root_commit_id"
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
            return plan_err!("lix_root_commit_id requires no arguments");
        }
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
            self.slots.root_commit_id(),
        )))
    }
}
