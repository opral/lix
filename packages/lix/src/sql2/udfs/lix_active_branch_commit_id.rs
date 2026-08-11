use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use super::execution_slots::ExecutionSlots;

#[derive(Clone)]
pub(super) struct LixActiveBranchCommitId {
    slots: Arc<ExecutionSlots>,
}

impl LixActiveBranchCommitId {
    pub(super) fn new(slots: Arc<ExecutionSlots>) -> Self {
        Self { slots }
    }
}

impl PartialEq for LixActiveBranchCommitId {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for LixActiveBranchCommitId {}

impl std::hash::Hash for LixActiveBranchCommitId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl std::fmt::Debug for LixActiveBranchCommitId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixActiveBranchCommitId").finish()
    }
}

impl ScalarUDFImpl for LixActiveBranchCommitId {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "lix_active_branch_commit_id"
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
            return plan_err!("lix_active_branch_commit_id requires no arguments");
        }
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
            self.slots.active_branch_commit_id(),
        )))
    }
}
