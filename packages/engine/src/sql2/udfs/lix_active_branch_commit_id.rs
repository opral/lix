use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{plan_err, Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) struct LixActiveBranchCommitId {
    commit_id: Option<String>,
}

impl LixActiveBranchCommitId {
    pub(super) fn new(commit_id: Option<String>) -> Self {
        Self { commit_id }
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

    fn name(&self) -> &str {
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
            self.commit_id.clone(),
        )))
    }
}
