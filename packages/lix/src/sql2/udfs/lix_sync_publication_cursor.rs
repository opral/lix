use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use super::execution_slots::ExecutionSlots;

#[derive(Clone)]
pub(super) struct LixSyncPublicationCursor {
    slots: Arc<ExecutionSlots>,
}

impl LixSyncPublicationCursor {
    pub(super) fn new(slots: Arc<ExecutionSlots>) -> Self {
        Self { slots }
    }
}

impl PartialEq for LixSyncPublicationCursor {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for LixSyncPublicationCursor {}

impl std::hash::Hash for LixSyncPublicationCursor {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl std::fmt::Debug for LixSyncPublicationCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LixSyncPublicationCursor").finish()
    }
}

impl ScalarUDFImpl for LixSyncPublicationCursor {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &'static str {
        "lix_sync_publication_cursor"
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
            return plan_err!("lix_sync_publication_cursor requires no arguments");
        }
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
            self.slots.sync_publication_cursor(),
        )))
    }
}
