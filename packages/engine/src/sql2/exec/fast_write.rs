use crate::sql2::optimize::simple_write::FastWritePlan;
use crate::sql2::SqlWriteExecutionContext;
use crate::{LixError, Value};

pub(crate) async fn try_execute_simple_write(
    _ctx: &mut dyn SqlWriteExecutionContext,
    _plan: FastWritePlan,
    _params: &[Value],
) -> Result<Option<u64>, LixError> {
    Ok(None)
}
