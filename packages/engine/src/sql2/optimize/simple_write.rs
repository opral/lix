use crate::sql2::plan::LogicalWritePlan;
use crate::LixError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FastWritePlan {
    Insert(FastInsertPlan),
    Update(FastUpdatePlan),
    Delete(FastDeletePlan),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FastInsertPlan {
    pub(crate) write: LogicalWritePlan,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FastUpdatePlan {
    pub(crate) write: LogicalWritePlan,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FastDeletePlan {
    pub(crate) write: LogicalWritePlan,
}

pub(crate) fn try_make_fast_write_plan(
    _plan: &LogicalWritePlan,
) -> Result<Option<FastWritePlan>, LixError> {
    Ok(None)
}
