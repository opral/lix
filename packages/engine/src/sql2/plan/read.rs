use crate::sql2::bind::read::BoundRead;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LogicalReadPlan {
    pub(crate) bound: BoundRead,
}
