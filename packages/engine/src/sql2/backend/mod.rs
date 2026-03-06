use crate::sql2::planner::ir::{PlannedWrite, ReadCommand};
use crate::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PushdownSupport {
    Exact,
    Inexact,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RejectedPredicate {
    pub(crate) predicate: String,
    pub(crate) reason: String,
    pub(crate) support: PushdownSupport,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct PushdownDecision {
    pub(crate) accepted_predicates: Vec<String>,
    pub(crate) rejected_predicates: Vec<RejectedPredicate>,
    pub(crate) residual_predicates: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LoweredSqlProgram {
    pub(crate) sql: String,
    pub(crate) params: Vec<Value>,
    pub(crate) pushdown_decision: PushdownDecision,
}

pub(crate) trait BackendLowerer {
    fn lower_read(&self, command: &ReadCommand) -> LoweredSqlProgram;
    fn lower_write(&self, command: &PlannedWrite) -> LoweredSqlProgram;
}
