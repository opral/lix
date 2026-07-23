use super::expr::{BoundColumnRef, BoundExpr, BoundParamRef};
use super::read::BoundRead;
use crate::sql2::plan::branch_scope::BranchScope;
use crate::sql2::plan::predicate::BoundPredicate;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundWrite {
    pub(crate) target: BoundWriteTarget,
    pub(crate) op: BoundWriteOp,
    pub(crate) input: BoundWriteInput,
    pub(crate) predicate: BoundPredicate,
    pub(crate) assignments: Vec<BoundAssignment>,
    pub(crate) conflict: Option<BoundInsertConflict>,
    /// The pre-change projection requested by `DELETE … RETURNING`.
    ///
    /// This remains part of the bound write rather than a post-hoc read so
    /// execution can project directly from the exact batch being deleted.
    pub(crate) returning: Option<BoundReturning>,
    pub(crate) params: BoundParamMap,
    pub(crate) branch_scope: BranchScope,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundReturning {
    pub(crate) items: Vec<BoundReturningItem>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundReturningItem {
    pub(crate) expr: BoundExpr,
    pub(crate) output_name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BoundWriteTarget {
    SchemaDefinition,
    LixState,
    LixStateByBranch,
    Entity(EntityWriteSurface),
    File(FileWriteSurface),
    Directory(DirectoryWriteSurface),
    Branch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum EntityWriteSurface {
    Base { schema_key: String },
    ByBranch { schema_key: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FileWriteSurface {
    Base,
    ByBranch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DirectoryWriteSurface {
    Base,
    ByBranch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BoundWriteOp {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BoundWriteInput {
    Values(BoundInsertValues),
    Query {
        query: Box<BoundRead>,
        columns: Vec<BoundColumnRef>,
    },
    None,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundInsertValues {
    pub(crate) columns: Vec<BoundColumnRef>,
    pub(crate) rows: Vec<Vec<BoundExpr>>,
}

impl BoundInsertValues {
    pub(crate) fn column_index(&self, column_name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|column| column.name == column_name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundAssignment {
    pub(crate) column: BoundColumnRef,
    pub(crate) value: BoundExpr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundInsertConflict {
    pub(crate) target_columns: Vec<BoundColumnRef>,
    pub(crate) action: BoundConflictAction,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BoundConflictAction {
    /// `DO UPDATE SET ...` — apply the assignments to the conflicting row.
    DoUpdate { assignments: Vec<BoundAssignment> },
    /// `DO NOTHING` — keep the existing conflicting row unchanged.
    DoNothing,
}

impl BoundConflictAction {
    /// The DO UPDATE assignments, or an empty slice for DO NOTHING.
    pub(crate) fn assignments(&self) -> &[BoundAssignment] {
        match self {
            Self::DoUpdate { assignments } => assignments,
            Self::DoNothing => &[],
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct BoundParamMap {
    pub(crate) params: BTreeMap<usize, BoundParamRef>,
}
