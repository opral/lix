use super::expr::{BoundColumnRef, BoundExpr, BoundParamRef};
use super::read::BoundRead;
use crate::sql2::plan::predicate::BoundPredicate;
use crate::sql2::plan::version_scope::VersionScope;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundWrite {
    pub(crate) target: BoundWriteTarget,
    pub(crate) op: BoundWriteOp,
    pub(crate) input: BoundWriteInput,
    pub(crate) predicate: BoundPredicate,
    pub(crate) assignments: Vec<BoundAssignment>,
    pub(crate) params: BoundParamMap,
    pub(crate) version_scope: VersionScope,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BoundWriteTarget {
    LixState,
    LixStateByVersion,
    Entity(EntityWriteSurface),
    File(FileWriteSurface),
    Directory(DirectoryWriteSurface),
    Version,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum EntityWriteSurface {
    Base { schema_key: String },
    ByVersion { schema_key: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FileWriteSurface {
    Base,
    ByVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DirectoryWriteSurface {
    Base,
    ByVersion,
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

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct BoundParamMap {
    pub(crate) params: BTreeMap<usize, BoundParamRef>,
}
