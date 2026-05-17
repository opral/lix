use std::collections::BTreeSet;

use crate::sql2::bind::expr::BoundExpr;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FilterSet<T> {
    All,
    Some(BTreeSet<T>),
    None,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BoundPredicate {
    True,
    False,
    And(Vec<BoundPredicate>),
    Or(Vec<BoundPredicate>),
    Eq(BoundExpr, BoundExpr),
    In {
        expr: BoundExpr,
        values: Vec<BoundExpr>,
    },
}
