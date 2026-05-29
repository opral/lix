use crate::sql2::bind::expr::BoundExpr;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FilterSet {
    All,
    None,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BoundPredicate {
    True,
    False,
    And(Vec<Self>),
    Or(Vec<Self>),
    Eq(BoundExpr, BoundExpr),
    IsNull(BoundExpr),
    IsNotNull(BoundExpr),
    In {
        expr: BoundExpr,
        values: Vec<BoundExpr>,
    },
}
