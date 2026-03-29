use sqlparser::ast::Expr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PushdownSupport {
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RejectedPredicate {
    pub(crate) predicate: Expr,
    pub(crate) reason: String,
    pub(crate) support: PushdownSupport,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct PushdownDecision {
    pub(crate) accepted_predicates: Vec<Expr>,
    pub(crate) rejected_predicates: Vec<RejectedPredicate>,
    pub(crate) residual_predicates: Vec<Expr>,
}

impl PushdownDecision {
    #[cfg(test)]
    pub(crate) fn accepted_predicate_sql(&self) -> Vec<String> {
        self.accepted_predicates
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn residual_predicate_sql(&self) -> Vec<String> {
        self.residual_predicates
            .iter()
            .map(ToString::to_string)
            .collect()
    }
}
