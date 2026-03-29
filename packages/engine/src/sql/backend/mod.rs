#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PushdownSupport {
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
