pub(crate) enum StaleDecision {
    Direct,
    Reconciled,
    UnrelatedOwner,
}

pub(crate) fn classify_stale_commit(changed: bool, same_owner: bool) -> StaleDecision {
    if !changed {
        StaleDecision::Direct
    } else if same_owner {
        StaleDecision::Reconciled
    } else {
        StaleDecision::UnrelatedOwner
    }
}
