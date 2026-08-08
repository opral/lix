//! Dependency-free model for the first transaction migration oracle.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decision {
    Publish,
    Noop,
    Unsupported,
    FailClosed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Failure {
    Missing,
    Malformed,
    WrongKind,
    WrongOwner,
    StaleSameOwner,
    StaleUnrelatedOwner,
    Cyclic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OpeningView {
    pub owner: u64,
    pub selector_epoch: u64,
    pub state_root: u64,
    pub checkpoint_root: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Publication {
    pub decision: Decision,
    pub view_count: u8,
    pub prepared_publications: u8,
    pub storage_plans: u8,
    pub prepared_writes: u8,
    pub commits: u8,
    pub independent_commits: u8,
    pub selector_cas: u8,
    pub epoch_cas: u8,
    pub legacy_reads: u8,
    pub legacy_writes: u8,
    pub fallback_on_corruption: u8,
}

impl Publication {
    pub const fn publish() -> Self {
        Self {
            decision: Decision::Publish,
            view_count: 1,
            prepared_publications: 1,
            storage_plans: 1,
            prepared_writes: 1,
            commits: 1,
            independent_commits: 0,
            selector_cas: 1,
            epoch_cas: 1,
            legacy_reads: 0,
            legacy_writes: 0,
            fallback_on_corruption: 0,
        }
    }

    pub const fn zero(decision: Decision) -> Self {
        Self {
            decision,
            view_count: 0,
            prepared_publications: 0,
            storage_plans: 0,
            prepared_writes: 0,
            commits: 0,
            independent_commits: 0,
            selector_cas: 0,
            epoch_cas: 0,
            legacy_reads: 0,
            legacy_writes: 0,
            fallback_on_corruption: 0,
        }
    }
}

pub fn validate_publication(publication: Publication) -> Result<(), &'static str> {
    if publication.legacy_reads != 0 || publication.legacy_writes != 0 {
        return Err("legacy tracked-head access");
    }
    if publication.independent_commits != 0 {
        return Err("independent commit");
    }
    if publication.fallback_on_corruption != 0 {
        return Err("corruption fallback");
    }
    match publication.decision {
        Decision::Publish
            if publication.view_count == 1
                && publication.prepared_publications == 1
                && publication.storage_plans == 1
                && publication.prepared_writes == 1
                && publication.commits == 1
                && publication.selector_cas == 1
                && publication.epoch_cas == 1 => Ok(()),
        Decision::Noop | Decision::Unsupported | Decision::FailClosed
            if publication.view_count == 0
                && publication.prepared_publications == 0
                && publication.storage_plans == 0
                && publication.prepared_writes == 0
                && publication.commits == 0
                && publication.selector_cas == 0
                && publication.epoch_cas == 0 => Ok(()),
        _ => Err("publication cardinality or CAS mismatch"),
    }
}

pub fn classify_race(view: OpeningView, owner: u64, epoch: u64) -> Decision {
    if owner != view.owner {
        Decision::FailClosed
    } else if epoch != view.selector_epoch {
        Decision::FailClosed
    } else {
        Decision::Publish
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advanced_publication_has_one_view_plan_commit_and_two_cas_guards() {
        assert!(validate_publication(Publication::publish()).is_ok());
    }

    #[test]
    fn no_op_has_zero_durable_work() {
        assert!(validate_publication(Publication::zero(Decision::Noop)).is_ok());
    }

    #[test]
    fn unsupported_has_zero_durable_work() {
        assert!(validate_publication(Publication::zero(Decision::Unsupported)).is_ok());
    }

    #[test]
    fn corruption_has_zero_durable_work() {
        assert!(validate_publication(Publication::zero(Decision::FailClosed)).is_ok());
    }

    #[test]
    fn same_owner_epoch_change_is_stale() {
        let view = OpeningView {
            owner: 7,
            selector_epoch: 3,
            state_root: 11,
            checkpoint_root: 13,
        };
        assert_eq!(classify_race(view, 7, 4), Decision::FailClosed);
    }

    #[test]
    fn unrelated_owner_is_stale() {
        let view = OpeningView {
            owner: 7,
            selector_epoch: 3,
            state_root: 11,
            checkpoint_root: 13,
        };
        assert_eq!(classify_race(view, 8, 3), Decision::FailClosed);
    }

    #[test]
    fn valid_owner_and_epoch_can_publish() {
        let view = OpeningView {
            owner: 7,
            selector_epoch: 3,
            state_root: 11,
            checkpoint_root: 13,
        };
        assert_eq!(classify_race(view, 7, 3), Decision::Publish);
    }

    #[test]
    fn legacy_read_is_rejected() {
        let mut publication = Publication::publish();
        publication.legacy_reads = 1;
        assert_eq!(
            validate_publication(publication),
            Err("legacy tracked-head access")
        );
    }

    #[test]
    fn corruption_fallback_is_rejected() {
        let mut publication = Publication::zero(Decision::FailClosed);
        publication.fallback_on_corruption = 1;
        assert_eq!(
            validate_publication(publication),
            Err("corruption fallback")
        );
    }
}
