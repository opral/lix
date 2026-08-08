//! Dependency-free model for GC/current-generation ownership and progress.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decision {
    Publish,
    Noop,
    FailClosed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GcView {
    pub owner: u64,
    pub selector_epoch: u64,
    pub progress_epoch: u64,
    pub branch_root: u64,
    pub global_root: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GcPlan {
    pub decision: Decision,
    pub coherent_views: u8,
    pub plans: u8,
    pub prepared_writes: u8,
    pub commits: u8,
    pub owner_selector_cas: u8,
    pub progress_epoch_cas: u8,
    pub independent_commits: u8,
    pub legacy_reads: u8,
    pub legacy_writes: u8,
    pub corruption_fallbacks: u8,
}

impl GcPlan {
    pub const fn publish() -> Self {
        Self {
            decision: Decision::Publish,
            coherent_views: 1,
            plans: 1,
            prepared_writes: 1,
            commits: 1,
            owner_selector_cas: 1,
            progress_epoch_cas: 1,
            independent_commits: 0,
            legacy_reads: 0,
            legacy_writes: 0,
            corruption_fallbacks: 0,
        }
    }

    pub const fn zero(decision: Decision) -> Self {
        Self {
            decision,
            coherent_views: 0,
            plans: 0,
            prepared_writes: 0,
            commits: 0,
            owner_selector_cas: 0,
            progress_epoch_cas: 0,
            independent_commits: 0,
            legacy_reads: 0,
            legacy_writes: 0,
            corruption_fallbacks: 0,
        }
    }
}

pub fn validate_plan(plan: GcPlan) -> Result<(), &'static str> {
    if plan.independent_commits != 0 {
        return Err("independent GC commit");
    }
    if plan.legacy_reads != 0 || plan.legacy_writes != 0 {
        return Err("legacy current-generation access");
    }
    if plan.corruption_fallbacks != 0 {
        return Err("corruption fallback");
    }
    match plan.decision {
        Decision::Publish
            if plan.coherent_views == 1
                && plan.plans == 1
                && plan.prepared_writes == 1
                && plan.commits == 1
                && plan.owner_selector_cas == 1
                && plan.progress_epoch_cas == 1 => Ok(()),
        Decision::Noop | Decision::FailClosed
            if plan.coherent_views == 0
                && plan.plans == 0
                && plan.prepared_writes == 0
                && plan.commits == 0
                && plan.owner_selector_cas == 0
                && plan.progress_epoch_cas == 0 => Ok(()),
        _ => Err("GC cardinality or fence mismatch"),
    }
}

pub fn fence(view: GcView, owner: u64, selector_epoch: u64, progress_epoch: u64) -> Decision {
    if owner != view.owner
        || selector_epoch != view.selector_epoch
        || progress_epoch != view.progress_epoch
    {
        Decision::FailClosed
    } else {
        Decision::Publish
    }
}

pub fn drain_once(queue: &[u64], cursor: usize, limit: usize) -> (Vec<u64>, usize) {
    let end = (cursor + limit).min(queue.len());
    (queue[cursor..end].to_vec(), end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_has_one_view_plan_commit_and_two_fences() {
        assert!(validate_plan(GcPlan::publish()).is_ok());
    }

    #[test]
    fn no_op_has_no_durable_work() {
        assert!(validate_plan(GcPlan::zero(Decision::Noop)).is_ok());
    }

    #[test]
    fn corruption_has_no_durable_work() {
        assert!(validate_plan(GcPlan::zero(Decision::FailClosed)).is_ok());
    }

    #[test]
    fn publication_first_stales_old_view() {
        let view = GcView {
            owner: 1,
            selector_epoch: 4,
            progress_epoch: 8,
            branch_root: 10,
            global_root: 11,
        };
        assert_eq!(fence(view, 1, 5, 8), Decision::FailClosed);
    }

    #[test]
    fn unrelated_owner_is_rejected() {
        let view = GcView {
            owner: 1,
            selector_epoch: 4,
            progress_epoch: 8,
            branch_root: 10,
            global_root: 11,
        };
        assert_eq!(fence(view, 2, 4, 8), Decision::FailClosed);
    }

    #[test]
    fn matching_view_can_publish() {
        let view = GcView {
            owner: 1,
            selector_epoch: 4,
            progress_epoch: 8,
            branch_root: 10,
            global_root: 11,
        };
        assert_eq!(fence(view, 1, 4, 8), Decision::Publish);
    }

    #[test]
    fn sixty_five_entries_are_64_plus_suffix_without_skip() {
        let queue: Vec<u64> = (0..65).collect();
        let (first, cursor) = drain_once(&queue, 0, 64);
        let (suffix, end) = drain_once(&queue, cursor, 64);
        assert_eq!(first.len(), 64);
        assert_eq!(suffix, vec![64]);
        assert_eq!(end, 65);
        assert_eq!(first.into_iter().chain(suffix).collect::<Vec<_>>(), queue);
    }

    #[test]
    fn repeated_drain_is_monotonic() {
        let queue: Vec<u64> = (0..65).collect();
        let (_, cursor) = drain_once(&queue, 0, 64);
        let (_, end) = drain_once(&queue, cursor, 64);
        assert!(end > cursor);
    }

    #[test]
    fn independent_commit_is_rejected() {
        let mut plan = GcPlan::publish();
        plan.independent_commits = 1;
        assert_eq!(validate_plan(plan), Err("independent GC commit"));
    }

    #[test]
    fn legacy_access_is_rejected() {
        let mut plan = GcPlan::publish();
        plan.legacy_reads = 1;
        assert_eq!(validate_plan(plan), Err("legacy current-generation access"));
    }

    #[test]
    fn fallback_is_rejected() {
        let mut plan = GcPlan::zero(Decision::FailClosed);
        plan.corruption_fallbacks = 1;
        assert_eq!(validate_plan(plan), Err("corruption fallback"));
    }
}
