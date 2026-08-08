//! Standalone, dependency-free model for the whole-module deletion contract.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cohort {
    WorkingDiff,
    Generation,
    Gc,
    Init,
    Deterministic,
    Schema,
    SqlWorkingDiff,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decision {
    Publish,
    Noop,
    Unsupported,
    FailClosed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Plan {
    pub cohort: Cohort,
    pub decision: Decision,
    pub coherent_views: u8,
    pub prepared_publications: u8,
    pub storage_plans: u8,
    pub prepared_writes: u8,
    pub commits: u8,
    pub independent_commits: u8,
    pub legacy_authorities: u8,
    pub corruption_fallbacks: u8,
    pub selector_rotations: u8,
}

impl Plan {
    pub const fn no_durable_work(cohort: Cohort, decision: Decision) -> Self {
        Self {
            cohort,
            decision,
            coherent_views: 0,
            prepared_publications: 0,
            storage_plans: 0,
            prepared_writes: 0,
            commits: 0,
            independent_commits: 0,
            legacy_authorities: 0,
            corruption_fallbacks: 0,
            selector_rotations: 0,
        }
    }

    pub const fn publish(cohort: Cohort) -> Self {
        Self {
            cohort,
            decision: Decision::Publish,
            coherent_views: 1,
            prepared_publications: 1,
            storage_plans: 1,
            prepared_writes: 1,
            commits: 1,
            independent_commits: 0,
            legacy_authorities: 0,
            corruption_fallbacks: 0,
            selector_rotations: 1,
        }
    }
}

pub fn validate(plan: Plan) -> Result<(), &'static str> {
    if plan.independent_commits != 0 {
        return Err("independent commit");
    }
    if plan.legacy_authorities != 0 {
        return Err("legacy authority");
    }
    if plan.corruption_fallbacks != 0 {
        return Err("corruption fallback");
    }
    match plan.decision {
        Decision::Publish
            if plan.coherent_views == 1
                && plan.prepared_publications == 1
                && plan.storage_plans == 1
                && plan.prepared_writes == 1
                && plan.commits == 1
                && plan.selector_rotations == 1 => Ok(()),
        Decision::Noop | Decision::Unsupported | Decision::FailClosed
            if plan.coherent_views == 0
                && plan.prepared_publications == 0
                && plan.storage_plans == 0
                && plan.prepared_writes == 0
                && plan.commits == 0
                && plan.selector_rotations == 0 => Ok(()),
        _ => Err("publication cardinality or selector rotation mismatch"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_has_one_view_one_plan_one_commit() {
        assert!(validate(Plan::publish(Cohort::WorkingDiff)).is_ok());
    }

    #[test]
    fn no_op_is_zero_write() {
        assert!(validate(Plan::no_durable_work(Cohort::Init, Decision::Noop)).is_ok());
    }

    #[test]
    fn unsupported_is_zero_write() {
        assert!(validate(Plan::no_durable_work(Cohort::Gc, Decision::Unsupported)).is_ok());
    }

    #[test]
    fn corruption_is_zero_write() {
        assert!(validate(Plan::no_durable_work(Cohort::Schema, Decision::FailClosed)).is_ok());
    }

    #[test]
    fn legacy_authority_is_rejected() {
        let mut plan = Plan::publish(Cohort::Generation);
        plan.legacy_authorities = 1;
        assert_eq!(validate(plan), Err("legacy authority"));
    }

    #[test]
    fn independent_commit_is_rejected() {
        let mut plan = Plan::publish(Cohort::Gc);
        plan.independent_commits = 1;
        assert_eq!(validate(plan), Err("independent commit"));
    }

    #[test]
    fn corruption_fallback_is_rejected() {
        let mut plan = Plan::no_durable_work(Cohort::SqlWorkingDiff, Decision::FailClosed);
        plan.corruption_fallbacks = 1;
        assert_eq!(validate(plan), Err("corruption fallback"));
    }
}
