//! Dependency-free acceptance model for the TrackedHeadContext hard cut.
//!
//! This file is intentionally not wired into Cargo or production. It can be
//! compiled as a standalone test after a candidate passes the source gate.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cohort {
    WorkingDiff,
    CollectionGeneration,
    GcReachability,
    InitPublication,
    DeterministicSequence,
    SchemaResolver,
    SqlWorkingDiff,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Authority {
    Selector,
    CommitCatalog,
    ChangeCatalog,
    StateRoot,
    DerivedProjection,
    TransactionOverlay,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Verdict {
    Noop,
    Publish,
    Unsupported,
    FailClosed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublicationPlan {
    pub cohort: Cohort,
    pub verdict: Verdict,
    pub coherent_views: u8,
    pub prepared_publications: u8,
    pub storage_plans: u8,
    pub prepare_write_sets: u8,
    pub backend_commits: u8,
    pub independent_commits: u8,
    pub selector_rotation: bool,
    pub durable_derived_authority: bool,
    pub fallback_on_corruption: bool,
}

impl PublicationPlan {
    pub const fn noop(cohort: Cohort) -> Self {
        Self {
            cohort,
            verdict: Verdict::Noop,
            coherent_views: 0,
            prepared_publications: 0,
            storage_plans: 0,
            prepare_write_sets: 0,
            backend_commits: 0,
            independent_commits: 0,
            selector_rotation: false,
            durable_derived_authority: false,
            fallback_on_corruption: false,
        }
    }

    pub const fn unsupported(cohort: Cohort) -> Self {
        Self {
            cohort,
            verdict: Verdict::Unsupported,
            ..Self::noop(cohort)
        }
    }

    pub const fn publish(cohort: Cohort) -> Self {
        Self {
            cohort,
            verdict: Verdict::Publish,
            coherent_views: 1,
            prepared_publications: 1,
            storage_plans: 1,
            prepare_write_sets: 1,
            backend_commits: 1,
            independent_commits: 0,
            selector_rotation: true,
            durable_derived_authority: false,
            fallback_on_corruption: false,
        }
    }
}

pub fn validate(plan: PublicationPlan) -> Result<(), &'static str> {
    if plan.independent_commits != 0 {
        return Err("independent publication commit");
    }
    if plan.durable_derived_authority {
        return Err("derived projection became durable authority");
    }
    if plan.fallback_on_corruption {
        return Err("corruption was masked by fallback");
    }
    match plan.verdict {
        Verdict::Publish => {
            if plan.coherent_views != 1
                || plan.prepared_publications != 1
                || plan.storage_plans != 1
                || plan.prepare_write_sets != 1
                || plan.backend_commits != 1
                || !plan.selector_rotation
            {
                return Err("publication is not one-view/one-plan/one-commit");
            }
        }
        Verdict::Noop | Verdict::Unsupported | Verdict::FailClosed => {
            if plan.coherent_views != 0
                || plan.prepared_publications != 0
                || plan.storage_plans != 0
                || plan.prepare_write_sets != 0
                || plan.backend_commits != 0
                || plan.selector_rotation
            {
                return Err("non-publication path created durable work");
            }
        }
    }
    Ok(())
}

pub const REQUIRED_AUTHORITIES: &[(Cohort, Authority)] = &[
    (Cohort::WorkingDiff, Authority::StateRoot),
    (Cohort::WorkingDiff, Authority::CommitCatalog),
    (Cohort::CollectionGeneration, Authority::StateRoot),
    (Cohort::GcReachability, Authority::Selector),
    (Cohort::GcReachability, Authority::CommitCatalog),
    (Cohort::InitPublication, Authority::Selector),
    (Cohort::InitPublication, Authority::StateRoot),
    (Cohort::DeterministicSequence, Authority::Selector),
    (Cohort::DeterministicSequence, Authority::StateRoot),
    (Cohort::SchemaResolver, Authority::StateRoot),
    (Cohort::SchemaResolver, Authority::TransactionOverlay),
    (Cohort::SqlWorkingDiff, Authority::DerivedProjection),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_is_exactly_one_atomic_plan() {
        validate(PublicationPlan::publish(Cohort::WorkingDiff)).unwrap();
    }

    #[test]
    fn noop_has_no_selector_or_write() {
        validate(PublicationPlan::noop(Cohort::DeterministicSequence)).unwrap();
    }

    #[test]
    fn unsupported_has_no_plan() {
        validate(PublicationPlan::unsupported(Cohort::GcReachability)).unwrap();
    }

    #[test]
    fn independent_commit_is_rejected() {
        let mut plan = PublicationPlan::publish(Cohort::InitPublication);
        plan.independent_commits = 1;
        assert_eq!(validate(plan), Err("independent publication commit"));
    }

    #[test]
    fn durable_derived_authority_is_rejected() {
        let mut plan = PublicationPlan::publish(Cohort::CollectionGeneration);
        plan.durable_derived_authority = true;
        assert_eq!(
            validate(plan),
            Err("derived projection became durable authority")
        );
    }

    #[test]
    fn corruption_fallback_is_rejected() {
        let mut plan = PublicationPlan::noop(Cohort::SqlWorkingDiff);
        plan.fallback_on_corruption = true;
        assert_eq!(validate(plan), Err("corruption was masked by fallback"));
    }
}
