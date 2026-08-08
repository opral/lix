//! Stateful selector/root model for the TrackedHead hard cut.
//!
//! This remains a pure model: object bytes are symbolic and the digest is a
//! deterministic test checksum, not the production BLAKE3 codec. Unlike the
//! predecessor, corruption is represented by mutating an authenticated
//! selector/root fixture before one retained read validates it.

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
enum Domain {
    GlobalSelector,
    BranchSelector,
    StateRoot,
    CatalogRoot,
    CheckpointRoot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AuthenticatedRef {
    domain: Domain,
    id: u64,
    bytes: Vec<u8>,
    present: bool,
}

impl AuthenticatedRef {
    fn new(domain: Domain, nonce: u64) -> Self {
        let bytes = format!("{domain:?}:v1:{nonce}").into_bytes();
        let id = checksum(&bytes);
        Self {
            domain,
            id,
            bytes,
            present: true,
        }
    }

    fn validate(&self, expected_domain: Domain, expected_id: u64) -> Result<(), Corruption> {
        if !self.present {
            return Err(Corruption::Missing);
        }
        if self.domain != expected_domain {
            return Err(Corruption::WrongKind);
        }
        if self.bytes.is_empty()
            || !self
                .bytes
                .starts_with(format!("{expected_domain:?}").as_bytes())
        {
            return Err(Corruption::Malformed);
        }
        if checksum(&self.bytes) != self.id || self.id != expected_id {
            return Err(Corruption::IdentitySubstitution);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Corruption {
    Malformed,
    Missing,
    WrongKind,
    IdentitySubstitution,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct View {
    global_selector_id: u64,
    branch_selector_id: u64,
    state_root_id: u64,
    catalog_root_id: u64,
    checkpoint_root_id: u64,
    epoch: u64,
    view_id: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Counters {
    retained_reads: u8,
    retained_views: u8,
    plans: u8,
    prepared_writes: u8,
    commits: u8,
    selector_rotations: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Store {
    global_selector: AuthenticatedRef,
    branch_selector: AuthenticatedRef,
    state_root: AuthenticatedRef,
    catalog_root: AuthenticatedRef,
    checkpoint_root: AuthenticatedRef,
    expected_global_selector_id: u64,
    expected_branch_selector_id: u64,
    expected_state_root_id: u64,
    expected_catalog_root_id: u64,
    expected_checkpoint_root_id: u64,
    epoch: u64,
    counters: Counters,
}

impl Store {
    fn new() -> Self {
        let global_selector = AuthenticatedRef::new(Domain::GlobalSelector, 1);
        let branch_selector = AuthenticatedRef::new(Domain::BranchSelector, 2);
        let state_root = AuthenticatedRef::new(Domain::StateRoot, 3);
        let catalog_root = AuthenticatedRef::new(Domain::CatalogRoot, 4);
        let checkpoint_root = AuthenticatedRef::new(Domain::CheckpointRoot, 5);
        Self {
            expected_global_selector_id: global_selector.id,
            expected_branch_selector_id: branch_selector.id,
            expected_state_root_id: state_root.id,
            expected_catalog_root_id: catalog_root.id,
            expected_checkpoint_root_id: checkpoint_root.id,
            global_selector,
            branch_selector,
            state_root,
            catalog_root,
            checkpoint_root,
            epoch: 0,
            counters: Counters::default(),
        }
    }

    fn open_coherent_view(&mut self) -> Result<View, Corruption> {
        self.counters.retained_reads += 1;
        self.counters.retained_views += 1;
        self.global_selector
            .validate(Domain::GlobalSelector, self.expected_global_selector_id)?;
        self.branch_selector
            .validate(Domain::BranchSelector, self.expected_branch_selector_id)?;
        self.state_root
            .validate(Domain::StateRoot, self.expected_state_root_id)?;
        self.catalog_root
            .validate(Domain::CatalogRoot, self.expected_catalog_root_id)?;
        self.checkpoint_root
            .validate(Domain::CheckpointRoot, self.expected_checkpoint_root_id)?;
        Ok(View {
            global_selector_id: self.global_selector.id,
            branch_selector_id: self.branch_selector.id,
            state_root_id: self.state_root.id,
            catalog_root_id: self.catalog_root.id,
            checkpoint_root_id: self.checkpoint_root.id,
            epoch: self.epoch,
            view_id: self.view_id(),
        })
    }

    fn publish(&mut self, view: &View, cohort: Cohort) -> Result<Plan, &'static str> {
        if view.view_id != self.view_id() {
            return Err("stale coherent view");
        }
        self.counters.plans += 1;
        self.counters.prepared_writes += 1;
        self.counters.commits += 1;
        self.counters.selector_rotations += 1;
        Ok(Plan {
            cohort,
            decision: Decision::Publish,
            retained_reads: self.counters.retained_reads,
            retained_views: self.counters.retained_views,
            plans: self.counters.plans,
            prepared_writes: self.counters.prepared_writes,
            commits: self.counters.commits,
            selector_rotations: self.counters.selector_rotations,
            independent_commits: 0,
            legacy_authorities: 0,
            corruption_fallbacks: 0,
        })
    }

    fn view_id(&self) -> u64 {
        let mut value = self.epoch;
        for reference in [
            &self.global_selector,
            &self.branch_selector,
            &self.state_root,
            &self.catalog_root,
            &self.checkpoint_root,
        ] {
            value = value.rotate_left(7) ^ reference.id;
        }
        value
    }

    fn corrupt(&mut self, kind: Corruption) {
        match kind {
            Corruption::Malformed => self.state_root.bytes = b"not-canonical".to_vec(),
            Corruption::Missing => self.state_root.present = false,
            Corruption::WrongKind => {
                self.state_root = AuthenticatedRef::new(Domain::CatalogRoot, 33)
            }
            Corruption::IdentitySubstitution => self.state_root.id ^= 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Plan {
    cohort: Cohort,
    decision: Decision,
    retained_reads: u8,
    retained_views: u8,
    plans: u8,
    prepared_writes: u8,
    commits: u8,
    selector_rotations: u8,
    independent_commits: u8,
    legacy_authorities: u8,
    corruption_fallbacks: u8,
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
            if plan.retained_reads == 1
                && plan.retained_views == 1
                && plan.plans == 1
                && plan.prepared_writes == 1
                && plan.commits == 1
                && plan.selector_rotations == 1 =>
        {
            Ok(())
        }
        Decision::Noop | Decision::Unsupported | Decision::FailClosed
            if plan.retained_reads == 1
                && plan.retained_views == 1
                && plan.plans == 0
                && plan.prepared_writes == 0
                && plan.commits == 0
                && plan.selector_rotations == 0 =>
        {
            Ok(())
        }
        _ => Err("publication cardinality, view, or selector mismatch"),
    }
}

fn checksum(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |value, byte| {
        value
            .wrapping_mul(0x1000_0000_01b3)
            .wrapping_add(u64::from(*byte))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_has_one_retained_view_one_plan_one_commit() {
        let mut store = Store::new();
        let view = store.open_coherent_view().unwrap();
        let plan = store.publish(&view, Cohort::WorkingDiff).unwrap();
        assert_eq!(plan.cohort, Cohort::WorkingDiff);
        assert!(validate(plan).is_ok());
    }

    #[test]
    fn malformed_missing_wrong_kind_and_identity_substitution_fail_closed() {
        for kind in [
            Corruption::Malformed,
            Corruption::Missing,
            Corruption::WrongKind,
            Corruption::IdentitySubstitution,
        ] {
            let mut store = Store::new();
            store.corrupt(kind);
            let before = store.counters;
            assert_eq!(store.open_coherent_view(), Err(kind));
            assert_eq!(store.counters.retained_reads, before.retained_reads + 1);
            assert_eq!(store.counters.retained_views, before.retained_views + 1);
            assert_eq!(store.counters.plans, before.plans);
            assert_eq!(store.counters.prepared_writes, before.prepared_writes);
            assert_eq!(store.counters.commits, before.commits);
            assert_eq!(store.counters.selector_rotations, before.selector_rotations);
        }
    }

    #[test]
    fn no_op_has_one_validation_view_and_zero_durable_work() {
        let mut store = Store::new();
        let view = store.open_coherent_view().unwrap();
        let plan = Plan {
            cohort: Cohort::Init,
            decision: Decision::Noop,
            retained_reads: 1,
            retained_views: 1,
            plans: 0,
            prepared_writes: 0,
            commits: 0,
            selector_rotations: 0,
            independent_commits: 0,
            legacy_authorities: 0,
            corruption_fallbacks: 0,
        };
        assert_eq!(view.epoch, store.epoch);
        assert!(validate(plan).is_ok());
    }

    #[test]
    fn unsupported_has_one_validation_view_and_zero_durable_work() {
        let mut store = Store::new();
        let view = store.open_coherent_view().unwrap();
        assert_eq!(view.view_id, store.view_id());
        let plan = Plan {
            cohort: Cohort::Gc,
            decision: Decision::Unsupported,
            retained_reads: 1,
            retained_views: 1,
            plans: 0,
            prepared_writes: 0,
            commits: 0,
            selector_rotations: 0,
            independent_commits: 0,
            legacy_authorities: 0,
            corruption_fallbacks: 0,
        };
        assert!(validate(plan).is_ok());
    }

    #[test]
    fn legacy_authority_is_rejected() {
        let mut store = Store::new();
        let view = store.open_coherent_view().unwrap();
        let mut plan = store.publish(&view, Cohort::Generation).unwrap();
        plan.legacy_authorities = 1;
        assert_eq!(validate(plan), Err("legacy authority"));
    }

    #[test]
    fn independent_commit_is_rejected() {
        let mut store = Store::new();
        let view = store.open_coherent_view().unwrap();
        let mut plan = store.publish(&view, Cohort::Gc).unwrap();
        plan.independent_commits = 1;
        assert_eq!(validate(plan), Err("independent commit"));
    }

    #[test]
    fn corruption_fallback_is_rejected() {
        let mut store = Store::new();
        let view = store.open_coherent_view().unwrap();
        let mut plan = Plan {
            cohort: Cohort::SqlWorkingDiff,
            decision: Decision::FailClosed,
            retained_reads: 1,
            retained_views: 1,
            plans: 0,
            prepared_writes: 0,
            commits: 0,
            selector_rotations: 0,
            independent_commits: 0,
            legacy_authorities: 0,
            corruption_fallbacks: 1,
        };
        assert_eq!(view.state_root_id, store.state_root.id);
        assert_eq!(validate(plan), Err("corruption fallback"));
        plan.corruption_fallbacks = 0;
        assert!(validate(plan).is_ok());
    }
}
