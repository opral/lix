#![forbid(unsafe_code)]

use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StorageRead {
    id: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CoherentView {
    read_id: u64,
    view_id: u64,
}

impl CoherentView {
    fn from_read(read: StorageRead, view_id: u64) -> Self {
        Self {
            read_id: read.id,
            view_id,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ForkTreeReadFacade {
    read: StorageRead,
    view: CoherentView,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Failure {
    ReadAlias,
    ViewAlias,
    FreshRead,
    RawStore,
    SecondGraph,
    Fallback,
    Cache,
    Compatibility,
    IncompletePlan,
    CommitCount,
    StateChanged,
}

impl ForkTreeReadFacade {
    fn from_opening(read: StorageRead, view: CoherentView) -> Result<Self, Failure> {
        if view.read_id != read.id {
            return Err(Failure::ViewAlias);
        }
        Ok(Self { read, view })
    }

    fn checkpoint_history_from_head(&self) -> Vec<&'static str> {
        vec!["root", "checkpoint", "ordinary"]
    }

    fn diff_state_rows_between_commits(&self) -> Vec<&'static str> {
        vec!["ordinary"]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PreparedPlan {
    read_id: u64,
    view_id: u64,
    chronology_len: usize,
    diff_len: usize,
    writes: usize,
    before_state: u64,
    after_state: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PlanSource {
    Facade,
    RawStore,
    SecondGraph,
    Fallback,
    Cache,
    Compatibility,
}

impl PreparedPlan {
    fn from_facade(
        facade: &ForkTreeReadFacade,
        source: PlanSource,
        writes: usize,
        before_state: u64,
        after_state: u64,
    ) -> Result<Self, Failure> {
        match source {
            PlanSource::Facade => {}
            PlanSource::RawStore => return Err(Failure::RawStore),
            PlanSource::SecondGraph => return Err(Failure::SecondGraph),
            PlanSource::Fallback => return Err(Failure::Fallback),
            PlanSource::Cache => return Err(Failure::Cache),
            PlanSource::Compatibility => return Err(Failure::Compatibility),
        }
        let chronology = facade.checkpoint_history_from_head();
        let diff = facade.diff_state_rows_between_commits();
        let plan = Self {
            read_id: facade.read.id,
            view_id: facade.view.view_id,
            chronology_len: chronology.len(),
            diff_len: diff.len(),
            writes,
            before_state,
            after_state,
        };
        plan.validate_complete()
    }

    fn validate_complete(self) -> Result<Self, Failure> {
        if self.chronology_len == 0 || self.diff_len == 0 {
            return Err(Failure::IncompletePlan);
        }
        if self.writes == 0 && self.before_state != self.after_state {
            return Err(Failure::IncompletePlan);
        }
        Ok(self)
    }
}

#[derive(Debug)]
struct AtomicCommit {
    read: StorageRead,
    view: CoherentView,
    commit_count: u32,
    state: u64,
    durable_rows: BTreeMap<&'static str, u64>,
}

impl AtomicCommit {
    fn new(read: StorageRead, view: CoherentView, state: u64) -> Self {
        Self {
            read,
            view,
            commit_count: 0,
            state,
            durable_rows: BTreeMap::from([("state", state)]),
        }
    }

    fn commit(
        &mut self,
        read: StorageRead,
        view: CoherentView,
        plan: PreparedPlan,
    ) -> Result<(), Failure> {
        let plan = plan.validate_complete()?;
        if self.commit_count != 0 {
            return Err(Failure::CommitCount);
        }
        if read != self.read || plan.read_id != read.id {
            return Err(Failure::FreshRead);
        }
        if view != self.view || plan.view_id != view.view_id || view.read_id != read.id {
            return Err(Failure::ReadAlias);
        }
        if plan.before_state != self.state {
            return Err(Failure::StateChanged);
        }
        self.state = plan.after_state;
        self.durable_rows.insert("state", self.state);
        self.commit_count += 1;
        Ok(())
    }
}

#[derive(Debug)]
struct Backend {
    next_read: u64,
    begin_reads: u32,
}

impl Backend {
    fn new() -> Self {
        Self {
            next_read: 1,
            begin_reads: 0,
        }
    }

    fn begin_read(&mut self) -> StorageRead {
        let read = StorageRead { id: self.next_read };
        self.next_read += 1;
        self.begin_reads += 1;
        read
    }
}

fn execute_one_operation(
    backend: &mut Backend,
    publisher: &mut AtomicCommit,
    writes: usize,
    before_state: u64,
    after_state: u64,
) -> Result<(), Failure> {
    let read = backend.begin_read();
    let view = CoherentView::from_read(read, 17);
    let facade = ForkTreeReadFacade::from_opening(read, view)?;
    let plan = PreparedPlan::from_facade(
        &facade,
        PlanSource::Facade,
        writes,
        before_state,
        after_state,
    )?;
    publisher.commit(read, view, plan)
}

#[test]
fn positive_one_opening_read_complete_plan_and_one_atomic_commit() {
    let mut backend = Backend::new();
    let opening = StorageRead { id: 1 };
    let opening_view = CoherentView::from_read(opening, 17);
    let mut publisher = AtomicCommit::new(opening, opening_view, 10);

    execute_one_operation(&mut backend, &mut publisher, 1, 10, 11).unwrap();

    assert_eq!(backend.begin_reads, 1);
    assert_eq!(publisher.commit_count, 1);
    assert_eq!(publisher.durable_rows.get("state"), Some(&11));
}

#[test]
fn negative_swapped_read_view_and_facade_aliases_fail_closed() {
    let read_a = StorageRead { id: 1 };
    let read_b = StorageRead { id: 2 };
    let view_b = CoherentView::from_read(read_b, 17);
    assert_eq!(
        ForkTreeReadFacade::from_opening(read_a, view_b),
        Err(Failure::ViewAlias)
    );

    let view_a = CoherentView::from_read(read_a, 17);
    let facade = ForkTreeReadFacade::from_opening(read_a, view_a).unwrap();
    let plan = PreparedPlan::from_facade(&facade, PlanSource::Facade, 1, 10, 11).unwrap();
    let mut publisher = AtomicCommit::new(read_a, view_a, 10);
    assert_eq!(
        publisher.commit(read_b, view_a, plan),
        Err(Failure::FreshRead)
    );
}

#[test]
fn negative_fresh_begin_read_is_not_the_retained_operation_read() {
    let mut backend = Backend::new();
    let retained = backend.begin_read();
    let retained_view = CoherentView::from_read(retained, 18);
    let facade = ForkTreeReadFacade::from_opening(retained, retained_view).unwrap();
    let plan = PreparedPlan::from_facade(&facade, PlanSource::Facade, 1, 20, 21).unwrap();
    let fresh = backend.begin_read();
    let fresh_view = CoherentView::from_read(fresh, 18);
    let mut publisher = AtomicCommit::new(retained, retained_view, 20);

    assert_eq!(
        publisher.commit(fresh, fresh_view, plan),
        Err(Failure::FreshRead)
    );
    assert_eq!(backend.begin_reads, 2);
}

#[test]
fn negative_raw_graph_fallback_cache_and_compatibility_authorities_fail_closed() {
    let read = StorageRead { id: 3 };
    let view = CoherentView::from_read(read, 19);
    let facade = ForkTreeReadFacade::from_opening(read, view).unwrap();
    for (source, failure) in [
        (PlanSource::RawStore, Failure::RawStore),
        (PlanSource::SecondGraph, Failure::SecondGraph),
        (PlanSource::Fallback, Failure::Fallback),
        (PlanSource::Cache, Failure::Cache),
        (PlanSource::Compatibility, Failure::Compatibility),
    ] {
        assert_eq!(
            PreparedPlan::from_facade(&facade, source, 1, 1, 2),
            Err(failure)
        );
    }
}

#[test]
fn negative_partial_plan_and_duplicate_commit_fail_without_state_change() {
    let read = StorageRead { id: 4 };
    let view = CoherentView::from_read(read, 20);
    let facade = ForkTreeReadFacade::from_opening(read, view).unwrap();
    assert_eq!(
        PreparedPlan::from_facade(&facade, PlanSource::Facade, 0, 5, 6),
        Err(Failure::IncompletePlan)
    );

    let noop = PreparedPlan::from_facade(&facade, PlanSource::Facade, 0, 5, 5).unwrap();
    let mut publisher = AtomicCommit::new(read, view, 5);
    let before_rows = publisher.durable_rows.clone();
    publisher.commit(read, view, noop).unwrap();
    assert_eq!(publisher.state, 5);
    assert_eq!(
        publisher.commit(read, view, noop),
        Err(Failure::CommitCount)
    );
    assert_eq!(publisher.state, 5);
    assert_eq!(publisher.durable_rows, before_rows);

    let mut stale_publisher = AtomicCommit::new(read, view, 5);
    let stale_plan = PreparedPlan {
        read_id: read.id,
        view_id: view.view_id,
        chronology_len: 3,
        diff_len: 1,
        writes: 1,
        before_state: 4,
        after_state: 6,
    };
    assert_eq!(
        stale_publisher.commit(read, view, stale_plan),
        Err(Failure::StateChanged)
    );
    assert_eq!(stale_publisher.commit_count, 0);
    assert_eq!(stale_publisher.state, 5);
    assert_eq!(stale_publisher.durable_rows, BTreeMap::from([("state", 5)]));
}
