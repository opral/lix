//! Executable structural GREEN fixture for W1b-2.
//!
//! This is deliberately self-contained: it models the sealed opening
//! `StorageRead` identity and the operation-owned ForkTree facade without
//! importing a production crate.  Unlike the old fixture, the tests exercise
//! the complete plan and the single atomic commit boundary.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq)]
enum Value {
    Null,
    Json(&'static str),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OwnerProof {
    file_id: &'static str,
    plugin_key: &'static str,
    generation: &'static str,
    revision: u64,
    change_id: &'static str,
    selector_id: &'static str,
    commit_id: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RegistryProof {
    plugin_key: &'static str,
    generation: &'static str,
    revision: u64,
    change_id: &'static str,
    selector_id: &'static str,
    commit_id: &'static str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CurrentState {
    revision: u64,
    selector_id: &'static str,
    commit_id: &'static str,
    owners: BTreeMap<&'static str, OwnerProof>,
    registry: RegistryProof,
    changed_keys: BTreeSet<(&'static str, &'static str)>,
    idempotency: BTreeMap<&'static str, String>,
}

/// The only read acquired by this fixture.  The facade borrows this exact
/// value, so a swapped view has a distinct identity and cannot be accepted.
struct OpeningStorageRead {
    identity: u64,
    selector_id: &'static str,
    commit_id: &'static str,
    revision: u64,
    owner: OwnerProof,
    registry: RegistryProof,
    facade_binds: Cell<u32>,
}

struct ForkTreeReadFacade<'read> {
    read: &'read OpeningStorageRead,
}

impl<'read> ForkTreeReadFacade<'read> {
    fn new(read: &'read OpeningStorageRead) -> Result<Self, Error> {
        let binds = read.facade_binds.get().saturating_add(1);
        read.facade_binds.set(binds);
        if binds != 1 {
            return Err(Error::SecondRead);
        }
        Ok(Self { read })
    }

    fn read_identity(&self) -> u64 {
        self.read.identity
    }

    fn owner_proof(&self, file_id: &'static str) -> Result<&OwnerProof, Error> {
        if self.read.owner.file_id == file_id {
            Ok(&self.read.owner)
        } else {
            Err(Error::MissingOwner)
        }
    }

    fn registry_proof(&self) -> &RegistryProof {
        &self.read.registry
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreparedWrite {
    operation_id: &'static str,
    file_id: &'static str,
    entity: &'static str,
    value: Value,
    rank: u64,
    base_revision: u64,
    base_change_id: &'static str,
    base_selector_id: &'static str,
    base_commit_id: &'static str,
}

impl PreparedWrite {
    fn fingerprint(&self) -> String {
        format!(
            "{}|{}|{}|{:?}",
            self.operation_id, self.file_id, self.entity, self.value
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreparedPlan {
    view_identity: u64,
    expected_write_count: usize,
    writes: Vec<PreparedWrite>,
    digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Error {
    SwappedRead,
    PartialPlan,
    SecondRead,
    SecondCommit,
    MissingOwner,
    OwnerMismatch,
    RegistryMismatch,
    SelectorMismatch,
    CommitMismatch,
    DuplicateOperation,
    IdempotencyMismatch,
    PartialIdempotency,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Outcome {
    Direct { operations: Vec<&'static str> },
    UnrelatedOwnerSuccess { operations: Vec<&'static str> },
    Idempotent { operations: Vec<&'static str> },
    Reconciled { operations: Vec<&'static str> },
}

#[derive(Default)]
struct AtomicCommit {
    commits: u32,
    plans: Vec<PreparedPlan>,
}

impl AtomicCommit {
    fn commit(&mut self, read: &OpeningStorageRead, plan: PreparedPlan) -> Result<(), Error> {
        if self.commits != 0 {
            return Err(Error::SecondCommit);
        }
        if plan.view_identity != read.identity
            || plan.expected_write_count != plan.writes.len()
            || plan.writes.is_empty()
            || plan.digest != digest(&plan.writes)
            || !is_sorted_unique(&plan.writes)
        {
            return Err(Error::PartialPlan);
        }
        self.commits = 1;
        self.plans.push(plan);
        Ok(())
    }
}

struct Transaction {
    opening: OpeningStorageRead,
    current: CurrentState,
    atomic: AtomicCommit,
}

impl Transaction {
    fn commit_prepared(&mut self, writes: &[PreparedWrite]) -> Result<Outcome, Error> {
        if self.atomic.commits != 0 {
            return Err(Error::SecondCommit);
        }
        let facade = ForkTreeReadFacade::new(&self.opening)?;
        let (outcome, plan) = self.reconcile_with_facade(&facade, writes)?;
        if !matches!(outcome, Outcome::Idempotent { .. }) {
            self.atomic.commit(&self.opening, plan)?;
        }
        Ok(outcome)
    }

    fn reconcile_with_facade(
        &self,
        facade: &ForkTreeReadFacade<'_>,
        writes: &[PreparedWrite],
    ) -> Result<(Outcome, PreparedPlan), Error> {
        if facade.read_identity() != self.opening.identity
            || !std::ptr::eq(facade.read, &self.opening)
        {
            return Err(Error::SwappedRead);
        }
        let ordered = complete_plan(writes, facade.read_identity())?;
        authenticate_all(facade, &self.current, &ordered)?;
        let outcome = idempotency_outcome(&self.current.idempotency, &ordered)?;
        if let Some(outcome) = outcome {
            return Ok((outcome, plan_for(&ordered, facade.read_identity())));
        }

        let operations = ordered.iter().map(|write| write.operation_id).collect();
        let outcome = if self.opening.revision == self.current.revision {
            Outcome::Direct { operations }
        } else if ordered.iter().all(|write| {
            !self
                .current
                .changed_keys
                .contains(&(write.file_id, write.entity))
        }) {
            Outcome::UnrelatedOwnerSuccess { operations }
        } else {
            Outcome::Reconciled { operations }
        };
        Ok((outcome, plan_for(&ordered, facade.read_identity())))
    }

    fn try_second_read(&self) -> Result<ForkTreeReadFacade<'_>, Error> {
        ForkTreeReadFacade::new(&self.opening)
    }

    fn commit_partial_plan(&mut self, plan: PreparedPlan) -> Result<(), Error> {
        self.atomic.commit(&self.opening, plan)
    }
}

fn complete_plan(
    writes: &[PreparedWrite],
    view_identity: u64,
) -> Result<Vec<PreparedWrite>, Error> {
    if writes.is_empty() {
        return Err(Error::PartialPlan);
    }
    let mut ordered = writes.to_vec();
    ordered.sort_by_key(|write| (write.rank, write.operation_id));
    if ordered
        .windows(2)
        .any(|pair| pair[0].operation_id == pair[1].operation_id)
    {
        return Err(Error::DuplicateOperation);
    }
    let plan = plan_for(&ordered, view_identity);
    if plan.expected_write_count != plan.writes.len() {
        return Err(Error::PartialPlan);
    }
    Ok(ordered)
}

fn plan_for(writes: &[PreparedWrite], view_identity: u64) -> PreparedPlan {
    PreparedPlan {
        view_identity,
        expected_write_count: writes.len(),
        writes: writes.to_vec(),
        digest: digest(writes),
    }
}

fn digest(writes: &[PreparedWrite]) -> String {
    writes
        .iter()
        .map(PreparedWrite::fingerprint)
        .collect::<Vec<_>>()
        .join(";")
}

fn is_sorted_unique(writes: &[PreparedWrite]) -> bool {
    writes
        .windows(2)
        .all(|pair| (pair[0].rank, pair[0].operation_id) < (pair[1].rank, pair[1].operation_id))
}

fn authenticate_all(
    facade: &ForkTreeReadFacade<'_>,
    current: &CurrentState,
    writes: &[PreparedWrite],
) -> Result<(), Error> {
    for write in writes {
        if write.base_revision != facade.read.revision
            || write.base_change_id != change_id(facade.read.revision)
            || write.base_selector_id != facade.read.selector_id
            || write.base_commit_id != facade.read.commit_id
        {
            return Err(Error::OwnerMismatch);
        }
        if current.selector_id != facade.read.selector_id {
            return Err(Error::SelectorMismatch);
        }
        validate_snapshot_identity(
            facade.read.selector_id,
            facade.read.commit_id,
            facade.owner_proof(write.file_id)?,
            facade.registry_proof(),
            write.file_id,
            facade.read.revision,
            change_id(facade.read.revision),
            facade.read.selector_id,
            facade.read.commit_id,
        )?;
        let owner = current
            .owners
            .get(write.file_id)
            .ok_or(Error::MissingOwner)?;
        validate_snapshot_identity(
            current.selector_id,
            current.commit_id,
            owner,
            &current.registry,
            write.file_id,
            current.revision,
            change_id(current.revision),
            current.selector_id,
            current.commit_id,
        )?;
    }
    Ok(())
}

fn validate_snapshot_identity(
    selector_id: &str,
    commit_id: &str,
    owner: &OwnerProof,
    registry: &RegistryProof,
    file_id: &str,
    revision: u64,
    expected_change_id: &str,
    expected_selector: &str,
    expected_commit: &str,
) -> Result<(), Error> {
    if selector_id != expected_selector
        || owner.selector_id != expected_selector
        || registry.selector_id != expected_selector
    {
        return Err(Error::SelectorMismatch);
    }
    if commit_id != expected_commit
        || owner.commit_id != expected_commit
        || registry.commit_id != expected_commit
    {
        return Err(Error::CommitMismatch);
    }
    if owner.file_id != file_id
        || owner.revision != revision
        || owner.change_id != expected_change_id
    {
        return Err(Error::OwnerMismatch);
    }
    if registry.revision != revision || registry.change_id != expected_change_id {
        return Err(Error::RegistryMismatch);
    }
    if owner.plugin_key != registry.plugin_key || owner.generation != registry.generation {
        return Err(Error::RegistryMismatch);
    }
    Ok(())
}

fn idempotency_outcome(
    idempotency: &BTreeMap<&'static str, String>,
    writes: &[PreparedWrite],
) -> Result<Option<Outcome>, Error> {
    let mut matching = Vec::new();
    let mut idempotent_count = 0;
    let mut missing = 0;
    for write in writes {
        match idempotency.get(write.operation_id) {
            Some(fingerprint) if fingerprint == &write.fingerprint() => {
                matching.push(write.operation_id);
                idempotent_count += 1;
            }
            Some(_) => return Err(Error::IdempotencyMismatch),
            None => missing += 1,
        }
    }
    if !matching.is_empty() && missing != 0 {
        return Err(Error::PartialIdempotency);
    }
    if idempotent_count == writes.len() {
        return Ok(Some(Outcome::Idempotent {
            operations: matching,
        }));
    }
    Ok(None)
}

fn change_id(revision: u64) -> &'static str {
    if revision == 1 {
        "change-1"
    } else {
        "change-2"
    }
}

fn opening() -> OpeningStorageRead {
    OpeningStorageRead {
        identity: 11,
        selector_id: "selector-1",
        commit_id: "commit-1",
        revision: 1,
        owner: owner("file-a", 1, "change-1", "selector-1", "commit-1"),
        registry: registry(1, "change-1", "selector-1", "commit-1"),
        facade_binds: Cell::new(0),
    }
}

fn owner(
    file_id: &'static str,
    revision: u64,
    change_id: &'static str,
    selector_id: &'static str,
    commit_id: &'static str,
) -> OwnerProof {
    OwnerProof {
        file_id,
        plugin_key: "plugin-a",
        generation: "generation-a",
        revision,
        change_id,
        selector_id,
        commit_id,
    }
}

fn registry(
    revision: u64,
    change_id: &'static str,
    selector_id: &'static str,
    commit_id: &'static str,
) -> RegistryProof {
    RegistryProof {
        plugin_key: "plugin-a",
        generation: "generation-a",
        revision,
        change_id,
        selector_id,
        commit_id,
    }
}

fn current_state(revision: u64) -> CurrentState {
    CurrentState {
        revision,
        selector_id: "selector-1",
        commit_id: if revision == 1 {
            "commit-1"
        } else {
            "commit-2"
        },
        owners: BTreeMap::from([(
            "file-a",
            owner(
                "file-a",
                revision,
                change_id(revision),
                "selector-1",
                if revision == 1 {
                    "commit-1"
                } else {
                    "commit-2"
                },
            ),
        )]),
        registry: registry(
            revision,
            change_id(revision),
            "selector-1",
            if revision == 1 {
                "commit-1"
            } else {
                "commit-2"
            },
        ),
        changed_keys: BTreeSet::new(),
        idempotency: BTreeMap::new(),
    }
}

fn transaction(revision: u64) -> Transaction {
    Transaction {
        opening: opening(),
        current: current_state(revision),
        atomic: AtomicCommit::default(),
    }
}

fn write(
    operation_id: &'static str,
    entity: &'static str,
    value: Value,
    rank: u64,
) -> PreparedWrite {
    PreparedWrite {
        operation_id,
        file_id: "file-a",
        entity,
        value,
        rank,
        base_revision: 1,
        base_change_id: "change-1",
        base_selector_id: "selector-1",
        base_commit_id: "commit-1",
    }
}

fn two_writes() -> [PreparedWrite; 2] {
    [
        write("op-low", "row-a", Value::Null, 1),
        write("op-high", "row-b", Value::Json("high"), 2),
    ]
}

#[test]
fn complete_multi_write_plan_uses_one_opening_read_and_one_atomic_commit() {
    let mut tx = transaction(2);
    tx.current.changed_keys.insert(("file-a", "row-a"));
    let result = tx.commit_prepared(&two_writes()).expect("reconcile");
    assert_eq!(
        result,
        Outcome::Reconciled {
            operations: vec!["op-low", "op-high"]
        }
    );
    assert_eq!(tx.opening.facade_binds.get(), 1);
    assert_eq!(tx.atomic.commits, 1);
    assert_eq!(tx.atomic.plans.len(), 1);
    assert_eq!(tx.atomic.plans[0].writes.len(), 2);
    assert!(is_sorted_unique(&tx.atomic.plans[0].writes));
}

#[test]
fn swapped_view_is_rejected_before_plan_or_commit() {
    let tx = transaction(2);
    let other = opening();
    let facade = ForkTreeReadFacade::new(&other).expect("other view can bind itself");
    let error = tx
        .reconcile_with_facade(&facade, &two_writes())
        .expect_err("swapped view");
    assert_eq!(error, Error::SwappedRead);
    assert_eq!(tx.atomic.commits, 0);
    assert_eq!(tx.opening.facade_binds.get(), 0);
}

#[test]
fn partial_plan_is_rejected_without_atomic_commit() {
    let mut tx = transaction(2);
    let writes = two_writes();
    let plan = PreparedPlan {
        view_identity: tx.opening.identity,
        expected_write_count: writes.len(),
        writes: vec![writes[0].clone()],
        digest: digest(&writes[..1]),
    };
    assert_eq!(tx.commit_partial_plan(plan), Err(Error::PartialPlan));
    assert_eq!(tx.atomic.commits, 0);
}

#[test]
fn second_commit_and_second_read_are_rejected() {
    let mut tx = transaction(1);
    tx.commit_prepared(&two_writes()).expect("first commit");
    assert_eq!(tx.commit_prepared(&two_writes()), Err(Error::SecondCommit));
    assert!(matches!(tx.try_second_read(), Err(Error::SecondRead)));
    assert_eq!(tx.opening.facade_binds.get(), 2);
    assert_eq!(tx.atomic.commits, 1);
}

#[test]
fn mixed_idempotency_match_and_mismatch_checks_every_write() {
    let mut tx = transaction(2);
    let writes = two_writes();
    tx.current
        .idempotency
        .insert("op-low", writes[0].fingerprint());
    tx.current
        .idempotency
        .insert("op-high", "forged-payload".into());
    assert_eq!(tx.commit_prepared(&writes), Err(Error::IdempotencyMismatch));
    assert_eq!(tx.atomic.commits, 0);
}

#[test]
fn all_idempotency_operations_must_match_for_replay() {
    let mut tx = transaction(2);
    let writes = two_writes();
    for write in &writes {
        tx.current
            .idempotency
            .insert(write.operation_id, write.fingerprint());
    }
    assert_eq!(
        tx.commit_prepared(&writes),
        Ok(Outcome::Idempotent {
            operations: vec!["op-low", "op-high"]
        })
    );
    assert_eq!(tx.atomic.commits, 0);
}

#[test]
fn reordered_inputs_and_duplicate_operations_are_deterministic() {
    let mut tx = transaction(2);
    tx.current.changed_keys.insert(("file-a", "row-a"));
    let writes = two_writes();
    let reversed = [writes[1].clone(), writes[0].clone()];
    let first = tx
        .reconcile_with_facade(&ForkTreeReadFacade::new(&tx.opening).unwrap(), &reversed)
        .expect("reordered plan");
    assert_eq!(
        first.0,
        Outcome::Reconciled {
            operations: vec!["op-low", "op-high"]
        }
    );
    let mut duplicate_tx = transaction(2);
    assert_eq!(
        duplicate_tx.commit_prepared(&[writes[0].clone(), writes[0].clone()]),
        Err(Error::DuplicateOperation)
    );
}

#[test]
fn registry_identity_is_required_even_for_unrelated_owner_changes() {
    let mut tx = transaction(2);
    tx.current
        .changed_keys
        .insert(("other-file", "unrelated-row"));
    tx.current.registry.plugin_key = "plugin-forged";
    assert_eq!(
        tx.commit_prepared(&two_writes()),
        Err(Error::RegistryMismatch)
    );
    assert_eq!(tx.atomic.commits, 0);
}

#[test]
fn valid_unrelated_owner_change_succeeds() {
    let mut tx = transaction(2);
    tx.current
        .changed_keys
        .insert(("other-file", "unrelated-row"));
    assert_eq!(
        tx.commit_prepared(&two_writes()),
        Ok(Outcome::UnrelatedOwnerSuccess {
            operations: vec!["op-low", "op-high"]
        })
    );
    assert_eq!(tx.atomic.commits, 1);
}
