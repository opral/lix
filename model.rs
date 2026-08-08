//! Pure acceptance model for the b59 TrackedState transaction migration.
//!
//! This file deliberately has no Lix imports and no storage adapter dependency.
//! It is intended to be compiled later with rustc --edition=2021 --test.
//! The model tests ownership and atomicity rules; it is not the production
//! codec or adapter test.

use std::collections::BTreeMap;

type Key = String;
type Bytes = Vec<u8>;
type CommitId = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Cell {
    Value(Option<String>),
    Tombstone,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Snapshot {
    commit: CommitId,
    parent: Option<CommitId>,
    generation: u64,
    branch_id: String,
    global: BTreeMap<Key, Cell>,
    local: BTreeMap<Key, Cell>,
    state_root: Bytes,
    catalog_root: Bytes,
    checkpoint_root: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct View {
    global_selector: Bytes,
    branch_selector: Bytes,
    epoch: u64,
    owner_id: String,
    owner_epoch: u64,
    branch_id: String,
    state_root: Bytes,
    catalog_root: Bytes,
    checkpoint_root: Bytes,
    snapshot: Snapshot,
    snapshot_bytes: Bytes,
    view_id: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Intent {
    Noop,
    Rows,
    UndoRedo { target: CommitId },
    Checkpoint { target: CommitId },
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Transaction {
    view: View,
    staged: BTreeMap<Key, Cell>,
    savepoints: Vec<BTreeMap<Key, Cell>>,
    intent: Intent,
    idempotency_key: Option<String>,
    idempotency_digest: u64,
    expected_change: Option<(Key, u64)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Plan {
    expected_global_selector: Bytes,
    expected_branch_selector: Bytes,
    expected_epoch: u64,
    expected_state_root: Bytes,
    expected_catalog_root: Bytes,
    expected_checkpoint_root: Bytes,
    branch_id: String,
    parent: CommitId,
    generation: u64,
    staged: BTreeMap<Key, Cell>,
    intent: Intent,
    idempotency_key: Option<String>,
    idempotency_digest: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum OpenError {
    MissingSelector,
    SelectorAuthentication,
    WrongSelectorKind,
    MissingRoot,
    RootAuthentication,
    WrongRootKind,
    MissingSnapshot,
    SnapshotAuthentication,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CommitError {
    StaleView,
    WrongOwner,
    ExpectedChangeMismatch,
    MissingCommit,
    CorruptHistory,
    FaultAfterObjectStage,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ReadError {
    MissingCommit,
    WrongBranch,
    SnapshotAuthentication,
    CorruptSnapshot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Reconciliation {
    SameOwnerStable,
    UnrelatedOwner,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Fault {
    None,
    AfterObjectStage,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Counters {
    begin_reads: u64,
    plans: u64,
    prepares: u64,
    commits: u64,
    writes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Store {
    global_selector: Bytes,
    branch_selector: Bytes,
    epoch: u64,
    branch_id: String,
    state_root: Bytes,
    catalog_root: Bytes,
    checkpoint_root: Bytes,
    global_rows: BTreeMap<Key, Cell>,
    branch_rows: BTreeMap<Key, Cell>,
    history: BTreeMap<CommitId, Snapshot>,
    idempotency: BTreeMap<String, u64>,
    next_commit: CommitId,
    checkpoint_floor: CommitId,
    checkpoint_target: Option<CommitId>,
    redo_target: Option<CommitId>,
    owner_epochs: BTreeMap<String, u64>,
    fault: Fault,
    counters: Counters,
}

impl Store {
    fn new() -> Self {
        let branch_id = "main".to_owned();
        let mut store = Self {
            global_selector: b"global-selector-v1:0".to_vec(),
            branch_selector: b"branch-selector-v1:main:0".to_vec(),
            epoch: 0,
            branch_id: branch_id.clone(),
            state_root: b"state-root-v1:0".to_vec(),
            catalog_root: b"catalog-root-v1:0".to_vec(),
            checkpoint_root: b"checkpoint-root-v1:0".to_vec(),
            global_rows: BTreeMap::new(),
            branch_rows: BTreeMap::new(),
            history: BTreeMap::new(),
            idempotency: BTreeMap::new(),
            next_commit: 1,
            checkpoint_floor: 0,
            checkpoint_target: None,
            redo_target: None,
            owner_epochs: BTreeMap::from([(branch_id.clone(), 0), ("unrelated".to_owned(), 0)]),
            fault: Fault::None,
            counters: Counters::default(),
        };
        let initial = Snapshot {
            commit: 0,
            parent: None,
            generation: 0,
            branch_id,
            global: BTreeMap::new(),
            local: BTreeMap::new(),
            state_root: store.state_root.clone(),
            catalog_root: store.catalog_root.clone(),
            checkpoint_root: store.checkpoint_root.clone(),
        };
        store.history.insert(0, initial);
        store
    }

    fn begin_view(&mut self) -> Result<View, OpenError> {
        self.counters.begin_reads += 1;
        validate_selector(&self.global_selector, b"global-selector-v1:")?;
        validate_selector(&self.branch_selector, b"branch-selector-v1:")?;
        validate_root(&self.state_root, b"state-root-v1:")?;
        validate_root(&self.catalog_root, b"catalog-root-v1:")?;
        validate_root(&self.checkpoint_root, b"checkpoint-root-v1:")?;
        let current_commit = self.next_commit.saturating_sub(1);
        let snapshot = self
            .history
            .get(&current_commit)
            .cloned()
            .ok_or(OpenError::MissingSnapshot)?;
        if !snapshot_matches_store(&snapshot, self) {
            return Err(OpenError::SnapshotAuthentication);
        }
        let owner_epoch = self
            .owner_epochs
            .get(&self.branch_id)
            .copied()
            .ok_or(OpenError::MissingSnapshot)?;
        let snapshot_bytes = snapshot_bytes(&snapshot);
        Ok(View {
            global_selector: self.global_selector.clone(),
            branch_selector: self.branch_selector.clone(),
            epoch: self.epoch,
            owner_id: self.branch_id.clone(),
            owner_epoch,
            branch_id: self.branch_id.clone(),
            state_root: self.state_root.clone(),
            catalog_root: self.catalog_root.clone(),
            checkpoint_root: self.checkpoint_root.clone(),
            snapshot,
            snapshot_bytes,
            view_id: view_id(
                &self.global_selector,
                &self.branch_selector,
                self.epoch,
                &self.state_root,
                &self.catalog_root,
                &self.checkpoint_root,
            ),
        })
    }

    fn reconcile_owner(
        &self,
        tx: &Transaction,
        changed_owner: &str,
    ) -> Result<Reconciliation, CommitError> {
        let current_epoch = self
            .owner_epochs
            .get(changed_owner)
            .copied()
            .ok_or(CommitError::CorruptHistory)?;
        if changed_owner == tx.view.owner_id {
            if current_epoch != tx.view.owner_epoch {
                return Err(CommitError::StaleView);
            }
            return Ok(Reconciliation::SameOwnerStable);
        }
        Ok(Reconciliation::UnrelatedOwner)
    }

    fn begin_transaction(&mut self) -> Result<Transaction, OpenError> {
        let view = self.begin_view()?;
        Ok(Transaction {
            view,
            staged: BTreeMap::new(),
            savepoints: Vec::new(),
            intent: Intent::Noop,
            idempotency_key: None,
            idempotency_digest: 0,
            expected_change: None,
        })
    }

    fn read_at_view(&self, tx: &Transaction, key: &str) -> Result<Option<Cell>, ReadError> {
        validate_captured_view(tx)?;
        if let Some(cell) = tx.staged.get(key) {
            return Ok(match cell {
                Cell::Tombstone => None,
                other => Some(other.clone()),
            });
        }
        if let Some(cell) = tx.view.snapshot.local.get(key) {
            return Ok(match cell {
                Cell::Tombstone => None,
                other => Some(other.clone()),
            });
        }
        Ok(tx
            .view
            .snapshot
            .global
            .get(key)
            .and_then(|cell| match cell {
                Cell::Tombstone => None,
                other => Some(other.clone()),
            }))
    }

    fn read_snapshot(
        &self,
        tx: &Transaction,
        commit: CommitId,
        key: &str,
    ) -> Result<Option<Cell>, ReadError> {
        validate_captured_view(tx)?;
        let snapshot = self.history.get(&commit).ok_or(ReadError::MissingCommit)?;
        if snapshot.branch_id != tx.view.branch_id {
            return Err(ReadError::WrongBranch);
        }
        validate_snapshot_record(snapshot)?;
        if let Some(cell) = snapshot.local.get(key) {
            return Ok(match cell {
                Cell::Tombstone => None,
                other => Some(other.clone()),
            });
        }
        Ok(snapshot.global.get(key).cloned())
    }

    fn classify_intent(&self, tx: &Transaction) -> Intent {
        match &tx.intent {
            Intent::Rows if tx.staged.is_empty() => Intent::Noop,
            intent => intent.clone(),
        }
    }

    fn prepare(&mut self, tx: &Transaction) -> Result<Option<Plan>, CommitError> {
        match self.classify_intent(tx) {
            Intent::Noop | Intent::Unsupported => return Ok(None),
            intent => {
                if !captured_view_is_valid(&tx.view) {
                    return Err(CommitError::CorruptHistory);
                }
                if tx.view.branch_id != self.branch_id {
                    return Err(CommitError::WrongOwner);
                }
                if tx.view.global_selector != self.global_selector
                    || tx.view.branch_selector != self.branch_selector
                    || tx.view.epoch != self.epoch
                    || tx.view.state_root != self.state_root
                    || tx.view.catalog_root != self.catalog_root
                    || tx.view.checkpoint_root != self.checkpoint_root
                {
                    return Err(CommitError::StaleView);
                }
                if let Some((key, expected)) = &tx.expected_change {
                    let actual = self
                        .history
                        .get(&self.next_commit.saturating_sub(1))
                        .and_then(|snapshot| snapshot.global.get(key))
                        .map(cell_digest)
                        .unwrap_or(0);
                    if actual != *expected {
                        return Err(CommitError::ExpectedChangeMismatch);
                    }
                }
                self.counters.plans += 1;
                self.counters.prepares += 1;
                Ok(Some(Plan {
                    expected_global_selector: tx.view.global_selector.clone(),
                    expected_branch_selector: tx.view.branch_selector.clone(),
                    expected_epoch: tx.view.epoch,
                    expected_state_root: tx.view.state_root.clone(),
                    expected_catalog_root: tx.view.catalog_root.clone(),
                    expected_checkpoint_root: tx.view.checkpoint_root.clone(),
                    branch_id: tx.view.branch_id.clone(),
                    parent: self.next_commit.saturating_sub(1),
                    generation: self
                        .history
                        .get(&self.next_commit.saturating_sub(1))
                        .map(|snapshot| snapshot.generation + 1)
                        .unwrap_or(0),
                    staged: tx.staged.clone(),
                    intent,
                    idempotency_key: tx.idempotency_key.clone(),
                    idempotency_digest: tx.idempotency_digest,
                }))
            }
        }
    }

    fn commit(&mut self, plan: Plan) -> Result<CommitId, CommitError> {
        if plan.expected_global_selector != self.global_selector
            || plan.expected_branch_selector != self.branch_selector
            || plan.expected_epoch != self.epoch
            || plan.expected_state_root != self.state_root
            || plan.expected_catalog_root != self.catalog_root
            || plan.expected_checkpoint_root != self.checkpoint_root
            || plan.branch_id != self.branch_id
        {
            return Err(CommitError::StaleView);
        }
        if let Some(key) = &plan.idempotency_key {
            if let Some(previous) = self.idempotency.get(key) {
                if *previous == plan.idempotency_digest {
                    return Ok(self.next_commit.saturating_sub(1));
                }
                return Err(CommitError::StaleView);
            }
        }

        let before = self.clone();
        let commit = self.next_commit;
        let mut global = self.global_rows.clone();
        let mut local = self.branch_rows.clone();
        match &plan.intent {
            Intent::Rows => {
                for (key, cell) in &plan.staged {
                    local.insert(key.clone(), cell.clone());
                }
            }
            Intent::UndoRedo { target } => {
                let target_snapshot = self.history.get(target).ok_or(CommitError::MissingCommit)?;
                if !valid_snapshot_roots(target_snapshot) {
                    return Err(CommitError::CorruptHistory);
                }
                global = target_snapshot.global.clone();
                local = target_snapshot.local.clone();
                self.redo_target = Some(plan.parent);
            }
            Intent::Checkpoint { target } => {
                if !self.history.contains_key(target) {
                    return Err(CommitError::MissingCommit);
                }
                if !valid_snapshot_roots(self.history.get(target).unwrap()) {
                    return Err(CommitError::CorruptHistory);
                }
                self.checkpoint_floor = self.checkpoint_floor.max(*target);
                self.checkpoint_target = Some(*target);
            }
            Intent::Noop | Intent::Unsupported => return Err(CommitError::StaleView),
        }
        self.global_rows = global.clone();
        self.branch_rows = local.clone();
        self.state_root = root_bytes("state-root-v1", commit);
        self.catalog_root = root_bytes("catalog-root-v1", commit);
        if matches!(
            &plan.intent,
            Intent::UndoRedo { .. } | Intent::Checkpoint { .. }
        ) {
            self.checkpoint_root = root_bytes("checkpoint-root-v1", commit);
        }
        self.global_selector = selector_bytes("global-selector-v1", self.epoch + 1);
        self.branch_selector = branch_selector_bytes("branch-selector-v1", &self.branch_id, commit);
        self.epoch += 1;
        *self.owner_epochs.entry(self.branch_id.clone()).or_default() += 1;
        let snapshot = Snapshot {
            commit,
            parent: Some(plan.parent),
            generation: plan.generation,
            branch_id: self.branch_id.clone(),
            global: global.clone(),
            local: local.clone(),
            state_root: self.state_root.clone(),
            catalog_root: self.catalog_root.clone(),
            checkpoint_root: self.checkpoint_root.clone(),
        };
        self.history.insert(commit, snapshot);
        if let Some(key) = plan.idempotency_key {
            self.idempotency.insert(key, plan.idempotency_digest);
        }
        self.next_commit += 1;
        self.counters.commits += 1;
        self.counters.writes += 1;
        if self.fault == Fault::AfterObjectStage {
            *self = before;
            return Err(CommitError::FaultAfterObjectStage);
        }
        Ok(commit)
    }

    fn external_epoch_advance(&mut self) {
        self.epoch += 1;
        self.global_selector = selector_bytes("global-selector-v1", self.epoch);
        *self.owner_epochs.entry(self.branch_id.clone()).or_default() += 1;
    }

    fn external_unrelated_owner_advance(&mut self) {
        *self.owner_epochs.entry("unrelated".to_owned()).or_default() += 1;
    }

    fn external_publish_row(&mut self, key: impl Into<String>, cell: Cell) {
        let parent = self.next_commit.saturating_sub(1);
        let commit = self.next_commit;
        self.branch_rows.insert(key.into(), cell);
        self.state_root = root_bytes("state-root-v1", commit);
        self.catalog_root = root_bytes("catalog-root-v1", commit);
        self.global_selector = selector_bytes("global-selector-v1", self.epoch + 1);
        self.branch_selector = branch_selector_bytes("branch-selector-v1", &self.branch_id, commit);
        self.epoch += 1;
        *self.owner_epochs.entry(self.branch_id.clone()).or_default() += 1;
        self.history.insert(
            commit,
            Snapshot {
                commit,
                parent: Some(parent),
                generation: self
                    .history
                    .get(&parent)
                    .map(|snapshot| snapshot.generation + 1)
                    .unwrap_or(0),
                branch_id: self.branch_id.clone(),
                global: self.global_rows.clone(),
                local: self.branch_rows.clone(),
                state_root: self.state_root.clone(),
                catalog_root: self.catalog_root.clone(),
                checkpoint_root: self.checkpoint_root.clone(),
            },
        );
        self.next_commit += 1;
        self.counters.commits += 1;
        self.counters.writes += 1;
    }

    fn seed_global_row(&mut self, key: impl Into<String>, cell: Cell) {
        self.global_rows.insert(key.into(), cell);
        let current = self.next_commit.saturating_sub(1);
        if let Some(snapshot) = self.history.get_mut(&current) {
            snapshot.global = self.global_rows.clone();
        }
    }

    fn corrupt_global_selector(&mut self) {
        self.global_selector = b"wrong-kind".to_vec();
    }

    fn corrupt_state_root(&mut self) {
        self.state_root = b"missing-object".to_vec();
    }

    fn corrupt_history_root(&mut self, commit: CommitId) {
        if let Some(snapshot) = self.history.get_mut(&commit) {
            snapshot.state_root = b"corrupt-history-root".to_vec();
        }
    }

    fn set_fault(&mut self, fault: Fault) {
        self.fault = fault;
    }

    fn reopen(&self) -> Self {
        self.clone()
    }
}

impl Transaction {
    fn read(&self, store: &Store, key: &str) -> Result<Option<Cell>, ReadError> {
        store.read_at_view(self, key)
    }

    fn stage(&mut self, key: impl Into<String>, cell: Cell) {
        self.intent = Intent::Rows;
        self.staged.insert(key.into(), cell);
    }

    fn stage_null(&mut self, key: impl Into<String>) {
        self.stage(key, Cell::Value(None));
    }

    fn stage_tombstone(&mut self, key: impl Into<String>) {
        self.stage(key, Cell::Tombstone);
    }

    fn savepoint(&mut self) {
        self.savepoints.push(self.staged.clone());
    }

    fn rollback_to_savepoint(&mut self) {
        if let Some(staged) = self.savepoints.pop() {
            self.staged = staged;
        }
    }

    fn set_unsupported(&mut self) {
        self.intent = Intent::Unsupported;
    }

    fn set_checkpoint(&mut self, target: CommitId) {
        self.intent = Intent::Checkpoint { target };
    }

    fn set_undo(&mut self, target: CommitId) {
        self.intent = Intent::UndoRedo { target };
    }

    fn set_idempotency(&mut self, key: impl Into<String>, digest: u64) {
        self.idempotency_key = Some(key.into());
        self.idempotency_digest = digest;
    }

    fn expect_change(&mut self, key: impl Into<String>, digest: u64) {
        self.expected_change = Some((key.into(), digest));
    }

    fn apply_transition(
        &mut self,
        store: &Store,
        source: CommitId,
        desired: CommitId,
        key: &str,
        expected_change: u64,
    ) -> Result<(), CommitError> {
        let source_snapshot = store
            .history
            .get(&source)
            .ok_or(CommitError::MissingCommit)?;
        let desired_snapshot = store
            .history
            .get(&desired)
            .ok_or(CommitError::MissingCommit)?;
        if !valid_snapshot_roots(source_snapshot) || !valid_snapshot_roots(desired_snapshot) {
            return Err(CommitError::CorruptHistory);
        }
        let source_cell = source_snapshot.global.get(key).map(cell_digest);
        if source_cell != Some(expected_change) {
            return Err(CommitError::ExpectedChangeMismatch);
        }
        let desired_cell = desired_snapshot
            .global
            .get(key)
            .cloned()
            .unwrap_or(Cell::Tombstone);
        self.stage(key.to_owned(), desired_cell);
        Ok(())
    }
}

fn validate_selector(value: &[u8], prefix: &[u8]) -> Result<(), OpenError> {
    if value.is_empty() {
        return Err(OpenError::MissingSelector);
    }
    if !value.starts_with(prefix) {
        return Err(OpenError::SelectorAuthentication);
    }
    if value.contains(&b'?') {
        return Err(OpenError::WrongSelectorKind);
    }
    Ok(())
}

fn validate_root(value: &[u8], prefix: &[u8]) -> Result<(), OpenError> {
    if value.is_empty() {
        return Err(OpenError::MissingRoot);
    }
    if !value.starts_with(prefix) {
        return Err(OpenError::RootAuthentication);
    }
    if value.contains(&b'?') {
        return Err(OpenError::WrongRootKind);
    }
    Ok(())
}

fn selector_bytes(prefix: &str, generation: u64) -> Bytes {
    format!("{prefix}:{generation}").into_bytes()
}

fn branch_selector_bytes(prefix: &str, branch: &str, generation: u64) -> Bytes {
    format!("{prefix}:{branch}:{generation}").into_bytes()
}

fn root_bytes(prefix: &str, commit: CommitId) -> Bytes {
    format!("{prefix}:{commit}").into_bytes()
}

fn cell_digest(cell: &Cell) -> u64 {
    match cell {
        Cell::Value(None) => 1,
        Cell::Value(Some(value)) => value.bytes().fold(17_u64, |digest, byte| {
            digest.wrapping_mul(31).wrapping_add(byte as u64)
        }),
        Cell::Tombstone => 2,
    }
}

fn view_id(
    global_selector: &[u8],
    branch_selector: &[u8],
    epoch: u64,
    state_root: &[u8],
    catalog_root: &[u8],
    checkpoint_root: &[u8],
) -> Bytes {
    let mut digest = epoch.wrapping_add(0x9e37_79b9);
    for part in [
        global_selector,
        branch_selector,
        state_root,
        catalog_root,
        checkpoint_root,
    ] {
        for byte in part {
            digest = digest.rotate_left(5) ^ u64::from(*byte);
        }
    }
    digest.to_be_bytes().to_vec()
}

fn snapshot_bytes(snapshot: &Snapshot) -> Bytes {
    format!("{snapshot:?}").into_bytes()
}

fn valid_snapshot_roots(snapshot: &Snapshot) -> bool {
    snapshot.state_root.starts_with(b"state-root-v1:")
        && snapshot.catalog_root.starts_with(b"catalog-root-v1:")
        && snapshot.checkpoint_root.starts_with(b"checkpoint-root-v1:")
}

fn snapshot_matches_store(snapshot: &Snapshot, store: &Store) -> bool {
    snapshot.branch_id == store.branch_id
        && snapshot.global == store.global_rows
        && snapshot.local == store.branch_rows
        && snapshot.state_root == store.state_root
        && snapshot.catalog_root == store.catalog_root
        && snapshot.checkpoint_root == store.checkpoint_root
        && valid_snapshot_roots(snapshot)
}

fn captured_view_is_valid(view: &View) -> bool {
    view.snapshot_bytes == snapshot_bytes(&view.snapshot)
        && view.snapshot.branch_id == view.branch_id
        && view.snapshot.state_root == view.state_root
        && view.snapshot.catalog_root == view.catalog_root
        && view.snapshot.checkpoint_root == view.checkpoint_root
        && valid_snapshot_roots(&view.snapshot)
}

fn validate_captured_view(tx: &Transaction) -> Result<(), ReadError> {
    if !captured_view_is_valid(&tx.view) {
        return Err(ReadError::SnapshotAuthentication);
    }
    Ok(())
}

fn validate_snapshot_record(snapshot: &Snapshot) -> Result<(), ReadError> {
    if valid_snapshot_roots(snapshot) {
        Ok(())
    } else {
        Err(ReadError::CorruptSnapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_opening_read_and_one_publication() {
        let mut store = Store::new();
        let mut tx = store.begin_transaction().unwrap();
        assert_eq!(store.counters.begin_reads, 1);
        tx.stage("title", Cell::Value(Some("one".to_owned())));
        let plan = store.prepare(&tx).unwrap().unwrap();
        assert_eq!(store.counters.plans, 1);
        assert_eq!(store.counters.prepares, 1);
        store.commit(plan).unwrap();
        assert_eq!(store.counters.commits, 1);
        assert_eq!(store.counters.writes, 1);
        assert_eq!(store.counters.begin_reads, 1);
    }

    #[test]
    fn branch_global_null_tombstone_and_generation_scope() {
        let mut store = Store::new();
        store.seed_global_row("same", Cell::Value(Some("global".to_owned())));
        let mut tx = store.begin_transaction().unwrap();
        assert_eq!(
            tx.read(&store, "same").unwrap(),
            Some(Cell::Value(Some("global".to_owned())))
        );
        tx.stage_null("same");
        assert_eq!(tx.read(&store, "same").unwrap(), Some(Cell::Value(None)));
        tx.stage_tombstone("same");
        assert_eq!(tx.read(&store, "same").unwrap(), None);
        let parent_generation = store.history.get(&0).unwrap().generation;
        let plan = store.prepare(&tx).unwrap().unwrap();
        assert_eq!(plan.generation, parent_generation + 1);
        store.commit(plan).unwrap();
        assert_eq!(store.history.get(&1).unwrap().branch_id, "main");
    }

    #[test]
    fn savepoint_rollback_restores_staged_overlay() {
        let mut store = Store::new();
        let mut tx = store.begin_transaction().unwrap();
        tx.stage("a", Cell::Value(Some("before".to_owned())));
        tx.savepoint();
        tx.stage("a", Cell::Value(Some("after".to_owned())));
        tx.stage("b", Cell::Value(Some("new".to_owned())));
        tx.rollback_to_savepoint();
        assert_eq!(
            tx.read(&store, "a").unwrap(),
            Some(Cell::Value(Some("before".to_owned())))
        );
        assert_eq!(tx.read(&store, "b").unwrap(), None);
    }

    #[test]
    fn no_op_and_unsupported_classify_before_plan() {
        let mut store = Store::new();
        let tx = store.begin_transaction().unwrap();
        assert_eq!(store.classify_intent(&tx), Intent::Noop);
        assert!(store.prepare(&tx).unwrap().is_none());
        let before_plans = store.counters.plans;
        let before_prepares = store.counters.prepares;
        let before_commits = store.counters.commits;
        let before_writes = store.counters.writes;
        let before_epoch = store.epoch;
        let mut unsupported = store.begin_transaction().unwrap();
        unsupported.set_unsupported();
        assert_eq!(store.classify_intent(&unsupported), Intent::Unsupported);
        assert!(store.prepare(&unsupported).unwrap().is_none());
        assert_eq!(store.counters.plans, before_plans);
        assert_eq!(store.counters.prepares, before_prepares);
        assert_eq!(store.counters.commits, before_commits);
        assert_eq!(store.counters.writes, before_writes);
        assert_eq!(store.epoch, before_epoch);
    }

    #[test]
    fn stale_epoch_is_zero_write_and_no_partial_publication() {
        let mut store = Store::new();
        let mut tx = store.begin_transaction().unwrap();
        tx.stage("a", Cell::Value(Some("v".to_owned())));
        store.external_epoch_advance();
        let before = store.clone();
        assert_eq!(store.prepare(&tx), Err(CommitError::StaleView));
        assert_eq!(store.counters.writes, before.counters.writes);
        assert_eq!(store.history, before.history);
    }

    #[test]
    fn captured_view_is_stable_after_external_mutation_and_cas_is_stale() {
        let mut store = Store::new();
        store.external_publish_row("a", Cell::Value(Some("old".to_owned())));
        let mut reader = store.begin_transaction().unwrap();
        let before_reads = store.counters.begin_reads;
        store.external_publish_row("a", Cell::Value(Some("new".to_owned())));
        assert_eq!(
            reader.read(&store, "a").unwrap(),
            Some(Cell::Value(Some("old".to_owned())))
        );
        reader.stage("a", Cell::Value(Some("attempt".to_owned())));
        assert_eq!(store.prepare(&reader), Err(CommitError::StaleView));
        assert_eq!(store.counters.begin_reads, before_reads);
        assert_eq!(store.counters.writes, 2);
    }

    #[test]
    fn reconciliation_distinguishes_same_owner_stale_from_unrelated_owner() {
        let mut store = Store::new();
        let same_owner = store.begin_transaction().unwrap();
        store.external_epoch_advance();
        assert_eq!(
            store.reconcile_owner(&same_owner, "main"),
            Err(CommitError::StaleView)
        );

        let unrelated_owner = store.begin_transaction().unwrap();
        store.external_unrelated_owner_advance();
        assert_eq!(
            store.reconcile_owner(&unrelated_owner, "unrelated"),
            Ok(Reconciliation::UnrelatedOwner)
        );
    }

    #[test]
    fn idempotency_replay_is_not_second_commit() {
        let mut store = Store::new();
        let mut tx = store.begin_transaction().unwrap();
        tx.stage("a", Cell::Value(Some("v".to_owned())));
        tx.set_idempotency("request-1", 77);
        let plan = store.prepare(&tx).unwrap().unwrap();
        let first = store.commit(plan).unwrap();
        let mut replay = store.begin_transaction().unwrap();
        replay.stage("a", Cell::Value(Some("v".to_owned())));
        replay.set_idempotency("request-1", 77);
        let replay_plan = store.prepare(&replay).unwrap().unwrap();
        let commits = store.counters.commits;
        assert_eq!(store.commit(replay_plan).unwrap(), first);
        assert_eq!(store.counters.commits, commits);
    }

    #[test]
    fn undo_redo_preserves_history_identity_and_checkpoint_floor() {
        let mut store = Store::new();
        let mut first = store.begin_transaction().unwrap();
        first.stage("a", Cell::Value(Some("one".to_owned())));
        let first_plan = store.prepare(&first).unwrap().unwrap();
        let first_commit = store.commit(first_plan).unwrap();
        let mut checkpoint = store.begin_transaction().unwrap();
        checkpoint.set_checkpoint(first_commit);
        let checkpoint_plan = store.prepare(&checkpoint).unwrap().unwrap();
        store.commit(checkpoint_plan).unwrap();
        let mut undo = store.begin_transaction().unwrap();
        undo.set_undo(0);
        let undo_plan = store.prepare(&undo).unwrap().unwrap();
        let undo_commit = store.commit(undo_plan).unwrap();
        assert_eq!(
            store.history.get(&first_commit).unwrap().commit,
            first_commit
        );
        assert_eq!(store.checkpoint_floor, first_commit);
        assert_eq!(store.checkpoint_target, Some(first_commit));
        assert_eq!(store.redo_target, Some(2));
        assert_eq!(store.history.get(&undo_commit).unwrap().parent, Some(2));
    }

    #[test]
    fn corrupt_missing_wrong_kind_roots_fail_closed() {
        let mut missing = Store::new();
        missing.global_selector.clear();
        assert_eq!(missing.begin_view(), Err(OpenError::MissingSelector));
        let mut wrong_kind = Store::new();
        wrong_kind.corrupt_global_selector();
        assert_eq!(
            wrong_kind.begin_view(),
            Err(OpenError::SelectorAuthentication)
        );
        let mut missing_root = Store::new();
        missing_root.corrupt_state_root();
        assert_eq!(
            missing_root.begin_view(),
            Err(OpenError::RootAuthentication)
        );
        let mut absent_root = Store::new();
        absent_root.state_root.clear();
        assert_eq!(absent_root.begin_view(), Err(OpenError::MissingRoot));

        let mut tampered_view = Store::new();
        let mut tx = tampered_view.begin_transaction().unwrap();
        tx.view.snapshot_bytes.push(0);
        assert_eq!(
            tx.read(&tampered_view, "missing"),
            Err(ReadError::SnapshotAuthentication)
        );
    }

    #[test]
    fn cold_reopen_preserves_roots_and_rows() {
        let mut store = Store::new();
        let mut tx = store.begin_transaction().unwrap();
        tx.stage("a", Cell::Value(Some("v".to_owned())));
        let plan = store.prepare(&tx).unwrap().unwrap();
        store.commit(plan).unwrap();
        let mut reopened = store.reopen();
        let view = reopened.begin_view().unwrap();
        let tx = Transaction {
            view,
            staged: BTreeMap::new(),
            savepoints: Vec::new(),
            intent: Intent::Noop,
            idempotency_key: None,
            idempotency_digest: 0,
            expected_change: None,
        };
        assert!(!tx.view.view_id.is_empty());
        assert_eq!(
            tx.read(&reopened, "a").unwrap(),
            Some(Cell::Value(Some("v".to_owned())))
        );
        assert_eq!(reopened.state_root, store.state_root);
        assert_eq!(reopened.catalog_root, store.catalog_root);
        assert_eq!(
            reopened.history.get(&1).unwrap().state_root,
            store.history.get(&1).unwrap().state_root
        );
    }

    #[test]
    fn transition_expected_change_mismatch_is_stale() {
        let mut store = Store::new();
        store.seed_global_row("a", Cell::Value(Some("source".to_owned())));
        let mut tx = store.begin_transaction().unwrap();
        assert_eq!(
            tx.apply_transition(&store, 0, 0, "a", 999),
            Err(CommitError::ExpectedChangeMismatch)
        );
        assert!(tx.staged.is_empty());
    }

    #[test]
    fn missing_transition_commits_and_corrupt_roots_fail_closed() {
        let mut store = Store::new();
        let mut tx = store.begin_transaction().unwrap();
        assert_eq!(
            tx.apply_transition(&store, 99, 0, "a", 0),
            Err(CommitError::MissingCommit)
        );
        assert_eq!(
            tx.apply_transition(&store, 0, 99, "a", 0),
            Err(CommitError::MissingCommit)
        );
        assert!(tx.staged.is_empty());

        store.corrupt_history_root(0);
        assert_eq!(
            store.read_snapshot(&tx, 0, "a"),
            Err(ReadError::CorruptSnapshot)
        );
        assert_eq!(
            tx.apply_transition(&store, 0, 0, "a", 0),
            Err(CommitError::CorruptHistory)
        );
    }

    #[test]
    fn partial_publication_fault_rolls_back_all() {
        let mut store = Store::new();
        let mut tx = store.begin_transaction().unwrap();
        tx.stage("a", Cell::Value(Some("v".to_owned())));
        let plan = store.prepare(&tx).unwrap().unwrap();
        let before = store.clone();
        store.set_fault(Fault::AfterObjectStage);
        assert_eq!(store.commit(plan), Err(CommitError::FaultAfterObjectStage));
        let mut after = store.clone();
        after.fault = before.fault;
        assert_eq!(after.global_selector, before.global_selector);
        assert_eq!(after.branch_selector, before.branch_selector);
        assert_eq!(after.epoch, before.epoch);
        assert_eq!(after.history, before.history);
        assert_eq!(after.global_rows, before.global_rows);
        assert_eq!(after.branch_rows, before.branch_rows);
        assert_eq!(after.counters.commits, before.counters.commits);
        assert_eq!(after.counters.writes, before.counters.writes);
    }

    #[test]
    fn one_view_for_historical_and_current_reads() {
        let mut store = Store::new();
        let mut tx = store.begin_transaction().unwrap();
        tx.stage("a", Cell::Value(Some("old".to_owned())));
        let plan = store.prepare(&tx).unwrap().unwrap();
        store.commit(plan).unwrap();
        let mut reader = store.begin_transaction().unwrap();
        let before_reads = store.counters.begin_reads;
        assert_eq!(
            store.read_snapshot(&reader, 1, "a").unwrap(),
            Some(Cell::Value(Some("old".to_owned())))
        );
        reader.stage("a", Cell::Value(Some("new".to_owned())));
        assert_eq!(
            reader.read(&store, "a").unwrap(),
            Some(Cell::Value(Some("new".to_owned())))
        );
        assert_eq!(store.counters.begin_reads, before_reads);
    }

    #[test]
    fn expected_change_is_checked_before_publication() {
        let mut store = Store::new();
        let mut tx = store.begin_transaction().unwrap();
        tx.stage("a", Cell::Value(Some("v".to_owned())));
        tx.expect_change("a", 1234);
        assert_eq!(store.prepare(&tx), Err(CommitError::ExpectedChangeMismatch));
        assert_eq!(store.counters.plans, 0);
        assert_eq!(store.counters.writes, 0);
    }
}
