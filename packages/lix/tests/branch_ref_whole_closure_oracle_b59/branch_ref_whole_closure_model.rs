use std::collections::{BTreeMap, BTreeSet};

const REPOSITORY_OWNER: &str = "repository:fixture";
const SELECTOR_CATALOG_ROOT: &str = "catalog:root";
const GLOBAL_SELECTOR_KEY: &str = "selector:global";

fn authenticated_fingerprint(bytes: &str) -> String {
    // The model intentionally uses a deterministic, dependency-free tag. The
    // important property for this oracle is that every authenticated field is
    // in the canonical bytes and that same-size substitutions do not compare
    // equal; production authentication remains owned by ForkTree.
    let mut left = 0xcbf29ce484222325_u64;
    let mut right = 0x84222325cbf29ce4_u64;
    for (index, byte) in bytes.bytes().enumerate() {
        left ^= u64::from(byte);
        left = left.wrapping_mul(0x100000001b3_u64);
        right ^= u64::from(byte).wrapping_add(index as u64).rotate_left(1);
        right = right.wrapping_mul(0x100000001b3_u64);
    }
    format!("{left:016x}{right:016x}:{}", bytes.len())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GlobalSelector {
    selector_key: String,
    root: String,
    epoch: u64,
    generation: u64,
    owner: String,
    selector_bytes: String,
    auth_fingerprint: String,
}

impl GlobalSelector {
    fn new(root: &str, epoch: u64, generation: u64) -> Self {
        let owner = REPOSITORY_OWNER.to_owned();
        let selector_key = GLOBAL_SELECTOR_KEY.to_owned();
        let selector_bytes = format!(
            "global|key={selector_key}|owner={owner}|root={root}|epoch={epoch}|generation={generation}"
        );
        let auth_fingerprint = authenticated_fingerprint(&selector_bytes);
        Self {
            selector_key,
            root: root.to_owned(),
            epoch,
            generation,
            owner,
            selector_bytes,
            auth_fingerprint,
        }
    }

    fn is_authenticated(&self) -> bool {
        self.selector_bytes
            == format!(
                "global|key={}|owner={}|root={}|epoch={}|generation={}",
                self.selector_key, self.owner, self.root, self.epoch, self.generation
            )
            && self.auth_fingerprint == authenticated_fingerprint(&self.selector_bytes)
            && self.owner == REPOSITORY_OWNER
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BranchSelector {
    selector_key: String,
    branch: String,
    snapshot: String,
    generation: u64,
    owner: String,
    catalog_root: String,
    selector_bytes: String,
    auth_fingerprint: String,
}

impl BranchSelector {
    fn new(branch: &str, snapshot: &str, generation: u64) -> Self {
        let owner = format!("branch:{branch}");
        let selector_key = format!("selector:branch:{branch}");
        let catalog_root = SELECTOR_CATALOG_ROOT.to_owned();
        let selector_bytes = format!(
            "branch|key={selector_key}|owner={owner}|branch={branch}|catalog={catalog_root}|root={snapshot}|generation={generation}"
        );
        let auth_fingerprint = authenticated_fingerprint(&selector_bytes);
        Self {
            selector_key,
            branch: branch.to_owned(),
            snapshot: snapshot.to_owned(),
            generation,
            owner,
            catalog_root,
            selector_bytes,
            auth_fingerprint,
        }
    }

    fn is_authenticated(&self) -> bool {
        self.selector_bytes
            == format!(
                "branch|key={}|owner={}|branch={}|catalog={}|root={}|generation={}",
                self.selector_key,
                self.owner,
                self.branch,
                self.catalog_root,
                self.snapshot,
                self.generation
            )
            && self.auth_fingerprint == authenticated_fingerprint(&self.selector_bytes)
            && self.owner == format!("branch:{}", self.branch)
            && self.selector_key == format!("selector:branch:{}", self.branch)
            && self.catalog_root == SELECTOR_CATALOG_ROOT
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CatalogRoot {
    object_id: String,
    kind: String,
    back_edge: String,
    auth_fingerprint: String,
}

impl CatalogRoot {
    fn new(object_id: &str) -> Self {
        let kind = "selector_catalog".to_owned();
        let back_edge = GLOBAL_SELECTOR_KEY.to_owned();
        let auth_fingerprint = authenticated_fingerprint(&format!(
            "catalog|id={object_id}|kind={kind}|back_edge={back_edge}"
        ));
        Self {
            object_id: object_id.to_owned(),
            kind,
            back_edge,
            auth_fingerprint,
        }
    }

    fn is_authenticated(&self) -> bool {
        let bytes = format!(
            "catalog|id={}|kind={}|back_edge={}",
            self.object_id, self.kind, self.back_edge
        );
        self.auth_fingerprint == authenticated_fingerprint(&bytes)
            && self.object_id == SELECTOR_CATALOG_ROOT
            && self.kind == "selector_catalog"
            && self.back_edge == GLOBAL_SELECTOR_KEY
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SelectorFingerprint {
    global_selector_key: String,
    branch_selector_key: String,
    global_root: String,
    branch_root: String,
    global_epoch: u64,
    global_generation: u64,
    branch_generation: u64,
    global_selector_bytes: String,
    branch_selector_bytes: String,
    global_owner: String,
    branch_owner: String,
    catalog_root: String,
    global_auth_fingerprint: String,
    branch_auth_fingerprint: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CoherentView {
    global: GlobalSelector,
    branch: BranchSelector,
    read_id: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RetainedRead {
    branch: String,
    branch_selector_key: String,
    branch_snapshot: String,
    global_selector_key: String,
    global_root: String,
    global_epoch: u64,
    global_generation: u64,
}

impl RetainedRead {
    fn from_view(global: &GlobalSelector, branch: &BranchSelector) -> Self {
        Self {
            branch: branch.branch.clone(),
            branch_selector_key: branch.selector_key.clone(),
            branch_snapshot: branch.snapshot.clone(),
            global_selector_key: global.selector_key.clone(),
            global_root: global.root.clone(),
            global_epoch: global.epoch,
            global_generation: global.generation,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StateFingerprint {
    active_branch: Option<String>,
    histories: BTreeMap<String, Vec<String>>,
    objects: BTreeSet<String>,
    live_objects: BTreeSet<String>,
    allocations: BTreeSet<String>,
    catalog_objects: BTreeMap<String, CatalogRoot>,
    retained_views: BTreeMap<u64, RetainedRead>,
    selector_fingerprints: BTreeMap<String, SelectorFingerprint>,
    global_epoch: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicationKind {
    Create,
    Advance,
    Delete,
    Retire,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicationAuthority {
    Selector,
    DerivedBranchRef,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreparedPublication {
    expected_global: GlobalSelector,
    expected_branch: Option<BranchSelector>,
    next_global: GlobalSelector,
    next_branch: Option<BranchSelector>,
    staged_objects: BTreeSet<String>,
    next_active_branch: Option<String>,
    owner: String,
    read_id: u64,
    view_count: u8,
    commit_count: u8,
    selector_cas_count: u8,
    authority: PublicationAuthority,
    kind: PublicationKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Failure {
    InvalidBranchIdentity,
    StaleSelector,
    UnrelatedOwner,
    CorruptSelector,
    MissingRoot,
    Cycle,
    DualAuthority,
    InvalidGlobalSequence,
    RetiredBranch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum OperationResult {
    Published,
    NoOp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Repository {
    global: Option<GlobalSelector>,
    branches: BTreeMap<String, BranchSelector>,
    histories: BTreeMap<String, Vec<String>>,
    objects: BTreeSet<String>,
    live_objects: BTreeSet<String>,
    allocations: BTreeSet<String>,
    catalog_objects: BTreeMap<String, CatalogRoot>,
    derived_branch_refs: BTreeMap<String, String>,
    retired: BTreeSet<String>,
    retained_views: BTreeMap<u64, RetainedRead>,
    epoch_history: Vec<u64>,
    active_branch: Option<String>,
    cycles: BTreeSet<String>,
    views: u64,
    read_acquisitions: u64,
    writes: u64,
    commits: u64,
    next_read_id: u64,
}

impl Repository {
    fn bootstrap() -> Self {
        let mut objects = BTreeSet::new();
        objects.insert("root-global".into());
        objects.insert(SELECTOR_CATALOG_ROOT.into());
        let catalog_objects = std::iter::once((
            SELECTOR_CATALOG_ROOT.to_owned(),
            CatalogRoot::new(SELECTOR_CATALOG_ROOT),
        ))
        .collect();
        let live_objects = objects.clone();
        Self {
            global: Some(GlobalSelector::new("root-global", 1, 1)),
            branches: BTreeMap::new(),
            histories: BTreeMap::new(),
            objects,
            live_objects,
            allocations: BTreeSet::new(),
            catalog_objects,
            derived_branch_refs: BTreeMap::new(),
            retired: BTreeSet::new(),
            retained_views: BTreeMap::new(),
            epoch_history: vec![1],
            active_branch: None,
            cycles: BTreeSet::new(),
            views: 0,
            read_acquisitions: 0,
            writes: 0,
            commits: 0,
            next_read_id: 1,
        }
    }

    fn selector_fingerprint(&self, branch: &str) -> Result<SelectorFingerprint, Failure> {
        let global = self.global.as_ref().ok_or(Failure::MissingRoot)?;
        let branch_selector = self.branches.get(branch).ok_or(Failure::MissingRoot)?;
        Ok(SelectorFingerprint {
            global_selector_key: global.selector_key.clone(),
            branch_selector_key: branch_selector.selector_key.clone(),
            global_root: global.root.clone(),
            branch_root: branch_selector.snapshot.clone(),
            global_epoch: global.epoch,
            global_generation: global.generation,
            branch_generation: branch_selector.generation,
            global_selector_bytes: global.selector_bytes.clone(),
            branch_selector_bytes: branch_selector.selector_bytes.clone(),
            global_owner: global.owner.clone(),
            branch_owner: branch_selector.owner.clone(),
            catalog_root: branch_selector.catalog_root.clone(),
            global_auth_fingerprint: global.auth_fingerprint.clone(),
            branch_auth_fingerprint: branch_selector.auth_fingerprint.clone(),
        })
    }

    fn fingerprint(&self) -> StateFingerprint {
        let selector_fingerprints = self
            .branches
            .keys()
            .filter_map(|branch| {
                self.selector_fingerprint(branch)
                    .ok()
                    .map(|fingerprint| (branch.clone(), fingerprint))
            })
            .collect();
        StateFingerprint {
            active_branch: self.active_branch.clone(),
            histories: self.histories.clone(),
            objects: self.objects.clone(),
            live_objects: self.live_objects.clone(),
            allocations: self.allocations.clone(),
            catalog_objects: self.catalog_objects.clone(),
            retained_views: self.retained_views.clone(),
            selector_fingerprints,
            global_epoch: self.global.as_ref().map_or(0, |selector| selector.epoch),
        }
    }

    fn validate_global(&self, global: &GlobalSelector) -> Result<(), Failure> {
        if global.is_authenticated() {
            Ok(())
        } else {
            Err(Failure::CorruptSelector)
        }
    }

    fn validate_global_root(&self, global: &GlobalSelector) -> Result<(), Failure> {
        if !self.objects.contains(&global.root) || !self.live_objects.contains(&global.root) {
            return Err(Failure::MissingRoot);
        }
        Ok(())
    }

    fn validate_branch_selector(&self, branch: &BranchSelector) -> Result<(), Failure> {
        if branch.is_authenticated() {
            Ok(())
        } else {
            Err(Failure::CorruptSelector)
        }
    }

    fn validate_branch_snapshot(
        &self,
        expected_branch: &str,
        selector: &BranchSelector,
    ) -> Result<(), Failure> {
        if selector.branch != expected_branch {
            return Err(Failure::CorruptSelector);
        }
        if !self.objects.contains(&selector.snapshot)
            || !self.live_objects.contains(&selector.snapshot)
        {
            return Err(Failure::MissingRoot);
        }
        Ok(())
    }

    fn validate_catalog_root(&self, catalog_root: &str) -> Result<(), Failure> {
        if !self.objects.contains(catalog_root) || !self.live_objects.contains(catalog_root) {
            return Err(Failure::MissingRoot);
        }
        let catalog = self
            .catalog_objects
            .get(catalog_root)
            .ok_or(Failure::MissingRoot)?;
        if catalog.object_id != catalog_root || !catalog.is_authenticated() {
            return Err(Failure::CorruptSelector);
        }
        Ok(())
    }

    fn create_branch(&mut self, branch: &str, snapshot: &str) -> Result<OperationResult, Failure> {
        validate_branch(branch)?;
        if self.branches.contains_key(branch) || self.retired.contains(branch) {
            return Err(Failure::StaleSelector);
        }
        let previously_allocated = self.allocations.contains(snapshot);
        self.stage_object(snapshot);
        let view = match self.open_create_view(branch, snapshot) {
            Ok(view) => view,
            Err(error) => {
                if !previously_allocated {
                    self.allocations.remove(snapshot);
                }
                return Err(error);
            }
        };
        let result = self
            .prepare_create_branch(&view)
            .and_then(|prepared| self.publish(prepared));
        self.release_view(&view);
        result
    }

    fn switch_branch(&mut self, branch: &str) -> Result<OperationResult, Failure> {
        let view = self.open_view(branch)?;
        if self.active_branch.as_deref() == Some(branch) {
            self.release_view(&view);
            return Ok(OperationResult::NoOp);
        }
        // Switching the session's selected branch does not rewrite selector
        // authority; the retained view proves the target branch first.
        self.active_branch = Some(view.branch.branch.clone());
        self.release_view(&view);
        Ok(OperationResult::Published)
    }

    fn open_view(&mut self, branch: &str) -> Result<CoherentView, Failure> {
        validate_branch(branch)?;
        let global = self.global.clone().ok_or(Failure::MissingRoot)?;
        self.validate_global(&global)?;
        self.validate_global_root(&global)?;
        if self.retired.contains(branch) {
            return Err(Failure::RetiredBranch);
        }
        let selector = self
            .branches
            .get(branch)
            .cloned()
            .ok_or(Failure::MissingRoot)?;
        self.validate_branch_selector(&selector)?;
        self.validate_catalog_root(&selector.catalog_root)?;
        if self.cycles.contains(branch) {
            return Err(Failure::Cycle);
        }
        self.validate_branch_snapshot(branch, &selector)?;
        let read_id = self.next_read_id;
        self.next_read_id += 1;
        self.views += 1;
        self.read_acquisitions += 1;
        self.retained_views
            .insert(read_id, RetainedRead::from_view(&global, &selector));
        Ok(CoherentView {
            global,
            branch: selector,
            read_id,
        })
    }

    fn open_create_view(&mut self, branch: &str, snapshot: &str) -> Result<CoherentView, Failure> {
        validate_branch(branch)?;
        if self.branches.contains_key(branch) || self.retired.contains(branch) {
            return Err(Failure::StaleSelector);
        }
        let global = self.global.clone().ok_or(Failure::MissingRoot)?;
        self.validate_global(&global)?;
        self.validate_global_root(&global)?;
        self.validate_catalog_root(SELECTOR_CATALOG_ROOT)?;
        if !self.allocations.contains(snapshot) {
            return Err(Failure::MissingRoot);
        }
        let selector = BranchSelector::new(branch, snapshot, 0);
        self.validate_branch_selector(&selector)?;
        let read_id = self.next_read_id;
        self.next_read_id += 1;
        self.views += 1;
        self.read_acquisitions += 1;
        self.retained_views
            .insert(read_id, RetainedRead::from_view(&global, &selector));
        Ok(CoherentView {
            global,
            branch: selector,
            read_id,
        })
    }

    fn release_view(&mut self, view: &CoherentView) {
        self.retained_views.remove(&view.read_id);
    }

    fn stage_object(&mut self, object: &str) {
        self.allocations.insert(object.into());
    }

    fn seed_live_object(&mut self, object: &str) {
        self.objects.insert(object.into());
        self.live_objects.insert(object.into());
    }

    fn prepare_create_branch(&self, view: &CoherentView) -> Result<PreparedPublication, Failure> {
        validate_branch(&view.branch.branch)?;
        self.validate_global(&view.global)?;
        self.validate_global_root(&view.global)?;
        self.validate_branch_selector(&view.branch)?;
        self.validate_catalog_root(&view.branch.catalog_root)?;
        if self.branches.contains_key(&view.branch.branch)
            || self.retired.contains(&view.branch.branch)
        {
            return Err(Failure::StaleSelector);
        }
        let expected_read = RetainedRead::from_view(&view.global, &view.branch);
        if self.retained_views.get(&view.read_id) != Some(&expected_read)
            || !self.allocations.contains(&view.branch.snapshot)
        {
            return Err(Failure::StaleSelector);
        }
        let global = view.global.clone();
        let next_branch = BranchSelector::new(
            &view.branch.branch,
            &view.branch.snapshot,
            view.branch.generation + 1,
        );
        Ok(PreparedPublication {
            expected_global: global.clone(),
            expected_branch: None,
            next_global: GlobalSelector::new(&global.root, global.epoch + 1, global.generation + 1),
            next_branch: Some(next_branch),
            staged_objects: std::iter::once(view.branch.snapshot.clone()).collect(),
            next_active_branch: Some(view.branch.branch.clone()),
            owner: view.branch.owner.clone(),
            read_id: view.read_id,
            view_count: 1,
            commit_count: 1,
            selector_cas_count: 2,
            authority: PublicationAuthority::Selector,
            kind: PublicationKind::Create,
        })
    }

    fn prepare_branch(
        &self,
        view: &CoherentView,
        next_snapshot: &str,
    ) -> Result<PreparedPublication, Failure> {
        self.validate_global(&view.global)?;
        self.validate_global_root(&view.global)?;
        self.validate_branch_selector(&view.branch)?;
        self.validate_catalog_root(&view.branch.catalog_root)?;
        let expected_read = RetainedRead::from_view(&view.global, &view.branch);
        if self.retained_views.get(&view.read_id) != Some(&expected_read) {
            return Err(Failure::StaleSelector);
        }
        if !self.objects.contains(next_snapshot) || !self.live_objects.contains(next_snapshot) {
            return Err(Failure::MissingRoot);
        }
        let next_global = GlobalSelector::new(
            &view.global.root,
            view.global.epoch + 1,
            view.global.generation + 1,
        );
        let next_branch = BranchSelector::new(
            &view.branch.branch,
            next_snapshot,
            view.branch.generation + 1,
        );
        Ok(PreparedPublication {
            expected_global: view.global.clone(),
            expected_branch: Some(view.branch.clone()),
            next_global,
            next_branch: Some(next_branch),
            staged_objects: BTreeSet::new(),
            next_active_branch: self.active_branch.clone(),
            owner: view.branch.owner.clone(),
            read_id: view.read_id,
            view_count: 1,
            commit_count: 1,
            selector_cas_count: 2,
            authority: PublicationAuthority::Selector,
            kind: PublicationKind::Advance,
        })
    }

    fn prepare_delete(&self, view: &CoherentView, kind: PublicationKind) -> PreparedPublication {
        PreparedPublication {
            expected_global: view.global.clone(),
            expected_branch: Some(view.branch.clone()),
            next_global: GlobalSelector::new(
                &view.global.root,
                view.global.epoch + 1,
                view.global.generation + 1,
            ),
            next_branch: None,
            staged_objects: BTreeSet::new(),
            next_active_branch: self
                .active_branch
                .as_deref()
                .filter(|active| *active != view.branch.branch)
                .map(str::to_owned),
            owner: view.branch.owner.clone(),
            read_id: view.read_id,
            view_count: 1,
            commit_count: 1,
            selector_cas_count: 2,
            authority: PublicationAuthority::Selector,
            kind,
        }
    }

    fn retire_branch(&mut self, branch: &str) -> Result<OperationResult, Failure> {
        let view = self.open_view(branch)?;
        let prepared = self.prepare_delete(&view, PublicationKind::Retire);
        let result = self.publish(prepared);
        self.release_view(&view);
        result
    }

    fn publish(&mut self, prepared: PreparedPublication) -> Result<OperationResult, Failure> {
        if prepared.authority != PublicationAuthority::Selector
            || prepared.view_count != 1
            || prepared.commit_count != 1
            || prepared.selector_cas_count != 2
        {
            return Err(Failure::DualAuthority);
        }
        match prepared.kind {
            PublicationKind::Create if prepared.expected_branch.is_some() => {
                return Err(Failure::DualAuthority)
            }
            PublicationKind::Advance if prepared.expected_branch.is_none() => {
                return Err(Failure::DualAuthority)
            }
            PublicationKind::Delete | PublicationKind::Retire if prepared.next_branch.is_some() => {
                return Err(Failure::DualAuthority)
            }
            _ => {}
        }
        let expected_read_branch = prepared
            .expected_branch
            .as_ref()
            .or(prepared.next_branch.as_ref());
        let Some(expected_read_branch) = expected_read_branch else {
            return Err(Failure::DualAuthority);
        };
        let expected_read =
            RetainedRead::from_view(&prepared.expected_global, expected_read_branch);
        if prepared.read_id == 0
            || self.retained_views.get(&prepared.read_id) != Some(&expected_read)
        {
            return Err(Failure::StaleSelector);
        }
        if prepared.kind == PublicationKind::Advance {
            let Some(next_branch) = prepared.next_branch.as_ref() else {
                return Err(Failure::DualAuthority);
            };
            if !self.objects.contains(&next_branch.snapshot)
                || !self.live_objects.contains(&next_branch.snapshot)
                || !prepared.staged_objects.is_empty()
            {
                return Err(Failure::MissingRoot);
            }
        }
        if let Some(expected_branch) = prepared.expected_branch.as_ref() {
            if prepared.owner != expected_branch.owner {
                return Err(Failure::UnrelatedOwner);
            }
        } else if prepared.owner
            != prepared
                .next_branch
                .as_ref()
                .map_or_else(String::new, |branch| branch.owner.clone())
        {
            return Err(Failure::UnrelatedOwner);
        }
        self.validate_global(&prepared.expected_global)?;
        self.validate_global(&prepared.next_global)?;
        if self.global.as_ref() != Some(&prepared.expected_global) {
            return Err(Failure::StaleSelector);
        }
        if prepared.expected_branch.is_none() {
            if let Some(next_branch) = prepared.next_branch.as_ref() {
                self.validate_branch_selector(next_branch)?;
                if self.branches.contains_key(&next_branch.branch) {
                    return Err(Failure::StaleSelector);
                }
            }
        }
        if let Some(expected_branch) = prepared.expected_branch.as_ref() {
            self.validate_branch_selector(expected_branch)?;
            if self.branches.get(&expected_branch.branch) != Some(expected_branch) {
                return Err(Failure::StaleSelector);
            }
        }

        // Compute every mutation before assigning any durable field. This is
        // the model's no-partial-commit boundary.
        let mut next_branches = self.branches.clone();
        let mut next_histories = self.histories.clone();
        let mut next_objects = self.objects.clone();
        let mut next_live = self.live_objects.clone();
        let mut next_allocations = self.allocations.clone();
        let next_global = prepared.next_global.clone();
        for object in &prepared.staged_objects {
            next_objects.insert(object.clone());
            next_live.insert(object.clone());
            next_allocations.remove(object);
        }
        if let Some(next_branch) = prepared.next_branch {
            if let Some(expected_branch) = prepared.expected_branch.as_ref() {
                // Replacing a branch selector drops the old root from the
                // selector-derived live set; retained views keep it alive
                // only through the separate read lease frontier.
                next_live.remove(&expected_branch.snapshot);
            }
            next_live.insert(next_branch.snapshot.clone());
            if prepared.expected_branch.is_none() {
                next_histories.insert(next_branch.branch.clone(), Vec::new());
            }
            next_branches.insert(next_branch.branch.clone(), next_branch);
        } else if let Some(expected_branch) = prepared.expected_branch {
            next_branches.remove(&expected_branch.branch);
            next_histories.remove(&expected_branch.branch);
            // A retired/deleted selector is no longer live. An old retained
            // view keeps the immutable root reachable when GC reconstructs
            // live roots from retained_views.
            next_live.remove(&expected_branch.snapshot);
            self.retired.insert(expected_branch.branch);
        }

        self.global = Some(next_global);
        self.branches = next_branches;
        self.histories = next_histories;
        self.objects = next_objects;
        self.live_objects = next_live;
        self.allocations = next_allocations;
        self.active_branch = prepared.next_active_branch;
        self.epoch_history.push(self.global.as_ref().unwrap().epoch);
        self.writes += 1;
        self.commits += 1;
        Ok(OperationResult::Published)
    }

    fn empty_undo(&mut self, branch: &str) -> Result<OperationResult, Failure> {
        validate_branch(branch)?;
        if self.histories.get(branch).map_or(true, Vec::is_empty) {
            return Ok(OperationResult::NoOp);
        }
        Err(Failure::DualAuthority)
    }

    fn empty_redo(&mut self, branch: &str) -> Result<OperationResult, Failure> {
        self.empty_undo(branch)
    }

    fn gc(&mut self) {
        let mut live = self.live_objects.clone();
        live.extend(
            self.retained_views
                .values()
                .map(|read| read.branch_snapshot.clone()),
        );
        self.objects.retain(|object| live.contains(object));
        self.writes += 1;
    }

    fn reopen(&self) -> Result<Self, Failure> {
        if self
            .epoch_history
            .windows(2)
            .any(|window| window[1] != window[0] + 1)
        {
            return Err(Failure::InvalidGlobalSequence);
        }
        let Some(global) = &self.global else {
            return Err(Failure::MissingRoot);
        };
        self.validate_global(global)?;
        self.validate_global_root(global)?;
        if self.epoch_history.last() != Some(&global.epoch) {
            return Err(Failure::InvalidGlobalSequence);
        }
        if !self.objects.contains(&global.root) || !self.objects.contains(SELECTOR_CATALOG_ROOT) {
            return Err(Failure::MissingRoot);
        }
        self.validate_catalog_root(SELECTOR_CATALOG_ROOT)?;
        for (branch, selector) in &self.branches {
            self.validate_branch_selector(selector)?;
            self.validate_catalog_root(&selector.catalog_root)?;
            self.validate_branch_snapshot(branch, selector)?;
            if selector.catalog_root != SELECTOR_CATALOG_ROOT {
                return Err(Failure::CorruptSelector);
            }
        }
        Ok(self.clone())
    }
}

fn validate_branch(branch: &str) -> Result<(), Failure> {
    let bytes = branch.as_bytes();
    if bytes.len() != 36
        || !matches!(bytes[8], b'-')
        || !matches!(bytes[13], b'-')
        || !matches!(bytes[18], b'-')
        || !matches!(bytes[23], b'-')
    {
        return Err(Failure::InvalidBranchIdentity);
    }
    for (index, byte) in bytes.iter().copied().enumerate() {
        if matches!(index, 8 | 13 | 18 | 23) {
            continue;
        }
        if !matches!(byte, b'0'..=b'9' | b'a'..=b'f') {
            return Err(Failure::InvalidBranchIdentity);
        }
    }
    Ok(())
}

const BRANCH_A: &str = "01920000-0000-7000-8000-0000000000a1";
const BRANCH_B: &str = "01920000-0000-7000-8000-0000000000b1";

fn repository_with_branch() -> Repository {
    let mut repository = Repository::bootstrap();
    repository.create_branch(BRANCH_A, "root-a").unwrap();
    repository
}

#[test]
fn authenticated_fingerprint_covers_every_selector_authority_field() {
    let repository = repository_with_branch();
    let fingerprint = repository.selector_fingerprint(BRANCH_A).unwrap();
    assert_eq!(fingerprint.global_selector_key, GLOBAL_SELECTOR_KEY);
    assert_eq!(
        fingerprint.branch_selector_key,
        format!("selector:branch:{BRANCH_A}")
    );
    assert_eq!(fingerprint.global_root, "root-global");
    assert_eq!(fingerprint.branch_root, "root-a");
    assert_eq!(fingerprint.global_epoch, 2);
    assert_eq!(fingerprint.global_generation, 2);
    assert_eq!(fingerprint.branch_generation, 1);
    assert!(fingerprint
        .global_selector_bytes
        .contains("root=root-global"));
    assert!(fingerprint.branch_selector_bytes.contains("root=root-a"));
    assert_eq!(fingerprint.global_owner, REPOSITORY_OWNER);
    assert_eq!(fingerprint.branch_owner, format!("branch:{BRANCH_A}"));
    assert_eq!(fingerprint.catalog_root, SELECTOR_CATALOG_ROOT);
    assert_eq!(
        fingerprint.global_auth_fingerprint,
        authenticated_fingerprint(&fingerprint.global_selector_bytes)
    );
    assert_eq!(
        fingerprint.branch_auth_fingerprint,
        authenticated_fingerprint(&fingerprint.branch_selector_bytes)
    );
}

#[test]
fn selector_bytes_bind_exact_root_catalog_generation_and_owner() {
    let mut repository = repository_with_branch();
    let original = repository.branches[BRANCH_A].clone();
    let mut forged = original.clone();
    forged.snapshot = "root-b".into();
    repository.branches.insert(BRANCH_A.into(), forged);
    assert_eq!(
        repository.open_view(BRANCH_A),
        Err(Failure::CorruptSelector)
    );

    repository
        .branches
        .insert(BRANCH_A.into(), original.clone());
    let mut wrong_catalog = original.clone();
    wrong_catalog.catalog_root = "catalog:other".into();
    repository.branches.insert(BRANCH_A.into(), wrong_catalog);
    assert_eq!(
        repository.open_view(BRANCH_A),
        Err(Failure::CorruptSelector)
    );
}

#[test]
fn malformed_selector_authentication_is_corrupt_selector() {
    let mut malformed_global = repository_with_branch();
    malformed_global
        .global
        .as_mut()
        .unwrap()
        .auth_fingerprint
        .push('x');
    assert_eq!(
        malformed_global.open_view(BRANCH_A),
        Err(Failure::CorruptSelector)
    );

    let mut malformed_branch = repository_with_branch();
    malformed_branch
        .branches
        .get_mut(BRANCH_A)
        .unwrap()
        .auth_fingerprint
        .push('x');
    assert_eq!(
        malformed_branch.open_view(BRANCH_A),
        Err(Failure::CorruptSelector)
    );
}

#[test]
fn fingerprint_covers_state_and_in_flight_allocations() {
    let mut repository = repository_with_branch();
    repository.stage_object("staged-a");
    let fingerprint = repository.fingerprint();
    assert_eq!(fingerprint.active_branch.as_deref(), Some(BRANCH_A));
    assert!(fingerprint.histories.contains_key(BRANCH_A));
    assert!(fingerprint.objects.contains("root-global"));
    assert!(fingerprint.live_objects.contains("root-a"));
    assert!(fingerprint.allocations.contains("staged-a"));
    assert!(fingerprint.selector_fingerprints.contains_key(BRANCH_A));
}

#[test]
fn one_retained_view_and_one_prepared_publication_one_commit() {
    let mut repository = repository_with_branch();
    let baseline_views = repository.views;
    let baseline_reads = repository.read_acquisitions;
    repository.seed_live_object("root-next");
    let view = repository.open_view(BRANCH_A).unwrap();
    let prepared = repository.prepare_branch(&view, "root-next").unwrap();
    assert_eq!(prepared.read_id, view.read_id);
    assert_eq!(repository.publish(prepared), Ok(OperationResult::Published));
    assert_eq!(repository.views, baseline_views + 1); // this retained read
    assert_eq!(repository.read_acquisitions, baseline_reads + 1);
    assert_eq!(repository.writes, 2); // create plus advance
    assert_eq!(repository.commits, 2);
    assert!(repository.allocations.is_empty());
    repository.release_view(&view);
}

#[test]
fn create_publication_requires_exact_retained_read_ownership() {
    let mut repository = Repository::bootstrap();
    repository.stage_object("root-a");
    let view = repository.open_create_view(BRANCH_A, "root-a").unwrap();
    let prepared = repository.prepare_create_branch(&view).unwrap();
    assert_ne!(prepared.read_id, 0);

    let mut zero_read = prepared.clone();
    zero_read.read_id = 0;
    assert_eq!(repository.publish(zero_read), Err(Failure::StaleSelector));

    repository.release_view(&view);
    let writes = repository.writes;
    assert_eq!(repository.publish(prepared), Err(Failure::StaleSelector));
    assert_eq!(repository.writes, writes);
    assert!(repository.branches.is_empty());
}

#[test]
fn retained_read_binds_branch_snapshot_and_global_selector() {
    let mut repository = Repository::bootstrap();
    repository.create_branch(BRANCH_A, "root-shared").unwrap();
    repository.create_branch(BRANCH_B, "root-shared").unwrap();
    repository.seed_live_object("root-next");

    let view_a = repository.open_view(BRANCH_A).unwrap();
    let view_b = repository.open_view(BRANCH_B).unwrap();
    let prepared = repository.prepare_branch(&view_a, "root-next").unwrap();
    let writes = repository.writes;

    let mut other_branch_read = prepared.clone();
    other_branch_read.read_id = view_b.read_id;
    assert_eq!(
        repository.publish(other_branch_read),
        Err(Failure::StaleSelector)
    );
    assert_eq!(repository.writes, writes);

    let mut wrong_global_root = prepared.clone();
    wrong_global_root.expected_global = GlobalSelector::new(
        "root-other",
        prepared.expected_global.epoch,
        prepared.expected_global.generation,
    );
    assert_eq!(
        repository.publish(wrong_global_root),
        Err(Failure::StaleSelector)
    );
    assert_eq!(repository.writes, writes);

    let mut wrong_branch_root = prepared;
    wrong_branch_root.expected_branch = Some(BranchSelector::new(
        BRANCH_A,
        "root-other",
        view_a.branch.generation,
    ));
    assert_eq!(
        repository.publish(wrong_branch_root),
        Err(Failure::StaleSelector)
    );
    assert_eq!(repository.writes, writes);
}

#[test]
fn same_owner_stale_cas_and_unrelated_owner_are_distinct_failures() {
    let mut stale = repository_with_branch();
    stale.seed_live_object("root-next");
    let stale_view = stale.open_view(BRANCH_A).unwrap();
    let stale_prepared = stale.prepare_branch(&stale_view, "root-next").unwrap();
    stale.branches.get_mut(BRANCH_A).unwrap().generation += 1;
    let stale_writes = stale.writes;
    assert_eq!(stale.publish(stale_prepared), Err(Failure::StaleSelector));
    assert_eq!(stale.writes, stale_writes);

    let mut unrelated = repository_with_branch();
    unrelated.seed_live_object("root-next");
    let unrelated_view = unrelated.open_view(BRANCH_A).unwrap();
    let mut unrelated_prepared = unrelated
        .prepare_branch(&unrelated_view, "root-next")
        .unwrap();
    unrelated_prepared.owner = format!("branch:{BRANCH_B}");
    let unrelated_writes = unrelated.writes;
    assert_eq!(
        unrelated.publish(unrelated_prepared),
        Err(Failure::UnrelatedOwner)
    );
    assert_eq!(unrelated.writes, unrelated_writes);
}

#[test]
fn second_authority_negative_cannot_publish_or_change_selected_root() {
    let mut repository = repository_with_branch();
    repository
        .derived_branch_refs
        .insert(BRANCH_A.into(), "fake-root".into());
    let view = repository.open_view(BRANCH_A).unwrap();
    assert_eq!(view.branch.snapshot, "root-a");
    let mut prepared = repository.prepare_delete(&view, PublicationKind::Delete);
    prepared.authority = PublicationAuthority::DerivedBranchRef;
    let before = repository.fingerprint();
    let writes = repository.writes;
    assert_eq!(repository.publish(prepared), Err(Failure::DualAuthority));
    assert_eq!(repository.fingerprint(), before);
    assert_eq!(repository.writes, writes);
}

#[test]
fn malformed_identity_missing_root_and_cycle_fail_closed() {
    let mut repository = Repository::bootstrap();
    assert_eq!(
        repository.open_view("not-a-branch"),
        Err(Failure::InvalidBranchIdentity)
    );
    repository.branches.insert(
        BRANCH_A.into(),
        BranchSelector::new(BRANCH_A, "missing-root", 1),
    );
    assert_eq!(repository.open_view(BRANCH_A), Err(Failure::MissingRoot));
    let mut mismatched_identity = repository_with_branch();
    mismatched_identity
        .branches
        .insert(BRANCH_A.into(), BranchSelector::new(BRANCH_B, "root-a", 1));
    assert_eq!(
        mismatched_identity.open_view(BRANCH_A),
        Err(Failure::CorruptSelector)
    );
    repository.objects.insert("root-cycle".into());
    repository.live_objects.insert("root-cycle".into());
    repository.branches.insert(
        BRANCH_B.into(),
        BranchSelector::new(BRANCH_B, "root-cycle", 1),
    );
    repository.cycles.insert(BRANCH_B.into());
    assert_eq!(repository.open_view(BRANCH_B), Err(Failure::Cycle));
    assert_eq!(repository.writes, 0); // direct corruption setup is not a publication
}

#[test]
fn open_view_rejects_missing_or_substituted_catalog_object() {
    let mut missing_physical = repository_with_branch();
    missing_physical.objects.remove(SELECTOR_CATALOG_ROOT);
    missing_physical.live_objects.remove(SELECTOR_CATALOG_ROOT);
    assert_eq!(
        missing_physical.open_view(BRANCH_A),
        Err(Failure::MissingRoot)
    );

    let mut missing_catalog_record = repository_with_branch();
    missing_catalog_record
        .catalog_objects
        .remove(SELECTOR_CATALOG_ROOT);
    assert_eq!(
        missing_catalog_record.open_view(BRANCH_A),
        Err(Failure::MissingRoot)
    );

    let mut wrong_kind = repository_with_branch();
    wrong_kind
        .catalog_objects
        .get_mut(SELECTOR_CATALOG_ROOT)
        .unwrap()
        .kind = "branch_snapshot".into();
    assert_eq!(
        wrong_kind.open_view(BRANCH_A),
        Err(Failure::CorruptSelector)
    );

    let mut wrong_object_id = repository_with_branch();
    wrong_object_id
        .catalog_objects
        .get_mut(SELECTOR_CATALOG_ROOT)
        .unwrap()
        .object_id = "catalog:other".into();
    assert_eq!(
        wrong_object_id.open_view(BRANCH_A),
        Err(Failure::CorruptSelector)
    );

    let mut wrong_back_edge = repository_with_branch();
    wrong_back_edge
        .catalog_objects
        .get_mut(SELECTOR_CATALOG_ROOT)
        .unwrap()
        .back_edge = "selector:other".into();
    assert_eq!(
        wrong_back_edge.open_view(BRANCH_A),
        Err(Failure::CorruptSelector)
    );
}

#[test]
fn open_view_rejects_missing_or_dead_global_root() {
    let mut missing = repository_with_branch();
    missing.objects.remove("root-global");
    missing.live_objects.remove("root-global");
    assert_eq!(missing.open_view(BRANCH_A), Err(Failure::MissingRoot));

    let mut dead = repository_with_branch();
    dead.live_objects.remove("root-global");
    assert_eq!(dead.open_view(BRANCH_A), Err(Failure::MissingRoot));
}

#[test]
fn non_live_objects_cannot_be_prepared_or_resurrected() {
    let mut repository = repository_with_branch();
    repository.objects.insert("root-dead".into());
    repository.stage_object("root-staged");
    let view = repository.open_view(BRANCH_A).unwrap();
    assert_eq!(
        repository.prepare_branch(&view, "root-dead"),
        Err(Failure::MissingRoot)
    );
    assert_eq!(
        repository.prepare_branch(&view, "root-staged"),
        Err(Failure::MissingRoot)
    );

    let mut prepared = repository.prepare_branch(&view, "root-a").unwrap();
    prepared.next_branch = Some(BranchSelector::new(BRANCH_A, "root-dead", 2));
    prepared.staged_objects.insert("root-dead".into());
    let before = repository.fingerprint();
    let writes = repository.writes;
    assert_eq!(repository.publish(prepared), Err(Failure::MissingRoot));
    assert_eq!(repository.fingerprint(), before);
    assert_eq!(repository.writes, writes);
    assert!(!repository.live_objects.contains("root-dead"));
    repository.release_view(&view);
}

#[test]
fn empty_undo_redo_are_true_no_ops() {
    let mut repository = repository_with_branch();
    let before = repository.fingerprint();
    assert_eq!(repository.empty_undo(BRANCH_A), Ok(OperationResult::NoOp));
    assert_eq!(repository.empty_redo(BRANCH_A), Ok(OperationResult::NoOp));
    assert_eq!(repository.fingerprint(), before);
    assert_eq!(repository.writes, 1);
    assert_eq!(repository.commits, 1);
}

#[test]
fn create_switch_delete_retire_gc_and_cold_reopen_are_one_authority() {
    let mut repository = Repository::bootstrap();
    assert_eq!(
        repository.create_branch(BRANCH_A, "root-a"),
        Ok(OperationResult::Published)
    );
    assert_eq!(
        repository.create_branch(BRANCH_B, "root-b"),
        Ok(OperationResult::Published)
    );
    assert_eq!(
        repository.switch_branch(BRANCH_A),
        Ok(OperationResult::Published)
    );
    repository.seed_live_object("root-a2");
    let old_view = repository.open_view(BRANCH_A).unwrap();
    let advance = repository.prepare_branch(&old_view, "root-a2").unwrap();
    repository.publish(advance).unwrap();
    assert_eq!(repository.active_branch.as_deref(), Some(BRANCH_A));
    assert_eq!(
        repository.retire_branch(BRANCH_B),
        Ok(OperationResult::Published)
    );
    assert!(repository.retired.contains(BRANCH_B));
    assert_eq!(repository.open_view(BRANCH_B), Err(Failure::RetiredBranch));
    let reopened = repository.reopen().unwrap();
    assert_eq!(reopened.fingerprint(), repository.fingerprint());
    repository.release_view(&old_view);
    repository.gc();
    assert!(repository.objects.contains("root-global"));
    assert!(repository.objects.contains("root-a2"));
}

#[test]
fn delete_and_gc_reclaim_final_branch_reference_only() {
    let mut repository = repository_with_branch();
    let view = repository.open_view(BRANCH_A).unwrap();
    let before = repository.fingerprint();
    let prepared = repository.prepare_delete(&view, PublicationKind::Delete);
    assert_eq!(repository.publish(prepared), Ok(OperationResult::Published));
    assert_ne!(repository.fingerprint(), before);
    repository.release_view(&view);
    repository.gc();
    assert!(!repository.objects.contains("root-a"));
    assert!(repository.objects.contains("root-global"));
    assert!(repository.objects.contains(SELECTOR_CATALOG_ROOT));
}

#[test]
fn old_view_survives_rotation_and_reopen_until_released() {
    let mut repository = repository_with_branch();
    repository.seed_live_object("root-next");
    let old_view = repository.open_view(BRANCH_A).unwrap();
    let prepared = repository.prepare_branch(&old_view, "root-next").unwrap();
    repository.publish(prepared).unwrap();
    repository.gc();
    assert!(repository.objects.contains("root-a"));
    assert_eq!(old_view.branch.snapshot, "root-a");
    let reopened = repository.reopen().unwrap();
    assert_eq!(reopened.fingerprint(), repository.fingerprint());
    repository.release_view(&old_view);
    repository.gc();
    assert!(!repository.objects.contains("root-a"));
}

#[test]
fn reopen_requires_live_global_branch_and_catalog_closure() {
    let mut dead_global = repository_with_branch();
    dead_global.live_objects.remove("root-global");
    assert_eq!(dead_global.reopen(), Err(Failure::MissingRoot));

    let mut dead_branch = repository_with_branch();
    dead_branch.live_objects.remove("root-a");
    assert_eq!(dead_branch.reopen(), Err(Failure::MissingRoot));

    let mut dead_catalog = repository_with_branch();
    dead_catalog.live_objects.remove(SELECTOR_CATALOG_ROOT);
    assert_eq!(dead_catalog.reopen(), Err(Failure::MissingRoot));
}

#[test]
fn reopen_rejects_global_epoch_gap() {
    let mut repository = repository_with_branch();
    repository.epoch_history.push(99);
    repository.global.as_mut().unwrap().epoch = 99;
    assert_eq!(repository.reopen(), Err(Failure::InvalidGlobalSequence));
}

#[test]
fn invalid_multi_authority_publication_rejects_before_write() {
    let mut repository = repository_with_branch();
    let view = repository.open_view(BRANCH_A).unwrap();
    let mut prepared = repository.prepare_delete(&view, PublicationKind::Delete);
    prepared.view_count = 2;
    let writes = repository.writes;
    assert_eq!(repository.publish(prepared), Err(Failure::DualAuthority));
    assert_eq!(repository.writes, writes);
}
