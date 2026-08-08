use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

const OBJECT_SPACE: &str = "OBJECT_SPACE";
const SELECTOR_SPACE: &str = "SELECTOR_SPACE";
const OWNER: u64 = 7;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum RootClass {
    History,
    Serving,
    Checkpoint,
    BranchControl,
    Upload,
    RecoveryAlias,
    Undo,
    PluginRegistry,
    FinalReference,
}

impl RootClass {
    const ALL: [Self; 9] = [
        Self::History,
        Self::Serving,
        Self::Checkpoint,
        Self::BranchControl,
        Self::Upload,
        Self::RecoveryAlias,
        Self::Undo,
        Self::PluginRegistry,
        Self::FinalReference,
    ];
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum ObjectKind {
    History,
    Serving,
    Checkpoint,
    Payload,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Fence {
    owner: u64,
    epoch: u64,
    progress: u64,
    selector: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GraphObject {
    id: String,
    kind: ObjectKind,
    payload: String,
    parents: Vec<String>,
    generation: u64,
    auth_tag: String,
}

fn auth_tag(kind: ObjectKind, payload: &str, parents: &[String], generation: u64) -> String {
    format!("{kind:?}:{generation}:{payload}:{}", parents.join(","))
}

impl GraphObject {
    fn seal(kind: ObjectKind, payload: &str, parents: Vec<String>, generation: u64) -> Self {
        let id = auth_tag(kind, payload, &parents, generation);
        Self {
            id: id.clone(),
            kind,
            payload: payload.to_owned(),
            parents,
            generation,
            auth_tag: id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RootGraph {
    objects: BTreeMap<String, GraphObject>,
    selectors: BTreeMap<RootClass, String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Error {
    StaleFence,
    OwnerMismatch,
    ReadExpired,
    InvalidCursor,
    MissingRoot,
    MissingObject,
    Cycle,
    Malformed,
    WrongKind,
    Substitution,
    RootPinned,
    PersistedMismatch,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl RootGraph {
    fn fixture() -> Self {
        let h0 = GraphObject::seal(ObjectKind::History, "commit-0", Vec::new(), 0);
        let h1 = GraphObject::seal(ObjectKind::History, "commit-1", vec![h0.id.clone()], 1);
        let serving = GraphObject::seal(ObjectKind::Serving, "serving-1", vec![h1.id.clone()], 2);
        let checkpoint = GraphObject::seal(
            ObjectKind::Checkpoint,
            "checkpoint-1",
            vec![serving.id.clone()],
            3,
        );
        let shared = GraphObject::seal(ObjectKind::Payload, "shared-upload-branch", Vec::new(), 0);
        let final_reference =
            GraphObject::seal(ObjectKind::Payload, "final-reference", Vec::new(), 0);
        let recovery = GraphObject::seal(ObjectKind::Payload, "recovery-alias", Vec::new(), 0);
        let undo = GraphObject::seal(ObjectKind::Payload, "undo-root", Vec::new(), 0);
        let plugin = GraphObject::seal(ObjectKind::Payload, "plugin-registry", Vec::new(), 0);
        let unreachable = GraphObject::seal(ObjectKind::Payload, "unreachable-debt", Vec::new(), 0);
        let mut objects = BTreeMap::new();
        for object in [
            h0,
            h1.clone(),
            serving.clone(),
            checkpoint.clone(),
            shared.clone(),
            final_reference.clone(),
            recovery.clone(),
            undo.clone(),
            plugin.clone(),
            unreachable,
        ] {
            objects.insert(object.id.clone(), object);
        }
        let selectors = BTreeMap::from([
            (RootClass::History, h1.id),
            (RootClass::Serving, serving.id),
            (RootClass::Checkpoint, checkpoint.id),
            (RootClass::BranchControl, shared.id.clone()),
            (RootClass::Upload, shared.id),
            (RootClass::RecoveryAlias, recovery.id),
            (RootClass::Undo, undo.id),
            (RootClass::PluginRegistry, plugin.id),
            (RootClass::FinalReference, final_reference.id),
        ]);
        Self { objects, selectors }
    }

    fn expected_kind(root: RootClass) -> ObjectKind {
        match root {
            RootClass::History => ObjectKind::History,
            RootClass::Serving => ObjectKind::Serving,
            RootClass::Checkpoint => ObjectKind::Checkpoint,
            RootClass::BranchControl
            | RootClass::Upload
            | RootClass::RecoveryAlias
            | RootClass::Undo
            | RootClass::PluginRegistry
            | RootClass::FinalReference => ObjectKind::Payload,
        }
    }

    fn parent_kind(kind: ObjectKind) -> Option<ObjectKind> {
        match kind {
            ObjectKind::History => Some(ObjectKind::History),
            ObjectKind::Serving => Some(ObjectKind::History),
            ObjectKind::Checkpoint => Some(ObjectKind::Serving),
            ObjectKind::Payload => None,
        }
    }

    fn authenticated_transitive_closure(&self) -> Result<BTreeSet<String>, Error> {
        if self.selectors.is_empty() {
            return Err(Error::MissingRoot);
        }
        let mut visiting = BTreeSet::new();
        let mut reachable = BTreeSet::new();
        for (root, id) in &self.selectors {
            self.walk(
                id,
                Self::expected_kind(*root),
                &mut visiting,
                &mut reachable,
            )?;
        }
        Ok(reachable)
    }

    fn validate(&self) -> Result<BTreeSet<String>, Error> {
        self.authenticated_transitive_closure()
    }

    fn authenticate_selected_root(&self, root: RootClass) -> Result<BTreeSet<String>, Error> {
        let id = self.selectors.get(&root).ok_or(Error::MissingRoot)?.clone();
        let mut visiting = BTreeSet::new();
        let mut reachable = BTreeSet::new();
        self.walk(
            &id,
            Self::expected_kind(root),
            &mut visiting,
            &mut reachable,
        )?;
        Ok(reachable)
    }

    fn walk(
        &self,
        id: &str,
        expected: ObjectKind,
        visiting: &mut BTreeSet<String>,
        reachable: &mut BTreeSet<String>,
    ) -> Result<(), Error> {
        if !visiting.insert(id.to_owned()) {
            return Err(Error::Cycle);
        }
        let object = self.objects.get(id).ok_or(Error::MissingObject)?.clone();
        if object.kind != expected {
            return Err(Error::WrongKind);
        }
        if object.payload.is_empty()
            || object
                .parents
                .windows(2)
                .any(|parents| parents[0] >= parents[1])
            || (object.kind == ObjectKind::Payload && !object.parents.is_empty())
        {
            return Err(Error::Malformed);
        }
        if let Some(parent_kind) = Self::parent_kind(object.kind) {
            for parent_id in &object.parents {
                self.walk(parent_id, parent_kind, visiting, reachable)?;
                let parent = self.objects.get(parent_id).ok_or(Error::MissingObject)?;
                if parent.generation >= object.generation {
                    return Err(Error::Malformed);
                }
            }
        } else if !object.parents.is_empty() {
            return Err(Error::Malformed);
        }
        visiting.remove(id);
        if object.id != id
            || object.auth_tag
                != auth_tag(
                    object.kind,
                    &object.payload,
                    &object.parents,
                    object.generation,
                )
        {
            return Err(Error::Substitution);
        }
        reachable.insert(id.to_owned());
        Ok(())
    }

    fn selector_digest(&self) -> String {
        self.selectors
            .iter()
            .map(|(root, id)| format!("{root:?}={id}"))
            .collect::<Vec<_>>()
            .join(";")
    }

    fn object_digest(&self) -> String {
        self.objects
            .iter()
            .map(|(id, object)| format!("{id}:{:?}:{}", object.kind, object.auth_tag))
            .collect::<Vec<_>>()
            .join(";")
    }

    fn add_queue_object(&mut self, label: &str) -> String {
        let object = GraphObject::seal(ObjectKind::Payload, label, Vec::new(), 0);
        let id = object.id.clone();
        self.objects.insert(id.clone(), object);
        id
    }

    fn remove_unselected(&mut self, id: &str, reachable: &BTreeSet<String>) -> Result<bool, Error> {
        if reachable.contains(id) {
            return Err(Error::RootPinned);
        }
        self.objects.remove(id).ok_or(Error::MissingObject)?;
        Ok(true)
    }

    fn drop_selector(&mut self, root: RootClass) {
        self.selectors.remove(&root);
    }

    fn reclaim_unreachable(&mut self) -> Result<usize, Error> {
        let reachable = self.authenticated_transitive_closure()?;
        let before = self.objects.len();
        self.objects.retain(|id, _| reachable.contains(id));
        Ok(before - self.objects.len())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct QueueEntry {
    object: String,
    blocked: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PageResult {
    advanced: bool,
    drained: bool,
    reclaimed: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PublicationPlan {
    owner: u64,
    expected: Fence,
    root: RootClass,
    object: GraphObject,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GcPlan {
    owner: u64,
    expected: Fence,
    object: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CoherentRead {
    id: u64,
    owner: u64,
    fence: Fence,
    selectors: BTreeMap<RootClass, String>,
    selector_digest: String,
    reachable: BTreeSet<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReopenedState {
    fence: Fence,
    queue_head: usize,
    queue_tail: usize,
    calls: u32,
    reclaimed: usize,
    object_count: usize,
    object_digest: String,
    selector_digest: String,
    state_tag: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Authority {
    fence: Fence,
    graph: RootGraph,
    entries: Vec<QueueEntry>,
    queue_head: usize,
    queue_tail: usize,
    debt_tokens: u32,
    calls: u32,
    reclaimed: usize,
    begin_reads: u64,
    pinned_objects: BTreeMap<String, BTreeSet<(u64, u64)>>,
}

impl Authority {
    fn new(graph: RootGraph, entries: Vec<QueueEntry>) -> Self {
        Self {
            fence: Fence {
                owner: OWNER,
                epoch: 0,
                progress: 0,
                selector: 0,
            },
            graph,
            queue_tail: entries.len(),
            entries,
            queue_head: 0,
            debt_tokens: 0,
            calls: 0,
            reclaimed: 0,
            begin_reads: 0,
            pinned_objects: BTreeMap::new(),
        }
    }

    fn check_fence(&self, owner: u64, expected: Fence) -> Result<(), Error> {
        if owner != self.fence.owner || expected.owner != self.fence.owner {
            return Err(Error::OwnerMismatch);
        }
        if expected != self.fence {
            return Err(Error::StaleFence);
        }
        Ok(())
    }

    fn prepare_publication(
        &self,
        owner: u64,
        root: RootClass,
        object: GraphObject,
    ) -> Result<PublicationPlan, Error> {
        if owner != self.fence.owner {
            return Err(Error::OwnerMismatch);
        }
        Ok(PublicationPlan {
            owner,
            expected: self.fence,
            root,
            object,
        })
    }

    fn prepare_gc(&self, owner: u64, object: String) -> Result<GcPlan, Error> {
        if owner != self.fence.owner {
            return Err(Error::OwnerMismatch);
        }
        Ok(GcPlan {
            owner,
            expected: self.fence,
            object,
        })
    }

    fn commit_publication(&mut self, plan: PublicationPlan) -> Result<(), Error> {
        self.check_fence(plan.owner, plan.expected)?;
        let object_id = plan.object.id.clone();
        self.graph.objects.insert(object_id.clone(), plan.object);
        self.graph.selectors.insert(plan.root, object_id);
        self.fence.selector += 1;
        Ok(())
    }

    fn commit_gc(&mut self, plan: GcPlan) -> Result<(), Error> {
        self.check_fence(plan.owner, plan.expected)?;
        let reachable = self.graph.authenticated_transitive_closure()?;
        if reachable.contains(&plan.object) || self.pinned_objects.contains_key(&plan.object) {
            return Err(Error::RootPinned);
        }
        self.graph.remove_unselected(&plan.object, &reachable)?;
        self.fence.epoch += 1;
        self.fence.progress += 1;
        self.reclaimed += 1;
        Ok(())
    }

    fn begin_read(&mut self, owner: u64, root: RootClass) -> Result<CoherentRead, Error> {
        if owner == 0 {
            return Err(Error::OwnerMismatch);
        }
        let reachable = self.graph.authenticate_selected_root(root)?;
        self.begin_reads += 1;
        Ok(CoherentRead {
            id: self.begin_reads,
            owner,
            fence: self.fence,
            selectors: self.graph.selectors.clone(),
            selector_digest: self.graph.selector_digest(),
            reachable,
        })
    }

    fn pin_read(&mut self, owner: u64, view_id: u64, objects: &BTreeSet<String>) {
        for object in objects {
            self.pinned_objects
                .entry(object.clone())
                .or_default()
                .insert((owner, view_id));
        }
    }

    fn unpin_read(
        &mut self,
        owner: u64,
        view_id: u64,
        objects: &BTreeSet<String>,
    ) -> Result<(), Error> {
        let exact = (owner, view_id);
        let mut exact_found = false;
        let mut foreign_collision = false;
        for object in objects {
            if let Some(owners) = self.pinned_objects.get(object) {
                exact_found |= owners.contains(&exact);
                foreign_collision |= owners.iter().any(|(pinned_owner, pinned_view)| {
                    *pinned_view == view_id && *pinned_owner != owner
                });
            }
        }
        if foreign_collision && !exact_found {
            return Err(Error::OwnerMismatch);
        }
        if !exact_found {
            return Err(Error::ReadExpired);
        }
        for object in objects {
            if let Some(owners) = self.pinned_objects.get_mut(object) {
                owners.remove(&exact);
                if owners.is_empty() {
                    self.pinned_objects.remove(object);
                }
            }
        }
        Ok(())
    }

    fn process_page(&mut self, max_entries: usize) -> Result<PageResult, Error> {
        self.calls += 1;
        if self.queue_head == self.entries.len() {
            return Ok(PageResult {
                advanced: false,
                drained: true,
                reclaimed: 0,
            });
        }
        if self.entries[self.queue_head].blocked {
            self.debt_tokens = 1;
            return Ok(PageResult {
                advanced: false,
                drained: false,
                reclaimed: 0,
            });
        }
        let old_head = self.queue_head;
        let new_head = (old_head + max_entries).min(self.entries.len());
        let object_ids: Vec<String> = self.entries[old_head..new_head]
            .iter()
            .map(|entry| entry.object.clone())
            .collect();
        let reachable = self.graph.authenticated_transitive_closure()?;
        for object in &object_ids {
            if reachable.contains(object)
                || self.pinned_objects.contains_key(object)
                || !self.graph.objects.contains_key(object)
            {
                return Err(
                    if reachable.contains(object) || self.pinned_objects.contains_key(object) {
                        Error::RootPinned
                    } else {
                        Error::MissingObject
                    },
                );
            }
        }
        for object in object_ids {
            self.graph.objects.remove(&object);
        }
        self.queue_head = new_head;
        self.fence.epoch += 1;
        self.fence.progress += 1;
        self.reclaimed += new_head - old_head;
        if self.debt_tokens > 0 {
            self.debt_tokens -= 1;
        }
        Ok(PageResult {
            advanced: true,
            drained: new_head == self.entries.len(),
            reclaimed: new_head - old_head,
        })
    }

    fn release_blocked_head(&mut self) {
        if self.queue_head < self.entries.len() {
            self.entries[self.queue_head].blocked = false;
        }
    }

    fn persist(&self) -> String {
        let state_tag = Self::state_tag(
            self.fence,
            self.queue_head,
            self.queue_tail,
            self.calls,
            self.reclaimed,
            self.graph.objects.len(),
            &self.graph.object_digest(),
            &self.graph.selector_digest(),
        );
        format!(
            "owner={}|epoch={}|progress={}|selector={}|head={}|tail={}|calls={}|reclaimed={}|objects={}|object_digest={}|selector_digest={}|state_tag={}",
            self.fence.owner,
            self.fence.epoch,
            self.fence.progress,
            self.fence.selector,
            self.queue_head,
            self.queue_tail,
            self.calls,
            self.reclaimed,
            self.graph.objects.len(),
            self.graph.object_digest(),
            self.graph.selector_digest(),
            state_tag,
        )
    }

    fn state_tag(
        fence: Fence,
        queue_head: usize,
        queue_tail: usize,
        calls: u32,
        reclaimed: usize,
        object_count: usize,
        object_digest: &str,
        selector_digest: &str,
    ) -> String {
        format!(
            "{}:{}:{}:{}:{}:{}:{}:{}:{}:{}:{}",
            fence.owner,
            fence.epoch,
            fence.progress,
            fence.selector,
            queue_head,
            queue_tail,
            calls,
            reclaimed,
            object_count,
            object_digest,
            selector_digest,
        )
    }

    fn reopen(encoded: &str, graph: &RootGraph) -> Result<ReopenedState, Error> {
        let fields: Vec<&str> = encoded.split('|').collect();
        if fields.len() != 12 {
            return Err(Error::Malformed);
        }
        let value = |index: usize, key: &str| -> Result<&str, Error> {
            fields[index].strip_prefix(key).ok_or(Error::Malformed)
        };
        let owner = value(0, "owner=")?.parse().map_err(|_| Error::Malformed)?;
        let epoch = value(1, "epoch=")?.parse().map_err(|_| Error::Malformed)?;
        let progress = value(2, "progress=")?
            .parse()
            .map_err(|_| Error::Malformed)?;
        let selector = value(3, "selector=")?
            .parse()
            .map_err(|_| Error::Malformed)?;
        let queue_head = value(4, "head=")?.parse().map_err(|_| Error::Malformed)?;
        let queue_tail = value(5, "tail=")?.parse().map_err(|_| Error::Malformed)?;
        let calls = value(6, "calls=")?.parse().map_err(|_| Error::Malformed)?;
        let reclaimed = value(7, "reclaimed=")?
            .parse()
            .map_err(|_| Error::Malformed)?;
        let object_count = value(8, "objects=")?
            .parse()
            .map_err(|_| Error::Malformed)?;
        let object_digest = value(9, "object_digest=")?.to_owned();
        let selector_digest = value(10, "selector_digest=")?.to_owned();
        let state_tag = value(11, "state_tag=")?.to_owned();
        if owner == 0
            || queue_head > queue_tail
            || object_count == 0
            || object_digest.is_empty()
            || selector_digest.is_empty()
            || state_tag.is_empty()
        {
            return Err(Error::Malformed);
        }
        graph.validate()?;
        if object_count != graph.objects.len()
            || object_digest != graph.object_digest()
            || selector_digest != graph.selector_digest()
            || state_tag
                != Self::state_tag(
                    Fence {
                        owner,
                        epoch,
                        progress,
                        selector,
                    },
                    queue_head,
                    queue_tail,
                    calls,
                    reclaimed,
                    object_count,
                    &object_digest,
                    &selector_digest,
                )
        {
            return Err(Error::PersistedMismatch);
        }
        Ok(ReopenedState {
            fence: Fence {
                owner,
                epoch,
                progress,
                selector,
            },
            queue_head,
            queue_tail,
            calls,
            reclaimed,
            object_count,
            object_digest,
            selector_digest,
            state_tag,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct View {
    id: u64,
    owner: u64,
    read: CoherentRead,
    root_id: String,
    valid: bool,
    last_key: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Cursor {
    owner: u64,
    view_id: u64,
    root_id: String,
    after: Option<String>,
    proof: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Resume {
    Start,
    Excluded(String),
}

struct ReaderModel {
    owner: u64,
    next_view: u64,
    views: BTreeMap<u64, View>,
    pinned_roots: BTreeMap<String, BTreeSet<u64>>,
}

impl ReaderModel {
    fn new(owner: u64) -> Self {
        Self {
            owner,
            next_view: 0,
            views: BTreeMap::new(),
            pinned_roots: BTreeMap::new(),
        }
    }

    fn begin_read(&mut self, authority: &mut Authority, root: RootClass) -> Result<View, Error> {
        let read = authority.begin_read(self.owner, root)?;
        let root_id = read
            .selectors
            .get(&root)
            .cloned()
            .ok_or(Error::MissingRoot)?;
        self.next_view += 1;
        let view = View {
            id: self.next_view,
            owner: self.owner,
            read,
            root_id: root_id.clone(),
            valid: true,
            last_key: None,
        };
        self.pinned_roots
            .entry(root_id)
            .or_default()
            .insert(view.id);
        authority.pin_read(view.owner, view.id, &view.read.reachable);
        self.views.insert(view.id, view.clone());
        Ok(view)
    }

    fn unpin(&mut self, root_id: &str, view_id: u64) {
        if let Some(owners) = self.pinned_roots.get_mut(root_id) {
            owners.remove(&view_id);
            if owners.is_empty() {
                self.pinned_roots.remove(root_id);
            }
        }
    }

    fn cursor(view: &View, after: Option<String>) -> Cursor {
        let proof = auth_tag(
            ObjectKind::Payload,
            &format!(
                "owner={} view={} root={} after={after:?}",
                view.owner, view.id, view.root_id
            ),
            Vec::new().as_slice(),
            view.read.fence.selector,
        );
        Cursor {
            owner: view.owner,
            view_id: view.id,
            root_id: view.root_id.clone(),
            after,
            proof,
        }
    }

    fn fail_page(
        &mut self,
        authority: &mut Authority,
        view_id: u64,
        delivered: &str,
    ) -> Result<(), Error> {
        let view = self.views.get_mut(&view_id).ok_or(Error::ReadExpired)?;
        view.valid = false;
        view.last_key = Some(delivered.to_owned());
        let root_id = view.root_id.clone();
        let owner = view.owner;
        let reachable = view.read.reachable.clone();
        self.unpin(&root_id, view_id);
        authority.unpin_read(owner, view_id, &reachable)?;
        Err(Error::Malformed)
    }

    fn close(&mut self, authority: &mut Authority, view_id: u64) -> Result<(), Error> {
        let view = self.views.get_mut(&view_id).ok_or(Error::ReadExpired)?;
        view.valid = false;
        let root_id = view.root_id.clone();
        let owner = view.owner;
        let reachable = view.read.reachable.clone();
        self.unpin(&root_id, view_id);
        authority.unpin_read(owner, view_id, &reachable)
    }

    fn resume(&self, cursor: &Cursor) -> Result<Resume, Error> {
        let view = self.views.get(&cursor.view_id).ok_or(Error::ReadExpired)?;
        if !view.valid || view.owner != cursor.owner || view.root_id != cursor.root_id {
            return Err(Error::ReadExpired);
        }
        let expected = Self::cursor(view, cursor.after.clone()).proof;
        if cursor.proof != expected {
            return Err(Error::InvalidCursor);
        }
        if let Some(last_key) = &view.last_key {
            if cursor.after.as_ref() != Some(last_key) {
                return Err(Error::InvalidCursor);
            }
        }
        Ok(match &cursor.after {
            Some(key) => Resume::Excluded(key.clone()),
            None => Resume::Start,
        })
    }
}

fn authority_with_queue(count: usize, blocked_first: bool) -> Authority {
    let mut graph = RootGraph::fixture();
    let entries = (0..count)
        .map(|index| QueueEntry {
            object: graph.add_queue_object(&format!("queue-{index:02}")),
            blocked: blocked_first && index == 0,
        })
        .collect();
    Authority::new(graph, entries)
}

#[test]
fn w5_r7_object_selector_plane_and_h_s_c_chronology_are_authenticated() {
    assert_eq!(OBJECT_SPACE, "OBJECT_SPACE");
    assert_eq!(SELECTOR_SPACE, "SELECTOR_SPACE");
    let authority = authority_with_queue(0, false);
    let reachable = authority.graph.validate().expect("valid root graph");
    assert_eq!(authority.graph.selectors.len(), RootClass::ALL.len());
    assert!(RootClass::ALL
        .iter()
        .all(|root| authority.graph.selectors.contains_key(root)));
    assert!(reachable.len() < authority.graph.objects.len());

    let history = &authority.graph.objects[&authority.graph.selectors[&RootClass::History]];
    let serving = &authority.graph.objects[&authority.graph.selectors[&RootClass::Serving]];
    let checkpoint = &authority.graph.objects[&authority.graph.selectors[&RootClass::Checkpoint]];
    assert_eq!(history.kind, ObjectKind::History);
    assert_eq!(serving.kind, ObjectKind::Serving);
    assert_eq!(checkpoint.kind, ObjectKind::Checkpoint);
    assert_eq!(serving.parents, vec![history.id.clone()]);
    assert_eq!(checkpoint.parents, vec![serving.id.clone()]);
    assert_ne!(
        authority.graph.selectors[&RootClass::History],
        authority.graph.selectors[&RootClass::Serving]
    );
    assert_ne!(
        authority.graph.selectors[&RootClass::Serving],
        authority.graph.selectors[&RootClass::Checkpoint]
    );
}

#[test]
fn w5_r7_coherent_read_and_owner_bound_cas_are_exact() {
    let mut authority = authority_with_queue(0, false);
    let read = authority
        .begin_read(OWNER, RootClass::Checkpoint)
        .expect("one coherent read");
    assert_eq!(authority.begin_reads, 1);
    assert_eq!(read.fence, authority.fence);
    assert_eq!(read.selector_digest, authority.graph.selector_digest());

    let object = GraphObject::seal(ObjectKind::Payload, "new-upload", Vec::new(), 0);
    let mut wrong_owner = authority
        .prepare_publication(OWNER, RootClass::Upload, object.clone())
        .unwrap();
    wrong_owner.owner = OWNER + 1;
    assert_eq!(
        authority.commit_publication(wrong_owner),
        Err(Error::OwnerMismatch)
    );

    let plan = authority
        .prepare_publication(OWNER, RootClass::Upload, object)
        .unwrap();
    assert_eq!(authority.commit_publication(plan), Ok(()));
    assert_eq!(authority.fence.selector, 1);
}

#[test]
fn w5_r7_publication_first_and_gc_first_are_discriminating() {
    let mut publication_first = authority_with_queue(0, false);
    let orphan = publication_first
        .graph
        .add_queue_object("orphan-publication-first");
    publication_first.entries.push(QueueEntry {
        object: orphan.clone(),
        blocked: false,
    });
    publication_first.queue_tail = 1;
    let publication = publication_first
        .prepare_publication(
            OWNER,
            RootClass::Serving,
            GraphObject::seal(
                ObjectKind::Serving,
                "serving-2",
                vec![publication_first.graph.selectors[&RootClass::History].clone()],
                2,
            ),
        )
        .unwrap();
    let gc = publication_first.prepare_gc(OWNER, orphan).unwrap();
    assert_eq!(publication_first.commit_publication(publication), Ok(()));
    assert_eq!(publication_first.commit_gc(gc), Err(Error::StaleFence));
    assert_eq!(publication_first.fence.selector, 1);
    assert!(publication_first
        .graph
        .selectors
        .contains_key(&RootClass::Serving));

    let mut gc_first = authority_with_queue(0, false);
    let orphan = gc_first.graph.add_queue_object("orphan-gc-first");
    let publication = gc_first
        .prepare_publication(
            OWNER,
            RootClass::Serving,
            GraphObject::seal(
                ObjectKind::Serving,
                "serving-2",
                vec![gc_first.graph.selectors[&RootClass::History].clone()],
                2,
            ),
        )
        .unwrap();
    let gc = gc_first.prepare_gc(OWNER, orphan.clone()).unwrap();
    assert_eq!(gc_first.commit_gc(gc), Ok(()));
    assert!(!gc_first.graph.objects.contains_key(&orphan));
    assert_eq!(
        gc_first.commit_publication(publication),
        Err(Error::StaleFence)
    );
    assert_eq!(gc_first.fence.selector, 0);
}

#[test]
fn w5_r7_retireable_65_entry_queue_drains_suffix() {
    let mut authority = authority_with_queue(65, false);
    let first = authority.process_page(64).unwrap();
    assert_eq!(
        first,
        PageResult {
            advanced: true,
            drained: false,
            reclaimed: 64
        }
    );
    assert_eq!(authority.queue_head, 64);
    assert_eq!(authority.queue_tail, 65);
    assert_eq!(authority.reclaimed, 64);
    assert_eq!(
        authority.fence,
        Fence {
            owner: OWNER,
            epoch: 1,
            progress: 1,
            selector: 0
        }
    );

    let second = authority.process_page(64).unwrap();
    assert_eq!(
        second,
        PageResult {
            advanced: true,
            drained: true,
            reclaimed: 1
        }
    );
    assert_eq!(authority.queue_head, 65);
    assert_eq!(authority.reclaimed, 65);
    assert_eq!(authority.process_page(64).unwrap().reclaimed, 0);
}

#[test]
fn w5_r7_blocked_head_preserves_debt_without_spin_until_release() {
    let mut authority = authority_with_queue(2, true);
    let blocked = authority.process_page(64).unwrap();
    assert_eq!(
        blocked,
        PageResult {
            advanced: false,
            drained: false,
            reclaimed: 0
        }
    );
    assert_eq!(authority.debt_tokens, 1);
    assert_eq!(authority.calls, 1);
    assert_eq!(authority.reclaimed, 0);

    authority.release_blocked_head();
    let drained = authority.process_page(64).unwrap();
    assert_eq!(drained.reclaimed, 2);
    assert!(drained.drained);
    assert_eq!(authority.debt_tokens, 0);
    assert_eq!(authority.calls, 2);
}

#[test]
fn w5_r7_all_root_classes_release_shared_and_final_references() {
    let mut graph = RootGraph::fixture();
    let shared = graph.selectors[&RootClass::Upload].clone();
    let final_reference = graph.selectors[&RootClass::FinalReference].clone();
    assert!(graph.validate().is_ok());
    assert_eq!(graph.reclaim_unreachable().unwrap(), 1);

    graph.drop_selector(RootClass::BranchControl);
    assert!(graph.validate().unwrap().contains(&shared));
    assert_eq!(graph.reclaim_unreachable().unwrap(), 0);

    graph.drop_selector(RootClass::Upload);
    assert_eq!(graph.reclaim_unreachable().unwrap(), 1);
    assert!(!graph.objects.contains_key(&shared));

    graph.drop_selector(RootClass::FinalReference);
    assert_eq!(graph.reclaim_unreachable().unwrap(), 1);
    assert!(!graph.objects.contains_key(&final_reference));
}

#[test]
fn w5_r7_pinned_view_poison_and_authenticated_excluded_restart() {
    let mut authority = authority_with_queue(0, false);
    let mut readers = ReaderModel::new(OWNER);
    let first = readers
        .begin_read(&mut authority, RootClass::Checkpoint)
        .unwrap();
    let second = readers
        .begin_read(&mut authority, RootClass::Checkpoint)
        .unwrap();
    let start = ReaderModel::cursor(&first, None);
    assert_eq!(readers.resume(&start), Ok(Resume::Start));
    assert_eq!(
        readers.pinned_roots[&first.root_id],
        BTreeSet::from([first.id, second.id])
    );
    assert_eq!(
        readers.fail_page(&mut authority, first.id, "row-09"),
        Err(Error::Malformed)
    );
    assert_eq!(
        readers.pinned_roots[&second.root_id],
        BTreeSet::from([second.id])
    );
    assert_eq!(readers.resume(&start), Err(Error::ReadExpired));
    let second_cursor = ReaderModel::cursor(&second, None);
    assert_eq!(readers.resume(&second_cursor), Ok(Resume::Start));

    let fresh = readers
        .begin_read(&mut authority, RootClass::Checkpoint)
        .unwrap();
    let continuation = ReaderModel::cursor(&fresh, Some("row-09".to_owned()));
    assert_eq!(
        readers.resume(&continuation),
        Ok(Resume::Excluded("row-09".to_owned()))
    );
    let mut forged = continuation.clone();
    forged.root_id = "substituted-root".to_owned();
    assert_eq!(readers.resume(&forged), Err(Error::ReadExpired));
    readers.close(&mut authority, second.id).unwrap();
    assert_eq!(
        readers.pinned_roots[&fresh.root_id],
        BTreeSet::from([fresh.id])
    );
    readers.close(&mut authority, fresh.id).unwrap();
    assert!(readers.pinned_roots.is_empty());
    assert_eq!(authority.begin_reads, 3);
}

#[test]
fn w5_r7_transitive_h_s_c_closure_stays_pinned_across_publication_and_gc() {
    let mut authority = authority_with_queue(0, false);
    let mut readers = ReaderModel::new(OWNER);
    let view = readers
        .begin_read(&mut authority, RootClass::Checkpoint)
        .unwrap();
    let history_head = authority.graph.selectors[&RootClass::History].clone();
    let history_parent = authority.graph.objects[&history_head].parents[0].clone();
    let serving = authority.graph.selectors[&RootClass::Serving].clone();
    let checkpoint = authority.graph.selectors[&RootClass::Checkpoint].clone();
    assert!(view.read.reachable.contains(&history_head));
    assert!(view.read.reachable.contains(&history_parent));
    assert!(view.read.reachable.contains(&serving));
    assert!(view.read.reachable.contains(&checkpoint));
    for object in [&history_head, &history_parent, &serving, &checkpoint] {
        assert_eq!(
            authority.pinned_objects[object],
            BTreeSet::from([(OWNER, view.id)])
        );
    }

    let replacement = GraphObject::seal(
        ObjectKind::Serving,
        "serving-2",
        vec![history_head.clone()],
        2,
    );
    let publication = authority
        .prepare_publication(OWNER, RootClass::Serving, replacement)
        .unwrap();
    assert_eq!(authority.commit_publication(publication), Ok(()));
    let retire_old_serving = authority.prepare_gc(OWNER, serving.clone()).unwrap();
    assert_eq!(
        authority.commit_gc(retire_old_serving),
        Err(Error::RootPinned)
    );
    assert!(authority.graph.objects.contains_key(&serving));

    readers.close(&mut authority, view.id).unwrap();
    // A still-live checkpoint selector keeps its transitive closure pinned after view close.
    assert_eq!(
        authority.commit_gc(
            authority
                .prepare_gc(OWNER, serving.clone())
                .expect("checkpoint still owns the old serving closure")
        ),
        Err(Error::RootPinned)
    );
    authority.graph.drop_selector(RootClass::Checkpoint);
    let retire_old_serving = authority.prepare_gc(OWNER, serving.clone()).unwrap();
    assert_eq!(authority.commit_gc(retire_old_serving), Ok(()));
    assert!(!authority.graph.objects.contains_key(&serving));
}

#[test]
fn w5_r7_reader_pins_are_owner_and_view_scoped_without_cross_owner_release() {
    let mut authority = authority_with_queue(0, false);
    let mut owner_a = ReaderModel::new(OWNER);
    let mut owner_b = ReaderModel::new(OWNER + 1);
    let first = owner_a
        .begin_read(&mut authority, RootClass::Checkpoint)
        .unwrap();
    let second = owner_b
        .begin_read(&mut authority, RootClass::Checkpoint)
        .unwrap();
    assert_eq!(first.id, second.id, "local view IDs intentionally collide");

    let before = authority.pinned_objects.clone();
    assert_eq!(
        authority.unpin_read(OWNER + 2, first.id, &first.read.reachable),
        Err(Error::OwnerMismatch)
    );
    assert_eq!(authority.pinned_objects, before);
    assert_eq!(
        owner_b.resume(&ReaderModel::cursor(&first, None)),
        Err(Error::ReadExpired)
    );
    assert_eq!(
        owner_b.close(&mut authority, first.id),
        Ok(()),
        "owner B closes only its own colliding local view"
    );
    assert!(authority.pinned_objects.values().all(|pins| {
        pins.iter()
            .all(|(owner, view_id)| *owner == OWNER && *view_id == first.id)
    }));
    owner_a.close(&mut authority, first.id).unwrap();
    assert!(authority.pinned_objects.is_empty());
}

#[test]
fn w5_r7_h_s_c_reachability_is_transitive_and_selective() {
    let mut graph = RootGraph::fixture();
    let history_head = graph.selectors[&RootClass::History].clone();
    let history_parent = graph.objects[&history_head].parents[0].clone();
    let serving = graph.selectors[&RootClass::Serving].clone();
    let checkpoint = graph.selectors[&RootClass::Checkpoint].clone();
    let reachable = graph.validate().unwrap();
    for object in [&history_head, &history_parent, &serving, &checkpoint] {
        assert!(reachable.contains(object));
    }
    assert!(reachable.len() < graph.objects.len());
    assert_eq!(graph.reclaim_unreachable().unwrap(), 1);

    graph.drop_selector(RootClass::Checkpoint);
    assert_eq!(graph.reclaim_unreachable().unwrap(), 1);
    assert!(!graph.objects.contains_key(&checkpoint));
    graph.drop_selector(RootClass::Serving);
    assert_eq!(graph.reclaim_unreachable().unwrap(), 1);
    assert!(!graph.objects.contains_key(&serving));
    graph.drop_selector(RootClass::History);
    assert_eq!(graph.reclaim_unreachable().unwrap(), 2);
    assert!(!graph.objects.contains_key(&history_head));
    assert!(!graph.objects.contains_key(&history_parent));
}

#[test]
fn w5_r7_begin_read_authenticates_selected_root_before_pinning() {
    let mut missing = authority_with_queue(0, false);
    missing.graph.drop_selector(RootClass::Checkpoint);
    let mut missing_readers = ReaderModel::new(OWNER);
    assert_eq!(
        missing_readers.begin_read(&mut missing, RootClass::Checkpoint),
        Err(Error::MissingRoot)
    );
    assert!(missing_readers.pinned_roots.is_empty());
    assert_eq!(missing.begin_reads, 0);

    let mut wrong_kind = authority_with_queue(0, false);
    let checkpoint_id = wrong_kind.graph.selectors[&RootClass::Checkpoint].clone();
    wrong_kind
        .graph
        .objects
        .get_mut(&checkpoint_id)
        .unwrap()
        .kind = ObjectKind::Payload;
    let mut wrong_kind_readers = ReaderModel::new(OWNER);
    assert_eq!(
        wrong_kind_readers.begin_read(&mut wrong_kind, RootClass::Checkpoint),
        Err(Error::WrongKind)
    );
    assert!(wrong_kind_readers.pinned_roots.is_empty());
    assert_eq!(wrong_kind.begin_reads, 0);

    let mut substituted = authority_with_queue(0, false);
    let checkpoint_id = substituted.graph.selectors[&RootClass::Checkpoint].clone();
    let serving_id = substituted.graph.selectors[&RootClass::Serving].clone();
    substituted.graph.objects.insert(
        checkpoint_id.clone(),
        GraphObject::seal(ObjectKind::Checkpoint, "replacement", vec![serving_id], 3),
    );
    let mut substituted_readers = ReaderModel::new(OWNER);
    assert_eq!(
        substituted_readers.begin_read(&mut substituted, RootClass::Checkpoint),
        Err(Error::Substitution)
    );
    assert!(substituted_readers.pinned_roots.is_empty());
    assert_eq!(substituted.begin_reads, 0);
}

#[test]
fn w5_r7_missing_wrong_kind_malformed_cycle_and_substitution_fail_closed() {
    let history_id = RootGraph::fixture().selectors[&RootClass::History].clone();

    let mut missing = RootGraph::fixture();
    missing.objects.remove(&history_id);
    assert_eq!(missing.validate(), Err(Error::MissingObject));

    let mut wrong_kind = RootGraph::fixture();
    wrong_kind.objects.get_mut(&history_id).unwrap().kind = ObjectKind::Serving;
    assert_eq!(wrong_kind.validate(), Err(Error::WrongKind));

    let mut malformed = RootGraph::fixture();
    malformed
        .objects
        .get_mut(&history_id)
        .unwrap()
        .payload
        .clear();
    assert_eq!(malformed.validate(), Err(Error::Malformed));

    let mut duplicate = RootGraph::fixture();
    let parent = duplicate.objects[&history_id].parents[0].clone();
    duplicate.objects.get_mut(&history_id).unwrap().parents = vec![parent.clone(), parent];
    assert_eq!(duplicate.validate(), Err(Error::Malformed));

    let mut non_chronological = RootGraph::fixture();
    non_chronological
        .objects
        .get_mut(&history_id)
        .unwrap()
        .generation = 0;
    assert_eq!(non_chronological.validate(), Err(Error::Malformed));

    let mut cycle = RootGraph::fixture();
    let parent = cycle.objects[&history_id].parents[0].clone();
    cycle.objects.get_mut(&parent).unwrap().parents = vec![history_id.clone()];
    assert_eq!(cycle.validate(), Err(Error::Cycle));

    let mut substitution = RootGraph::fixture();
    let replacement = GraphObject::seal(ObjectKind::History, "other-commit", Vec::new(), 0);
    substitution.objects.insert(history_id, replacement);
    assert_eq!(substitution.validate(), Err(Error::Substitution));
}

#[test]
fn w5_r7_cold_reopen_preserves_authenticated_authority_and_queue() {
    let mut authority = authority_with_queue(2, false);
    authority.process_page(1).unwrap();
    let encoded = authority.persist();
    let recovered = Authority::reopen(&encoded, &authority.graph).unwrap();
    assert_eq!(recovered.fence, authority.fence);
    assert_eq!(recovered.queue_head, authority.queue_head);
    assert_eq!(recovered.queue_tail, authority.queue_tail);
    assert_eq!(recovered.calls, authority.calls);
    assert_eq!(recovered.reclaimed, authority.reclaimed);
    assert_eq!(recovered.object_count, authority.graph.objects.len());
    assert_eq!(recovered.object_digest, authority.graph.object_digest());
    assert_eq!(recovered.selector_digest, authority.graph.selector_digest());
    assert_eq!(
        recovered.state_tag,
        Authority::state_tag(
            authority.fence,
            authority.queue_head,
            authority.queue_tail,
            authority.calls,
            authority.reclaimed,
            authority.graph.objects.len(),
            &authority.graph.object_digest(),
            &authority.graph.selector_digest(),
        )
    );
    assert_eq!(
        Authority::reopen(
            &encoded.replace("object_digest=", "object_digest=forged-"),
            &authority.graph,
        ),
        Err(Error::PersistedMismatch)
    );
    assert_eq!(
        Authority::reopen(
            &encoded.replace("selector_digest=", "selector_digest=forged-"),
            &authority.graph,
        ),
        Err(Error::PersistedMismatch)
    );
    assert_eq!(
        Authority::reopen(
            &encoded.replace("state_tag=", "state_tag=forged-"),
            &authority.graph,
        ),
        Err(Error::PersistedMismatch)
    );
    assert_eq!(
        Authority::reopen(
            &encoded.replace("progress=1", "progress=oops"),
            &authority.graph
        ),
        Err(Error::Malformed)
    );
    assert_eq!(
        Authority::reopen("owner=7|epoch=1", &authority.graph),
        Err(Error::Malformed)
    );
}
