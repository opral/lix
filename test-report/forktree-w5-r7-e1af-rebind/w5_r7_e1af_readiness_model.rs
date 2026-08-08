use std::collections::{BTreeMap, BTreeSet};

type ObjectId = String;
type RootId = String;
type OwnerId = String;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Space {
    Object,
    Selector,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObjectKind {
    Root,
    Node,
    Payload,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObjectRecord {
    id: ObjectId,
    kind: ObjectKind,
    refs: Vec<ObjectId>,
    body: Vec<u8>,
    fingerprint: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GlobalSelectorV1 {
    epoch: u64,
    progress: u64,
    owner: OwnerId,
    root: RootId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BranchSelectorV1 {
    branch: String,
    epoch: u64,
    progress: u64,
    owner: OwnerId,
    root: RootId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ObjectSpaceV1 {
    objects: BTreeMap<ObjectId, ObjectRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SelectorSpaceV1 {
    global: Option<GlobalSelectorV1>,
    branches: BTreeMap<String, BranchSelectorV1>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GraphAuthority {
    objects: ObjectSpaceV1,
    selectors: SelectorSpaceV1,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum AuthError {
    Missing { space: Space, id: String },
    WrongKind,
    IdentitySubstituted,
    Cycle,
    Malformed,
    Integrity,
    Order,
    StaleEpoch,
    OwnerMismatch,
}

fn object_fingerprint(kind: ObjectKind, refs: &[ObjectId], body: &[u8]) -> u64 {
    let kind_tag = match kind {
        ObjectKind::Root => 1,
        ObjectKind::Node => 2,
        ObjectKind::Payload => 3,
    };
    let mut hash = 0xcbf29ce484222325u64 ^ kind_tag;
    for reference in refs {
        for byte in reference.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    for byte in body {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn object_record(id: &str, kind: ObjectKind, refs: &[&str], body: &[u8]) -> ObjectRecord {
    let refs = refs
        .iter()
        .map(|reference| (*reference).into())
        .collect::<Vec<_>>();
    ObjectRecord {
        id: id.into(),
        kind,
        fingerprint: object_fingerprint(kind, &refs, body),
        refs,
        body: body.to_vec(),
    }
}

fn expected_kind(id: &str) -> Option<ObjectKind> {
    if id.starts_with("root:") {
        Some(ObjectKind::Root)
    } else if id.starts_with("node:") {
        Some(ObjectKind::Node)
    } else if id.starts_with("payload:") {
        Some(ObjectKind::Payload)
    } else {
        None
    }
}

impl GraphAuthority {
    fn authenticate_global(&self, expected_epoch: u64, owner: &str) -> Result<(), AuthError> {
        let selector = self
            .selectors
            .global
            .as_ref()
            .ok_or_else(|| AuthError::Missing {
                space: Space::Selector,
                id: "global".into(),
            })?;
        self.validate_selector(
            selector.epoch,
            selector.progress,
            &selector.owner,
            &selector.root,
            expected_epoch,
            owner,
        )?;
        self.authenticate_root(&selector.root)
    }

    fn authenticate_branch(
        &self,
        branch: &str,
        expected_epoch: u64,
        owner: &str,
    ) -> Result<(), AuthError> {
        let selector = self
            .selectors
            .branches
            .get(branch)
            .ok_or_else(|| AuthError::Missing {
                space: Space::Selector,
                id: branch.into(),
            })?;
        if selector.branch != branch {
            return Err(AuthError::IdentitySubstituted);
        }
        self.validate_selector(
            selector.epoch,
            selector.progress,
            &selector.owner,
            &selector.root,
            expected_epoch,
            owner,
        )?;
        self.authenticate_root(&selector.root)
    }

    fn validate_selector(
        &self,
        epoch: u64,
        progress: u64,
        selector_owner: &str,
        root: &str,
        expected_epoch: u64,
        expected_owner: &str,
    ) -> Result<(), AuthError> {
        if selector_owner != expected_owner {
            return Err(AuthError::OwnerMismatch);
        }
        if epoch != expected_epoch {
            return Err(AuthError::StaleEpoch);
        }
        if progress > epoch || expected_kind(root) != Some(ObjectKind::Root) {
            return Err(AuthError::Malformed);
        }
        Ok(())
    }

    fn authenticate_root(&self, root: &str) -> Result<(), AuthError> {
        let mut visiting = BTreeSet::new();
        let mut visited = BTreeSet::new();
        self.authenticate_object(root, &mut visiting, &mut visited)
    }

    fn authenticate_root_closure(&self, roots: &[&str]) -> Result<BTreeSet<ObjectId>, AuthError> {
        if roots.is_empty() {
            return Err(AuthError::Malformed);
        }
        let mut visiting = BTreeSet::new();
        let mut visited = BTreeSet::new();
        for root in roots {
            if expected_kind(root) != Some(ObjectKind::Root) {
                return Err(AuthError::WrongKind);
            }
            self.authenticate_object(root, &mut visiting, &mut visited)?;
        }
        Ok(visited)
    }

    fn authenticate_object(
        &self,
        id: &str,
        visiting: &mut BTreeSet<ObjectId>,
        visited: &mut BTreeSet<ObjectId>,
    ) -> Result<(), AuthError> {
        if visiting.contains(id) {
            return Err(AuthError::Cycle);
        }
        if visited.contains(id) {
            return Ok(());
        }
        let record = self
            .objects
            .objects
            .get(id)
            .ok_or_else(|| AuthError::Missing {
                space: Space::Object,
                id: id.into(),
            })?;
        if record.id != id {
            return Err(AuthError::IdentitySubstituted);
        }
        if expected_kind(id) != Some(record.kind) {
            return Err(AuthError::WrongKind);
        }
        if record.fingerprint != object_fingerprint(record.kind, &record.refs, &record.body) {
            return Err(AuthError::Integrity);
        }
        if record.body.is_empty() && record.kind != ObjectKind::Payload {
            return Err(AuthError::Malformed);
        }
        let mut unique_refs = BTreeSet::new();
        visiting.insert(id.into());
        for child in &record.refs {
            if !unique_refs.insert(child.clone()) {
                return Err(AuthError::Malformed);
            }
            self.authenticate_object(child, visiting, visited)?;
        }
        visiting.remove(id);
        visited.insert(id.into());
        Ok(())
    }
}

fn valid_graph() -> GraphAuthority {
    let mut objects = BTreeMap::new();
    objects.insert(
        "root:global".into(),
        object_record("root:global", ObjectKind::Root, &["node:global"], b"root"),
    );
    objects.insert(
        "node:global".into(),
        object_record(
            "node:global",
            ObjectKind::Node,
            &["payload:global"],
            b"node",
        ),
    );
    objects.insert(
        "payload:global".into(),
        object_record("payload:global", ObjectKind::Payload, &[], b""),
    );
    let global = GlobalSelectorV1 {
        epoch: 7,
        progress: 7,
        owner: "owner-a".into(),
        root: "root:global".into(),
    };
    let branch = BranchSelectorV1 {
        branch: "main".into(),
        epoch: 7,
        progress: 7,
        owner: "owner-a".into(),
        root: "root:global".into(),
    };
    GraphAuthority {
        objects: ObjectSpaceV1 { objects },
        selectors: SelectorSpaceV1 {
            global: Some(global),
            branches: BTreeMap::from([("main".into(), branch)]),
        },
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Fence {
    epoch: u64,
    progress: u64,
    selector: u64,
    owner: OwnerId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SelectorPlaneState {
    fence: Fence,
    root: RootId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum FenceError {
    StaleEpoch,
    OwnerMismatch,
}

impl SelectorPlaneState {
    fn new(owner: &str) -> Self {
        Self {
            fence: Fence {
                epoch: 0,
                progress: 0,
                selector: 0,
                owner: owner.into(),
            },
            root: "root:global".into(),
        }
    }

    fn advance_gc(&mut self, expected: &Fence, owner: &str) -> Result<(), FenceError> {
        self.validate_fence(expected, owner)?;
        self.fence.epoch += 1;
        self.fence.progress += 1;
        Ok(())
    }

    fn publish(&mut self, expected: &Fence, owner: &str, root: &str) -> Result<(), FenceError> {
        self.validate_fence(expected, owner)?;
        self.fence.selector += 1;
        self.root = root.into();
        Ok(())
    }

    fn commit_gc_and_publish(
        &mut self,
        expected: &Fence,
        owner: &str,
        root: &str,
    ) -> Result<(), FenceError> {
        self.validate_fence(expected, owner)?;
        self.fence.epoch += 1;
        self.fence.progress += 1;
        self.fence.selector += 1;
        self.root = root.into();
        Ok(())
    }

    fn validate_fence(&self, expected: &Fence, owner: &str) -> Result<(), FenceError> {
        if owner != self.fence.owner || expected.owner != owner {
            return Err(FenceError::OwnerMismatch);
        }
        if expected != &self.fence {
            return Err(FenceError::StaleEpoch);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct QueueEntry {
    object: String,
    blocked: bool,
    fingerprint: u64,
}

impl QueueEntry {
    fn new(object: &str, blocked: bool) -> Self {
        Self {
            object: object.into(),
            blocked,
            fingerprint: Self::fingerprint_for(object, blocked),
        }
    }

    fn fingerprint_for(object: &str, blocked: bool) -> u64 {
        let mut hash = 0x84222325cbf29ce4u64 ^ u64::from(blocked);
        for byte in object.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash
    }

    fn authenticate(&self) -> Result<(), AuthError> {
        if self.fingerprint != Self::fingerprint_for(&self.object, self.blocked) {
            Err(AuthError::Integrity)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PageResult {
    advanced: bool,
    drained: bool,
    reclaimed: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct QueueState {
    plane: SelectorPlaneState,
    entries: Vec<QueueEntry>,
    queue_head: usize,
    queue_tail: usize,
    deleted: BTreeSet<String>,
    debt_tokens: u32,
    calls: u32,
}

impl QueueState {
    fn new(entries: Vec<QueueEntry>) -> Self {
        let queue_tail = entries.len();
        Self {
            plane: SelectorPlaneState::new("gc-owner"),
            entries,
            queue_head: 0,
            queue_tail,
            deleted: BTreeSet::new(),
            debt_tokens: 0,
            calls: 0,
        }
    }

    fn process(&mut self, max_entries: usize) -> PageResult {
        self.process_checked(max_entries)
            .expect("queue state is authenticated before processing")
    }

    fn process_checked(&mut self, max_entries: usize) -> Result<PageResult, AuthError> {
        let expected = self.plane.fence.clone();
        let result = self.process_entries_only(max_entries)?;
        if result.advanced {
            self.plane
                .advance_gc(&expected, "gc-owner")
                .expect("queue owns its selector fence");
        }
        Ok(result)
    }

    fn process_entries_only(&mut self, max_entries: usize) -> Result<PageResult, AuthError> {
        if max_entries == 0
            || self.queue_head > self.queue_tail
            || self.queue_tail != self.entries.len()
        {
            return Err(AuthError::Malformed);
        }
        let mut seen = BTreeSet::new();
        for entry in &self.entries {
            entry.authenticate()?;
            if !seen.insert(entry.object.clone()) {
                return Err(AuthError::Order);
            }
        }
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
        let mut reclaimed = 0;
        for entry in &self.entries[old_head..new_head] {
            if self.deleted.insert(entry.object.clone()) {
                reclaimed += 1;
            }
        }
        self.queue_head = new_head;
        Ok(PageResult {
            advanced: true,
            drained: new_head == self.entries.len(),
            reclaimed,
        })
    }

    fn release_blocked_head(&mut self) {
        if let Some(entry) = self.entries.get_mut(self.queue_head) {
            entry.blocked = false;
            entry.fingerprint = QueueEntry::fingerprint_for(&entry.object, entry.blocked);
        }
    }

    fn persist(&self) -> String {
        let encoded_entries = self
            .entries
            .iter()
            .map(|entry| {
                format!(
                    "{}:{}:{}",
                    entry.object,
                    u8::from(entry.blocked),
                    entry.fingerprint
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}",
            self.plane.fence.epoch,
            self.plane.fence.progress,
            self.plane.fence.selector,
            self.plane.fence.owner,
            self.plane.root,
            self.queue_head,
            self.queue_tail,
            self.deleted.len(),
            encoded_entries
        )
    }

    fn reopen(encoded: &str, entries: Vec<QueueEntry>) -> Result<Self, AuthError> {
        let parts: Vec<_> = encoded.split('|').collect();
        if parts.len() != 9 || parts[3] != "gc-owner" || parts[4] != "root:global" {
            return Err(AuthError::Malformed);
        }
        let parse = |index: usize| {
            parts[index]
                .parse::<u64>()
                .map_err(|_| AuthError::Malformed)
        };
        let epoch = parse(0)?;
        let progress = parse(1)?;
        let selector = parse(2)?;
        let queue_head = parse(5)? as usize;
        let queue_tail = parse(6)? as usize;
        let deleted_len = parse(7)? as usize;
        if queue_tail != entries.len()
            || queue_head > queue_tail
            || deleted_len > queue_head
            || progress > epoch
        {
            return Err(AuthError::Malformed);
        }
        let mut seen = BTreeSet::new();
        for (encoded, entry) in parts[8].split(',').zip(&entries) {
            let fields: Vec<_> = encoded.split(':').collect();
            if fields.len() != 3
                || fields[0] != entry.object
                || fields[1] != u8::from(entry.blocked).to_string()
                || fields[2] != entry.fingerprint.to_string()
            {
                return Err(AuthError::Integrity);
            }
            if !seen.insert(entry.object.clone()) {
                return Err(AuthError::Order);
            }
            entry.authenticate()?;
        }
        if parts[8].split(',').count() != entries.len() {
            return Err(AuthError::Order);
        }
        let deleted = entries[..deleted_len]
            .iter()
            .map(|entry| entry.object.clone())
            .collect();
        Ok(Self {
            plane: SelectorPlaneState {
                fence: Fence {
                    epoch,
                    progress,
                    selector,
                    owner: "gc-owner".into(),
                },
                root: "root:global".into(),
            },
            entries,
            queue_head,
            queue_tail,
            deleted,
            debt_tokens: 0,
            calls: 0,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ReadId(u64);

#[derive(Clone, Debug, Eq, PartialEq)]
struct View {
    id: ReadId,
    owner: OwnerId,
    root: RootId,
    valid: bool,
    last_key: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Cursor {
    owner: OwnerId,
    read_id: ReadId,
    last_key: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReadError {
    ReadExpired,
    InvalidCursor,
    Malformed,
    OwnerMismatch,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReaderPins {
    next_read: u64,
    views: BTreeMap<ReadId, View>,
    roots: BTreeMap<RootId, BTreeSet<ReadId>>,
}

impl ReaderPins {
    fn new() -> Self {
        Self {
            next_read: 0,
            views: BTreeMap::new(),
            roots: BTreeMap::new(),
        }
    }

    fn begin_read(&mut self, owner: &str, root: &str) -> View {
        self.next_read += 1;
        let view = View {
            id: ReadId(self.next_read),
            owner: owner.into(),
            root: root.into(),
            valid: true,
            last_key: None,
        };
        self.roots.entry(root.into()).or_default().insert(view.id);
        self.views.insert(view.id, view.clone());
        view
    }

    fn poison(&mut self, owner: &str, read_id: ReadId, delivered: &str) -> Result<(), ReadError> {
        let view = self.views.get_mut(&read_id).ok_or(ReadError::ReadExpired)?;
        if view.owner != owner {
            return Err(ReadError::OwnerMismatch);
        }
        view.valid = false;
        view.last_key = Some(delivered.into());
        if let Some(readers) = self.roots.get_mut(&view.root) {
            readers.remove(&read_id);
            if readers.is_empty() {
                self.roots.remove(&view.root);
            }
        }
        Err(ReadError::Malformed)
    }

    fn close(&mut self, owner: &str, read_id: ReadId) -> Result<(), ReadError> {
        let view = self.views.get(&read_id).ok_or(ReadError::ReadExpired)?;
        if view.owner != owner {
            return Err(ReadError::OwnerMismatch);
        }
        let view = self.views.remove(&read_id).ok_or(ReadError::ReadExpired)?;
        if let Some(readers) = self.roots.get_mut(&view.root) {
            readers.remove(&read_id);
            if readers.is_empty() {
                self.roots.remove(&view.root);
            }
        }
        Ok(())
    }

    fn resume(&self, cursor: &Cursor) -> Result<String, ReadError> {
        let view = self
            .views
            .get(&cursor.read_id)
            .ok_or(ReadError::ReadExpired)?;
        if !view.valid {
            return Err(ReadError::ReadExpired);
        }
        if view.owner != cursor.owner {
            return Err(ReadError::OwnerMismatch);
        }
        if view.last_key != cursor.last_key {
            return Err(ReadError::InvalidCursor);
        }
        Ok(cursor.last_key.clone().unwrap_or_else(|| "<start>".into()))
    }

    fn pin_count(&self, root: &str) -> usize {
        self.roots.get(root).map_or(0, BTreeSet::len)
    }
}

struct CoherentRead {
    view_id: ReadId,
    root: RootId,
    begin_reads: u32,
}

impl CoherentRead {
    fn open(root: &str) -> Self {
        Self {
            view_id: ReadId(1),
            root: root.into(),
            begin_reads: 1,
        }
    }

    fn observe(&self, view_id: ReadId, label: &str) -> Result<(), ReadError> {
        if self.begin_reads != 1 || self.view_id != view_id || self.root.is_empty() {
            return Err(ReadError::InvalidCursor);
        }
        if label.is_empty() {
            return Err(ReadError::Malformed);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RootOwners {
    owners: BTreeMap<RootId, BTreeSet<String>>,
}

impl RootOwners {
    fn new() -> Self {
        Self {
            owners: BTreeMap::new(),
        }
    }

    fn retain(&mut self, root: &str, owner: &str) {
        self.owners
            .entry(root.into())
            .or_default()
            .insert(owner.into());
    }

    fn release(&mut self, root: &str, owner: &str) -> Result<(), AuthError> {
        let owners = self.owners.get_mut(root).ok_or(AuthError::Missing {
            space: Space::Object,
            id: root.into(),
        })?;
        if !owners.remove(owner) {
            return Err(AuthError::OwnerMismatch);
        }
        Ok(())
    }

    fn reclaim_released(&mut self) -> usize {
        let released: Vec<_> = self
            .owners
            .iter()
            .filter(|(_, owners)| owners.is_empty())
            .map(|(root, _)| root.clone())
            .collect();
        let count = released.len();
        for root in released {
            self.owners.remove(&root);
        }
        count
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AtomicW5State {
    plane: SelectorPlaneState,
    queue: QueueState,
    roots: RootOwners,
    pins: ReaderPins,
}

impl AtomicW5State {
    fn new(entries: Vec<QueueEntry>) -> Self {
        let queue = QueueState::new(entries);
        Self {
            plane: queue.plane.clone(),
            queue,
            roots: RootOwners::new(),
            pins: ReaderPins::new(),
        }
    }

    fn apply(
        &mut self,
        expected: &Fence,
        owner: &str,
        new_root: &str,
        max_entries: usize,
        graph: &GraphAuthority,
        roots: &[&str],
    ) -> Result<PageResult, AuthError> {
        let mut staged = self.clone();
        staged
            .plane
            .validate_fence(expected, owner)
            .map_err(|error| match error {
                FenceError::StaleEpoch => AuthError::StaleEpoch,
                FenceError::OwnerMismatch => AuthError::OwnerMismatch,
            })?;
        graph.authenticate_root_closure(roots)?;
        if expected_kind(new_root) != Some(ObjectKind::Root) {
            return Err(AuthError::WrongKind);
        }
        let result = staged.queue.process_entries_only(max_entries)?;
        if result.advanced {
            staged
                .plane
                .commit_gc_and_publish(expected, owner, new_root)
                .map_err(|error| match error {
                    FenceError::StaleEpoch => AuthError::StaleEpoch,
                    FenceError::OwnerMismatch => AuthError::OwnerMismatch,
                })?;
            staged.queue.plane = staged.plane.clone();
        } else if staged.plane.root != new_root {
            return Err(AuthError::StaleEpoch);
        }
        *self = staged;
        Ok(result)
    }
}

#[test]
fn transitive_h_s_c_roots_share_authenticated_closure_and_fail_closed() {
    let mut graph = valid_graph();
    for root in ["root:history", "root:serving", "root:checkpoint"] {
        graph.objects.objects.insert(
            root.into(),
            object_record(root, ObjectKind::Root, &["node:global"], b"root"),
        );
    }
    let closure = graph
        .authenticate_root_closure(&["root:history", "root:serving", "root:checkpoint"])
        .expect("all H/S/C roots authenticate");
    assert_eq!(closure.len(), 5);

    let mut missing = graph.clone();
    missing.objects.objects.remove("root:checkpoint");
    assert!(matches!(
        missing.authenticate_root_closure(&["root:history", "root:serving", "root:checkpoint"]),
        Err(AuthError::Missing { .. })
    ));

    let mut cycle = graph.clone();
    let checkpoint = cycle
        .objects
        .objects
        .get_mut("root:checkpoint")
        .expect("checkpoint");
    checkpoint.refs = vec!["root:checkpoint".into()];
    checkpoint.fingerprint =
        object_fingerprint(checkpoint.kind, &checkpoint.refs, &checkpoint.body);
    assert_eq!(
        cycle.authenticate_root_closure(&["root:history", "root:serving", "root:checkpoint"]),
        Err(AuthError::Cycle)
    );

    let mut substituted = graph;
    substituted
        .objects
        .objects
        .get_mut("root:serving")
        .expect("serving")
        .body = b"transplanted".to_vec();
    assert_eq!(
        substituted.authenticate_root_closure(&["root:history", "root:serving", "root:checkpoint"]),
        Err(AuthError::Integrity)
    );
}

#[test]
fn queue_identity_order_and_reopen_substitution_fail_without_mutation() {
    let mut queue = QueueState::new(vec![
        QueueEntry::new("queue-a", false),
        QueueEntry::new("queue-b", false),
    ]);
    queue.entries[0].fingerprint ^= 1;
    let corrupted = queue.clone();
    assert_eq!(queue.process_checked(1), Err(AuthError::Integrity));
    assert_eq!(queue, corrupted);

    let mut duplicate = QueueState::new(vec![
        QueueEntry::new("queue-a", false),
        QueueEntry::new("queue-b", false),
    ]);
    duplicate.entries[1] = QueueEntry::new("queue-a", false);
    let duplicate_before = duplicate.clone();
    assert_eq!(duplicate.process_checked(1), Err(AuthError::Order));
    assert_eq!(duplicate, duplicate_before);

    let mut progressed = QueueState::new(vec![
        QueueEntry::new("queue-a", false),
        QueueEntry::new("queue-b", false),
    ]);
    assert_eq!(progressed.process(1).reclaimed, 1);
    let encoded = progressed.persist();
    let substituted = QueueState::reopen(
        &encoded,
        vec![
            QueueEntry::new("queue-a", false),
            QueueEntry::new("queue-x", false),
        ],
    );
    assert_eq!(substituted, Err(AuthError::Integrity));
    let reordered = QueueState::reopen(
        &encoded,
        vec![
            QueueEntry::new("queue-b", false),
            QueueEntry::new("queue-a", false),
        ],
    );
    assert_eq!(reordered, Err(AuthError::Integrity));

    let duplicate_reopen = QueueState::new(vec![
        QueueEntry::new("queue-a", false),
        QueueEntry::new("queue-a", false),
    ]);
    let duplicate_encoded = duplicate_reopen.persist();
    assert_eq!(
        QueueState::reopen(&duplicate_encoded, duplicate_reopen.entries.clone()),
        Err(AuthError::Order)
    );
}

#[test]
fn atomic_publication_gc_rejects_bad_graph_fence_and_root_without_partial_state() {
    let graph = valid_graph();
    let mut state = AtomicW5State::new(vec![QueueEntry::new("atomic", false)]);
    let fence = state.plane.fence.clone();
    let before = state.clone();

    let mut malformed = graph.clone();
    malformed
        .objects
        .objects
        .get_mut("node:global")
        .expect("node")
        .body = b"bad".to_vec();
    assert_eq!(
        state.apply(
            &fence,
            "gc-owner",
            "root:global",
            1,
            &malformed,
            &["root:global"]
        ),
        Err(AuthError::Integrity)
    );
    assert_eq!(state, before);
    assert_eq!(
        state.apply(
            &fence,
            "other-owner",
            "root:global",
            1,
            &graph,
            &["root:global"]
        ),
        Err(AuthError::OwnerMismatch)
    );
    assert_eq!(state, before);
    assert_eq!(
        state.apply(
            &fence,
            "gc-owner",
            "node:global",
            1,
            &graph,
            &["root:global"]
        ),
        Err(AuthError::WrongKind)
    );
    assert_eq!(state, before);

    assert_eq!(
        state.apply(
            &fence,
            "gc-owner",
            "root:global",
            1,
            &graph,
            &["root:global"]
        ),
        Ok(PageResult {
            advanced: true,
            drained: true,
            reclaimed: 1,
        })
    );
    assert_eq!(state.queue.queue_head, 1);
    assert_eq!(state.plane.fence.epoch, fence.epoch + 1);
    assert_eq!(state.plane.fence.progress, fence.progress + 1);
    assert_eq!(state.plane.fence.selector, fence.selector + 1);
    assert_eq!(state.queue.plane, state.plane);
    let after = state.clone();
    assert_eq!(
        state.apply(
            &fence,
            "gc-owner",
            "root:global",
            1,
            &graph,
            &["root:global"]
        ),
        Err(AuthError::StaleEpoch)
    );
    assert_eq!(state, after);
}

#[test]
fn authenticated_selector_and_graph_uses_two_typed_planes() {
    let graph = valid_graph();
    assert_eq!(graph.authenticate_global(7, "owner-a"), Ok(()));
    assert_eq!(graph.authenticate_branch("main", 7, "owner-a"), Ok(()));
}

#[test]
fn graph_corruption_missing_wrong_kind_substitution_cycle_and_stale_owner_fail_closed() {
    let mut missing_selector = valid_graph();
    missing_selector.selectors.global = None;
    assert!(matches!(
        missing_selector.authenticate_global(7, "owner-a"),
        Err(AuthError::Missing {
            space: Space::Selector,
            ..
        })
    ));

    let mut missing_root = valid_graph();
    missing_root
        .selectors
        .global
        .as_mut()
        .expect("global selector")
        .root = "root:missing".into();
    assert!(matches!(
        missing_root.authenticate_global(7, "owner-a"),
        Err(AuthError::Missing {
            space: Space::Object,
            ..
        })
    ));

    let mut wrong_kind = valid_graph();
    wrong_kind
        .objects
        .objects
        .get_mut("root:global")
        .expect("root")
        .kind = ObjectKind::Payload;
    assert_eq!(
        wrong_kind.authenticate_global(7, "owner-a"),
        Err(AuthError::WrongKind)
    );

    let mut substituted = valid_graph();
    substituted
        .objects
        .objects
        .get_mut("root:global")
        .expect("root")
        .id = "root:other".into();
    assert_eq!(
        substituted.authenticate_global(7, "owner-a"),
        Err(AuthError::IdentitySubstituted)
    );

    let mut cyclic = valid_graph();
    cyclic
        .objects
        .objects
        .get_mut("payload:global")
        .expect("payload")
        .refs
        .push("root:global".into());
    let cyclic_payload = cyclic
        .objects
        .objects
        .get_mut("payload:global")
        .expect("payload");
    cyclic_payload.fingerprint = object_fingerprint(
        cyclic_payload.kind,
        &cyclic_payload.refs,
        &cyclic_payload.body,
    );
    assert_eq!(
        cyclic.authenticate_global(7, "owner-a"),
        Err(AuthError::Cycle)
    );
    let mut corrupted_body = valid_graph();
    corrupted_body
        .objects
        .objects
        .get_mut("node:global")
        .expect("node")
        .body = b"substituted".to_vec();
    assert_eq!(
        corrupted_body.authenticate_global(7, "owner-a"),
        Err(AuthError::Integrity)
    );
    assert_eq!(
        valid_graph().authenticate_global(8, "owner-a"),
        Err(AuthError::StaleEpoch)
    );
    assert_eq!(
        valid_graph().authenticate_global(7, "owner-b"),
        Err(AuthError::OwnerMismatch)
    );
}

#[test]
fn publication_first_gc_first_stale_epoch_and_owner_fences_fail_closed() {
    let mut publication_first = SelectorPlaneState::new("owner-a");
    let prepared = publication_first.fence.clone();
    publication_first
        .advance_gc(&prepared, "owner-a")
        .expect("gc advances first");
    assert_eq!(
        publication_first.publish(&prepared, "owner-a", "root:next"),
        Err(FenceError::StaleEpoch)
    );

    let mut gc_first = SelectorPlaneState::new("owner-a");
    let prepared = gc_first.fence.clone();
    gc_first
        .publish(&prepared, "owner-a", "root:next")
        .expect("publication advances first");
    assert_eq!(
        gc_first.advance_gc(&prepared, "owner-a"),
        Err(FenceError::StaleEpoch)
    );

    let current = gc_first.fence.clone();
    assert_eq!(
        gc_first.publish(&current, "owner-b", "root:other"),
        Err(FenceError::OwnerMismatch)
    );
}

#[test]
fn retireable_65_entry_queue_drains_64_then_suffix_exactly() {
    let entries = (0..65)
        .map(|index| QueueEntry::new(&format!("root-{index:02}"), false))
        .collect();
    let mut queue = QueueState::new(entries);
    assert_eq!(
        queue.process(64),
        PageResult {
            advanced: true,
            drained: false,
            reclaimed: 64,
        }
    );
    assert_eq!(queue.queue_head, 64);
    assert_eq!(queue.queue_tail, 65);
    assert_eq!(queue.deleted.len(), 64);
    assert_eq!(queue.plane.fence.epoch, 1);
    assert_eq!(
        queue.process(64),
        PageResult {
            advanced: true,
            drained: true,
            reclaimed: 1,
        }
    );
    assert_eq!(queue.queue_head, 65);
    assert_eq!(queue.deleted.len(), 65);
    assert_eq!(queue.process(64).reclaimed, 0);
}

#[test]
fn blocked_head_keeps_one_debt_without_spin_and_releases_at_safe_point() {
    let mut queue = QueueState::new(vec![
        QueueEntry::new("blocked-source", true),
        QueueEntry::new("released-suffix", false),
    ]);
    assert_eq!(
        queue.process(64),
        PageResult {
            advanced: false,
            drained: false,
            reclaimed: 0,
        }
    );
    assert_eq!(queue.debt_tokens, 1);
    assert_eq!(queue.calls, 1);
    queue.release_blocked_head();
    assert_eq!(queue.process(64).reclaimed, 2);
    assert_eq!(queue.debt_tokens, 1);
    assert_eq!(queue.calls, 2);
}

#[test]
fn poisoned_view_releases_only_its_pin_and_cursor_restart_is_exclusive() {
    let mut readers = ReaderPins::new();
    let first = readers.begin_read("owner-a", "root:global");
    let second = readers.begin_read("owner-b", "root:global");
    assert_eq!(readers.pin_count("root:global"), 2);
    let cursor = Cursor {
        owner: "owner-b".into(),
        read_id: second.id,
        last_key: None,
    };
    assert_eq!(readers.resume(&cursor), Ok("<start>".into()));
    assert_eq!(
        readers.poison("owner-a", first.id, "row-09"),
        Err(ReadError::Malformed)
    );
    assert_eq!(readers.pin_count("root:global"), 1);
    assert_eq!(readers.resume(&cursor), Ok("<start>".into()));
    assert_eq!(
        readers.close("owner-a", second.id),
        Err(ReadError::OwnerMismatch)
    );
    let fresh = readers.begin_read("owner-a", "root:global");
    assert_eq!(
        readers.resume(&Cursor {
            owner: "owner-a".into(),
            read_id: fresh.id,
            last_key: Some("row-09".into()),
        }),
        Err(ReadError::InvalidCursor)
    );
    readers
        .close("owner-b", second.id)
        .expect("second view closes");
    assert_eq!(readers.pin_count("root:global"), 1);
    readers
        .close("owner-a", fresh.id)
        .expect("fresh view closes");
    assert_eq!(readers.pin_count("root:global"), 0);
}

#[test]
fn one_coherent_read_carries_all_operation_events() {
    let read = CoherentRead::open("root:global");
    assert_eq!(read.begin_reads, 1);
    assert_eq!(read.observe(read.view_id, "selector"), Ok(()));
    assert_eq!(read.observe(read.view_id, "queue/object/roots"), Ok(()));
    assert_eq!(
        read.observe(ReadId(2), "selector"),
        Err(ReadError::InvalidCursor)
    );
}

#[test]
fn uploads_shared_and_final_roots_reclaim_only_after_last_owner_release() {
    let mut roots = RootOwners::new();
    roots.retain("root:shared", "branch");
    roots.retain("root:shared", "upload");
    roots.retain("root:final", "branch");
    roots
        .release("root:shared", "branch")
        .expect("branch release");
    assert_eq!(roots.reclaim_released(), 0);
    assert_eq!(
        roots.release("root:shared", "other"),
        Err(AuthError::OwnerMismatch)
    );
    roots
        .release("root:shared", "upload")
        .expect("upload release");
    assert_eq!(roots.reclaim_released(), 1);
    roots
        .release("root:final", "branch")
        .expect("final release");
    assert_eq!(roots.reclaim_released(), 1);
}

#[test]
fn cold_reopen_reauthenticates_persisted_queue_and_fence() {
    let mut queue = QueueState::new(vec![
        QueueEntry::new("reopen-root", false),
        QueueEntry::new("reopen-tail", false),
    ]);
    assert_eq!(queue.process(1).reclaimed, 1);
    let encoded = queue.persist();
    let reopened = QueueState::reopen(
        &encoded,
        vec![
            QueueEntry::new("reopen-root", false),
            QueueEntry::new("reopen-tail", false),
        ],
    )
    .expect("authenticated persisted queue");
    assert_eq!(reopened.queue_head, 1);
    assert_eq!(reopened.queue_tail, 2);
    assert_eq!(reopened.deleted.len(), 1);
    assert_eq!(reopened.plane.fence.epoch, 1);
    assert!(QueueState::reopen("1|1|0|wrong-owner|root:global|1|2|1", Vec::new()).is_err());
}
