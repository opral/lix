// TEST/REPORT-ONLY executable model bound to exact b59.
// It is intentionally independent of Lix production modules and is not
// compiled or run by the source verifier.
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq)]
struct CommitRef {
    id: String,
    generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ObjectKind {
    Commit,
    CommitCatalog,
    Root,
    Member,
    Payload,
    PluginRegistry,
    FileOwner,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ReadError {
    Missing { kind: ObjectKind, id: String },
    WrongKind { expected: ObjectKind, id: String },
    Malformed(String),
    IdentityMismatch { expected: String, observed: String },
    BindingMismatch(String),
    GenerationMismatch { expected: u64, observed: u64 },
    ReadOwnerMismatch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Cell {
    Null,
    Value { payload_id: String, digest: String },
    Tombstone,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Row {
    cell: Cell,
    metadata: Option<String>,
    file_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CommitRecord {
    id: String,
    generation: u64,
    parents: Vec<String>,
    root_id: String,
    catalog_id: String,
    registry_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CommitCatalog {
    id: String,
    commit_id: String,
    members: BTreeMap<String, String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Root {
    id: String,
    commit_id: String,
    generation: u64,
    catalog_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Member {
    id: String,
    commit_id: String,
    key: String,
    row: Row,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Payload {
    id: String,
    member_id: String,
    digest: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct PluginRegistry {
    id: String,
    commit_id: String,
    owners: BTreeMap<String, String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct FileOwner {
    id: String,
    commit_id: String,
    file_id: String,
    plugin: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Stored {
    Commit(CommitRecord),
    CommitCatalog(CommitCatalog),
    Root(Root),
    Member(Member),
    Payload(Payload),
    PluginRegistry(PluginRegistry),
    FileOwner(FileOwner),
}
impl Stored {
    fn kind(&self) -> ObjectKind {
        match self {
            Self::Commit(_) => ObjectKind::Commit,
            Self::CommitCatalog(_) => ObjectKind::CommitCatalog,
            Self::Root(_) => ObjectKind::Root,
            Self::Member(_) => ObjectKind::Member,
            Self::Payload(_) => ObjectKind::Payload,
            Self::PluginRegistry(_) => ObjectKind::PluginRegistry,
            Self::FileOwner(_) => ObjectKind::FileOwner,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ObjectStore {
    objects: BTreeMap<String, Stored>,
}
impl ObjectStore {
    fn put(&mut self, id: impl Into<String>, object: Stored) {
        self.objects.insert(id.into(), object);
    }
    fn remove(&mut self, id: &str) {
        self.objects.remove(id);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ReadIdentity {
    reader_instance: u64,
    view_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReadEvent {
    identity: ReadIdentity,
    object: String,
    kind: ObjectKind,
}

// Exactly one retained read/view identity belongs to the caller.
// MergeOperation borrows it; it has no begin_read, refresh, clone, extraction,
// or detached cache.
struct RetainedStorageRead {
    identity: ReadIdentity,
    store: ObjectStore,
    events: Vec<ReadEvent>,
}
impl RetainedStorageRead {
    fn new(reader_instance: u64, view_id: u64, store: ObjectStore) -> Self {
        Self {
            identity: ReadIdentity {
                reader_instance,
                view_id,
            },
            store,
            events: Vec::new(),
        }
    }
    fn object(&mut self, id: &str, kind: ObjectKind) -> Result<Stored, ReadError> {
        self.events.push(ReadEvent {
            identity: self.identity,
            object: id.to_owned(),
            kind,
        });
        let object = self
            .store
            .objects
            .get(id)
            .cloned()
            .ok_or_else(|| ReadError::Missing {
                kind,
                id: id.to_owned(),
            })?;
        if object.kind() != kind {
            return Err(ReadError::WrongKind {
                expected: kind,
                id: id.to_owned(),
            });
        }
        Ok(object)
    }
    fn assert_one_owner(&self) -> Result<(), ReadError> {
        if self
            .events
            .iter()
            .all(|event| event.identity == self.identity)
        {
            Ok(())
        } else {
            Err(ReadError::ReadOwnerMismatch)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Loaded {
    commit: CommitRecord,
    rows: BTreeMap<String, Row>,
    owners: BTreeMap<String, FileOwner>,
}
fn identity(expected: &str, observed: &str) -> ReadError {
    ReadError::IdentityMismatch {
        expected: expected.to_owned(),
        observed: observed.to_owned(),
    }
}
fn load_commit(
    read: &mut RetainedStorageRead,
    expected: &CommitRef,
) -> Result<CommitRecord, ReadError> {
    let Stored::Commit(commit) = read.object(&expected.id, ObjectKind::Commit)? else {
        unreachable!()
    };
    if commit.id != expected.id {
        return Err(identity(&expected.id, &commit.id));
    }
    if commit.generation != expected.generation {
        return Err(ReadError::GenerationMismatch {
            expected: expected.generation,
            observed: commit.generation,
        });
    }
    Ok(commit)
}

fn load_state(read: &mut RetainedStorageRead, expected: &CommitRef) -> Result<Loaded, ReadError> {
    let commit = load_commit(read, expected)?;
    let Stored::Root(root) = read.object(&commit.root_id, ObjectKind::Root)? else {
        unreachable!()
    };
    if root.id != commit.root_id {
        return Err(identity(&commit.root_id, &root.id));
    }
    if root.commit_id != commit.id || root.generation != commit.generation {
        return Err(ReadError::BindingMismatch(format!("root {}", root.id)));
    }
    if root.catalog_id != commit.catalog_id {
        return Err(ReadError::BindingMismatch("root/catalog".to_owned()));
    }
    let Stored::CommitCatalog(catalog) =
        read.object(&commit.catalog_id, ObjectKind::CommitCatalog)?
    else {
        unreachable!()
    };
    if catalog.id != commit.catalog_id {
        return Err(identity(&commit.catalog_id, &catalog.id));
    }
    if catalog.commit_id != commit.id {
        return Err(ReadError::BindingMismatch(format!(
            "catalog {}",
            catalog.id
        )));
    }
    let Stored::PluginRegistry(registry) =
        read.object(&commit.registry_id, ObjectKind::PluginRegistry)?
    else {
        unreachable!()
    };
    if registry.id != commit.registry_id || registry.commit_id != commit.id {
        return Err(ReadError::BindingMismatch(format!(
            "registry {}",
            registry.id
        )));
    }
    let mut rows = BTreeMap::new();
    for (key, member_id) in &catalog.members {
        if key.is_empty() || key.contains('\0') {
            return Err(ReadError::Malformed("catalog key".to_owned()));
        }
        let Stored::Member(member) = read.object(member_id, ObjectKind::Member)? else {
            unreachable!()
        };
        if member.id != *member_id {
            return Err(identity(member_id, &member.id));
        }
        if member.commit_id != commit.id || member.key != *key {
            return Err(ReadError::BindingMismatch(format!("member {}", member.id)));
        }
        let mut row = member.row;
        if let Cell::Value { payload_id, .. } = &row.cell {
            let Stored::Payload(payload) = read.object(payload_id, ObjectKind::Payload)? else {
                unreachable!()
            };
            if payload.id != *payload_id || payload.member_id != member.id {
                return Err(identity(payload_id, &payload.id));
            }
            if payload.digest.is_empty() {
                return Err(ReadError::Malformed("empty payload digest".to_owned()));
            }
            row.cell = Cell::Value {
                payload_id: payload.id,
                digest: payload.digest,
            };
        }
        rows.insert(key.clone(), row);
    }
    let mut owners = BTreeMap::new();
    for (file_id, owner_id) in &registry.owners {
        let Stored::FileOwner(owner) = read.object(owner_id, ObjectKind::FileOwner)? else {
            unreachable!()
        };
        if owner.id != *owner_id {
            return Err(identity(owner_id, &owner.id));
        }
        if owner.commit_id != commit.id || owner.file_id != *file_id {
            return Err(ReadError::BindingMismatch(format!(
                "file owner {}",
                owner.id
            )));
        }
        owners.insert(file_id.clone(), owner);
    }
    Ok(Loaded {
        commit,
        rows,
        owners,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ChangeKind {
    Added,
    Updated,
    Deleted,
    Unchanged,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Change {
    key: String,
    kind: ChangeKind,
    before: Option<Row>,
    after: Row,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Conflict {
    key: String,
    target: Change,
    source: Change,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct PluginHandoff {
    file_id: String,
    source_owner: String,
    target_owner: String,
    source_plugin: String,
    target_plugin: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct MergeRequest {
    merge_base: CommitRef,
    base: CommitRef,
    source: CommitRef,
    target: CommitRef,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct MergeIdentities {
    merge_base: CommitRef,
    base: CommitRef,
    source: CommitRef,
    target: CommitRef,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct MergeResult {
    identities: MergeIdentities,
    source_picks: Vec<Change>,
    unchanged: Vec<Change>,
    conflicts: Vec<Conflict>,
    plugin_handoffs: Vec<PluginHandoff>,
    read_identity: ReadIdentity,
}

fn semantic_equal(left: &Row, right: &Row) -> bool {
    let cells = match (&left.cell, &right.cell) {
        (Cell::Null, Cell::Null) | (Cell::Tombstone, Cell::Tombstone) => true,
        (Cell::Value { digest: a, .. }, Cell::Value { digest: b, .. }) => a == b,
        _ => false,
    };
    cells && left.metadata == right.metadata && left.file_id == right.file_id
}
fn classify(
    base: Option<&Row>,
    side: Option<&Row>,
    key: &str,
) -> Result<Option<Change>, ReadError> {
    let Some(side) = side else {
        return if base.is_some() {
            Err(ReadError::Malformed(format!("missing tombstone {key}")))
        } else {
            Ok(None)
        };
    };
    let kind = match base {
        None => ChangeKind::Added,
        Some(before) if semantic_equal(before, side) => ChangeKind::Unchanged,
        Some(_) if side.cell == Cell::Tombstone => ChangeKind::Deleted,
        Some(_) => ChangeKind::Updated,
    };
    Ok(Some(Change {
        key: key.to_owned(),
        kind,
        before: base.cloned(),
        after: side.clone(),
    }))
}

struct MergeOperation<'a> {
    read: &'a mut RetainedStorageRead,
}
impl<'a> MergeOperation<'a> {
    fn new(read: &'a mut RetainedStorageRead) -> Self {
        Self { read }
    }
    fn analyze(&mut self, request: &MergeRequest) -> Result<MergeResult, ReadError> {
        let merge_base = load_state(self.read, &request.merge_base)?;
        let base = load_state(self.read, &request.base)?;
        let source = load_state(self.read, &request.source)?;
        let target = load_state(self.read, &request.target)?;
        if !base.commit.parents.contains(&merge_base.commit.id)
            || !source.commit.parents.contains(&base.commit.id)
            || !target.commit.parents.contains(&base.commit.id)
        {
            return Err(ReadError::BindingMismatch(
                "commit identities/generations".to_owned(),
            ));
        }
        let mut keys = BTreeSet::new();
        keys.extend(base.rows.keys().cloned());
        keys.extend(source.rows.keys().cloned());
        keys.extend(target.rows.keys().cloned());
        let mut source_picks = Vec::new();
        let mut unchanged = Vec::new();
        let mut conflicts = Vec::new();
        for key in keys {
            let Some(source_change) = classify(base.rows.get(&key), source.rows.get(&key), &key)?
            else {
                continue;
            };
            let target_change = classify(base.rows.get(&key), target.rows.get(&key), &key)?;
            if source_change.kind == ChangeKind::Unchanged {
                unchanged.push(source_change);
                continue;
            }
            let Some(target_change) = target_change else {
                source_picks.push(source_change);
                continue;
            };
            if target_change.kind == ChangeKind::Unchanged {
                source_picks.push(source_change);
            } else if !semantic_equal(&source_change.after, &target_change.after) {
                conflicts.push(Conflict {
                    key,
                    target: target_change,
                    source: source_change,
                });
            }
        }
        let mut plugin_handoffs = Vec::new();
        for (file_id, source_owner_id) in &source.owners {
            if let Some(target_owner_id) = target.owners.get(file_id) {
                let source_owner = source.owners.get(file_id).expect("loaded source owner");
                let target_owner = target.owners.get(file_id).expect("loaded target owner");
                plugin_handoffs.push(PluginHandoff {
                    file_id: file_id.clone(),
                    source_owner: source_owner_id.id.clone(),
                    target_owner: target_owner_id.id.clone(),
                    source_plugin: source_owner.plugin.clone(),
                    target_plugin: target_owner.plugin.clone(),
                });
            }
        }
        Ok(MergeResult {
            identities: MergeIdentities {
                merge_base: request.merge_base.clone(),
                base: request.base.clone(),
                source: request.source.clone(),
                target: request.target.clone(),
            },
            source_picks,
            unchanged,
            conflicts,
            plugin_handoffs,
            read_identity: self.read.identity,
        })
    }
}

#[derive(Clone)]
enum SeedCell {
    Null,
    Value(&'static str),
    Tombstone,
}
#[derive(Clone)]
struct Seed {
    cell: SeedCell,
    metadata: Option<&'static str>,
    file_id: Option<&'static str>,
}
fn build_commit(
    store: &mut ObjectStore,
    id: &str,
    generation: u64,
    parents: Vec<String>,
    seeds: BTreeMap<String, Seed>,
    plugin: &str,
) -> CommitRef {
    let root_id = format!("root-{id}");
    let catalog_id = format!("catalog-{id}");
    let registry_id = format!("registry-{id}");
    let mut members = BTreeMap::new();
    let mut owners = BTreeMap::new();
    for (key, seed) in seeds {
        let member_id = format!("member-{id}-{key}");
        let cell = match seed.cell {
            SeedCell::Null => Cell::Null,
            SeedCell::Tombstone => Cell::Tombstone,
            SeedCell::Value(digest) => {
                let payload_id = format!("payload-{id}-{key}");
                store.put(
                    payload_id.clone(),
                    Stored::Payload(Payload {
                        id: payload_id.clone(),
                        member_id: member_id.clone(),
                        digest: digest.to_owned(),
                    }),
                );
                Cell::Value {
                    payload_id,
                    digest: String::new(),
                }
            }
        };
        let row = Row {
            cell,
            metadata: seed.metadata.map(str::to_owned),
            file_id: seed.file_id.map(str::to_owned),
        };
        store.put(
            member_id.clone(),
            Stored::Member(Member {
                id: member_id.clone(),
                commit_id: id.to_owned(),
                key: key.clone(),
                row,
            }),
        );
        members.insert(key, member_id);
        if let Some(file_id) = seed.file_id {
            let owner_id = format!("owner-{id}-{file_id}");
            store.put(
                owner_id.clone(),
                Stored::FileOwner(FileOwner {
                    id: owner_id.clone(),
                    commit_id: id.to_owned(),
                    file_id: file_id.to_owned(),
                    plugin: plugin.to_owned(),
                }),
            );
            owners.insert(file_id.to_owned(), owner_id);
        }
    }
    store.put(
        catalog_id.clone(),
        Stored::CommitCatalog(CommitCatalog {
            id: catalog_id.clone(),
            commit_id: id.to_owned(),
            members,
        }),
    );
    store.put(
        registry_id.clone(),
        Stored::PluginRegistry(PluginRegistry {
            id: registry_id.clone(),
            commit_id: id.to_owned(),
            owners,
        }),
    );
    store.put(
        root_id.clone(),
        Stored::Root(Root {
            id: root_id.clone(),
            commit_id: id.to_owned(),
            generation,
            catalog_id: catalog_id.clone(),
        }),
    );
    store.put(
        id,
        Stored::Commit(CommitRecord {
            id: id.to_owned(),
            generation,
            parents,
            root_id,
            catalog_id,
            registry_id,
        }),
    );
    CommitRef {
        id: id.to_owned(),
        generation,
    }
}
fn base_rows() -> BTreeMap<String, Seed> {
    BTreeMap::from([
        (
            "a".to_owned(),
            Seed {
                cell: SeedCell::Value("base-a"),
                metadata: Some("base"),
                file_id: Some("file-1"),
            },
        ),
        (
            "b".to_owned(),
            Seed {
                cell: SeedCell::Value("base-b"),
                metadata: None,
                file_id: None,
            },
        ),
        (
            "n".to_owned(),
            Seed {
                cell: SeedCell::Null,
                metadata: None,
                file_id: None,
            },
        ),
    ])
}
fn fixture() -> (ObjectStore, MergeRequest) {
    let mut store = ObjectStore::default();
    let merge_base = build_commit(&mut store, "M", 10, vec![], base_rows(), "plugin-m");
    let base = build_commit(
        &mut store,
        "B",
        20,
        vec![merge_base.id.clone()],
        base_rows(),
        "plugin-b",
    );
    let mut source_rows = base_rows();
    source_rows.insert(
        "a".to_owned(),
        Seed {
            cell: SeedCell::Value("source-a"),
            metadata: Some("source"),
            file_id: Some("file-1"),
        },
    );
    source_rows.insert(
        "b".to_owned(),
        Seed {
            cell: SeedCell::Value("source-b"),
            metadata: None,
            file_id: None,
        },
    );
    source_rows.insert(
        "c".to_owned(),
        Seed {
            cell: SeedCell::Value("added-c"),
            metadata: Some("new"),
            file_id: Some("file-2"),
        },
    );
    let source = build_commit(
        &mut store,
        "S",
        21,
        vec![base.id.clone()],
        source_rows,
        "plugin-source",
    );
    let mut target_rows = base_rows();
    target_rows.insert(
        "a".to_owned(),
        Seed {
            cell: SeedCell::Value("target-a"),
            metadata: Some("target"),
            file_id: Some("file-1"),
        },
    );
    target_rows.insert(
        "b".to_owned(),
        Seed {
            cell: SeedCell::Tombstone,
            metadata: None,
            file_id: None,
        },
    );
    let target = build_commit(
        &mut store,
        "T",
        22,
        vec![base.id.clone()],
        target_rows,
        "plugin-target",
    );
    (
        store,
        MergeRequest {
            merge_base,
            base,
            source,
            target,
        },
    )
}
fn run(store: ObjectStore, request: MergeRequest) -> Result<MergeResult, ReadError> {
    let mut read = RetainedStorageRead::new(1, 100, store);
    let result = MergeOperation::new(&mut read).analyze(&request);
    read.assert_one_owner().expect("one retained StorageRead");
    result
}
fn commit(store: &ObjectStore, id: &str) -> CommitRecord {
    match store.objects.get(id).expect("fixture commit") {
        Stored::Commit(c) => c.clone(),
        _ => unreachable!(),
    }
}

fn main() {
    let (store, request) = fixture();
    let mut read = RetainedStorageRead::new(1, 100, store.clone());
    let result = MergeOperation::new(&mut read)
        .analyze(&request)
        .expect("valid merge");
    read.assert_one_owner().expect("one owner for every read");
    assert_eq!(
        result.read_identity,
        ReadIdentity {
            reader_instance: 1,
            view_id: 100
        }
    );
    assert_eq!(result.identities.merge_base, request.merge_base);
    assert_eq!(result.identities.base, request.base);
    assert_eq!(result.identities.source, request.source);
    assert_eq!(result.identities.target, request.target);
    assert!(result
        .source_picks
        .iter()
        .any(|c| c.kind == ChangeKind::Added && c.key == "c"));
    assert!(result
        .conflicts
        .iter()
        .any(|c| c.key == "a" && c.source.kind == ChangeKind::Updated));
    assert!(result
        .conflicts
        .iter()
        .any(|c| c.key == "b" && c.target.kind == ChangeKind::Deleted));
    assert!(result
        .unchanged
        .iter()
        .any(|c| c.key == "n" && c.kind == ChangeKind::Unchanged));
    assert!(result.plugin_handoffs.iter().any(|h| h.file_id == "file-1"
        && h.source_plugin == "plugin-source"
        && h.target_plugin == "plugin-target"));

    // Equal semantic values with distinct payload IDs converge without conflict.
    let mut convergent = store.clone();
    let mut equal_source = base_rows();
    equal_source.insert(
        "a".to_owned(),
        Seed {
            cell: SeedCell::Value("same-value"),
            metadata: Some("same"),
            file_id: Some("file-1"),
        },
    );
    let source = build_commit(
        &mut convergent,
        "CS",
        21,
        vec![request.base.id.clone()],
        equal_source,
        "plugin-source",
    );
    let mut equal_target = base_rows();
    equal_target.insert(
        "a".to_owned(),
        Seed {
            cell: SeedCell::Value("same-value"),
            metadata: Some("same"),
            file_id: Some("file-1"),
        },
    );
    let target = build_commit(
        &mut convergent,
        "CT",
        22,
        vec![request.base.id.clone()],
        equal_target,
        "plugin-target",
    );
    let convergent_result = run(
        convergent,
        MergeRequest {
            source,
            target,
            ..request.clone()
        },
    )
    .expect("convergent merge succeeds");
    assert!(convergent_result.conflicts.iter().all(|c| c.key != "a"));

    // A truly disjoint merge: source adds c while target independently adds d.
    let mut disjoint_store = store.clone();
    let mut disjoint_source_rows = base_rows();
    disjoint_source_rows.insert(
        "c".to_owned(),
        Seed {
            cell: SeedCell::Value("source-only"),
            metadata: None,
            file_id: None,
        },
    );
    let disjoint_source = build_commit(
        &mut disjoint_store,
        "DS",
        21,
        vec![request.base.id.clone()],
        disjoint_source_rows,
        "plugin-source",
    );
    let mut disjoint_target_rows = base_rows();
    disjoint_target_rows.insert(
        "d".to_owned(),
        Seed {
            cell: SeedCell::Value("target-only"),
            metadata: None,
            file_id: None,
        },
    );
    let disjoint_target = build_commit(
        &mut disjoint_store,
        "DT",
        22,
        vec![request.base.id.clone()],
        disjoint_target_rows,
        "plugin-target",
    );
    let disjoint_result = run(
        disjoint_store,
        MergeRequest {
            source: disjoint_source,
            target: disjoint_target,
            ..request.clone()
        },
    )
    .expect("disjoint merge succeeds");
    assert_eq!(
        disjoint_result
            .source_picks
            .iter()
            .map(|change| change.key.as_str())
            .collect::<Vec<_>>(),
        vec!["c"]
    );
    assert!(
        disjoint_result.conflicts.is_empty(),
        "disjoint changes must not conflict"
    );

    // Missing, malformed, wrong-kind, and identity-substituted authority fail closed.
    let mut missing_catalog = store.clone();
    let catalog_id = commit(&missing_catalog, "B").catalog_id;
    missing_catalog.remove(&catalog_id);
    assert!(matches!(
        run(missing_catalog, request.clone()),
        Err(ReadError::Missing {
            kind: ObjectKind::CommitCatalog,
            ..
        })
    ));
    let mut substituted_catalog = store.clone();
    let catalog_id = commit(&substituted_catalog, "S").catalog_id;
    substituted_catalog.put(
        catalog_id.clone(),
        Stored::CommitCatalog(CommitCatalog {
            id: "substituted-catalog".to_owned(),
            commit_id: "S".to_owned(),
            members: BTreeMap::new(),
        }),
    );
    assert!(matches!(
        run(substituted_catalog, request.clone()),
        Err(ReadError::IdentityMismatch { .. })
    ));
    let mut wrong_catalog = store.clone();
    let catalog_id = commit(&wrong_catalog, "T").catalog_id;
    wrong_catalog.put(
        catalog_id.clone(),
        Stored::Root(Root {
            id: catalog_id,
            commit_id: "T".to_owned(),
            generation: 22,
            catalog_id: "catalog-T".to_owned(),
        }),
    );
    assert!(matches!(
        run(wrong_catalog, request.clone()),
        Err(ReadError::WrongKind {
            expected: ObjectKind::CommitCatalog,
            ..
        })
    ));
    let mut wrong_registry = store.clone();
    let registry_id = commit(&wrong_registry, "S").registry_id;
    wrong_registry.put(
        registry_id.clone(),
        Stored::Root(Root {
            id: registry_id,
            commit_id: "S".to_owned(),
            generation: 21,
            catalog_id: "catalog-S".to_owned(),
        }),
    );
    assert!(matches!(
        run(wrong_registry, request.clone()),
        Err(ReadError::WrongKind {
            expected: ObjectKind::PluginRegistry,
            ..
        })
    ));
    let mut bad_generation = request.clone();
    bad_generation.source.generation += 1;
    assert!(matches!(
        run(store.clone(), bad_generation),
        Err(ReadError::GenerationMismatch { .. })
    ));
    let mut wrong_root = store.clone();
    let source_commit = commit(&wrong_root, "S");
    wrong_root.put(
        source_commit.root_id.clone(),
        Stored::Payload(Payload {
            id: source_commit.root_id,
            member_id: "wrong".to_owned(),
            digest: "x".to_owned(),
        }),
    );
    assert!(matches!(
        run(wrong_root, request.clone()),
        Err(ReadError::WrongKind {
            expected: ObjectKind::Root,
            ..
        })
    ));
    let mut missing_root = store.clone();
    let root_id = commit(&missing_root, "S").root_id;
    missing_root.remove(&root_id);
    assert!(matches!(
        run(missing_root, request.clone()),
        Err(ReadError::Missing {
            kind: ObjectKind::Root,
            ..
        })
    ));
    let mut malformed_root = store.clone();
    let source_commit = commit(&malformed_root, "S");
    malformed_root.put(
        source_commit.root_id.clone(),
        Stored::Root(Root {
            id: source_commit.root_id,
            commit_id: "wrong-commit".to_owned(),
            generation: source_commit.generation,
            catalog_id: source_commit.catalog_id,
        }),
    );
    assert!(matches!(
        run(malformed_root, request.clone()),
        Err(ReadError::BindingMismatch(_))
    ));
    let mut substituted_root = store.clone();
    let source_commit = commit(&substituted_root, "S");
    substituted_root.put(
        source_commit.root_id.clone(),
        Stored::Root(Root {
            id: "substituted-root".to_owned(),
            commit_id: source_commit.id,
            generation: source_commit.generation,
            catalog_id: source_commit.catalog_id,
        }),
    );
    assert!(matches!(
        run(substituted_root, request.clone()),
        Err(ReadError::IdentityMismatch { .. })
    ));
    let mut malformed_catalog = store.clone();
    let catalog_id = commit(&malformed_catalog, "S").catalog_id;
    let Stored::CommitCatalog(mut catalog) = malformed_catalog.objects.remove(&catalog_id).unwrap()
    else {
        unreachable!()
    };
    catalog
        .members
        .insert(String::new(), "member-S-a".to_owned());
    malformed_catalog.put(catalog_id, Stored::CommitCatalog(catalog));
    assert!(matches!(
        run(malformed_catalog, request.clone()),
        Err(ReadError::Malformed(_))
    ));
    let mut missing_member = store.clone();
    missing_member.remove("member-S-a");
    assert!(matches!(
        run(missing_member, request.clone()),
        Err(ReadError::Missing {
            kind: ObjectKind::Member,
            ..
        })
    ));
    let mut malformed_member = store.clone();
    malformed_member.put(
        "member-S-a",
        Stored::Member(Member {
            id: "member-S-a".to_owned(),
            commit_id: "S".to_owned(),
            key: "wrong-key".to_owned(),
            row: Row {
                cell: Cell::Tombstone,
                metadata: None,
                file_id: None,
            },
        }),
    );
    assert!(matches!(
        run(malformed_member, request.clone()),
        Err(ReadError::BindingMismatch(_))
    ));
    let mut substituted_member = store.clone();
    substituted_member.put(
        "member-S-a",
        Stored::Member(Member {
            id: "substituted-member".to_owned(),
            commit_id: "S".to_owned(),
            key: "a".to_owned(),
            row: Row {
                cell: Cell::Value {
                    payload_id: "payload-S-a".to_owned(),
                    digest: String::new(),
                },
                metadata: None,
                file_id: None,
            },
        }),
    );
    assert!(matches!(
        run(substituted_member, request.clone()),
        Err(ReadError::IdentityMismatch { .. })
    ));
    let mut wrong_member = store.clone();
    wrong_member.put(
        "member-S-a",
        Stored::Root(Root {
            id: "member-S-a".to_owned(),
            commit_id: "S".to_owned(),
            generation: 21,
            catalog_id: "catalog-S".to_owned(),
        }),
    );
    assert!(matches!(
        run(wrong_member, request.clone()),
        Err(ReadError::WrongKind {
            expected: ObjectKind::Member,
            ..
        })
    ));
    let mut missing_payload = store.clone();
    missing_payload.remove("payload-S-a");
    assert!(matches!(
        run(missing_payload, request.clone()),
        Err(ReadError::Missing {
            kind: ObjectKind::Payload,
            ..
        })
    ));
    let mut wrong_payload = store.clone();
    wrong_payload.put(
        "payload-S-a",
        Stored::Member(Member {
            id: "payload-S-a".to_owned(),
            commit_id: "S".to_owned(),
            key: "a".to_owned(),
            row: Row {
                cell: Cell::Null,
                metadata: None,
                file_id: None,
            },
        }),
    );
    assert!(matches!(
        run(wrong_payload, request.clone()),
        Err(ReadError::WrongKind {
            expected: ObjectKind::Payload,
            ..
        })
    ));
    let mut malformed_payload = store.clone();
    malformed_payload.put(
        "payload-S-a",
        Stored::Payload(Payload {
            id: "payload-S-a".to_owned(),
            member_id: "member-S-a".to_owned(),
            digest: String::new(),
        }),
    );
    assert!(matches!(
        run(malformed_payload, request.clone()),
        Err(ReadError::Malformed(_))
    ));
    let mut substituted_payload = store.clone();
    substituted_payload.put(
        "payload-S-a",
        Stored::Payload(Payload {
            id: "other-payload".to_owned(),
            member_id: "member-S-a".to_owned(),
            digest: "source-a".to_owned(),
        }),
    );
    assert!(matches!(
        run(substituted_payload, request.clone()),
        Err(ReadError::IdentityMismatch { .. })
    ));
    let mut missing_file_owner = store.clone();
    missing_file_owner.remove("owner-S-file-1");
    assert!(matches!(
        run(missing_file_owner, request.clone()),
        Err(ReadError::Missing {
            kind: ObjectKind::FileOwner,
            ..
        })
    ));
    let mut malformed_file_owner = store.clone();
    malformed_file_owner.put(
        "owner-S-file-1",
        Stored::FileOwner(FileOwner {
            id: "owner-S-file-1".to_owned(),
            commit_id: "S".to_owned(),
            file_id: "wrong-file".to_owned(),
            plugin: "plugin-source".to_owned(),
        }),
    );
    assert!(matches!(
        run(malformed_file_owner, request.clone()),
        Err(ReadError::BindingMismatch(_))
    ));
    let mut wrong_file_owner = store.clone();
    wrong_file_owner.put(
        "owner-S-file-1",
        Stored::Payload(Payload {
            id: "owner-S-file-1".to_owned(),
            member_id: "wrong".to_owned(),
            digest: "wrong-kind".to_owned(),
        }),
    );
    assert!(matches!(
        run(wrong_file_owner, request.clone()),
        Err(ReadError::WrongKind {
            expected: ObjectKind::FileOwner,
            ..
        })
    ));
    let mut substituted_file_owner = store.clone();
    substituted_file_owner.put(
        "owner-S-file-1",
        Stored::FileOwner(FileOwner {
            id: "substituted-owner".to_owned(),
            commit_id: "S".to_owned(),
            file_id: "file-1".to_owned(),
            plugin: "plugin-source".to_owned(),
        }),
    );
    assert!(matches!(
        run(substituted_file_owner, request),
        Err(ReadError::IdentityMismatch { .. })
    ));
}

#[cfg(test)]
mod read_identity_tests {
    use super::*;

    #[test]
    fn separate_reader_instances_and_view_ids_are_not_interchangeable() {
        let (store, _) = fixture();
        let mut retained = RetainedStorageRead::new(11, 110, store.clone());
        let mut foreign = RetainedStorageRead::new(22, 220, store);
        retained
            .object("M", ObjectKind::Commit)
            .expect("retained read");
        foreign
            .object("M", ObjectKind::Commit)
            .expect("foreign read");
        retained.events.extend(foreign.events.clone());
        assert_eq!(
            retained.assert_one_owner(),
            Err(ReadError::ReadOwnerMismatch)
        );
        assert_ne!(retained.identity, foreign.identity);
    }

    #[test]
    fn merge_result_preserves_actual_reader_and_view_identity() {
        let (store, request) = fixture();
        let mut retained = RetainedStorageRead::new(31, 310, store);
        let result = MergeOperation::new(&mut retained)
            .analyze(&request)
            .expect("valid merge");
        assert_eq!(
            result.read_identity,
            ReadIdentity {
                reader_instance: 31,
                view_id: 310
            }
        );
        retained
            .assert_one_owner()
            .expect("all events use the actual retained identity");
    }
}
