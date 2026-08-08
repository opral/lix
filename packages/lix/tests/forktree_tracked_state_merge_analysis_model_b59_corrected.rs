// TEST/REPORT-ONLY typed merge-analysis model bound to exact b59.
// It is independent of Lix production modules and is not executed by the
// source-only verifier. Its main function is a future standalone model gate.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CommitRef {
    id: String,
    generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
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
    Malformed { detail: String },
    IdentityMismatch { expected: String, observed: String },
    BindingMismatch { detail: String },
    GenerationMismatch { id: String, expected: u64, observed: u64 },
    MissingTombstone { key: String },
    ReadOwnerMismatch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Cell {
    Null,
    Value {
        payload_id: String,
        digest: String,
    },
    Tombstone,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SemanticRow {
    cell: Cell,
    metadata: Option<String>,
    file_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CommitRecord {
    id: String,
    generation: u64,
    parent_ids: Vec<String>,
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
struct RootRecord {
    id: String,
    commit_id: String,
    generation: u64,
    catalog_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MemberRecord {
    id: String,
    commit_id: String,
    key: String,
    row: SemanticRow,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PayloadRecord {
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
    plugin_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum StoredObject {
    Commit(CommitRecord),
    CommitCatalog(CommitCatalog),
    Root(RootRecord),
    Member(MemberRecord),
    Payload(PayloadRecord),
    PluginRegistry(PluginRegistry),
    FileOwner(FileOwner),
}

impl StoredObject {
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
    objects: BTreeMap<String, StoredObject>,
}

impl ObjectStore {
    fn put(&mut self, id: String, object: StoredObject) {
        self.objects.insert(id, object);
    }

    fn remove(&mut self, id: &str) {
        self.objects.remove(id);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ReadEvent {
    owner_id: String,
    object_id: String,
    kind: ObjectKind,
}

struct RetainedStorageRead {
    owner_id: String,
    store: ObjectStore,
    events: Vec<ReadEvent>,
}

impl RetainedStorageRead {
    // This is caller-owned. MergeOperation only borrows &mut this value; it
    // has no begin_read, refresh, clone, extraction, or retry operation.
    fn new(owner_id: &str, store: ObjectStore) -> Self {
        Self {
            owner_id: owner_id.to_owned(),
            store,
            events: Vec::new(),
        }
    }

    fn object(&mut self, id: &str, expected: ObjectKind) -> Result<StoredObject, ReadError> {
        self.events.push(ReadEvent {
            owner_id: self.owner_id.clone(),
            object_id: id.to_owned(),
            kind: expected,
        });
        let object = self
            .store
            .objects
            .get(id)
            .cloned()
            .ok_or_else(|| ReadError::Missing {
                kind: expected,
                id: id.to_owned(),
            })?;
        if object.kind() != expected {
            return Err(ReadError::WrongKind {
                expected,
                id: id.to_owned(),
            });
        }
        Ok(object)
    }

    fn assert_one_owner(&self) -> Result<(), ReadError> {
        if self.events.iter().all(|event| event.owner_id == self.owner_id) {
            Ok(())
        } else {
            Err(ReadError::ReadOwnerMismatch)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LoadedState {
    commit: CommitRecord,
    rows: BTreeMap<String, SemanticRow>,
    owners: BTreeMap<String, FileOwner>,
    registry_id: String,
}

fn identity_mismatch(expected: &str, observed: &str) -> ReadError {
    ReadError::IdentityMismatch {
        expected: expected.to_owned(),
        observed: observed.to_owned(),
    }
}

fn load_commit(
    read: &mut RetainedStorageRead,
    expected: &CommitRef,
) -> Result<CommitRecord, ReadError> {
    let object = read.object(&expected.id, ObjectKind::Commit)?;
    let StoredObject::Commit(commit) = object else {
        unreachable!("object kind was checked");
    };
    if commit.id != expected.id {
        return Err(identity_mismatch(&expected.id, &commit.id));
    }
    if commit.generation != expected.generation {
        return Err(ReadError::GenerationMismatch {
            id: expected.id.clone(),
            expected: expected.generation,
            observed: commit.generation,
        });
    }
    Ok(commit)
}

fn load_root(
    read: &mut RetainedStorageRead,
    commit: &CommitRecord,
) -> Result<RootRecord, ReadError> {
    let object = read.object(&commit.root_id, ObjectKind::Root)?;
    let StoredObject::Root(root) = object else {
        unreachable!("object kind was checked");
    };
    if root.id != commit.root_id {
        return Err(identity_mismatch(&commit.root_id, &root.id));
    }
    if root.commit_id != commit.id || root.generation != commit.generation {
        return Err(ReadError::BindingMismatch {
            detail: format!("root {} is not bound to {}", root.id, commit.id),
        });
    }
    if root.catalog_id != commit.catalog_id {
        return Err(ReadError::BindingMismatch {
            detail: format!("root {} names the wrong catalog", root.id),
        });
    }
    Ok(root)
}

fn load_catalog(
    read: &mut RetainedStorageRead,
    commit: &CommitRecord,
) -> Result<CommitCatalog, ReadError> {
    let object = read.object(&commit.catalog_id, ObjectKind::CommitCatalog)?;
    let StoredObject::CommitCatalog(catalog) = object else {
        unreachable!("object kind was checked");
    };
    if catalog.id != commit.catalog_id {
        return Err(identity_mismatch(&commit.catalog_id, &catalog.id));
    }
    if catalog.commit_id != commit.id {
        return Err(ReadError::BindingMismatch {
            detail: format!("catalog {} is not bound to {}", catalog.id, commit.id),
        });
    }
    Ok(catalog)
}

fn load_member(
    read: &mut RetainedStorageRead,
    member_id: &str,
    commit_id: &str,
    key: &str,
) -> Result<MemberRecord, ReadError> {
    let object = read.object(member_id, ObjectKind::Member)?;
    let StoredObject::Member(member) = object else {
        unreachable!("object kind was checked");
    };
    if member.id != member_id {
        return Err(identity_mismatch(member_id, &member.id));
    }
    if member.commit_id != commit_id || member.key != key {
        return Err(ReadError::BindingMismatch {
            detail: format!("member {} is not bound to {}", member.id, key),
        });
    }
    Ok(member)
}

fn load_payload(
    read: &mut RetainedStorageRead,
    payload_id: &str,
    member_id: &str,
) -> Result<PayloadRecord, ReadError> {
    let object = read.object(payload_id, ObjectKind::Payload)?;
    let StoredObject::Payload(payload) = object else {
        unreachable!("object kind was checked");
    };
    if payload.id != payload_id {
        return Err(identity_mismatch(payload_id, &payload.id));
    }
    if payload.member_id != member_id {
        return Err(ReadError::BindingMismatch {
            detail: format!("payload {} names the wrong member", payload.id),
        });
    }
    Ok(payload)
}

fn load_registry(
    read: &mut RetainedStorageRead,
    commit: &CommitRecord,
) -> Result<PluginRegistry, ReadError> {
    let object = read.object(&commit.registry_id, ObjectKind::PluginRegistry)?;
    let StoredObject::PluginRegistry(registry) = object else {
        unreachable!("object kind was checked");
    };
    if registry.id != commit.registry_id {
        return Err(identity_mismatch(&commit.registry_id, &registry.id));
    }
    if registry.commit_id != commit.id {
        return Err(ReadError::BindingMismatch {
            detail: format!("registry {} is not bound to {}", registry.id, commit.id),
        });
    }
    Ok(registry)
}

fn load_owner(
    read: &mut RetainedStorageRead,
    owner_id: &str,
    commit_id: &str,
    file_id: &str,
) -> Result<FileOwner, ReadError> {
    let object = read.object(owner_id, ObjectKind::FileOwner)?;
    let StoredObject::FileOwner(owner) = object else {
        unreachable!("object kind was checked");
    };
    if owner.id != owner_id {
        return Err(identity_mismatch(owner_id, &owner.id));
    }
    if owner.commit_id != commit_id || owner.file_id != file_id {
        return Err(ReadError::BindingMismatch {
            detail: format!("owner {} is not bound to {}", owner.id, file_id),
        });
    }
    Ok(owner)
}

fn load_state(
    read: &mut RetainedStorageRead,
    expected: &CommitRef,
) -> Result<LoadedState, ReadError> {
    let commit = load_commit(read, expected)?;
    let _root = load_root(read, &commit)?;
    let catalog = load_catalog(read, &commit)?;
    let registry = load_registry(read, &commit)?;
    let mut rows = BTreeMap::new();

    for (key, member_id) in &catalog.members {
        if key.is_empty() || key.contains('\0') {
            return Err(ReadError::Malformed {
                detail: format!("malformed state identity {key:?}"),
            });
        }
        let member = load_member(read, member_id, &commit.id, key)?;
        let mut row = member.row;
        if let Cell::Value { payload_id, .. } = &row.cell {
            let payload = load_payload(read, payload_id, &member.id)?;
            row.cell = Cell::Value {
                payload_id: payload.id,
                digest: payload.digest,
            };
        }
        rows.insert(key.clone(), row);
    }

    let mut owners = BTreeMap::new();
    for (file_id, owner_id) in &registry.owners {
        let owner = load_owner(read, owner_id, &commit.id, file_id)?;
        owners.insert(file_id.clone(), owner);
    }

    Ok(LoadedState {
        commit,
        rows,
        owners,
        registry_id: registry.id,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ChangeKind {
    Added,
    Updated,
    Deleted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Change {
    key: String,
    kind: ChangeKind,
    before: Option<SemanticRow>,
    after: SemanticRow,
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
    source_owner_id: String,
    target_owner_id: String,
    source_plugin_key: String,
    target_plugin_key: String,
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
    conflicts: Vec<Conflict>,
    plugin_handoffs: Vec<PluginHandoff>,
    read_owner: String,
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

        if !base.commit.parent_ids.contains(&merge_base.commit.id)
            || !source.commit.parent_ids.contains(&base.commit.id)
            || !target.commit.parent_ids.contains(&base.commit.id)
        {
            return Err(ReadError::BindingMismatch {
                detail: "merge-base/base/source/target topology is not bound".to_owned(),
            });
        }

        let mut keys = BTreeSet::new();
        keys.extend(base.rows.keys().cloned());
        keys.extend(source.rows.keys().cloned());
        keys.extend(target.rows.keys().cloned());

        let mut source_picks = Vec::new();
        let mut conflicts = Vec::new();

        for key in keys {
            let base_row = base.rows.get(&key);
            let source_row = source.rows.get(&key);
            let target_row = target.rows.get(&key);
            let source_change = classify(base_row, source_row, &key)?;
            let target_change = classify(base_row, target_row, &key)?;

            let Some(source_change) = source_change else {
                continue;
            };
            match target_change {
                None => source_picks.push(source_change),
                Some(target_change)
                    if semantic_equal(&target_change.after, &source_change.after) =>
                {
                    // Equal live values, NULL, metadata, or tombstones converge.
                }
                Some(target_change) => conflicts.push(Conflict {
                    key,
                    target: target_change,
                    source: source_change,
                }),
            }
        }

        let mut handoffs = Vec::new();
        for (file_id, source_owner) in &source.owners {
            if let Some(target_owner) = target.owners.get(file_id) {
                handoffs.push(PluginHandoff {
                    file_id: file_id.clone(),
                    source_owner_id: source_owner.id.clone(),
                    target_owner_id: target_owner.id.clone(),
                    source_plugin_key: source_owner.plugin_key.clone(),
                    target_plugin_key: target_owner.plugin_key.clone(),
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
            conflicts,
            plugin_handoffs: handoffs,
            read_owner: self.read.owner_id.clone(),
        })
    }
}

fn semantic_equal(left: &SemanticRow, right: &SemanticRow) -> bool {
    let cells_equal = match (&left.cell, &right.cell) {
        (Cell::Null, Cell::Null) | (Cell::Tombstone, Cell::Tombstone) => true,
        (
            Cell::Value { digest: left, .. },
            Cell::Value { digest: right, .. },
        ) => left == right,
        _ => false,
    };
    cells_equal && left.metadata == right.metadata && left.file_id == right.file_id
}

fn classify(
    base: Option<&SemanticRow>,
    side: Option<&SemanticRow>,
    key: &str,
) -> Result<Option<Change>, ReadError> {
    let Some(side) = side else {
        if base.is_some() {
            return Err(ReadError::MissingTombstone {
                key: key.to_owned(),
            });
        }
        return Ok(None);
    };

    let kind = match base {
        None => ChangeKind::Added,
        Some(before) if semantic_equal(before, side) => return Ok(None),
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

#[derive(Clone, Debug)]
enum SeedCell {
    Null,
    Value(String),
    Tombstone,
}

#[derive(Clone, Debug)]
struct SeedRow {
    cell: SeedCell,
    metadata: Option<String>,
    file_id: Option<String>,
}

fn build_commit(
    store: &mut ObjectStore,
    id: &str,
    generation: u64,
    parent_ids: Vec<String>,
    seeds: BTreeMap<String, SeedRow>,
    plugin_key: &str,
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
                    StoredObject::Payload(PayloadRecord {
                        id: payload_id.clone(),
                        member_id: member_id.clone(),
                        digest,
                    }),
                );
                Cell::Value {
                    payload_id,
                    digest: String::new(),
                }
            }
        };
        let row = SemanticRow {
            cell,
            metadata: seed.metadata,
            file_id: seed.file_id.clone(),
        };
        store.put(
            member_id.clone(),
            StoredObject::Member(MemberRecord {
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
                StoredObject::FileOwner(FileOwner {
                    id: owner_id.clone(),
                    commit_id: id.to_owned(),
                    file_id: file_id.clone(),
                    plugin_key: plugin_key.to_owned(),
                }),
            );
            owners.insert(file_id, owner_id);
        }
    }

    store.put(
        catalog_id.clone(),
        StoredObject::CommitCatalog(CommitCatalog {
            id: catalog_id.clone(),
            commit_id: id.to_owned(),
            members,
        }),
    );
    store.put(
        registry_id.clone(),
        StoredObject::PluginRegistry(PluginRegistry {
            id: registry_id.clone(),
            commit_id: id.to_owned(),
            owners,
        }),
    );
    store.put(
        root_id.clone(),
        StoredObject::Root(RootRecord {
            id: root_id.clone(),
            commit_id: id.to_owned(),
            generation,
            catalog_id: catalog_id.clone(),
        }),
    );
    store.put(
        id.to_owned(),
        StoredObject::Commit(CommitRecord {
            id: id.to_owned(),
            generation,
            parent_ids,
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

fn base_seeds() -> BTreeMap<String, SeedRow> {
    BTreeMap::from([
        (
            "a".to_owned(),
            SeedRow {
                cell: SeedCell::Value("base-a".to_owned()),
                metadata: Some("plugin-base".to_owned()),
                file_id: Some("file-1".to_owned()),
            },
        ),
        (
            "b".to_owned(),
            SeedRow {
                cell: SeedCell::Value("base-b".to_owned()),
                metadata: None,
                file_id: None,
            },
        ),
        (
            "n".to_owned(),
            SeedRow {
                cell: SeedCell::Null,
                metadata: None,
                file_id: None,
            },
        ),
    ])
}

fn fixture() -> (ObjectStore, MergeRequest) {
    let mut store = ObjectStore::default();
    let merge_base = build_commit(&mut store, "M", 10, Vec::new(), base_seeds(), "plugin-m");
    let base = build_commit(
        &mut store,
        "B",
        20,
        vec![merge_base.id.clone()],
        base_seeds(),
        "plugin-b",
    );

    let mut source_seeds = base_seeds();
    source_seeds.insert(
        "a".to_owned(),
        SeedRow {
            cell: SeedCell::Value("source-a".to_owned()),
            metadata: Some("plugin-source".to_owned()),
            file_id: Some("file-1".to_owned()),
        },
    );
    source_seeds.insert(
        "b".to_owned(),
        SeedRow {
            cell: SeedCell::Value("source-b".to_owned()),
            metadata: None,
            file_id: None,
        },
    );
    source_seeds.insert(
        "c".to_owned(),
        SeedRow {
            cell: SeedCell::Value("added-c".to_owned()),
            metadata: Some("plugin-new".to_owned()),
            file_id: Some("file-2".to_owned()),
        },
    );
    let source = build_commit(
        &mut store,
        "S",
        21,
        vec![base.id.clone()],
        source_seeds,
        "plugin-source",
    );

    let mut target_seeds = base_seeds();
    target_seeds.insert(
        "a".to_owned(),
        SeedRow {
            cell: SeedCell::Value("target-a".to_owned()),
            metadata: Some("plugin-target".to_owned()),
            file_id: Some("file-1".to_owned()),
        },
    );
    target_seeds.insert(
        "b".to_owned(),
        SeedRow {
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
        target_seeds,
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
    let mut read = RetainedStorageRead::new("opening-read-1", store);
    let result = MergeOperation::new(&mut read).analyze(&request);
    if result.is_ok() {
        read.assert_one_owner()?;
    }
    result
}

fn main() {
    let (store, request) = fixture();
    let mut read = RetainedStorageRead::new("opening-read-1", store.clone());
    let result = MergeOperation::new(&mut read)
        .analyze(&request)
        .expect("valid authenticated merge");
    read.assert_one_owner().expect("one retained read owner");

    assert_eq!(result.identities.merge_base, request.merge_base);
    assert_eq!(result.identities.base, request.base);
    assert_eq!(result.identities.source, request.source);
    assert_eq!(result.identities.target, request.target);
    assert_eq!(
        result.source_picks.iter().map(|change| change.key.as_str()).collect::<Vec<_>>(),
        vec!["c"]
    );
    assert_eq!(
        result.conflicts.iter().map(|conflict| conflict.key.as_str()).collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    assert!(result.plugin_handoffs.iter().any(|handoff| {
        handoff.file_id == "file-1"
            && handoff.source_plugin_key == "plugin-source"
            && handoff.target_plugin_key == "plugin-target"
    }));
    assert!(read.events.len() > 12);

    // A disjoint source-only addition succeeds with canonical ordering.
    let mut disjoint_store = store.clone();
    let mut disjoint_seeds = base_seeds();
    disjoint_seeds.insert(
        "c".to_owned(),
        SeedRow {
            cell: SeedCell::Value("added-c".to_owned()),
            metadata: None,
            file_id: None,
        },
    );
    let disjoint_source = build_commit(
        &mut disjoint_store,
        "D",
        21,
        vec![request.base.id.clone()],
        disjoint_seeds,
        "plugin-disjoint",
    );
    let mut disjoint_request = request.clone();
    disjoint_request.source = disjoint_source;
    let disjoint = run(disjoint_store, disjoint_request).expect("disjoint merge succeeds");
    assert_eq!(disjoint.source_picks.len(), 1);
    assert!(disjoint.conflicts.is_empty());

    // Wrong identity/generation and malformed semantic identities fail closed.
    let mut bad_request = request.clone();
    bad_request.source.generation += 1;
    assert!(matches!(
        run(store.clone(), bad_request),
        Err(ReadError::GenerationMismatch { .. })
    ));

    let mut malformed_store = store.clone();
    let base_commit = match malformed_store.objects.get("B").cloned().unwrap() {
        StoredObject::Commit(commit) => commit,
        _ => unreachable!(),
    };
    let catalog_id = base_commit.catalog_id.clone();
    malformed_store.remove(&catalog_id);
    assert!(matches!(
        run(malformed_store, request.clone()),
        Err(ReadError::Missing {
            kind: ObjectKind::CommitCatalog,
            ..
        })
    ));

    let mut wrong_kind_root = store.clone();
    let source_commit = match wrong_kind_root.objects.get("S").cloned().unwrap() {
        StoredObject::Commit(commit) => commit,
        _ => unreachable!(),
    };
    wrong_kind_root.put(
        source_commit.root_id.clone(),
        StoredObject::Payload(PayloadRecord {
            id: source_commit.root_id.clone(),
            member_id: "wrong".to_owned(),
            digest: "wrong-kind".to_owned(),
        }),
    );
    assert!(matches!(
        run(wrong_kind_root, request.clone()),
        Err(ReadError::WrongKind {
            expected: ObjectKind::Root,
            ..
        })
    ));

    let mut wrong_catalog_kind = store.clone();
    let target_commit = match wrong_catalog_kind.objects.get("T").cloned().unwrap() {
        StoredObject::Commit(commit) => commit,
        _ => unreachable!(),
    };
    wrong_catalog_kind.put(
        target_commit.catalog_id.clone(),
        StoredObject::Root(RootRecord {
            id: target_commit.catalog_id.clone(),
            commit_id: target_commit.id.clone(),
            generation: target_commit.generation,
            catalog_id: target_commit.catalog_id.clone(),
        }),
    );
    assert!(matches!(
        run(wrong_catalog_kind, request.clone()),
        Err(ReadError::WrongKind {
            expected: ObjectKind::CommitCatalog,
            ..
        })
    ));

    let mut member_substitution = store.clone();
    let source_catalog = match member_substitution.objects.get("catalog-S").cloned().unwrap() {
        StoredObject::CommitCatalog(catalog) => catalog,
        _ => unreachable!(),
    };
    let member_id = source_catalog.members.get("a").unwrap().clone();
    member_substitution.put(
        member_id.clone(),
        StoredObject::Member(MemberRecord {
            id: "substituted-member".to_owned(),
            commit_id: "S".to_owned(),
            key: "a".to_owned(),
            row: SemanticRow {
                cell: Cell::Value {
                    payload_id: "payload-S-a".to_owned(),
                    digest: "source-a".to_owned(),
                },
                metadata: Some("plugin-source".to_owned()),
                file_id: Some("file-1".to_owned()),
            },
        }),
    );
    assert!(matches!(
        run(member_substitution, request.clone()),
        Err(ReadError::IdentityMismatch { .. })
    ));

    let mut wrong_member_kind = store.clone();
    wrong_member_kind.put(
        member_id.clone(),
        StoredObject::Root(RootRecord {
            id: member_id.clone(),
            commit_id: "S".to_owned(),
            generation: 21,
            catalog_id: "catalog-S".to_owned(),
        }),
    );
    assert!(matches!(
        run(wrong_member_kind, request.clone()),
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

    let mut wrong_payload_kind = store.clone();
    wrong_payload_kind.put(
        "payload-S-a".to_owned(),
        StoredObject::Member(MemberRecord {
            id: "payload-S-a".to_owned(),
            commit_id: "S".to_owned(),
            key: "a".to_owned(),
            row: SemanticRow {
                cell: Cell::Tombstone,
                metadata: None,
                file_id: None,
            },
        }),
    );
    assert!(matches!(
        run(wrong_payload_kind, request.clone()),
        Err(ReadError::WrongKind {
            expected: ObjectKind::Payload,
            ..
        })
    ));

    let mut payload_substitution = store.clone();
    payload_substitution.put(
        "payload-S-a".to_owned(),
        StoredObject::Payload(PayloadRecord {
            id: "substituted-payload".to_owned(),
            member_id: "member-S-a".to_owned(),
            digest: "source-a".to_owned(),
        }),
    );
    assert!(matches!(
        run(payload_substitution, request.clone()),
        Err(ReadError::IdentityMismatch { .. })
    ));

    let mut malformed_catalog = store.clone();
    let mut catalog = match malformed_catalog.objects.get("catalog-S").cloned().unwrap() {
        StoredObject::CommitCatalog(catalog) => catalog,
        _ => unreachable!(),
    };
    catalog.members.insert("".to_owned(), "member-S-a".to_owned());
    malformed_catalog.put(
        "catalog-S".to_owned(),
        StoredObject::CommitCatalog(catalog),
    );
    assert!(matches!(
        run(malformed_catalog, request.clone()),
        Err(ReadError::Malformed { .. })
    ));

    let mut malformed_root = store.clone();
    let source_commit = match malformed_root.objects.get("S").cloned().unwrap() {
        StoredObject::Commit(commit) => commit,
        _ => unreachable!(),
    };
    malformed_root.put(
        source_commit.root_id.clone(),
        StoredObject::Root(RootRecord {
            id: source_commit.root_id.clone(),
            commit_id: "wrong-commit".to_owned(),
            generation: source_commit.generation,
            catalog_id: source_commit.catalog_id.clone(),
        }),
    );
    assert!(matches!(
        run(malformed_root, request.clone()),
        Err(ReadError::BindingMismatch { .. })
    ));

    let mut missing_owner = store.clone();
    missing_owner.remove("owner-S-file-1");
    assert!(matches!(
        run(missing_owner, request.clone()),
        Err(ReadError::Missing {
            kind: ObjectKind::FileOwner,
            ..
        })
    ));

    // A missing source row is not silently treated as deletion; only an
    // authenticated Tombstone row is a deleted value.
    let mut absent_source = store.clone();
    let source_catalog = match absent_source.objects.get("catalog-S").cloned().unwrap() {
        StoredObject::CommitCatalog(catalog) => catalog,
        _ => unreachable!(),
    };
    let source_b = source_catalog.members.get("b").unwrap().clone();
    absent_source.remove(&source_b);
    assert!(matches!(
        run(absent_source, request),
        Err(ReadError::Missing {
            kind: ObjectKind::Member,
            ..
        })
    ));
}
