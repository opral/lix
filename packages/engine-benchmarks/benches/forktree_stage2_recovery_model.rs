//! Test-only Stage-2 crash/recovery/corruption/race acceptance model.
//! It deliberately reuses the accepted reader-lease storage bridge while
//! defining no production codec, API, adapter path, or Stage-2 wiring.

use super::*;
use std::collections::{BTreeSet, VecDeque};

const REC_META: lix::storage::StorageSpace = synthetic_space_for_bench(75, ValueSemantics::Mutable);
const REC_OBJECTS: lix::storage::StorageSpace =
    synthetic_space_for_bench(76, ValueSemantics::Immutable);
const AUTHORITY_KEY: &[u8] = b"authority";
const PROGRESS_KEY: &[u8] = b"gc/progress";
const SELECTOR_PREFIX: &[u8] = b"selector/";
const LEASE2_PREFIX: &[u8] = b"lease/";
const PAGE: usize = 32;
const LEASE_SPAN: u64 = 64;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
enum Kind {
    Catalog = 1,
    State = 2,
    Blob = 3,
    Receipt = 4,
    Edge = 5,
}

impl Kind {
    fn decode(value: u8) -> Result<Self, StorageError> {
        match value {
            1 => Ok(Self::Catalog),
            2 => Ok(Self::State),
            3 => Ok(Self::Blob),
            4 => Ok(Self::Receipt),
            5 => Ok(Self::Edge),
            _ => Err(corruption("typed object kind is unknown")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RootRef {
    kind: Kind,
    id: Id,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Child {
    kind: Kind,
    id: Id,
}

#[derive(Clone, Debug)]
struct DecodedObject {
    kind: Kind,
    generation: u64,
    children: Vec<Child>,
}

#[derive(Clone, Debug)]
struct Graph {
    root: RootRef,
    entries: Vec<PutEntry>,
    ids: Vec<Id>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Authority {
    epoch: u64,
    gc_watermark: u64,
    next_lease_generation: u64,
    selected_generation: u64,
    active: Id,
}

impl Authority {
    fn encode(&self) -> Bytes {
        let mut body = Vec::with_capacity(68);
        body.extend_from_slice(b"S2A1");
        body.extend_from_slice(&self.epoch.to_be_bytes());
        body.extend_from_slice(&self.gc_watermark.to_be_bytes());
        body.extend_from_slice(&self.next_lease_generation.to_be_bytes());
        body.extend_from_slice(&self.selected_generation.to_be_bytes());
        body.extend_from_slice(&self.active);
        append_checksum(body)
    }

    fn decode(raw: &Bytes) -> Result<Self, StorageError> {
        let body = authenticated_body(raw, b"S2A1", 68)?;
        let value = Self {
            epoch: read_u64(body, 4)?,
            gc_watermark: read_u64(body, 12)?,
            next_lease_generation: read_u64(body, 20)?,
            selected_generation: read_u64(body, 28)?,
            active: read_id(body, 36)?,
        };
        if value.epoch == 0
            || value.next_lease_generation == 0
            || value.selected_generation == 0
            || value.active == [0; 32]
        {
            return Err(corruption("global authority is noncanonical"));
        }
        Ok(value)
    }

    fn rotated(&self) -> Self {
        Self {
            epoch: self.epoch.checked_add(1).expect("authority epoch overflow"),
            ..self.clone()
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum SelectorRole {
    Recovery = 1,
    Checkpoint = 2,
    ChildBranch = 3,
    Upload = 4,
}

impl SelectorRole {
    fn decode(value: u8) -> Result<Self, StorageError> {
        match value {
            1 => Ok(Self::Recovery),
            2 => Ok(Self::Checkpoint),
            3 => Ok(Self::ChildBranch),
            4 => Ok(Self::Upload),
            _ => Err(corruption("selector role is unknown")),
        }
    }

    fn expected_kind(self) -> Kind {
        match self {
            Self::Upload => Kind::Receipt,
            Self::Recovery | Self::Checkpoint | Self::ChildBranch => Kind::Catalog,
        }
    }

    fn label(self) -> &'static [u8] {
        match self {
            Self::Recovery => b"recovery",
            Self::Checkpoint => b"checkpoint",
            Self::ChildBranch => b"child",
            Self::Upload => b"upload",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Selector {
    role: SelectorRole,
    generation: u64,
    root: Id,
}

impl Selector {
    fn encode(&self) -> Bytes {
        let mut body = Vec::with_capacity(45);
        body.extend_from_slice(b"S2S1");
        body.push(self.role as u8);
        body.extend_from_slice(&self.generation.to_be_bytes());
        body.extend_from_slice(&self.root);
        append_checksum(body)
    }

    fn decode(key: &Key, raw: &Bytes) -> Result<Self, StorageError> {
        let body = authenticated_body(raw, b"S2S1", 45)?;
        let value = Self {
            role: SelectorRole::decode(body[4])?,
            generation: read_u64(body, 5)?,
            root: read_id(body, 13)?,
        };
        if key != &selector_key(value.role) || value.generation == 0 || value.root == [0; 32] {
            return Err(corruption("selector identity is noncanonical"));
        }
        Ok(value)
    }

    fn root_ref(&self) -> RootRef {
        RootRef {
            kind: self.role.expected_kind(),
            id: self.root,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Lease2 {
    id: u64,
    generation: u64,
    root: Id,
    view: Id,
    valid_through_epoch: u64,
}

impl Lease2 {
    fn encode(&self) -> Bytes {
        let mut body = Vec::with_capacity(92);
        body.extend_from_slice(b"S2L1");
        body.extend_from_slice(&self.id.to_be_bytes());
        body.extend_from_slice(&self.generation.to_be_bytes());
        body.extend_from_slice(&self.root);
        body.extend_from_slice(&self.view);
        body.extend_from_slice(&self.valid_through_epoch.to_be_bytes());
        append_checksum(body)
    }

    fn decode(key: &Key, raw: &Bytes, authority: &Authority) -> Result<Self, StorageError> {
        let body = authenticated_body(raw, b"S2L1", 92)?;
        let value = Self {
            id: read_u64(body, 4)?,
            generation: read_u64(body, 12)?,
            root: read_id(body, 20)?,
            view: read_id(body, 52)?,
            valid_through_epoch: read_u64(body, 84)?,
        };
        if key != &lease2_key(value.id)
            || value.generation == 0
            || value.generation >= authority.next_lease_generation
            || value.root == [0; 32]
            || value.view == [0; 32]
        {
            return Err(corruption("reader lease is noncanonical"));
        }
        Ok(value)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Cursor2 {
    lease_id: u64,
    lease_generation: u64,
    root: Id,
    view: Id,
    resume_after: u64,
}

impl Cursor2 {
    fn encode(&self) -> Bytes {
        let mut body = Vec::with_capacity(92);
        body.extend_from_slice(b"S2C1");
        body.extend_from_slice(&self.lease_id.to_be_bytes());
        body.extend_from_slice(&self.lease_generation.to_be_bytes());
        body.extend_from_slice(&self.root);
        body.extend_from_slice(&self.view);
        body.extend_from_slice(&self.resume_after.to_be_bytes());
        append_checksum(body)
    }

    fn decode(raw: &Bytes) -> Result<Self, StorageError> {
        let body = authenticated_body(raw, b"S2C1", 92)?;
        Ok(Self {
            lease_id: read_u64(body, 4)?,
            lease_generation: read_u64(body, 12)?,
            root: read_id(body, 20)?,
            view: read_id(body, 52)?,
            resume_after: read_u64(body, 84)?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Progress {
    cycle: u64,
    fenced_authority: Bytes,
    minimum_live_generation: u64,
    live_lease_count: u64,
    live_lease_digest: Id,
    root_count: u64,
    root_digest: Id,
}

impl Progress {
    fn encode(&self) -> Bytes {
        let mut body = Vec::with_capacity(220);
        body.extend_from_slice(b"S2G1");
        body.extend_from_slice(&self.cycle.to_be_bytes());
        body.extend_from_slice(&(self.fenced_authority.len() as u32).to_be_bytes());
        body.extend_from_slice(&self.fenced_authority);
        body.extend_from_slice(&self.minimum_live_generation.to_be_bytes());
        body.extend_from_slice(&self.live_lease_count.to_be_bytes());
        body.extend_from_slice(&self.live_lease_digest);
        body.extend_from_slice(&self.root_count.to_be_bytes());
        body.extend_from_slice(&self.root_digest);
        append_checksum(body)
    }

    fn decode(raw: &Bytes) -> Result<Self, StorageError> {
        verify_checksum(raw)?;
        if raw.len() < 4 + 8 + 4 + 8 + 8 + 32 + 8 + 32 + 32 || &raw[..4] != b"S2G1" {
            return Err(corruption("GC progress is malformed"));
        }
        let authority_len = u32::from_be_bytes(
            raw[12..16]
                .try_into()
                .map_err(|_| corruption("GC authority length is malformed"))?,
        ) as usize;
        let end = 16usize
            .checked_add(authority_len)
            .ok_or_else(|| corruption("GC authority length overflow"))?;
        if end + 88 + 32 != raw.len() {
            return Err(corruption("GC progress lengths are inconsistent"));
        }
        Ok(Self {
            cycle: read_u64(raw, 4)?,
            fenced_authority: Bytes::copy_from_slice(&raw[16..end]),
            minimum_live_generation: read_u64(raw, end)?,
            live_lease_count: read_u64(raw, end + 8)?,
            live_lease_digest: read_id(raw, end + 16)?,
            root_count: read_u64(raw, end + 48)?,
            root_digest: read_id(raw, end + 56)?,
        })
    }
}

#[derive(Clone)]
struct PreparedPublish {
    raw_authority: Bytes,
    authority: Authority,
    expected_root: Id,
    next_root: Id,
    raw_progress: Option<Bytes>,
}

#[derive(Clone)]
struct PreparedGc2 {
    raw_authority: Bytes,
    next_authority: Authority,
    progress: Progress,
    expired: Vec<(Key, Bytes)>,
}

#[derive(Clone)]
struct SequenceState {
    initial: Graph,
    staged_orphan: Graph,
}

fn selector_key(role: SelectorRole) -> Key {
    let mut value = SELECTOR_PREFIX.to_vec();
    value.extend_from_slice(role.label());
    key(Bytes::from(value))
}

fn lease2_key(id: u64) -> Key {
    prefixed_key(LEASE2_PREFIX, id)
}

fn object2_key(id: Id) -> Key {
    key(Bytes::copy_from_slice(&id))
}

fn view2(root: Id, lease_id: u64, generation: u64) -> Id {
    let mut hash = blake3::Hasher::new();
    hash.update(b"forktree-stage2-reader-view-v1");
    hash.update(&root);
    hash.update(&lease_id.to_be_bytes());
    hash.update(&generation.to_be_bytes());
    *hash.finalize().as_bytes()
}

fn encode_object(kind: Kind, generation: u64, children: &[Child], payload: &[u8]) -> (Id, Bytes) {
    let mut body = Vec::with_capacity(64 + children.len() * 33 + payload.len());
    body.extend_from_slice(b"S2O1");
    body.push(kind as u8);
    body.extend_from_slice(&generation.to_be_bytes());
    body.extend_from_slice(&(children.len() as u16).to_be_bytes());
    body.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    for child in children {
        body.push(child.kind as u8);
        body.extend_from_slice(&child.id);
    }
    body.extend_from_slice(payload);
    let raw = append_checksum(body);
    (*blake3::hash(&raw).as_bytes(), raw)
}

fn decode_object2(id: Id, raw: &Bytes) -> Result<DecodedObject, StorageError> {
    if blake3::hash(raw).as_bytes() != &id {
        return Err(corruption("immutable object key/content hash mismatch"));
    }
    verify_checksum(raw)?;
    if raw.len() < 19 + 32 || &raw[..4] != b"S2O1" {
        return Err(corruption("typed immutable object is malformed"));
    }
    let kind = Kind::decode(raw[4])?;
    let generation = read_u64(raw, 5)?;
    if generation == 0 {
        return Err(corruption("typed immutable object generation is zero"));
    }
    let child_count = u16::from_be_bytes(
        raw[13..15]
            .try_into()
            .map_err(|_| corruption("object child count is malformed"))?,
    ) as usize;
    let payload_len = u32::from_be_bytes(
        raw[15..19]
            .try_into()
            .map_err(|_| corruption("object payload length is malformed"))?,
    ) as usize;
    let children_end = 19usize
        .checked_add(
            child_count
                .checked_mul(33)
                .ok_or_else(|| corruption("child length overflow"))?,
        )
        .ok_or_else(|| corruption("child length overflow"))?;
    if children_end
        .checked_add(payload_len)
        .and_then(|value| value.checked_add(32))
        != Some(raw.len())
    {
        return Err(corruption("typed object lengths are inconsistent"));
    }
    let mut children = Vec::with_capacity(child_count);
    for index in 0..child_count {
        let offset = 19 + index * 33;
        children.push(Child {
            kind: Kind::decode(raw[offset])?,
            id: read_id(raw, offset + 1)?,
        });
    }
    Ok(DecodedObject {
        kind,
        generation,
        children,
    })
}

fn graph(seed: u64) -> Graph {
    let payload = |label: u8, width: usize| vec![(seed as u8) ^ label; width];
    let (blob_id, blob) = encode_object(Kind::Blob, 1, &[], &payload(1, 257));
    let (state_id, state) = encode_object(
        Kind::State,
        2,
        &[Child {
            kind: Kind::Blob,
            id: blob_id,
        }],
        &payload(2, 97),
    );
    let (edge_id, edge) = encode_object(
        Kind::Edge,
        3,
        &[Child {
            kind: Kind::State,
            id: state_id,
        }],
        &payload(3, 41),
    );
    let (catalog_id, catalog) = encode_object(
        Kind::Catalog,
        4,
        &[
            Child {
                kind: Kind::State,
                id: state_id,
            },
            Child {
                kind: Kind::Edge,
                id: edge_id,
            },
        ],
        &payload(4, 73),
    );
    Graph {
        root: RootRef {
            kind: Kind::Catalog,
            id: catalog_id,
        },
        entries: vec![
            PutEntry {
                key: object2_key(blob_id),
                value: StoredValue { bytes: blob },
            },
            PutEntry {
                key: object2_key(state_id),
                value: StoredValue { bytes: state },
            },
            PutEntry {
                key: object2_key(edge_id),
                value: StoredValue { bytes: edge },
            },
            PutEntry {
                key: object2_key(catalog_id),
                value: StoredValue { bytes: catalog },
            },
        ],
        ids: vec![blob_id, state_id, edge_id, catalog_id],
    }
}

fn receipt_graph(seed: u64) -> Graph {
    let payload = vec![(seed as u8) ^ 0x5a; 513];
    let (blob_id, blob) = encode_object(Kind::Blob, 1, &[], &payload);
    let (receipt_id, receipt) = encode_object(
        Kind::Receipt,
        2,
        &[Child {
            kind: Kind::Blob,
            id: blob_id,
        }],
        &seed.to_be_bytes(),
    );
    Graph {
        root: RootRef {
            kind: Kind::Receipt,
            id: receipt_id,
        },
        entries: vec![
            PutEntry {
                key: object2_key(blob_id),
                value: StoredValue { bytes: blob },
            },
            PutEntry {
                key: object2_key(receipt_id),
                value: StoredValue { bytes: receipt },
            },
        ],
        ids: vec![blob_id, receipt_id],
    }
}

async fn stage<S: Storage>(
    storage: &S,
    graph: &Graph,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    put_batches(
        storage,
        WriteOptions::default(),
        vec![(
            REC_OBJECTS,
            PutBatch {
                entries: graph.entries.clone(),
            },
        )],
        Vec::new(),
        metrics,
    )
    .await
}

async fn raw_get<S: Storage>(
    storage: &S,
    space: lix::storage::StorageSpace,
    wanted: &[Key],
    metrics: &mut Metrics,
) -> Result<Vec<Option<Bytes>>, StorageError> {
    get_values(
        storage,
        &[GetManyRequest {
            space,
            keys: wanted,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }],
        metrics,
    )
    .await
}

async fn load_authority<S: Storage>(
    storage: &S,
    metrics: &mut Metrics,
) -> Result<(Bytes, Authority), StorageError> {
    let values = raw_get(
        storage,
        REC_META,
        &[key(Bytes::from_static(AUTHORITY_KEY))],
        metrics,
    )
    .await?;
    let raw = values[0]
        .clone()
        .ok_or_else(|| corruption("Stage-2 authority is absent"))?;
    let value = Authority::decode(&raw)?;
    Ok((raw, value))
}

async fn progress_value<S: Storage>(
    storage: &S,
    metrics: &mut Metrics,
) -> Result<Option<Bytes>, StorageError> {
    Ok(raw_get(
        storage,
        REC_META,
        &[key(Bytes::from_static(PROGRESS_KEY))],
        metrics,
    )
    .await?[0]
        .clone())
}

fn progress_precondition(raw: &Option<Bytes>) -> Precondition {
    match raw {
        Some(value) => Precondition::KeyValueEquals {
            space: REC_META,
            key: key(Bytes::from_static(PROGRESS_KEY)),
            expected: value.clone(),
        },
        None => Precondition::KeyAbsent {
            space: REC_META,
            key: key(Bytes::from_static(PROGRESS_KEY)),
        },
    }
}

async fn seed<S: Storage>(
    storage: &S,
    metrics: &mut Metrics,
) -> Result<SequenceState, StorageError> {
    let initial = graph(1);
    stage(storage, &initial, metrics).await?;
    let authority = Authority {
        epoch: 1,
        gc_watermark: 0,
        next_lease_generation: 1,
        selected_generation: 1,
        active: initial.root.id,
    };
    let selectors: Vec<PutEntry> = [SelectorRole::Checkpoint, SelectorRole::ChildBranch]
        .into_iter()
        .map(|role| PutEntry {
            key: selector_key(role),
            value: StoredValue {
                bytes: Selector {
                    role,
                    generation: 1,
                    root: initial.root.id,
                }
                .encode(),
            },
        })
        .collect();
    put_batches(
        storage,
        WriteOptions {
            preconditions: vec![Precondition::KeyAbsent {
                space: REC_META,
                key: key(Bytes::from_static(AUTHORITY_KEY)),
            }],
            ..WriteOptions::default()
        },
        vec![(
            REC_META,
            PutBatch {
                entries: std::iter::once(PutEntry {
                    key: key(Bytes::from_static(AUTHORITY_KEY)),
                    value: StoredValue {
                        bytes: authority.encode(),
                    },
                })
                .chain(selectors)
                .collect(),
            },
        )],
        Vec::new(),
        metrics,
    )
    .await?;

    // Crash before staging: constructing a graph has no durable effect.
    let staged_orphan = graph(2);
    Ok(SequenceState {
        initial,
        staged_orphan,
    })
}

async fn read_object_from<R: StorageRead>(
    read: &R,
    id: Id,
    metrics: &mut Metrics,
) -> Result<Bytes, StorageError> {
    let keys = [object2_key(id)];
    get_values_from(
        read,
        &[GetManyRequest {
            space: REC_OBJECTS,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }],
        metrics,
    )
    .await?[0]
        .clone()
        .ok_or_else(|| corruption("reachable typed object is missing"))
}

async fn trace_from_read<R: StorageRead>(
    read: &R,
    roots: &[RootRef],
    metrics: &mut Metrics,
) -> Result<BTreeSet<Id>, StorageError> {
    let mut seen = BTreeSet::new();
    let mut queue = VecDeque::from(roots.to_vec());
    while let Some(expected) = queue.pop_front() {
        if !seen.insert(expected.id) {
            continue;
        }
        let raw = read_object_from(read, expected.id, metrics).await?;
        let decoded = decode_object2(expected.id, &raw)?;
        if decoded.kind != expected.kind {
            return Err(corruption(
                "typed object kind does not match its authenticated edge",
            ));
        }
        for child in decoded.children {
            let child_raw = read_object_from(read, child.id, metrics).await?;
            let child_object = decode_object2(child.id, &child_raw)?;
            if child_object.kind != child.kind || child_object.generation >= decoded.generation {
                return Err(corruption("typed child kind/generation is invalid"));
            }
            queue.push_back(RootRef {
                kind: child.kind,
                id: child.id,
            });
        }
    }
    Ok(seen)
}

async fn validate_root<S: Storage>(
    storage: &S,
    root: RootRef,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let read = storage.begin_read(ReadOptions::default()).await?;
    trace_from_read(&read, &[root], metrics).await.map(|_| ())
}

async fn verify_active<S: Storage>(
    storage: &S,
    expected: Id,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let (_, authority) = load_authority(storage, metrics).await?;
    if authority.active != expected {
        return Err(corruption("active selector changed across crash boundary"));
    }
    validate_root(
        storage,
        RootRef {
            kind: Kind::Catalog,
            id: expected,
        },
        metrics,
    )
    .await
}

async fn prepare_publish<S: Storage>(
    storage: &S,
    expected_root: Id,
    next_root: Id,
    metrics: &mut Metrics,
) -> Result<PreparedPublish, StorageError> {
    validate_root(
        storage,
        RootRef {
            kind: Kind::Catalog,
            id: next_root,
        },
        metrics,
    )
    .await?;
    let (raw_authority, authority) = load_authority(storage, metrics).await?;
    if authority.active != expected_root {
        return Err(corruption(
            "publication was prepared from a stale selected root",
        ));
    }
    Ok(PreparedPublish {
        raw_authority,
        authority,
        expected_root,
        next_root,
        raw_progress: progress_value(storage, metrics).await?,
    })
}

async fn selector_value<S: Storage>(
    storage: &S,
    role: SelectorRole,
    metrics: &mut Metrics,
) -> Result<Option<(Bytes, Selector)>, StorageError> {
    let wanted = selector_key(role);
    let raw = raw_get(storage, REC_META, std::slice::from_ref(&wanted), metrics).await?[0].clone();
    raw.map(|raw| Selector::decode(&wanted, &raw).map(|value| (raw, value)))
        .transpose()
}

async fn set_selector<S: Storage>(
    storage: &S,
    role: SelectorRole,
    root: Id,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    validate_root(
        storage,
        RootRef {
            kind: role.expected_kind(),
            id: root,
        },
        metrics,
    )
    .await?;
    let (raw_authority, authority) = load_authority(storage, metrics).await?;
    let raw_progress = progress_value(storage, metrics).await?;
    let existing = selector_value(storage, role, metrics).await?;
    let mut preconditions = vec![
        Precondition::KeyValueEquals {
            space: REC_META,
            key: key(Bytes::from_static(AUTHORITY_KEY)),
            expected: raw_authority,
        },
        progress_precondition(&raw_progress),
    ];
    preconditions.push(match existing {
        Some((raw, _)) => Precondition::KeyValueEquals {
            space: REC_META,
            key: selector_key(role),
            expected: raw,
        },
        None => Precondition::KeyAbsent {
            space: REC_META,
            key: selector_key(role),
        },
    });
    let next = authority.rotated();
    put_batches(
        storage,
        WriteOptions {
            preconditions,
            ..WriteOptions::default()
        },
        vec![(
            REC_META,
            PutBatch {
                entries: vec![
                    PutEntry {
                        key: key(Bytes::from_static(AUTHORITY_KEY)),
                        value: StoredValue {
                            bytes: next.encode(),
                        },
                    },
                    PutEntry {
                        key: selector_key(role),
                        value: StoredValue {
                            bytes: Selector {
                                role,
                                generation: next.selected_generation,
                                root,
                            }
                            .encode(),
                        },
                    },
                ],
            },
        )],
        vec![(REC_META, vec![key(Bytes::from_static(PROGRESS_KEY))])],
        metrics,
    )
    .await
}

async fn delete_selector<S: Storage>(
    storage: &S,
    role: SelectorRole,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let (raw_authority, authority) = load_authority(storage, metrics).await?;
    let raw_progress = progress_value(storage, metrics).await?;
    let (raw_selector, _) = selector_value(storage, role, metrics)
        .await?
        .ok_or_else(|| corruption("selector to delete is absent"))?;
    put_batches(
        storage,
        WriteOptions {
            preconditions: vec![
                Precondition::KeyValueEquals {
                    space: REC_META,
                    key: key(Bytes::from_static(AUTHORITY_KEY)),
                    expected: raw_authority,
                },
                Precondition::KeyValueEquals {
                    space: REC_META,
                    key: selector_key(role),
                    expected: raw_selector,
                },
                progress_precondition(&raw_progress),
            ],
            ..WriteOptions::default()
        },
        vec![(
            REC_META,
            PutBatch {
                entries: vec![PutEntry {
                    key: key(Bytes::from_static(AUTHORITY_KEY)),
                    value: StoredValue {
                        bytes: authority.rotated().encode(),
                    },
                }],
            },
        )],
        vec![(
            REC_META,
            vec![selector_key(role), key(Bytes::from_static(PROGRESS_KEY))],
        )],
        metrics,
    )
    .await
}

async fn restore_selector<S: Storage>(
    storage: &S,
    role: SelectorRole,
    metrics: &mut Metrics,
) -> Result<Id, StorageError> {
    let read = storage.begin_read(ReadOptions::default()).await?;
    let wanted = [
        key(Bytes::from_static(AUTHORITY_KEY)),
        selector_key(role),
        key(Bytes::from_static(PROGRESS_KEY)),
    ];
    let values = get_values_from(
        &read,
        &[GetManyRequest {
            space: REC_META,
            keys: &wanted,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }],
        metrics,
    )
    .await?;
    let raw_authority = values[0]
        .clone()
        .ok_or_else(|| corruption("authority is absent during restore"))?;
    let authority = Authority::decode(&raw_authority)?;
    let raw_selector = values[1]
        .clone()
        .ok_or_else(|| corruption("restore selector is absent"))?;
    let selector = Selector::decode(&wanted[1], &raw_selector)?;
    trace_from_read(&read, &[selector.root_ref()], metrics).await?;
    drop(read);

    let mut next = authority.rotated();
    next.selected_generation += 1;
    next.active = selector.root;
    let recovery = Selector {
        role: SelectorRole::Recovery,
        generation: next.selected_generation,
        root: authority.active,
    };
    let mut preconditions = vec![
        Precondition::KeyValueEquals {
            space: REC_META,
            key: key(Bytes::from_static(AUTHORITY_KEY)),
            expected: raw_authority,
        },
        Precondition::KeyValueEquals {
            space: REC_META,
            key: selector_key(role),
            expected: raw_selector,
        },
        progress_precondition(&values[2]),
    ];
    if role == SelectorRole::Recovery {
        preconditions.pop();
        preconditions.push(progress_precondition(&values[2]));
    }
    put_batches(
        storage,
        WriteOptions {
            preconditions,
            ..WriteOptions::default()
        },
        vec![(
            REC_META,
            PutBatch {
                entries: vec![
                    PutEntry {
                        key: key(Bytes::from_static(AUTHORITY_KEY)),
                        value: StoredValue {
                            bytes: next.encode(),
                        },
                    },
                    PutEntry {
                        key: selector_key(SelectorRole::Recovery),
                        value: StoredValue {
                            bytes: recovery.encode(),
                        },
                    },
                ],
            },
        )],
        vec![(REC_META, vec![key(Bytes::from_static(PROGRESS_KEY))])],
        metrics,
    )
    .await?;
    Ok(selector.root)
}

async fn acquire_lease<S: Storage>(
    storage: &S,
    lease_id: u64,
    metrics: &mut Metrics,
) -> Result<(Lease2, Cursor2), StorageError> {
    let (raw_authority, authority) = load_authority(storage, metrics).await?;
    validate_root(
        storage,
        RootRef {
            kind: Kind::Catalog,
            id: authority.active,
        },
        metrics,
    )
    .await?;
    let raw_progress = progress_value(storage, metrics).await?;
    let generation = authority.next_lease_generation;
    let lease = Lease2 {
        id: lease_id,
        generation,
        root: authority.active,
        view: view2(authority.active, lease_id, generation),
        valid_through_epoch: authority.epoch + 1 + LEASE_SPAN,
    };
    let mut next = authority.rotated();
    next.next_lease_generation += 1;
    put_batches(
        storage,
        WriteOptions {
            preconditions: vec![
                Precondition::KeyValueEquals {
                    space: REC_META,
                    key: key(Bytes::from_static(AUTHORITY_KEY)),
                    expected: raw_authority,
                },
                Precondition::KeyAbsent {
                    space: REC_META,
                    key: lease2_key(lease_id),
                },
                progress_precondition(&raw_progress),
            ],
            ..WriteOptions::default()
        },
        vec![(
            REC_META,
            PutBatch {
                entries: vec![
                    PutEntry {
                        key: key(Bytes::from_static(AUTHORITY_KEY)),
                        value: StoredValue {
                            bytes: next.encode(),
                        },
                    },
                    PutEntry {
                        key: lease2_key(lease_id),
                        value: StoredValue {
                            bytes: lease.encode(),
                        },
                    },
                ],
            },
        )],
        vec![(REC_META, vec![key(Bytes::from_static(PROGRESS_KEY))])],
        metrics,
    )
    .await?;
    let cursor = Cursor2 {
        lease_id,
        lease_generation: generation,
        root: lease.root,
        view: lease.view,
        resume_after: 0,
    };
    Ok((lease, cursor))
}

async fn renew_lease<S: Storage>(
    storage: &S,
    lease_id: u64,
    metrics: &mut Metrics,
) -> Result<(Lease2, Cursor2), StorageError> {
    let (raw_authority, authority) = load_authority(storage, metrics).await?;
    let lease_key = lease2_key(lease_id);
    let raw = raw_get(storage, REC_META, std::slice::from_ref(&lease_key), metrics).await?[0]
        .clone()
        .ok_or(StorageError::ReadExpired)?;
    let old = Lease2::decode(&lease_key, &raw, &authority)?;
    if old.valid_through_epoch < authority.epoch {
        return Err(StorageError::ReadExpired);
    }
    let generation = authority.next_lease_generation;
    let next_lease = Lease2 {
        generation,
        view: view2(old.root, lease_id, generation),
        valid_through_epoch: authority.epoch + 1 + LEASE_SPAN,
        ..old
    };
    let raw_progress = progress_value(storage, metrics).await?;
    let mut next = authority.rotated();
    next.next_lease_generation += 1;
    put_batches(
        storage,
        WriteOptions {
            preconditions: vec![
                Precondition::KeyValueEquals {
                    space: REC_META,
                    key: key(Bytes::from_static(AUTHORITY_KEY)),
                    expected: raw_authority,
                },
                Precondition::KeyValueEquals {
                    space: REC_META,
                    key: lease_key.clone(),
                    expected: raw,
                },
                progress_precondition(&raw_progress),
            ],
            ..WriteOptions::default()
        },
        vec![(
            REC_META,
            PutBatch {
                entries: vec![
                    PutEntry {
                        key: key(Bytes::from_static(AUTHORITY_KEY)),
                        value: StoredValue {
                            bytes: next.encode(),
                        },
                    },
                    PutEntry {
                        key: lease_key,
                        value: StoredValue {
                            bytes: next_lease.encode(),
                        },
                    },
                ],
            },
        )],
        vec![(REC_META, vec![key(Bytes::from_static(PROGRESS_KEY))])],
        metrics,
    )
    .await?;
    let cursor = Cursor2 {
        lease_id,
        lease_generation: generation,
        root: next_lease.root,
        view: next_lease.view,
        resume_after: 0,
    };
    Ok((next_lease, cursor))
}

async fn release_lease2<S: Storage>(
    storage: &S,
    lease_id: u64,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let (raw_authority, authority) = load_authority(storage, metrics).await?;
    let lease_key = lease2_key(lease_id);
    let raw = raw_get(storage, REC_META, std::slice::from_ref(&lease_key), metrics).await?[0]
        .clone()
        .ok_or(StorageError::ReadExpired)?;
    Lease2::decode(&lease_key, &raw, &authority)?;
    let raw_progress = progress_value(storage, metrics).await?;
    put_batches(
        storage,
        WriteOptions {
            preconditions: vec![
                Precondition::KeyValueEquals {
                    space: REC_META,
                    key: key(Bytes::from_static(AUTHORITY_KEY)),
                    expected: raw_authority,
                },
                Precondition::KeyValueEquals {
                    space: REC_META,
                    key: lease_key.clone(),
                    expected: raw,
                },
                progress_precondition(&raw_progress),
            ],
            ..WriteOptions::default()
        },
        vec![(
            REC_META,
            PutBatch {
                entries: vec![PutEntry {
                    key: key(Bytes::from_static(AUTHORITY_KEY)),
                    value: StoredValue {
                        bytes: authority.rotated().encode(),
                    },
                }],
            },
        )],
        vec![(
            REC_META,
            vec![lease_key, key(Bytes::from_static(PROGRESS_KEY))],
        )],
        metrics,
    )
    .await
}

async fn resume_cursor2<S: Storage>(
    storage: &S,
    encoded: &Bytes,
    metrics: &mut Metrics,
) -> Result<Cursor2, StorageError> {
    let cursor = Cursor2::decode(encoded)?;
    let read = storage.begin_read(ReadOptions::default()).await?;
    let wanted = [
        key(Bytes::from_static(AUTHORITY_KEY)),
        lease2_key(cursor.lease_id),
    ];
    let values = get_values_from(
        &read,
        &[GetManyRequest {
            space: REC_META,
            keys: &wanted,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }],
        metrics,
    )
    .await?;
    let authority = Authority::decode(
        values[0]
            .as_ref()
            .ok_or_else(|| corruption("cursor authority is absent"))?,
    )?;
    let lease = Lease2::decode(
        &wanted[1],
        values[1].as_ref().ok_or(StorageError::ReadExpired)?,
        &authority,
    )?;
    if lease.generation != cursor.lease_generation
        || lease.root != cursor.root
        || lease.view != cursor.view
        || lease.valid_through_epoch < authority.epoch
    {
        return Err(StorageError::ReadExpired);
    }
    trace_from_read(
        &read,
        &[RootRef {
            kind: Kind::Catalog,
            id: cursor.root,
        }],
        metrics,
    )
    .await?;
    Ok(Cursor2 {
        resume_after: cursor.resume_after + 1,
        ..cursor
    })
}

fn digest_roots(roots: &[RootRef]) -> Id {
    let mut canonical = roots.to_vec();
    canonical.sort_unstable();
    canonical.dedup();
    let mut digest = blake3::Hasher::new();
    digest.update(b"forktree-stage2-gc-roots-v1");
    for root in canonical {
        digest.update(&[root.kind as u8]);
        digest.update(&root.id);
    }
    *digest.finalize().as_bytes()
}

async fn collect_roots_from<R: StorageRead>(
    read: &R,
    authority: &Authority,
    expiry_epoch: u64,
    metrics: &mut Metrics,
) -> Result<(Vec<RootRef>, Vec<(u64, Bytes)>, Vec<(Key, Bytes)>), StorageError> {
    let mut roots = vec![RootRef {
        kind: Kind::Catalog,
        id: authority.active,
    }];
    for (selector_key, raw) in scan_prefix(read, REC_META, SELECTOR_PREFIX, metrics).await? {
        let selector = Selector::decode(&selector_key, &raw)?;
        roots.push(selector.root_ref());
    }
    let mut live = Vec::new();
    let mut expired = Vec::new();
    for (lease_key, raw) in scan_prefix(read, REC_META, LEASE2_PREFIX, metrics).await? {
        let lease = Lease2::decode(&lease_key, &raw, authority)?;
        if lease.valid_through_epoch >= expiry_epoch {
            roots.push(RootRef {
                kind: Kind::Catalog,
                id: lease.root,
            });
            live.push((lease.generation, raw));
        } else {
            expired.push((lease_key, raw));
        }
    }
    live.sort_by_key(|(generation, _)| *generation);
    roots.sort_unstable();
    roots.dedup();
    Ok((roots, live, expired))
}

fn lease_digest(live: &[(u64, Bytes)]) -> Id {
    let mut digest = blake3::Hasher::new();
    digest.update(b"forktree-stage2-live-leases-v1");
    for (_, raw) in live {
        digest.update(&(raw.len() as u64).to_be_bytes());
        digest.update(raw);
    }
    *digest.finalize().as_bytes()
}

async fn prepare_gc2<S: Storage>(
    storage: &S,
    metrics: &mut Metrics,
) -> Result<PreparedGc2, StorageError> {
    let read = storage.begin_read(ReadOptions::default()).await?;
    let wanted = [key(Bytes::from_static(AUTHORITY_KEY))];
    let values = get_values_from(
        &read,
        &[GetManyRequest {
            space: REC_META,
            keys: &wanted,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }],
        metrics,
    )
    .await?;
    let raw_authority = values[0]
        .clone()
        .ok_or_else(|| corruption("GC authority is absent"))?;
    let authority = Authority::decode(&raw_authority)?;
    let next_authority = authority.rotated();
    let (roots, live, expired) =
        collect_roots_from(&read, &authority, next_authority.epoch, metrics).await?;

    // Every owner graph is authenticated before GC changes any durable byte.
    trace_from_read(&read, &roots, metrics).await?;
    let progress = Progress {
        cycle: next_authority.epoch,
        fenced_authority: next_authority.encode(),
        minimum_live_generation: live.first().map_or(0, |(generation, _)| *generation),
        live_lease_count: live.len() as u64,
        live_lease_digest: lease_digest(&live),
        root_count: roots.len() as u64,
        root_digest: digest_roots(&roots),
    };
    Ok(PreparedGc2 {
        raw_authority,
        next_authority,
        progress,
        expired,
    })
}

async fn start_gc2<S: Storage>(
    storage: &S,
    prepared: &PreparedGc2,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let raw_progress = prepared.progress.encode();
    let mut preconditions = vec![
        Precondition::KeyValueEquals {
            space: REC_META,
            key: key(Bytes::from_static(AUTHORITY_KEY)),
            expected: prepared.raw_authority.clone(),
        },
        Precondition::KeyAbsent {
            space: REC_META,
            key: key(Bytes::from_static(PROGRESS_KEY)),
        },
    ];
    preconditions.extend(prepared.expired.iter().map(|(lease_key, raw)| {
        Precondition::KeyValueEquals {
            space: REC_META,
            key: lease_key.clone(),
            expected: raw.clone(),
        }
    }));
    let deletes = if prepared.expired.is_empty() {
        Vec::new()
    } else {
        vec![(
            REC_META,
            prepared
                .expired
                .iter()
                .map(|(lease_key, _)| lease_key.clone())
                .collect(),
        )]
    };
    put_batches(
        storage,
        WriteOptions {
            preconditions,
            ..WriteOptions::default()
        },
        vec![(
            REC_META,
            PutBatch {
                entries: vec![
                    PutEntry {
                        key: key(Bytes::from_static(AUTHORITY_KEY)),
                        value: StoredValue {
                            bytes: prepared.next_authority.encode(),
                        },
                    },
                    PutEntry {
                        key: key(Bytes::from_static(PROGRESS_KEY)),
                        value: StoredValue {
                            bytes: raw_progress,
                        },
                    },
                ],
            },
        )],
        deletes,
        metrics,
    )
    .await
}

fn gc_fence(raw_authority: &Bytes, raw_progress: &Bytes) -> Vec<Precondition> {
    vec![
        Precondition::KeyValueEquals {
            space: REC_META,
            key: key(Bytes::from_static(AUTHORITY_KEY)),
            expected: raw_authority.clone(),
        },
        Precondition::KeyValueEquals {
            space: REC_META,
            key: key(Bytes::from_static(PROGRESS_KEY)),
            expected: raw_progress.clone(),
        },
    ]
}

async fn resume_finish_gc<S: Storage>(
    storage: &S,
    metrics: &mut Metrics,
) -> Result<u64, StorageError> {
    let read = storage.begin_read(ReadOptions::default()).await?;
    let wanted = [
        key(Bytes::from_static(AUTHORITY_KEY)),
        key(Bytes::from_static(PROGRESS_KEY)),
    ];
    let values = get_values_from(
        &read,
        &[GetManyRequest {
            space: REC_META,
            keys: &wanted,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }],
        metrics,
    )
    .await?;
    let raw_authority = values[0]
        .clone()
        .ok_or_else(|| corruption("GC fenced authority is absent"))?;
    let authority = Authority::decode(&raw_authority)?;
    let raw_progress = values[1]
        .clone()
        .ok_or_else(|| corruption("GC progress is absent"))?;
    let progress = Progress::decode(&raw_progress)?;
    if progress.fenced_authority != raw_authority || progress.cycle != authority.epoch {
        return Err(corruption("GC progress does not bind the raw authority"));
    }
    let (roots, live, expired) =
        collect_roots_from(&read, &authority, authority.epoch, metrics).await?;
    if !expired.is_empty()
        || progress.minimum_live_generation != live.first().map_or(0, |(generation, _)| *generation)
        || progress.live_lease_count != live.len() as u64
        || progress.live_lease_digest != lease_digest(&live)
        || progress.root_count != roots.len() as u64
        || progress.root_digest != digest_roots(&roots)
    {
        return Err(corruption("GC persisted owner closure is stale or corrupt"));
    }
    // Complete graph validation precedes the first sweep delete.
    let reachable = trace_from_read(&read, &roots, metrics).await?;
    let objects = scan_prefix(&read, REC_OBJECTS, b"", metrics).await?;
    drop(read);
    let mut swept = 0u64;
    for page in objects.chunks(PAGE) {
        let deletes = page
            .iter()
            .filter_map(|(object_key, _)| {
                let id = read_id(&object_key.0, 0).expect("typed object key");
                (!reachable.contains(&id)).then_some(object_key.clone())
            })
            .collect::<Vec<_>>();
        if !deletes.is_empty() {
            swept += deletes.len() as u64;
            put_batches(
                storage,
                WriteOptions {
                    preconditions: gc_fence(&raw_authority, &raw_progress),
                    ..WriteOptions::default()
                },
                Vec::new(),
                vec![(REC_OBJECTS, deletes)],
                metrics,
            )
            .await?;
        }
    }
    let mut completed = authority.rotated();
    completed.gc_watermark = progress.cycle;
    put_batches(
        storage,
        WriteOptions {
            preconditions: gc_fence(&raw_authority, &raw_progress),
            ..WriteOptions::default()
        },
        vec![(
            REC_META,
            PutBatch {
                entries: vec![PutEntry {
                    key: key(Bytes::from_static(AUTHORITY_KEY)),
                    value: StoredValue {
                        bytes: completed.encode(),
                    },
                }],
            },
        )],
        vec![(REC_META, vec![key(Bytes::from_static(PROGRESS_KEY))])],
        metrics,
    )
    .await?;
    Ok(swept)
}

async fn collect_gc2<S: Storage>(storage: &S, metrics: &mut Metrics) -> Result<u64, StorageError> {
    let prepared = prepare_gc2(storage, metrics).await?;
    start_gc2(storage, &prepared, metrics).await?;
    resume_finish_gc(storage, metrics).await
}

async fn object2_present<S: Storage>(
    storage: &S,
    id: Id,
    metrics: &mut Metrics,
) -> Result<bool, StorageError> {
    Ok(raw_get(storage, REC_OBJECTS, &[object2_key(id)], metrics).await?[0].is_some())
}

async fn assert_graph_presence<S: Storage>(
    storage: &S,
    graph: &Graph,
    expected: bool,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    for id in &graph.ids {
        assert_eq!(object2_present(storage, *id, metrics).await?, expected);
    }
    Ok(())
}

async fn capture_checkpoint<S: Storage>(
    storage: &S,
    metrics: &mut Metrics,
) -> Result<Id, StorageError> {
    let (_, authority) = load_authority(storage, metrics).await?;
    set_selector(storage, SelectorRole::Checkpoint, authority.active, metrics).await?;
    Ok(authority.active)
}

async fn continue_after_selector_crash<S: Storage>(
    storage: &S,
    initial: &Graph,
    published: &Graph,
    metrics: &mut Metrics,
) -> Result<(Id, Graph), StorageError> {
    verify_active(storage, published.root.id, metrics).await?;
    let recovery = selector_value(storage, SelectorRole::Recovery, metrics)
        .await?
        .expect("recovery selector after publication")
        .1;
    assert_eq!(recovery.root, initial.root.id);

    // Recovery selector and checkpoint restore each rebind the sole global
    // authority and preserve the displaced root as the next recovery point.
    let checkpoint_root = capture_checkpoint(storage, metrics).await?;
    assert_eq!(checkpoint_root, published.root.id);
    let next = graph(4);
    stage(storage, &next, metrics).await?;
    let prepared = prepare_publish(storage, published.root.id, next.root.id, metrics).await?;
    commit_publish(storage, &prepared, metrics).await?;
    assert_eq!(
        restore_selector(storage, SelectorRole::Checkpoint, metrics).await?,
        published.root.id
    );
    assert_eq!(
        restore_selector(storage, SelectorRole::Recovery, metrics).await?,
        next.root.id
    );
    verify_active(storage, next.root.id, metrics).await?;

    // Exact lease generation/view binding: renewal invalidates the old cursor,
    // release invalidates the renewed cursor.
    let (_, cursor) = acquire_lease(storage, 7, metrics).await?;
    resume_cursor2(storage, &cursor.encode(), metrics).await?;
    let (_, renewed) = renew_lease(storage, 7, metrics).await?;
    assert!(matches!(
        resume_cursor2(storage, &cursor.encode(), metrics).await,
        Err(StorageError::ReadExpired)
    ));
    resume_cursor2(storage, &renewed.encode(), metrics).await?;
    release_lease2(storage, 7, metrics).await?;
    assert!(matches!(
        resume_cursor2(storage, &renewed.encode(), metrics).await,
        Err(StorageError::ReadExpired)
    ));

    // An open multipart receipt is an independent authenticated GC root.
    let upload = receipt_graph(50);
    stage(storage, &upload, metrics).await?;
    set_selector(storage, SelectorRole::Upload, upload.root.id, metrics).await?;
    collect_gc2(storage, metrics).await?;
    assert_graph_presence(storage, &upload, true, metrics).await?;

    // Publication wins before GC start: the stale start CAS must reject.
    let stale_gc = prepare_gc2(storage, metrics).await?;
    let publication_first = graph(5);
    stage(storage, &publication_first, metrics).await?;
    let prepared =
        prepare_publish(storage, next.root.id, publication_first.root.id, metrics).await?;
    commit_publish(storage, &prepared, metrics).await?;
    assert!(matches!(
        start_gc2(storage, &stale_gc, metrics).await,
        Err(StorageError::PreconditionFailed(_))
    ));

    // GC wins first: publication consumes and invalidates exact progress; a
    // stale sweep cannot continue after the new root becomes visible.
    let gc_first = prepare_gc2(storage, metrics).await?;
    start_gc2(storage, &gc_first, metrics).await?;
    let publication_after_gc = graph(6);
    stage(storage, &publication_after_gc, metrics).await?;
    let prepared = prepare_publish(
        storage,
        publication_first.root.id,
        publication_after_gc.root.id,
        metrics,
    )
    .await?;
    commit_publish(storage, &prepared, metrics).await?;
    assert!(resume_finish_gc(storage, metrics).await.is_err());

    // Two writers prepared from one selector: one wins; the losing writer's
    // immutable staging is an ordinary orphan and is never made authoritative.
    let winner = graph(7);
    let loser = graph(8);
    stage(storage, &winner, metrics).await?;
    stage(storage, &loser, metrics).await?;
    let win = prepare_publish(
        storage,
        publication_after_gc.root.id,
        winner.root.id,
        metrics,
    )
    .await?;
    let lose = prepare_publish(
        storage,
        publication_after_gc.root.id,
        loser.root.id,
        metrics,
    )
    .await?;
    commit_publish(storage, &win, metrics).await?;
    assert!(matches!(
        commit_publish(storage, &lose, metrics).await,
        Err(StorageError::PreconditionFailed(_))
    ));

    // Final-reference rule: recovery, checkpoint, child branch, and a reader
    // all retain the old root. Removing any strict subset cannot reclaim it.
    set_selector(storage, SelectorRole::Checkpoint, winner.root.id, metrics).await?;
    set_selector(storage, SelectorRole::ChildBranch, winner.root.id, metrics).await?;
    let (_, final_cursor) = acquire_lease(storage, 9, metrics).await?;
    let final_active = graph(9);
    stage(storage, &final_active, metrics).await?;
    let prepared = prepare_publish(storage, winner.root.id, final_active.root.id, metrics).await?;
    commit_publish(storage, &prepared, metrics).await?;
    delete_selector(storage, SelectorRole::Recovery, metrics).await?;
    delete_selector(storage, SelectorRole::Checkpoint, metrics).await?;
    delete_selector(storage, SelectorRole::ChildBranch, metrics).await?;
    collect_gc2(storage, metrics).await?;
    assert_graph_presence(storage, &winner, true, metrics).await?;
    resume_cursor2(storage, &final_cursor.encode(), metrics).await?;
    release_lease2(storage, 9, metrics).await?;
    collect_gc2(storage, metrics).await?;
    assert_graph_presence(storage, &winner, false, metrics).await?;
    assert_graph_presence(storage, &loser, false, metrics).await?;

    delete_selector(storage, SelectorRole::Upload, metrics).await?;
    collect_gc2(storage, metrics).await?;
    assert_graph_presence(storage, &upload, false, metrics).await?;
    verify_active(storage, final_active.root.id, metrics).await?;

    // Crash after GC start: progress contains only a derived closure proof.
    // Reopen must recompute that closure, then finish safely.
    let crash_orphan = graph(10);
    stage(storage, &crash_orphan, metrics).await?;
    let pending = prepare_gc2(storage, metrics).await?;
    start_gc2(storage, &pending, metrics).await?;
    Ok((final_active.root.id, crash_orphan))
}

async fn verify_gc_recovery<S: Storage>(
    storage: &S,
    final_active: Id,
    orphan: &Graph,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let swept = resume_finish_gc(storage, metrics).await?;
    assert!(swept >= orphan.ids.len() as u64);
    assert_graph_presence(storage, orphan, false, metrics).await?;
    verify_active(storage, final_active, metrics).await
}

async fn overwrite_meta<S: Storage>(
    storage: &S,
    entry_key: Key,
    raw: Bytes,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    put_batches(
        storage,
        WriteOptions::default(),
        vec![(
            REC_META,
            PutBatch {
                entries: vec![PutEntry {
                    key: entry_key,
                    value: StoredValue { bytes: raw },
                }],
            },
        )],
        Vec::new(),
        metrics,
    )
    .await
}

async fn stage_raw_objects<S: Storage>(
    storage: &S,
    entries: Vec<PutEntry>,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    put_batches(
        storage,
        WriteOptions::default(),
        vec![(REC_OBJECTS, PutBatch { entries })],
        Vec::new(),
        metrics,
    )
    .await
}

fn corrupted_leaf(kind: Kind, generation: u64) -> (Id, Bytes) {
    let (_, raw) = encode_object(kind, generation, &[], b"corrupt-me");
    let mut bytes = raw.to_vec();
    let last = bytes.len() - 1;
    bytes[last] ^= 0x80;
    let raw = Bytes::from(bytes);
    (*blake3::hash(&raw).as_bytes(), raw)
}

async fn install_probe_selector<S: Storage>(
    storage: &S,
    role: SelectorRole,
    root: Id,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    overwrite_meta(
        storage,
        selector_key(role),
        Selector {
            role,
            generation: 1,
            root,
        }
        .encode(),
        metrics,
    )
    .await
}

async fn object_count<S: Storage>(
    storage: &S,
    metrics: &mut Metrics,
) -> Result<usize, StorageError> {
    let read = storage.begin_read(ReadOptions::default()).await?;
    Ok(scan_prefix(&read, REC_OBJECTS, b"", metrics).await?.len())
}

async fn assert_corruption_without_sweep<S: Storage>(
    storage: &S,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let before = object_count(storage, metrics).await?;
    assert!(prepare_gc2(storage, metrics).await.is_err());
    let after = object_count(storage, metrics).await?;
    assert_eq!(before, after, "corrupt closure must reject before deletion");
    Ok(())
}

async fn corruption_oracle<S: Storage>(storage: &S) -> Result<Metrics, StorageError> {
    let mut metrics = Metrics::default();
    let state = seed(storage, &mut metrics).await?;

    // Malformed recovery selector is not bootstrap absence and cannot restore.
    overwrite_meta(
        storage,
        selector_key(SelectorRole::Recovery),
        Bytes::from_static(b"torn-selector"),
        &mut metrics,
    )
    .await?;
    assert!(
        restore_selector(storage, SelectorRole::Recovery, &mut metrics)
            .await
            .is_err()
    );
    assert_corruption_without_sweep(storage, &mut metrics).await?;

    // Torn graph: the root is authentic, but its required state child is absent.
    let missing = [0x44; 32];
    let (torn_id, torn_raw) = encode_object(
        Kind::Catalog,
        2,
        &[Child {
            kind: Kind::State,
            id: missing,
        }],
        b"torn",
    );
    stage_raw_objects(
        storage,
        vec![PutEntry {
            key: object2_key(torn_id),
            value: StoredValue { bytes: torn_raw },
        }],
        &mut metrics,
    )
    .await?;
    install_probe_selector(storage, SelectorRole::Recovery, torn_id, &mut metrics).await?;
    assert_corruption_without_sweep(storage, &mut metrics).await?;

    // Mistyped graph: an authenticated Blob cannot satisfy a State edge.
    let (blob_id, blob_raw) = encode_object(Kind::Blob, 1, &[], b"valid-blob");
    let (mistyped_id, mistyped_raw) = encode_object(
        Kind::Catalog,
        2,
        &[Child {
            kind: Kind::State,
            id: blob_id,
        }],
        b"mistyped",
    );
    stage_raw_objects(
        storage,
        vec![
            PutEntry {
                key: object2_key(blob_id),
                value: StoredValue { bytes: blob_raw },
            },
            PutEntry {
                key: object2_key(mistyped_id),
                value: StoredValue {
                    bytes: mistyped_raw,
                },
            },
        ],
        &mut metrics,
    )
    .await?;
    install_probe_selector(storage, SelectorRole::Recovery, mistyped_id, &mut metrics).await?;
    assert_corruption_without_sweep(storage, &mut metrics).await?;

    // A correctly typed edge with a non-decreasing generation is corrupt.
    let (same_generation_state, same_generation_state_raw) =
        encode_object(Kind::State, 2, &[], b"same-generation-state");
    let (same_generation_catalog, same_generation_catalog_raw) = encode_object(
        Kind::Catalog,
        2,
        &[Child {
            kind: Kind::State,
            id: same_generation_state,
        }],
        b"same-generation-catalog",
    );
    stage_raw_objects(
        storage,
        vec![
            PutEntry {
                key: object2_key(same_generation_state),
                value: StoredValue {
                    bytes: same_generation_state_raw,
                },
            },
            PutEntry {
                key: object2_key(same_generation_catalog),
                value: StoredValue {
                    bytes: same_generation_catalog_raw,
                },
            },
        ],
        &mut metrics,
    )
    .await?;
    install_probe_selector(
        storage,
        SelectorRole::Recovery,
        same_generation_catalog,
        &mut metrics,
    )
    .await?;
    assert_corruption_without_sweep(storage, &mut metrics).await?;

    // Catalog, state, blob, and edge corruption are each reached through the
    // exact typed owner graph; receipt corruption is reached through upload.
    for (case, corrupt_kind, expected_kind) in [
        (100u64, Kind::Catalog, Kind::Catalog),
        (101, Kind::State, Kind::State),
        (102, Kind::Blob, Kind::Blob),
        (103, Kind::Edge, Kind::Edge),
    ] {
        let generation = match corrupt_kind {
            Kind::Catalog => 4,
            Kind::Edge => 3,
            Kind::State => 2,
            Kind::Blob => 1,
            Kind::Receipt => unreachable!(),
        };
        let (bad_id, bad_raw) = corrupted_leaf(corrupt_kind, generation);
        let root = if corrupt_kind == Kind::Catalog {
            bad_id
        } else {
            let (catalog_id, catalog_raw) = encode_object(
                Kind::Catalog,
                generation + 1,
                &[Child {
                    kind: expected_kind,
                    id: bad_id,
                }],
                &case.to_be_bytes(),
            );
            stage_raw_objects(
                storage,
                vec![PutEntry {
                    key: object2_key(catalog_id),
                    value: StoredValue { bytes: catalog_raw },
                }],
                &mut metrics,
            )
            .await?;
            catalog_id
        };
        stage_raw_objects(
            storage,
            vec![PutEntry {
                key: object2_key(bad_id),
                value: StoredValue { bytes: bad_raw },
            }],
            &mut metrics,
        )
        .await?;
        install_probe_selector(storage, SelectorRole::Recovery, root, &mut metrics).await?;
        assert_corruption_without_sweep(storage, &mut metrics).await?;
    }

    let (bad_receipt_id, bad_receipt_raw) = corrupted_leaf(Kind::Receipt, 2);
    stage_raw_objects(
        storage,
        vec![PutEntry {
            key: object2_key(bad_receipt_id),
            value: StoredValue {
                bytes: bad_receipt_raw,
            },
        }],
        &mut metrics,
    )
    .await?;
    // Repair recovery to isolate the receipt-root failure.
    install_probe_selector(
        storage,
        SelectorRole::Recovery,
        state.initial.root.id,
        &mut metrics,
    )
    .await?;
    install_probe_selector(storage, SelectorRole::Upload, bad_receipt_id, &mut metrics).await?;
    assert_corruption_without_sweep(storage, &mut metrics).await?;
    Ok(metrics)
}

fn print_result(
    backend: &str,
    wall_us: f64,
    cpu_us: f64,
    allocated: u64,
    allocation_calls: u64,
    rss_before: u64,
    rss_after: u64,
    disk_bytes: u64,
    metrics: &Metrics,
    physical: SlateDBIoSnapshot,
) {
    println!(
        "forktree_stage2_recovery,verdict=green,backend={backend},wall_us={wall_us:.3},cpu_us={cpu_us:.3},alloc_bytes={allocated},alloc_calls={allocation_calls},rss_before={rss_before},rss_after={rss_after},disk_bytes={disk_bytes},get_calls={},get_keys={},get_bytes={},scan_calls={},scan_rows={},scan_bytes={},commits={},puts={},deletes={},logical_write_bytes={},slate_read_objects={},slate_read_bytes={},slate_write_objects={},slate_write_bytes={},slate_list_operations={},slate_deleted_objects={},crash_before_stage=pass,crash_after_stage=pass,crash_after_selector_cas=pass,crash_after_gc_start=pass,cold_reopen=pass,recovery_restore=pass,checkpoint_restore=pass,lease_renew_release=pass,publication_gc_both_orders=pass,final_reference=pass,corruption_fail_closed=pass",
        metrics.get_calls,
        metrics.get_keys,
        metrics.get_bytes,
        metrics.scan_calls,
        metrics.scan_rows,
        metrics.scan_bytes,
        metrics.commits,
        metrics.puts,
        metrics.deletes,
        metrics.write_bytes,
        physical.read_objects,
        physical.read_bytes,
        physical.write_objects,
        physical.write_bytes,
        physical.list_operations,
        physical.deleted_objects,
    );
}

async fn run_rocks() -> Result<(), StorageError> {
    let main_dir = tempfile::tempdir().expect("create RocksDB recovery-oracle directory");
    let main_path = main_dir.path().join("main");
    let corrupt_dir = tempfile::tempdir().expect("create RocksDB corruption-oracle directory");
    let corrupt_path = corrupt_dir.path().join("corrupt");
    let mut metrics = Metrics::default();
    let rss_before = process_resident_bytes();
    let cpu_before = process_cpu_nanos();
    begin_profile();
    let started = Instant::now();

    let state = {
        let storage = RocksDB::open(&main_path).expect("open RocksDB seed phase");
        let state = seed(&storage, &mut metrics).await?;
        storage.flush()?;
        state
    };
    {
        let storage = RocksDB::open(&main_path).expect("reopen before-stage RocksDB");
        verify_active(&storage, state.initial.root.id, &mut metrics).await?;
        assert_graph_presence(&storage, &state.staged_orphan, false, &mut metrics).await?;
        stage(&storage, &state.staged_orphan, &mut metrics).await?;
        storage.flush()?;
    }
    let published = graph(3);
    {
        let storage = RocksDB::open(&main_path).expect("reopen staged RocksDB");
        verify_active(&storage, state.initial.root.id, &mut metrics).await?;
        assert_graph_presence(&storage, &state.staged_orphan, true, &mut metrics).await?;
        collect_gc2(&storage, &mut metrics).await?;
        assert_graph_presence(&storage, &state.staged_orphan, false, &mut metrics).await?;
        stage(&storage, &published, &mut metrics).await?;
        let prepared = prepare_publish(
            &storage,
            state.initial.root.id,
            published.root.id,
            &mut metrics,
        )
        .await?;
        commit_publish(&storage, &prepared, &mut metrics).await?;
        storage.flush()?;
    }
    let (final_active, crash_orphan) = {
        let storage = RocksDB::open(&main_path).expect("reopen published RocksDB");
        let result =
            continue_after_selector_crash(&storage, &state.initial, &published, &mut metrics)
                .await?;
        storage.flush()?;
        result
    };
    {
        let storage = RocksDB::open(&main_path).expect("reopen pending-GC RocksDB");
        verify_gc_recovery(&storage, final_active, &crash_orphan, &mut metrics).await?;
        storage.flush()?;
    }
    {
        let storage = RocksDB::open(&corrupt_path).expect("open RocksDB corruption phase");
        metrics += corruption_oracle(&storage).await?;
        storage.flush()?;
    }

    let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
    let (allocated, allocation_calls) = end_profile();
    let cpu_us = process_cpu_nanos().saturating_sub(cpu_before) as f64 / 1_000.0;
    print_result(
        "rocksdb",
        wall_us,
        cpu_us,
        allocated,
        allocation_calls,
        rss_before,
        process_resident_bytes(),
        directory_bytes(&main_path) + directory_bytes(&corrupt_path),
        &metrics,
        SlateDBIoSnapshot::default(),
    );
    Ok(())
}

async fn run_slate() -> Result<(), StorageError> {
    let main_dir = tempfile::tempdir().expect("create SlateDB recovery-oracle directory");
    let main_path = main_dir.path().join("main");
    let corrupt_dir = tempfile::tempdir().expect("create SlateDB corruption-oracle directory");
    let corrupt_path = corrupt_dir.path().join("corrupt");
    let counters = SlateDBIoCounters::default();
    let before_physical = counters.snapshot();
    let mut metrics = Metrics::default();
    let rss_before = process_resident_bytes();
    let cpu_before = process_cpu_nanos();
    begin_profile();
    let started = Instant::now();

    let state = {
        let storage = SlateDB::open_with_io_counters(&main_path, counters.clone())
            .expect("open SlateDB seed phase");
        let state = seed(&storage, &mut metrics).await?;
        storage.flush().await?;
        state
    };
    {
        let storage = SlateDB::open_with_io_counters(&main_path, counters.clone())
            .expect("reopen before-stage SlateDB");
        verify_active(&storage, state.initial.root.id, &mut metrics).await?;
        assert_graph_presence(&storage, &state.staged_orphan, false, &mut metrics).await?;
        stage(&storage, &state.staged_orphan, &mut metrics).await?;
        storage.flush().await?;
    }
    let published = graph(3);
    {
        let storage = SlateDB::open_with_io_counters(&main_path, counters.clone())
            .expect("reopen staged SlateDB");
        verify_active(&storage, state.initial.root.id, &mut metrics).await?;
        assert_graph_presence(&storage, &state.staged_orphan, true, &mut metrics).await?;
        collect_gc2(&storage, &mut metrics).await?;
        assert_graph_presence(&storage, &state.staged_orphan, false, &mut metrics).await?;
        stage(&storage, &published, &mut metrics).await?;
        let prepared = prepare_publish(
            &storage,
            state.initial.root.id,
            published.root.id,
            &mut metrics,
        )
        .await?;
        commit_publish(&storage, &prepared, &mut metrics).await?;
        storage.flush().await?;
    }
    let (final_active, crash_orphan) = {
        let storage = SlateDB::open_with_io_counters(&main_path, counters.clone())
            .expect("reopen published SlateDB");
        let result =
            continue_after_selector_crash(&storage, &state.initial, &published, &mut metrics)
                .await?;
        storage.flush().await?;
        result
    };
    {
        let storage = SlateDB::open_with_io_counters(&main_path, counters.clone())
            .expect("reopen pending-GC SlateDB");
        verify_gc_recovery(&storage, final_active, &crash_orphan, &mut metrics).await?;
        storage.flush().await?;
    }
    {
        let storage = SlateDB::open_with_io_counters(&corrupt_path, counters.clone())
            .expect("open SlateDB corruption phase");
        metrics += corruption_oracle(&storage).await?;
        storage.flush().await?;
    }

    let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
    let (allocated, allocation_calls) = end_profile();
    let cpu_us = process_cpu_nanos().saturating_sub(cpu_before) as f64 / 1_000.0;
    print_result(
        "slatedb",
        wall_us,
        cpu_us,
        allocated,
        allocation_calls,
        rss_before,
        process_resident_bytes(),
        directory_bytes(&main_path) + directory_bytes(&corrupt_path),
        &metrics,
        counters.snapshot().saturating_sub(before_physical),
    );
    Ok(())
}

pub(super) async fn run_backend(backend: &str) -> Result<(), StorageError> {
    match backend {
        "rocksdb" => run_rocks().await,
        "slatedb" => run_slate().await,
        other => panic!("unknown Stage-2 recovery backend '{other}'"),
    }
}

async fn commit_publish<S: Storage>(
    storage: &S,
    prepared: &PreparedPublish,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let mut next = prepared.authority.rotated();
    next.selected_generation += 1;
    next.active = prepared.next_root;
    let recovery = Selector {
        role: SelectorRole::Recovery,
        generation: next.selected_generation,
        root: prepared.expected_root,
    };
    put_batches(
        storage,
        WriteOptions {
            preconditions: vec![
                Precondition::KeyValueEquals {
                    space: REC_META,
                    key: key(Bytes::from_static(AUTHORITY_KEY)),
                    expected: prepared.raw_authority.clone(),
                },
                progress_precondition(&prepared.raw_progress),
            ],
            ..WriteOptions::default()
        },
        vec![(
            REC_META,
            PutBatch {
                entries: vec![
                    PutEntry {
                        key: key(Bytes::from_static(AUTHORITY_KEY)),
                        value: StoredValue {
                            bytes: next.encode(),
                        },
                    },
                    PutEntry {
                        key: selector_key(SelectorRole::Recovery),
                        value: StoredValue {
                            bytes: recovery.encode(),
                        },
                    },
                ],
            },
        )],
        vec![(REC_META, vec![key(Bytes::from_static(PROGRESS_KEY))])],
        metrics,
    )
    .await
}
