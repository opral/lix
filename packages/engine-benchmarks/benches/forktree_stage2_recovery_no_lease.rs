//! Test/model-only ForkTree Stage-2 crash and recovery oracle.
//! Reader lifetime is exclusively the live `StorageRead`; no durable reader or
//! cursor authority exists in this model.

use std::alloc::GlobalAlloc;
use std::collections::{BTreeSet, VecDeque};
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use lix::storage::{
    CoreProjection, GetManyRequest, GetOptions, Key, KeyRange, Precondition, ProjectedValue,
    PutBatch, PutEntry, ReadOptions, ScanOptions, Storage, StorageError, StorageRead, StorageWrite,
    StoredValue, ValueSemantics, WriteOptions,
};
use lix::storage_bench::synthetic_space_for_bench;
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

struct CountingAllocator;
static PROFILE_ALLOCATED: AtomicU64 = AtomicU64::new(0);
static PROFILE_CALLS: AtomicU64 = AtomicU64::new(0);
static PROFILE_ENABLED: AtomicBool = AtomicBool::new(false);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() && PROFILE_ENABLED.load(Ordering::Relaxed) {
            PROFILE_ALLOCATED.fetch_add(layout.size() as u64, Ordering::Relaxed);
            PROFILE_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: std::alloc::Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(pointer, layout) };
    }

    unsafe fn realloc(
        &self,
        pointer: *mut u8,
        layout: std::alloc::Layout,
        new_size: usize,
    ) -> *mut u8 {
        let replacement = unsafe { mimalloc::MiMalloc.realloc(pointer, layout, new_size) };
        if !replacement.is_null()
            && new_size >= layout.size()
            && PROFILE_ENABLED.load(Ordering::Relaxed)
        {
            PROFILE_ALLOCATED.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
            PROFILE_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        replacement
    }
}

const META: lix::storage::StorageSpace = synthetic_space_for_bench(78, ValueSemantics::Mutable);
const OBJECTS: lix::storage::StorageSpace =
    synthetic_space_for_bench(79, ValueSemantics::Immutable);
const ROWS: lix::storage::StorageSpace = synthetic_space_for_bench(80, ValueSemantics::Immutable);
const AUTHORITY_KEY: &[u8] = b"authority";
const PROGRESS_KEY: &[u8] = b"gc/progress";
const SELECTOR_PREFIX: &[u8] = b"selector/";
const PAGE_ROWS: usize = 8;

type Id = [u8; 32];

#[derive(Clone, Debug, Default)]
struct Metrics {
    gets: u64,
    get_keys: u64,
    get_bytes: u64,
    scans: u64,
    scan_rows: u64,
    scan_bytes: u64,
    commits: u64,
    puts: u64,
    deletes: u64,
    write_bytes: u64,
    swept_objects: u64,
    swept_rows: u64,
}

impl std::ops::AddAssign for Metrics {
    fn add_assign(&mut self, rhs: Self) {
        self.gets += rhs.gets;
        self.get_keys += rhs.get_keys;
        self.get_bytes += rhs.get_bytes;
        self.scans += rhs.scans;
        self.scan_rows += rhs.scan_rows;
        self.scan_bytes += rhs.scan_bytes;
        self.commits += rhs.commits;
        self.puts += rhs.puts;
        self.deletes += rhs.deletes;
        self.write_bytes += rhs.write_bytes;
        self.swept_objects += rhs.swept_objects;
        self.swept_rows += rhs.swept_rows;
    }
}

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
    fn decode(byte: u8) -> Result<Self, StorageError> {
        match byte {
            1 => Ok(Self::Catalog),
            2 => Ok(Self::State),
            3 => Ok(Self::Blob),
            4 => Ok(Self::Receipt),
            5 => Ok(Self::Edge),
            _ => Err(corruption("unknown typed object kind")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Root {
    kind: Kind,
    id: Id,
}

#[derive(Clone, Copy, Debug)]
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
    root: Root,
    objects: Vec<PutEntry>,
    rows: Vec<PutEntry>,
    object_ids: Vec<Id>,
    row_keys: Vec<Key>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Authority {
    epoch: u64,
    watermark: u64,
    selected_generation: u64,
    active: Id,
}

impl Authority {
    fn encode(&self) -> Bytes {
        let mut body = Vec::with_capacity(60);
        body.extend_from_slice(b"NLA1");
        body.extend_from_slice(&self.epoch.to_be_bytes());
        body.extend_from_slice(&self.watermark.to_be_bytes());
        body.extend_from_slice(&self.selected_generation.to_be_bytes());
        body.extend_from_slice(&self.active);
        authenticate(body)
    }

    fn decode(raw: &Bytes) -> Result<Self, StorageError> {
        let body = authenticated_body(raw, b"NLA1", 60)?;
        let value = Self {
            epoch: read_u64(body, 4)?,
            watermark: read_u64(body, 12)?,
            selected_generation: read_u64(body, 20)?,
            active: read_id(body, 28)?,
        };
        if value.epoch == 0 || value.selected_generation == 0 || value.active == [0; 32] {
            return Err(corruption("authority is noncanonical"));
        }
        Ok(value)
    }

    fn rotated(&self) -> Self {
        Self {
            epoch: self.epoch.checked_add(1).expect("epoch overflow"),
            ..self.clone()
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
enum Role {
    Recovery = 1,
    Checkpoint = 2,
    Undo = 3,
    Redo = 4,
    Child = 5,
    Upload = 6,
}

impl Role {
    fn decode(byte: u8) -> Result<Self, StorageError> {
        match byte {
            1 => Ok(Self::Recovery),
            2 => Ok(Self::Checkpoint),
            3 => Ok(Self::Undo),
            4 => Ok(Self::Redo),
            5 => Ok(Self::Child),
            6 => Ok(Self::Upload),
            _ => Err(corruption("unknown selector role")),
        }
    }

    fn label(self) -> &'static [u8] {
        match self {
            Self::Recovery => b"recovery",
            Self::Checkpoint => b"checkpoint",
            Self::Undo => b"undo",
            Self::Redo => b"redo",
            Self::Child => b"child",
            Self::Upload => b"upload",
        }
    }

    fn expected_kind(self) -> Kind {
        if self == Self::Upload {
            Kind::Receipt
        } else {
            Kind::Catalog
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Selector {
    role: Role,
    generation: u64,
    root: Id,
}

impl Selector {
    fn encode(&self) -> Bytes {
        let mut body = Vec::with_capacity(45);
        body.extend_from_slice(b"NLS1");
        body.push(self.role as u8);
        body.extend_from_slice(&self.generation.to_be_bytes());
        body.extend_from_slice(&self.root);
        authenticate(body)
    }

    fn decode(entry_key: &Key, raw: &Bytes) -> Result<Self, StorageError> {
        let body = authenticated_body(raw, b"NLS1", 45)?;
        let value = Self {
            role: Role::decode(body[4])?,
            generation: read_u64(body, 5)?,
            root: read_id(body, 13)?,
        };
        if entry_key != &selector_key(value.role) || value.generation == 0 || value.root == [0; 32]
        {
            return Err(corruption("selector identity is noncanonical"));
        }
        Ok(value)
    }

    fn root(&self) -> Root {
        Root {
            kind: self.role.expected_kind(),
            id: self.root,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Progress {
    cycle: u64,
    fenced_authority: Bytes,
    root_count: u64,
    root_digest: Id,
}

impl Progress {
    fn encode(&self) -> Bytes {
        let mut body = Vec::with_capacity(160);
        body.extend_from_slice(b"NLG1");
        body.extend_from_slice(&self.cycle.to_be_bytes());
        body.extend_from_slice(&(self.fenced_authority.len() as u32).to_be_bytes());
        body.extend_from_slice(&self.fenced_authority);
        body.extend_from_slice(&self.root_count.to_be_bytes());
        body.extend_from_slice(&self.root_digest);
        authenticate(body)
    }

    fn decode(raw: &Bytes) -> Result<Self, StorageError> {
        verify_checksum(raw)?;
        if raw.len() < 4 + 8 + 4 + 8 + 32 + 32 || &raw[..4] != b"NLG1" {
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
        if end + 40 + 32 != raw.len() {
            return Err(corruption("GC progress lengths are inconsistent"));
        }
        Ok(Self {
            cycle: read_u64(raw, 4)?,
            fenced_authority: Bytes::copy_from_slice(&raw[16..end]),
            root_count: read_u64(raw, end)?,
            root_digest: read_id(raw, end + 8)?,
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
struct PreparedGc {
    raw_authority: Bytes,
    next_authority: Authority,
    progress: Progress,
}

#[derive(Clone, Debug)]
struct Cursor {
    view_id: Id,
    root: Id,
    last_delivered: Vec<u8>,
}

impl Cursor {
    fn encode(&self) -> Bytes {
        let mut body = Vec::with_capacity(96 + self.last_delivered.len());
        body.extend_from_slice(b"NLC1");
        body.extend_from_slice(&self.view_id);
        body.extend_from_slice(&self.root);
        body.extend_from_slice(&(self.last_delivered.len() as u32).to_be_bytes());
        body.extend_from_slice(&self.last_delivered);
        authenticate(body)
    }

    fn decode(raw: &Bytes) -> Result<Self, StorageError> {
        verify_checksum(raw).map_err(|_| StorageError::InvalidCursor)?;
        if raw.len() < 4 + 32 + 32 + 4 + 32 || &raw[..4] != b"NLC1" {
            return Err(StorageError::InvalidCursor);
        }
        let length = u32::from_be_bytes(
            raw[68..72]
                .try_into()
                .map_err(|_| StorageError::InvalidCursor)?,
        ) as usize;
        if 72 + length + 32 != raw.len() {
            return Err(StorageError::InvalidCursor);
        }
        Ok(Self {
            view_id: read_id(raw, 4).map_err(|_| StorageError::InvalidCursor)?,
            root: read_id(raw, 36).map_err(|_| StorageError::InvalidCursor)?,
            last_delivered: raw[72..72 + length].to_vec(),
        })
    }
}

struct PinnedView<R> {
    read: R,
    root: Id,
    view_id: Id,
}

fn corruption(message: impl Into<String>) -> StorageError {
    StorageError::Corruption(message.into())
}

fn authenticate(mut body: Vec<u8>) -> Bytes {
    body.extend_from_slice(blake3::hash(&body).as_bytes());
    Bytes::from(body)
}

fn verify_checksum(raw: &Bytes) -> Result<(), StorageError> {
    if raw.len() < 32 {
        return Err(corruption("authenticated value is truncated"));
    }
    let split = raw.len() - 32;
    if blake3::hash(&raw[..split]).as_bytes() != &raw[split..] {
        return Err(corruption("authenticated value checksum mismatch"));
    }
    Ok(())
}

fn authenticated_body<'a>(
    raw: &'a Bytes,
    magic: &[u8; 4],
    body_len: usize,
) -> Result<&'a [u8], StorageError> {
    if raw.len() != body_len + 32 {
        return Err(corruption("authenticated value length mismatch"));
    }
    verify_checksum(raw)?;
    if &raw[..4] != magic {
        return Err(corruption("authenticated value magic mismatch"));
    }
    Ok(&raw[..body_len])
}

fn read_u64(raw: &[u8], offset: usize) -> Result<u64, StorageError> {
    raw.get(offset..offset + 8)
        .ok_or_else(|| corruption("u64 field is truncated"))?
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| corruption("u64 field is malformed"))
}

fn read_id(raw: &[u8], offset: usize) -> Result<Id, StorageError> {
    raw.get(offset..offset + 32)
        .ok_or_else(|| corruption("identity field is truncated"))?
        .try_into()
        .map_err(|_| corruption("identity field is malformed"))
}

fn key(raw: impl Into<Bytes>) -> Key {
    Key(raw.into())
}

fn object_key(id: Id) -> Key {
    key(Bytes::copy_from_slice(&id))
}

fn selector_key(role: Role) -> Key {
    let mut raw = SELECTOR_PREFIX.to_vec();
    raw.extend_from_slice(role.label());
    key(Bytes::from(raw))
}

fn prefix_range(prefix: &[u8]) -> KeyRange {
    let lower = key(Bytes::copy_from_slice(prefix));
    let mut upper = prefix.to_vec();
    for index in (0..upper.len()).rev() {
        if upper[index] != u8::MAX {
            upper[index] += 1;
            upper.truncate(index + 1);
            return KeyRange {
                lower: Bound::Included(lower),
                upper: Bound::Excluded(key(Bytes::from(upper))),
            };
        }
    }
    KeyRange {
        lower: Bound::Included(lower),
        upper: Bound::Unbounded,
    }
}

fn encode_object(kind: Kind, generation: u64, children: &[Child], payload: &[u8]) -> (Id, Bytes) {
    let mut body = Vec::with_capacity(64 + children.len() * 33 + payload.len());
    body.extend_from_slice(b"NLO1");
    body.push(kind as u8);
    body.extend_from_slice(&generation.to_be_bytes());
    body.extend_from_slice(&(children.len() as u16).to_be_bytes());
    body.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    for child in children {
        body.push(child.kind as u8);
        body.extend_from_slice(&child.id);
    }
    body.extend_from_slice(payload);
    let raw = authenticate(body);
    (*blake3::hash(&raw).as_bytes(), raw)
}

fn decode_object(id: Id, raw: &Bytes) -> Result<DecodedObject, StorageError> {
    if blake3::hash(raw).as_bytes() != &id {
        return Err(corruption("object key/content hash mismatch"));
    }
    verify_checksum(raw)?;
    if raw.len() < 19 + 32 || &raw[..4] != b"NLO1" {
        return Err(corruption("typed object is malformed"));
    }
    let kind = Kind::decode(raw[4])?;
    let generation = read_u64(raw, 5)?;
    let child_count = u16::from_be_bytes(
        raw[13..15]
            .try_into()
            .map_err(|_| corruption("child count is malformed"))?,
    ) as usize;
    let payload_len = u32::from_be_bytes(
        raw[15..19]
            .try_into()
            .map_err(|_| corruption("payload length is malformed"))?,
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
        || generation == 0
    {
        return Err(corruption("typed object lengths/generation are invalid"));
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

fn row_key(root: Id, logical_key: &[u8]) -> Key {
    let mut raw = Vec::with_capacity(32 + logical_key.len());
    raw.extend_from_slice(&root);
    raw.extend_from_slice(logical_key);
    key(Bytes::from(raw))
}

fn row_value(root: Id, logical_key: &[u8], payload: &[u8]) -> Bytes {
    let mut body = Vec::with_capacity(48 + logical_key.len() + payload.len());
    body.extend_from_slice(b"NLR1");
    body.extend_from_slice(&root);
    body.extend_from_slice(&(logical_key.len() as u16).to_be_bytes());
    body.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    body.extend_from_slice(logical_key);
    body.extend_from_slice(payload);
    authenticate(body)
}

fn decode_row(root: Id, entry_key: &Key, raw: &Bytes) -> Result<(Vec<u8>, Bytes), StorageError> {
    verify_checksum(raw)?;
    if raw.len() < 42 + 32 || &raw[..4] != b"NLR1" || read_id(raw, 4)? != root {
        return Err(corruption("row owner/root is malformed"));
    }
    let key_len = u16::from_be_bytes(
        raw[36..38]
            .try_into()
            .map_err(|_| corruption("row key length is malformed"))?,
    ) as usize;
    let payload_len = u32::from_be_bytes(
        raw[38..42]
            .try_into()
            .map_err(|_| corruption("row payload length is malformed"))?,
    ) as usize;
    if 42 + key_len + payload_len + 32 != raw.len() {
        return Err(corruption("row lengths are inconsistent"));
    }
    let logical_key = raw[42..42 + key_len].to_vec();
    if entry_key != &row_key(root, &logical_key) {
        return Err(corruption("row key does not bind its owner/value"));
    }
    Ok((
        logical_key,
        Bytes::copy_from_slice(&raw[42 + key_len..42 + key_len + payload_len]),
    ))
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
    let mut rows = Vec::new();
    let mut row_keys = Vec::new();
    for index in 0..24u64 {
        let logical_key = format!("row/{index:04}").into_bytes();
        let entry_key = row_key(catalog_id, &logical_key);
        row_keys.push(entry_key.clone());
        rows.push(PutEntry {
            key: entry_key,
            value: StoredValue {
                bytes: row_value(
                    catalog_id,
                    &logical_key,
                    format!("seed={seed},row={index}").as_bytes(),
                ),
            },
        });
    }
    Graph {
        root: Root {
            kind: Kind::Catalog,
            id: catalog_id,
        },
        objects: vec![
            PutEntry {
                key: object_key(blob_id),
                value: StoredValue { bytes: blob },
            },
            PutEntry {
                key: object_key(state_id),
                value: StoredValue { bytes: state },
            },
            PutEntry {
                key: object_key(edge_id),
                value: StoredValue { bytes: edge },
            },
            PutEntry {
                key: object_key(catalog_id),
                value: StoredValue { bytes: catalog },
            },
        ],
        rows,
        object_ids: vec![blob_id, state_id, edge_id, catalog_id],
        row_keys,
    }
}

fn upload_graph(seed: u64) -> Graph {
    let (blob_id, blob) = encode_object(Kind::Blob, 1, &[], &[seed as u8; 513]);
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
        root: Root {
            kind: Kind::Receipt,
            id: receipt_id,
        },
        objects: vec![
            PutEntry {
                key: object_key(blob_id),
                value: StoredValue { bytes: blob },
            },
            PutEntry {
                key: object_key(receipt_id),
                value: StoredValue { bytes: receipt },
            },
        ],
        rows: Vec::new(),
        object_ids: vec![blob_id, receipt_id],
        row_keys: Vec::new(),
    }
}

fn full_value(value: Option<ProjectedValue>) -> Result<Option<Bytes>, StorageError> {
    match value {
        None => Ok(None),
        Some(ProjectedValue::FullValue(raw)) => Ok(Some(raw)),
        Some(ProjectedValue::KeyOnly) => Err(corruption("unexpected key-only projection")),
    }
}

async fn get_from<R: StorageRead>(
    read: &R,
    space: lix::storage::StorageSpace,
    keys: &[Key],
    metrics: &mut Metrics,
) -> Result<Vec<Option<Bytes>>, StorageError> {
    metrics.gets += 1;
    metrics.get_keys += keys.len() as u64;
    let values = read
        .get_many(&[GetManyRequest {
            space,
            keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await?;
    values
        .values
        .into_iter()
        .map(|value| {
            let value = full_value(value)?;
            metrics.get_bytes += value.as_ref().map_or(0, |raw| raw.len() as u64);
            Ok(value)
        })
        .collect()
}

async fn get<S: Storage>(
    storage: &S,
    space: lix::storage::StorageSpace,
    keys: &[Key],
    metrics: &mut Metrics,
) -> Result<Vec<Option<Bytes>>, StorageError> {
    let read = storage.begin_read(ReadOptions::default()).await?;
    get_from(&read, space, keys, metrics).await
}

async fn scan_prefix<R: StorageRead>(
    read: &R,
    space: lix::storage::StorageSpace,
    prefix: &[u8],
    metrics: &mut Metrics,
) -> Result<Vec<(Key, Bytes)>, StorageError> {
    let range = prefix_range(prefix);
    let mut resume_after = None;
    let mut rows = Vec::new();
    loop {
        metrics.scans += 1;
        let page = read
            .scan(
                space,
                range.clone(),
                ScanOptions {
                    projection: CoreProjection::FullValue,
                    limit_rows: PAGE_ROWS,
                    resume_after: resume_after.clone(),
                },
            )
            .await?;
        if page.has_more && page.entries.is_empty() {
            return Err(corruption("scan returned an empty continuation page"));
        }
        for entry in page.entries {
            if resume_after.as_ref().is_some_and(|last| last >= &entry.key) {
                return Err(corruption("scan order is noncanonical"));
            }
            let raw = full_value(Some(entry.value))?
                .ok_or_else(|| corruption("scan row has no value"))?;
            metrics.scan_rows += 1;
            metrics.scan_bytes += (entry.key.0.len() + raw.len()) as u64;
            resume_after = Some(entry.key.clone());
            rows.push((entry.key, raw));
        }
        if !page.has_more {
            return Ok(rows);
        }
    }
}

async fn write_batches<S: Storage>(
    storage: &S,
    options: WriteOptions,
    puts: Vec<(lix::storage::StorageSpace, PutBatch)>,
    deletes: Vec<(lix::storage::StorageSpace, Vec<Key>)>,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let mut write = storage.begin_write(options).await?;
    for (space, batch) in puts {
        metrics.puts += batch.entries.len() as u64;
        metrics.write_bytes += batch
            .entries
            .iter()
            .map(|entry| (entry.key.0.len() + entry.value.bytes.len()) as u64)
            .sum::<u64>();
        write.put_many(space, batch).await?;
    }
    for (space, keys) in deletes {
        metrics.deletes += keys.len() as u64;
        metrics.write_bytes += keys.iter().map(|entry| entry.0.len() as u64).sum::<u64>();
        write.delete_many(space, &keys).await?;
    }
    write.commit().await?;
    metrics.commits += 1;
    Ok(())
}

async fn stage<S: Storage>(
    storage: &S,
    graph: &Graph,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let mut puts = vec![(
        OBJECTS,
        PutBatch {
            entries: graph.objects.clone(),
        },
    )];
    if !graph.rows.is_empty() {
        puts.push((
            ROWS,
            PutBatch {
                entries: graph.rows.clone(),
            },
        ));
    }
    write_batches(storage, WriteOptions::default(), puts, Vec::new(), metrics).await
}

async fn load_authority<S: Storage>(
    storage: &S,
    metrics: &mut Metrics,
) -> Result<(Bytes, Authority), StorageError> {
    let wanted = [key(Bytes::from_static(AUTHORITY_KEY))];
    let raw = get(storage, META, &wanted, metrics).await?[0]
        .clone()
        .ok_or_else(|| corruption("authority is absent"))?;
    Ok((raw.clone(), Authority::decode(&raw)?))
}

async fn progress_value<S: Storage>(
    storage: &S,
    metrics: &mut Metrics,
) -> Result<Option<Bytes>, StorageError> {
    Ok(get(
        storage,
        META,
        &[key(Bytes::from_static(PROGRESS_KEY))],
        metrics,
    )
    .await?[0]
        .clone())
}

fn progress_precondition(raw: &Option<Bytes>) -> Precondition {
    match raw {
        Some(raw) => Precondition::KeyValueEquals {
            space: META,
            key: key(Bytes::from_static(PROGRESS_KEY)),
            expected: raw.clone(),
        },
        None => Precondition::KeyAbsent {
            space: META,
            key: key(Bytes::from_static(PROGRESS_KEY)),
        },
    }
}

async fn seed<S: Storage>(storage: &S, metrics: &mut Metrics) -> Result<Graph, StorageError> {
    let initial = graph(1);
    stage(storage, &initial, metrics).await?;
    let authority = Authority {
        epoch: 1,
        watermark: 0,
        selected_generation: 1,
        active: initial.root.id,
    };
    let selectors = [Role::Checkpoint, Role::Undo, Role::Redo, Role::Child]
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
        });
    write_batches(
        storage,
        WriteOptions {
            preconditions: vec![Precondition::KeyAbsent {
                space: META,
                key: key(Bytes::from_static(AUTHORITY_KEY)),
            }],
            ..WriteOptions::default()
        },
        vec![(
            META,
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
    Ok(initial)
}

async fn read_object<R: StorageRead>(
    read: &R,
    id: Id,
    metrics: &mut Metrics,
) -> Result<Bytes, StorageError> {
    get_from(read, OBJECTS, &[object_key(id)], metrics).await?[0]
        .clone()
        .ok_or_else(|| corruption("reachable object is missing"))
}

async fn trace<R: StorageRead>(
    read: &R,
    roots: &[Root],
    metrics: &mut Metrics,
) -> Result<BTreeSet<Id>, StorageError> {
    let mut seen = BTreeSet::new();
    let mut queue = VecDeque::from(roots.to_vec());
    while let Some(expected) = queue.pop_front() {
        if !seen.insert(expected.id) {
            continue;
        }
        let raw = read_object(read, expected.id, metrics).await?;
        let object = decode_object(expected.id, &raw)?;
        if object.kind != expected.kind {
            return Err(corruption("typed root/edge kind mismatch"));
        }
        for child in object.children {
            let child_raw = read_object(read, child.id, metrics).await?;
            let child_object = decode_object(child.id, &child_raw)?;
            if child_object.kind != child.kind || child_object.generation >= object.generation {
                return Err(corruption("typed child kind/generation mismatch"));
            }
            queue.push_back(Root {
                kind: child.kind,
                id: child.id,
            });
        }
    }
    Ok(seen)
}

async fn validate_root<S: Storage>(
    storage: &S,
    root: Root,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let read = storage.begin_read(ReadOptions::default()).await?;
    trace(&read, &[root], metrics).await.map(|_| ())
}

async fn open_pinned<'a, S: Storage>(
    storage: &'a S,
    metrics: &mut Metrics,
) -> Result<PinnedView<S::Read<'a>>, StorageError> {
    let read = storage.begin_read(ReadOptions::default()).await?;
    let wanted = [key(Bytes::from_static(AUTHORITY_KEY))];
    let raw = get_from(&read, META, &wanted, metrics).await?[0]
        .clone()
        .ok_or_else(|| corruption("pinned view authority is absent"))?;
    let authority = Authority::decode(&raw)?;
    let root = Root {
        kind: Kind::Catalog,
        id: authority.active,
    };
    trace(&read, &[root], metrics).await?;
    let mut digest = blake3::Hasher::new();
    digest.update(b"no-lease-live-storage-read-v1");
    digest.update(&raw);
    digest.update(&authority.active);
    if let Some(cache_key) = read.snapshot_cache_key() {
        digest.update(&cache_key.to_be_bytes());
    }
    Ok(PinnedView {
        read,
        root: authority.active,
        view_id: *digest.finalize().as_bytes(),
    })
}

async fn page<R: StorageRead>(
    view: &PinnedView<R>,
    encoded_cursor: Option<&Bytes>,
    restart_after: Option<&[u8]>,
    limit: usize,
    metrics: &mut Metrics,
) -> Result<(Vec<(Vec<u8>, Bytes)>, Option<Bytes>), StorageError> {
    let cursor = encoded_cursor.map(Cursor::decode).transpose()?;
    if let Some(cursor) = &cursor {
        if cursor.view_id != view.view_id || cursor.root != view.root || restart_after.is_some() {
            return Err(StorageError::InvalidCursor);
        }
    }
    let lower = cursor
        .as_ref()
        .map(|cursor| cursor.last_delivered.as_slice())
        .or(restart_after);
    let prefix = view.root;
    let range = prefix_range(&prefix);
    let resume_after = lower.map(|logical| row_key(view.root, logical));
    metrics.scans += 1;
    let chunk = view
        .read
        .scan(
            ROWS,
            range,
            ScanOptions {
                projection: CoreProjection::FullValue,
                limit_rows: limit,
                resume_after,
            },
        )
        .await?;
    let mut result = Vec::new();
    for entry in chunk.entries {
        let raw =
            full_value(Some(entry.value))?.ok_or_else(|| corruption("page row has no value"))?;
        let decoded = decode_row(view.root, &entry.key, &raw)?;
        if lower.is_some_and(|last| decoded.0.as_slice() <= last)
            || result
                .last()
                .is_some_and(|(last, _): &(Vec<u8>, Bytes)| last >= &decoded.0)
        {
            return Err(corruption("page output is not strictly Excluded/ordered"));
        }
        metrics.scan_rows += 1;
        metrics.scan_bytes += (entry.key.0.len() + raw.len()) as u64;
        result.push(decoded);
    }
    let next = if chunk.has_more {
        let last = result
            .last()
            .ok_or_else(|| corruption("empty page claims continuation"))?
            .0
            .clone();
        Some(
            Cursor {
                view_id: view.view_id,
                root: view.root,
                last_delivered: last,
            }
            .encode(),
        )
    } else {
        None
    };
    Ok((result, next))
}

fn resume_without_live_read(_: &Bytes) -> Result<(), StorageError> {
    Err(StorageError::ReadExpired)
}

async fn selector_value<S: Storage>(
    storage: &S,
    role: Role,
    metrics: &mut Metrics,
) -> Result<Option<(Bytes, Selector)>, StorageError> {
    let wanted = selector_key(role);
    get(storage, META, std::slice::from_ref(&wanted), metrics).await?[0]
        .clone()
        .map(|raw| Selector::decode(&wanted, &raw).map(|decoded| (raw, decoded)))
        .transpose()
}

async fn prepare_publish<S: Storage>(
    storage: &S,
    expected_root: Id,
    next_root: Id,
    metrics: &mut Metrics,
) -> Result<PreparedPublish, StorageError> {
    validate_root(
        storage,
        Root {
            kind: Kind::Catalog,
            id: next_root,
        },
        metrics,
    )
    .await?;
    let (raw_authority, authority) = load_authority(storage, metrics).await?;
    if authority.active != expected_root {
        return Err(corruption("publication prepared from stale root"));
    }
    Ok(PreparedPublish {
        raw_authority,
        authority,
        expected_root,
        next_root,
        raw_progress: progress_value(storage, metrics).await?,
    })
}

async fn commit_publish<S: Storage>(
    storage: &S,
    prepared: &PreparedPublish,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let mut next = prepared.authority.rotated();
    next.selected_generation += 1;
    next.active = prepared.next_root;
    write_batches(
        storage,
        WriteOptions {
            preconditions: vec![
                Precondition::KeyValueEquals {
                    space: META,
                    key: key(Bytes::from_static(AUTHORITY_KEY)),
                    expected: prepared.raw_authority.clone(),
                },
                progress_precondition(&prepared.raw_progress),
            ],
            ..WriteOptions::default()
        },
        vec![(
            META,
            PutBatch {
                entries: vec![
                    PutEntry {
                        key: key(Bytes::from_static(AUTHORITY_KEY)),
                        value: StoredValue {
                            bytes: next.encode(),
                        },
                    },
                    PutEntry {
                        key: selector_key(Role::Recovery),
                        value: StoredValue {
                            bytes: Selector {
                                role: Role::Recovery,
                                generation: next.selected_generation,
                                root: prepared.expected_root,
                            }
                            .encode(),
                        },
                    },
                ],
            },
        )],
        vec![(META, vec![key(Bytes::from_static(PROGRESS_KEY))])],
        metrics,
    )
    .await
}

async fn set_selector<S: Storage>(
    storage: &S,
    role: Role,
    root: Id,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    validate_root(
        storage,
        Root {
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
            space: META,
            key: key(Bytes::from_static(AUTHORITY_KEY)),
            expected: raw_authority,
        },
        progress_precondition(&raw_progress),
    ];
    preconditions.push(match existing {
        Some((raw, _)) => Precondition::KeyValueEquals {
            space: META,
            key: selector_key(role),
            expected: raw,
        },
        None => Precondition::KeyAbsent {
            space: META,
            key: selector_key(role),
        },
    });
    let next = authority.rotated();
    write_batches(
        storage,
        WriteOptions {
            preconditions,
            ..WriteOptions::default()
        },
        vec![(
            META,
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
        vec![(META, vec![key(Bytes::from_static(PROGRESS_KEY))])],
        metrics,
    )
    .await
}

async fn delete_selector<S: Storage>(
    storage: &S,
    role: Role,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let (raw_authority, authority) = load_authority(storage, metrics).await?;
    let raw_progress = progress_value(storage, metrics).await?;
    let (raw_selector, _) = selector_value(storage, role, metrics)
        .await?
        .ok_or_else(|| corruption("selector to delete is absent"))?;
    write_batches(
        storage,
        WriteOptions {
            preconditions: vec![
                Precondition::KeyValueEquals {
                    space: META,
                    key: key(Bytes::from_static(AUTHORITY_KEY)),
                    expected: raw_authority,
                },
                Precondition::KeyValueEquals {
                    space: META,
                    key: selector_key(role),
                    expected: raw_selector,
                },
                progress_precondition(&raw_progress),
            ],
            ..WriteOptions::default()
        },
        vec![(
            META,
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
            META,
            vec![selector_key(role), key(Bytes::from_static(PROGRESS_KEY))],
        )],
        metrics,
    )
    .await
}

async fn restore_selector<S: Storage>(
    storage: &S,
    role: Role,
    metrics: &mut Metrics,
) -> Result<Id, StorageError> {
    let read = storage.begin_read(ReadOptions::default()).await?;
    let wanted = [
        key(Bytes::from_static(AUTHORITY_KEY)),
        selector_key(role),
        key(Bytes::from_static(PROGRESS_KEY)),
    ];
    let values = get_from(&read, META, &wanted, metrics).await?;
    let raw_authority = values[0]
        .clone()
        .ok_or_else(|| corruption("restore authority is absent"))?;
    let authority = Authority::decode(&raw_authority)?;
    let raw_selector = values[1]
        .clone()
        .ok_or_else(|| corruption("restore selector is absent"))?;
    let selector = Selector::decode(&wanted[1], &raw_selector)?;
    trace(&read, &[selector.root()], metrics).await?;
    drop(read);
    let mut next = authority.rotated();
    next.selected_generation += 1;
    next.active = selector.root;
    write_batches(
        storage,
        WriteOptions {
            preconditions: vec![
                Precondition::KeyValueEquals {
                    space: META,
                    key: key(Bytes::from_static(AUTHORITY_KEY)),
                    expected: raw_authority,
                },
                Precondition::KeyValueEquals {
                    space: META,
                    key: selector_key(role),
                    expected: raw_selector,
                },
                progress_precondition(&values[2]),
            ],
            ..WriteOptions::default()
        },
        vec![(
            META,
            PutBatch {
                entries: vec![
                    PutEntry {
                        key: key(Bytes::from_static(AUTHORITY_KEY)),
                        value: StoredValue {
                            bytes: next.encode(),
                        },
                    },
                    PutEntry {
                        key: selector_key(Role::Recovery),
                        value: StoredValue {
                            bytes: Selector {
                                role: Role::Recovery,
                                generation: next.selected_generation,
                                root: authority.active,
                            }
                            .encode(),
                        },
                    },
                ],
            },
        )],
        vec![(META, vec![key(Bytes::from_static(PROGRESS_KEY))])],
        metrics,
    )
    .await?;
    Ok(selector.root)
}

fn root_digest(roots: &[Root]) -> Id {
    let mut canonical = roots.to_vec();
    canonical.sort_unstable();
    canonical.dedup();
    let mut digest = blake3::Hasher::new();
    digest.update(b"no-lease-gc-roots-v1");
    for root in canonical {
        digest.update(&[root.kind as u8]);
        digest.update(&root.id);
    }
    *digest.finalize().as_bytes()
}

async fn collect_roots<R: StorageRead>(
    read: &R,
    authority: &Authority,
    metrics: &mut Metrics,
) -> Result<Vec<Root>, StorageError> {
    let mut roots = vec![Root {
        kind: Kind::Catalog,
        id: authority.active,
    }];
    for (selector_key, raw) in scan_prefix(read, META, SELECTOR_PREFIX, metrics).await? {
        roots.push(Selector::decode(&selector_key, &raw)?.root());
    }
    roots.sort_unstable();
    roots.dedup();
    Ok(roots)
}

async fn prepare_gc<S: Storage>(
    storage: &S,
    metrics: &mut Metrics,
) -> Result<PreparedGc, StorageError> {
    let read = storage.begin_read(ReadOptions::default()).await?;
    let wanted = [key(Bytes::from_static(AUTHORITY_KEY))];
    let values = get_from(&read, META, &wanted, metrics).await?;
    let raw_authority = values[0]
        .clone()
        .ok_or_else(|| corruption("GC authority is absent"))?;
    let authority = Authority::decode(&raw_authority)?;
    let roots = collect_roots(&read, &authority, metrics).await?;
    trace(&read, &roots, metrics).await?;
    let next_authority = authority.rotated();
    Ok(PreparedGc {
        raw_authority,
        next_authority: next_authority.clone(),
        progress: Progress {
            cycle: next_authority.epoch,
            fenced_authority: next_authority.encode(),
            root_count: roots.len() as u64,
            root_digest: root_digest(&roots),
        },
    })
}

async fn start_gc<S: Storage>(
    storage: &S,
    prepared: &PreparedGc,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    write_batches(
        storage,
        WriteOptions {
            preconditions: vec![
                Precondition::KeyValueEquals {
                    space: META,
                    key: key(Bytes::from_static(AUTHORITY_KEY)),
                    expected: prepared.raw_authority.clone(),
                },
                Precondition::KeyAbsent {
                    space: META,
                    key: key(Bytes::from_static(PROGRESS_KEY)),
                },
            ],
            ..WriteOptions::default()
        },
        vec![(
            META,
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
                            bytes: prepared.progress.encode(),
                        },
                    },
                ],
            },
        )],
        Vec::new(),
        metrics,
    )
    .await
}

fn gc_fence(authority: &Bytes, progress: &Bytes) -> Vec<Precondition> {
    vec![
        Precondition::KeyValueEquals {
            space: META,
            key: key(Bytes::from_static(AUTHORITY_KEY)),
            expected: authority.clone(),
        },
        Precondition::KeyValueEquals {
            space: META,
            key: key(Bytes::from_static(PROGRESS_KEY)),
            expected: progress.clone(),
        },
    ]
}

async fn resume_gc<S: Storage>(storage: &S, metrics: &mut Metrics) -> Result<(), StorageError> {
    let read = storage.begin_read(ReadOptions::default()).await?;
    let wanted = [
        key(Bytes::from_static(AUTHORITY_KEY)),
        key(Bytes::from_static(PROGRESS_KEY)),
    ];
    let values = get_from(&read, META, &wanted, metrics).await?;
    let raw_authority = values[0]
        .clone()
        .ok_or_else(|| corruption("fenced authority is absent"))?;
    let authority = Authority::decode(&raw_authority)?;
    let raw_progress = values[1]
        .clone()
        .ok_or_else(|| corruption("GC progress is absent"))?;
    let progress = Progress::decode(&raw_progress)?;
    if progress.fenced_authority != raw_authority || progress.cycle != authority.epoch {
        return Err(corruption("GC progress does not bind raw authority"));
    }
    let roots = collect_roots(&read, &authority, metrics).await?;
    if progress.root_count != roots.len() as u64 || progress.root_digest != root_digest(&roots) {
        return Err(corruption("GC root closure is stale/corrupt"));
    }
    let reachable = trace(&read, &roots, metrics).await?;
    let objects = scan_prefix(&read, OBJECTS, b"", metrics).await?;
    let rows = scan_prefix(&read, ROWS, b"", metrics).await?;
    drop(read);

    for chunk in objects.chunks(PAGE_ROWS) {
        let deletes = chunk
            .iter()
            .filter_map(|(entry_key, _)| {
                let id = read_id(&entry_key.0, 0).expect("object identity key");
                (!reachable.contains(&id)).then_some(entry_key.clone())
            })
            .collect::<Vec<_>>();
        if !deletes.is_empty() {
            metrics.swept_objects += deletes.len() as u64;
            write_batches(
                storage,
                WriteOptions {
                    preconditions: gc_fence(&raw_authority, &raw_progress),
                    ..WriteOptions::default()
                },
                Vec::new(),
                vec![(OBJECTS, deletes)],
                metrics,
            )
            .await?;
        }
    }
    for chunk in rows.chunks(PAGE_ROWS) {
        let deletes = chunk
            .iter()
            .filter_map(|(entry_key, _)| {
                let owner = read_id(&entry_key.0, 0).expect("row owner key");
                (!reachable.contains(&owner)).then_some(entry_key.clone())
            })
            .collect::<Vec<_>>();
        if !deletes.is_empty() {
            metrics.swept_rows += deletes.len() as u64;
            write_batches(
                storage,
                WriteOptions {
                    preconditions: gc_fence(&raw_authority, &raw_progress),
                    ..WriteOptions::default()
                },
                Vec::new(),
                vec![(ROWS, deletes)],
                metrics,
            )
            .await?;
        }
    }
    let mut completed = authority.rotated();
    completed.watermark = progress.cycle;
    write_batches(
        storage,
        WriteOptions {
            preconditions: gc_fence(&raw_authority, &raw_progress),
            ..WriteOptions::default()
        },
        vec![(
            META,
            PutBatch {
                entries: vec![PutEntry {
                    key: key(Bytes::from_static(AUTHORITY_KEY)),
                    value: StoredValue {
                        bytes: completed.encode(),
                    },
                }],
            },
        )],
        vec![(META, vec![key(Bytes::from_static(PROGRESS_KEY))])],
        metrics,
    )
    .await
}

async fn collect_gc<S: Storage>(storage: &S, metrics: &mut Metrics) -> Result<(), StorageError> {
    let prepared = prepare_gc(storage, metrics).await?;
    start_gc(storage, &prepared, metrics).await?;
    resume_gc(storage, metrics).await
}

async fn graph_present<S: Storage>(
    storage: &S,
    graph: &Graph,
    expected: bool,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    for id in &graph.object_ids {
        assert_eq!(
            get(storage, OBJECTS, &[object_key(*id)], metrics).await?[0].is_some(),
            expected
        );
    }
    for row in &graph.row_keys {
        assert_eq!(
            get(storage, ROWS, std::slice::from_ref(row), metrics).await?[0].is_some(),
            expected
        );
    }
    Ok(())
}

async fn assert_no_persisted_reader_state<S: Storage>(
    storage: &S,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let read = storage.begin_read(ReadOptions::default()).await?;
    // These are forbidden residue probes, not owner spaces used by the model.
    for forbidden in [b"lease/".as_slice(), b"reader/", b"cursor/"] {
        assert!(
            scan_prefix(&read, META, forbidden, metrics)
                .await?
                .is_empty(),
            "persisted reader/cursor authority residue under {forbidden:?}"
        );
    }
    for (entry_key, _) in scan_prefix(&read, META, b"", metrics).await? {
        assert!(
            entry_key.0.as_ref() == AUTHORITY_KEY
                || entry_key.0.as_ref() == PROGRESS_KEY
                || entry_key.0.starts_with(SELECTOR_PREFIX),
            "unexpected persisted metadata owner {:?}",
            entry_key.0
        );
    }
    Ok(())
}

async fn overwrite_selector_raw<S: Storage>(
    storage: &S,
    role: Role,
    raw: Bytes,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    write_batches(
        storage,
        WriteOptions::default(),
        vec![(
            META,
            PutBatch {
                entries: vec![PutEntry {
                    key: selector_key(role),
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
    write_batches(
        storage,
        WriteOptions::default(),
        vec![(OBJECTS, PutBatch { entries })],
        Vec::new(),
        metrics,
    )
    .await
}

fn corrupt_leaf(kind: Kind, generation: u64) -> (Id, Bytes) {
    let (_, raw) = encode_object(kind, generation, &[], b"corrupt");
    let mut bytes = raw.to_vec();
    let last = bytes.len() - 1;
    bytes[last] ^= 0x80;
    let raw = Bytes::from(bytes);
    (*blake3::hash(&raw).as_bytes(), raw)
}

async fn object_count<S: Storage>(
    storage: &S,
    metrics: &mut Metrics,
) -> Result<usize, StorageError> {
    let read = storage.begin_read(ReadOptions::default()).await?;
    Ok(scan_prefix(&read, OBJECTS, b"", metrics).await?.len())
}

async fn assert_corruption_before_delete<S: Storage>(
    storage: &S,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let before = object_count(storage, metrics).await?;
    assert!(prepare_gc(storage, metrics).await.is_err());
    assert_eq!(object_count(storage, metrics).await?, before);
    Ok(())
}

async fn corruption_oracle<S: Storage>(storage: &S) -> Result<Metrics, StorageError> {
    let mut metrics = Metrics::default();
    let initial = seed(storage, &mut metrics).await?;

    overwrite_selector_raw(
        storage,
        Role::Recovery,
        Bytes::from_static(b"malformed-selector"),
        &mut metrics,
    )
    .await?;
    assert!(
        restore_selector(storage, Role::Recovery, &mut metrics)
            .await
            .is_err()
    );
    assert_corruption_before_delete(storage, &mut metrics).await?;

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
            key: object_key(torn_id),
            value: StoredValue { bytes: torn_raw },
        }],
        &mut metrics,
    )
    .await?;
    overwrite_selector_raw(
        storage,
        Role::Recovery,
        Selector {
            role: Role::Recovery,
            generation: 1,
            root: torn_id,
        }
        .encode(),
        &mut metrics,
    )
    .await?;
    assert_corruption_before_delete(storage, &mut metrics).await?;

    let (blob_id, blob) = encode_object(Kind::Blob, 1, &[], b"blob");
    let (mistyped_id, mistyped) = encode_object(
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
                key: object_key(blob_id),
                value: StoredValue { bytes: blob },
            },
            PutEntry {
                key: object_key(mistyped_id),
                value: StoredValue { bytes: mistyped },
            },
        ],
        &mut metrics,
    )
    .await?;
    overwrite_selector_raw(
        storage,
        Role::Recovery,
        Selector {
            role: Role::Recovery,
            generation: 1,
            root: mistyped_id,
        }
        .encode(),
        &mut metrics,
    )
    .await?;
    assert_corruption_before_delete(storage, &mut metrics).await?;

    let (same_state, same_state_raw) = encode_object(Kind::State, 2, &[], b"state");
    let (same_catalog, same_catalog_raw) = encode_object(
        Kind::Catalog,
        2,
        &[Child {
            kind: Kind::State,
            id: same_state,
        }],
        b"catalog",
    );
    stage_raw_objects(
        storage,
        vec![
            PutEntry {
                key: object_key(same_state),
                value: StoredValue {
                    bytes: same_state_raw,
                },
            },
            PutEntry {
                key: object_key(same_catalog),
                value: StoredValue {
                    bytes: same_catalog_raw,
                },
            },
        ],
        &mut metrics,
    )
    .await?;
    overwrite_selector_raw(
        storage,
        Role::Recovery,
        Selector {
            role: Role::Recovery,
            generation: 1,
            root: same_catalog,
        }
        .encode(),
        &mut metrics,
    )
    .await?;
    assert_corruption_before_delete(storage, &mut metrics).await?;

    for (case, kind, expected) in [
        (100u64, Kind::Catalog, Kind::Catalog),
        (101, Kind::State, Kind::State),
        (102, Kind::Blob, Kind::Blob),
        (103, Kind::Edge, Kind::Edge),
    ] {
        let generation = match kind {
            Kind::Catalog => 4,
            Kind::Edge => 3,
            Kind::State => 2,
            Kind::Blob => 1,
            Kind::Receipt => unreachable!(),
        };
        let (bad_id, bad_raw) = corrupt_leaf(kind, generation);
        let root = if kind == Kind::Catalog {
            bad_id
        } else {
            let (catalog_id, catalog_raw) = encode_object(
                Kind::Catalog,
                generation + 1,
                &[Child {
                    kind: expected,
                    id: bad_id,
                }],
                &case.to_be_bytes(),
            );
            stage_raw_objects(
                storage,
                vec![PutEntry {
                    key: object_key(catalog_id),
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
                key: object_key(bad_id),
                value: StoredValue { bytes: bad_raw },
            }],
            &mut metrics,
        )
        .await?;
        overwrite_selector_raw(
            storage,
            Role::Recovery,
            Selector {
                role: Role::Recovery,
                generation: 1,
                root,
            }
            .encode(),
            &mut metrics,
        )
        .await?;
        assert_corruption_before_delete(storage, &mut metrics).await?;
    }

    let (bad_receipt, bad_receipt_raw) = corrupt_leaf(Kind::Receipt, 2);
    stage_raw_objects(
        storage,
        vec![PutEntry {
            key: object_key(bad_receipt),
            value: StoredValue {
                bytes: bad_receipt_raw,
            },
        }],
        &mut metrics,
    )
    .await?;
    overwrite_selector_raw(
        storage,
        Role::Recovery,
        Selector {
            role: Role::Recovery,
            generation: 1,
            root: initial.root.id,
        }
        .encode(),
        &mut metrics,
    )
    .await?;
    overwrite_selector_raw(
        storage,
        Role::Upload,
        Selector {
            role: Role::Upload,
            generation: 1,
            root: bad_receipt,
        }
        .encode(),
        &mut metrics,
    )
    .await?;
    assert_corruption_before_delete(storage, &mut metrics).await?;
    assert_no_persisted_reader_state(storage, &mut metrics).await?;
    Ok(metrics)
}

async fn verify_active<S: Storage>(
    storage: &S,
    expected: Id,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    let (_, authority) = load_authority(storage, metrics).await?;
    assert_eq!(authority.active, expected);
    validate_root(
        storage,
        Root {
            kind: Kind::Catalog,
            id: expected,
        },
        metrics,
    )
    .await
}

async fn after_selector_crash<S: Storage>(
    storage: &S,
    initial: &Graph,
    published: &Graph,
    metrics: &mut Metrics,
) -> Result<(Id, Graph), StorageError> {
    verify_active(storage, published.root.id, metrics).await?;
    assert_eq!(
        selector_value(storage, Role::Recovery, metrics)
            .await?
            .expect("recovery selector")
            .1
            .root,
        initial.root.id
    );

    // Checkpoint, recovery, undo, and redo each restore through the same global
    // authority; child is retained as an independent dependency root.
    assert_eq!(
        restore_selector(storage, Role::Checkpoint, metrics).await?,
        initial.root.id
    );
    assert_eq!(
        restore_selector(storage, Role::Recovery, metrics).await?,
        published.root.id
    );
    assert_eq!(
        restore_selector(storage, Role::Undo, metrics).await?,
        initial.root.id
    );
    assert_eq!(
        restore_selector(storage, Role::Recovery, metrics).await?,
        published.root.id
    );
    assert_eq!(
        restore_selector(storage, Role::Redo, metrics).await?,
        initial.root.id
    );
    assert_eq!(
        restore_selector(storage, Role::Recovery, metrics).await?,
        published.root.id
    );

    let upload = upload_graph(40);
    stage(storage, &upload, metrics).await?;
    set_selector(storage, Role::Upload, upload.root.id, metrics).await?;
    collect_gc(storage, metrics).await?;
    graph_present(storage, &upload, true, metrics).await?;

    // Publication-first and GC-first are both fenced by raw authority plus
    // exact progress presence/value.
    let stale_gc = prepare_gc(storage, metrics).await?;
    let publication_first = graph(4);
    stage(storage, &publication_first, metrics).await?;
    let prepared = prepare_publish(
        storage,
        published.root.id,
        publication_first.root.id,
        metrics,
    )
    .await?;
    commit_publish(storage, &prepared, metrics).await?;
    assert!(matches!(
        start_gc(storage, &stale_gc, metrics).await,
        Err(StorageError::PreconditionFailed(_))
    ));

    let gc_first = prepare_gc(storage, metrics).await?;
    start_gc(storage, &gc_first, metrics).await?;
    let publication_after_gc = graph(5);
    stage(storage, &publication_after_gc, metrics).await?;
    let prepared = prepare_publish(
        storage,
        publication_first.root.id,
        publication_after_gc.root.id,
        metrics,
    )
    .await?;
    commit_publish(storage, &prepared, metrics).await?;
    assert!(resume_gc(storage, metrics).await.is_err());

    let winner = graph(6);
    let loser = graph(7);
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

    // All persisted roots are moved to the selected graph before opening the
    // old live read. After publication, removing recovery makes the old graph
    // unreachable to new reads, yet the live StorageRead must finish pages.
    for role in [Role::Checkpoint, Role::Undo, Role::Redo, Role::Child] {
        set_selector(storage, role, winner.root.id, metrics).await?;
    }
    let old_view = open_pinned(storage, metrics).await?;
    let (first_page, cursor) = page(&old_view, None, None, 8, metrics).await?;
    assert_eq!(first_page.len(), 8);
    let cursor = cursor.expect("old view continuation");
    let last_authenticated = first_page.last().expect("first page").0.clone();

    let next = graph(8);
    stage(storage, &next, metrics).await?;
    let prepared = prepare_publish(storage, winner.root.id, next.root.id, metrics).await?;
    commit_publish(storage, &prepared, metrics).await?;
    for role in [Role::Checkpoint, Role::Undo, Role::Redo, Role::Child] {
        set_selector(storage, role, next.root.id, metrics).await?;
    }
    delete_selector(storage, Role::Recovery, metrics).await?;
    collect_gc(storage, metrics).await?;
    graph_present(storage, &winner, false, metrics).await?;
    let (second_page, _) = page(&old_view, Some(&cursor), None, 8, metrics).await?;
    assert_eq!(second_page.len(), 8);
    assert!(
        get_from(
            &old_view.read,
            OBJECTS,
            &[object_key(winner.root.id)],
            metrics
        )
        .await?[0]
            .is_some(),
        "live StorageRead lost a physically swept object"
    );

    let fresh = open_pinned(storage, metrics).await?;
    assert!(matches!(
        page(&fresh, Some(&cursor), None, 8, metrics).await,
        Err(StorageError::InvalidCursor)
    ));
    let (restarted, _) = page(&fresh, None, Some(&last_authenticated), 8, metrics).await?;
    assert_eq!(restarted.len(), 8);
    assert!(restarted[0].0 > last_authenticated);
    drop(fresh);
    drop(old_view);
    assert!(matches!(
        resume_without_live_read(&cursor),
        Err(StorageError::ReadExpired)
    ));

    // Cancellation releases only the process-local read; continuation cannot
    // manufacture a durable owner after Drop.
    let cancelled = open_pinned(storage, metrics).await?;
    let (_, cancelled_cursor) = page(&cancelled, None, None, 8, metrics).await?;
    let cancelled_cursor = cancelled_cursor.expect("cancelled continuation");
    drop(cancelled);
    assert!(matches!(
        resume_without_live_read(&cancelled_cursor),
        Err(StorageError::ReadExpired)
    ));

    // Malformed cursor is invalid in a live view; malformed page terminates the
    // view and any subsequent continuation is expired.
    let malformed_view = open_pinned(storage, metrics).await?;
    let mut malformed_cursor = Cursor {
        view_id: malformed_view.view_id,
        root: malformed_view.root,
        last_delivered: b"row/0000".to_vec(),
    }
    .encode()
    .to_vec();
    *malformed_cursor.last_mut().expect("cursor checksum") ^= 1;
    assert!(matches!(
        page(
            &malformed_view,
            Some(&Bytes::from(malformed_cursor)),
            None,
            8,
            metrics
        )
        .await,
        Err(StorageError::InvalidCursor)
    ));
    drop(malformed_view);

    let mut malformed_graph = graph(9);
    let bad_token = Cursor {
        view_id: [0x55; 32],
        root: malformed_graph.root.id,
        last_delivered: Vec::new(),
    }
    .encode();
    let row = &mut malformed_graph.rows[0].value.bytes;
    let mut raw = row.to_vec();
    *raw.last_mut().expect("row checksum") ^= 1;
    *row = Bytes::from(raw);
    stage(storage, &malformed_graph, metrics).await?;
    let prepared = prepare_publish(storage, next.root.id, malformed_graph.root.id, metrics).await?;
    commit_publish(storage, &prepared, metrics).await?;
    let bad_view = open_pinned(storage, metrics).await?;
    assert!(page(&bad_view, None, None, 8, metrics).await.is_err());
    drop(bad_view);
    assert!(matches!(
        resume_without_live_read(&bad_token),
        Err(StorageError::ReadExpired)
    ));

    // Final-reference reclamation is selector-only. Removing a strict subset
    // retains the shared graph; deleting the final child dependency permits it.
    let shared = graph(10);
    stage(storage, &shared, metrics).await?;
    let prepared =
        prepare_publish(storage, malformed_graph.root.id, shared.root.id, metrics).await?;
    commit_publish(storage, &prepared, metrics).await?;
    for role in [Role::Checkpoint, Role::Undo, Role::Redo, Role::Child] {
        set_selector(storage, role, shared.root.id, metrics).await?;
    }
    let final_active = graph(11);
    stage(storage, &final_active, metrics).await?;
    let prepared = prepare_publish(storage, shared.root.id, final_active.root.id, metrics).await?;
    commit_publish(storage, &prepared, metrics).await?;
    delete_selector(storage, Role::Recovery, metrics).await?;
    for role in [Role::Checkpoint, Role::Undo, Role::Redo] {
        delete_selector(storage, role, metrics).await?;
    }
    collect_gc(storage, metrics).await?;
    graph_present(storage, &shared, true, metrics).await?;
    delete_selector(storage, Role::Child, metrics).await?;
    collect_gc(storage, metrics).await?;
    graph_present(storage, &shared, false, metrics).await?;
    graph_present(storage, &loser, false, metrics).await?;

    delete_selector(storage, Role::Upload, metrics).await?;
    collect_gc(storage, metrics).await?;
    graph_present(storage, &upload, false, metrics).await?;
    verify_active(storage, final_active.root.id, metrics).await?;
    assert_no_persisted_reader_state(storage, metrics).await?;

    // Crash after GC start leaves only rebuildable progress. Reopen recomputes
    // exact roots and completes deletion of this unpublished graph.
    let crash_orphan = graph(12);
    stage(storage, &crash_orphan, metrics).await?;
    let pending = prepare_gc(storage, metrics).await?;
    start_gc(storage, &pending, metrics).await?;
    Ok((final_active.root.id, crash_orphan))
}

async fn verify_gc_reopen<S: Storage>(
    storage: &S,
    active: Id,
    orphan: &Graph,
    metrics: &mut Metrics,
) -> Result<(), StorageError> {
    resume_gc(storage, metrics).await?;
    graph_present(storage, orphan, false, metrics).await?;
    verify_active(storage, active, metrics).await?;
    assert_no_persisted_reader_state(storage, metrics).await
}

fn begin_profile() {
    PROFILE_ALLOCATED.store(0, Ordering::Relaxed);
    PROFILE_CALLS.store(0, Ordering::Relaxed);
    PROFILE_ENABLED.store(true, Ordering::Relaxed);
}

fn end_profile() -> (u64, u64) {
    PROFILE_ENABLED.store(false, Ordering::Relaxed);
    (
        PROFILE_ALLOCATED.load(Ordering::Relaxed),
        PROFILE_CALLS.load(Ordering::Relaxed),
    )
}

fn process_rss() -> u64 {
    std::fs::read_to_string("/proc/self/statm")
        .expect("read statm")
        .split_whitespace()
        .nth(1)
        .expect("resident pages")
        .parse::<u64>()
        .expect("parse resident pages")
        * 4096
}

fn process_cpu_nanos() -> u64 {
    std::fs::read_dir("/proc/self/task")
        .expect("read task directory")
        .flatten()
        .filter_map(|entry| std::fs::read_to_string(entry.path().join("schedstat")).ok())
        .filter_map(|raw| raw.split_whitespace().next()?.parse::<u64>().ok())
        .sum()
}

fn directory_bytes(path: &Path) -> u64 {
    fn visit(path: &Path) -> u64 {
        let Ok(metadata) = std::fs::symlink_metadata(path) else {
            return 0;
        };
        if metadata.is_file() {
            return metadata.len();
        }
        if !metadata.is_dir() {
            return 0;
        }
        std::fs::read_dir(path).map_or(0, |entries| {
            entries.flatten().map(|entry| visit(&entry.path())).sum()
        })
    }
    visit(path)
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
        "forktree_stage2_recovery_no_lease,verdict=green,backend={backend},wall_us={wall_us:.3},cpu_us={cpu_us:.3},alloc_bytes={allocated},alloc_calls={allocation_calls},rss_before={rss_before},rss_after={rss_after},disk_bytes={disk_bytes},get_calls={},get_keys={},get_bytes={},scan_calls={},scan_rows={},scan_bytes={},commits={},puts={},deletes={},logical_write_bytes={},swept_objects={},swept_rows={},slate_read_objects={},slate_read_bytes={},slate_write_objects={},slate_write_bytes={},slate_list_operations={},slate_deleted_objects={},crash_before_stage=pass,crash_after_stage=pass,crash_after_selector_cas=pass,crash_after_gc_start=pass,live_storage_read_pin=pass,cursor_drop_expiry=pass,cursor_cancel_expiry=pass,malformed_cursor=pass,malformed_page_expiry=pass,excluded_restart=pass,checkpoint_recovery_undo_redo=pass,child_dependency=pass,upload_root=pass,publication_gc_both_orders=pass,final_reference=pass,typed_corruption=pass,no_persisted_reader_authority=pass",
        metrics.gets,
        metrics.get_keys,
        metrics.get_bytes,
        metrics.scans,
        metrics.scan_rows,
        metrics.scan_bytes,
        metrics.commits,
        metrics.puts,
        metrics.deletes,
        metrics.write_bytes,
        metrics.swept_objects,
        metrics.swept_rows,
        physical.read_objects,
        physical.read_bytes,
        physical.write_objects,
        physical.write_bytes,
        physical.list_operations,
        physical.deleted_objects,
    );
}

async fn run_rocks() -> Result<(), StorageError> {
    let main_dir = tempfile::tempdir().expect("RocksDB oracle directory");
    let main_path = main_dir.path().join("main");
    let corrupt_dir = tempfile::tempdir().expect("RocksDB corruption directory");
    let corrupt_path = corrupt_dir.path().join("corrupt");
    let mut metrics = Metrics::default();
    let rss_before = process_rss();
    let cpu_before = process_cpu_nanos();
    begin_profile();
    let started = Instant::now();

    let initial = {
        let storage = RocksDB::open(&main_path).expect("open RocksDB seed");
        let initial = seed(&storage, &mut metrics).await?;
        storage.flush()?;
        initial
    };
    let staged_orphan = graph(2);
    {
        let storage = RocksDB::open(&main_path).expect("reopen before stage");
        verify_active(&storage, initial.root.id, &mut metrics).await?;
        graph_present(&storage, &staged_orphan, false, &mut metrics).await?;
        stage(&storage, &staged_orphan, &mut metrics).await?;
        storage.flush()?;
    }
    let published = graph(3);
    {
        let storage = RocksDB::open(&main_path).expect("reopen after stage");
        graph_present(&storage, &staged_orphan, true, &mut metrics).await?;
        collect_gc(&storage, &mut metrics).await?;
        graph_present(&storage, &staged_orphan, false, &mut metrics).await?;
        stage(&storage, &published, &mut metrics).await?;
        let prepared =
            prepare_publish(&storage, initial.root.id, published.root.id, &mut metrics).await?;
        commit_publish(&storage, &prepared, &mut metrics).await?;
        storage.flush()?;
    }
    let (active, crash_orphan) = {
        let storage = RocksDB::open(&main_path).expect("reopen after selector CAS");
        let result = after_selector_crash(&storage, &initial, &published, &mut metrics).await?;
        storage.flush()?;
        result
    };
    {
        let storage = RocksDB::open(&main_path).expect("reopen after GC start");
        verify_gc_reopen(&storage, active, &crash_orphan, &mut metrics).await?;
        storage.flush()?;
    }
    {
        let storage = RocksDB::open(&corrupt_path).expect("open RocksDB corruption");
        metrics += corruption_oracle(&storage).await?;
        storage.flush()?;
    }
    let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
    let (allocated, calls) = end_profile();
    print_result(
        "rocksdb",
        wall_us,
        process_cpu_nanos().saturating_sub(cpu_before) as f64 / 1_000.0,
        allocated,
        calls,
        rss_before,
        process_rss(),
        directory_bytes(&main_path) + directory_bytes(&corrupt_path),
        &metrics,
        SlateDBIoSnapshot::default(),
    );
    Ok(())
}

async fn run_slate() -> Result<(), StorageError> {
    let main_dir = tempfile::tempdir().expect("SlateDB oracle directory");
    let main_path = main_dir.path().join("main");
    let corrupt_dir = tempfile::tempdir().expect("SlateDB corruption directory");
    let corrupt_path = corrupt_dir.path().join("corrupt");
    let counters = SlateDBIoCounters::default();
    let physical_before = counters.snapshot();
    let mut metrics = Metrics::default();
    let rss_before = process_rss();
    let cpu_before = process_cpu_nanos();
    begin_profile();
    let started = Instant::now();

    let initial = {
        let storage = SlateDB::open_with_io_counters(&main_path, counters.clone())
            .expect("open SlateDB seed");
        let initial = seed(&storage, &mut metrics).await?;
        storage.flush().await?;
        initial
    };
    let staged_orphan = graph(2);
    {
        let storage = SlateDB::open_with_io_counters(&main_path, counters.clone())
            .expect("reopen before stage");
        verify_active(&storage, initial.root.id, &mut metrics).await?;
        graph_present(&storage, &staged_orphan, false, &mut metrics).await?;
        stage(&storage, &staged_orphan, &mut metrics).await?;
        storage.flush().await?;
    }
    let published = graph(3);
    {
        let storage = SlateDB::open_with_io_counters(&main_path, counters.clone())
            .expect("reopen after stage");
        graph_present(&storage, &staged_orphan, true, &mut metrics).await?;
        collect_gc(&storage, &mut metrics).await?;
        graph_present(&storage, &staged_orphan, false, &mut metrics).await?;
        stage(&storage, &published, &mut metrics).await?;
        let prepared =
            prepare_publish(&storage, initial.root.id, published.root.id, &mut metrics).await?;
        commit_publish(&storage, &prepared, &mut metrics).await?;
        storage.flush().await?;
    }
    let (active, crash_orphan) = {
        let storage = SlateDB::open_with_io_counters(&main_path, counters.clone())
            .expect("reopen after selector CAS");
        let result = after_selector_crash(&storage, &initial, &published, &mut metrics).await?;
        storage.flush().await?;
        result
    };
    {
        let storage = SlateDB::open_with_io_counters(&main_path, counters.clone())
            .expect("reopen after GC start");
        verify_gc_reopen(&storage, active, &crash_orphan, &mut metrics).await?;
        storage.flush().await?;
    }
    {
        let storage = SlateDB::open_with_io_counters(&corrupt_path, counters.clone())
            .expect("open SlateDB corruption");
        metrics += corruption_oracle(&storage).await?;
        storage.flush().await?;
    }
    let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
    let (allocated, calls) = end_profile();
    print_result(
        "slatedb",
        wall_us,
        process_cpu_nanos().saturating_sub(cpu_before) as f64 / 1_000.0,
        allocated,
        calls,
        rss_before,
        process_rss(),
        directory_bytes(&main_path) + directory_bytes(&corrupt_path),
        &metrics,
        counters.snapshot().saturating_sub(physical_before),
    );
    Ok(())
}

fn main() {
    let backend = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "rocksdb".to_owned());
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build recovery-oracle runtime")
        .block_on(async {
            match backend.as_str() {
                "rocksdb" => run_rocks().await,
                "slatedb" => run_slate().await,
                other => panic!("unknown backend '{other}'"),
            }
            .expect("no-lease Stage-2 recovery qualification");
        });
}
