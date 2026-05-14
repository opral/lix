use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::backend_v2::conformance::{BackendFactory, BackendTestConfig};
use crate::backend_v2::{
    Backend, BackendCapabilities, BackendError, BackendRead, BackendWrite, Capability,
    CommitResult, Cursor, GetManyResult, GetOptions, GetSlot, Key, KeyRange, PointOrder,
    ProjectedValue, PutBatch, ReadBatch, ReadEntry, ReadOptions, ReadStats, ReadSupport,
    ScanDirection, ScanOptions, ScanPage, SpaceId, StoredValue, ValueProjection, WriteConcurrency,
    WriteOptions, WriteStats,
};

type ConformanceMap = BTreeMap<(SpaceId, Key), Bytes>;

#[derive(Clone, Debug, Default)]
pub struct ConformanceBackend {
    entries: Arc<Mutex<ConformanceMap>>,
    next_read_id: Arc<Mutex<u64>>,
}

#[derive(Clone, Debug, Default)]
pub struct ConformanceBackendFactory;

pub struct ConformanceRead {
    read_id: u64,
    entries: ConformanceMap,
}

pub struct ConformanceWrite {
    read_id: u64,
    parent: Arc<Mutex<ConformanceMap>>,
    entries: ConformanceMap,
}

impl ConformanceBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

impl BackendFactory for ConformanceBackendFactory {
    type Backend = ConformanceBackend;

    fn fresh(&self) -> Self::Backend {
        ConformanceBackend::new()
    }

    fn config(&self) -> BackendTestConfig {
        BackendTestConfig {
            supports_concurrent_writers: false,
            ..BackendTestConfig::default()
        }
    }
}

impl Backend for ConformanceBackend {
    type Read<'a>
        = ConformanceRead
    where
        Self: 'a;
    type Write<'a>
        = ConformanceWrite
    where
        Self: 'a;

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::v0(WriteConcurrency::SingleWriter)
    }

    fn begin_read(&self, _opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        Ok(ConformanceRead {
            read_id: self.allocate_read_id()?,
            entries: self.snapshot()?,
        })
    }

    fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        Ok(ConformanceWrite {
            read_id: self.allocate_read_id()?,
            parent: Arc::clone(&self.entries),
            entries: self.snapshot()?,
        })
    }
}

impl BackendRead for ConformanceRead {
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<GetManyResult, BackendError> {
        get_many_from_map(&self.entries, space, keys, opts)
    }

    fn scan_range(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions<'_>,
    ) -> Result<ScanPage, BackendError> {
        scan_range_from_map(&self.entries, self.read_id, space, range, opts)
    }
}

impl BackendRead for ConformanceWrite {
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<GetManyResult, BackendError> {
        get_many_from_map(&self.entries, space, keys, opts)
    }

    fn scan_range(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions<'_>,
    ) -> Result<ScanPage, BackendError> {
        scan_range_from_map(&self.entries, self.read_id, space, range, opts)
    }
}

impl BackendWrite for ConformanceWrite {
    fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError> {
        for entry in entries.entries {
            self.entries
                .insert((space, entry.key), stored_value_bytes(entry.value));
        }
        Ok(())
    }

    fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), BackendError> {
        for key in keys {
            self.entries.remove(&(space, key.clone()));
        }
        Ok(())
    }

    fn commit(self) -> Result<CommitResult, BackendError> {
        *self
            .parent
            .lock()
            .map_err(|_| BackendError::Io("memory backend lock poisoned".to_string()))? =
            self.entries;
        Ok(CommitResult {
            commit_id: None,
            stats: WriteStats::default(),
        })
    }

    fn rollback(self) -> Result<(), BackendError> {
        Ok(())
    }
}

impl ConformanceBackend {
    fn snapshot(&self) -> Result<ConformanceMap, BackendError> {
        self.entries
            .lock()
            .map_err(|_| BackendError::Io("memory backend lock poisoned".to_string()))
            .map(|entries| entries.clone())
    }

    fn allocate_read_id(&self) -> Result<u64, BackendError> {
        let mut next_read_id = self
            .next_read_id
            .lock()
            .map_err(|_| BackendError::Io("memory backend read id lock poisoned".to_string()))?;
        let read_id = *next_read_id;
        *next_read_id += 1;
        Ok(read_id)
    }
}

fn get_many_from_map(
    entries: &ConformanceMap,
    space: SpaceId,
    keys: &[Key],
    opts: GetOptions<'_>,
) -> Result<GetManyResult, BackendError> {
    if !opts.predicates.is_empty() {
        return Err(BackendError::Unsupported(Capability::PredicatePushdown));
    }
    match opts.order {
        PointOrder::Caller => {}
        PointOrder::KeyAsc => {
            return Err(BackendError::Unsupported(Capability::KeyOrderedPoints));
        }
        PointOrder::Unordered => {
            return Err(BackendError::Unsupported(Capability::UnorderedPoints));
        }
    }

    let slots = keys
        .iter()
        .enumerate()
        .map(|(index, key)| {
            let value = entries
                .get(&(space, key.clone()))
                .map(|value| project_value(value, opts.projection))
                .transpose()?;
            Ok(GetSlot {
                requested_index: Some(index),
                key: key.clone(),
                value,
            })
        })
        .collect::<Result<Vec<_>, BackendError>>()?;

    Ok(GetManyResult {
        entries: slots,
        support: ReadSupport::exact(opts.projection),
        stats: ReadStats {
            backend_calls: 1,
            emitted_entries: keys.len() as u64,
            ..ReadStats::default()
        },
    })
}

fn scan_range_from_map(
    entries: &ConformanceMap,
    read_id: u64,
    space: SpaceId,
    range: KeyRange,
    opts: ScanOptions<'_>,
) -> Result<ScanPage, BackendError> {
    if !opts.predicates.is_empty() {
        return Err(BackendError::Unsupported(Capability::PredicatePushdown));
    }
    if opts.direction == ScanDirection::Reverse {
        return Err(BackendError::Unsupported(Capability::ReverseScan));
    }

    let after = if let Some(cursor) = opts.cursor {
        let decoded = decode_cursor(cursor)?;
        if decoded.read_id != read_id
            || decoded.space != space
            || decoded.range != range
            || decoded.projection != opts.projection
            || decoded.direction != opts.direction
        {
            return Err(BackendError::InvalidCursor);
        }
        Some(decoded.last_key)
    } else {
        None
    };
    let limit = opts.limit_rows.unwrap_or(usize::MAX);
    if limit == 0 {
        return Ok(ScanPage {
            entries: ReadBatch {
                entries: Vec::new(),
            },
            next_cursor: None,
            support: ReadSupport::exact(opts.projection),
            stats: ReadStats {
                backend_calls: 1,
                ..ReadStats::default()
            },
        });
    }

    let mut scanned_entries = 0u64;
    let mut read_entries = Vec::new();
    let mut next_cursor = None;

    for ((entry_space, key), value) in entries {
        if *entry_space != space || !range_contains(&range, key) {
            continue;
        }
        if after.as_ref().is_some_and(|after| key <= after) {
            continue;
        }

        scanned_entries += 1;
        if read_entries.len() == limit {
            next_cursor = read_entries
                .last()
                .map(|entry: &ReadEntry| {
                    encode_cursor(
                        read_id,
                        space,
                        &range,
                        opts.projection,
                        opts.direction,
                        &entry.key,
                    )
                })
                .transpose()?;
            break;
        }

        read_entries.push(ReadEntry {
            key: key.clone(),
            value: project_value(value, opts.projection)?,
        });
    }

    Ok(ScanPage {
        entries: ReadBatch {
            entries: read_entries,
        },
        next_cursor,
        support: ReadSupport::exact(opts.projection),
        stats: ReadStats {
            scanned_entries,
            emitted_entries: scanned_entries.min(limit as u64),
            backend_calls: 1,
            ..ReadStats::default()
        },
    })
}

#[derive(Debug)]
struct DecodedCursor {
    read_id: u64,
    space: SpaceId,
    range: KeyRange,
    projection: ValueProjection,
    direction: ScanDirection,
    last_key: Key,
}

fn encode_cursor(
    read_id: u64,
    space: SpaceId,
    range: &KeyRange,
    projection: ValueProjection,
    direction: ScanDirection,
    last_key: &Key,
) -> Result<Cursor, BackendError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"lix-bv2-cursor\0");
    bytes.extend_from_slice(&read_id.to_be_bytes());
    bytes.extend_from_slice(&space.0.to_be_bytes());
    bytes.push(value_projection_tag(projection));
    bytes.push(scan_direction_tag(direction));
    push_bound(&mut bytes, &range.lower)?;
    push_bound(&mut bytes, &range.upper)?;
    push_bytes(&mut bytes, &last_key.0)?;
    Ok(Cursor(Bytes::from(bytes)))
}

fn decode_cursor(cursor: &Cursor) -> Result<DecodedCursor, BackendError> {
    let bytes = cursor.0.as_ref();
    let mut offset = 0usize;
    let magic = read_exact(bytes, &mut offset, b"lix-bv2-cursor\0".len())?;
    if magic != b"lix-bv2-cursor\0" {
        return Err(BackendError::InvalidCursor);
    }
    let read_id = u64::from_be_bytes(
        read_exact(bytes, &mut offset, 8)?
            .try_into()
            .map_err(|_| BackendError::InvalidCursor)?,
    );
    let space = SpaceId(u32::from_be_bytes(
        read_exact(bytes, &mut offset, 4)?
            .try_into()
            .map_err(|_| BackendError::InvalidCursor)?,
    ));
    let projection = value_projection_from_tag(read_u8(bytes, &mut offset)?)?;
    let direction = scan_direction_from_tag(read_u8(bytes, &mut offset)?)?;
    let lower = read_bound(bytes, &mut offset)?;
    let upper = read_bound(bytes, &mut offset)?;
    let last_key = Key(read_bytes(bytes, &mut offset)?);
    if offset != bytes.len() {
        return Err(BackendError::InvalidCursor);
    }
    Ok(DecodedCursor {
        read_id,
        space,
        range: KeyRange { lower, upper },
        projection,
        direction,
        last_key,
    })
}

fn push_bound(out: &mut Vec<u8>, bound: &Bound<Key>) -> Result<(), BackendError> {
    match bound {
        Bound::Unbounded => out.push(0),
        Bound::Included(key) => {
            out.push(1);
            push_bytes(out, &key.0)?;
        }
        Bound::Excluded(key) => {
            out.push(2);
            push_bytes(out, &key.0)?;
        }
    }
    Ok(())
}

fn read_bound(bytes: &[u8], offset: &mut usize) -> Result<Bound<Key>, BackendError> {
    match read_u8(bytes, offset)? {
        0 => Ok(Bound::Unbounded),
        1 => Ok(Bound::Included(Key(read_bytes(bytes, offset)?))),
        2 => Ok(Bound::Excluded(Key(read_bytes(bytes, offset)?))),
        _ => Err(BackendError::InvalidCursor),
    }
}

fn push_bytes(out: &mut Vec<u8>, bytes: &Bytes) -> Result<(), BackendError> {
    let len = u32::try_from(bytes.len()).map_err(|_| BackendError::InvalidCursor)?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn read_bytes(bytes: &[u8], offset: &mut usize) -> Result<Bytes, BackendError> {
    let len = u32::from_be_bytes(
        read_exact(bytes, offset, 4)?
            .try_into()
            .map_err(|_| BackendError::InvalidCursor)?,
    ) as usize;
    Ok(Bytes::copy_from_slice(read_exact(bytes, offset, len)?))
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> Result<u8, BackendError> {
    let value = *bytes.get(*offset).ok_or(BackendError::InvalidCursor)?;
    *offset += 1;
    Ok(value)
}

fn read_exact<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
    len: usize,
) -> Result<&'a [u8], BackendError> {
    let end = offset.checked_add(len).ok_or(BackendError::InvalidCursor)?;
    let slice = bytes.get(*offset..end).ok_or(BackendError::InvalidCursor)?;
    *offset = end;
    Ok(slice)
}

fn value_projection_tag(projection: ValueProjection) -> u8 {
    match projection {
        ValueProjection::KeyOnly => 0,
        ValueProjection::Header => 1,
        ValueProjection::Refs => 2,
        ValueProjection::HeaderAndRefs => 3,
        ValueProjection::Payload => 4,
        ValueProjection::FullValue => 5,
    }
}

fn value_projection_from_tag(tag: u8) -> Result<ValueProjection, BackendError> {
    match tag {
        0 => Ok(ValueProjection::KeyOnly),
        1 => Ok(ValueProjection::Header),
        2 => Ok(ValueProjection::Refs),
        3 => Ok(ValueProjection::HeaderAndRefs),
        4 => Ok(ValueProjection::Payload),
        5 => Ok(ValueProjection::FullValue),
        _ => Err(BackendError::InvalidCursor),
    }
}

fn scan_direction_tag(direction: ScanDirection) -> u8 {
    match direction {
        ScanDirection::Forward => 0,
        ScanDirection::Reverse => 1,
    }
}

fn scan_direction_from_tag(tag: u8) -> Result<ScanDirection, BackendError> {
    match tag {
        0 => Ok(ScanDirection::Forward),
        1 => Ok(ScanDirection::Reverse),
        _ => Err(BackendError::InvalidCursor),
    }
}

fn range_contains(range: &KeyRange, key: &Key) -> bool {
    let lower_matches = match &range.lower {
        Bound::Included(lower) => key >= lower,
        Bound::Excluded(lower) => key > lower,
        Bound::Unbounded => true,
    };
    let upper_matches = match &range.upper {
        Bound::Included(upper) => key <= upper,
        Bound::Excluded(upper) => key < upper,
        Bound::Unbounded => true,
    };
    lower_matches && upper_matches
}

fn project_value(
    value: &Bytes,
    projection: ValueProjection,
) -> Result<ProjectedValue, BackendError> {
    match projection {
        ValueProjection::KeyOnly => Ok(ProjectedValue::KeyOnly),
        ValueProjection::FullValue => Ok(ProjectedValue::FullValue(value.clone())),
        other => Err(BackendError::Unsupported(Capability::Projection(other))),
    }
}

fn stored_value_bytes(value: StoredValue) -> Bytes {
    match value {
        StoredValue::FullValue(bytes) => bytes,
        StoredValue::Envelope {
            header,
            refs,
            payload,
        } => {
            let mut bytes = Vec::with_capacity(header.len() + refs.len() + payload.len());
            bytes.extend_from_slice(&header);
            bytes.extend_from_slice(&refs);
            bytes.extend_from_slice(&payload);
            Bytes::from(bytes)
        }
    }
}
