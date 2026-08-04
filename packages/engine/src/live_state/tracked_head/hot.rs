//! V21 row-addressable current state with columnar-base coordinates.
//!
//! V12 packed every file member of one logical entity into a group. That made
//! a logical-PK lookup cheap, but it also made every normal commit read,
//! decode, merge, and rewrite each predecessor group. V17 keeps the fixed row
//! value codec and branch-control publication fence, makes a full row identity
//! the physical mutation unit, and stores each value only in the authoritative
//! file-first row index.

use std::cmp::Ordering;
#[cfg(test)]
use std::collections::VecDeque;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::mem::size_of;
use std::ops::Range;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use crate::wasm::WasmCreateContext;
use bytes::Bytes;
use smallvec::SmallVec;
use tracing::Instrument as _;

#[cfg(test)]
use crate::branch::BranchHeadControlContext;
use crate::storage_adapter::{BufferRange, EncodedMutationBatch, EncodedPut};
use crate::tracked_state::TrackedStateReadColumns;
use crate::wasm::WasmCertifiedEntityBatch;

use super::*;

pub(crate) const HOT_ROW_NAMESPACE: &str = "live_state.hot_row.v21";
pub(crate) const HOT_FILE_NAMESPACE: &str = "live_state.hot_file_schema.v18";
pub(crate) const HOT_COLLECTION_CONTROL_NAMESPACE: &str = "live_state.hot_collection_control.v1";
pub(crate) const HOT_ROW_SPACE: StorageSpace =
    StorageSpace::mutable(StorageSpaceId(0x0004_001b), HOT_ROW_NAMESPACE);
/// Conservative `(branch, generation, schema)` file-membership markers.
///
/// The authoritative hot row owns every value and file identity. Markers are
/// never removed within a generation, so they may produce a harmless false
/// positive after the last file member is deleted but cannot hide live rows.
pub(crate) const HOT_FILE_SPACE: StorageSpace =
    StorageSpace::mutable(StorageSpaceId(0x0004_001c), HOT_FILE_NAMESPACE);
pub(crate) const HOT_COLLECTION_CONTROL_SPACE: StorageSpace = StorageSpace::mutable(
    StorageSpaceId(0x0004_0023),
    HOT_COLLECTION_CONTROL_NAMESPACE,
);
/// One immutable tracked-state root used as the baseline for a sparse branch
/// generation. Branch creation publishes this 16-byte reference instead of
/// copying every tracked row into branch-local HOT storage.
pub(crate) const ROOT_CURRENT_BASE_SPACE: StorageSpace = StorageSpace::mutable(
    StorageSpaceId(0x0004_0028),
    "live_state.root_current_base.v1",
);
const HOT_DENSE_SCAN_MIN_IDENTITIES: usize = 64;
const HOT_DENSE_SCAN_MAX_OVERREAD: usize = 2;
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const CERTIFIED_ENTITY_BATCH_MAGIC_V2: &[u8; 4] = b"CEB2";

fn append_batch_text(output: &mut Vec<u8>, value: &str) -> Result<(), LixError> {
    output.extend_from_slice(
        &u16::try_from(value.len())
            .map_err(|_| head_value_error("certified entity batch text exceeds 64KiB"))?
            .to_le_bytes(),
    );
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn certified_zstd_packet_page_header(page: &[u8]) -> Result<((u32, u32), usize, &[u8]), LixError> {
    let (header, compressed) = page
        .split_at_checked(12)
        .ok_or_else(|| head_value_error("compressed certified packet page is truncated"))?;
    let first_local_ref = u32::from_le_bytes(
        header[..4]
            .try_into()
            .expect("compressed packet first local ref"),
    );
    let last_local_ref = u32::from_le_bytes(
        header[4..8]
            .try_into()
            .expect("compressed packet last local ref"),
    );
    if first_local_ref > last_local_ref {
        return Err(head_value_error(
            "compressed certified packet page has an inverted local-ref range",
        ));
    }
    let uncompressed_len = u32::from_le_bytes(
        header[8..12]
            .try_into()
            .expect("compressed packet uncompressed length"),
    ) as usize;
    if uncompressed_len == 0 || uncompressed_len > 64 * 1024 * 1024 {
        return Err(head_value_error(
            "compressed certified packet page has an invalid uncompressed length",
        ));
    }
    Ok((
        (first_local_ref, last_local_ref),
        uncompressed_len,
        compressed,
    ))
}

fn decode_certified_zstd_packet_page(page: &[u8]) -> Result<Vec<u8>, LixError> {
    let (_, uncompressed_len, compressed) = certified_zstd_packet_page_header(page)?;
    let decoded =
        crate::compression::decompress_zstd(compressed, uncompressed_len).map_err(|error| {
            head_value_error(format!(
                "compressed certified packet page failed to decode: {error}"
            ))
        })?;
    if decoded.len() != uncompressed_len {
        return Err(head_value_error(format!(
            "compressed certified packet page decoded to {} bytes, expected {uncompressed_len}",
            decoded.len(),
        )));
    }
    Ok(decoded)
}

/// Expands the authoritative rows needed to publish a host-produced packet.
/// Commit deltas are self-contained, so the packet is decoded once here and
/// never becomes a second durable payload authority.
pub(crate) fn materialize_certified_root_rows(
    branch_id: &str,
    file_id: &str,
    commit_id: CommitId,
    timestamp: LixTimestamp,
    batch: &WasmCertifiedEntityBatch,
) -> Result<MaterializedLiveStateBatch, LixError> {
    let schema_bytes = batch
        .schema_keys
        .iter()
        .try_fold(2usize, |total, schema| total.checked_add(2 + schema.len()))
        .ok_or_else(|| head_value_error("certified root schema list size overflowed"))?;
    let mut header = Vec::with_capacity(
        4 + schema_bytes
            + 2
            + file_id.len()
            + 16
            + 2
            + timestamp.to_string().len()
            + 26
            + batch.pages.len().saturating_mul(12),
    );
    header.extend_from_slice(CERTIFIED_ENTITY_BATCH_MAGIC_V2);
    header.extend_from_slice(
        &u16::try_from(batch.schema_keys.len())
            .map_err(|_| head_value_error("certified root batch has too many schemas"))?
            .to_le_bytes(),
    );
    for schema_key in &batch.schema_keys {
        append_batch_text(&mut header, schema_key)?;
    }
    append_batch_text(&mut header, file_id)?;
    header.extend_from_slice(commit_id.as_uuid().as_bytes());
    append_batch_text(&mut header, &timestamp.to_string())?;
    header.extend_from_slice(&batch.format.to_le_bytes());
    header.extend_from_slice(&batch.row_count.to_le_bytes());
    header.extend_from_slice(&batch.creates.high.to_le_bytes());
    header.extend_from_slice(&batch.creates.low.to_le_bytes());
    header.extend_from_slice(
        &u32::try_from(batch.pages.len())
            .map_err(|_| head_value_error("certified root batch has too many pages"))?
            .to_le_bytes(),
    );
    let mut pages = Vec::with_capacity(batch.pages.len());
    for (page_index, page) in batch.pages.iter().enumerate() {
        header.extend_from_slice(&0_u32.to_le_bytes());
        header.extend_from_slice(&u32::MAX.to_le_bytes());
        header.extend_from_slice(
            &u32::try_from(page.len())
                .map_err(|_| head_value_error("certified root batch page exceeds 4GiB"))?
                .to_le_bytes(),
        );
        pages.push((
            u32::try_from(page_index)
                .map_err(|_| head_value_error("certified root batch has too many pages"))?,
            page.clone(),
        ));
    }
    let request = TrackedStateScanRequest::default();
    let filter_index = CertifiedScanFilterIndex::new(&request);
    let row_count = usize::try_from(batch.row_count)
        .map_err(|_| head_value_error("certified root row count exceeds usize"))?;
    let mut builder = MaterializedLiveStateBatchBuilder::with_capacity(row_count);
    decode_certified_entity_batch_rows(
        &header,
        Some(&pages),
        branch_id,
        &request,
        &filter_index,
        true,
        None,
        &mut builder,
    )?;
    Ok(builder.finish())
}

struct CertifiedScanFilterIndex {
    schema_keys: Option<HashSet<String>>,
    file_ids: Option<HashSet<String>>,
    entity_pks: Option<HashSet<EntityPk>>,
}

impl CertifiedScanFilterIndex {
    fn new(request: &TrackedStateScanRequest) -> Self {
        let file_ids = if request.filter.file_ids.is_empty()
            || request
                .filter
                .file_ids
                .iter()
                .any(|file_id| matches!(file_id, NullableKeyFilter::Any))
        {
            None
        } else {
            Some(
                request
                    .filter
                    .file_ids
                    .iter()
                    .filter_map(|file_id| match file_id {
                        NullableKeyFilter::Value(file_id) => Some(file_id.clone()),
                        NullableKeyFilter::Any | NullableKeyFilter::Null => None,
                    })
                    .collect(),
            )
        };
        Self {
            schema_keys: (!request.filter.schema_keys.is_empty())
                .then(|| request.filter.schema_keys.iter().cloned().collect()),
            file_ids,
            entity_pks: (!request.filter.entity_pks.is_empty())
                .then(|| request.filter.entity_pks.iter().cloned().collect()),
        }
    }

    fn includes_any_schema(&self, schema_keys: &[&str]) -> bool {
        self.schema_keys.as_ref().is_none_or(|selected| {
            schema_keys
                .iter()
                .any(|schema_key| selected.contains(*schema_key))
        })
    }

    fn includes_schema(&self, schema_key: &str) -> bool {
        self.schema_keys
            .as_ref()
            .is_none_or(|selected| selected.contains(schema_key))
    }

    fn includes_file(&self, file_id: &str) -> bool {
        self.file_ids
            .as_ref()
            .is_none_or(|selected| selected.contains(file_id))
    }

    fn includes_entity(&self, entity_pk: &EntityPk) -> bool {
        self.entity_pks
            .as_ref()
            .is_none_or(|selected| selected.contains(entity_pk))
    }
}

fn decode_certified_entity_batch_rows(
    bytes: &[u8],
    external_pages: Option<&[(u32, Bytes)]>,
    branch_id: &str,
    request: &TrackedStateScanRequest,
    filter_index: &CertifiedScanFilterIndex,
    needs_snapshot: bool,
    limit: Option<usize>,
    builder: &mut MaterializedLiveStateBatchBuilder,
) -> Result<(), LixError> {
    let mut input = CertifiedBatchReader::new(bytes)?;
    let schema_count = input.u16()? as usize;
    if schema_count == 0 {
        return Err(head_value_error("certified entity batch has no schemas"));
    }
    let mut schema_keys = Vec::with_capacity(schema_count);
    for _ in 0..schema_count {
        schema_keys.push(input.text()?);
    }
    let file_id = input.text()?;
    let commit_id = CommitId::new(
        uuid::Uuid::from_slice(input.bytes(16)?)
            .map_err(|error| head_value_error(format!("invalid certified commit id: {error}")))?,
    );
    let timestamp = LixTimestamp::parse(input.text()?).map_err(head_value_error)?;
    let format = input.u16()?;
    if format != 1
        && format != 2
        && format != crate::wasm::HOST_CERTIFIED_PACKET_FORMAT
        && format != crate::wasm::HOST_CERTIFIED_ZSTD_PACKET_FORMAT
    {
        return Err(head_value_error(format!(
            "unsupported certified entity batch format {format}"
        )));
    }
    let declared_rows = input.u64()?;
    let creates = WasmCreateContext {
        high: input.u64()?,
        low: input.u32()?,
    };
    // Exact reads from a generated-id row segment compare compact local
    // references before materializing an `EntityPk` or snapshot.
    let selected_schema_row_local_refs = (format == 1 && !request.filter.entity_pks.is_empty())
        .then(|| {
            let high = creates.high.to_be_bytes();
            let low = creates.low.to_be_bytes();
            request
                .filter
                .entity_pks
                .iter()
                .filter_map(|entity_pk| match entity_pk.components.as_slice() {
                    [crate::entity_pk::EntityPkComponent::Uuid(bytes)]
                        if bytes[..8] == high && bytes[8..12] == low =>
                    {
                        Some(u32::from_be_bytes(
                            bytes[12..]
                                .try_into()
                                .expect("UUID local-reference suffix is four bytes"),
                        ))
                    }
                    _ => None,
                })
                .collect::<BTreeSet<_>>()
        });
    let page_count = input.u32()?;
    if !filter_index.includes_any_schema(&schema_keys) {
        return Ok(());
    }
    if !filter_index.includes_file(file_id) {
        return Ok(());
    }

    let complete_pages = external_pages.is_some_and(|pages| pages.len() == page_count as usize);
    let mut decoded_rows = 0_u64;
    for page_index in 0..page_count {
        let _first_local_ref = input.u32()?;
        let _last_local_ref = input.u32()?;
        let page_len = input.u32()? as usize;
        let Some((_, page)) = external_pages.and_then(|pages| {
            pages
                .binary_search_by_key(&page_index, |(page_index, _)| *page_index)
                .ok()
                .map(|index| &pages[index])
        }) else {
            continue;
        };
        if page.len() != page_len {
            return Err(head_value_error(
                "certified entity batch page length does not match its header",
            ));
        }
        let page = page.as_ref();
        let decoded_page;
        let page = if format == crate::wasm::HOST_CERTIFIED_ZSTD_PACKET_FORMAT {
            decoded_page = decode_certified_zstd_packet_page(page)?;
            decoded_page.as_slice()
        } else {
            page
        };
        if format == 2
            || format == crate::wasm::HOST_CERTIFIED_PACKET_FORMAT
            || format == crate::wasm::HOST_CERTIFIED_ZSTD_PACKET_FORMAT
        {
            decoded_rows = decoded_rows.saturating_add(decode_certified_packet_rows(
                page,
                &creates,
                commit_id,
                timestamp,
                branch_id,
                file_id,
                filter_index,
                needs_snapshot,
                limit,
                decoded_rows,
                builder,
            )?);
            if limit.is_some_and(|limit| builder.len() >= limit) {
                return Ok(());
            }
            continue;
        }
        let entity_page = lix_plugin_wire::Page::decode(page).map_err(|error| {
            head_value_error(format!("invalid certified entity page: {error:?}"))
        })?;
        let section = entity_page.section().map_err(|error| {
            head_value_error(format!("invalid certified entity-page section: {error:?}"))
        })?;
        if section.representation != lix_plugin_wire::Representation::SchemaRows
            || section.operation != lix_plugin_wire::Operation::Create
        {
            return Err(head_value_error(
                "certified schema-row entity page must contain created rows",
            ));
        }
        let layout = lix_plugin_layout::CompiledLayout::parse(section.layout)
            .map_err(|error| head_value_error(format!("invalid schema-row layout: {error}")))?;
        let mut rows = layout
            .rows(section.payload, section.record_count)
            .map_err(|error| head_value_error(format!("invalid schema-row payload: {error}")))?;
        let mut rendered_snapshots = Vec::new();
        while let Some(rendered) = rows
            .render_next(&mut rendered_snapshots)
            .map_err(|error| head_value_error(format!("invalid schema row: {error}")))?
        {
            let local_ref = u32::try_from(rendered.local_ref)
                .map_err(|_| head_value_error("schema-row local reference exceeds u32"))?;
            decoded_rows = decoded_rows.saturating_add(1);
            let selected = selected_schema_row_local_refs
                .as_ref()
                .is_none_or(|selected| selected.contains(&local_ref));
            if !selected {
                rendered_snapshots.clear();
                continue;
            }
            let id = creates
                .component_uuid_bytes(u64::from(local_ref))
                .map_err(|error| head_value_error(error.to_string()))?;
            let entity_pk = EntityPk::uuid_from_bytes(id);
            let snapshot = if needs_snapshot {
                let json = lix_plugin_layout::insert_generated_id(
                    &rendered_snapshots[rendered.snapshot],
                    layout.generated_id_path(),
                    &uuid::Uuid::from_bytes(id).to_string(),
                )
                .map_err(|error| {
                    head_value_error(format!("invalid generated identity: {error}"))
                })?;
                Some(
                    SharedStr::from_utf8(Bytes::from(json))
                        .map_err(|error| head_value_error(error.to_string()))?,
                )
            } else {
                None
            };
            builder.push_materialized(
                entity_pk,
                section.schema_key.to_owned(),
                Some(file_id.to_owned()),
                snapshot,
                None,
                false,
                timestamp,
                timestamp,
                false,
                Some(ChangeId::new(uuid::Uuid::from_bytes(id))),
                Some(commit_id),
                false,
                branch_id,
            );
            rendered_snapshots.clear();
            if limit.is_some_and(|limit| builder.len() >= limit) {
                return Ok(());
            }
        }
        rows.finish()
            .map_err(|error| head_value_error(format!("invalid schema-row payload: {error}")))?;
    }
    if complete_pages && decoded_rows != declared_rows {
        return Err(head_value_error(format!(
            "certified entity batch declared {declared_rows} rows but decoded {decoded_rows}"
        )));
    }
    if input.offset != input.bytes.len() {
        return Err(head_value_error(
            "certified entity batch has trailing storage bytes",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn decode_certified_packet_rows(
    page: &[u8],
    creates: &WasmCreateContext,
    commit_id: CommitId,
    timestamp: LixTimestamp,
    branch_id: &str,
    file_id: &str,
    filter_index: &CertifiedScanFilterIndex,
    needs_snapshot: bool,
    limit: Option<usize>,
    base_ordinal: u64,
    builder: &mut MaterializedLiveStateBatchBuilder,
) -> Result<u64, LixError> {
    let mut input = CertifiedPacketReader {
        bytes: page,
        offset: 0,
    };
    let mut decoded = 0_u64;
    while input.offset < input.bytes.len() {
        let record_len = input.u32()? as usize;
        let record_bytes = input.bytes(record_len)?;
        let mut record = CertifiedPacketReader {
            bytes: record_bytes,
            offset: 0,
        };
        let tag = record.u8()?;
        let schema_len = record.u32()? as usize;
        let schema_key = std::str::from_utf8(record.bytes(schema_len)?)
            .map_err(|error| head_value_error(format!("invalid packet schema: {error}")))?;
        let (entity_pk, created_id) = match tag {
            0 => {
                let component_count = record.u32()? as usize;
                let mut components = Vec::with_capacity(component_count);
                for _ in 0..component_count {
                    let component_len = record.u32()? as usize;
                    let component =
                        std::str::from_utf8(record.bytes(component_len)?).map_err(|error| {
                            head_value_error(format!("invalid packet key: {error}"))
                        })?;
                    components.push(
                        SharedStr::from_utf8(Bytes::copy_from_slice(component.as_bytes()))
                            .map_err(|error| head_value_error(error.to_string()))?,
                    );
                }
                if record.u8()? > 1 {
                    return Err(head_value_error(
                        "certified packet upsert has invalid effect",
                    ));
                }
                (
                    EntityPk::from_shared_parts(components)
                        .map_err(|error| head_value_error(error.to_string()))?,
                    None,
                )
            }
            2 => {
                let local_ref = record.u64()?;
                let id = creates
                    .component_uuid_bytes(local_ref)
                    .map_err(|error| head_value_error(error.to_string()))?;
                (EntityPk::uuid_from_bytes(id), Some(id))
            }
            3 => {
                if record.u32()? != 1 {
                    return Err(head_value_error(
                        "resolved certified create must have one generated key component",
                    ));
                }
                let component_len = record.u32()? as usize;
                let component =
                    std::str::from_utf8(record.bytes(component_len)?).map_err(|error| {
                        head_value_error(format!("invalid generated identity: {error}"))
                    })?;
                let id = uuid::Uuid::parse_str(component)
                    .map_err(|error| head_value_error(format!("invalid generated UUID: {error}")))?
                    .into_bytes();
                (EntityPk::uuid_from_bytes(id), Some(id))
            }
            _ => {
                return Err(head_value_error(
                    "certified packet contains a non-snapshot record",
                ));
            }
        };
        if record.u8()? != 0 {
            return Err(head_value_error(
                "certified create packet snapshot is not inline",
            ));
        }
        let snapshot_len = record.u32()? as usize;
        let snapshot_bytes = record.bytes(snapshot_len)?;
        if record.offset != record.bytes.len() {
            return Err(head_value_error(
                "certified create packet record has trailing bytes",
            ));
        }
        decoded = decoded.saturating_add(1);
        let selected =
            filter_index.includes_schema(schema_key) && filter_index.includes_entity(&entity_pk);
        if !selected {
            continue;
        }
        let snapshot = if needs_snapshot {
            Some(
                SharedStr::from_utf8(Bytes::copy_from_slice(snapshot_bytes))
                    .map_err(|error| head_value_error(error.to_string()))?,
            )
        } else {
            None
        };
        let change_id = if let Some(id) = &created_id {
            ChangeId::new(uuid::Uuid::from_bytes(*id))
        } else {
            certified_keyed_change_id(
                commit_id,
                schema_key,
                file_id,
                &entity_pk,
                base_ordinal.saturating_add(decoded),
            )
        };
        builder.push_materialized(
            entity_pk,
            schema_key.to_owned(),
            Some(file_id.to_owned()),
            snapshot,
            None,
            false,
            timestamp,
            timestamp,
            false,
            Some(change_id),
            Some(commit_id),
            false,
            branch_id,
        );
        if limit.is_some_and(|limit| builder.len() >= limit) {
            break;
        }
    }
    Ok(decoded)
}

fn certified_keyed_change_id(
    commit_id: CommitId,
    schema_key: &str,
    file_id: &str,
    entity_pk: &EntityPk,
    ordinal: u64,
) -> ChangeId {
    let identity = crate::tracked_state::encode_key_ref(TrackedStateKeyRef {
        schema_key,
        file_id: Some(file_id),
        entity_pk,
    });
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"lix.certified.keyed-change.v1\0");
    hasher.update(commit_id.as_uuid().as_bytes());
    hasher.update(identity.as_slice());
    hasher.update(&ordinal.to_be_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    ChangeId::new(uuid::Uuid::from_bytes(bytes))
}

struct CertifiedBatchReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> CertifiedBatchReader<'a> {
    fn new(bytes: &'a [u8]) -> Result<Self, LixError> {
        if !bytes.starts_with(CERTIFIED_ENTITY_BATCH_MAGIC_V2) {
            return Err(head_value_error(
                "unsupported certified entity batch format; recreate the repository",
            ));
        }
        Ok(Self { bytes, offset: 4 })
    }

    fn bytes(&mut self, length: usize) -> Result<&'a [u8], LixError> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| head_value_error("truncated certified entity batch"))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn text(&mut self) -> Result<&'a str, LixError> {
        let length = self.u16()? as usize;
        std::str::from_utf8(self.bytes(length)?)
            .map_err(|error| head_value_error(format!("invalid certified batch text: {error}")))
    }

    fn u16(&mut self) -> Result<u16, LixError> {
        Ok(u16::from_le_bytes(
            self.bytes(2)?.try_into().expect("fixed batch u16 width"),
        ))
    }

    fn u32(&mut self) -> Result<u32, LixError> {
        Ok(u32::from_le_bytes(
            self.bytes(4)?.try_into().expect("fixed batch u32 width"),
        ))
    }

    fn u64(&mut self) -> Result<u64, LixError> {
        Ok(u64::from_le_bytes(
            self.bytes(8)?.try_into().expect("fixed batch u64 width"),
        ))
    }
}

struct CertifiedPacketReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> CertifiedPacketReader<'a> {
    fn bytes(&mut self, length: usize) -> Result<&'a [u8], LixError> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| head_value_error("truncated certified packet page"))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, LixError> {
        Ok(self.bytes(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, LixError> {
        Ok(u32::from_le_bytes(
            self.bytes(4)?.try_into().expect("fixed packet u32 width"),
        ))
    }

    fn u64(&mut self) -> Result<u64, LixError> {
        Ok(u64::from_le_bytes(
            self.bytes(8)?.try_into().expect("fixed packet u64 width"),
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, musli::Encode, musli::Decode)]
#[musli(packed)]
// The digest is the third positional field in repository protocol v40. Older
// two-field controls belong to v39 repositories, which Engine::new rejects at
// the protocol boundary before any HOT control is decoded.
struct HotCollectionControl {
    active_generation: CommitId,
    live_count: u64,
    ordered_identity_digest: Option<[u8; 32]>,
}

// Root-backed branches defer collection cardinality until an operation
// actually asks for it. Ordinary sparse edits must not scan the immutable
// root merely to maintain an eager count.
// Root-backed branches deliberately avoid counting every inherited row during
// creation or the first sparse edit. This reserved persisted value means that
// the count must be derived by a live scan when an API actually requests it.
const DEFERRED_ROOT_LIVE_COUNT: u64 = crate::collection_generation::DEFERRED_LIVE_COUNT;

const TRANSACTION_HOT_STATE_CACHE_MAX_ENTRIES: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct HotCollectionCacheKey {
    branch_id: String,
    generation: CommitId,
    schema_key: String,
    file_id: Option<String>,
}

/// Bounded serving metadata retained for one transaction snapshot.
#[derive(Default)]
pub(crate) struct HotStateTransactionCache {
    collection_controls: StdMutex<BTreeMap<HotCollectionCacheKey, HotCollectionControl>>,
}

impl HotStateTransactionCache {
    fn collection_control(
        &self,
        key: &HotCollectionCacheKey,
    ) -> Result<Option<HotCollectionControl>, LixError> {
        Ok(self
            .collection_controls
            .lock()
            .map_err(|_| hot_state_cache_lock_error())?
            .get(key)
            .copied())
    }

    fn remember_collection_control(
        &self,
        key: HotCollectionCacheKey,
        control: HotCollectionControl,
    ) -> Result<(), LixError> {
        let mut entries = self
            .collection_controls
            .lock()
            .map_err(|_| hot_state_cache_lock_error())?;
        if entries.len() < TRANSACTION_HOT_STATE_CACHE_MAX_ENTRIES {
            entries.entry(key).or_insert(control);
        }
        Ok(())
    }
}

fn hot_state_cache_lock_error() -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        "transaction hot-state metadata cache lock is poisoned",
    )
}

fn hot_collection_control_key(
    branch_id: &str,
    branch_generation: CommitId,
    scope: crate::collection_generation::CollectionScopeRef<'_>,
) -> Vec<u8> {
    let mut key = hot_scope_prefix(branch_id, branch_generation);
    write_key_string(&mut key, scope.schema_key, KEY_PART_FINAL);
    write_file_id(&mut key, scope.file_id);
    key
}

async fn load_root_current_base_commit(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
) -> Result<Option<CommitId>, LixError> {
    let key = StorageKey(Bytes::from(hot_scope_prefix(branch_id, generation)));
    let value = PointReadPlan::new(ROOT_CURRENT_BASE_SPACE, &[key])
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .next()
        .flatten();
    let Some(value) = value else {
        return Ok(None);
    };
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(head_value_error(
            "root current-base read unexpectedly omitted its value",
        ));
    };
    if bytes.len() != 16 {
        return Err(head_value_error(
            "root current-base reference must contain one commit UUID",
        ));
    }
    Ok(Some(CommitId::new(
        uuid::Uuid::from_slice(&bytes).map_err(|error| head_value_error(error.to_string()))?,
    )))
}

async fn load_hot_collection_control(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    branch_generation: CommitId,
    scope: crate::collection_generation::CollectionScopeRef<'_>,
) -> Result<HotCollectionControl, LixError> {
    if let Some(control) =
        load_stored_hot_collection_control(store, branch_id, branch_generation, scope).await?
    {
        return Ok(control);
    }
    if load_root_current_base_commit(store, branch_id, branch_generation)
        .await?
        .is_some()
    {
        Box::pin(load_root_collection_control(
            store,
            branch_id,
            branch_generation,
            scope,
        ))
        .await
    } else {
        Ok(HotCollectionControl {
            active_generation: branch_generation,
            live_count: 0,
            ordered_identity_digest: None,
        })
    }
}

async fn load_stored_hot_collection_control(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    branch_generation: CommitId,
    scope: crate::collection_generation::CollectionScopeRef<'_>,
) -> Result<Option<HotCollectionControl>, LixError> {
    let key = StorageKey(Bytes::from(hot_collection_control_key(
        branch_id,
        branch_generation,
        scope,
    )));
    let value = PointReadPlan::new(HOT_COLLECTION_CONTROL_SPACE, &[key])
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .next()
        .flatten();
    match value {
        Some(value) => {
            let StorageProjectedValue::FullValue(bytes) = value else {
                return Err(head_value_error(
                    "hot collection-control read unexpectedly omitted its value",
                ));
            };
            storage_codec::decode("hot collection control", &bytes).map(Some)
        }
        None => Ok(None),
    }
}

async fn load_hot_collection_visibility_control(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    branch_generation: CommitId,
    scope: crate::collection_generation::CollectionScopeRef<'_>,
) -> Result<HotCollectionControl, LixError> {
    let key = StorageKey(Bytes::from(hot_collection_control_key(
        branch_id,
        branch_generation,
        scope,
    )));
    let value = PointReadPlan::new(HOT_COLLECTION_CONTROL_SPACE, &[key])
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .next()
        .flatten();
    let Some(value) = value else {
        // Visibility does not need the immutable root's exact count.
        return Ok(HotCollectionControl {
            active_generation: branch_generation,
            live_count: 1,
            ordered_identity_digest: None,
        });
    };
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(head_value_error(
            "hot collection-control visibility read unexpectedly omitted its value",
        ));
    };
    storage_codec::decode("hot collection control", &bytes)
}

async fn load_root_collection_control(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    branch_generation: CommitId,
    scope: crate::collection_generation::CollectionScopeRef<'_>,
) -> Result<HotCollectionControl, LixError> {
    let Some(base_commit_id) =
        load_root_current_base_commit(store, branch_id, branch_generation).await?
    else {
        return Ok(HotCollectionControl {
            active_generation: branch_generation,
            live_count: 0,
            ordered_identity_digest: None,
        });
    };
    let active_generation = load_root_active_collection_generations(store, base_commit_id, [scope])
        .await?
        .get(&(
            scope.schema_key.to_owned(),
            scope.file_id.map(str::to_owned),
        ))
        .map(|generation| generation.commit_id)
        .unwrap_or(branch_generation);
    Ok(HotCollectionControl {
        active_generation,
        live_count: DEFERRED_ROOT_LIVE_COUNT,
        ordered_identity_digest: None,
    })
}

async fn load_hot_collection_controls(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    branch_generation: CommitId,
    scopes: &[crate::collection_generation::CollectionScopeRef<'_>],
) -> Result<Vec<HotCollectionControl>, LixError> {
    if scopes.is_empty() {
        return Ok(Vec::new());
    }
    let values =
        load_stored_hot_collection_controls(store, branch_id, branch_generation, scopes).await?;
    let mut controls = Vec::with_capacity(scopes.len());
    for (scope, value) in scopes.iter().copied().zip(values) {
        controls.push(match value {
            Some(control) => Ok(control),
            None => {
                if load_root_current_base_commit(store, branch_id, branch_generation)
                    .await?
                    .is_some()
                {
                    Box::pin(load_root_collection_control(
                        store,
                        branch_id,
                        branch_generation,
                        scope,
                    ))
                    .await
                } else {
                    Ok(HotCollectionControl {
                        active_generation: branch_generation,
                        live_count: 0,
                        ordered_identity_digest: None,
                    })
                }
            }
        }?);
    }
    Ok(controls)
}

async fn load_stored_hot_collection_controls(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    branch_generation: CommitId,
    scopes: &[crate::collection_generation::CollectionScopeRef<'_>],
) -> Result<Vec<Option<HotCollectionControl>>, LixError> {
    let keys = scopes
        .iter()
        .copied()
        .map(|scope| {
            StorageKey(Bytes::from(hot_collection_control_key(
                branch_id,
                branch_generation,
                scope,
            )))
        })
        .collect::<Vec<_>>();
    let values = PointReadPlan::new(HOT_COLLECTION_CONTROL_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value;
    values
        .into_iter()
        .map(|value| match value {
            Some(value) => {
                let StorageProjectedValue::FullValue(bytes) = value else {
                    return Err(head_value_error(
                        "hot collection-control batch read unexpectedly omitted its value",
                    ));
                };
                storage_codec::decode("hot collection control", &bytes).map(Some)
            }
            None => Ok(None),
        })
        .collect()
}

fn stage_hot_collection_control(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    branch_generation: CommitId,
    scope: crate::collection_generation::CollectionScopeRef<'_>,
    control: HotCollectionControl,
) -> Result<(), LixError> {
    writes.put(
        HOT_COLLECTION_CONTROL_SPACE,
        StorageKey(Bytes::from(hot_collection_control_key(
            branch_id,
            branch_generation,
            scope,
        ))),
        StorageValue {
            bytes: Bytes::from(storage_codec::encode("hot collection control", &control)?),
        },
    );
    Ok(())
}

async fn load_incremental_collection_controls(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    branch_generation: CommitId,
    deltas: &[&CurrentStateDeltaRef<'_>],
) -> Result<BTreeMap<(String, Option<String>), HotCollectionControl>, LixError> {
    use crate::collection_generation::{
        COLLECTION_GENERATION_SCHEMA_KEY, CollectionScopeRef, collection_scope_from_entity_pk,
    };

    let mut owned_scopes = BTreeSet::<(String, Option<String>)>::new();
    for delta in deltas {
        if delta.schema_key == COLLECTION_GENERATION_SCHEMA_KEY {
            owned_scopes.insert(collection_scope_from_entity_pk(delta.entity_pk)?);
            continue;
        }
        owned_scopes.insert((delta.schema_key.to_string(), None));
        if let Some(file_id) = delta.file_id {
            owned_scopes.insert((delta.schema_key.to_string(), Some(file_id.to_string())));
        }
    }
    if owned_scopes.is_empty() {
        return Ok(BTreeMap::new());
    }
    let scopes = owned_scopes
        .iter()
        .map(|(schema_key, file_id)| CollectionScopeRef {
            schema_key,
            file_id: file_id.as_deref(),
        })
        .collect::<Vec<_>>();
    let controls =
        load_hot_collection_controls(store, branch_id, branch_generation, &scopes).await?;
    Ok(scopes
        .iter()
        .copied()
        .zip(controls)
        .map(|(scope, control)| {
            (
                (
                    scope.schema_key.to_string(),
                    scope.file_id.map(str::to_string),
                ),
                control,
            )
        })
        .collect())
}

fn stage_incremental_collection_controls(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    branch_generation: CommitId,
    deltas: &[&CurrentStateDeltaRef<'_>],
    previous_values: &[Option<CertifiedCurrentStatePredecessor>],
    mut controls: BTreeMap<(String, Option<String>), HotCollectionControl>,
    certified_live_increments: &BTreeMap<(String, Option<String>), u64>,
) -> Result<(), LixError> {
    use crate::collection_generation::{
        COLLECTION_GENERATION_SCHEMA_KEY, CollectionScopeRef, collection_scope_from_entity_pk,
    };
    let mut dirty_scopes = BTreeSet::new();
    for (delta, previous) in deltas.iter().zip(previous_values) {
        if delta.schema_key == COLLECTION_GENERATION_SCHEMA_KEY {
            let scope = collection_scope_from_entity_pk(delta.entity_pk)?;
            dirty_scopes.insert(scope);
            continue;
        }

        // The compact digest certifies one untouched packed generation. Any
        // row-shaped overlay invalidates it, including a value-only update;
        // later complete-replacement proofs then use the ordinary exact scan.
        for scope in [
            Some((delta.schema_key.to_string(), None)),
            delta
                .file_id
                .map(|file_id| (delta.schema_key.to_string(), Some(file_id.to_string()))),
        ]
        .into_iter()
        .flatten()
        {
            let control = controls
                .get_mut(&scope)
                .expect("row collection scope was loaded above");
            control.ordered_identity_digest = None;
            dirty_scopes.insert(scope);
        }

        let previous_live = previous
            .as_ref()
            .map(CertifiedCurrentStatePredecessor::view)
            .transpose()?
            .is_some_and(|value| {
                !value.deleted
                    && row_belongs_to_active_collection_generation(
                        &controls,
                        branch_generation,
                        delta.schema_key,
                        delta.file_id,
                        value.commit_id,
                    )
            });
        let belongs_to_active_generation = row_belongs_to_active_collection_generation(
            &controls,
            branch_generation,
            delta.schema_key,
            delta.file_id,
            delta.commit_id,
        );
        let next_live = !delta.deleted && belongs_to_active_generation;
        if previous_live == next_live {
            continue;
        }
        for scope in [
            Some((delta.schema_key.to_string(), None)),
            delta
                .file_id
                .map(|file_id| (delta.schema_key.to_string(), Some(file_id.to_string()))),
        ]
        .into_iter()
        .flatten()
        {
            let control = controls
                .get_mut(&scope)
                .expect("row collection scope was loaded above");
            if control.live_count == DEFERRED_ROOT_LIVE_COUNT {
                // Keep the inherited cardinality lazy. The sparse overlay is
                // already included when collection_generation derives it.
                dirty_scopes.insert(scope);
                continue;
            }
            control.live_count = if next_live {
                control
                    .live_count
                    .checked_add(1)
                    .ok_or_else(|| head_value_error("hot collection live count exceeds u64"))?
            } else {
                control
                    .live_count
                    .checked_sub(1)
                    .ok_or_else(|| head_value_error("hot collection live count underflow"))?
            };
            dirty_scopes.insert(scope);
        }
    }

    for (scope, increment) in certified_live_increments {
        let control = controls
            .get_mut(scope)
            .expect("certified collection scope was loaded above");
        if control.live_count != DEFERRED_ROOT_LIVE_COUNT {
            control.live_count = control
                .live_count
                .checked_add(*increment)
                .ok_or_else(|| head_value_error("hot collection live count exceeds u64"))?;
        }
        control.ordered_identity_digest = None;
        dirty_scopes.insert(scope.clone());
    }

    for ((schema_key, file_id), control) in controls {
        if !dirty_scopes.contains(&(schema_key.clone(), file_id.clone())) {
            continue;
        }
        stage_hot_collection_control(
            writes,
            branch_id,
            branch_generation,
            CollectionScopeRef {
                schema_key: &schema_key,
                file_id: file_id.as_deref(),
            },
            control,
        )?;
    }
    Ok(())
}

fn apply_incremental_collection_generation_deltas(
    controls: &mut BTreeMap<(String, Option<String>), HotCollectionControl>,
    deltas: &[&CurrentStateDeltaRef<'_>],
) -> Result<(), LixError> {
    use crate::collection_generation::{
        COLLECTION_GENERATION_SCHEMA_KEY, collection_scope_from_entity_pk,
    };

    for delta in deltas {
        if delta.schema_key != COLLECTION_GENERATION_SCHEMA_KEY {
            continue;
        }
        if delta.deleted {
            return Err(head_value_error(
                "collection-generation controls cannot be tombstoned",
            ));
        }
        let scope = collection_scope_from_entity_pk(delta.entity_pk)?;
        let control = controls
            .get_mut(&scope)
            .expect("collection marker target was loaded above");
        control.active_generation = delta
            .commit_id
            .ok_or_else(|| head_value_error("tracked collection-generation row lacks commit_id"))?;
        control.live_count = 0;
        control.ordered_identity_digest = None;
    }
    Ok(())
}

fn row_belongs_to_active_collection_generation(
    controls: &BTreeMap<(String, Option<String>), HotCollectionControl>,
    branch_generation: CommitId,
    schema_key: &str,
    file_id: Option<&str>,
    commit_id: Option<CommitId>,
) -> bool {
    [
        Some((schema_key.to_string(), None)),
        file_id.map(|file_id| (schema_key.to_string(), Some(file_id.to_string()))),
    ]
    .into_iter()
    .flatten()
    .all(|scope| {
        let control = controls
            .get(&scope)
            .expect("row collection scope was loaded above");
        control.active_generation == branch_generation
            || commit_id.is_some_and(|commit_id| commit_id > control.active_generation)
    })
}

fn stage_complete_collection_controls(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    branch_generation: CommitId,
    rows: &HotRowMap,
) -> Result<(), LixError> {
    use crate::collection_generation::{
        COLLECTION_GENERATION_SCHEMA_KEY, CollectionScopeRef, collection_scope_from_entity_pk,
    };

    let mut controls = BTreeMap::<(String, Option<String>), HotCollectionControl>::new();
    for (identity, bytes) in rows {
        if identity.schema_key == COLLECTION_GENERATION_SCHEMA_KEY {
            let target = collection_scope_from_entity_pk(&identity.entity_pk)?;
            let marker = decode_head_value(bytes)?;
            let active_generation = marker.commit_id.ok_or_else(|| {
                head_value_error("tracked collection-generation row lacks commit_id")
            })?;
            controls.insert(
                target,
                HotCollectionControl {
                    active_generation,
                    live_count: 0,
                    ordered_identity_digest: None,
                },
            );
            continue;
        }
        controls
            .entry((identity.schema_key.clone(), None))
            .or_insert(HotCollectionControl {
                active_generation: branch_generation,
                live_count: 0,
                ordered_identity_digest: None,
            });
        if let Some(file_id) = &identity.file_id {
            controls
                .entry((identity.schema_key.clone(), Some(file_id.clone())))
                .or_insert(HotCollectionControl {
                    active_generation: branch_generation,
                    live_count: 0,
                    ordered_identity_digest: None,
                });
        }
    }

    for (identity, bytes) in rows {
        if identity.schema_key == COLLECTION_GENERATION_SCHEMA_KEY {
            continue;
        }
        let value = decode_head_value(bytes)?;
        if value.deleted {
            continue;
        }
        let schema_scope = (identity.schema_key.clone(), None);
        let schema_control = controls
            .get(&schema_scope)
            .expect("complete row schema control was initialized above");
        let visible_after_schema_generation = schema_control.active_generation == branch_generation
            || value
                .commit_id
                .is_some_and(|commit_id| commit_id > schema_control.active_generation);
        let file_scope = identity
            .file_id
            .as_ref()
            .map(|file_id| (identity.schema_key.clone(), Some(file_id.clone())));
        let visible_after_file_generation = file_scope
            .as_ref()
            .and_then(|scope| controls.get(scope))
            .is_none_or(|control| {
                control.active_generation == branch_generation
                    || value
                        .commit_id
                        .is_some_and(|commit_id| commit_id > control.active_generation)
            });
        if !visible_after_schema_generation || !visible_after_file_generation {
            continue;
        }
        for scope in [Some(schema_scope), file_scope].into_iter().flatten() {
            let control = controls
                .get_mut(&scope)
                .expect("complete row collection control was initialized above");
            control.live_count = control
                .live_count
                .checked_add(1)
                .ok_or_else(|| head_value_error("hot collection live count exceeds u64"))?;
        }
    }

    for ((schema_key, file_id), control) in controls {
        stage_hot_collection_control(
            writes,
            branch_id,
            branch_generation,
            CollectionScopeRef {
                schema_key: &schema_key,
                file_id: file_id.as_deref(),
            },
            control,
        )?;
    }
    Ok(())
}

/// One untracked HOT row overlaid on the authoritative Arrow root.
#[derive(Clone, Debug)]
pub(crate) struct EntityColumnarOverlayRow {
    pub(crate) entity_pk: EntityPk,
    pub(crate) snapshot_content: Option<Bytes>,
    pub(crate) deleted: bool,
    pub(crate) columnar_base_coordinate: Option<ColumnarBaseCoordinate>,
}

/// One physical group in a flattened current-state OLAP layout. A collection
/// may span many content-addressed Arrow leaves; this route preserves the
/// owning set and local group index while DataFusion sees one ordered group
/// sequence.
#[derive(Clone, Debug)]
pub(crate) struct EntityColumnarGroupSource {
    pub(crate) state_set_id: crate::columnar_row_group::ArrowStateSetId,
    pub(crate) manifest: std::sync::Arc<crate::columnar_row_group::RowGroupManifest>,
    pub(crate) manifest_digest: [u8; 32],
    pub(crate) group_index: usize,
}

// Columnar planning temporarily overlaps encoded HOT input, its materialized
// batch, and the final typed overlay. Reserve half of one 256 MiB admission
// envelope for each adjacent representation instead of using a row-count
// policy. These are conservative admission estimates, not allocator metering;
// exceeding either half falls back to the authoritative generic row path.
const ENTITY_COLUMNAR_OVERLAY_INPUT_ADMISSION_BYTES: usize = 128 * 1024 * 1024;
const ENTITY_COLUMNAR_OVERLAY_OUTPUT_ADMISSION_BYTES: usize = 128 * 1024 * 1024;

fn materialized_columnar_overlay_admission_bytes(
    rows: &MaterializedLiveStateBatch,
) -> Result<usize, LixError> {
    rows.iter().try_fold(0_usize, |bytes, row| {
        bytes
            .checked_add(size_of::<MaterializedLiveStateRow>())
            .and_then(|bytes| bytes.checked_add(row.schema_key().len()))
            .and_then(|bytes| bytes.checked_add(row.file_id().map_or(0, str::len)))
            .and_then(|bytes| bytes.checked_add(row.branch_id().len()))
            .and_then(|bytes| bytes.checked_add(row.entity_pk().estimated_heap_bytes()))
            .and_then(|bytes| {
                bytes.checked_add(row.snapshot_content().map_or(0, |value| value.len()))
            })
            .and_then(|bytes| bytes.checked_add(row.metadata().map_or(0, |value| value.len())))
            .ok_or_else(|| head_value_error("entity columnar overlay byte size overflow"))
    })
}

fn push_root_current_base_row(
    rows: &mut MaterializedLiveStateBatchBuilder,
    row: crate::tracked_state::MaterializedTrackedStateRowRef<'_>,
    branch_id: &str,
) {
    let ordinal = rows.push_materialized_ref(
        row.entity_pk(),
        row.schema_key(),
        row.file_id(),
        row.snapshot_content().cloned(),
        row.metadata().cloned(),
        row.deleted(),
        row.created_at(),
        row.updated_at(),
        branch_id == crate::GLOBAL_BRANCH_ID,
        Some(row.change_id()),
        Some(row.commit_id()),
        false,
        branch_id,
    );
    rows.set_durable_predecessor(
        ordinal,
        CertifiedCurrentStatePredecessor::ArrowRoot(ArrowRootHeadValue {
            change_id: row.change_id(),
            commit_id: row.commit_id(),
            deleted: row.deleted(),
            created_at: row.created_at(),
            updated_at: row.updated_at(),
            columnar_base_coordinate: None,
        }),
    );
}

async fn scan_root_current_base_rows(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    request: &TrackedStateScanRequest,
) -> Result<MaterializedLiveStateBatch, LixError> {
    let Some(base_commit_id) = load_root_current_base_commit(store, branch_id, generation).await?
    else {
        return Ok(MaterializedLiveStateBatch::default());
    };
    let mut reader = crate::tracked_state::TrackedStateContext::new().reader(store);
    let tracked =
        Box::pin(reader.scan_batch_at_commit(&base_commit_id.to_string(), request)).await?;
    let scopes = tracked
        .iter()
        .filter(|row| {
            row.schema_key() != crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
        })
        .flat_map(|row| {
            [
                Some((row.schema_key().to_owned(), None)),
                row.file_id()
                    .map(|file_id| (row.schema_key().to_owned(), Some(file_id.to_owned()))),
            ]
            .into_iter()
            .flatten()
        })
        .collect::<BTreeSet<_>>();
    let scope_refs = scopes
        .iter()
        .map(
            |(schema_key, file_id)| crate::collection_generation::CollectionScopeRef {
                schema_key,
                file_id: file_id.as_deref(),
            },
        )
        .collect::<Vec<_>>();
    let active_generations =
        load_root_active_collection_generations(store, base_commit_id, scope_refs.iter().copied())
            .await?;
    let stored_control_values =
        load_stored_hot_collection_controls(store, branch_id, generation, &scope_refs).await?;
    let stored_controls = scopes
        .iter()
        .cloned()
        .zip(stored_control_values)
        .filter_map(|(scope, control)| control.map(|control| (scope, control)))
        .collect::<BTreeMap<_, _>>();
    let mut rows = MaterializedLiveStateBatchBuilder::with_capacity(tracked.len());
    for row in tracked.iter() {
        if !root_tracked_row_is_active(row, generation, &active_generations, &stored_controls) {
            continue;
        }
        push_root_current_base_row(&mut rows, row, branch_id);
    }
    Ok(rows.finish())
}

async fn scan_root_current_base_rows_for_merge(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    request: &TrackedStateScanRequest,
    other_candidate_count: usize,
) -> Result<MaterializedLiveStateBatch, LixError> {
    let Some(base_commit_id) = load_root_current_base_commit(store, branch_id, generation).await?
    else {
        return Ok(MaterializedLiveStateBatch::default());
    };
    let exact_scopes = (!request.filter.schema_keys.is_empty()
        && !request.filter.file_ids.is_empty()
        && request
            .filter
            .file_ids
            .iter()
            .all(|file_id| !matches!(file_id, NullableKeyFilter::Any)))
    .then(|| {
        request
            .filter
            .schema_keys
            .iter()
            .flat_map(|schema_key| {
                std::iter::once(crate::collection_generation::CollectionScopeRef {
                    schema_key,
                    file_id: None,
                })
                .chain(request.filter.file_ids.iter().filter_map(|file_id| {
                    if let NullableKeyFilter::Value(file_id) = file_id {
                        Some(crate::collection_generation::CollectionScopeRef {
                            schema_key,
                            file_id: Some(file_id),
                        })
                    } else {
                        None
                    }
                }))
            })
            .collect::<Vec<_>>()
    });
    let has_local_collection_replacement = if let Some(scopes) = exact_scopes.as_deref() {
        load_stored_hot_collection_controls(store, branch_id, generation, scopes)
            .await?
            .into_iter()
            .flatten()
            .any(|control| control.active_generation != generation)
    } else {
        let mut control_entries = Vec::new();
        if request.filter.schema_keys.is_empty() {
            control_entries = ScanPlan::prefix(
                HOT_COLLECTION_CONTROL_SPACE,
                StoragePrefix {
                    bytes: Bytes::from(hot_scope_prefix(branch_id, generation)),
                },
            )
            .collect(store, StorageScanOptions::default())
            .await?
            .value
            .entries;
        } else {
            for schema_key in &request.filter.schema_keys {
                let mut prefix = hot_scope_prefix(branch_id, generation);
                write_key_string(&mut prefix, schema_key, KEY_PART_FINAL);
                control_entries.extend(
                    ScanPlan::prefix(
                        HOT_COLLECTION_CONTROL_SPACE,
                        StoragePrefix {
                            bytes: Bytes::from(prefix),
                        },
                    )
                    .collect(store, StorageScanOptions::default())
                    .await?
                    .value
                    .entries,
                );
            }
        }
        control_entries
            .into_iter()
            .try_fold(false, |found, entry| -> Result<_, LixError> {
                let value = full_value_bytes(entry.value)?;
                let control: HotCollectionControl =
                    storage_codec::decode("hot collection control", &value)?;
                Ok(found || control.active_generation != generation)
            })?
    };
    let root_has_collection_replacement = if let Some(scopes) = exact_scopes {
        !load_root_active_collection_generations(store, base_commit_id, scopes)
            .await?
            .is_empty()
    } else {
        let mut marker_reader = crate::tracked_state::TrackedStateContext::new().reader(store);
        let root_collection_markers = Box::pin(marker_reader.scan_batch_at_commit(
            &base_commit_id.to_string(),
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    schema_keys: vec![
                        crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY.to_owned(),
                    ],
                    ..TrackedStateFilter::default()
                },
                read_columns: TrackedStateReadColumns {
                    columns: vec!["change_id".to_owned()],
                },
                limit: Some(1),
            },
        ))
        .await?;
        root_collection_markers.iter().next().is_some()
    };
    let mut root_request = request.clone();
    if has_local_collection_replacement || root_has_collection_replacement {
        root_request.limit = None;
    } else if let Some(limit) = root_request.limit.as_mut() {
        // Every sparse candidate can shadow at most one root identity. Reading
        // that many extra ordered root rows preserves the caller's final LIMIT
        // without materializing history-sized state.
        *limit = limit.saturating_add(other_candidate_count);
    }
    scan_root_current_base_rows(store, branch_id, generation, &root_request).await
}

async fn load_root_current_base_exact(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    keys: &[TrackedStateKeyRef<'_>],
    projection: ChangeRecordProjection,
) -> Result<MaterializedLiveStateExactBatch, LixError> {
    let Some(base_commit_id) = load_root_current_base_commit(store, branch_id, generation).await?
    else {
        return MaterializedLiveStateExactBatch::new(
            MaterializedLiveStateBatch::default(),
            vec![None; keys.len()],
        );
    };
    let mut reader = crate::tracked_state::TrackedStateContext::new().reader(store);
    let tracked = Box::pin(reader.load_projected_batch_at_commit_refs(
        &base_commit_id.to_string(),
        keys,
        &projection,
    ))
    .await?;
    let scopes = keys
        .iter()
        .filter(|key| {
            key.schema_key != crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
        })
        .flat_map(|key| {
            [
                Some((key.schema_key.to_owned(), None)),
                key.file_id
                    .map(|file_id| (key.schema_key.to_owned(), Some(file_id.to_owned()))),
            ]
            .into_iter()
            .flatten()
        })
        .collect::<BTreeSet<_>>();
    let scope_refs = scopes
        .iter()
        .map(
            |(schema_key, file_id)| crate::collection_generation::CollectionScopeRef {
                schema_key,
                file_id: file_id.as_deref(),
            },
        )
        .collect::<Vec<_>>();
    let active_generations =
        load_root_active_collection_generations(store, base_commit_id, scope_refs.iter().copied())
            .await?;
    let stored_control_values =
        load_stored_hot_collection_controls(store, branch_id, generation, &scope_refs).await?;
    let stored_controls = scopes
        .iter()
        .cloned()
        .zip(stored_control_values)
        .filter_map(|(scope, control)| control.map(|control| (scope, control)))
        .collect::<BTreeMap<_, _>>();
    let mut rows = MaterializedLiveStateBatchBuilder::with_capacity(keys.len());
    let mut slots = Vec::with_capacity(keys.len());
    for index in 0..tracked.len() {
        slots.push(
            tracked
                .row(index)
                .filter(|row| {
                    root_tracked_row_is_active(
                        *row,
                        generation,
                        &active_generations,
                        &stored_controls,
                    )
                })
                .map(|row| {
                    let ordinal = u32::try_from(rows.len())
                        .expect("root current-base exact result exceeds u32 rows");
                    push_root_current_base_row(&mut rows, row, branch_id);
                    ordinal
                }),
        );
    }
    MaterializedLiveStateExactBatch::new(rows.finish(), slots)
}

async fn load_root_active_collection_generations<'a>(
    store: &(impl StorageAdapterRead + ?Sized),
    base_commit_id: CommitId,
    scopes: impl IntoIterator<Item = crate::collection_generation::CollectionScopeRef<'a>>,
) -> Result<BTreeMap<(String, Option<String>), RootCollectionGeneration>, LixError> {
    let scopes = scopes
        .into_iter()
        .map(|scope| {
            (
                scope.schema_key.to_owned(),
                scope.file_id.map(str::to_owned),
            )
        })
        .collect::<BTreeSet<_>>();
    if scopes.is_empty() {
        return Ok(BTreeMap::new());
    }
    let marker_keys = scopes
        .iter()
        .map(|(schema_key, file_id)| TrackedStateKey {
            schema_key: crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY.to_owned(),
            file_id: None,
            entity_pk: EntityPk::single(crate::collection_generation::collection_scope_key(
                crate::collection_generation::CollectionScopeRef {
                    schema_key,
                    file_id: file_id.as_deref(),
                },
            )),
        })
        .collect::<Vec<_>>();
    let marker_refs = marker_keys
        .iter()
        .map(|key| TrackedStateKeyRef {
            schema_key: &key.schema_key,
            file_id: key.file_id.as_deref(),
            entity_pk: &key.entity_pk,
        })
        .collect::<Vec<_>>();
    let mut reader = crate::tracked_state::TrackedStateContext::new().reader(store);
    let markers = Box::pin(reader.load_projected_batch_at_commit_refs(
        &base_commit_id.to_string(),
        &marker_refs,
        &ChangeRecordProjection::identity_only(),
    ))
    .await?;
    Ok(scopes
        .into_iter()
        .enumerate()
        .filter_map(|(index, scope)| {
            markers.row(index).map(|row| {
                (
                    scope,
                    RootCollectionGeneration {
                        commit_id: row.commit_id(),
                    },
                )
            })
        })
        .collect())
}

#[derive(Clone, Copy)]
struct RootCollectionGeneration {
    commit_id: CommitId,
}

fn root_tracked_row_is_active(
    row: crate::tracked_state::MaterializedTrackedStateRowRef<'_>,
    branch_generation: CommitId,
    active_generations: &BTreeMap<(String, Option<String>), RootCollectionGeneration>,
    stored_controls: &BTreeMap<(String, Option<String>), HotCollectionControl>,
) -> bool {
    if row.schema_key() == crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY {
        return true;
    }
    [
        Some((row.schema_key().to_owned(), None)),
        row.file_id()
            .map(|file_id| (row.schema_key().to_owned(), Some(file_id.to_owned()))),
    ]
    .into_iter()
    .flatten()
    .all(|scope| {
        let root_generation = active_generations
            .get(&scope)
            .map_or(branch_generation, |generation| generation.commit_id);
        if stored_controls
            .get(&scope)
            .is_some_and(|control| control.active_generation != root_generation)
        {
            return false;
        }
        active_generations
            .get(&scope)
            .is_none_or(|generation| row.commit_id() > generation.commit_id)
    })
}

#[cfg(test)]
fn compare_materialized_live_identities(
    left: &MaterializedLiveStateRow,
    right: &MaterializedLiveStateRow,
) -> Ordering {
    left.schema_key
        .cmp(&right.schema_key)
        .then_with(|| left.entity_pk.cmp(&right.entity_pk))
        .then_with(|| left.file_id.cmp(&right.file_id))
}

#[cfg(test)]
fn merge_ordered_live_rows(
    left: Vec<MaterializedLiveStateRow>,
    right: Vec<MaterializedLiveStateRow>,
) -> Vec<MaterializedLiveStateRow> {
    let mut left = VecDeque::from(left);
    let mut right = VecDeque::from(right);
    let mut merged = Vec::with_capacity(left.len().saturating_add(right.len()));
    while let (Some(left_row), Some(right_row)) = (left.front(), right.front()) {
        match compare_materialized_live_identities(left_row, right_row) {
            Ordering::Less => {
                merged.push(left.pop_front().expect("peeked left row exists"));
            }
            Ordering::Greater => {
                merged.push(right.pop_front().expect("peeked right row exists"));
            }
            Ordering::Equal => {
                let left_row = left.pop_front().expect("peeked left row exists");
                let right_row = right.pop_front().expect("peeked right row exists");
                if left_row.commit_id < right_row.commit_id {
                    merged.push(right_row);
                } else {
                    merged.push(left_row);
                }
            }
        }
    }
    merged.extend(left);
    merged.extend(right);
    merged
}

fn compare_materialized_live_identity_refs(
    left: MaterializedLiveStateRowRef<'_>,
    right: MaterializedLiveStateRowRef<'_>,
) -> Ordering {
    left.schema_key()
        .cmp(right.schema_key())
        .then_with(|| left.entity_pk().cmp(right.entity_pk()))
        .then_with(|| left.file_id().cmp(&right.file_id()))
}

/// Merge two identity-ordered materialized batches without expanding their
/// dictionary and payload columns into row-owned DTOs.
fn merge_ordered_live_batches(
    left: MaterializedLiveStateBatch,
    right: MaterializedLiveStateBatch,
) -> MaterializedLiveStateBatch {
    if left.is_empty() {
        return right;
    }
    if right.is_empty() {
        return left;
    }
    let mut merged =
        MaterializedLiveStateBatchBuilder::with_capacity(left.len().saturating_add(right.len()));
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.len() && right_index < right.len() {
        let left_row = left.row(left_index);
        let right_row = right.row(right_index);
        match compare_materialized_live_identity_refs(left_row, right_row) {
            Ordering::Less => {
                merged.push_ref(left_row, None);
                left_index += 1;
            }
            Ordering::Greater => {
                merged.push_ref(right_row, None);
                right_index += 1;
            }
            Ordering::Equal => {
                if left_row.commit_id() < right_row.commit_id() {
                    merged.push_ref(right_row, None);
                } else {
                    merged.push_ref(left_row, None);
                }
                left_index += 1;
                right_index += 1;
            }
        }
    }
    while left_index < left.len() {
        merged.push_ref(left.row(left_index), None);
        left_index += 1;
    }
    while right_index < right.len() {
        merged.push_ref(right.row(right_index), None);
        right_index += 1;
    }
    merged.finish()
}

/// Direct reader for one published hot generation.
pub(crate) struct HotStateStoreReader<S> {
    pub(super) store: S,
    pub(super) transaction_cache: Option<Arc<HotStateTransactionCache>>,
}

impl<S> HotStateStoreReader<S>
where
    S: StorageAdapterRead,
{
    async fn collection_control(
        &self,
        branch_id: &str,
        generation: CommitId,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<HotCollectionControl, LixError> {
        let key = HotCollectionCacheKey {
            branch_id: branch_id.to_owned(),
            generation,
            schema_key: scope.schema_key.to_owned(),
            file_id: scope.file_id.map(str::to_owned),
        };
        if let Some(cache) = self.transaction_cache.as_deref()
            && let Some(control) = cache.collection_control(&key)?
        {
            return Ok(control);
        }
        let control =
            load_hot_collection_control(&self.store, branch_id, generation, scope).await?;
        if let Some(cache) = self.transaction_cache.as_deref() {
            cache.remember_collection_control(key, control)?;
        }
        Ok(control)
    }

    pub(crate) async fn collection_generation(
        &self,
        branch_id: &str,
        branch_generation: CommitId,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<crate::collection_generation::CollectionGeneration, LixError> {
        let control = self
            .collection_control(branch_id, branch_generation, scope)
            .await?;
        Ok(crate::collection_generation::CollectionGeneration {
            active_generation: control.active_generation,
            live_count: control.live_count,
            ordered_identity_digest: control.ordered_identity_digest,
        })
    }

    pub(crate) async fn exact_collection_live_count(
        &self,
        branch_id: &str,
        branch_generation: CommitId,
        scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<u64, LixError> {
        let rows = Box::pin(self.scan_live_batch_for_generation(
            branch_id,
            branch_generation,
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    schema_keys: vec![scope.schema_key.to_owned()],
                    file_ids: scope.file_id.map_or_else(Vec::new, |file_id| {
                        vec![NullableKeyFilter::Value(file_id.to_owned())]
                    }),
                    ..TrackedStateFilter::default()
                },
                read_columns: TrackedStateReadColumns {
                    columns: vec!["change_id".to_owned()],
                },
                limit: None,
            },
        ))
        .await?;
        u64::try_from(rows.len())
            .map_err(|_| head_value_error("hot collection live count exceeds u64"))
    }

    pub(crate) async fn scan_live_batch(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        request: &TrackedStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        self.scan_live_batch_for_generation(branch_id, control.generation, request)
            .await
    }

    pub(crate) async fn scan_live_rows(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        self.scan_live_batch(branch_id, control, request)
            .await
            .map(MaterializedLiveStateBatch::into_rows)
    }

    pub(crate) async fn scan_live_batches_for_controls(
        &self,
        controls: &[(String, BranchHeadControl)],
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<(String, MaterializedLiveStateBatch)>, LixError> {
        let mut rows = Vec::with_capacity(controls.len());
        for (branch_id, control) in controls {
            let branch_rows = self.scan_live_batch(branch_id, *control, request).await?;
            rows.push((branch_id.clone(), branch_rows));
        }
        Ok(rows)
    }

    pub(crate) async fn has_schema_rows(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        schema_key: &str,
    ) -> Result<bool, LixError> {
        let mut prefix = hot_scope_prefix(branch_id, control.generation);
        write_key_string(&mut prefix, schema_key, KEY_PART_FINAL);
        let page = ScanPlan::prefix(
            HOT_ROW_SPACE,
            StoragePrefix {
                bytes: Bytes::from(prefix),
            },
        )
        .collect(
            &self.store,
            StorageScanOptions {
                projection: StorageCoreProjection::KeyOnly,
                limit_rows: 1,
                ..StorageScanOptions::default()
            },
        )
        .await?;
        if !page.value.entries.is_empty() {
            return Ok(true);
        }
        let root = if load_root_current_base_commit(&self.store, branch_id, control.generation)
            .await?
            .is_some()
        {
            Box::pin(scan_root_current_base_rows(
                &self.store,
                branch_id,
                control.generation,
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec![schema_key.to_owned()],
                        ..TrackedStateFilter::default()
                    },
                    read_columns: TrackedStateReadColumns {
                        columns: vec!["change_id".to_owned()],
                    },
                    // Root collection-generation filtering happens after the
                    // tracked-tree scan, so an early limit could select only
                    // a retired row while a later live row still exists.
                    limit: None,
                },
            ))
            .await?
        } else {
            MaterializedLiveStateBatch::default()
        };
        if !root.is_empty() {
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) async fn scan_entity_snapshots(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        schema_key: &str,
        entity_pks: &[EntityPk],
        limit: Option<usize>,
    ) -> Result<Vec<Option<Bytes>>, LixError> {
        self.scan_entity_snapshots_for_generation(
            branch_id,
            control.generation,
            schema_key,
            entity_pks,
            limit,
        )
        .await
    }
    pub(crate) async fn scan_entity_primary_keys(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        schema_key: &str,
        entity_pks: &[EntityPk],
        limit: Option<usize>,
    ) -> Result<Vec<EntityPk>, LixError> {
        if matches!(limit, Some(0)) {
            return Ok(Vec::new());
        }
        let rows = self
            .scan_live_batch_for_generation(
                branch_id,
                control.generation,
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec![schema_key.to_owned()],
                        entity_pks: entity_pks.to_vec(),
                        include_tombstones: false,
                        ..TrackedStateFilter::default()
                    },
                    // The provider drops snapshot bytes before Arrow conversion
                    // because only identity columns were requested.
                    read_columns: TrackedStateReadColumns {
                        columns: vec!["snapshot_content".to_owned()],
                    },
                    limit,
                },
            )
            .await?;
        Ok(rows.into_identity_ordered_primary_keys())
    }

    /// Plans a typed scan directly from the immutable Arrow root plus the
    /// bounded untracked HOT overlay.
    pub(crate) async fn entity_columnar_layout(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        schema_key: &str,
    ) -> Result<
        Option<(
            crate::columnar_row_group::ArrowStateSetId,
            crate::columnar_row_group::RowGroupManifest,
            Vec<EntityColumnarGroupSource>,
            Vec<EntityColumnarOverlayRow>,
            u64,
        )>,
        LixError,
    > {
        if let Some(root_commit_id) =
            load_root_current_base_commit(&self.store, branch_id, control.generation).await?
        {
            let collection_scope = crate::collection_generation::CollectionScopeRef {
                schema_key,
                file_id: None,
            };
            if !load_root_active_collection_generations(
                &self.store,
                root_commit_id,
                [collection_scope],
            )
            .await?
            .is_empty()
            {
                return Ok(None);
            }
            let published = crate::tracked_state::load_published_commit_state_manifest(
                &self.store,
                root_commit_id,
            )
            .await?
            .ok_or_else(|| head_value_error("root-backed columnar head is missing its manifest"))?;
            let descriptors = crate::tracked_state::load_current_state_scope_descriptors(
                &self.store,
                &published,
                &crate::tracked_state::CommitDeltaReplacementScope {
                    schema_key: schema_key.to_owned(),
                    file_id: None,
                },
            )
            .await?;
            if descriptors.is_empty() {
                return Ok(None);
            }
            let mut group_sources = Vec::with_capacity(descriptors.len());
            let mut layout_hasher =
                blake3::Hasher::new_derive_key("lix current-state flattened Arrow collection v1");
            for descriptor in descriptors {
                let manifest = crate::columnar_row_group::load_row_group_manifest(
                    &self.store,
                    descriptor.state_set_id,
                )
                .await?
                .ok_or_else(|| {
                    head_value_error("Arrow state descriptor references a missing leaf")
                })?;
                if manifest.namespace != "lix.tracked_state.arrow_leaf.v1"
                    || manifest.metadata.get("lix.layout").map(String::as_str)
                        != Some(crate::sql2::ENTITY_ARROW_STATE_LAYOUT)
                {
                    return Err(head_value_error(
                        "current-state descriptor does not reference the canonical Arrow leaf layout",
                    ));
                }
                let local_group = descriptor.state_group_index as usize;
                manifest.groups.get(local_group).ok_or_else(|| {
                    head_value_error("Arrow state descriptor has an invalid group index")
                })?;
                let manifest_digest = manifest.content_digest()?;
                layout_hasher.update(&descriptor.state_set_id.as_bytes());
                layout_hasher.update(&descriptor.state_group_index.to_be_bytes());
                group_sources.push(EntityColumnarGroupSource {
                    state_set_id: descriptor.state_set_id,
                    manifest: std::sync::Arc::new(manifest),
                    manifest_digest,
                    group_index: local_group,
                });
            }
            let first_manifest = &group_sources
                .first()
                .expect("non-empty descriptors have a source")
                .manifest;
            let common_fields = first_manifest
                .fields
                .iter()
                .filter(|field| {
                    group_sources.iter().all(|source| {
                        source
                            .manifest
                            .fields
                            .iter()
                            .find(|candidate| candidate.name == field.name)
                            == Some(*field)
                    })
                })
                .cloned()
                .collect::<Vec<_>>();
            let common_field_names = common_fields
                .iter()
                .map(|field| field.name.clone())
                .collect::<Vec<_>>();
            let mut flattened_manifest = first_manifest.as_ref().clone();
            flattened_manifest.fields = common_fields;
            flattened_manifest.groups = group_sources
                .iter()
                .map(|source| {
                    crate::columnar_row_group::remap_row_group_statistics(
                        &source.manifest,
                        source.group_index,
                        &common_field_names,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let layout_id = crate::columnar_row_group::ArrowStateSetId::from_digest(
                *layout_hasher.finalize().as_bytes(),
            );

            let filter = TrackedStateFilter {
                schema_keys: vec![schema_key.to_owned()],
                include_tombstones: true,
                ..TrackedStateFilter::default()
            };
            let Some(entries) = hot_scan_entries(
                &self.store,
                branch_id,
                control.generation,
                &filter,
                None,
                Some(ENTITY_COLUMNAR_OVERLAY_INPUT_ADMISSION_BYTES),
            )
            .await?
            else {
                return Ok(None);
            };
            let rows = materialize_hot_scan_entries(
                &self.store,
                entries,
                ChangeRecordProjection::from_columns(&["snapshot_content".to_owned()]),
                branch_id,
            )
            .await?;
            if materialized_columnar_overlay_admission_bytes(&rows)?
                > ENTITY_COLUMNAR_OVERLAY_OUTPUT_ADMISSION_BYTES
            {
                return Ok(None);
            }
            let mut overlay = Vec::with_capacity(rows.len());
            for row in rows.iter() {
                if row.file_id().is_some() || row.global() || !row.untracked() {
                    return Ok(None);
                }
                overlay.push(EntityColumnarOverlayRow {
                    entity_pk: row.entity_pk().clone(),
                    snapshot_content: row
                        .snapshot_content()
                        .map(|snapshot| Bytes::copy_from_slice(snapshot.as_bytes())),
                    deleted: row.deleted(),
                    columnar_base_coordinate: None,
                });
            }
            let live_count = self
                .exact_collection_live_count(
                    branch_id,
                    control.generation,
                    crate::collection_generation::CollectionScopeRef {
                        schema_key,
                        file_id: None,
                    },
                )
                .await?;
            return Ok(Some((
                layout_id,
                flattened_manifest,
                group_sources,
                overlay,
                live_count,
            )));
        }

        Ok(None)
    }

    #[cfg(test)]
    pub(crate) async fn scan_live_rows_if_current(
        &self,
        branch_id: &str,
        expected_head: &str,
        request: &TrackedStateScanRequest,
    ) -> Result<Option<Vec<MaterializedLiveStateRow>>, LixError> {
        let expected_head = CommitId::parse_lix(expected_head, "hot-state expected commit")?;
        let control = BranchHeadControlContext::new()
            .reader(&self.store)
            .load(branch_id)
            .await?;
        let Some(control) = control.filter(|control| control.head_commit_id == expected_head)
        else {
            return Ok(None);
        };
        Ok(Some(
            self.scan_live_batch_for_generation(branch_id, control.generation, request)
                .await?
                .into_rows(),
        ))
    }

    async fn scan_live_batch_for_generation(
        &self,
        branch_id: &str,
        generation: CommitId,
        request: &TrackedStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        let collection_control = match request.filter.schema_keys.as_slice() {
            [schema_key]
                if schema_key != crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY =>
            {
                Some(
                    load_hot_collection_visibility_control(
                        &self.store,
                        branch_id,
                        generation,
                        crate::collection_generation::CollectionScopeRef {
                            schema_key,
                            file_id: None,
                        },
                    )
                    .await?,
                )
            }
            _ => None,
        };
        let replaced_generation =
            collection_control.filter(|control| control.active_generation != generation);
        // A storage prefix is ordered by identity, but tombstones are filtered
        // only after decoding the value. Applying SQL LIMIT to the raw scan
        // would therefore let one tombstone hide a later live row.
        let mut entries = hot_scan_entries(
            &self.store,
            branch_id,
            generation,
            &request.filter,
            None,
            None,
        )
        .await?
        .expect("unbounded HOT scan cannot exhaust a byte budget");
        if let Some(control) = replaced_generation {
            filter_hot_scan_entries_by_collection_generation(&mut entries, control)?;
        }
        let projection = ChangeRecordProjection::from_columns(&request.read_columns.columns);
        let rows =
            materialize_hot_scan_entries(&self.store, entries, projection, branch_id).await?;
        let rows = rows.filter(
            |row| {
                replaced_generation.is_none_or(|control| {
                    row.commit_id()
                        .is_some_and(|commit_id| commit_id > control.active_generation)
                })
            },
            None,
        );
        // The root owns committed tracked rows; HOT contains only untracked
        // rows. Resolve winners from those two sources and nowhere else.
        let root_rows = Box::pin(scan_root_current_base_rows_for_merge(
            &self.store,
            branch_id,
            generation,
            request,
            rows.len(),
        ))
        .await?;
        let rows = merge_ordered_live_batches(rows, root_rows);
        if request.filter.include_tombstones
            && request.limit.is_none()
            && replaced_generation.is_none()
        {
            return Ok(rows);
        }
        Ok(rows.filter(
            |row| request.filter.include_tombstones || !row.deleted(),
            request.limit,
        ))
    }

    #[cfg(test)]
    pub(crate) async fn load_projected_live_rows_if_current(
        &self,
        branch_id: &str,
        expected_head: &str,
        keys: &[TrackedStateKey],
        projection: &ChangeRecordProjection,
    ) -> Result<Option<Vec<Option<MaterializedLiveStateRow>>>, LixError> {
        let expected_head = CommitId::parse_lix(expected_head, "hot-state expected commit")?;
        let control = BranchHeadControlContext::new()
            .reader(&self.store)
            .load(branch_id)
            .await?;
        let Some(control) = control.filter(|control| control.head_commit_id == expected_head)
        else {
            return Ok(None);
        };
        Ok(Some(
            self.load_projected_live_batch(branch_id, control, keys, projection)
                .await?
                .into_rows(),
        ))
    }

    pub(crate) async fn load_projected_live_rows(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        keys: &[TrackedStateKey],
        projection: &ChangeRecordProjection,
    ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
        self.load_projected_live_batch(branch_id, control, keys, projection)
            .await
            .map(MaterializedLiveStateExactBatch::into_rows)
    }

    pub(crate) async fn load_projected_live_batch(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        keys: &[TrackedStateKey],
        projection: &ChangeRecordProjection,
    ) -> Result<MaterializedLiveStateExactBatch, LixError> {
        let keys = keys
            .iter()
            .map(|key| TrackedStateKeyRef {
                schema_key: key.schema_key.as_str(),
                file_id: key.file_id.as_deref(),
                entity_pk: &key.entity_pk,
            })
            .collect::<Vec<_>>();
        self.load_projected_live_batch_refs(branch_id, control, &keys, projection)
            .await
    }

    pub(crate) async fn load_projected_live_batch_refs(
        &self,
        branch_id: &str,
        control: BranchHeadControl,
        keys: &[TrackedStateKeyRef<'_>],
        projection: &ChangeRecordProjection,
    ) -> Result<MaterializedLiveStateExactBatch, LixError> {
        Box::pin(self.load_projected_live_batch_for_generation_refs(
            branch_id,
            control.generation,
            keys,
            projection,
        ))
        .await
    }

    async fn load_projected_live_batch_for_generation_refs(
        &self,
        branch_id: &str,
        generation: CommitId,
        keys: &[TrackedStateKeyRef<'_>],
        projection: &ChangeRecordProjection,
    ) -> Result<MaterializedLiveStateExactBatch, LixError> {
        if keys.is_empty() {
            return Ok(MaterializedLiveStateExactBatch::default());
        }
        let replaced_generation = keys
            .first()
            .filter(|first| keys.iter().all(|key| key.schema_key == first.schema_key))
            .filter(|first| {
                first.schema_key != crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
            })
            .map(|first| async {
                load_hot_collection_visibility_control(
                    &self.store,
                    branch_id,
                    generation,
                    crate::collection_generation::CollectionScopeRef {
                        schema_key: first.schema_key,
                        file_id: None,
                    },
                )
                .await
            });
        let replaced_generation = match replaced_generation {
            Some(control) => {
                let control = control.await?;
                (control.active_generation != generation).then_some(control)
            }
            None => None,
        };
        if replaced_generation.is_some_and(|control| control.live_count == 0) {
            return MaterializedLiveStateExactBatch::new(
                MaterializedLiveStateBatch::default(),
                vec![None; keys.len()],
            );
        }
        let mut values =
            hot_load_identity_ref_bytes(&self.store, branch_id, generation, keys).await?;
        if let Some(control) = replaced_generation {
            for value in &mut values {
                let visible = value
                    .as_deref()
                    .map(decode_head_value)
                    .transpose()?
                    .and_then(|value| value.commit_id)
                    .is_some_and(|commit_id| commit_id > control.active_generation);
                if !visible {
                    *value = None;
                }
            }
        }
        let mut slots = Vec::with_capacity(values.len());
        let mut entries = Vec::with_capacity(values.iter().flatten().count());
        for (identity, value) in keys.iter().copied().zip(values) {
            slots.push(value.map(|value| {
                let ordinal =
                    u32::try_from(entries.len()).expect("live-state exact batch exceeds u32 rows");
                entries.push((identity, value));
                ordinal
            }));
        }
        let rows = materialize_live_entries(&self.store, entries, *projection, branch_id).await?;

        let root_backed = load_root_current_base_commit(&self.store, branch_id, generation)
            .await?
            .is_some();
        let root = if root_backed {
            Box::pin(load_root_current_base_exact(
                &self.store,
                branch_id,
                generation,
                keys,
                *projection,
            ))
            .await?
        } else {
            MaterializedLiveStateExactBatch::new(
                MaterializedLiveStateBatch::default(),
                vec![None; keys.len()],
            )?
        };
        let mut resolved = Vec::with_capacity(keys.len());
        for (index, slot) in slots.into_iter().enumerate() {
            let mut row = slot.and_then(|slot| rows.get(slot as usize));
            for candidate in [root.row(index)].into_iter().flatten() {
                if row.is_none_or(
                    |current| match (current.commit_id(), candidate.commit_id()) {
                        (Some(current), Some(candidate)) => candidate > current,
                        (None, Some(_)) => false,
                        (Some(_), None) => false,
                        (None, None) => false,
                    },
                ) {
                    row = Some(candidate);
                }
            }
            resolved.push(row.filter(|row| {
                replaced_generation.is_none_or(|control| {
                    row.commit_id()
                        .is_some_and(|commit_id| commit_id >= control.active_generation)
                })
            }));
        }
        let mut builder = MaterializedLiveStateBatchBuilder::with_capacity(keys.len());
        let mut combined_slots = Vec::with_capacity(keys.len());
        for row in resolved {
            combined_slots.push(
                row.map(|row| {
                    u32::try_from(builder.push_ref(row, None)).map_err(|_| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "exact live-state result exceeds u32 rows",
                        )
                    })
                })
                .transpose()?,
            );
        }
        MaterializedLiveStateExactBatch::new(builder.finish(), combined_slots)
    }

    pub(crate) async fn untracked_json_refs(
        &self,
        controls: &[(String, BranchHeadControl)],
    ) -> Result<Vec<JsonRef>, LixError> {
        let mut refs = BTreeSet::new();
        for (branch_id, control) in controls {
            let scope = hot_scope_prefix(branch_id, control.generation);
            let plan = ScanPlan::prefix(
                HOT_ROW_SPACE,
                StoragePrefix {
                    bytes: Bytes::from(scope),
                },
            );
            let mut resume_after = None;
            loop {
                let page = plan
                    .collect(
                        &self.store,
                        StorageScanOptions {
                            resume_after: resume_after.clone(),
                            ..StorageScanOptions::default()
                        },
                    )
                    .await?;
                resume_after = page.value.entries.last().map(|entry| entry.key.clone());
                for entry in page.value.entries {
                    let bytes = full_value_bytes(entry.value)?;
                    let value = decode_head_value(&bytes)?;
                    collect_hot_untracked_refs(value, &mut refs);
                }
                if !page.value.has_more || resume_after.is_none() {
                    break;
                }
            }
        }
        Ok(refs.into_iter().map(JsonRef::from_hash_bytes).collect())
    }

    pub(crate) async fn working_diff_for_control(
        &self,
        _branch_id: &str,
        control: BranchHeadControl,
        request: &TrackedStateDiffRequest,
    ) -> Result<Option<TrackedWorkingDiff>, LixError> {
        let Some(checkpoint_commit_id) = control.working_diff_checkpoint_commit_id else {
            return Ok(None);
        };
        let mut tracked = crate::tracked_state::TrackedStateContext::new().reader(&self.store);
        let diff = tracked
            .diff_commits(
                &checkpoint_commit_id.to_string(),
                &control.head_commit_id.to_string(),
                request,
            )
            .await?;
        Ok(Some(TrackedWorkingDiff {
            checkpoint_commit_id,
            diff,
        }))
    }

    async fn scan_entity_snapshots_for_generation(
        &self,
        branch_id: &str,
        generation: CommitId,
        schema_key: &str,
        entity_pks: &[EntityPk],
        limit: Option<usize>,
    ) -> Result<Vec<Option<Bytes>>, LixError> {
        if matches!(limit, Some(0)) {
            return Ok(Vec::new());
        }
        let rows = self
            .scan_live_batch_for_generation(
                branch_id,
                generation,
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec![schema_key.to_owned()],
                        entity_pks: entity_pks.to_vec(),
                        include_tombstones: false,
                        ..TrackedStateFilter::default()
                    },
                    read_columns: TrackedStateReadColumns {
                        columns: vec!["snapshot_content".to_owned()],
                    },
                    limit,
                },
            )
            .await?;
        Ok(rows.into_identity_ordered_snapshots())
    }
}

type HotRowMap = BTreeMap<HeadRowIdentity, Bytes>;

/// An owned tracked snapshot used only while a lifecycle publication is being
/// staged. Normal commits mutate the published hot generation in place; a
/// checkpoint, merge, or branch move instead builds one complete replacement
/// generation before the branch control makes it visible.
///
/// The snapshot deliberately stores the already encoded row values. That
/// keeps large JSON slots as refs/inline values and makes a root staged in
/// this write set usable as the parent of another root without reading the
/// uncommitted write set back through storage.
#[derive(Clone, Default)]
pub(crate) struct HotTrackedSnapshot {
    rows: HotRowMap,
}

impl HotTrackedSnapshot {
    #[cfg(test)]
    pub(crate) fn from_materialized_rows(
        tracked_rows: Vec<MaterializedTrackedStateRow>,
    ) -> Result<Self, LixError> {
        let mut rows = BTreeMap::new();
        for row in tracked_rows {
            let identity = HeadRowIdentity {
                schema_key: row.schema_key,
                entity_pk: row.entity_pk,
                file_id: row.file_id,
            };
            let value = HeadValueRef {
                change_id: Some(row.change_id),
                commit_id: Some(row.commit_id),
                untracked: false,
                deleted: row.deleted,
                created_at: LixTimestamp::expect_parse(
                    "hot tracked snapshot created_at",
                    &row.created_at,
                ),
                updated_at: LixTimestamp::expect_parse(
                    "hot tracked snapshot updated_at",
                    &row.updated_at,
                ),
                snapshot: row
                    .snapshot_content
                    .as_deref()
                    .map_or(JsonSlotRef::None, JsonSlotRef::Inline),
                metadata: row
                    .metadata
                    .as_deref()
                    .map_or(JsonSlotRef::None, JsonSlotRef::Inline),
                columnar_base_coordinate: None,
            };
            if rows
                .insert(identity, Bytes::from(encode_head_value(&value)?))
                .is_some()
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "tracked hot snapshot contains duplicate row identity",
                ));
            }
        }
        Ok(Self { rows })
    }
}

/// Writer for row-addressable current state.
pub(crate) struct HotStateWriter<'a, S: ?Sized> {
    pub(super) store: &'a S,
    pub(super) writes: &'a mut StorageWriteSet,
}

impl<S> HotStateWriter<'_, S>
where
    S: StorageAdapterRead + ?Sized,
{
    /// Publishes an immutable tracked root as the baseline of a new sparse
    /// branch generation. The root is already authoritative for every tracked
    /// identity at `head_commit_id`; later branch-local HOT rows shadow it.
    pub(crate) fn stage_root_current_base(
        &mut self,
        branch_id: &str,
        generation: CommitId,
        head_commit_id: CommitId,
    ) {
        self.writes.put(
            ROOT_CURRENT_BASE_SPACE,
            StorageKey(Bytes::from(hot_scope_prefix(branch_id, generation))),
            StorageValue {
                bytes: Bytes::copy_from_slice(head_commit_id.as_uuid().as_bytes()),
            },
        );
    }

    /// Publishes a transaction-certified ordered insert batch as immutable Arrow state.
    #[cfg(any(test, feature = "storage-benches"))]
    pub(crate) async fn stage_commit(
        &mut self,
        branch_id: &str,
        parent_generation: Option<CommitId>,
        new_head: CommitId,
        deltas: &[TrackedHeadDeltaRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
        parent_rows: Option<Vec<MaterializedTrackedStateRow>>,
    ) -> Result<CommitId, LixError> {
        let deltas = deltas
            .iter()
            .map(TrackedHeadDeltaRef::as_current)
            .collect::<Vec<_>>();
        let generation = self
            .stage_current_state(
                branch_id,
                parent_generation,
                new_head,
                &deltas,
                absence_guards,
                parent_rows,
                None,
            )
            .await?;
        #[cfg(test)]
        stage_test_current_control(self.writes, branch_id, new_head, generation, None)?;
        Ok(generation)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn stage_current_state(
        &mut self,
        branch_id: &str,
        parent_generation: Option<CommitId>,
        new_head: CommitId,
        deltas: &[CurrentStateDeltaRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
        parent_rows: Option<Vec<MaterializedTrackedStateRow>>,
        preserved_untracked_rows: Option<Vec<MaterializedLiveStateRow>>,
    ) -> Result<CommitId, LixError> {
        self.stage_current_state_inner(
            branch_id,
            parent_generation,
            new_head,
            deltas,
            &[],
            absence_guards,
            parent_rows,
            preserved_untracked_rows,
            false,
            None,
            None,
            &BTreeMap::new(),
        )
        .await
    }

    /// Stages deltas whose absence was already validated against the coherent
    /// transaction snapshot. The caller must publish the corresponding branch
    /// control with a compare-and-swap precondition from that same snapshot.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn stage_validated_insert_current_state(
        &mut self,
        branch_id: &str,
        parent_generation: Option<CommitId>,
        new_head: CommitId,
        deltas: &[CurrentStateDeltaRef<'_>],
        absence_guards: &[TrackedStateKeyRef<'_>],
        parent_rows: Option<Vec<MaterializedTrackedStateRow>>,
        preserved_untracked_rows: Option<Vec<MaterializedLiveStateRow>>,
        validated_absent_file_id: Option<&str>,
    ) -> Result<CommitId, LixError> {
        if parent_generation.is_none() {
            let owned_guards = absence_guards
                .iter()
                .map(|guard| TrackedStateKey {
                    schema_key: guard.schema_key.to_string(),
                    file_id: guard.file_id.map(str::to_string),
                    entity_pk: guard.entity_pk.clone(),
                })
                .collect::<BTreeSet<_>>();
            return self
                .stage_current_state_inner(
                    branch_id,
                    parent_generation,
                    new_head,
                    deltas,
                    &[],
                    &owned_guards,
                    parent_rows,
                    preserved_untracked_rows,
                    true,
                    validated_absent_file_id,
                    None,
                    &BTreeMap::new(),
                )
                .await;
        }
        let no_owned_guards = BTreeSet::new();
        self.stage_current_state_inner(
            branch_id,
            parent_generation,
            new_head,
            deltas,
            &[],
            &no_owned_guards,
            parent_rows,
            preserved_untracked_rows,
            true,
            validated_absent_file_id,
            Some(absence_guards),
            &BTreeMap::new(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn stage_current_state_inner(
        &mut self,
        branch_id: &str,
        parent_generation: Option<CommitId>,
        new_head: CommitId,
        deltas: &[CurrentStateDeltaRef<'_>],
        durable_predecessors: &[CertifiedCurrentStatePredecessorRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
        parent_rows: Option<Vec<MaterializedTrackedStateRow>>,
        preserved_untracked_rows: Option<Vec<MaterializedLiveStateRow>>,
        absence_guards_validated: bool,
        validated_absent_file_id: Option<&str>,
        borrowed_absence_guards: Option<&[TrackedStateKeyRef<'_>]>,
        certified_live_increments: &BTreeMap<(String, Option<String>), u64>,
    ) -> Result<CommitId, LixError> {
        let generation = parent_generation.unwrap_or(new_head);
        let sorted = {
            let _span = tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.materialization.hot.sort"
            )
            .entered();
            let mut sorted = deltas.iter().collect::<Vec<_>>();
            for delta in &sorted {
                delta.validate()?;
            }
            let mut already_strictly_sorted = true;
            for pair in sorted.windows(2) {
                match compare_hot_deltas(pair[0], pair[1]) {
                    Ordering::Less => {}
                    Ordering::Equal => {
                        return Err(current_state_duplicate_delta_error(pair[1]));
                    }
                    Ordering::Greater => {
                        already_strictly_sorted = false;
                        break;
                    }
                }
            }
            if !already_strictly_sorted {
                sorted.sort_unstable_by(|left, right| compare_hot_deltas(left, right));
                for pair in sorted.windows(2) {
                    if compare_hot_deltas(pair[0], pair[1]).is_eq() {
                        return Err(current_state_duplicate_delta_error(pair[1]));
                    }
                }
            }
            sorted
        };

        let durable_previous_values = {
            let mut predecessor_index = 0usize;
            let mut aligned = Vec::with_capacity(sorted.len());
            for delta in &sorted {
                let predecessor = durable_predecessors.get(predecessor_index);
                match predecessor
                    .map(|predecessor| compare_certified_predecessor_to_delta(predecessor, delta))
                {
                    Some(Ordering::Less) => {
                        return Err(head_value_error(
                            "certified predecessor does not belong to a staged delta",
                        ));
                    }
                    Some(Ordering::Equal) => {
                        let value = durable_predecessors[predecessor_index].value.view()?;
                        if value.untracked || value.deleted {
                            return Err(head_value_error(
                                "certified predecessor must be a live tracked row",
                            ));
                        }
                        aligned.push(Some(durable_predecessors[predecessor_index].value.clone()));
                        predecessor_index += 1;
                    }
                    Some(Ordering::Greater) | None => aligned.push(None),
                }
            }
            if predecessor_index != durable_predecessors.len() {
                return Err(head_value_error(
                    "certified predecessor does not belong to a staged delta",
                ));
            }
            aligned
        };

        if parent_generation.is_none() {
            if !durable_predecessors.is_empty() {
                return Err(head_value_error(
                    "bootstrap publication cannot carry durable predecessors",
                ));
            }
            stage_hot_bootstrap(
                self.writes,
                branch_id,
                generation,
                parent_rows.unwrap_or_default(),
                preserved_untracked_rows.unwrap_or_default(),
                &sorted,
                absence_guards,
            )?;
            return Ok(generation);
        }

        let identities = {
            let _span = tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.materialization.hot.identities"
            )
            .entered();
            encode_hot_mutation_identities(branch_id, generation, &sorted)
        };
        // Mutation validation must use primary rows rather than the file-id
        // projection. The projection is an equally-valued read accelerator,
        // not an ownership record.
        let loaded_previous_values = hot_load_primary_mutation_identity_refs(
            self.store,
            &identities,
            &sorted,
            &durable_previous_values,
            absence_guards_validated,
            validated_absent_file_id,
        )
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.materialization.hot.previous"
        ))
        .await?;
        let mut loaded_previous_values = loaded_previous_values.into_iter();
        let mut previous_values = sorted
            .iter()
            .zip(durable_previous_values.iter())
            .map(|(delta, durable_predecessor)| {
                if hot_delta_is_guarded_by_absent_file(
                    delta,
                    absence_guards_validated,
                    validated_absent_file_id,
                ) {
                    None
                } else if let Some(durable_predecessor) = durable_predecessor {
                    Some(durable_predecessor.clone())
                } else {
                    loaded_previous_values
                        .next()
                        .expect("every unguarded hot delta has one loaded previous value")
                }
            })
            .collect::<Vec<_>>();
        debug_assert_eq!(loaded_previous_values.len(), 0);
        let packed_previous_indices = durable_previous_values
            .iter()
            .enumerate()
            .filter_map(|(index, predecessor)| predecessor.is_none().then_some(index))
            .collect::<Vec<_>>();
        let packed_previous_keys = packed_previous_indices
            .iter()
            .map(|&index| {
                let delta = sorted[index];
                TrackedStateKeyRef {
                    schema_key: delta.schema_key,
                    entity_pk: delta.entity_pk,
                    file_id: delta.file_id,
                }
            })
            .collect::<Vec<_>>();
        let root_previous = if load_root_current_base_commit(self.store, branch_id, generation)
            .await?
            .is_some()
        {
            Box::pin(load_root_current_base_exact(
                self.store,
                branch_id,
                generation,
                &packed_previous_keys,
                ChangeRecordProjection::identity_only(),
            ))
            .await?
        } else {
            MaterializedLiveStateExactBatch::new(
                MaterializedLiveStateBatch::default(),
                vec![None; packed_previous_keys.len()],
            )?
        };
        for (index, candidate) in packed_previous_indices
            .iter()
            .copied()
            .zip((0..packed_previous_keys.len()).map(|index| root_previous.row(index)))
        {
            let Some(candidate) = candidate else {
                continue;
            };
            let candidate_is_newer = previous_values[index]
                .as_ref()
                .map(CertifiedCurrentStatePredecessor::view)
                .transpose()?
                .is_none_or(|previous| {
                    candidate.commit_id().is_some_and(|candidate| {
                        previous
                            .commit_id
                            .is_none_or(|previous| candidate > previous)
                    })
                });
            if candidate_is_newer {
                previous_values[index] = candidate.durable_predecessor().cloned();
            }
        }
        let mut collection_controls =
            load_incremental_collection_controls(self.store, branch_id, generation, &sorted)
                .await?;
        let missing_certified_scopes = certified_live_increments
            .keys()
            .filter(|scope| !collection_controls.contains_key(*scope))
            .map(
                |(schema_key, file_id)| crate::collection_generation::CollectionScopeRef {
                    schema_key,
                    file_id: file_id.as_deref(),
                },
            )
            .collect::<Vec<_>>();
        let missing_certified_controls = load_hot_collection_controls(
            self.store,
            branch_id,
            generation,
            &missing_certified_scopes,
        )
        .await?;
        collection_controls.extend(
            missing_certified_scopes
                .into_iter()
                .zip(missing_certified_controls)
                .map(|(scope, control)| {
                    (
                        (
                            scope.schema_key.to_owned(),
                            scope.file_id.map(str::to_owned),
                        ),
                        control,
                    )
                }),
        );
        // Collection-generation markers retire every older member in their
        // scope atomically. Apply them before interpreting previous row values
        // so checkpoint-expanded tombstones do not decrement the freshly reset
        // live count.
        apply_incremental_collection_generation_deltas(&mut collection_controls, &sorted)?;
        for (delta, previous) in sorted.iter().zip(&mut previous_values) {
            if delta.schema_key == crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY {
                continue;
            }
            let belongs_to_retired_generation = previous
                .as_ref()
                .map(CertifiedCurrentStatePredecessor::view)
                .transpose()?
                .is_some_and(|value| {
                    !row_belongs_to_active_collection_generation(
                        &collection_controls,
                        generation,
                        delta.schema_key,
                        delta.file_id,
                        value.commit_id,
                    )
                });
            if belongs_to_retired_generation {
                *previous = None;
            }
        }
        let mut created_ats = Vec::with_capacity(sorted.len());
        let mut retired_untracked_json_refs = BTreeSet::new();
        for (delta, previous) in sorted.iter().zip(&previous_values) {
            let Some(previous) = previous else {
                created_ats.push(delta.created_at);
                continue;
            };
            let existing = previous.view()?;
            if let Some(borrowed_absence_guards) = borrowed_absence_guards {
                reject_borrowed_guarded_live_member(borrowed_absence_guards, delta, existing)?;
            } else {
                reject_guarded_live_member(absence_guards, delta, existing)?;
            }
            reject_retention_change(delta, existing)?;
            if existing.untracked {
                collect_retired_untracked_json_refs(
                    existing,
                    delta,
                    &mut retired_untracked_json_refs,
                );
            }
            created_ats.push(existing.created_at);
        }
        let identities = encode_hot_mutation_identities(branch_id, generation, &sorted);
        let unmatched_guards = if absence_guards_validated || absence_guards.is_empty() {
            BTreeSet::new()
        } else {
            let validated_delta_keys = sorted
                .iter()
                .map(|delta| TrackedStateKey {
                    schema_key: delta.schema_key.to_string(),
                    entity_pk: delta.entity_pk.clone(),
                    file_id: delta.file_id.map(str::to_string),
                })
                .collect::<BTreeSet<_>>();
            absence_guards
                .iter()
                .filter(|key| !validated_delta_keys.contains(*key))
                .cloned()
                .collect::<BTreeSet<_>>()
        };
        reject_hot_absence_guards(self.store, branch_id, generation, &unmatched_guards).await?;

        let next_value_capacity = sorted
            .iter()
            .zip(&previous_values)
            .try_fold(0_usize, |total, (delta, previous)| {
                let inherited_coordinate = previous.as_ref().is_some_and(|previous| {
                    previous
                        .view()
                        .expect("HOT predecessor was validated before capacity planning")
                        .columnar_base_coordinate
                        .is_some()
                });
                checked_add_hot_next_value_capacity(total, delta, inherited_coordinate)
            })
            // Preserve the encoder's fallible behavior for impossible input:
            // overflow must not turn into an attempted `usize::MAX`
            // allocation. The row encoder will report its normal length
            // error while this arena falls back to ordinary growth.
            .unwrap_or(0);
        let mut next_value_ranges = Vec::with_capacity(sorted.len());
        let mut next_value_bytes = Vec::with_capacity(next_value_capacity);
        {
            let _span = tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.materialization.hot.values"
            )
            .entered();
            for (delta, (created_at, previous)) in
                sorted.iter().zip(created_ats.iter().zip(&previous_values))
            {
                next_value_ranges.push(if delta.physically_deletes() {
                    None
                } else {
                    let mut value = delta.value_ref(*created_at);
                    value.columnar_base_coordinate =
                        next_columnar_base_coordinate(delta, previous.as_ref())?;
                    Some(append_head_value(&mut next_value_bytes, &value)?)
                });
            }
        }
        let next_value_bytes = Bytes::from(next_value_bytes);
        stage_incremental_collection_controls(
            self.writes,
            branch_id,
            generation,
            &sorted,
            &previous_values,
            collection_controls,
            certified_live_increments,
        )?;

        async {
            stage_hot_mutation_batch(self.writes, identities, next_value_bytes, next_value_ranges);
            stage_incremental_file_delete_cascades(
                self.store,
                self.writes,
                branch_id,
                generation,
                &sorted,
                &mut retired_untracked_json_refs,
            )
            .await
        }
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.materialization.hot.stage"
        ))
        .await?;
        JsonStoreWriter::stage_untracked_reclaim_candidates(
            self.writes,
            retired_untracked_json_refs
                .into_iter()
                .map(JsonRef::from_hash_bytes),
        );
        Ok(generation)
    }

    /// Publishes a complete replacement generation for a lifecycle event.
    ///
    /// The supplied snapshot is the target commit's tracked portion.  Any
    /// branch-local untracked rows are copied from the previous generation,
    /// then this transaction's untracked mutations are applied before its
    /// tracked mutations.  That order admits the one legitimate mixed case:
    /// deleting an untracked row and selecting a tracked row with the same
    /// identity in the same atomic publication.  Every other retention
    /// collision fails before the new control can become visible.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn stage_complete_current_state(
        &mut self,
        branch_id: &str,
        generation: CommitId,
        parent_tracked: HotTrackedSnapshot,
        preserved_untracked_generation: Option<CommitId>,
        tracked_deltas: &[CurrentStateDeltaRef<'_>],
        untracked_deltas: &[CurrentStateDeltaRef<'_>],
        absence_guards: &BTreeSet<TrackedStateKey>,
    ) -> Result<(HotTrackedSnapshot, BTreeSet<String>), LixError> {
        let mut rows = parent_tracked.rows;
        let mut untracked_rows = match preserved_untracked_generation {
            Some(previous_generation) => {
                load_hot_untracked_generation(self.store, branch_id, previous_generation).await?
            }
            None => BTreeMap::new(),
        };

        let sorted_untracked = sorted_lifecycle_hot_deltas(untracked_deltas, true)?;
        let sorted_tracked = sorted_lifecycle_hot_deltas(tracked_deltas, false)?;
        reject_lifecycle_retention_collisions(&sorted_untracked, &sorted_tracked)?;

        let mut retired_untracked_json_refs = BTreeSet::new();
        for delta in &sorted_untracked {
            apply_complete_hot_snapshot_delta(
                &mut untracked_rows,
                delta,
                absence_guards,
                &mut retired_untracked_json_refs,
            )?;
        }
        merge_final_untracked_rows(&mut rows, untracked_rows)?;
        for delta in &sorted_tracked {
            apply_complete_hot_snapshot_delta(
                &mut rows,
                delta,
                absence_guards,
                &mut retired_untracked_json_refs,
            )?;
        }

        let mut final_tracked = BTreeMap::new();
        let mut schema_keys = BTreeSet::new();
        for (identity, bytes) in &rows {
            schema_keys.insert(identity.schema_key.clone());
            if !decode_head_value(bytes.as_ref())?.untracked {
                final_tracked.insert(identity.clone(), bytes.clone());
            }
        }

        stage_complete_collection_controls(self.writes, branch_id, generation, &rows)?;
        stage_complete_hot_rows(self.writes, branch_id, generation, rows);
        JsonStoreWriter::stage_untracked_reclaim_candidates(
            self.writes,
            retired_untracked_json_refs
                .into_iter()
                .map(JsonRef::from_hash_bytes),
        );
        Ok((
            HotTrackedSnapshot {
                rows: final_tracked,
            },
            schema_keys,
        ))
    }
}

async fn stage_incremental_file_delete_cascades(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    branch_id: &str,
    generation: CommitId,
    deltas: &[&CurrentStateDeltaRef<'_>],
    retired_untracked_json_refs: &mut BTreeSet<[u8; JSON_REF_BYTES]>,
) -> Result<(), LixError> {
    let mut cascades = BTreeMap::<String, &CurrentStateDeltaRef<'_>>::new();
    for cascade in deltas {
        let Some(file_id) = file_delete_cascade_id(cascade)? else {
            continue;
        };
        cascades.insert(file_id, cascade);
    }
    if cascades.is_empty() {
        return Ok(());
    }
    #[cfg(test)]
    INCREMENTAL_CASCADE_EXPLICIT_INDEX_BUILDS.with(|builds| {
        builds.set(builds.get().saturating_add(1));
    });
    let explicit = deltas
        .iter()
        .map(|delta| HeadRowIdentity {
            schema_key: delta.schema_key.to_string(),
            entity_pk: delta.entity_pk.clone(),
            file_id: delta.file_id.map(str::to_string),
        })
        .collect::<BTreeSet<_>>();
    let identities =
        hot_load_file_scope_identities(store, branch_id, generation, &cascades).await?;
    let values = hot_load_primary_identity_bytes(store, &identities).await?;
    let scope = hot_scope_prefix(branch_id, generation);
    let key_capacity = identities
        .iter()
        .try_fold(0_usize, |total, identity| {
            let key_len = encoded_hot_identity_key_len(
                scope.len(),
                &identity.schema_key,
                &identity.entity_pk,
                identity.file_id.as_deref(),
            )?;
            total.checked_add(key_len)
        })
        .unwrap_or(0);
    let mut mutations = HotCascadeMutationBuffers::with_capacity(identities.len(), key_capacity);
    for (identity, previous) in identities.into_iter().zip(values) {
        let row_identity = identity.clone().into_row_identity();
        if explicit.contains(&row_identity) {
            continue;
        }
        let _cascade = cascades
            .get(
                identity
                    .file_id
                    .as_deref()
                    .expect("file-backed identity requires file id"),
            )
            .expect("file scan only returns requested cascade ids");
        let Some(previous) = previous else {
            return Err(head_value_error(
                "hot file-backed identity has no authoritative primary row",
            ));
        };
        let existing = decode_head_value(&previous)?;
        if !existing.untracked || existing.deleted {
            continue;
        }
        let row_start = mutations.key_bytes.len();
        mutations.key_bytes.extend_from_slice(&scope);
        write_key_string(
            &mut mutations.key_bytes,
            &identity.schema_key,
            KEY_PART_FINAL,
        );
        write_file_id(&mut mutations.key_bytes, identity.file_id.as_deref());
        write_entity_pk(&mut mutations.key_bytes, &identity.entity_pk);
        let row_key = BufferRange::new(row_start, mutations.key_bytes.len() - row_start);
        collect_hot_untracked_refs(existing, retired_untracked_json_refs);
        mutations.row_deletes.push(row_key);
    }
    if !mutations.row_puts.is_empty() || !mutations.row_deletes.is_empty() {
        stage_hot_encoded_mutation_ranges(
            writes,
            Bytes::from(mutations.key_bytes),
            Bytes::from(mutations.value_bytes),
            mutations.row_puts,
            mutations.row_deletes,
            Vec::new(),
        );
    }
    Ok(())
}

struct HotCascadeMutationBuffers {
    key_bytes: Vec<u8>,
    value_bytes: Vec<u8>,
    row_puts: Vec<EncodedPut>,
    row_deletes: Vec<BufferRange>,
}

impl HotCascadeMutationBuffers {
    fn with_capacity(row_capacity: usize, key_capacity: usize) -> Self {
        let value_bytes_per_row =
            HEAD_VALUE_HEADER_BYTES.checked_add(COLUMNAR_BASE_COORDINATE_BYTES);
        let value_capacity = value_bytes_per_row
            .and_then(|value_bytes| row_capacity.checked_mul(value_bytes))
            .unwrap_or(0);
        Self {
            key_bytes: Vec::with_capacity(key_capacity),
            value_bytes: Vec::with_capacity(value_capacity),
            row_puts: Vec::with_capacity(row_capacity),
            row_deletes: Vec::with_capacity(row_capacity),
        }
    }
}

#[cfg(test)]
std::thread_local! {
    static INCREMENTAL_CASCADE_EXPLICIT_INDEX_BUILDS: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn incremental_cascade_explicit_index_builds() -> usize {
    INCREMENTAL_CASCADE_EXPLICIT_INDEX_BUILDS.with(std::cell::Cell::get)
}

fn next_columnar_base_coordinate(
    delta: &CurrentStateDeltaRef<'_>,
    previous: Option<&CertifiedCurrentStatePredecessor>,
) -> Result<Option<ColumnarBaseCoordinate>, LixError> {
    Ok(delta.columnar_base_coordinate.or(previous
        .map(CertifiedCurrentStatePredecessor::view)
        .transpose()?
        .and_then(|value| value.columnar_base_coordinate)))
}

async fn load_hot_untracked_generation(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
) -> Result<HotRowMap, LixError> {
    let filter = TrackedStateFilter {
        include_tombstones: true,
        ..TrackedStateFilter::default()
    };
    let HotScanEntries::Decoded(entries) =
        hot_scan_entries(store, branch_id, generation, &filter, None, None)
            .await?
            .expect("unbounded HOT scan cannot exhaust a byte budget")
    else {
        unreachable!("an unconstrained HOT scan cannot select the finite point-read route");
    };
    let mut rows = BTreeMap::new();
    for (identity, bytes) in entries {
        let value = decode_head_value(bytes.as_ref())?;
        if !value.untracked {
            continue;
        }
        if value.deleted {
            return Err(head_value_error(
                "untracked hot row must be physically removed rather than tombstoned",
            ));
        }
        match rows.entry(identity.into_row_identity()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(bytes);
            }
            std::collections::btree_map::Entry::Occupied(entry) => {
                let identity = entry.key();
                return Err(LixError::new(
                    LixError::CODE_UNIQUE,
                    format!(
                        "hot generation contains duplicate untracked identity in schema '{}' entity_pk {:?}",
                        identity.schema_key, identity.entity_pk
                    ),
                ));
            }
        }
    }
    Ok(rows)
}

fn merge_final_untracked_rows(
    rows: &mut HotRowMap,
    untracked_rows: HotRowMap,
) -> Result<(), LixError> {
    for (identity, bytes) in untracked_rows {
        match rows.entry(identity) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(bytes);
            }
            std::collections::btree_map::Entry::Occupied(entry) => {
                let identity = entry.key();
                return Err(LixError::new(
                    LixError::CODE_UNIQUE,
                    format!(
                        "cannot materialize tracked and untracked hot rows with the same identity in schema '{}' entity_pk {:?}",
                        identity.schema_key, identity.entity_pk
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn sorted_lifecycle_hot_deltas<'a>(
    deltas: &'a [CurrentStateDeltaRef<'a>],
    expect_untracked: bool,
) -> Result<Vec<&'a CurrentStateDeltaRef<'a>>, LixError> {
    let mut sorted = Vec::with_capacity(deltas.len());
    for delta in deltas {
        delta.validate()?;
        if delta.untracked != expect_untracked {
            return Err(head_value_error(if expect_untracked {
                "untracked lifecycle delta was marked tracked"
            } else {
                "tracked lifecycle delta was marked untracked"
            }));
        }
        sorted.push(delta);
    }
    sorted.sort_unstable_by(|left, right| compare_hot_deltas(left, right));
    for pair in sorted.windows(2) {
        if compare_hot_deltas(pair[0], pair[1]).is_eq() {
            return Err(current_state_duplicate_delta_error(pair[1]));
        }
    }
    Ok(sorted)
}

fn reject_lifecycle_retention_collisions(
    untracked: &[&CurrentStateDeltaRef<'_>],
    tracked: &[&CurrentStateDeltaRef<'_>],
) -> Result<(), LixError> {
    let mut untracked_index = 0;
    let mut tracked_index = 0;
    while untracked_index < untracked.len() && tracked_index < tracked.len() {
        match compare_hot_deltas(untracked[untracked_index], tracked[tracked_index]) {
            Ordering::Less => untracked_index += 1,
            Ordering::Greater => tracked_index += 1,
            Ordering::Equal => {
                if !untracked[untracked_index].physically_deletes() {
                    return Err(current_state_duplicate_delta_error(tracked[tracked_index]));
                }
                untracked_index += 1;
                tracked_index += 1;
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_complete_hot_snapshot_delta(
    rows: &mut HotRowMap,
    delta: &CurrentStateDeltaRef<'_>,
    absence_guards: &BTreeSet<TrackedStateKey>,
    retired_untracked_json_refs: &mut BTreeSet<[u8; JSON_REF_BYTES]>,
) -> Result<(), LixError> {
    apply_complete_file_delete_cascade(rows, delta, retired_untracked_json_refs)?;
    let identity = HeadRowIdentity {
        schema_key: delta.schema_key.to_string(),
        entity_pk: delta.entity_pk.clone(),
        file_id: delta.file_id.map(str::to_string),
    };
    let previous = rows.get(&identity).map(|bytes| bytes.as_ref());
    if let Some(previous) = previous {
        let existing = decode_head_value(previous)?;
        reject_guarded_live_member(absence_guards, delta, existing)?;
        reject_retention_change(delta, existing)?;
        if existing.untracked {
            collect_retired_untracked_json_refs(existing, delta, retired_untracked_json_refs);
        }
    }
    if delta.physically_deletes() {
        rows.remove(&identity);
    } else {
        let created_at = previous
            .map(decode_head_value)
            .transpose()?
            .map_or(delta.created_at, |value| value.created_at);
        rows.insert(
            identity,
            Bytes::from(encode_head_value(&{
                let mut value = delta.value_ref(created_at);
                value.columnar_base_coordinate = None;
                value
            })?),
        );
    }
    Ok(())
}

fn apply_complete_file_delete_cascade(
    rows: &mut HotRowMap,
    delta: &CurrentStateDeltaRef<'_>,
    retired_untracked_json_refs: &mut BTreeSet<[u8; JSON_REF_BYTES]>,
) -> Result<(), LixError> {
    let Some(file_id) = file_delete_cascade_id(delta)? else {
        return Ok(());
    };
    let identities = rows
        .keys()
        .filter(|identity| identity.file_id.as_deref() == Some(file_id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    for identity in identities {
        let Some(previous) = rows.get(&identity) else {
            continue;
        };
        let existing = decode_head_value(previous.as_ref())?;
        if (delta.untracked && !existing.untracked) || existing.deleted {
            continue;
        }
        if existing.untracked {
            collect_hot_untracked_refs(existing, retired_untracked_json_refs);
            rows.remove(&identity);
            continue;
        }
        rows.insert(
            identity,
            Bytes::from(encode_head_value(&HeadValueRef {
                change_id: delta.change_id,
                commit_id: delta.commit_id,
                untracked: false,
                deleted: true,
                created_at: existing.created_at,
                updated_at: delta.updated_at,
                snapshot: JsonSlotRef::None,
                metadata: JsonSlotRef::None,
                columnar_base_coordinate: existing.columnar_base_coordinate,
            })?),
        );
    }
    Ok(())
}

fn file_delete_cascade_id(delta: &CurrentStateDeltaRef<'_>) -> Result<Option<String>, LixError> {
    if delta.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY || !delta.deleted {
        return Ok(None);
    }
    delta
        .entity_pk
        .as_single_string_owned()
        .map(Some)
        .map_err(|error| {
            head_value_error(&format!(
                "file descriptor tombstone has invalid identity: {error}"
            ))
        })
}

fn compare_hot_deltas(
    left: &CurrentStateDeltaRef<'_>,
    right: &CurrentStateDeltaRef<'_>,
) -> Ordering {
    left.schema_key
        .cmp(right.schema_key)
        .then_with(|| left.entity_pk.cmp(right.entity_pk))
        .then_with(|| left.file_id.cmp(&right.file_id))
}

fn compare_certified_predecessor_to_delta(
    predecessor: &CertifiedCurrentStatePredecessorRef<'_>,
    delta: &CurrentStateDeltaRef<'_>,
) -> Ordering {
    predecessor
        .schema_key
        .cmp(delta.schema_key)
        .then_with(|| predecessor.entity_pk.cmp(delta.entity_pk))
        .then_with(|| predecessor.file_id.cmp(&delta.file_id))
}

fn hot_identity(
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
    entity_pk: &EntityPk,
    file_id: Option<&str>,
) -> HeadIdentity {
    HeadIdentity {
        branch_id: branch_id.to_string(),
        generation,
        schema_key: schema_key.to_string(),
        entity_pk: entity_pk.clone(),
        file_id: file_id.map(str::to_string),
    }
}

struct EncodedHotMutationIdentities {
    key_bytes: Bytes,
    key_ranges: Vec<EncodedHotMutationIdentityRanges>,
}

#[derive(Clone, Copy)]
struct EncodedHotMutationIdentityRanges {
    row_key: BufferRange,
    file_schema_key: Option<BufferRange>,
}

fn encode_hot_mutation_identities(
    branch_id: &str,
    generation: CommitId,
    deltas: &[&CurrentStateDeltaRef<'_>],
) -> EncodedHotMutationIdentities {
    let scope = hot_scope_prefix(branch_id, generation);
    let encoded_capacity = encoded_hot_mutation_identity_capacity(scope.len(), deltas).unwrap_or(0);
    let mut encoded = Vec::with_capacity(encoded_capacity);
    let mut key_ranges = Vec::with_capacity(deltas.len());
    for delta in deltas {
        key_ranges.push(append_hot_mutation_identity(&mut encoded, &scope, delta));
    }
    EncodedHotMutationIdentities {
        key_bytes: Bytes::from(encoded),
        key_ranges,
    }
}

fn encoded_hot_mutation_identity_capacity(
    scope_len: usize,
    deltas: &[&CurrentStateDeltaRef<'_>],
) -> Option<usize> {
    deltas.iter().try_fold(0_usize, |total, delta| {
        let key_len = encoded_hot_identity_key_len(
            scope_len,
            delta.schema_key,
            delta.entity_pk,
            delta.file_id,
        )?;
        let marker_len = if delta.file_id.is_some() {
            scope_len.checked_add(encoded_key_bytes_len(delta.schema_key.as_bytes())?)?
        } else {
            0
        };
        total.checked_add(key_len)?.checked_add(marker_len)
    })
}

fn append_hot_mutation_identity(
    encoded: &mut Vec<u8>,
    scope: &[u8],
    delta: &CurrentStateDeltaRef<'_>,
) -> EncodedHotMutationIdentityRanges {
    let row_start = encoded.len();
    encoded.extend_from_slice(scope);
    write_key_string(encoded, delta.schema_key, KEY_PART_FINAL);
    write_file_id(encoded, delta.file_id);
    write_entity_pk(encoded, delta.entity_pk);
    let row_key = BufferRange::new(row_start, encoded.len() - row_start);

    let file_schema_key = delta.file_id.map(|_| {
        let marker_start = encoded.len();
        encoded.extend_from_slice(scope);
        write_key_string(encoded, delta.schema_key, KEY_PART_FINAL);
        BufferRange::new(marker_start, encoded.len() - marker_start)
    });
    EncodedHotMutationIdentityRanges {
        row_key,
        file_schema_key,
    }
}

async fn hot_load_primary_mutation_identity_refs(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: &EncodedHotMutationIdentities,
    deltas: &[&CurrentStateDeltaRef<'_>],
    durable_predecessors: &[Option<CertifiedCurrentStatePredecessor>],
    absence_guards_validated: bool,
    validated_absent_file_id: Option<&str>,
) -> Result<Vec<Option<CertifiedCurrentStatePredecessor>>, LixError> {
    assert_eq!(
        identities.key_ranges.len(),
        deltas.len(),
        "every hot mutation identity must have one source delta"
    );
    assert_eq!(
        durable_predecessors.len(),
        deltas.len(),
        "every hot mutation identity must have one predecessor slot"
    );
    let read_count = deltas
        .iter()
        .zip(durable_predecessors)
        .filter(|(delta, durable_predecessor)| {
            durable_predecessor.is_none()
                && !hot_delta_is_guarded_by_absent_file(
                    delta,
                    absence_guards_validated,
                    validated_absent_file_id,
                )
        })
        .count();
    if read_count == 0 {
        return Ok(Vec::new());
    }
    if read_count == deltas.len()
        && let Some(values) = hot_scan_dense_mutation_identity_range(store, identities).await?
    {
        return Ok(values
            .into_iter()
            .map(|value| value.map(CertifiedCurrentStatePredecessor::Encoded))
            .collect());
    }
    let mut keys = Vec::with_capacity(read_count);
    for ((identity, delta), durable_predecessor) in identities
        .key_ranges
        .iter()
        .zip(deltas)
        .zip(durable_predecessors)
    {
        if durable_predecessor.is_some()
            || hot_delta_is_guarded_by_absent_file(
                delta,
                absence_guards_validated,
                validated_absent_file_id,
            )
        {
            continue;
        }
        let start = identity.row_key.offset();
        keys.push(StorageKey(
            identities
                .key_bytes
                .slice(start..start + identity.row_key.len()),
        ));
    }
    PointReadPlan::new(HOT_ROW_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .map(|value| {
            value
                .map(full_value_bytes)
                .transpose()
                .map(|value| value.map(CertifiedCurrentStatePredecessor::Encoded))
        })
        .collect()
}

async fn hot_scan_dense_mutation_identity_range(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: &EncodedHotMutationIdentities,
) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
    hot_scan_dense_encoded_key_range(store, identities.key_ranges.len(), |index| {
        let range = identities.key_ranges[index].row_key;
        let start = range.offset();
        &identities.key_bytes[start..start.saturating_add(range.len())]
    })
    .await
}

fn hot_delta_is_guarded_by_absent_file(
    delta: &CurrentStateDeltaRef<'_>,
    absence_guards_validated: bool,
    validated_absent_file_id: Option<&str>,
) -> bool {
    absence_guards_validated
        && validated_absent_file_id.is_some_and(|file_id| delta.file_id == Some(file_id))
}

/// Adds one encoded current-state value to the shared arena's capacity plan.
///
/// Physical untracked deletes produce no value. Every operation is checked so an impossible
/// batch can safely use a zero-capacity growth fallback and reach the normal
/// fallible encoder instead of attempting an overflowing allocation.
fn checked_add_hot_next_value_capacity(
    total: usize,
    delta: &CurrentStateDeltaRef<'_>,
    inherited_coordinate: bool,
) -> Option<usize> {
    if delta.physically_deletes() {
        return Some(total);
    }
    let (snapshot_len, metadata_len) = if delta.deleted {
        (0, 0)
    } else {
        (
            encoded_hot_slot_len(delta.snapshot),
            encoded_hot_slot_len(delta.metadata),
        )
    };
    // Keep the plan bounded by the same on-disk u32 fields the encoder checks.
    u32::try_from(snapshot_len).ok()?;
    u32::try_from(metadata_len).ok()?;
    let encoded_len = HEAD_VALUE_HEADER_BYTES
        .checked_add(snapshot_len)?
        .checked_add(metadata_len)?
        .checked_add(
            (delta.columnar_base_coordinate.is_some() || inherited_coordinate)
                .then_some(COLUMNAR_BASE_COORDINATE_BYTES)
                .unwrap_or(0),
        )?;
    total.checked_add(encoded_len)
}

fn encoded_hot_slot_len(slot: JsonSlotRef<'_>) -> usize {
    match slot {
        JsonSlotRef::None => 0,
        JsonSlotRef::Ref(_) => JSON_REF_BYTES,
        JsonSlotRef::Inline(json) => json.len(),
    }
}

fn stage_hot_mutation_batch(
    writes: &mut StorageWriteSet,
    identities: EncodedHotMutationIdentities,
    value_bytes: Bytes,
    value_ranges: Vec<Option<Range<usize>>>,
) {
    assert_eq!(
        identities.key_ranges.len(),
        value_ranges.len(),
        "every hot mutation identity must have one staged value"
    );
    let put_count = value_ranges.iter().flatten().count();
    let delete_count = value_ranges.len() - put_count;
    let file_count = identities
        .key_ranges
        .iter()
        .filter(|identity| identity.file_schema_key.is_some())
        .count();
    let mut row_puts = Vec::with_capacity(put_count);
    let mut row_deletes = Vec::with_capacity(delete_count);
    let mut file_schema_puts = Vec::with_capacity(file_count);
    for (identity, value) in identities.key_ranges.iter().zip(&value_ranges) {
        if let Some(value) = value {
            let value = buffer_range(value);
            row_puts.push(EncodedPut {
                key: identity.row_key,
                value,
            });
        } else {
            row_deletes.push(identity.row_key);
        }
        if let Some(key) = identity.file_schema_key {
            file_schema_puts.push(key);
        }
    }
    file_schema_puts.sort_unstable_by(|left, right| {
        let left = &identities.key_bytes[left.offset()..left.offset().saturating_add(left.len())];
        let right =
            &identities.key_bytes[right.offset()..right.offset().saturating_add(right.len())];
        left.cmp(right)
    });
    file_schema_puts.dedup_by(|left, right| {
        identities.key_bytes[left.offset()..left.offset().saturating_add(left.len())]
            == identities.key_bytes[right.offset()..right.offset().saturating_add(right.len())]
    });

    stage_hot_encoded_mutation_ranges(
        writes,
        identities.key_bytes,
        value_bytes,
        row_puts,
        row_deletes,
        file_schema_puts,
    );
}

fn stage_hot_encoded_mutation_ranges(
    writes: &mut StorageWriteSet,
    key_bytes: Bytes,
    value_bytes: Bytes,
    row_puts: Vec<EncodedPut>,
    row_deletes: Vec<BufferRange>,
    mut file_schema_puts: Vec<BufferRange>,
) {
    let row_batch = EncodedMutationBatch::try_new(
        key_bytes.clone(),
        value_bytes.clone(),
        row_puts,
        row_deletes,
    )
    .expect("hot row ranges originate in the supplied encoded buffers");
    writes.stage_encoded_batch(HOT_ROW_SPACE, row_batch);
    file_schema_puts.retain(|key| {
        !writes.contains_put(
            HOT_FILE_SPACE,
            &key_bytes[key.offset()..key.offset().saturating_add(key.len())],
        )
    });
    if !file_schema_puts.is_empty() {
        let file_puts = file_schema_puts
            .into_iter()
            .map(|key| EncodedPut {
                key,
                value: BufferRange::new(0, 0),
            })
            .collect();
        let file_batch =
            EncodedMutationBatch::try_new(key_bytes, Bytes::new(), file_puts, Vec::new())
                .expect("hot file schema ranges originate in the supplied encoded buffers");
        writes.stage_encoded_batch(HOT_FILE_SPACE, file_batch);
    }
}

fn buffer_range(range: &Range<usize>) -> BufferRange {
    BufferRange::new(range.start, range.end - range.start)
}

fn stage_complete_hot_rows(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    generation: CommitId,
    rows: HotRowMap,
) {
    if rows.is_empty() {
        return;
    }
    let scope = hot_scope_prefix(branch_id, generation);
    let file_schema_keys = rows
        .keys()
        .filter(|identity| identity.file_id.is_some())
        .map(|identity| identity.schema_key.clone())
        .collect::<BTreeSet<_>>();
    let value_capacity = rows.values().map(Bytes::len).sum();
    let marker_key_capacity = file_schema_keys
        .iter()
        .map(|schema_key| {
            scope
                .len()
                .saturating_add(encoded_key_bytes_len(schema_key.as_bytes()).unwrap_or(0))
        })
        .sum::<usize>();
    let key_capacity = rows
        .len()
        .saturating_mul(scope.len() + 32)
        .saturating_add(
            rows.keys()
                .map(|identity| {
                    identity
                        .schema_key
                        .len()
                        .saturating_add(identity.entity_pk.estimated_heap_bytes())
                        .saturating_add(
                            identity
                                .file_id
                                .as_ref()
                                .map_or(0, |file_id| file_id.len().saturating_mul(2)),
                        )
                })
                .sum(),
        )
        .saturating_add(marker_key_capacity);
    let mut key_bytes = Vec::with_capacity(key_capacity);
    let mut value_bytes = Vec::with_capacity(value_capacity);
    let mut row_puts = Vec::with_capacity(rows.len());
    let mut file_puts = Vec::with_capacity(file_schema_keys.len());
    for (identity, value) in rows {
        let value_start = value_bytes.len();
        value_bytes.extend_from_slice(value.as_ref());
        let value = BufferRange::new(value_start, value_bytes.len() - value_start);

        let row_start = key_bytes.len();
        key_bytes.extend_from_slice(&scope);
        write_key_string(&mut key_bytes, &identity.schema_key, KEY_PART_FINAL);
        write_file_id(&mut key_bytes, identity.file_id.as_deref());
        write_entity_pk(&mut key_bytes, &identity.entity_pk);
        row_puts.push(EncodedPut {
            key: BufferRange::new(row_start, key_bytes.len() - row_start),
            value,
        });
    }
    for schema_key in file_schema_keys {
        let file_start = key_bytes.len();
        key_bytes.extend_from_slice(&scope);
        write_key_string(&mut key_bytes, &schema_key, KEY_PART_FINAL);
        file_puts.push(EncodedPut {
            key: BufferRange::new(file_start, key_bytes.len() - file_start),
            value: BufferRange::new(0, 0),
        });
    }
    let key_bytes = Bytes::from(key_bytes);
    let value_bytes = Bytes::from(value_bytes);
    let row_batch =
        EncodedMutationBatch::try_new(key_bytes.clone(), value_bytes.clone(), row_puts, Vec::new())
            .expect("complete hot row ranges originate in the supplied encoded buffers");
    writes.stage_encoded_batch(HOT_ROW_SPACE, row_batch);
    file_puts.retain(|put| {
        !writes.contains_put(
            HOT_FILE_SPACE,
            &key_bytes[put.key.offset()..put.key.offset().saturating_add(put.key.len())],
        )
    });
    if !file_puts.is_empty() {
        let file_batch =
            EncodedMutationBatch::try_new(key_bytes, Bytes::new(), file_puts, Vec::new())
                .expect("complete hot file ranges originate in the supplied encoded buffers");
        writes.stage_encoded_batch(HOT_FILE_SPACE, file_batch);
    }
}

#[cfg(test)]
pub(super) fn stage_test_hot_value(
    writes: &mut StorageWriteSet,
    identity: &HeadIdentity,
    value: &HeadValue,
) -> Result<(), LixError> {
    let rows = BTreeMap::from([(
        HeadRowIdentity {
            schema_key: identity.schema_key.clone(),
            entity_pk: identity.entity_pk.clone(),
            file_id: identity.file_id.clone(),
        },
        Bytes::from(encode_head_value(&value.as_ref())?),
    )]);
    stage_complete_hot_rows(writes, &identity.branch_id, identity.generation, rows);
    Ok(())
}

fn stage_hot_bootstrap(
    writes: &mut StorageWriteSet,
    branch_id: &str,
    generation: CommitId,
    parent_rows: Vec<MaterializedTrackedStateRow>,
    preserved_untracked_rows: Vec<MaterializedLiveStateRow>,
    deltas: &[&CurrentStateDeltaRef<'_>],
    absence_guards: &BTreeSet<TrackedStateKey>,
) -> Result<(), LixError> {
    let mut rows = HotRowMap::new();
    for row in parent_rows {
        let key = TrackedStateKey {
            schema_key: row.schema_key.clone(),
            entity_pk: row.entity_pk.clone(),
            file_id: row.file_id.clone(),
        };
        if absence_guards.contains(&key) && !row.deleted {
            return Err(tracked_head_duplicate_insert_error(&key));
        }
        let identity = HeadRowIdentity {
            schema_key: row.schema_key,
            entity_pk: row.entity_pk,
            file_id: row.file_id,
        };
        let value = HeadValueRef {
            change_id: Some(row.change_id),
            commit_id: Some(row.commit_id),
            untracked: false,
            deleted: row.deleted,
            created_at: LixTimestamp::expect_parse("hot bootstrap created_at", &row.created_at),
            updated_at: LixTimestamp::expect_parse("hot bootstrap updated_at", &row.updated_at),
            snapshot: row
                .snapshot_content
                .as_deref()
                .map_or(JsonSlotRef::None, JsonSlotRef::Inline),
            metadata: row
                .metadata
                .as_deref()
                .map_or(JsonSlotRef::None, JsonSlotRef::Inline),
            columnar_base_coordinate: None,
        };
        if rows
            .insert(identity, Bytes::from(encode_head_value(&value)?))
            .is_some()
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "hot bootstrap contains duplicate tracked row identity",
            ));
        }
    }
    for row in preserved_untracked_rows {
        if !row.untracked || row.deleted {
            return Err(head_value_error(
                "hot bootstrap preserved state must contain only live untracked rows",
            ));
        }
        let key = TrackedStateKey {
            schema_key: row.schema_key.clone(),
            entity_pk: row.entity_pk.clone(),
            file_id: row.file_id.clone(),
        };
        if absence_guards.contains(&key) {
            return Err(tracked_head_duplicate_insert_error(&key));
        }
        let identity = HeadRowIdentity {
            schema_key: row.schema_key,
            entity_pk: row.entity_pk,
            file_id: row.file_id,
        };
        let value = HeadValueRef {
            change_id: None,
            commit_id: None,
            untracked: true,
            deleted: false,
            created_at: row.created_at,
            updated_at: row.updated_at,
            snapshot: row
                .snapshot_content
                .as_deref()
                .map_or(JsonSlotRef::None, JsonSlotRef::Inline),
            metadata: row
                .metadata
                .as_deref()
                .map_or(JsonSlotRef::None, JsonSlotRef::Inline),
            columnar_base_coordinate: None,
        };
        if rows
            .insert(identity, Bytes::from(encode_head_value(&value)?))
            .is_some()
        {
            return Err(LixError::new(
                LixError::CODE_UNIQUE,
                "cannot materialize tracked and untracked hot rows with the same identity",
            ));
        }
    }
    let mut retired_untracked_json_refs = BTreeSet::new();
    for delta in deltas {
        apply_complete_file_delete_cascade(&mut rows, delta, &mut retired_untracked_json_refs)?;
        let identity = HeadRowIdentity {
            schema_key: delta.schema_key.to_string(),
            entity_pk: delta.entity_pk.clone(),
            file_id: delta.file_id.map(str::to_string),
        };
        let previous = rows.get(&identity).map(|bytes| bytes.as_ref());
        if let Some(previous) = previous {
            let existing = decode_head_value(previous)?;
            reject_guarded_live_member(absence_guards, delta, existing)?;
            reject_retention_change(delta, existing)?;
            if existing.untracked {
                collect_retired_untracked_json_refs(
                    existing,
                    delta,
                    &mut retired_untracked_json_refs,
                );
            }
        }
        if delta.physically_deletes() {
            rows.remove(&identity);
        } else {
            let created_at = previous
                .map(decode_head_value)
                .transpose()?
                .map_or(delta.created_at, |value| value.created_at);
            rows.insert(
                identity,
                Bytes::from(encode_head_value(&{
                    let mut value = delta.value_ref(created_at);
                    value.columnar_base_coordinate = None;
                    value
                })?),
            );
        }
    }
    stage_complete_collection_controls(writes, branch_id, generation, &rows)?;
    stage_complete_hot_rows(writes, branch_id, generation, rows);
    JsonStoreWriter::stage_untracked_reclaim_candidates(
        writes,
        retired_untracked_json_refs
            .into_iter()
            .map(JsonRef::from_hash_bytes),
    );
    Ok(())
}

async fn reject_hot_absence_guards(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    guards: &BTreeSet<TrackedStateKey>,
) -> Result<(), LixError> {
    if guards.is_empty() {
        return Ok(());
    }
    let identities = guards
        .iter()
        .map(|key| {
            hot_identity(
                branch_id,
                generation,
                &key.schema_key,
                &key.entity_pk,
                key.file_id.as_deref(),
            )
        })
        .collect::<Vec<_>>();
    for (identity, value) in identities
        .iter()
        .zip(hot_load_primary_identity_bytes(store, &identities).await?)
    {
        let Some(value) = value else {
            continue;
        };
        let value = decode_head_value(&value)?;
        if !value.deleted {
            return Err(tracked_head_duplicate_insert_error(&TrackedStateKey {
                schema_key: identity.schema_key.clone(),
                entity_pk: identity.entity_pk.clone(),
                file_id: identity.file_id.clone(),
            }));
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct EncodedHotPointKeyRanges {
    primary: BufferRange,
}

struct EncodedHotPointKeys {
    bytes: Bytes,
    ranges: Vec<EncodedHotPointKeyRanges>,
}

impl EncodedHotPointKeys {
    fn primary_key(&self, index: usize) -> StorageKey {
        self.key_for_range(self.ranges[index].primary)
    }

    fn primary_key_bytes(&self, index: usize) -> &[u8] {
        let range = self.ranges[index].primary;
        &self.bytes[range.offset()..range.offset() + range.len()]
    }

    fn key_for_range(&self, range: BufferRange) -> StorageKey {
        let start = range.offset();
        StorageKey(self.bytes.slice(start..start + range.len()))
    }
}

fn encode_hot_point_keys(
    branch_id: &str,
    generation: CommitId,
    keys: &[TrackedStateKeyRef<'_>],
) -> EncodedHotPointKeys {
    encode_hot_point_keys_with(branch_id, generation, keys.len(), |index| keys[index])
}

fn encode_hot_point_keys_with<'a>(
    branch_id: &str,
    generation: CommitId,
    key_count: usize,
    mut key_at: impl FnMut(usize) -> TrackedStateKeyRef<'a>,
) -> EncodedHotPointKeys {
    let scope = hot_scope_prefix(branch_id, generation);
    let planned_capacity = (0..key_count).try_fold(0_usize, |total, index| {
        let key = key_at(index);
        let primary_len =
            encoded_hot_identity_key_len(scope.len(), key.schema_key, key.entity_pk, key.file_id)?;
        total.checked_add(primary_len)
    });
    let capacity = planned_capacity.unwrap_or(0);
    let mut bytes = Vec::with_capacity(capacity);
    let mut ranges = Vec::with_capacity(key_count);
    for index in 0..key_count {
        let key = key_at(index);
        let primary_start = bytes.len();
        bytes.extend_from_slice(&scope);
        write_key_string(&mut bytes, key.schema_key, KEY_PART_FINAL);
        write_file_id(&mut bytes, key.file_id);
        write_entity_pk(&mut bytes, key.entity_pk);
        let primary = BufferRange::new(primary_start, bytes.len() - primary_start);

        ranges.push(EncodedHotPointKeyRanges { primary });
    }
    debug_assert!(planned_capacity.is_none() || bytes.len() == capacity);
    EncodedHotPointKeys {
        bytes: Bytes::from(bytes),
        ranges,
    }
}

#[derive(Clone, Copy)]
struct FiniteHotIdentityRef<'a> {
    entity_pk: &'a EntityPk,
    file_id: Option<&'a str>,
}

/// One exact schema partition whose invariant identity components are retained
/// once for the entire point-read batch.
///
/// Entity and file descriptors borrow the caller's filter. Physical primary
/// keys share one immutable arena, so a dense-range probe can reuse the exact
/// same ranges before falling back to MultiGet.
struct FiniteHotIdentityBatchRef<'a> {
    branch_id: &'a str,
    generation: CommitId,
    schema_key: &'a str,
    identities: Vec<FiniteHotIdentityRef<'a>>,
    encoded: EncodedHotPointKeys,
}

impl<'a> FiniteHotIdentityBatchRef<'a> {
    fn new(
        branch_id: &'a str,
        generation: CommitId,
        schema_key: &'a str,
        mut entity_pks: Vec<&'a EntityPk>,
        mut file_ids: Vec<Option<&'a str>>,
    ) -> Option<Self> {
        entity_pks.sort_unstable();
        entity_pks.dedup();
        file_ids.sort_unstable();
        file_ids.dedup();
        let identity_count = entity_pks.len().checked_mul(file_ids.len())?;
        let mut identities = Vec::with_capacity(identity_count);
        for entity_pk in entity_pks {
            for &file_id in &file_ids {
                identities.push(FiniteHotIdentityRef { entity_pk, file_id });
            }
        }
        let encoded =
            encode_hot_point_keys_with(branch_id, generation, identities.len(), |index| {
                TrackedStateKeyRef {
                    schema_key,
                    entity_pk: identities[index].entity_pk,
                    file_id: identities[index].file_id,
                }
            });
        Some(Self {
            branch_id,
            generation,
            schema_key,
            identities,
            encoded,
        })
    }

    fn len(&self) -> usize {
        self.identities.len()
    }

    fn key_ref(&self, index: usize) -> TrackedStateKeyRef<'a> {
        let identity = self.identities[index];
        TrackedStateKeyRef {
            schema_key: self.schema_key,
            entity_pk: identity.entity_pk,
            file_id: identity.file_id,
        }
    }
}

struct FiniteHotEntryBatchRef<'a> {
    identities: FiniteHotIdentityBatchRef<'a>,
    values: Vec<Option<Bytes>>,
}

/// Identity decoded from a storage scan without constructing row-owned
/// `String` buffers for key metadata.
///
/// The immutable storage key is retained once. Schema and file ids normally
/// remain compact ranges into that buffer; only an escaped-NUL key part uses
/// the owned fallback. String and byte entity-PK components retain `Bytes`
/// slices of the same key allocation.
#[derive(Debug)]
struct HotScanIdentity {
    key: Bytes,
    schema_key: HotScanString,
    entity_pk: EntityPk,
    file_id: Option<HotScanString>,
}

#[derive(Debug)]
enum HotScanString {
    Borrowed(Range<u32>),
    Owned(String),
}

impl HotScanString {
    fn as_str<'a>(&'a self, key: &'a Bytes) -> &'a str {
        match self {
            Self::Borrowed(range) => {
                let range = range.start as usize..range.end as usize;
                // SAFETY: `read_hot_scan_key_string` validates this exact
                // range as UTF-8 before constructing the descriptor.
                unsafe { std::str::from_utf8_unchecked(&key[range]) }
            }
            Self::Owned(value) => value,
        }
    }

    fn into_shared_str(self, key: &Bytes) -> SharedStr {
        match self {
            Self::Borrowed(range) => {
                let range = range.start as usize..range.end as usize;
                let value = {
                    // SAFETY: the decoder validated this exact range.
                    unsafe { std::str::from_utf8_unchecked(&key[range]) }
                };
                SharedStr::from_utf8_slice(key.clone(), value)
                    .expect("decoded key string remains inside its retained key")
            }
            Self::Owned(value) => SharedStr::from(value),
        }
    }

    fn into_string(self, key: &Bytes) -> String {
        match self {
            Self::Borrowed(range) => {
                let range = range.start as usize..range.end as usize;
                // SAFETY: the decoder validated this exact range.
                unsafe { std::str::from_utf8_unchecked(&key[range]) }.to_owned()
            }
            Self::Owned(value) => value,
        }
    }

    #[cfg(test)]
    fn owns_fallback_buffer(&self) -> bool {
        matches!(self, Self::Owned(_))
    }
}

impl HotScanIdentity {
    fn schema_key(&self) -> &str {
        self.schema_key.as_str(&self.key)
    }

    fn file_id(&self) -> Option<&str> {
        self.file_id
            .as_ref()
            .map(|file_id| file_id.as_str(&self.key))
    }

    fn matches_filter(&self, filter: &TrackedStateFilter) -> bool {
        (filter.schema_keys.is_empty()
            || filter
                .schema_keys
                .iter()
                .any(|schema_key| schema_key == self.schema_key()))
            && (filter.entity_pks.is_empty() || filter.entity_pks.contains(&self.entity_pk))
            && (filter.file_ids.is_empty()
                || filter.file_ids.iter().any(|filter| match filter {
                    NullableKeyFilter::Any => true,
                    NullableKeyFilter::Null => self.file_id().is_none(),
                    NullableKeyFilter::Value(value) => self.file_id() == Some(value.as_str()),
                }))
    }

    fn into_row_identity(self) -> HeadRowIdentity {
        let Self {
            key,
            schema_key,
            entity_pk,
            file_id,
        } = self;
        HeadRowIdentity {
            schema_key: schema_key.into_string(&key),
            entity_pk,
            file_id: file_id.map(|file_id| file_id.into_string(&key)),
        }
    }

    #[cfg(test)]
    fn owned_metadata_buffer_count(&self) -> usize {
        usize::from(self.schema_key.owns_fallback_buffer())
            + usize::from(
                self.file_id
                    .as_ref()
                    .is_some_and(HotScanString::owns_fallback_buffer),
            )
    }
}

impl PartialEq for HotScanIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.schema_key() == other.schema_key()
            && self.entity_pk == other.entity_pk
            && self.file_id() == other.file_id()
    }
}

impl Eq for HotScanIdentity {}

impl PartialOrd for HotScanIdentity {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HotScanIdentity {
    fn cmp(&self, other: &Self) -> Ordering {
        self.schema_key()
            .cmp(other.schema_key())
            .then_with(|| self.entity_pk.cmp(&other.entity_pk))
            .then_with(|| self.file_id().cmp(&other.file_id()))
    }
}

impl LiveMaterializationIdentity for HotScanIdentity {
    fn push_materialized(
        self,
        rows: &mut MaterializedLiveStateBatchBuilder,
        snapshot_content: Option<SharedStr>,
        metadata: Option<SharedStr>,
        deleted: bool,
        created_at: LixTimestamp,
        updated_at: LixTimestamp,
        global: bool,
        change_id: Option<ChangeId>,
        commit_id: Option<CommitId>,
        untracked: bool,
        branch_id: &str,
    ) {
        rows.push_materialized_ref(
            &self.entity_pk,
            self.schema_key(),
            self.file_id(),
            snapshot_content,
            metadata,
            deleted,
            created_at,
            updated_at,
            global,
            change_id,
            commit_id,
            untracked,
            branch_id,
        );
    }
}

enum HotScanEntries<'a> {
    Finite(Vec<FiniteHotEntryBatchRef<'a>>),
    Decoded(Vec<(HotScanIdentity, Bytes)>),
}

fn filter_hot_scan_entries_by_collection_generation(
    entries: &mut HotScanEntries<'_>,
    control: HotCollectionControl,
) -> Result<(), LixError> {
    let visible = |bytes: &Bytes| -> Result<bool, LixError> {
        Ok(decode_head_value(bytes)?
            .commit_id
            .is_some_and(|commit_id| commit_id > control.active_generation))
    };
    match entries {
        HotScanEntries::Decoded(rows) => {
            let mut retained = Vec::with_capacity(rows.len());
            for (identity, bytes) in rows.drain(..) {
                if visible(&bytes)? {
                    retained.push((identity, bytes));
                }
            }
            *rows = retained;
        }
        HotScanEntries::Finite(batches) => {
            for batch in batches {
                for value in &mut batch.values {
                    if value
                        .as_ref()
                        .map(&visible)
                        .transpose()?
                        .is_some_and(|visible| !visible)
                    {
                        *value = None;
                    }
                }
            }
        }
    }
    Ok(())
}

fn hot_exact_identity_batches<'a>(
    branch_id: &'a str,
    generation: CommitId,
    filter: &'a TrackedStateFilter,
) -> Option<Vec<FiniteHotIdentityBatchRef<'a>>> {
    if filter.schema_keys.is_empty() || filter.entity_pks.is_empty() {
        return None;
    }
    let mut schema_keys = filter
        .schema_keys
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    schema_keys.sort_unstable();
    schema_keys.dedup();
    let entity_pks = filter.entity_pks.iter().collect::<Vec<_>>();
    let file_ids = if filter.file_ids.is_empty() {
        vec![None]
    } else {
        filter
            .file_ids
            .iter()
            .map(|file_id| match file_id {
                NullableKeyFilter::Null => Some(None),
                NullableKeyFilter::Value(value) => Some(Some(value.as_str())),
                NullableKeyFilter::Any => None,
            })
            .collect::<Option<Vec<_>>>()?
    };
    schema_keys
        .into_iter()
        .map(|schema_key| {
            FiniteHotIdentityBatchRef::new(
                branch_id,
                generation,
                schema_key,
                entity_pks.clone(),
                file_ids.clone(),
            )
        })
        .collect()
}

async fn hot_load_finite_identity_bytes(
    store: &(impl StorageAdapterRead + ?Sized),
    batch: &FiniteHotIdentityBatchRef<'_>,
) -> Result<Vec<Option<Bytes>>, LixError> {
    if batch.identities.is_empty() {
        return Ok(Vec::new());
    }
    let keys = (0..batch.len())
        .map(|index| batch.encoded.primary_key(index))
        .collect::<Vec<_>>();
    PointReadPlan::new(HOT_ROW_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .map(|value| value.map(full_value_bytes).transpose())
        .collect()
}

async fn hot_scan_finite_identity_batches<'a>(
    store: &(impl StorageAdapterRead + ?Sized),
    batches: Vec<FiniteHotIdentityBatchRef<'a>>,
    limit: Option<usize>,
) -> Result<Vec<FiniteHotEntryBatchRef<'a>>, LixError> {
    let expected_generation = batches.first().map(|batch| batch.generation);
    let mut remaining = limit.unwrap_or(usize::MAX);
    let mut entries = Vec::with_capacity(batches.len());
    for identities in batches {
        debug_assert_eq!(Some(identities.generation), expected_generation);
        if remaining == 0 {
            break;
        }
        let mut values = if limit.is_none()
            && let Some(values) = hot_scan_dense_identity_range(store, &identities).await?
        {
            values
        } else {
            hot_load_finite_identity_bytes(store, &identities).await?
        };
        if limit.is_some() {
            for value in &mut values {
                if value.is_none() {
                    continue;
                }
                if remaining == 0 {
                    *value = None;
                } else {
                    remaining -= 1;
                }
            }
        }
        entries.push(FiniteHotEntryBatchRef { identities, values });
    }
    Ok(entries)
}

async fn materialize_hot_scan_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    entries: HotScanEntries<'_>,
    projection: ChangeRecordProjection,
    branch_id: &str,
) -> Result<MaterializedLiveStateBatch, LixError> {
    match entries {
        HotScanEntries::Decoded(entries) => {
            materialize_live_entries(store, entries, projection, branch_id).await
        }
        HotScanEntries::Finite(batches) => {
            let row_count = batches
                .iter()
                .map(|batch| batch.values.iter().flatten().count())
                .sum();
            let mut entries = Vec::with_capacity(row_count);
            for batch in batches {
                debug_assert_eq!(batch.identities.branch_id, branch_id);
                for (index, value) in batch.values.into_iter().enumerate() {
                    let Some(value) = value else {
                        continue;
                    };
                    entries.push((batch.identities.key_ref(index), value));
                }
            }
            materialize_live_entries(store, entries, projection, branch_id).await
        }
    }
}

async fn hot_load_identity_ref_bytes(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    identities: &[TrackedStateKeyRef<'_>],
) -> Result<Vec<Option<Bytes>>, LixError> {
    if identities.is_empty() {
        return Ok(Vec::new());
    }
    let encoded = encode_hot_point_keys(branch_id, generation, identities);
    let keys = (0..identities.len())
        .map(|index| encoded.primary_key(index))
        .collect::<Vec<_>>();
    PointReadPlan::new(HOT_ROW_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .map(|value| value.map(full_value_bytes).transpose())
        .collect()
}

/// Loads the authoritative primary row for every identity.
async fn hot_load_primary_identity_bytes(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: &[HeadIdentity],
) -> Result<Vec<Option<Bytes>>, LixError> {
    if identities.is_empty() {
        return Ok(Vec::new());
    }
    let scope = &identities[0];
    debug_assert!(identities.iter().all(|identity| {
        identity.branch_id == scope.branch_id && identity.generation == scope.generation
    }));
    let identities = identities
        .iter()
        .map(|identity| TrackedStateKeyRef {
            schema_key: identity.schema_key.as_str(),
            file_id: identity.file_id.as_deref(),
            entity_pk: &identity.entity_pk,
        })
        .collect::<Vec<_>>();
    let encoded = encode_hot_point_keys(scope.branch_id.as_str(), scope.generation, &identities);
    let keys = (0..identities.len())
        .map(|index| encoded.primary_key(index))
        .collect::<Vec<_>>();
    PointReadPlan::new(HOT_ROW_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .map(|value| value.map(full_value_bytes).transpose())
        .collect()
}

async fn hot_load_file_scope_identities(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    cascades: &BTreeMap<String, &CurrentStateDeltaRef<'_>>,
) -> Result<Vec<HeadIdentity>, LixError> {
    let scope = hot_scope_prefix(branch_id, generation);
    let plan = ScanPlan::prefix(
        HOT_ROW_SPACE,
        StoragePrefix {
            bytes: Bytes::from(scope.clone()),
        },
    );
    let mut identities = Vec::new();
    let mut resume_after = None;
    loop {
        let page = plan
            .collect(
                store,
                StorageScanOptions {
                    projection: StorageCoreProjection::KeyOnly,
                    resume_after: resume_after.clone(),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());
        for entry in page.value.entries {
            let row = decode_hot_row_key_in_scope(entry.key.0.as_ref(), &scope)?;
            if !row
                .file_id
                .as_ref()
                .is_some_and(|file_id| cascades.contains_key(file_id))
            {
                continue;
            }
            identities.push(HeadIdentity {
                branch_id: branch_id.to_string(),
                generation,
                schema_key: row.schema_key,
                entity_pk: row.entity_pk,
                file_id: row.file_id,
            });
        }
        if !page.value.has_more || resume_after.is_none() {
            break;
        }
    }
    Ok(identities)
}

async fn hot_scan_entries<'a>(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &'a str,
    generation: CommitId,
    filter: &'a TrackedStateFilter,
    limit: Option<usize>,
    retained_byte_budget: Option<usize>,
) -> Result<Option<HotScanEntries<'a>>, LixError> {
    // The null-file member is a true point key. A logical-PK scan can use a
    // single MultiGet only when this schema has no file-backed members; if it
    // does, fall through to the complete primary-prefix route so UPDATE and
    // DELETE still see every candidate member.
    if let Some(identities) = hot_exact_identity_batches(branch_id, generation, filter) {
        let may_use_null_point_batch = !filter.file_ids.is_empty()
            || !hot_schema_has_file_members(store, branch_id, generation, &filter.schema_keys)
                .await?;
        if may_use_null_point_batch {
            let entries = HotScanEntries::Finite(
                hot_scan_finite_identity_batches(store, identities, limit).await?,
            );
            return Ok(hot_scan_entries_fit_budget(entries, retained_byte_budget));
        }
    }

    // The authoritative hot index is file-first, so filesystem queries such as
    // `WHERE file_id = ?` read one contiguous hydrated range without a second
    // value projection or random point-read hydration.
    if let Some(prefixes) = hot_file_scan_prefixes(branch_id, generation, filter) {
        let entries = HotScanEntries::Decoded(
            scan_hot_file_entries(store, branch_id, generation, prefixes, filter, limit).await?,
        );
        return Ok(hot_scan_entries_fit_budget(entries, retained_byte_budget));
    }

    let scope = hot_scope_prefix(branch_id, generation);
    let mut prefixes = hot_row_scan_prefixes(&scope, filter);
    prefixes.sort();
    prefixes.dedup();
    let mut rows = Vec::new();
    let mut retained_bytes = 0_usize;
    for prefix in prefixes {
        let plan = ScanPlan::prefix(
            HOT_ROW_SPACE,
            StoragePrefix {
                bytes: Bytes::from(prefix),
            },
        );
        let mut resume_after = None;
        loop {
            let remaining = limit.map(|limit| limit.saturating_sub(rows.len()));
            if matches!(remaining, Some(0)) {
                return Ok(Some(HotScanEntries::Decoded(rows)));
            }
            let page = plan
                .collect(
                    store,
                    StorageScanOptions {
                        resume_after: resume_after.clone(),
                        limit_rows: remaining
                            .unwrap_or_else(|| StorageScanOptions::default().limit_rows),
                        ..StorageScanOptions::default()
                    },
                )
                .await?;
            resume_after = page.value.entries.last().map(|entry| entry.key.clone());
            for entry in page.value.entries {
                let encoded_key_bytes = entry.key.0.len();
                let identity = decode_hot_scan_row_key_in_scope(entry.key.0, &scope)?;
                if identity.matches_filter(filter) {
                    let value = full_value_bytes(entry.value)?;
                    retained_bytes = retained_bytes
                        .checked_add(encoded_key_bytes)
                        .and_then(|bytes| bytes.checked_add(value.len()))
                        .and_then(|bytes| bytes.checked_add(size_of::<(HotScanIdentity, Bytes)>()))
                        .ok_or_else(|| head_value_error("HOT scan resident byte size overflow"))?;
                    if retained_byte_budget.is_some_and(|budget| retained_bytes > budget) {
                        return Ok(None);
                    }
                    rows.push((identity, value));
                    if limit.is_some_and(|limit| rows.len() >= limit) {
                        return Ok(Some(HotScanEntries::Decoded(rows)));
                    }
                }
            }
            if !page.value.has_more || resume_after.is_none() {
                break;
            }
        }
    }
    Ok(Some(HotScanEntries::Decoded(rows)))
}

fn hot_scan_entries_fit_budget<'a>(
    entries: HotScanEntries<'a>,
    retained_byte_budget: Option<usize>,
) -> Option<HotScanEntries<'a>> {
    let Some(budget) = retained_byte_budget else {
        return Some(entries);
    };
    let retained_bytes = match &entries {
        HotScanEntries::Decoded(rows) => rows
            .capacity()
            .saturating_mul(size_of::<(HotScanIdentity, Bytes)>())
            .saturating_add(rows.iter().fold(0_usize, |bytes, (identity, value)| {
                bytes
                    .saturating_add(identity.key.len())
                    .saturating_add(value.len())
                    .saturating_add(match &identity.schema_key {
                        HotScanString::Borrowed(_) => 0,
                        HotScanString::Owned(value) => value.capacity(),
                    })
                    .saturating_add(match &identity.file_id {
                        None | Some(HotScanString::Borrowed(_)) => 0,
                        Some(HotScanString::Owned(value)) => value.capacity(),
                    })
            })),
        HotScanEntries::Finite(batches) => batches
            .capacity()
            .saturating_mul(size_of::<FiniteHotEntryBatchRef<'_>>())
            .saturating_add(batches.iter().fold(0_usize, |bytes, batch| {
                bytes
                    .saturating_add(
                        batch
                            .identities
                            .identities
                            .capacity()
                            .saturating_mul(size_of::<FiniteHotIdentityRef<'_>>()),
                    )
                    .saturating_add(batch.identities.encoded.bytes.len())
                    .saturating_add(
                        batch
                            .identities
                            .encoded
                            .ranges
                            .capacity()
                            .saturating_mul(size_of::<EncodedHotPointKeyRanges>()),
                    )
                    .saturating_add(
                        batch
                            .values
                            .capacity()
                            .saturating_mul(size_of::<Option<Bytes>>()),
                    )
                    .saturating_add(
                        batch
                            .values
                            .iter()
                            .flatten()
                            .fold(0_usize, |bytes, value| bytes.saturating_add(value.len())),
                    )
            })),
    };
    (retained_bytes <= budget).then_some(entries)
}

async fn hot_scan_dense_identity_range(
    store: &(impl StorageAdapterRead + ?Sized),
    identities: &FiniteHotIdentityBatchRef<'_>,
) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
    hot_scan_dense_encoded_key_range(store, identities.len(), |index| {
        identities.encoded.primary_key_bytes(index)
    })
    .await
}

async fn hot_scan_dense_encoded_key_range<'a>(
    store: &(impl StorageAdapterRead + ?Sized),
    key_count: usize,
    key_at: impl Fn(usize) -> &'a [u8],
) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
    if key_count < HOT_DENSE_SCAN_MIN_IDENTITIES {
        return Ok(None);
    }
    if key_count == 0 {
        return Ok(Some(Vec::new()));
    }
    if (1..key_count).any(|index| key_at(index - 1) > key_at(index)) {
        return Ok(None);
    }
    let first_key = StorageKey(Bytes::copy_from_slice(key_at(0)));
    let last_key = StorageKey(Bytes::copy_from_slice(key_at(key_count - 1)));
    let plan = ScanPlan::range(
        HOT_ROW_SPACE,
        crate::storage_adapter::StorageKeyRange {
            lower: std::ops::Bound::Included(first_key),
            upper: std::ops::Bound::Included(last_key),
        },
    );
    let scan_budget = key_count.saturating_mul(HOT_DENSE_SCAN_MAX_OVERREAD);
    let mut scanned = 0;
    let mut requested_index = 0;
    let mut resume_after = None;
    let mut values = vec![None; key_count];
    loop {
        let remaining_budget = scan_budget.saturating_sub(scanned);
        if remaining_budget == 0 {
            return Ok(None);
        }
        let page = plan
            .collect(
                store,
                StorageScanOptions {
                    resume_after: resume_after.clone(),
                    limit_rows: remaining_budget.min(StorageScanOptions::default().limit_rows),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());
        scanned += page.value.entries.len();
        for entry in page.value.entries {
            while requested_index < key_count && key_at(requested_index) < entry.key.0.as_ref() {
                requested_index += 1;
            }
            if requested_index < key_count && key_at(requested_index) == entry.key.0.as_ref() {
                values[requested_index] = Some(full_value_bytes(entry.value)?);
                requested_index += 1;
            }
        }
        if requested_index == key_count || !page.value.has_more || resume_after.is_none() {
            return Ok(Some(values));
        }
    }
}

fn hot_row_scan_prefixes(scope: &[u8], filter: &TrackedStateFilter) -> Vec<Vec<u8>> {
    if filter.schema_keys.is_empty() {
        return vec![scope.to_vec()];
    }
    filter
        .schema_keys
        .iter()
        .map(|schema_key| {
            let mut prefix = scope.to_vec();
            write_key_string(&mut prefix, schema_key, KEY_PART_FINAL);
            prefix
        })
        .collect()
}

fn hot_file_scan_prefixes(
    branch_id: &str,
    generation: CommitId,
    filter: &TrackedStateFilter,
) -> Option<Vec<Vec<u8>>> {
    if filter.schema_keys.is_empty()
        || filter.file_ids.is_empty()
        || !filter.entity_pks.is_empty()
        || filter
            .file_ids
            .iter()
            .any(|file_id| !matches!(file_id, NullableKeyFilter::Value(_)))
    {
        return None;
    }
    let mut prefixes = Vec::with_capacity(filter.schema_keys.len() * filter.file_ids.len());
    for schema_key in &filter.schema_keys {
        for file_id in &filter.file_ids {
            let NullableKeyFilter::Value(file_id) = file_id else {
                unreachable!("file-id projection predicate was checked above");
            };
            let mut prefix = hot_scope_prefix(branch_id, generation);
            write_key_string(&mut prefix, schema_key, KEY_PART_FINAL);
            write_file_id(&mut prefix, Some(file_id));
            prefixes.push(prefix);
        }
    }
    prefixes.sort();
    prefixes.dedup();
    Some(prefixes)
}

async fn scan_hot_file_entries(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    prefixes: Vec<Vec<u8>>,
    filter: &TrackedStateFilter,
    limit: Option<usize>,
) -> Result<Vec<(HotScanIdentity, Bytes)>, LixError> {
    let scope = hot_scope_prefix(branch_id, generation);
    let mut rows = Vec::new();
    for prefix in prefixes {
        let plan = ScanPlan::prefix(
            HOT_ROW_SPACE,
            StoragePrefix {
                bytes: Bytes::from(prefix),
            },
        );
        let mut resume_after = None;
        loop {
            let page = plan
                .collect(
                    store,
                    StorageScanOptions {
                        resume_after: resume_after.clone(),
                        ..StorageScanOptions::default()
                    },
                )
                .await?;
            resume_after = page.value.entries.last().map(|entry| entry.key.clone());
            for entry in page.value.entries {
                let identity = decode_hot_scan_row_key_in_scope(entry.key.0, &scope)?;
                if identity.matches_filter(filter) {
                    rows.push((identity, full_value_bytes(entry.value)?));
                }
            }
            if !page.value.has_more || resume_after.is_none() {
                break;
            }
        }
    }
    // Physical rows are ordered `(schema, file_id, entity_pk)`, while SQL rows
    // are ordered `(schema, entity_pk, file_id)`. Restore the public order
    // after multi-file scans and defend against repeated predicates.
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    rows.dedup_by(|left, right| left.0 == right.0);
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    Ok(rows)
}

async fn hot_schema_has_file_members(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    schema_keys: &[String],
) -> Result<bool, LixError> {
    // Exact identity batches always name their schema. Broad scans deliberately
    // take the primary-prefix route, which already sees all file members.
    if schema_keys.is_empty() {
        return Ok(true);
    }
    for schema_key in schema_keys {
        if hot_schema_has_file_member(store, branch_id, generation, schema_key).await? {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn hot_schema_has_file_member(
    store: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
) -> Result<bool, LixError> {
    let scope = hot_scope_prefix(branch_id, generation);
    let key = StorageKey(Bytes::from(encode_hot_file_schema_key(&scope, schema_key)));
    let values = PointReadPlan::new(HOT_FILE_SPACE, &[key])
        .materialize(
            store,
            StorageGetOptions {
                projection: StorageCoreProjection::KeyOnly,
            },
        )
        .await?;
    Ok(values.value.into_iter().next().flatten().is_some())
}

fn hot_scope_prefix(branch_id: &str, generation: CommitId) -> Vec<u8> {
    encode_scope_prefix(branch_id, generation)
}

#[cfg(test)]
pub(super) fn encode_hot_row_key(identity: &HeadIdentity) -> Vec<u8> {
    encode_hot_row_key_parts(
        &identity.branch_id,
        identity.generation,
        &identity.schema_key,
        &identity.entity_pk,
        identity.file_id.as_deref(),
    )
}

#[cfg(test)]
fn encode_hot_row_key_parts(
    branch_id: &str,
    generation: CommitId,
    schema_key: &str,
    entity_pk: &EntityPk,
    file_id: Option<&str>,
) -> Vec<u8> {
    let mut key = hot_scope_prefix(branch_id, generation);
    write_key_string(&mut key, schema_key, KEY_PART_FINAL);
    write_file_id(&mut key, file_id);
    write_entity_pk(&mut key, entity_pk);
    key
}

fn encoded_hot_identity_key_len(
    scope_len: usize,
    schema_key: &str,
    entity_pk: &EntityPk,
    file_id: Option<&str>,
) -> Option<usize> {
    let file_id_len = match file_id {
        Some(file_id) => encoded_key_bytes_len(file_id.as_bytes())?,
        None => 0,
    };
    scope_len
        .checked_add(encoded_key_bytes_len(schema_key.as_bytes())?)?
        .checked_add(encoded_entity_pk_len(entity_pk)?)?
        .checked_add(1)?
        .checked_add(file_id_len)
}

fn encoded_entity_pk_len(entity_pk: &EntityPk) -> Option<usize> {
    entity_pk
        .components
        .iter()
        .try_fold(1_usize, |total, component| {
            let payload_len = match component {
                crate::entity_pk::EntityPkComponent::Uuid(_) => Some(16 + 1),
                crate::entity_pk::EntityPkComponent::Integer(_) => Some(8 + 1),
                crate::entity_pk::EntityPkComponent::String(value) => {
                    encoded_key_bytes_len(value.as_bytes())
                }
                crate::entity_pk::EntityPkComponent::Bytes(value) => {
                    encoded_key_bytes_len(value.as_ref())
                }
            }?;
            total.checked_add(1)?.checked_add(payload_len)
        })
}

fn encoded_key_bytes_len(value: &[u8]) -> Option<usize> {
    value
        .len()
        .checked_add(memchr::memchr_iter(KEY_PART_FINAL, value).count())?
        .checked_add(2)
}

fn decode_hot_scan_row_key_in_scope(key: Bytes, scope: &[u8]) -> Result<HotScanIdentity, LixError> {
    if !key.starts_with(scope) {
        return Err(key_codec_error(
            "hot row does not begin with its scanned scope",
        ));
    }
    let mut offset = scope.len();
    let (schema_key, schema_terminator) =
        read_hot_scan_key_string(&key, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot row schema key has an invalid terminator",
        ));
    }
    let file_id = read_hot_scan_file_id(&key, &mut offset)?;
    let entity_pk = read_hot_scan_entity_pk(&key, &mut offset)?;
    if offset != key.len() {
        return Err(key_codec_error("hot row key has trailing bytes"));
    }
    Ok(HotScanIdentity {
        key,
        schema_key,
        entity_pk,
        file_id,
    })
}

fn read_hot_scan_entity_pk(bytes: &Bytes, offset: &mut usize) -> Result<EntityPk, LixError> {
    let version = bytes
        .get(*offset)
        .copied()
        .ok_or_else(|| key_codec_error("is truncated before entity primary key version"))?;
    *offset += 1;
    if version != ENTITY_PK_CODEC_V1 {
        return Err(key_codec_error(&format!(
            "has unsupported entity primary key codec version {version}"
        )));
    }
    let mut components = SmallVec::new();
    loop {
        let (part, terminator) = read_hot_scan_entity_pk_part(bytes, offset)?;
        components.push(part);
        match terminator {
            KEY_PART_FINAL => break,
            KEY_PART_MORE => {}
            _ => {
                return Err(key_codec_error(
                    "entity primary key has an invalid terminator",
                ));
            }
        }
    }
    EntityPk::from_components(components).map_err(|error| {
        key_codec_error(&format!("contains an invalid entity primary key: {error}"))
    })
}

fn read_hot_scan_entity_pk_part(
    bytes: &Bytes,
    offset: &mut usize,
) -> Result<(crate::entity_pk::EntityPkComponent, u8), LixError> {
    let tag = bytes
        .get(*offset)
        .copied()
        .ok_or_else(|| key_codec_error("is truncated before entity primary key part tag"))?;
    *offset += 1;
    match tag {
        ENTITY_PK_STRING => {
            let (value, terminator) =
                read_hot_scan_key_string(bytes, offset, "entity primary key")?;
            Ok((
                crate::entity_pk::EntityPkComponent::String(value.into_shared_str(bytes)),
                terminator,
            ))
        }
        ENTITY_PK_BYTES => {
            let (value, terminator) =
                read_hot_scan_shared_bytes(bytes, offset, "entity primary key bytes")?;
            Ok((
                crate::entity_pk::EntityPkComponent::Bytes(value),
                terminator,
            ))
        }
        ENTITY_PK_UUID => {
            let uuid_end = offset
                .checked_add(16)
                .ok_or_else(|| key_codec_error("UUIDv7 entity primary key offset overflow"))?;
            let uuid_bytes: [u8; 16] = bytes
                .get(*offset..uuid_end)
                .ok_or_else(|| key_codec_error("is truncated in UUIDv7 entity primary key"))?
                .try_into()
                .expect("UUIDv7 slice has fixed length");
            let terminator = bytes
                .get(uuid_end)
                .copied()
                .ok_or_else(|| key_codec_error("is truncated after UUIDv7 entity primary key"))?;
            if !matches!(terminator, KEY_PART_FINAL | KEY_PART_MORE) {
                return Err(key_codec_error(
                    "UUIDv7 entity primary key has an invalid terminator",
                ));
            }
            *offset = uuid_end + 1;
            Ok((
                crate::entity_pk::EntityPkComponent::Uuid(uuid_bytes),
                terminator,
            ))
        }
        ENTITY_PK_INTEGER => {
            let integer_end = offset
                .checked_add(8)
                .ok_or_else(|| key_codec_error("integer entity primary key offset overflow"))?;
            let ordered = u64::from_be_bytes(
                bytes
                    .get(*offset..integer_end)
                    .ok_or_else(|| key_codec_error("is truncated in integer entity primary key"))?
                    .try_into()
                    .expect("integer slice has fixed length"),
            );
            let terminator = bytes
                .get(integer_end)
                .copied()
                .ok_or_else(|| key_codec_error("is truncated after integer entity primary key"))?;
            if !matches!(terminator, KEY_PART_FINAL | KEY_PART_MORE) {
                return Err(key_codec_error(
                    "integer entity primary key has an invalid terminator",
                ));
            }
            *offset = integer_end + 1;
            Ok((
                crate::entity_pk::EntityPkComponent::Integer(i64::from_be_bytes(
                    (ordered ^ (1_u64 << 63)).to_be_bytes(),
                )),
                terminator,
            ))
        }
        _ => Err(key_codec_error(
            "has an unknown entity primary key part tag",
        )),
    }
}

fn read_hot_scan_file_id(
    bytes: &Bytes,
    offset: &mut usize,
) -> Result<Option<HotScanString>, LixError> {
    let tag = *bytes
        .get(*offset)
        .ok_or_else(|| key_codec_error("is truncated before file id"))?;
    *offset += 1;
    match tag {
        FILE_ID_NONE => Ok(None),
        FILE_ID_SOME => {
            let (file_id, terminator) = read_hot_scan_key_string(bytes, offset, "file id")?;
            if terminator != KEY_PART_FINAL {
                return Err(key_codec_error("file id has an invalid terminator"));
            }
            Ok(Some(file_id))
        }
        _ => Err(key_codec_error("has an invalid file id tag")),
    }
}

fn read_hot_scan_key_string(
    bytes: &Bytes,
    offset: &mut usize,
    field: &str,
) -> Result<(HotScanString, u8), LixError> {
    let start = *offset;
    let mut cursor = start;
    loop {
        let byte = *bytes
            .get(cursor)
            .ok_or_else(|| key_codec_error(&format!("is truncated in {field}")))?;
        cursor += 1;
        if byte != KEY_PART_FINAL {
            continue;
        }
        let terminator = *bytes
            .get(cursor)
            .ok_or_else(|| key_codec_error(&format!("is truncated after {field}")))?;
        cursor += 1;
        if terminator != KEY_ESCAPE {
            let end = cursor - 2;
            std::str::from_utf8(&bytes[start..end])
                .map_err(|error| key_codec_error(&format!("{field} is not UTF-8: {error}")))?;
            let start = u32::try_from(start)
                .map_err(|_| key_codec_error(&format!("{field} offset exceeds u32")))?;
            let end = u32::try_from(end)
                .map_err(|_| key_codec_error(&format!("{field} offset exceeds u32")))?;
            *offset = cursor;
            return Ok((HotScanString::Borrowed(start..end), terminator));
        }
        break;
    }

    // Embedded NULs require unescaping. Preserve that uncommon case without
    // imposing an owned buffer on generated schema and file identifiers.
    *offset = start;
    read_key_string(bytes.as_ref(), offset, field)
        .map(|(value, terminator)| (HotScanString::Owned(value), terminator))
}

fn read_hot_scan_shared_bytes(
    bytes: &Bytes,
    offset: &mut usize,
    field: &str,
) -> Result<(Bytes, u8), LixError> {
    let start = *offset;
    let mut cursor = start;
    loop {
        let byte = *bytes
            .get(cursor)
            .ok_or_else(|| key_codec_error(&format!("is truncated in {field}")))?;
        cursor += 1;
        if byte != KEY_PART_FINAL {
            continue;
        }
        let terminator = *bytes
            .get(cursor)
            .ok_or_else(|| key_codec_error(&format!("is truncated after {field}")))?;
        cursor += 1;
        if terminator != KEY_ESCAPE {
            *offset = cursor;
            return Ok((bytes.slice(start..cursor - 2), terminator));
        }
        break;
    }

    *offset = start;
    read_key_bytes(bytes.as_ref(), offset, field)
        .map(|(value, terminator)| (Bytes::from(value), terminator))
}

fn decode_hot_row_key_in_scope(bytes: &[u8], scope: &[u8]) -> Result<HeadRowIdentity, LixError> {
    if !bytes.starts_with(scope) {
        return Err(key_codec_error(
            "hot row does not begin with its scanned scope",
        ));
    }
    let mut offset = scope.len();
    let (schema_key, schema_terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot row schema key has an invalid terminator",
        ));
    }
    let file_id = read_file_id(bytes, &mut offset)?;
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("hot row key has trailing bytes"));
    }
    Ok(HeadRowIdentity {
        schema_key,
        entity_pk,
        file_id,
    })
}

fn decode_hot_row_scope(bytes: &[u8]) -> Result<(String, CommitId), LixError> {
    let mut offset = 0;
    let (branch_id, branch_terminator) = read_key_string(bytes, &mut offset, "branch id")?;
    if branch_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot row branch id has an invalid terminator",
        ));
    }
    let generation = read_generation(bytes, &mut offset)?;
    let (schema_key, schema_terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot row schema key has an invalid terminator",
        ));
    }
    let file_id = read_file_id(bytes, &mut offset)?;
    let entity_pk = read_entity_pk(bytes, &mut offset)?;
    if offset != bytes.len() {
        return Err(key_codec_error("hot row key has trailing bytes"));
    }
    let _ = (schema_key, entity_pk, file_id);
    Ok((branch_id, generation))
}

fn encode_hot_file_schema_key(scope: &[u8], schema_key: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(
        scope
            .len()
            .saturating_add(encoded_key_bytes_len(schema_key.as_bytes()).unwrap_or(0)),
    );
    key.extend_from_slice(scope);
    write_key_string(&mut key, schema_key, KEY_PART_FINAL);
    key
}

fn decode_hot_file_scope(bytes: &[u8]) -> Result<(String, CommitId), LixError> {
    let mut offset = 0;
    let (branch_id, branch_terminator) = read_key_string(bytes, &mut offset, "branch id")?;
    if branch_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot file schema branch id has an invalid terminator",
        ));
    }
    let generation = read_generation(bytes, &mut offset)?;
    let (schema_key, schema_terminator) = read_key_string(bytes, &mut offset, "schema key")?;
    if schema_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot file schema key has an invalid terminator",
        ));
    }
    if offset != bytes.len() {
        return Err(key_codec_error("hot file schema key has trailing bytes"));
    }
    let _ = schema_key;
    Ok((branch_id, generation))
}

fn collect_hot_untracked_refs(value: HeadValueView<'_>, refs: &mut BTreeSet<[u8; JSON_REF_BYTES]>) {
    if !value.untracked {
        return;
    }
    for slot in [value.snapshot, value.metadata] {
        if let HeadSlotView::Ref(json_ref) = slot {
            refs.insert(*json_ref.as_hash_array());
        }
    }
}

pub(crate) async fn stage_collect_stale_hot_generations<S>(
    store: &S,
    writes: &mut StorageWriteSet,
    controls: &[(String, BranchHeadControl)],
) -> Result<Vec<JsonRef>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let active = active_current_state_generations(controls);
    let mut stale_untracked_refs = BTreeSet::new();
    stage_collect_stale_hot_space(
        store,
        writes,
        HOT_ROW_SPACE,
        decode_hot_row_scope,
        &active,
        &mut stale_untracked_refs,
    )
    .await?;
    // Sweep schema membership markers independently so orphaned generations
    // cannot retain conservative file-membership hints.
    stage_collect_stale_hot_space(
        store,
        writes,
        HOT_FILE_SPACE,
        decode_hot_file_scope,
        &active,
        &mut stale_untracked_refs,
    )
    .await?;
    stage_collect_stale_hot_space(
        store,
        writes,
        ROOT_CURRENT_BASE_SPACE,
        decode_hot_collection_control_scope,
        &active,
        &mut stale_untracked_refs,
    )
    .await?;
    stage_collect_stale_hot_collection_controls(store, writes, &active).await?;
    Ok(stale_untracked_refs
        .into_iter()
        .map(JsonRef::from_hash_bytes)
        .collect())
}

fn decode_hot_collection_control_scope(bytes: &[u8]) -> Result<(String, CommitId), LixError> {
    let mut offset = 0;
    let (branch_id, branch_terminator) = read_key_string(bytes, &mut offset, "branch id")?;
    if branch_terminator != KEY_PART_FINAL {
        return Err(key_codec_error(
            "hot collection-control branch id has an invalid terminator",
        ));
    }
    let generation = read_generation(bytes, &mut offset)?;
    Ok((branch_id, generation))
}

async fn stage_collect_stale_hot_collection_controls(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    active: &BTreeSet<(String, CommitId)>,
) -> Result<(), LixError> {
    let plan = ScanPlan::prefix(
        HOT_COLLECTION_CONTROL_SPACE,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    );
    let mut resume_after = None;
    loop {
        let page = plan
            .collect(
                store,
                StorageScanOptions {
                    resume_after: resume_after.clone(),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());
        for entry in page.value.entries {
            let keep = decode_hot_collection_control_scope(entry.key.0.as_ref())
                .is_ok_and(|scope| active.contains(&scope));
            if !keep {
                writes.delete(HOT_COLLECTION_CONTROL_SPACE, entry.key);
            }
        }
        if !page.value.has_more || resume_after.is_none() {
            break;
        }
    }
    Ok(())
}

async fn stage_collect_stale_hot_space(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    space: StorageSpace,
    decode_key: fn(&[u8]) -> Result<(String, CommitId), LixError>,
    active: &BTreeSet<(String, CommitId)>,
    stale_untracked_refs: &mut BTreeSet<[u8; JSON_REF_BYTES]>,
) -> Result<bool, LixError> {
    let plan = ScanPlan::prefix(
        space,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    );
    let mut deleted_any = false;
    let mut resume_after = None;
    loop {
        let page = plan
            .collect(
                store,
                StorageScanOptions {
                    resume_after: resume_after.clone(),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());
        for entry in page.value.entries {
            let active_generation =
                decode_key(entry.key.0.as_ref()).is_ok_and(|identity| active.contains(&identity));
            if active_generation {
                continue;
            }
            deleted_any = true;
            if let StorageProjectedValue::FullValue(bytes) = &entry.value
                && let Ok(value) = decode_head_value(bytes)
            {
                collect_hot_untracked_refs(value, stale_untracked_refs);
            }
            writes.delete(space, entry.key);
        }
        if !page.value.has_more || resume_after.is_none() {
            break;
        }
    }
    Ok(deleted_any)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;

    use super::*;
    use crate::storage_adapter::{
        Memory, StorageAdapter, StorageGetManyRequest, StorageGetManyResult, StorageKeyRange,
        StorageReadOptions, StorageScanChunk, StorageScanOptions, StorageWriteOptions,
    };

    #[test]
    fn keyed_certified_change_ids_include_full_identity() {
        let commit_id = CommitId::for_test_label("keyed-certified-change");
        let entity_pk = EntityPk::single("same-key");
        let first = certified_keyed_change_id(commit_id, "test_schema", "first.csv", &entity_pk, 1);
        let second =
            certified_keyed_change_id(commit_id, "test_schema", "second.csv", &entity_pk, 1);
        assert_ne!(first, second);
        assert_eq!(
            first,
            certified_keyed_change_id(commit_id, "test_schema", "first.csv", &entity_pk, 1,)
        );

        let mut record = Vec::new();
        record.push(0);
        record.extend_from_slice(&("test_schema".len() as u32).to_le_bytes());
        record.extend_from_slice(b"test_schema");
        record.extend_from_slice(&1_u32.to_le_bytes());
        record.extend_from_slice(&("same-key".len() as u32).to_le_bytes());
        record.extend_from_slice(b"same-key");
        record.push(0);
        record.push(0);
        record.extend_from_slice(&2_u32.to_le_bytes());
        record.extend_from_slice(b"{}");
        let mut page = Vec::new();
        page.extend_from_slice(&(record.len() as u32).to_le_bytes());
        page.extend_from_slice(&record);
        let batch = WasmCertifiedEntityBatch {
            format: 2,
            schema_keys: vec!["test_schema".to_owned()],
            row_count: 1,
            creates: WasmCreateContext { high: 0, low: 0 },
            create_ranges: Vec::new(),
            complete_file_state: true,
            pages: vec![Bytes::from(page)],
        };
        let materialized = |file_id| {
            materialize_certified_root_rows("main", file_id, commit_id, timestamp(), &batch)
                .expect("keyed packet should materialize")
                .row(0)
                .change_id()
                .expect("keyed packet row should have a change id")
        };
        assert_eq!(materialized("first.csv"), first);
        assert_eq!(materialized("first.csv"), materialized("first.csv"));
        assert_ne!(materialized("first.csv"), materialized("second.csv"));
    }

    #[tokio::test]
    async fn transaction_reader_reuses_collection_control_point_read() {
        const BRANCH_ID: &str = "collection-control-cache-branch";
        const SCHEMA_KEY: &str = "collection_control_cache_schema";
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label("collection-control-cache-generation");
        let mut writes = StorageWriteSet::new();
        stage_hot_collection_control(
            &mut writes,
            BRANCH_ID,
            generation,
            crate::collection_generation::CollectionScopeRef {
                schema_key: SCHEMA_KEY,
                file_id: None,
            },
            HotCollectionControl {
                active_generation: generation,
                live_count: 7,
                ordered_identity_digest: Some([3; 32]),
            },
        )
        .expect("stage collection control fixture");
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("publish collection control fixture");

        let get_many_calls = Arc::new(AtomicUsize::new(0));
        let reader = HotStateStoreReader {
            store: CountingRead {
                inner: storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("open collection control fixture read"),
                get_many_calls: Arc::clone(&get_many_calls),
                scan_calls: None,
            },
            transaction_cache: Some(Arc::new(HotStateTransactionCache::default())),
        };
        for _ in 0..2 {
            let control = reader
                .collection_generation(
                    BRANCH_ID,
                    generation,
                    crate::collection_generation::CollectionScopeRef {
                        schema_key: SCHEMA_KEY,
                        file_id: None,
                    },
                )
                .await
                .expect("load cached collection control");
            assert_eq!(control.live_count, 7);
            assert_eq!(control.ordered_identity_digest, Some([3; 32]));
        }
        assert_eq!(
            get_many_calls.load(Ordering::Relaxed),
            1,
            "the immutable transaction snapshot should point-read a control once"
        );
    }

    struct CountingRead<R> {
        inner: R,
        get_many_calls: Arc<AtomicUsize>,
        scan_calls: Option<Arc<AtomicUsize>>,
    }

    impl<R: StorageAdapterRead> StorageAdapterRead for CountingRead<R> {
        fn snapshot_cache_key(&self) -> Option<u128> {
            self.inner.snapshot_cache_key()
        }

        async fn get_many(
            &self,
            requests: &[StorageGetManyRequest<'_>],
        ) -> Result<StorageGetManyResult, crate::storage_adapter::StorageError> {
            self.get_many_calls.fetch_add(1, Ordering::Relaxed);
            self.inner.get_many(requests).await
        }

        async fn scan(
            &self,
            space: StorageSpace,
            range: StorageKeyRange,
            opts: StorageScanOptions,
        ) -> Result<StorageScanChunk, crate::storage_adapter::StorageError> {
            if let Some(scan_calls) = &self.scan_calls {
                scan_calls.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.scan(space, range, opts).await
        }
    }

    fn timestamp() -> LixTimestamp {
        LixTimestamp::expect_parse("hot working-diff test timestamp", "2026-01-01T00:00:00Z")
    }

    fn live_row(entity_pk: &str, commit_label: &str) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: EntityPk::single(entity_pk),
            schema_key: "schema".to_owned(),
            file_id: None,
            snapshot_content: None,
            metadata: None,
            deleted: false,
            created_at: timestamp(),
            updated_at: timestamp(),
            global: false,
            change_id: Some(ChangeId::for_test_label(&format!("change-{commit_label}"))),
            commit_id: Some(CommitId::for_test_label(commit_label)),
            untracked: false,
            branch_id: Arc::from("branch"),
        }
    }

    #[test]
    fn sparse_hot_overlay_merges_with_packed_rows_in_identity_order() {
        let hot = vec![live_row("c", "hot-c")];
        let packed = vec![live_row("a", "packed-a"), live_row("b", "packed-b")];

        let merged = merge_ordered_live_rows(hot, packed);

        assert_eq!(
            merged
                .iter()
                .map(|row| row.entity_pk.as_single_string_owned().expect("single key"))
                .collect::<Vec<_>>(),
            ["a", "b", "c"]
        );
    }

    #[test]
    fn compressed_certified_page_rejects_invalid_bounds_and_corruption() {
        let mut inverted = Vec::new();
        inverted.extend_from_slice(&2_u32.to_le_bytes());
        inverted.extend_from_slice(&1_u32.to_le_bytes());
        inverted.extend_from_slice(&1_u32.to_le_bytes());
        inverted.push(0);
        assert!(certified_zstd_packet_page_header(&inverted).is_err());

        let mut oversized = Vec::new();
        oversized.extend_from_slice(&1_u32.to_le_bytes());
        oversized.extend_from_slice(&2_u32.to_le_bytes());
        oversized.extend_from_slice(&(64_u32 * 1024 * 1024 + 1).to_le_bytes());
        oversized.push(0);
        assert!(certified_zstd_packet_page_header(&oversized).is_err());

        let mut corrupt = Vec::new();
        corrupt.extend_from_slice(&1_u32.to_le_bytes());
        corrupt.extend_from_slice(&2_u32.to_le_bytes());
        corrupt.extend_from_slice(&16_u32.to_le_bytes());
        corrupt.extend_from_slice(b"not a zstd frame");
        assert!(decode_certified_zstd_packet_page(&corrupt).is_err());
    }

    fn diff_identity(branch_id: &str, generation: CommitId, entity: &str) -> HeadIdentity {
        HeadIdentity {
            branch_id: branch_id.to_string(),
            generation,
            schema_key: "schema".to_string(),
            entity_pk: EntityPk::single(entity),
            file_id: None,
        }
    }

    #[test]
    fn ten_thousand_finite_hot_identities_share_one_primary_key_arena() {
        let generation = CommitId::for_test_label("point-key-generation");
        let entity_pks = (0..5_000)
            .map(|index| EntityPk::single(format!("entity-{index:05}")))
            .collect::<Vec<_>>();
        let branch_id = String::from("branch");
        let schema_key = String::from("schema");
        let batch = FiniteHotIdentityBatchRef::new(
            &branch_id,
            generation,
            &schema_key,
            entity_pks.iter().collect(),
            vec![None, Some("file")],
        )
        .expect("test identity count is representable");

        assert_eq!(batch.len(), 10_000);
        assert_eq!(batch.encoded.ranges.len(), batch.len());
        assert_eq!(batch.encoded.ranges.capacity(), batch.len());
        assert_eq!(
            batch
                .encoded
                .ranges
                .last()
                .map(|ranges| ranges.primary.offset() + ranges.primary.len()),
            Some(batch.encoded.bytes.len())
        );
        for index in [0, 1, 9_999] {
            let identity = batch.key_ref(index);
            assert_eq!(batch.branch_id.as_ptr(), branch_id.as_ptr());
            assert_eq!(identity.schema_key.as_ptr(), schema_key.as_ptr());
            let primary = batch.encoded.ranges[index].primary;
            let key = batch.encoded.primary_key(index);
            assert_eq!(
                key.0.as_ptr(),
                batch.encoded.bytes[primary.offset()..].as_ptr(),
                "primary point key {index} must remain a slice of the batch arena"
            );
        }
    }

    #[test]
    fn ten_thousand_hot_scan_identities_borrow_repeated_metadata() {
        const ROW_COUNT: usize = 10_000;
        let generation = CommitId::for_test_label("borrowed-scan-generation");
        let scope = hot_scope_prefix("branch", generation);
        let schema_key = "shared_schema";
        let file_id = "shared_file";
        let entity_pks = (0..ROW_COUNT)
            .map(|index| EntityPk::single(format!("entity-{index:05}")))
            .collect::<Vec<_>>();
        let capacity = entity_pks
            .iter()
            .try_fold(0_usize, |total, entity_pk| {
                total.checked_add(
                    encoded_hot_identity_key_len(scope.len(), schema_key, entity_pk, Some(file_id))
                        .expect("test key size is representable"),
                )
            })
            .expect("test key arena size is representable");
        let mut key_bytes = Vec::with_capacity(capacity);
        let ranges = entity_pks
            .iter()
            .map(|entity_pk| {
                let start = key_bytes.len();
                key_bytes.extend_from_slice(&scope);
                write_key_string(&mut key_bytes, schema_key, KEY_PART_FINAL);
                write_file_id(&mut key_bytes, Some(file_id));
                write_entity_pk(&mut key_bytes, entity_pk);
                start..key_bytes.len()
            })
            .collect::<Vec<_>>();
        assert_eq!(key_bytes.len(), capacity);
        let key_bytes = Bytes::from(key_bytes);
        let identities = ranges
            .into_iter()
            .map(|range| {
                decode_hot_scan_row_key_in_scope(key_bytes.slice(range), &scope)
                    .expect("decode borrowed hot scan key")
            })
            .collect::<Vec<_>>();

        assert_eq!(identities.len(), ROW_COUNT);
        assert_eq!(
            identities
                .iter()
                .map(HotScanIdentity::owned_metadata_buffer_count)
                .sum::<usize>(),
            0,
            "normal schema and file ids must remain ranges over storage keys"
        );
        assert!(identities.iter().all(|identity| {
            identity.schema_key() == schema_key && identity.file_id() == Some(file_id)
        }));

        let mut rows = MaterializedLiveStateBatchBuilder::with_capacity(ROW_COUNT);
        for identity in identities {
            identity.push_materialized(
                &mut rows,
                None,
                None,
                false,
                timestamp(),
                timestamp(),
                false,
                None,
                None,
                true,
                "branch",
            );
        }
        let rows = rows.finish();

        assert_eq!(rows.len(), ROW_COUNT);
        assert_eq!(rows.dictionary_entry_count(), 3);
        assert_eq!(rows.dictionary_arena_buffer_count(), 1);
        assert_eq!(
            rows.dictionary_arena_allocation_count(),
            1,
            "materialization should allocate one small identity arena, not per-row buffers"
        );
        assert_eq!(rows.dictionary_arena_large_allocation_count(), 0);
        assert_eq!(
            rows.row(0).schema_key().as_ptr(),
            rows.row(ROW_COUNT - 1).schema_key().as_ptr()
        );
        assert_eq!(
            rows.row(0).file_id().expect("file").as_ptr(),
            rows.row(ROW_COUNT - 1).file_id().expect("file").as_ptr()
        );
    }

    #[test]
    fn hot_scan_admission_is_bounded_by_retained_bytes_not_row_count() {
        const TINY_ROW_COUNT: usize = 5_000;
        const BUDGET: usize = 4 * 1024 * 1024;
        let generation = CommitId::for_test_label("hot-scan-byte-budget");
        let scope = hot_scope_prefix("branch", generation);
        let tiny_rows = (0..TINY_ROW_COUNT)
            .map(|index| {
                let entity_pk = EntityPk::single(format!("entity-{index:05}"));
                let key = Bytes::from(encode_hot_row_key_parts(
                    "branch", generation, "schema", &entity_pk, None,
                ));
                let identity = decode_hot_scan_row_key_in_scope(key, &scope)
                    .expect("decode tiny HOT scan identity");
                (identity, Bytes::from_static(b"{}"))
            })
            .collect::<Vec<_>>();
        assert!(
            hot_scan_entries_fit_budget(HotScanEntries::Decoded(tiny_rows), Some(BUDGET),)
                .is_some(),
            "thousands of narrow rows must not trip a cardinality policy"
        );

        let entity_pk = EntityPk::single("large");
        let key = Bytes::from(encode_hot_row_key_parts(
            "branch", generation, "schema", &entity_pk, None,
        ));
        let identity =
            decode_hot_scan_row_key_in_scope(key, &scope).expect("decode large HOT scan identity");
        let wide_rows = vec![(identity, Bytes::from(vec![0_u8; BUDGET]))];
        assert!(
            hot_scan_entries_fit_budget(HotScanEntries::Decoded(wide_rows), Some(BUDGET),)
                .is_none(),
            "retained payload bytes must govern fallback even for one row"
        );
    }

    #[test]
    fn columnar_base_coordinate_survives_repeated_hot_updates_and_tombstones() {
        let entity_pk = EntityPk::single("coordinated-row");
        let coordinate = ColumnarBaseCoordinate {
            state_set_id: crate::columnar_row_group::ArrowStateSetId::from_digest([7; 32]),
            group_index: 7,
            row_index: 31,
        };
        let previous = HeadValueRef {
            change_id: Some(ChangeId::for_test_label("coordinate-before-change")),
            commit_id: Some(CommitId::for_test_label("coordinate-before-commit")),
            untracked: false,
            deleted: false,
            created_at: timestamp(),
            updated_at: timestamp(),
            snapshot: JsonSlotRef::Inline("{}"),
            metadata: JsonSlotRef::None,
            columnar_base_coordinate: Some(coordinate),
        };
        let mut predecessor = CertifiedCurrentStatePredecessor::Encoded(Bytes::from(
            encode_head_value(&previous).expect("encode coordinated predecessor"),
        ));
        for deleted in [false, true] {
            let delta = CurrentStateDeltaRef {
                schema_key: "schema",
                file_id: None,
                entity_pk: &entity_pk,
                change_id: Some(ChangeId::for_test_label(if deleted {
                    "coordinate-delete-change"
                } else {
                    "coordinate-update-change"
                })),
                commit_id: Some(CommitId::for_test_label(if deleted {
                    "coordinate-delete-commit"
                } else {
                    "coordinate-update-commit"
                })),
                untracked: false,
                deleted,
                created_at: timestamp(),
                updated_at: timestamp(),
                snapshot: JsonSlotRef::Inline("{\"updated\":true}"),
                metadata: JsonSlotRef::None,
                columnar_base_coordinate: None,
            };
            let inherited = next_columnar_base_coordinate(&delta, Some(&predecessor))
                .expect("inherit coordinate");
            assert_eq!(inherited, Some(coordinate));
            let mut next = delta.value_ref(timestamp());
            next.columnar_base_coordinate = inherited;
            predecessor = CertifiedCurrentStatePredecessor::Encoded(Bytes::from(
                encode_head_value(&next).expect("encode repeated coordinated mutation"),
            ));
        }
        assert!(predecessor.view().expect("decode tombstone").deleted);
        assert_eq!(
            predecessor
                .view()
                .expect("decode tombstone coordinate")
                .columnar_base_coordinate,
            Some(coordinate)
        );
    }

    #[test]
    fn hot_mutation_keys_append_into_one_exact_arena() {
        let generation = CommitId::for_test_label("shared-hot-mutation-generation");
        let scope = hot_scope_prefix("branch", generation);
        let first_pk = EntityPk::single("first\0entity");
        let second_pk = EntityPk::single("second");
        let first = CurrentStateDeltaRef {
            schema_key: "schema\0escaped",
            file_id: Some("file\0id"),
            entity_pk: &first_pk,
            change_id: None,
            commit_id: None,
            untracked: true,
            deleted: false,
            created_at: timestamp(),
            updated_at: timestamp(),
            snapshot: JsonSlotRef::Inline("{}"),
            metadata: JsonSlotRef::None,
            columnar_base_coordinate: None,
        };
        let second = CurrentStateDeltaRef {
            schema_key: "schema_without_file",
            file_id: None,
            entity_pk: &second_pk,
            change_id: None,
            commit_id: None,
            untracked: true,
            deleted: false,
            created_at: timestamp(),
            updated_at: timestamp(),
            snapshot: JsonSlotRef::Inline("{}"),
            metadata: JsonSlotRef::None,
            columnar_base_coordinate: None,
        };
        let deltas = [&first, &second];
        let capacity = encoded_hot_mutation_identity_capacity(scope.len(), &deltas)
            .expect("test identities have a representable encoded size");
        let mut key_bytes = Vec::with_capacity(capacity);
        let allocation = key_bytes.as_ptr();
        let ranges = deltas
            .iter()
            .map(|delta| append_hot_mutation_identity(&mut key_bytes, &scope, delta))
            .collect::<Vec<_>>();

        assert_eq!(key_bytes.len(), capacity);
        assert_eq!(key_bytes.capacity(), capacity);
        assert_eq!(key_bytes.as_ptr(), allocation);
        assert_eq!(ranges[0].row_key.offset(), 0);
        assert_eq!(
            ranges[0]
                .file_schema_key
                .expect("first identity has a file schema marker")
                .offset()
                + ranges[0]
                    .file_schema_key
                    .expect("first identity has a file schema marker")
                    .len(),
            ranges[1].row_key.offset()
        );
        for (range, delta) in ranges.iter().zip(deltas) {
            let row_start = range.row_key.offset();
            let row = decode_hot_row_key_in_scope(
                &key_bytes[row_start..row_start + range.row_key.len()],
                &scope,
            )
            .expect("decode shared row key");
            assert_eq!(row.schema_key, delta.schema_key);
            assert_eq!(row.entity_pk, *delta.entity_pk);
            assert_eq!(row.file_id.as_deref(), delta.file_id);

            let scan_row = decode_hot_scan_row_key_in_scope(
                Bytes::copy_from_slice(&key_bytes[row_start..row_start + range.row_key.len()]),
                &scope,
            )
            .expect("decode shared row key for direct scan");
            assert_eq!(scan_row.schema_key(), delta.schema_key);
            assert_eq!(scan_row.entity_pk, *delta.entity_pk);
            assert_eq!(scan_row.file_id(), delta.file_id);
            assert_eq!(
                scan_row.owned_metadata_buffer_count(),
                usize::from(delta.schema_key.contains('\0'))
                    + usize::from(delta.file_id.is_some_and(|file_id| file_id.contains('\0'))),
                "only escaped metadata should take an owned fallback"
            );

            if let Some(marker) = range.file_schema_key {
                let marker_start = marker.offset();
                assert_eq!(
                    &key_bytes[marker_start..marker_start + marker.len()],
                    encode_hot_file_schema_key(&scope, delta.schema_key)
                );
            }
        }

        let encoded = encode_hot_mutation_identities("branch", generation, &deltas);
        assert_eq!(encoded.key_bytes.as_ref(), key_bytes);
        assert_eq!(encoded.key_ranges.len(), ranges.len());
        for (encoded, expected) in encoded.key_ranges.iter().zip(ranges) {
            assert_eq!(encoded.row_key, expected.row_key);
            assert_eq!(encoded.file_schema_key, expected.file_schema_key);
        }
    }

    #[test]
    fn hot_tracked_snapshot_clones_share_encoded_row_values() {
        let snapshot =
            HotTrackedSnapshot::from_materialized_rows(vec![MaterializedTrackedStateRow {
                entity_pk: EntityPk::single("entity"),
                schema_key: "schema".to_string(),
                file_id: None,
                snapshot_content: None,
                metadata: None,
                deleted: false,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                updated_at: "2026-01-01T00:00:00Z".to_string(),
                change_id: ChangeId::for_test_label("hot-shared-value-change"),
                commit_id: CommitId::for_test_label("hot-shared-value-commit"),
            }])
            .expect("encode tracked snapshot");
        let cloned = snapshot.clone();
        let source = snapshot.rows.values().next().expect("source encoded row");
        let retained = cloned.rows.values().next().expect("cloned encoded row");

        assert_eq!(source.as_ptr(), retained.as_ptr());
        assert_eq!(source.len(), retained.len());
    }

    #[test]
    fn hot_batch_staging_retains_encoded_key_and_value_arenas() {
        let key_bytes = Bytes::from_static(b"row-keymarker");
        let value_bytes = Bytes::from_static(b"value");
        let identities = EncodedHotMutationIdentities {
            key_bytes: key_bytes.clone(),
            key_ranges: vec![EncodedHotMutationIdentityRanges {
                row_key: BufferRange::new(0, 7),
                file_schema_key: Some(BufferRange::new(7, 6)),
            }],
        };
        let mut writes = StorageWriteSet::new();

        stage_hot_mutation_batch(&mut writes, identities, value_bytes, vec![Some(0..5)]);

        let stats = writes.arena_stats();
        assert_eq!(stats.spaces, 2);
        assert_eq!(stats.put_descriptors, 2);
        assert_eq!(stats.key_inline_bytes, 0);
        assert_eq!(stats.value_inline_bytes, 0);
        assert_eq!(stats.key_shared_buffers, 2);
        assert_eq!(stats.value_shared_buffers, 2);
    }

    #[tokio::test]
    async fn ordinary_incremental_import_skips_file_cascade_identity_index() {
        const DELTAS: usize = 4096;

        let storage = StorageAdapter::new(Memory::new());
        let read = crate::storage_adapter::SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("open ordinary incremental import read"),
        );
        let entity_pk = EntityPk::single("ordinary");
        let timestamp = timestamp();
        let delta = CurrentStateDeltaRef {
            schema_key: "ordinary_schema",
            file_id: Some("ordinary.json"),
            entity_pk: &entity_pk,
            change_id: None,
            commit_id: None,
            untracked: true,
            deleted: false,
            created_at: timestamp,
            updated_at: timestamp,
            snapshot: JsonSlotRef::Inline("{}"),
            metadata: JsonSlotRef::None,
            columnar_base_coordinate: None,
        };
        let deltas = vec![&delta; DELTAS];
        let generation = CommitId::for_test_label("ordinary-import-generation");
        let mut writes = StorageWriteSet::new();
        let mut retired_untracked_json_refs = BTreeSet::new();
        let explicit_index_builds = incremental_cascade_explicit_index_builds();

        stage_incremental_file_delete_cascades(
            &read,
            &mut writes,
            "ordinary-import",
            generation,
            &deltas,
            &mut retired_untracked_json_refs,
        )
        .await
        .expect("ordinary imports do not need file-delete cascade staging");

        assert_eq!(
            incremental_cascade_explicit_index_builds(),
            explicit_index_builds,
            "ordinary imports must return before allocating the batch-sized explicit identity index"
        );
        assert!(writes.is_empty());
    }

    #[tokio::test]
    async fn dense_mutation_identity_range_scan_matches_point_reads() {
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label("dense-mutation-generation");
        let entity_pks = (0..HOT_DENSE_SCAN_MIN_IDENTITIES)
            .map(|index| EntityPk::single(format!("{index:04}")))
            .collect::<Vec<_>>();
        let timestamp = timestamp();
        let deltas = entity_pks
            .iter()
            .map(|entity_pk| CurrentStateDeltaRef {
                schema_key: "schema",
                file_id: None,
                entity_pk,
                change_id: None,
                commit_id: None,
                untracked: true,
                deleted: false,
                created_at: timestamp,
                updated_at: timestamp,
                snapshot: JsonSlotRef::Inline("{}"),
                metadata: JsonSlotRef::None,
                columnar_base_coordinate: None,
            })
            .collect::<Vec<_>>();
        let delta_refs = deltas.iter().collect::<Vec<_>>();
        let encoded = encode_hot_mutation_identities("branch", generation, &delta_refs);
        let keys = encoded
            .key_ranges
            .iter()
            .map(|ranges| {
                let start = ranges.row_key.offset();
                StorageKey(
                    encoded
                        .key_bytes
                        .slice(start..start.saturating_add(ranges.row_key.len())),
                )
            })
            .collect::<Vec<_>>();
        let mut writes = StorageWriteSet::new();
        for key in &keys {
            writes.put(
                HOT_ROW_SPACE,
                key.clone(),
                StorageValue {
                    bytes: Bytes::from_static(b"row"),
                },
            );
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit dense mutation fixture");
        let read = crate::storage_adapter::SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("open dense mutation read"),
        );

        let dense = hot_scan_dense_mutation_identity_range(&read, &encoded)
            .await
            .expect("scan dense mutation range")
            .expect("dense mutation range should stay on the scan path");
        let point = PointReadPlan::new(HOT_ROW_SPACE, &keys)
            .materialize(&read, StorageGetOptions::default())
            .await
            .expect("point-read dense mutation identities")
            .value
            .into_iter()
            .map(|value| value.map(full_value_bytes).transpose())
            .collect::<Result<Vec<_>, _>>()
            .expect("decode dense mutation point reads");

        assert_eq!(dense, point);
        assert_eq!(
            dense.iter().flatten().count(),
            HOT_DENSE_SCAN_MIN_IDENTITIES
        );
    }

    #[tokio::test]
    async fn dense_identity_range_scan_returns_requested_rows() {
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label("dense-range-generation");
        let all_identities = (0..HOT_DENSE_SCAN_MIN_IDENTITIES * 2)
            .map(|index| diff_identity("branch", generation, &format!("{index:04}")))
            .collect::<Vec<_>>();
        let requested = all_identities
            .iter()
            .step_by(2)
            .cloned()
            .collect::<Vec<_>>();
        let requested_batch = FiniteHotIdentityBatchRef::new(
            "branch",
            generation,
            "schema",
            requested
                .iter()
                .map(|identity| &identity.entity_pk)
                .collect(),
            vec![None],
        )
        .expect("dense identity count is representable");
        let mut writes = StorageWriteSet::new();
        for identity in &all_identities {
            writes.put(
                HOT_ROW_SPACE,
                StorageKey(Bytes::from(encode_hot_row_key(identity))),
                StorageValue {
                    bytes: Bytes::from_static(b"row"),
                },
            );
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit dense-range fixture");
        let read = crate::storage_adapter::SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("open dense-range read"),
        );

        let dense = hot_scan_dense_identity_range(&read, &requested_batch)
            .await
            .expect("scan dense identity range")
            .expect("dense range should stay on the scan path");
        let point = hot_load_finite_identity_bytes(&read, &requested_batch)
            .await
            .expect("point-read the same dense identities");

        assert_eq!(dense, point);
        assert_eq!(dense.iter().flatten().count(), requested.len());
    }

    #[tokio::test]
    async fn sparse_identity_range_scan_returns_to_point_reads() {
        let storage = StorageAdapter::new(Memory::new());
        let generation = CommitId::for_test_label("sparse-range-generation");
        let all_identities = (0..HOT_DENSE_SCAN_MIN_IDENTITIES * 4)
            .map(|index| diff_identity("branch", generation, &format!("{index:04}")))
            .collect::<Vec<_>>();
        let requested = all_identities
            .iter()
            .step_by(4)
            .cloned()
            .collect::<Vec<_>>();
        let requested_batch = FiniteHotIdentityBatchRef::new(
            "branch",
            generation,
            "schema",
            requested
                .iter()
                .map(|identity| &identity.entity_pk)
                .collect(),
            vec![None],
        )
        .expect("sparse identity count is representable");
        let mut writes = StorageWriteSet::new();
        for identity in &all_identities {
            writes.put(
                HOT_ROW_SPACE,
                StorageKey(Bytes::from(encode_hot_row_key(identity))),
                StorageValue {
                    bytes: Bytes::from_static(b"row"),
                },
            );
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit sparse-range fixture");
        let read = crate::storage_adapter::SharedStorageAdapterRead::new(
            storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("open sparse-range read"),
        );

        let rows = hot_scan_dense_identity_range(&read, &requested_batch)
            .await
            .expect("probe sparse identity range");

        assert!(
            rows.is_none(),
            "sparse ranges must return to the exact point-read path"
        );
        let point = hot_load_finite_identity_bytes(&read, &requested_batch)
            .await
            .expect("point-read sparse identities after dense fallback");
        assert_eq!(point.iter().flatten().count(), requested.len());
    }
}
