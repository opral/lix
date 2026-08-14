//! Immutable, projection-addressable Arrow groups.
//!
//! This module owns a generic physical format. It deliberately knows nothing
//! about SQL plans, current-state visibility, or commit publication. Callers
//! provide a stable 16-byte owner and publish the returned immutable writes
//! inside their own atomic visibility transaction.

use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray,
};
use datafusion::arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::compute::{concat, concat_batches};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::LixError;
use crate::storage_adapter::{
    BufferRange, EncodedMutationBatch, EncodedPut, PointReadPlan, StorageAdapterRead,
    StorageGetOptions, StorageKey, StorageProjectedValue, StorageSpace, StorageSpaceId,
    StorageValue, StorageWriteSet, ValueSemantics,
};

pub(crate) const ROW_GROUP_MAX_ROWS: usize = 64 * 1024;
/// Independently compressed point-read unit inside a scan-oriented row group.
pub(crate) const ROW_GROUP_PAGE_ROWS: usize = 2 * 1024;
/// Backpressure budget expressed in projected physical column pages. Wider
/// projections therefore use smaller coordinate batches automatically.
const ROW_GROUP_POINT_READ_MAX_COLUMN_PAGES: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RowGroupRowLocation {
    pub(crate) group_index: u32,
    pub(crate) row_index: u32,
}
pub(crate) const ROW_GROUP_MANIFEST_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_0029),
    "row.columnar_row_group_manifest.v1",
    ValueSemantics::Immutable,
);
pub(crate) const ROW_GROUP_COLUMN_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_002a),
    "row.columnar_row_group_column.v1",
    ValueSemantics::Immutable,
);

const MANIFEST_MAGIC: &[u8; 8] = b"LXRGM004";
const COLUMN_MAGIC: &[u8; 8] = b"LXRGC001";
const COMPRESSED_MAGIC: &[u8; 8] = b"LXRGZ001";
const BLAKE3_DIGEST_LEN: usize = 32;
const MAX_DECODED_COLUMN_BYTES: usize = 256 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RowGroupSetId([u8; 16]);

impl RowGroupSetId {
    pub(crate) const fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub(crate) const fn as_bytes(self) -> [u8; 16] {
        self.0
    }

    fn manifest_key(self) -> StorageKey {
        StorageKey(Bytes::copy_from_slice(&self.0))
    }

    fn column_key(
        self,
        group_index: usize,
        page_index: usize,
        column_index: usize,
    ) -> Result<StorageKey, LixError> {
        let group_index = u32::try_from(group_index)
            .map_err(|_| row_group_error("row-group index exceeds u32"))?;
        let column_index = u16::try_from(column_index)
            .map_err(|_| row_group_error("row-group column index exceeds u16"))?;
        let page_index = u16::try_from(page_index)
            .map_err(|_| row_group_error("row-group page index exceeds u16"))?;
        let mut key = Vec::with_capacity(24);
        key.extend_from_slice(&self.0);
        key.extend_from_slice(&group_index.to_be_bytes());
        key.extend_from_slice(&page_index.to_be_bytes());
        key.extend_from_slice(&column_index.to_be_bytes());
        Ok(StorageKey(Bytes::from(key)))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum RowGroupDataType {
    String = 1,
    Int64 = 2,
    Float64 = 3,
    Boolean = 4,
    Json = 5,
}

impl RowGroupDataType {
    fn from_arrow(value: &DataType) -> Result<Self, LixError> {
        match value {
            DataType::Utf8 => Ok(Self::String),
            DataType::Int64 => Ok(Self::Int64),
            DataType::Float64 => Ok(Self::Float64),
            DataType::Boolean => Ok(Self::Boolean),
            other => Err(row_group_error(format!(
                "unsupported row-group Arrow type {other}"
            ))),
        }
    }

    fn from_field(field: &Field) -> Result<Self, LixError> {
        if field.data_type() == &DataType::Utf8
            && field.metadata().get("lix.value_type").map(String::as_str) == Some("json")
        {
            return Ok(Self::Json);
        }
        Self::from_arrow(field.data_type())
    }

    pub(crate) fn to_arrow(self) -> DataType {
        match self {
            Self::String => DataType::Utf8,
            Self::Int64 => DataType::Int64,
            Self::Float64 => DataType::Float64,
            Self::Boolean => DataType::Boolean,
            Self::Json => DataType::Utf8,
        }
    }

    fn decode(value: u8) -> Result<Self, LixError> {
        match value {
            1 => Ok(Self::String),
            2 => Ok(Self::Int64),
            3 => Ok(Self::Float64),
            4 => Ok(Self::Boolean),
            5 => Ok(Self::Json),
            _ => Err(row_group_error(
                "row-group manifest has an unknown column type",
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum RowGroupScalar {
    String(String),
    Int64(i64),
    Float64(f64),
    Boolean(bool),
}

impl PartialEq for RowGroupScalar {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(left), Self::String(right)) => left == right,
            (Self::Int64(left), Self::Int64(right)) => left == right,
            // Statistics are physical metadata. Bit equality makes NaNs and
            // signed zero round-trip without weakening corruption checks.
            (Self::Float64(left), Self::Float64(right)) => left.to_bits() == right.to_bits(),
            (Self::Boolean(left), Self::Boolean(right)) => left == right,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RowGroupColumnStatistics {
    pub(crate) null_count: u32,
    pub(crate) min: Option<RowGroupScalar>,
    pub(crate) max: Option<RowGroupScalar>,
    pub(crate) sum: Option<RowGroupScalar>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RowGroupStatistics {
    pub(crate) row_count: u32,
    pub(crate) columns: Vec<RowGroupColumnStatistics>,
    column_page_digests: Vec<Vec<[u8; BLAKE3_DIGEST_LEN]>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RowGroupField {
    pub(crate) name: String,
    pub(crate) data_type: RowGroupDataType,
    pub(crate) nullable: bool,
    pub(crate) metadata: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RowGroupManifest {
    pub(crate) namespace: String,
    pub(crate) metadata: HashMap<String, String>,
    pub(crate) fields: Vec<RowGroupField>,
    pub(crate) groups: Vec<RowGroupStatistics>,
    pub(crate) encoded_digest: [u8; BLAKE3_DIGEST_LEN],
}

impl RowGroupManifest {
    /// Content identity of the complete persisted manifest, including every
    /// column digest. Decoded-column caches use this in addition to the set
    /// identifier so corrupted or replaced manifests can never alias.
    pub(crate) fn content_digest(&self) -> Result<[u8; BLAKE3_DIGEST_LEN], LixError> {
        if self.encoded_digest != [0; BLAKE3_DIGEST_LEN] {
            return Ok(self.encoded_digest);
        }
        Ok(*blake3::hash(&encode_manifest(self)?).as_bytes())
    }

    pub(crate) fn row_count(&self) -> u64 {
        self.groups
            .iter()
            .map(|group| u64::from(group.row_count))
            .sum()
    }

    /// Only the codec tests reconstruct the full Arrow schema; every read path
    /// projects columns through `row_group_projected_schema` instead.
    #[cfg(test)]
    pub(crate) fn schema(&self) -> SchemaRef {
        let fields = self.fields.iter().map(|field| {
            Field::new(&field.name, field.data_type.to_arrow(), field.nullable)
                .with_metadata(field.metadata.clone())
        });
        Arc::new(Schema::new_with_metadata(
            fields.collect::<Vec<_>>(),
            self.metadata.clone(),
        ))
    }

    pub(crate) fn estimated_heap_bytes(&self) -> usize {
        fn map_bytes(values: &HashMap<String, String>) -> usize {
            values
                .capacity()
                .saturating_mul(size_of::<(String, String)>())
                .saturating_add(
                    values
                        .iter()
                        .map(|(key, value)| key.capacity().saturating_add(value.capacity()))
                        .sum(),
                )
        }

        fn scalar_bytes(value: &Option<RowGroupScalar>) -> usize {
            match value {
                Some(RowGroupScalar::String(value)) => value.capacity(),
                Some(
                    RowGroupScalar::Int64(_)
                    | RowGroupScalar::Float64(_)
                    | RowGroupScalar::Boolean(_),
                )
                | None => 0,
            }
        }

        self.namespace
            .capacity()
            .saturating_add(map_bytes(&self.metadata))
            .saturating_add(
                self.fields
                    .capacity()
                    .saturating_mul(size_of::<RowGroupField>()),
            )
            .saturating_add(
                self.fields
                    .iter()
                    .map(|field| {
                        field
                            .name
                            .capacity()
                            .saturating_add(map_bytes(&field.metadata))
                    })
                    .sum(),
            )
            .saturating_add(
                self.groups
                    .capacity()
                    .saturating_mul(size_of::<RowGroupStatistics>()),
            )
            .saturating_add(
                self.groups
                    .iter()
                    .map(|group| {
                        group
                            .columns
                            .capacity()
                            .saturating_mul(size_of::<RowGroupColumnStatistics>())
                            .saturating_add(
                                group
                                    .column_page_digests
                                    .capacity()
                                    .saturating_mul(size_of::<Vec<[u8; BLAKE3_DIGEST_LEN]>>())
                                    .saturating_add(
                                        group
                                            .column_page_digests
                                            .iter()
                                            .map(|digests| {
                                                digests.capacity().saturating_mul(size_of::<
                                                    [u8; BLAKE3_DIGEST_LEN],
                                                >(
                                                ))
                                            })
                                            .sum(),
                                    ),
                            )
                            .saturating_add(
                                group
                                    .columns
                                    .iter()
                                    .map(|column| {
                                        scalar_bytes(&column.min)
                                            .saturating_add(scalar_bytes(&column.max))
                                            .saturating_add(scalar_bytes(&column.sum))
                                    })
                                    .sum(),
                            )
                    })
                    .sum(),
            )
    }
}

#[derive(Clone, Debug)]
struct EncodedColumn {
    group_index: usize,
    page_index: usize,
    column_index: usize,
    value: BufferRange,
}

#[derive(Clone, Debug)]
pub(crate) struct EncodedRowGroupSet {
    pub(crate) manifest: RowGroupManifest,
    manifest_bytes: Bytes,
    columns: Vec<EncodedColumn>,
    column_values: Bytes,
}

impl EncodedRowGroupSet {
    #[cfg(test)]
    fn column_bytes(&self, column: &EncodedColumn) -> &[u8] {
        let start = column.value.offset();
        &self.column_values[start..start + column.value.len()]
    }
}

/// Whole-set load result. Production readers stream one group at a time via
/// `load_row_group_batch`; only the codec/storage tests materialize every
/// group at once.
#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct LoadedRowGroupSet {
    pub(crate) manifest: RowGroupManifest,
    pub(crate) batches: Vec<RecordBatch>,
}

/// Encodes a whole set in one shot. The commit path stages pre-encoded groups
/// through `stage_row_group_set`; this whole-set encoder is a test fixture
/// helper only.
#[cfg(test)]
pub(crate) fn encode_row_group_set(
    namespace: impl Into<String>,
    schema: SchemaRef,
    batches: &[RecordBatch],
) -> Result<EncodedRowGroupSet, LixError> {
    encode_row_group_set_impl(namespace.into(), schema, batches, false)
}

pub(crate) fn encode_row_group_set_preserving_batches(
    namespace: impl Into<String>,
    schema: SchemaRef,
    batches: &[RecordBatch],
) -> Result<EncodedRowGroupSet, LixError> {
    encode_row_group_set_impl(namespace.into(), schema, batches, true)
}

fn encode_row_group_set_impl(
    namespace: String,
    schema: SchemaRef,
    batches: &[RecordBatch],
    preserve_batch_boundaries: bool,
) -> Result<EncodedRowGroupSet, LixError> {
    validate_input_batches(&schema, batches)?;
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            Ok(RowGroupField {
                name: field.name().clone(),
                data_type: RowGroupDataType::from_field(field)?,
                nullable: field.is_nullable(),
                metadata: field.metadata().clone(),
            })
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    if fields.len() > usize::from(u16::MAX) {
        return Err(row_group_error(
            "row-group schema has more than u16 columns",
        ));
    }

    let row_groups = partition_batches(&schema, batches, preserve_batch_boundaries)?;
    let mut groups = Vec::with_capacity(row_groups.len());
    let mut columns = Vec::with_capacity(row_groups.len().saturating_mul(fields.len()));
    let mut column_values = Vec::new();
    for (group_index, batch) in row_groups.iter().enumerate() {
        let row_count = u32::try_from(batch.num_rows())
            .map_err(|_| row_group_error("row-group row count exceeds u32"))?;
        let mut statistics = Vec::with_capacity(fields.len());
        let mut column_page_digests = Vec::with_capacity(fields.len());
        for (column_index, (array, field)) in batch.columns().iter().zip(&fields).enumerate() {
            let stats = column_statistics(array, field.data_type)?;
            statistics.push(stats);
            let mut page_digests = Vec::with_capacity(array.len().div_ceil(ROW_GROUP_PAGE_ROWS));
            for (page_index, offset) in (0..array.len()).step_by(ROW_GROUP_PAGE_ROWS).enumerate() {
                let page = array.slice(offset, ROW_GROUP_PAGE_ROWS.min(array.len() - offset));
                let encoded = encode_column(&page, field.data_type)?;
                page_digests.push(*blake3::hash(&encoded).as_bytes());
                let value = BufferRange::new(column_values.len(), encoded.len());
                column_values.extend_from_slice(&encoded);
                columns.push(EncodedColumn {
                    group_index,
                    page_index,
                    column_index,
                    value,
                });
            }
            column_page_digests.push(page_digests);
        }
        groups.push(RowGroupStatistics {
            row_count,
            columns: statistics,
            column_page_digests,
        });
    }
    let mut manifest = RowGroupManifest {
        namespace,
        metadata: schema.metadata().clone(),
        fields,
        groups,
        encoded_digest: [0; BLAKE3_DIGEST_LEN],
    };
    let manifest_bytes = Bytes::from(encode_manifest(&manifest)?);
    manifest.encoded_digest = *blake3::hash(&manifest_bytes).as_bytes();
    Ok(EncodedRowGroupSet {
        manifest,
        manifest_bytes,
        columns,
        column_values: Bytes::from(column_values),
    })
}

pub(crate) fn stage_row_group_set(
    writes: &mut StorageWriteSet,
    id: RowGroupSetId,
    encoded: &EncodedRowGroupSet,
) -> Result<(), LixError> {
    writes.reserve_space(ROW_GROUP_MANIFEST_SPACE, 1, 0);
    let mut key_bytes = Vec::with_capacity(encoded.columns.len().saturating_mul(24));
    let mut puts = Vec::with_capacity(encoded.columns.len());
    for column in &encoded.columns {
        let key = id.column_key(column.group_index, column.page_index, column.column_index)?;
        let key_range = BufferRange::new(key_bytes.len(), key.0.len());
        key_bytes.extend_from_slice(&key.0);
        puts.push(EncodedPut {
            key: key_range,
            value: column.value,
        });
    }
    writes.stage_encoded_batch(
        ROW_GROUP_COLUMN_SPACE,
        EncodedMutationBatch::try_new(
            Bytes::from(key_bytes),
            encoded.column_values.clone(),
            puts,
            Vec::new(),
        )
        .map_err(|error| row_group_error(error.to_string()))?,
    );
    writes.put(
        ROW_GROUP_MANIFEST_SPACE,
        id.manifest_key(),
        StorageValue {
            bytes: encoded.manifest_bytes.clone(),
        },
    );
    Ok(())
}

pub(crate) async fn load_row_group_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    id: RowGroupSetId,
) -> Result<Option<RowGroupManifest>, LixError> {
    let result = PointReadPlan::new(ROW_GROUP_MANIFEST_SPACE, &[id.manifest_key()])
        .materialize(store, StorageGetOptions::default())
        .await?;
    let Some(value) = result.value.into_iter().next().flatten() else {
        return Ok(None);
    };
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(row_group_error("row-group manifest read omitted its value"));
    };
    decode_manifest(&bytes).map(Some)
}

/// Loads an immutable manifest through the transaction-local write overlay.
/// Row-group publication and current-state publication share one atomic write
/// set, so a serving descriptor must validate the exact bytes being staged
/// without requiring an intermediate storage commit.
pub(crate) fn load_staged_row_group_manifest(
    writes: &StorageWriteSet,
    id: RowGroupSetId,
) -> Result<Option<RowGroupManifest>, LixError> {
    if let Some(bytes) = writes.staged_value(ROW_GROUP_MANIFEST_SPACE, id.as_bytes().as_slice()) {
        return decode_manifest(&bytes).map(Some);
    }
    Ok(None)
}

/// Stages deletion of one immutable set and every addressed column. The
/// owning commit remains the lifecycle authority; repository GC invokes this
/// only after that commit leaves the reachable history graph.
pub(crate) async fn stage_delete_row_group_set(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    id: RowGroupSetId,
) -> Result<(), LixError> {
    let Some(manifest) = load_row_group_manifest(store, id).await? else {
        return Ok(());
    };
    writes.delete(ROW_GROUP_MANIFEST_SPACE, id.manifest_key());
    for group_index in 0..manifest.groups.len() {
        let page_count =
            (manifest.groups[group_index].row_count as usize).div_ceil(ROW_GROUP_PAGE_ROWS);
        for page_index in 0..page_count {
            for column_index in 0..manifest.fields.len() {
                writes.delete(
                    ROW_GROUP_COLUMN_SPACE,
                    id.column_key(group_index, page_index, column_index)?,
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) async fn load_row_group_set(
    store: &(impl StorageAdapterRead + ?Sized),
    id: RowGroupSetId,
    projection: &[usize],
) -> Result<Option<LoadedRowGroupSet>, LixError> {
    let Some(manifest) = load_row_group_manifest(store, id).await? else {
        return Ok(None);
    };
    let mut batches = Vec::with_capacity(manifest.groups.len());
    for group_index in 0..manifest.groups.len() {
        batches.push(load_row_group_batch(store, id, &manifest, group_index, projection).await?);
    }
    Ok(Some(LoadedRowGroupSet { manifest, batches }))
}

/// Loads exactly one row group and only its projected columns.
///
/// Keeping manifest acquisition separate lets a streaming execution source
/// fetch one bounded group per poll and apply statistics pruning before it
/// issues any column reads.
pub(crate) async fn load_row_group_batch(
    store: &(impl StorageAdapterRead + ?Sized),
    id: RowGroupSetId,
    manifest: &RowGroupManifest,
    group_index: usize,
    projection: &[usize],
) -> Result<RecordBatch, LixError> {
    validate_projection(manifest, projection)?;
    let group = manifest.groups.get(group_index).ok_or_else(|| {
        row_group_error(format!(
            "row-group index {group_index} is outside {} groups",
            manifest.groups.len()
        ))
    })?;
    let projected_schema = projected_schema(manifest, projection);
    if projection.is_empty() {
        return RecordBatch::try_new_with_options(
            projected_schema,
            Vec::new(),
            &RecordBatchOptions::new().with_row_count(Some(group.row_count as usize)),
        )
        .map_err(|error| row_group_error(error.to_string()));
    }

    let arrays = load_row_group_columns(store, id, manifest, group_index, projection).await?;
    RecordBatch::try_new(projected_schema, arrays)
        .map_err(|error| row_group_error(error.to_string()))
}

/// Loads, verifies, decompresses, and decodes exactly the requested columns.
/// The returned arrays follow `projection` order and retain no storage bytes.
pub(crate) async fn load_row_group_columns(
    store: &(impl StorageAdapterRead + ?Sized),
    id: RowGroupSetId,
    manifest: &RowGroupManifest,
    group_index: usize,
    projection: &[usize],
) -> Result<Vec<ArrayRef>, LixError> {
    validate_projection(manifest, projection)?;
    let group = manifest.groups.get(group_index).ok_or_else(|| {
        row_group_error(format!(
            "row-group index {group_index} is outside {} groups",
            manifest.groups.len()
        ))
    })?;
    if projection.is_empty() {
        return Ok(Vec::new());
    }
    let page_count = (group.row_count as usize).div_ceil(ROW_GROUP_PAGE_ROWS);
    let keys = projection
        .iter()
        .flat_map(|&column_index| {
            (0..page_count)
                .map(move |page_index| id.column_key(group_index, page_index, column_index))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let values = PointReadPlan::from_unique_keys(ROW_GROUP_COLUMN_SPACE, keys)
        .materialize(store, StorageGetOptions::default())
        .await?
        .value;
    let mut values = values.into_iter();
    let mut arrays = Vec::with_capacity(projection.len());
    for &column_index in projection {
        let field = &manifest.fields[column_index];
        let mut pages = Vec::with_capacity(page_count);
        for page_index in 0..page_count {
            let value = values.next().flatten().ok_or_else(|| {
                row_group_error(format!(
                    "row-group {group_index} page {page_index} column {column_index} is missing"
                ))
            })?;
            let StorageProjectedValue::FullValue(bytes) = value else {
                return Err(row_group_error("row-group column read omitted its value"));
            };
            let page_rows = ROW_GROUP_PAGE_ROWS
                .min(group.row_count as usize - page_index.saturating_mul(ROW_GROUP_PAGE_ROWS));
            pages.push(decode_verified_column(
                &bytes,
                group.column_page_digests[column_index][page_index],
                field.data_type,
                page_rows,
            )?);
        }
        let page_refs = pages.iter().map(|page| page.as_ref()).collect::<Vec<_>>();
        arrays.push(concat(&page_refs).map_err(|error| row_group_error(error.to_string()))?);
    }
    if values.next().is_some() {
        return Err(row_group_error(
            "row-group read returned excess column values",
        ));
    }
    Ok(arrays)
}

/// Loads one independently compressed page from one logical scan group.
pub(crate) async fn load_row_group_page(
    store: &(impl StorageAdapterRead + ?Sized),
    id: RowGroupSetId,
    manifest: &RowGroupManifest,
    group_index: usize,
    page_index: usize,
    projection: &[usize],
) -> Result<RecordBatch, LixError> {
    load_row_group_pages(
        store,
        id,
        manifest,
        &[(group_index, page_index)],
        projection,
    )
    .await?
    .pop()
    .ok_or_else(|| row_group_error("row-group page read returned no batch"))
}

/// Loads multiple pages from one immutable set in one backend point-read
/// batch. Caller order and duplicate coordinates are preserved.
pub(crate) async fn load_row_group_pages(
    store: &(impl StorageAdapterRead + ?Sized),
    id: RowGroupSetId,
    manifest: &RowGroupManifest,
    pages: &[(usize, usize)],
    projection: &[usize],
) -> Result<Vec<RecordBatch>, LixError> {
    let mut batches = Vec::with_capacity(pages.len());
    visit_row_group_pages(store, id, manifest, pages, projection, |_, batch| {
        batches.push(batch);
        Ok(())
    })
    .await?;
    Ok(batches)
}

/// Visits requested pages in caller order while bounding compressed and
/// decoded working state. A page batch is dropped before the next physical
/// read unless the caller deliberately retains it. A projection wider than
/// the budget still reads one indivisible coordinate at a time; the budget
/// limits physical working state rather than imposing a query-size policy.
pub(crate) async fn visit_row_group_pages(
    store: &(impl StorageAdapterRead + ?Sized),
    id: RowGroupSetId,
    manifest: &RowGroupManifest,
    pages: &[(usize, usize)],
    projection: &[usize],
    mut visit: impl FnMut((usize, usize), RecordBatch) -> Result<(), LixError>,
) -> Result<crate::storage_adapter::StorageReadStats, LixError> {
    validate_projection(manifest, projection)?;
    let mut stats = crate::storage_adapter::StorageReadStats::default();
    let coordinates_per_batch = if projection.is_empty() {
        ROW_GROUP_POINT_READ_MAX_COLUMN_PAGES
    } else {
        (ROW_GROUP_POINT_READ_MAX_COLUMN_PAGES / projection.len()).max(1)
    };
    for coordinate_batch in pages.chunks(coordinates_per_batch) {
        let mut page_row_counts = Vec::with_capacity(coordinate_batch.len());
        let mut keys = Vec::with_capacity(coordinate_batch.len().saturating_mul(projection.len()));
        for &(group_index, page_index) in coordinate_batch {
            let group = manifest.groups.get(group_index).ok_or_else(|| {
                row_group_error(format!(
                    "row-group index {group_index} is outside the manifest"
                ))
            })?;
            let page_count = (group.row_count as usize).div_ceil(ROW_GROUP_PAGE_ROWS);
            if page_index >= page_count {
                return Err(row_group_error(format!(
                    "row-group page index {page_index} is outside {page_count} pages"
                )));
            }
            page_row_counts
                .push(ROW_GROUP_PAGE_ROWS.min(
                    group.row_count as usize - page_index.saturating_mul(ROW_GROUP_PAGE_ROWS),
                ));
            for &column_index in projection {
                keys.push(id.column_key(group_index, page_index, column_index)?);
            }
        }
        let loaded = PointReadPlan::new(ROW_GROUP_COLUMN_SPACE, &keys)
            .materialize(store, StorageGetOptions::default())
            .await?;
        stats.add(loaded.stats);
        let mut values = loaded.value.into_iter();
        for (&(group_index, page_index), page_rows) in coordinate_batch.iter().zip(page_row_counts)
        {
            if projection.is_empty() {
                visit(
                    (group_index, page_index),
                    RecordBatch::try_new_with_options(
                        projected_schema(manifest, projection),
                        Vec::new(),
                        &RecordBatchOptions::new().with_row_count(Some(page_rows)),
                    )
                    .map_err(|error| row_group_error(error.to_string()))?,
                )?;
                continue;
            }
            let group = &manifest.groups[group_index];
            let mut arrays = Vec::with_capacity(projection.len());
            for &column_index in projection {
                let bytes = values
                    .next()
                    .flatten()
                    .and_then(|value| match value {
                        StorageProjectedValue::FullValue(bytes) => Some(bytes),
                        StorageProjectedValue::KeyOnly => None,
                    })
                    .ok_or_else(|| {
                        row_group_error(format!(
                            "row-group {group_index} page {page_index} column {column_index} is missing"
                        ))
                    })?;
                arrays.push(decode_verified_column(
                    &bytes,
                    group.column_page_digests[column_index][page_index],
                    manifest.fields[column_index].data_type,
                    page_rows,
                )?);
            }
            visit(
                (group_index, page_index),
                RecordBatch::try_new(projected_schema(manifest, projection), arrays)
                    .map_err(|error| row_group_error(error.to_string()))?,
            )?;
        }
        if values.next().is_some() {
            return Err(row_group_error(
                "row-group page read returned excess column values",
            ));
        }
    }
    Ok(stats)
}

/// Loads one page through ordinary storage plus the current atomic write set.
/// Only columns absent from the overlay are issued as physical point reads.
pub(crate) fn load_staged_row_group_page(
    writes: &StorageWriteSet,
    id: RowGroupSetId,
    manifest: &RowGroupManifest,
    group_index: usize,
    page_index: usize,
    projection: &[usize],
) -> Result<RecordBatch, LixError> {
    validate_projection(manifest, projection)?;
    let group = manifest.groups.get(group_index).ok_or_else(|| {
        row_group_error(format!(
            "row-group index {group_index} is outside the manifest"
        ))
    })?;
    let page_count = (group.row_count as usize).div_ceil(ROW_GROUP_PAGE_ROWS);
    if page_index >= page_count {
        return Err(row_group_error(format!(
            "row-group page index {page_index} is outside {page_count} pages"
        )));
    }
    let page_rows = ROW_GROUP_PAGE_ROWS
        .min(group.row_count as usize - page_index.saturating_mul(ROW_GROUP_PAGE_ROWS));
    let keys = projection
        .iter()
        .map(|&column_index| id.column_key(group_index, page_index, column_index))
        .collect::<Result<Vec<_>, _>>()?;
    let values = keys
        .iter()
        .map(|key| writes.staged_value(ROW_GROUP_COLUMN_SPACE, key.0.as_ref()))
        .collect::<Vec<_>>();
    let mut arrays = Vec::with_capacity(projection.len());
    for ((&column_index, bytes), key) in projection.iter().zip(values).zip(keys) {
        let bytes = bytes.ok_or_else(|| {
            row_group_error(format!(
                "row-group {group_index} page {page_index} column {column_index} is missing at {:?}",
                key.0
            ))
        })?;
        arrays.push(decode_verified_column(
            &bytes,
            group.column_page_digests[column_index][page_index],
            manifest.fields[column_index].data_type,
            page_rows,
        )?);
    }
    RecordBatch::try_new(projected_schema(manifest, projection), arrays)
        .map_err(|error| row_group_error(error.to_string()))
}

pub(crate) fn row_group_projected_schema(
    manifest: &RowGroupManifest,
    projection: &[usize],
) -> Result<SchemaRef, LixError> {
    validate_projection(manifest, projection)?;
    Ok(projected_schema(manifest, projection))
}

fn validate_input_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<(), LixError> {
    for batch in batches {
        if batch.schema().as_ref() != schema.as_ref() {
            return Err(row_group_error("row-group input batch schema mismatch"));
        }
    }
    Ok(())
}

fn partition_batches(
    schema: &SchemaRef,
    batches: &[RecordBatch],
    preserve_batch_boundaries: bool,
) -> Result<Vec<RecordBatch>, LixError> {
    if preserve_batch_boundaries {
        let mut groups = Vec::new();
        for batch in batches {
            for offset in (0..batch.num_rows()).step_by(ROW_GROUP_MAX_ROWS) {
                groups.push(batch.slice(offset, ROW_GROUP_MAX_ROWS.min(batch.num_rows() - offset)));
            }
        }
        return Ok(groups);
    }
    let mut groups = Vec::new();
    let mut pending = Vec::new();
    let mut pending_rows = 0;
    for batch in batches {
        let mut offset = 0;
        while offset < batch.num_rows() {
            let take = (ROW_GROUP_MAX_ROWS - pending_rows).min(batch.num_rows() - offset);
            pending.push(batch.slice(offset, take));
            pending_rows += take;
            offset += take;
            if pending_rows == ROW_GROUP_MAX_ROWS {
                groups.push(finish_group(schema, &mut pending)?);
                pending_rows = 0;
            }
        }
    }
    if pending_rows != 0 {
        groups.push(finish_group(schema, &mut pending)?);
    }
    Ok(groups)
}

fn finish_group(
    schema: &SchemaRef,
    pending: &mut Vec<RecordBatch>,
) -> Result<RecordBatch, LixError> {
    let group = if pending.len() == 1 {
        pending.pop().expect("one pending row-group batch")
    } else {
        concat_batches(schema, pending.iter())
            .map_err(|error| row_group_error(error.to_string()))?
    };
    pending.clear();
    Ok(group)
}

fn validate_projection(manifest: &RowGroupManifest, projection: &[usize]) -> Result<(), LixError> {
    let mut seen = vec![false; manifest.fields.len()];
    for &index in projection {
        let Some(slot) = seen.get_mut(index) else {
            return Err(row_group_error(format!(
                "row-group projection column {index} is outside the schema"
            )));
        };
        if std::mem::replace(slot, true) {
            return Err(row_group_error(format!(
                "row-group projection repeats column {index}"
            )));
        }
    }
    Ok(())
}

fn projected_schema(manifest: &RowGroupManifest, projection: &[usize]) -> SchemaRef {
    let fields = projection.iter().map(|&index| {
        let field = &manifest.fields[index];
        Field::new(&field.name, field.data_type.to_arrow(), field.nullable)
            .with_metadata(field.metadata.clone())
    });
    Arc::new(Schema::new_with_metadata(
        fields.collect::<Vec<_>>(),
        manifest.metadata.clone(),
    ))
}

fn encode_column(array: &ArrayRef, data_type: RowGroupDataType) -> Result<Vec<u8>, LixError> {
    let row_count = u32::try_from(array.len())
        .map_err(|_| row_group_error("row-group column length exceeds u32"))?;
    let validity = encode_validity(array.as_ref());
    let mut raw = Vec::new();
    raw.extend_from_slice(COLUMN_MAGIC);
    raw.push(data_type as u8);
    raw.extend_from_slice(&row_count.to_le_bytes());
    raw.extend_from_slice(
        &u32::try_from(validity.len())
            .map_err(|_| row_group_error("row-group validity length exceeds u32"))?
            .to_le_bytes(),
    );
    raw.extend_from_slice(&validity);
    match data_type {
        RowGroupDataType::String | RowGroupDataType::Json => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| row_group_error("row-group String column downcast failed"))?;
            let mut data = Vec::new();
            let mut offsets = Vec::with_capacity(values.len() + 1);
            offsets.push(0_i32);
            for index in 0..values.len() {
                if values.is_valid(index) {
                    if data_type == RowGroupDataType::Json {
                        let value = serde_json::from_str(values.value(index)).map_err(|error| {
                            row_group_error(format!("row-group JSON value is invalid: {error}"))
                        })?;
                        encode_binary_json(&value, &mut data)?;
                    } else {
                        data.extend_from_slice(values.value(index).as_bytes());
                    }
                }
                offsets.push(i32::try_from(data.len()).map_err(|_| {
                    row_group_error("row-group String data exceeds Arrow i32 offsets")
                })?);
            }
            raw.extend_from_slice(
                &u32::try_from(data.len())
                    .map_err(|_| row_group_error("row-group String data exceeds u32"))?
                    .to_le_bytes(),
            );
            for offset in offsets {
                raw.extend_from_slice(&offset.to_le_bytes());
            }
            raw.extend_from_slice(&data);
        }
        RowGroupDataType::Int64 => {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| row_group_error("row-group Int64 column downcast failed"))?;
            for index in 0..values.len() {
                raw.extend_from_slice(&values.value(index).to_le_bytes());
            }
        }
        RowGroupDataType::Float64 => {
            let values = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| row_group_error("row-group Float64 column downcast failed"))?;
            for index in 0..values.len() {
                raw.extend_from_slice(&values.value(index).to_bits().to_le_bytes());
            }
        }
        RowGroupDataType::Boolean => {
            let values = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| row_group_error("row-group Boolean column downcast failed"))?;
            let mut bits = vec![0_u8; values.len().div_ceil(8)];
            for index in 0..values.len() {
                if values.value(index) {
                    set_bit(&mut bits, index);
                }
            }
            raw.extend_from_slice(&bits);
        }
    }
    compress_column(&raw)
}

fn decode_column(
    encoded: &[u8],
    expected_type: RowGroupDataType,
    expected_rows: usize,
) -> Result<ArrayRef, LixError> {
    let raw = decompress_column(encoded)?;
    let mut cursor = Cursor::new(&raw);
    cursor.expect_magic(COLUMN_MAGIC, "row-group column")?;
    let observed_type = RowGroupDataType::decode(cursor.u8()?)?;
    if observed_type != expected_type {
        return Err(row_group_error(
            "row-group column type does not match the manifest",
        ));
    }
    let row_count = cursor.u32_le()? as usize;
    if row_count != expected_rows {
        return Err(row_group_error(
            "row-group column length does not match the manifest",
        ));
    }
    let validity_len = cursor.u32_le()? as usize;
    if validity_len != row_count.div_ceil(8) {
        return Err(row_group_error(
            "row-group validity bitmap has an invalid length",
        ));
    }
    let validity = cursor.bytes(validity_len)?.to_vec();
    clear_unused_bits_are_zero(&validity, row_count, "validity")?;
    let nulls = (validity.iter().any(|&byte| byte != u8::MAX) || row_count % 8 != 0)
        .then(|| NullBuffer::new(BooleanBuffer::new(Buffer::from(validity), 0, row_count)));

    let array: ArrayRef = match expected_type {
        RowGroupDataType::String | RowGroupDataType::Json => {
            let data_len = cursor.u32_le()? as usize;
            let offset_count = row_count
                .checked_add(1)
                .ok_or_else(|| row_group_error("row-group String offset count overflow"))?;
            let mut offsets = Vec::with_capacity(offset_count);
            for _ in 0..offset_count {
                offsets.push(cursor.i32_le()?);
            }
            if offsets.first() != Some(&0)
                || offsets.windows(2).any(|window| window[0] > window[1])
                || offsets
                    .last()
                    .copied()
                    .and_then(|value| usize::try_from(value).ok())
                    != Some(data_len)
            {
                return Err(row_group_error("row-group String offsets are invalid"));
            }
            let data = cursor.bytes(data_len)?;
            if expected_type == RowGroupDataType::Json {
                let mut rendered = Vec::with_capacity(data_len);
                let mut rendered_offsets = Vec::with_capacity(offset_count);
                rendered_offsets.push(0_i32);
                for index in 0..row_count {
                    if nulls.as_ref().is_none_or(|nulls| nulls.is_valid(index)) {
                        let start = usize::try_from(offsets[index]).map_err(|_| {
                            row_group_error("row-group JSON offset is negative")
                        })?;
                        let end = usize::try_from(offsets[index + 1]).map_err(|_| {
                            row_group_error("row-group JSON offset is negative")
                        })?;
                        let value = decode_binary_json(&data[start..end])?;
                        serde_json::to_writer(&mut rendered, &value).map_err(|error| {
                            row_group_error(format!("row-group JSON rendering failed: {error}"))
                        })?;
                    }
                    rendered_offsets.push(i32::try_from(rendered.len()).map_err(|_| {
                        row_group_error("row-group rendered JSON exceeds Arrow i32 offsets")
                    })?);
                }
                Arc::new(
                    StringArray::try_new(
                        OffsetBuffer::new(ScalarBuffer::from(rendered_offsets)),
                        Buffer::from(rendered),
                        nulls,
                    )
                    .map_err(|error| row_group_error(error.to_string()))?,
                )
            } else {
                Arc::new(
                    StringArray::try_new(
                        OffsetBuffer::new(ScalarBuffer::from(offsets)),
                        Buffer::from(data.to_vec()),
                        nulls,
                    )
                    .map_err(|error| row_group_error(error.to_string()))?,
                )
            }
        }
        RowGroupDataType::Int64 => {
            let mut values = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                values.push(cursor.i64_le()?);
            }
            Arc::new(Int64Array::new(ScalarBuffer::from(values), nulls))
        }
        RowGroupDataType::Float64 => {
            let mut values = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                values.push(f64::from_bits(cursor.u64_le()?));
            }
            Arc::new(Float64Array::new(ScalarBuffer::from(values), nulls))
        }
        RowGroupDataType::Boolean => {
            let bit_len = row_count.div_ceil(8);
            let values = cursor.bytes(bit_len)?.to_vec();
            clear_unused_bits_are_zero(&values, row_count, "Boolean values")?;
            Arc::new(BooleanArray::new(
                BooleanBuffer::new(Buffer::from(values), 0, row_count),
                nulls,
            ))
        }
    };
    if !cursor.is_empty() {
        return Err(row_group_error("row-group column has trailing bytes"));
    }
    Ok(array)
}

const JSON_NULL: u8 = 0;
const JSON_FALSE: u8 = 1;
const JSON_TRUE: u8 = 2;
const JSON_I64: u8 = 3;
const JSON_U64: u8 = 4;
const JSON_F64: u8 = 5;
const JSON_STRING: u8 = 6;
const JSON_ARRAY: u8 = 7;
const JSON_OBJECT: u8 = 8;

fn encode_binary_json(value: &serde_json::Value, output: &mut Vec<u8>) -> Result<(), LixError> {
    match value {
        serde_json::Value::Null => output.push(JSON_NULL),
        serde_json::Value::Bool(false) => output.push(JSON_FALSE),
        serde_json::Value::Bool(true) => output.push(JSON_TRUE),
        serde_json::Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                output.push(JSON_I64);
                output.extend_from_slice(&value.to_be_bytes());
            } else if let Some(value) = number.as_u64() {
                output.push(JSON_U64);
                output.extend_from_slice(&value.to_be_bytes());
            } else if let Some(value) = number.as_f64() {
                output.push(JSON_F64);
                output.extend_from_slice(&value.to_be_bytes());
            } else {
                return Err(row_group_error("row-group JSON number is not finite"));
            }
        }
        serde_json::Value::String(value) => {
            output.push(JSON_STRING);
            encode_binary_json_bytes(value.as_bytes(), output)?;
        }
        serde_json::Value::Array(values) => {
            output.push(JSON_ARRAY);
            encode_binary_json_len(values.len(), output)?;
            for value in values {
                encode_binary_json(value, output)?;
            }
        }
        serde_json::Value::Object(values) => {
            output.push(JSON_OBJECT);
            encode_binary_json_len(values.len(), output)?;
            let mut fields = values.iter().collect::<Vec<_>>();
            fields.sort_unstable_by(|left, right| left.0.cmp(right.0));
            for (name, value) in fields {
                encode_binary_json_bytes(name.as_bytes(), output)?;
                encode_binary_json(value, output)?;
            }
        }
    }
    Ok(())
}

fn encode_binary_json_len(value: usize, output: &mut Vec<u8>) -> Result<(), LixError> {
    let value = u32::try_from(value)
        .map_err(|_| row_group_error("row-group JSON length exceeds u32"))?;
    output.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn encode_binary_json_bytes(value: &[u8], output: &mut Vec<u8>) -> Result<(), LixError> {
    encode_binary_json_len(value.len(), output)?;
    output.extend_from_slice(value);
    Ok(())
}

fn decode_binary_json(encoded: &[u8]) -> Result<serde_json::Value, LixError> {
    let mut cursor = BinaryJsonCursor { encoded, position: 0 };
    let value = cursor.value()?;
    if cursor.position != encoded.len() {
        return Err(row_group_error("row-group JSON value has trailing bytes"));
    }
    Ok(value)
}

struct BinaryJsonCursor<'a> {
    encoded: &'a [u8],
    position: usize,
}

impl BinaryJsonCursor<'_> {
    fn value(&mut self) -> Result<serde_json::Value, LixError> {
        Ok(match self.byte()? {
            JSON_NULL => serde_json::Value::Null,
            JSON_FALSE => serde_json::Value::Bool(false),
            JSON_TRUE => serde_json::Value::Bool(true),
            JSON_I64 => serde_json::Value::Number(i64::from_be_bytes(self.eight()?).into()),
            JSON_U64 => serde_json::Value::Number(u64::from_be_bytes(self.eight()?).into()),
            JSON_F64 => serde_json::Number::from_f64(f64::from_be_bytes(self.eight()?))
                .map(serde_json::Value::Number)
                .ok_or_else(|| row_group_error("row-group JSON contains a non-finite number"))?,
            JSON_STRING => serde_json::Value::String(self.string()?.to_owned()),
            JSON_ARRAY => {
                let count = self.len()?;
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(self.value()?);
                }
                serde_json::Value::Array(values)
            }
            JSON_OBJECT => {
                let count = self.len()?;
                let mut values = serde_json::Map::new();
                let mut previous: Option<String> = None;
                for _ in 0..count {
                    let name = self.string()?.to_owned();
                    if previous.as_ref().is_some_and(|previous| previous >= &name) {
                        return Err(row_group_error(
                            "row-group JSON object keys are not strictly ordered",
                        ));
                    }
                    previous = Some(name.clone());
                    values.insert(name, self.value()?);
                }
                serde_json::Value::Object(values)
            }
            tag => return Err(row_group_error(format!("row-group JSON has unknown tag {tag}"))),
        })
    }

    fn byte(&mut self) -> Result<u8, LixError> {
        let value = self.encoded.get(self.position).copied().ok_or_else(|| {
            row_group_error("row-group JSON value is truncated")
        })?;
        self.position += 1;
        Ok(value)
    }

    fn eight(&mut self) -> Result<[u8; 8], LixError> {
        self.take(8)?.try_into().map_err(|_| row_group_error("row-group JSON value is truncated"))
    }

    fn len(&mut self) -> Result<usize, LixError> {
        let bytes: [u8; 4] = self.take(4)?.try_into().map_err(|_| {
            row_group_error("row-group JSON length is truncated")
        })?;
        Ok(u32::from_be_bytes(bytes) as usize)
    }

    fn string(&mut self) -> Result<&str, LixError> {
        let len = self.len()?;
        std::str::from_utf8(self.take(len)?)
            .map_err(|_| row_group_error("row-group JSON string is not UTF-8"))
    }

    fn take(&mut self, len: usize) -> Result<&[u8], LixError> {
        let end = self.position.checked_add(len).ok_or_else(|| {
            row_group_error("row-group JSON range overflowed")
        })?;
        let value = self.encoded.get(self.position..end).ok_or_else(|| {
            row_group_error("row-group JSON value is truncated")
        })?;
        self.position = end;
        Ok(value)
    }
}

fn decode_verified_column(
    encoded: &[u8],
    expected_digest: [u8; BLAKE3_DIGEST_LEN],
    expected_type: RowGroupDataType,
    expected_rows: usize,
) -> Result<ArrayRef, LixError> {
    if blake3::hash(encoded).as_bytes() != &expected_digest {
        return Err(row_group_error(
            "row-group compressed column digest does not match the manifest",
        ));
    }
    decode_column(encoded, expected_type, expected_rows)
}

fn encode_validity(array: &dyn Array) -> Vec<u8> {
    let mut validity = vec![0_u8; array.len().div_ceil(8)];
    for index in 0..array.len() {
        if array.is_valid(index) {
            set_bit(&mut validity, index);
        }
    }
    validity
}

fn set_bit(bits: &mut [u8], index: usize) {
    bits[index / 8] |= 1 << (index % 8);
}

fn clear_unused_bits_are_zero(bits: &[u8], len: usize, label: &str) -> Result<(), LixError> {
    let used = len % 8;
    if used != 0
        && bits
            .last()
            .is_some_and(|last| last & !((1_u8 << used) - 1) != 0)
    {
        return Err(row_group_error(format!(
            "row-group {label} bitmap has nonzero padding"
        )));
    }
    Ok(())
}

fn column_statistics(
    array: &ArrayRef,
    data_type: RowGroupDataType,
) -> Result<RowGroupColumnStatistics, LixError> {
    let null_count = u32::try_from(array.null_count())
        .map_err(|_| row_group_error("row-group null count exceeds u32"))?;
    let (min, max, sum) = match data_type {
        RowGroupDataType::String | RowGroupDataType::Json => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| row_group_error("row-group String statistics downcast failed"))?;
            let mut observed = values.iter().flatten();
            let Some(first) = observed.next() else {
                return Ok(RowGroupColumnStatistics {
                    null_count,
                    min: None,
                    max: None,
                    sum: None,
                });
            };
            let (mut min, mut max) = (first, first);
            for value in observed {
                if value < min {
                    min = value;
                }
                if value > max {
                    max = value;
                }
            }
            (
                Some(RowGroupScalar::String(min.to_owned())),
                Some(RowGroupScalar::String(max.to_owned())),
                None,
            )
        }
        RowGroupDataType::Int64 => {
            let values = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| row_group_error("row-group Int64 statistics downcast failed"))?;
            let mut observed = values.iter().flatten();
            let Some(first) = observed.next() else {
                return Ok(RowGroupColumnStatistics {
                    null_count,
                    min: None,
                    max: None,
                    sum: None,
                });
            };
            let (mut min, mut max, mut sum) = (first, first, Some(first));
            for value in observed {
                min = min.min(value);
                max = max.max(value);
                sum = sum.and_then(|sum| sum.checked_add(value));
            }
            (
                Some(RowGroupScalar::Int64(min)),
                Some(RowGroupScalar::Int64(max)),
                sum.map(RowGroupScalar::Int64),
            )
        }
        RowGroupDataType::Float64 => {
            let values = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| row_group_error("row-group Float64 statistics downcast failed"))?;
            let mut observed = values.iter().flatten();
            let Some(first) = observed.next() else {
                return Ok(RowGroupColumnStatistics {
                    null_count,
                    min: None,
                    max: None,
                    sum: None,
                });
            };
            let (mut min, mut max, mut sum) = (first, first, first);
            for value in observed {
                if value.total_cmp(&min).is_lt() {
                    min = value;
                }
                if value.total_cmp(&max).is_gt() {
                    max = value;
                }
                sum += value;
            }
            (
                Some(RowGroupScalar::Float64(min)),
                Some(RowGroupScalar::Float64(max)),
                Some(RowGroupScalar::Float64(sum)),
            )
        }
        RowGroupDataType::Boolean => {
            let values = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| row_group_error("row-group Boolean statistics downcast failed"))?;
            let mut observed = values.iter().flatten();
            let Some(first) = observed.next() else {
                return Ok(RowGroupColumnStatistics {
                    null_count,
                    min: None,
                    max: None,
                    sum: None,
                });
            };
            let (mut min, mut max) = (first, first);
            for value in observed {
                min &= value;
                max |= value;
            }
            (
                Some(RowGroupScalar::Boolean(min)),
                Some(RowGroupScalar::Boolean(max)),
                None,
            )
        }
    };
    Ok(RowGroupColumnStatistics {
        null_count,
        min,
        max,
        sum,
    })
}

/// Derive exact physical statistics from an already reconciled Arrow batch.
///
/// Columnar overlay scans use this after suppressing stale base rows. The
/// result has the same semantics as persisted row-group statistics but no
/// column digests because it describes an in-memory derived batch rather than
/// stored column payloads.
pub(crate) fn exact_record_batch_statistics(
    batch: &RecordBatch,
) -> Result<RowGroupStatistics, LixError> {
    let row_count = u32::try_from(batch.num_rows())
        .map_err(|_| row_group_error("record-batch row count exceeds u32"))?;
    let columns = batch
        .columns()
        .iter()
        .zip(batch.schema().fields())
        .map(|(array, field)| {
            let data_type = RowGroupDataType::from_arrow(field.data_type())?;
            column_statistics(array, data_type)
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    Ok(RowGroupStatistics {
        row_count,
        column_page_digests: vec![Vec::new(); columns.len()],
        columns,
    })
}

fn compress_column(raw: &[u8]) -> Result<Vec<u8>, LixError> {
    if raw.len() > MAX_DECODED_COLUMN_BYTES {
        return Err(row_group_error(
            "row-group decoded column exceeds the safety limit",
        ));
    }
    let compressed = crate::compression::compress_zstd_level_1(raw)
        .map_err(|error| row_group_error(format!("row-group compression failed: {error}")))?;
    let mut output = Vec::with_capacity(12 + compressed.len());
    output.extend_from_slice(COMPRESSED_MAGIC);
    output.extend_from_slice(
        &u32::try_from(raw.len())
            .map_err(|_| row_group_error("row-group decoded column exceeds u32"))?
            .to_le_bytes(),
    );
    output.extend_from_slice(&compressed);
    Ok(output)
}

fn decompress_column(encoded: &[u8]) -> Result<Vec<u8>, LixError> {
    let mut cursor = Cursor::new(encoded);
    cursor.expect_magic(COMPRESSED_MAGIC, "compressed row-group column")?;
    let decoded_len = cursor.u32_le()? as usize;
    if decoded_len > MAX_DECODED_COLUMN_BYTES {
        return Err(row_group_error(
            "row-group decoded column exceeds the safety limit",
        ));
    }
    crate::compression::decompress_zstd(cursor.remaining(), decoded_len)
        .map_err(|error| row_group_error(format!("row-group decompression failed: {error}")))
}

fn encode_manifest(manifest: &RowGroupManifest) -> Result<Vec<u8>, LixError> {
    let mut output = Vec::new();
    output.extend_from_slice(MANIFEST_MAGIC);
    put_string(&mut output, &manifest.namespace)?;
    put_metadata(&mut output, &manifest.metadata)?;
    put_u16_len(&mut output, manifest.fields.len(), "manifest field count")?;
    for field in &manifest.fields {
        put_string(&mut output, &field.name)?;
        output.push(field.data_type as u8);
        output.push(u8::from(field.nullable));
        put_metadata(&mut output, &field.metadata)?;
    }
    put_u32_len(&mut output, manifest.groups.len(), "manifest group count")?;
    for group in &manifest.groups {
        output.extend_from_slice(&group.row_count.to_le_bytes());
        if group.row_count as usize > ROW_GROUP_MAX_ROWS {
            return Err(row_group_error("manifest group exceeds the row limit"));
        }
        if group.columns.len() != manifest.fields.len() {
            return Err(row_group_error(
                "manifest statistics width does not match its schema",
            ));
        }
        if group.column_page_digests.len() != manifest.fields.len() {
            return Err(row_group_error(
                "manifest column digest width does not match its schema",
            ));
        }
        let page_count = (group.row_count as usize).div_ceil(ROW_GROUP_PAGE_ROWS);
        for ((stats, page_digests), field) in group
            .columns
            .iter()
            .zip(&group.column_page_digests)
            .zip(&manifest.fields)
        {
            if page_digests.len() != page_count {
                return Err(row_group_error(
                    "manifest column page digest count disagrees with its row count",
                ));
            }
            for digest in page_digests {
                output.extend_from_slice(digest);
            }
            output.extend_from_slice(&stats.null_count.to_le_bytes());
            put_optional_scalar(&mut output, stats.min.as_ref(), field.data_type)?;
            put_optional_scalar(&mut output, stats.max.as_ref(), field.data_type)?;
            put_optional_scalar(&mut output, stats.sum.as_ref(), field.data_type)?;
        }
    }
    let checksum = blake3::hash(&output);
    output.extend_from_slice(checksum.as_bytes());
    Ok(output)
}

fn decode_manifest(encoded: &[u8]) -> Result<RowGroupManifest, LixError> {
    let body_len = encoded
        .len()
        .checked_sub(BLAKE3_DIGEST_LEN)
        .ok_or_else(|| row_group_error("row-group manifest checksum is missing"))?;
    let (body, encoded_checksum) = encoded.split_at(body_len);
    if blake3::hash(body).as_bytes() != encoded_checksum {
        return Err(row_group_error("row-group manifest checksum mismatch"));
    }
    let mut cursor = Cursor::new(body);
    cursor.expect_magic(MANIFEST_MAGIC, "row-group manifest")?;
    let namespace = cursor.string()?;
    let metadata = cursor.metadata()?;
    let field_count = cursor.u16_le()? as usize;
    // Counts are encoded inside storage-controlled bytes. Do not turn a forged,
    // self-checksummed count into an unbounded allocation before the decoder has
    // established that the corresponding entries actually exist.
    let mut fields = Vec::with_capacity(field_count.min(1_024));
    for _ in 0..field_count {
        let name = cursor.string()?;
        let data_type = RowGroupDataType::decode(cursor.u8()?)?;
        let nullable = match cursor.u8()? {
            0 => false,
            1 => true,
            _ => return Err(row_group_error("row-group field nullable flag is invalid")),
        };
        fields.push(RowGroupField {
            name,
            data_type,
            nullable,
            metadata: cursor.metadata()?,
        });
    }
    let group_count = cursor.u32_le()? as usize;
    let mut groups = Vec::with_capacity(group_count.min(1_024));
    for _ in 0..group_count {
        let row_count = cursor.u32_le()?;
        if row_count == 0 || row_count as usize > ROW_GROUP_MAX_ROWS {
            return Err(row_group_error(
                "row-group manifest has an invalid group row count",
            ));
        }
        let mut columns = Vec::with_capacity(field_count);
        let page_count = (row_count as usize).div_ceil(ROW_GROUP_PAGE_ROWS);
        let mut column_page_digests = Vec::with_capacity(field_count);
        for field in &fields {
            let mut page_digests = Vec::with_capacity(page_count);
            for _ in 0..page_count {
                page_digests.push(
                    <[u8; BLAKE3_DIGEST_LEN]>::try_from(cursor.bytes(BLAKE3_DIGEST_LEN)?).map_err(
                        |_| row_group_error("row-group column digest has an invalid length"),
                    )?,
                );
            }
            let null_count = cursor.u32_le()?;
            if null_count > row_count {
                return Err(row_group_error(
                    "row-group null count exceeds its row count",
                ));
            }
            let min = cursor.optional_scalar(field.data_type)?;
            let max = cursor.optional_scalar(field.data_type)?;
            let sum = cursor.optional_scalar(field.data_type)?;
            if (min.is_none() || max.is_none()) != (null_count == row_count) {
                return Err(row_group_error(
                    "row-group min/max presence contradicts null count",
                ));
            }
            columns.push(RowGroupColumnStatistics {
                null_count,
                min,
                max,
                sum,
            });
            column_page_digests.push(page_digests);
        }
        groups.push(RowGroupStatistics {
            row_count,
            columns,
            column_page_digests,
        });
    }
    if !cursor.is_empty() {
        return Err(row_group_error("row-group manifest has trailing bytes"));
    }
    Ok(RowGroupManifest {
        namespace,
        metadata,
        fields,
        groups,
        encoded_digest: *blake3::hash(encoded).as_bytes(),
    })
}

fn put_optional_scalar(
    output: &mut Vec<u8>,
    scalar: Option<&RowGroupScalar>,
    expected: RowGroupDataType,
) -> Result<(), LixError> {
    let Some(scalar) = scalar else {
        output.push(0);
        return Ok(());
    };
    output.push(1);
    match (expected, scalar) {
        (RowGroupDataType::String | RowGroupDataType::Json, RowGroupScalar::String(value)) => {
            put_string(output, value)?
        }
        (RowGroupDataType::Int64, RowGroupScalar::Int64(value)) => {
            output.extend_from_slice(&value.to_le_bytes());
        }
        (RowGroupDataType::Float64, RowGroupScalar::Float64(value)) => {
            output.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        (RowGroupDataType::Boolean, RowGroupScalar::Boolean(value)) => {
            output.push(u8::from(*value));
        }
        _ => {
            return Err(row_group_error(
                "row-group scalar type does not match its field",
            ));
        }
    }
    Ok(())
}

fn put_metadata(output: &mut Vec<u8>, metadata: &HashMap<String, String>) -> Result<(), LixError> {
    let mut entries = metadata.iter().collect::<Vec<_>>();
    entries.sort_unstable();
    put_u16_len(output, entries.len(), "metadata entry count")?;
    for (key, value) in entries {
        put_string(output, key)?;
        put_string(output, value)?;
    }
    Ok(())
}

fn put_string(output: &mut Vec<u8>, value: &str) -> Result<(), LixError> {
    put_u32_len(output, value.len(), "string length")?;
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

fn put_u16_len(output: &mut Vec<u8>, len: usize, label: &str) -> Result<(), LixError> {
    output.extend_from_slice(
        &u16::try_from(len)
            .map_err(|_| row_group_error(format!("row-group {label} exceeds u16")))?
            .to_le_bytes(),
    );
    Ok(())
}

fn put_u32_len(output: &mut Vec<u8>, len: usize, label: &str) -> Result<(), LixError> {
    output.extend_from_slice(
        &u32::try_from(len)
            .map_err(|_| row_group_error(format!("row-group {label} exceeds u32")))?
            .to_le_bytes(),
    );
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
    fn remaining(&self) -> &'a [u8] {
        &self.bytes[self.offset..]
    }
    fn bytes(&mut self, len: usize) -> Result<&'a [u8], LixError> {
        let end = self
            .offset
            .checked_add(len)
            .filter(|&end| end <= self.bytes.len())
            .ok_or_else(|| row_group_error("row-group value is truncated"))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }
    fn expect_magic(&mut self, magic: &[u8], label: &str) -> Result<(), LixError> {
        if self.bytes(magic.len())? != magic {
            return Err(row_group_error(format!("{label} has invalid magic")));
        }
        Ok(())
    }
    fn u8(&mut self) -> Result<u8, LixError> {
        Ok(self.bytes(1)?[0])
    }
    fn u16_le(&mut self) -> Result<u16, LixError> {
        Ok(u16::from_le_bytes(
            self.bytes(2)?.try_into().expect("two-byte slice"),
        ))
    }
    fn u32_le(&mut self) -> Result<u32, LixError> {
        Ok(u32::from_le_bytes(
            self.bytes(4)?.try_into().expect("four-byte slice"),
        ))
    }
    fn i32_le(&mut self) -> Result<i32, LixError> {
        Ok(i32::from_le_bytes(
            self.bytes(4)?.try_into().expect("four-byte slice"),
        ))
    }
    fn i64_le(&mut self) -> Result<i64, LixError> {
        Ok(i64::from_le_bytes(
            self.bytes(8)?.try_into().expect("eight-byte slice"),
        ))
    }
    fn u64_le(&mut self) -> Result<u64, LixError> {
        Ok(u64::from_le_bytes(
            self.bytes(8)?.try_into().expect("eight-byte slice"),
        ))
    }
    fn string(&mut self) -> Result<String, LixError> {
        let len = self.u32_le()? as usize;
        String::from_utf8(self.bytes(len)?.to_vec())
            .map_err(|error| row_group_error(format!("row-group string is not UTF-8: {error}")))
    }
    fn metadata(&mut self) -> Result<HashMap<String, String>, LixError> {
        let count = self.u16_le()? as usize;
        let mut values = HashMap::with_capacity(count);
        for _ in 0..count {
            let key = self.string()?;
            let value = self.string()?;
            if values.insert(key, value).is_some() {
                return Err(row_group_error(
                    "row-group metadata contains a duplicate key",
                ));
            }
        }
        Ok(values)
    }
    fn optional_scalar(
        &mut self,
        data_type: RowGroupDataType,
    ) -> Result<Option<RowGroupScalar>, LixError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(match data_type {
                RowGroupDataType::String | RowGroupDataType::Json => {
                    RowGroupScalar::String(self.string()?)
                }
                RowGroupDataType::Int64 => RowGroupScalar::Int64(self.i64_le()?),
                RowGroupDataType::Float64 => {
                    RowGroupScalar::Float64(f64::from_bits(self.u64_le()?))
                }
                RowGroupDataType::Boolean => RowGroupScalar::Boolean(match self.u8()? {
                    0 => false,
                    1 => true,
                    _ => return Err(row_group_error("row-group Boolean statistic is invalid")),
                }),
            })),
            _ => Err(row_group_error("row-group scalar presence flag is invalid")),
        }
    }
}

fn row_group_error(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INTERNAL_ERROR, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    fn fixture(rows: usize) -> (SchemaRef, Vec<RecordBatch>) {
        let mut schema_metadata = HashMap::new();
        schema_metadata.insert("layout".to_string(), "typed".to_string());
        let mut field_metadata = HashMap::new();
        field_metadata.insert("semantic".to_string(), "identity".to_string());
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("name", DataType::Utf8, true).with_metadata(field_metadata),
                Field::new("ordinal", DataType::Int64, true),
                Field::new("score", DataType::Float64, true),
                Field::new("enabled", DataType::Boolean, true),
            ],
            schema_metadata,
        ));
        let names = (0..rows)
            .map(|index| (index % 11 != 0).then(|| format!("row-{index:05}")))
            .collect::<Vec<_>>();
        let ordinals = (0..rows)
            .map(|index| (index % 13 != 0).then_some(index as i64 - 20))
            .collect::<Vec<_>>();
        let scores = (0..rows)
            .map(|index| (index % 17 != 0).then_some(index as f64 * 0.25))
            .collect::<Vec<_>>();
        let enabled = (0..rows)
            .map(|index| (index % 19 != 0).then_some(index % 2 == 0))
            .collect::<Vec<_>>();
        let names: ArrayRef = Arc::new(StringArray::from(names));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                names,
                Arc::new(Int64Array::from(ordinals)),
                Arc::new(Float64Array::from(scores)),
                Arc::new(BooleanArray::from(enabled)),
            ],
        )
        .expect("fixture batch");
        // Exercise row groups that span input RecordBatch boundaries.
        let split = rows.min(7_000);
        let batches = if split == rows {
            vec![batch]
        } else {
            vec![batch.slice(0, split), batch.slice(split, rows - split)]
        };
        (schema, batches)
    }

    #[test]
    fn typed_codec_round_trips_multiple_groups_and_statistics() {
        let rows = ROW_GROUP_MAX_ROWS + 9;
        let (schema, batches) = fixture(rows);
        let encoded = encode_row_group_set("fixture", Arc::clone(&schema), &batches)
            .expect("encode row groups");
        assert_eq!(encoded.manifest.groups.len(), 2);
        assert_eq!(
            encoded.manifest.groups[0].row_count as usize,
            ROW_GROUP_MAX_ROWS
        );
        assert_eq!(encoded.manifest.groups[1].row_count, 9);
        assert_eq!(encoded.manifest.row_count(), rows as u64);
        assert_eq!(encoded.manifest.schema().as_ref(), schema.as_ref());
        assert_eq!(
            decode_manifest(&encoded.manifest_bytes).expect("decode manifest"),
            encoded.manifest
        );
        assert!(
            encoded.manifest.groups[0]
                .columns
                .iter()
                .all(|stats| stats.null_count > 0)
        );

        for column in &encoded.columns {
            let group = &encoded.manifest.groups[column.group_index];
            let field = &encoded.manifest.fields[column.column_index];
            let page_rows = ROW_GROUP_PAGE_ROWS.min(
                group.row_count as usize - column.page_index.saturating_mul(ROW_GROUP_PAGE_ROWS),
            );
            let column_bytes = encoded.column_bytes(column);
            let decoded =
                decode_column(column_bytes, field.data_type, page_rows).expect("decode column");
            assert_eq!(decoded.len(), page_rows);
            assert_eq!(
                *blake3::hash(column_bytes).as_bytes(),
                group.column_page_digests[column.column_index][column.page_index]
            );
        }
    }

    #[test]
    fn json_column_uses_canonical_binary_cells_and_fails_closed() {
        let mut metadata = HashMap::new();
        metadata.insert("lix.value_type".to_string(), "json".to_string());
        let schema = Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Utf8, true).with_metadata(metadata),
        ]));
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some(r#"{"z":[null,true,3],"a":{"nested":"value"}}"#),
            Some(r#"{"a":{"nested":"value"},"z":[null,true,3]}"#),
            Some("[]"),
            None,
        ]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![values])
            .expect("JSON fixture batch");
        let encoded = encode_row_group_set("json", schema, &[batch]).expect("encode JSON");
        assert_eq!(encoded.manifest.fields[0].data_type, RowGroupDataType::Json);
        let column = &encoded.columns[0];
        let decoded = decode_column(
            encoded.column_bytes(column),
            RowGroupDataType::Json,
            4,
        )
        .expect("decode JSON");
        let decoded = decoded
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("decoded JSON is surfaced as UTF-8");
        assert_eq!(decoded.value(0), decoded.value(1));
        assert_eq!(decoded.value(2), "[]");
        assert!(decoded.is_null(3));

        for malformed in [
            vec![],
            vec![99],
            vec![JSON_I64, 0],
            vec![JSON_STRING, 0, 0, 0, 2, b'a'],
            vec![JSON_ARRAY, 0, 0, 0, 1],
            vec![JSON_NULL, JSON_TRUE],
            vec![JSON_OBJECT, 0, 0, 0, 2, 0, 0, 0, 1, b'z', JSON_NULL,
                 0, 0, 0, 1, b'a', JSON_NULL],
        ] {
            assert!(
                decode_binary_json(&malformed).is_err(),
                "malformed JSON unexpectedly decoded: {malformed:?}"
            );
        }
    }

    #[tokio::test]
    async fn storage_load_projects_columns_and_preserves_metadata() {
        let rows = ROW_GROUP_MAX_ROWS + 3;
        let (schema, batches) = fixture(rows);
        let encoded = encode_row_group_set("fixture", schema, &batches).expect("encode");
        let adapter = StorageAdapter::new(Memory::new());
        let id = RowGroupSetId::new(*b"row-group-set-01");
        let mut writes = adapter.new_write_set();
        stage_row_group_set(&mut writes, id, &encoded).expect("stage");
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read");
        let loaded = load_row_group_set(&read, id, &[3, 0])
            .await
            .expect("load")
            .expect("present");
        assert_eq!(loaded.manifest.namespace, "fixture");
        assert_eq!(loaded.batches.len(), 2);
        assert_eq!(
            loaded
                .batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            rows
        );
        assert_eq!(loaded.batches[0].schema().field(0).name(), "enabled");
        assert_eq!(loaded.batches[0].schema().field(1).name(), "name");
        assert_eq!(
            loaded.batches[0]
                .schema()
                .field(1)
                .metadata()
                .get("semantic"),
            Some(&"identity".to_string())
        );
        let last = load_row_group_batch(&read, id, &loaded.manifest, 1, &[0])
            .await
            .expect("load one projected group");
        assert_eq!(last.num_rows(), 3);
        assert_eq!(last.num_columns(), 1);
        assert_eq!(last.schema().field(0).name(), "name");
        let pages =
            load_row_group_pages(&read, id, &loaded.manifest, &[(0, 0), (0, 1), (0, 0)], &[1])
                .await
                .expect("load projected pages in one batch");
        assert_eq!(
            pages.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
            vec![
                ROW_GROUP_PAGE_ROWS,
                ROW_GROUP_PAGE_ROWS,
                ROW_GROUP_PAGE_ROWS
            ]
        );
        assert_eq!(pages[0], pages[2], "duplicate coordinates preserve order");
        let all_page_coordinates = (0..loaded.manifest.groups.len())
            .flat_map(|group_index| {
                let page_count = (loaded.manifest.groups[group_index].row_count as usize)
                    .div_ceil(ROW_GROUP_PAGE_ROWS);
                (0..page_count).map(move |page_index| (group_index, page_index))
            })
            .collect::<Vec<_>>();
        assert!(
            all_page_coordinates.len() > ROW_GROUP_POINT_READ_MAX_COLUMN_PAGES,
            "fixture must cross the physical page backpressure boundary"
        );
        let mut visited = Vec::new();
        let visit_stats = visit_row_group_pages(
            &read,
            id,
            &loaded.manifest,
            &all_page_coordinates,
            &[0],
            |coordinate, batch| {
                visited.push((coordinate, batch.num_rows()));
                Ok(())
            },
        )
        .await
        .expect("visit pages across the backpressure boundary");
        assert_eq!(
            visited
                .iter()
                .map(|(coordinate, _)| *coordinate)
                .collect::<Vec<_>>(),
            all_page_coordinates
        );
        assert_eq!(
            visit_stats.storage_calls, 2,
            "33 projected pages must collapse from 33 calls to two bounded batches"
        );
        let full_projection = (0..loaded.manifest.fields.len()).collect::<Vec<_>>();
        let full_stats = visit_row_group_pages(
            &read,
            id,
            &loaded.manifest,
            &all_page_coordinates,
            &full_projection,
            |_, _| Ok(()),
        )
        .await
        .expect("visit fully projected pages with bounded batching");
        let coordinates_per_batch =
            (ROW_GROUP_POINT_READ_MAX_COLUMN_PAGES / full_projection.len()).max(1);
        assert_eq!(
            full_stats.storage_calls,
            all_page_coordinates.len().div_ceil(coordinates_per_batch) as u64
        );
        assert!(
            full_stats.storage_calls < all_page_coordinates.len() as u64,
            "full-page hydration must issue fewer calls than one request per page"
        );
        assert!(
            load_row_group_manifest(&read, RowGroupSetId::new([9; 16]))
                .await
                .expect("missing read")
                .is_none()
        );
    }

    #[tokio::test]
    async fn lifecycle_delete_removes_manifest_and_columns() {
        let (schema, batches) = fixture(ROW_GROUP_MAX_ROWS + 3);
        let encoded = encode_row_group_set("fixture", schema, &batches).expect("encode");
        let adapter = StorageAdapter::new(Memory::new());
        let id = RowGroupSetId::new(*b"row-group-set-03");
        let mut writes = adapter.new_write_set();
        stage_row_group_set(&mut writes, id, &encoded).expect("stage");
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit");

        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read");
        let mut deletes = adapter.new_write_set();
        stage_delete_row_group_set(&read, &mut deletes, id)
            .await
            .expect("stage lifecycle delete");
        drop(read);
        adapter
            .commit_write_set(deletes, StorageWriteOptions::default())
            .await
            .expect("commit lifecycle delete");

        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("verify read");
        assert!(
            load_row_group_manifest(&read, id)
                .await
                .expect("load deleted manifest")
                .is_none()
        );
        let column_keys = encoded
            .columns
            .iter()
            .map(|column| {
                id.column_key(column.group_index, column.page_index, column.column_index)
                    .expect("key")
            })
            .collect::<Vec<_>>();
        assert!(
            PointReadPlan::new(ROW_GROUP_COLUMN_SPACE, &column_keys)
                .materialize(&read, StorageGetOptions::default())
                .await
                .expect("load deleted columns")
                .value
                .into_iter()
                .all(|value| value.is_none())
        );
    }

    #[test]
    fn corrupt_values_fail_closed() {
        let (schema, batches) = fixture(32);
        let encoded = encode_row_group_set("fixture", schema, &batches).expect("encode");

        let mut bad_manifest = encoded.manifest_bytes.to_vec();
        bad_manifest[0] ^= 0xff;
        assert!(decode_manifest(&bad_manifest).is_err());

        let mut bad_manifest_checksum = encoded.manifest_bytes.to_vec();
        let checksum_index = bad_manifest_checksum.len() - 1;
        bad_manifest_checksum[checksum_index] ^= 0xff;
        assert!(decode_manifest(&bad_manifest_checksum).is_err());

        let mut truncated = encoded.column_bytes(&encoded.columns[0]).to_vec();
        truncated.truncate(truncated.len() / 2);
        assert!(decode_column(&truncated, RowGroupDataType::String, 32).is_err());

        let mut oversized = Vec::from(COMPRESSED_MAGIC);
        oversized.extend_from_slice(&u32::MAX.to_le_bytes());
        oversized.extend_from_slice(b"not-zstd");
        assert!(decode_column(&oversized, RowGroupDataType::String, 32).is_err());

        let mut forged_count = Vec::from(MANIFEST_MAGIC);
        forged_count.extend_from_slice(&0_u32.to_le_bytes()); // namespace
        forged_count.extend_from_slice(&0_u16.to_le_bytes()); // metadata
        forged_count.extend_from_slice(&0_u16.to_le_bytes()); // fields
        forged_count.extend_from_slice(&u32::MAX.to_le_bytes()); // groups
        let checksum = blake3::hash(&forged_count);
        forged_count.extend_from_slice(checksum.as_bytes());
        assert!(decode_manifest(&forged_count).is_err());
    }

    #[test]
    fn manifest_stat_corruption_fails_closed() {
        let (schema, batches) = fixture(32);
        let encoded = encode_row_group_set("fixture", schema, &batches).expect("encode");

        // The final byte before the checksum is part of the final column's
        // persisted statistics. The checksum rejects the modified statistic
        // before the manifest parser can trust it.
        let mut bad_manifest_stat = encoded.manifest_bytes.to_vec();
        let stat_index = bad_manifest_stat.len() - BLAKE3_DIGEST_LEN - 1;
        bad_manifest_stat[stat_index] ^= 0xff;
        assert!(decode_manifest(&bad_manifest_stat).is_err());
    }

    #[tokio::test]
    async fn column_byte_corruption_fails_closed_during_load() {
        let (schema, batches) = fixture(32);
        let mut encoded = encode_row_group_set("fixture", schema, &batches).expect("encode");

        let corrupt_range = encoded.columns[0].value;
        let mut column_values = encoded.column_values.to_vec();
        let corrupt_index = corrupt_range.offset() + corrupt_range.len() - 1;
        column_values[corrupt_index] ^= 0xff;
        encoded.column_values = Bytes::from(column_values);

        let adapter = StorageAdapter::new(Memory::new());
        let id = RowGroupSetId::new(*b"row-group-set-02");
        let mut writes = adapter.new_write_set();
        stage_row_group_set(&mut writes, id, &encoded).expect("stage corrupt column");
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit corrupt column");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read");
        let manifest = load_row_group_manifest(&read, id)
            .await
            .expect("load authenticated manifest")
            .expect("manifest present");
        assert!(
            load_row_group_batch(&read, id, &manifest, 0, &[0])
                .await
                .is_err()
        );
    }

    #[test]
    fn rejects_unsupported_types_and_duplicate_projection() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "small",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::new_empty(Arc::clone(&schema));
        assert!(encode_row_group_set("fixture", schema, &[batch]).is_err());

        let (schema, batches) = fixture(1);
        let encoded = encode_row_group_set("fixture", schema, &batches).expect("encode");
        assert!(validate_projection(&encoded.manifest, &[0, 0]).is_err());
        assert!(validate_projection(&encoded.manifest, &[4]).is_err());
    }
}
