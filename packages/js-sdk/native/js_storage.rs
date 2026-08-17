use std::future::Future;
use std::ops::Bound;
use std::pin::Pin;

use bytes::Bytes;
use lix::storage::{
    BeginScanOptions, Capability, CommitResult, CoreProjection, GetManyRequest, GetManyResult, Key,
    KeyRange, Precondition, PreconditionFailure, ProjectedValue, PutBatch, ReadConsistency,
    ReadDurability, ReadEntry, ReadOptions, ScanChunk, ScanCursor, ScanOrder, Storage,
    StorageError, StorageRead, StorageScanSource, StorageSpace, StorageWrite, ValueIntegrity,
    ValueSemantics, WriteOptions, WriteStats,
};
use serde::{Deserialize, Serialize};
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::JsFuture;

#[wasm_bindgen]
extern "C" {
    #[derive(Clone)]
    pub type JsStorageProvider;

    #[wasm_bindgen(method, js_name = beginRead)]
    fn begin_read(this: &JsStorageProvider, options: JsValue) -> js_sys::Promise;

    #[wasm_bindgen(method, js_name = beginWrite)]
    fn begin_write(this: &JsStorageProvider, options: JsValue) -> js_sys::Promise;

    #[wasm_bindgen(method)]
    pub fn close(this: &JsStorageProvider) -> js_sys::Promise;

    #[derive(Clone)]
    type JsStorageReadHandle;

    #[wasm_bindgen(method, js_name = snapshotCacheKey)]
    fn snapshot_cache_key(this: &JsStorageReadHandle) -> JsValue;

    #[wasm_bindgen(method, js_name = getMany)]
    fn get_many(this: &JsStorageReadHandle, requests: JsValue) -> js_sys::Promise;

    #[wasm_bindgen(method, js_name = beginScan)]
    fn begin_scan(
        this: &JsStorageReadHandle,
        space: JsValue,
        range: JsValue,
        options: JsValue,
    ) -> js_sys::Promise;

    #[derive(Clone)]
    type JsStorageScanHandle;

    #[wasm_bindgen(method, js_name = nextPage)]
    fn next_page(this: &JsStorageScanHandle, limit_rows: usize) -> js_sys::Promise;

    #[derive(Clone)]
    type JsStorageWriteHandle;

    #[wasm_bindgen(method, js_name = putMany)]
    fn put_many(this: &JsStorageWriteHandle, space: JsValue, entries: JsValue) -> js_sys::Promise;

    #[wasm_bindgen(method, js_name = deleteMany)]
    fn delete_many(this: &JsStorageWriteHandle, space: JsValue, keys: JsValue) -> js_sys::Promise;

    #[wasm_bindgen(method, js_name = deleteRange)]
    fn delete_range(this: &JsStorageWriteHandle, space: JsValue, range: JsValue)
    -> js_sys::Promise;

    #[wasm_bindgen(method)]
    fn commit(this: &JsStorageWriteHandle) -> js_sys::Promise;

    #[wasm_bindgen(method)]
    fn rollback(this: &JsStorageWriteHandle) -> js_sys::Promise;
}

#[derive(Clone)]
struct SendProvider(JsStorageProvider);

#[derive(Clone)]
struct SendRead(JsStorageReadHandle);

#[derive(Clone)]
struct SendScan(JsStorageScanHandle);

struct SendWrite(JsStorageWriteHandle);

// Browser WASM and every imported provider run on one dedicated worker. The
// engine traits retain Send/Sync so native adapters can use multithreaded
// executors; these wrappers never cross a browser worker boundary.
unsafe impl Send for SendProvider {}
unsafe impl Sync for SendProvider {}
unsafe impl Send for SendRead {}
unsafe impl Sync for SendRead {}
unsafe impl Send for SendScan {}
unsafe impl Send for SendWrite {}

struct SendJsFuture(JsFuture);

unsafe impl Send for SendJsFuture {}

impl Future for SendJsFuture {
    type Output = Result<JsValue, JsValue>;

    fn poll(
        mut self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        Pin::new(&mut self.0).poll(context)
    }
}

#[derive(Clone)]
pub struct JsStorage {
    provider: SendProvider,
}

pub struct JsStorageRead {
    handle: SendRead,
}

pub struct JsStorageWrite {
    handle: SendWrite,
}

struct JsStorageScanSource {
    handle: SendScan,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StorageSpaceDto {
    id: u32,
    name: &'static str,
    value_semantics: &'static str,
    value_integrity: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReadOptionsDto {
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot: Option<ByteDto>,
    consistency: &'static str,
    durability: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WriteOptionsDto {
    #[serde(skip_serializing_if = "Option::is_none")]
    base_snapshot: Option<ByteDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    idempotency_key: Option<ByteDto>,
    await_durable: bool,
    preconditions: Vec<PreconditionDto>,
    batch_capacity_hint_bytes: usize,
}

#[derive(Serialize)]
struct GetManyRequestDto {
    space: StorageSpaceDto,
    keys: Vec<ByteDto>,
    options: GetOptionsDto,
}

#[derive(Serialize)]
struct GetOptionsDto {
    projection: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BeginScanOptionsDto {
    projection: &'static str,
    order: &'static str,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum BoundDto {
    Unbounded,
    Included { key: ByteDto },
    Excluded { key: ByteDto },
}

#[derive(Serialize)]
struct KeyRangeDto {
    lower: BoundDto,
    upper: BoundDto,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum PreconditionDto {
    KeyAbsent {
        space: StorageSpaceDto,
        key: ByteDto,
    },
    KeyPresent {
        space: StorageSpaceDto,
        key: ByteDto,
    },
    KeyValueEquals {
        space: StorageSpaceDto,
        key: ByteDto,
        expected: ByteDto,
    },
    KeyValueHashEquals {
        space: StorageSpaceDto,
        key: ByteDto,
        hash: ByteDto,
    },
    RangeEmpty {
        space: StorageSpaceDto,
        range: KeyRangeDto,
    },
    BranchEquals {
        #[serde(rename = "refKey")]
        ref_key: ByteDto,
        expected: ByteDto,
    },
}

#[derive(Serialize)]
struct PutEntryDto {
    key: ByteDto,
    value: ByteDto,
}

#[derive(Serialize)]
#[serde(transparent)]
struct ByteDto(#[serde(with = "serde_bytes")] Vec<u8>);

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum ProjectedValueDto {
    KeyOnly,
    FullValue {
        #[serde(with = "serde_bytes")]
        value: Vec<u8>,
    },
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScanChunkDto {
    entries: Vec<ReadEntryDto>,
    has_more: bool,
}

#[derive(Deserialize)]
struct ReadEntryDto {
    #[serde(with = "serde_bytes")]
    key: Vec<u8>,
    value: ProjectedValueDto,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CommitResultDto {
    #[serde(default, with = "option_bytes")]
    commit_id: Option<Vec<u8>>,
    stats: WriteStatsDto,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WriteStatsDto {
    put_entries: u64,
    deleted_entries: u64,
    deleted_ranges: u64,
    written_bytes: u64,
    storage_calls: u64,
}

mod option_bytes {
    use serde::{Deserialize, Deserializer};
    use serde_bytes::ByteBuf;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<ByteBuf>::deserialize(deserializer).map(|value| value.map(ByteBuf::into_vec))
    }
}

impl std::fmt::Debug for JsStorage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("JsStorage").finish_non_exhaustive()
    }
}

impl JsStorage {
    pub fn new(provider: JsStorageProvider) -> Self {
        Self {
            provider: SendProvider(provider),
        }
    }

    pub async fn close(&self) -> Result<(), StorageError> {
        SendJsFuture(JsFuture::from(self.provider.0.close()))
            .await
            .map_err(storage_error)?;
        Ok(())
    }
}

impl Storage for JsStorage {
    type Read<'a>
        = JsStorageRead
    where
        Self: 'a;
    type Write<'a>
        = JsStorageWrite
    where
        Self: 'a;

    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        let options = to_js(&read_options_dto(options), "read options")?;
        let handle = SendJsFuture(JsFuture::from(self.provider.0.begin_read(options)))
            .await
            .map_err(storage_error)?
            .unchecked_into::<JsStorageReadHandle>();
        Ok(JsStorageRead {
            handle: SendRead(handle),
        })
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        let options = to_js(&write_options_dto(options), "write options")?;
        let handle = SendJsFuture(JsFuture::from(self.provider.0.begin_write(options)))
            .await
            .map_err(storage_error)?
            .unchecked_into::<JsStorageWriteHandle>();
        Ok(JsStorageWrite {
            handle: SendWrite(handle),
        })
    }
}

impl StorageRead for JsStorageRead {
    fn snapshot_cache_key(&self) -> Option<u128> {
        let value = self.handle.0.snapshot_cache_key();
        if value.is_null() || value.is_undefined() {
            return None;
        }
        value
            .as_string()
            .and_then(|value| value.parse::<u128>().ok())
    }

    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        let expected_values = requests
            .iter()
            .map(|request| request.keys.len())
            .sum::<usize>();
        let requests = requests
            .iter()
            .map(|request| GetManyRequestDto {
                space: storage_space_dto(request.space),
                keys: request
                    .keys
                    .iter()
                    .map(|key| ByteDto(key.0.to_vec()))
                    .collect(),
                options: GetOptionsDto {
                    projection: projection_name(request.opts.projection),
                },
            })
            .collect::<Vec<_>>();
        let requests = to_js(&requests, "get-many requests")?;
        let values = SendJsFuture(JsFuture::from(self.handle.0.get_many(requests)))
            .await
            .map_err(storage_error)?;
        let values: Vec<Option<ProjectedValueDto>> = from_js(values, "get-many result")?;
        if values.len() != expected_values {
            return Err(StorageError::Corruption(format!(
                "JS storage get-many returned {} values for {expected_values} requested keys",
                values.len()
            )));
        }
        Ok(GetManyResult::new(
            values
                .into_iter()
                .map(|value| value.map(projected_value))
                .collect(),
        ))
    }

    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        options: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        ScanCursor::validate_range(&range)?;
        let space_value = to_js(&storage_space_dto(space), "scan space")?;
        let range_value = to_js(&key_range_dto(&range), "scan range")?;
        let options_value = to_js(
            &BeginScanOptionsDto {
                projection: projection_name(options.projection),
                order: scan_order_name(options.order),
            },
            "scan options",
        )?;
        let handle = SendJsFuture(JsFuture::from(self.handle.0.begin_scan(
            space_value,
            range_value,
            options_value,
        )))
        .await
        .map_err(storage_error)?
        .unchecked_into::<JsStorageScanHandle>();
        Ok(ScanCursor::from_source(
            range.clone(),
            options.order,
            JsStorageScanSource {
                handle: SendScan(handle),
            },
        )?)
    }
}

impl StorageScanSource for JsStorageScanSource {
    fn next_page(
        &mut self,
        limit_rows: usize,
    ) -> Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
        Box::pin(async move {
            let page = SendJsFuture(JsFuture::from(self.handle.0.next_page(limit_rows)))
                .await
                .map_err(storage_error)?;
            let page: ScanChunkDto = from_js(page, "scan page")?;
            Ok(ScanChunk::new(
                page.entries
                    .into_iter()
                    .map(|entry| ReadEntry {
                        key: Key(Bytes::from(entry.key)),
                        value: projected_value(entry.value),
                    })
                    .collect(),
                page.has_more,
            ))
        })
    }
}

impl StorageWrite for JsStorageWrite {
    async fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        let space = to_js(&storage_space_dto(space), "put space")?;
        let entries = entries
            .entries
            .into_iter()
            .map(|entry| PutEntryDto {
                key: ByteDto(entry.key.0.to_vec()),
                value: ByteDto(entry.value.bytes.to_vec()),
            })
            .collect::<Vec<_>>();
        let entries = to_js(&entries, "put entries")?;
        SendJsFuture(JsFuture::from(self.handle.0.put_many(space, entries)))
            .await
            .map_err(storage_error)?;
        Ok(())
    }

    async fn delete_many(&mut self, space: StorageSpace, keys: &[Key]) -> Result<(), StorageError> {
        let space = to_js(&storage_space_dto(space), "delete space")?;
        let keys = keys
            .iter()
            .map(|key| ByteDto(key.0.to_vec()))
            .collect::<Vec<_>>();
        let keys = to_js(&keys, "delete keys")?;
        SendJsFuture(JsFuture::from(self.handle.0.delete_many(space, keys)))
            .await
            .map_err(storage_error)?;
        Ok(())
    }

    async fn delete_range(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageError> {
        let space = to_js(&storage_space_dto(space), "delete-range space")?;
        let range = to_js(&key_range_dto(&range), "delete range")?;
        SendJsFuture(JsFuture::from(self.handle.0.delete_range(space, range)))
            .await
            .map_err(storage_error)?;
        Ok(())
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        let result = SendJsFuture(JsFuture::from(self.handle.0.commit()))
            .await
            .map_err(storage_error)?;
        let result: CommitResultDto = from_js(result, "commit result")?;
        Ok(CommitResult {
            commit_id: result.commit_id.map(Bytes::from),
            stats: WriteStats {
                put_entries: result.stats.put_entries,
                deleted_entries: result.stats.deleted_entries,
                deleted_ranges: result.stats.deleted_ranges,
                written_bytes: result.stats.written_bytes,
                storage_calls: result.stats.storage_calls,
            },
        })
    }

    async fn rollback(self) -> Result<(), StorageError> {
        SendJsFuture(JsFuture::from(self.handle.0.rollback()))
            .await
            .map_err(storage_error)?;
        Ok(())
    }
}

fn storage_space_dto(space: StorageSpace) -> StorageSpaceDto {
    StorageSpaceDto {
        id: space.id.0,
        name: space.name,
        value_semantics: match space.value_semantics {
            ValueSemantics::Mutable => "mutable",
            ValueSemantics::Immutable => "immutable",
        },
        value_integrity: match space.value_integrity {
            ValueIntegrity::BackendVerified => "backendVerified",
            ValueIntegrity::ContentAddressed => "contentAddressed",
        },
    }
}

fn read_options_dto(options: ReadOptions) -> ReadOptionsDto {
    ReadOptionsDto {
        snapshot: options.snapshot.map(|value| ByteDto(value.0.to_vec())),
        consistency: match options.consistency {
            ReadConsistency::Snapshot => "snapshot",
            ReadConsistency::StaleOk => "staleOk",
            ReadConsistency::Latest => "latest",
        },
        durability: match options.durability {
            ReadDurability::Visible => "visible",
            ReadDurability::Durable => "durable",
        },
    }
}

fn write_options_dto(options: WriteOptions) -> WriteOptionsDto {
    WriteOptionsDto {
        base_snapshot: options.base_snapshot.map(|value| ByteDto(value.0.to_vec())),
        idempotency_key: options.idempotency_key.map(|value| ByteDto(value.to_vec())),
        await_durable: options.await_durable,
        preconditions: options.preconditions.iter().map(precondition_dto).collect(),
        batch_capacity_hint_bytes: options.batch_capacity_hint_bytes,
    }
}

fn precondition_dto(precondition: &Precondition) -> PreconditionDto {
    match precondition {
        Precondition::KeyAbsent { space, key } => PreconditionDto::KeyAbsent {
            space: storage_space_dto(*space),
            key: ByteDto(key.0.to_vec()),
        },
        Precondition::KeyPresent { space, key } => PreconditionDto::KeyPresent {
            space: storage_space_dto(*space),
            key: ByteDto(key.0.to_vec()),
        },
        Precondition::KeyValueEquals {
            space,
            key,
            expected,
        } => PreconditionDto::KeyValueEquals {
            space: storage_space_dto(*space),
            key: ByteDto(key.0.to_vec()),
            expected: ByteDto(expected.to_vec()),
        },
        Precondition::KeyValueHashEquals { space, key, hash } => {
            PreconditionDto::KeyValueHashEquals {
                space: storage_space_dto(*space),
                key: ByteDto(key.0.to_vec()),
                hash: ByteDto(hash.to_vec()),
            }
        }
        Precondition::RangeEmpty { space, range } => PreconditionDto::RangeEmpty {
            space: storage_space_dto(*space),
            range: key_range_dto(range),
        },
        Precondition::BranchEquals { ref_key, expected } => PreconditionDto::BranchEquals {
            ref_key: ByteDto(ref_key.0.to_vec()),
            expected: ByteDto(expected.to_vec()),
        },
    }
}

fn key_range_dto(range: &KeyRange) -> KeyRangeDto {
    KeyRangeDto {
        lower: bound_dto(&range.lower),
        upper: bound_dto(&range.upper),
    }
}

fn bound_dto(bound: &Bound<Key>) -> BoundDto {
    match bound {
        Bound::Unbounded => BoundDto::Unbounded,
        Bound::Included(key) => BoundDto::Included {
            key: ByteDto(key.0.to_vec()),
        },
        Bound::Excluded(key) => BoundDto::Excluded {
            key: ByteDto(key.0.to_vec()),
        },
    }
}

fn projection_name(projection: CoreProjection) -> &'static str {
    match projection {
        CoreProjection::KeyOnly => "keyOnly",
        CoreProjection::FullValue => "fullValue",
    }
}

fn scan_order_name(order: ScanOrder) -> &'static str {
    match order {
        ScanOrder::Ascending => "ascending",
        ScanOrder::Descending => "descending",
    }
}

fn projected_value(value: ProjectedValueDto) -> ProjectedValue {
    match value {
        ProjectedValueDto::KeyOnly => ProjectedValue::KeyOnly,
        ProjectedValueDto::FullValue { value } => ProjectedValue::FullValue(Bytes::from(value)),
    }
}

fn to_js<T: Serialize>(value: &T, label: &str) -> Result<JsValue, StorageError> {
    serde_wasm_bindgen::to_value(value)
        .map_err(|error| StorageError::Io(format!("could not encode JS storage {label}: {error}")))
}

fn from_js<T: for<'de> Deserialize<'de>>(value: JsValue, label: &str) -> Result<T, StorageError> {
    serde_wasm_bindgen::from_value(value).map_err(|error| {
        StorageError::Corruption(format!("could not decode JS storage {label}: {error}"))
    })
}

fn storage_error(error: JsValue) -> StorageError {
    let code = js_property_string(&error, "code");
    let message = js_property_string(&error, "message")
        .or_else(|| error.as_string())
        .unwrap_or_else(|| "JavaScript storage operation failed".to_string());
    match code.as_deref() {
        Some("LIX_STORAGE_INVALID_KEY") => StorageError::InvalidKey,
        Some("LIX_STORAGE_INVALID_CURSOR") => StorageError::InvalidCursor,
        Some("LIX_STORAGE_READ_EXPIRED") => StorageError::ReadExpired,
        Some("LIX_STORAGE_WRITE_CONFLICT") => StorageError::WriteConflict,
        Some("LIX_STORAGE_DURABILITY") => StorageError::Durability,
        Some("LIX_STORAGE_FENCED") => StorageError::Fenced,
        Some("LIX_STORAGE_CLOSED") => StorageError::Closed(message),
        Some("LIX_STORAGE_COMMIT_OUTCOME_UNKNOWN") => StorageError::CommitOutcomeUnknown(message),
        Some("LIX_STORAGE_CORRUPTION") => StorageError::Corruption(message),
        Some("LIX_STORAGE_PRECONDITION_FAILED") => {
            StorageError::PreconditionFailed(precondition_failures(&error))
        }
        Some("LIX_STORAGE_UNSUPPORTED") => StorageError::Unsupported(
            capability_from_error(&error).unwrap_or(Capability::Preconditions),
        ),
        _ => StorageError::Io(message),
    }
}

fn precondition_failures(error: &JsValue) -> Vec<PreconditionFailure> {
    let Ok(details) = js_sys::Reflect::get(error, &JsValue::from_str("details")) else {
        return Vec::new();
    };
    let Ok(failures) = js_sys::Reflect::get(&details, &JsValue::from_str("failures")) else {
        return Vec::new();
    };
    serde_wasm_bindgen::from_value::<Vec<PreconditionFailureDto>>(failures)
        .unwrap_or_default()
        .into_iter()
        .map(|failure| PreconditionFailure {
            index: failure.index,
        })
        .collect()
}

#[derive(Deserialize)]
struct PreconditionFailureDto {
    index: usize,
}

fn capability_from_error(error: &JsValue) -> Option<Capability> {
    let details = js_sys::Reflect::get(error, &JsValue::from_str("details")).ok()?;
    let capability = js_sys::Reflect::get(&details, &JsValue::from_str("capability"))
        .ok()?
        .as_string()?;
    match capability.as_str() {
        "envelopeProjection" => Some(Capability::EnvelopeProjection),
        "keyOrderedPoints" => Some(Capability::KeyOrderedPoints),
        "unorderedPoints" => Some(Capability::UnorderedPoints),
        "reverseScan" => Some(Capability::ReverseScan),
        "deleteRange" => Some(Capability::DeleteRange),
        "preconditions" => Some(Capability::Preconditions),
        "idempotentCommit" => Some(Capability::IdempotentCommit),
        "predicatePushdown" => Some(Capability::PredicatePushdown),
        "parallelPartitions" => Some(Capability::ParallelPartitions),
        _ => None,
    }
}

fn js_property_string(value: &JsValue, property: &str) -> Option<String> {
    js_sys::Reflect::get(value, &JsValue::from_str(property))
        .ok()?
        .as_string()
}
