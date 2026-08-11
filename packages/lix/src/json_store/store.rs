use crate::LixError;
use crate::json_store::compression::{JsonBatchCompressor, decode_json_zstd_payload};
use crate::json_store::encoded::JsonCodec;
use crate::json_store::types::{JsonReadScopeRef, JsonRef};
use crate::storage_adapter::{PointReadPlan, StorageAdapterRead, StorageSpace, ValueSemantics};
use crate::storage_adapter::{
    StorageGetOptions, StorageKey, StorageProjectedValue, StorageSpaceId,
};
use bytes::Bytes;
use std::collections::{HashMap, hash_map::Entry};
use std::ops::Range;

pub(crate) const JSON_NAMESPACE: &str = "json_store.json";
pub(crate) const JSON_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0002_0001),
    JSON_NAMESPACE,
    ValueSemantics::Mutable,
);
const STORED_JSON_MAGIC: &[u8] = b"lix-json:v1";
const STORED_JSON_HEADER_LEN: usize = STORED_JSON_MAGIC.len() + 1 + 8;
/// Compression floor. Payloads at or under the inline threshold never
/// reach the store, so everything here is at least mid-size JSON, which
/// compresses individually; the savings guard below keeps poor compressors
/// raw. The historical 16 KiB floor predates inline payloads, when the
/// store was dominated by ~127-byte rows that zstd inflates.
const ZSTD_MIN_JSON_BYTES: usize = 512;
const MIN_ZSTD_SAVINGS_BYTES: usize = 128;

/// Capacity facts collected while JSON-store payloads are deduplicated.
///
/// The raw value size is a safe upper bound because compressed frames are
/// selected only when they are smaller than their source. The encoder uses a
/// representative first-row ratio for its normal one-allocation arena and can
/// grow once to this bound for a heterogeneous batch.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct StoredJsonBatchPlan {
    row_count: usize,
    raw_value_capacity: usize,
    compressible_payload_bytes: usize,
    max_compressible_payload_len: usize,
}

impl StoredJsonBatchPlan {
    pub(crate) fn push_json(&mut self, json: &str) -> Result<(), LixError> {
        self.row_count = self
            .row_count
            .checked_add(1)
            .ok_or_else(|| LixError::unknown("JSON-store batch row count overflow"))?;
        self.raw_value_capacity = self
            .raw_value_capacity
            .checked_add(STORED_JSON_HEADER_LEN)
            .and_then(|capacity| capacity.checked_add(json.len()))
            .ok_or_else(|| {
                LixError::unknown("JSON-store value batch exceeds addressable memory")
            })?;
        if json.len() >= ZSTD_MIN_JSON_BYTES {
            self.compressible_payload_bytes = self
                .compressible_payload_bytes
                .checked_add(json.len())
                .ok_or_else(|| {
                    LixError::unknown("JSON-store value batch exceeds addressable memory")
                })?;
            self.max_compressible_payload_len = self.max_compressible_payload_len.max(json.len());
        }
        Ok(())
    }

    pub(crate) fn encoder(self) -> Result<StoredJsonBatchEncoder, LixError> {
        if self.raw_value_capacity > isize::MAX.unsigned_abs() {
            return Err(LixError::unknown(
                "JSON-store value batch exceeds addressable memory",
            ));
        }
        Ok(StoredJsonBatchEncoder {
            plan: self,
            value_bytes: Vec::new(),
            compressor: JsonBatchCompressor::with_max_input_len(self.max_compressible_payload_len)?,
            appended_rows: 0,
            arena_initialized: false,
            used_raw_bound_fallback: false,
            #[cfg(test)]
            value_arena_allocations: 0,
        })
    }

    fn projected_value_capacity(self, sample_raw_len: usize, sample_selected_len: usize) -> usize {
        let fixed_bytes = self
            .raw_value_capacity
            .saturating_sub(self.compressible_payload_bytes);
        let projected_compressible_bytes = if self.compressible_payload_bytes == 0 {
            0
        } else if sample_raw_len >= ZSTD_MIN_JSON_BYTES {
            scale_ratio_ceil(
                self.compressible_payload_bytes,
                sample_selected_len,
                sample_raw_len,
            )
        } else {
            // A leading 257-511 byte out-of-band payload cannot provide a zstd
            // ratio. Start at a conservative half-size estimate and retain the
            // same one-fallback guarantee as a heterogeneous compressed batch.
            self.compressible_payload_bytes.div_ceil(2)
        };
        let headroom = projected_compressible_bytes
            .div_ceil(4)
            .max(self.row_count.saturating_mul(8));
        let projected_compressible_bytes = projected_compressible_bytes
            .saturating_add(headroom)
            .min(self.compressible_payload_bytes);
        fixed_bytes
            .saturating_add(projected_compressible_bytes)
            .max(STORED_JSON_HEADER_LEN.saturating_add(sample_selected_len))
            .min(self.raw_value_capacity)
    }
}

fn scale_ratio_ceil(total: usize, numerator: usize, denominator: usize) -> usize {
    debug_assert_ne!(denominator, 0);
    debug_assert!(numerator <= denominator);
    let scaled = (total as u128)
        .saturating_mul(numerator as u128)
        .div_ceil(denominator as u128);
    usize::try_from(scaled).unwrap_or(total).min(total)
}

/// Encodes all JSON-store values into one shared arena.
///
/// zstd output is produced once per eligible row into the compressor's
/// reusable max-row scratch. The selected frame (or borrowed raw JSON) is
/// copied immediately into this arena and represented by an exact range.
pub(crate) struct StoredJsonBatchEncoder {
    plan: StoredJsonBatchPlan,
    value_bytes: Vec<u8>,
    compressor: JsonBatchCompressor,
    appended_rows: usize,
    arena_initialized: bool,
    used_raw_bound_fallback: bool,
    #[cfg(test)]
    value_arena_allocations: usize,
}

impl std::fmt::Debug for StoredJsonBatchEncoder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoredJsonBatchEncoder")
            .field("plan", &self.plan)
            .field("value_bytes_len", &self.value_bytes.len())
            .field("value_bytes_capacity", &self.value_bytes.capacity())
            .field("compressor", &self.compressor)
            .field("appended_rows", &self.appended_rows)
            .field("arena_initialized", &self.arena_initialized)
            .field("used_raw_bound_fallback", &self.used_raw_bound_fallback)
            .finish()
    }
}

impl StoredJsonBatchEncoder {
    pub(crate) fn append_json_with_ref(
        &mut self,
        json: &str,
        json_ref: JsonRef,
    ) -> Result<Range<usize>, LixError> {
        debug_assert_eq!(JsonRef::for_content(json.as_bytes()), json_ref);
        let raw_data = json.as_bytes();
        let (codec, selected_data) = if raw_data.len() >= ZSTD_MIN_JSON_BYTES {
            let compressed = self.compressor.compress(raw_data)?;
            if raw_data.len().saturating_sub(compressed.len()) >= MIN_ZSTD_SAVINGS_BYTES {
                (JsonCodec::Zstd, compressed)
            } else {
                (JsonCodec::Raw, raw_data)
            }
        } else {
            (JsonCodec::Raw, raw_data)
        };

        let selected_len = selected_data.len();
        let uncompressed_len = u64::try_from(raw_data.len()).map_err(|_| {
            LixError::unknown("JSON-store payload length exceeds its storage header")
        })?;
        let encoded_len = STORED_JSON_HEADER_LEN
            .checked_add(selected_len)
            .ok_or_else(|| {
                LixError::unknown("JSON-store value batch exceeds addressable memory")
            })?;
        let required_len = self
            .value_bytes
            .len()
            .checked_add(encoded_len)
            .ok_or_else(|| {
                LixError::unknown("JSON-store value batch exceeds addressable memory")
            })?;
        if required_len > self.plan.raw_value_capacity {
            return Err(LixError::unknown(
                "JSON-store value batch exceeds planned raw bound",
            ));
        }

        if !self.arena_initialized {
            let initial_capacity = self
                .plan
                .projected_value_capacity(raw_data.len(), selected_len);
            self.value_bytes = Vec::with_capacity(initial_capacity);
            self.arena_initialized = true;
            #[cfg(test)]
            {
                self.value_arena_allocations += usize::from(initial_capacity != 0);
            }
        }
        if required_len > self.value_bytes.capacity() {
            debug_assert!(
                !self.used_raw_bound_fallback,
                "the raw value bound must cover every selected payload"
            );
            let additional = self
                .plan
                .raw_value_capacity
                .checked_sub(self.value_bytes.len())
                .ok_or_else(|| {
                    LixError::unknown("JSON-store value batch exceeds planned raw bound")
                })?;
            self.value_bytes.reserve_exact(additional);
            self.used_raw_bound_fallback = true;
            #[cfg(test)]
            {
                self.value_arena_allocations += 1;
            }
        }

        let value_offset = self.value_bytes.len();
        self.value_bytes.extend_from_slice(STORED_JSON_MAGIC);
        self.value_bytes.push(json_codec_byte(codec));
        self.value_bytes
            .extend_from_slice(&uncompressed_len.to_be_bytes());
        self.value_bytes.extend_from_slice(selected_data);
        self.appended_rows += 1;
        debug_assert_eq!(self.value_bytes.len(), required_len);
        debug_assert!(self.value_bytes.len() <= self.plan.raw_value_capacity);
        Ok(value_offset..self.value_bytes.len())
    }

    pub(crate) fn finish(self) -> Vec<u8> {
        debug_assert_eq!(
            self.appended_rows, self.plan.row_count,
            "every planned JSON payload must be appended before finishing"
        );
        self.value_bytes
    }

    #[cfg(test)]
    fn value_allocation(&self) -> (*const u8, usize) {
        (self.value_bytes.as_ptr(), self.value_bytes.capacity())
    }

    #[cfg(test)]
    fn compression_scratch_allocation(&self) -> (*const u8, usize) {
        self.compressor.scratch_allocation()
    }

    #[cfg(test)]
    fn value_arena_allocations(&self) -> usize {
        self.value_arena_allocations
    }

    #[cfg(test)]
    fn compression_attempts(&self) -> usize {
        self.compressor.compression_attempts()
    }

    #[cfg(test)]
    fn used_raw_bound_fallback(&self) -> bool {
        self.used_raw_bound_fallback
    }
}

struct StoredJsonPayload {
    codec: JsonCodec,
    uncompressed_len: usize,
    data: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonHashCheck {
    /// Hot reads trust the local storage layer and pack directory. Content
    /// hashes are computed at write time; exhaustive verification belongs in
    /// explicit integrity-check/fsck callers rather than every row scan.
    TrustedHotRead,
    #[cfg(test)]
    Verify,
}

#[cfg(test)]
fn raw_json_ref_for_content(json: &str) -> JsonRef {
    JsonRef::from_hash(blake3::hash(json.as_bytes()))
}

#[cfg(test)]
struct TestStoredJson {
    json_ref: JsonRef,
    codec: JsonCodec,
    raw_data: Bytes,
    stored_bytes: Vec<u8>,
}

#[cfg(test)]
fn encode_json(json: &str) -> Result<TestStoredJson, LixError> {
    let json_ref = raw_json_ref_for_content(json);
    let mut plan = StoredJsonBatchPlan::default();
    plan.push_json(json)?;
    let mut encoder = plan.encoder()?;
    let range = encoder.append_json_with_ref(json, json_ref)?;
    let stored_bytes = encoder.finish();
    debug_assert_eq!(range, 0..stored_bytes.len());
    let codec = read_json_codec(stored_bytes[STORED_JSON_MAGIC.len()])?;
    Ok(TestStoredJson {
        json_ref,
        codec,
        raw_data: Bytes::copy_from_slice(json.as_bytes()),
        stored_bytes,
    })
}

#[cfg(test)]
async fn load_json_bytes_direct(
    store: &(impl StorageAdapterRead + ?Sized),
    json_ref: &JsonRef,
) -> Result<Option<Bytes>, LixError> {
    let result = load_json_values(store, JSON_SPACE, std::iter::once(*json_ref))
        .await?
        .into_iter()
        .next()
        .flatten();
    let Some(bytes) = result else {
        return Ok(None);
    };
    let stored_payload = decode_stored_json_payload(bytes)?;
    let _ = store;
    decode_json_payload(json_ref, stored_payload, JsonHashCheck::TrustedHotRead).map(Some)
}

pub(crate) async fn load_json_bytes_many_in_scope(
    store: &(impl StorageAdapterRead + ?Sized),
    json_refs: &[JsonRef],
    scope: JsonReadScopeRef,
) -> Result<Vec<Option<Bytes>>, LixError> {
    load_json_bytes_many_in_scope_with_hash_check(
        store,
        json_refs,
        scope,
        JsonHashCheck::TrustedHotRead,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn verify_json_bytes_many_in_scope(
    store: &impl StorageAdapterRead,
    json_refs: &[JsonRef],
    scope: JsonReadScopeRef,
) -> Result<Vec<Option<Bytes>>, LixError> {
    load_json_bytes_many_in_scope_with_hash_check(store, json_refs, scope, JsonHashCheck::Verify)
        .await
}

async fn load_json_bytes_many_in_scope_with_hash_check(
    store: &(impl StorageAdapterRead + ?Sized),
    json_refs: &[JsonRef],
    scope: JsonReadScopeRef,
    hash_check: JsonHashCheck,
) -> Result<Vec<Option<Bytes>>, LixError> {
    if json_refs.is_empty() {
        return Ok(Vec::new());
    }

    let mut unique_refs = Vec::with_capacity(json_refs.len());
    let mut key_indexes = HashMap::<[u8; 32], usize>::with_capacity(json_refs.len());
    // The normal tracked-state read has unique refs in request order. Allocate
    // a remap column only if a duplicate actually appears.
    let mut requested_indexes = None::<Vec<usize>>;
    for (request_index, json_ref) in json_refs.iter().enumerate() {
        let hash = *json_ref.as_hash_array();
        let index = match key_indexes.entry(hash) {
            Entry::Occupied(entry) => {
                if requested_indexes.is_none() {
                    let mut indexes = Vec::with_capacity(json_refs.len());
                    indexes.extend(0..request_index);
                    requested_indexes = Some(indexes);
                }
                *entry.get()
            }
            Entry::Vacant(entry) => {
                let index = unique_refs.len();
                unique_refs.push(*json_ref);
                entry.insert(index);
                index
            }
        };
        if let Some(requested_indexes) = &mut requested_indexes {
            requested_indexes.push(index);
        }
    }

    let JsonReadScopeRef::OutOfBand = scope;
    let mut loaded = load_json_values(store, JSON_SPACE, unique_refs.iter().copied()).await?;
    if loaded.len() != unique_refs.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "json_store batch load returned {} values for {} requested refs",
                loaded.len(),
                unique_refs.len()
            ),
        ));
    }

    for (unique_index, value_slot) in loaded.iter_mut().enumerate() {
        let Some(stored_bytes) = value_slot.take() else {
            continue;
        };
        let stored_payload = decode_stored_json_payload(stored_bytes)?;
        let _ = store;
        *value_slot = Some(decode_json_payload(
            &unique_refs[unique_index],
            stored_payload,
            hash_check,
        )?);
    }

    Ok(json_values_in_request_order(loaded, requested_indexes))
}

fn json_values_in_request_order(
    unique_values: Vec<Option<Bytes>>,
    requested_indexes: Option<Vec<usize>>,
) -> Vec<Option<Bytes>> {
    let Some(requested_indexes) = requested_indexes else {
        return unique_values;
    };
    requested_indexes
        .into_iter()
        .map(|index| unique_values[index].clone())
        .collect()
}

/// Loads fixed-width JSON hashes using one immutable key arena.
///
/// `get_many` still needs one lightweight key descriptor per request, but all
/// descriptors retain slices of this single buffer. Large reads therefore do
/// not allocate and clone a 32-byte `Vec` for every JSON reference.
async fn load_json_values(
    store: &(impl StorageAdapterRead + ?Sized),
    space: StorageSpace,
    json_refs: impl ExactSizeIterator<Item = JsonRef>,
) -> Result<Vec<Option<Bytes>>, LixError> {
    let keys = json_read_keys(json_refs)?;
    let result = PointReadPlan::new(space, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    Ok(result
        .value
        .into_iter()
        .map(|value| match value {
            Some(StorageProjectedValue::FullValue(bytes)) => Some(bytes),
            Some(StorageProjectedValue::KeyOnly) | None => None,
        })
        .collect())
}

fn json_read_keys(
    json_refs: impl ExactSizeIterator<Item = JsonRef>,
) -> Result<Vec<StorageKey>, LixError> {
    const JSON_REF_BYTES: usize = 32;
    let key_count = json_refs.len();
    let key_bytes_len = key_count
        .checked_mul(JSON_REF_BYTES)
        .ok_or_else(|| LixError::unknown("JSON read key batch exceeds addressable memory"))?;
    let mut key_bytes = Vec::with_capacity(key_bytes_len);
    for json_ref in json_refs {
        key_bytes.extend_from_slice(json_ref.as_hash_bytes());
    }
    debug_assert_eq!(key_bytes.len(), key_bytes_len);
    let key_bytes = Bytes::from(key_bytes);
    let keys = (0..key_count)
        .map(|index| {
            let start = index * JSON_REF_BYTES;
            StorageKey(key_bytes.slice(start..start + JSON_REF_BYTES))
        })
        .collect::<Vec<_>>();
    Ok(keys)
}

#[cfg(test)]
fn encode_stored_json_payload(encoded_json: &TestStoredJson) -> Vec<u8> {
    encoded_json.stored_bytes.clone()
}

/// Decodes one stored JSON row back to its exact JSON text.
///
/// The audit tooling needs this to recompute the row's content address from
/// the bytes on disk, independently of any in-memory ref.
#[cfg(feature = "storage-benches")]
pub(crate) fn decode_stored_json(bytes: &[u8]) -> Result<Bytes, LixError> {
    let stored_payload = decode_stored_json_payload(Bytes::copy_from_slice(bytes))?;
    match stored_payload.codec {
        JsonCodec::Raw => Ok(stored_payload.data),
        JsonCodec::Zstd => decode_json_zstd_payload(
            &stored_payload.data,
            stored_payload.uncompressed_len,
            "audit",
        )
        .map(Bytes::from),
    }
}

#[expect(clippy::cast_possible_truncation)]
fn decode_stored_json_payload(bytes: Bytes) -> Result<StoredJsonPayload, LixError> {
    if bytes.len() < STORED_JSON_HEADER_LEN {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "stored JSON payload is truncated",
        ));
    }
    if &bytes[..STORED_JSON_MAGIC.len()] != STORED_JSON_MAGIC {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "stored JSON payload has invalid header",
        ));
    }
    let codec = read_json_codec(bytes[STORED_JSON_MAGIC.len()])?;
    let len_start = STORED_JSON_MAGIC.len() + 1;
    let len_end = len_start + 8;
    let uncompressed_len = u64::from_be_bytes(
        bytes[len_start..len_end]
            .try_into()
            .expect("stored JSON length header is fixed size"),
    ) as usize;
    Ok(StoredJsonPayload {
        codec,
        uncompressed_len,
        data: bytes.slice(len_end..),
    })
}

fn json_codec_byte(codec: JsonCodec) -> u8 {
    match codec {
        JsonCodec::Raw => 0,
        JsonCodec::Zstd => 1,
    }
}

fn read_json_codec(byte: u8) -> Result<JsonCodec, LixError> {
    match byte {
        0 => Ok(JsonCodec::Raw),
        1 => Ok(JsonCodec::Zstd),
        _ => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("stored JSON payload has unknown codec byte {byte}"),
        )),
    }
}

fn decode_json_payload(
    json_ref: &JsonRef,
    stored_payload: StoredJsonPayload,
    hash_check: JsonHashCheck,
) -> Result<Bytes, LixError> {
    #[cfg(not(test))]
    let _ = hash_check;

    let data = match stored_payload.codec {
        JsonCodec::Raw => Ok(stored_payload.data),
        JsonCodec::Zstd => decode_json_zstd_payload(
            &stored_payload.data,
            stored_payload.uncompressed_len,
            &json_ref.to_hex(),
        )
        .map(Bytes::from),
    }?;
    if data.len() != stored_payload.uncompressed_len {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "json ref '{}' decoded to {} bytes, expected {}",
                json_ref.to_hex(),
                data.len(),
                stored_payload.uncompressed_len
            ),
        ));
    }
    #[cfg(test)]
    if hash_check == JsonHashCheck::Verify && cfg!(debug_assertions) {
        let actual_hash = blake3::hash(&data);
        if actual_hash.as_bytes() != json_ref.as_hash_bytes() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("json ref '{}' hash mismatch", json_ref.to_hex()),
            ));
        }
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::StorageAdapter;
    use crate::storage_adapter::{
        Memory, StorageKey, StorageReadOptions, StorageValue, StorageWriteOptions,
    };

    #[test]
    fn many_large_payloads_reuse_one_compression_scratch_and_one_value_arena() {
        const ROW_COUNT: usize = 1_024;

        let repeated = "shared-json-field-value/".repeat(128);
        let payloads = (0..ROW_COUNT)
            .map(|index| {
                format!(r#"{{"row":"{index:08x}","kind":"bulk","payload":"{repeated}"}}"#)
            })
            .collect::<Vec<_>>();
        let mut plan = StoredJsonBatchPlan::default();
        for payload in &payloads {
            plan.push_json(payload).expect("plan large JSON payload");
        }

        let raw_value_capacity = plan.raw_value_capacity;
        let mut encoder = plan.encoder().expect("create batch encoder");
        let scratch_allocation = encoder.compression_scratch_allocation();
        assert!(scratch_allocation.1 >= payloads[0].len());
        let mut value_allocation = None;
        let mut ranges = Vec::with_capacity(ROW_COUNT);
        let mut refs = Vec::with_capacity(ROW_COUNT);
        for payload in &payloads {
            let json_ref = JsonRef::for_content(payload.as_bytes());
            refs.push(json_ref);
            ranges.push(
                encoder
                    .append_json_with_ref(payload, json_ref)
                    .expect("append large JSON payload"),
            );
            let current_value_allocation = encoder.value_allocation();
            if let Some((pointer, capacity)) = value_allocation {
                assert_eq!(
                    current_value_allocation,
                    (pointer, capacity),
                    "representative bulk JSON must not grow its shared value arena"
                );
            } else {
                value_allocation = Some(current_value_allocation);
            }
            assert_eq!(
                encoder.compression_scratch_allocation(),
                scratch_allocation,
                "all rows must reuse the planned max-row compression scratch"
            );
        }

        assert_eq!(encoder.compression_attempts(), ROW_COUNT);
        assert_eq!(encoder.value_arena_allocations(), 1);
        assert!(!encoder.used_raw_bound_fallback());
        assert!(
            encoder.value_allocation().1 < raw_value_capacity / 2,
            "compressible batches must not retain a raw-sized final arena"
        );
        let encoded = encoder.finish();
        assert!(
            ranges.windows(2).all(|pair| pair[0].end == pair[1].start),
            "value descriptors must cover one contiguous arena without gaps"
        );

        for index in [0, ROW_COUNT / 2, ROW_COUNT - 1] {
            let stored =
                decode_stored_json_payload(Bytes::copy_from_slice(&encoded[ranges[index].clone()]))
                    .expect("decode staged payload header");
            assert_eq!(stored.codec, JsonCodec::Zstd);
            let decoded = decode_json_payload(&refs[index], stored, JsonHashCheck::TrustedHotRead)
                .expect("decode staged compressed payload");
            assert_eq!(decoded.as_ref(), payloads[index].as_bytes());
        }
    }

    #[test]
    fn heterogeneous_payloads_fall_back_to_raw_bound_at_most_once() {
        let compressible = format!(
            r#"{{"kind":"compressible","payload":"{}"}}"#,
            "same-value/".repeat(4_096)
        );
        let mut entropy = String::with_capacity(64 * 1_024);
        let mut counter = 0_u64;
        while entropy.len() < 64 * 1_024 {
            for byte in blake3::hash(&counter.to_le_bytes()).as_bytes() {
                use std::fmt::Write as _;
                write!(&mut entropy, "{byte:02x}").expect("write deterministic entropy");
            }
            counter += 1;
        }
        let irregular = format!(r#"{{"kind":"irregular","payload":"{entropy}"}}"#);
        let payloads = [&compressible, &irregular, &irregular];
        let mut plan = StoredJsonBatchPlan::default();
        for payload in payloads {
            plan.push_json(payload).expect("plan heterogeneous JSON");
        }
        let raw_value_capacity = plan.raw_value_capacity;
        let mut encoder = plan.encoder().expect("create batch encoder");
        for payload in payloads {
            encoder
                .append_json_with_ref(payload, JsonRef::for_content(payload.as_bytes()))
                .expect("append heterogeneous JSON");
        }

        assert!(encoder.used_raw_bound_fallback());
        assert_eq!(encoder.value_arena_allocations(), 2);
        assert_eq!(encoder.compression_attempts(), payloads.len());
        assert!(encoder.value_allocation().1 >= raw_value_capacity);
        assert!(encoder.value_bytes.len() <= raw_value_capacity);
    }

    #[tokio::test]
    async fn json_roundtrips_raw_payload() {
        let storage = StorageAdapter::new(Memory::new());
        let json = "{\"value\":\"small\"}";
        let encoded = encode_json(json).expect("json should encode");
        assert_eq!(encoded.codec, JsonCodec::Raw);

        let mut writes = storage.new_write_set();
        writes.put(
            JSON_SPACE,
            StorageKey(Bytes::copy_from_slice(encoded.json_ref.as_hash_bytes())),
            StorageValue {
                bytes: Bytes::from(encode_stored_json_payload(&encoded)),
            },
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("writes should commit");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            load_json_bytes_direct(&store, &encoded.json_ref)
                .await
                .expect("json should load"),
            Some(Bytes::copy_from_slice(json.as_bytes()))
        );
    }

    #[tokio::test]
    async fn json_batch_load_roundtrips_in_request_order() {
        let storage = StorageAdapter::new(Memory::new());
        let first = encode_json("{\"value\":\"first\"}").expect("first json should encode");
        let second = encode_json("{\"value\":\"second\"}").expect("second json should encode");

        let mut writes = storage.new_write_set();
        writes.put(
            JSON_SPACE,
            StorageKey(Bytes::copy_from_slice(first.json_ref.as_hash_bytes())),
            StorageValue {
                bytes: Bytes::from(encode_stored_json_payload(&first)),
            },
        );
        writes.put(
            JSON_SPACE,
            StorageKey(Bytes::copy_from_slice(second.json_ref.as_hash_bytes())),
            StorageValue {
                bytes: Bytes::from(encode_stored_json_payload(&second)),
            },
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("writes should commit");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let values = load_json_bytes_many_in_scope(
            &store,
            &[second.json_ref, first.json_ref, second.json_ref],
            JsonReadScopeRef::OutOfBand,
        )
        .await
        .expect("json batch should load");

        assert_eq!(
            values,
            vec![
                Some(second.raw_data.clone()),
                Some(first.raw_data.clone()),
                Some(second.raw_data.clone()),
            ]
        );
        assert_eq!(
            values[0].as_ref().expect("second payload").as_ptr(),
            values[2]
                .as_ref()
                .expect("duplicate second payload")
                .as_ptr(),
            "duplicate refs must clone one shared payload, not allocate a row buffer"
        );
    }

    #[test]
    fn large_json_read_batch_uses_one_key_arena() {
        let refs = (0_u32..10_000)
            .map(|index| JsonRef::for_content(&index.to_be_bytes()))
            .collect::<Vec<_>>();
        let keys = json_read_keys(refs.iter().copied()).expect("read keys should lower");

        assert_eq!(keys.len(), refs.len());
        let arena_start = keys[0].0.as_ptr() as usize;
        for (index, key) in keys.iter().enumerate() {
            assert_eq!(key.0.len(), 32);
            assert_eq!(
                key.0.as_ptr() as usize,
                arena_start + index * 32,
                "every read key should be a contiguous slice of one arena"
            );
        }
    }

    #[tokio::test]
    async fn verified_batch_load_rejects_hash_mismatch() {
        let storage = StorageAdapter::new(Memory::new());
        let requested_ref = JsonRef::for_content(br#"{"value":"requested"}"#);
        let stored = encode_json("{\"value\":\"different\"}").expect("stored json should encode");

        let mut writes = storage.new_write_set();
        writes.put(
            JSON_SPACE,
            StorageKey(Bytes::copy_from_slice(requested_ref.as_hash_bytes())),
            StorageValue {
                bytes: Bytes::from(encode_stored_json_payload(&stored)),
            },
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("writes should commit");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let trusted =
            load_json_bytes_many_in_scope(&store, &[requested_ref], JsonReadScopeRef::OutOfBand)
                .await
                .expect("trusted hot read should not hash-check");
        assert_eq!(trusted, vec![Some(stored.raw_data.clone())]);

        let error =
            verify_json_bytes_many_in_scope(&store, &[requested_ref], JsonReadScopeRef::OutOfBand)
                .await
                .expect_err("verified read should reject mismatched content address");
        assert!(
            error.to_string().contains("hash mismatch"),
            "error should mention hash mismatch: {error}"
        );
    }
}
