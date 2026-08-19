//! Request-side blob splice cache, matching the JS remote client.

use super::wire::{RequestBlobSplice, RequestWireValue};
use crate::{LixError, Value, WireValue};
use sha2::{Digest as _, Sha256};
use std::collections::HashMap;

const REQUEST_BLOB_DELTA_MIN_BYTES: usize = 32 * 1024;
const REQUEST_BLOB_DELTA_MIN_WIRE_RATIO: f64 = 0.9;
const REQUEST_BLOB_COMPARE_WORD_BYTES: usize = 8;
const REQUEST_BLOB_BASE_MAX_ENTRIES: usize = 8;
const REQUEST_BLOB_BASE_MAX_BYTES: usize = 16 * 1024 * 1024;
const WIRE_BLOB_JSON_ENVELOPE_BYTES: usize = 27; // {"kind":"blob","base64":""}

#[derive(Clone)]
pub(crate) struct RequestBlobBase {
    pub sha256: String,
    pub bytes: Vec<u8>,
}

pub(crate) struct RequestBlobCache {
    bases: HashMap<String, RequestBlobBase>,
    order: Vec<String>,
    bytes: usize,
}

impl RequestBlobCache {
    pub(crate) fn new() -> Self {
        Self {
            bases: HashMap::new(),
            order: Vec::new(),
            bytes: 0,
        }
    }

    pub(crate) fn prepare(
        &self,
        params: &[Value],
        slot: impl Fn(usize) -> String,
    ) -> Result<PreparedRequestParams, LixError> {
        let mut prepared = Vec::with_capacity(params.len());
        for (index, param) in params.iter().enumerate() {
            prepared.push(prepare_param(self, param, &slot(index))?);
        }
        Ok(PreparedRequestParams {
            params: prepared.iter().map(|param| param.value.clone()).collect(),
            full_params: prepared.iter().map(|param| param.full.clone()).collect(),
            cache_updates: prepared
                .iter()
                .filter_map(|param| param.cache_update.clone())
                .collect(),
            cache_blobs: prepared.iter().any(|param| param.cache_update.is_some()),
            has_delta: prepared
                .iter()
                .any(|param| matches!(param.value, RequestWireValue::BlobSplice(_))),
        })
    }

    pub(crate) fn commit(&mut self, updates: &[RequestBlobCacheUpdate]) {
        for update in updates {
            if let Some(previous) = self.bases.remove(&update.slot) {
                self.bytes = self.bytes.saturating_sub(previous.bytes.len());
                self.order.retain(|slot| slot != &update.slot);
            }
            if update.base.bytes.len() > REQUEST_BLOB_BASE_MAX_BYTES {
                continue;
            }
            while self.bases.len() >= REQUEST_BLOB_BASE_MAX_ENTRIES
                || self.bytes + update.base.bytes.len() > REQUEST_BLOB_BASE_MAX_BYTES
            {
                let Some(oldest) = self.order.first().cloned() else {
                    break;
                };
                self.order.remove(0);
                if let Some(previous) = self.bases.remove(&oldest) {
                    self.bytes = self.bytes.saturating_sub(previous.bytes.len());
                }
            }
            self.bytes += update.base.bytes.len();
            self.order.push(update.slot.clone());
            self.bases.insert(update.slot.clone(), update.base.clone());
        }
    }
}

#[derive(Clone)]
pub(crate) struct RequestBlobCacheUpdate {
    pub slot: String,
    pub base: RequestBlobBase,
}

pub(crate) struct PreparedRequestParams {
    pub params: Vec<RequestWireValue>,
    pub full_params: Vec<RequestWireValue>,
    pub cache_updates: Vec<RequestBlobCacheUpdate>,
    pub cache_blobs: bool,
    pub has_delta: bool,
}

struct PreparedRequestParam {
    value: RequestWireValue,
    full: RequestWireValue,
    cache_update: Option<RequestBlobCacheUpdate>,
}

fn prepare_param(
    cache: &RequestBlobCache,
    param: &Value,
    slot: &str,
) -> Result<PreparedRequestParam, LixError> {
    let Value::Blob(blob) = param else {
        let full = RequestWireValue::Value(WireValue::try_from_engine(param)?);
        return Ok(PreparedRequestParam {
            value: full.clone(),
            full,
            cache_update: None,
        });
    };
    let bytes = blob.as_ref();
    if bytes.len() < REQUEST_BLOB_DELTA_MIN_BYTES || bytes.len() > REQUEST_BLOB_BASE_MAX_BYTES
    {
        let full = RequestWireValue::Value(WireValue::try_from_engine(param)?);
        return Ok(PreparedRequestParam {
            value: full.clone(),
            full,
            cache_update: None,
        });
    }
    let result_sha256 = sha256_hex(bytes);
    let cache_update = RequestBlobCacheUpdate {
        slot: slot.to_owned(),
        base: RequestBlobBase {
            sha256: result_sha256.clone(),
            bytes: bytes.to_vec(),
        },
    };
    let full = RequestWireValue::Value(WireValue::try_from_engine(param)?);
    let Some(base) = cache.bases.get(slot) else {
        return Ok(PreparedRequestParam {
            value: full.clone(),
            full,
            cache_update: Some(cache_update),
        });
    };
    let delta = plan_blob_splice(base, bytes, &result_sha256);
    if !blob_splice_is_at_least_ten_percent_smaller(&delta, bytes) {
        return Ok(PreparedRequestParam {
            value: full.clone(),
            full,
            cache_update: Some(cache_update),
        });
    }
    Ok(PreparedRequestParam {
        value: RequestWireValue::BlobSplice(delta),
        full,
        cache_update: Some(cache_update),
    })
}

fn plan_blob_splice(
    base: &RequestBlobBase,
    result: &[u8],
    result_sha256: &str,
) -> RequestBlobSplice {
    let prefix_limit = base.bytes.len().min(result.len());
    let mut prefix_bytes = 0;
    while prefix_limit - prefix_bytes >= REQUEST_BLOB_COMPARE_WORD_BYTES
        && base.bytes[prefix_bytes..prefix_bytes + REQUEST_BLOB_COMPARE_WORD_BYTES]
            == result[prefix_bytes..prefix_bytes + REQUEST_BLOB_COMPARE_WORD_BYTES]
    {
        prefix_bytes += REQUEST_BLOB_COMPARE_WORD_BYTES;
    }
    while prefix_bytes < prefix_limit && base.bytes[prefix_bytes] == result[prefix_bytes] {
        prefix_bytes += 1;
    }

    let suffix_limit = (base.bytes.len() - prefix_bytes).min(result.len() - prefix_bytes);
    let mut suffix_bytes = 0;
    while suffix_limit - suffix_bytes >= REQUEST_BLOB_COMPARE_WORD_BYTES {
        let base_offset = base.bytes.len() - suffix_bytes - REQUEST_BLOB_COMPARE_WORD_BYTES;
        let result_offset = result.len() - suffix_bytes - REQUEST_BLOB_COMPARE_WORD_BYTES;
        if base.bytes[base_offset..base_offset + REQUEST_BLOB_COMPARE_WORD_BYTES]
            != result[result_offset..result_offset + REQUEST_BLOB_COMPARE_WORD_BYTES]
        {
            break;
        }
        suffix_bytes += REQUEST_BLOB_COMPARE_WORD_BYTES;
    }
    while suffix_bytes < suffix_limit
        && base.bytes[base.bytes.len() - suffix_bytes - 1]
            == result[result.len() - suffix_bytes - 1]
    {
        suffix_bytes += 1;
    }

    let insert = &result[prefix_bytes..result.len() - suffix_bytes];
    RequestBlobSplice {
        kind: "blob-splice",
        base_sha256: base.sha256.clone(),
        result_sha256: result_sha256.to_owned(),
        prefix_bytes: prefix_bytes as u64,
        suffix_bytes: suffix_bytes as u64,
        insert_base64: base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            insert,
        ),
    }
}

fn blob_splice_is_at_least_ten_percent_smaller(
    delta: &RequestBlobSplice,
    full: &[u8],
) -> bool {
    let delta_envelope = serde_json::json!({
        "kind": "blob-splice",
        "baseSha256": delta.base_sha256,
        "resultSha256": delta.result_sha256,
        "prefixBytes": delta.prefix_bytes,
        "suffixBytes": delta.suffix_bytes,
        "insertBase64": "",
    });
    let delta_envelope_bytes = serde_json::to_vec(&delta_envelope)
        .map(|bytes| bytes.len())
        .unwrap_or(0);
    let insert_len = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &delta.insert_base64,
    )
    .map(|bytes| bytes.len())
    .unwrap_or(0);
    let delta_bytes = delta_envelope_bytes + base64_encoded_length(insert_len);
    let full_bytes = WIRE_BLOB_JSON_ENVELOPE_BYTES + base64_encoded_length(full.len());
    (delta_bytes as f64) < (full_bytes as f64) * REQUEST_BLOB_DELTA_MIN_WIRE_RATIO
}

fn base64_encoded_length(byte_length: usize) -> usize {
    4 * byte_length.div_ceil(3)
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn request_blob_slot(
    kind: &str,
    sql: &str,
    param_index: usize,
    statement_index: Option<usize>,
) -> String {
    serde_json::to_string(&(kind, statement_index, sql, param_index))
        .expect("blob slot identity is json")
}
