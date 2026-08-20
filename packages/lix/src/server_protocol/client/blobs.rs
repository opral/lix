use std::collections::HashMap;

use base64::Engine as _;
use sha2::{Digest as _, Sha256};

use crate::{Value, WireValue};

use super::wire::RequestWireValue;

pub const REQUEST_BLOB_DELTA_MIN_BYTES: usize = 32 * 1024;
const REQUEST_BLOB_DELTA_MIN_WIRE_RATIO: f64 = 0.9;
const REQUEST_BLOB_COMPARE_WORD_BYTES: usize = 8;
const REQUEST_BLOB_BASE_MAX_ENTRIES: usize = 8;
const REQUEST_BLOB_BASE_MAX_BYTES: usize = 16 * 1024 * 1024;
const WIRE_BLOB_JSON_ENVELOPE_BYTES: usize = 24;

#[derive(Debug, Clone)]
pub struct BlobBase {
    pub sha256: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct BlobCache {
    bases: HashMap<String, BlobBase>,
    bytes: usize,
}

impl Default for BlobCache {
    fn default() -> Self {
        Self {
            bases: HashMap::new(),
            bytes: 0,
        }
    }
}

impl BlobCache {
    pub fn prepare(
        &self,
        values: &[Value],
        slot: impl Fn(usize) -> String,
    ) -> PreparedRequestParams {
        let mut prepared = Vec::with_capacity(values.len());
        for (index, value) in values.iter().enumerate() {
            prepared.push(prepare_param(self, value, &slot(index)));
        }
        PreparedRequestParams {
            params: prepared.iter().map(|param| param.value.clone()).collect(),
            full_params: prepared.iter().map(|param| param.full.clone()).collect(),
            cache_updates: prepared
                .into_iter()
                .filter_map(|param| param.cache_update)
                .collect(),
        }
    }

    pub fn commit(&mut self, updates: &[BlobCacheUpdate]) {
        for update in updates {
            if let Some(previous) = self.bases.remove(&update.slot) {
                self.bytes = self.bytes.saturating_sub(previous.bytes.len());
            }
            if update.base.bytes.len() > REQUEST_BLOB_BASE_MAX_BYTES {
                continue;
            }
            while self.bases.len() >= REQUEST_BLOB_BASE_MAX_ENTRIES
                || self.bytes + update.base.bytes.len() > REQUEST_BLOB_BASE_MAX_BYTES
            {
                let Some(oldest) = self.bases.keys().next().cloned() else {
                    break;
                };
                if let Some(removed) = self.bases.remove(&oldest) {
                    self.bytes = self.bytes.saturating_sub(removed.bytes.len());
                }
            }
            self.bytes += update.base.bytes.len();
            self.bases.insert(update.slot.clone(), update.base.clone());
        }
    }
}

#[derive(Debug, Clone)]
pub struct BlobCacheUpdate {
    pub slot: String,
    pub base: BlobBase,
}

#[derive(Debug, Clone)]
pub struct PreparedRequestParams {
    pub params: Vec<RequestWireValue>,
    pub full_params: Vec<RequestWireValue>,
    pub cache_updates: Vec<BlobCacheUpdate>,
}

#[derive(Debug, Clone)]
struct PreparedRequestParam {
    value: RequestWireValue,
    full: RequestWireValue,
    cache_update: Option<BlobCacheUpdate>,
}

pub fn request_blob_slot(
    sql: &str,
    param_index: usize,
    statement_index: Option<usize>,
) -> String {
    serde_json::to_string(&(statement_index, sql, param_index))
        .unwrap_or_else(|_| format!("{statement_index:?}:{sql}:{param_index}"))
}

fn prepare_param(cache: &BlobCache, value: &Value, slot: &str) -> PreparedRequestParam {
    let Ok(full_wire) = WireValue::try_from_engine(value) else {
        return PreparedRequestParam {
            value: RequestWireValue::Value(WireValue::Null { value: () }),
            full: RequestWireValue::Value(WireValue::Null { value: () }),
            cache_update: None,
        };
    };
    let full = RequestWireValue::Value(full_wire.clone());
    let Value::Blob(blob) = value else {
        return PreparedRequestParam {
            value: full.clone(),
            full,
            cache_update: None,
        };
    };
    let bytes = blob.as_bytes();
    if bytes.len() < REQUEST_BLOB_DELTA_MIN_BYTES || bytes.len() > REQUEST_BLOB_BASE_MAX_BYTES {
        return PreparedRequestParam {
            value: full.clone(),
            full,
            cache_update: None,
        };
    }
    let result_sha256 = sha256_hex(bytes);
    let cache_update = BlobCacheUpdate {
        slot: slot.to_owned(),
        base: BlobBase {
            sha256: result_sha256.clone(),
            bytes: bytes.to_vec(),
        },
    };
    let Some(base) = cache.bases.get(slot) else {
        return PreparedRequestParam {
            value: full.clone(),
            full,
            cache_update: Some(cache_update),
        };
    };
    let Some(delta) = plan_blob_splice(&base.bytes, bytes, &base.sha256, &result_sha256) else {
        return PreparedRequestParam {
            value: full.clone(),
            full,
            cache_update: Some(cache_update),
        };
    };
    if !blob_splice_is_smaller(&delta, bytes.len()) {
        return PreparedRequestParam {
            value: full.clone(),
            full,
            cache_update: Some(cache_update),
        };
    }
    PreparedRequestParam {
        value: RequestWireValue::BlobSplice(super::wire::RequestBlobSplice {
            kind: super::wire::BlobSpliceKind::BlobSplice,
            base_sha256: delta.base_sha256,
            result_sha256: delta.result_sha256,
            prefix_bytes: delta.prefix_bytes as u64,
            suffix_bytes: delta.suffix_bytes as u64,
            insert_base64: base64::engine::general_purpose::STANDARD.encode(&delta.insert),
        }),
        full,
        cache_update: Some(cache_update),
    }
}

struct BlobSplicePlan {
    base_sha256: String,
    result_sha256: String,
    prefix_bytes: usize,
    suffix_bytes: usize,
    insert: Vec<u8>,
}

fn plan_blob_splice(
    base: &[u8],
    result: &[u8],
    base_sha256: &str,
    result_sha256: &str,
) -> Option<BlobSplicePlan> {
    let mut prefix_bytes = 0;
    let prefix_limit = base.len().min(result.len());
    while prefix_limit - prefix_bytes >= REQUEST_BLOB_COMPARE_WORD_BYTES
        && base[prefix_bytes..prefix_bytes + REQUEST_BLOB_COMPARE_WORD_BYTES]
            == result[prefix_bytes..prefix_bytes + REQUEST_BLOB_COMPARE_WORD_BYTES]
    {
        prefix_bytes += REQUEST_BLOB_COMPARE_WORD_BYTES;
    }
    while prefix_bytes < prefix_limit && base[prefix_bytes] == result[prefix_bytes] {
        prefix_bytes += 1;
    }

    let mut suffix_bytes = 0;
    let suffix_limit = (base.len() - prefix_bytes).min(result.len() - prefix_bytes);
    while suffix_limit - suffix_bytes >= REQUEST_BLOB_COMPARE_WORD_BYTES {
        let base_offset = base.len() - suffix_bytes - REQUEST_BLOB_COMPARE_WORD_BYTES;
        let result_offset = result.len() - suffix_bytes - REQUEST_BLOB_COMPARE_WORD_BYTES;
        if base[base_offset..base_offset + REQUEST_BLOB_COMPARE_WORD_BYTES]
            != result[result_offset..result_offset + REQUEST_BLOB_COMPARE_WORD_BYTES]
        {
            break;
        }
        suffix_bytes += REQUEST_BLOB_COMPARE_WORD_BYTES;
    }
    while suffix_bytes < suffix_limit
        && base[base.len() - suffix_bytes - 1] == result[result.len() - suffix_bytes - 1]
    {
        suffix_bytes += 1;
    }

    Some(BlobSplicePlan {
        base_sha256: base_sha256.to_owned(),
        result_sha256: result_sha256.to_owned(),
        prefix_bytes,
        suffix_bytes,
        insert: result[prefix_bytes..result.len() - suffix_bytes].to_vec(),
    })
}

fn blob_splice_is_smaller(delta: &BlobSplicePlan, full_len: usize) -> bool {
    let delta_envelope_bytes = serde_json::json!({
        "kind": "blob-splice",
        "baseSha256": delta.base_sha256,
        "resultSha256": delta.result_sha256,
        "prefixBytes": delta.prefix_bytes,
        "suffixBytes": delta.suffix_bytes,
        "insertBase64": "",
    })
    .to_string()
    .len();
    let delta_bytes = delta_envelope_bytes + base64_encoded_length(delta.insert.len());
    let full_bytes = WIRE_BLOB_JSON_ENVELOPE_BYTES + base64_encoded_length(full_len);
    (delta_bytes as f64) < (full_bytes as f64) * REQUEST_BLOB_DELTA_MIN_WIRE_RATIO
}

fn base64_encoded_length(byte_length: usize) -> usize {
    4 * byte_length.div_ceil(3)
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}
