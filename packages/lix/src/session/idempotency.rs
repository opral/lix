use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::storage_adapter::{
    StorageAdapterRead, StorageCoreProjection, StorageGetManyRequest, StorageGetOptions,
    StorageKey, StorageProjectedValue, StorageSpace, exact_get_many,
};
use crate::{LixError, LixNotice, Value};

use super::execute::ExecuteResult;

/// Opaque replay identity for one HTTP SQL mutation.
///
/// The protocol derives `request_fingerprint` from the complete semantic
/// request. Reusing a key with a different fingerprint is rejected rather
/// than replaying or executing a different mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecuteIdempotency {
    scope: Option<String>,
    key: String,
    request_fingerprint: [u8; 32],
    branch_id: Option<String>,
}

impl ExecuteIdempotency {
    /// Creates a replay identity scoped by a trusted outer principal, when
    /// one is available. A protocol host must never pass a client-controlled
    /// value as `scope`; Lixray injects its authenticated user id after
    /// authorization.
    pub fn new(scope: Option<String>, key: String, request_fingerprint: [u8; 32]) -> Self {
        Self {
            scope,
            key,
            request_fingerprint,
            branch_id: None,
        }
    }

    /// Binds this identity to the branch used by the executing session.
    ///
    /// The protocol applies this only after it holds the precise session
    /// instance that will execute the request. A branch is an implicit semantic
    /// input to SQL, so replaying a matching body on another branch would be a
    /// false acknowledgement rather than the original operation.
    #[doc(hidden)]
    pub fn with_branch(mut self, branch_id: String) -> Self {
        self.branch_id = Some(branch_id);
        self
    }

    pub(crate) fn key(&self) -> &str {
        &self.key
    }

    fn scope(&self) -> Option<&str> {
        self.scope.as_deref()
    }

    fn request_fingerprint(&self) -> &[u8; 32] {
        &self.request_fingerprint
    }

    fn branch_id(&self) -> Option<&str> {
        self.branch_id.as_deref()
    }
}

/// Internal, principal-scoped receipts for server `/execute` mutations.
///
/// This is not a user table. A receipt is staged into the same storage write
/// set as the mutation and guarded by `KeyAbsent`, so a published receipt and
/// its mutation have one atomic storage outcome.
pub(crate) const EXECUTE_IDEMPOTENCY_RECEIPT_SPACE: StorageSpace = StorageSpace::engine_declared(
    0x0007_0005,
    "session.execute_idempotency_receipt.v1",
    crate::storage_adapter::ValueSemantics::Mutable,
);

const RECEIPT_VERSION: u8 = 2;
/// Keep the retry ledger bounded even when a mutation uses `RETURNING` with a
/// large blob. The request is rejected before its transaction commits, so a
/// successful mutation always has a replayable response.
const MAX_RECEIPT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct ExecuteIdempotencyReceipt {
    version: u8,
    scope: Option<String>,
    branch_id: Option<String>,
    request_fingerprint: [u8; 32],
    results: Vec<StoredExecuteResult>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct StoredExecuteResult {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    rows_affected: u64,
    notices: Vec<LixNotice>,
}

impl ExecuteIdempotencyReceipt {
    pub(crate) fn single(
        idempotency: &ExecuteIdempotency,
        result: &ExecuteResult,
    ) -> Result<Self, LixError> {
        Self::batch(idempotency, std::slice::from_ref(result))
    }

    pub(crate) fn batch(
        idempotency: &ExecuteIdempotency,
        results: &[ExecuteResult],
    ) -> Result<Self, LixError> {
        Ok(Self {
            version: RECEIPT_VERSION,
            scope: idempotency.scope.clone(),
            branch_id: idempotency.branch_id.clone(),
            request_fingerprint: *idempotency.request_fingerprint(),
            results: results
                .iter()
                .map(StoredExecuteResult::from_execute_result)
                .collect::<Result<_, _>>()?,
        })
    }

    pub(crate) fn matches(&self, idempotency: &ExecuteIdempotency) -> bool {
        self.version == RECEIPT_VERSION
            && self.scope.as_deref() == idempotency.scope()
            && self.branch_id.as_deref() == idempotency.branch_id()
            && self.request_fingerprint == *idempotency.request_fingerprint()
    }

    pub(crate) fn into_single_result(self) -> Result<ExecuteResult, LixError> {
        let mut results = self.into_results();
        if results.len() != 1 {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "idempotency receipt has a batch result where one result was expected",
            ));
        }
        Ok(results.remove(0))
    }

    pub(crate) fn into_results(self) -> Vec<ExecuteResult> {
        self.results.into_iter().map(ExecuteResult::from).collect()
    }
}

impl StoredExecuteResult {
    fn from_execute_result(result: &ExecuteResult) -> Result<Self, LixError> {
        let rows = result
            .rows()
            .iter()
            .map(|row| {
                row.values()
                    .iter()
                    .map(replayable_value)
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            columns: result.columns().to_vec(),
            rows,
            rows_affected: result.rows_affected(),
            notices: result.notices().to_vec(),
        })
    }
}

impl From<StoredExecuteResult> for ExecuteResult {
    fn from(result: StoredExecuteResult) -> Self {
        Self::from_idempotency_parts(
            result.columns,
            result.rows,
            result.rows_affected,
            result.notices,
        )
    }
}

pub(crate) fn encode_receipt(
    idempotency: &ExecuteIdempotency,
    receipt: &ExecuteIdempotencyReceipt,
) -> Result<(StorageKey, Vec<u8>), LixError> {
    let value = serde_json::to_vec(receipt).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("serialize execute idempotency receipt: {error}"),
        )
    })?;
    if value.len() > MAX_RECEIPT_BYTES {
        return Err(LixError::new(
            LixError::CODE_IDEMPOTENCY_RESPONSE_TOO_LARGE,
            format!(
                "execute idempotency response exceeds the {MAX_RECEIPT_BYTES}-byte replay limit"
            ),
        )
        .with_details(serde_json::json!({
            "limitBytes": MAX_RECEIPT_BYTES,
            "responseBytes": value.len(),
        })));
    }
    Ok((receipt_key(idempotency)?, value))
}

pub(crate) async fn load_receipt(
    store: &(impl StorageAdapterRead + ?Sized),
    idempotency: &ExecuteIdempotency,
) -> Result<Option<ExecuteIdempotencyReceipt>, LixError> {
    let key = receipt_key(idempotency)?;
    let values = exact_get_many(
        store,
        &[StorageGetManyRequest {
            space: EXECUTE_IDEMPOTENCY_RECEIPT_SPACE,
            keys: &[key],
            opts: StorageGetOptions {
                projection: StorageCoreProjection::FullValue,
            },
        }],
    )
    .await?;
    let Some(value) = values.values.into_iter().next().flatten() else {
        return Ok(None);
    };
    let StorageProjectedValue::FullValue(value) = value else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "idempotency receipt read returned no value bytes",
        ));
    };
    serde_json::from_slice(&value).map(Some).map_err(|error| {
        LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!("decode execute idempotency receipt: {error}"),
        )
    })
}

fn receipt_key(idempotency: &ExecuteIdempotency) -> Result<StorageKey, LixError> {
    let scope = idempotency.scope().map_or(&[][..], str::as_bytes);
    let key = idempotency.key().as_bytes();
    // Length-prefix both components, including the absence/presence bit for
    // scope, so no pair of valid UTF-8 strings can alias another receipt key.
    let scope_len = u32::try_from(scope.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            "idempotency scope exceeds the storage key length limit",
        )
    })?;
    let key_len = u32::try_from(key.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            "Idempotency-Key exceeds the storage key length limit",
        )
    })?;
    let mut bytes = Vec::with_capacity(1 + 4 + scope.len() + 4 + key.len());
    bytes.push(u8::from(idempotency.scope().is_some()));
    bytes.extend_from_slice(&scope_len.to_be_bytes());
    bytes.extend_from_slice(scope);
    bytes.extend_from_slice(&key_len.to_be_bytes());
    bytes.extend_from_slice(key);
    Ok(StorageKey(Bytes::from(bytes)))
}

fn replayable_value(value: &Value) -> Result<Value, LixError> {
    if let Value::Real(value) = value
        && !value.is_finite()
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "execute result contains a non-finite value that cannot be replayed over the protocol",
        ));
    }
    Ok(value.clone())
}
