use base64::Engine as _;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::storage_adapter::StorageSpaceId;
use crate::storage_adapter::{
    StorageAdapterRead, StorageCoreProjection, StorageGetManyRequest, StorageGetOptions,
    StorageKey, StorageProjectedValue, StorageSpace, ValueSemantics, exact_get_many,
};
use crate::{LixError, LixNotice, ResultColumnType, Value};

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
pub(crate) const EXECUTE_IDEMPOTENCY_RECEIPT_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_0005),
    "session.execute_idempotency_receipt.v1",
    ValueSemantics::Mutable,
);

const BASE64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

const RECEIPT_VERSION: u8 = 3;
/// Keep the retry ledger bounded even when a mutation uses `RETURNING` with a
/// large blob. The request is rejected before its transaction commits, so a
/// successful mutation always has a replayable response.
const MAX_RECEIPT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct ExecuteIdempotencyReceipt {
    version: u8,
    branch_id: Option<String>,
    /// Base64 rather than `[u8; 32]`: `serde_json` renders a byte array as 32
    /// decimal numbers plus separators, which costs ~115 bytes for 32 bytes of
    /// information and dominates a receipt for a mutation with no `RETURNING`.
    request_fingerprint: String,
    results: Vec<StoredExecuteResult>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct StoredExecuteResult {
    columns: Vec<String>,
    #[serde(default)]
    column_types: Vec<ResultColumnType>,
    rows: Vec<Vec<StoredValue>>,
    rows_affected: u64,
    notices: Vec<LixNotice>,
}

/// Replay-storage form of a SQL value.
///
/// This exists for one variant. `Value`'s own `serde` representation renders
/// `Blob` as an array of decimal numbers, which costs 3.57 stored bytes per
/// payload byte, so a mutation that projects file content through `RETURNING`
/// writes almost four permanent copies of that content into the retry ledger.
/// The match below is exhaustive, so a new `Value` variant is a compile error
/// here rather than a silently unreplayable receipt.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum StoredValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Real(f64),
    Text(String),
    Jsonb(crate::Json),
    RowRef(String),
    Timestamptz(i64),
    Blob(String),
}

impl StoredValue {
    fn from_value(value: &Value) -> Result<Self, LixError> {
        Ok(match value {
            Value::Null => Self::Null,
            Value::Boolean(value) => Self::Boolean(*value),
            Value::Integer(value) => Self::Integer(*value),
            Value::Real(value) => {
                if !value.is_finite() {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "execute result contains a non-finite value that cannot be replayed over the protocol",
                    ));
                }
                Self::Real(*value)
            }
            Value::Text(value) => Self::Text(value.clone()),
            Value::Jsonb(value) => Self::Jsonb(value.clone()),
            Value::RowRef(value) => Self::RowRef(value.as_str().to_owned()),
            Value::Timestamptz(value) => Self::Timestamptz(*value),
            Value::Blob(value) => Self::Blob(BASE64.encode(value.as_ref())),
        })
    }

    fn into_value(self) -> Result<Value, LixError> {
        Ok(match self {
            Self::Null => Value::Null,
            Self::Boolean(value) => Value::Boolean(value),
            Self::Integer(value) => Value::Integer(value),
            Self::Real(value) => Value::Real(value),
            Self::Text(value) => Value::Text(value),
            Self::Jsonb(value) => Value::Jsonb(value),
            Self::RowRef(value) => {
                crate::row_ref::decode_str(&value)?;
                Value::RowRef(crate::RowRef(value))
            }
            Self::Timestamptz(value) => Value::Timestamptz(value),
            Self::Blob(value) => Value::Blob(
                BASE64
                    .decode(value.as_bytes())
                    .map_err(|error| {
                        LixError::new(
                            LixError::CODE_STORAGE_ERROR,
                            format!("decode execute idempotency receipt blob: {error}"),
                        )
                    })?
                    .into(),
            ),
        })
    }
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
            branch_id: idempotency.branch_id.clone(),
            request_fingerprint: BASE64.encode(idempotency.request_fingerprint()),
            results: results
                .iter()
                .map(StoredExecuteResult::from_execute_result)
                .collect::<Result<_, _>>()?,
        })
    }

    /// `scope` is deliberately not a stored field. [`receipt_key`] is injective
    /// over `(scope, key)` — it length-prefixes both components and carries the
    /// scope's presence bit — so a receipt read at a key *is* a receipt for that
    /// key's scope. Storing it again would make the value a second authority for
    /// a fact the key already decides.
    pub(crate) fn matches(&self, idempotency: &ExecuteIdempotency) -> bool {
        self.version == RECEIPT_VERSION
            && self.branch_id.as_deref() == idempotency.branch_id()
            && self.request_fingerprint == BASE64.encode(idempotency.request_fingerprint())
    }

    pub(crate) fn into_single_result(self) -> Result<ExecuteResult, LixError> {
        let mut results = self.into_results()?;
        if results.len() != 1 {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "idempotency receipt has a batch result where one result was expected",
            ));
        }
        Ok(results.remove(0))
    }

    pub(crate) fn into_results(self) -> Result<Vec<ExecuteResult>, LixError> {
        self.results
            .into_iter()
            .map(StoredExecuteResult::into_execute_result)
            .collect()
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
                    .map(StoredValue::from_value)
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            columns: result.columns().to_vec(),
            column_types: result.column_types().to_vec(),
            rows,
            rows_affected: result.rows_affected(),
            notices: result.notices().to_vec(),
        })
    }

    fn into_execute_result(self) -> Result<ExecuteResult, LixError> {
        let rows = self
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(StoredValue::into_value)
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ExecuteResult::from_idempotency_parts(
            self.columns,
            self.column_types,
            rows,
            self.rows_affected,
            self.notices,
        ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Blob;

    fn identity() -> ExecuteIdempotency {
        ExecuteIdempotency::new(
            Some("usr_expIR".to_owned()),
            "key-expIR".to_owned(),
            [7u8; 32],
        )
        .with_branch("0199aaaa-bbbb-cccc-dddd-eeeeeeeeeeee".to_owned())
    }

    fn roundtrip(values: Vec<Value>) -> Vec<Value> {
        let identity = identity();
        let result = ExecuteResult::from_rows(vec!["c".to_owned()], vec![values]);
        let receipt = ExecuteIdempotencyReceipt::single(&identity, &result).expect("build receipt");
        let (_, encoded) = encode_receipt(&identity, &receipt).expect("encode receipt");
        let decoded: ExecuteIdempotencyReceipt =
            serde_json::from_slice(&encoded).expect("decode receipt");
        assert!(decoded.matches(&identity), "decoded receipt must replay");
        decoded
            .into_single_result()
            .expect("replay result")
            .rows()
            .first()
            .expect("one row")
            .values()
            .to_vec()
    }

    /// Every SQL value shape must survive the storage form byte for byte; the
    /// replay contract is that a retry sees the recorded response.
    #[test]
    fn every_value_variant_replays_unchanged() {
        let values = vec![
            Value::Null,
            Value::Boolean(true),
            Value::Integer(-42),
            Value::Real(1.5),
            Value::Text("text".to_owned()),
            Value::Jsonb(serde_json::json!({"nested": [true, 42]}).into()),
            Value::Timestamptz(1_700_000_000_123_456),
            Value::Blob(Blob::from(vec![0u8, 255, 1, 128, 7])),
        ];
        assert_eq!(roundtrip(values.clone()), values);
    }

    /// Blob payloads are the reason this encoding exists: `Value`'s own serde
    /// form spends 3.57 stored bytes per payload byte.
    #[test]
    fn blob_payloads_cost_roughly_their_own_size() {
        let payload = (0..4096u32).map(|byte| byte as u8).collect::<Vec<_>>();
        let identity = identity();
        let result = ExecuteResult::from_rows(
            vec!["content".to_owned()],
            vec![vec![Value::Blob(Blob::from(payload.clone()))]],
        );
        let receipt = ExecuteIdempotencyReceipt::single(&identity, &result).expect("build receipt");
        let (_, encoded) = encode_receipt(&identity, &receipt).expect("encode receipt");
        assert!(
            encoded.len() < payload.len() * 2,
            "a {}-byte payload must not cost {} receipt bytes",
            payload.len(),
            encoded.len()
        );
    }

    /// A non-finite float cannot cross the protocol, so it must be refused
    /// while the mutation can still be rejected rather than at replay time.
    #[test]
    fn non_finite_reals_are_refused_before_commit() {
        let identity = identity();
        let result =
            ExecuteResult::from_rows(vec!["c".to_owned()], vec![vec![Value::Real(f64::INFINITY)]]);
        let error =
            ExecuteIdempotencyReceipt::single(&identity, &result).expect_err("must be refused");
        assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
    }

    /// The storage key already decides the scope, so the value must not carry
    /// it — but a differing scope must still never replay, because it lands on
    /// a different key.
    #[test]
    fn scope_is_decided_by_the_key_alone() {
        let scoped =
            ExecuteIdempotency::new(Some("usr_a".to_owned()), "shared-key".to_owned(), [7u8; 32]);
        let other =
            ExecuteIdempotency::new(Some("usr_b".to_owned()), "shared-key".to_owned(), [7u8; 32]);
        let unscoped = ExecuteIdempotency::new(None, "shared-key".to_owned(), [7u8; 32]);
        let empty_scope =
            ExecuteIdempotency::new(Some(String::new()), "shared-key".to_owned(), [7u8; 32]);
        let keys = [&scoped, &other, &unscoped, &empty_scope]
            .map(|identity| receipt_key(identity).expect("build key"));
        for left in 0..keys.len() {
            for right in (left + 1)..keys.len() {
                assert_ne!(keys[left], keys[right], "receipt keys must not alias");
            }
        }
    }
}
