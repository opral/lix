use serde_json::Value as JsonValue;

use crate::lix::Lix;
use lix_engine::{LixError, Memory, Storage, Value};

// Client-state rows deliberately use ordinary `lix_key_value` entities. The
// private physical prefix keeps their key identity disjoint from built-in and
// workspace KV rows even when the client store is inspected through SQL.
const CLIENT_STATE_KEY_PREFIX: &str = "lix_client_state:";

const GET_SQL: &str = "SELECT value \
    FROM lix_key_value_by_branch \
    WHERE key = $1 \
      AND lixcol_branch_id = 'global' \
      AND lixcol_untracked = true";

const ENTRIES_SQL: &str = "SELECT key, value \
    FROM lix_key_value_by_branch \
    WHERE lixcol_branch_id = 'global' \
      AND lixcol_untracked = true \
    ORDER BY key";

const SET_SQL: &str = "INSERT INTO lix_key_value_by_branch \
    (key, value, lixcol_branch_id, lixcol_global, lixcol_untracked) \
    VALUES ($1, $2, 'global', true, true) \
    ON CONFLICT (key, lixcol_branch_id) \
    DO UPDATE SET value = excluded.value";

const DELETE_SQL: &str = "DELETE FROM lix_key_value_by_branch \
    WHERE key = $1 \
      AND lixcol_branch_id = 'global' \
      AND lixcol_untracked = true";

/// A borrowed handle to client-local JSON state stored by Lix.
///
/// Client state is represented by ordinary global, untracked
/// `lix_key_value` rows in the storage backing this Lix handle. The handle
/// therefore uses the same SQL transaction, validation, and commit path as
/// other Lix state while keeping logical client keys separate from built-in KV
/// keys through a private physical prefix.
///
/// Placement is determined by which Lix owns this handle. Remote SDKs should
/// construct this handle from their local client-only Lix, not from the remote
/// workspace Lix.
#[derive(Clone, Copy)]
#[expect(missing_debug_implementations)]
pub struct ClientState<'lix, StorageImpl = Memory>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix: &'lix Lix<StorageImpl>,
}

impl<'lix, StorageImpl> ClientState<'lix, StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Reads every logical client-state entry in key order.
    pub async fn entries(&self) -> Result<Vec<(String, JsonValue)>, LixError> {
        let result = self.lix.execute(ENTRIES_SQL, &[]).await?;
        let mut entries = Vec::new();
        for row in result.rows() {
            let key = row.get::<String>("key")?;
            let Some(logical_key) = key.strip_prefix(CLIENT_STATE_KEY_PREFIX) else {
                continue;
            };
            entries.push((logical_key.to_string(), value_to_json(row.value("value")?)?));
        }
        Ok(entries)
    }

    pub(crate) fn new(lix: &'lix Lix<StorageImpl>) -> Self {
        Self { lix }
    }

    /// Reads one logical client-state key.
    ///
    /// A stored JSON `null` is returned as `Some(JsonValue::Null)`; `None`
    /// means that the key has not been stored.
    pub async fn get(&self, key: &str) -> Result<Option<JsonValue>, LixError> {
        let result = self
            .lix
            .execute(GET_SQL, &[Value::Text(physical_key(key)?)])
            .await?;

        if result.len() > 1 {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "client state key resolved to more than one lix_key_value row",
            ));
        }
        let Some(row) = result.rows().first() else {
            return Ok(None);
        };
        Ok(Some(value_to_json(row.value("value")?)?))
    }

    /// Inserts or replaces one logical client-state key.
    pub async fn set(&self, key: &str, value: JsonValue) -> Result<(), LixError> {
        self.lix
            .execute(
                SET_SQL,
                &[Value::Text(physical_key(key)?), Value::Json(value)],
            )
            .await?;
        Ok(())
    }

    /// Deletes one logical client-state key. Missing keys are a no-op.
    pub async fn delete(&self, key: &str) -> Result<(), LixError> {
        self.lix
            .execute(DELETE_SQL, &[Value::Text(physical_key(key)?)])
            .await?;
        Ok(())
    }
}

fn physical_key(key: &str) -> Result<String, LixError> {
    if key.is_empty() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "client state key must be a non-empty string",
        ));
    }
    let mut physical = String::with_capacity(CLIENT_STATE_KEY_PREFIX.len() + key.len());
    physical.push_str(CLIENT_STATE_KEY_PREFIX);
    physical.push_str(key);
    Ok(physical)
}

fn value_to_json(value: &Value) -> Result<JsonValue, LixError> {
    match value {
        Value::Null => Ok(JsonValue::Null),
        Value::Boolean(value) => Ok(JsonValue::Bool(*value)),
        Value::Integer(value) => Ok(JsonValue::Number((*value).into())),
        Value::Real(value) => serde_json::Number::from_f64(*value)
            .map(JsonValue::Number)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "client state contained a non-finite JSON number",
                )
            }),
        Value::Text(value) => Ok(JsonValue::String(value.clone())),
        Value::Json(value) => Ok(value.clone()),
        Value::Blob(_) => Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "client state value was a blob instead of JSON",
        )),
    }
}
