//! Row-first synchronization internals.
//!
//! A client exports completed local work as a transaction pack of semantic row
//! replacements and tombstones. The pack names the authoritative branch and
//! server commit on which that work began. A server serializes admission per
//! repository and stages the rows directly through Lix's ordinary transaction
//! pipeline, atomically publishing the canonical commit and replay receipt.
//! Different row identities compose. When packs replace the same row identity,
//! authoritative admission order is last-writer-wins; branch three-way mergers
//! are intentionally outside this MVP path.
//!
//! File-owned plugin rows retain their stable `file_id`. Inline file payloads
//! changed by the transaction are carried alongside those rows so ordinary
//! filesystem files can follow the same admission path. Large prepared-CAS
//! payloads remain a later blob-transfer optimization; the 90% path uses the
//! inline bytes already present in the transaction.

#[cfg(not(target_family = "wasm"))]
mod runtime;
#[cfg(not(target_family = "wasm"))]
mod transport;

#[cfg(not(target_family = "wasm"))]
pub(crate) use runtime::{SyncRuntime, activate_sync_mode};

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::{future::Future, pin::Pin};

use serde::{Deserialize, Serialize};

use crate::binary_cas::BlobDataReader;
use crate::changelog::CommitId;
use crate::json_store::{JsonLoadRequestRef, JsonReadScopeRef, JsonSlot, JsonStoreContext};
use crate::session::ExecuteIdempotency;
use crate::storage_adapter::{
    Storage, StorageAdapterRead, StorageCoreProjection, StorageGetManyRequest, StorageGetOptions,
    StorageKey, StoragePrecondition, StorageProjectedValue, StorageReadOptions, StorageSpace,
    StorageSpaceId, StorageWriteOptions, StorageWriteSet, ValueSemantics, exact_get_many,
};
use crate::tracked_state::{TrackedStateContext, TrackedStateDiffRequest, TrackedStateFilter};
use crate::transaction::PreparedWriteSet;
use crate::transaction_types::{RawWriteBatch, TransactionJson, TransactionWriteRow};
use crate::{CreateBranchOptions, GLOBAL_BRANCH_ID, Lix, LixError, Value};

pub(crate) const SYNC_EVENT_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_0008),
    "sync.canonical_event.v1",
    ValueSemantics::Immutable,
);

pub(crate) const SYNC_HEAD_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_0009),
    "sync.branch_head.v1",
    ValueSemantics::Mutable,
);

pub(crate) const SYNC_CLIENT_STATE_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_000a),
    "sync.client_state.v1",
    ValueSemantics::Mutable,
);

pub(crate) const SYNC_REPLICA_CONFIG_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_000b),
    "sync.replica_config.v1",
    ValueSemantics::Mutable,
);

/// Each pending transaction pack is stored independently from the small
/// branch manifest. This keeps a long offline queue from rewriting all row
/// and file payloads whenever only the cursor or one acknowledgement changes.
pub(crate) const SYNC_CLIENT_PENDING_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_000c),
    "sync.client_pending.v1",
    ValueSemantics::Mutable,
);

/// A per-replica, per-scope receipt for a canonical event whose row/plugin
/// transaction has already committed.  It is deliberately separate from the
/// cursor manifest: the marker is written in the same storage commit as the
/// applied rows, so a crash between row application and cursor persistence
/// cannot make the next pull execute plugin reconciliation twice.
pub(crate) const SYNC_CLIENT_APPLIED_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_000d),
    "sync.client_applied.v1",
    ValueSemantics::Mutable,
);

/// Internal demand marker used when the SQL shape cannot be mapped to a
/// finite set of relations safely (for example a CTE or nested subquery).
/// Such queries take the correctness-first full-history path.
const FULL_SYNC_SCOPE: &str = "\0__lix_sync_all__";
/// Readiness marker for the global branch/commit control plane. It is
/// intentionally separate from semantic row scopes because topology is
/// reconciled by a compact catalog pull, not by user row packs.
pub(crate) const CONTROL_SYNC_SCOPE: &str = "\0__lix_sync_control__";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum SyncRole {
    #[default]
    Disabled,
    Authority,
    Replica {
        remote_id: String,
    },
}

/// Process-local synchronization role shared by every session of one engine.
#[derive(Clone, Debug)]
pub(crate) struct SyncModeState {
    role: Arc<RwLock<SyncRole>>,
    scopes: Arc<RwLock<BTreeSet<String>>>,
    hydrated_scopes: Arc<RwLock<BTreeSet<String>>>,
    scope_notify: Arc<tokio::sync::Notify>,
    internal_scope_depth: Arc<AtomicU64>,
    /// Monotonically changes whenever the active branch changes. Hydration is
    /// asynchronous, so an old-branch worker must not publish readiness after
    /// a switch has reset the process-local marks.
    scope_generation: Arc<AtomicU64>,
}

impl Default for SyncModeState {
    fn default() -> Self {
        Self {
            role: Arc::new(RwLock::new(SyncRole::Disabled)),
            scopes: Arc::new(RwLock::new(BTreeSet::new())),
            hydrated_scopes: Arc::new(RwLock::new(BTreeSet::new())),
            scope_notify: Arc::new(tokio::sync::Notify::new()),
            internal_scope_depth: Arc::new(AtomicU64::new(0)),
            scope_generation: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl SyncModeState {
    pub(crate) fn role(&self) -> Result<SyncRole, LixError> {
        self.role.read().map(|role| role.clone()).map_err(|_| {
            LixError::new(LixError::CODE_INTERNAL_ERROR, "sync mode state is poisoned")
        })
    }

    pub(crate) fn set_role(&self, role: SyncRole) -> Result<(), LixError> {
        *self.role.write().map_err(|_| {
            LixError::new(LixError::CODE_INTERNAL_ERROR, "sync mode state is poisoned")
        })? = role;
        Ok(())
    }

    /// Registers the schemas touched by an application query. Lazy sync uses
    /// this process-local set as its demand signal; the durable cursor remains
    /// independent so a replica can advance past unrelated repository data.
    pub(crate) fn register_sql_scope(&self, sql: &str) {
        if self.internal_scope_depth.load(Ordering::Acquire) != 0 {
            return;
        }
        let schemas = extract_sql_scope_schema_keys(sql);
        if schemas.is_empty() {
            return;
        }
        let schemas = if schemas.len() > MAX_SYNC_SCOPE_KEYS
            || schemas
                .iter()
                .any(|schema| schema.len() > MAX_SYNC_SCOPE_KEY_BYTES)
        {
            vec![FULL_SYNC_SCOPE.to_owned()]
        } else {
            schemas
        };
        let full_hydrated = self
            .hydrated_scopes
            .read()
            .is_ok_and(|hydrated| hydrated.contains(FULL_SYNC_SCOPE));
        if let Ok(mut scopes) = self.scopes.write() {
            if schemas.iter().any(|schema| schema == FULL_SYNC_SCOPE)
                || scopes.len().saturating_add(schemas.len()) > MAX_SYNC_SCOPE_KEYS
            {
                // An unbounded demand set would make every subsequent
                // manifest write grow without limit. A complete-history
                // marker is the correctness-preserving fallback.
                scopes.clear();
                scopes.insert(FULL_SYNC_SCOPE.to_owned());
            } else {
                scopes.extend(schemas.iter().cloned());
            }
        }
        // Once a full history has been materialized, any later demand is
        // already satisfied. Marking the newly discovered relation prevents a
        // subsequent observer from waiting forever for a redundant hydrate.
        if full_hydrated && let Ok(mut hydrated) = self.hydrated_scopes.write() {
            hydrated.extend(schemas);
            self.scope_notify.notify_waiters();
        }
    }

    pub(crate) fn scopes(&self) -> Vec<String> {
        self.scopes
            .read()
            .map(|scopes| scopes.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub(crate) fn mark_scope_hydrated(&self, schema_key: &str) {
        self.mark_scope_hydrated_if_generation(schema_key, self.scope_generation());
    }

    pub(crate) fn scope_generation(&self) -> u64 {
        self.scope_generation.load(Ordering::Acquire)
    }

    pub(crate) fn mark_scope_hydrated_if_generation(&self, schema_key: &str, generation: u64) {
        if self.scope_generation() != generation {
            return;
        }
        if let Ok(mut hydrated) = self.hydrated_scopes.write() {
            if self.scope_generation() != generation {
                return;
            }
            hydrated.insert(schema_key.to_owned());
            self.scope_notify.notify_waiters();
        }
    }

    /// Drops the process-local hydration marks after a replica switches
    /// branches. Hydration is branch-specific durable state, but this demand
    /// registry is shared by all sessions in an engine. Keeping marks from the
    /// previous branch would let the next query skip its first hydration and
    /// observe rows from the wrong branch until the polling worker caught up.
    pub(crate) fn reset_scope_hydration(&self) {
        self.scope_generation.fetch_add(1, Ordering::AcqRel);
        if let Ok(mut hydrated) = self.hydrated_scopes.write() {
            hydrated.clear();
            self.scope_notify.notify_waiters();
        }
    }

    pub(crate) async fn wait_for_scope_hydration(&self) -> Result<(), LixError> {
        #[cfg(not(target_family = "wasm"))]
        {
            if self.internal_scope_depth.load(Ordering::Acquire) != 0 {
                return Ok(());
            }
            if !matches!(self.role(), Ok(SyncRole::Replica { .. })) {
                return Ok(());
            }
            let deadline = std::time::Duration::from_secs(5);
            let wait = async {
                loop {
                    let ready = self
                        .scopes
                        .read()
                        .ok()
                        .zip(self.hydrated_scopes.read().ok())
                        .is_some_and(|(scopes, hydrated)| {
                            scopes.iter().all(|scope| hydrated.contains(scope))
                        });
                    if ready {
                        return;
                    }
                    self.scope_notify.notified().await;
                }
            };
            tokio::time::timeout(deadline, wait).await.map_err(|_| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync scope hydration did not complete before the readiness deadline",
                )
                .with_hint(
                    "Retry after the replica reconnects, or query only data already hydrated locally.",
                )
            })?;
        }
        Ok(())
    }

    /// Suppresses scope registration/readiness waits for internal sync
    /// catalog maintenance. Public SQL never obtains this guard.
    pub(crate) fn begin_internal_scope(&self) -> InternalScopeGuard {
        self.internal_scope_depth.fetch_add(1, Ordering::AcqRel);
        InternalScopeGuard {
            depth: Arc::clone(&self.internal_scope_depth),
        }
    }
}

pub(crate) struct InternalScopeGuard {
    depth: Arc<AtomicU64>,
}

impl Drop for InternalScopeGuard {
    fn drop(&mut self) {
        self.depth.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Conservative SQL scope extraction for the lazy-sync MVP. The SQL planner
/// remains authoritative for execution; this lexer only finds relation names
/// after common table-introducing keywords and therefore opts into a complete
/// history hydrate rather than risking a false-negative scope.
fn extract_sql_scope_schema_keys(sql: &str) -> Vec<String> {
    // Preserve correctness for quoted/case-sensitive and qualified
    // identifiers. The lightweight lexer below intentionally has no SQL
    // parser; lowercasing or splitting `namespace.table` could hydrate a
    // different relation and return an empty result. A complete hydrate is
    // the safe fallback for those shapes (and is still bounded by the normal
    // canonical pull limits).
    if has_unsafe_qualified_identifier(sql) {
        return vec![FULL_SYNC_SCOPE.to_owned()];
    }
    let tokens = sql
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .filter(|token| !token.is_empty())
        .map(|token| token.to_ascii_lowercase())
        .collect::<Vec<_>>();
    // The lexer is intentionally conservative. A false positive scope can
    // cause an unnecessary request, but a false negative can return an empty
    // query result. Complex relation syntax therefore opts into a complete
    // canonical-history hydrate.
    let has_unsafe_shape = sql.contains("--")
        || sql.contains("/*")
        || tokens.iter().any(|token| {
            matches!(
                token.as_str(),
                "with" | "union" | "intersect" | "except" | "returning"
            )
        });
    let mut scopes = BTreeSet::new();
    for (index, token) in tokens.iter().enumerate() {
        if matches!(token.as_str(), "from" | "join" | "into" | "update")
            && let Some(table) = tokens.get(index + 1)
        {
            if table == "select" {
                return vec![FULL_SYNC_SCOPE.to_owned()];
            }
            if is_sync_control_schema(table) {
                scopes.insert(CONTROL_SYNC_SCOPE.to_owned());
            } else if !table.starts_with("lix_") {
                scopes.insert(table.clone());
            }
        }
        if token == "lix_file" {
            scopes.insert(token.clone());
        }
    }
    if has_unsafe_shape {
        vec![FULL_SYNC_SCOPE.to_owned()]
    } else {
        scopes.into_iter().collect()
    }
}

fn is_sync_control_schema(schema_key: &str) -> bool {
    matches!(
        schema_key,
        "lix_branch"
            | "lix_change"
            | "lix_commit"
            | "lix_commit_edge"
            | "lix_diff"
            | "lix_working_diff"
            | "lix_branch_descriptor"
            | "lix_branch_ref"
    )
}

/// Returns true for identifier quoting/qualification that the token lexer
/// cannot preserve. Punctuation inside a SQL string literal is data, not
/// relation syntax (for example the common `WHERE path = '/file.md'` query),
/// so it must not force an unnecessarily expensive full-history hydrate.
fn has_unsafe_qualified_identifier(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut single_quoted = false;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if single_quoted {
            if byte == b'\'' {
                if bytes.get(index + 1) == Some(&b'\'') {
                    index += 2;
                    continue;
                }
                single_quoted = false;
            }
            index += 1;
            continue;
        }
        match byte {
            b'\'' => single_quoted = true,
            b'"' | b'.' | b'[' | b']' => return true,
            _ => {}
        }
        index += 1;
    }
    single_quoted
}

pub const DEFAULT_SYNC_PULL_LIMIT: usize = 128;
pub const MAX_SYNC_PULL_LIMIT: usize = 512;
pub(crate) const MAX_SYNC_SCOPE_KEYS: usize = 64;
pub(crate) const MAX_SYNC_SCOPE_KEY_BYTES: usize = 255;
pub(crate) const MAX_SYNC_PACK_ROWS: usize = 8_192;
pub(crate) const MAX_SYNC_PACK_FILES: usize = 1_024;
// Leave framing headroom under the protocol's default 64 MiB request limit.
// File bytes are base64-encoded on JSON transport, so the encoded pack—not
// only its decoded payload—must fit below this ceiling.
pub(crate) const MAX_SYNC_PACK_BYTES: usize = 60 * 1024 * 1024;
pub(crate) const MAX_SYNC_PENDING_OPERATIONS: usize = 4_096;
pub(crate) const MAX_SYNC_PENDING_BYTES: usize = 128 * 1024 * 1024;
pub(crate) const MAX_SYNC_PULL_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const MAX_SYNC_REMOTE_ID_BYTES: usize = 4 * 1024;
const MAX_SYNC_MARKER_BYTES: usize = 16 * 1024;
// Legacy v1 manifests embedded the pending queue, so retain enough room to
// migrate the documented offline queue limit while bounding corrupt state.
const MAX_SYNC_CLIENT_MANIFEST_BYTES: usize = MAX_SYNC_PENDING_BYTES + 4 * 1024 * 1024;

fn validate_sync_identity_component(
    field: &str,
    value: &str,
    max_bytes: usize,
) -> Result<(), LixError> {
    if value.is_empty() || value.len() > max_bytes {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync {field} must contain 1 to {max_bytes} bytes"),
        ));
    }
    Ok(())
}

fn validate_sync_remote_id(remote_id: &str) -> Result<(), LixError> {
    validate_sync_identity_component("remoteId", remote_id, MAX_SYNC_REMOTE_ID_BYTES)
}

fn validate_sync_branch_id(branch_id: &str) -> Result<(), LixError> {
    validate_sync_identity_component("branchId", branch_id, MAX_SYNC_SCOPE_KEY_BYTES)
}

fn validate_sync_operation_id(operation_id: &str) -> Result<(), LixError> {
    validate_sync_identity_component("operationId", operation_id, MAX_SYNC_SCOPE_KEY_BYTES)
}

fn validate_optional_sync_commit_id(commit_id: Option<&str>) -> Result<(), LixError> {
    if let Some(commit_id) = commit_id {
        validate_sync_identity_component("serverCommitId", commit_id, MAX_SYNC_SCOPE_KEY_BYTES)?;
    }
    Ok(())
}

/// One semantic row replacement inside a sync transaction pack.
///
/// `snapshot` is `None` for a tombstone. File-owned plugin rows carry their
/// stable `file_id`; any changed inline bytes are carried by the pack's
/// [`SyncFileMutation`] entries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncRowMutation {
    pub schema_key: String,
    pub file_id: Option<String>,
    pub row_pk: serde_json::Value,
    pub snapshot: Option<serde_json::Value>,
    pub metadata: Option<serde_json::Value>,
}

/// Inline bytes belonging to a file mutation in a transaction pack.
///
/// The descriptor/blob-ref rows remain the source of identity and path
/// semantics. This payload is only the content companion needed to materialize
/// the file on a replica. `content` is intentionally allowed to be empty so an
/// empty write or deletion can clear an existing file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncFileMutation {
    pub file_id: String,
    pub path: Option<String>,
    pub filename: Option<String>,
    pub global: bool,
    pub untracked: bool,
    /// Base64 is used on the JSON wire so binary files do not expand into one
    /// JSON number per byte. The decoded representation remains an owned
    /// byte vector at the transaction boundary.
    #[serde(with = "base64_bytes")]
    pub content: Vec<u8>,
}

mod base64_bytes {
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(bytes))
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(encoded.as_bytes())
            .map_err(serde::de::Error::custom)
    }
}

/// A completed local Lix transaction represented as semantic row writes.
///
/// `operation_id` is the idempotency key for server admission.
/// `base_server_commit_id` is the server-history cursor on which the local work
/// began; it need not equal any commit ID in the client's local history.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncTransactionPack {
    pub operation_id: String,
    pub branch_id: String,
    pub base_server_commit_id: String,
    pub local_commit_id: String,
    #[serde(default)]
    pub rows: Vec<SyncRowMutation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<SyncFileMutation>,
}

/// Validates resource-bearing fields before they reach the transaction
/// planner. The HTTP body limit is intentionally larger than one pack so a
/// request can carry protocol framing, but a single pack and offline queue are
/// bounded independently of that transport setting.
pub(crate) fn validate_sync_transaction_pack(
    pack: &SyncTransactionPack,
) -> Result<usize, LixError> {
    if pack.operation_id.is_empty() || pack.operation_id.len() > MAX_SYNC_SCOPE_KEY_BYTES {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync operationId must contain 1 to 255 bytes",
        ));
    }
    for (field, value) in [
        ("branchId", &pack.branch_id),
        ("baseServerCommitId", &pack.base_server_commit_id),
        ("localCommitId", &pack.local_commit_id),
    ] {
        if value.is_empty() || value.len() > MAX_SYNC_SCOPE_KEY_BYTES {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("sync {field} must contain 1 to 255 bytes"),
            ));
        }
    }
    if pack.rows.len() > MAX_SYNC_PACK_ROWS {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync pack contains too many rows (maximum {MAX_SYNC_PACK_ROWS})"),
        ));
    }
    if pack.files.len() > MAX_SYNC_PACK_FILES {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync pack contains too many files (maximum {MAX_SYNC_PACK_FILES})"),
        ));
    }
    if pack.rows.is_empty() && pack.files.is_empty() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync pack must contain at least one row or file mutation",
        ));
    }
    let encoded = serde_json::to_vec(pack).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("encode sync transaction pack: {error}"),
        )
    })?;
    if encoded.len() > MAX_SYNC_PACK_BYTES {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync transaction pack exceeds {MAX_SYNC_PACK_BYTES} bytes"),
        ));
    }
    for row in &pack.rows {
        if row.schema_key.is_empty() || row.schema_key.len() > MAX_SYNC_SCOPE_KEY_BYTES {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync row schemaKey must contain 1 to 255 bytes",
            ));
        }
        if row
            .file_id
            .as_ref()
            .is_some_and(|file_id| file_id.len() > MAX_SYNC_SCOPE_KEY_BYTES)
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync row fileId must contain at most 255 bytes",
            ));
        }
    }
    for file in &pack.files {
        if file.file_id.is_empty() || file.file_id.len() > MAX_SYNC_SCOPE_KEY_BYTES {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync fileId must contain 1 to 255 bytes",
            ));
        }
        if file.global || file.untracked {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync file mutations must be tracked branch-local files",
            ));
        }
        if file.path.as_deref().is_none_or(str::is_empty) {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync file mutation is missing its logical path",
            ));
        }
        for (field, value) in [
            ("path", file.path.as_deref()),
            ("filename", file.filename.as_deref()),
        ] {
            if value.is_some_and(|value| value.len() > 4_096) {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("sync file {field} is too long"),
                ));
            }
        }
    }
    Ok(encoded.len())
}

fn validate_sync_canonical_event_identity(event: &SyncCanonicalEvent) -> Result<(), LixError> {
    if event.cursor == 0
        || event.canonical_commit_id.is_empty()
        || event.canonical_commit_id.len() > MAX_SYNC_SCOPE_KEY_BYTES
        || (!event.pack_fingerprint.is_empty() && event.pack_fingerprint.len() != 64)
    {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync canonical event has an invalid cursor or commit identity",
        ));
    }
    Ok(())
}

/// One transaction in the authoritative order of a branch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncCanonicalEvent {
    pub cursor: u64,
    pub canonical_commit_id: String,
    /// Digest of the unfiltered canonical pack. Scoped responses retain it so
    /// applied markers compare the same event regardless of projection order.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub pack_fingerprint: String,
    pub pack: SyncTransactionPack,
}

/// Ordered canonical events returned by one pull request.
///
/// A scoped pull may carry an empty pack for a cursor position whose payload is
/// outside the requested schema scope. The event still advances the global
/// authoritative cursor without materializing unrelated rows locally.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncPullResponse {
    pub branch_id: String,
    pub events: Vec<SyncCanonicalEvent>,
    pub next_cursor: u64,
    pub head_cursor: u64,
    pub head_commit_id: String,
}

/// Durable acknowledgement returned after authoritative admission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncAdmission {
    pub operation_id: String,
    pub branch_id: String,
    pub canonical_commit_id: String,
    pub cursor: u64,
}

/// Branch catalog entry used by the background replica worker. Branch
/// topology is control-plane state, so it is intentionally separate from
/// row transaction packs and never appears as a public sync API.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncBranch {
    pub(crate) id: String,
    pub(crate) name: String,
    pub(crate) hidden: bool,
    pub(crate) commit_id: String,
}

/// A boxed request future returned by [`SyncTransport`].
#[cfg(not(target_family = "wasm"))]
pub type SyncTransportFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, LixError>> + Send + 'a>>;

#[cfg(target_family = "wasm")]
pub type SyncTransportFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, LixError>> + 'a>>;

/// The two operations required from a remote sync server.
///
/// The Server Protocol is one transport for this interface. Keeping the core
/// state machine transport-neutral also lets embedded hosts and tests connect
/// two Lix repositories without making the database depend on an HTTP client.
#[cfg(not(target_family = "wasm"))]
pub trait SyncTransport: Send + Sync {
    /// Stable identity of the remote repository (normally its canonical URL).
    fn remote_id(&self) -> &str;

    fn admit<'a>(&'a self, pack: &'a SyncTransactionPack)
    -> SyncTransportFuture<'a, SyncAdmission>;

    fn pull<'a>(
        &'a self,
        branch_id: &'a str,
        after_cursor: u64,
        limit: usize,
        // Empty means an unscoped/head-only request; non-empty requests only
        // materialize row/file payloads for these schema keys.
        schema_keys: &'a [String],
    ) -> SyncTransportFuture<'a, SyncPullResponse>;

    /// Returns the current global branch catalog. Transports that do not
    /// expose control-plane enumeration can leave this empty; row sync remains
    /// fully functional and the runtime retries topology after reconnect.
    fn list_branches<'a>(&'a self) -> SyncTransportFuture<'a, Vec<SyncBranch>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

/// Browser/WASM variant of [`SyncTransport`] whose fetch futures and handles
/// are not required to cross threads.
#[cfg(target_family = "wasm")]
pub trait SyncTransport {
    fn remote_id(&self) -> &str;

    fn admit<'a>(&'a self, pack: &'a SyncTransactionPack)
    -> SyncTransportFuture<'a, SyncAdmission>;

    fn pull<'a>(
        &'a self,
        branch_id: &'a str,
        after_cursor: u64,
        limit: usize,
        // Empty means an unscoped/head-only request; non-empty requests only
        // materialize row/file payloads for these schema keys.
        schema_keys: &'a [String],
    ) -> SyncTransportFuture<'a, SyncPullResponse>;

    fn list_branches<'a>(&'a self) -> SyncTransportFuture<'a, Vec<SyncBranch>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

/// Applies the remote global branch catalog to a local repository. This is a
/// control-plane reconciliation path, separate from semantic row events. A
/// branch whose source commit is not materialized locally yet is left for a
/// later pass rather than being created at an incorrect head.
pub(crate) async fn reconcile_sync_branches<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    transport: &Transport,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    let remote_branches = transport.list_branches().await?;
    let active_branch_id = lix.active_branch_id().await?;
    for remote in remote_branches {
        if remote.id == active_branch_id || remote.id == GLOBAL_BRANCH_ID {
            continue;
        }
        let existing = lix
            .execute(
                "SELECT id, name, hidden, commit_id FROM lix_branch WHERE id = $1",
                &[Value::Text(remote.id.clone())],
            )
            .await?;
        if existing.rows().is_empty() {
            match lix
                .create_branch(CreateBranchOptions {
                    id: Some(remote.id.clone()),
                    name: remote.name.clone(),
                    from_commit_id: Some(remote.commit_id.clone()),
                })
                .await
            {
                // A lazy replica may not have materialized the branch's
                // source commit yet. Leave this branch for a later catalog
                // pass after its commit scope is requested.
                Err(error) if error.code == LixError::CODE_COMMIT_NOT_FOUND => {
                    continue;
                }
                Err(error) => return Err(error),
                Ok(_) => {}
            }
        }

        let current_commit = existing
            .rows()
            .first()
            .and_then(|row| row.get::<String>("commit_id").ok());
        let current_name = existing
            .rows()
            .first()
            .and_then(|row| row.get::<String>("name").ok());
        let current_hidden = existing
            .rows()
            .first()
            .and_then(|row| row.get::<bool>("hidden").ok());
        if current_commit.as_deref() != Some(remote.commit_id.as_str())
            || current_name.as_deref() != Some(remote.name.as_str())
            || current_hidden != Some(remote.hidden)
        {
            lix.execute(
                "INSERT INTO lix_branch (id, name, hidden, commit_id) VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (id) DO UPDATE SET name = excluded.name, hidden = excluded.hidden, \
                 commit_id = excluded.commit_id",
                &[
                    Value::Text(remote.id),
                    Value::Text(remote.name),
                    Value::Boolean(remote.hidden),
                    Value::Text(remote.commit_id),
                ],
            )
            .await?;
        }
    }
    Ok(())
}

/// Durable result of draining a client's pending queue.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncFlushReceipt {
    pub cursor: u64,
    pub server_commit_id: String,
    pub pending_operations: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SyncClientState {
    version: u8,
    remote_id: String,
    branch_id: String,
    cursor: u64,
    server_commit_id: Option<String>,
    pending: Vec<SyncTransactionPack>,
    #[serde(default)]
    hydrated_scopes: BTreeSet<String>,
    #[serde(default)]
    scope_cursors: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SyncClientManifest {
    version: u8,
    remote_id: String,
    branch_id: String,
    cursor: u64,
    server_commit_id: Option<String>,
    pending_operations: Vec<String>,
    #[serde(default)]
    hydrated_scopes: BTreeSet<String>,
    #[serde(default)]
    scope_cursors: BTreeMap<String, u64>,
}

const SYNC_CLIENT_STATE_VERSION: u8 = 1;
const SYNC_CLIENT_MANIFEST_VERSION: u8 = 2;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SyncReplicaConfig {
    version: u8,
    remote_id: String,
    branch_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SyncAppliedEventMarker {
    pub(crate) version: u8,
    pub(crate) remote_id: String,
    pub(crate) branch_id: String,
    pub(crate) scope: String,
    pub(crate) cursor: u64,
    pub(crate) canonical_commit_id: String,
    pub(crate) pack_fingerprint: String,
}

const SYNC_APPLIED_EVENT_LEGACY_VERSION: u8 = 1;
const SYNC_APPLIED_EVENT_VERSION: u8 = 2;

/// A local, durable synchronization session for one branch.
///
/// Methods take `&mut self`, deliberately serializing pull, admission, cursor
/// advancement, and pending-overlay replay inside one client instance.
pub struct SyncClient<StorageImpl, Transport>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix: Lix<StorageImpl>,
    transport: Transport,
    state: SyncClientState,
    persisted_value: Option<Vec<u8>>,
    persisted_value_durable: bool,
    pending_storage_dirty: bool,
    persisted_pending_operations: BTreeSet<String>,
    require_certified_bootstrap: bool,
    scope_state: SyncModeState,
}

impl<StorageImpl, Transport> std::fmt::Debug for SyncClient<StorageImpl, Transport>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SyncClient")
            .field("branch_id", &self.state.branch_id)
            .field("cursor", &self.state.cursor)
            .field("pending_operations", &self.state.pending.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct SyncHead {
    cursor: u64,
    commit_id: String,
}

#[derive(Clone, Debug)]
pub(crate) struct SyncAdmissionPlan {
    pub(crate) cursor: u64,
    previous_head_value: Option<Vec<u8>>,
}

impl<StorageImpl> Lix<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    pub(crate) async fn persist_sync_replica_config(
        &self,
        remote_id: &str,
        branch_id: &str,
    ) -> Result<(), LixError> {
        validate_sync_remote_id(remote_id)?;
        validate_sync_branch_id(branch_id)?;
        let config = SyncReplicaConfig {
            version: 1,
            remote_id: remote_id.to_owned(),
            branch_id: branch_id.to_owned(),
        };
        let value = serde_json::to_vec(&config).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("encode sync replica config: {error}"),
            )
        })?;
        let adapter = self.storage_adapter();
        let mut writes = adapter.new_write_set();
        writes.put(
            SYNC_REPLICA_CONFIG_SPACE,
            sync_replica_config_key(remote_id),
            value,
        );
        adapter
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    await_durable: true,
                    ..StorageWriteOptions::default()
                },
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn load_sync_replica_branch(
        &self,
        remote_id: &str,
    ) -> Result<Option<String>, LixError> {
        validate_sync_remote_id(remote_id)?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let values = exact_get_many(
            &read,
            &[StorageGetManyRequest {
                space: SYNC_REPLICA_CONFIG_SPACE,
                keys: &[sync_replica_config_key(remote_id)],
                opts: StorageGetOptions {
                    projection: StorageCoreProjection::FullValue,
                },
            }],
        )
        .await?;
        let Some(StorageProjectedValue::FullValue(value)) =
            values.values.into_iter().next().flatten()
        else {
            return Ok(None);
        };
        let config = serde_json::from_slice::<SyncReplicaConfig>(&value).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("decode sync replica config: {error}"),
            )
        })?;
        if config.version != 1 || config.remote_id != remote_id {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync replica config has an unsupported version or remote identity",
            ));
        }
        validate_sync_remote_id(&config.remote_id)?;
        validate_sync_branch_id(&config.branch_id)?;
        Ok(Some(config.branch_id))
    }

    pub(crate) async fn has_initialized_sync_replica(
        &self,
        remote_id: &str,
    ) -> Result<bool, LixError> {
        validate_sync_remote_id(remote_id)?;
        let branch_id = self.active_branch_id().await?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        Ok(load_sync_client_state(&read, remote_id, &branch_id)
            .await?
            .is_some())
    }

    pub(crate) async fn restore_sync_scope_readiness(
        &self,
        remote_id: &str,
    ) -> Result<(), LixError> {
        validate_sync_remote_id(remote_id)?;
        let branch_id = self.active_branch_id().await?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        if let Some((_, state, _)) = load_sync_client_state(&read, remote_id, &branch_id).await? {
            let sync_mode = self.sync_mode_state();
            for scope in state.hydrated_scopes {
                sync_mode.mark_scope_hydrated(&scope);
            }
        }
        Ok(())
    }

    /// Opens the durable sync state machine for the active branch.
    #[doc(hidden)]
    pub async fn sync<Transport>(
        &self,
        transport: Transport,
    ) -> Result<SyncClient<StorageImpl, Transport>, LixError>
    where
        Transport: SyncTransport,
    {
        self.open_sync_client(transport, false).await
    }

    pub(crate) async fn sync_lifecycle<Transport>(
        &self,
        transport: Transport,
    ) -> Result<SyncClient<StorageImpl, Transport>, LixError>
    where
        Transport: SyncTransport,
    {
        self.open_sync_client(transport, true).await
    }

    async fn open_sync_client<Transport>(
        &self,
        transport: Transport,
        require_certified_bootstrap: bool,
    ) -> Result<SyncClient<StorageImpl, Transport>, LixError>
    where
        Transport: SyncTransport,
    {
        let branch_id = self.active_branch_id().await?;
        let remote_id = transport.remote_id().to_owned();
        validate_sync_remote_id(&remote_id)?;
        validate_sync_branch_id(&branch_id)?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let loaded = load_sync_client_state(&read, &remote_id, &branch_id).await?;
        let (persisted_value, state, persisted_pending_operations) = loaded.map_or_else(
            || {
                (
                    None,
                    SyncClientState {
                        version: SYNC_CLIENT_MANIFEST_VERSION,
                        remote_id,
                        branch_id,
                        cursor: 0,
                        server_commit_id: None,
                        pending: Vec::new(),
                        hydrated_scopes: BTreeSet::new(),
                        scope_cursors: BTreeMap::new(),
                    },
                    BTreeSet::new(),
                )
            },
            |(value, state, persisted_pending_operations)| {
                (Some(value), state, persisted_pending_operations)
            },
        );
        let client = SyncClient {
            lix: self.clone(),
            transport,
            state,
            // A reopened client cannot infer whether the previous manifest
            // write crossed the backend durability boundary. Force the first
            // checkpoint to establish that fence.
            persisted_value_durable: false,
            pending_storage_dirty: false,
            persisted_value,
            persisted_pending_operations,
            require_certified_bootstrap,
            scope_state: self.sync_mode_state(),
        };
        for scope in &client.state.hydrated_scopes {
            client.scope_state.mark_scope_hydrated(scope);
        }
        // A manual client may be opened on a storage snapshot whose local
        // application commit is not the active view anymore (for example
        // after reopening an offline filesystem). Restore that optimistic
        // overlay once. The lifecycle worker skips this because it already
        // owns the live local session and would otherwise create a commit on
        // every polling iteration.
        if !require_certified_bootstrap {
            client.replay_pending_overlay().await?;
        }
        Ok(client)
    }

    /// Exports selected semantic row writes between two local commits.
    ///
    /// Selecting semantic schemas at this boundary excludes derived rows such
    /// as plugin-rendered file state. The resulting pack contains replacement
    /// snapshots and tombstones, not SQL statements or file blobs.
    #[doc(hidden)]
    pub async fn create_sync_transaction_pack(
        &self,
        operation_id: impl Into<String>,
        base_server_commit_id: impl Into<String>,
        from_local_commit_id: &str,
        to_local_commit_id: &str,
        semantic_schema_keys: &[&str],
    ) -> Result<SyncTransactionPack, LixError> {
        if semantic_schema_keys.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync transaction pack requires at least one semantic schema",
            ));
        }
        let branch_id = self.active_branch_id().await?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let mut tracked = TrackedStateContext::new().reader(read);
        let diff = tracked
            .diff_commits(
                from_local_commit_id,
                to_local_commit_id,
                &TrackedStateDiffRequest {
                    filter: TrackedStateFilter {
                        schema_keys: semantic_schema_keys
                            .iter()
                            .map(|key| (*key).to_owned())
                            .collect(),
                        ..TrackedStateFilter::default()
                    },
                    retain_payloads: true,
                },
            )
            .await?;

        let mut rows = Vec::with_capacity(diff.entries.len());
        for entry in &diff.entries {
            let (snapshot, metadata) =
                materialize_diff_payload(tracked.store(), &diff, entry.after.as_ref()).await?;
            rows.push(SyncRowMutation {
                schema_key: entry.identity.schema_key().to_owned(),
                file_id: entry.identity.file_id().map(str::to_owned),
                row_pk: entry.identity.row_pk().as_json_array_value()?,
                snapshot,
                metadata,
            });
        }

        Ok(SyncTransactionPack {
            operation_id: operation_id.into(),
            branch_id,
            base_server_commit_id: base_server_commit_id.into(),
            local_commit_id: to_local_commit_id.to_owned(),
            rows,
            files: Vec::new(),
        })
    }

    /// Stages a semantic row pack through the ordinary transaction pipeline.
    ///
    /// Schema validation, plugin reconciliation, commit creation, and derived
    /// file rendering are therefore identical to direct local row writes. The
    /// server base is a synchronization cursor, not a local commit identity;
    /// applying successive canonical packs creates independent local commits.
    #[doc(hidden)]
    pub async fn apply_sync_transaction_pack(
        &self,
        pack: &SyncTransactionPack,
    ) -> Result<(), LixError> {
        validate_sync_transaction_pack(pack)?;
        self.apply_sync_transaction_pack_inner(pack, None, None, None)
            .await
    }

    /// Applies one canonical event once per requested replica scope. The
    /// applied marker is committed with the row/plugin transaction, so a
    /// process crash before the cursor manifest is persisted is replay-safe.
    /// If overlapping lazy scopes are hydrated at different times, only the
    /// scopes without a receipt are projected and applied.
    async fn apply_sync_canonical_event(
        &self,
        event: &SyncCanonicalEvent,
        remote_id: &str,
        scopes: &[String],
    ) -> Result<(), LixError> {
        validate_sync_canonical_event_identity(event)?;
        validate_sync_transaction_pack(&event.pack)?;
        let pack_fingerprint = if event.pack_fingerprint.is_empty() {
            // Pre-v2 servers omitted the full-pack digest. Commit identity is
            // still projection-independent and is the safe compatibility
            // fallback; new servers always provide the stronger digest.
            sync_event_identity_fingerprint(event)
        } else {
            event.pack_fingerprint.clone()
        };
        let marker_scopes = if scopes.is_empty() {
            vec![FULL_SYNC_SCOPE.to_owned()]
        } else {
            scopes.to_vec()
        };
        let file_view_requested = marker_scopes.iter().any(|scope| scope == "lix_file");
        let mut coverage_scopes = marker_scopes.clone();
        // `lix_file` is the aggregate receipt for the file view, including
        // every plugin schema rendered into it. Keeping one marker instead of
        // one marker per plugin schema prevents a single heterogeneous event
        // from exceeding the bounded marker-scope set.
        coverage_scopes.sort();
        coverage_scopes.dedup();
        if coverage_scopes.len() > MAX_SYNC_SCOPE_KEYS {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!(
                    "sync event touches too many marker scopes (maximum {MAX_SYNC_SCOPE_KEYS})"
                ),
            ));
        }
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let mut previous = BTreeMap::new();
        let mut missing = Vec::new();
        for scope in &coverage_scopes {
            let marker =
                load_sync_applied_marker(&read, remote_id, &event.pack.branch_id, scope).await?;
            let already_applied = marker
                .as_ref()
                .is_some_and(|(_, marker)| marker_covers_event(marker, event, &pack_fingerprint));
            if let Some((_, marker)) = marker.as_ref()
                && marker.cursor == event.cursor
                && (marker.canonical_commit_id != event.canonical_commit_id
                    || (marker.version != SYNC_APPLIED_EVENT_LEGACY_VERSION
                        && marker.pack_fingerprint != pack_fingerprint))
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "sync applied marker for scope '{scope}' disagrees at cursor {}",
                        event.cursor
                    ),
                ));
            }
            if !already_applied {
                missing.push(scope.clone());
            }
            previous.insert(scope.clone(), marker);
        }
        if missing.is_empty() {
            return Ok(());
        }

        let mut pack = event.pack.clone();
        if marker_scopes[0] != FULL_SYNC_SCOPE {
            let wanted = marker_scopes.iter().cloned().collect::<HashSet<_>>();
            pack.rows.retain(|row| {
                let in_scope = wanted.contains(&row.schema_key)
                    || (file_view_requested
                        && row.file_id.is_some()
                        && !row.schema_key.starts_with("lix_"));
                let semantic_marker_covers_row = previous
                    .get(&row.schema_key)
                    .and_then(|marker| marker.as_ref())
                    .is_some_and(|(_, marker)| {
                        marker_covers_event(marker, event, &pack_fingerprint)
                    });
                let file_marker_covers_row = file_view_requested
                    && row.file_id.is_some()
                    && previous
                        .get("lix_file")
                        .and_then(|marker| marker.as_ref())
                        .is_some_and(|(_, marker)| {
                            marker_covers_event(marker, event, &pack_fingerprint)
                        });
                in_scope && !semantic_marker_covers_row && !file_marker_covers_row
            });
            if !file_view_requested {
                pack.files.clear();
            } else if previous
                .get("lix_file")
                .and_then(|marker| marker.as_ref())
                .is_some_and(|(_, marker)| marker_covers_event(marker, event, &pack_fingerprint))
            {
                pack.files.clear();
            }
        }
        let markers = missing
            .into_iter()
            .filter_map(|scope| {
                let previous_value = previous
                    .get(&scope)
                    .and_then(|marker| marker.as_ref().map(|(value, _)| value.clone()));
                Some((scope, previous_value))
            })
            .map(|(scope, previous)| {
                (
                    SyncAppliedEventMarker {
                        version: SYNC_APPLIED_EVENT_VERSION,
                        remote_id: remote_id.to_owned(),
                        branch_id: event.pack.branch_id.clone(),
                        scope,
                        cursor: event.cursor,
                        canonical_commit_id: event.canonical_commit_id.clone(),
                        pack_fingerprint: pack_fingerprint.clone(),
                    },
                    previous,
                )
            })
            .collect::<Vec<_>>();
        self.apply_sync_transaction_pack_inner(
            &pack,
            None,
            Some(&markers),
            Some(&event.canonical_commit_id),
        )
            .await
    }

    /// Replays the complete optimistic queue in one local transaction. The
    /// canonical server events are still applied one at a time so cursor
    /// advancement remains durable, but pending local work is only a view
    /// overlay and does not need one commit per operation.
    async fn apply_sync_transaction_packs(
        &self,
        packs: &[SyncTransactionPack],
    ) -> Result<(), LixError> {
        let Some(_) = packs.first() else {
            return Ok(());
        };
        let mut transaction = self.begin_transaction().await?;
        let branch_id = transaction.active_branch_id()?.to_owned();
        for pack in packs {
            validate_sync_transaction_pack(pack)?;
            require_branch(&branch_id, &pack.branch_id)?;
            let rows = sync_write_batch(pack, &branch_id)?;
            if rows.is_empty() && pack.files.is_empty() {
                continue;
            }
            transaction
                .stage_sync_pack(rows, pack.files.clone())
                .await?;
        }
        transaction.commit().await
    }

    /// Stages a target-branch pack on the current proposal branch.
    ///
    /// Alternate merge-based admission implementations can use this after
    /// validating `authoritative_branch_id` and opening a staging branch at
    /// `pack.base_server_commit_id`. The pack remains immutable.
    #[doc(hidden)]
    pub async fn stage_sync_transaction_pack(
        &self,
        pack: &SyncTransactionPack,
        authoritative_branch_id: &str,
    ) -> Result<(), LixError> {
        require_branch(authoritative_branch_id, &pack.branch_id)?;
        self.apply_sync_transaction_pack_inner(pack, Some(authoritative_branch_id), None, None)
            .await
    }

    pub(crate) async fn admit_sync_transaction_pack(
        &self,
        pack: &SyncTransactionPack,
        idempotency: &ExecuteIdempotency,
    ) -> Result<SyncAdmission, LixError> {
        validate_sync_transaction_pack(pack)?;
        require_branch(&self.active_branch_id().await?, &pack.branch_id)?;
        self.require_sync_base_on_active_branch(&pack.base_server_commit_id)
            .await?;
        let admission_plan = self.sync_admission_plan(&pack.branch_id).await?;
        let mut transaction = self.begin_transaction().await?;
        require_branch(transaction.active_branch_id()?, &pack.branch_id)?;
        let transaction_branch_id = transaction.active_branch_id()?.to_owned();
        let rows = sync_write_batch(pack, &transaction_branch_id)?;
        if !rows.is_empty() || !pack.files.is_empty() {
            transaction
                .stage_sync_pack(rows, pack.files.clone())
                .await?;
        }
        let admission = transaction
            .stage_sync_admission_receipt(idempotency, pack, admission_plan)
            .await?;
        transaction.commit().await?;
        Ok(admission)
    }

    pub(crate) async fn pull_sync_events(
        &self,
        branch_id: &str,
        after_cursor: u64,
        limit: usize,
        schema_keys: Option<&[String]>,
    ) -> Result<SyncPullResponse, LixError> {
        require_branch(&self.active_branch_id().await?, branch_id)?;
        if limit > MAX_SYNC_PULL_LIMIT {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("sync pull limit must be between 0 and {MAX_SYNC_PULL_LIMIT}"),
            ));
        }
        if let Some(schema_keys) = schema_keys {
            if schema_keys.len() > MAX_SYNC_SCOPE_KEYS {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("sync pull requests too many schemas (maximum {MAX_SYNC_SCOPE_KEYS})"),
                ));
            }
            if schema_keys
                .iter()
                .any(|schema| schema.is_empty() || schema.len() > MAX_SYNC_SCOPE_KEY_BYTES)
            {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("sync schema keys must contain 1 to {MAX_SYNC_SCOPE_KEY_BYTES} bytes"),
                ));
            }
        }
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let stored_head = load_sync_head(&read, branch_id).await?;
        let (head_cursor, head_commit_id) = if let Some((_, head)) = stored_head.as_ref() {
            (head.cursor, head.commit_id.clone())
        } else {
            let current_commit_id = self
                .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
                .await?
                .rows()[0]
                .get::<String>("commit_id")?;
            (0, current_commit_id)
        };
        if after_cursor > head_cursor {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("sync cursor {after_cursor} is ahead of branch head cursor {head_cursor}"),
            ));
        }
        let event_count = usize::try_from(head_cursor.saturating_sub(after_cursor))
            .unwrap_or(usize::MAX)
            .min(limit);
        let keys = (1..=event_count)
            .map(|offset| {
                let offset = u64::try_from(offset).expect("bounded sync pull offset fits u64");
                sync_event_key(branch_id, after_cursor + offset)
            })
            .collect::<Vec<_>>();
        // Build the scope index once per pull page. Rebuilding it in the event
        // iterator would add an allocation per canonical event and erase the
        // benefit of constant-time row membership checks on large pages.
        let schema_scope = schema_keys.map(SyncFilterScope::new);
        let mut events = Vec::with_capacity(event_count);
        let mut encoded_events_len = 0usize;
        for (index, key) in keys.iter().enumerate() {
            // Read one canonical value at a time. A single pack is bounded,
            // but a 512-event page must not allocate hundreds of megabytes
            // before the response-size guard can reject it.
            let values = exact_get_many(
                &read,
                &[StorageGetManyRequest {
                    space: SYNC_EVENT_SPACE,
                    keys: std::slice::from_ref(key),
                    opts: StorageGetOptions {
                        projection: StorageCoreProjection::FullValue,
                    },
                }],
            )
            .await?;
            let Some(StorageProjectedValue::FullValue(value)) =
                values.values.into_iter().next().flatten()
            else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "sync event {} is missing below advertised head cursor {head_cursor}",
                        after_cursor + u64::try_from(index).unwrap_or(u64::MAX) + 1
                    ),
                ));
            };
            if value.len() > MAX_SYNC_PACK_BYTES + 1024 {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "canonical sync event exceeds its pack size limit",
                ));
            }
            let event = serde_json::from_slice::<SyncCanonicalEvent>(&value).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("decode canonical sync event: {error}"),
                )
            })?;
            validate_sync_canonical_event_identity(&event)?;
            validate_sync_transaction_pack(&event.pack)?;
            if event.pack.branch_id != branch_id {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "canonical sync event is stored under another branch",
                ));
            }
            let event = filter_sync_event(event, schema_scope.as_ref());
            encoded_events_len = encoded_events_len
                .checked_add(
                    serde_json::to_vec(&event)
                        .map_err(|error| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!("encode filtered canonical sync event: {error}"),
                            )
                        })?
                        .len(),
                )
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "sync pull response size overflow",
                    )
                })?;
            if encoded_events_len > MAX_SYNC_PULL_RESPONSE_BYTES {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!(
                        "sync pull response exceeds {MAX_SYNC_PULL_RESPONSE_BYTES} bytes; reduce the pull limit"
                    ),
                ));
            }
            events.push(event);
        }
        let next_cursor = events.last().map_or(after_cursor, |event| event.cursor);
        // Branch switches are independent session operations. Recheck after
        // reading the page so a switch cannot combine branch-A event keys
        // with branch-B head metadata in one response.
        require_branch(&self.active_branch_id().await?, branch_id)?;
        let response = SyncPullResponse {
            branch_id: branch_id.to_owned(),
            events,
            next_cursor,
            head_cursor,
            head_commit_id,
        };
        let encoded_len = serde_json::to_vec(&response)
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("encode sync pull response: {error}"),
                )
            })?
            .len();
        if encoded_len > MAX_SYNC_PULL_RESPONSE_BYTES {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!(
                    "sync pull response exceeds {MAX_SYNC_PULL_RESPONSE_BYTES} bytes; reduce the pull limit"
                ),
            ));
        }
        Ok(response)
    }

    async fn sync_admission_plan(&self, branch_id: &str) -> Result<SyncAdmissionPlan, LixError> {
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let stored = load_sync_head(&read, branch_id).await?;
        let cursor = stored
            .as_ref()
            .map_or(0, |(_, head)| head.cursor)
            .checked_add(1)
            .ok_or_else(|| LixError::new(LixError::CODE_INTERNAL_ERROR, "sync cursor overflow"))?;
        Ok(SyncAdmissionPlan {
            cursor,
            previous_head_value: stored.map(|(value, _)| value),
        })
    }

    pub(crate) async fn require_sync_base_on_active_branch(
        &self,
        base_commit_id: &str,
    ) -> Result<String, LixError> {
        let identity = self
            .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
            .await?;
        let current_commit_id = identity.rows()[0].get::<String>("commit_id")?;
        let current = CommitId::parse_lix(&current_commit_id, "sync active branch commit_id")?;
        let base = CommitId::parse_lix(base_commit_id, "sync base commit_id")?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let mut graph = crate::commit_graph::CommitGraphContext::new().reader(read);
        let merge_base = graph.merge_base(&current, &base).await?;
        if merge_base != base {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!(
                    "sync base commit '{base_commit_id}' is not reachable from active branch head '{current_commit_id}'"
                ),
            ));
        }
        Ok(current_commit_id)
    }

    async fn apply_sync_transaction_pack_inner(
        &self,
        pack: &SyncTransactionPack,
        authoritative_branch_id: Option<&str>,
        applied_markers: Option<&[(SyncAppliedEventMarker, Option<Vec<u8>>)]>,
        canonical_commit_id: Option<&str>,
    ) -> Result<(), LixError> {
        // The canonical event was validated before projection. A projection
        // can legitimately become empty when an overlapping lazy scope has
        // already applied every row in the event; the applied marker still
        // needs its own atomic commit to advance that scope.
        if applied_markers.is_none() {
            validate_sync_transaction_pack(pack)?;
        }
        let mut transaction = self.begin_transaction().await?;
        let branch_id = transaction.active_branch_id()?.to_owned();
        if authoritative_branch_id.is_none() {
            require_branch(&branch_id, &pack.branch_id)?;
        }
        let rows = sync_write_batch(pack, &branch_id)?;
        if rows.is_empty() && pack.files.is_empty() && applied_markers.is_none() {
            return Ok(());
        }
        if authoritative_branch_id.is_some() {
            let identity = transaction
                .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
                .await?;
            let current_commit_id = identity.rows()[0].get::<String>("commit_id")?;
            if current_commit_id != pack.base_server_commit_id {
                return Err(LixError::new(
                    LixError::CODE_TRANSACTION_CONFLICT,
                    format!(
                        "sync staging base mismatch: expected '{}', found '{current_commit_id}'",
                        pack.base_server_commit_id
                    ),
                ));
            }
        }
        transaction
            .stage_sync_pack_with_commit_id(rows, pack.files.clone(), canonical_commit_id)
            .await?;
        if let Some(markers) = applied_markers {
            transaction.stage_sync_applied_event_markers(markers)?;
        }
        transaction.commit().await
    }
}

struct SyncFilterScope<'a> {
    keys: HashSet<&'a str>,
    wants_file_views: bool,
}

impl<'a> SyncFilterScope<'a> {
    fn new(schema_keys: &'a [String]) -> Self {
        let keys = schema_keys
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        let wants_file_views = keys.contains("lix_file");
        Self {
            keys,
            wants_file_views,
        }
    }
}

fn filter_sync_event(
    mut event: SyncCanonicalEvent,
    schema_scope: Option<&SyncFilterScope<'_>>,
) -> SyncCanonicalEvent {
    let Some(schema_scope) = schema_scope else {
        return event;
    };
    filter_sync_pack(&mut event.pack, schema_scope);
    event
}

/// Applies the same lazy scope projection to a transaction pack without
/// changing its operation/branch/cursor identity. Keeping this helper shared
/// by server responses and pending-overlay comparisons is important: a
/// multi-schema local transaction is expected to compare equal to its
/// projected canonical event, not to the full unfiltered pack.
fn filter_sync_pack(pack: &mut SyncTransactionPack, schema_scope: &SyncFilterScope<'_>) {
    // A file read is a demand for its plugin-owned semantic rows as well as
    // raw bytes. Those rows carry stable file IDs; keeping them here lets each
    // replica run its own plugin renderer instead of treating file bytes as
    // canonical state.
    pack.rows.retain(|row| {
        schema_scope.keys.contains(row.schema_key.as_str())
            || (schema_scope.wants_file_views
                && row.file_id.is_some()
                && !row.schema_key.starts_with("lix_"))
    });
    if !schema_scope.wants_file_views {
        pack.files.clear();
    }
}

impl<StorageImpl, Transport> SyncClient<StorageImpl, Transport>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    /// Records one already-committed local transaction in the durable queue.
    ///
    /// A pull happens first so the pack names the newest known server base.
    /// Any downloaded canonical rows are followed by replaying all local
    /// pending packs, preserving the local optimistic view.
    pub async fn enqueue_transaction(
        &mut self,
        operation_id: impl Into<String>,
        from_local_commit_id: &str,
        to_local_commit_id: &str,
        semantic_schema_keys: &[&str],
    ) -> Result<bool, LixError> {
        if self.state.server_commit_id.is_none() {
            self.pull_to_head().await?;
        }
        let base_server_commit_id = self.state.server_commit_id.clone().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync pull did not provide a server head commit",
            )
        })?;
        let pack = self
            .lix
            .create_sync_transaction_pack(
                operation_id,
                base_server_commit_id,
                from_local_commit_id,
                to_local_commit_id,
                semantic_schema_keys,
            )
            .await?;
        if pack.rows.is_empty() {
            return Ok(false);
        }
        validate_sync_transaction_pack(&pack)?;
        if self
            .state
            .pending
            .iter()
            .any(|pending| pending.operation_id == pack.operation_id)
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("sync operation '{}' is already pending", pack.operation_id),
            ));
        }
        self.state.pending.push(pack);
        self.pending_storage_dirty = true;
        self.persist_state().await?;
        Ok(true)
    }

    /// Pulls canonical work and admits queued local transactions until both
    /// sides agree that the durable pending queue is empty.
    pub async fn flush(&mut self) -> Result<SyncFlushReceipt, LixError> {
        self.hydrate_requested_scopes().await?;
        self.pull_to_head().await?;
        while let Some(pack) = self.state.pending.first().cloned() {
            let admission = match self.transport.admit(&pack).await {
                Ok(admission) => admission,
                Err(error) if error.code == LixError::CODE_TRANSACTION_CONFLICT => {
                    // A local transaction may have been created against an
                    // older server head while this replica was offline, or
                    // while another client won admission first. Pull the
                    // authoritative history, preserve the semantic row pack,
                    // and retry it against the newest canonical commit. The
                    // server's admission order remains last-writer-wins for
                    // overlapping row identities.
                    let previous_head = self.state.server_commit_id.clone();
                    self.pull_to_head().await?;
                    let Some(current_head) = self.state.server_commit_id.clone() else {
                        return Err(error);
                    };
                    if previous_head.as_deref() == Some(current_head.as_str())
                        && pack.base_server_commit_id == current_head
                    {
                        return Err(error);
                    }
                    if let Some(pending) = self.state.pending.first_mut() {
                        pending.base_server_commit_id = current_head;
                        self.pending_storage_dirty = true;
                    }
                    self.persist_state().await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            require_sync_identity(
                "admission operation",
                &pack.operation_id,
                &admission.operation_id,
            )?;
            require_sync_identity(
                "admission branch",
                &self.state.branch_id,
                &admission.branch_id,
            )?;
            if admission.cursor <= self.state.cursor {
                self.remove_pending_operation(&admission.operation_id);
                self.persist_state().await?;
            } else {
                let previous_cursor = self.state.cursor;
                self.pull_to_head().await?;
                if self.state.cursor < admission.cursor || self.state.cursor == previous_cursor {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "sync server acknowledged cursor {} but did not publish it",
                            admission.cursor
                        ),
                    ));
                }
            }
        }
        self.pull_to_head().await?;
        // Applied rows/markers are durable per event. Persist the cursor and
        // pending overlay durably once the complete flush has converged;
        // intermediate progress can be replayed safely from those markers.
        self.persist_state().await?;
        Ok(SyncFlushReceipt {
            cursor: self.state.cursor,
            server_commit_id: self.state.server_commit_id.clone().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "sync flush completed without a server head commit",
                )
            })?,
            pending_operations: self.state.pending.len(),
        })
    }

    /// Continuously flushes at `interval` until `shutdown` resolves.
    ///
    /// This polling loop is the MVP real-time motion. Admission and cursor
    /// correctness remain in [`Self::flush`], so a later push notification or
    /// subscription can wake the same state machine without changing it.
    #[cfg(not(target_family = "wasm"))]
    pub async fn run_polling_until<Shutdown>(
        &mut self,
        interval: std::time::Duration,
        shutdown: Shutdown,
    ) -> Result<(), LixError>
    where
        Shutdown: Future<Output = ()>,
    {
        use futures_util::future::{Either, select};

        let mut shutdown = Box::pin(shutdown);
        loop {
            let flush = Box::pin(self.flush());
            let flush_result = match select(flush, shutdown).await {
                Either::Left((result, pending_shutdown)) => {
                    shutdown = pending_shutdown;
                    result
                }
                Either::Right(((), _pending_flush)) => return Ok(()),
            };
            if let Err(error) = flush_result
                && error.code == LixError::CODE_INVALID_PARAM
            {
                return Err(error);
            }
            let delay = Box::pin(tokio::time::sleep(interval));
            match select(delay, shutdown).await {
                Either::Left(((), pending_shutdown)) => shutdown = pending_shutdown,
                Either::Right(((), _pending_delay)) => return Ok(()),
            }
        }
    }

    pub fn pending_operations(&self) -> usize {
        self.state.pending.len()
    }

    pub fn cursor(&self) -> u64 {
        self.state.cursor
    }

    async fn pull_to_head(&mut self) -> Result<(), LixError> {
        let full_hydrated = self.state.hydrated_scopes.contains(FULL_SYNC_SCOPE);
        let schema_keys = self
            .state
            .hydrated_scopes
            .iter()
            .filter(|schema| {
                schema.as_str() != FULL_SYNC_SCOPE && schema.as_str() != CONTROL_SYNC_SCOPE
            })
            .cloned()
            .collect::<Vec<_>>();
        let schema_scope = (!full_hydrated).then(|| SyncFilterScope::new(&schema_keys));
        if schema_keys.is_empty() && !full_hydrated {
            return self.pull_head_only().await;
        }
        loop {
            let response = self
                .transport
                .pull(
                    &self.state.branch_id,
                    self.state.cursor,
                    DEFAULT_SYNC_PULL_LIMIT,
                    &schema_keys,
                )
                .await?;
            require_sync_identity("pull branch", &self.state.branch_id, &response.branch_id)?;
            if response.next_cursor < self.state.cursor
                || response.next_cursor > response.head_cursor
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "sync pull returned invalid cursor bounds",
                ));
            }
            if response.events.is_empty() {
                if response.next_cursor != self.state.cursor {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "sync pull advanced its cursor without events",
                    ));
                }
                if self.require_certified_bootstrap
                    && self.state.cursor == 0
                    && self.state.server_commit_id.is_none()
                {
                    let local_head = self
                        .lix
                        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
                        .await?
                        .rows()[0]
                        .get::<String>("commit_id")?;
                    if local_head != response.head_commit_id {
                        return Err(LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            "sync bootstrap requires matching local and server state when the server has no canonical events",
                        )
                        .with_hint(
                            "Initialize the local replica from the server repository before enabling sync.",
                        ));
                    }
                }
                self.state.server_commit_id = Some(response.head_commit_id);
                self.persist_state().await?;
                return Ok(());
            }
            for event in response.events {
                validate_sync_canonical_event_identity(&event)?;
                let expected_cursor = self.state.cursor.checked_add(1).ok_or_else(|| {
                    LixError::new(LixError::CODE_INTERNAL_ERROR, "sync cursor overflow")
                })?;
                if event.cursor != expected_cursor {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "sync pull cursor gap: expected {expected_cursor}, received {}",
                            event.cursor
                        ),
                    ));
                }
                require_sync_identity(
                    "event branch",
                    &self.state.branch_id,
                    &event.pack.branch_id,
                )?;
                if event.pack.rows.is_empty() && event.pack.files.is_empty() {
                    // A scoped pull advances the authoritative cursor through
                    // unrelated events without materializing their payload.
                    // If the event is our pending operation, its local write
                    // is already visible in the optimistic overlay and can be
                    // acknowledged without comparing against the filtered
                    // payload.
                } else {
                    validate_sync_transaction_pack(&event.pack)?;
                    self.require_matching_pending_event(&event.pack, schema_scope.as_ref())?;
                    let marker_scopes = if full_hydrated {
                        &[][..]
                    } else {
                        schema_keys.as_slice()
                    };
                    self.lix
                        .apply_sync_canonical_event(&event, &self.state.remote_id, marker_scopes)
                        .await?;
                }
                self.state.cursor = event.cursor;
                self.state.server_commit_id = Some(event.canonical_commit_id);
                self.remove_pending_operation(&event.pack.operation_id);
                self.persist_state_progress().await?;
            }
            self.replay_pending_overlay().await?;
            if self.state.cursor >= response.head_cursor {
                return Ok(());
            }
        }
    }

    async fn pull_head_only(&mut self) -> Result<(), LixError> {
        let response = self
            .transport
            .pull(&self.state.branch_id, self.state.cursor, 0, &[])
            .await?;
        require_sync_identity(
            "head pull branch",
            &self.state.branch_id,
            &response.branch_id,
        )?;
        if !response.events.is_empty() || response.next_cursor != self.state.cursor {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "head-only sync pull returned events or advanced its cursor",
            ));
        }
        if self.require_certified_bootstrap
            && self.state.cursor == 0
            && self.state.server_commit_id.is_none()
            && response.head_cursor == 0
        {
            let local_head = self
                .lix
                .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
                .await?
                .rows()[0]
                .get::<String>("commit_id")?;
            if local_head != response.head_commit_id {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync bootstrap requires matching local and server state when the server has no canonical events",
                )
                .with_hint(
                    "Initialize the local replica from the server repository before enabling sync.",
                ));
            }
        }
        self.state.cursor = response.head_cursor;
        self.state.server_commit_id = Some(response.head_commit_id);
        self.persist_state().await?;
        Ok(())
    }

    async fn replay_pending_overlay(&self) -> Result<(), LixError> {
        if self.state.pending.is_empty() {
            return Ok(());
        }
        self.lix
            .apply_sync_transaction_packs(&self.state.pending)
            .await
    }

    async fn hydrate_requested_scopes(&mut self) -> Result<(), LixError> {
        let scope_generation = self.scope_state.scope_generation();
        let requested = self
            .scope_state
            .scopes()
            .into_iter()
            // The branch/commit catalog is reconciled by the runtime's
            // control-plane pass. It is not a semantic row scope and must
            // never force replay of the registered-schema bootstrap stream.
            .filter(|scope| scope != CONTROL_SYNC_SCOPE)
            .collect::<Vec<_>>();
        if requested.is_empty() {
            return Ok(());
        }
        if requested.iter().any(|scope| scope == FULL_SYNC_SCOPE)
            && !self.state.hydrated_scopes.contains(FULL_SYNC_SCOPE)
        {
            self.hydrate_scope(FULL_SYNC_SCOPE).await?;
            self.state
                .hydrated_scopes
                .insert(FULL_SYNC_SCOPE.to_owned());
            self.scope_state
                .mark_scope_hydrated_if_generation(FULL_SYNC_SCOPE, scope_generation);
            self.persist_state().await?;
            self.replay_pending_overlay().await?;
        }
        // A full hydrate covers every future relation demand. Persisting the
        // individual marks keeps readiness branch-local and avoids a second
        // history scan when a later query names a simple relation.
        if self.state.hydrated_scopes.contains(FULL_SYNC_SCOPE) {
            let mut changed = false;
            for schema_key in requested {
                if !self.state.hydrated_scopes.contains(&schema_key) {
                    self.state.hydrated_scopes.insert(schema_key.clone());
                    self.scope_state
                        .mark_scope_hydrated_if_generation(&schema_key, scope_generation);
                    changed = true;
                }
            }
            if changed {
                self.persist_state().await?;
            }
            return Ok(());
        }
        if !self.state.hydrated_scopes.contains("lix_registered_schema") {
            self.hydrate_scope("lix_registered_schema").await?;
            self.state
                .hydrated_scopes
                .insert("lix_registered_schema".to_owned());
            self.scope_state
                .mark_scope_hydrated_if_generation("lix_registered_schema", scope_generation);
            self.persist_state().await?;
            self.replay_pending_overlay().await?;
        }
        for schema_key in requested {
            if schema_key == FULL_SYNC_SCOPE || schema_key == CONTROL_SYNC_SCOPE {
                continue;
            }
            if self.state.hydrated_scopes.contains(&schema_key) {
                continue;
            }
            self.hydrate_scope(&schema_key).await?;
            self.state.hydrated_scopes.insert(schema_key.clone());
            self.scope_state
                .mark_scope_hydrated_if_generation(&schema_key, scope_generation);
            self.persist_state().await?;
            self.replay_pending_overlay().await?;
        }
        Ok(())
    }

    /// Reconstructs one demanded schema from its filtered canonical history.
    /// This cursor is deliberately local to the scope: the replica's global
    /// cursor may already be at the repository head because unrelated history
    /// was skipped during lazy bootstrap.
    async fn hydrate_scope(&mut self, schema_key: &str) -> Result<(), LixError> {
        // An empty schema list is the explicit unscoped pull form. The HTTP
        // transport omits the query parameter in that case, preserving full
        // event payloads instead of turning it into an empty filter.
        let scope = if schema_key == FULL_SYNC_SCOPE {
            Vec::new()
        } else {
            vec![schema_key.to_owned()]
        };
        let marker_scopes = scope.clone();
        let schema_scope = (!scope.is_empty()).then(|| SyncFilterScope::new(&scope));
        let mut cursor = self
            .state
            .scope_cursors
            .get(schema_key)
            .copied()
            .unwrap_or(0);
        loop {
            let response = self
                .transport
                .pull(
                    &self.state.branch_id,
                    cursor,
                    DEFAULT_SYNC_PULL_LIMIT,
                    &scope,
                )
                .await?;
            require_sync_identity(
                "scoped pull branch",
                &self.state.branch_id,
                &response.branch_id,
            )?;
            if response.next_cursor < cursor || response.next_cursor > response.head_cursor {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "scoped sync pull returned invalid cursor bounds",
                ));
            }
            for event in response.events {
                validate_sync_canonical_event_identity(&event)?;
                if !event.pack.rows.is_empty() || !event.pack.files.is_empty() {
                    validate_sync_transaction_pack(&event.pack)?;
                    self.require_matching_pending_event(&event.pack, schema_scope.as_ref())?;
                    self.lix
                        .apply_sync_canonical_event(&event, &self.state.remote_id, &marker_scopes)
                        .await?;
                }
                cursor = event.cursor;
                self.state
                    .scope_cursors
                    .insert(schema_key.to_owned(), cursor);
                self.persist_state_progress().await?;
            }
            if cursor == 0 || cursor >= response.head_cursor {
                return Ok(());
            }
            if cursor >= response.head_cursor {
                return Ok(());
            }
        }
    }

    fn remove_pending_operation(&mut self, operation_id: &str) {
        let before = self.state.pending.len();
        self.state
            .pending
            .retain(|pending| pending.operation_id != operation_id);
        if self.state.pending.len() != before {
            self.pending_storage_dirty = true;
        }
    }

    fn require_matching_pending_event(
        &self,
        event_pack: &SyncTransactionPack,
        schema_scope: Option<&SyncFilterScope<'_>>,
    ) -> Result<(), LixError> {
        if let Some(pending) = self
            .state
            .pending
            .iter()
            .find(|pending| pending.operation_id == event_pack.operation_id)
        {
            let mut expected = pending.clone();
            if let Some(schema_scope) = schema_scope {
                filter_sync_pack(&mut expected, schema_scope);
            }
            if expected != *event_pack {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!(
                        "sync operation '{}' was published with another payload",
                        event_pack.operation_id
                    ),
                ));
            }
        }
        Ok(())
    }

    async fn persist_state(&mut self) -> Result<(), LixError> {
        self.persist_state_with_durability(true).await
    }

    /// Persists cursor progress after a durable applied-marker commit. A
    /// crash may roll this manifest write back, but replaying the canonical
    /// event is safe because the row/plugin transaction and its marker were
    /// already acknowledged durably. The final `flush()` checkpoint remains
    /// durable.
    async fn persist_state_progress(&mut self) -> Result<(), LixError> {
        self.persist_state_with_durability(false).await
    }

    async fn persist_state_with_durability(&mut self, await_durable: bool) -> Result<(), LixError> {
        validate_sync_remote_id(&self.state.remote_id)?;
        validate_sync_branch_id(&self.state.branch_id)?;
        // Normal transactions hold this same gate through their atomic row +
        // outbox commit. Serializing cursor persistence here prevents a
        // background pull from making an otherwise valid application execute
        // fail its sync-state precondition.
        let _write_guard = self.lix.lock_collaboration_writes().await;
        let manifest = SyncClientManifest {
            version: self.state.version,
            remote_id: self.state.remote_id.clone(),
            branch_id: self.state.branch_id.clone(),
            cursor: self.state.cursor,
            server_commit_id: self.state.server_commit_id.clone(),
            pending_operations: self
                .state
                .pending
                .iter()
                .map(|pending| pending.operation_id.clone())
                .collect(),
            hydrated_scopes: self.state.hydrated_scopes.clone(),
            scope_cursors: self.state.scope_cursors.clone(),
        };
        validate_optional_sync_commit_id(manifest.server_commit_id.as_deref())?;
        validate_sync_scope_state(&manifest.hydrated_scopes, &manifest.scope_cursors)?;
        let value = serde_json::to_vec(&manifest).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("encode sync client state: {error}"),
            )
        })?;
        if self.persisted_value.as_deref() == Some(value.as_slice())
            && !self.pending_storage_dirty
            && (!await_durable || self.persisted_value_durable)
        {
            return Ok(());
        }
        let adapter = self.lix.storage_adapter();
        let key = sync_client_state_key(&self.state.remote_id, &self.state.branch_id);
        let mut writes = adapter.new_write_set();
        writes.put(SYNC_CLIENT_STATE_SPACE, key.clone(), value.clone());
        let precondition = self.persisted_value.as_ref().map_or_else(
            || StoragePrecondition::KeyAbsent {
                space: SYNC_CLIENT_STATE_SPACE,
                key: key.clone(),
            },
            |expected| StoragePrecondition::KeyValueEquals {
                space: SYNC_CLIENT_STATE_SPACE,
                key: key.clone(),
                expected: expected.clone().into(),
            },
        );
        let current_pending_operations = manifest
            .pending_operations
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if current_pending_operations.len() > MAX_SYNC_PENDING_OPERATIONS {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("sync pending queue exceeds {MAX_SYNC_PENDING_OPERATIONS} operations"),
            ));
        }
        if current_pending_operations.len() != self.state.pending.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync pending queue contains duplicate operation IDs",
            ));
        }
        // Cursor-only progress writes do not change pending packs. Avoid
        // revalidating and reserializing an offline queue for every canonical
        // event; queue mutations always use a durable checkpoint and take
        // this validation path.
        let pending_changed = current_pending_operations != self.persisted_pending_operations;
        if await_durable || pending_changed {
            let mut pending_bytes = 0usize;
            for pending in &self.state.pending {
                validate_sync_transaction_pack(pending)?;
                let encoded = serde_json::to_vec(pending).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("encode sync pending transaction: {error}"),
                    )
                })?;
                pending_bytes = pending_bytes.checked_add(encoded.len()).ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "sync pending queue byte size overflow",
                    )
                })?;
            }
            if pending_bytes > MAX_SYNC_PENDING_BYTES {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("sync pending queue exceeds {MAX_SYNC_PENDING_BYTES} bytes"),
                ));
            }
        }
        let pending_by_operation = self
            .state
            .pending
            .iter()
            .map(|pending| (pending.operation_id.as_str(), pending))
            .collect::<BTreeMap<_, _>>();
        let mut preconditions = vec![precondition];
        for operation_id in &current_pending_operations {
            if !self.persisted_pending_operations.contains(operation_id) {
                let pending = pending_by_operation
                    .get(operation_id.as_str())
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("sync pending manifest is missing operation '{operation_id}'"),
                        )
                    })?;
                let pending_value = serde_json::to_vec(pending).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("encode sync pending transaction: {error}"),
                    )
                })?;
                let pending_key = sync_client_pending_key(
                    &self.state.remote_id,
                    &self.state.branch_id,
                    operation_id,
                );
                writes.put(
                    SYNC_CLIENT_PENDING_SPACE,
                    pending_key.clone(),
                    pending_value,
                );
                preconditions.push(StoragePrecondition::KeyAbsent {
                    space: SYNC_CLIENT_PENDING_SPACE,
                    key: pending_key,
                });
            }
        }
        for operation_id in self
            .persisted_pending_operations
            .difference(&current_pending_operations)
        {
            let pending_key =
                sync_client_pending_key(&self.state.remote_id, &self.state.branch_id, operation_id);
            writes.delete(SYNC_CLIENT_PENDING_SPACE, pending_key.clone());
            preconditions.push(StoragePrecondition::KeyPresent {
                space: SYNC_CLIENT_PENDING_SPACE,
                key: pending_key,
            });
        }
        let commit = adapter
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    await_durable,
                    preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await;
        if let Err(error) = commit {
            // The lifecycle worker normally discards this client and reloads
            // on its next iteration. Keep manual/internal callers retry-safe
            // too: never leave the in-memory cursor ahead of durable state.
            let read = adapter.begin_read(StorageReadOptions::default()).await?;
            if let Some((persisted_value, state, persisted_pending_operations)) =
                load_sync_client_state(&read, &self.state.remote_id, &self.state.branch_id).await?
            {
                self.persisted_value = Some(persisted_value);
                self.persisted_value_durable = false;
                self.pending_storage_dirty = false;
                self.state = state;
                self.persisted_pending_operations = persisted_pending_operations;
            }
            return Err(error.into());
        }
        self.persisted_value = Some(value);
        self.persisted_value_durable = await_durable;
        self.pending_storage_dirty = false;
        self.persisted_pending_operations = current_pending_operations;
        Ok(())
    }
}

async fn load_sync_client_state<R>(
    read: &R,
    remote_id: &str,
    branch_id: &str,
) -> Result<Option<(Vec<u8>, SyncClientState, BTreeSet<String>)>, LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    validate_sync_remote_id(remote_id)?;
    validate_sync_branch_id(branch_id)?;
    let Some(manifest) = load_sync_client_manifest(read, remote_id, branch_id).await? else {
        return Ok(None);
    };
    let persisted_pending_operations = if manifest.legacy_pending.is_some() {
        BTreeSet::new()
    } else {
        manifest
            .pending_operations
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
    };
    let pending = match manifest.legacy_pending {
        Some(pending) => pending,
        None => {
            load_sync_pending_packs(read, remote_id, branch_id, &manifest.pending_operations)
                .await?
        }
    };
    if pending.len() > MAX_SYNC_PENDING_OPERATIONS {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync client pending queue exceeds the operation limit",
        ));
    }
    let pending_bytes = pending.iter().try_fold(0usize, |total, pack| {
        validate_sync_transaction_pack(pack)?;
        let bytes = serde_json::to_vec(pack)
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("encode loaded sync pending operation: {error}"),
                )
            })?
            .len();
        total.checked_add(bytes).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync pending queue byte size overflow",
            )
        })
    })?;
    if pending_bytes > MAX_SYNC_PENDING_BYTES {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync client pending queue exceeds the byte limit",
        ));
    }
    Ok(Some((
        manifest.value,
        SyncClientState {
            version: SYNC_CLIENT_MANIFEST_VERSION,
            remote_id: manifest.remote_id,
            branch_id: manifest.branch_id,
            cursor: manifest.cursor,
            server_commit_id: manifest.server_commit_id,
            pending,
            hydrated_scopes: manifest.hydrated_scopes,
            scope_cursors: manifest.scope_cursors,
        },
        persisted_pending_operations,
    )))
}

struct LoadedSyncClientManifest {
    value: Vec<u8>,
    remote_id: String,
    branch_id: String,
    cursor: u64,
    server_commit_id: Option<String>,
    pending_operations: Vec<String>,
    hydrated_scopes: BTreeSet<String>,
    scope_cursors: BTreeMap<String, u64>,
    /// Present only for the pre-manifest v1 representation. The next atomic
    /// write promotes these packs into `SYNC_CLIENT_PENDING_SPACE`.
    legacy_pending: Option<Vec<SyncTransactionPack>>,
}

async fn load_sync_client_manifest<R>(
    read: &R,
    remote_id: &str,
    branch_id: &str,
) -> Result<Option<LoadedSyncClientManifest>, LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    validate_sync_remote_id(remote_id)?;
    validate_sync_branch_id(branch_id)?;
    let key = sync_client_state_key(remote_id, branch_id);
    let values = exact_get_many(
        read,
        &[StorageGetManyRequest {
            space: SYNC_CLIENT_STATE_SPACE,
            keys: &[key],
            opts: StorageGetOptions {
                projection: StorageCoreProjection::FullValue,
            },
        }],
    )
    .await?;
    let Some(StorageProjectedValue::FullValue(value)) = values.values.into_iter().next().flatten()
    else {
        return Ok(None);
    };
    if value.len() > MAX_SYNC_CLIENT_MANIFEST_BYTES {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync client manifest exceeds its size limit",
        ));
    }
    if let Ok(manifest) = serde_json::from_slice::<SyncClientManifest>(&value) {
        validate_sync_client_identity(
            manifest.version,
            &manifest.remote_id,
            &manifest.branch_id,
            remote_id,
            branch_id,
            SYNC_CLIENT_MANIFEST_VERSION,
        )?;
        let pending_operations = manifest
            .pending_operations
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if pending_operations.len() != manifest.pending_operations.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync client manifest contains duplicate pending operation IDs",
            ));
        }
        for operation_id in &manifest.pending_operations {
            validate_sync_operation_id(operation_id).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "sync client manifest contains an invalid pending operation ID",
                )
            })?;
        }
        if pending_operations.len() > MAX_SYNC_PENDING_OPERATIONS {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync client manifest exceeds the pending operation limit",
            ));
        }
        validate_sync_scope_state(&manifest.hydrated_scopes, &manifest.scope_cursors)?;
        validate_optional_sync_commit_id(manifest.server_commit_id.as_deref())?;
        return Ok(Some(LoadedSyncClientManifest {
            value: value.to_vec(),
            remote_id: manifest.remote_id,
            branch_id: manifest.branch_id,
            cursor: manifest.cursor,
            server_commit_id: manifest.server_commit_id,
            pending_operations: manifest.pending_operations,
            hydrated_scopes: manifest.hydrated_scopes,
            scope_cursors: manifest.scope_cursors,
            legacy_pending: None,
        }));
    }

    // Version-one sync clients wrote the complete queue into this value. Read
    // that shape so an interrupted upgrade can migrate it atomically on the
    // next cursor or acknowledgement write.
    let state = serde_json::from_slice::<SyncClientState>(&value).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("decode sync client state: {error}"),
        )
    })?;
    validate_sync_client_identity(
        state.version,
        &state.remote_id,
        &state.branch_id,
        remote_id,
        branch_id,
        SYNC_CLIENT_STATE_VERSION,
    )?;
    validate_optional_sync_commit_id(state.server_commit_id.as_deref())?;
    let pending_operations = state
        .pending
        .iter()
        .map(|pending| pending.operation_id.clone())
        .collect::<Vec<_>>();
    for operation_id in &pending_operations {
        validate_sync_operation_id(operation_id).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync client state contains an invalid pending operation ID",
            )
        })?;
    }
    if pending_operations.iter().collect::<BTreeSet<_>>().len() != pending_operations.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync client state contains duplicate pending operation IDs",
        ));
    }
    if pending_operations.len() > MAX_SYNC_PENDING_OPERATIONS {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync client state exceeds the pending operation limit",
        ));
    }
    validate_sync_scope_state(&state.hydrated_scopes, &state.scope_cursors)?;
    Ok(Some(LoadedSyncClientManifest {
        value: value.to_vec(),
        remote_id: state.remote_id,
        branch_id: state.branch_id,
        cursor: state.cursor,
        server_commit_id: state.server_commit_id,
        pending_operations,
        hydrated_scopes: BTreeSet::new(),
        scope_cursors: BTreeMap::new(),
        legacy_pending: Some(state.pending),
    }))
}

fn validate_sync_client_identity(
    version: u8,
    stored_remote_id: &str,
    stored_branch_id: &str,
    remote_id: &str,
    branch_id: &str,
    expected_version: u8,
) -> Result<(), LixError> {
    validate_sync_remote_id(stored_remote_id)?;
    validate_sync_branch_id(stored_branch_id)?;
    if version != expected_version || stored_remote_id != remote_id || stored_branch_id != branch_id
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync client state has an unsupported version or branch identity",
        ));
    }
    Ok(())
}

fn validate_sync_scope_state(
    hydrated_scopes: &BTreeSet<String>,
    scope_cursors: &BTreeMap<String, u64>,
) -> Result<(), LixError> {
    let all_scopes = hydrated_scopes
        .iter()
        .chain(scope_cursors.keys())
        .collect::<BTreeSet<_>>();
    if all_scopes.len() > MAX_SYNC_SCOPE_KEYS {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync client state contains too many hydrated scopes",
        ));
    }
    if all_scopes
        .iter()
        .any(|scope| scope.is_empty() || scope.len() > MAX_SYNC_SCOPE_KEY_BYTES)
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync client state contains an invalid scope key",
        ));
    }
    Ok(())
}

async fn load_sync_pending_packs<R>(
    read: &R,
    remote_id: &str,
    branch_id: &str,
    operation_ids: &[String],
) -> Result<Vec<SyncTransactionPack>, LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    validate_sync_remote_id(remote_id)?;
    validate_sync_branch_id(branch_id)?;
    if operation_ids.is_empty() {
        return Ok(Vec::new());
    }
    for operation_id in operation_ids {
        validate_sync_operation_id(operation_id)?;
    }
    let keys = operation_ids
        .iter()
        .map(|operation_id| sync_client_pending_key(remote_id, branch_id, operation_id))
        .collect::<Vec<_>>();
    let values = exact_get_many(
        read,
        &[StorageGetManyRequest {
            space: SYNC_CLIENT_PENDING_SPACE,
            keys: &keys,
            opts: StorageGetOptions {
                projection: StorageCoreProjection::FullValue,
            },
        }],
    )
    .await?;
    let packs = values
        .values
        .into_iter()
        .zip(operation_ids)
        .map(|(value, operation_id)| {
            let Some(StorageProjectedValue::FullValue(value)) = value else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("sync pending operation '{operation_id}' is missing"),
                ));
            };
            let pack = serde_json::from_slice::<SyncTransactionPack>(&value).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("decode sync pending operation '{operation_id}': {error}"),
                )
            })?;
            if pack.operation_id != *operation_id || pack.branch_id != branch_id {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("sync pending operation '{operation_id}' has an invalid identity"),
                ));
            }
            validate_sync_transaction_pack(&pack)?;
            Ok(pack)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let total_bytes = packs.iter().try_fold(0usize, |total, pack| {
        let bytes = serde_json::to_vec(pack)
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("encode loaded sync pending operation: {error}"),
                )
            })?
            .len();
        total.checked_add(bytes).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync pending queue byte size overflow",
            )
        })
    })?;
    if total_bytes > MAX_SYNC_PENDING_BYTES {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync client pending queue exceeds the byte limit",
        ));
    }
    Ok(packs)
}

fn sync_client_state_key(remote_id: &str, branch_id: &str) -> StorageKey {
    let remote = remote_id.as_bytes();
    let branch = branch_id.as_bytes();
    let mut key = Vec::with_capacity(8 + remote.len() + branch.len());
    key.extend_from_slice(
        &u32::try_from(remote.len())
            .expect("sync remote identity length fits u32")
            .to_be_bytes(),
    );
    key.extend_from_slice(remote);
    key.extend_from_slice(
        &u32::try_from(branch.len())
            .expect("sync branch identity length fits u32")
            .to_be_bytes(),
    );
    key.extend_from_slice(branch);
    StorageKey(bytes::Bytes::from(key))
}

fn sync_client_pending_key(remote_id: &str, branch_id: &str, operation_id: &str) -> StorageKey {
    let remote = remote_id.as_bytes();
    let branch = branch_id.as_bytes();
    let operation = operation_id.as_bytes();
    let mut key = Vec::with_capacity(12 + remote.len() + branch.len() + operation.len());
    key.extend_from_slice(
        &u32::try_from(remote.len())
            .expect("sync remote identity length fits u32")
            .to_be_bytes(),
    );
    key.extend_from_slice(remote);
    key.extend_from_slice(
        &u32::try_from(branch.len())
            .expect("sync branch identity length fits u32")
            .to_be_bytes(),
    );
    key.extend_from_slice(branch);
    key.extend_from_slice(
        &u32::try_from(operation.len())
            .expect("sync operation identity length fits u32")
            .to_be_bytes(),
    );
    key.extend_from_slice(operation);
    StorageKey(bytes::Bytes::from(key))
}

fn sync_client_applied_key(remote_id: &str, branch_id: &str, scope: &str) -> StorageKey {
    let remote = remote_id.as_bytes();
    let branch = branch_id.as_bytes();
    let scope = scope.as_bytes();
    let mut key = Vec::with_capacity(12 + remote.len() + branch.len() + scope.len());
    for component in [remote, branch, scope] {
        key.extend_from_slice(
            &u32::try_from(component.len())
                .expect("sync applied marker component length fits u32")
                .to_be_bytes(),
        );
        key.extend_from_slice(component);
    }
    StorageKey(bytes::Bytes::from(key))
}

/// Loads the receipt that protects a canonical event from being applied twice
/// after a cursor-manifest crash. The returned bytes are retained for the CAS
/// precondition used by the following atomic row+marker commit.
pub(crate) async fn load_sync_applied_marker<R>(
    read: &R,
    remote_id: &str,
    branch_id: &str,
    scope: &str,
) -> Result<Option<(Vec<u8>, SyncAppliedEventMarker)>, LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    validate_sync_remote_id(remote_id)?;
    validate_sync_branch_id(branch_id)?;
    validate_sync_identity_component("scope", scope, MAX_SYNC_SCOPE_KEY_BYTES)?;
    let key = sync_client_applied_key(remote_id, branch_id, scope);
    let values = exact_get_many(
        read,
        &[StorageGetManyRequest {
            space: SYNC_CLIENT_APPLIED_SPACE,
            keys: &[key],
            opts: StorageGetOptions {
                projection: StorageCoreProjection::FullValue,
            },
        }],
    )
    .await?;
    let Some(StorageProjectedValue::FullValue(value)) = values.values.into_iter().next().flatten()
    else {
        return Ok(None);
    };
    if value.len() > MAX_SYNC_MARKER_BYTES {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync applied event marker exceeds its size limit",
        ));
    }
    let marker = serde_json::from_slice::<SyncAppliedEventMarker>(&value).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("decode sync applied event marker: {error}"),
        )
    })?;
    if validate_sync_applied_event_marker(&marker).is_err()
        || marker.remote_id != remote_id
        || marker.branch_id != branch_id
        || marker.scope != scope
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync applied event marker has an invalid identity",
        ));
    }
    Ok(Some((value.to_vec(), marker)))
}

/// Stages applied-event receipts into the transaction's metadata lane. Each
/// receipt is guarded by the value observed immediately before staging so two
/// independent workers cannot silently move a scope marker backwards.
pub(crate) fn stage_sync_applied_event_markers(
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
    markers: &[(SyncAppliedEventMarker, Option<Vec<u8>>)],
) -> Result<(), LixError> {
    for (marker, previous_value) in markers {
        validate_sync_applied_event_marker(marker)?;
        let key = sync_client_applied_key(&marker.remote_id, &marker.branch_id, &marker.scope);
        let value = serde_json::to_vec(marker).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("encode sync applied event marker: {error}"),
            )
        })?;
        writes.put(SYNC_CLIENT_APPLIED_SPACE, key.clone(), value);
        preconditions.push(previous_value.as_ref().map_or_else(
            || StoragePrecondition::KeyAbsent {
                space: SYNC_CLIENT_APPLIED_SPACE,
                key: key.clone(),
            },
            |expected| StoragePrecondition::KeyValueEquals {
                space: SYNC_CLIENT_APPLIED_SPACE,
                key: key.clone(),
                expected: expected.clone().into(),
            },
        ));
    }
    Ok(())
}

fn validate_sync_applied_event_marker(marker: &SyncAppliedEventMarker) -> Result<(), LixError> {
    if !matches!(
        marker.version,
        SYNC_APPLIED_EVENT_LEGACY_VERSION | SYNC_APPLIED_EVENT_VERSION
    ) || marker.remote_id.is_empty()
        || marker.remote_id.len() > MAX_SYNC_REMOTE_ID_BYTES
        || marker.branch_id.is_empty()
        || marker.branch_id.len() > MAX_SYNC_SCOPE_KEY_BYTES
        || marker.scope.is_empty()
        || marker.scope.len() > MAX_SYNC_SCOPE_KEY_BYTES
        || marker.canonical_commit_id.is_empty()
        || marker.canonical_commit_id.len() > MAX_SYNC_SCOPE_KEY_BYTES
        || marker.pack_fingerprint.len() != 64
        || marker.cursor == 0
    {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync applied event marker has an invalid identity",
        ));
    }
    let encoded = serde_json::to_vec(marker).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("encode sync applied event marker: {error}"),
        )
    })?;
    if encoded.len() > MAX_SYNC_MARKER_BYTES {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync applied event marker exceeds its size limit",
        ));
    }
    Ok(())
}

fn sync_pack_fingerprint(pack: &SyncTransactionPack) -> Result<String, LixError> {
    let encoded = serde_json::to_vec(pack).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("encode sync pack fingerprint: {error}"),
        )
    })?;
    Ok(blake3::hash(&encoded).to_hex().to_string())
}

fn sync_event_identity_fingerprint(event: &SyncCanonicalEvent) -> String {
    let mut identity = Vec::with_capacity(8 + event.canonical_commit_id.len());
    identity.extend_from_slice(&event.cursor.to_be_bytes());
    identity.extend_from_slice(event.canonical_commit_id.as_bytes());
    blake3::hash(&identity).to_hex().to_string()
}

fn marker_covers_event(
    marker: &SyncAppliedEventMarker,
    event: &SyncCanonicalEvent,
    pack_fingerprint: &str,
) -> bool {
    marker.cursor > event.cursor
        || (marker.cursor == event.cursor
            && marker.canonical_commit_id == event.canonical_commit_id
            && (marker.version == SYNC_APPLIED_EVENT_LEGACY_VERSION
                || marker.pack_fingerprint == pack_fingerprint))
}

fn sync_replica_config_key(remote_id: &str) -> StorageKey {
    StorageKey(bytes::Bytes::copy_from_slice(remote_id.as_bytes()))
}

pub(crate) fn stage_sync_event_publication(
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
    pack: &SyncTransactionPack,
    canonical_commit_id: &str,
    plan: &SyncAdmissionPlan,
) -> Result<SyncCanonicalEvent, LixError> {
    validate_sync_transaction_pack(pack)?;
    validate_sync_branch_id(&pack.branch_id)?;
    validate_sync_identity_component(
        "canonicalCommitId",
        canonical_commit_id,
        MAX_SYNC_SCOPE_KEY_BYTES,
    )?;
    if plan.cursor == 0 {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync canonical event cursor must be non-zero",
        ));
    }
    let pack_fingerprint = sync_pack_fingerprint(pack)?;
    let event = SyncCanonicalEvent {
        cursor: plan.cursor,
        canonical_commit_id: canonical_commit_id.to_owned(),
        pack_fingerprint,
        pack: pack.clone(),
    };
    let event_key = sync_event_key(&pack.branch_id, plan.cursor);
    let event_value = serde_json::to_vec(&event).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("encode canonical sync event: {error}"),
        )
    })?;
    let head_key = sync_head_key(&pack.branch_id);
    let head_value = serde_json::to_vec(&SyncHead {
        cursor: plan.cursor,
        commit_id: canonical_commit_id.to_owned(),
    })
    .map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("encode sync branch head: {error}"),
        )
    })?;
    writes.put(SYNC_EVENT_SPACE, event_key.clone(), event_value);
    writes.put(SYNC_HEAD_SPACE, head_key.clone(), head_value);
    preconditions.push(StoragePrecondition::KeyAbsent {
        space: SYNC_EVENT_SPACE,
        key: event_key,
    });
    preconditions.push(plan.previous_head_value.as_ref().map_or_else(
        || StoragePrecondition::KeyAbsent {
            space: SYNC_HEAD_SPACE,
            key: head_key.clone(),
        },
        |expected| StoragePrecondition::KeyValueEquals {
            space: SYNC_HEAD_SPACE,
            key: head_key.clone(),
            expected: expected.clone().into(),
        },
    ));
    Ok(event)
}

/// Builds and stages the canonical row event for an ordinary transaction.
///
/// This hook runs after commit-time reconciliation and validation, so the
/// event describes the rows the server is actually about to publish. Sync
/// admission supplies its own payload-bound event and is skipped by callers
/// when explicit atomic metadata is already present.
pub(crate) async fn stage_ordinary_transaction_event<R>(
    read: &R,
    blob_reader: &dyn BlobDataReader,
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
    prepared: &PreparedWriteSet,
    parent_heads: &BTreeMap<String, Option<CommitId>>,
    branch_id: &str,
) -> Result<bool, LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let Some(change_refs) = prepared.commit_change_refs_by_branch.get(branch_id) else {
        return Ok(false);
    };
    let commit_id = change_refs.commit_id.to_string();
    let base_server_commit_id = parent_heads
        .get(branch_id)
        .copied()
        .flatten()
        .map_or_else(|| commit_id.clone(), |parent| parent.to_string());
    let rows = prepared_sync_rows(prepared, branch_id)?;
    let files = prepared_sync_files(blob_reader, prepared, branch_id).await?;
    if rows.is_empty() && files.is_empty() {
        return Ok(false);
    }
    let pack = SyncTransactionPack {
        operation_id: format!("server:{commit_id}"),
        branch_id: branch_id.to_owned(),
        base_server_commit_id,
        local_commit_id: commit_id.clone(),
        rows,
        files,
    };
    let stored = load_sync_head(read, branch_id).await?;
    let cursor = stored
        .as_ref()
        .map_or(0, |(_, head)| head.cursor)
        .checked_add(1)
        .ok_or_else(|| LixError::new(LixError::CODE_INTERNAL_ERROR, "sync cursor overflow"))?;
    let plan = SyncAdmissionPlan {
        cursor,
        previous_head_value: stored.map(|(value, _)| value),
    };
    stage_sync_event_publication(writes, preconditions, &pack, &commit_id, &plan)?;
    Ok(true)
}

/// Appends a completed local semantic transaction to the replica outbox in
/// the same storage commit as its rows and history.
pub(crate) async fn stage_local_transaction_outbox<R>(
    read: &R,
    blob_reader: &dyn BlobDataReader,
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
    prepared: &PreparedWriteSet,
    branch_id: &str,
    remote_id: &str,
) -> Result<bool, LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let Some(change_refs) = prepared.commit_change_refs_by_branch.get(branch_id) else {
        return Ok(false);
    };
    let rows = prepared_sync_rows(prepared, branch_id)?;
    let files = prepared_sync_files(blob_reader, prepared, branch_id).await?;
    if rows.is_empty() && files.is_empty() {
        return Ok(false);
    }
    let commit_id = change_refs.commit_id.to_string();
    let Some(manifest) = load_sync_client_manifest(read, remote_id, branch_id).await? else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync replica has no initialized client state",
        ));
    };
    let base_server_commit_id = manifest.server_commit_id.clone().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync replica has not observed a server head",
        )
    })?;
    let operation_id = format!("client:{commit_id}");
    if manifest
        .pending_operations
        .iter()
        .any(|pending| pending == &operation_id)
    {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync operation '{operation_id}' is already pending"),
        ));
    }
    let pack = SyncTransactionPack {
        operation_id,
        branch_id: branch_id.to_owned(),
        base_server_commit_id,
        local_commit_id: commit_id,
        rows,
        files,
    };
    validate_sync_transaction_pack(&pack)?;
    if manifest.pending_operations.len() >= MAX_SYNC_PENDING_OPERATIONS {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync pending queue exceeds {MAX_SYNC_PENDING_OPERATIONS} operations"),
        ));
    }
    let mut pending_operations = manifest.pending_operations.clone();
    pending_operations.push(pack.operation_id.clone());
    let next_manifest = SyncClientManifest {
        version: SYNC_CLIENT_MANIFEST_VERSION,
        remote_id: manifest.remote_id.clone(),
        branch_id: manifest.branch_id.clone(),
        cursor: manifest.cursor,
        server_commit_id: manifest.server_commit_id.clone(),
        pending_operations,
        hydrated_scopes: manifest.hydrated_scopes,
        scope_cursors: manifest.scope_cursors,
    };
    let next_value = serde_json::to_vec(&next_manifest).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("encode sync client manifest: {error}"),
        )
    })?;
    let key = sync_client_state_key(remote_id, branch_id);
    writes.put(SYNC_CLIENT_STATE_SPACE, key.clone(), next_value);
    preconditions.push(StoragePrecondition::KeyValueEquals {
        space: SYNC_CLIENT_STATE_SPACE,
        key,
        expected: manifest.value.into(),
    });
    // Legacy state carries its payloads in the manifest; promote those packs
    // in the same atomic write as the new pack and manifest. New-format state
    // only needs one independent pending put.
    if let Some(legacy_pending) = manifest.legacy_pending {
        for pending in legacy_pending {
            let pending_key = sync_client_pending_key(remote_id, branch_id, &pending.operation_id);
            let pending_value = serde_json::to_vec(&pending).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("encode sync pending transaction: {error}"),
                )
            })?;
            writes.put(
                SYNC_CLIENT_PENDING_SPACE,
                pending_key.clone(),
                pending_value,
            );
            preconditions.push(StoragePrecondition::KeyAbsent {
                space: SYNC_CLIENT_PENDING_SPACE,
                key: pending_key,
            });
        }
    }
    let pending_key = sync_client_pending_key(remote_id, branch_id, &pack.operation_id);
    let pending_value = serde_json::to_vec(&pack).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("encode sync pending transaction: {error}"),
        )
    })?;
    writes.put(
        SYNC_CLIENT_PENDING_SPACE,
        pending_key.clone(),
        pending_value,
    );
    preconditions.push(StoragePrecondition::KeyAbsent {
        space: SYNC_CLIENT_PENDING_SPACE,
        key: pending_key,
    });
    Ok(true)
}

fn prepared_sync_rows(
    prepared: &PreparedWriteSet,
    branch_id: &str,
) -> Result<Vec<SyncRowMutation>, LixError> {
    prepared
        .state_rows
        .iter()
        .filter(|row| {
            !row.untracked
                && !row.global
                && row.branch_id.as_str() == branch_id
                && ordinary_sync_schema(row.schema_key.as_str())
        })
        .map(|row| {
            Ok(SyncRowMutation {
                schema_key: row.schema_key.to_string(),
                file_id: row.file_id.map(ToString::to_string),
                row_pk: row.row_pk.as_json_array_value()?,
                snapshot: row.snapshot.map(|value| value.value().clone()),
                metadata: row.metadata.map(|value| value.value().clone()),
            })
        })
        .collect()
}

async fn prepared_sync_files(
    blob_reader: &dyn BlobDataReader,
    prepared: &PreparedWriteSet,
    branch_id: &str,
) -> Result<Vec<SyncFileMutation>, LixError> {
    // A plugin transaction's semantic rows are the canonical representation;
    // its rendered file bytes are a derived view and may be based on a
    // plugin-local reconciliation context that cannot be replayed as a raw
    // filesystem write. Native binary writes have no semantic rows and take
    // the payload lane below.
    if prepared.state_rows.iter().any(|row| {
        !row.untracked
            && !row.global
            && row.branch_id.as_str() == branch_id
            && !row.schema_key.starts_with("lix_")
    }) {
        return Ok(Vec::new());
    }
    let mut hashes = Vec::new();
    for file in &prepared.file_content_writes {
        if !file.global
            && !file.untracked
            && file.branch_id == branch_id
            && file.inline_data().is_none()
            && let Some(hash) = file.blob_hash()
        {
            hashes.push(hash);
        }
    }
    let blob_bytes = if hashes.is_empty() {
        Vec::new()
    } else {
        blob_reader.load_bytes_many(&hashes).await?.into_vec()
    };
    let mut hash_index = 0;
    let mut files = Vec::new();
    for file in &prepared.file_content_writes {
        if file.global || file.untracked || file.branch_id != branch_id {
            continue;
        }
        let content = if let Some(inline) = file.inline_data() {
            inline.to_vec()
        } else if file.blob_hash().is_some() {
            let Some(Some(content)) = blob_bytes.get(hash_index) else {
                hash_index += 1;
                continue;
            };
            hash_index += 1;
            content.clone()
        } else {
            continue;
        };
        files.push(SyncFileMutation {
            file_id: file.file_id.clone(),
            path: file.path.clone(),
            filename: file.filename.clone(),
            global: file.global,
            untracked: file.untracked,
            content,
        });
    }
    Ok(files)
}

fn ordinary_sync_schema(schema_key: &str) -> bool {
    !schema_key.starts_with("lix_")
        || matches!(schema_key, "lix_registered_schema" | "lix_key_value")
}

async fn load_sync_head<R>(
    read: &R,
    branch_id: &str,
) -> Result<Option<(Vec<u8>, SyncHead)>, LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    validate_sync_branch_id(branch_id)?;
    let key = sync_head_key(branch_id);
    let values = exact_get_many(
        read,
        &[StorageGetManyRequest {
            space: SYNC_HEAD_SPACE,
            keys: &[key],
            opts: StorageGetOptions {
                projection: StorageCoreProjection::FullValue,
            },
        }],
    )
    .await?;
    let Some(StorageProjectedValue::FullValue(value)) = values.values.into_iter().next().flatten()
    else {
        return Ok(None);
    };
    let head = serde_json::from_slice::<SyncHead>(&value).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("decode sync branch head: {error}"),
        )
    })?;
    if head.cursor == 0
        || head.commit_id.is_empty()
        || head.commit_id.len() > MAX_SYNC_SCOPE_KEY_BYTES
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync branch head has an invalid cursor or commit identity",
        ));
    }
    Ok(Some((value.to_vec(), head)))
}

fn sync_head_key(branch_id: &str) -> StorageKey {
    StorageKey(bytes::Bytes::copy_from_slice(branch_id.as_bytes()))
}

fn sync_event_key(branch_id: &str, cursor: u64) -> StorageKey {
    let branch_bytes = branch_id.as_bytes();
    let mut key = Vec::with_capacity(4 + branch_bytes.len() + 8);
    key.extend_from_slice(
        &u32::try_from(branch_bytes.len())
            .expect("branch ID length fits u32")
            .to_be_bytes(),
    );
    key.extend_from_slice(branch_bytes);
    key.extend_from_slice(&cursor.to_be_bytes());
    StorageKey(bytes::Bytes::from(key))
}

fn sync_write_batch(
    pack: &SyncTransactionPack,
    branch_id: &str,
) -> Result<RawWriteBatch, LixError> {
    let mut rows = RawWriteBatch::with_capacity(pack.rows.len());
    for mutation in &pack.rows {
        let row_pk =
            crate::row_pk::RowPk::from_json_array_value(&mutation.row_pk).map_err(|error| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!("invalid sync row primary key: {error}"),
                )
            })?;
        rows.push(TransactionWriteRow {
            row_pk: Some(row_pk),
            schema_key: mutation.schema_key.clone().into(),
            file_id: mutation.file_id.clone().map(Into::into),
            snapshot: mutation
                .snapshot
                .clone()
                .map(|snapshot| TransactionJson::from_value(snapshot, "sync row snapshot"))
                .transpose()?,
            metadata: mutation
                .metadata
                .clone()
                .map(|metadata| TransactionJson::from_value(metadata, "sync row metadata"))
                .transpose()?,
            origin: None,
            created_at: None,
            updated_at: None,
            global: false,
            change_id: None,
            commit_id: None,
            untracked: false,
            branch_id: branch_id.to_owned().into(),
        });
    }
    Ok(rows)
}

async fn materialize_diff_payload<S>(
    store: &S,
    diff: &crate::tracked_state::TrackedStateDiff,
    row: Option<&crate::tracked_state::TrackedStateDiffRow>,
) -> Result<(Option<serde_json::Value>, Option<serde_json::Value>), LixError>
where
    S: StorageAdapterRead,
{
    let Some(row) = row else {
        return Ok((None, None));
    };
    let payload = diff.payloads().get(row.change_id).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("sync diff omitted payload for change '{}'", row.change_id),
        )
    })?;
    let snapshot = if row.deleted {
        None
    } else {
        Some(
            materialize_json_slot(store, payload.snapshot, row.change_id, "snapshot")
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("live sync row '{}' has no snapshot", row.change_id),
                    )
                })?,
        )
    };
    let metadata =
        materialize_json_slot(store, payload.metadata, row.change_id, "metadata").await?;
    Ok((snapshot, metadata))
}

async fn materialize_json_slot<S>(
    store: &S,
    slot: &JsonSlot,
    change_id: crate::changelog::ChangeId,
    field: &str,
) -> Result<Option<serde_json::Value>, LixError>
where
    S: StorageAdapterRead,
{
    let json = match slot {
        JsonSlot::None => return Ok(None),
        JsonSlot::Inline(json) => json.as_bytes().to_vec(),
        JsonSlot::Ref(json_ref) => JsonStoreContext::new()
            .load_bytes_many(
                store,
                JsonLoadRequestRef {
                    refs: std::slice::from_ref(json_ref),
                    scope: JsonReadScopeRef::OutOfBand,
                },
            )
            .await?
            .into_values()
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("sync diff references missing {field} JSON payload for '{change_id}'"),
                )
            })?
            .to_vec(),
    };
    serde_json::from_slice(&json).map(Some).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("sync diff contains invalid {field} JSON for '{change_id}': {error}"),
        )
    })
}

fn require_branch(expected: &str, actual: &str) -> Result<(), LixError> {
    if expected == actual {
        Ok(())
    } else {
        Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync branch mismatch: expected '{expected}', received '{actual}'"),
        ))
    }
}

fn require_sync_identity(label: &str, expected: &str, actual: &str) -> Result<(), LixError> {
    if expected == actual {
        Ok(())
    } else {
        Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync {label} mismatch: expected '{expected}', received '{actual}'"),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CONTROL_SYNC_SCOPE, FULL_SYNC_SCOPE, MAX_SYNC_PACK_ROWS, SyncCanonicalEvent,
        SyncFileMutation, SyncFilterScope, SyncRowMutation, SyncTransactionPack,
        extract_sql_scope_schema_keys, filter_sync_pack, sync_pack_fingerprint,
        validate_sync_transaction_pack,
    };

    #[tokio::test]
    async fn canonical_event_marker_makes_replay_after_manifest_loss_a_noop() {
        let lix = crate::open_lix().await.expect("open replica");
        let branch_id = lix.active_branch_id().await.expect("active branch");
        let event = SyncCanonicalEvent {
            cursor: 1,
            canonical_commit_id: "canonical-1".to_owned(),
            pack_fingerprint: String::new(),
            pack: SyncTransactionPack {
                operation_id: "operation-1".to_owned(),
                branch_id,
                base_server_commit_id: "base".to_owned(),
                local_commit_id: "local".to_owned(),
                rows: vec![SyncRowMutation {
                    schema_key: "lix_key_value".to_owned(),
                    file_id: None,
                    row_pk: serde_json::json!(["marker-replay"]),
                    snapshot: Some(serde_json::json!({
                        "key": "marker-replay",
                        "value": "once"
                    })),
                    metadata: None,
                }],
                files: Vec::new(),
            },
        };
        lix.apply_sync_canonical_event(&event, "remote", &[])
            .await
            .expect("apply canonical event");
        let first_commit = lix
            .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
            .await
            .expect("read first commit")
            .rows()[0]
            .get::<String>("commit_id")
            .expect("first commit id");
        assert_eq!(
            first_commit,
            crate::changelog::CommitId::for_test_label("canonical-1").to_string(),
            "canonical event replay must retain the server commit identity"
        );

        // Model a crash after the row/plugin commit but before the cursor
        // manifest commit: the canonical event is delivered again while the
        // marker remains durable.
        lix.apply_sync_canonical_event(&event, "remote", &[])
            .await
            .expect("replay canonical event");
        let second_commit = lix
            .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
            .await
            .expect("read second commit")
            .rows()[0]
            .get::<String>("commit_id")
            .expect("second commit id");
        assert_eq!(first_commit, second_commit);
        let rows = lix
            .execute(
                "SELECT COUNT(*) AS count FROM lix_key_value WHERE key = 'marker-replay'",
                &[],
            )
            .await
            .expect("read replayed row");
        assert_eq!(rows.rows()[0].get::<i64>("count").unwrap(), 1);
        lix.close().await.expect("close replica");
    }

    #[tokio::test]
    async fn file_and_semantic_scope_markers_deduplicate_in_either_order() {
        let lix = crate::open_lix().await.expect("open replica");
        lix.execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[crate::Value::Text(
                serde_json::json!({
                    "$schema": "https://lix.dev/schema-v1.json",
                    "key": "sync_marker_row",
                    "columns": [
                        {"name": "id", "type": "text", "nullable": false},
                        {"name": "value", "type": "text", "nullable": false}
                    ],
                    "primary_key": ["id"]
                })
                .to_string(),
            )],
        )
        .await
        .expect("register marker test schema");
        let file_id = "01920000-0000-7000-8000-0000000000a2";
        lix.execute(
            "INSERT INTO lix_file (id, path, content) VALUES ($1, $2, $3)",
            &[
                crate::Value::Text(file_id.to_owned()),
                crate::Value::Text("/marker-order.txt".to_owned()),
                crate::Value::Blob(b"before".to_vec().into()),
            ],
        )
        .await
        .expect("seed marker test file descriptor");
        let branch_id = lix.active_branch_id().await.expect("active branch");
        let mut event = SyncCanonicalEvent {
            cursor: 1,
            canonical_commit_id: "canonical-marker-order".to_owned(),
            pack_fingerprint: String::new(),
            pack: SyncTransactionPack {
                operation_id: "operation-marker-order".to_owned(),
                branch_id,
                base_server_commit_id: "base".to_owned(),
                local_commit_id: "local".to_owned(),
                rows: vec![SyncRowMutation {
                    schema_key: "sync_marker_row".to_owned(),
                    file_id: Some(file_id.to_owned()),
                    row_pk: serde_json::json!(["row-1"]),
                    snapshot: Some(serde_json::json!({"id": "row-1", "value": "one"})),
                    metadata: None,
                }],
                files: vec![SyncFileMutation {
                    file_id: file_id.to_owned(),
                    path: Some("/marker-order.txt".to_owned()),
                    filename: Some("marker-order.txt".to_owned()),
                    global: false,
                    untracked: false,
                    content: b"rendered".to_vec(),
                }],
            },
        };
        event.pack_fingerprint = sync_pack_fingerprint(&event.pack).expect("event fingerprint");

        // Semantic hydration owns the row and renders its local view, but it
        // deliberately leaves the raw file payload for a later file demand.
        lix.apply_sync_canonical_event(&event, "remote-order", &["sync_marker_row".to_owned()])
            .await
            .expect("apply semantic scope");
        lix.apply_sync_canonical_event(&event, "remote-order", &["lix_file".to_owned()])
            .await
            .expect("apply file scope");
        let file = lix
            .execute(
                "SELECT content FROM lix_file WHERE path = '/marker-order.txt'",
                &[],
            )
            .await
            .expect("read file payload");
        assert_eq!(
            file.rows()[0].get::<Vec<u8>>("content").unwrap(),
            b"rendered"
        );

        let before = lix
            .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
            .await
            .expect("read marker-order head")
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        lix.apply_sync_canonical_event(&event, "remote-order", &["sync_marker_row".to_owned()])
            .await
            .expect("replay semantic scope");
        let after = lix
            .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
            .await
            .expect("read marker-order replay head")
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        assert_eq!(
            before, after,
            "file-first marker must cover semantic replay"
        );
        lix.close().await.expect("close replica");
    }

    #[test]
    fn sql_scope_extraction_registers_the_control_plane_without_hydrating_rows() {
        assert_eq!(
            extract_sql_scope_schema_keys(
                "SELECT value FROM needed_rows n JOIN other_rows o ON n.id = o.id"
            ),
            vec![FULL_SYNC_SCOPE.to_owned()]
        );
        assert_eq!(
            extract_sql_scope_schema_keys(
                "INSERT INTO lix_registered_schema (value) VALUES ($1); SELECT content FROM lix_file"
            ),
            vec!["lix_file".to_owned()]
        );
        assert_eq!(
            extract_sql_scope_schema_keys("SELECT id, commit_id FROM lix_branch"),
            vec![CONTROL_SYNC_SCOPE.to_owned()]
        );
        assert_eq!(
            extract_sql_scope_schema_keys("SELECT * FROM lix_diff"),
            vec![CONTROL_SYNC_SCOPE.to_owned()]
        );
        assert_eq!(
            extract_sql_scope_schema_keys("SELECT content FROM lix_file WHERE path = '/shared.md'"),
            vec!["lix_file".to_owned()]
        );
        assert_eq!(
            extract_sql_scope_schema_keys(
                "WITH selected AS (SELECT id FROM actual_rows) SELECT id FROM selected"
            ),
            vec![FULL_SYNC_SCOPE.to_owned()]
        );
        assert_eq!(
            extract_sql_scope_schema_keys("SELECT id FROM (SELECT id FROM nested_rows) AS nested"),
            vec![FULL_SYNC_SCOPE.to_owned()]
        );
        assert_eq!(
            extract_sql_scope_schema_keys("SELECT id FROM \"CaseSensitiveRows\""),
            vec![FULL_SYNC_SCOPE.to_owned()]
        );
        assert_eq!(
            extract_sql_scope_schema_keys("SELECT id FROM tenant.rows"),
            vec![FULL_SYNC_SCOPE.to_owned()]
        );
    }

    #[test]
    fn sync_pack_limits_reject_unbounded_row_fanout() {
        let row = SyncRowMutation {
            schema_key: "rows".to_owned(),
            file_id: None,
            row_pk: serde_json::json!(["id"]),
            snapshot: Some(serde_json::json!({"value": "x"})),
            metadata: None,
        };
        let pack = SyncTransactionPack {
            operation_id: "op".to_owned(),
            branch_id: "branch".to_owned(),
            base_server_commit_id: "base".to_owned(),
            local_commit_id: "local".to_owned(),
            rows: vec![row; MAX_SYNC_PACK_ROWS + 1],
            files: Vec::new(),
        };
        let error = validate_sync_transaction_pack(&pack).expect_err("row fanout is bounded");
        assert_eq!(error.code, crate::LixError::CODE_INVALID_PARAM);
    }

    #[test]
    fn scoped_projection_matches_only_the_requested_part_of_a_multi_schema_pack() {
        let row = |schema_key: &str| SyncRowMutation {
            schema_key: schema_key.to_owned(),
            file_id: None,
            row_pk: serde_json::json!([schema_key]),
            snapshot: Some(serde_json::json!({"value": schema_key})),
            metadata: None,
        };
        let mut pack = SyncTransactionPack {
            operation_id: "op".to_owned(),
            branch_id: "branch".to_owned(),
            base_server_commit_id: "base".to_owned(),
            local_commit_id: "local".to_owned(),
            rows: vec![row("first"), row("second")],
            files: Vec::new(),
        };
        let scope_keys = vec!["first".to_owned()];
        let scope = SyncFilterScope::new(&scope_keys);
        filter_sync_pack(&mut pack, &scope);
        assert_eq!(pack.rows.len(), 1);
        assert_eq!(pack.rows[0].schema_key, "first");
    }
}
