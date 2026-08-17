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

mod platform;
#[cfg(not(target_family = "wasm"))]
mod platform_native;
#[cfg(target_family = "wasm")]
mod platform_wasm;
mod runtime;
#[cfg(not(target_family = "wasm"))]
mod transport;

#[cfg(target_family = "wasm")]
mod transport_wasm;

pub(crate) use runtime::{SyncRuntime, activate_sync_mode};

/// Performs the one fresh-store handshake needed to seed a local repository's
/// main branch with the server's default branch identity. Reopened stores do
/// not call this path; they use their durable branch binding and reconnect in
/// the worker so offline reads remain local.
pub(crate) async fn probe_sync_branch_id(server: &crate::ServerOptions) -> Result<String, LixError> {
    let transport = platform::HttpSyncTransport::connect(
        &server.url,
        &server.headers,
        None,
    )
    .await?;
    let branch_id = transport.branch_id().to_owned();
    transport.close().await?;
    Ok(branch_id)
}

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::{future::Future, pin::Pin, time::Duration};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::binary_cas::BlobDataReader;
use crate::changelog::{ChangeRecordProjection, CommitId};
use crate::commit_graph::CommitGraphContext;
use crate::common::LixTimestamp;
use crate::json_store::{JsonLoadRequestRef, JsonReadScopeRef, JsonSlot, JsonStoreContext};
use crate::plugin::runtime::{PLUGIN_REGISTRY_KEY, is_reservation_key};
use crate::session::ExecuteIdempotency;
use crate::storage_adapter::{
    SharedStorageAdapterRead, Storage, StorageAdapterRead, StorageBeginScanOptions,
    StorageCoreProjection, StorageGetManyRequest, StorageGetOptions, StorageKey,
    StoragePrecondition, StoragePrefix, StorageProjectedValue, StorageReadOptions, StorageSpace,
    StorageSpaceId, StorageValue, StorageWriteOptions, StorageWriteSet, ValueSemantics,
    exact_get_many,
};
use crate::tracked_state::{
    TrackedStateContext, TrackedStateDiffRequest, TrackedStateFilter, TrackedStateKey,
    TrackedStateScanRequest, load_commit_delta_change_records,
    load_commit_delta_members_with_payloads,
};
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

/// Durable raw file projections fetched by a lazy file-scope pull. These are
/// intentionally separate from `lix_file` rows: the canonical descriptor and
/// blob-ref rows already belong to the commit graph, while a late byte demand
/// must survive restart without manufacturing a second commit or changing
/// tracked/untracked retention.
pub(crate) const SYNC_CLIENT_FILE_PROJECTION_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_000e),
    "sync.client_file_projection.v1",
    ValueSemantics::Mutable,
);

/// Internal public-surface filter for synthetic commits created while a
/// pristine local engine is being rebound to its server branch. The commit
/// records remain immutable and addressable by graph/jump machinery; only
/// derived user-facing commit/change projections omit these local bootstrap
/// facts so the replica has the same visible topology as the server.
pub(crate) const SYNC_HIDDEN_COMMIT_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_000f),
    "sync.hidden_commit.v1",
    ValueSemantics::Mutable,
);

const SYNC_FILE_PROJECTION_VERSION: u8 = 1;

fn sync_hidden_commit_key(commit_id: CommitId) -> StorageKey {
    StorageKey(Bytes::from(commit_id.to_string().into_bytes()))
}

fn stage_sync_hidden_commit_marker(writes: &mut StorageWriteSet, commit_id: CommitId) {
    // Use the batch-oriented write path for this non-changelog marker. The
    // changelog write-site census intentionally looks for direct `.put(`
    // calls because those are commit-record choke points; keeping this
    // metadata lane on the content-addressed helper avoids disguising a
    // marker as a new commit writer.
    writes.put_content_addressed_batch(
        SYNC_HIDDEN_COMMIT_SPACE,
        [(
            sync_hidden_commit_key(commit_id),
            StorageValue {
                bytes: Bytes::new(),
            },
        )],
    );
}

/// Loads the small internal set of synthetic bootstrap commits that must stay
/// available for local graph/jump calculations but must not appear in public
/// `lix_commit`, `lix_commit_edge`, or derived `lix_change` surfaces.
pub(crate) async fn load_sync_hidden_commit_ids<S>(
    store: &S,
) -> Result<BTreeSet<CommitId>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let range = StoragePrefix {
        bytes: Bytes::new(),
    }
    .to_range()?;
    let mut cursor = store
        .begin_scan(
            SYNC_HIDDEN_COMMIT_SPACE,
            range,
            StorageBeginScanOptions::default(),
        )
        .await?;
    let mut hidden = BTreeSet::new();
    loop {
        let (page, more) = cursor
            .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
            .await?
            .into_parts();
        for entry in page {
            let value = std::str::from_utf8(entry.key.0.as_ref()).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("sync hidden commit key is not UTF-8: {error}"),
                )
            })?;
            hidden.insert(CommitId::parse_lix(value, "sync hidden commit key")?);
        }
        if !more {
            break;
        }
    }
    Ok(hidden)
}

/// Internal demand marker used when the SQL shape cannot be mapped to a
/// finite set of relations safely (for example a CTE or nested subquery).
/// Such queries take the correctness-first full-history path.
pub(crate) const FULL_SYNC_SCOPE: &str = "\0__lix_sync_all__";
/// Wire-only scope used to request every semantic row while retaining only
/// plugin archives. Ordinary file/blob bytes stay a separate lazy projection.
const FULL_SYNC_PULL_SCOPE: &str = "__lix_sync_full__";
/// Wire-only scope used by topology backfills that need commit IDs/parents
/// but no row or file payload at all.
const TOPOLOGY_SYNC_PULL_SCOPE: &str = "__lix_sync_topology__";
/// Wire-only scope used to replay the authoritative global branch catalog.
/// Unlike the local `CONTROL_SYNC_SCOPE` readiness marker, this is sent to
/// the server and filters canonical packs down to descriptor/ref rows.
const CONTROL_SYNC_PULL_SCOPE: &str = "__lix_sync_control__";
/// Readiness marker for the global branch/commit control plane. It is
/// intentionally separate from semantic row scopes because topology is
/// reconciled by a compact catalog pull, not by user row packs.
pub(crate) const CONTROL_SYNC_SCOPE: &str = "\0__lix_sync_control__";

/// Maximum time an event-bearing sync pull waits for the authoritative head
/// to move. The endpoint is deliberately a long-poll endpoint: clients do
/// not choose between polling and waiting, and there is no wire-level
/// `waitMs` escape hatch. A timeout simply gives the client a heartbeat so it
/// can reconnect after a proxy or server restart.
pub(crate) const SYNC_LONG_POLL_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) fn is_sync_control_scope(schema_key: &str) -> bool {
    schema_key == CONTROL_SYNC_SCOPE
}

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
    /// The hot SQL path only needs to know whether this engine is a replica.
    /// Keep that predicate lock-free; the full role (including the remote
    /// identity) remains behind `role` for lifecycle/error paths.
    role_kind: Arc<AtomicU8>,
    /// SQL scope extraction is deliberately conservative, but it still scans
    /// every query string. Cache the immutable query-shape result so a warm
    /// local read pays only the branch/readiness checks, not the lexer again.
    scope_parse_cache: Arc<RwLock<BTreeMap<String, Vec<String>>>>,
    /// Query shapes that have already passed their scope barrier. A warm
    /// cached read can therefore return an empty wait set after one cheap
    /// lookup instead of reparsing/merging scopes on every call.
    ready_queries_by_branch: Arc<RwLock<BTreeMap<String, BTreeSet<String>>>>,
    scopes_by_branch: Arc<RwLock<BTreeMap<String, BTreeSet<String>>>>,
    hydrated_scopes_by_branch: Arc<RwLock<BTreeMap<String, BTreeSet<String>>>>,
    scope_notify: Arc<tokio::sync::Notify>,
    /// A level-triggered wake version for authoritative server long-polls.
    /// Unlike an edge notification, a watch receiver observes a version even
    /// when a commit lands between
    /// the storage read and waiter registration, closing the lost-wakeup
    /// race in a long-poll handler.
    change_version: Arc<AtomicU64>,
    change_watch: Arc<tokio::sync::watch::Sender<u64>>,
    /// Serializes canonical event application across foreground hydration,
    /// the background worker, and topology backfills sharing one repository.
    /// The graph identity check and its row/marker commit must be one local
    /// critical section or two clients can both replay the same event and one
    /// will advance the branch with a generated projection commit.
    apply_gate: Arc<tokio::sync::Mutex<()>>,
    /// Monotonically changes whenever the active branch changes. Hydration is
    /// asynchronous, so an old-branch worker must not publish readiness after
    /// a switch has reset the process-local marks.
    scope_generation: Arc<AtomicU64>,
}

impl Default for SyncModeState {
    fn default() -> Self {
        Self {
            role: Arc::new(RwLock::new(SyncRole::Disabled)),
            role_kind: Arc::new(AtomicU8::new(0)),
            scope_parse_cache: Arc::new(RwLock::new(BTreeMap::new())),
            ready_queries_by_branch: Arc::new(RwLock::new(BTreeMap::new())),
            scopes_by_branch: Arc::new(RwLock::new(BTreeMap::new())),
            hydrated_scopes_by_branch: Arc::new(RwLock::new(BTreeMap::new())),
            scope_notify: Arc::new(tokio::sync::Notify::new()),
            change_version: Arc::new(AtomicU64::new(0)),
            change_watch: Arc::new(tokio::sync::watch::channel(0u64).0),
            apply_gate: Arc::new(tokio::sync::Mutex::new(())),
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

    pub(crate) async fn lock_apply_gate(&self) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.apply_gate).lock_owned().await
    }

    pub(crate) fn change_watcher(&self) -> tokio::sync::watch::Receiver<u64> {
        self.change_watch.subscribe()
    }

    pub(crate) fn notify_sync_change(&self) {
        let version = self.change_version.fetch_add(1, Ordering::AcqRel) + 1;
        self.change_watch.send_replace(version);
    }

    pub(crate) fn set_role(&self, role: SyncRole) -> Result<(), LixError> {
        let role_kind = match &role {
            SyncRole::Disabled => 0,
            SyncRole::Authority => 1,
            SyncRole::Replica { .. } => 2,
        };
        *self.role.write().map_err(|_| {
            LixError::new(LixError::CODE_INTERNAL_ERROR, "sync mode state is poisoned")
        })? = role;
        self.role_kind.store(role_kind, Ordering::Release);
        Ok(())
    }

    pub(crate) fn is_replica(&self) -> bool {
        self.role_kind.load(Ordering::Acquire) == 2
    }

    /// Registers the schemas touched by an application query on one branch.
    /// The durable cursor remains independent so a replica can advance past
    /// unrelated repository data without materializing it.
    pub(crate) fn register_sql_scope_for_branch(&self, sql: &str, branch_id: &str) -> Vec<String> {
        if !self.is_replica() {
            return Vec::new();
        }
        if let Ok(branches) = self.ready_queries_by_branch.read()
            && branches
                .get(branch_id)
                .is_some_and(|queries| queries.contains(sql))
        {
            return Vec::new();
        }
        let schemas = self
            .scope_parse_cache
            .read()
            .ok()
            .and_then(|cache| cache.get(sql).cloned())
            .unwrap_or_else(|| {
                let schemas = extract_sql_scope_schema_keys(sql);
                if let Ok(mut cache) = self.scope_parse_cache.write() {
                    // Keep this cache bounded because query text is supplied
                    // by applications and is not a repository fact.
                    if cache.len() >= 512 {
                        cache.clear();
                    }
                    cache.insert(sql.to_owned(), schemas.clone());
                }
                schemas
            });
        if schemas.is_empty() {
            return Vec::new();
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
        // Most steady-state reads repeat a previously seen query shape. Avoid
        // taking the write lock when every scope is already registered for the
        // branch; the first demand still takes the write path below.
        if let Ok(branches) = self.scopes_by_branch.read()
            && branches
                .get(branch_id)
                .is_some_and(|registered| schemas.iter().all(|scope| registered.contains(scope)))
        {
            if self.scopes_are_hydrated_for_branch(&schemas, branch_id) {
                self.remember_ready_query(branch_id, sql);
                return Vec::new();
            }
            // The demand is already registered and the worker owns its
            // hydration retry. Do not emit an edge wake for every foreground
            // read while it is pending: a polling query (or two observers)
            // would otherwise cancel the worker's admission/pull on every
            // attempt and can starve the queue indefinitely. The first
            // registration below still wakes a worker blocked in a long-poll.
            return schemas;
        }
        let mut effective_scopes = schemas.clone();
        if let Ok(mut branches) = self.scopes_by_branch.write() {
            let scopes = branches.entry(branch_id.to_owned()).or_default();
            if schemas.iter().any(|schema| schema == FULL_SYNC_SCOPE)
                || scopes.len().saturating_add(schemas.len()) > MAX_SYNC_SCOPE_KEYS
            {
                // An unbounded demand set would make every subsequent
                // manifest write grow without limit. A complete-history
                // marker is the correctness-preserving fallback.
                scopes.clear();
                scopes.insert(FULL_SYNC_SCOPE.to_owned());
                effective_scopes = vec![FULL_SYNC_SCOPE.to_owned()];
            } else {
                scopes.extend(schemas.iter().cloned());
            }
        }
        // Scope registration is local state, not a canonical commit, but it
        // is still a worker input. Interrupt an in-flight long-poll so the
        // requested projection can hydrate immediately.
        self.notify_sync_change();
        effective_scopes
    }

    /// Registers an explicit scope supplied by the manual `SyncClient`
    /// adapter. Unlike SQL-shape registration this path is intentionally
    /// usable before the engine has entered lifecycle replica mode: the
    /// caller has already opted into synchronization by opening a client.
    /// Registration is only a demand marker; it must not mark the scope as
    /// hydrated before `hydrate_requested_scopes` has replayed its canonical
    /// history.
    pub(crate) fn register_explicit_scopes_for_branch(
        &self,
        branch_id: &str,
        schema_keys: &[&str],
    ) -> Vec<String> {
        let schemas = schema_keys
            .iter()
            .filter(|schema| !schema.is_empty())
            .map(|schema| (*schema).to_owned())
            .collect::<Vec<_>>();
        if schemas.is_empty() {
            return Vec::new();
        }
        let effective_scopes = if schemas.len() > MAX_SYNC_SCOPE_KEYS
            || schemas
                .iter()
                .any(|schema| schema.len() > MAX_SYNC_SCOPE_KEY_BYTES)
        {
            vec![FULL_SYNC_SCOPE.to_owned()]
        } else {
            schemas
        };
        let mut scope_changed = false;
        if let Ok(mut branches) = self.scopes_by_branch.write() {
            let registered = branches.entry(branch_id.to_owned()).or_default();
            if effective_scopes
                .iter()
                .any(|scope| scope == FULL_SYNC_SCOPE)
                || registered.len().saturating_add(effective_scopes.len()) > MAX_SYNC_SCOPE_KEYS
            {
                scope_changed = registered.len() != 1 || !registered.contains(FULL_SYNC_SCOPE);
                registered.clear();
                registered.insert(FULL_SYNC_SCOPE.to_owned());
                if scope_changed {
                    self.notify_sync_change();
                }
                return vec![FULL_SYNC_SCOPE.to_owned()];
            }
            for scope in &effective_scopes {
                scope_changed |= registered.insert(scope.clone());
            }
        }
        // Manual clients use the same worker wake path as SQL-derived scope
        // demand. The next flush hydrates the relation before acknowledging
        // the transaction pack.
        if scope_changed {
            self.notify_sync_change();
        }
        effective_scopes
    }

    fn remember_ready_query(&self, branch_id: &str, sql: &str) {
        if let Ok(mut branches) = self.ready_queries_by_branch.write() {
            let queries = branches.entry(branch_id.to_owned()).or_default();
            if queries.len() >= 512 {
                queries.clear();
            }
            queries.insert(sql.to_owned());
        }
    }

    pub(crate) fn scopes_for_branch(&self, branch_id: &str) -> Vec<String> {
        self.scopes_by_branch
            .read()
            .ok()
            .and_then(|branches| branches.get(branch_id).cloned())
            .map(|scopes| scopes.into_iter().collect())
            .unwrap_or_default()
    }

    pub(crate) fn scopes_are_hydrated_for_branch(
        &self,
        requested_scopes: &[String],
        branch_id: &str,
    ) -> bool {
        requested_scopes.is_empty()
            || self
                .hydrated_scopes_by_branch
                .read()
                .ok()
                .and_then(|branches| branches.get(branch_id).cloned())
                .is_some_and(|hydrated| {
                    (hydrated.contains(FULL_SYNC_SCOPE)
                        // FULL_SYNC_SCOPE is the row/topology projection. It
                        // deliberately omits ordinary file bytes, so a raw
                        // file-view demand still needs its own marker.
                        && requested_scopes.iter().all(|scope| scope != "lix_file"))
                        || requested_scopes
                            .iter()
                            .all(|scope| hydrated.contains(scope))
                })
    }

    pub(crate) fn hydrated_scopes_snapshot_for_branch(&self, branch_id: &str) -> BTreeSet<String> {
        self.hydrated_scopes_by_branch
            .read()
            .ok()
            .and_then(|branches| branches.get(branch_id).cloned())
            .unwrap_or_default()
    }

    pub(crate) fn restore_hydrated_scopes_for_branch(
        &self,
        branch_id: &str,
        scopes: BTreeSet<String>,
    ) {
        if let Ok(mut branches) = self.hydrated_scopes_by_branch.write() {
            branches.insert(branch_id.to_owned(), scopes);
            self.scope_notify.notify_waiters();
        }
    }

    /// Invalidates selected process-local readiness marks for one branch.
    ///
    /// A scope marker means that the background worker completed a pull at
    /// some point in the past; it is not a lease that guarantees a control
    /// catalog still contains every object created afterwards. Retry paths
    /// that observed a local not-found therefore invalidate the relevant
    /// marker before waiting, forcing the next worker iteration to reconcile
    /// the authoritative scope without putting ordinary cached reads on the
    /// network hot path.
    pub(crate) fn invalidate_scopes_for_branch(&self, branch_id: &str, scopes: &[&str]) {
        if let Ok(mut branches) = self.hydrated_scopes_by_branch.write()
            && let Some(hydrated) = branches.get_mut(branch_id)
        {
            for scope in scopes {
                hydrated.remove(*scope);
            }
        }
        if let Ok(mut queries) = self.ready_queries_by_branch.write() {
            // A query cached as ready may have observed the missing object;
            // force its next execution through the scope registration path.
            queries.remove(branch_id);
        }
        self.scope_notify.notify_waiters();
        // Invalidating a readiness marker is also a worker demand. In
        // particular, a remote branch can be created through the server's
        // catalog without a canonical row event; the next control pass must
        // be woken even when the SQL scope was already registered earlier.
        self.notify_sync_change();
    }

    pub(crate) fn scope_generation(&self) -> u64 {
        self.scope_generation.load(Ordering::Acquire)
    }

    pub(crate) fn mark_scope_hydrated_for_branch(
        &self,
        branch_id: &str,
        schema_key: &str,
        generation: u64,
    ) {
        if self.scope_generation() != generation {
            return;
        }
        if let Ok(mut branches) = self.hydrated_scopes_by_branch.write() {
            if self.scope_generation() != generation {
                return;
            }
            branches
                .entry(branch_id.to_owned())
                .or_default()
                .insert(schema_key.to_owned());
            self.scope_notify.notify_waiters();
        }
    }

    /// Advances the generation used to reject readiness marks from an
    /// in-flight worker that belongs to a previous branch selection. Existing
    /// durable marks for other branches remain valid and are intentionally
    /// retained for lazy re-entry.
    pub(crate) fn reset_scope_hydration(&self) {
        self.scope_generation.fetch_add(1, Ordering::AcqRel);
        if let Ok(mut queries) = self.ready_queries_by_branch.write() {
            queries.clear();
        }
        self.scope_notify.notify_waiters();
    }

    pub(crate) async fn wait_for_scope_hydration_for_branch(
        &self,
        requested_scopes: &[String],
        branch_id: &str,
    ) -> Result<(), LixError> {
        #[cfg(not(target_family = "wasm"))]
        {
            if !self.is_replica() || requested_scopes.is_empty() {
                return Ok(());
            }
            let deadline = Duration::from_secs(5);
            let wait = async {
                loop {
                    let ready = self
                        .hydrated_scopes_by_branch
                        .read()
                        .ok()
                        .and_then(|branches| branches.get(branch_id).cloned())
                        .is_some_and(|hydrated| {
                            (hydrated.contains(FULL_SYNC_SCOPE)
                                && requested_scopes.iter().all(|scope| scope != "lix_file"))
                                || requested_scopes
                                    .iter()
                                    .all(|scope| hydrated.contains(scope))
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
        #[cfg(target_family = "wasm")]
        {
            use futures_util::future::{Either, select};

            if !self.is_replica() || requested_scopes.is_empty() {
                return Ok(());
            }
            let wait = async {
                loop {
                    let ready = self
                        .hydrated_scopes_by_branch
                        .read()
                        .ok()
                        .and_then(|branches| branches.get(branch_id).cloned())
                        .is_some_and(|hydrated| {
                            (hydrated.contains(FULL_SYNC_SCOPE)
                                && requested_scopes.iter().all(|scope| scope != "lix_file"))
                                || requested_scopes
                                    .iter()
                                    .all(|scope| hydrated.contains(scope))
                    });
                    if ready {
                        return Ok::<(), LixError>(());
                    }
                    self.scope_notify.notified().await;
                }
            };
            // Browser WASM is single-threaded. The shared session API keeps a
            // `Send` future signature for native callers, so erase the
            // non-Send marker carried by JavaScript's Promise future here.
            let timeout = unsafe {
                crate::session::AssumeSendFuture::new(platform::sleep(Duration::from_secs(5)))
            };
            futures_util::pin_mut!(wait, timeout);
            match select(wait, timeout).await {
                Either::Left((result, _)) => result?,
                Either::Right((timer, _)) => {
                    timer?;
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "sync scope hydration did not complete before the readiness deadline",
                    )
                    .with_hint(
                        "Retry after the replica reconnects, or query only data already hydrated locally.",
                    ));
                }
            }
        }
        Ok(())
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
    // The token list below discards punctuation. A comma in a FROM list
    // would therefore make `FROM a, b` look like a single-table query and
    // could let an offline replica return an incomplete result. Treat that
    // shape as a conservative full-history demand until the SQL planner can
    // provide relation identities directly.
    if has_comma_from_list(sql) {
        return vec![FULL_SYNC_SCOPE.to_owned()];
    }
    let tokens = sql
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .filter(|token| !token.is_empty())
        .map(|token| token.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let mut scopes = BTreeSet::new();
    if tokens.iter().any(|token| {
        matches!(
            token.as_str(),
            "lix_active_branch_id" | "lix_active_branch_commit_id"
        )
    }) {
        // Keep extracting relation scopes as well. A query can combine a
        // control-plane function with an ordinary relation, and both sides
        // must be ready before executing against the local replica.
        scopes.insert(CONTROL_SYNC_SCOPE.to_owned());
    }
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
    for (index, token) in tokens.iter().enumerate() {
        if matches!(token.as_str(), "from" | "join" | "into" | "update")
            && let Some(table) = tokens.get(index + 1)
        {
            if table == "select" {
                return vec![FULL_SYNC_SCOPE.to_owned()];
            } else if table == "lix_registered_schema" && token == "from" {
                // The registered-schema catalog is a semantic bootstrap lane,
                // not the branch/topology control lane. A write that targets
                // an uncached application table waits for this catalog before
                // planning; waiting only for CONTROL_SYNC_SCOPE can wake as
                // soon as branch refs reconcile, before the schema row is
                // visible locally.
                scopes.insert(table.clone());
            } else if is_sync_history_schema(table) {
                scopes.insert(FULL_SYNC_SCOPE.to_owned());
            } else if is_sync_control_schema(table) {
                scopes.insert(CONTROL_SYNC_SCOPE.to_owned());
            } else if table != "lix_registered_schema"
                && (ordinary_sync_schema(table) || table == "lix_file")
            {
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

fn has_comma_from_list(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let is_word = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_';
    let stop_words = [
        "where",
        "group",
        "order",
        "having",
        "limit",
        "offset",
        "union",
        "intersect",
        "except",
        "returning",
    ];
    let mut index = 0;
    while index < bytes.len() {
        let Some(relative) = lower[index..].find("from") else {
            break;
        };
        let start = index + relative;
        let end = start + 4;
        if (start == 0 || !is_word(bytes[start - 1]))
            && (end == bytes.len() || !is_word(bytes[end]))
        {
            let mut token_start = end;
            let mut saw_comma = false;
            for (offset, byte) in bytes[end..].iter().copied().enumerate() {
                let absolute = end + offset;
                if byte == b',' {
                    saw_comma = true;
                    continue;
                }
                if is_word(byte) {
                    if absolute + 1 == bytes.len() || !is_word(bytes[absolute + 1]) {
                        let token = &lower[token_start..=absolute];
                        if stop_words.contains(&token) {
                            break;
                        }
                        token_start = absolute + 1;
                    }
                } else {
                    token_start = absolute + 1;
                }
                if byte == b';' {
                    break;
                }
            }
            if saw_comma {
                return true;
            }
        }
        index = end;
    }
    false
}

fn is_sync_control_schema(schema_key: &str) -> bool {
    matches!(
        schema_key,
        "lix_branch" | "lix_branch_descriptor" | "lix_branch_ref"
    )
}

fn is_file_projection_sync_schema(schema_key: &str) -> bool {
    matches!(
        schema_key,
        "lix_file_descriptor" | "lix_directory_descriptor" | "lix_binary_blob_ref"
    )
}

/// History and diff surfaces depend on commit/change topology rather than a
/// single semantic row scope. Until commit packs are independently hydrated,
/// these queries conservatively request the complete canonical event stream
/// instead of risking a locally incomplete diff.
fn is_sync_history_schema(schema_key: &str) -> bool {
    matches!(
        schema_key,
        "lix_change" | "lix_commit" | "lix_commit_edge" | "lix_diff" | "lix_working_diff"
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

pub(crate) fn validate_sync_branch_id(branch_id: &str) -> Result<(), LixError> {
    validate_sync_identity_component("branchId", branch_id, MAX_SYNC_SCOPE_KEY_BYTES)?;
    if crate::storage_codec::id_string::uuid_bytes_from_canonical(branch_id).is_none() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync branchId must be a canonical UUID",
        ));
    }
    Ok(())
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
    /// Control-plane rows are global facts (`lix_branch_descriptor` and
    /// `lix_branch_ref`) rather than branch-local semantic rows. They travel
    /// in the same atomic transaction pack, but the server validates these
    /// flags against the allow-listed control schemas before applying them.
    #[serde(default, skip_serializing_if = "is_false")]
    pub global: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub untracked: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
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
    /// Optional commit-graph parents supplied by a local merge/checkpoint.
    /// Ordinary writes leave this empty and the server derives the first
    /// parent from its authoritative admission head.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parent_commit_ids: Vec<String>,
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
    let mut parent_ids = BTreeSet::new();
    for parent in &pack.parent_commit_ids {
        let parent = CommitId::parse_lix(parent, "sync parent_commit_id")?;
if pack.local_commit_id == parent || !parent_ids.insert(parent) {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync pack contains an invalid parent topology",
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
        let control = is_sync_control_schema(&row.schema_key);
        if row.global != control || row.untracked != (row.schema_key == "lix_branch_ref") {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync row control flags do not match its schema",
            ));
        }
        if control && row.file_id.is_some() {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync branch control rows cannot carry a fileId",
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

/// Validates a canonical event after a lazy projection removed every payload
/// row and file. Empty packs are never accepted for admission, but a scoped
/// pull may legitimately carry only the event identity/topology so history
/// surfaces can materialize its graph node without downloading blobs.
fn validate_sync_topology_event_pack(pack: &SyncTransactionPack) -> Result<usize, LixError> {
    if !pack.rows.is_empty() || !pack.files.is_empty() {
        return validate_sync_transaction_pack(pack);
    }
    for (field, value) in [
        ("operationId", &pack.operation_id),
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
    let mut parents = BTreeSet::new();
    for parent in &pack.parent_commit_ids {
        let parent = CommitId::parse_lix(parent, "sync topology parent_commit_id")?;
if parent == pack.local_commit_id || !parents.insert(parent) {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync topology pack contains an invalid parent topology",
            ));
        }
    }
    let encoded = serde_json::to_vec(pack).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("encode sync topology pack: {error}"),
        )
    })?;
    if encoded.len() > MAX_SYNC_PACK_BYTES {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("sync topology pack exceeds {MAX_SYNC_PACK_BYTES} bytes"),
        ));
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
    let canonical = CommitId::parse_lix(&event.canonical_commit_id, "sync canonical commit_id")?;
    let mut seen = BTreeSet::new();
    for parent in &event.parent_commit_ids {
        let parent = CommitId::parse_lix(parent, "sync canonical parent_commit_id")?;
        if parent == canonical || !seen.insert(parent) {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync canonical event contains an invalid parent topology",
            ));
        }
    }
    Ok(())
}

/// Validates the branch head advertised alongside a pull page before it can
/// become the replica's durable server cursor. Event identities are checked
/// independently, but a head-only page has no event to carry that validation;
/// accepting an arbitrary value here would poison the next offline admission
/// base and could make a malformed transport response persist indefinitely.
fn validate_sync_pull_head_commit_id(commit_id: &str) -> Result<(), LixError> {
    if commit_id.is_empty() || commit_id.len() > MAX_SYNC_SCOPE_KEY_BYTES {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync pull head has an invalid commit identity",
        ));
    }
    CommitId::parse_lix(commit_id, "sync pull head commit_id")?;
    Ok(())
}

/// One transaction in the authoritative order of a branch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncCanonicalEvent {
    pub cursor: u64,
    pub canonical_commit_id: String,
    /// Ordered canonical parents. Older servers omitted this field; clients
    /// retain the safe generated-parent fallback when it is absent.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parent_commit_ids: Vec<String>,
    /// Digest of the unfiltered canonical pack. Scoped responses retain it so
    /// applied markers compare the same event regardless of projection order.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub pack_fingerprint: String,
    pub pack: SyncTransactionPack,
}

/// The download-side representation of one authoritative commit.
///
/// Admission and replication have different data needs. A client proposal
/// needs its operation id, local commit id, and server-base cursor so the
/// server can validate and acknowledge the write. A canonical pull only needs
/// the committed identity/topology and the row/file delta. Keeping this
/// projection explicit prevents a future commit-pull transport from carrying
/// proposal-only fields back to every replica, while the current wire event
/// remains backward-compatible through [`SyncCanonicalEvent::pack`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SyncCommitPack {
    pub(crate) branch_id: String,
    pub(crate) canonical_commit_id: String,
    pub(crate) parent_commit_ids: Vec<String>,
    pub(crate) rows: Vec<SyncRowMutation>,
    pub(crate) files: Vec<SyncFileMutation>,
    pub(crate) pack_fingerprint: String,
}

impl SyncCanonicalEvent {
    /// Extracts the canonical commit payload from a legacy transaction-shaped
    /// event. The conversion is deliberately lossless for replication fields
    /// and deliberately drops admission metadata (`operation_id`,
    /// `base_server_commit_id`, and `local_commit_id`).
    pub(crate) fn commit_pack(&self) -> SyncCommitPack {
        SyncCommitPack {
            branch_id: self.pack.branch_id.clone(),
            canonical_commit_id: self.canonical_commit_id.clone(),
            parent_commit_ids: if self.parent_commit_ids.is_empty() {
                self.pack.parent_commit_ids.clone()
            } else {
                self.parent_commit_ids.clone()
            },
            rows: self.pack.rows.clone(),
            files: self.pack.files.clone(),
            pack_fingerprint: self.pack_fingerprint.clone(),
        }
    }
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

/// Returns true for errors that mean the remote cannot currently be reached.
/// A manual client may still queue a local write against its last durable
/// server head in this case; malformed protocol data and server-side
/// validation errors must continue to fail closed.
fn is_sync_transport_unavailable(error: &LixError) -> bool {
    if error.code != LixError::CODE_INTERNAL_ERROR {
        return false;
    }
    let message = error.message.to_ascii_lowercase();
    [
        "offline",
        "error sending request",
        "connection refused",
        "connection reset",
        "timed out",
        "timeout",
        "dns error",
        "transport",
    ]
    .iter()
    .any(|marker| message.contains(marker))
}

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

    /// Whether [`list_branches`](Self::list_branches) is an authoritative
    /// catalog for this transport. The default is deliberately false: an
    /// empty result from an embedded or test transport must not be interpreted
    /// as proof that every local branch was deleted. Only an explicit catalog
    /// implementation may drive destructive branch pruning.
    fn has_authoritative_branch_catalog(&self) -> bool {
        false
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

    fn has_authoritative_branch_catalog(&self) -> bool {
        false
    }
}

/// Applies the remote global branch catalog to a local repository. This is a
/// control-plane reconciliation path, separate from semantic row events. A
/// branch whose source commit is not materialized locally yet is left for a
/// later pass rather than being created at an incorrect head. The post-flush
/// pass may prune local branches absent from an explicitly authoritative
/// catalog; the pre-flush pass deliberately does not, so an offline local
/// branch can first enter the admission queue.
pub(crate) async fn reconcile_sync_branches<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    transport: &Transport,
    prune_missing: bool,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    let remote_branches = transport.list_branches().await?;
    let active_branch_id = lix.active_branch_id().await?;
    // A first-contact head-only pull advances the durable cursor without
    // replaying its payload. Once control metadata is demanded, materialize
    // the selected branch's canonical head as well; otherwise the local
    // branch ref can remain on its synthetic bootstrap commit forever even
    // though the server catalog advertises a different head. Never do this
    // while a local outbox is pending: the normal sync client must preserve
    // that optimistic overlay and reconcile it through admission.
    if let Some(remote_active) = remote_branches
        .iter()
        .find(|branch| branch.id == active_branch_id)
    {
        validate_sync_branch_catalog_entry(remote_active)?;
        if !lix
            .sync_branch_has_pending(transport.remote_id(), &active_branch_id)
            .await?
        {
            let local_head = lix
                .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
                .await?
                .rows()
                .first()
                .and_then(|row| row.get::<String>("commit_id").ok());
            if local_head.as_deref() != Some(remote_active.commit_id.as_str()) {
                hydrate_sync_commit_from_active_branch(lix, transport, &remote_active.commit_id)
                    .await?;
            }
            // Branch refs live on the global control branch, while their
            // targets are commits from the user branch. A lazy replica may
            // therefore have the active commit in its source-branch graph but
            // not yet in the global graph used to validate
            // `lix_branch_ref.commit_id`. Materialize that identity on the
            // control branch before any catalog write. Skip the bridge while
            // an optimistic local queue is pending; admission will publish
            // the canonical head and the next clean pass bridges it.
            ensure_sync_commit_on_global_branch(lix, &remote_active.commit_id).await?;
        }
    }
    // A pre-sync branch may be the only canonical event carrying a commit
    // that is now a parent of the selected branch's bootstrap head. Import
    // just those dependency events after the selected branch's semantic
    // history: otherwise a topology-only replay of the same canonical parent
    // can make the active row event look already materialized and skip the
    // schema/row projection needed by this branch.
    if !lix
        .sync_mode_state()
        .scopes_are_hydrated_for_branch(&[CONTROL_SYNC_SCOPE.to_owned()], &active_branch_id)
    {
        hydrate_sync_active_parent_dependencies(lix, transport, &remote_branches).await?;
    }
    // Import repository-global control topology only after the selected
    // branch's semantic history has been materialized. Global events can
    // reference branch commits as parents; doing this first would make those
    // canonical nodes appear present and cause the active schema/row payload
    // to be skipped by the idempotent replay path.
    if transport.has_authoritative_branch_catalog() {
        hydrate_sync_branch_events(lix, transport, GLOBAL_BRANCH_ID).await?;
        // A pristine replica may have a local control root whose current
        // catalog predates the canonical global event (for example after the
        // first-contact default-branch rebind). Recreate the selected branch
        // descriptor/ref from the authoritative catalog when that projection
        // is absent; this is idempotent and does not touch application rows.
        if let Some(remote_active) = remote_branches
            .iter()
            .find(|branch| branch.id == active_branch_id)
        {
            let local_active = lix
                .execute(
                    "SELECT id FROM lix_branch WHERE id = $1",
                    &[Value::Text(active_branch_id.clone())],
                )
                .await?;
            if local_active.rows().is_empty() {
                // The selected branch was created from the canonical global
                // root. A later topology-only replay can advance that root
                // without projecting its descriptor into the selected
                // branch's derived view. Do not call create_branch here: the
                // branch/ref identity already exists and re-creating it is a
                // duplicate untracked-primary-key write. Re-stage the
                // authoritative control rows on the selected branch's
                // current root instead.
                restore_sync_active_branch_catalog(lix, remote_active).await?;
            }
        }
    }
    let remote_ids = remote_branches
        .iter()
        .map(|branch| {
            validate_sync_branch_catalog_entry(branch)?;
            Ok(branch.id.clone())
        })
        .collect::<Result<BTreeSet<_>, LixError>>()?;
    for remote in remote_branches {
        if remote.id == GLOBAL_BRANCH_ID {
            continue;
        }
        let existing = lix
            .execute(
                "SELECT id, name, hidden, commit_id FROM lix_branch WHERE id = $1",
                &[Value::Text(remote.id.clone())],
            )
            .await?;
        if existing.rows().is_empty() {
            // First give the selected branch a chance to materialize a source
            // commit that is already in its canonical stream. The branch
            // descriptor/ref itself is then created through the suppressed
            // global lane; never publish this control operation on the
            // selected application branch.
            if let Err(error) =
                hydrate_sync_commit_from_active_branch(lix, transport, &remote.commit_id).await
            {
                tracing::warn!(
                    error = ?error,
                    branch = %remote.id,
                    "sync new-branch head replay deferred"
                );
            }
            ensure_sync_commit_on_global_branch(lix, &remote.commit_id).await?;
            ensure_sync_source_branch(lix, &remote).await?;

            let source = lix
                .execute(
                    "SELECT commit_id FROM lix_branch WHERE id = $1",
                    &[Value::Text(remote.id.clone())],
                )
                .await?;
            let source_commit = source
                .rows()
                .first()
                .and_then(|row| row.get::<String>("commit_id").ok());
            if source_commit.as_deref() != Some(remote.commit_id.as_str()) {
                // If the source commit was not on the selected branch, its
                // own event stream may still provide a canonical bootstrap.
                // Keep semantic rows on that branch and update the catalog
                // through the global control session only when the commit is
                // materialized locally.
                if let Err(error) = hydrate_sync_branch_events(lix, transport, &remote.id).await {
                    tracing::warn!(
                        error = ?error,
                        branch = %remote.id,
                        "sync new-branch topology replay deferred"
                    );
                }
                ensure_sync_commit_on_global_branch(lix, &remote.commit_id).await?;
                let global = lix
                    .open_internal_session_suppressed(
                        GLOBAL_BRANCH_ID.to_owned(),
                        lix.active_account_id().to_owned(),
                    )
                    .await?;
                let update = global
                    .execute(
                        "UPDATE lix_branch SET name = $1, hidden = $2, commit_id = $3 WHERE id = $4",
                        &[
                            Value::Text(remote.name.clone()),
                            Value::Boolean(remote.hidden),
                            Value::Text(remote.commit_id.clone()),
                            Value::Text(remote.id.clone()),
                        ],
                    )
                    .await;
                global.close().await?;
                if let Err(error) = update
                    && error.code != LixError::CODE_COMMIT_NOT_FOUND
                    && error.code != LixError::CODE_FOREIGN_KEY
                {
                    return Err(error);
                }
            }
            continue;
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
            if current_commit.as_deref() != Some(remote.commit_id.as_str()) {
                // Pull the branch's own event stream before updating its ref.
                // This materializes source commits that are not ancestors of
                // the currently selected branch (for example a feature head
                // that will later become a merge parent).
                if let Err(error) = hydrate_sync_branch_events(lix, transport, &remote.id).await {
                    tracing::warn!(
                        error = ?error,
                        branch = %remote.id,
                        "sync branch topology replay deferred"
                    );
                }
                if let Err(error) =
                    hydrate_sync_commit_from_active_branch(lix, transport, &remote.commit_id)
                        .await
                {
                    tracing::warn!(
                        error = ?error,
                        branch = %remote.id,
                        "sync branch head replay deferred"
                    );
                }
                ensure_sync_commit_on_global_branch(lix, &remote.commit_id).await?;
            }
            let global = lix
                .open_internal_session_suppressed(
                    GLOBAL_BRANCH_ID.to_owned(),
                    lix.active_account_id().to_owned(),
                )
                .await?;
            let update = global
                .execute(
                    "UPDATE lix_branch SET name = $2, hidden = $3, commit_id = $4 WHERE id = $1",
                    &[
                        Value::Text(remote.id),
                        Value::Text(remote.name),
                        Value::Boolean(remote.hidden),
                        Value::Text(remote.commit_id),
                    ],
                )
                .await;
            global.close().await?;
            if let Err(error) = update
                && error.code != LixError::CODE_COMMIT_NOT_FOUND
                && error.code != LixError::CODE_FOREIGN_KEY
            {
                return Err(error);
            }
        }
    }
    if prune_missing && transport.has_authoritative_branch_catalog() {
        let default_branch_id = lix
            .execute(
                "SELECT value FROM lix_key_value WHERE key = $1",
                &[Value::Text(crate::init::DEFAULT_BRANCH_KEY.to_owned())],
            )
            .await?
            .rows()
            .first()
            .and_then(|row| row.get::<serde_json::Value>("value").ok())
            .and_then(|value| value.as_str().map(str::to_owned));
        let local = lix
            .execute(
                "SELECT id FROM lix_branch WHERE id != $1",
                &[Value::Text(GLOBAL_BRANCH_ID.to_owned())],
            )
            .await?;
        for row in local.rows() {
            let Some(branch_id) = row.get::<String>("id").ok() else {
                continue;
            };
            if branch_id == active_branch_id
                || default_branch_id.as_deref() == Some(branch_id.as_str())
                || remote_ids.contains(branch_id.as_str())
            {
                continue;
            }
            // A remote catalog can delete a branch while this replica still
            // has an offline outbox for it. Keep the local branch descriptor
            // until that durable queue is either admitted or explicitly
            // discarded; pruning it here would strand the pending writes and
            // make reconnect unable to drain the branch.
            if lix
                .sync_branch_has_pending(transport.remote_id(), &branch_id)
                .await?
            {
                continue;
            }
            // The worker session suppresses its own outbox, so this catalog
            // cleanup cannot echo a remote deletion back to the server.
            lix.execute(
                "DELETE FROM lix_branch WHERE id = $1",
                &[Value::Text(branch_id)],
            )
            .await?;
        }
    }
    Ok(())
}

/// Hydrates a direct parent from the remote branch that owns it.
///
/// A pre-sync branch can be the only canonical event carrying a commit that is
/// now a parent of the selected branch's bootstrap head. The dependency must
/// be replayed on its own local branch session: applying the source pack on the
/// selected branch would temporarily (or permanently, when that scope remains
/// lazy) leak the source branch's semantic rows into the active view.
async fn hydrate_sync_active_parent_dependencies<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    transport: &Transport,
    remote_branches: &[SyncBranch],
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    let active_branch_id = lix.active_branch_id().await?;
    let active_response = transport
        .pull(
            &active_branch_id,
            0,
            DEFAULT_SYNC_PULL_LIMIT,
            &[TOPOLOGY_SYNC_PULL_SCOPE.to_owned()],
        )
        .await?;
    require_sync_identity(
        "sync active dependency pull branch",
        &active_branch_id,
        &active_response.branch_id,
    )?;
    validate_sync_pull_head_commit_id(&active_response.head_commit_id)?;

    let mut needed = BTreeSet::new();
    for event in active_response.events {
        validate_sync_canonical_event_identity(&event)?;
        for parent in event.parent_commit_ids {
            let parent = CommitId::parse_lix(&parent, "sync active dependency parent")?;
            needed.insert(parent.to_string());
        }
    }
    if needed.is_empty() {
        return Ok(());
    }

    let adapter = lix.storage_adapter();
    let read = adapter.begin_read(StorageReadOptions::default()).await?;
    let mut graph = CommitGraphContext::new().reader(read);
    let needed_ids = needed
        .iter()
        .map(|id| CommitId::parse_lix(id, "sync active dependency parent"))
        .collect::<Result<Vec<_>, _>>()?;
    let existing = graph.load_nodes(&needed_ids).await?;
    let mut missing = BTreeSet::new();
    for (commit_id, node) in existing {
        if node.is_none() {
            missing.insert(commit_id.to_string());
        }
    }
    if missing.is_empty() {
        return Ok(());
    }

    for remote in remote_branches {
        validate_sync_branch_catalog_entry(remote)?;
        if remote.id == active_branch_id || remote.id == GLOBAL_BRANCH_ID {
            continue;
        }
        let remote_head = CommitId::parse_lix(&remote.commit_id, "sync dependency branch head")?;
        if !missing.contains(&remote_head.to_string()) {
            continue;
        }

        // The source branch may not exist locally yet. Create only its
        // control-plane placeholder, rooted at the local active head; the
        // suppressed hydration below immediately advances it to the canonical
        // source event without producing an outbox proposal.
        ensure_sync_source_branch(lix, remote).await?;

        // `hydrate_sync_branch_events` owns a suppressed session for the
        // source branch, so semantic rows and its moving ref remain isolated
        // from the active branch while the shared changelog gains the exact
        // canonical commit identity.
        hydrate_sync_branch_events(lix, transport, &remote.id).await?;
        let adapter = lix.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let mut graph = CommitGraphContext::new().reader(read);
        if graph.load_node(&remote_head).await?.is_some() {
            missing.remove(&remote_head.to_string());
        }
    }
    Ok(())
}

/// Validates control-plane data before a remote branch catalog can influence
/// local SQL writes.  Branch entries are decoded from an untrusted transport,
/// so validating the identity and commit topology here keeps malformed values
/// from reaching branch lifecycle code (or being retained in the local
/// catalog).  The server-side catalog is already constrained by the same
/// commit parser; this check protects embedded/custom transports too.
fn validate_sync_branch_catalog_entry(branch: &SyncBranch) -> Result<(), LixError> {
    validate_sync_branch_id(&branch.id)?;
    if branch.name.len() > MAX_SYNC_SCOPE_KEY_BYTES {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync branch name exceeds the maximum identity size",
        ));
    }
    let commit_id = CommitId::parse_lix(&branch.commit_id, "sync branch commit_id")?;
    if commit_id.to_string().len() > MAX_SYNC_SCOPE_KEY_BYTES {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync branch commit_id exceeds the maximum identity size",
        ));
    }
    Ok(())
}

/// Backfills canonical semantic events for the currently selected branch
/// until `commit_id` exists locally. Branch catalog entries can point at a
/// commit that was skipped by a head-only lazy pull; replaying the filtered
/// history here establishes the same commit identity and state topology
/// before the ordinary branch lifecycle creates its ref.
async fn hydrate_sync_commit_from_active_branch<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    transport: &Transport,
    commit_id: &str,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    let target = CommitId::parse_lix(commit_id, "sync branch source commit_id")?;
    let target_id = target.to_string();
    let active_branch_id = lix.active_branch_id().await?;
    let history_scope = vec![FULL_SYNC_PULL_SCOPE.to_owned()];
    let mut cursor = 0;
    loop {
        let response = transport
            .pull(
                &active_branch_id,
                cursor,
                DEFAULT_SYNC_PULL_LIMIT,
                &history_scope,
            )
            .await?;
        require_sync_identity(
            "topology pull branch",
            &active_branch_id,
            &response.branch_id,
        )?;
        validate_sync_pull_head_commit_id(&response.head_commit_id)?;
        let response_empty = response.events.is_empty();
        for event in response.events {
            cursor = event.cursor;
            validate_sync_canonical_event_identity(&event)?;
            // A pre-sync branch can point at a commit whose parent was
            // published on another branch. Hydrate that parent branch before
            // replaying this event; otherwise the normal replay transaction
            // must fall back to a locally generated parent and the canonical
            // edge can never be repaired later.
            hydrate_missing_sync_parent_branches(lix, transport, &event, &active_branch_id).await?;
            // A branch-control-only event may refer to the commit that is
            // already the branch source. Its global ref cannot be replayed
            // before that commit exists, so leave control rows to catalog
            // reconciliation and only materialize semantic payloads here.
            let mut semantic_event = event.clone();
            semantic_event
                .pack
                .rows
                .retain(|row| !is_sync_control_schema(&row.schema_key));
            if !semantic_event.pack.rows.is_empty() || !semantic_event.pack.files.is_empty() {
                lix.apply_sync_canonical_event(&semantic_event, transport.remote_id(), &[])
                    .await?;
            } else {
                // A control-only event still contributes a canonical graph
                // node.  Preserve its identity/parents without replaying the
                // global control rows on the selected application branch.
                lix.apply_sync_topology_event(&event).await?;
            }
            // A concurrent topology pass may materialize the target graph
            // node before this semantic replay reaches it. Stop only after
            // the target event itself has been applied; graph presence alone
            // is not evidence that its row projection or scope marker exists.
            if event.canonical_commit_id == target_id {
                return Ok(());
            }
        }
        if cursor >= response.head_cursor || response_empty {
            return Ok(());
        }
    }
}

/// Hydrates the canonical topology for a branch that already has a local
/// placeholder ref. This intentionally requests no row/file payload: source
/// branches are not visible in the active SQL scope until the application
/// switches to them, so eagerly replaying their semantic rows would create
/// projection commits (and require schemas that the active branch does not
/// own). A later branch-local query hydrates the same stream through the
/// ordinary lazy scope path.
async fn hydrate_sync_branch_events<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    transport: &Transport,
    branch_id: &str,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    let target = lix
        .open_internal_session_suppressed(branch_id.to_owned(), lix.active_account_id().to_owned())
        .await?;
    // The branch catalog endpoint is the authoritative control plane. The
    // global event stream is therefore hydrated as topology only; descriptors
    // and moving refs are admitted from the validated catalog after source
    // commit heads are materialized, avoiding foreign-key races on bootstrap.
    // The global branch is the authoritative branch catalog. Replay its
    // descriptor rows through the canonical event stream so a replica does
    // not manufacture a local branch-management commit for every remote
    // branch. Other branches remain topology-only until their rows are
    // requested lazily.
    let history_scope = if branch_id == GLOBAL_BRANCH_ID {
        vec![CONTROL_SYNC_PULL_SCOPE.to_owned()]
    } else {
        vec![TOPOLOGY_SYNC_PULL_SCOPE.to_owned()]
    };
    let mut cursor = 0;
    loop {
        // Branch-catalog hydration is a foreground prerequisite for a lazy
        // query. Probe the finite head first; an event-bearing pull at the
        // current head is intentionally a 30-second server long-poll, which
        // would make an otherwise local query hit the readiness deadline.
        let probe = transport
            .pull(branch_id, cursor, 0, &history_scope)
            .await?;
        require_sync_identity("branch topology head probe", branch_id, &probe.branch_id)?;
        validate_sync_pull_head_commit_id(&probe.head_commit_id)?;
        if cursor >= probe.head_cursor {
            break;
        }
        let response = transport
            .pull(branch_id, cursor, DEFAULT_SYNC_PULL_LIMIT, &history_scope)
            .await?;
        require_sync_identity("branch topology pull", branch_id, &response.branch_id)?;
        validate_sync_pull_head_commit_id(&response.head_commit_id)?;
        let response_empty = response.events.is_empty();
        for event in response.events {
            cursor = event.cursor;
            let canonical_commit_id = CommitId::parse_lix(
                &event.canonical_commit_id,
                "sync topology canonical commit_id",
            )?;
            let adapter = lix.storage_adapter();
            let read = adapter.begin_read(StorageReadOptions::default()).await?;
            let control_marker = if branch_id == GLOBAL_BRANCH_ID {
                load_sync_applied_marker(
                    &read,
                    transport.remote_id(),
                    branch_id,
                    CONTROL_SYNC_SCOPE,
                )
                .await?
            } else {
                None
            };
            let mut graph = CommitGraphContext::new().reader(read);
            if graph.load_node(&canonical_commit_id).await?.is_some() {
                // A selected-branch semantic pull can materialize a global
                // commit's graph node as a parent before the control rows are
                // ever requested. Do not let that topology-only presence
                // suppress the catalog projection: the control marker is the
                // receipt that proves descriptors/refs were actually applied.
                if branch_id != GLOBAL_BRANCH_ID {
                    continue;
                }
                if control_marker
                    .as_ref()
                    .is_some_and(|(_, marker)| marker_covers_event(marker, &event, &event.pack_fingerprint))
                {
                    continue;
                }
            }
            // The global control stream is the catalog itself. Do not recurse
            // into source branch streams while importing it: a branch's
            // bootstrap commit can point back at this same global event, so
            // parent discovery would form a cycle before the catalog rows
            // have been admitted. `apply_sync_topology_event` already has a
            // safe missing-parent fallback; branch-local history can still
            // use the targeted parent assist below.
            if branch_id != GLOBAL_BRANCH_ID {
            // Global catalog events are control-plane topology. Do not recurse
            // from that stream into every source branch while the global
            // stream is still being replayed: a pre-sync branch may point at
            // a global parent and the reciprocal lookup otherwise walks the
            // same two streams indefinitely. The branch-ref bridge below
            // materializes the required commit identities; semantic/full
            // history hydration repairs exact parent edges on demand.
            if branch_id != GLOBAL_BRANCH_ID {
                hydrate_missing_sync_parent_branches(lix, transport, &event, branch_id)
                    .await?;
            }
            }
            if branch_id == GLOBAL_BRANCH_ID && !event.pack.rows.is_empty() {
                // The wire scope has already removed application rows and
                // files. Apply only the catalog projection while retaining
                // the canonical commit/parent identity. The local marker is
                // the control readiness scope, not the wire filter token.
                target
                    .apply_sync_canonical_event(
                        &event,
                        transport.remote_id(),
                        &[CONTROL_SYNC_SCOPE.to_owned()],
                    )
                    .await?;
            } else {
                // The topology scope is guaranteed to carry no semantic
                // payload. Preserve the canonical commit identity and
                // parents without creating a branch-local projection commit.
                target.apply_sync_topology_event(&event).await?;
            }
        }
        if cursor >= response.head_cursor || response_empty {
            break;
        }
    }
    target.close().await
}

/// Hydrates only the remote branch streams that can provide missing parents
/// for one canonical event. This is intentionally a topology assist rather
/// than a full-history prefetch: ordinary row scopes still pull their own
/// payloads lazily, while a merge/pre-sync event waits for a known branch head
/// before it is admitted into the local graph.
async fn hydrate_missing_sync_parent_branches<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    transport: &Transport,
    event: &SyncCanonicalEvent,
    current_branch_id: &str,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    if event.parent_commit_ids.is_empty() {
        return Ok(());
    }
    let parent_ids = event
        .parent_commit_ids
        .iter()
        .map(|parent| CommitId::parse_lix(parent, "sync canonical parent_commit_id"))
        .collect::<Result<Vec<_>, _>>()?;
    let adapter = lix.storage_adapter();
    let read = adapter.begin_read(StorageReadOptions::default()).await?;
    let mut graph = CommitGraphContext::new().reader(read);
    let nodes = graph.load_nodes(&parent_ids).await?;
    let missing = parent_ids
        .iter()
        .zip(nodes.into_iter())
        .filter_map(|(parent, (_, node))| node.is_none().then_some(*parent))
        .collect::<BTreeSet<_>>();
    if missing.is_empty() {
        return Ok(());
    }

    // A branch catalog is optional for embedded transports. In that case the
    // existing generated-parent fallback remains the compatibility behavior;
    // HTTP/server transports advertise the authoritative catalog and can
    // prove which stream owns a missing parent.
    let branches = transport.list_branches().await?;
    let mut attempted = false;
    for branch in branches {
        validate_sync_branch_catalog_entry(&branch)?;
        if branch.id == current_branch_id
            || branch.id == GLOBAL_BRANCH_ID
            || !missing.contains(&CommitId::parse_lix(
                &branch.commit_id,
                "sync branch commit_id",
            )?)
        {
            continue;
        }
        attempted = true;
        ensure_sync_source_branch(lix, &branch).await?;
        // Branch streams can have merge parents on another branch. Box the
        // recursive hydration edge so the async state machine remains finite;
        // each recursive call advances that source branch's cursor toward its
        // head and graph checks make repeated edges idempotent.
        Box::pin(hydrate_sync_branch_events(lix, transport, &branch.id)).await?;
    }
    if !attempted {
        return Ok(());
    }

    // A branch stream may expose only its current bootstrap snapshot, while a
    // missing parent can predate sync event logging entirely. Keep the normal
    // generated-parent fallback in that case; the active-branch dependency
    // backfill above handles the exact parent when a suitable canonical event
    // is available without making ordinary reads fail closed.
    Ok(())
}

/// Ensures a remote source branch has a local control-plane placeholder before
/// its canonical event stream is replayed. The descriptor/ref mutation is
/// staged on the suppressed global lane so branch hydration cannot move or
/// dirty the selected application branch.
async fn restore_sync_active_branch_catalog<StorageImpl>(
    lix: &Lix<StorageImpl>,
    remote: &SyncBranch,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let branch_id = lix.active_branch_id().await?;
    if branch_id != remote.id {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync active catalog restore targeted a different branch",
        ));
    }
    let session = lix
        .open_internal_session_suppressed(branch_id, lix.active_account_id().to_owned())
        .await?;
    let mut transaction = session.begin_transaction().await?;
    let commit_id = CommitId::parse_lix(&remote.commit_id, "sync active branch commit_id")?;
    let mut rows = RawWriteBatch::with_capacity(2);
    rows.push(crate::branch::branch_descriptor_stage_row(
        &remote.id,
        &remote.name,
        remote.hidden,
    ));
    rows.push(crate::branch::branch_ref_stage_row(&remote.id, &commit_id));
    transaction.stage_sync_rows(rows).await?;
    transaction.commit().await?;
    session.close().await
}

/// Makes a canonical commit visible from the global control branch without
/// copying any semantic rows. Branch descriptors and refs are global control
/// facts, and the local branch-ref validator consequently requires their
/// target commit to be reachable from that branch. A lazy replica often first
/// materializes the commit on its owning application branch; this small
/// topology-only bridge keeps branch catalog replication from depending on a
/// full repository prefetch.
async fn ensure_sync_commit_on_global_branch<StorageImpl>(
    lix: &Lix<StorageImpl>,
    commit_id: &str,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let commit_id = CommitId::parse_lix(commit_id, "sync global branch commit_id")?;
    let global = lix
        .open_internal_session_suppressed(
            GLOBAL_BRANCH_ID.to_owned(),
            lix.active_account_id().to_owned(),
        )
        .await?;
    let present = global
        .execute(
            "SELECT id FROM lix_commit WHERE id = $1",
            &[Value::Text(commit_id.to_string())],
        )
        .await?
        .rows()
        .iter()
        .any(|row| row.get::<String>("id").is_ok());
    if present {
        global.close().await?;
        return Ok(());
    }

    // The bridge is deliberately parentless from the control branch's point
    // of view. The source branch retains the authoritative parent edges; this
    // branch only needs a reachable commit identity so its FK-protected ref
    // can be admitted. Later topology hydration can replace the bridge with
    // the exact parent chain when those parents are requested globally.
    let mut transaction = global.begin_transaction().await?;
    transaction.stage_sync_topology_commit(&commit_id.to_string(), &[])?;
    let result = transaction.commit().await;
    global.close().await?;
    match result {
        Ok(_) => Ok(()),
        Err(error) if error.code == LixError::CODE_TRANSACTION_CONFLICT => Ok(()),
        Err(error) => Err(error),
    }
}

async fn ensure_sync_source_branch<StorageImpl>(
    lix: &Lix<StorageImpl>,
    remote: &SyncBranch,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    validate_sync_branch_catalog_entry(remote)?;
    let existing = lix
        .execute(
            "SELECT id FROM lix_branch WHERE id = $1",
            &[Value::Text(remote.id.clone())],
        )
        .await?;
    if !existing.rows().is_empty() {
        return Ok(());
    }
    let global = lix
        .open_internal_session_suppressed(
            GLOBAL_BRANCH_ID.to_owned(),
            lix.active_account_id().to_owned(),
        )
        .await?;
    let result = match global
        .create_branch(CreateBranchOptions {
            id: Some(remote.id.clone()),
            name: remote.name.clone(),
            from_commit_id: Some(remote.commit_id.clone()),
        })
        .await
    {
        Ok(receipt) => Ok(receipt),
        Err(error) if error.code == LixError::CODE_COMMIT_NOT_FOUND => {
            // A source commit outside the selected branch may only become
            // available after its branch bootstrap is replayed. Keep a
            // global placeholder so that replay can run in an isolated source
            // session without manufacturing a selected-branch commit.
            global
                .create_branch(CreateBranchOptions {
                    id: Some(remote.id.clone()),
                    name: remote.name.clone(),
                    from_commit_id: None,
                })
                .await
        }
        Err(error) => Err(error),
    };
    global.close().await?;
    result.map(|_| ())
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
    /// A pristine local engine has one temporary seed commit. Keep this
    /// durable until canonical bootstrap rows replace it and the one-shot
    /// orphan sweep commits successfully.
    #[serde(default)]
    bootstrap_cleanup_pending: bool,
    #[serde(default)]
    bootstrap_cleanup_commit_id: Option<String>,
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
    #[serde(default)]
    bootstrap_cleanup_pending: bool,
    #[serde(default)]
    bootstrap_cleanup_commit_id: Option<String>,
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

/// Durable payload for one lazy raw-file projection. The key is scoped by
/// remote, branch, and file identity; retaining the identity in the value as
/// well makes corruption and cross-repository reuse fail closed during load.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SyncFileProjection {
    version: u8,
    remote_id: String,
    branch_id: String,
    pub(crate) file_id: String,
    pub(crate) path: String,
    #[serde(with = "base64_bytes")]
    pub(crate) content: Vec<u8>,
}

impl SyncFileProjection {
    fn new(remote_id: &str, branch_id: &str, file: &SyncFileMutation) -> Result<Self, LixError> {
        let path = file.path.clone().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync file mutation is missing its logical path",
            )
        })?;
        validate_sync_remote_id(remote_id)?;
        validate_sync_branch_id(branch_id)?;
        validate_sync_identity_component("fileId", &file.file_id, MAX_SYNC_SCOPE_KEY_BYTES)?;
        crate::common::LixPath::try_from_file_path(&path)?;
        Ok(Self {
            version: SYNC_FILE_PROJECTION_VERSION,
            remote_id: remote_id.to_owned(),
            branch_id: branch_id.to_owned(),
            file_id: file.file_id.clone(),
            path,
            content: file.content.clone(),
        })
    }
}

fn sync_file_projection_key(
    remote_id: &str,
    branch_id: &str,
    file_id: &str,
) -> Result<StorageKey, LixError> {
    validate_sync_remote_id(remote_id)?;
    validate_sync_branch_id(branch_id)?;
    validate_sync_identity_component("fileId", file_id, MAX_SYNC_SCOPE_KEY_BYTES)?;
    let mut key = Vec::with_capacity(
        12usize
            .saturating_add(remote_id.len())
            .saturating_add(branch_id.len())
            .saturating_add(file_id.len()),
    );
    for component in [remote_id, branch_id, file_id] {
        let length = u32::try_from(component.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync file projection identity is too long",
            )
        })?;
        key.extend_from_slice(&length.to_be_bytes());
        key.extend_from_slice(component.as_bytes());
    }
Ok(StorageKey(Bytes::from(key)))
}

fn sync_file_projection_prefix(
    remote_id: &str,
    branch_id: &str,
) -> Result<StoragePrefix, LixError> {
    validate_sync_remote_id(remote_id)?;
    validate_sync_branch_id(branch_id)?;
    let mut prefix = Vec::with_capacity(
        8usize
            .saturating_add(remote_id.len())
            .saturating_add(branch_id.len()),
    );
    for component in [remote_id, branch_id] {
        let length = u32::try_from(component.len()).map_err(|_| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync file projection identity is too long",
            )
        })?;
        prefix.extend_from_slice(&length.to_be_bytes());
        prefix.extend_from_slice(component.as_bytes());
    }
    Ok(StoragePrefix {
bytes: Bytes::from(prefix),
    })
}

/// Adds durable lazy-file puts to an existing transaction metadata write set.
/// The caller commits applied-event markers into the same set, so a crash
/// cannot leave a cursor/marker that claims the bytes were applied while the
/// projection itself is missing.
pub(crate) fn stage_sync_file_projection_metadata(
    writes: &mut StorageWriteSet,
    remote_id: &str,
    branch_id: &str,
    files: &[SyncFileMutation],
) -> Result<(), LixError> {
    let mut seen = BTreeSet::new();
    for file in files {
        if !seen.insert(file.file_id.clone()) {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!("sync file projection repeats fileId '{}'", file.file_id),
            ));
        }
        let projection = SyncFileProjection::new(remote_id, branch_id, file)?;
        let key = sync_file_projection_key(remote_id, branch_id, &file.file_id)?;
        let value = serde_json::to_vec(&projection).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("encode sync file projection: {error}"),
            )
        })?;
        writes.put(SYNC_CLIENT_FILE_PROJECTION_SPACE, key, value);
    }
    Ok(())
}

/// Removes durable lazy-file projections after a canonical or local tracked
/// file write makes the blob-ref row authoritative again.
pub(crate) fn clear_sync_file_projection_metadata(
    writes: &mut StorageWriteSet,
    remote_id: &str,
    branch_id: &str,
    file_ids: impl IntoIterator<Item = String>,
) -> Result<(), LixError> {
    for file_id in file_ids {
        let key = sync_file_projection_key(remote_id, branch_id, &file_id)?;
        writes.delete(SYNC_CLIENT_FILE_PROJECTION_SPACE, key);
    }
    Ok(())
}

/// Loads all late raw-file projections for one branch. These entries are
/// already scoped to bytes the replica requested, so this is not a repository
/// download; it only restores local cached views after restart/new sessions.
pub(crate) async fn load_sync_file_projections<R>(
    read: &R,
    remote_id: &str,
    branch_id: &str,
) -> Result<Vec<SyncFileProjection>, LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let prefix = sync_file_projection_prefix(remote_id, branch_id)?;
    let mut cursor = read
        .begin_scan(
            SYNC_CLIENT_FILE_PROJECTION_SPACE,
            prefix.to_range().map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("build sync file projection range: {error}"),
                )
            })?,
            StorageBeginScanOptions::default(),
        )
        .await
        .map_err(LixError::from)?;
    let entries = cursor.collect_all().await.map_err(LixError::from)?;
    let mut projections = Vec::with_capacity(entries.len());
    for entry in entries {
        let StorageProjectedValue::FullValue(value) = entry.value else {
            continue;
        };
        let projection = serde_json::from_slice::<SyncFileProjection>(&value).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("decode sync file projection: {error}"),
            )
        })?;
        if projection.version != SYNC_FILE_PROJECTION_VERSION
            || projection.remote_id != remote_id
            || projection.branch_id != branch_id
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync file projection identity does not match its storage scope",
            ));
        }
        crate::common::LixPath::try_from_file_path(&projection.path)?;
        projections.push(projection);
    }
    Ok(projections)
}

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
    /// Lifecycle workers use the server's event long-poll. Manual `sync()`
    /// clients keep flush finite: they ask for the current head and return
    /// once their queue is acknowledged, while the endpoint itself remains
    /// hard-cut to long-poll for event-bearing requests.
    long_poll: bool,
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

    pub(crate) async fn sync_branch_has_pending(
        &self,
        remote_id: &str,
        branch_id: &str,
    ) -> Result<bool, LixError> {
        validate_sync_remote_id(remote_id)?;
        validate_sync_branch_id(branch_id)?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        Ok(load_sync_client_state(&read, remote_id, branch_id)
            .await?
            .is_some_and(|(_, state, _)| !state.pending.is_empty()))
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
                sync_mode.mark_scope_hydrated_for_branch(
                    &branch_id,
                    &scope,
                    sync_mode.scope_generation(),
                );
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

    /// Captures all commit projections that exist before a pristine replica's
    /// first canonical pull. These are local engine/bootstrap and temporary
    /// branch-management commits; retaining their immutable records keeps
    /// local jump metadata valid, while the derived public providers hide
    /// them so the visible graph starts at the server's history.
    pub(crate) async fn mark_sync_bootstrap_commits_hidden(&self) -> Result<(), LixError> {
        let _write_guard = self.lock_collaboration_writes().await;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let mut graph = CommitGraphContext::new().reader(read);
        let commits = graph.all_nodes().await?;
        let mut writes = adapter.new_write_set();
        for commit in commits {
            stage_sync_hidden_commit_marker(&mut writes, commit.commit_id);
        }
        if !writes.is_empty() {
            adapter
                .commit_write_set(
                    writes,
                    StorageWriteOptions {
                        await_durable: true,
                        ..StorageWriteOptions::default()
                    },
                )
                .await?;
        }
        Ok(())
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
                        bootstrap_cleanup_pending: false,
                        bootstrap_cleanup_commit_id: None,
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
            long_poll: require_certified_bootstrap,
            scope_state: self.sync_mode_state(),
        };
        for scope in &client.state.hydrated_scopes {
            client.scope_state.mark_scope_hydrated_for_branch(
                &client.state.branch_id,
                scope,
                client.scope_state.scope_generation(),
            );
        }
        // Pending packs can survive a process restart before their semantic
        // scope was marked hydrated. Reconstruct those demands from the
        // durable payload so the next online flush imports skipped canonical
        // history before admitting the queue. Scope readiness itself remains
        // separate: only the hydration pass may mark these keys complete.
        let pending_scopes = client
            .state
            .pending
            .iter()
            .flat_map(|pack| pack.rows.iter().map(|row| row.schema_key.as_str()))
            .collect::<BTreeSet<_>>();
        for scope in pending_scopes {
            client
                .scope_state
                .register_explicit_scopes_for_branch(&client.state.branch_id, &[scope]);
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
                global: false,
                untracked: false,
            });
        }

        Ok(SyncTransactionPack {
            operation_id: operation_id.into(),
            branch_id,
            base_server_commit_id: base_server_commit_id.into(),
            local_commit_id: to_local_commit_id.to_owned(),
            parent_commit_ids: Vec::new(),
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
        self.apply_sync_transaction_pack_inner(pack, None, None, None, None)
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
        self.apply_sync_canonical_event_with_seed(event, remote_id, scopes, None)
            .await
    }

    async fn apply_sync_canonical_event_with_seed(
        &self,
        event: &SyncCanonicalEvent,
        remote_id: &str,
        scopes: &[String],
        synthetic_seed_commit_id: Option<&str>,
    ) -> Result<(), LixError> {
        let _apply_guard = self.sync_mode_state().lock_apply_gate().await;
        validate_sync_canonical_event_identity(event)?;
        validate_sync_transaction_pack(&event.pack)?;
        let commit_pack = event.commit_pack();
        let pack_fingerprint = if commit_pack.pack_fingerprint.is_empty() {
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
        // The transport keeps the unfiltered pack fingerprint on scoped
        // events. When the filtered payload still hashes to that fingerprint,
        // the request received the complete canonical commit (the normal
        // commit-level projection path), not a row/file subset. Record the
        // aggregate receipt as well as the requested scope so a later scope
        // query reuses the same canonical node instead of manufacturing a
        // projection child commit.
        let complete_pack_projection = !event.pack_fingerprint.is_empty()
            && sync_pack_fingerprint(&event.pack)
                .ok()
                .is_some_and(|fingerprint| fingerprint == pack_fingerprint);
        let mut coverage_scopes = marker_scopes.clone();
        if complete_pack_projection && !coverage_scopes.iter().any(|scope| scope == FULL_SYNC_SCOPE)
        {
            coverage_scopes.push(FULL_SYNC_SCOPE.to_owned());
        }
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

        // A lazy scope may be hydrated after another scope has already
        // materialized this canonical commit. Replaying that event with the
        // canonical label would attach an ancestor-sized ID to a new child
        // and create a local parent cycle. Preserve canonical identity only
        // for the first local materialization; later projections get an
        // ordinary local commit while their durable marker still records the
        // server identity.
        let canonical_commit_id =
            CommitId::parse_lix(&commit_pack.canonical_commit_id, "sync canonical commit_id")?;
        let (canonical_already_local, canonical_parent_commit_ids) = {
            let graph_read = adapter.begin_read(StorageReadOptions::default()).await?;
            let mut graph = CommitGraphContext::new().reader(graph_read);
            let canonical_already_local = graph.load_node(&canonical_commit_id).await?.is_some();
            let synthetic_seed_commit_id = synthetic_seed_commit_id
                .map(|value| CommitId::parse_lix(value, "sync synthetic seed commit"))
                .transpose()?;
            let parent_ids = if commit_pack.parent_commit_ids.is_empty() {
                // An explicit root must stay a root. Passing `None` here
                // would make the ordinary transaction path infer the local
                // branch head and invent a parent that is not in the
                // canonical server topology.
                Some(Vec::new())
            } else {
                let mut available = true;
                for parent in &commit_pack.parent_commit_ids {
                    let parent = CommitId::parse_lix(parent, "sync canonical parent_commit_id")?;
                    if synthetic_seed_commit_id == Some(parent) {
                        available = false;
                        break;
                    }
                    if graph.load_node(&parent).await?.is_none() {
                        available = false;
                        break;
                    }
                }
                if available {
                    Some(commit_pack.parent_commit_ids.clone())
                } else {
                    // A bootstrap stream may name an engine-only root that
                    // is intentionally absent from the public commit graph.
                    // Preserve the canonical child identity without
                    // attaching it to the receiving replica's synthetic
                    // seed. A later topology backfill can add a real parent
                    // only when that parent is materialized canonically.
                    Some(Vec::new())
                }
            };
            (canonical_already_local, parent_ids)
        };
        let replay_commit_id =
            (!canonical_already_local).then_some(event.canonical_commit_id.as_str());

        // A topology/backfill pass (or an earlier full-history demand) may
        // already have materialized every row in this event while the
        // requested narrow scope has no marker yet. In that case only record
        // the new scope receipt; replaying its filtered rows would create a
        // local child commit and move the branch head away from the canonical
        // commit, which breaks history/merge identity. A different semantic
        // scope without this full marker still needs its projected rows below.
        // Only inspect the full marker when the canonical graph node is
        // present; this keeps the common first-materialization path to one
        // marker scan per requested scope.
        let full_event_already_applied = if canonical_already_local {
            let full_marker = if coverage_scopes.iter().any(|scope| scope == FULL_SYNC_SCOPE) {
                previous
                    .get(FULL_SYNC_SCOPE)
                    .and_then(|marker| marker.as_ref().map(|(_, marker)| marker.clone()))
            } else {
                load_sync_applied_marker(&read, remote_id, &event.pack.branch_id, FULL_SYNC_SCOPE)
                    .await?
                    .map(|(_, marker)| marker)
            };
            full_marker
                .as_ref()
                .is_some_and(|marker| marker_covers_event(marker, event, &pack_fingerprint))
        } else {
            false
        };
        // A file-scope marker is an aggregate receipt for the plugin rows
        // rendered from that file's bytes. Treat it as covering a later
        // semantic demand too; otherwise querying the plugin table after a
        // file query would replay identical rows in a local child commit.
        let file_event_already_applied = if canonical_already_local {
            let file_marker = if coverage_scopes.iter().any(|scope| scope == "lix_file") {
                previous
                    .get("lix_file")
                    .and_then(|marker| marker.as_ref().map(|(_, marker)| marker.clone()))
            } else {
                load_sync_applied_marker(&read, remote_id, &event.pack.branch_id, "lix_file")
                    .await?
                    .map(|(_, marker)| marker)
            };
            file_marker
                .as_ref()
                .is_some_and(|marker| marker_covers_event(marker, event, &pack_fingerprint))
        } else {
            false
        };
        // A full row/topology marker deliberately does not cover the lazy
        // file-byte projection. If a later query asks for `lix_file`, it must
        // still materialize the bytes even though the canonical commit and
        // its descriptor/blob rows are already local.
        let event_projection_already_applied = if missing.iter().any(|scope| scope == "lix_file") {
            file_event_already_applied
        } else {
            full_event_already_applied || file_event_already_applied
        };

        // Apply the explicit download-side projection. Admission metadata is
        // retained only on the event wrapper for pending-operation matching;
        // the payload being staged is the canonical commit pack.
        let mut pack = event.pack.clone();
        pack.branch_id = commit_pack.branch_id.clone();
        pack.parent_commit_ids = commit_pack.parent_commit_ids.clone();
        pack.rows = commit_pack.rows.clone();
        pack.files = commit_pack.files.clone();
        // Branch refs are moving control pointers, not semantic commit
        // members. For ordinary application-branch replay the authoritative
        // catalog pass owns them, so omit the raw untracked row. The one
        // exception is the global control projection: its canonical event
        // already carries the branch catalog and its refs must be replayed
        // together, otherwise reconciliation would manufacture a local
        // branch-management commit merely to create a missing ref.
        let replay_global_control_refs = event.pack.branch_id == GLOBAL_BRANCH_ID
            && scopes.iter().any(|scope| scope == CONTROL_SYNC_SCOPE);
        if !replay_global_control_refs {
            pack.rows.retain(|row| row.schema_key != "lix_branch_ref");
        }
        // A filesystem clone may already contain the canonical commit and
        // its registered plugin schemas, but not the sync scope receipt (the
        // common case when a repository is copied before opening in sync
        // mode). Re-running the registration row would ask the active plugin
        // to migrate/delete its owned schema and correctly fail closed. The
        // canonical graph plus the existing registration is already the
        // materialized value; acknowledge this control row without invoking
        // plugin lifecycle code again. New replicas still replay it because
        // `canonical_already_local` is false on first materialization.
        if canonical_already_local {
            pack.rows
                .retain(|row| row.schema_key != "lix_registered_schema");
        }
        // A file-view demand is the one scoped projection that must not make
        // a second tracked commit: raw bytes are retained in the durable
        // projection lane while the canonical descriptor/row commit already
        // exists. Other scopes are different: a later semantic demand may
        // carry rows from the same canonical event that the first scope did
        // not materialize. Those rows must be applied in a normal local child
        // commit rather than discarded, or a query for the second relation
        // would permanently return an empty result.
        // A pull page can carry several requested scopes at once. Filter the
        // already-projected event again to only the scopes whose markers are
        // missing. Otherwise a row covered by an existing marker (for
        // example `sync_mode_row`) is replayed when an unrelated scope (such
        // as `lix_registered_schema`) is acknowledged, manufacturing a local
        // child commit and moving the branch head away from the canonical
        // event.
        if !missing.iter().any(|scope| scope == FULL_SYNC_SCOPE) {
            let missing_scope = SyncFilterScope::new(&missing);
            filter_sync_pack(&mut pack, &missing_scope);
        }
        // A copied filesystem replica can already contain a plugin's
        // descriptor/blob identity rows while the bootstrap event still
        // carries those identities through more than one projection lane.
        // Collapse duplicate row identities before lowering to the ordinary
        // transaction planner; the canonical payload is a replacement, so
        // retaining the first occurrence avoids a staged primary-key
        // collision without changing the resulting state.
        let mut seen_rows = HashSet::new();
        pack.rows.retain(|row| {
            seen_rows.insert((
                row.schema_key.clone(),
                row.file_id.clone(),
                row.row_pk.to_string(),
            ))
        });
        // A late file-only pull can arrive after the canonical commit and its
        // descriptor/blob rows are already local. Persist the bytes through
        // the projection lane instead of creating a second tracked commit.
        // A fresh event still takes the normal canonical row path so its file
        // identity is materialized before the projection is acknowledged.
        // A fresh canonical event must retain its file descriptor/blob rows so
        // the `lix_file` provider has an identity to enumerate. The raw-byte
        // projection is late-only once another scope has already materialized
        // the canonical graph node; otherwise storing bytes without rows
        // leaves the file overlay unreachable by SQL.
        let file_projection_candidate = canonical_already_local
            && missing.iter().any(|scope| scope == "lix_file")
            && !pack.files.is_empty()
            && pack.rows.iter().all(|row| {
                is_file_projection_sync_schema(&row.schema_key)
                    || is_sync_control_schema(&row.schema_key)
            });
        // A topology-only pull may have installed the canonical graph node
        // without its file descriptors. Check the local descriptor lane
        // before choosing the byte-only path; otherwise the projection is
        // durable but has no identity for `lix_file` to enumerate.
        let file_projection_rows_missing = if file_projection_candidate {
            let mut missing_descriptor = false;
            for file in &pack.files {
                let descriptor = self
                    .execute(
                        "SELECT id FROM lix_file WHERE id = $1",
                        &[Value::Text(file.file_id.clone())],
                    )
                    .await?;
                if descriptor.rows().is_empty() {
                    missing_descriptor = true;
                    break;
                }
            }
            missing_descriptor
        } else {
            false
        };
        let file_projection_only = file_projection_candidate && !file_projection_rows_missing;
        let file_projection_with_rows = file_projection_candidate && file_projection_rows_missing;
        if canonical_already_local {
            // The canonical identity is already present in the local graph.
            // Preserve newly requested semantic rows and stage them as a
            // local child commit; only the late file lane drops descriptor
            // rows because its bytes are staged as a projection without
            // moving the tracked branch head.
            if event_projection_already_applied || file_projection_only {
                pack.rows.clear();
            }
            if !file_projection_only && !file_projection_with_rows {
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
        if file_projection_only || file_projection_with_rows {
            self.apply_sync_transaction_pack_inner_mode(
                &pack,
                None,
                Some(&markers),
                replay_commit_id,
                None,
                false,
                false,
                true,
                file_projection_with_rows,
            )
            .await
        } else {
            self.apply_sync_transaction_pack_inner(
                &pack,
                None,
                Some(&markers),
                replay_commit_id,
                (!canonical_already_local)
                    .then_some(canonical_parent_commit_ids)
                    .flatten()
                    .as_deref(),
            )
            .await
        }
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
            let rows = sync_write_batch(pack, &branch_id, false)?;
            if rows.is_empty() && pack.files.is_empty() {
                continue;
            }
            transaction
                .stage_sync_pack(rows, pack.files.clone())
                .await?;
        }
        transaction.commit().await
    }

    /// Materializes only the graph identity of a canonical event whose
    /// requested projection contains no semantic rows.  This is used for
    /// control-plane commits during full-history hydration: the event must be
    /// visible to `lix_commit`/`lix_commit_edge`, but downloading or inventing
    /// a user row would violate lazy scope ownership.
    async fn apply_sync_topology_event(&self, event: &SyncCanonicalEvent) -> Result<(), LixError> {
        self.apply_sync_topology_event_with_seed(event, None).await
    }

    async fn apply_sync_topology_event_with_seed(
        &self,
        event: &SyncCanonicalEvent,
        synthetic_seed_commit_id: Option<&str>,
    ) -> Result<(), LixError> {
        let _apply_guard = self.sync_mode_state().lock_apply_gate().await;
        validate_sync_canonical_event_identity(event)?;
        validate_sync_topology_event_pack(&event.pack)?;
        let branch_id = self.active_branch_id().await?;
        require_branch(&branch_id, &event.pack.branch_id)?;

        // The topology worker and a foreground full-history demand can
        // discover the same payload-less event concurrently.  The caller's
        // graph check happens before this apply gate, so it is not sufficient
        // to prevent both transactions from staging the same canonical ID.
        // Recheck while holding the gate and make the topology application
        // idempotent, just like the semantic canonical-event path.
        let canonical_commit_id = CommitId::parse_lix(
            &event.canonical_commit_id,
            "sync topology canonical commit_id",
        )?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let mut graph = CommitGraphContext::new().reader(read);
        if graph.load_node(&canonical_commit_id).await?.is_some() {
            return Ok(());
        }

        let parent_ids = event
            .parent_commit_ids
            .iter()
            .map(|parent| CommitId::parse_lix(parent, "sync topology parent_commit_id"))
            .collect::<Result<Vec<_>, _>>()?;
        let canonical_parents = if !parent_ids.is_empty() {
            let synthetic_seed_commit_id = synthetic_seed_commit_id
                .map(|value| CommitId::parse_lix(value, "sync synthetic seed commit"))
                .transpose()?;
            let adapter = self.storage_adapter();
            let read = adapter.begin_read(StorageReadOptions::default()).await?;
            let mut graph = CommitGraphContext::new().reader(read);
            let nodes = graph.load_nodes(&parent_ids).await?;
            if nodes.iter().all(|(_, node)| node.is_some())
                && synthetic_seed_commit_id.is_none_or(|seed| !parent_ids.contains(&seed))
            {
                event.parent_commit_ids.as_slice()
            } else {
                // A legacy/pre-sync bootstrap can reference a parent for
                // which no canonical event exists on any stream. Match the
                // normal canonical replay fallback: retain the child identity
                // and attach it to the receiving branch's local head rather
                // than failing the whole lazy history request. Whenever the
                // parent stream is available, the exact edge is preserved.
                &[]
            }
        } else {
            &[]
        };

        let mut transaction = self.begin_transaction().await?;
        require_branch(transaction.active_branch_id()?, &event.pack.branch_id)?;
        transaction.stage_sync_topology_commit(&event.canonical_commit_id, canonical_parents)?;
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
        self.apply_sync_transaction_pack_inner(
            pack,
            Some(authoritative_branch_id),
            None,
            None,
            None,
        )
        .await
    }

    pub(crate) async fn admit_sync_transaction_pack(
        &self,
        pack: &SyncTransactionPack,
        idempotency: &ExecuteIdempotency,
    ) -> Result<SyncAdmission, LixError> {
        validate_sync_transaction_pack(pack)?;
        require_branch(&self.active_branch_id().await?, &pack.branch_id)?;
        let canonical_base_commit_id = self
            .require_sync_base_on_active_branch(&pack.base_server_commit_id)
            .await?;
        let canonical_pack = canonicalize_sync_control_rows(pack, &canonical_base_commit_id)?;
        if !canonical_pack.parent_commit_ids.is_empty() {
            let adapter = self.storage_adapter();
            let read = adapter.begin_read(StorageReadOptions::default()).await?;
            let mut graph = CommitGraphContext::new().reader(read);
            for parent in &canonical_pack.parent_commit_ids {
                let parent = CommitId::parse_lix(parent, "sync admission parent_commit_id")?;
                if graph.load_node(&parent).await?.is_none() {
                    return Err(LixError::new(
                        LixError::CODE_COMMIT_NOT_FOUND,
                        format!("sync admission parent commit '{parent}' is not materialized"),
                    ));
                }
            }
        }
        let admission_plan = self.sync_admission_plan(&pack.branch_id).await?;
        let mut transaction = self.begin_transaction().await?;
        require_branch(transaction.active_branch_id()?, &pack.branch_id)?;
        let transaction_branch_id = transaction.active_branch_id()?.to_owned();
        let rows = sync_write_batch(&canonical_pack, &transaction_branch_id, false)?;
        if !rows.is_empty() || !canonical_pack.files.is_empty() {
            transaction
                .stage_sync_pack(rows, canonical_pack.files.clone())
                .await?;
        }
        let admission = transaction
            .stage_sync_admission_receipt(idempotency, &canonical_pack, admission_plan)
            .await?;
        transaction.commit().await?;
        Ok(admission)
    }

    /// Materializes the first authoritative sync events for a branch that was
    /// created before the sync protocol was enabled.
    ///
    /// Ordinary server commits publish an event as part of the same storage
    /// transaction as their rows. Older repositories have no such event
    /// history, so a fresh replica would otherwise see a head cursor of zero
    /// and have no safe way to obtain either the current rows or the commit
    /// graph behind them. Bootstrap therefore publishes the reachable commit
    /// DAG oldest-first. Each event carries that commit's authenticated row
    /// delta when one exists; an empty delta is still a topology event. The
    /// first event carries schema/plugin bootstrap bytes as a bounded
    /// exception, while ordinary project files remain a lazy projection.
    async fn ensure_sync_bootstrap_event(&self, branch_id: &str) -> Result<(), LixError> {
        validate_sync_branch_id(branch_id)?;
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        if load_sync_head(&read, branch_id).await?.is_some() {
            return Ok(());
        }
        let current_commit_id = self
            .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
            .await?
            .rows()[0]
            .get::<String>("commit_id")?;
        let current_commit =
            CommitId::parse_lix(&current_commit_id, "sync bootstrap active branch commit_id")?;

        // Reachability is topology-only, so this read does not materialize
        // commit payloads or blobs. Generation orders the events so every
        // canonical parent is available before its child is replayed.
        let reachable = {
            let mut graph = CommitGraphContext::new().reader(&read);
            graph.reachable_nodes(&current_commit).await?
        };
        let mut nodes = reachable
            .iter()
            .map(|reachable| reachable.commit.clone())
            .collect::<Vec<_>>();
        nodes.sort_by(|left, right| {
            left.generation
                .cmp(&right.generation)
                .then_with(|| left.commit_id.cmp(&right.commit_id))
        });

        // Schema definitions and plugin archives are the only first-contact
        // projections that must precede historical row deltas. Attach them
        // to the first event so every later delta can be validated by the
        // local catalog/plugin runtime. Ordinary project files remain lazy.
        let bootstrap_pack = self
            .build_sync_bootstrap_pack(branch_id, &current_commit_id)
            .await?;
        let bootstrap_schema_rows = bootstrap_pack
            .rows
            .iter()
            .filter(|row| row.schema_key == "lix_registered_schema")
            .cloned()
            .collect::<Vec<_>>();
        let bootstrap_control_rows = if branch_id == GLOBAL_BRANCH_ID {
            bootstrap_pack
                .rows
                .iter()
                .filter(|row| is_sync_control_schema(&row.schema_key))
                .cloned()
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let bootstrap_plugin_files = bootstrap_pack
            .files
            .iter()
            .filter(|file| is_plugin_archive_sync_file(file))
            .cloned()
            .collect::<Vec<_>>();
        let bootstrap_project_files = bootstrap_pack
            .files
            .iter()
            .filter(|file| !is_plugin_archive_sync_file(file))
            .cloned()
            .collect::<Vec<_>>();

        let mut events = Vec::with_capacity(nodes.len());
        for (index, node) in nodes.iter().enumerate() {
            let commit_id = node.commit_id.to_string();
            let parent_commit_ids = node
                .parent_commit_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            let members = load_commit_delta_members_with_payloads(&read, node.commit_id).await?;
            let payloads = crate::changelog::materialize_known_change_payloads_in_order(
                &read,
                members.iter().map(|member| member.change.clone()),
                ChangeRecordProjection::full(),
            )
            .await?;
            // A pristine local engine already owns its private `.lix`
            // directory tree (and may generate different IDs for it). Those
            // implementation descriptors are not repository data and must not
            // enter the canonical bootstrap pack; otherwise the local path
            // index rejects the authoritative duplicate namespace. Keep
            // project descriptors/blob refs so plugin ownership can still be
            // validated before semantic rows are replayed.
            let mut directory_records = BTreeMap::<String, (Option<String>, String)>::new();
            let mut file_records = Vec::<(String, Option<String>)>::new();
            for (member, (_, payload)) in members.iter().zip(payloads.iter()) {
                let Some(snapshot) = payload.snapshot_content.as_deref() else {
                    continue;
                };
                let Ok(snapshot) = serde_json::from_str::<serde_json::Value>(snapshot) else {
                    continue;
                };
                match member.key.schema_key.as_str() {
                    "lix_directory_descriptor" => {
                        let Some(id) = snapshot.get("id").and_then(serde_json::Value::as_str)
                        else {
                            continue;
                        };
                        let parent_id = snapshot
                            .get("parent_id")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned);
                        let name = snapshot
                            .get("name")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default()
                            .to_owned();
                        directory_records.insert(id.to_owned(), (parent_id, name));
                    }
                    "lix_file_descriptor" => {
                        let Some(id) = snapshot.get("id").and_then(serde_json::Value::as_str)
                        else {
                            continue;
                        };
                        let directory_id = snapshot
                            .get("directory_id")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned);
                        file_records.push((id.to_owned(), directory_id));
                    }
                    _ => {}
                }
            }
            let mut internal_directory_ids = BTreeSet::new();
            loop {
                let before = internal_directory_ids.len();
                for (id, (parent_id, name)) in &directory_records {
                    if name == ".lix"
                        || parent_id
                            .as_deref()
                            .is_some_and(|parent| internal_directory_ids.contains(parent))
                    {
                        internal_directory_ids.insert(id.clone());
                    }
                }
                if internal_directory_ids.len() == before {
                    break;
                }
            }
            let internal_file_ids = file_records
                .into_iter()
                .filter_map(|(id, directory_id)| {
                    directory_id
                        .as_deref()
                        .is_some_and(|directory| internal_directory_ids.contains(directory))
                        .then_some(id)
                })
                .collect::<BTreeSet<_>>();
            let mut rows = Vec::with_capacity(members.len());
            for (member, (_, payload)) in members.into_iter().zip(payloads) {
                // Engine/control rows have branch-specific storage scopes and
                // are reconciled by the control lane. User/plugin rows are the
                // canonical row-first payload for this historical commit.
                let is_filesystem_identity = matches!(
                    member.key.schema_key.as_str(),
                    "lix_file_descriptor" | "lix_directory_descriptor" | "lix_binary_blob_ref"
                );
                let internal_filesystem_row = match member.key.schema_key.as_str() {
                    "lix_directory_descriptor" => payload
                        .snapshot_content
                        .as_deref()
                        .and_then(|snapshot| {
                            serde_json::from_str::<serde_json::Value>(snapshot).ok()
                        })
                        .and_then(|snapshot| {
                            snapshot
                                .get("id")
                                .and_then(serde_json::Value::as_str)
                                .map(|id| internal_directory_ids.contains(id))
                        })
                        .unwrap_or(false),
                    "lix_file_descriptor" | "lix_binary_blob_ref" => payload
                        .snapshot_content
                        .as_deref()
                        .and_then(|snapshot| {
                            serde_json::from_str::<serde_json::Value>(snapshot).ok()
                        })
                        .and_then(|snapshot| {
                            snapshot
                                .get("id")
                                .and_then(serde_json::Value::as_str)
                                .map(|id| internal_file_ids.contains(id))
                        })
                        .unwrap_or(false),
                    _ => false,
                };
                if (!is_filesystem_identity && member.key.schema_key.starts_with("lix_"))
                    || !ordinary_sync_schema(&member.key.schema_key)
                    || internal_filesystem_row
                {
                    continue;
                }
                let snapshot = payload
                    .snapshot_content
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|error| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("decode sync bootstrap snapshot: {error}"),
                        )
                    })?;
                let metadata = payload
                    .metadata
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|error| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("decode sync bootstrap metadata: {error}"),
                        )
                    })?;
                rows.push(SyncRowMutation {
                    schema_key: member.key.schema_key,
                    file_id: member.key.file_id,
                    row_pk: member.key.row_pk.as_json_array_value()?,
                    snapshot,
                    metadata,
                    global: false,
                    untracked: false,
                });
            }
            if index == 0 {
                rows.extend(bootstrap_schema_rows.iter().cloned());
            }
            // Control rows are a current global catalog projection. A
            // pre-sync repository may have created branches before sync was
            // enabled, so those descriptor/ref changes do not have ordinary
            // sync events. Attach the complete catalog to the final global
            // bootstrap event; applying it there preserves canonical graph
            // identity and avoids a local branch-management commit.
            if branch_id == GLOBAL_BRANCH_ID && index + 1 == nodes.len() {
                rows.extend(bootstrap_control_rows.iter().cloned());
            }
            // The first canonical event carries the bounded set of bootstrap
            // file projections (plugin archives plus files referenced by
            // plugin rows). Scope filtering decides whether those bytes are
            // sent: row/schema pulls retain only archives, while a file pull
            // can request the component bytes without downloading unrelated
            // project files. Storing the complete bootstrap companion here is
            // what lets a delayed first pull hydrate a file view correctly.
            // Plugin archives must arrive before the first semantic row can
            // be validated. Ordinary project bytes are a separate projection
            // lane and belong on the final bootstrap event, after all
            // historical descriptor/blob deltas have been replayed. If both
            // lanes collapse to one event, combine them without duplicating
            // archive payloads.
            let files = if index == 0 && index + 1 == nodes.len() {
                bootstrap_pack.files.clone()
            } else if index == 0 {
                bootstrap_plugin_files.clone()
            } else if index + 1 == nodes.len() {
                bootstrap_project_files.clone()
            } else {
                Vec::new()
            };
            let base_server_commit_id = parent_commit_ids
                .first()
                .cloned()
                .unwrap_or_else(|| commit_id.clone());
            let pack = SyncTransactionPack {
                operation_id: format!("bootstrap:{branch_id}:{commit_id}"),
                branch_id: branch_id.to_owned(),
                base_server_commit_id,
                local_commit_id: commit_id.clone(),
                parent_commit_ids: parent_commit_ids.clone(),
                rows,
                files,
            };
            validate_sync_topology_event_pack(&pack)?;
            let event = SyncCanonicalEvent {
                cursor: u64::try_from(index + 1).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "sync bootstrap cursor overflow",
                    )
                })?,
                canonical_commit_id: commit_id,
                parent_commit_ids,
                pack_fingerprint: sync_pack_fingerprint(&pack)?,
                pack,
            };
            validate_sync_canonical_event_identity(&event)?;
            events.push(event);
        }
        let final_event = events.last().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync bootstrap graph has no reachable commits",
            )
        })?;
        let mut writes = adapter.new_write_set();
        let mut preconditions = Vec::new();
        for event in &events {
            stage_sync_event_record(&mut writes, &mut preconditions, event)?;
        }
        let head_key = sync_head_key(branch_id);
        let head_value = serde_json::to_vec(&SyncHead {
            cursor: final_event.cursor,
            commit_id: final_event.canonical_commit_id.clone(),
        })
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("encode sync bootstrap branch head: {error}"),
            )
        })?;
        writes.put(SYNC_HEAD_SPACE, head_key.clone(), head_value);
        preconditions.push(StoragePrecondition::KeyAbsent {
            space: SYNC_HEAD_SPACE,
            key: head_key,
        });
        match adapter
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    await_durable: true,
                    preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
        {
            Ok(_) => Ok(()),
            // Another concurrent pull won the key-absent race. Re-read the
            // head on the next pull instead of turning a harmless bootstrap
            // race into a user-visible synchronization failure.
            Err(error) => {
                let error: LixError = error.into();
                if error.code == LixError::CODE_TRANSACTION_CONFLICT {
                    Ok(())
                } else {
                    Err(error)
                }
            }
        }
    }

    /// Builds a current-state row snapshot for first-contact bootstrap. The
    /// semantic rows are canonical; plugin archives are included as the one
    /// exceptional file projection needed to install a plugin before its rows
    /// can be queried. Ordinary project files remain lazy and are fetched by
    /// their normal file scope.
    async fn build_sync_bootstrap_pack(
        &self,
        branch_id: &str,
        commit_id: &str,
    ) -> Result<SyncTransactionPack, LixError> {
        let adapter = self.storage_adapter();
        let read = adapter.begin_read(StorageReadOptions::default()).await?;
        let mut tracked = TrackedStateContext::new().reader(read);
        let batch = tracked
            .scan_batch_at_commit(commit_id, &TrackedStateScanRequest::default())
            .await?;
        let mut rows_by_identity =
            BTreeMap::<(String, Option<String>, String), SyncRowMutation>::new();
        append_sync_bootstrap_rows(&batch, &mut rows_by_identity, branch_id == GLOBAL_BRANCH_ID)?;

        // A branch root can omit global catalog rows from its tracked-state
        // root. Read the global branch as well, then let the identity map keep
        // one copy of any inherited row. This is what makes a fresh replica
        // able to resolve a plugin schema before the first application query.
        if branch_id != GLOBAL_BRANCH_ID {
            let global = self
                .open_internal_session_suppressed(
                    GLOBAL_BRANCH_ID.to_owned(),
                    self.active_account_id().to_owned(),
                )
                .await?;
            let global_commit_id = global
                .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
                .await?
                .rows()[0]
                .get::<String>("commit_id")?;
            let global_adapter = global.storage_adapter();
            let global_read = global_adapter
                .begin_read(StorageReadOptions::default())
                .await?;
            let mut global_tracked = TrackedStateContext::new().reader(global_read);
            let global_batch = global_tracked
                .scan_batch_at_commit(&global_commit_id, &TrackedStateScanRequest::default())
                .await?;
            // A branch bootstrap carries semantic/plugin rows from the
            // repository-global lane, but not the global branch catalog
            // itself. The global stream gets the control rows when it is
            // bootstrapped directly below.
            append_sync_bootstrap_rows(&global_batch, &mut rows_by_identity, false)?;
            global.close().await?;
        }

        // Branch refs are direct control facts rather than tracked-state rows,
        // so they do not appear in `scan_batch_at_commit`. A first-contact
        // global bootstrap must nevertheless carry the complete catalog
        // heads; otherwise replaying the descriptor snapshot leaves branches
        // without refs and the public `lix_branch` view silently drops them.
        // Keep these rows on the global event only. Their target commits are
        // immutable canonical identities and are validated by the normal sync
        // pack path before the control publication is committed.
        if branch_id == GLOBAL_BRANCH_ID {
            let branch_refs = self
                .execute("SELECT id, commit_id FROM lix_branch_ref ORDER BY id", &[])
                .await?;
            for row in branch_refs.rows() {
                let id = row.get::<String>("id")?;
                // The global branch's own moving head is published by the
                // canonical topology commit that owns this event. Replaying
                // a duplicate ref here would point at the final global head
                // before that later event has been materialized and violate
                // the branch-ref foreign key.
                if id == GLOBAL_BRANCH_ID {
                    continue;
                }
                let commit_id = CommitId::parse_lix(
                    &row.get::<String>("commit_id")?,
                    "sync bootstrap branch-ref commit_id",
                )?;
                let row_pk = serde_json::Value::Array(vec![serde_json::Value::String(id.clone())]);
                rows_by_identity.insert(
                    ("lix_branch_ref".to_owned(), None, row_pk.to_string()),
                    SyncRowMutation {
                        schema_key: "lix_branch_ref".to_owned(),
                        file_id: None,
                        row_pk,
                        snapshot: Some(serde_json::json!({
                            "id": id,
                            "commit_id": commit_id.to_string(),
                        })),
                        metadata: None,
                        global: true,
                        untracked: true,
                    },
                );
            }
        }

        // A plugin row is canonical, but its file ownership certificate still
        // needs the corresponding component bytes when a completely fresh
        // replica receives a current-state bootstrap. Transfer only files
        // referenced by plugin semantic rows (plus plugin archives); ordinary
        // project files remain lazy and are not part of first contact.
        let plugin_component_file_ids = rows_by_identity
            .values()
            .filter(|row| !row.schema_key.starts_with("lix_") && row.file_id.is_some())
            .filter_map(|row| row.file_id.clone())
            .collect::<BTreeSet<_>>();
        let mut file_query =
            "SELECT id, path, content FROM lix_file WHERE path LIKE '/.lix/plugins/%'".to_owned();
        let mut file_args = Vec::with_capacity(plugin_component_file_ids.len());
        for (index, file_id) in plugin_component_file_ids.iter().enumerate() {
            let parameter = index + 1;
            file_query.push_str(&format!(" OR id = ${parameter}"));
            file_args.push(Value::Text(file_id.clone()));
        }
        let mut files = Vec::new();
        let file_rows = self.execute(&file_query, &file_args).await?;
        for row in file_rows.rows() {
            let file_id = row.get::<String>("id")?;
            let path = row.get::<String>("path")?;
            let content = row.get::<Vec<u8>>("content")?;
            let filename = path
                .rsplit('/')
                .next()
                .filter(|name| !name.is_empty())
                .map(str::to_owned);
            files.push(SyncFileMutation {
                file_id,
                path: Some(path),
                filename,
                global: false,
                untracked: false,
                content,
            });
        }

        Ok(SyncTransactionPack {
            operation_id: String::new(),
            branch_id: branch_id.to_owned(),
            base_server_commit_id: commit_id.to_owned(),
            local_commit_id: commit_id.to_owned(),
            parent_commit_ids: Vec::new(),
            rows: rows_by_identity.into_values().collect(),
            files,
        })
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
        if stored_head.is_none() {
            drop(read);
            self.ensure_sync_bootstrap_event(branch_id).await?;
        }
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
            validate_sync_topology_event_pack(&event.pack)?;
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
        if stored.is_none() {
            // The protocol may receive a local proposal before any replica has
            // performed its first pull. Publish the pre-sync commit DAG first
            // so admitting this proposal cannot create cursor 1 and strand
            // older authoritative history outside the canonical stream.
            drop(read);
            self.ensure_sync_bootstrap_event(branch_id).await?;
            let read = adapter.begin_read(StorageReadOptions::default()).await?;
            let stored = load_sync_head(&read, branch_id).await?;
            let cursor = stored
                .as_ref()
                .map_or(0, |(_, head)| head.cursor)
                .checked_add(1)
                .ok_or_else(|| {
                    LixError::new(LixError::CODE_INTERNAL_ERROR, "sync cursor overflow")
                })?;
            return Ok(SyncAdmissionPlan {
                cursor,
                previous_head_value: stored.map(|(value, _)| value),
            });
        }
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
        let mut graph = CommitGraphContext::new().reader(read);
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
        canonical_parent_commit_ids: Option<&[String]>,
    ) -> Result<(), LixError> {
        // The canonical event was validated before projection. A projection
        // can legitimately become empty when an overlapping lazy scope has
        // already applied every row in the event; the applied marker still
        // needs its own atomic commit to advance that scope.
        if applied_markers.is_none() {
            validate_sync_transaction_pack(pack)?;
        }
        // A server admission must always use the ordinary renderer-backed
        // path: its derived file projection is authoritative. A canonical
        // replica pull normally uses that same path too, but a row-only lazy
        // projection can be missing the source bytes (or a local plugin
        // observation) that the renderer needs. Retry that one narrow case
        // in a fresh transaction with the already-validated canonical rows.
        // This keeps the public API identical while allowing a fresh replica
        // to hydrate semantic rows without downloading file bytes.
        let canonical_replica_pull = authoritative_branch_id.is_none()
            && (canonical_commit_id.is_some() || applied_markers.is_some());
        if canonical_replica_pull {
            match Box::pin(self.apply_sync_transaction_pack_inner_mode(
                pack,
                authoritative_branch_id,
                applied_markers,
                canonical_commit_id,
                canonical_parent_commit_ids,
                false,
                true,
                false,
                false,
            ))
            .await
            {
                Ok(()) => Ok(()),
                Err(error) if should_retry_canonical_row_lane(pack, &error) => {
                    Box::pin(self.apply_sync_transaction_pack_inner_mode(
                        pack,
                        authoritative_branch_id,
                        applied_markers,
                        canonical_commit_id,
                        canonical_parent_commit_ids,
                        true,
                        false,
                        false,
                        false,
                    ))
                    .await
                }
                Err(error) => Err(error),
            }
        } else {
            Box::pin(self.apply_sync_transaction_pack_inner_mode(
                pack,
                authoritative_branch_id,
                applied_markers,
                canonical_commit_id,
                canonical_parent_commit_ids,
                false,
                false,
                false,
                false,
            ))
            .await
        }
    }

    async fn apply_sync_transaction_pack_inner_mode(
        &self,
        pack: &SyncTransactionPack,
        authoritative_branch_id: Option<&str>,
        applied_markers: Option<&[(SyncAppliedEventMarker, Option<Vec<u8>>)]>,
        canonical_commit_id: Option<&str>,
        canonical_parent_commit_ids: Option<&[String]>,
        trusted_canonical_rows: bool,
        canonical_renderer_rows: bool,
        untracked_file_projection: bool,
        include_file_projection_rows: bool,
    ) -> Result<(), LixError> {
        let mut transaction = self.begin_transaction().await?;
        let branch_id = transaction.active_branch_id()?.to_owned();
        if authoritative_branch_id.is_none() {
            require_branch(&branch_id, &pack.branch_id)?;
        }
        // When a canonical file payload is present, descriptor/blob rows are
        // derived by the renderer from those bytes. Keeping the rows in the
        // wire pack remains useful for row-only hydration, but staging them
        // alongside the byte mutation would duplicate directory/file
        // identities. Row-only pulls still retain them because `files` is
        // empty in that lane.
        let rows = sync_write_batch(pack, &branch_id, include_file_projection_rows)?;
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
        if untracked_file_projection {
            if !rows.is_empty() && !include_file_projection_rows {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "late sync file projection unexpectedly carries semantic rows",
                ));
            }
            let remote_id = applied_markers
                .and_then(|markers| markers.first().map(|(marker, _)| marker.remote_id.as_str()))
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "late sync file projection is missing its replica identity",
                    )
                })?;
            if !rows.is_empty() {
                transaction.stage_sync_canonical_rows(rows).await?;
            }
            transaction
                .stage_sync_file_projection(remote_id, pack.files.clone())
                .await?;
        } else if trusted_canonical_rows {
            transaction
                .stage_sync_canonical_pack_with_commit_and_parents(
                    rows,
                    pack.files.clone(),
                    canonical_commit_id,
                    canonical_parent_commit_ids,
                )
                .await?;
        } else if canonical_renderer_rows {
            transaction
                .stage_sync_canonical_renderer_pack_with_commit_and_parents(
                    rows,
                    pack.files.clone(),
                    canonical_commit_id,
                    canonical_parent_commit_ids,
                )
                .await?;
        } else {
            transaction
                .stage_sync_pack_with_commit_and_parents(
                    rows,
                    pack.files.clone(),
                    canonical_commit_id,
                    canonical_parent_commit_ids,
                )
                .await?;
        }
        if let Some(markers) = applied_markers {
            transaction.stage_sync_applied_event_markers(markers)?;
        }
        transaction.commit().await
    }
}

/// Returns whether a canonical row-only projection can safely retry through
/// the trusted row lane. The server-authored pack has already passed the
/// structural and event-fingerprint validators; the fallback only bypasses a
/// local plugin renderer that cannot run without source/materialization state.
/// Ordinary schema, constraint, branch, and filesystem errors must remain
/// hard failures rather than being hidden by the trusted path.
fn should_retry_canonical_row_lane(pack: &SyncTransactionPack, error: &LixError) -> bool {
    if !pack.files.is_empty() || pack.rows.is_empty() {
        return false;
    }
    match error.code.as_str() {
        LixError::CODE_PLUGIN_UNAVAILABLE | LixError::CODE_PLUGIN_OBSERVATION_STALE => true,
        LixError::CODE_INTERNAL_ERROR => {
            error.message.contains("component materialization root")
                || error.message.contains("plugin observation")
                || error.message.contains("plugin source")
        }
        LixError::CODE_CONSTRAINT_VIOLATION => {
            (error.message.contains("owned component plugin file")
                && error
                    .message
                    .contains("must resolve to exactly one path in its own lane; found 0"))
                || (error.message.contains("registered schema '")
                    && error.message.contains("owned by active plugin"))
                || (error.message.contains("plugin-owned schema")
                    && error.message.contains("unowned file"))
        }
        // A filesystem clone can already contain the descriptor/blob identity
        // rows for a canonical plugin commit. The renderer-backed replay path
        // stages those identities once from the incoming pack and once from
        // its local file reconciliation, yielding a duplicate blob-ref row.
        // The trusted canonical lane keeps the authenticated rows but skips
        // local rendering; it is safe for this structural collision and lets
        // the scope marker advance so the application can issue its write.
        LixError::CODE_UNIQUE => {
            error.message.contains("duplicate staged rows")
                && error.message.contains("lix_binary_blob_ref")
        }
        _ => false,
    }
}

struct SyncFilterScope<'a> {
    keys: HashSet<&'a str>,
    wants_full_history: bool,
    topology_only: bool,
    wants_control_rows: bool,
    wants_file_views: bool,
    wants_plugin_archives: bool,
}

impl<'a> SyncFilterScope<'a> {
    fn new(schema_keys: &'a [String]) -> Self {
        let keys = schema_keys
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        let wants_full_history = keys.contains(FULL_SYNC_PULL_SCOPE);
        let topology_only = keys.contains(TOPOLOGY_SYNC_PULL_SCOPE);
        let wants_control_rows =
            keys.contains(CONTROL_SYNC_PULL_SCOPE) || keys.contains(CONTROL_SYNC_SCOPE);
        let wants_file_views = keys.contains("lix_file");
        // The registered-schema catalog is also the bootstrap point for
        // plugin archives. Keeping only archive payloads here lets a fresh
        // replica discover plugin-backed row tables without downloading
        // ordinary project files.
        let wants_plugin_archives = keys.contains("lix_registered_schema");
        Self {
            keys,
            wants_full_history,
            topology_only,
            wants_control_rows,
            wants_file_views,
            wants_plugin_archives,
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

/// Applies the lazy scope projection to a transaction pack without changing
/// its operation/branch/cursor identity.
///
/// A canonical commit is an atomic graph node. Once a requested scope touches
/// a commit, the complete pack must be retained so that later scope hydration
/// cannot create a second, divergent local projection of the same commit. The
/// projection therefore skips unrelated commits, but never slices a matching
/// commit row-by-row.
fn filter_sync_pack(pack: &mut SyncTransactionPack, schema_scope: &SyncFilterScope<'_>) {
    if schema_scope.topology_only {
        pack.rows.clear();
        pack.files.clear();
        return;
    }
    if schema_scope.wants_control_rows {
        pack.rows
            .retain(|row| is_sync_control_schema(&row.schema_key));
        pack.files.clear();
        return;
    }
    if schema_scope.wants_full_history {
        // Full history is a row/topology demand, not a request for every
        // project blob. Plugin archives remain the bounded exception needed
        // to install a renderer before applying plugin-owned canonical rows.
        pack.files.retain(is_plugin_archive_sync_file);
        return;
    }
    // A file read is a demand for its plugin-owned semantic rows as well as
    // raw bytes. Those rows carry stable file IDs; keeping them here lets each
    // replica run its own plugin renderer instead of treating file bytes as
    // canonical state.
    let touches_scope = pack.rows.iter().any(|row| {
        schema_scope.keys.contains(row.schema_key.as_str())
            || (schema_scope.wants_file_views
                && row.file_id.is_some()
                && (!row.schema_key.starts_with("lix_")
                    || is_file_projection_sync_schema(&row.schema_key)))
    }) || (schema_scope.wants_file_views && !pack.files.is_empty())
        || (schema_scope.wants_plugin_archives
            && pack.files.iter().any(is_plugin_archive_sync_file));
    if !touches_scope {
        pack.rows.clear();
        pack.files.clear();
    } else if schema_scope.wants_file_views {
        // A pack carrying fresh bytes is served directly from the byte
        // projection. Do not replay semantic rows beside those bytes: the
        // renderer could interpret the transfer as a second source
        // transition and overwrite the requested file. Conversely, a later
        // semantic-only event (for example a markdown row edit) has no byte
        // payload; retain its semantic rows so the local plugin renderer can
        // advance the cached file view. File identity rows remain useful in
        // both cases.
        if !pack.files.is_empty() {
            pack.rows.retain(|row| {
                !is_sync_semantic_row(row) && row.schema_key != "lix_registered_schema"
            });
        } else {
            pack.rows
                .retain(|row| row.schema_key != "lix_registered_schema");
        }
    } else if !schema_scope.wants_file_views {
        // Semantic rows are canonical. Keep descriptor/blob-reference rows
        // with them so a fresh replica can establish file ownership without
        // downloading the source bytes. The file-view demand takes the
        // complementary projection above. Only the registered-schema
        // bootstrap scope is allowed to retain plugin archive bytes.
        if schema_scope.wants_plugin_archives {
            // The registered-schema bootstrap scope needs plugin archive
            // bytes to install the plugin that owns the requested table.
            pack.files.retain(is_plugin_archive_sync_file);
        } else {
            pack.files.clear();
            // Keep descriptor/blob-reference identities with semantic rows.
            // Plugin-owned rows carry a file_id and the local validator needs
            // that durable identity before it can admit a fresh row-first
            // projection. The raw bytes remain lazy; only the small
            // ownership rows cross the semantic scope boundary.
        }
    }
}

fn sync_packs_match_admission_projection(
    expected: &SyncTransactionPack,
    actual: &SyncTransactionPack,
) -> bool {
    if expected.operation_id != actual.operation_id
        || expected.branch_id != actual.branch_id
        || expected.base_server_commit_id != actual.base_server_commit_id
        || expected.local_commit_id != actual.local_commit_id
        || expected.files != actual.files
        || expected.rows.len() != actual.rows.len()
    {
        return false;
    }
    expected
        .rows
        .iter()
        .zip(&actual.rows)
        .all(|(expected, actual)| {
            if expected.schema_key != actual.schema_key
                || expected.file_id != actual.file_id
                || expected.row_pk != actual.row_pk
                || expected.metadata != actual.metadata
                || expected.global != actual.global
                || expected.untracked != actual.untracked
            {
                return false;
            }
            if expected.schema_key != "lix_branch_ref" {
                return expected.snapshot == actual.snapshot;
            }
            let Some(expected_snapshot) = expected.snapshot.as_ref() else {
                return actual.snapshot.is_none();
            };
            let Some(actual_snapshot) = actual.snapshot.as_ref() else {
                return false;
            };
            let (Some(expected_object), Some(actual_object)) =
                (expected_snapshot.as_object(), actual_snapshot.as_object())
            else {
                return false;
            };
            let mut expected_object = expected_object.clone();
            let Some(actual_commit_id) = actual_object.get("commit_id") else {
                return false;
            };
            expected_object.insert("commit_id".to_owned(), actual_commit_id.clone());
            expected_object == *actual_object
        })
}

impl<StorageImpl, Transport> SyncClient<StorageImpl, Transport>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    /// Completes the initial lifecycle catch-up without holding the caller on
    /// an idle heartbeat. Steady-state background iterations use `flush()` on
    /// a lifecycle client and therefore retain the mandatory long-poll.
    pub(crate) async fn flush_without_wait(&mut self) -> Result<SyncFlushReceipt, LixError> {
        let previous = self.long_poll;
        self.long_poll = false;
        let result = self.flush().await;
        self.long_poll = previous;
        result
    }

    /// Marks a first-contact lifecycle client for one post-bootstrap orphan
    /// sweep. The local engine had to seed a temporary root before it could
    /// execute any SQL; after canonical history is applied that root must not
    /// remain visible through the ordinary commit graph.
    pub(crate) fn mark_fresh_bootstrap_cleanup(&mut self, commit_id: String) {
        self.state.bootstrap_cleanup_pending = true;
        self.state.bootstrap_cleanup_commit_id = Some(commit_id);
    }

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
        // A manual client supplies its schema demand directly rather than
        // going through the SQL scope extractor. Register the demand before
        // the first pull, but leave the durable hydrated set untouched until
        // canonical history has actually been replayed.
        self.scope_state
            .register_explicit_scopes_for_branch(&self.state.branch_id, semantic_schema_keys);
        let requested_scopes = self.scope_state.scopes_for_branch(&self.state.branch_id);
        let needs_scope_hydration = requested_scopes
            .iter()
            .any(|scope| !self.state.hydrated_scopes.contains(scope));
        if self.state.server_commit_id.is_none() || needs_scope_hydration {
            let had_server_head = self.state.server_commit_id.is_some();
            // A fresh manual client must import the requested relation's
            // history before advancing its global cursor. Otherwise the
            // initial head-only pull would skip pre-existing remote rows and
            // the subsequent transaction would incorrectly mark that scope
            // as ready. The same applies when a manual client has already
            // performed an unscoped head pull and is enqueueing its first
            // relation afterwards. `hydrate_requested_scopes` also installs
            // the registered-schema/plugin bootstrap lane when needed.
            let hydration = async {
                self.hydrate_requested_scopes().await?;
                self.pull_to_head().await
            }
            .await;
            if let Err(error) = hydration {
                // Offline manual clients may still enqueue an optimistic
                // pack against the last durable server head. They cannot
                // certify a newly requested scope until reconnect, but
                // rejecting the local write would violate the offline queue
                // contract. Never suppress a first-contact error (there is
                // no trustworthy base), and never suppress validation or
                // protocol errors; only transport-unavailable failures take
                // this deferred path.
                if !had_server_head || !is_sync_transport_unavailable(&error) {
                    return Err(error);
                }
            }
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
        // The explicit scopes remain a demand until `flush()` runs the normal
        // hydration path. Marking them hydrated here would suppress replay of
        // pre-existing canonical rows on a manual client that is already
        // connected to a non-empty remote history.
        self.state.pending.push(pack);
        self.pending_storage_dirty = true;
        self.persist_state().await?;
        Ok(true)
    }

    /// Pulls canonical work and admits queued local transactions until both
    /// sides agree that the durable pending queue is empty.
    pub async fn flush(&mut self) -> Result<SyncFlushReceipt, LixError> {
        let hydrated_before = self.state.hydrated_scopes.clone();
        let cursor_before = self.state.cursor;
        self.hydrate_requested_scopes().await?;
        // A lifecycle flush must return after it has materialized a newly
        // demanded scope. Calling the trailing event long-poll in the same
        // turn would hold the worker before it can refresh the primary
        // session's lazy file overlay, making the first file read wait for a
        // heartbeat. The next worker iteration immediately resumes the
        // mandatory long-poll, so this is only a scheduling boundary.
        let scope_hydration_work = self.long_poll
            && (self.state.hydrated_scopes != hydrated_before
                || self.state.cursor != cursor_before);
        if self.state.pending.is_empty() {
            if !scope_hydration_work {
                self.pull_to_head().await?;
            }
        } else {
            // A pending local pack must be admitted without waiting for an
            // idle event heartbeat. First perform the immediate metadata-only
            // head probe; only pull an event page when the authoritative head
            // actually moved. This keeps local writes fast while still
            // rebasing against a concurrent remote commit before admission.
            let probe = self
                .transport
                .pull(&self.state.branch_id, self.state.cursor, 0, &[])
                .await?;
            require_sync_identity(
                "pending head probe branch",
                &self.state.branch_id,
                &probe.branch_id,
            )?;
            validate_sync_pull_head_commit_id(&probe.head_commit_id)?;
            if probe.head_cursor > self.state.cursor {
                self.pull_to_head().await?;
            } else {
                self.state.server_commit_id = Some(probe.head_commit_id);
                self.persist_state().await?;
            }
        }
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
                // A lazy head-only pull may advance the authoritative cursor
                // without returning the event payload. The optimistic local
                // transaction is already visible, so acknowledge the exact
                // pending operation once its cursor is known to be published.
                self.remove_pending_operation(&admission.operation_id);
            }
            // Branch creation/deletion packs mutate the repository-global
            // catalog while their admission session is pinned to the source
            // application branch. Pulling that branch's event stream cannot
            // update the local `lix_branch` ref, so reconcile the tiny control
            // catalog immediately after the authoritative receipt. The
            // worker's periodic pass remains the retry path if this best
            // effort read races another global write.
            if pack.rows.iter().any(|row| is_sync_control_schema(&row.schema_key)) {
                let topology_session = self
                    .lix
                    .open_internal_session_suppressed(
                        self.state.branch_id.clone(),
                        self.lix.active_account_id().to_owned(),
                    )
                    .await?;
                let topology_result =
                    reconcile_sync_branches(&topology_session, &self.transport, false).await;
                topology_session.close().await?;
                if let Err(error) = topology_result {
                    tracing::warn!(
                        error = ?error,
                        "sync admission topology reconciliation failed"
                    );
                }
            }
        }
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
interval: Duration,
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
        // Once at least one relation is materialized, live pulls can request
        // only those projections. Unrelated events still advance the global
        // cursor with an empty pack, while the per-scope cursor lets a later
        // query reconstruct skipped history. A full-history demand retains
        // every semantic row/topology fact, while ordinary file bytes remain
        // a separate lazy projection.
        let full_scope = [FULL_SYNC_PULL_SCOPE.to_owned()];
        let full_schema_scope = SyncFilterScope::new(&full_scope);
        let schema_scope = if full_hydrated {
            Some(full_schema_scope)
        } else if !schema_keys.is_empty() {
            Some(SyncFilterScope::new(&schema_keys))
        } else {
            None
        };
        let pull_scopes = if full_hydrated {
            &full_scope[..]
        } else {
            schema_keys.as_slice()
        };
        if schema_keys.is_empty() && !full_hydrated {
            return self.pull_head_only().await;
        }
        // Manual SyncClient callers do not run the background lifecycle
        // worker, so they must never issue an event-bearing pull while the
        // cursor is already at the head. The server deliberately long-polls
        // every event request; use the metadata-only probe to decide whether
        // there is work before asking for a page.
        if !self.long_poll {
            let probe = self
                .transport
                .pull(&self.state.branch_id, self.state.cursor, 0, pull_scopes)
                .await?;
            require_sync_identity("head probe branch", &self.state.branch_id, &probe.branch_id)?;
            validate_sync_pull_head_commit_id(&probe.head_commit_id)?;
            if probe.head_cursor <= self.state.cursor {
                self.state.server_commit_id = Some(probe.head_commit_id);
                self.persist_state().await?;
                return Ok(());
            }
        }
        loop {
            let response = self
                .transport
                .pull(
                    &self.state.branch_id,
                    self.state.cursor,
                    DEFAULT_SYNC_PULL_LIMIT,
                    pull_scopes,
                )
                .await?;
            require_sync_identity("pull branch", &self.state.branch_id, &response.branch_id)?;
            validate_sync_pull_head_commit_id(&response.head_commit_id)?;
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
                hydrate_missing_sync_parent_branches(
                    &self.lix,
                    &self.transport,
                    &event,
                    &self.state.branch_id,
                )
                .await?;
                if event.pack.rows.is_empty() && event.pack.files.is_empty() {
                    if full_hydrated {
                        // Full history requests the graph even when a commit
                        // has no semantic row/file payload. Narrow scopes
                        // only advance their cursor through this event.
                        self.lix
                            .apply_sync_topology_event_with_seed(
                                &event,
                                self.state.bootstrap_cleanup_commit_id.as_deref(),
                            )
                            .await?;
                    }
                } else {
                    validate_sync_transaction_pack(&event.pack)?;
                    self.require_matching_pending_event(&event.pack, schema_scope.as_ref())?;
                    let marker_scopes = if full_hydrated {
                        &[][..]
                    } else {
                        schema_keys.as_slice()
                    };
                    self.lix
                        .apply_sync_canonical_event_with_seed(
                            &event,
                            &self.state.remote_id,
                            marker_scopes,
                            self.state.bootstrap_cleanup_commit_id.as_deref(),
                        )
                        .await?;
                }
                self.state.cursor = event.cursor;
                self.state.server_commit_id = Some(event.canonical_commit_id);
                self.remove_pending_operation(&event.pack.operation_id);
            }
            // Applied markers are committed with each canonical event, so a
            // crash between pages can safely replay the page without running
            // plugin reconciliation twice. Persist the small cursor/pending
            // manifest once per page instead of once per event; this keeps
            // durable replay correctness while avoiding one mutable-storage
            // write for every historical commit in a backlog.
            self.persist_state_progress().await?;
            self.reclaim_fresh_bootstrap_orphan_if_ready().await?;
            self.replay_pending_overlay().await?;
            if self.state.cursor >= response.head_cursor {
                return Ok(());
            }
        }
    }

    async fn pull_head_only(&mut self) -> Result<(), LixError> {
        if !self.long_poll {
            return self.pull_head_only_now().await;
        }
        // A replica with no materialized SQL scope still needs a live cursor
        // so branch/catalog changes can be observed. Ask for topology-only
        // events instead of using the old limit=0 head probe: the server's
        // event-bearing endpoint is now a mandatory long-poll.
        let topology_scope = [TOPOLOGY_SYNC_PULL_SCOPE.to_owned()];
        loop {
            let response = self
                .transport
                .pull(
                    &self.state.branch_id,
                    self.state.cursor,
                    DEFAULT_SYNC_PULL_LIMIT,
                    &topology_scope,
                )
                .await?;
            require_sync_identity(
                "head pull branch",
                &self.state.branch_id,
                &response.branch_id,
            )?;
            validate_sync_pull_head_commit_id(&response.head_commit_id)?;
            if response.next_cursor < self.state.cursor
                || response.next_cursor > response.head_cursor
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "topology sync pull returned invalid cursor bounds",
                ));
            }
            if response.events.is_empty() {
                if response.next_cursor != self.state.cursor {
                    return Err(LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "topology sync pull advanced its cursor without events",
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
                            "topology sync pull cursor gap: expected {expected_cursor}, received {}",
                            event.cursor
                        ),
                    ));
                }
                // The topology scope intentionally strips rows and files. The
                // branch catalog pass will materialize control rows lazily;
                // this cursor only records that the canonical event position
                // has been observed.
                self.state.cursor = event.cursor;
                self.state.server_commit_id = Some(event.canonical_commit_id);
            }
            self.persist_state_progress().await?;
            if self.state.cursor >= response.head_cursor {
                return Ok(());
            }
        }
    }

    /// Finite head metadata used by explicit/manual `flush()` calls. The
    /// server still exposes long-polling for every event-bearing request; the
    /// existing `limit=0` shape is only a local completion probe and never
    /// transfers events.
    async fn pull_head_only_now(&mut self) -> Result<(), LixError> {
        let response = self
            .transport
            .pull(&self.state.branch_id, self.state.cursor, 0, &[])
            .await?;
        require_sync_identity(
            "head pull branch",
            &self.state.branch_id,
            &response.branch_id,
        )?;
        validate_sync_pull_head_commit_id(&response.head_commit_id)?;
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
            .scopes_for_branch(&self.state.branch_id)
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
            self.scope_state.mark_scope_hydrated_for_branch(
                &self.state.branch_id,
                FULL_SYNC_SCOPE,
                scope_generation,
            );
            self.persist_state().await?;
            self.replay_pending_overlay().await?;
        }
        // A full hydrate covers every future row/topology demand. Ordinary
        // file bytes remain a separate lazy projection, so leave `lix_file`
        // in the request set for the normal file-scope pass below.
        if self.state.hydrated_scopes.contains(FULL_SYNC_SCOPE) {
            let mut changed = false;
            let mut file_scope_pending = false;
            for schema_key in &requested {
                if schema_key == "lix_file" {
                    file_scope_pending = true;
                    continue;
                }
                if !self.state.hydrated_scopes.contains(schema_key) {
                    self.state.hydrated_scopes.insert(schema_key.clone());
                    self.scope_state.mark_scope_hydrated_for_branch(
                        &self.state.branch_id,
                        schema_key,
                        scope_generation,
                    );
                    changed = true;
                }
            }
            if changed {
                self.persist_state().await?;
            }
            if !file_scope_pending {
                return Ok(());
            }
        }
        if !self.state.hydrated_scopes.contains("lix_registered_schema") {
            self.hydrate_scope("lix_registered_schema").await?;
            self.state
                .hydrated_scopes
                .insert("lix_registered_schema".to_owned());
            self.scope_state.mark_scope_hydrated_for_branch(
                &self.state.branch_id,
                "lix_registered_schema",
                scope_generation,
            );
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
            self.scope_state.mark_scope_hydrated_for_branch(
                &self.state.branch_id,
                &schema_key,
                scope_generation,
            );
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
        // Full-history SQL on the selected branch only needs that branch's
        // canonical commit/change stream. Global catalog and non-active branch
        // topology are reconciled by the background control pass; importing
        // their refs here can fail while a source commit is still absent from
        // the global domain and would block an otherwise local history read.
        // Full-history hydration is a row/topology demand. Use the internal
        // wire scope so the server can omit ordinary file bytes while still
        // returning every semantic row.
        let scope = if schema_key == FULL_SYNC_SCOPE {
            vec![FULL_SYNC_PULL_SCOPE.to_owned()]
        } else {
            vec![schema_key.to_owned()]
        };
        let marker_scopes = if schema_key == FULL_SYNC_SCOPE {
            vec![FULL_SYNC_SCOPE.to_owned()]
        } else {
            scope.clone()
        };
        let schema_scope = (!scope.is_empty()).then(|| SyncFilterScope::new(&scope));
        let mut cursor = self
            .state
            .scope_cursors
            .get(schema_key)
            .copied()
            .unwrap_or(0);
        loop {
            // Scoped hydration is a foreground prerequisite for a query even
            // when the lifecycle worker normally uses long-polling. Probe the
            // finite head first so a scope that is already caught up does not
            // wait for the server heartbeat before the local readiness barrier
            // can complete. The next worker iteration resumes the live
            // event-bearing long-poll after this scope is marked hydrated.
            let probe = self
                .transport
                .pull(&self.state.branch_id, cursor, 0, &scope)
                .await?;
            require_sync_identity(
                "scoped head probe branch",
                &self.state.branch_id,
                &probe.branch_id,
            )?;
            validate_sync_pull_head_commit_id(&probe.head_commit_id)?;
            if cursor >= probe.head_cursor {
                self.state.server_commit_id = Some(probe.head_commit_id);
                self.persist_state().await?;
                // Non-active branch topology is reconciled by the background
                // control pass. Keeping it out of this foreground scope
                // barrier prevents one malformed/still-unmaterialized branch
                // ref from blocking an otherwise local full-history query.
                return Ok(());
            }
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
            validate_sync_pull_head_commit_id(&response.head_commit_id)?;
            if response.next_cursor < cursor || response.next_cursor > response.head_cursor {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "scoped sync pull returned invalid cursor bounds",
                ));
            }
            for event in response.events {
                validate_sync_canonical_event_identity(&event)?;
                hydrate_missing_sync_parent_branches(
                    &self.lix,
                    &self.transport,
                    &event,
                    &self.state.branch_id,
                )
                .await?;
                if !event.pack.rows.is_empty() || !event.pack.files.is_empty() {
                    validate_sync_transaction_pack(&event.pack)?;
                    self.require_matching_pending_event(&event.pack, schema_scope.as_ref())?;
                    self.lix
                        .apply_sync_canonical_event_with_seed(
                            &event,
                            &self.state.remote_id,
                            &marker_scopes,
                            self.state.bootstrap_cleanup_commit_id.as_deref(),
                        )
                        .await?;
                } else if schema_key == FULL_SYNC_SCOPE {
                    self.lix
                        .apply_sync_topology_event_with_seed(
                            &event,
                            self.state.bootstrap_cleanup_commit_id.as_deref(),
                        )
                        .await?;
                }
                cursor = event.cursor;
                self.state
                    .scope_cursors
                    .insert(schema_key.to_owned(), cursor);
            }
            // The applied marker for every event above is already durable and
            // is the replay fence. Checkpoint the scope cursor once per page;
            // if the process stops earlier, the next scoped pull simply
            // re-reads the bounded page and marker checks turn it into a
            // storage-only no-op.
            self.persist_state_progress().await?;
            self.reclaim_fresh_bootstrap_orphan_if_ready().await?;
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
            if !sync_packs_match_admission_projection(&expected, event_pack) {
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

    /// Records the synthetic commits created while a pristine local engine is
    /// rebound to its server branch. Their immutable changelog rows stay in
    /// place because local graph jumps and future writes may still need them;
    /// the derived public commit/change providers filter this internal set.
    /// Marking the whole seed-descendant closure also covers the temporary
    /// branch-management commits made before the first canonical page.
    async fn reclaim_fresh_bootstrap_orphan_if_ready(&mut self) -> Result<(), LixError> {
        if !self.state.bootstrap_cleanup_pending
            || !self.state.pending.is_empty()
            || self.state.cursor == 0
        {
            return Ok(());
        }
        let seed_commit_id = self
            .state
            .bootstrap_cleanup_commit_id
            .as_deref()
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "fresh sync bootstrap cleanup is missing its seed commit",
                )
            })
            .and_then(|value| CommitId::parse_lix(value, "fresh sync bootstrap seed commit"))?;

        let _write_guard = self.lix.lock_collaboration_writes().await;
        let adapter = self.lix.storage_adapter();
        let read =
            SharedStorageAdapterRead::new(adapter.begin_read(StorageReadOptions::default()).await?);

        let hidden = load_sync_hidden_commit_ids(&read).await?;
        let mut writes = adapter.new_write_set();
        if !hidden.contains(&seed_commit_id) {
            stage_sync_hidden_commit_marker(&mut writes, seed_commit_id);
        }
        if !writes.is_empty() {
            adapter
                .commit_write_set(
                    writes,
                    StorageWriteOptions {
                        await_durable: true,
                        ..StorageWriteOptions::default()
                    },
                )
                .await?;
        }
        drop(_write_guard);
        self.state.bootstrap_cleanup_pending = false;
        self.state.bootstrap_cleanup_commit_id = None;
        // Persist the cleared latch after the marker set. If this manifest
        // checkpoint fails, a restarted worker will safely retry the
        // idempotent marker write instead of losing cleanup intent.
        self.persist_state_with_durability(true).await?;
        Ok(())
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
            bootstrap_cleanup_pending: self.state.bootstrap_cleanup_pending,
            bootstrap_cleanup_commit_id: self.state.bootstrap_cleanup_commit_id.clone(),
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
            bootstrap_cleanup_pending: manifest.bootstrap_cleanup_pending,
            bootstrap_cleanup_commit_id: manifest.bootstrap_cleanup_commit_id,
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
    bootstrap_cleanup_pending: bool,
    bootstrap_cleanup_commit_id: Option<String>,
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
            bootstrap_cleanup_pending: manifest.bootstrap_cleanup_pending,
            bootstrap_cleanup_commit_id: manifest.bootstrap_cleanup_commit_id,
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
        bootstrap_cleanup_pending: state.bootstrap_cleanup_pending,
        bootstrap_cleanup_commit_id: state.bootstrap_cleanup_commit_id,
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
StorageKey(Bytes::from(key))
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
StorageKey(Bytes::from(key))
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
StorageKey(Bytes::from(key))
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

fn append_sync_bootstrap_rows(
    batch: &crate::tracked_state::MaterializedTrackedStateBatch,
    rows: &mut BTreeMap<(String, Option<String>, String), SyncRowMutation>,
    include_control_rows: bool,
) -> Result<(), LixError> {
    for row in batch.iter() {
        if row.deleted() {
            continue;
        }
        let schema_key = row.schema_key().to_owned();
        // Branch descriptors/refs belong to the global control stream. They
        // are included only when that stream is bootstrapped directly; a
        // semantic branch bootstrap must not replay them into a placeholder
        // branch or invent a second local catalog entry.
        if !include_control_rows && is_sync_control_schema(&schema_key)
            || !is_sync_control_schema(&schema_key) && !ordinary_sync_schema(&schema_key)
        {
            continue;
        }
        let Some(snapshot_content) = row.snapshot_content() else {
            continue;
        };
        let snapshot: serde_json::Value =
            serde_json::from_str(snapshot_content.as_str()).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("decode sync bootstrap row snapshot: {error}"),
                )
            })?;
        // Built-in Lix schemas are already present in every repository. Their
        // catalog rows are part of the server's tracked state, but replaying
        // them through the runtime registration path would treat a built-in
        // such as `lix_account` as an application registration and fail with
        // the reserved-namespace guard. Only user/plugin registrations are
        // portable bootstrap data.
        if schema_key == "lix_registered_schema"
            && snapshot
                .get("value")
                .and_then(|value| value.get("key"))
                .and_then(serde_json::Value::as_str)
                .is_some_and(crate::sql2::PublicCatalog::runtime_schema_key_uses_reserved_namespace)
        {
            continue;
        }
        let metadata = row
            .metadata()
            .map(|content| serde_json::from_str(content.as_str()))
            .transpose()
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("decode sync bootstrap row metadata: {error}"),
                )
            })?;
        let row_pk = row.row_pk().as_json_array_value()?;
        let file_id = row.file_id().map(str::to_owned);
        let identity = (schema_key.clone(), file_id.clone(), row_pk.to_string());
        let mutation = SyncRowMutation {
            schema_key: schema_key.clone(),
            file_id,
            row_pk,
            snapshot: Some(snapshot),
            metadata,
            global: is_sync_control_schema(&schema_key),
            untracked: schema_key == "lix_branch_ref",
        };
        if !is_engine_managed_sync_row(&mutation) {
            rows.entry(identity).or_insert(mutation);
        }
    }
    Ok(())
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
StorageKey(Bytes::copy_from_slice(remote_id.as_bytes()))
}

pub(crate) fn stage_sync_event_publication(
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
    pack: &SyncTransactionPack,
    canonical_commit_id: &str,
    plan: &SyncAdmissionPlan,
    parent_commit_ids: &[String],
) -> Result<SyncCanonicalEvent, LixError> {
    validate_sync_topology_event_pack(pack)?;
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
        parent_commit_ids: parent_commit_ids.to_vec(),
        pack_fingerprint,
        pack: pack.clone(),
    };
    stage_sync_event_record(writes, preconditions, &event)?;
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
    writes.put(SYNC_HEAD_SPACE, head_key.clone(), head_value);
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

/// Stages one immutable canonical event without publishing a branch head.
/// Bootstrap uses this to publish a complete oldest-first commit DAG in one
/// atomic write set; only the final event receives the branch-head mutation.
fn stage_sync_event_record(
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
    event: &SyncCanonicalEvent,
) -> Result<(), LixError> {
    validate_sync_canonical_event_identity(event)?;
    let event_key = sync_event_key(&event.pack.branch_id, event.cursor);
    let event_value = serde_json::to_vec(event).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("encode canonical sync event: {error}"),
        )
    })?;
    writes.put(SYNC_EVENT_SPACE, event_key.clone(), event_value);
    preconditions.push(StoragePrecondition::KeyAbsent {
        space: SYNC_EVENT_SPACE,
        key: event_key,
    });
    Ok(())
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
    let mut rows = prepared_sync_rows(prepared, branch_id)?;
    append_selected_sync_rows(read, prepared, branch_id, &mut rows).await?;
    append_certified_sync_rows(
        prepared,
        change_refs.commit_id,
        change_refs.created_at,
        branch_id,
        &mut rows,
    )?;
    rows.retain(|row| !is_engine_managed_sync_row(row));
    let files = prepared_sync_files(blob_reader, prepared, branch_id).await?;
    if sync_pack_has_semantic_rows(&rows) {
        // Plugin-backed rows are canonical. Keep descriptor/blob-reference
        // rows alongside them because they establish file ownership for row
        // validation without requiring file bytes.
        // Certified plugin batches are the semantic source of truth. The
        // rendered file bytes remain in the canonical event only so a later
        // file-view demand can materialize its local projection; scoped row
        // pulls strip them before admission.
    } else if !files.is_empty() {
        // File bytes are a secondary projection, but the descriptor/blob-ref
        // rows remain canonical identity facts. Keep them in the event so a
        // lazy file pull can establish the local file view without inventing
        // a descriptor commit from raw bytes alone.
    }
    if files.iter().any(is_plugin_archive_sync_file) {
        rows.retain(|row| row.schema_key != "lix_registered_schema");
    }
    if rows.is_empty() && files.is_empty() {
        return Ok(false);
    }
    let mut pack = SyncTransactionPack {
        operation_id: format!("server:{commit_id}"),
        branch_id: branch_id.to_owned(),
        base_server_commit_id,
        local_commit_id: commit_id.clone(),
        parent_commit_ids: Vec::new(),
        rows,
        files,
    };
    let mut parent_commit_ids = prepared
        .first_commit_parent_override_by_branch
        .get(branch_id)
        .copied()
        .or_else(|| parent_heads.get(branch_id).copied().flatten())
        .into_iter()
        .map(|parent| parent.to_string())
        .collect::<Vec<_>>();
    if let Some(extra) = prepared.extra_commit_parents_by_branch.get(branch_id) {
        parent_commit_ids.extend(extra.iter().map(ToString::to_string));
    }
    parent_commit_ids.retain(|parent| parent != &commit_id);
    pack.parent_commit_ids = parent_commit_ids.clone();
    let stored = load_sync_head(read, branch_id).await?;
    // Before the first pull, the authority has no durable cursor. Deferring
    // this ordinary event lets the first bootstrap page publish the complete
    // reachable DAG (including this commit) instead of making cursor 1 look
    // like the repository's genesis and losing pre-sync history.
    let Some(stored) = stored else {
        return Ok(false);
    };
    let cursor = stored
        .1
        .cursor
        .checked_add(1)
        .ok_or_else(|| LixError::new(LixError::CODE_INTERNAL_ERROR, "sync cursor overflow"))?;
    let plan = SyncAdmissionPlan {
        cursor,
        previous_head_value: Some(stored.0),
    };
    stage_sync_event_publication(
        writes,
        preconditions,
        &pack,
        &commit_id,
        &plan,
        &parent_commit_ids,
    )?;
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
    let mut rows = prepared_sync_rows(prepared, branch_id)?;
    append_selected_sync_rows(read, prepared, branch_id, &mut rows).await?;
    let change_refs = prepared
        .commit_change_refs_by_branch
        .get(branch_id)
        .or_else(|| prepared.commit_change_refs_by_branch.values().next())
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync replica transaction has no finalized commit refs",
            )
        })?;
    append_certified_sync_rows(
        prepared,
        change_refs.commit_id,
        change_refs.created_at,
        branch_id,
        &mut rows,
    )?;
    // Ownership is durable engine state on the authoritative branch. A local
    // proposal carries the semantic edit (and any source bytes), while the
    // server's canonical event publishes the owner row after admission.
    rows.retain(|row| !is_sync_plugin_owner_mutation(row));
    rows.retain(|row| !is_engine_managed_sync_row(row));
    let files = prepared_sync_files(blob_reader, prepared, branch_id).await?;
    if sync_pack_has_semantic_rows(&rows) {
        // A replica's semantic edit is already anchored to its local file
        // ownership. Do not enqueue the derived blob-reference rows again:
        // admission treats those rows as inserts, so replaying an unchanged
        // blob ref would collide with the server's existing identity.
        rows.retain(|row| !is_file_projection_sync_schema(&row.schema_key));
        // Keep file bytes in the canonical event for an eventual file-view
        // demand. The row-scope projection removes them from the transfer.
    } else if !files.is_empty() {
        // Preserve canonical file identity rows in the admission pack. The
        // server may regenerate/validate them from the bytes, while replicas
        // need the rows to materialize a file view lazily.
    }
    if files.iter().any(is_plugin_archive_sync_file) {
        rows.retain(|row| row.schema_key != "lix_registered_schema");
    }
    if rows.is_empty() && files.is_empty() {
        return Ok(false);
    }
    // Branch-control rows are committed on the global control branch by the
    // ordinary transaction planner, while the replica admission stream is
    // pinned to the caller's active branch. Use the transaction's first
    // finalized commit as the local identity regardless of which branch owns
    // that commit; the server assigns the authoritative branch commit during
    // admission. Semantic branch-local writes still take their own branch's
    // commit identity.
    let commit_id = prepared
        .commit_change_refs_by_branch
        .get(branch_id)
        .or_else(|| prepared.commit_change_refs_by_branch.values().next())
        .map(|refs| refs.commit_id.to_string())
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync control rows have no finalized transaction commit",
            )
        })?;
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
    let mut parent_commit_ids = vec![base_server_commit_id.clone()];
    if let Some(extra) = prepared.extra_commit_parents_by_branch.get(branch_id) {
        parent_commit_ids.extend(extra.iter().map(ToString::to_string));
    }
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
        parent_commit_ids,
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
        bootstrap_cleanup_pending: manifest.bootstrap_cleanup_pending,
        bootstrap_cleanup_commit_id: manifest.bootstrap_cleanup_commit_id,
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
            let is_control = is_sync_control_schema(row.schema_key.as_str());
            if is_control {
                row.global
                    && row.branch_id.as_str() == GLOBAL_BRANCH_ID
                    && row.untracked == (row.schema_key == "lix_branch_ref")
            } else if row.schema_key == "lix_registered_schema" {
                !row.untracked
                    && ((row.global && row.branch_id.as_str() == GLOBAL_BRANCH_ID)
                        || (!row.global && row.branch_id.as_str() == branch_id))
            } else {
                !row.untracked
                    && !row.global
                    && row.branch_id.as_str() == branch_id
                    && ordinary_sync_schema(row.schema_key.as_str())
            }
        })
        .map(|row| {
            Ok(SyncRowMutation {
                schema_key: row.schema_key.to_string(),
                file_id: row.file_id.map(ToString::to_string),
                row_pk: row.row_pk.as_json_array_value()?,
                snapshot: row.snapshot.map(|value| value.value().clone()),
                metadata: row.metadata.map(|value| value.value().clone()),
                global: row.global,
                untracked: row.untracked,
            })
        })
        .collect()
}

/// Certified plugin batches are committed directly into the hot-state row
/// store, so they do not appear in `PreparedWriteSet::state_rows` or selected
/// changelog references. A sync event still needs a self-contained semantic
/// payload. Decode the validated batch once at the event boundary and lower
/// its live rows into the same row mutation format used by ordinary writes.
///
/// This keeps plugin rows canonical while leaving file bytes as a derived,
/// lazy projection. The batch's commit identity and timestamp are already
/// fixed in the staged commit refs, which makes the decoded row metadata
/// identical to the rows published by normal commit materialization.
fn append_certified_sync_rows(
    prepared: &PreparedWriteSet,
    commit_id: CommitId,
    created_at: LixTimestamp,
    branch_id: &str,
    rows: &mut Vec<SyncRowMutation>,
) -> Result<(), LixError> {
    // Certified batches can contain thousands of rows. Keep deduplication
    // linear in the event size instead of rescanning the accumulated payload
    // for every materialized row.
    let mut seen = rows
        .iter()
        .map(|row| {
            (
                row.schema_key.clone(),
                row.file_id.clone(),
                row.row_pk.to_string(),
            )
        })
        .collect::<HashSet<_>>();
    for file in prepared.file_content_writes.iter().filter(|file| {
        file.branch_id == branch_id
            && !file.global
            && !file.untracked
            && !file.certified_row_batches().is_empty()
    }) {
        for batch in file.certified_row_batches() {
            let materialized = crate::hot_state::materialize_certified_root_rows(
                branch_id,
                &file.file_id,
                commit_id,
                created_at,
                batch,
            )?;
            for row in materialized.into_rows() {
                if row.deleted || row.untracked || row.global {
                    continue;
                }
                let snapshot = row
                    .snapshot_content
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|error| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("decode certified sync row snapshot: {error}"),
                        )
                    })?;
                let metadata = row
                    .metadata
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|error| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("decode certified sync row metadata: {error}"),
                        )
                    })?;
                let row_pk = row.row_pk.as_json_array_value()?;
                let identity = (
                    row.schema_key.clone(),
                    row.file_id.clone(),
                    row_pk.to_string(),
                );
                if !seen.insert(identity) {
                    continue;
                }
                rows.push(SyncRowMutation {
                    schema_key: row.schema_key,
                    file_id: row.file_id,
                    row_pk,
                    snapshot,
                    metadata,
                    global: row.global,
                    untracked: row.untracked,
                });
            }
        }
    }
    Ok(())
}

/// Merge commits retain source changes as immutable selected references rather
/// than restaging every selected row in `state_rows`.  The selected reference
/// is enough for the local commit materializer, but a sync event must carry a
/// self-contained semantic row payload so a replica can apply the same merge
/// without already having the source branch's changelog.  Resolve those
/// references in one batch per source commit and append only rows that were
/// not already materialized by plugin-specific merge resolution.
async fn append_selected_sync_rows<R>(
    read: &R,
    prepared: &PreparedWriteSet,
    branch_id: &str,
    rows: &mut Vec<SyncRowMutation>,
) -> Result<(), LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let Some(change_refs) = prepared.commit_change_refs_by_branch.get(branch_id) else {
        return Ok(());
    };
    let selected = change_refs
        .selected_changes()
        .filter(|change| ordinary_sync_schema(change.schema_key()))
        .collect::<Vec<_>>();
    if selected.is_empty() {
        return Ok(());
    }

    let mut source_requests = BTreeMap::<CommitId, Vec<(usize, TrackedStateKey)>>::new();
    for (index, change) in selected.iter().enumerate() {
        if change.deleted {
            continue;
        }
        source_requests
            .entry(change.source_commit_id)
            .or_default()
            .push((
                index,
                TrackedStateKey {
                    schema_key: change.schema_key().to_owned(),
                    file_id: change.file_id().map(str::to_owned),
                    row_pk: change.row_pk().clone(),
                },
            ));
    }
    let mut payloads = vec![None; selected.len()];
    for (source_commit_id, requests) in source_requests {
        let keys = requests
            .iter()
            .map(|(_, key)| key.clone())
            .collect::<Vec<_>>();
        let records = load_commit_delta_change_records(read, source_commit_id, &keys).await?;
        let mut materialize = Vec::with_capacity(records.len());
        let mut positions = Vec::with_capacity(records.len());
        for ((index, _), record) in requests.into_iter().zip(records) {
            let Some(record) = record else {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "sync merge selected change at source commit '{source_commit_id}' has no payload"
                    ),
                ));
            };
            positions.push(index);
            materialize.push(record);
        }
        let hydrated = crate::changelog::materialize_known_change_payloads_in_order(
            read,
            materialize.into_iter(),
            ChangeRecordProjection::full(),
        )
        .await?;
        for (index, (change_id, payload)) in positions.into_iter().zip(hydrated) {
            if change_id != selected[index].change_id {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "sync merge selected payload identity does not match its change reference",
                ));
            }
            payloads[index] = Some(payload);
        }
    }

    for (index, change) in selected.into_iter().enumerate() {
        let snapshot = if change.deleted {
            None
        } else {
            let payload = payloads[index].as_ref().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "sync merge selected live row is missing its payload",
                )
            })?;
            let snapshot = payload.snapshot_content.as_deref().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "sync merge selected live row has no snapshot payload",
                )
            })?;
            Some(serde_json::from_str(snapshot).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("decode sync merge selected row snapshot: {error}"),
                )
            })?)
        };
        let metadata = payloads[index]
            .as_ref()
            .and_then(|payload| payload.metadata.as_deref())
            .map(|metadata| {
                serde_json::from_str(metadata).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("decode sync merge selected row metadata: {error}"),
                    )
                })
            })
            .transpose()?;
        let row_pk = change.row_pk().as_json_array_value()?;
        let duplicate = rows.iter().any(|existing| {
            existing.schema_key == change.schema_key()
                && existing.file_id.as_deref() == change.file_id()
                && existing.row_pk == row_pk
        });
        if !duplicate {
            rows.push(SyncRowMutation {
                schema_key: change.schema_key().to_owned(),
                file_id: change.file_id().map(str::to_owned),
                row_pk,
                snapshot,
                metadata,
                global: false,
                untracked: false,
            });
        }
    }
    Ok(())
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
        || matches!(
            schema_key,
            "lix_registered_schema"
                | "lix_key_value"
                | "lix_file_descriptor"
                | "lix_directory_descriptor"
                | "lix_binary_blob_ref"
        )
}

/// Plugin installation and reservation rows are engine-managed state. Plugin
/// installation and reservation mutations remain local to the receiving
/// catalog, while the durable per-file owner is carried as a certified sync
/// fact and admitted through the transaction's narrow owner lane. Keeping the
/// owner in the canonical pack is what lets a fresh row-only replica establish
/// plugin ownership without downloading a source file.
fn is_engine_managed_sync_row(row: &SyncRowMutation) -> bool {
    if row.schema_key != "lix_key_value" {
        return false;
    }
    let mut row_key = row
        .row_pk
        .as_array()
        .and_then(|parts| parts.first())
        .and_then(serde_json::Value::as_str)
        .into_iter()
        .chain(
            row.snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.get("key"))
                .and_then(serde_json::Value::as_str),
        );
    row_key.any(|key| PLUGIN_REGISTRY_KEY == key || is_reservation_key(key))
}

fn is_sync_plugin_owner_mutation(row: &SyncRowMutation) -> bool {
    if row.schema_key != "lix_key_value" {
        return false;
    }
    row.row_pk
        .as_array()
        .and_then(|parts| parts.first())
        .and_then(serde_json::Value::as_str)
        == Some(crate::plugin::runtime::PLUGIN_OWNER_KEY)
        || row
            .snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.get("key"))
            .and_then(serde_json::Value::as_str)
            == Some(crate::plugin::runtime::PLUGIN_OWNER_KEY)
}

fn is_plugin_archive_sync_file(file: &SyncFileMutation) -> bool {
    file.path
        .as_deref()
        .is_some_and(|path| path.starts_with("/.lix/plugins/"))
}

fn is_sync_semantic_row(row: &SyncRowMutation) -> bool {
    !is_sync_control_schema(&row.schema_key) && !is_file_projection_sync_schema(&row.schema_key)
}

fn sync_pack_has_semantic_rows(rows: &[SyncRowMutation]) -> bool {
    rows.iter().any(is_sync_semantic_row)
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
StorageKey(Bytes::copy_from_slice(branch_id.as_bytes()))
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
StorageKey(Bytes::from(key))
}

fn sync_write_batch(
    pack: &SyncTransactionPack,
    branch_id: &str,
    include_file_projection_rows: bool,
) -> Result<RawWriteBatch, LixError> {
    let mut rows = RawWriteBatch::with_capacity(pack.rows.len());
    // Branch refs have a foreign key to their descriptor. Canonical control
    // packs are immutable and may arrive in either storage order, so impose
    // the dependency order at the local admission boundary as well. This is
    // cheap (control packs are tiny) and prevents a transient FK failure from
    // dropping an otherwise valid branch catalog update during scale bursts.
    let mut mutations = pack
        .rows
        .iter()
        .filter(|mutation| {
            !is_engine_managed_sync_row(mutation)
                && !(!include_file_projection_rows
                    && !pack.files.is_empty()
                    && is_file_projection_sync_schema(&mutation.schema_key)
                    && mutation.snapshot.is_some())
                && !(pack.files.iter().any(is_plugin_archive_sync_file)
                    && mutation.schema_key == "lix_registered_schema")
        })
        .collect::<Vec<_>>();
    mutations.sort_by_key(|mutation| match mutation.schema_key.as_str() {
        "lix_branch_descriptor" => 0u8,
        "lix_branch_ref" => 1u8,
        _ => 2u8,
    });
    for mutation in mutations {
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
            global: mutation.global,
            change_id: None,
            commit_id: None,
            untracked: mutation.untracked,
            branch_id: if mutation.global {
                GLOBAL_BRANCH_ID.to_owned().into()
            } else {
                branch_id.to_owned().into()
            },
        });
    }
    Ok(rows)
}

/// Rewrites branch-ref control rows to the authoritative source head during
/// admission. A local replica may have a different commit identifier for the
/// same optimistic branch head; publishing that identifier would violate the
/// server's foreign-key contract. The descriptor remains byte-for-byte as
/// proposed, while the server-owned ref points at the canonical base.
pub(crate) fn canonicalize_sync_control_rows(
    pack: &SyncTransactionPack,
    canonical_base_commit_id: &str,
) -> Result<SyncTransactionPack, LixError> {
    validate_sync_transaction_pack(pack)?;
    CommitId::parse_lix(
        canonical_base_commit_id,
        "sync canonical branch base commit_id",
    )?;
    let mut canonical = pack.clone();
    if !canonical.parent_commit_ids.is_empty() {
        canonical.parent_commit_ids[0] = canonical_base_commit_id.to_owned();
        let mut seen = BTreeSet::new();
        canonical.parent_commit_ids.retain(|parent| {
            CommitId::parse_lix(parent, "sync canonical parent_commit_id").is_ok()
                && seen.insert(parent.clone())
        });
    }
    for row in &mut canonical.rows {
        if row.schema_key != "lix_branch_ref" {
            continue;
        }
        let Some(snapshot) = row.snapshot.as_mut() else {
            continue;
        };
        let object = snapshot.as_object_mut().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync branch ref snapshot must be a JSON object",
            )
        })?;
        object.insert(
            "commit_id".to_owned(),
            serde_json::Value::String(canonical_base_commit_id.to_owned()),
        );
    }
    validate_sync_transaction_pack(&canonical)?;
    Ok(canonical)
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
        CONTROL_SYNC_PULL_SCOPE, CONTROL_SYNC_SCOPE, FULL_SYNC_PULL_SCOPE, FULL_SYNC_SCOPE,
        GLOBAL_BRANCH_ID, MAX_SYNC_PACK_ROWS, SyncBranch, SyncCanonicalEvent, SyncFileMutation,
        SyncFilterScope, SyncModeState, SyncRole, SyncRowMutation, SyncTransactionPack,
        TOPOLOGY_SYNC_PULL_SCOPE, clear_sync_file_projection_metadata,
        extract_sql_scope_schema_keys, filter_sync_event, filter_sync_pack,
        is_engine_managed_sync_row, load_sync_file_projections, should_retry_canonical_row_lane,
        stage_sync_file_projection_metadata, sync_pack_fingerprint,
        validate_sync_branch_catalog_entry, validate_sync_transaction_pack,
    };
    use crate::plugin::runtime::PLUGIN_OWNER_KEY;
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    #[test]
    fn canonical_commit_pack_is_separate_from_admission_metadata() {
        let event = SyncCanonicalEvent {
            cursor: 7,
            canonical_commit_id: "0198a000-0000-7000-8000-0000000000d1".to_owned(),
            parent_commit_ids: vec!["0198a000-0000-7000-8000-0000000000d0".to_owned()],
            pack_fingerprint: "fingerprint".to_owned(),
            pack: SyncTransactionPack {
                operation_id: "client:operation".to_owned(),
                branch_id: "0198a000-0000-7000-8000-0000000000c1".to_owned(),
                base_server_commit_id: "0198a000-0000-7000-8000-0000000000d0".to_owned(),
                local_commit_id: "0198a000-0000-7000-8000-0000000000cf".to_owned(),
                parent_commit_ids: Vec::new(),
                rows: vec![SyncRowMutation {
                    schema_key: "example_row".to_owned(),
                    file_id: None,
                    row_pk: serde_json::json!(["row"]),
                    snapshot: Some(serde_json::json!({"value": "canonical"})),
                    metadata: None,
                    global: false,
                    untracked: false,
                }],
                files: Vec::new(),
            },
        };

        let commit = event.commit_pack();
        assert_eq!(commit.branch_id, event.pack.branch_id);
        assert_eq!(commit.canonical_commit_id, event.canonical_commit_id);
        assert_eq!(commit.parent_commit_ids, event.parent_commit_ids);
        assert_eq!(commit.rows, event.pack.rows);
        assert_ne!(event.pack.operation_id, commit.canonical_commit_id);
    }

    #[tokio::test]
    async fn sync_file_projection_metadata_survives_storage_round_trip() {
        let adapter = StorageAdapter::new(Memory::new());
        let branch_id = "0198a000-0000-7000-8000-0000000000a1";
        let file = SyncFileMutation {
            file_id: "file-1".to_owned(),
            path: Some("/document.md".to_owned()),
            filename: Some("document.md".to_owned()),
            global: false,
            untracked: false,
            content: b"durable projection".to_vec(),
        };
        let mut writes = adapter.new_write_set();
        stage_sync_file_projection_metadata(
            &mut writes,
            "https://sync.example/repository",
            branch_id,
            std::slice::from_ref(&file),
        )
        .expect("projection metadata stages");
        adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("projection metadata commits");

        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("projection metadata opens");
        let projections =
            load_sync_file_projections(&read, "https://sync.example/repository", branch_id)
                .await
                .expect("projection metadata loads");
        assert_eq!(projections.len(), 1);
        assert_eq!(projections[0].file_id, file.file_id);
        assert_eq!(projections[0].path, "/document.md");
        assert_eq!(projections[0].content, file.content);

        let mut clear = adapter.new_write_set();
        clear_sync_file_projection_metadata(
            &mut clear,
            "https://sync.example/repository",
            branch_id,
            [file.file_id.clone()],
        )
        .expect("projection metadata clears");
        adapter
            .commit_write_set(clear, StorageWriteOptions::default())
            .await
            .expect("projection metadata clear commits");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("cleared projection metadata opens");
        assert!(
            load_sync_file_projections(&read, "https://sync.example/repository", branch_id)
                .await
                .expect("cleared projection metadata loads")
                .is_empty()
        );
    }

    #[test]
    fn branch_catalog_rejects_untrusted_identity_and_topology() {
        let valid = SyncBranch {
            id: "0198a000-0000-7000-8000-0000000000c1".to_owned(),
            name: "feature".to_owned(),
            hidden: false,
            commit_id: "0198a000-0000-7000-8000-0000000000c2".to_owned(),
        };
        validate_sync_branch_catalog_entry(&valid).expect("valid branch catalog entry");

        let mut malformed_id = valid.clone();
        malformed_id.id = "not-a-branch-id".to_owned();
        assert!(validate_sync_branch_catalog_entry(&malformed_id).is_err());

        let mut malformed_commit = valid.clone();
        malformed_commit.commit_id.clear();
        assert!(validate_sync_branch_catalog_entry(&malformed_commit).is_err());

        let mut empty_name = valid;
        empty_name.name.clear();
        validate_sync_branch_catalog_entry(&empty_name)
            .expect("branch names retain ordinary Lix semantics");

        let mut oversized_name = empty_name;
        oversized_name.name = "x".repeat(super::MAX_SYNC_SCOPE_KEY_BYTES + 1);
        assert!(validate_sync_branch_catalog_entry(&oversized_name).is_err());
    }

    #[tokio::test]
    async fn scope_readiness_waits_only_for_the_current_query_demand() {
        let state = SyncModeState::default();
        state
            .set_role(SyncRole::Replica {
                remote_id: "https://sync.example/repository".to_owned(),
            })
            .expect("set replica role");
        let first = state.register_sql_scope_for_branch("SELECT * FROM first_scope", "main");
        let second = state.register_sql_scope_for_branch("SELECT * FROM second_scope", "main");
        let generation = state.scope_generation();
        state.mark_scope_hydrated_for_branch("main", "first_scope", generation);

        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            state.wait_for_scope_hydration_for_branch(&first, "main"),
        )
        .await
        .expect("the first query must not wait on unrelated demand")
        .expect("the first scope is hydrated");

        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(100),
                state.wait_for_scope_hydration_for_branch(&second, "main"),
            )
            .await
            .is_err(),
            "an uncached scope must remain pending"
        );
    }

    #[test]
    fn scope_demands_are_isolated_per_branch() {
        let state = SyncModeState::default();
        state
            .set_role(SyncRole::Replica {
                remote_id: "https://sync.example/repository".to_owned(),
            })
            .expect("set replica role");
        let main = state.register_sql_scope_for_branch("SELECT * FROM main_rows", "main");
        let feature = state.register_sql_scope_for_branch("SELECT * FROM feature_rows", "feature");

        assert_eq!(main, vec!["main_rows"]);
        assert_eq!(feature, vec!["feature_rows"]);
        assert_eq!(state.scopes_for_branch("main"), vec!["main_rows"]);
        assert_eq!(state.scopes_for_branch("feature"), vec!["feature_rows"]);
    }

    #[test]
    fn explicit_manual_scope_registration_is_not_hydration() {
        let state = SyncModeState::default();
        let scopes = state.register_explicit_scopes_for_branch("main", &["remote_rows"]);

        assert_eq!(scopes, vec!["remote_rows"]);
        assert_eq!(state.scopes_for_branch("main"), vec!["remote_rows"]);
        assert!(!state.scopes_are_hydrated_for_branch(&["remote_rows".to_owned()], "main"));
    }

    #[test]
    fn row_full_scope_does_not_claim_raw_file_bytes_are_hydrated() {
        let state = SyncModeState::default();
        state
            .set_role(SyncRole::Replica {
                remote_id: "https://sync.example/repository".to_owned(),
            })
            .expect("set replica role");
        let generation = state.scope_generation();
        state.mark_scope_hydrated_for_branch("main", FULL_SYNC_SCOPE, generation);

        assert!(state.scopes_are_hydrated_for_branch(&["sync_mode_row".to_owned()], "main"));
        assert!(!state.scopes_are_hydrated_for_branch(&["lix_file".to_owned()], "main"));

        state.mark_scope_hydrated_for_branch("main", "lix_file", generation);
        assert!(state.scopes_are_hydrated_for_branch(&["lix_file".to_owned()], "main"));
    }

    #[test]
    fn comma_from_lists_fail_closed_to_full_sync_scope() {
        assert_eq!(
            extract_sql_scope_schema_keys(
                "SELECT * FROM first_rows, second_rows WHERE first_rows.id = second_rows.id",
            ),
            vec![FULL_SYNC_SCOPE]
        );
        assert_eq!(
            extract_sql_scope_schema_keys("SELECT * FROM first_rows WHERE id IN (1, 2)"),
            vec!["first_rows"]
        );
    }

    #[tokio::test]
    async fn canonical_event_marker_makes_replay_after_manifest_loss_a_noop() {
        let lix = crate::open_lix().await.expect("open replica");
        let branch_id = lix.active_branch_id().await.expect("active branch");
        let event = SyncCanonicalEvent {
            cursor: 1,
            canonical_commit_id: "canonical-1".to_owned(),
            parent_commit_ids: Vec::new(),
            pack_fingerprint: String::new(),
            pack: SyncTransactionPack {
                operation_id: "operation-1".to_owned(),
                branch_id,
                base_server_commit_id: "base".to_owned(),
                local_commit_id: "local".to_owned(),
                parent_commit_ids: Vec::new(),
                rows: vec![SyncRowMutation {
                    schema_key: "lix_key_value".to_owned(),
                    file_id: None,
                    row_pk: serde_json::json!(["marker-replay"]),
                    snapshot: Some(serde_json::json!({
                        "key": "marker-replay",
                        "value": "once"
                    })),
                    metadata: None,
                    global: false,
                    untracked: false,
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
            parent_commit_ids: Vec::new(),
            pack_fingerprint: String::new(),
            pack: SyncTransactionPack {
                operation_id: "operation-marker-order".to_owned(),
                branch_id,
                base_server_commit_id: "base".to_owned(),
                local_commit_id: "local".to_owned(),
                parent_commit_ids: Vec::new(),
                rows: vec![
                    // A real file-backed canonical event carries the
                    // descriptor and blob-reference rows alongside plugin or
                    // application rows. The semantic scope retains these
                    // small identity rows while omitting only the byte
                    // payload, allowing the later file scope to hydrate CAS.
                    SyncRowMutation {
                        schema_key: "lix_file_descriptor".to_owned(),
                        file_id: None,
                        row_pk: serde_json::json!([file_id]),
                        snapshot: Some(serde_json::json!({
                            "id": file_id,
                            "directory_id": null,
                            "name": "marker-order.txt"
                        })),
                        metadata: None,
                        global: false,
                        untracked: false,
                    },
                    SyncRowMutation {
                        schema_key: "lix_binary_blob_ref".to_owned(),
                        file_id: Some(file_id.to_owned()),
                        row_pk: serde_json::json!([file_id]),
                        snapshot: Some(serde_json::json!({
                            "id": file_id,
                            "blob_hash": crate::binary_cas::BlobId::from_content(b"rendered")
                                .to_hex(),
                            "size_bytes": 8
                        })),
                        metadata: None,
                        global: false,
                        untracked: false,
                    },
                    SyncRowMutation {
                        schema_key: "sync_marker_row".to_owned(),
                        file_id: Some(file_id.to_owned()),
                        row_pk: serde_json::json!(["row-1"]),
                        snapshot: Some(serde_json::json!({"id": "row-1", "value": "one"})),
                        metadata: None,
                        global: false,
                        untracked: false,
                    },
                ],
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

        // Exercise the same filtered payloads that a scoped server pull
        // returns. Semantic hydration owns the row but deliberately leaves
        // the raw file payload for a later file demand.
        let semantic_keys = vec!["sync_marker_row".to_owned()];
        let semantic_event =
            filter_sync_event(event.clone(), Some(&SyncFilterScope::new(&semantic_keys)));
        lix.apply_sync_canonical_event(&semantic_event, "remote-order", &semantic_keys)
            .await
            .expect("apply semantic scope");
        let file_keys = vec!["lix_file".to_owned()];
        let file_event = filter_sync_event(event.clone(), Some(&SyncFilterScope::new(&file_keys)));
        lix.apply_sync_canonical_event(&file_event, "remote-order", &file_keys)
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

        // A fresh session must restore the durable lazy projection rather
        // than requiring another network pull before an offline read.
        lix.set_sync_role(SyncRole::Replica {
            remote_id: "remote-order".to_owned(),
        })
        .expect("set replica role for fresh-session restore");
        let branch_id = lix.active_branch_id().await.expect("fresh-session branch");
        let generation = lix.sync_mode_state().scope_generation();
        lix.sync_mode_state()
            .mark_scope_hydrated_for_branch(&branch_id, "lix_file", generation);
        lix.sync_mode_state().mark_scope_hydrated_for_branch(
            &branch_id,
            CONTROL_SYNC_SCOPE,
            generation,
        );
        let fresh_session = lix
            .open_another_session()
            .await
            .expect("open fresh replica session");
        let fresh_file = fresh_session
            .execute(
                "SELECT content FROM lix_file WHERE path = '/marker-order.txt'",
                &[],
            )
            .await
            .expect("read file payload from fresh session");
        assert_eq!(
            fresh_file.rows()[0].get::<Vec<u8>>("content").unwrap(),
            b"rendered"
        );
        fresh_session
            .close()
            .await
            .expect("close fresh replica session");

        let before = lix
            .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
            .await
            .expect("read marker-order head")
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        lix.apply_sync_canonical_event(&semantic_event, "remote-order", &semantic_keys)
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
            extract_sql_scope_schema_keys("SELECT value FROM lix_registered_schema"),
            vec!["lix_registered_schema".to_owned()]
        );
        assert_eq!(
            extract_sql_scope_schema_keys("SELECT id, commit_id FROM lix_branch"),
            vec![CONTROL_SYNC_SCOPE.to_owned()]
        );
        assert_eq!(
            extract_sql_scope_schema_keys("SELECT key, value FROM lix_key_value"),
            vec!["lix_key_value".to_owned()]
        );
        assert_eq!(
            extract_sql_scope_schema_keys("SELECT lix_active_branch_commit_id()"),
            vec![CONTROL_SYNC_SCOPE.to_owned()]
        );
        assert_eq!(
            extract_sql_scope_schema_keys(
                "SELECT lix_active_branch_id(), value FROM project_rows"
            ),
            vec![CONTROL_SYNC_SCOPE.to_owned(), "project_rows".to_owned()]
        );
        assert_eq!(
            extract_sql_scope_schema_keys("SELECT * FROM lix_diff"),
            vec![FULL_SYNC_SCOPE.to_owned()]
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
            global: false,
            untracked: false,
        };
        let pack = SyncTransactionPack {
            operation_id: "op".to_owned(),
            branch_id: "branch".to_owned(),
            base_server_commit_id: "base".to_owned(),
            local_commit_id: "local".to_owned(),
            parent_commit_ids: Vec::new(),
            rows: vec![row; MAX_SYNC_PACK_ROWS + 1],
            files: Vec::new(),
        };
        let error = validate_sync_transaction_pack(&pack).expect_err("row fanout is bounded");
        assert_eq!(error.code, crate::LixError::CODE_INVALID_PARAM);
    }

    #[test]
    fn scoped_projection_retains_a_matching_commit_as_an_atomic_pack() {
        let row = |schema_key: &str| SyncRowMutation {
            schema_key: schema_key.to_owned(),
            file_id: None,
            row_pk: serde_json::json!([schema_key]),
            snapshot: Some(serde_json::json!({"value": schema_key})),
            metadata: None,
            global: false,
            untracked: false,
        };
        let mut pack = SyncTransactionPack {
            operation_id: "op".to_owned(),
            branch_id: "branch".to_owned(),
            base_server_commit_id: "base".to_owned(),
            local_commit_id: "local".to_owned(),
            parent_commit_ids: Vec::new(),
            rows: vec![row("first"), row("second")],
            files: Vec::new(),
        };
        let scope_keys = vec!["first".to_owned()];
        let scope = SyncFilterScope::new(&scope_keys);
        filter_sync_pack(&mut pack, &scope);
        assert_eq!(pack.rows.len(), 2);
        assert_eq!(pack.rows[0].schema_key, "first");
        assert_eq!(pack.rows[1].schema_key, "second");

        let mut unrelated = pack.clone();
        let unrelated_keys = ["missing".to_owned()];
        let unrelated_scope = SyncFilterScope::new(&unrelated_keys);
        filter_sync_pack(&mut unrelated, &unrelated_scope);
        assert!(unrelated.rows.is_empty());
        assert!(unrelated.files.is_empty());

        let file = |path: &str| SyncFileMutation {
            file_id: path.to_owned(),
            path: Some(path.to_owned()),
            filename: None,
            global: false,
            untracked: false,
            content: vec![1, 2, 3],
        };
        let mut catalog_files = SyncTransactionPack {
            operation_id: "catalog-files".to_owned(),
            branch_id: "branch".to_owned(),
            base_server_commit_id: "base".to_owned(),
            local_commit_id: "local".to_owned(),
            parent_commit_ids: Vec::new(),
            rows: Vec::new(),
            files: vec![
                file("/.lix/plugins/plugin_markdown.lixplugin"),
                file("/project.md"),
            ],
        };
        let catalog_keys = ["lix_registered_schema".to_owned()];
        filter_sync_pack(&mut catalog_files, &SyncFilterScope::new(&catalog_keys));
        assert_eq!(catalog_files.files.len(), 1);
        assert_eq!(
            catalog_files.files[0].path.as_deref(),
            Some("/.lix/plugins/plugin_markdown.lixplugin")
        );

        let mut full_history = SyncTransactionPack {
            operation_id: "full-history".to_owned(),
            branch_id: "branch".to_owned(),
            base_server_commit_id: "base".to_owned(),
            local_commit_id: "local".to_owned(),
            parent_commit_ids: Vec::new(),
            rows: vec![row("history_rows")],
            files: vec![
                file("/.lix/plugins/plugin_markdown.lixplugin"),
                file("/project.md"),
            ],
        };
        filter_sync_pack(
            &mut full_history,
            &SyncFilterScope::new(&[FULL_SYNC_PULL_SCOPE.to_owned()]),
        );
        assert_eq!(full_history.rows.len(), 1);
        assert_eq!(full_history.files.len(), 1);
        assert_eq!(
            full_history.files[0].path.as_deref(),
            Some("/.lix/plugins/plugin_markdown.lixplugin")
        );

        let mut topology = full_history.clone();
        filter_sync_pack(
            &mut topology,
            &SyncFilterScope::new(&[TOPOLOGY_SYNC_PULL_SCOPE.to_owned()]),
        );
        assert!(topology.rows.is_empty());
        assert!(topology.files.is_empty());
    }

    #[test]
    fn control_projection_keeps_catalog_rows_without_application_rows() {
        let mut pack = SyncTransactionPack {
            operation_id: "control-projection".to_owned(),
            branch_id: GLOBAL_BRANCH_ID.to_owned(),
            base_server_commit_id: "base".to_owned(),
            local_commit_id: "local".to_owned(),
            parent_commit_ids: Vec::new(),
            rows: vec![
                SyncRowMutation {
                    schema_key: "lix_branch_descriptor".to_owned(),
                    file_id: None,
                    row_pk: serde_json::json!(["0198a000-0000-7000-8000-0000000000d1"]),
                    snapshot: Some(serde_json::json!({
                        "id": "0198a000-0000-7000-8000-0000000000d1",
                        "name": "feature",
                        "hidden": false
                    })),
                    metadata: None,
                    global: true,
                    untracked: false,
                },
                SyncRowMutation {
                    schema_key: "application_row".to_owned(),
                    file_id: None,
                    row_pk: serde_json::json!(["row-1"]),
                    snapshot: Some(serde_json::json!({"value": "not-control"})),
                    metadata: None,
                    global: false,
                    untracked: false,
                },
            ],
            files: vec![SyncFileMutation {
                file_id: "file".to_owned(),
                path: Some("/project.txt".to_owned()),
                filename: Some("project.txt".to_owned()),
                global: false,
                untracked: false,
                content: b"bytes".to_vec(),
            }],
        };
        filter_sync_pack(
            &mut pack,
            &SyncFilterScope::new(&[CONTROL_SYNC_PULL_SCOPE.to_owned()]),
        );
        assert_eq!(pack.rows.len(), 1);
        assert_eq!(pack.rows[0].schema_key, "lix_branch_descriptor");
        assert!(pack.files.is_empty());

        // The local readiness marker uses a different identity from the wire
        // token, but it must apply the same projection if a caller filters a
        // canonical event at the application boundary.
        let mut marker_projection = pack.clone();
        marker_projection.rows.push(SyncRowMutation {
            schema_key: "application_row".to_owned(),
            file_id: None,
            row_pk: serde_json::json!(["row-2"]),
            snapshot: Some(serde_json::json!({"value": "not-control"})),
            metadata: None,
            global: false,
            untracked: false,
        });
        filter_sync_pack(
            &mut marker_projection,
            &SyncFilterScope::new(&[CONTROL_SYNC_SCOPE.to_owned()]),
        );
        assert_eq!(marker_projection.rows.len(), 1);
        assert_eq!(
            marker_projection.rows[0].schema_key,
            "lix_branch_descriptor"
        );
    }

    #[test]
    fn semantic_scope_keeps_canonical_plugin_rows_without_source_bytes() {
        let mut pack = SyncTransactionPack {
            operation_id: "plugin-row-scope".to_owned(),
            branch_id: "branch".to_owned(),
            base_server_commit_id: "base".to_owned(),
            local_commit_id: "local".to_owned(),
            parent_commit_ids: Vec::new(),
            rows: vec![
                SyncRowMutation {
                    schema_key: "lix_file_descriptor".to_owned(),
                    file_id: None,
                    row_pk: serde_json::json!(["file-id"]),
                    snapshot: Some(serde_json::json!({"id": "file-id"})),
                    metadata: None,
                    global: false,
                    untracked: false,
                },
                SyncRowMutation {
                    schema_key: "lix_key_value".to_owned(),
                    file_id: Some("file-id".to_owned()),
                    row_pk: serde_json::json!([PLUGIN_OWNER_KEY]),
                    snapshot: Some(serde_json::json!({
                        "key": PLUGIN_OWNER_KEY,
                        "value": {
                            "version": 1,
                            "plugin_key": "plugin_markdown",
                            "schema_keys": ["markdown_node"]
                        }
                    })),
                    metadata: None,
                    global: false,
                    untracked: false,
                },
                SyncRowMutation {
                    schema_key: "markdown_node".to_owned(),
                    file_id: Some("file-id".to_owned()),
                    row_pk: serde_json::json!(["node-1"]),
                    snapshot: Some(serde_json::json!({"id": "node-1"})),
                    metadata: None,
                    global: false,
                    untracked: false,
                },
            ],
            files: vec![SyncFileMutation {
                file_id: "file-id".to_owned(),
                path: Some("/document.md".to_owned()),
                filename: Some("document.md".to_owned()),
                global: false,
                untracked: false,
                content: b"source bytes are a projection".to_vec(),
            }],
        };

        let keys = vec!["markdown_node".to_owned()];
        filter_sync_pack(&mut pack, &SyncFilterScope::new(&keys));

        assert!(
            pack.files.is_empty(),
            "row hydration must not transfer bytes"
        );
        assert!(
            pack.rows
                .iter()
                .any(|row| row.schema_key == "markdown_node"),
            "semantic rows are the canonical row-scope payload"
        );
        assert!(
            pack.rows.iter().any(|row| {
                row.schema_key == "lix_key_value"
                    && row.row_pk == serde_json::json!([PLUGIN_OWNER_KEY])
            }),
            "plugin ownership metadata must bootstrap a fresh row replica"
        );
    }

    #[test]
    fn engine_managed_sync_keys_check_both_row_and_snapshot_identities() {
        let reserved = |row_pk, snapshot| SyncRowMutation {
            schema_key: "lix_key_value".to_owned(),
            file_id: None,
            row_pk,
            snapshot,
            metadata: None,
            global: false,
            untracked: false,
        };
        assert!(is_engine_managed_sync_row(&reserved(
            serde_json::json!(["not-reserved"]),
            Some(serde_json::json!({"key": "lix_plugin_registry_v2"})),
        )));
        assert!(is_engine_managed_sync_row(&reserved(
            serde_json::json!(["lix_plugin_create_v1:0123456789abcdef01234567"]),
            Some(serde_json::json!({"key": "not-reserved"})),
        )));
        assert!(!is_engine_managed_sync_row(&reserved(
            serde_json::json!([PLUGIN_OWNER_KEY]),
            Some(serde_json::json!({"key": PLUGIN_OWNER_KEY})),
        )));
    }

    #[test]
    fn canonical_row_fallback_is_limited_to_local_plugin_materialization_errors() {
        let pack = SyncTransactionPack {
            operation_id: "fallback".to_owned(),
            branch_id: "branch".to_owned(),
            base_server_commit_id: "base".to_owned(),
            local_commit_id: "local".to_owned(),
            parent_commit_ids: Vec::new(),
            rows: vec![SyncRowMutation {
                schema_key: "plugin_row".to_owned(),
                file_id: Some("file".to_owned()),
                row_pk: serde_json::json!(["row"]),
                snapshot: Some(serde_json::json!({"id": "row"})),
                metadata: None,
                global: false,
                untracked: false,
            }],
            files: Vec::new(),
        };
        assert!(should_retry_canonical_row_lane(
            &pack,
            &crate::LixError::new(
                crate::LixError::CODE_INTERNAL_ERROR,
                "component materialization root is missing change_id",
            )
        ));
        assert!(!should_retry_canonical_row_lane(
            &pack,
            &crate::LixError::new(
                crate::LixError::CODE_SCHEMA_VALIDATION,
                "plugin row has an invalid value",
            )
        ));
        assert!(should_retry_canonical_row_lane(
            &pack,
            &crate::LixError::new(
                crate::LixError::CODE_CONSTRAINT_VIOLATION,
                "plugin-owned schema 'plugin_row' cannot be written for unowned file 'file'",
            )
        ));
    }
}
