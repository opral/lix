use crate::{Config, config::SlateDBCacheConfig};
use anyhow::{Context, Result};
use axum::{body::Body, http::Request};
use blake3::Hasher;
use fs2::FileExt as _;
use futures_util::TryStreamExt as _;
use lix_sdk::server_protocol::{
    LixServerProtocol, ServerProtocolBody, ServerProtocolContext, ServerProtocolResponse,
};
use lix_slatedb_storage::{
    SlateDB, SlateDBCacheOptions, SlateDBIoCounters, SlateDBObjectStoreOptions,
};
#[cfg(test)]
use object_store::memory::InMemory;
#[cfg(test)]
use object_store::{ClientConfigKey, ObjectStoreExt, path::Path as ObjectPath};
use object_store::{ClientOptions, ObjectStore, RetryConfig, aws::AmazonS3Builder};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
#[cfg(test)]
use tokio::sync::Notify;
use tokio::sync::{Mutex, OnceCell, watch};
use tokio::task::JoinSet;
use tracing::{Instrument, info, info_span};

#[cfg(test)]
fn test_telemetry_sink() -> Arc<dyn lix_sdk::telemetry::TelemetrySink> {
    Arc::new(lix_sdk::telemetry::CallbackTelemetrySink::new(|_| {}))
}

// The Lix SlateDB adapter fetches object-store cache parts in 2 MiB chunks.
// A 15 second request timeout still permits a part to arrive at roughly 140
// KiB/s, while a single stalled request can no longer occupy a lix runtime
// for the object_store default three-minute retry budget. One retry preserves recovery
// from a transient slow path without turning an interactive request into a
// multi-minute wait.
const S3_REQUEST_BUDGET: S3RequestBudget = S3RequestBudget {
    request_timeout: Duration::from_secs(15),
    connect_timeout: Duration::from_secs(3),
    retry_timeout: Duration::from_secs(30),
    max_retries: 1,
};

const CACHE_NAMESPACE_LAYOUT: &[u8] = b"lix-server-slatedb-cache\0v1";
// This cannot be a legacy Lix cache child: Lix IDs do not permit dots.
const CACHE_NAMESPACE_PARENT: &str = ".lix-server-cache-v1";

// These identifiers describe the physical store opened by this manager, not a
// benchmark caller's requested cohort. Keep them alongside the adapter so a
// future backend or layout change must update the server attestation too.
const STORAGE_BACKEND: &str = "slatedb";
const STORAGE_LAYOUT: &str = "lix-slatedb-lz4-v1";

#[derive(Clone, Copy)]
struct S3RequestBudget {
    request_timeout: Duration,
    connect_timeout: Duration,
    retry_timeout: Duration,
    max_retries: usize,
}

pub(crate) struct LixService {
    protocol: LixServerProtocol<SlateDB>,
}

struct LixRuntime {
    state: Mutex<LixRuntimeState>,
}

enum LixRuntimeState {
    Active(Arc<LixService>),
    Recovering(Arc<LixService>),
    Evicting,
}

pub struct LixRuntimeManager {
    backend: StorageBackend,
    max_open_lixes: usize,
    recovery_watchdog: RecoveryWatchdog,
    state: Mutex<ManagerState>,
    telemetry: Arc<dyn lix_sdk::telemetry::TelemetrySink>,
    #[cfg(test)]
    open_gate: Option<TestOpenGate>,
    // Keep this last: struct fields drop in declaration order. The exclusive
    // cache-root lease must outlive every retained Lix runtime and its
    // SlateDB workers during manager teardown.
    _cache_root_lease: Option<CacheRootLease>,
}

impl fmt::Debug for LixRuntimeManager {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LixRuntimeManager")
            .field("storage_backend", &self.storage_backend())
            .field("storage_layout", &self.storage_layout())
            .field("max_open_lixes", &self.max_open_lixes)
            .finish_non_exhaustive()
    }
}

type RecoveryTimeoutAction = dyn Fn() + Send + Sync;

struct RecoveryWatchdog {
    timeout: Duration,
    on_timeout: Arc<RecoveryTimeoutAction>,
}

impl RecoveryWatchdog {
    fn production(timeout: Duration) -> Self {
        Self {
            timeout,
            on_timeout: Arc::new(|| std::process::abort()),
        }
    }

    #[cfg(test)]
    fn test_default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            on_timeout: Arc::new(|| panic!("recovery close watchdog fired in a test")),
        }
    }
}

#[derive(Clone)]
enum StorageBackend {
    #[cfg(test)]
    Memory { object_store: Arc<dyn ObjectStore> },
    S3 {
        object_store: Arc<dyn ObjectStore>,
        prefix: String,
        cache: SlateDBCacheOptions,
    },
}

#[derive(Default)]
struct ManagerState {
    entries: HashMap<String, RuntimeEntry>,
    cleaning: HashMap<String, CleanupTombstone>,
    failed_upgrades: HashMap<String, FailedUpgrade>,
    shutting_down: bool,
    clock: u64,
    cleanup_sequence: u64,
    #[cfg(test)]
    state_drop_probe: Option<CacheRootLeaseDropProbe>,
}

#[derive(Clone, Copy)]
struct FailedMigration {
    from_version: u32,
    to_version: u32,
}

#[derive(Clone, Copy)]
enum FailedUpgrade {
    Versioned(FailedMigration),
    Unversioned,
}

#[cfg(test)]
struct CacheRootLeaseDropProbe {
    root: PathBuf,
    observed_lease_held: Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(test)]
impl Drop for CacheRootLeaseDropProbe {
    fn drop(&mut self) {
        self.observed_lease_held.store(
            CacheRootLease::acquire(&self.root).is_err(),
            std::sync::atomic::Ordering::SeqCst,
        );
    }
}

struct RuntimeEntry {
    runtime: Arc<OnceCell<Arc<LixRuntime>>>,
    opened: watch::Receiver<RuntimeOpenState>,
    last_used: u64,
}

#[derive(Debug)]
struct CacheRootLease {
    _owner_lock: File,
}

struct CleanupTombstone {
    sequence: u64,
    done: watch::Receiver<CleanupState>,
}

struct EvictedRuntime {
    lix_id: String,
    runtime: Arc<LixRuntime>,
    service: Arc<LixService>,
    sequence: u64,
    done: watch::Sender<CleanupState>,
}

#[derive(Clone)]
enum CleanupState {
    Running,
    Complete,
    Failed(Arc<str>),
}

#[derive(Clone)]
enum RuntimeOpenState {
    Opening,
    Migrating { from_version: u32, to_version: u32 },
    MigrationFailed { from_version: u32, to_version: u32 },
    UpgradeFailed,
    Ready,
    Failed(Arc<str>),
}

fn runtime_open_state_from_progress(progress: lix_sdk::OpenProgress) -> Option<RuntimeOpenState> {
    match progress.phase {
        lix_sdk::OpenPhase::Migrating | lix_sdk::OpenPhase::Validating => {
            progress
                .from_format
                .map(|from_version| RuntimeOpenState::Migrating {
                    from_version,
                    to_version: progress.to_format,
                })
        }
        lix_sdk::OpenPhase::Opening => Some(RuntimeOpenState::Opening),
        _ => None,
    }
}

fn runtime_error_from_failed_upgrade(failure: FailedUpgrade) -> LixRuntimeError {
    match failure {
        FailedUpgrade::Versioned(failure) => LixRuntimeError::MigrationFailed {
            from_version: failure.from_version,
            to_version: failure.to_version,
        },
        FailedUpgrade::Unversioned => LixRuntimeError::UpgradeFailed,
    }
}

fn is_repository_upgrade_failure(error: &anyhow::Error) -> bool {
    error.chain().any(|source| {
        source
            .downcast_ref::<lix_sdk::LixError>()
            .is_some_and(|error| error.code == "LIX_ERROR_MIGRATION_FAILED")
    })
}

struct PendingRuntimeOpen {
    lix_id: String,
    runtime: Arc<OnceCell<Arc<LixRuntime>>>,
    done: watch::Sender<RuntimeOpenState>,
}

#[cfg(test)]
#[derive(Clone)]
struct TestOpenGate {
    started: Arc<Notify>,
    release: Arc<Notify>,
    starts: Arc<std::sync::atomic::AtomicUsize>,
    fail_next: Arc<std::sync::atomic::AtomicBool>,
}

enum GetRuntimeAction {
    WaitForCleanup(watch::Receiver<CleanupState>),
    EvictAndWait {
        evicted: EvictedRuntime,
        cleanup: watch::Receiver<CleanupState>,
    },
    WaitForOpen {
        runtime: Arc<OnceCell<Arc<LixRuntime>>>,
        opened: watch::Receiver<RuntimeOpenState>,
    },
}

#[derive(Debug)]
pub(crate) enum LixRuntimeError {
    InvalidId,
    AtCapacity { max: usize },
    Migrating { from_version: u32, to_version: u32 },
    MigrationFailed { from_version: u32, to_version: u32 },
    UpgradeFailed,
    Recovering,
    ShuttingDown,
    Cleanup(Arc<str>),
    Open(anyhow::Error),
}

impl LixRuntimeManager {
    pub fn new(
        config: &Config,
        telemetry: Arc<dyn lix_sdk::telemetry::TelemetrySink>,
    ) -> Result<Arc<Self>> {
        let storage = &config.storage;
        let request_budget = S3_REQUEST_BUDGET;
        // The configured root is the single-process ownership boundary. The
        // derived namespace is only where this backend's cached bytes live.
        // Keeping these paths distinct prevents a backend change from
        // bypassing cache ownership or consuming stale cached object data.
        let owner_root = &storage.cache.root_folder;
        let cache_root_lease = CacheRootLease::acquire(owner_root)?;
        let namespace_root = cache_namespace_root(
            owner_root,
            &storage.endpoint,
            &storage.bucket,
            &storage.region,
            &storage.prefix,
            &storage.access_key_id,
        );
        prepare_cache_namespace(owner_root, &namespace_root)?;
        let cache = cache_options(&storage.cache, config.max_open_lixes, namespace_root);
        let object_store: Arc<dyn ObjectStore> = Arc::new(
            AmazonS3Builder::new()
                .with_endpoint(&storage.endpoint)
                .with_bucket_name(&storage.bucket)
                .with_access_key_id(&storage.access_key_id)
                .with_secret_access_key(&storage.secret_access_key)
                .with_region(&storage.region)
                .with_virtual_hosted_style_request(false)
                .with_client_options(s3_client_options(request_budget))
                .with_retry(s3_retry_config(request_budget))
                .with_allow_http(storage.allow_http)
                .build()
                .context("build S3 object store")?,
        );
        let backend = StorageBackend::S3 {
            object_store,
            prefix: storage.prefix.clone(),
            cache,
        };

        Ok(Arc::new(Self {
            backend,
            max_open_lixes: config.max_open_lixes,
            recovery_watchdog: RecoveryWatchdog::production(config.recovery_close_timeout),
            state: Mutex::new(ManagerState::default()),
            telemetry,
            #[cfg(test)]
            open_gate: None,
            _cache_root_lease: Some(cache_root_lease),
        }))
    }

    #[cfg(test)]
    pub(crate) fn new_in_memory(max_open_lixes: usize) -> Arc<Self> {
        Arc::new(Self {
            backend: StorageBackend::Memory {
                object_store: Arc::new(InMemory::new()),
            },
            max_open_lixes,
            recovery_watchdog: RecoveryWatchdog::test_default(),
            state: Mutex::new(ManagerState::default()),
            telemetry: test_telemetry_sink(),
            open_gate: None,
            _cache_root_lease: None,
        })
    }

    pub(crate) fn storage_backend(&self) -> &'static str {
        STORAGE_BACKEND
    }

    pub(crate) fn storage_layout(&self) -> &'static str {
        STORAGE_LAYOUT
    }

    pub(crate) async fn get(
        self: &Arc<Self>,
        lix_id: &str,
    ) -> std::result::Result<Arc<LixService>, LixRuntimeError> {
        if !valid_lix_id(lix_id) {
            return Err(LixRuntimeError::InvalidId);
        }

        let runtime_cell = 'select_runtime: loop {
            let action = {
                let mut state = self.state.lock().await;
                if state.shutting_down {
                    return Err(LixRuntimeError::ShuttingDown);
                } else if let Some(cleanup) = state.cleaning.get(lix_id) {
                    GetRuntimeAction::WaitForCleanup(cleanup.done.clone())
                } else if let Some(failure) = state.failed_upgrades.get(lix_id) {
                    return Err(runtime_error_from_failed_upgrade(*failure));
                } else if state.entries.contains_key(lix_id) {
                    state.clock = state.clock.wrapping_add(1);
                    let now = state.clock;
                    let entry = state
                        .entries
                        .get_mut(lix_id)
                        .expect("entry was present when updating Lix recency");
                    entry.last_used = now;
                    GetRuntimeAction::WaitForOpen {
                        runtime: Arc::clone(&entry.runtime),
                        opened: entry.opened.clone(),
                    }
                } else if state.entries.len() + state.cleaning.len() >= self.max_open_lixes {
                    // An eviction remains a live SlateDB runtime until its
                    // protocol has closed and its cache child is retired.
                    // Count it against the same cap so churn cannot bypass
                    // the server-wide resource bound with closing runtimes.
                    if let Some(cleanup) = state
                        .cleaning
                        .values()
                        .min_by_key(|cleanup| cleanup.sequence)
                    {
                        GetRuntimeAction::WaitForCleanup(cleanup.done.clone())
                    } else {
                        let Some((eviction_id, runtime, service)) =
                            take_idle_lru_runtime(&mut state.entries)
                        else {
                            return Err(LixRuntimeError::AtCapacity {
                                max: self.max_open_lixes,
                            });
                        };
                        let (sequence, done) = start_cleanup(&mut state, eviction_id.clone())
                            .expect("an active runtime cannot already be cleaning");
                        let cleanup = state
                            .cleaning
                            .get(&eviction_id)
                            .expect("started cleanup must retain a waiter")
                            .done
                            .clone();
                        GetRuntimeAction::EvictAndWait {
                            evicted: EvictedRuntime {
                                lix_id: eviction_id,
                                runtime,
                                service,
                                sequence,
                                done,
                            },
                            cleanup,
                        }
                    }
                } else {
                    state.clock = state.clock.wrapping_add(1);
                    let now = state.clock;
                    let runtime = Arc::new(OnceCell::new());
                    let (done, opened) = watch::channel(RuntimeOpenState::Opening);
                    state.entries.insert(
                        lix_id.to_string(),
                        RuntimeEntry {
                            runtime: Arc::clone(&runtime),
                            opened: opened.clone(),
                            last_used: now,
                        },
                    );
                    // The manager, rather than this request, owns opening.
                    // `get` callers may be cancelled without leaving an
                    // empty entry or detaching a second same-ID opener.
                    self.spawn_runtime_open(PendingRuntimeOpen {
                        lix_id: lix_id.to_string(),
                        runtime: Arc::clone(&runtime),
                        done,
                    });
                    GetRuntimeAction::WaitForOpen { runtime, opened }
                }
            };

            match action {
                GetRuntimeAction::WaitForCleanup(mut cleanup) => {
                    // The sender is dropped after the old runtime has closed
                    // and its cache child has been retired and deleted.
                    // `watch` makes this wait immune to a completion racing
                    // with receiver registration.
                    self.wait_for_cleanup(&mut cleanup).await?;
                }
                GetRuntimeAction::EvictAndWait {
                    evicted,
                    mut cleanup,
                } => {
                    self.spawn_eviction_cleanup(evicted);
                    self.wait_for_cleanup(&mut cleanup).await?;
                }
                GetRuntimeAction::WaitForOpen {
                    runtime,
                    mut opened,
                } => loop {
                    let open_state = opened.borrow().clone();
                    match open_state {
                        RuntimeOpenState::Ready => break 'select_runtime runtime,
                        RuntimeOpenState::Failed(error) => {
                            return Err(LixRuntimeError::Open(anyhow::anyhow!(
                                "open lix runtime: {error}"
                            )));
                        }
                        RuntimeOpenState::Migrating {
                            from_version,
                            to_version,
                        } => {
                            return Err(LixRuntimeError::Migrating {
                                from_version,
                                to_version,
                            });
                        }
                        RuntimeOpenState::MigrationFailed {
                            from_version,
                            to_version,
                        } => {
                            return Err(LixRuntimeError::MigrationFailed {
                                from_version,
                                to_version,
                            });
                        }
                        RuntimeOpenState::UpgradeFailed => {
                            return Err(LixRuntimeError::UpgradeFailed);
                        }
                        RuntimeOpenState::Opening => {
                            if opened.changed().await.is_err() {
                                return Err(LixRuntimeError::Open(anyhow::anyhow!(
                                    "lix runtime opener stopped before completing"
                                )));
                            }
                        }
                    }
                },
            }
        };

        let runtime = runtime_cell
            .get()
            .expect("runtime opener reported success before storing its runtime");
        if self.state.lock().await.shutting_down {
            return Err(LixRuntimeError::ShuttingDown);
        }
        match runtime.acquire().await {
            Some(service) => Ok(service),
            None => Err(LixRuntimeError::Recovering),
        }
    }

    fn spawn_runtime_open(self: &Arc<Self>, opener: PendingRuntimeOpen) {
        let manager = Arc::clone(self);
        let span = info_span!(
            "lix.runtime.open",
            lix.id = %opener.lix_id,
            storage.backend = STORAGE_BACKEND,
        );
        tokio::spawn(
            async move {
                let opened = manager
                    .open_lix_for_handler(opener.lix_id.clone(), opener.done.clone())
                    .await;
                match opened {
                    Ok(runtime) => {
                        assert!(
                            opener.runtime.set(runtime).is_ok(),
                            "managed opener is the sole OnceCell initializer"
                        );
                        opener.done.send_replace(RuntimeOpenState::Ready);
                    }
                    Err(error) => {
                        let failed_upgrade = match opener.done.borrow().clone() {
                            RuntimeOpenState::Migrating {
                                from_version,
                                to_version,
                            } => Some(FailedUpgrade::Versioned(FailedMigration {
                                from_version,
                                to_version,
                            })),
                            _ if is_repository_upgrade_failure(&error) => {
                                Some(FailedUpgrade::Unversioned)
                            }
                            _ => None,
                        };
                        // Preserve the complete anyhow/Lix error chain after it crosses
                        // this async opener boundary while keeping one physical log line.
                        let error = Arc::<str>::from(format!("{error:#}").replace('\n', " | "));
                        if let Some(failure) = failed_upgrade {
                            match failure {
                                FailedUpgrade::Versioned(failure) => {
                                    tracing::error!(
                                        lix_id = %opener.lix_id,
                                        from_version = failure.from_version,
                                        to_version = failure.to_version,
                                        error = %error,
                                        "Lix repository migration failed"
                                    );
                                    opener.done.send_replace(RuntimeOpenState::MigrationFailed {
                                        from_version: failure.from_version,
                                        to_version: failure.to_version,
                                    });
                                }
                                FailedUpgrade::Unversioned => {
                                    tracing::error!(
                                        lix_id = %opener.lix_id,
                                        error = %error,
                                        "Lix repository upgrade failed"
                                    );
                                    opener.done.send_replace(RuntimeOpenState::UpgradeFailed);
                                }
                            }
                        }
                        if let Some((sequence, done)) = manager
                            .begin_failed_open_cleanup(&opener.lix_id, &opener.runtime)
                            .await
                        {
                            let cleanup = manager
                                .retire_and_delete_cache_child(&opener.lix_id, sequence)
                                .await;
                            if cleanup.is_ok() {
                                if let Some(failure) = failed_upgrade {
                                    manager
                                        .state
                                        .lock()
                                        .await
                                        .failed_upgrades
                                        .insert(opener.lix_id.clone(), failure);
                                }
                            }
                            manager
                                .finish_cleanup(&opener.lix_id, sequence, done, cleanup)
                                .await;
                        }
                        if failed_upgrade.is_none() {
                            opener.done.send_replace(RuntimeOpenState::Failed(error));
                        }
                    }
                }
            }
            .instrument(span),
        );
    }

    async fn open_lix_for_handler(
        self: &Arc<Self>,
        lix_id: String,
        opened: watch::Sender<RuntimeOpenState>,
    ) -> Result<Arc<LixRuntime>> {
        #[cfg(test)]
        if let Some(gate) = &self.open_gate {
            gate.starts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            gate.started.notify_one();
            gate.release.notified().await;
            if gate
                .fail_next
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(anyhow::anyhow!("test-controlled Lix open failure"));
            }
        }
        let manager = Arc::clone(self);
        let runtime = Handle::current();
        let span = tracing::Span::current();
        tokio::task::spawn_blocking(move || {
            runtime.block_on(manager.open_lix(&lix_id, &opened).instrument(span))
        })
        .await
        .context("join lix runtime initialization")?
    }

    async fn open_lix(
        &self,
        lix_id: &str,
        opened: &watch::Sender<RuntimeOpenState>,
    ) -> Result<Arc<LixRuntime>> {
        let started = Instant::now();
        let io = SlateDBIoCounters::default();
        let storage_started = Instant::now();
        let storage =
            info_span!("lix.storage.open", lix.id = lix_id).in_scope(|| match &self.backend {
                #[cfg(test)]
                StorageBackend::Memory { object_store } => {
                    SlateDB::open_object_store_with_options_and_io_counters(
                        lix_id,
                        Arc::clone(object_store),
                        SlateDBObjectStoreOptions::default(),
                        io.clone(),
                    )
                    .context("open in-memory Lix SlateDB storage")
                }
                StorageBackend::S3 {
                    object_store,
                    prefix,
                    cache,
                } => {
                    let storage_prefix = if prefix.is_empty() {
                        lix_id.to_string()
                    } else {
                        format!("{prefix}/{lix_id}")
                    };
                    let mut lix_cache = cache.clone();
                    lix_cache.root_folder = cache_child_path(&cache.root_folder, lix_id)
                        .expect("Lix IDs are validated before opening cached storage");
                    SlateDB::open_object_store_with_options_and_io_counters(
                        storage_prefix,
                        Arc::clone(object_store),
                        SlateDBObjectStoreOptions {
                            cache: Some(lix_cache),
                        },
                        io.clone(),
                    )
                    .context("open cached S3-backed Lix SlateDB storage")
                }
            })?;
        let storage_open_ms = elapsed_millis(storage_started);

        let engine_started = Instant::now();
        // The canonical protocol owns the repository engine directly. Server
        // admission creates no hidden application session; each successful
        // handshake owns exactly one client session.
        let open_state = opened.clone();
        let open_progress =
            lix_sdk::CallbackOpenProgressSink::new(move |progress: lix_sdk::OpenProgress| {
                if let Some(state) = runtime_open_state_from_progress(progress) {
                    open_state.send_replace(state);
                }
            });
        let protocol = async {
            lix_sdk::open_lix()
                .with_storage(storage)
                .with_telemetry(Arc::clone(&self.telemetry))
                .with_open_progress_sink(Arc::new(open_progress))
                .serve()
                .with_lix_id(lix_id)
                .await
                .context("serve Lix repository through the canonical server protocol")
        }
        .instrument(info_span!("lix.engine.open", lix.id = lix_id))
        .await?;
        let engine_open_ms = elapsed_millis(engine_started);
        let io = io.snapshot();
        info!(
            lix_id,
            elapsed_ms = elapsed_millis(started),
            storage_open_ms,
            engine_open_ms,
            storage.read_objects = io.read_objects,
            storage.read_bytes = io.read_bytes,
            storage.write_objects = io.write_objects,
            storage.write_bytes = io.write_bytes,
            storage.list_operations = io.list_operations,
            storage.cache_filesystem_reads = io.cache_filesystem_reads,
            storage.manifest_read_objects = io.manifest.read_objects,
            storage.manifest_write_objects = io.manifest.write_objects,
            storage.wal_read_objects = io.wal.read_objects,
            storage.wal_write_objects = io.wal.write_objects,
            storage.compacted_read_objects = io.compacted.read_objects,
            "opened lix runtime"
        );
        Ok(Arc::new(LixRuntime::new(LixService { protocol })))
    }

    fn spawn_eviction_cleanup(self: &Arc<Self>, evicted: EvictedRuntime) {
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            let EvictedRuntime {
                lix_id,
                runtime,
                service,
                sequence,
                done,
            } = evicted;
            let started = Instant::now();

            match close_lix_service(service).await {
                Ok(()) => info!(
                    lix_id,
                    elapsed_ms = elapsed_millis(started),
                    "closed evicted lix runtime"
                ),
                Err(error) => tracing::warn!(
                    lix_id,
                    elapsed_ms = elapsed_millis(started),
                    error = %error,
                    "released evicted lix runtime after close error"
                ),
            }
            // The protocol owns the Lix root and its SlateDB storage. Drop
            // both before moving this Lix's disk-cache child out of the
            // active cache tree.
            drop(runtime);
            let cleanup = manager
                .retire_and_delete_cache_child(&lix_id, sequence)
                .await;

            // The next cold open waits on this tombstone. Releasing it only
            // after deletion prevents eviction churn from retaining an
            // unbounded number of workers, cache directories, or delete
            // tasks outside the server-wide cap.
            manager
                .finish_cleanup(&lix_id, sequence, done, cleanup)
                .await;
        });
    }

    fn cache_root(&self) -> Option<PathBuf> {
        // Cache retirement moves per-Lix children under this root. It must be
        // owned exclusively by this server process; construction holds the
        // cache-root lock for the manager's lifetime.
        match &self.backend {
            #[cfg(test)]
            StorageBackend::Memory { .. } => None,
            StorageBackend::S3 { cache, .. } => Some(cache.root_folder.clone()),
        }
    }

    async fn wait_for_cleanup(
        &self,
        cleanup: &mut watch::Receiver<CleanupState>,
    ) -> std::result::Result<(), LixRuntimeError> {
        loop {
            let cleanup_state = cleanup.borrow().clone();
            match cleanup_state {
                CleanupState::Complete => return Ok(()),
                CleanupState::Failed(error) => return Err(LixRuntimeError::Cleanup(error)),
                CleanupState::Running => {
                    if cleanup.changed().await.is_err() {
                        return Err(LixRuntimeError::Cleanup(Arc::from(
                            "lix runtime cleanup stopped before completing",
                        )));
                    }
                }
            }
        }
    }

    async fn finish_cleanup(
        &self,
        lix_id: &str,
        sequence: u64,
        done: watch::Sender<CleanupState>,
        result: Result<()>,
    ) {
        let mut state = self.state.lock().await;
        let matches_cleanup = state
            .cleaning
            .get(lix_id)
            .is_some_and(|cleanup| cleanup.sequence == sequence);
        if !matches_cleanup {
            done.send_replace(CleanupState::Failed(Arc::from(
                "lix runtime cleanup lost its manager state",
            )));
            return;
        }
        match result {
            Ok(()) => {
                state.cleaning.remove(lix_id);
                done.send_replace(CleanupState::Complete);
            }
            Err(error) => {
                let error = Arc::<str>::from(format!("{error:#}"));
                tracing::error!(
                    lix_id,
                    sequence,
                    error = %error,
                    "retaining failed Lix cleanup slot to preserve cache bounds"
                );
                done.send_replace(CleanupState::Failed(error));
            }
        }
    }

    async fn retire_and_delete_cache_child(&self, lix_id: &str, sequence: u64) -> Result<()> {
        let Some(cache_root) = self.cache_root() else {
            return Ok(());
        };
        let cleanup_lix_id = lix_id.to_string();
        let retired = tokio::task::spawn_blocking(move || {
            retire_cache_child(&cache_root, &cleanup_lix_id, sequence)
        })
        .await
        .context("join disk-cache retirement task")??;

        let Some(retired) = retired else {
            return Ok(());
        };
        let display_path = retired.display().to_string();
        tokio::task::spawn_blocking(move || delete_retired_cache_child(&retired))
            .await
            .context("join retired disk-cache deletion task")??;
        info!(lix_id, path = %display_path, "deleted retired Lix disk cache");
        Ok(())
    }

    async fn begin_failed_open_cleanup(
        &self,
        lix_id: &str,
        expected_runtime: &Arc<OnceCell<Arc<LixRuntime>>>,
    ) -> Option<(u64, watch::Sender<CleanupState>)> {
        let mut state = self.state.lock().await;
        if !state
            .entries
            .get(lix_id)
            .is_some_and(|entry| Arc::ptr_eq(&entry.runtime, expected_runtime))
        {
            return None;
        }
        state.entries.remove(lix_id);
        let cleanup = start_cleanup(&mut state, lix_id.to_string())
            .expect("an active opening cannot already be cleaning");
        info!(lix_id, "removed failed lix runtime opening");
        Some(cleanup)
    }

    pub(crate) async fn recover(self: &Arc<Self>, lix_id: &str, service: &Arc<LixService>) {
        let runtime = {
            let state = self.state.lock().await;
            state
                .entries
                .get(lix_id)
                .and_then(|entry| entry.runtime.get())
                .cloned()
        };
        let Some(runtime) = runtime else {
            return;
        };
        let Some(draining_service) = runtime.begin_recovery(service).await else {
            return;
        };

        info!(lix_id, "lix runtime entering recovery");
        let manager = Arc::clone(self);
        let lix_id = lix_id.to_string();
        tokio::spawn(async move {
            let started = Instant::now();
            // Close first: an SSE body holds a service lease, but close() is
            // what ends that stream. Waiting for leases before close would
            // leave a recovering runtime permanently stuck behind its own
            // observation response.
            let Some(close_result) = await_recovery_close(
                &manager.recovery_watchdog,
                &lix_id,
                close_lix_service(Arc::clone(&draining_service)),
            )
            .await
            else {
                return;
            };
            match close_result {
                Ok(()) => info!(
                    lix_id,
                    elapsed_ms = elapsed_millis(started),
                    "closed recovered lix runtime"
                ),
                Err(error) => tracing::warn!(
                    lix_id,
                    elapsed_ms = elapsed_millis(started),
                    error = %error,
                    "released recovered lix runtime after close error"
                ),
            }
            // The task and the recovering runtime each retain one reference.
            // Closing above has ended protocol streams; wait for their HTTP
            // bodies to release the remaining leases before retiring storage.
            while Arc::strong_count(&draining_service) > 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            if let Some((sequence, done)) = manager
                .begin_recovered_runtime_cleanup(&lix_id, &runtime)
                .await
            {
                // Installing the tombstone above prevents new `get` calls
                // from retaining the runtime. An in-flight caller can still
                // own the runtime cell, whose initialized value retains this
                // Arc even though it does not add a direct service Arc.
                while Arc::strong_count(&runtime) > 1 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                // `draining_service` was closed above. Dropping every
                // retained runtime before touching its cache path keeps
                // SlateDB workers from observing a renamed directory.
                drop(draining_service);
                drop(runtime);
                let cleanup = manager
                    .retire_and_delete_cache_child(&lix_id, sequence)
                    .await;
                manager
                    .finish_cleanup(&lix_id, sequence, done, cleanup)
                    .await;
            }
        });
    }

    async fn begin_recovered_runtime_cleanup(
        &self,
        lix_id: &str,
        expected_runtime: &Arc<LixRuntime>,
    ) -> Option<(u64, watch::Sender<CleanupState>)> {
        let mut state = self.state.lock().await;
        if !state.entries.get(lix_id).is_some_and(|entry| {
            entry
                .runtime
                .get()
                .is_some_and(|runtime| Arc::ptr_eq(runtime, expected_runtime))
        }) {
            return None;
        }
        state.entries.remove(lix_id);
        let cleanup = start_cleanup(&mut state, lix_id.to_string())
            .expect("an active recovered runtime cannot already be cleaning");
        info!(lix_id, "removed recovered lix runtime");
        Some(cleanup)
    }

    /// Prevents new opens and closes protocol servers so their live streams
    /// cannot keep HTTP graceful shutdown from reaching the storage owners.
    pub async fn shutdown(&self) -> Result<()> {
        let runtimes = {
            let mut state = self.state.lock().await;
            state.shutting_down = true;
            state
                .entries
                .iter()
                .map(|(lix_id, entry)| {
                    (
                        lix_id.clone(),
                        Arc::clone(&entry.runtime),
                        entry.opened.clone(),
                    )
                })
                .collect::<Vec<_>>()
        };

        let mut closers = JoinSet::new();
        for (lix_id, runtime, opened) in runtimes {
            // Evicting runtimes already have a dedicated close task. This
            // also waits for an opening captured by the shutdown gate and
            // closes recovering protocols, so neither can retain an SSE.
            closers.spawn(async move { (lix_id, close_runtime_after_open(runtime, opened).await) });
        }

        let mut first_error = None;
        while let Some(closed) = closers.join_next().await {
            match closed {
                Ok((lix_id, Ok(()))) => info!(lix_id, "closed Lix protocol for shutdown"),
                Ok((lix_id, Err(error))) => {
                    tracing::error!(lix_id, error = %error, "failed to close Lix protocol during shutdown");
                    if first_error.is_none() {
                        first_error = Some(error.context(format!("close Lix runtime {lix_id}")));
                    }
                }
                Err(error) => {
                    tracing::error!(%error, "failed to join Lix protocol shutdown task");
                    if first_error.is_none() {
                        first_error =
                            Some(anyhow::anyhow!("join Lix protocol shutdown task: {error}"));
                    }
                }
            }
        }

        first_error.map_or(Ok(()), Err)
    }

    pub(crate) fn max_open_lixes(&self) -> usize {
        self.max_open_lixes
    }

    #[cfg(test)]
    pub(crate) async fn cached_lix_count(&self) -> usize {
        self.state.lock().await.entries.len()
    }

    #[cfg(test)]
    fn has_state_drop_probe(&self) -> bool {
        self.state
            .try_lock()
            .expect("inspect idle manager state")
            .state_drop_probe
            .is_some()
    }
}

impl LixService {
    pub(crate) fn handle_protocol(
        &self,
        request: Request<Body>,
        context: ServerProtocolContext,
    ) -> Pin<Box<dyn Future<Output = ServerProtocolResponse> + Send + 'static>> {
        let (parts, body) = request.into_parts();
        let body =
            ServerProtocolBody::stream(body.into_data_stream().map_err(std::io::Error::other));
        let protocol = self.protocol.clone();
        Box::pin(async move {
            protocol
                .handle(Request::from_parts(parts, body), context)
                .await
        })
    }

    #[cfg(test)]
    fn protocol_router(&self) -> axum::Router {
        use axum::routing::any;

        let service = self.protocol.clone();
        axum::Router::new().fallback(any(move |request: Request<Body>| {
            let protocol = service.clone();
            async move {
                let (parts, body) = request.into_parts();
                let body = ServerProtocolBody::stream(
                    body.into_data_stream().map_err(std::io::Error::other),
                );
                let response = protocol
                    .handle(
                        Request::from_parts(parts, body),
                        ServerProtocolContext::anonymous(),
                    )
                    .await;
                let (parts, body) = response.into_parts();
                axum::response::Response::from_parts(parts, Body::new(body))
            }
        }))
    }

    async fn close(&self) -> Result<()> {
        self.protocol
            .close()
            .await
            .context("close Lix protocol sessions")
    }
}

impl LixRuntime {
    fn new(service: LixService) -> Self {
        Self {
            state: Mutex::new(LixRuntimeState::Active(Arc::new(service))),
        }
    }

    async fn acquire(&self) -> Option<Arc<LixService>> {
        let state = self.state.lock().await;
        match &*state {
            LixRuntimeState::Active(service) => Some(Arc::clone(service)),
            LixRuntimeState::Recovering(_) | LixRuntimeState::Evicting => None,
        }
    }

    async fn acquire_for_shutdown(&self) -> Option<Arc<LixService>> {
        let state = self.state.lock().await;
        match &*state {
            LixRuntimeState::Active(service) | LixRuntimeState::Recovering(service) => {
                Some(Arc::clone(service))
            }
            LixRuntimeState::Evicting => None,
        }
    }

    async fn begin_recovery(&self, expected_service: &Arc<LixService>) -> Option<Arc<LixService>> {
        let mut state = self.state.lock().await;
        let service = match &*state {
            LixRuntimeState::Active(service) if Arc::ptr_eq(service, expected_service) => {
                Arc::clone(service)
            }
            LixRuntimeState::Active(_)
            | LixRuntimeState::Recovering(_)
            | LixRuntimeState::Evicting => {
                return None;
            }
        };
        *state = LixRuntimeState::Recovering(Arc::clone(&service));
        Some(service)
    }

    fn try_begin_eviction(&self) -> Option<Arc<LixService>> {
        let mut state = self.state.try_lock().ok()?;
        match &*state {
            LixRuntimeState::Active(service)
                if Arc::strong_count(service) == 1 && service.protocol.is_idle() =>
            {
                let LixRuntimeState::Active(service) =
                    std::mem::replace(&mut *state, LixRuntimeState::Evicting)
                else {
                    unreachable!("matched active runtime state")
                };
                Some(service)
            }
            LixRuntimeState::Active(_)
            | LixRuntimeState::Recovering(_)
            | LixRuntimeState::Evicting => None,
        }
    }

    fn is_idle(&self) -> bool {
        let Ok(state) = self.state.try_lock() else {
            return false;
        };
        matches!(
            &*state,
            LixRuntimeState::Active(service)
                if Arc::strong_count(service) == 1 && service.protocol.is_idle()
        )
    }
}

async fn close_lix_service(service: Arc<LixService>) -> Result<()> {
    let runtime = Handle::current();
    tokio::task::spawn_blocking(move || {
        let result = runtime.block_on(service.close());
        drop(service);
        result
    })
    .await
    .context("join Lix protocol close")?
}

async fn await_recovery_close<F, T>(
    watchdog: &RecoveryWatchdog,
    lix_id: &str,
    close: F,
) -> Option<T>
where
    F: Future<Output = T>,
{
    match tokio::time::timeout(watchdog.timeout, close).await {
        Ok(result) => Some(result),
        Err(_elapsed) => {
            tracing::error!(
                lix_id,
                timeout_secs = watchdog.timeout.as_secs(),
                "lix runtime recovery close exceeded its deadline; aborting process"
            );
            (watchdog.on_timeout)();
            None
        }
    }
}

async fn close_runtime_after_open(
    runtime: Arc<OnceCell<Arc<LixRuntime>>>,
    mut opened: watch::Receiver<RuntimeOpenState>,
) -> Result<()> {
    loop {
        let open_state = opened.borrow().clone();
        match open_state {
            RuntimeOpenState::Opening | RuntimeOpenState::Migrating { .. } => {
                opened
                    .changed()
                    .await
                    .context("wait for Lix runtime opening during shutdown")?;
            }
            RuntimeOpenState::MigrationFailed { .. }
            | RuntimeOpenState::UpgradeFailed
            | RuntimeOpenState::Failed(_) => return Ok(()),
            RuntimeOpenState::Ready => {
                let runtime = runtime
                    .get()
                    .expect("runtime opener reported success before storing its runtime");
                if let Some(service) = runtime.acquire_for_shutdown().await {
                    close_lix_service(service).await?;
                }
                return Ok(());
            }
        }
    }
}

impl fmt::Display for LixRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidId => write!(formatter, "invalid lix ID"),
            Self::AtCapacity { max } => write!(
                formatter,
                "lix service is at its capacity of {max} active runtimes"
            ),
            Self::Migrating {
                from_version,
                to_version,
            } => write!(
                formatter,
                "lix repository is migrating from v{from_version} to v{to_version}"
            ),
            Self::MigrationFailed {
                from_version,
                to_version,
            } => write!(
                formatter,
                "lix repository migration from v{from_version} to v{to_version} failed"
            ),
            Self::UpgradeFailed => write!(formatter, "lix repository upgrade failed"),
            Self::Recovering => write!(formatter, "lix runtime is recovering"),
            Self::ShuttingDown => write!(formatter, "lix server is shutting down"),
            Self::Cleanup(error) => write!(formatter, "lix runtime cleanup: {error}"),
            Self::Open(error) => write!(formatter, "open lix runtime: {error:#}"),
        }
    }
}

impl Error for LixRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Open(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

impl CacheRootLease {
    const OWNER_LOCK_NAME: &'static str = ".lix-server-cache-owner.lock";

    fn acquire(root_folder: &Path) -> Result<Self> {
        ensure_real_directory(root_folder, "SlateDB cache root")?;

        let lock_path = root_folder.join(Self::OWNER_LOCK_NAME);
        let owner_lock = open_cache_owner_lock(&lock_path)?;
        owner_lock.try_lock_exclusive().map_err(|error| {
            anyhow::anyhow!(
                "SlateDB cache root {} is already owned by another lix-server process; configure a distinct SLATEDB_CACHE_DIR",
                root_folder.display()
            )
            .context(error)
        })?;

        Ok(Self {
            _owner_lock: owner_lock,
        })
    }
}

fn ensure_real_directory(path: &Path, label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => require_real_directory(path, label, &metadata),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(path)
                .with_context(|| format!("create {label} {}", path.display()))?;
            let metadata = fs::symlink_metadata(path)
                .with_context(|| format!("inspect {label} {}", path.display()))?;
            require_real_directory(path, label, &metadata)
        }
        Err(error) => Err(error).with_context(|| format!("inspect {label} {}", path.display())),
    }
}

fn ensure_direct_child_directory(parent: &Path, name: &str, label: &str) -> Result<PathBuf> {
    let child = parent.join(name);
    match fs::symlink_metadata(&child) {
        Ok(metadata) => require_real_directory(&child, label, &metadata)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            match fs::create_dir(&child) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("create {label} {}", child.display()));
                }
            }
            let metadata = fs::symlink_metadata(&child)
                .with_context(|| format!("inspect {label} {}", child.display()))?;
            require_real_directory(&child, label, &metadata)?;
        }
        Err(error) => {
            return Err(error).with_context(|| format!("inspect {label} {}", child.display()));
        }
    }
    Ok(child)
}

fn require_real_directory(path: &Path, label: &str, metadata: &fs::Metadata) -> Result<()> {
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        anyhow::bail!("{label} must be a real directory: {}", path.display());
    }
    Ok(())
}

fn open_cache_owner_lock(lock_path: &Path) -> Result<File> {
    // Avoid following a pre-existing symlink. If another creator races our
    // create_new call, inspect its finished entry before opening it instead.
    for _ in 0..2 {
        match fs::symlink_metadata(lock_path) {
            Ok(metadata) => {
                if !metadata.is_file() || metadata.file_type().is_symlink() {
                    anyhow::bail!(
                        "SlateDB cache-root lock must be a regular file: {}",
                        lock_path.display()
                    );
                }
                return OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(lock_path)
                    .with_context(|| {
                        format!("open SlateDB cache-root lock {}", lock_path.display())
                    });
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(lock_path)
                {
                    Ok(lock) => return Ok(lock),
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(error) => {
                        return Err(error).with_context(|| {
                            format!("create SlateDB cache-root lock {}", lock_path.display())
                        });
                    }
                }
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("inspect SlateDB cache-root lock {}", lock_path.display())
                });
            }
        }
    }
    anyhow::bail!(
        "SlateDB cache-root lock changed while opening: {}",
        lock_path.display()
    )
}

fn cache_namespace_root(
    base_root: &Path,
    endpoint: &str,
    bucket: &str,
    region: &str,
    prefix: &str,
    access_key_id: &str,
) -> PathBuf {
    let mut hasher = Hasher::new();
    hasher.update(CACHE_NAMESPACE_LAYOUT);
    // Access-key IDs distinguish S3-compatible multi-tenant endpoints. Hash
    // it with public location fields, but never put credentials in a path.
    for component in [endpoint, bucket, region, prefix, access_key_id] {
        let bytes = component.as_bytes();
        hasher.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
        hasher.update(bytes);
    }
    base_root
        .join(CACHE_NAMESPACE_PARENT)
        .join(hasher.finalize().to_hex().to_string())
}

fn prepare_cache_namespace(owner_root: &Path, namespace_root: &Path) -> Result<()> {
    let namespace_parent = ensure_direct_child_directory(
        owner_root,
        CACHE_NAMESPACE_PARENT,
        "SlateDB cache namespace parent",
    )?;
    let namespace_name = namespace_root
        .file_name()
        .and_then(|name| name.to_str())
        .context("SlateDB cache namespace name must be valid UTF-8")?;
    if !valid_cache_namespace_name(namespace_name)
        || namespace_root != namespace_parent.join(namespace_name)
    {
        anyhow::bail!(
            "refuse to use invalid SlateDB cache namespace {}",
            namespace_root.display()
        );
    }

    reap_inactive_cache_namespaces(&namespace_parent, namespace_name)?;
    let current_namespace = ensure_direct_child_directory(
        &namespace_parent,
        namespace_name,
        "SlateDB cache namespace",
    )?;
    reap_retired_cache_children(&current_namespace)?;
    // Every direct Lix child belongs to a predecessor process because the
    // base-root lease is held before this startup reaches SlateDB. Removing
    // them deliberately cold-starts the cache after a process restart and
    // keeps the aggregate disk budget from growing with successive working
    // sets.
    reap_stale_cache_children(&current_namespace)
}

fn valid_cache_namespace_name(name: &str) -> bool {
    name.len() == 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn reap_inactive_cache_namespaces(namespace_parent: &Path, current_name: &str) -> Result<()> {
    for entry in fs::read_dir(namespace_parent).with_context(|| {
        format!(
            "list SlateDB cache namespace parent {}",
            namespace_parent.display()
        )
    })? {
        let entry = entry.with_context(|| {
            format!(
                "read SlateDB cache namespace parent {}",
                namespace_parent.display()
            )
        })?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if name == current_name {
            continue;
        }
        if !valid_cache_namespace_name(name) {
            // Only the namespaced layout is ours. Leave legacy or operator
            // content untouched rather than recursively deleting it.
            continue;
        }
        delete_cache_directory(&entry.path(), "inactive SlateDB cache namespace")?;
    }
    Ok(())
}

fn reap_stale_cache_children(namespace_root: &Path) -> Result<()> {
    for entry in fs::read_dir(namespace_root).with_context(|| {
        format!(
            "list active SlateDB cache namespace {}",
            namespace_root.display()
        )
    })? {
        let entry = entry.with_context(|| {
            format!(
                "read active SlateDB cache namespace {}",
                namespace_root.display()
            )
        })?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        if name == ".trash" || !valid_lix_id(name) {
            continue;
        }
        delete_cache_directory(&entry.path(), "stale Lix cache child")?;
    }
    Ok(())
}

fn valid_lix_id(lix_id: &str) -> bool {
    !lix_id.is_empty()
        && lix_id.len() <= 128
        && lix_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn start_cleanup(
    state: &mut ManagerState,
    lix_id: String,
) -> Option<(u64, watch::Sender<CleanupState>)> {
    if state.cleaning.contains_key(&lix_id) {
        return None;
    }
    state.cleanup_sequence = state.cleanup_sequence.wrapping_add(1);
    let sequence = state.cleanup_sequence;
    let (done, waiter) = watch::channel(CleanupState::Running);
    state.cleaning.insert(
        lix_id,
        CleanupTombstone {
            sequence,
            done: waiter,
        },
    );
    Some((sequence, done))
}

fn take_idle_lru_runtime(
    entries: &mut HashMap<String, RuntimeEntry>,
) -> Option<(String, Arc<LixRuntime>, Arc<LixService>)> {
    let mut candidates = entries
        .iter()
        .filter(|(_, entry)| {
            Arc::strong_count(&entry.runtime) == 1
                && entry
                    .runtime
                    .get()
                    .is_some_and(|runtime| Arc::strong_count(runtime) == 1 && runtime.is_idle())
        })
        .map(|(lix_id, entry)| (lix_id.clone(), entry.last_used))
        .collect::<Vec<_>>();
    candidates.sort_unstable_by_key(|(_, last_used)| *last_used);

    for (lix_id, _) in candidates {
        let runtime = entries
            .get(&lix_id)
            .and_then(|entry| entry.runtime.get())
            .cloned()?;
        let Some(service) = runtime.try_begin_eviction() else {
            continue;
        };
        entries.remove(&lix_id);
        info!(lix_id = %lix_id, "evicted idle lix runtime");
        return Some((lix_id, runtime, service));
    }
    None
}

fn cache_options(
    cache: &SlateDBCacheConfig,
    max_open_lixes: usize,
    root_folder: PathBuf,
) -> SlateDBCacheOptions {
    SlateDBCacheOptions {
        root_folder,
        // Each active Lix has its own SlateDB cache root and evictor. Split
        // all cache budgets across the bounded live-runtime set.
        max_disk_cache_bytes: per_lix_usize(cache.max_disk_cache_bytes, max_open_lixes),
        block_cache_bytes: per_lix_u64(cache.block_cache_bytes, max_open_lixes),
        metadata_cache_bytes: per_lix_u64(cache.metadata_cache_bytes, max_open_lixes),
    }
}

fn cache_child_path(root_folder: &Path, lix_id: &str) -> Option<PathBuf> {
    valid_lix_id(lix_id).then(|| root_folder.join(lix_id))
}

fn retired_cache_path(root_folder: &Path, lix_id: &str, sequence: u64) -> Option<PathBuf> {
    valid_lix_id(lix_id).then(|| {
        root_folder
            .join(".trash")
            .join(format!("{lix_id}-{}-{sequence}", std::process::id()))
    })
}

fn valid_retired_cache_child_name(name: &str) -> bool {
    let mut components = name.rsplitn(3, '-');
    let Some(sequence) = components.next() else {
        return false;
    };
    let Some(pid) = components.next() else {
        return false;
    };
    let Some(lix_id) = components.next() else {
        return false;
    };
    valid_lix_id(lix_id)
        && pid.parse::<u32>().is_ok_and(|pid| pid > 0)
        && sequence.parse::<u64>().is_ok()
}

fn retire_cache_child(root_folder: &Path, lix_id: &str, sequence: u64) -> Result<Option<PathBuf>> {
    let cache_child =
        cache_child_path(root_folder, lix_id).context("validate evicted Lix cache child path")?;
    let metadata = match fs::symlink_metadata(&cache_child) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect cache child {}", cache_child.display()));
        }
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        anyhow::bail!(
            "refuse to retire non-directory cache child {}",
            cache_child.display()
        );
    }

    ensure_direct_child_directory(root_folder, ".trash", "disk-cache trash root")?;

    let retired = retired_cache_path(root_folder, lix_id, sequence)
        .context("validate retired Lix cache path")?;
    match fs::symlink_metadata(&retired) {
        Ok(_) => anyhow::bail!(
            "refuse to overwrite existing retired cache child {}",
            retired.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect retired cache child {}", retired.display()));
        }
    }
    match fs::rename(&cache_child, &retired) {
        Ok(()) => Ok(Some(retired)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| {
            format!(
                "move evicted Lix cache child {} to {}",
                cache_child.display(),
                retired.display()
            )
        }),
    }
}

fn delete_retired_cache_child(retired: &Path) -> Result<()> {
    delete_cache_directory(retired, "retired cache child")
}

fn delete_cache_directory(path: &Path, label: &str) -> Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error).with_context(|| format!("inspect {label} {}", path.display()));
        }
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        anyhow::bail!("refuse to delete non-directory {label} {}", path.display());
    }
    for entry in fs::read_dir(path).with_context(|| format!("list {label} {}", path.display()))? {
        let entry = entry.with_context(|| format!("read {label} {}", path.display()))?;
        let child = entry.path();
        let child_metadata = fs::symlink_metadata(&child)
            .with_context(|| format!("inspect {label} entry {}", child.display()))?;
        if child_metadata.file_type().is_symlink() {
            anyhow::bail!(
                "refuse to recurse through symlink in {label} {}",
                child.display()
            );
        }
        if child_metadata.is_dir() {
            delete_cache_directory(&child, label)?;
        } else if child_metadata.is_file() {
            fs::remove_file(&child)
                .with_context(|| format!("delete {label} file {}", child.display()))?;
        } else {
            anyhow::bail!(
                "refuse to delete non-file entry in {label} {}",
                child.display()
            );
        }
    }
    fs::remove_dir(path).with_context(|| format!("delete {label} {}", path.display()))
}

fn reap_retired_cache_children(root_folder: &Path) -> Result<()> {
    let trash_root = root_folder.join(".trash");
    let metadata = match fs::symlink_metadata(&trash_root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("inspect disk-cache trash root {}", trash_root.display())
            });
        }
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        anyhow::bail!(
            "refuse to reap non-directory disk-cache trash root {}",
            trash_root.display()
        );
    }

    for entry in fs::read_dir(&trash_root)
        .with_context(|| format!("list disk-cache trash root {}", trash_root.display()))?
    {
        let entry = entry
            .with_context(|| format!("read disk-cache trash root {}", trash_root.display()))?;
        let file_name = entry.file_name();
        let name = file_name
            .to_str()
            .context("retired disk-cache child name must be valid UTF-8")?;
        if !valid_retired_cache_child_name(name) {
            anyhow::bail!(
                "refuse to reap unexpected retired cache child {}",
                entry.path().display()
            );
        }
        delete_retired_cache_child(&entry.path())?;
    }
    Ok(())
}

fn per_lix_usize(total: usize, lixes: usize) -> usize {
    total.checked_div(lixes).unwrap_or(0).max(1)
}

fn per_lix_u64(total: u64, lixes: usize) -> u64 {
    total
        .checked_div(u64::try_from(lixes).unwrap_or(u64::MAX))
        .unwrap_or(0)
        .max(1)
}

fn elapsed_millis(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn s3_client_options(budget: S3RequestBudget) -> ClientOptions {
    ClientOptions::new()
        .with_timeout(budget.request_timeout)
        .with_connect_timeout(budget.connect_timeout)
}

fn s3_retry_config(budget: S3RequestBudget) -> RetryConfig {
    RetryConfig {
        max_retries: budget.max_retries,
        retry_timeout: budget.retry_timeout,
        ..RetryConfig::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, header},
    };
    use http_body_util::BodyExt as _;
    use serde_json::{Value as JsonValue, json};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use tower::ServiceExt as _;

    const LIX_A: &str = "11111111-1111-4111-8111-111111111111";
    const LIX_B: &str = "22222222-2222-4222-8222-222222222222";

    struct TestCacheRoot(PathBuf);

    impl TestCacheRoot {
        fn new(name: &str) -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let root = std::env::temp_dir().join(format!(
                "lix-server-store-{name}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&root).expect("create test cache root");
            Self(root)
        }
    }

    impl Drop for TestCacheRoot {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn recovery_close_watchdog_fires_for_a_stuck_close() {
        let fired = Arc::new(AtomicBool::new(false));
        let fired_by_watchdog = Arc::clone(&fired);
        let watchdog = RecoveryWatchdog {
            timeout: Duration::from_millis(1),
            on_timeout: Arc::new(move || {
                fired_by_watchdog.store(true, Ordering::SeqCst);
            }),
        };

        let result = await_recovery_close(&watchdog, LIX_A, std::future::pending::<()>()).await;

        assert!(result.is_none());
        assert!(fired.load(Ordering::SeqCst));
    }

    fn namespace_root(base_root: &Path, endpoint: &str) -> PathBuf {
        cache_namespace_root(
            base_root,
            endpoint,
            "tenant-a",
            "us-east-1",
            "production/lix",
            "AKIA-TENANT-A",
        )
    }

    #[test]
    fn cache_namespace_is_stable_backend_scoped_and_hides_identity() {
        let base_root = Path::new("/var/cache/lix-server");
        let primary = cache_namespace_root(
            base_root,
            "https://s3.us-east-1.example",
            "tenant-a",
            "us-east-1",
            "production/lix",
            "AKIA-TENANT-A",
        );

        assert_eq!(
            primary,
            cache_namespace_root(
                base_root,
                "https://s3.us-east-1.example",
                "tenant-a",
                "us-east-1",
                "production/lix",
                "AKIA-TENANT-A",
            )
        );
        assert!(primary.starts_with(base_root.join(CACHE_NAMESPACE_PARENT)));
        assert!(!valid_lix_id(CACHE_NAMESPACE_PARENT));
        let digest = primary
            .file_name()
            .and_then(|name| name.to_str())
            .expect("namespace digest is UTF-8");
        assert_eq!(digest.len(), 64);
        assert!(
            digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        );
        let rendered = primary.to_string_lossy();
        assert!(!rendered.contains("s3.us-east-1.example"));
        assert!(!rendered.contains("tenant-a"));
        assert!(!rendered.contains("production/lix"));
        assert!(!rendered.contains("AKIA-TENANT-A"));

        for changed_backend in [
            cache_namespace_root(
                base_root,
                "https://s3.us-west-2.example",
                "tenant-a",
                "us-east-1",
                "production/lix",
                "AKIA-TENANT-A",
            ),
            cache_namespace_root(
                base_root,
                "https://s3.us-east-1.example",
                "tenant-b",
                "us-east-1",
                "production/lix",
                "AKIA-TENANT-A",
            ),
            cache_namespace_root(
                base_root,
                "https://s3.us-east-1.example",
                "tenant-a",
                "us-west-2",
                "production/lix",
                "AKIA-TENANT-A",
            ),
            cache_namespace_root(
                base_root,
                "https://s3.us-east-1.example",
                "tenant-a",
                "us-east-1",
                "staging/lix",
                "AKIA-TENANT-A",
            ),
            cache_namespace_root(
                base_root,
                "https://s3.us-east-1.example",
                "tenant-a",
                "us-east-1",
                "production/lix",
                "AKIA-TENANT-B",
            ),
        ] {
            assert_ne!(primary, changed_backend);
        }
        assert_ne!(
            cache_namespace_root(base_root, "a", "bc", "", "", "key"),
            cache_namespace_root(base_root, "ab", "c", "", "", "key")
        );
    }

    #[test]
    fn cache_namespace_separates_same_lix_id_across_backends() {
        let base_root = Path::new("/var/cache/lix-server");
        let first = namespace_root(base_root, "https://first.example").join("lix-123");
        let second = namespace_root(base_root, "https://second.example").join("lix-123");

        assert_ne!(first, second);
        assert!(first.starts_with(base_root.join(CACHE_NAMESPACE_PARENT)));
        assert!(second.starts_with(base_root.join(CACHE_NAMESPACE_PARENT)));
        assert_eq!(
            first.file_name().and_then(|name| name.to_str()),
            Some("lix-123")
        );
        assert_eq!(
            second.file_name().and_then(|name| name.to_str()),
            Some("lix-123")
        );
    }

    fn memory_manager(max_open_lixes: usize) -> Arc<LixRuntimeManager> {
        LixRuntimeManager::new_in_memory(max_open_lixes)
    }

    fn memory_manager_with_open_gate(
        max_open_lixes: usize,
        open_gate: TestOpenGate,
    ) -> Arc<LixRuntimeManager> {
        Arc::new(LixRuntimeManager {
            backend: StorageBackend::Memory {
                object_store: Arc::new(InMemory::new()),
            },
            max_open_lixes,
            recovery_watchdog: RecoveryWatchdog::test_default(),
            state: Mutex::new(ManagerState::default()),
            telemetry: test_telemetry_sink(),
            _cache_root_lease: None,
            open_gate: Some(open_gate),
        })
    }

    #[tokio::test]
    async fn concurrent_gets_share_one_runtime() {
        let manager = memory_manager(2);
        let (left, right) = tokio::join!(manager.get(LIX_A), manager.get(LIX_A));
        let left = left.expect("open left runtime");
        let right = right.expect("open right runtime");

        assert!(Arc::ptr_eq(&left, &right));
    }

    #[tokio::test]
    async fn cancelled_get_does_not_cancel_the_manager_owned_open() {
        let gate = TestOpenGate {
            started: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
            starts: Arc::new(AtomicUsize::new(0)),
            fail_next: Arc::new(AtomicBool::new(false)),
        };
        let manager = memory_manager_with_open_gate(1, gate.clone());
        let started = gate.started.notified();
        let opening_manager = Arc::clone(&manager);
        let opening = tokio::spawn(async move { opening_manager.get(LIX_A).await });
        started.await;
        opening.abort();
        let _ = opening.await;

        let waiting_manager = Arc::clone(&manager);
        let mut later_get = tokio::spawn(async move { waiting_manager.get(LIX_A).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut later_get)
                .await
                .is_err(),
            "a later caller must wait on the original managed opener"
        );
        assert_eq!(gate.starts.load(Ordering::SeqCst), 1);

        gate.release.notify_one();
        let service = tokio::time::timeout(Duration::from_secs(10), later_get)
            .await
            .expect("managed opener should finish")
            .expect("later get task")
            .expect("open in-memory Lix");
        assert_eq!(gate.starts.load(Ordering::SeqCst), 1);
        assert_eq!(manager.cached_lix_count().await, 1);
        drop(service);
    }

    #[tokio::test]
    async fn get_reports_manager_owned_migration_without_waiting_for_open() {
        let manager = memory_manager(1);
        let runtime = Arc::new(OnceCell::new());
        let (_migration_owner, opened) = watch::channel(RuntimeOpenState::Migrating {
            from_version: 68,
            to_version: 71,
        });
        manager.state.lock().await.entries.insert(
            LIX_A.to_string(),
            RuntimeEntry {
                runtime,
                opened,
                last_used: 1,
            },
        );

        let result = tokio::time::timeout(Duration::from_millis(25), manager.get(LIX_A))
            .await
            .expect("migration state must return without waiting for the opener");
        assert!(matches!(
            result,
            Err(LixRuntimeError::Migrating {
                from_version: 68,
                to_version: 71,
            })
        ));
    }

    #[tokio::test]
    async fn failed_migration_remains_terminal_for_every_caller() {
        let manager = memory_manager(1);
        manager.state.lock().await.failed_upgrades.insert(
            LIX_A.to_string(),
            FailedUpgrade::Versioned(FailedMigration {
                from_version: 68,
                to_version: 71,
            }),
        );

        assert!(matches!(
            manager.get(LIX_A).await,
            Err(LixRuntimeError::MigrationFailed {
                from_version: 68,
                to_version: 71,
            })
        ));
        assert!(matches!(
            manager.get(LIX_A).await,
            Err(LixRuntimeError::MigrationFailed {
                from_version: 68,
                to_version: 71,
            })
        ));
    }

    #[tokio::test]
    async fn failed_managed_open_removes_its_entry_before_a_retry() {
        let gate = TestOpenGate {
            started: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
            starts: Arc::new(AtomicUsize::new(0)),
            fail_next: Arc::new(AtomicBool::new(true)),
        };
        let manager = memory_manager_with_open_gate(1, gate.clone());
        let first_started = gate.started.notified();
        let first_manager = Arc::clone(&manager);
        let first = tokio::spawn(async move { first_manager.get(LIX_A).await });
        first_started.await;
        gate.release.notify_one();
        assert!(matches!(
            first.await.expect("first get task"),
            Err(LixRuntimeError::Open(_))
        ));
        assert_eq!(manager.cached_lix_count().await, 0);

        let retry_started = gate.started.notified();
        let retry_manager = Arc::clone(&manager);
        let retry = tokio::spawn(async move { retry_manager.get(LIX_A).await });
        retry_started.await;
        gate.release.notify_one();
        let service = retry
            .await
            .expect("retry get task")
            .expect("retry opens after failed opener cleanup");
        assert_eq!(gate.starts.load(Ordering::SeqCst), 2);
        drop(service);
    }

    #[tokio::test]
    async fn evicts_the_least_recently_used_idle_runtime() {
        let manager = memory_manager(1);
        let first = manager.get(LIX_A).await.expect("open first runtime");
        drop(first);
        let second = manager.get(LIX_B).await.expect("open second runtime");

        assert_eq!(manager.cached_lix_count().await, 1);
        drop(second);
        let reopened = manager.get(LIX_A).await.expect("reopen first runtime");
        assert_eq!(manager.cached_lix_count().await, 1);
        drop(reopened);
    }

    #[tokio::test]
    async fn active_service_lease_prevents_runtime_eviction() {
        let manager = memory_manager(1);
        let active = manager.get(LIX_A).await.expect("open active runtime");

        assert!(matches!(
            manager.get(LIX_B).await,
            Err(LixRuntimeError::AtCapacity { max: 1 })
        ));

        drop(active);
        manager
            .get(LIX_B)
            .await
            .expect("evict runtime after its final service lease is released");
    }

    #[tokio::test]
    async fn live_protocol_session_prevents_runtime_eviction_between_requests() {
        use axum::{body::Body, http::Request};
        use tower::ServiceExt as _;

        let manager = memory_manager(1);
        let service = manager.get(LIX_A).await.expect("open lix runtime");
        let handshake = service
            .protocol_router()
            .oneshot(
                Request::builder()
                    .uri("/lix/v1/11111111-1111-4111-8111-111111111111/")
                    .body(Body::empty())
                    .expect("handshake request"),
            )
            .await
            .expect("handshake response");
        assert_eq!(handshake.status(), http::StatusCode::OK);
        drop(handshake);
        drop(service);

        assert!(matches!(
            manager.get(LIX_B).await,
            Err(LixRuntimeError::AtCapacity { max: 1 })
        ));
    }

    #[tokio::test]
    async fn shutdown_closes_active_observation_streams() {
        use axum::{
            body::Body,
            http::{Request, header},
        };
        use http_body_util::BodyExt as _;
        use tower::ServiceExt as _;

        let manager = memory_manager(1);
        let service = manager.get(LIX_A).await.expect("open lix runtime");
        let protocol_router = service.protocol_router();
        let handshake = protocol_router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/lix/v1/11111111-1111-4111-8111-111111111111/")
                    .body(Body::empty())
                    .expect("handshake request"),
            )
            .await
            .expect("handshake response");
        assert_eq!(handshake.status(), http::StatusCode::OK);
        let handshake = serde_json::from_slice::<serde_json::Value>(
            &handshake
                .into_body()
                .collect()
                .await
                .expect("handshake body")
                .to_bytes(),
        )
        .expect("handshake JSON");
        let session_id = handshake["sessionId"]
            .as_str()
            .expect("handshake session ID")
            .to_string();

        let observation = protocol_router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/11111111-1111-4111-8111-111111111111/observe")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("Lix-Session-Id", session_id)
                    .body(Body::from(r#"{"sql":"SELECT 1","params":[]}"#))
                    .expect("observation request"),
            )
            .await
            .expect("observation response");
        assert_eq!(observation.status(), http::StatusCode::OK);
        let mut body = observation.into_body();
        let first_frame = tokio::time::timeout(Duration::from_secs(1), body.frame())
            .await
            .expect("observation should produce an initial frame")
            .expect("observation body should remain open")
            .expect("valid observation frame");
        assert!(first_frame.is_data());

        manager.shutdown().await.expect("close active protocols");

        tokio::time::timeout(Duration::from_secs(1), async {
            while let Some(frame) = body.frame().await {
                frame.expect("valid observation frame while closing");
            }
        })
        .await
        .expect("shutdown should end the observation stream");
    }

    #[tokio::test]
    async fn shutdown_closes_recovering_observation_streams() {
        use axum::{
            body::Body,
            http::{Request, header},
        };
        use http_body_util::BodyExt as _;
        use tower::ServiceExt as _;

        let manager = memory_manager(1);
        let service = manager.get(LIX_A).await.expect("open lix runtime");
        let protocol_router = service.protocol_router();
        let handshake = protocol_router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/lix/v1/11111111-1111-4111-8111-111111111111/")
                    .body(Body::empty())
                    .expect("handshake request"),
            )
            .await
            .expect("handshake response");
        assert_eq!(handshake.status(), http::StatusCode::OK);
        let handshake = serde_json::from_slice::<serde_json::Value>(
            &handshake
                .into_body()
                .collect()
                .await
                .expect("handshake body")
                .to_bytes(),
        )
        .expect("handshake JSON");
        let session_id = handshake["sessionId"]
            .as_str()
            .expect("handshake session ID")
            .to_string();

        let observation = protocol_router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/11111111-1111-4111-8111-111111111111/observe")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("Lix-Session-Id", session_id)
                    .body(Body::from(r#"{"sql":"SELECT 1","params":[]}"#))
                    .expect("observation request"),
            )
            .await
            .expect("observation response");
        assert_eq!(observation.status(), http::StatusCode::OK);
        let mut body = observation.into_body();
        let first_frame = tokio::time::timeout(Duration::from_secs(1), body.frame())
            .await
            .expect("observation should produce an initial frame")
            .expect("observation body should remain open")
            .expect("valid observation frame");
        assert!(first_frame.is_data());

        manager.recover(LIX_A, &service).await;
        manager
            .shutdown()
            .await
            .expect("close recovering protocols");

        let stream_closed = tokio::time::timeout(Duration::from_secs(1), async {
            while let Some(frame) = body.frame().await {
                frame.expect("valid observation frame while closing");
            }
        })
        .await;
        // Keep a failing regression self-cleaning: recovery owns the close
        // after the last handler lease is released.
        drop(service);
        stream_closed.expect("shutdown should end a recovering observation stream");
    }

    #[tokio::test]
    async fn recovery_closes_observation_streams_before_waiting_for_response_leases() {
        let manager = memory_manager(1);
        let service = manager.get(LIX_A).await.expect("open lix runtime");
        let protocol_router = service.protocol_router();
        let handshake = protocol_router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/lix/v1/11111111-1111-4111-8111-111111111111/")
                    .body(Body::empty())
                    .expect("handshake request"),
            )
            .await
            .expect("handshake response");
        assert_eq!(handshake.status(), http::StatusCode::OK);
        let handshake = serde_json::from_slice::<serde_json::Value>(
            &handshake
                .into_body()
                .collect()
                .await
                .expect("handshake body")
                .to_bytes(),
        )
        .expect("handshake JSON");
        let session_id = handshake["sessionId"]
            .as_str()
            .expect("handshake session ID")
            .to_string();

        let observation = protocol_router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/11111111-1111-4111-8111-111111111111/observe")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("Lix-Session-Id", session_id)
                    .body(Body::from(r#"{"sql":"SELECT 1","params":[]}"#))
                    .expect("observation request"),
            )
            .await
            .expect("observation response");
        assert_eq!(observation.status(), http::StatusCode::OK);
        let mut body = observation.into_body();
        let first_frame = tokio::time::timeout(Duration::from_secs(1), body.frame())
            .await
            .expect("observation should produce an initial frame")
            .expect("observation body should remain open")
            .expect("valid observation frame");
        assert!(first_frame.is_data());

        manager.recover(LIX_A, &service).await;

        let stream_closed = tokio::time::timeout(Duration::from_secs(1), async {
            while let Some(frame) = body.frame().await {
                frame.expect("valid observation frame while recovering");
            }
        })
        .await;
        drop(service);
        stream_closed.expect("recovery should end an observation stream");

        tokio::time::timeout(Duration::from_secs(1), async {
            while manager.cached_lix_count().await != 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("recovery should retire the closed runtime");
    }

    #[tokio::test]
    async fn recovery_releases_an_eof_drained_observation_body_lease() {
        let manager = memory_manager(1);
        let app = crate::router(
            Arc::clone(&manager),
            None,
            Duration::from_secs(60),
            crate::telemetry::InFlightSqlRegistry::default(),
        );
        let session = open_protocol_session(&app, LIX_A).await;
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/11111111-1111-4111-8111-111111111111/observe")
                    .header(lix_sdk::server_protocol::SESSION_ID_HEADER, session)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"sql":"SELECT 1","params":[]}"#))
                    .expect("observation request"),
            )
            .await
            .expect("observation response");
        assert_eq!(response.status(), http::StatusCode::OK);
        let mut body = response.into_body();
        let first_frame = tokio::time::timeout(Duration::from_secs(1), body.frame())
            .await
            .expect("observation should produce an initial frame")
            .expect("observation body should remain open")
            .expect("valid observation frame");
        assert!(first_frame.is_data());

        let service = manager.get(LIX_A).await.expect("get active service");
        manager.recover(LIX_A, &service).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while let Some(frame) = body.frame().await {
                frame.expect("valid observation frame while recovering");
            }
        })
        .await
        .expect("recovery should end the observation stream");
        drop(service);

        // Keep the completed Body alive: EOF must release its lease so this
        // response object cannot hold the terminal runtime in recovery.
        tokio::time::timeout(Duration::from_secs(1), async {
            while manager.cached_lix_count().await != 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("EOF-drained observation body must not delay retirement");
        drop(body);
    }

    #[tokio::test]
    async fn shutdown_closes_runtime_opening_at_signal() {
        use axum::{body::Body, http::Request};
        use http_body_util::BodyExt as _;
        use tower::ServiceExt as _;

        let gate = TestOpenGate {
            started: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
            starts: Arc::new(AtomicUsize::new(0)),
            fail_next: Arc::new(AtomicBool::new(false)),
        };
        let manager = memory_manager_with_open_gate(1, gate.clone());
        let started = gate.started.notified();
        let opening_manager = Arc::clone(&manager);
        let opening = tokio::spawn(async move { opening_manager.get(LIX_A).await });
        started.await;
        opening.abort();
        assert!(matches!(opening.await, Err(error) if error.is_cancelled()));

        let shutdown_manager = Arc::clone(&manager);
        let mut shutdown = tokio::spawn(async move { shutdown_manager.shutdown().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if manager.state.lock().await.shutting_down {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shutdown should block new opens");
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut shutdown)
                .await
                .is_err(),
            "shutdown must wait for the manager-owned opening"
        );
        assert!(matches!(
            manager.get(LIX_B).await,
            Err(LixRuntimeError::ShuttingDown)
        ));

        gate.release.notify_one();
        shutdown
            .await
            .expect("join shutdown")
            .expect("close opened runtime");

        let runtime = {
            let state = manager.state.lock().await;
            Arc::clone(
                state
                    .entries
                    .get(LIX_A)
                    .expect("manager should retain opened runtime")
                    .runtime
                    .get()
                    .expect("opening should finish before shutdown returns"),
            )
        };
        let service = runtime
            .acquire()
            .await
            .expect("shutdown leaves the closed protocol inspectable");
        let response = service
            .protocol_router()
            .oneshot(
                Request::builder()
                    .uri("/lix/v1/11111111-1111-4111-8111-111111111111/")
                    .body(Body::empty())
                    .expect("handshake request"),
            )
            .await
            .expect("handshake response");
        assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        let body = serde_json::from_slice::<serde_json::Value>(
            &response
                .into_body()
                .collect()
                .await
                .expect("handshake body")
                .to_bytes(),
        )
        .expect("handshake JSON");
        assert_eq!(body["error"]["code"], "LIX_ERROR_PROTOCOL_SERVER_CLOSED");
    }

    #[tokio::test]
    async fn rejects_unsafe_lix_ids() {
        let manager = memory_manager(1);
        for lix_id in ["", "../escape", "contains/slash", "contains space"] {
            assert!(matches!(
                manager.get(lix_id).await,
                Err(LixRuntimeError::InvalidId)
            ));
        }
    }

    #[test]
    fn s3_request_policy_is_bounded() {
        let options = s3_client_options(S3_REQUEST_BUDGET);
        assert_eq!(
            options
                .get_config_value(&ClientConfigKey::Timeout)
                .as_deref(),
            Some("15s")
        );
        assert_eq!(
            options
                .get_config_value(&ClientConfigKey::ConnectTimeout)
                .as_deref(),
            Some("3s")
        );

        let retry = s3_retry_config(S3_REQUEST_BUDGET);
        assert_eq!(retry.max_retries, 1);
        assert_eq!(retry.retry_timeout, Duration::from_secs(30));
    }

    #[test]
    fn partitions_all_cache_budgets_per_active_lix() {
        let cache = SlateDBCacheConfig {
            root_folder: PathBuf::from("/tmp/lix-server-cache"),
            max_disk_cache_bytes: 2_048,
            block_cache_bytes: 128,
            metadata_cache_bytes: 32,
        };

        let namespace = namespace_root(&cache.root_folder, "https://s3.example");
        let options = cache_options(&cache, 4, namespace.clone());

        assert_eq!(options.root_folder, namespace);
        assert_eq!(options.max_disk_cache_bytes, 512);
        assert_eq!(options.block_cache_bytes, 32);
        assert_eq!(options.metadata_cache_bytes, 8);
        assert_eq!(
            cache_child_path(&options.root_folder, LIX_A),
            Some(options.root_folder.join(LIX_A))
        );
    }

    #[test]
    fn retires_only_the_validated_lix_cache_child() {
        let root = TestCacheRoot::new("retire-cache-child");
        let namespace = namespace_root(&root.0, "https://s3.example");
        let lix_cache = namespace.join(LIX_A);
        let untouched = namespace.join("lix-b");
        fs::create_dir_all(lix_cache.join("nested")).expect("create Lix cache child");
        fs::write(lix_cache.join("nested/cache-part"), b"cached bytes").expect("write cache part");
        fs::create_dir_all(&untouched).expect("create untouched cache child");

        let retired = retire_cache_child(&namespace, LIX_A, 7)
            .expect("retire cache child")
            .expect("cache child is present");

        assert!(!lix_cache.exists());
        assert!(retired.starts_with(namespace.join(".trash")));
        assert!(retired.is_dir());
        assert!(untouched.is_dir());
        assert!(namespace.is_dir());

        delete_retired_cache_child(&retired).expect("delete retired cache child");
        assert!(!retired.exists());
        assert!(untouched.is_dir());
        assert!(namespace.is_dir());
    }

    #[test]
    fn cache_cleanup_rejects_unsafe_lix_ids() {
        let root = TestCacheRoot::new("invalid-cache-child");

        assert_eq!(cache_child_path(&root.0, "../escape"), None);
        assert!(retire_cache_child(&root.0, "../escape", 1).is_err());
        assert!(root.0.is_dir());
    }

    #[test]
    fn cache_base_lease_excludes_a_second_backend_namespace_owner() {
        let root = TestCacheRoot::new("cache-root-lease");
        let first_namespace = namespace_root(&root.0, "https://first.example");
        let second_namespace = namespace_root(&root.0, "https://second.example");
        assert_ne!(first_namespace, second_namespace);
        let first = CacheRootLease::acquire(&root.0).expect("acquire first cache-root lease");
        prepare_cache_namespace(&root.0, &first_namespace)
            .expect("prepare first backend namespace");
        let error = CacheRootLease::acquire(&root.0).expect_err("reject second cache-root lease");
        assert!(
            format!("{error:#}").contains("already owned by another lix-server process"),
            "unexpected cache-root lease error: {error:#}"
        );
        drop(first);
        CacheRootLease::acquire(&root.0).expect("acquire cache-root lease after release");
    }

    #[test]
    fn cache_root_lease_outlives_manager_state_on_drop() {
        let root = TestCacheRoot::new("cache-root-drop-order");
        let observed_lease_held = Arc::new(AtomicBool::new(false));
        let manager = Arc::new(LixRuntimeManager {
            backend: StorageBackend::Memory {
                object_store: Arc::new(InMemory::new()),
            },
            max_open_lixes: 1,
            recovery_watchdog: RecoveryWatchdog::test_default(),
            state: Mutex::new(ManagerState {
                state_drop_probe: Some(CacheRootLeaseDropProbe {
                    root: root.0.clone(),
                    observed_lease_held: Arc::clone(&observed_lease_held),
                }),
                ..ManagerState::default()
            }),
            telemetry: test_telemetry_sink(),
            _cache_root_lease: Some(
                CacheRootLease::acquire(&root.0).expect("acquire cache-root lease"),
            ),
            open_gate: None,
        });
        assert!(manager.has_state_drop_probe());

        drop(manager);

        assert!(
            observed_lease_held.load(Ordering::SeqCst),
            "manager state must drop before releasing the cache-root lease"
        );
        CacheRootLease::acquire(&root.0).expect("lease is released after manager state drops");
    }

    #[test]
    fn cache_namespace_setup_reaps_only_managed_current_and_inactive_entries() {
        let root = TestCacheRoot::new("cache-root-reap");
        let current = namespace_root(&root.0, "https://current.example");
        let inactive = namespace_root(&root.0, "https://inactive.example");
        let retired = retired_cache_path(&current, LIX_A, 7).expect("valid retired path");
        fs::create_dir_all(retired.join("nested")).expect("create retired cache child");
        fs::write(retired.join("nested/cache-part"), b"cached bytes")
            .expect("write retired cache part");
        let stale_current_lix = current.join("lix-stale");
        let current_unknown = current.join(".operator-notes");
        fs::create_dir_all(stale_current_lix.join("nested"))
            .expect("create stale current Lix cache");
        fs::write(stale_current_lix.join("nested/cache-part"), b"cached bytes")
            .expect("write stale current Lix cache part");
        fs::create_dir_all(&current_unknown).expect("create current unknown entry");
        fs::create_dir_all(inactive.join("lix-old")).expect("create inactive namespace");
        fs::write(inactive.join("lix-old/cache-part"), b"cached bytes")
            .expect("write inactive namespace part");
        let legacy_trash = root.0.join(".trash/legacy");
        let legacy_v1_lix = root.0.join("v1/legacy");
        fs::create_dir_all(&legacy_trash).expect("create legacy trash");
        fs::create_dir_all(&legacy_v1_lix).expect("create legacy Lix cache");

        let _lease = CacheRootLease::acquire(&root.0).expect("acquire base cache lease");
        prepare_cache_namespace(&root.0, &current)
            .expect("reap active trash and inactive namespace");

        assert!(!retired.exists());
        assert!(!inactive.exists());
        assert!(!stale_current_lix.exists());
        assert!(current.is_dir());
        assert!(current_unknown.is_dir());
        assert!(legacy_trash.is_dir());
        assert!(legacy_v1_lix.is_dir());
        assert!(root.0.is_dir());
    }

    #[test]
    fn cache_namespace_setup_leaves_unknown_parent_entries_untouched() {
        let root = TestCacheRoot::new("cache-root-unknown-namespace");
        let current = namespace_root(&root.0, "https://current.example");
        let unknown = root.0.join(CACHE_NAMESPACE_PARENT).join("operator-notes");
        fs::create_dir_all(&unknown).expect("create unknown namespace entry");

        let _lease = CacheRootLease::acquire(&root.0).expect("acquire base cache lease");
        prepare_cache_namespace(&root.0, &current)
            .expect("leave unknown namespace entry untouched");

        assert!(unknown.is_dir());
        assert!(current.is_dir());
    }

    #[test]
    fn cache_namespace_setup_refuses_unexpected_active_trash_children() {
        let root = TestCacheRoot::new("cache-root-reap-unexpected");
        let current = namespace_root(&root.0, "https://current.example");
        fs::create_dir_all(current.join(".trash/unmanaged"))
            .expect("create unexpected retired cache child");

        let _lease = CacheRootLease::acquire(&root.0).expect("acquire base cache lease");
        let error = prepare_cache_namespace(&root.0, &current)
            .expect_err("refuse an unexpected retired cache child");

        assert!(format!("{error:#}").contains("unexpected retired cache child"));
    }

    #[cfg(unix)]
    #[test]
    fn cache_namespace_setup_rejects_symlinked_namespace_parent() {
        use std::os::unix::fs::symlink;

        let root = TestCacheRoot::new("cache-root-symlinked-parent");
        let outside = root.0.join("outside");
        fs::create_dir_all(&outside).expect("create symlink target");
        symlink(&outside, root.0.join(CACHE_NAMESPACE_PARENT))
            .expect("create namespace parent symlink");
        let current = namespace_root(&root.0, "https://current.example");

        let _lease = CacheRootLease::acquire(&root.0).expect("acquire base cache lease");
        let error = prepare_cache_namespace(&root.0, &current)
            .expect_err("reject symlinked namespace parent");

        assert!(format!("{error:#}").contains("must be a real directory"));
        assert!(outside.is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn cache_namespace_setup_rejects_symlinked_current_namespace() {
        use std::os::unix::fs::symlink;

        let root = TestCacheRoot::new("cache-root-symlinked-current");
        let current = namespace_root(&root.0, "https://current.example");
        let outside = root.0.join("outside");
        fs::create_dir_all(current.parent().expect("namespace parent"))
            .expect("create namespace parent");
        fs::create_dir_all(&outside).expect("create symlink target");
        symlink(&outside, &current).expect("create current namespace symlink");

        let _lease = CacheRootLease::acquire(&root.0).expect("acquire base cache lease");
        let error = prepare_cache_namespace(&root.0, &current)
            .expect_err("reject symlinked current namespace");

        assert!(format!("{error:#}").contains("must be a real directory"));
        assert!(outside.is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn cache_namespace_setup_rejects_symlinked_inactive_namespace() {
        use std::os::unix::fs::symlink;

        let root = TestCacheRoot::new("cache-root-symlinked-inactive");
        let current = namespace_root(&root.0, "https://current.example");
        let inactive = namespace_root(&root.0, "https://inactive.example");
        let outside = root.0.join("outside");
        fs::create_dir_all(inactive.parent().expect("namespace parent"))
            .expect("create namespace parent");
        fs::create_dir_all(&outside).expect("create symlink target");
        symlink(&outside, &inactive).expect("create inactive namespace symlink");

        let _lease = CacheRootLease::acquire(&root.0).expect("acquire base cache lease");
        let error = prepare_cache_namespace(&root.0, &current)
            .expect_err("refuse to remove a symlinked inactive namespace");

        assert!(format!("{error:#}").contains("refuse to delete non-directory"));
        assert!(outside.is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn cache_base_lease_rejects_a_symlinked_lock() {
        use std::os::unix::fs::symlink;

        let root = TestCacheRoot::new("cache-root-symlinked-lock");
        let outside = root.0.join("outside-lock");
        fs::write(&outside, b"not a lock").expect("write lock target");
        symlink(&outside, root.0.join(CacheRootLease::OWNER_LOCK_NAME))
            .expect("create cache lock symlink");

        let error = CacheRootLease::acquire(&root.0).expect_err("reject cache lock symlink");

        assert!(format!("{error:#}").contains("must be a regular file"));
        assert_eq!(fs::read(&outside).expect("read lock target"), b"not a lock");
    }

    #[test]
    fn cache_namespace_setup_rejects_malformed_namespace_paths() {
        let root = TestCacheRoot::new("cache-root-malformed-namespace");
        let malformed = root
            .0
            .join(CACHE_NAMESPACE_PARENT)
            .join("not-a-digest")
            .join("unexpected-child");

        let _lease = CacheRootLease::acquire(&root.0).expect("acquire base cache lease");
        let error = prepare_cache_namespace(&root.0, &malformed)
            .expect_err("reject malformed namespace path");

        assert!(format!("{error:#}").contains("invalid SlateDB cache namespace"));
    }

    #[cfg(unix)]
    #[test]
    fn cache_deletion_refuses_nested_symlinks() {
        use std::os::unix::fs::symlink;

        let root = TestCacheRoot::new("cache-delete-symlink");
        let cache = root.0.join("retired-cache");
        let outside = root.0.join("outside");
        fs::create_dir_all(&cache).expect("create retired cache");
        fs::create_dir_all(&outside).expect("create symlink target");
        fs::write(outside.join("keep"), b"outside").expect("write symlink target file");
        symlink(&outside, cache.join("escape")).expect("create nested cache symlink");

        let error = delete_cache_directory(&cache, "test cache")
            .expect_err("refuse to recurse through nested cache symlink");

        assert!(format!("{error:#}").contains("refuse to recurse through symlink"));
        assert!(outside.join("keep").is_file());
    }

    #[tokio::test]
    async fn same_lix_open_waits_for_eviction_cleanup() {
        let manager = memory_manager(1);
        let (done, cleanup_waiter) = watch::channel(CleanupState::Running);
        {
            let mut state = manager.state.lock().await;
            state.cleaning.insert(
                LIX_A.to_string(),
                CleanupTombstone {
                    sequence: 1,
                    done: cleanup_waiter,
                },
            );
        }

        let waiting_manager = Arc::clone(&manager);
        let mut opening = tokio::spawn(async move { waiting_manager.get(LIX_A).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut opening)
                .await
                .is_err(),
            "same-ID open must wait until cache retirement has finished"
        );

        manager.finish_cleanup(LIX_A, 1, done, Ok(())).await;
        let service = tokio::time::timeout(Duration::from_secs(10), opening)
            .await
            .expect("same-ID open should resume after cleanup")
            .expect("same-ID open task")
            .expect("open in-memory Lix");
        drop(service);
    }

    #[tokio::test]
    async fn new_lix_open_waits_for_inflight_cleanup_capacity() {
        let manager = memory_manager(1);
        let (sequence, done) = {
            let mut state = manager.state.lock().await;
            start_cleanup(&mut state, LIX_A.to_string()).expect("start test cleanup")
        };

        let waiting_manager = Arc::clone(&manager);
        let mut opening = tokio::spawn(async move { waiting_manager.get(LIX_B).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut opening)
                .await
                .is_err(),
            "an evicting runtime must retain its capacity slot"
        );

        manager.finish_cleanup(LIX_A, sequence, done, Ok(())).await;
        let service = tokio::time::timeout(Duration::from_secs(10), opening)
            .await
            .expect("new Lix should open after cleanup")
            .expect("new Lix open task")
            .expect("open in-memory Lix");
        drop(service);
    }

    #[tokio::test]
    async fn failed_cleanup_keeps_cold_admission_closed() {
        let manager = memory_manager(1);
        let (sequence, done) = {
            let mut state = manager.state.lock().await;
            start_cleanup(&mut state, LIX_A.to_string()).expect("start test cleanup")
        };
        manager
            .finish_cleanup(
                LIX_A,
                sequence,
                done,
                Err(anyhow::anyhow!("test-controlled cache delete failure")),
            )
            .await;

        assert!(matches!(
            manager.get(LIX_B).await,
            Err(LixRuntimeError::Cleanup(_))
        ));
    }

    #[tokio::test]
    async fn recovery_removes_the_runtime_before_a_same_id_reopen() {
        let manager = memory_manager(1);
        let service = manager.get(LIX_A).await.expect("open Lix to recover");
        manager.recover(LIX_A, &service).await;
        drop(service);

        let reopened = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match manager.get(LIX_A).await {
                    Ok(service) => break service,
                    Err(LixRuntimeError::Recovering) => {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    Err(error) => panic!("unexpected recovery result: {error}"),
                }
            }
        })
        .await
        .expect("recovery must release the runtime");
        drop(reopened);
    }

    #[tokio::test]
    async fn recovery_waits_for_inflight_runtime_cell_before_releasing_capacity() {
        let manager = memory_manager(1);
        let service = manager.get(LIX_A).await.expect("open Lix to recover");
        let held_runtime_cell = {
            let state = manager.state.lock().await;
            Arc::clone(
                &state
                    .entries
                    .get(LIX_A)
                    .expect("opened Lix must have a runtime entry")
                    .runtime,
            )
        };

        manager.recover(LIX_A, &service).await;
        drop(service);

        let cleanup_started = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let state = manager.state.lock().await;
                if state.cleaning.contains_key(LIX_A) {
                    return true;
                }
                if !state.entries.contains_key(LIX_A) {
                    return false;
                }
                drop(state);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("recovery should either start cleanup or remove the runtime");
        assert!(
            cleanup_started,
            "recovery completed while an in-flight runtime cell still retained the closed service"
        );

        let replacement_manager = Arc::clone(&manager);
        let mut replacement = tokio::spawn(async move { replacement_manager.get(LIX_B).await });
        assert!(
            tokio::time::timeout(Duration::from_millis(25), &mut replacement)
                .await
                .is_err(),
            "the retained runtime cell must keep recovery capacity occupied"
        );

        drop(held_runtime_cell);
        let replacement = tokio::time::timeout(Duration::from_secs(1), replacement)
            .await
            .expect("replacement should resume after the runtime cell is released")
            .expect("join replacement open")
            .expect("open replacement Lix");
        drop(replacement);
    }

    #[tokio::test]
    async fn fenced_protocol_runtime_retires_without_waiting_for_finite_error_body() {
        let manager = memory_manager(1);
        let app = crate::router(
            Arc::clone(&manager),
            None,
            Duration::from_secs(60),
            crate::telemetry::InFlightSqlRegistry::default(),
        );
        let session = open_protocol_session(&app, LIX_A).await;
        let object_store = match &manager.backend {
            StorageBackend::Memory { object_store } => Arc::clone(object_store),
            StorageBackend::S3 { .. } => unreachable!("memory manager must use in-memory storage"),
        };
        let fencer = SlateDB::open_object_store_with_options(
            LIX_A,
            object_store,
            SlateDBObjectStoreOptions::default(),
        )
        .expect("open newer SlateDB writer");

        // A completed second open fences the old SlateDB client, but the old
        // client's asynchronous manifest poll is what surfaces that terminal
        // state. Probe with an idempotent read rather than falsely assuming a
        // just-fenced non-durable mutation will fail synchronously.
        let deadline = Instant::now() + Duration::from_secs(10);
        let response = loop {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/lix/v1/11111111-1111-4111-8111-111111111111/execute")
                        .header(lix_sdk::server_protocol::SESSION_ID_HEADER, &session)
                        .header("content-type", "application/json")
                        .body(Body::from(r#"{"sql":"SELECT 1"}"#))
                        .expect("fence probe request"),
                )
                .await
                .expect("fence probe response");
            if response.status() == http::StatusCode::CONFLICT {
                break response;
            }
            assert_eq!(
                response.status(),
                http::StatusCode::OK,
                "fence probe must either succeed before SlateDB closes the old writer or report its terminal error"
            );
            drop(response);
            assert!(
                Instant::now() < deadline,
                "SlateDB did not report the fenced writer within the test deadline"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };

        assert_eq!(response.status(), http::StatusCode::CONFLICT);
        assert!(response.headers().get(header::RETRY_AFTER).is_none());
        assert!(matches!(
            manager.get(LIX_A).await,
            Err(LixRuntimeError::Recovering)
        ));

        // A finite JSON error is already serialized. Holding its body must
        // not keep the fenced runtime (and its capacity slot) alive forever.
        tokio::time::timeout(Duration::from_secs(10), async {
            while manager.cached_lix_count().await != 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("fenced runtime must retire without waiting for its finite response body");

        let error: JsonValue = serde_json::from_slice(
            &response
                .into_body()
                .collect()
                .await
                .expect("fenced response body")
                .to_bytes(),
        )
        .expect("fenced response JSON");
        assert_eq!(error["error"]["code"], "LIX_STORAGE_FENCED");
        assert_eq!(
            error["error"]["details"],
            json!({
                "retryable": false,
                "outcome": "unknown",
            })
        );

        drop(fencer);
        let reopened_session = open_protocol_session(&app, LIX_A).await;
        let reopened = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/lix/v1/11111111-1111-4111-8111-111111111111/execute")
                    .header(
                        lix_sdk::server_protocol::SESSION_ID_HEADER,
                        reopened_session,
                    )
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sql":"SELECT 1"}"#))
                    .expect("reopened query request"),
            )
            .await
            .expect("reopened query response");
        assert_eq!(reopened.status(), http::StatusCode::OK);
    }

    async fn open_protocol_session(app: &axum::Router, lix_id: &str) -> String {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/lix/v1/{lix_id}/"))
                    .body(Body::empty())
                    .expect("protocol handshake request"),
            )
            .await
            .expect("protocol handshake response");
        assert_eq!(response.status(), http::StatusCode::OK);
        let body: JsonValue = serde_json::from_slice(
            &response
                .into_body()
                .collect()
                .await
                .expect("protocol handshake body")
                .to_bytes(),
        )
        .expect("protocol handshake JSON");
        body["sessionId"]
            .as_str()
            .expect("protocol handshake session id")
            .to_string()
    }

    #[tokio::test]
    async fn s3_request_policy_times_out_a_blackholed_head_request() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind blackhole listener");
        let endpoint = format!(
            "http://{}",
            listener.local_addr().expect("blackhole listener address")
        );
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let blackhole = tokio::spawn(async move {
            let (connection, _) = listener
                .accept()
                .await
                .expect("accept object-store request");
            let _ = accepted_tx.send(());
            std::future::pending::<()>().await;
            drop(connection);
        });
        let test_budget = S3RequestBudget {
            request_timeout: Duration::from_millis(100),
            connect_timeout: Duration::from_millis(50),
            retry_timeout: Duration::from_millis(100),
            max_retries: 0,
        };
        let object_store = AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name("test-bucket")
            .with_access_key_id("test-access-key")
            .with_secret_access_key("test-secret-key")
            .with_region("auto")
            .with_client_options(s3_client_options(test_budget))
            .with_retry(s3_retry_config(test_budget))
            .with_allow_http(true)
            .build()
            .expect("build blackhole object store");

        let started = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            object_store.head(&ObjectPath::from("blackholed-head")),
        )
        .await;
        let acceptance = tokio::time::timeout(Duration::from_secs(1), accepted_rx).await;
        blackhole.abort();
        let _ = blackhole.await;
        assert!(
            matches!(acceptance, Ok(Ok(()))),
            "the object-store request never reached the blackhole"
        );

        assert!(
            matches!(result, Ok(Err(_))),
            "unexpected result: {result:?}"
        );
        let elapsed = started.elapsed();
        eprintln!("blackholed S3 HEAD returned after {elapsed:?}");
        assert!(
            elapsed < Duration::from_millis(750),
            "blackholed request exceeded the configured budget: {:?}",
            elapsed
        );
    }
}
