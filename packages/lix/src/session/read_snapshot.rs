//! Pinned read snapshots: several reads and streams at one repository state.
//!
//! A [`ReadSnapshot`] retains one storage read view plus the session's branch
//! selection at the moment it opened. Every buffered read and stream issued
//! through it runs against that view, so they observe one coherent state no
//! matter what commits in between. Streams opened on a snapshot share its view
//! (they do not open their own) and may be live concurrently.
//!
//! Lifetime: the view is released when the snapshot is closed, or once its
//! last handle and its last stream are dropped (a stream keeps reading after
//! the handle that opened it is gone). Closing waits until reads in flight
//! have dropped the view. Closing the session closes its snapshots.
//!
//! Nothing in the engine expires a snapshot. Two storage-level exceptions:
//! a backend may expire a long-held read view, and on a partial replica the
//! authority may stop retaining inputs the snapshot still has to fetch. Both
//! surface as `LIX_STORAGE_READ_EXPIRED`; open a new snapshot to continue.

use std::sync::{Arc, Mutex, Weak};

use crate::branch::BranchRefReader;
use crate::storage_adapter::{
    SharedStorageAdapterRead, Storage, StorageAdapter, StorageReadOptions,
};
use crate::{ExecuteResult, LixError, Value};

use super::execute::{
    QueryStreamProgress, QueryStreamRoute, assume_static_storage_read, finish_query_stream,
    normalize_sql_surface_error,
};
use super::query_stream::{PageLimits, QueryStreamRegistry, RowStream};
use super::{SessionBranch, SessionContext};

/// A storage read view together with the storage it borrows from.
///
/// The read is widened to `'static`; holding the storage `Arc` alongside it
/// keeps the borrowed storage alive. Fields drop in declaration order, so the
/// read is always released before its storage.
pub(crate) struct PinnedRead<StorageImpl: Storage + 'static> {
    pub(crate) read: SharedStorageAdapterRead<StorageImpl::Read<'static>>,
    storage: Arc<StorageAdapter<StorageImpl>>,
    /// Which extension of the snapshot's view this is (partial replicas
    /// layer hydrated inputs over the pinned view).
    generation: u64,
    _in_flight: InFlight,
}

impl<StorageImpl: Storage + 'static> Clone for PinnedRead<StorageImpl> {
    fn clone(&self) -> Self {
        Self {
            read: self.read.clone(),
            storage: Arc::clone(&self.storage),
            generation: self.generation,
            _in_flight: self._in_flight.clone(),
        }
    }
}

/// Counts live holders of a snapshot's view, so close can wait for them.
#[derive(Default)]
struct InFlightReads {
    count: std::sync::atomic::AtomicUsize,
    released: tokio::sync::Notify,
}

struct InFlight(Arc<InFlightReads>);

impl InFlight {
    fn new(reads: &Arc<InFlightReads>) -> Self {
        reads
            .count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Self(Arc::clone(reads))
    }
}

impl Clone for InFlight {
    fn clone(&self) -> Self {
        Self::new(&self.0)
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        if self
            .0
            .count
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel)
            == 1
        {
            self.0.released.notify_waiters();
        }
    }
}

enum SnapshotState<StorageImpl: Storage + 'static> {
    Open(PinnedRead<StorageImpl>),
    Closed,
}

/// One pinned repository state serving reads and streams. See the module docs.
pub(crate) struct ReadSnapshot<StorageImpl: Storage + 'static> {
    state: Mutex<SnapshotState<StorageImpl>>,
    /// The opening session with its branch selection frozen.
    session: Arc<SessionContext<StorageImpl>>,
    active_branch_id: String,
    commit_id: String,
    storage_mutation_revision: Option<Vec<u8>>,
    streams: Arc<QueryStreamRegistry>,
    in_flight: Arc<InFlightReads>,
    /// Partial replicas fetch missing immutable inputs through this channel
    /// and layer them over the pinned view; the pinned state never moves.
    sync_demand_tx: Option<tokio::sync::mpsc::Sender<crate::sync::SyncDemand>>,
}

/// Every open snapshot of one session, so closing the session closes them.
#[derive(Default)]
pub(crate) struct ReadSnapshotRegistry {
    snapshots: Mutex<Vec<Weak<dyn CloseSnapshot>>>,
}

pub(crate) trait CloseSnapshot: Send + Sync {
    fn close_snapshot(&self);
}

impl ReadSnapshotRegistry {
    fn register(&self, snapshot: Weak<dyn CloseSnapshot>) {
        let mut snapshots = self
            .snapshots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        snapshots.retain(|snapshot| snapshot.strong_count() > 0);
        snapshots.push(snapshot);
    }

    pub(crate) fn close_all(&self) {
        let snapshots = std::mem::take(
            &mut *self
                .snapshots
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for snapshot in snapshots.iter().filter_map(Weak::upgrade) {
            snapshot.close_snapshot();
        }
    }
}

impl<StorageImpl> CloseSnapshot for ReadSnapshot<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    fn close_snapshot(&self) {
        self.begin_close();
    }
}

fn snapshot_inputs_expired() -> LixError {
    LixError::new(
        LixError::CODE_STORAGE_READ_EXPIRED,
        "the authority no longer retains inputs this snapshot still has to fetch",
    )
    .with_hint("Open a new snapshot to read the current state.")
}

pub(crate) fn snapshot_closed() -> LixError {
    LixError::new(LixError::CODE_SNAPSHOT_CLOSED, "the snapshot is closed")
        .with_hint("Open a new snapshot to read the current state.")
}

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Pins the current repository state of this session's active branch.
    pub(crate) async fn open_read_snapshot(
        &self,
        sync_demand_tx: Option<tokio::sync::mpsc::Sender<crate::sync::SyncDemand>>,
    ) -> Result<Arc<ReadSnapshot<StorageImpl>>, LixError> {
        self.ensure_open()?;
        self.refresh_active_branch_base_if_stale().await?;
        let _operation_guard = self.begin_waitable_session_operation().await?;
        let branch_id = self.bound_branch_id()?;
        let storage = Arc::new(self.storage.clone());
        let in_flight = Arc::new(InFlightReads::default());
        let mut retries = crate::common::ExpiredReadRetryState::default();
        let (pinned, commit_id, storage_mutation_revision) = loop {
            let attempt = async {
                let read = storage.begin_read(StorageReadOptions::default()).await?;
                // SAFETY: the widened read is stored in a `PinnedRead` next to
                // the `Arc` that owns the storage it borrows, and that type
                // releases the read first.
                let read = unsafe { assume_static_storage_read::<StorageImpl>(read) };
                let pinned = PinnedRead {
                    read: SharedStorageAdapterRead::new(read),
                    storage: Arc::clone(&storage),
                    generation: 0,
                    _in_flight: InFlight::new(&in_flight),
                };
                let head = self
                    .branch_ctx
                    .ref_reader(pinned.read.clone())
                    .load_head(&branch_id)
                    .await?
                    .ok_or_else(|| {
                        LixError::branch_not_found(
                            branch_id.clone(),
                            "open snapshot",
                            "active branch",
                        )
                    })?;
                let revision =
                    StorageAdapter::<StorageImpl>::load_mutation_revision_from_read(&pinned.read)
                        .await?
                        .map(|revision| revision.to_vec());
                Ok::<_, LixError>((pinned, head.commit_id.to_string(), revision))
            }
            .await;
            match attempt {
                Ok(opened) => break opened,
                Err(error) => {
                    let Some(delay) = retries.next_delay(&error) else {
                        return Err(error);
                    };
                    tokio::task::yield_now().await;
                    if !delay.is_zero() {
                        crate::sync::sleep(delay).await;
                    }
                }
            }
        };
        let mut session = self.clone();
        session.branch = SessionBranch::new(branch_id.clone());
        let snapshot = Arc::new(ReadSnapshot {
            state: Mutex::new(SnapshotState::Open(pinned)),
            session: Arc::new(session),
            active_branch_id: branch_id,
            commit_id,
            storage_mutation_revision,
            streams: Arc::default(),
            in_flight,
            sync_demand_tx: sync_demand_tx
                .filter(|_| self.sync_mode.role() == crate::sync::SyncRole::PartialReplica),
        });
        let weak = Arc::downgrade(&snapshot);
        let weak: Weak<dyn CloseSnapshot> = weak;
        self.read_snapshots.register(weak);
        // A close that ran while this snapshot was opening has already
        // drained the registry; do not hand out a snapshot it missed.
        if let Err(error) = self.ensure_open() {
            snapshot.begin_close();
            return Err(error);
        }
        Ok(snapshot)
    }
}

impl<StorageImpl> ReadSnapshot<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    pub(crate) fn active_branch_id(&self) -> &str {
        &self.active_branch_id
    }

    pub(crate) fn commit_id(&self) -> &str {
        &self.commit_id
    }

    pub(crate) fn storage_mutation_revision(&self) -> Option<&[u8]> {
        self.storage_mutation_revision.as_deref()
    }

    pub(crate) fn is_closed(&self) -> bool {
        matches!(
            *self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            SnapshotState::Closed
        )
    }

    fn ensure_open(&self) -> Result<(), LixError> {
        self.session.ensure_open()?;
        if self.is_closed() {
            return Err(snapshot_closed());
        }
        Ok(())
    }

    fn pinned(&self) -> Result<PinnedRead<StorageImpl>, LixError> {
        self.session.ensure_open()?;
        match &*self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            SnapshotState::Open(pinned) => Ok(pinned.clone()),
            SnapshotState::Closed => Err(snapshot_closed()),
        }
    }

    /// Closes the snapshot for every clone: its streams stop and later reads
    /// fail with `LIX_ERROR_SNAPSHOT_CLOSED`. Reads already running keep their view
    /// until they finish.
    fn begin_close(&self) {
        let previous = std::mem::replace(
            &mut *self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            SnapshotState::Closed,
        );
        self.streams.cancel_all(snapshot_closed);
        drop(previous);
    }

    /// Closes the snapshot and waits until every read that was in flight has
    /// released the view. Idempotent.
    pub(crate) async fn close(&self) {
        self.begin_close();
        loop {
            let released = self.in_flight.released.notified();
            futures_util::pin_mut!(released);
            released.as_mut().enable();
            if self
                .in_flight
                .count
                .load(std::sync::atomic::Ordering::Acquire)
                == 0
            {
                return;
            }
            released.await;
        }
    }

    /// Runs one buffered read against the pinned state, under the read
    /// limits of the calling scope.
    pub(crate) async fn query(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecuteResult, LixError> {
        let session = &self.session;
        let statement = session.parse_read_only_statement(sql, params)?;
        let has_runtime_function = crate::sql2::statement_has_durable_runtime_function(&statement);
        let mut retry = crate::sync::SyncDemandRetry::default();
        let (read_result, file_view_mutations, _, captured_interests) = loop {
            // Hold the session guard only while reading, never across the
            // network wait of a hydration, which would stall writers and
            // close behind a slow transport.
            let operation_guard = session.begin_waitable_session_operation().await?;
            let pinned = self.pinned()?;
            let read_plan = crate::sql2::plan_read_statement(&statement, params);
            let acknowledge_file_views = read_plan.acknowledge_file_views;
            let error = match session
                .execute_read_statement_with_store(
                    pinned.read.clone(),
                    sql,
                    statement.clone(),
                    params,
                    acknowledge_file_views,
                    read_plan,
                    has_runtime_function,
                )
                .await
            {
                Ok(result) => break result,
                Err(error) => error,
            };
            drop(operation_guard);
            if !self
                .extend_with_hydrated_inputs(&pinned, &error, &mut retry)
                .await?
            {
                return Err(normalize_sql_surface_error(error, sql));
            }
        };
        // Outside deterministic mode uuidv7() and current_timestamp use the
        // system clock and persist nothing. Deterministic mode advances a
        // persisted sequence, which a pinned snapshot cannot do without
        // reusing values; such reads belong on the live handle.
        if read_result
            .runtime_functions
            .as_ref()
            .is_some_and(crate::functions::FunctionContext::deterministic_mode_enabled)
        {
            return Err(LixError::new(
                LixError::CODE_READ_ONLY,
                "in deterministic mode, uuidv7() and current_timestamp advance a persisted \
                 sequence that a pinned snapshot cannot advance",
            )
            .with_hint("Call them through Lix::query() or execute() instead of a snapshot."));
        }
        if let Some(capture) = captured_interests {
            capture.publish_capture()?;
        }
        let mut read_result = read_result;
        read_result.runtime_functions = None;
        let result = ExecuteResult::from_session_read_result(read_result)
            .with_file_view_mutations(file_view_mutations);
        session
            .file_views
            .apply_mutations(result.file_view_mutations().iter().cloned());
        session
            .flush_partial_read_interests()
            .await
            .map_err(super::context::non_retryable_after_execution)?;
        Ok(result)
    }

    /// On a partial replica, fetches the immutable inputs a failed read named
    /// and layers them over the pinned view, so the read can be retried at
    /// the same state. Returns false when the error is not such a miss.
    async fn extend_with_hydrated_inputs(
        &self,
        pinned: &PinnedRead<StorageImpl>,
        error: &LixError,
        retry: &mut crate::sync::SyncDemandRetry,
    ) -> Result<bool, LixError> {
        let Some(sender) = &self.sync_demand_tx else {
            return Ok(false);
        };
        if error.automatic_retry_is_forbidden() || !names_immutable_inputs(error)? {
            return Ok(false);
        }
        let hydrated = retry
            .hydrate_pinned_for_retry(Some(sender), error.clone())
            .await
            .map_err(|error| {
                if error.code == LixError::CODE_TRANSACTION_CONFLICT {
                    snapshot_inputs_expired()
                } else {
                    error
                }
            })?;
        // Layer the hydrated inputs over the snapshot's current view. When a
        // concurrent read extended the view meanwhile, extend that extension
        // instead of replacing it, so neither read's inputs are lost.
        let mut base = pinned.clone();
        loop {
            let Some(read) = crate::transaction::extend_pinned_read_with_hydrated_inputs(
                &base.storage,
                &base.read,
                error,
                &hydrated,
                |manifests| Ok(vec![false; manifests.len()]),
                snapshot_inputs_expired,
            )
            .await?
            else {
                return Ok(false);
            };
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match &mut *state {
                SnapshotState::Open(current) if current.generation == base.generation => {
                    // The extension borrows the same storage Arc the pinned
                    // read keeps alive.
                    *current = PinnedRead {
                        read,
                        storage: Arc::clone(&base.storage),
                        generation: base.generation + 1,
                        _in_flight: InFlight::new(&self.in_flight),
                    };
                    return Ok(true);
                }
                SnapshotState::Open(current) => base = current.clone(),
                SnapshotState::Closed => return Err(snapshot_closed()),
            }
        }
    }

    /// Opens a stream over the pinned state. Live streams share the
    /// snapshot's read view; statements that need their whole result first
    /// are read buffered (default byte cap) and paged.
    pub(crate) async fn stream(
        self: &Arc<Self>,
        sql: &str,
        params: &[Value],
        limits: PageLimits,
        buffer_limits: crate::common::ReadLimits,
    ) -> Result<RowStream, LixError> {
        let session = &self.session;
        let statement = session.parse_read_only_statement(sql, params)?;
        self.ensure_open()?;
        match session.query_stream_route(&statement, params) {
            QueryStreamRoute::Materialized => {
                let snapshot = Arc::clone(self);
                let sql = sql.to_owned();
                let params = params.to_vec();
                // SAFETY: the future owns its snapshot Arc and request values,
                // like the producer of a live stream.
                let result = unsafe {
                    super::AssumeSendFuture::new(async move {
                        crate::common::with_read_limits(
                            buffer_limits,
                            snapshot.query(&sql, &params),
                        )
                        .await
                        .map_err(crate::handle::buffered_stream_error)
                    })
                };
                session
                    .open_materialized_query_stream(result, limits, Some(&self.streams), || {
                        self.ensure_open()
                    })
                    .await
            }
            QueryStreamRoute::Live => {
                let pinned = self.pinned()?;
                let (sender, receiver) = super::query_stream::query_stream_channel();
                let producer_session = Arc::clone(session);
                let sql = sql.to_owned();
                let params = params.to_vec();
                // SAFETY: the producer owns the session Arc, the pinned read
                // (with the storage it borrows) and its request values.
                let producer = unsafe {
                    super::AssumeSendFuture::new(async move {
                        let mut progress = QueryStreamProgress::default();
                        let outcome = producer_session
                            .stream_read_statement_with_store(
                                pinned.read.clone(),
                                &sql,
                                statement,
                                &params,
                                limits,
                                &sender,
                                &mut progress,
                            )
                            .await;
                        finish_query_stream(outcome, &sql, &sender).await;
                        drop(pinned);
                    })
                };
                RowStream::open(
                    &[&session.query_streams, &self.streams],
                    || self.ensure_open(),
                    receiver,
                    producer,
                    Some(session.query_stream_admission()),
                )
                .await
            }
        }
    }
}

/// Whether `error` names immutable inputs a partial replica can fetch without
/// moving a pinned snapshot.
fn names_immutable_inputs(error: &LixError) -> Result<bool, LixError> {
    Ok(
        crate::tracked_state::NativeObjectRef::from_missing_error(error)?.is_some()
            || crate::tracked_state::NativeMetadataRef::from_missing_error(error)?.is_some()
            || crate::binary_cas::BlobManifestsRequired::from_error(error)?.is_some()
            || error.code == "LIX_SYNC_CHUNKS_REQUIRED",
    )
}
