mod state_change_record;

use crate::backend::{execute_ddl_batch, LixBackend, LixBackendTransaction, QueryExecutor};
use crate::sql::PlannedStateRow;
use crate::sql::{MutationOperation, MutationRow};
use crate::{LixError, Value};
use futures_util::future::poll_fn;
use futures_util::task::AtomicWaker;
use serde_json::Value as JsonValue;
pub(crate) use state_change_record::StateChangeRecord;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;

const MAX_PENDING_BATCHES_PER_LISTENER: usize = 256;
const DETERMINISTIC_SETTINGS_SCHEMA_KEY: &str = "lix_key_value";
const DETERMINISTIC_SETTINGS_ENTITY_ID: &str = "lix_deterministic_mode";
pub(crate) const DURABLE_STATE_COMMIT_CONSUMER_CURSOR_TABLE: &str =
    "lix_internal_change_consumer_cursor";
pub(crate) const LIVE_STATE_DURABLE_CONSUMER_KEY: &str = "live_state_projection";

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub enum StateCommitStreamOperation {
    Insert,
    Update,
    Delete,
}

/// Committed semantic change delivered through the state-commit stream.
///
/// `origin_key` is delivery metadata attached to the emitted batch. It is not
/// part of the durable row model exposed by query surfaces such as `lix_file`
/// or `lix_directory`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct StateCommitStreamChange {
    pub operation: StateCommitStreamOperation,
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: Option<String>,
    pub version_id: String,
    pub plugin_key: Option<String>,
    pub snapshot_content: Option<JsonValue>,
    pub untracked: bool,
    #[serde(default)]
    pub origin_key: Option<String>,
}

/// Subscription filter for the state-commit stream.
///
/// Origin filters operate on delivery metadata carried by the emitted change
/// batches. They do not require callers to project origin data through SQL.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct StateCommitStreamFilter {
    pub schema_keys: Vec<String>,
    pub entity_ids: Vec<String>,
    pub file_ids: Vec<String>,
    pub version_ids: Vec<String>,
    #[serde(default)]
    pub include_origin_keys: Vec<String>,
    #[serde(default)]
    pub exclude_origin_keys: Vec<String>,
    #[serde(default)]
    pub exclude_self: bool,
    pub include_untracked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DurableStateCommitCursor {
    pub(crate) change_id: String,
    pub(crate) created_at: String,
    pub(crate) visibility_append_seq: i64,
}

impl DurableStateCommitCursor {
    fn is_newer_than(&self, other: &Self) -> bool {
        self.created_at > other.created_at
            || (self.created_at == other.created_at && self.change_id > other.change_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DurableStateCommitCatchUp {
    pub(crate) latest_cursor: Option<DurableStateCommitCursor>,
    pub(crate) has_matching_changes: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DurableStateCommitConsumerCursor {
    pub(crate) consumer_key: String,
    pub(crate) cursor: DurableStateCommitCursor,
}

impl Default for StateCommitStreamFilter {
    fn default() -> Self {
        Self {
            schema_keys: Vec::new(),
            entity_ids: Vec::new(),
            file_ids: Vec::new(),
            version_ids: Vec::new(),
            include_origin_keys: Vec::new(),
            exclude_origin_keys: Vec::new(),
            exclude_self: false,
            include_untracked: true,
        }
    }
}

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    let statements = [
        format!(
            "CREATE TABLE IF NOT EXISTS {DURABLE_STATE_COMMIT_CONSUMER_CURSOR_TABLE} (\
             consumer_key TEXT PRIMARY KEY,\
             change_id TEXT NOT NULL,\
             change_created_at TEXT NOT NULL,\
             visibility_append_seq BIGINT NOT NULL DEFAULT 0\
             )"
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS idx_lix_internal_change_consumer_cursor_cursor \
             ON {DURABLE_STATE_COMMIT_CONSUMER_CURSOR_TABLE} \
             (visibility_append_seq, change_created_at, change_id)"
        ),
    ];
    let statement_refs = statements.iter().map(String::as_str).collect::<Vec<_>>();
    execute_ddl_batch(backend, "streams", &statement_refs).await
}

pub(crate) async fn latest_durable_state_commit_cursor(
    backend: &dyn LixBackend,
) -> Result<Option<DurableStateCommitCursor>, LixError> {
    let result = backend
        .execute(
            "SELECT id, created_at \
             FROM lix_internal_change \
             ORDER BY created_at DESC, id DESC \
             LIMIT 1",
            &[],
        )
        .await?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    let mut executor = backend;
    let visibility_append_seq =
        load_latest_untracked_visibility_append_seq_with_executor(&mut executor).await?;
    Ok(Some(DurableStateCommitCursor {
        change_id: required_text_value(row.first(), "lix_internal_change.id")?,
        created_at: required_text_value(row.get(1), "lix_internal_change.created_at")?,
        visibility_append_seq,
    }))
}

#[allow(dead_code)]
pub(crate) async fn load_durable_state_commit_consumer_cursors(
    backend: &dyn LixBackend,
) -> Result<Vec<DurableStateCommitConsumerCursor>, LixError> {
    let result = backend
        .execute(
            &format!(
                "SELECT consumer_key, change_id, change_created_at \
                 , visibility_append_seq \
                 FROM {DURABLE_STATE_COMMIT_CONSUMER_CURSOR_TABLE} \
                 ORDER BY visibility_append_seq ASC, change_created_at ASC, change_id ASC, consumer_key ASC"
            ),
            &[],
        )
        .await?;

    result
        .rows
        .iter()
        .map(|row| {
            Ok(DurableStateCommitConsumerCursor {
                consumer_key: required_text_value(
                    row.first(),
                    "lix_internal_change_consumer_cursor.consumer_key",
                )?,
                cursor: DurableStateCommitCursor {
                    change_id: required_text_value(
                        row.get(1),
                        "lix_internal_change_consumer_cursor.change_id",
                    )?,
                    created_at: required_text_value(
                        row.get(2),
                        "lix_internal_change_consumer_cursor.change_created_at",
                    )?,
                    visibility_append_seq: required_integer_value(
                        row.get(3),
                        "lix_internal_change_consumer_cursor.visibility_append_seq",
                    )?,
                },
            })
        })
        .collect()
}

#[allow(dead_code)]
pub(crate) async fn load_durable_state_commit_low_watermark(
    backend: &dyn LixBackend,
) -> Result<Option<DurableStateCommitCursor>, LixError> {
    let result = backend
        .execute(
            &format!(
                "SELECT change_id, change_created_at \
                 , visibility_append_seq \
                 FROM {DURABLE_STATE_COMMIT_CONSUMER_CURSOR_TABLE} \
                 ORDER BY visibility_append_seq ASC, change_created_at ASC, change_id ASC, consumer_key ASC \
                 LIMIT 1"
            ),
            &[],
        )
        .await?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    Ok(Some(DurableStateCommitCursor {
        change_id: required_text_value(
            row.first(),
            "lix_internal_change_consumer_cursor.change_id",
        )?,
        created_at: required_text_value(
            row.get(1),
            "lix_internal_change_consumer_cursor.change_created_at",
        )?,
        visibility_append_seq: required_integer_value(
            row.get(2),
            "lix_internal_change_consumer_cursor.visibility_append_seq",
        )?,
    }))
}

pub(crate) async fn upsert_durable_state_commit_consumer_cursor_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    consumer_key: &str,
    cursor: &DurableStateCommitCursor,
) -> Result<(), LixError> {
    let mut executor = &mut *transaction;
    upsert_durable_state_commit_consumer_cursor_with_executor(&mut executor, consumer_key, cursor)
        .await
}

pub(crate) async fn upsert_durable_state_commit_consumer_cursor_with_backend(
    backend: &dyn LixBackend,
    consumer_key: &str,
    cursor: &DurableStateCommitCursor,
) -> Result<(), LixError> {
    let mut executor = backend;
    upsert_durable_state_commit_consumer_cursor_with_executor(&mut executor, consumer_key, cursor)
        .await
}

async fn upsert_durable_state_commit_consumer_cursor_with_executor(
    executor: &mut dyn QueryExecutor,
    consumer_key: &str,
    cursor: &DurableStateCommitCursor,
) -> Result<(), LixError> {
    let p1 = executor.dialect().placeholder(1);
    let p2 = executor.dialect().placeholder(2);
    let p3 = executor.dialect().placeholder(3);
    let p4 = executor.dialect().placeholder(4);
    executor
        .execute(
            &format!(
                "INSERT INTO {DURABLE_STATE_COMMIT_CONSUMER_CURSOR_TABLE} \
                 (consumer_key, change_id, change_created_at, visibility_append_seq) \
                 VALUES ({p1}, {p2}, {p3}, {p4}) \
                 ON CONFLICT (consumer_key) DO UPDATE SET \
                   change_id = excluded.change_id, \
                   change_created_at = excluded.change_created_at, \
                   visibility_append_seq = excluded.visibility_append_seq"
            ),
            &[
                Value::Text(consumer_key.to_string()),
                Value::Text(cursor.change_id.clone()),
                Value::Text(cursor.created_at.clone()),
                Value::Integer(cursor.visibility_append_seq),
            ],
        )
        .await?;
    Ok(())
}

pub(crate) async fn load_latest_untracked_visibility_append_seq(
    backend: &dyn LixBackend,
) -> Result<i64, LixError> {
    let mut executor = backend;
    load_latest_untracked_visibility_append_seq_with_executor(&mut executor).await
}

pub(crate) async fn load_latest_untracked_visibility_append_seq_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<i64, LixError> {
    let mut executor = &mut *transaction;
    load_latest_untracked_visibility_append_seq_with_executor(&mut executor).await
}

async fn load_latest_untracked_visibility_append_seq_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<i64, LixError> {
    let result = executor
        .execute(
            "SELECT COALESCE(MAX(append_seq), 0) \
             FROM lix_internal_untracked_change_visibility",
            &[],
        )
        .await?;
    let Some(row) = result.rows.first() else {
        return Ok(0);
    };
    required_integer_value(
        row.first(),
        "lix_internal_untracked_change_visibility.append_seq",
    )
}

pub(crate) async fn delete_durable_state_commit_consumer_cursor_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    consumer_key: &str,
) -> Result<(), LixError> {
    let placeholder = transaction.dialect().placeholder(1);
    transaction
        .execute(
            &format!(
                "DELETE FROM {DURABLE_STATE_COMMIT_CONSUMER_CURSOR_TABLE} \
                 WHERE consumer_key = {placeholder}"
            ),
            &[Value::Text(consumer_key.to_string())],
        )
        .await?;
    Ok(())
}

pub(crate) async fn load_durable_state_commit_catch_up(
    backend: &dyn LixBackend,
    after: Option<&DurableStateCommitCursor>,
    filter: &StateCommitStreamFilter,
) -> Result<DurableStateCommitCatchUp, LixError> {
    let latest_cursor = latest_durable_state_commit_cursor(backend).await?;
    let Some(latest_cursor_ref) = latest_cursor.as_ref() else {
        return Ok(DurableStateCommitCatchUp {
            latest_cursor,
            has_matching_changes: false,
        });
    };
    if after.is_some_and(|cursor| !latest_cursor_ref.is_newer_than(cursor)) {
        return Ok(DurableStateCommitCatchUp {
            latest_cursor,
            has_matching_changes: false,
        });
    }

    let has_matching_changes =
        durable_state_commit_changes_exist_since(backend, after, filter).await?;
    Ok(DurableStateCommitCatchUp {
        latest_cursor,
        has_matching_changes,
    })
}

impl StateCommitStreamFilter {
    /// Drop batches authored by the current session's effective `origin_key`.
    pub fn exclude_self() -> Self {
        Self {
            exclude_self: true,
            ..Self::default()
        }
    }

    pub(crate) fn resolved_for_origin_key(&self, origin_key: &str) -> Self {
        let mut resolved = self.clone();
        if resolved.exclude_self {
            let origin_key = origin_key.trim();
            if !origin_key.is_empty()
                && !resolved
                    .exclude_origin_keys
                    .iter()
                    .any(|value| value.trim() == origin_key)
            {
                resolved.exclude_origin_keys.push(origin_key.to_string());
            }
        }
        resolved.exclude_self = false;
        resolved
    }

    pub(crate) fn resolved_without_session_origin(&self) -> Self {
        let mut resolved = self.clone();
        resolved.exclude_self = false;
        resolved
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StateCommitStreamRuntimeMetadata {
    pub origin_key: Option<String>,
}

impl StateCommitStreamRuntimeMetadata {
    pub fn from_runtime_origin_key(origin_key: Option<&str>) -> Self {
        Self {
            origin_key: origin_key.map(str::to_string),
        }
    }
}

pub fn state_commit_stream_changes_from_mutations(
    mutations: &[MutationRow],
    runtime_metadata: StateCommitStreamRuntimeMetadata,
) -> Vec<StateCommitStreamChange> {
    if mutations.is_empty() {
        return Vec::new();
    }

    mutations
        .iter()
        .map(|mutation| StateCommitStreamChange {
            operation: map_mutation_operation(&mutation.operation),
            entity_id: mutation.entity_id.clone(),
            schema_key: mutation.schema_key.clone(),
            schema_version: mutation.schema_version.clone(),
            file_id: mutation.file_id.clone(),
            version_id: mutation.version_id.clone(),
            plugin_key: mutation.plugin_key.clone(),
            snapshot_content: mutation.snapshot_content.clone(),
            untracked: mutation.untracked,
            origin_key: runtime_metadata.origin_key.clone(),
        })
        .collect()
}

pub(crate) fn state_commit_stream_changes_from_changes<Change: StateChangeRecord>(
    changes: &[Change],
    operation: StateCommitStreamOperation,
    runtime_metadata: StateCommitStreamRuntimeMetadata,
) -> Result<Vec<StateCommitStreamChange>, LixError> {
    if changes.is_empty() {
        return Ok(Vec::new());
    }

    let mut resolved = Vec::with_capacity(changes.len());
    for change in changes {
        let snapshot_content = match change.snapshot_content() {
            Some(snapshot_content) => Some(serde_json::from_str(snapshot_content).map_err(
                |error| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "change state commit stream expected JSON snapshot_content text: {error}"
                    ),
                    hint: None,
                },
            )?),
            None => None,
        };
        resolved.push(StateCommitStreamChange {
            operation,
            entity_id: change.entity_id().to_string(),
            schema_key: change.schema_key().to_string(),
            schema_version: change
                .schema_version()
                .ok_or_else(|| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: "change state commit stream requires schema_version".to_string(),
                    hint: None,
                })?
                .to_string(),
            file_id: change.file_id().map(str::to_string),
            version_id: change.version_id().to_string(),
            plugin_key: change.plugin_key().map(str::to_string),
            snapshot_content,
            untracked: false,
            origin_key: state_commit_stream_origin_key(change.origin_key(), &runtime_metadata),
        });
    }

    Ok(resolved)
}

pub fn state_commit_stream_changes_from_planned_rows(
    rows: &[PlannedStateRow],
    operation: StateCommitStreamOperation,
    untracked: bool,
    runtime_metadata: StateCommitStreamRuntimeMetadata,
) -> Result<Vec<StateCommitStreamChange>, LixError> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let mut resolved = Vec::with_capacity(rows.len());
    for row in rows {
        let file_id = planned_row_optional_text(row, "file_id");
        let plugin_key = planned_row_optional_text(row, "plugin_key");
        let schema_version = planned_row_required_text(row, "schema_version")?;
        let snapshot_content = planned_row_snapshot_content(row)?;
        let version_id = row
            .version_id
            .clone()
            .or_else(|| planned_row_optional_text(row, "version_id"));

        resolved.push(StateCommitStreamChange {
            operation,
            entity_id: row.entity_id.clone(),
            schema_key: row.schema_key.clone(),
            schema_version,
            file_id,
            version_id: version_id.ok_or_else(|| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "planned row state commit stream requires version_id".to_string(),
                hint: None,
            })?,
            plugin_key,
            snapshot_content,
            untracked,
            origin_key: runtime_metadata.origin_key.clone(),
        });
    }

    Ok(resolved)
}

pub fn should_invalidate_deterministic_settings_cache(
    mutations: &[MutationRow],
    state_commit_stream_changes: &[StateCommitStreamChange],
) -> bool {
    mutations.iter().any(|row| {
        row.schema_key == DETERMINISTIC_SETTINGS_SCHEMA_KEY
            && row.entity_id == DETERMINISTIC_SETTINGS_ENTITY_ID
    }) || state_commit_stream_changes.iter().any(|change| {
        change.schema_key == DETERMINISTIC_SETTINGS_SCHEMA_KEY
            && change.entity_id == DETERMINISTIC_SETTINGS_ENTITY_ID
    })
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct StateCommitStreamBatch {
    pub sequence: u64,
    pub changes: Vec<StateCommitStreamChange>,
}

pub struct StateCommitStream {
    listener_id: u64,
    queue: Arc<ListenerQueue>,
    bus: Arc<StateCommitStreamBus>,
    closed: AtomicBool,
}

impl StateCommitStream {
    pub fn try_next(&self) -> Option<StateCommitStreamBatch> {
        self.queue.try_pop()
    }

    pub async fn next(&self) -> Option<StateCommitStreamBatch> {
        poll_fn(|cx| {
            if let Some(batch) = self.queue.try_pop() {
                return Poll::Ready(Some(batch));
            }
            if self.closed.load(Ordering::SeqCst) {
                return Poll::Ready(None);
            }
            self.queue.waker.register(cx.waker());
            if let Some(batch) = self.queue.try_pop() {
                return Poll::Ready(Some(batch));
            }
            if self.closed.load(Ordering::SeqCst) {
                return Poll::Ready(None);
            }
            Poll::Pending
        })
        .await
    }

    pub fn close(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        self.bus.unsubscribe(self.listener_id);
        self.queue.waker.wake();
    }
}

impl Drop for StateCommitStream {
    fn drop(&mut self) {
        self.close();
    }
}

#[derive(Default)]
pub(crate) struct StateCommitStreamBus {
    inner: Mutex<StateCommitStreamBusInner>,
}

impl StateCommitStreamBus {
    pub(crate) fn subscribe(
        self: &Arc<Self>,
        filter: StateCommitStreamFilter,
    ) -> StateCommitStream {
        let compiled_filter = CompiledStateCommitStreamFilter::new(filter);
        let queue = Arc::new(ListenerQueue::default());

        let mut inner = self.inner.lock().unwrap();
        let listener_id = inner.next_listener_id;
        inner.next_listener_id = inner.next_listener_id.saturating_add(1);

        let listener_entry = ListenerEntry {
            filter: compiled_filter.clone(),
            queue: Arc::clone(&queue),
        };
        inner.listeners.insert(listener_id, listener_entry);

        if compiled_filter.is_wildcard_listener() {
            inner.wildcard_listeners.insert(listener_id);
        }
        index_listener(
            &mut inner.by_schema_key,
            &compiled_filter.schema_keys,
            listener_id,
        );
        index_listener(
            &mut inner.by_entity_id,
            &compiled_filter.entity_ids,
            listener_id,
        );
        index_listener(
            &mut inner.by_file_id,
            &compiled_filter.file_ids,
            listener_id,
        );
        index_listener(
            &mut inner.by_version_id,
            &compiled_filter.version_ids,
            listener_id,
        );
        index_listener(
            &mut inner.by_origin_key,
            &compiled_filter.include_origin_keys,
            listener_id,
        );

        StateCommitStream {
            listener_id,
            queue,
            bus: Arc::clone(self),
            closed: AtomicBool::new(false),
        }
    }

    pub(crate) fn latest_sequence(&self) -> Option<u64> {
        let inner = self.inner.lock().unwrap();
        inner.next_sequence.checked_sub(1)
    }

    pub(crate) fn emit(&self, changes: Vec<StateCommitStreamChange>) -> Option<u64> {
        if changes.is_empty() {
            return None;
        }

        let (sequence, batch, candidate_listeners) = {
            let mut inner = self.inner.lock().unwrap();
            let touched = TouchedFields::from_changes(&changes);
            let sequence = inner.next_sequence;
            inner.next_sequence = inner.next_sequence.saturating_add(1);

            let mut candidate_ids: HashSet<u64> = HashSet::new();
            candidate_ids.extend(inner.wildcard_listeners.iter().copied());
            extend_candidates(
                &mut candidate_ids,
                &inner.by_schema_key,
                touched.schema_keys.iter(),
            );
            extend_candidates(
                &mut candidate_ids,
                &inner.by_entity_id,
                touched.entity_ids.iter(),
            );
            extend_candidates(
                &mut candidate_ids,
                &inner.by_file_id,
                touched.file_ids.iter(),
            );
            extend_candidates(
                &mut candidate_ids,
                &inner.by_version_id,
                touched.version_ids.iter(),
            );
            extend_candidates(
                &mut candidate_ids,
                &inner.by_origin_key,
                touched.origin_keys.iter(),
            );

            if candidate_ids.is_empty() {
                return Some(sequence);
            }

            let batch = StateCommitStreamBatch { sequence, changes };

            let listeners = candidate_ids
                .into_iter()
                .filter_map(|listener_id| inner.listeners.get(&listener_id).cloned())
                .collect::<Vec<_>>();

            (sequence, batch, listeners)
        };

        for listener in candidate_listeners {
            if !listener.filter.matches_batch(&batch) {
                continue;
            }
            enqueue_batch(&listener.queue, batch.clone());
        }

        Some(sequence)
    }

    fn unsubscribe(&self, listener_id: u64) {
        let mut inner = self.inner.lock().unwrap();
        let Some(listener) = inner.listeners.remove(&listener_id) else {
            return;
        };

        inner.wildcard_listeners.remove(&listener_id);
        unindex_listener(
            &mut inner.by_schema_key,
            &listener.filter.schema_keys,
            listener_id,
        );
        unindex_listener(
            &mut inner.by_entity_id,
            &listener.filter.entity_ids,
            listener_id,
        );
        unindex_listener(
            &mut inner.by_file_id,
            &listener.filter.file_ids,
            listener_id,
        );
        unindex_listener(
            &mut inner.by_version_id,
            &listener.filter.version_ids,
            listener_id,
        );
        unindex_listener(
            &mut inner.by_origin_key,
            &listener.filter.include_origin_keys,
            listener_id,
        );
    }
}

#[derive(Default)]
struct StateCommitStreamBusInner {
    next_listener_id: u64,
    next_sequence: u64,
    listeners: HashMap<u64, ListenerEntry>,
    wildcard_listeners: HashSet<u64>,
    by_schema_key: HashMap<String, HashSet<u64>>,
    by_entity_id: HashMap<String, HashSet<u64>>,
    by_file_id: HashMap<String, HashSet<u64>>,
    by_version_id: HashMap<String, HashSet<u64>>,
    by_origin_key: HashMap<String, HashSet<u64>>,
}

#[derive(Clone)]
struct ListenerEntry {
    filter: CompiledStateCommitStreamFilter,
    queue: Arc<ListenerQueue>,
}

#[derive(Default)]
struct ListenerQueue {
    queue: Mutex<VecDeque<StateCommitStreamBatch>>,
    waker: AtomicWaker,
}

impl ListenerQueue {
    fn try_pop(&self) -> Option<StateCommitStreamBatch> {
        let mut queue = self.queue.lock().unwrap();
        queue.pop_front()
    }
}

#[derive(Debug, Clone)]
struct CompiledStateCommitStreamFilter {
    schema_keys: HashSet<String>,
    entity_ids: HashSet<String>,
    file_ids: HashSet<String>,
    version_ids: HashSet<String>,
    include_origin_keys: HashSet<String>,
    exclude_origin_keys: HashSet<String>,
    include_untracked: bool,
}

impl CompiledStateCommitStreamFilter {
    fn new(filter: StateCommitStreamFilter) -> Self {
        Self {
            schema_keys: normalize_filter_values(filter.schema_keys),
            entity_ids: normalize_filter_values(filter.entity_ids),
            file_ids: normalize_filter_values(filter.file_ids),
            version_ids: normalize_filter_values(filter.version_ids),
            include_origin_keys: normalize_filter_values(filter.include_origin_keys),
            exclude_origin_keys: normalize_filter_values(filter.exclude_origin_keys),
            include_untracked: filter.include_untracked,
        }
    }

    fn is_wildcard_listener(&self) -> bool {
        self.schema_keys.is_empty()
            && self.entity_ids.is_empty()
            && self.file_ids.is_empty()
            && self.version_ids.is_empty()
            && self.include_origin_keys.is_empty()
    }

    fn matches_batch(&self, batch: &StateCommitStreamBatch) -> bool {
        batch
            .changes
            .iter()
            .any(|change| self.matches_change(change))
    }

    fn matches_change(&self, change: &StateCommitStreamChange) -> bool {
        if !self.include_untracked && change.untracked {
            return false;
        }
        if !self.schema_keys.is_empty() && !self.schema_keys.contains(&change.schema_key) {
            return false;
        }
        if !self.entity_ids.is_empty() && !self.entity_ids.contains(&change.entity_id) {
            return false;
        }
        if !self.file_ids.is_empty()
            && change
                .file_id
                .as_ref()
                .is_none_or(|file_id| !self.file_ids.contains(file_id))
        {
            return false;
        }
        if !self.version_ids.is_empty() && !self.version_ids.contains(&change.version_id) {
            return false;
        }
        if !self.include_origin_keys.is_empty() {
            let Some(origin_key) = change.origin_key.as_ref() else {
                return false;
            };
            if !self.include_origin_keys.contains(origin_key) {
                return false;
            }
        }
        if let Some(origin_key) = change.origin_key.as_ref() {
            if self.exclude_origin_keys.contains(origin_key) {
                return false;
            }
        }
        true
    }
}

#[derive(Default)]
struct TouchedFields {
    schema_keys: HashSet<String>,
    entity_ids: HashSet<String>,
    file_ids: HashSet<String>,
    version_ids: HashSet<String>,
    origin_keys: HashSet<String>,
}

impl TouchedFields {
    fn from_changes(changes: &[StateCommitStreamChange]) -> Self {
        let mut touched = Self::default();
        for change in changes {
            touched.schema_keys.insert(change.schema_key.clone());
            touched.entity_ids.insert(change.entity_id.clone());
            if let Some(file_id) = change.file_id.as_ref() {
                touched.file_ids.insert(file_id.clone());
            }
            touched.version_ids.insert(change.version_id.clone());
            if let Some(origin_key) = change.origin_key.as_ref() {
                touched.origin_keys.insert(origin_key.clone());
            }
        }
        touched
    }
}

fn state_commit_stream_origin_key(
    row_origin_key: Option<&str>,
    runtime_metadata: &StateCommitStreamRuntimeMetadata,
) -> Option<String> {
    row_origin_key
        .map(str::to_string)
        .or_else(|| runtime_metadata.origin_key.clone())
}

fn planned_row_required_text(row: &PlannedStateRow, key: &str) -> Result<String, LixError> {
    planned_row_optional_text(row, key).ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("planned row state commit stream requires '{key}'"),
        hint: None,
    })
}

fn planned_row_optional_text(row: &PlannedStateRow, key: &str) -> Option<String> {
    match row.values.get(key) {
        Some(Value::Text(text)) => Some(text.clone()),
        Some(Value::Integer(number)) => Some(number.to_string()),
        _ => None,
    }
}

fn planned_row_snapshot_content(row: &PlannedStateRow) -> Result<Option<JsonValue>, LixError> {
    match row.values.get("snapshot_content") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Json(value)) => Ok(Some(value.clone())),
        Some(Value::Text(text)) => {
            let parsed = serde_json::from_str(text).map_err(|error| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "planned row state commit stream expected JSON snapshot_content text: {error}"
                ),
                hint: None,
            })?;
            Ok(Some(parsed))
        }
        Some(other) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "planned row state commit stream expected null/text snapshot_content, got {other:?}"
            ),
            hint: None,
        }),
    }
}

async fn durable_state_commit_changes_exist_since(
    backend: &dyn LixBackend,
    after: Option<&DurableStateCommitCursor>,
    filter: &StateCommitStreamFilter,
) -> Result<bool, LixError> {
    let dialect = backend.dialect();
    let mut params = Vec::new();
    let mut next_placeholder = 1usize;
    let mut predicates = Vec::new();

    if let Some(after) = after {
        let created_after = dialect.placeholder(next_placeholder);
        params.push(Value::Text(after.created_at.clone()));
        next_placeholder += 1;

        let created_equal = dialect.placeholder(next_placeholder);
        params.push(Value::Text(after.created_at.clone()));
        next_placeholder += 1;

        let change_after = dialect.placeholder(next_placeholder);
        params.push(Value::Text(after.change_id.clone()));
        next_placeholder += 1;

        predicates.push(format!(
            "(c.created_at > {created_after} OR (c.created_at = {created_equal} AND c.id > {change_after}))"
        ));
    }

    if !filter.include_untracked {
        predicates.push(
            "NOT EXISTS ( \
                 SELECT 1 \
                 FROM lix_internal_untracked_change_visibility uv \
                 WHERE uv.change_id = c.id \
             )"
            .to_string(),
        );
    }

    append_text_in_predicate(
        &mut predicates,
        "c.schema_key",
        &filter.schema_keys,
        dialect,
        &mut next_placeholder,
        &mut params,
    );
    append_text_in_predicate(
        &mut predicates,
        "c.entity_id",
        &filter.entity_ids,
        dialect,
        &mut next_placeholder,
        &mut params,
    );
    append_text_in_predicate(
        &mut predicates,
        "c.file_id",
        &filter.file_ids,
        dialect,
        &mut next_placeholder,
        &mut params,
    );

    let where_sql = if predicates.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", predicates.join(" AND "))
    };
    let sql = format!(
        "SELECT 1 \
         FROM lix_internal_change c \
         {where_sql} \
         LIMIT 1"
    );
    let result = backend.execute(&sql, &params).await?;
    Ok(!result.rows.is_empty())
}

fn append_text_in_predicate(
    predicates: &mut Vec<String>,
    column: &str,
    values: &[String],
    dialect: crate::SqlDialect,
    next_placeholder: &mut usize,
    params: &mut Vec<Value>,
) {
    if values.is_empty() {
        return;
    }

    let mut placeholders = Vec::with_capacity(values.len());
    for value in values {
        placeholders.push(dialect.placeholder(*next_placeholder));
        params.push(Value::Text(value.clone()));
        *next_placeholder += 1;
    }
    predicates.push(format!("{column} IN ({})", placeholders.join(", ")));
}

fn map_mutation_operation(operation: &MutationOperation) -> StateCommitStreamOperation {
    match operation {
        MutationOperation::Insert => StateCommitStreamOperation::Insert,
    }
}

fn required_text_value(value: Option<&Value>, field: &str) -> Result<String, LixError> {
    match value {
        Some(Value::Text(text)) if !text.is_empty() => Ok(text.clone()),
        Some(Value::Integer(number)) => Ok(number.to_string()),
        Some(other) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("expected text-like value for {field}, got {other:?}"),
            hint: None,
        }),
        None => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{field} is missing"),
            hint: None,
        }),
    }
}

fn required_integer_value(value: Option<&Value>, field: &str) -> Result<i64, LixError> {
    match value {
        Some(Value::Integer(number)) => Ok(*number),
        Some(other) => Err(LixError::unknown(format!(
            "expected integer for {field}, got {other:?}"
        ))),
        None => Err(LixError::unknown(format!("missing column {field}"))),
    }
}

fn normalize_filter_values(values: Vec<String>) -> HashSet<String> {
    values
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect()
}

fn index_listener(
    index: &mut HashMap<String, HashSet<u64>>,
    keys: &HashSet<String>,
    listener_id: u64,
) {
    for key in keys {
        index
            .entry(key.clone())
            .or_insert_with(HashSet::new)
            .insert(listener_id);
    }
}

fn unindex_listener(
    index: &mut HashMap<String, HashSet<u64>>,
    keys: &HashSet<String>,
    listener_id: u64,
) {
    for key in keys {
        if let Some(ids) = index.get_mut(key) {
            ids.remove(&listener_id);
            if ids.is_empty() {
                index.remove(key);
            }
        }
    }
}

fn extend_candidates<'a>(
    candidates: &mut HashSet<u64>,
    index: &HashMap<String, HashSet<u64>>,
    keys: impl Iterator<Item = &'a String>,
) {
    for key in keys {
        if let Some(listener_ids) = index.get(key) {
            candidates.extend(listener_ids.iter().copied());
        }
    }
}

fn enqueue_batch(queue: &ListenerQueue, batch: StateCommitStreamBatch) {
    let mut queue_guard = queue.queue.lock().unwrap();
    if queue_guard.len() >= MAX_PENDING_BATCHES_PER_LISTENER {
        queue_guard.pop_front();
    }
    queue_guard.push_back(batch);
    drop(queue_guard);
    queue.waker.wake();
}

#[cfg(test)]
mod tests {
    use super::{
        load_durable_state_commit_consumer_cursors, load_durable_state_commit_low_watermark,
        state_commit_stream_changes_from_changes, state_commit_stream_changes_from_planned_rows,
        upsert_durable_state_commit_consumer_cursor_in_transaction,
        CompiledStateCommitStreamFilter, DurableStateCommitCursor, StateCommitStreamChange,
        StateCommitStreamFilter, StateCommitStreamOperation, StateCommitStreamRuntimeMetadata,
    };
    use crate::backend::LixBackend;
    use crate::session::version_ops::commit::StagedChange;
    use crate::sql::PlannedStateRow;
    use crate::test_support::{init_test_backend_core, TestSqliteBackend};
    use crate::TransactionBeginMode;
    use crate::Value;
    use std::collections::BTreeMap;

    fn sample_change(origin_key: Option<&str>) -> StateCommitStreamChange {
        StateCommitStreamChange {
            operation: StateCommitStreamOperation::Insert,
            entity_id: "entity-1".to_string(),
            schema_key: "lix_key_value".to_string(),
            schema_version: "1".to_string(),
            file_id: None,
            version_id: "version-a".to_string(),
            plugin_key: None,
            snapshot_content: None,
            untracked: false,
            origin_key: origin_key.map(str::to_string),
        }
    }

    #[test]
    fn changes_map_to_update_changes() {
        let changes = state_commit_stream_changes_from_changes(
            &[StagedChange {
                id: None,
                entity_id: "entity-1".try_into().unwrap(),
                schema_key: "lix_key_value".try_into().unwrap(),
                schema_version: Some("1".try_into().unwrap()),
                file_id: None,
                plugin_key: None,
                snapshot_content: Some("{\"value\":\"after\"}".to_string()),
                metadata: None,
                version_id: "version-a".try_into().unwrap(),
                origin_key: Some("origin-a".to_string()),
                created_at: None,
            }],
            StateCommitStreamOperation::Update,
            StateCommitStreamRuntimeMetadata::default(),
        )
        .expect("changes should map");

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].operation, StateCommitStreamOperation::Update);
        assert_eq!(changes[0].entity_id, "entity-1");
        assert_eq!(changes[0].schema_key, "lix_key_value");
        assert_eq!(changes[0].origin_key.as_deref(), Some("origin-a"));
    }

    #[test]
    fn state_commit_stream_uses_runtime_origin_metadata_when_change_omits_it() {
        let changes = state_commit_stream_changes_from_changes(
            &[StagedChange {
                id: None,
                entity_id: "entity-1".try_into().unwrap(),
                schema_key: "lix_key_value".try_into().unwrap(),
                schema_version: Some("1".try_into().unwrap()),
                file_id: None,
                plugin_key: None,
                snapshot_content: Some("{\"value\":\"after\"}".to_string()),
                metadata: None,
                version_id: "version-a".try_into().unwrap(),
                origin_key: None,
                created_at: None,
            }],
            StateCommitStreamOperation::Update,
            StateCommitStreamRuntimeMetadata::from_runtime_origin_key(Some("origin-runtime")),
        )
        .expect("changes should map");

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].origin_key.as_deref(), Some("origin-runtime"));
    }

    #[test]
    fn state_commit_stream_prefers_change_origin_key_over_runtime_metadata() {
        let changes = state_commit_stream_changes_from_changes(
            &[StagedChange {
                id: None,
                entity_id: "entity-1".try_into().unwrap(),
                schema_key: "lix_key_value".try_into().unwrap(),
                schema_version: Some("1".try_into().unwrap()),
                file_id: None,
                plugin_key: None,
                snapshot_content: Some("{\"value\":\"after\"}".to_string()),
                metadata: None,
                version_id: "version-a".try_into().unwrap(),
                origin_key: Some("origin-change".to_string()),
                created_at: None,
            }],
            StateCommitStreamOperation::Update,
            StateCommitStreamRuntimeMetadata::from_runtime_origin_key(Some("origin-runtime")),
        )
        .expect("changes should map");

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].origin_key.as_deref(), Some("origin-change"));
    }

    #[test]
    fn state_commit_stream_filter_suppresses_matching_origin() {
        let filter = CompiledStateCommitStreamFilter::new(StateCommitStreamFilter {
            exclude_origin_keys: vec!["origin-a".to_string()],
            ..StateCommitStreamFilter::default()
        });

        assert!(
            !filter.matches_change(&sample_change(Some("origin-a"))),
            "matching origin metadata should be suppressed"
        );
    }

    #[test]
    fn state_commit_stream_filter_keeps_different_origin() {
        let filter = CompiledStateCommitStreamFilter::new(StateCommitStreamFilter {
            exclude_origin_keys: vec!["origin-a".to_string()],
            ..StateCommitStreamFilter::default()
        });

        assert!(
            filter.matches_change(&sample_change(Some("origin-b"))),
            "different origin metadata should still be delivered"
        );
    }

    #[test]
    fn state_commit_stream_filter_broadcasts_null_origin_without_include_filter() {
        let filter = CompiledStateCommitStreamFilter::new(StateCommitStreamFilter {
            exclude_origin_keys: vec!["origin-a".to_string()],
            ..StateCommitStreamFilter::default()
        });

        assert!(
            filter.matches_change(&sample_change(None)),
            "null origin should broadcast unless the subscription explicitly requires an origin"
        );
    }

    #[test]
    fn planned_rows_accept_structured_json_snapshot_content() {
        let mut values = BTreeMap::new();
        values.insert("file_id".to_string(), Value::Null);
        values.insert("plugin_key".to_string(), Value::Null);
        values.insert("schema_version".to_string(), Value::Text("1".to_string()));
        values.insert(
            "snapshot_content".to_string(),
            Value::Json(serde_json::json!({
                "key": "observe-untracked-external",
                "value": "u1"
            })),
        );

        let changes = state_commit_stream_changes_from_planned_rows(
            &[PlannedStateRow {
                entity_id: "observe-untracked-external".to_string(),
                schema_key: "lix_key_value".to_string(),
                version_id: Some("global".to_string()),
                values,
                origin_key: None,
                tombstone: false,
            }],
            StateCommitStreamOperation::Insert,
            true,
            StateCommitStreamRuntimeMetadata::default(),
        )
        .expect("planned rows should accept structured JSON snapshot_content");

        assert_eq!(changes.len(), 1);
        assert_eq!(
            changes[0].snapshot_content,
            Some(serde_json::json!({
                "key": "observe-untracked-external",
                "value": "u1"
            }))
        );
        assert!(changes[0].untracked);
    }

    #[tokio::test]
    async fn durable_state_commit_low_watermark_returns_oldest_registered_consumer_cursor() {
        let backend = TestSqliteBackend::new();
        init_test_backend_core(&backend)
            .await
            .expect("test backend init should succeed");
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should begin");

        upsert_durable_state_commit_consumer_cursor_in_transaction(
            transaction.as_mut(),
            "consumer-b",
            &DurableStateCommitCursor {
                change_id: "change-002".to_string(),
                created_at: "2026-04-15T00:00:02Z".to_string(),
                visibility_append_seq: 2,
            },
        )
        .await
        .expect("consumer-b cursor should persist");
        upsert_durable_state_commit_consumer_cursor_in_transaction(
            transaction.as_mut(),
            "consumer-a",
            &DurableStateCommitCursor {
                change_id: "change-001".to_string(),
                created_at: "2026-04-15T00:00:01Z".to_string(),
                visibility_append_seq: 1,
            },
        )
        .await
        .expect("consumer-a cursor should persist");
        transaction
            .commit()
            .await
            .expect("transaction commit should succeed");

        let cursors = load_durable_state_commit_consumer_cursors(&backend)
            .await
            .expect("consumer cursors should load");
        assert_eq!(cursors.len(), 2);
        assert_eq!(cursors[0].consumer_key, "consumer-a");
        assert_eq!(cursors[1].consumer_key, "consumer-b");

        let low_watermark = load_durable_state_commit_low_watermark(&backend)
            .await
            .expect("low watermark should load")
            .expect("low watermark should exist");
        assert_eq!(
            low_watermark,
            DurableStateCommitCursor {
                change_id: "change-001".to_string(),
                created_at: "2026-04-15T00:00:01Z".to_string(),
                visibility_append_seq: 1,
            }
        );
    }
}
