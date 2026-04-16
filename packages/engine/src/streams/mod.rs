mod state_change_record;

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
            })?;
            Ok(Some(parsed))
        }
        Some(other) => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "planned row state commit stream expected null/text snapshot_content, got {other:?}"
            ),
        }),
    }
}

fn map_mutation_operation(operation: &MutationOperation) -> StateCommitStreamOperation {
    match operation {
        MutationOperation::Insert => StateCommitStreamOperation::Insert,
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
        state_commit_stream_changes_from_changes, state_commit_stream_changes_from_planned_rows,
        CompiledStateCommitStreamFilter, StateCommitStreamChange, StateCommitStreamFilter,
        StateCommitStreamOperation, StateCommitStreamRuntimeMetadata,
    };
    use crate::session::version_ops::commit::StagedChange;
    use crate::sql::PlannedStateRow;
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
}
