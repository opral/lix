use crate::engine::sql::contracts::planned_statement::{MutationOperation, MutationRow};
use crate::{LixError, Value};
use futures_util::future::poll_fn;
use futures_util::task::AtomicWaker;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;

const MAX_PENDING_BATCHES_PER_LISTENER: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateCommitStreamFilter {
    // Matching semantics:
    // - OR within each field list (e.g. schema_keys = ["a", "b"] matches a OR b)
    // - AND across non-empty fields (e.g. schema_keys + entity_ids must both match)
    // - Empty field means "no constraint" for that dimension
    pub schema_keys: Vec<String>,
    pub entity_ids: Vec<String>,
    pub file_ids: Vec<String>,
    pub version_ids: Vec<String>,
    pub writer_keys: Vec<String>,
    pub exclude_writer_keys: Vec<String>,
    pub include_untracked: bool,
}

impl Default for StateCommitStreamFilter {
    fn default() -> Self {
        Self {
            schema_keys: Vec::new(),
            entity_ids: Vec::new(),
            file_ids: Vec::new(),
            version_ids: Vec::new(),
            writer_keys: Vec::new(),
            exclude_writer_keys: Vec::new(),
            include_untracked: true,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum StateCommitStreamOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StateCommitStreamChange {
    pub operation: StateCommitStreamOperation,
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub version_id: String,
    pub plugin_key: String,
    pub snapshot_content: Option<JsonValue>,
    pub untracked: bool,
    pub writer_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
            &mut inner.by_writer_key,
            &compiled_filter.writer_keys,
            listener_id,
        );

        StateCommitStream {
            listener_id,
            queue,
            bus: Arc::clone(self),
            closed: AtomicBool::new(false),
        }
    }

    pub(crate) fn emit(&self, changes: Vec<StateCommitStreamChange>) {
        if changes.is_empty() {
            return;
        }

        let (batch, candidate_listeners) = {
            let mut inner = self.inner.lock().unwrap();
            let touched = TouchedFields::from_changes(&changes);

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
                &inner.by_writer_key,
                touched.writer_keys.iter(),
            );

            if candidate_ids.is_empty() {
                return;
            }

            let sequence = inner.next_sequence;
            inner.next_sequence = inner.next_sequence.saturating_add(1);
            let batch = StateCommitStreamBatch { sequence, changes };

            let listeners = candidate_ids
                .into_iter()
                .filter_map(|listener_id| inner.listeners.get(&listener_id).cloned())
                .collect::<Vec<_>>();

            (batch, listeners)
        };

        for listener in candidate_listeners {
            if !listener.filter.matches_batch(&batch) {
                continue;
            }
            enqueue_batch(&listener.queue, batch.clone());
        }
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
            &mut inner.by_writer_key,
            &listener.filter.writer_keys,
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
    by_writer_key: HashMap<String, HashSet<u64>>,
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
    writer_keys: HashSet<String>,
    exclude_writer_keys: HashSet<String>,
    include_untracked: bool,
}

impl CompiledStateCommitStreamFilter {
    fn new(filter: StateCommitStreamFilter) -> Self {
        Self {
            schema_keys: normalize_filter_values(filter.schema_keys),
            entity_ids: normalize_filter_values(filter.entity_ids),
            file_ids: normalize_filter_values(filter.file_ids),
            version_ids: normalize_filter_values(filter.version_ids),
            writer_keys: normalize_filter_values(filter.writer_keys),
            exclude_writer_keys: normalize_filter_values(filter.exclude_writer_keys),
            include_untracked: filter.include_untracked,
        }
    }

    fn is_wildcard_listener(&self) -> bool {
        self.schema_keys.is_empty()
            && self.entity_ids.is_empty()
            && self.file_ids.is_empty()
            && self.version_ids.is_empty()
            && self.writer_keys.is_empty()
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
        if !self.file_ids.is_empty() && !self.file_ids.contains(&change.file_id) {
            return false;
        }
        if !self.version_ids.is_empty() && !self.version_ids.contains(&change.version_id) {
            return false;
        }
        if !self.writer_keys.is_empty() {
            let Some(writer_key) = change.writer_key.as_ref() else {
                return false;
            };
            if !self.writer_keys.contains(writer_key) {
                return false;
            }
        }
        if let Some(writer_key) = change.writer_key.as_ref() {
            if self.exclude_writer_keys.contains(writer_key) {
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
    writer_keys: HashSet<String>,
}

impl TouchedFields {
    fn from_changes(changes: &[StateCommitStreamChange]) -> Self {
        let mut touched = Self::default();
        for change in changes {
            touched.schema_keys.insert(change.schema_key.clone());
            touched.entity_ids.insert(change.entity_id.clone());
            touched.file_ids.insert(change.file_id.clone());
            touched.version_ids.insert(change.version_id.clone());
            if let Some(writer_key) = change.writer_key.as_ref() {
                touched.writer_keys.insert(writer_key.clone());
            }
        }
        touched
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

pub(crate) fn state_commit_stream_changes_from_mutations(
    mutations: &[MutationRow],
    writer_key: Option<&str>,
) -> Vec<StateCommitStreamChange> {
    if mutations.is_empty() {
        return Vec::new();
    }

    let writer_key = writer_key.map(str::to_string);

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
            writer_key: writer_key.clone(),
        })
        .collect()
}

pub(crate) fn state_commit_stream_changes_from_postprocess_rows(
    rows: &[Vec<Value>],
    schema_key: &str,
    operation: StateCommitStreamOperation,
    writer_key: Option<&str>,
) -> Result<Vec<StateCommitStreamChange>, LixError> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let writer_key = writer_key.map(str::to_string);
    let mut changes = Vec::with_capacity(rows.len());

    for row in rows {
        let entity_id = row_text(row, 0, "entity_id")?;
        let file_id = row_text(row, 1, "file_id")?;
        let version_id = row_text(row, 2, "version_id")?;
        let plugin_key = row_text(row, 3, "plugin_key")?;
        let schema_version = row_text(row, 4, "schema_version")?;
        let snapshot_content = row_snapshot_content(row, 5)?;

        changes.push(StateCommitStreamChange {
            operation,
            entity_id,
            schema_key: schema_key.to_string(),
            schema_version,
            file_id,
            version_id,
            plugin_key,
            snapshot_content,
            untracked: false,
            writer_key: writer_key.clone(),
        });
    }

    Ok(changes)
}

fn row_text(row: &[Value], index: usize, column: &str) -> Result<String, LixError> {
    let value = row.get(index).ok_or_else(|| LixError {
        message: format!(
            "postprocess state commit stream rows expected {column} column at index {index}"
        ),
    })?;
    match value {
        Value::Text(text) => Ok(text.clone()),
        other => Err(LixError {
            message: format!(
                "postprocess state commit stream expected text {column}, got {other:?}"
            ),
        }),
    }
}

fn row_snapshot_content(row: &[Value], index: usize) -> Result<Option<JsonValue>, LixError> {
    let value = row.get(index).ok_or_else(|| LixError {
        message: format!(
            "postprocess state commit stream rows expected snapshot_content column at index {index}"
        ),
    })?;
    match value {
        Value::Null => Ok(None),
        Value::Text(text) => {
            let parsed = serde_json::from_str(text).map_err(|error| LixError {
                message: format!(
                    "postprocess state commit stream expected JSON snapshot_content text: {error}"
                ),
            })?;
            Ok(Some(parsed))
        }
        other => Err(LixError {
            message: format!(
                "postprocess state commit stream expected null/text snapshot_content, got {other:?}"
            ),
        }),
    }
}

fn map_mutation_operation(operation: &MutationOperation) -> StateCommitStreamOperation {
    match operation {
        MutationOperation::Insert => StateCommitStreamOperation::Insert,
    }
}

#[cfg(test)]
mod tests {
    use super::{state_commit_stream_changes_from_postprocess_rows, StateCommitStreamOperation};
    use crate::Value;

    #[test]
    fn postprocess_rows_map_to_update_changes() {
        let rows = vec![vec![
            Value::Text("entity-1".to_string()),
            Value::Text("file-1".to_string()),
            Value::Text("version-1".to_string()),
            Value::Text("plugin-1".to_string()),
            Value::Text("1".to_string()),
            Value::Text("{\"name\":\"Ada\"}".to_string()),
            Value::Text("{}".to_string()),
            Value::Null,
            Value::Text("2026-02-24T00:00:00Z".to_string()),
        ]];

        let changes = state_commit_stream_changes_from_postprocess_rows(
            &rows,
            "lix_entity",
            StateCommitStreamOperation::Update,
            Some("writer-a"),
        )
        .expect("postprocess rows should map");

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].operation, StateCommitStreamOperation::Update);
        assert_eq!(changes[0].entity_id, "entity-1");
        assert_eq!(changes[0].schema_key, "lix_entity");
        assert_eq!(changes[0].writer_key.as_deref(), Some("writer-a"));
    }

    #[test]
    fn postprocess_rows_map_null_snapshot_for_delete_changes() {
        let rows = vec![vec![
            Value::Text("entity-2".to_string()),
            Value::Text("file-2".to_string()),
            Value::Text("version-2".to_string()),
            Value::Text("plugin-2".to_string()),
            Value::Text("1".to_string()),
            Value::Null,
            Value::Text("{}".to_string()),
            Value::Null,
            Value::Text("2026-02-24T00:00:00Z".to_string()),
        ]];

        let changes = state_commit_stream_changes_from_postprocess_rows(
            &rows,
            "lix_entity",
            StateCommitStreamOperation::Delete,
            None,
        )
        .expect("postprocess rows should map");

        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].operation, StateCommitStreamOperation::Delete);
        assert_eq!(changes[0].entity_id, "entity-2");
        assert_eq!(changes[0].snapshot_content, None);
        assert_eq!(changes[0].writer_key, None);
    }
}
