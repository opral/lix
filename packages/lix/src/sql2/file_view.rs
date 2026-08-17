use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::plugin::runtime::PluginObservation;

/// One session's private view of plugin-owned files.
///
/// The shared branch contains the merged state. This cache instead contains
/// the state a client is known to have observed or submitted, which is the
/// base required to distinguish a stale omission from an intentional delete.
#[derive(Clone, Default)]
pub(crate) struct SessionFileViews {
    inner: Arc<Mutex<SessionFileViewsState>>,
}

#[derive(Default)]
struct SessionFileViewsState {
    plugin_files: BTreeMap<SessionFileViewKey, SessionPluginFileView>,
    /// Bytes received as a lazy sync file projection. These bytes are a local
    /// read overlay: canonical descriptor/blob rows remain authoritative and
    /// the overlay is replaced by the next canonical file projection.
    /// Read collectors share this immutable map by `Arc`; a write clones it
    /// copy-on-write. This keeps ordinary row-only reads O(1) in the number of
    /// cached files instead of cloning every durable byte projection.
    sync_files: Arc<BTreeMap<SessionFileViewKey, SessionSyncFileView>>,
    /// Read collectors are isolated from the live session. They may use
    /// copy-on-write for temporary plugin acknowledgements; the canonical
    /// session must publish cache mutations by replacing the shared pointer
    /// even while a collector still holds an older map.
    read_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SessionFileViewKey {
    pub(crate) branch_id: String,
    pub(crate) file_id: String,
}

impl SessionFileViewKey {
    pub(crate) fn new(branch_id: impl Into<String>, file_id: impl Into<String>) -> Self {
        Self {
            branch_id: branch_id.into(),
            file_id: file_id.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SessionPluginFileView {
    pub(crate) path: String,
    pub(crate) plugin_key: String,
    pub(crate) plugin_generation: String,
    /// The durable owner row is the file incarnation boundary. Its change ID
    /// prevents an old view from becoming valid again after plugin -> raw ->
    /// the same plugin.
    pub(crate) owner_change_id: String,
    /// Exact v2 authority: an O(1) actor/document observation rather than a
    /// materialized semantic-state snapshot.
    pub(crate) observation: Option<PluginObservation>,
}

#[derive(Debug, Clone)]
pub(crate) struct SessionSyncFileView {
    pub(crate) path: String,
    pub(crate) content: Arc<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub(crate) enum SessionFileViewMutation {
    Set {
        key: SessionFileViewKey,
        view: SessionPluginFileView,
    },
    Remove {
        key: SessionFileViewKey,
    },
    SetSyncFile {
        key: SessionFileViewKey,
        view: SessionSyncFileView,
    },
    RemoveSyncFile {
        key: SessionFileViewKey,
    },
}

impl SessionFileViews {
    /// Creates an isolated read collector carrying only durable raw-file
    /// projections. Plugin acknowledgements are intentionally collected in a
    /// fresh map and applied to the session only after the read succeeds;
    /// sharing the whole cache would make a failed read publish derived state.
    pub(crate) fn fork_for_read(&self) -> Self {
        let state = self.lock();
        Self {
            inner: Arc::new(Mutex::new(SessionFileViewsState {
                plugin_files: BTreeMap::new(),
                sync_files: Arc::clone(&state.sync_files),
                read_only: true,
            })),
        }
    }

    pub(crate) fn has_plugin_file_at_path(&self, branch_id: &str, path: &str) -> bool {
        self.lock()
            .plugin_files
            .iter()
            .any(|(key, view)| key.branch_id == branch_id && view.path == path)
    }

    pub(crate) fn plugin_file_view(
        &self,
        key: &SessionFileViewKey,
        plugin_key: &str,
        plugin_generation: &str,
        owner_change_id: &str,
    ) -> Option<SessionPluginFileView> {
        self.lock()
            .plugin_files
            .get(key)
            .filter(|view| {
                view.plugin_key == plugin_key
                    && view.plugin_generation == plugin_generation
                    && view.owner_change_id == owner_change_id
            })
            .cloned()
    }

    /// Records a plugin state that was materialized for a read-only
    /// `lix_file.content` result and therefore delivered through this session.
    pub(crate) fn remember_plugin_file_view(
        &self,
        key: SessionFileViewKey,
        view: SessionPluginFileView,
    ) {
        let mut state = self.lock();
        Arc::make_mut(&mut state.sync_files).remove(&key);
        state.plugin_files.insert(key, view);
    }

    pub(crate) fn sync_file_content(&self, key: &SessionFileViewKey) -> Option<(String, Vec<u8>)> {
        let state = self.lock();
        state
            .sync_files
            .get(key)
            .map(|view| (view.path.clone(), view.content.as_ref().clone()))
    }

    /// Restores a durable raw-file projection after a process/session restart.
    /// Canonical plugin views still win when a later semantic read materializes
    /// the same file, exactly as they do for an in-process projection.
    pub(crate) fn remember_sync_file_view(
        &self,
        key: SessionFileViewKey,
        path: String,
        content: Vec<u8>,
    ) {
        let mut state = self.lock();
        state.plugin_files.remove(&key);
        let mut sync_files = (*state.sync_files).clone();
        sync_files.insert(
            key,
            SessionSyncFileView {
                path,
                content: Arc::new(content),
            },
        );
        state.sync_files = Arc::new(sync_files);
    }

    pub(crate) fn apply_mutations(
        &self,
        mutations: impl IntoIterator<Item = SessionFileViewMutation>,
    ) {
        let mut state = self.lock();
        // Read collectors hold an Arc to the current sync map. A normal
        // copy-on-write update would then publish the projection only to the
        // transaction/collector that happened to receive it, losing it from
        // the live session. Hydration is a durable cache publication, so
        // merge all sync mutations into a fresh map and replace the shared
        // pointer once per batch. Read forks keep their old immutable map;
        // subsequent reads see the newly published one.
        let mut next_sync_files: Option<BTreeMap<SessionFileViewKey, SessionSyncFileView>> = None;
        for mutation in mutations {
            match mutation {
                SessionFileViewMutation::Set { key, view } => {
                    if let Some(sync_files) = next_sync_files.as_mut() {
                        sync_files.remove(&key);
                    } else if state.read_only {
                        Arc::make_mut(&mut state.sync_files).remove(&key);
                    } else {
                        let mut sync_files = (*state.sync_files).clone();
                        sync_files.remove(&key);
                        next_sync_files = Some(sync_files);
                    }
                    state.plugin_files.insert(key, view);
                }
                SessionFileViewMutation::Remove { key } => {
                    if let Some(sync_files) = next_sync_files.as_mut() {
                        sync_files.remove(&key);
                    } else if state.read_only {
                        Arc::make_mut(&mut state.sync_files).remove(&key);
                    } else {
                        let mut sync_files = (*state.sync_files).clone();
                        sync_files.remove(&key);
                        next_sync_files = Some(sync_files);
                    }
                    state.plugin_files.remove(&key);
                }
                SessionFileViewMutation::SetSyncFile { key, view } => {
                    state.plugin_files.remove(&key);
                    let sync_files = next_sync_files.get_or_insert_with(|| {
                        (*state.sync_files).clone()
                    });
                    sync_files.insert(key, view);
                }
                SessionFileViewMutation::RemoveSyncFile { key } => {
                    if state.read_only {
                        Arc::make_mut(&mut state.sync_files).remove(&key);
                    } else {
                        let sync_files = next_sync_files.get_or_insert_with(|| {
                            (*state.sync_files).clone()
                        });
                        sync_files.remove(&key);
                    }
                }
            }
        }
        if let Some(sync_files) = next_sync_files {
            state.sync_files = Arc::new(sync_files);
        }
    }

    /// Discards every private acknowledgement after a commit outcome was
    /// recovered from durable storage. The transaction may have published
    /// before its post-commit actor publications and view updates ran; a cold
    /// open on the next exact read is safer than retaining stale state.
    pub(crate) fn clear(&self) {
        let mut state = self.lock();
        state.plugin_files.clear();
        state.sync_files = Arc::new(BTreeMap::new());
    }

    pub(crate) fn plugin_file_mutations(&self) -> Vec<SessionFileViewMutation> {
        self.lock()
            .plugin_files
            .iter()
            .map(|(key, view)| SessionFileViewMutation::Set {
                key: key.clone(),
                view: view.clone(),
            })
            .collect()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SessionFileViewsState> {
        self.inner
            .lock()
            .expect("session file view mutex should not poison")
    }
}
