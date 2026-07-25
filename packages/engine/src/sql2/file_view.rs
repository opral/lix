use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::plugin::PluginObservation;

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
pub(crate) enum SessionFileViewMutation {
    Set {
        key: SessionFileViewKey,
        view: SessionPluginFileView,
    },
    Remove {
        key: SessionFileViewKey,
    },
}

impl SessionFileViews {
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
    /// `lix_file.data` result and therefore delivered through this session.
    pub(crate) fn remember_plugin_file_view(
        &self,
        key: SessionFileViewKey,
        view: SessionPluginFileView,
    ) {
        self.lock().plugin_files.insert(key, view);
    }

    pub(crate) fn apply_mutations(
        &self,
        mutations: impl IntoIterator<Item = SessionFileViewMutation>,
    ) {
        let mut state = self.lock();
        for mutation in mutations {
            match mutation {
                SessionFileViewMutation::Set { key, view } => {
                    state.plugin_files.insert(key, view);
                }
                SessionFileViewMutation::Remove { key } => {
                    state.plugin_files.remove(&key);
                }
            }
        }
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
