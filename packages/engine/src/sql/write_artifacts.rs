use std::collections::{BTreeMap, BTreeSet};

use crate::common::Value;
use crate::streams::StateCommitStreamChange;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SessionStateDelta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_active_version_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_active_account_ids: Option<Vec<String>>,
    #[serde(default)]
    pub persist_workspace: bool,
}

impl SessionStateDelta {
    pub fn is_empty(&self) -> bool {
        self.next_active_version_id.is_none()
            && self.next_active_account_ids.is_none()
            && !self.persist_workspace
    }

    pub fn merge(&mut self, other: SessionStateDelta) {
        if other.next_active_version_id.is_some() {
            self.next_active_version_id = other.next_active_version_id;
        }
        if other.next_active_account_ids.is_some() {
            self.next_active_account_ids = other.next_active_account_ids;
        }
        self.persist_workspace |= other.persist_workspace;
    }

    #[allow(dead_code)]
    pub fn dependencies(&self) -> BTreeSet<crate::sql::QueryDependency> {
        let mut dependencies = BTreeSet::new();
        if self.next_active_version_id.is_some() {
            dependencies.insert(crate::sql::QueryDependency::ActiveVersion);
        }
        if self.next_active_account_ids.is_some() {
            dependencies.insert(crate::sql::QueryDependency::ActiveAccounts);
        }
        dependencies
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PlanEffects {
    pub state_commit_stream_changes: Vec<StateCommitStreamChange>,
    pub session_delta: SessionStateDelta,
    pub file_cache_refresh_targets: BTreeSet<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    Tracked,
    Untracked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteLane {
    ActiveVersion,
    SingleVersion(String),
    GlobalAdmin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpectedHead {
    CurrentHead,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdempotencyKey(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitPreconditions {
    pub write_lane: WriteLane,
    pub expected_head: ExpectedHead,
    pub idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlannedStateRow {
    pub entity_id: String,
    pub schema_key: String,
    pub version_id: Option<String>,
    pub values: BTreeMap<String, Value>,
    pub writer_key: Option<String>,
    pub tombstone: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedFilesystemDescriptor {
    pub directory_id: String,
    pub name: String,
    pub extension: Option<String>,
    pub metadata: Option<String>,
    pub hidden: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedFilesystemFile {
    pub file_id: String,
    pub version_id: String,
    pub untracked: bool,
    pub descriptor: Option<PlannedFilesystemDescriptor>,
    pub metadata_patch: OptionalTextPatch,
    pub data: Option<Vec<u8>>,
    pub deleted: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlannedFilesystemState {
    pub files: BTreeMap<(String, String), PlannedFilesystemFile>,
}

impl PlannedFilesystemState {
    pub fn merge_from(&mut self, next: &Self) {
        self.files.extend(next.files.clone());
    }

    pub fn has_binary_payloads(&self) -> bool {
        self.files.values().any(|file| file.data.is_some())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PlannedRowIdentity {
    pub schema_key: String,
    pub version_id: String,
    pub entity_id: String,
    pub file_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicChange {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: Option<String>,
    pub file_id: Option<String>,
    pub plugin_key: Option<String>,
    pub snapshot_content: Option<String>,
    pub metadata: Option<String>,
    pub version_id: String,
    pub writer_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticEffect {
    pub effect_key: String,
    pub target: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeBatch {
    pub changes: Vec<PublicChange>,
    pub write_lane: WriteLane,
    pub writer_key: Option<String>,
    pub semantic_effects: Vec<SemanticEffect>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptionalTextPatch {
    Unchanged,
}

impl OptionalTextPatch {
    pub fn apply(&self, current: Option<String>) -> Option<String> {
        current
    }
}
