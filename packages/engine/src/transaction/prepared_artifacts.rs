use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as JsonValue;

use crate::catalog::ResolvedRelation;
use crate::common::Value;
use crate::session::SessionStateDelta;
use crate::sql::{
    MutationRow, PreparedBatch, PreparedInsertOnConflictAction, PreparedPublicRead,
    PreparedWriteOperationKind, PreparedWriteStatementKind, ResultContract,
    SchemaLiveTableRequirement, WriteDiagnosticContext,
};
use crate::streams::StateCommitStreamChange;

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedPublicSurfaceRegistryMutation {
    UpsertRegisteredSchemaSnapshot { snapshot: JsonValue },
    RemoveDynamicSchema { schema_key: String },
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedPublicSurfaceRegistryEffect {
    None,
    ApplyMutations(Vec<PreparedPublicSurfaceRegistryMutation>),
    ReloadFromStorage,
}

#[allow(dead_code)]
impl PreparedPublicSurfaceRegistryEffect {
    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
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

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedResolvedWritePartition {
    pub execution_mode: WriteMode,
    pub authoritative_pre_state_rows: Vec<PlannedStateRow>,
    pub intended_post_state: Vec<PlannedStateRow>,
    pub writer_key_updates: BTreeMap<PlannedRowIdentity, Option<String>>,
    pub filesystem_state: PlannedFilesystemState,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedResolvedWritePlan {
    pub partitions: Vec<PreparedResolvedWritePartition>,
}

#[allow(dead_code)]
impl PreparedResolvedWritePlan {
    pub fn authoritative_pre_state_rows(&self) -> impl Iterator<Item = &PlannedStateRow> {
        self.partitions
            .iter()
            .flat_map(|partition| partition.authoritative_pre_state_rows.iter())
    }

    pub fn intended_post_state(&self) -> impl Iterator<Item = &PlannedStateRow> {
        self.partitions
            .iter()
            .flat_map(|partition| partition.intended_post_state.iter())
    }

    pub fn filesystem_state(&self) -> PlannedFilesystemState {
        let mut merged = PlannedFilesystemState::default();
        for partition in &self.partitions {
            merged.merge_from(&partition.filesystem_state);
        }
        merged
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedPublicWriteContract {
    pub operation_kind: PreparedWriteOperationKind,
    pub target: ResolvedRelation,
    pub on_conflict_action: Option<PreparedInsertOnConflictAction>,
    pub requested_version_id: Option<String>,
    pub active_account_ids: Vec<String>,
    pub writer_key: Option<String>,
    pub resolved_write_plan: Option<PreparedResolvedWritePlan>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedTrackedWriteExecution {
    pub schema_live_table_requirements: Vec<SchemaLiveTableRequirement>,
    pub change_batch: Option<ChangeBatch>,
    pub create_preconditions: CommitPreconditions,
    pub semantic_effects: PlanEffects,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedUntrackedWriteExecution {
    pub intended_post_state: Vec<PlannedStateRow>,
    pub semantic_effects: PlanEffects,
    pub persist_filesystem_payloads_before_write: bool,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedPublicWriteExecutionPartition {
    Tracked(PreparedTrackedWriteExecution),
    Untracked(PreparedUntrackedWriteExecution),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedPublicWriteMaterialization {
    pub partitions: Vec<PreparedPublicWriteExecutionPartition>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedPublicWritePlanArtifact {
    Noop,
    Materialize(PreparedPublicWriteMaterialization),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedPublicWrite {
    pub contract: PreparedPublicWriteContract,
    pub execution: PreparedPublicWritePlanArtifact,
}

#[allow(dead_code)]
impl PreparedPublicWrite {
    pub fn materialization(&self) -> Option<&PreparedPublicWriteMaterialization> {
        match &self.execution {
            PreparedPublicWritePlanArtifact::Noop => None,
            PreparedPublicWritePlanArtifact::Materialize(materialization) => Some(materialization),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedDirectWriteArtifact {
    pub prepared_batch: PreparedBatch,
    pub live_table_requirements: Vec<SchemaLiveTableRequirement>,
    pub mutations: Vec<MutationRow>,
    pub has_update_validations: bool,
    pub should_refresh_file_cache: bool,
    pub read_only_query: bool,
    pub filesystem_state: PlannedFilesystemState,
    pub effects: PlanEffects,
    pub writer_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum PreparedWriteArtifact {
    PublicRead(PreparedPublicRead),
    PublicWrite(PreparedPublicWrite),
    Direct(PreparedDirectWriteArtifact),
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub struct PreparedWriteStatement {
    pub statement_kind: PreparedWriteStatementKind,
    pub result_contract: ResultContract,
    pub artifact: PreparedWriteArtifact,
    pub diagnostic_context: WriteDiagnosticContext,
    pub public_surface_registry_effect: PreparedPublicSurfaceRegistryEffect,
}

#[allow(dead_code)]
impl PreparedWriteStatement {
    pub fn public_read(&self) -> Option<&PreparedPublicRead> {
        match &self.artifact {
            PreparedWriteArtifact::PublicRead(read) => Some(read),
            PreparedWriteArtifact::PublicWrite(_) | PreparedWriteArtifact::Direct(_) => None,
        }
    }

    pub fn public_write(&self) -> Option<&PreparedPublicWrite> {
        match &self.artifact {
            PreparedWriteArtifact::PublicWrite(write) => Some(write),
            PreparedWriteArtifact::PublicRead(_) | PreparedWriteArtifact::Direct(_) => None,
        }
    }

    pub fn direct_write(&self) -> Option<&PreparedDirectWriteArtifact> {
        match &self.artifact {
            PreparedWriteArtifact::Direct(direct) => Some(direct),
            PreparedWriteArtifact::PublicRead(_) | PreparedWriteArtifact::PublicWrite(_) => None,
        }
    }
}
