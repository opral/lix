use std::collections::{BTreeMap, BTreeSet};

use crate::contracts::{
    coalesce_live_table_requirements, ChangeBatch, CommitPreconditions, ExpectedHead,
    IdempotencyKey, MutationRow, OptionalTextPatch, PlanEffects, PlannedStateRow,
    PreparedInternalWriteArtifact, PreparedPublicWriteArtifact,
    PreparedPublicWriteExecutionArtifact, PreparedPublicWriteExecutionPartition,
    PreparedTrackedWriteExecution, PreparedUntrackedWriteExecution, PreparedWriteStep,
    ResultContract, RowIdentity, SchemaRegistration, SchemaRegistrationSet, WriteLane, WriteMode,
};
use crate::contracts::{PendingSemanticRow, PendingSemanticStorage};
use crate::execution::write::filesystem::runtime::{
    filesystem_transaction_state_has_binary_payloads, merge_filesystem_transaction_state,
    FilesystemTransactionFileState, FilesystemTransactionState,
};
use crate::execution::write::filesystem::state::filesystem_transaction_state_from_planned;
use crate::execution::write::{PendingTransactionView, PreparedWriteRuntimeState};
use crate::LixError;

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const GLOBAL_VERSION_ID: &str = "global";

#[derive(Clone)]
pub(crate) struct TrackedTxnUnit {
    pub(crate) public_writes: Vec<PreparedPublicWriteArtifact>,
    pub(crate) public_write: PreparedPublicWriteArtifact,
    pub(crate) execution: PreparedTrackedWriteExecution,
    pub(crate) filesystem_state: FilesystemTransactionState,
    pub(crate) runtime_state: PreparedWriteRuntimeState,
    pub(crate) writer_key: Option<String>,
}

impl TrackedTxnUnit {
    pub(crate) fn should_emit_observe_tick(&self) -> bool {
        self.has_compiler_only_filesystem_changes()
            || !self
                .execution
                .semantic_effects
                .state_commit_stream_changes
                .is_empty()
    }

    pub(crate) fn has_compiler_only_filesystem_changes(&self) -> bool {
        self.execution.change_batch.is_none() && !self.filesystem_state.files.is_empty()
    }

    pub(crate) fn is_merged_transaction_plan(&self) -> bool {
        self.public_writes.len() > 1
    }
}

fn build_tracked_txn_unit(
    public_write: &PreparedPublicWriteArtifact,
    execution: &PreparedTrackedWriteExecution,
    filesystem_state: &FilesystemTransactionState,
    runtime_state: &PreparedWriteRuntimeState,
) -> TrackedTxnUnit {
    TrackedTxnUnit {
        public_writes: vec![public_write.clone()],
        public_write: public_write.clone(),
        execution: execution.clone(),
        filesystem_state: filesystem_state.clone(),
        runtime_state: runtime_state.clone(),
        writer_key: public_write.contract.writer_key.clone(),
    }
}

#[derive(Clone)]
pub(crate) struct PlannedPublicUntrackedWriteUnit {
    pub(crate) execution: PreparedUntrackedWriteExecution,
    pub(crate) filesystem_state: FilesystemTransactionState,
    pub(crate) runtime_state: PreparedWriteRuntimeState,
    pub(crate) writer_key: Option<String>,
}

#[derive(Clone)]
pub(crate) struct PlannedInternalWriteUnit {
    pub(crate) execution: PreparedInternalWriteArtifact,
    pub(crate) result_contract: ResultContract,
    pub(crate) runtime_state: PreparedWriteRuntimeState,
}

#[derive(Clone)]
pub(crate) enum PlannedWriteUnit {
    PublicTracked(TrackedTxnUnit),
    PublicUntracked(PlannedPublicUntrackedWriteUnit),
    Internal(PlannedInternalWriteUnit),
}

#[derive(Clone, Default)]
pub(crate) struct PlannedWritePlan {
    pub(crate) units: Vec<PlannedWriteUnit>,
}

impl PlannedWritePlan {
    pub(crate) fn extend(&mut self, other: PlannedWritePlan) {
        self.units.extend(other.units);
        self.coalesce_filesystem_tracked_units();
    }

    pub(crate) fn coalesce_filesystem_tracked_units(&mut self) {
        let mut coalesced = Vec::with_capacity(self.units.len());
        for unit in std::mem::take(&mut self.units) {
            match unit {
                PlannedWriteUnit::PublicTracked(next)
                    if coalesced
                        .last_mut()
                        .and_then(|current| match current {
                            PlannedWriteUnit::PublicTracked(current) => Some(current),
                            _ => None,
                        })
                        .is_some_and(|current| {
                            try_merge_filesystem_tracked_plans(current, &next)
                        }) => {}
                other => coalesced.push(other),
            }
        }
        self.units = coalesced;
    }
}

#[derive(Clone)]
pub(crate) struct PlannedWriteDelta {
    materialization_plan: PlannedWritePlan,
    schema_registrations: SchemaRegistrationSet,
    registered_schema_overlay: Option<PendingRegisteredSchemaOverlay>,
    semantic_overlay: Option<PendingSemanticOverlay>,
    filesystem_overlay: Option<PendingFilesystemOverlay>,
    writer_key_overlay: Option<PendingWriterKeyOverlay>,
}

impl PlannedWriteDelta {
    pub(crate) fn from_materialization_plan(
        materialization_plan: PlannedWritePlan,
    ) -> Result<Self, LixError> {
        let schema_registrations =
            schema_registrations_for_planned_write_plan(&materialization_plan);
        let semantic_overlay =
            pending_semantic_overlay_for_planned_write_plan(&materialization_plan)?;
        let filesystem_overlay =
            pending_filesystem_overlay_for_planned_write_plan(&materialization_plan);
        let writer_key_overlay =
            pending_writer_key_overlay_for_planned_write_plan(&materialization_plan);
        let registered_schema_overlay = semantic_overlay
            .as_ref()
            .and_then(PendingSemanticOverlay::registered_schema_overlay);
        Ok(Self {
            materialization_plan,
            schema_registrations,
            registered_schema_overlay,
            semantic_overlay,
            filesystem_overlay,
            writer_key_overlay,
        })
    }

    pub(crate) fn materialization_plan(&self) -> &PlannedWritePlan {
        &self.materialization_plan
    }

    pub(crate) fn schema_registrations(&self) -> &SchemaRegistrationSet {
        &self.schema_registrations
    }

    pub(crate) fn registered_schema_overlay(&self) -> Option<PendingRegisteredSchemaOverlay> {
        self.registered_schema_overlay.clone()
    }

    pub(crate) fn semantic_overlay(&self) -> Option<PendingSemanticOverlay> {
        self.semantic_overlay.clone()
    }

    pub(crate) fn filesystem_overlay(&self) -> Option<PendingFilesystemOverlay> {
        self.filesystem_overlay.clone()
    }

    pub(crate) fn writer_key_overlay(&self) -> Option<PendingWriterKeyOverlay> {
        self.writer_key_overlay.clone()
    }

    pub(crate) fn pending_transaction_view(&self) -> Option<PendingTransactionView> {
        PendingTransactionView::new(
            self.registered_schema_overlay(),
            self.semantic_overlay(),
            self.filesystem_overlay(),
            self.writer_key_overlay(),
        )
    }

    fn supports_registered_schema_overlay(&self) -> bool {
        self.registered_schema_overlay.is_some()
    }

    pub(crate) fn extend(&mut self, incoming: PlannedWriteDelta) -> Result<(), LixError> {
        self.materialization_plan
            .extend(incoming.materialization_plan);
        self.schema_registrations
            .extend(incoming.schema_registrations);
        self.semantic_overlay = match (self.semantic_overlay.take(), incoming.semantic_overlay) {
            (Some(mut current), Some(incoming)) => {
                merge_pending_semantic_overlay(&mut current, incoming);
                Some(current)
            }
            (Some(current), None) => Some(current),
            (None, Some(incoming)) => Some(incoming),
            (None, None) => None,
        };
        self.filesystem_overlay =
            match (self.filesystem_overlay.take(), incoming.filesystem_overlay) {
                (Some(mut current), Some(incoming)) => {
                    merge_pending_filesystem_overlay(&mut current, incoming);
                    Some(current)
                }
                (Some(current), None) => Some(current),
                (None, Some(incoming)) => Some(incoming),
                (None, None) => None,
            };
        self.writer_key_overlay =
            match (self.writer_key_overlay.take(), incoming.writer_key_overlay) {
                (Some(mut current), Some(incoming)) => {
                    merge_pending_writer_key_overlay(&mut current, incoming);
                    Some(current)
                }
                (Some(current), None) => Some(current),
                (None, Some(incoming)) => Some(incoming),
                (None, None) => None,
            };
        self.registered_schema_overlay = self
            .semantic_overlay
            .as_ref()
            .and_then(PendingSemanticOverlay::registered_schema_overlay);
        Ok(())
    }
}

#[derive(Clone, Default)]
pub(crate) struct BufferedWriteJournal {
    staged_delta: Option<PlannedWriteDelta>,
    continuation_safe: bool,
}

impl BufferedWriteJournal {
    pub(crate) fn is_empty(&self) -> bool {
        self.staged_delta.is_none()
    }

    pub(crate) fn can_stage_delta(&self, delta: &PlannedWriteDelta) -> Result<bool, LixError> {
        let plan = delta.materialization_plan();
        let current_supports_registered_schema_overlay = self
            .staged_delta
            .as_ref()
            .is_some_and(PlannedWriteDelta::supports_registered_schema_overlay);
        if current_supports_registered_schema_overlay {
            return Ok(true);
        }

        let incoming_supports_registered_schema_overlay =
            delta.supports_registered_schema_overlay();

        Ok(self.current_materialization_plan().map_or_else(
            || {
                planned_write_plan_is_independent_filesystem(plan)
                    || incoming_supports_registered_schema_overlay
            },
            |current| planned_write_plans_can_continue_together(current, plan),
        ))
    }

    pub(crate) fn stage_delta(&mut self, incoming: PlannedWriteDelta) -> Result<(), LixError> {
        let continuation_safe = self.can_stage_delta(&incoming)?;
        match self.staged_delta.as_mut() {
            Some(current) => {
                current.extend(incoming)?;
                self.continuation_safe &= continuation_safe;
            }
            None => {
                self.continuation_safe = continuation_safe;
                self.staged_delta = Some(incoming);
            }
        }
        Ok(())
    }

    pub(crate) fn take_staged_delta(&mut self) -> Option<PlannedWriteDelta> {
        let delta = self.staged_delta.take()?;
        self.continuation_safe = false;
        Some(delta)
    }

    pub(crate) fn pending_transaction_view(
        &self,
    ) -> Result<Option<PendingTransactionView>, LixError> {
        Ok(self
            .staged_delta
            .as_ref()
            .and_then(PlannedWriteDelta::pending_transaction_view))
    }

    fn current_materialization_plan(&self) -> Option<&PlannedWritePlan> {
        self.staged_delta
            .as_ref()
            .map(PlannedWriteDelta::materialization_plan)
    }
}

#[derive(Clone, Default)]
pub(crate) struct PendingRegisteredSchemaOverlay {
    entries: BTreeMap<String, PendingRegisteredSchemaEntry>,
}

#[derive(Clone)]
pub(crate) struct PendingRegisteredSchemaEntry {
    pub(crate) snapshot_content: Option<String>,
}

impl PendingRegisteredSchemaOverlay {
    pub(crate) fn visible_entries(
        &self,
    ) -> impl Iterator<Item = (&str, &PendingRegisteredSchemaEntry)> {
        self.entries
            .iter()
            .map(|(entity_id, entry)| (entity_id.as_str(), entry))
    }
}

#[derive(Clone, Default)]
pub(crate) struct PendingSemanticOverlay {
    rows: BTreeMap<PendingSemanticRowIdentity, PendingSemanticRow>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct PendingSemanticRowIdentity {
    storage: PendingSemanticStorage,
    schema_key: String,
    entity_id: String,
    file_id: String,
    version_id: String,
    plugin_key: String,
    schema_version: String,
}

#[derive(Clone, Default)]
pub(crate) struct PendingFilesystemOverlay {
    directory_rows: BTreeMap<PendingSemanticRowIdentity, PendingSemanticRow>,
    files: BTreeMap<(String, String), FilesystemTransactionFileState>,
}

#[derive(Clone, Default)]
pub(crate) struct PendingWriterKeyOverlay {
    annotations: BTreeMap<RowIdentity, Option<String>>,
}

impl PendingFilesystemOverlay {
    pub(crate) fn visible_directory_rows<'a>(
        &'a self,
        storage: PendingSemanticStorage,
        schema_key: &'a str,
    ) -> impl Iterator<Item = &'a PendingSemanticRow> {
        self.directory_rows
            .values()
            .filter(move |row| row.storage == storage && row.schema_key == schema_key)
    }

    pub(crate) fn visible_files(&self) -> impl Iterator<Item = &FilesystemTransactionFileState> {
        self.files.values()
    }
}

impl PendingWriterKeyOverlay {
    pub(crate) fn annotation_for_state_row(
        &self,
        version_id: &str,
        schema_key: &str,
        entity_id: &str,
        file_id: &str,
    ) -> Option<&Option<String>> {
        self.annotations.get(&RowIdentity {
            version_id: version_id.to_string(),
            schema_key: schema_key.to_string(),
            entity_id: entity_id.to_string(),
            file_id: file_id.to_string(),
        })
    }
}

impl PendingSemanticOverlay {
    pub(crate) fn visible_rows<'a>(
        &'a self,
        storage: PendingSemanticStorage,
        schema_key: &'a str,
    ) -> impl Iterator<Item = &'a PendingSemanticRow> {
        self.rows
            .values()
            .filter(move |row| row.storage == storage && row.schema_key == schema_key)
    }

    pub(crate) fn registered_schema_overlay(&self) -> Option<PendingRegisteredSchemaOverlay> {
        let mut overlay = PendingRegisteredSchemaOverlay::default();
        for row in self.visible_rows(PendingSemanticStorage::Tracked, REGISTERED_SCHEMA_KEY) {
            if row.version_id != GLOBAL_VERSION_ID {
                continue;
            }
            overlay.entries.insert(
                row.entity_id.clone(),
                PendingRegisteredSchemaEntry {
                    snapshot_content: (!row.tombstone)
                        .then(|| row.snapshot_content.clone())
                        .flatten(),
                },
            );
        }
        (!overlay.entries.is_empty()).then_some(overlay)
    }
}

pub(crate) fn build_planned_write_plan(
    prepared: &PreparedWriteStep,
    runtime_state: &PreparedWriteRuntimeState,
) -> Option<PlannedWritePlan> {
    let mut units = Vec::new();

    if let Some(public_write) = prepared.public_write() {
        let filesystem_state = filesystem_transaction_state_from_planned(
            &public_write
                .contract
                .resolved_write_plan
                .as_ref()
                .map(|resolved| resolved.filesystem_state())
                .unwrap_or_default(),
        );
        if let PreparedPublicWriteExecutionArtifact::Materialize(materialization) =
            &public_write.execution
        {
            for partition in &materialization.partitions {
                match partition {
                    PreparedPublicWriteExecutionPartition::Tracked(tracked) => {
                        let tracked_plan = build_tracked_txn_unit(
                            public_write,
                            tracked,
                            &filesystem_state,
                            runtime_state,
                        );
                        units.push(PlannedWriteUnit::PublicTracked(tracked_plan));
                    }
                    PreparedPublicWriteExecutionPartition::Untracked(untracked) => {
                        units.push(PlannedWriteUnit::PublicUntracked(
                            PlannedPublicUntrackedWriteUnit {
                                execution: untracked.clone(),
                                filesystem_state: filesystem_state.clone(),
                                runtime_state: runtime_state.clone(),
                                writer_key: public_write.contract.writer_key.clone(),
                            },
                        ));
                    }
                }
            }
        }
        if let Some(resolved) = public_write.contract.resolved_write_plan.as_ref() {
            for partition in &resolved.partitions {
                if partition.execution_mode != WriteMode::Tracked
                    || !partition.intended_post_state.is_empty()
                    || partition.writer_key_updates.is_empty()
                {
                    continue;
                }
                units.push(PlannedWriteUnit::PublicTracked(build_tracked_txn_unit(
                    public_write,
                    &PreparedTrackedWriteExecution {
                        schema_live_table_requirements: Vec::new(),
                        change_batch: None,
                        create_preconditions: writer_key_only_commit_preconditions(public_write),
                        semantic_effects: PlanEffects::default(),
                    },
                    &filesystem_state,
                    runtime_state,
                )));
            }
        }
    } else if let Some(internal_execution) = prepared.internal_write() {
        if internal_execution.read_only_query {
            return None;
        }
        units.push(PlannedWriteUnit::Internal(PlannedInternalWriteUnit {
            execution: internal_execution.clone(),
            result_contract: prepared.result_contract,
            runtime_state: runtime_state.clone(),
        }));
    }

    if units.is_empty() {
        None
    } else {
        let mut plan = PlannedWritePlan { units };
        plan.coalesce_filesystem_tracked_units();
        Some(plan)
    }
}

pub(crate) fn build_planned_write_delta(
    prepared: &PreparedWriteStep,
    runtime_state: &PreparedWriteRuntimeState,
) -> Result<Option<PlannedWriteDelta>, LixError> {
    build_planned_write_plan(prepared, runtime_state)
        .map(PlannedWriteDelta::from_materialization_plan)
        .transpose()
}

fn writer_key_only_commit_preconditions(
    public_write: &PreparedPublicWriteArtifact,
) -> CommitPreconditions {
    let write_lane = public_write
        .contract
        .requested_version_id
        .as_ref()
        .map(|version_id| WriteLane::SingleVersion(version_id.clone()))
        .unwrap_or(WriteLane::ActiveVersion);
    CommitPreconditions {
        write_lane,
        expected_head: ExpectedHead::CurrentHead,
        idempotency_key: IdempotencyKey("writer-key-only-live-state-update".to_string()),
    }
}

fn schema_registrations_for_planned_write_plan(plan: &PlannedWritePlan) -> SchemaRegistrationSet {
    let mut registrations = SchemaRegistrationSet::default();
    for unit in &plan.units {
        match unit {
            PlannedWriteUnit::PublicTracked(tracked) => {
                for requirement in coalesce_live_table_requirements(
                    &tracked.execution.schema_live_table_requirements,
                ) {
                    match requirement.schema_definition.as_ref() {
                        Some(schema_definition) => {
                            registrations.insert(SchemaRegistration::with_schema_definition(
                                requirement.schema_key.clone(),
                                schema_definition.clone(),
                            ))
                        }
                        None => registrations.insert(requirement.schema_key.clone()),
                    }
                }
            }
            PlannedWriteUnit::PublicUntracked(untracked) => {
                for row in &untracked.execution.intended_post_state {
                    registrations.insert(row.schema_key.clone());
                }
            }
            PlannedWriteUnit::Internal(internal) => {
                for requirement in
                    coalesce_live_table_requirements(&internal.execution.live_table_requirements)
                {
                    match requirement.schema_definition.as_ref() {
                        Some(schema_definition) => {
                            registrations.insert(SchemaRegistration::with_schema_definition(
                                requirement.schema_key.clone(),
                                schema_definition.clone(),
                            ))
                        }
                        None => registrations.insert(requirement.schema_key.clone()),
                    }
                }
            }
        }
    }
    registrations
}

pub(crate) fn pending_semantic_overlay_for_planned_write_plan(
    plan: &PlannedWritePlan,
) -> Result<Option<PendingSemanticOverlay>, LixError> {
    let mut overlay = PendingSemanticOverlay::default();
    if !collect_semantic_overlay_from_planned_write_plan(plan, &mut overlay)? {
        return Ok(None);
    }
    Ok((!overlay.rows.is_empty()).then_some(overlay))
}

pub(crate) fn pending_filesystem_overlay_for_planned_write_plan(
    plan: &PlannedWritePlan,
) -> Option<PendingFilesystemOverlay> {
    let mut overlay = PendingFilesystemOverlay::default();
    if !collect_filesystem_overlay_from_planned_write_plan(plan, &mut overlay) {
        return None;
    }
    (!overlay.directory_rows.is_empty() || !overlay.files.is_empty()).then_some(overlay)
}

pub(crate) fn pending_writer_key_overlay_for_planned_write_plan(
    plan: &PlannedWritePlan,
) -> Option<PendingWriterKeyOverlay> {
    let mut overlay = PendingWriterKeyOverlay::default();
    for unit in &plan.units {
        match unit {
            PlannedWriteUnit::PublicTracked(tracked) => {
                for public_write in &tracked.public_writes {
                    let Some(resolved) = public_write.contract.resolved_write_plan.as_ref() else {
                        continue;
                    };
                    for partition in &resolved.partitions {
                        if partition.execution_mode != WriteMode::Tracked {
                            continue;
                        }
                        for (row_identity, writer_key) in &partition.writer_key_updates {
                            overlay.annotations.insert(
                                RowIdentity {
                                    schema_key: row_identity.schema_key.clone(),
                                    version_id: row_identity.version_id.clone(),
                                    entity_id: row_identity.entity_id.clone(),
                                    file_id: row_identity.file_id.clone(),
                                },
                                writer_key.clone(),
                            );
                        }
                        for row in &partition.intended_post_state {
                            let Some(version_id) = row.version_id.as_ref() else {
                                continue;
                            };
                            let Some(file_id) =
                                row.values.get("file_id").and_then(|value| match value {
                                    crate::Value::Text(value) => Some(value.clone()),
                                    _ => None,
                                })
                            else {
                                continue;
                            };
                            overlay.annotations.insert(
                                RowIdentity {
                                    schema_key: row.schema_key.clone(),
                                    version_id: version_id.clone(),
                                    entity_id: row.entity_id.clone(),
                                    file_id,
                                },
                                row.writer_key.clone(),
                            );
                        }
                    }
                }
            }
            PlannedWriteUnit::PublicUntracked(untracked) => {
                for row in &untracked.execution.intended_post_state {
                    let Some(version_id) = row.version_id.as_ref() else {
                        continue;
                    };
                    let Some(file_id) = row.values.get("file_id").and_then(|value| match value {
                        crate::Value::Text(value) => Some(value.clone()),
                        _ => None,
                    }) else {
                        continue;
                    };
                    overlay.annotations.insert(
                        RowIdentity {
                            schema_key: row.schema_key.clone(),
                            version_id: version_id.clone(),
                            entity_id: row.entity_id.clone(),
                            file_id,
                        },
                        row.writer_key
                            .clone()
                            .or_else(|| untracked.writer_key.clone()),
                    );
                }
            }
            PlannedWriteUnit::Internal(_) => {}
        }
    }
    (!overlay.annotations.is_empty()).then_some(overlay)
}

pub(crate) fn planned_write_plan_is_independent_filesystem(plan: &PlannedWritePlan) -> bool {
    !plan.units.is_empty()
        && plan.units.iter().all(|unit| match unit {
            PlannedWriteUnit::PublicTracked(tracked) => {
                tracked_plan_is_coalescible_filesystem(tracked)
            }
            PlannedWriteUnit::PublicUntracked(_) | PlannedWriteUnit::Internal(_) => false,
        })
}

pub(crate) fn planned_write_plans_can_continue_together(
    left: &PlannedWritePlan,
    right: &PlannedWritePlan,
) -> bool {
    if !planned_write_plan_is_independent_filesystem(left)
        || !planned_write_plan_is_independent_filesystem(right)
    {
        return false;
    }

    left.units.iter().all(|left_unit| {
        let PlannedWriteUnit::PublicTracked(left_tracked) = left_unit else {
            return false;
        };
        right.units.iter().all(|right_unit| {
            let PlannedWriteUnit::PublicTracked(right_tracked) = right_unit else {
                return false;
            };
            filesystem_tracked_plans_are_buffer_compatible(left_tracked, right_tracked)
        })
    })
}

fn merge_pending_semantic_overlay(
    current: &mut PendingSemanticOverlay,
    incoming: PendingSemanticOverlay,
) {
    current.rows.extend(incoming.rows);
}

fn merge_pending_filesystem_overlay(
    current: &mut PendingFilesystemOverlay,
    incoming: PendingFilesystemOverlay,
) {
    current.directory_rows.extend(incoming.directory_rows);
    current.files.extend(incoming.files);
}

fn merge_pending_writer_key_overlay(
    current: &mut PendingWriterKeyOverlay,
    incoming: PendingWriterKeyOverlay,
) {
    current.annotations.extend(incoming.annotations);
}

fn collect_semantic_overlay_from_planned_write_plan(
    plan: &PlannedWritePlan,
    overlay: &mut PendingSemanticOverlay,
) -> Result<bool, LixError> {
    if plan.units.is_empty() {
        return Ok(false);
    }

    for unit in &plan.units {
        let unit_supported = match unit {
            PlannedWriteUnit::PublicTracked(tracked) => {
                !filesystem_transaction_state_has_binary_payloads(&tracked.filesystem_state)
                    && tracked
                        .public_writes
                        .iter()
                        .try_fold(true, |supported, public_write| {
                            if !supported {
                                return Ok(false);
                            }
                            collect_semantic_overlay_from_public_write(public_write, overlay)
                        })?
            }
            PlannedWriteUnit::PublicUntracked(untracked) => {
                untracked.filesystem_state.files.is_empty()
                    && collect_semantic_overlay_from_planned_rows(
                        untracked.execution.intended_post_state.iter(),
                        PendingSemanticStorage::Untracked,
                        overlay,
                    )?
            }
            PlannedWriteUnit::Internal(internal) => {
                internal.execution.filesystem_state.files.is_empty()
                    && collect_semantic_overlay_from_mutation_rows(
                        &internal.execution.mutations,
                        overlay,
                    )?
                    && !internal.execution.has_update_validations
            }
        };

        if !unit_supported {
            return Ok(false);
        }
    }

    Ok(true)
}

fn collect_filesystem_overlay_from_planned_write_plan(
    plan: &PlannedWritePlan,
    overlay: &mut PendingFilesystemOverlay,
) -> bool {
    if plan.units.is_empty() {
        return false;
    }

    let mut saw_entry = false;
    for unit in &plan.units {
        let unit_supported = match unit {
            PlannedWriteUnit::PublicTracked(tracked) => {
                collect_filesystem_overlay_from_tracked_plan(tracked, overlay, &mut saw_entry)
            }
            PlannedWriteUnit::PublicUntracked(_) | PlannedWriteUnit::Internal(_) => false,
        };
        if !unit_supported {
            return false;
        }
    }

    saw_entry
}

fn collect_filesystem_overlay_from_tracked_plan(
    tracked: &TrackedTxnUnit,
    overlay: &mut PendingFilesystemOverlay,
    saw_entry: &mut bool,
) -> bool {
    if !matches!(
        tracked
            .public_write
            .contract
            .target
            .descriptor
            .public_name
            .as_str(),
        "lix_file" | "lix_file_by_version"
    ) {
        return false;
    }

    for public_write in &tracked.public_writes {
        let Some(resolved) = public_write.contract.resolved_write_plan.as_ref() else {
            return false;
        };
        for partition in &resolved.partitions {
            if partition.execution_mode != WriteMode::Tracked {
                return false;
            }
            if !collect_directory_descriptor_overlay_from_planned_rows(
                partition.intended_post_state.iter(),
                overlay,
            ) {
                return false;
            }
            if partition
                .intended_post_state
                .iter()
                .any(|row| row.schema_key.as_str() == "lix_directory_descriptor")
            {
                *saw_entry = true;
            }
        }
    }

    let state = tracked.filesystem_state.clone();
    if !state.files.is_empty() {
        *saw_entry = true;
    }
    overlay.files.extend(state.files);

    true
}

fn collect_directory_descriptor_overlay_from_planned_rows<'a>(
    rows: impl Iterator<Item = &'a PlannedStateRow>,
    overlay: &mut PendingFilesystemOverlay,
) -> bool {
    for row in rows {
        if row.schema_key.as_str() != "lix_directory_descriptor" {
            continue;
        }
        let file_id = match row.values.get("file_id") {
            Some(crate::Value::Text(value)) => value.clone(),
            _ => return false,
        };
        let version_id = match row.version_id.as_deref() {
            Some(value) => value.to_string(),
            None => return false,
        };
        let plugin_key = match row.values.get("plugin_key") {
            Some(crate::Value::Text(value)) => value.clone(),
            _ => return false,
        };
        let schema_version = match row.values.get("schema_version") {
            Some(crate::Value::Text(value)) => value.clone(),
            _ => return false,
        };
        let snapshot_content = match row.values.get("snapshot_content") {
            Some(crate::Value::Text(snapshot)) => Some(snapshot.clone()),
            Some(crate::Value::Null) | None if row.tombstone => None,
            None => None,
            _ => return false,
        };
        overlay.directory_rows.insert(
            PendingSemanticRowIdentity {
                storage: PendingSemanticStorage::Tracked,
                schema_key: row.schema_key.clone(),
                entity_id: row.entity_id.clone(),
                file_id: file_id.clone(),
                version_id: version_id.clone(),
                plugin_key: plugin_key.clone(),
                schema_version: schema_version.clone(),
            },
            PendingSemanticRow {
                storage: PendingSemanticStorage::Tracked,
                entity_id: row.entity_id.clone(),
                schema_key: row.schema_key.clone(),
                schema_version,
                file_id,
                version_id,
                plugin_key,
                snapshot_content,
                metadata: row.values.get("metadata").and_then(|value| match value {
                    crate::Value::Text(text) => Some(text.clone()),
                    _ => None,
                }),
                tombstone: row.tombstone,
            },
        );
    }
    true
}

fn collect_semantic_overlay_from_public_write(
    public_write: &PreparedPublicWriteArtifact,
    overlay: &mut PendingSemanticOverlay,
) -> Result<bool, LixError> {
    let Some(resolved) = public_write.contract.resolved_write_plan.as_ref() else {
        return Ok(false);
    };
    let skip_file_descriptor_rows = matches!(
        public_write.contract.target.descriptor.public_name.as_str(),
        "lix_file" | "lix_file_by_version"
    );
    let mut saw_row = false;
    for partition in &resolved.partitions {
        let storage = match partition.execution_mode {
            WriteMode::Tracked => PendingSemanticStorage::Tracked,
            WriteMode::Untracked => PendingSemanticStorage::Untracked,
        };
        saw_row |= collect_semantic_overlay_from_planned_rows(
            partition.intended_post_state.iter().filter(|row| {
                !(skip_file_descriptor_rows && row.schema_key == "lix_file_descriptor")
            }),
            storage,
            overlay,
        )?;
    }
    Ok(saw_row)
}

fn collect_semantic_overlay_from_planned_rows<'a>(
    rows: impl Iterator<Item = &'a PlannedStateRow>,
    storage: PendingSemanticStorage,
    overlay: &mut PendingSemanticOverlay,
) -> Result<bool, LixError> {
    let mut saw_row = false;
    for row in rows {
        saw_row = true;
        let file_id = match row.values.get("file_id") {
            Some(crate::Value::Text(value)) => value.clone(),
            _ => return Ok(false),
        };
        let version_id = match row.version_id.as_deref() {
            Some(value) => value.to_string(),
            None => return Ok(false),
        };
        let plugin_key = match row.values.get("plugin_key") {
            Some(crate::Value::Text(value)) => value.clone(),
            _ => return Ok(false),
        };
        let schema_version = match row.values.get("schema_version") {
            Some(crate::Value::Text(value)) => value.clone(),
            _ => return Ok(false),
        };
        let snapshot_content = match row.values.get("snapshot_content") {
            Some(crate::Value::Text(snapshot)) => Some(snapshot.clone()),
            Some(crate::Value::Null) | None if row.tombstone => None,
            None => None,
            _ => return Ok(false),
        };
        overlay.rows.insert(
            PendingSemanticRowIdentity {
                storage,
                schema_key: row.schema_key.clone(),
                entity_id: row.entity_id.clone(),
                file_id: file_id.clone(),
                version_id: version_id.clone(),
                plugin_key: plugin_key.clone(),
                schema_version: schema_version.clone(),
            },
            PendingSemanticRow {
                storage,
                entity_id: row.entity_id.clone(),
                schema_key: row.schema_key.clone(),
                schema_version,
                file_id,
                version_id,
                plugin_key,
                snapshot_content,
                metadata: row.values.get("metadata").and_then(|value| match value {
                    crate::Value::Text(text) => Some(text.clone()),
                    _ => None,
                }),
                tombstone: row.tombstone,
            },
        );
    }

    Ok(saw_row)
}

fn collect_semantic_overlay_from_mutation_rows(
    rows: &[MutationRow],
    overlay: &mut PendingSemanticOverlay,
) -> Result<bool, LixError> {
    if rows.is_empty() {
        return Ok(false);
    }

    for row in rows {
        let snapshot_content = row
            .snapshot_content
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("failed to serialize pending registered schema snapshot: {error}"),
                )
            })?;
        let storage = if row.untracked {
            PendingSemanticStorage::Untracked
        } else {
            PendingSemanticStorage::Tracked
        };
        overlay.rows.insert(
            PendingSemanticRowIdentity {
                storage,
                schema_key: row.schema_key.clone(),
                entity_id: row.entity_id.clone(),
                file_id: row.file_id.clone(),
                version_id: row.version_id.clone(),
                plugin_key: row.plugin_key.clone(),
                schema_version: row.schema_version.clone(),
            },
            PendingSemanticRow {
                storage,
                entity_id: row.entity_id.clone(),
                schema_key: row.schema_key.clone(),
                schema_version: row.schema_version.clone(),
                file_id: row.file_id.clone(),
                version_id: row.version_id.clone(),
                plugin_key: row.plugin_key.clone(),
                snapshot_content,
                metadata: None,
                tombstone: false,
            },
        );
    }

    Ok(true)
}

fn try_merge_filesystem_tracked_plans(current: &mut TrackedTxnUnit, next: &TrackedTxnUnit) -> bool {
    if !filesystem_tracked_plans_are_buffer_compatible(current, next) {
        return false;
    }

    for requirement in &next.execution.schema_live_table_requirements {
        if !current
            .execution
            .schema_live_table_requirements
            .contains(requirement)
        {
            current
                .execution
                .schema_live_table_requirements
                .push(requirement.clone());
        }
    }
    current.execution.change_batch = merge_optional_change_batches(
        current.execution.change_batch.as_ref(),
        next.execution.change_batch.as_ref(),
    );
    merge_plan_effects(
        &mut current.execution.semantic_effects,
        &next.execution.semantic_effects,
    );
    current.public_writes.extend(next.public_writes.clone());
    merge_filesystem_transaction_state(&mut current.filesystem_state, &next.filesystem_state);
    true
}

fn filesystem_tracked_plans_are_buffer_compatible(
    current: &TrackedTxnUnit,
    next: &TrackedTxnUnit,
) -> bool {
    if !tracked_plan_is_coalescible_filesystem(current)
        || !tracked_plan_is_coalescible_filesystem(next)
    {
        return false;
    }
    current.public_write.contract.target.descriptor.public_name
        == next.public_write.contract.target.descriptor.public_name
        && current.execution.create_preconditions.write_lane
            == next.execution.create_preconditions.write_lane
        && create_commit_expected_head_compatible(
            &current.execution.create_preconditions.expected_head,
            &next.execution.create_preconditions.expected_head,
        )
        && current.writer_key == next.writer_key
        && tracked_plan_entity_targets_disjoint(current, next)
}

fn tracked_plan_is_coalescible_filesystem(plan: &TrackedTxnUnit) -> bool {
    matches!(
        plan.public_write
            .contract
            .target
            .descriptor
            .public_name
            .as_str(),
        "lix_file" | "lix_file_by_version"
    )
}

fn create_commit_expected_head_compatible(left: &ExpectedHead, right: &ExpectedHead) -> bool {
    matches!(
        (left, right),
        (ExpectedHead::CurrentHead, ExpectedHead::CurrentHead)
    )
}

fn tracked_plan_entity_targets_disjoint(left: &TrackedTxnUnit, right: &TrackedTxnUnit) -> bool {
    let left_targets = tracked_plan_entity_targets(left);
    let right_targets = tracked_plan_entity_targets(right);
    left_targets.is_disjoint(&right_targets)
}

fn merge_plan_effects(current: &mut PlanEffects, next: &PlanEffects) {
    current
        .state_commit_stream_changes
        .extend(next.state_commit_stream_changes.clone());
    current.session_delta.merge(next.session_delta.clone());
    current
        .file_cache_refresh_targets
        .extend(next.file_cache_refresh_targets.clone());
}

fn tracked_plan_entity_targets(plan: &TrackedTxnUnit) -> BTreeSet<(String, String, String)> {
    let mut targets = BTreeSet::new();
    if let Some(batch) = plan.execution.change_batch.as_ref() {
        for change in &batch.changes {
            if change.schema_key == "lix_directory_descriptor" {
                continue;
            }
            targets.insert((
                change.entity_id.to_string(),
                change.schema_key.to_string(),
                change.version_id.to_string(),
            ));
        }
    }
    for file in plan.filesystem_state.files.values() {
        if file.deleted
            || file.descriptor.is_some()
            || !matches!(file.metadata_patch, OptionalTextPatch::Unchanged)
        {
            targets.insert((
                file.file_id.clone(),
                "lix_file_descriptor".to_string(),
                file.version_id.clone(),
            ));
        }
        if file.deleted || file.data.is_some() {
            targets.insert((
                file.file_id.clone(),
                "lix_binary_blob_ref".to_string(),
                file.version_id.clone(),
            ));
        }
    }
    targets
}

fn merge_optional_change_batches(
    left: Option<&ChangeBatch>,
    right: Option<&ChangeBatch>,
) -> Option<ChangeBatch> {
    match (left, right) {
        (None, None) => None,
        (Some(batch), None) | (None, Some(batch)) => Some(batch.clone()),
        (Some(left), Some(right)) => {
            let mut merged = BTreeMap::new();
            for change in left.changes.iter().chain(right.changes.iter()) {
                merged.insert(
                    (
                        change.entity_id.clone(),
                        change.schema_key.clone(),
                        change.version_id.clone(),
                    ),
                    change.clone(),
                );
            }
            let mut semantic_effects = left.semantic_effects.clone();
            semantic_effects.extend(right.semantic_effects.clone());
            Some(ChangeBatch {
                changes: merged.into_values().collect(),
                write_lane: left.write_lane.clone(),
                writer_key: left.writer_key.clone().or_else(|| right.writer_key.clone()),
                semantic_effects,
            })
        }
    }
}
