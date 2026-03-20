use std::collections::{BTreeMap, BTreeSet};

use crate::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::functions::SharedFunctionProvider;
use crate::sql::execution::contracts::effects::PlanEffects;
use crate::sql::execution::runtime_effects::{
    filesystem_transaction_state_has_binary_payloads, merge_filesystem_transaction_state,
    FilesystemTransactionFileState, FilesystemTransactionState,
};
use crate::sql::execution::shared_path::PreparedExecutionContext;
use crate::sql::public::planner::semantics::domain_changes::DomainChangeBatch;
use crate::sql::public::runtime::{
    build_tracked_write_txn_plan, PublicWriteExecutionPartition, TrackedWriteTxnPlan,
    UntrackedWriteExecution,
};
use crate::LixError;

use super::contracts::execution_plan::ExecutionPlan;

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const GLOBAL_VERSION_ID: &str = "global";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteTxnRunMode {
    Owned,
    Borrowed,
}

#[derive(Clone)]
pub(crate) struct PublicUntrackedWriteTxnPlan {
    pub(crate) execution: UntrackedWriteExecution,
    pub(crate) filesystem_state: FilesystemTransactionState,
    pub(crate) functions: SharedFunctionProvider<RuntimeFunctionProvider>,
    pub(crate) settings: DeterministicSettings,
    pub(crate) sequence_start: i64,
    pub(crate) writer_key: Option<String>,
}

#[derive(Clone)]
pub(crate) struct InternalWriteTxnPlan {
    pub(crate) plan: ExecutionPlan,
    pub(crate) filesystem_state: FilesystemTransactionState,
    pub(crate) functions: SharedFunctionProvider<RuntimeFunctionProvider>,
    pub(crate) settings: DeterministicSettings,
    pub(crate) sequence_start: i64,
    pub(crate) writer_key: Option<String>,
}

#[derive(Clone)]
pub(crate) enum WriteTxnUnit {
    PublicTracked(TrackedWriteTxnPlan),
    PublicUntracked(PublicUntrackedWriteTxnPlan),
    Internal(InternalWriteTxnPlan),
}

#[derive(Clone, Default)]
pub(crate) struct WriteTxnPlan {
    pub(crate) units: Vec<WriteTxnUnit>,
}

impl WriteTxnPlan {
    pub(crate) fn extend(&mut self, other: WriteTxnPlan) {
        self.units.extend(other.units);
        self.coalesce_filesystem_tracked_units();
    }

    pub(crate) fn bind_runtime(
        &mut self,
        settings: DeterministicSettings,
        sequence_start: i64,
        functions: SharedFunctionProvider<RuntimeFunctionProvider>,
    ) {
        for unit in &mut self.units {
            match unit {
                WriteTxnUnit::PublicTracked(tracked) => {
                    tracked.functions = functions.clone();
                }
                WriteTxnUnit::PublicUntracked(untracked) => {
                    untracked.settings = settings;
                    untracked.sequence_start = sequence_start;
                    untracked.functions = functions.clone();
                }
                WriteTxnUnit::Internal(internal) => {
                    internal.settings = settings;
                    internal.sequence_start = sequence_start;
                    internal.functions = functions.clone();
                }
            }
        }
    }

    pub(crate) fn coalesce_filesystem_tracked_units(&mut self) {
        let mut coalesced = Vec::with_capacity(self.units.len());
        for unit in std::mem::take(&mut self.units) {
            match unit {
                WriteTxnUnit::PublicTracked(next)
                    if coalesced
                        .last_mut()
                        .and_then(|current| match current {
                            WriteTxnUnit::PublicTracked(current) => Some(current),
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
pub(crate) enum TxnOp {
    WritePlan(WriteTxnPlan),
}

#[derive(Clone, Default)]
pub(crate) struct MutationJournal {
    ops: Vec<TxnOp>,
    continuation_safe: bool,
    pending_registered_schema_overlay: Option<PendingRegisteredSchemaOverlay>,
    pending_semantic_overlay: Option<PendingSemanticOverlay>,
    pending_filesystem_overlay: Option<PendingFilesystemOverlay>,
}

impl MutationJournal {
    pub(crate) fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    pub(crate) fn continuation_safe(&self) -> bool {
        self.continuation_safe
    }

    pub(crate) fn can_stage_write_plan(&self, plan: &WriteTxnPlan) -> Result<bool, LixError> {
        let current_supports_registered_schema_overlay =
            self.pending_registered_schema_overlay.is_some();
        if current_supports_registered_schema_overlay {
            return Ok(true);
        }

        let incoming_supports_registered_schema_overlay =
            pending_registered_schema_overlay_for_write_plan(plan)?.is_some();

        Ok(self.current_write_plan().map_or_else(
            || {
                write_txn_plan_is_independent_filesystem(plan)
                    || incoming_supports_registered_schema_overlay
            },
            |current| write_txn_plans_can_continue_together(current, plan),
        ))
    }

    pub(crate) fn stage_write_plan(&mut self, plan: WriteTxnPlan) -> Result<(), LixError> {
        let continuation_safe = self.can_stage_write_plan(&plan)?;
        self.apply_plan_to_pending_overlays(&plan)?;
        match self.ops.first_mut() {
            Some(TxnOp::WritePlan(current)) => {
                current.extend(plan);
                self.continuation_safe &= continuation_safe;
            }
            None => {
                self.continuation_safe = continuation_safe;
                self.ops.push(TxnOp::WritePlan(plan));
            }
        }
        Ok(())
    }

    pub(crate) fn take_staged_write_plan(&mut self) -> Option<WriteTxnPlan> {
        let plan = match self.ops.pop()? {
            TxnOp::WritePlan(plan) => plan,
        };
        self.continuation_safe = false;
        self.pending_registered_schema_overlay = None;
        self.pending_semantic_overlay = None;
        self.pending_filesystem_overlay = None;
        Some(plan)
    }

    pub(crate) fn pending_registered_schema_overlay(
        &self,
    ) -> Result<Option<PendingRegisteredSchemaOverlay>, LixError> {
        Ok(self.pending_registered_schema_overlay.clone())
    }

    pub(crate) fn pending_semantic_overlay(
        &self,
    ) -> Result<Option<PendingSemanticOverlay>, LixError> {
        Ok(self.pending_semantic_overlay.clone())
    }

    pub(crate) fn pending_filesystem_overlay(&self) -> Option<PendingFilesystemOverlay> {
        self.pending_filesystem_overlay.clone()
    }

    fn current_write_plan(&self) -> Option<&WriteTxnPlan> {
        match self.ops.first() {
            Some(TxnOp::WritePlan(plan)) => Some(plan),
            None => None,
        }
    }

    fn apply_plan_to_pending_overlays(&mut self, plan: &WriteTxnPlan) -> Result<(), LixError> {
        if self.ops.is_empty() {
            self.pending_semantic_overlay = pending_semantic_overlay_for_write_plan(plan)?;
            self.pending_filesystem_overlay = pending_filesystem_overlay_for_write_plan(plan);
        } else {
            self.pending_semantic_overlay = match self.pending_semantic_overlay.take() {
                Some(mut overlay) => {
                    collect_semantic_overlay_from_plan(plan, &mut overlay)?.then_some(overlay)
                }
                None => None,
            };
            self.pending_filesystem_overlay = match self.pending_filesystem_overlay.take() {
                Some(mut overlay) => {
                    collect_filesystem_overlay_from_plan(plan, &mut overlay).then_some(overlay)
                }
                None => None,
            };
        }
        self.pending_registered_schema_overlay = self
            .pending_semantic_overlay
            .as_ref()
            .and_then(PendingSemanticOverlay::registered_schema_overlay);
        Ok(())
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum PendingSemanticStorage {
    Tracked,
    Untracked,
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

#[derive(Clone)]
pub(crate) struct PendingSemanticRow {
    pub(crate) storage: PendingSemanticStorage,
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) plugin_key: String,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) tombstone: bool,
}

#[derive(Clone, Default)]
pub(crate) struct PendingFilesystemOverlay {
    directory_rows: BTreeMap<PendingSemanticRowIdentity, PendingSemanticRow>,
    files: BTreeMap<(String, String), FilesystemTransactionFileState>,
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

pub(crate) fn pending_registered_schema_overlay_for_write_plan(
    plan: &WriteTxnPlan,
) -> Result<Option<PendingRegisteredSchemaOverlay>, LixError> {
    Ok(pending_semantic_overlay_for_write_plan(plan)?
        .and_then(|overlay| overlay.registered_schema_overlay()))
}

pub(crate) fn pending_semantic_overlay_for_write_plan(
    plan: &WriteTxnPlan,
) -> Result<Option<PendingSemanticOverlay>, LixError> {
    let mut overlay = PendingSemanticOverlay::default();
    if !collect_semantic_overlay_from_plan(plan, &mut overlay)? {
        return Ok(None);
    }
    Ok((!overlay.rows.is_empty()).then_some(overlay))
}

pub(crate) fn pending_filesystem_overlay_for_write_plan(
    plan: &WriteTxnPlan,
) -> Option<PendingFilesystemOverlay> {
    let mut overlay = PendingFilesystemOverlay::default();
    if !collect_filesystem_overlay_from_plan(plan, &mut overlay) {
        return None;
    }
    (!overlay.directory_rows.is_empty() || !overlay.files.is_empty()).then_some(overlay)
}

pub(crate) fn write_txn_plan_is_independent_filesystem(plan: &WriteTxnPlan) -> bool {
    !plan.units.is_empty()
        && plan.units.iter().all(|unit| match unit {
            WriteTxnUnit::PublicTracked(tracked) => tracked_plan_is_coalescible_filesystem(tracked),
            WriteTxnUnit::PublicUntracked(_) | WriteTxnUnit::Internal(_) => false,
        })
}

pub(crate) fn write_txn_plans_can_continue_together(
    left: &WriteTxnPlan,
    right: &WriteTxnPlan,
) -> bool {
    if !write_txn_plan_is_independent_filesystem(left)
        || !write_txn_plan_is_independent_filesystem(right)
    {
        return false;
    }

    left.units.iter().all(|left_unit| {
        let WriteTxnUnit::PublicTracked(left_tracked) = left_unit else {
            return false;
        };
        right.units.iter().all(|right_unit| {
            let WriteTxnUnit::PublicTracked(right_tracked) = right_unit else {
                return false;
            };
            filesystem_tracked_plans_are_buffer_compatible(left_tracked, right_tracked)
        })
    })
}

pub(crate) fn build_write_txn_plan(
    prepared: &PreparedExecutionContext,
    writer_key: Option<&str>,
) -> Option<WriteTxnPlan> {
    let mut units = Vec::new();

    if let Some(public_write) = prepared.public_write.as_ref() {
        if let Some(execution) = public_write.execution.as_ref() {
            for partition in &execution.partitions {
                match partition {
                    PublicWriteExecutionPartition::Tracked(tracked) => {
                        let tracked_plan = build_tracked_write_txn_plan(
                            public_write,
                            tracked,
                            prepared,
                            writer_key,
                        );
                        units.push(WriteTxnUnit::PublicTracked(tracked_plan));
                    }
                    PublicWriteExecutionPartition::Untracked(untracked) => {
                        units.push(WriteTxnUnit::PublicUntracked(PublicUntrackedWriteTxnPlan {
                            execution: untracked.clone(),
                            filesystem_state: prepared.intent.filesystem_state.clone(),
                            functions: prepared.functions.clone(),
                            settings: prepared.settings,
                            sequence_start: prepared.sequence_start,
                            writer_key: writer_key.map(str::to_string),
                        }));
                    }
                }
            }
        }
    } else if !prepared.plan.requirements.read_only_query {
        units.push(WriteTxnUnit::Internal(InternalWriteTxnPlan {
            plan: prepared.plan.clone(),
            filesystem_state: prepared.intent.filesystem_state.clone(),
            functions: prepared.functions.clone(),
            settings: prepared.settings,
            sequence_start: prepared.sequence_start,
            writer_key: writer_key.map(str::to_string),
        }));
    }

    if units.is_empty() {
        None
    } else {
        let mut plan = WriteTxnPlan { units };
        plan.coalesce_filesystem_tracked_units();
        Some(plan)
    }
}

fn collect_semantic_overlay_from_plan(
    plan: &WriteTxnPlan,
    overlay: &mut PendingSemanticOverlay,
) -> Result<bool, LixError> {
    if plan.units.is_empty() {
        return Ok(false);
    }

    for unit in &plan.units {
        let unit_supported = match unit {
            WriteTxnUnit::PublicTracked(tracked) => {
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
            WriteTxnUnit::PublicUntracked(untracked) => {
                untracked.filesystem_state.files.is_empty()
                    && collect_semantic_overlay_from_planned_rows(
                        untracked.execution.intended_post_state.iter(),
                        PendingSemanticStorage::Untracked,
                        overlay,
                    )?
            }
            WriteTxnUnit::Internal(internal) => {
                internal.filesystem_state.files.is_empty()
                    && internal.plan.preprocess.internal_state.is_none()
                    && collect_semantic_overlay_from_mutation_rows(
                        &internal.plan.preprocess.mutations,
                        overlay,
                    )?
                    && internal.plan.preprocess.update_validations.is_empty()
            }
        };

        if !unit_supported {
            return Ok(false);
        }
    }

    Ok(true)
}

fn collect_filesystem_overlay_from_plan(
    plan: &WriteTxnPlan,
    overlay: &mut PendingFilesystemOverlay,
) -> bool {
    if plan.units.is_empty() {
        return false;
    }

    let mut saw_entry = false;
    for unit in &plan.units {
        let unit_supported = match unit {
            WriteTxnUnit::PublicTracked(tracked) => {
                collect_filesystem_overlay_from_tracked_plan(tracked, overlay, &mut saw_entry)
            }
            WriteTxnUnit::PublicUntracked(_) | WriteTxnUnit::Internal(_) => false,
        };
        if !unit_supported {
            return false;
        }
    }

    saw_entry
}

fn collect_filesystem_overlay_from_tracked_plan(
    tracked: &TrackedWriteTxnPlan,
    overlay: &mut PendingFilesystemOverlay,
    saw_entry: &mut bool,
) -> bool {
    if !matches!(
        tracked
            .public_write
            .planned_write
            .command
            .target
            .descriptor
            .public_name
            .as_str(),
        "lix_file" | "lix_file_by_version"
    ) {
        return false;
    }

    for public_write in &tracked.public_writes {
        let Some(resolved) = public_write.planned_write.resolved_write_plan.as_ref() else {
            return false;
        };
        for partition in &resolved.partitions {
            if partition.execution_mode != crate::sql::public::planner::ir::WriteMode::Tracked {
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
    rows: impl Iterator<Item = &'a crate::sql::public::planner::ir::PlannedStateRow>,
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
    public_write: &crate::sql::public::runtime::PreparedPublicWrite,
    overlay: &mut PendingSemanticOverlay,
) -> Result<bool, LixError> {
    let Some(resolved) = public_write.planned_write.resolved_write_plan.as_ref() else {
        return Ok(false);
    };
    let skip_file_descriptor_rows = matches!(
        public_write
            .planned_write
            .command
            .target
            .descriptor
            .public_name
            .as_str(),
        "lix_file" | "lix_file_by_version"
    );
    let mut saw_row = false;
    for partition in &resolved.partitions {
        let storage = match partition.execution_mode {
            crate::sql::public::planner::ir::WriteMode::Tracked => PendingSemanticStorage::Tracked,
            crate::sql::public::planner::ir::WriteMode::Untracked => {
                PendingSemanticStorage::Untracked
            }
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
    rows: impl Iterator<Item = &'a crate::sql::public::planner::ir::PlannedStateRow>,
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
    rows: &[crate::sql::execution::contracts::planned_statement::MutationRow],
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

fn try_merge_filesystem_tracked_plans(
    current: &mut TrackedWriteTxnPlan,
    next: &TrackedWriteTxnPlan,
) -> bool {
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
    current.execution.domain_change_batch = merge_optional_domain_change_batches(
        current.execution.domain_change_batch.as_ref(),
        next.execution.domain_change_batch.as_ref(),
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
    current: &TrackedWriteTxnPlan,
    next: &TrackedWriteTxnPlan,
) -> bool {
    if !tracked_plan_is_coalescible_filesystem(current)
        || !tracked_plan_is_coalescible_filesystem(next)
    {
        return false;
    }
    current
        .public_write
        .planned_write
        .command
        .target
        .descriptor
        .public_name
        == next
            .public_write
            .planned_write
            .command
            .target
            .descriptor
            .public_name
        && current.execution.create_preconditions.write_lane
            == next.execution.create_preconditions.write_lane
        && create_commit_expected_head_compatible(
            &current.execution.create_preconditions.expected_head,
            &next.execution.create_preconditions.expected_head,
        )
        && current.writer_key == next.writer_key
        && tracked_plan_entity_targets_disjoint(current, next)
}

fn tracked_plan_is_coalescible_filesystem(plan: &TrackedWriteTxnPlan) -> bool {
    matches!(
        plan.public_write
            .planned_write
            .command
            .target
            .descriptor
            .public_name
            .as_str(),
        "lix_file" | "lix_file_by_version"
    )
}

fn create_commit_expected_head_compatible(
    left: &crate::state::commit::CreateCommitExpectedHead,
    right: &crate::state::commit::CreateCommitExpectedHead,
) -> bool {
    match (left, right) {
        (
            crate::state::commit::CreateCommitExpectedHead::CurrentHead,
            crate::state::commit::CreateCommitExpectedHead::CurrentHead,
        ) => true,
        (
            crate::state::commit::CreateCommitExpectedHead::CommitId(left),
            crate::state::commit::CreateCommitExpectedHead::CommitId(right),
        ) => left == right,
        (
            crate::state::commit::CreateCommitExpectedHead::CreateIfMissing,
            crate::state::commit::CreateCommitExpectedHead::CreateIfMissing,
        ) => true,
        _ => false,
    }
}

fn tracked_plan_entity_targets_disjoint(
    left: &TrackedWriteTxnPlan,
    right: &TrackedWriteTxnPlan,
) -> bool {
    let left_targets = tracked_plan_entity_targets(left);
    let right_targets = tracked_plan_entity_targets(right);
    left_targets.is_disjoint(&right_targets)
}

fn merge_plan_effects(current: &mut PlanEffects, next: &PlanEffects) {
    current
        .state_commit_stream_changes
        .extend(next.state_commit_stream_changes.clone());
    if next.next_active_version_id.is_some() {
        current.next_active_version_id = next.next_active_version_id.clone();
    }
    current
        .file_cache_refresh_targets
        .extend(next.file_cache_refresh_targets.clone());
}

fn tracked_plan_entity_targets(plan: &TrackedWriteTxnPlan) -> BTreeSet<(String, String, String)> {
    let mut targets = BTreeSet::new();
    if let Some(batch) = plan.execution.domain_change_batch.as_ref() {
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
            || !matches!(
                file.metadata_patch,
                crate::sql::public::planner::ir::OptionalTextPatch::Unchanged
            )
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

fn merge_optional_domain_change_batches(
    left: Option<&DomainChangeBatch>,
    right: Option<&DomainChangeBatch>,
) -> Option<DomainChangeBatch> {
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
            Some(DomainChangeBatch {
                changes: merged.into_values().collect(),
                write_lane: left.write_lane.clone(),
                writer_key: left.writer_key.clone().or_else(|| right.writer_key.clone()),
                semantic_effects,
            })
        }
    }
}
