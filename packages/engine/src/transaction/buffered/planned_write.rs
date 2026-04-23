use std::collections::{BTreeMap, BTreeSet};

use crate::backend::PreparedBatch;
use crate::canonical::CanonicalChangeWrite;
use crate::catalog::{
    builtin_catalog_compiler_facade, CatalogCompilerApi, CatalogWriteTargetKind,
    FilesystemRelationKind, ResolvedRelation,
};
use crate::functions::{LixFunctionProvider, SharedFunctionProvider, SystemFunctionProvider};
use crate::live_state::{SchemaRegistration, SchemaRegistrationSet};
use crate::sql::{
    coalesce_live_table_requirements, ChangeBatch, ExpectedHead, MutationRow, OptionalTextPatch,
    PlanEffects, PlannedStateRow, ResultContract, SchemaLiveTableRequirement, WriteMode,
};
use crate::transaction::filesystem::runtime::{
    filesystem_transaction_state_has_binary_payloads, merge_filesystem_transaction_state,
    FilesystemTransactionFileState, FilesystemTransactionState,
};
use crate::transaction::filesystem::state::filesystem_transaction_state_from_planned;
use crate::transaction::overlay::PendingSemanticRow;
use crate::transaction::{
    PendingWriteOverlay, PreparedDirectWriteArtifact, PreparedPublicWrite,
    PreparedPublicWriteExecution, PreparedPublicWritePlanArtifact, PreparedWriteFunctionBindings,
    PreparedWriteStatement,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::LixError;

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";

#[derive(Clone)]
pub(crate) struct PublicWriteTxnUnit {
    pub(crate) public_writes: Vec<PreparedPublicWrite>,
    pub(crate) public_write: PreparedPublicWrite,
    pub(crate) execution: PreparedPublicWriteExecution,
    pub(crate) canonical_changes: Vec<CanonicalChangeWrite>,
    pub(crate) filesystem_state: FilesystemTransactionState,
    pub(crate) function_bindings: PreparedWriteFunctionBindings,
    pub(crate) origin_key: Option<String>,
}

impl PublicWriteTxnUnit {
    pub(crate) fn is_commit_member_write(&self) -> bool {
        self.execution.execution_mode == WriteMode::Tracked
    }

    pub(crate) fn is_immediate_write(&self) -> bool {
        self.execution.execution_mode == WriteMode::Untracked
    }

    pub(crate) fn has_compiler_only_filesystem_changes(&self) -> bool {
        self.is_commit_member_write()
            && self.execution.change_batch.is_none()
            && !self.filesystem_state.files.is_empty()
    }

    pub(crate) fn is_merged_transaction_plan(&self) -> bool {
        self.public_writes.len() > 1
    }
}

fn build_public_write_txn_unit(
    public_write: &PreparedPublicWrite,
    execution: &PreparedPublicWriteExecution,
    canonical_changes: Vec<CanonicalChangeWrite>,
    filesystem_state: &FilesystemTransactionState,
    function_bindings: &PreparedWriteFunctionBindings,
) -> PublicWriteTxnUnit {
    PublicWriteTxnUnit {
        public_writes: vec![public_write.clone()],
        public_write: public_write.clone(),
        execution: execution.clone(),
        canonical_changes,
        filesystem_state: filesystem_state.clone(),
        function_bindings: function_bindings.clone(),
        origin_key: public_write.contract.origin_key.clone(),
    }
}

#[derive(Clone)]
pub(crate) struct PlannedDirectWriteUnit {
    pub(crate) execution: PreparedDirectWriteArtifact,
    pub(crate) result_contract: ResultContract,
    pub(crate) function_bindings: PreparedWriteFunctionBindings,
}

#[derive(Clone)]
pub(crate) enum TransactionWriteUnit {
    Public(PublicWriteTxnUnit),
    Direct(PlannedDirectWriteUnit),
}

#[derive(Clone, Default)]
pub(crate) struct PlannedWritePlan {
    pub(crate) units: Vec<TransactionWriteUnit>,
}

impl PlannedWritePlan {
    pub(crate) fn extend(&mut self, other: PlannedWritePlan) {
        self.units.extend(other.units);
        self.coalesce_filesystem_commit_units();
    }

    pub(crate) fn coalesce_filesystem_commit_units(&mut self) {
        let mut coalesced = Vec::with_capacity(self.units.len());
        for unit in std::mem::take(&mut self.units) {
            match unit {
                TransactionWriteUnit::Public(next)
                    if coalesced
                        .last_mut()
                        .and_then(|current| match current {
                            TransactionWriteUnit::Public(current) => Some(current),
                            _ => None,
                        })
                        .is_some_and(|current| {
                            try_merge_filesystem_commit_plans(current, &next)
                        }) => {}
                other => coalesced.push(other),
            }
        }
        self.units = coalesced;
    }
}

#[derive(Clone)]
pub(crate) struct TransactionWriteDelta {
    materialization_plan: PlannedWritePlan,
    schema_registrations: SchemaRegistrationSet,
    registered_schema_overlay: Option<PendingRegisteredSchemaOverlay>,
    semantic_overlay: Option<PendingSemanticOverlay>,
    filesystem_overlay: Option<PendingFilesystemOverlay>,
}

impl TransactionWriteDelta {
    pub(crate) fn from_materialization_plan(
        materialization_plan: PlannedWritePlan,
    ) -> Result<Self, LixError> {
        let schema_registrations =
            schema_registrations_for_planned_write_plan(&materialization_plan);
        let semantic_overlay =
            pending_semantic_overlay_for_planned_write_plan(&materialization_plan)?;
        let filesystem_overlay =
            pending_filesystem_overlay_for_planned_write_plan(&materialization_plan);
        let registered_schema_overlay = semantic_overlay
            .as_ref()
            .and_then(PendingSemanticOverlay::registered_schema_overlay);
        Ok(Self {
            materialization_plan,
            schema_registrations,
            registered_schema_overlay,
            semantic_overlay,
            filesystem_overlay,
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
    pub(crate) fn pending_write_overlay(&self) -> Option<PendingWriteOverlay> {
        PendingWriteOverlay::new(
            self.registered_schema_overlay(),
            self.semantic_overlay(),
            self.filesystem_overlay(),
        )
    }

    fn supports_registered_schema_overlay(&self) -> bool {
        self.registered_schema_overlay.is_some()
    }

    pub(crate) fn extend(&mut self, incoming: TransactionWriteDelta) -> Result<(), LixError> {
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
        self.registered_schema_overlay = self
            .semantic_overlay
            .as_ref()
            .and_then(PendingSemanticOverlay::registered_schema_overlay);
        Ok(())
    }
}

#[derive(Clone, Default)]
pub(crate) struct BufferedWriteJournal {
    staged_delta: Option<TransactionWriteDelta>,
    continuation_safe: bool,
}

impl BufferedWriteJournal {
    pub(crate) fn is_empty(&self) -> bool {
        self.staged_delta.is_none()
    }

    pub(crate) fn can_stage_delta(&self, delta: &TransactionWriteDelta) -> Result<bool, LixError> {
        let plan = delta.materialization_plan();
        let current_supports_registered_schema_overlay = self
            .staged_delta
            .as_ref()
            .is_some_and(TransactionWriteDelta::supports_registered_schema_overlay);
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

    pub(crate) fn stage_delta(&mut self, incoming: TransactionWriteDelta) -> Result<(), LixError> {
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

    pub(crate) fn take_staged_delta(&mut self) -> Option<TransactionWriteDelta> {
        let delta = self.staged_delta.take()?;
        self.continuation_safe = false;
        Some(delta)
    }

    pub(crate) fn pending_write_overlay(&self) -> Result<Option<PendingWriteOverlay>, LixError> {
        Ok(self
            .staged_delta
            .as_ref()
            .and_then(TransactionWriteDelta::pending_write_overlay))
    }

    fn current_materialization_plan(&self) -> Option<&PlannedWritePlan> {
        self.staged_delta
            .as_ref()
            .map(TransactionWriteDelta::materialization_plan)
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
    untracked: bool,
    schema_key: String,
    entity_id: String,
    file_id: Option<String>,
    version_id: String,
    plugin_key: Option<String>,
    schema_version: String,
}

#[derive(Clone, Default)]
pub(crate) struct PendingFilesystemOverlay {
    directory_rows: BTreeMap<PendingSemanticRowIdentity, PendingSemanticRow>,
    files: BTreeMap<(String, String), FilesystemTransactionFileState>,
}

impl PendingFilesystemOverlay {
    pub(crate) fn visible_directory_rows<'a>(
        &'a self,
        untracked: bool,
        schema_key: &'a str,
    ) -> impl Iterator<Item = &'a PendingSemanticRow> {
        self.directory_rows
            .values()
            .filter(move |row| row.untracked == untracked && row.schema_key == schema_key)
    }

    pub(crate) fn visible_files(&self) -> impl Iterator<Item = &FilesystemTransactionFileState> {
        self.files.values()
    }
}

impl PendingSemanticOverlay {
    #[allow(dead_code)]
    pub(crate) fn visible_all_rows(&self) -> impl Iterator<Item = &PendingSemanticRow> {
        self.rows.values()
    }

    pub(crate) fn visible_rows<'a>(
        &'a self,
        untracked: bool,
        schema_key: &'a str,
    ) -> impl Iterator<Item = &'a PendingSemanticRow> {
        self.rows
            .values()
            .filter(move |row| row.untracked == untracked && row.schema_key == schema_key)
    }

    pub(crate) fn registered_schema_overlay(&self) -> Option<PendingRegisteredSchemaOverlay> {
        let mut overlay = PendingRegisteredSchemaOverlay::default();
        for row in self.visible_rows(false, REGISTERED_SCHEMA_KEY) {
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
    prepared: &PreparedWriteStatement,
    function_bindings: &PreparedWriteFunctionBindings,
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
        if let PreparedPublicWritePlanArtifact::Materialize(materialization) =
            &public_write.execution
        {
            for partition in &materialization.partitions {
                match partition.execution_mode {
                    WriteMode::Tracked => {
                        let unit = build_public_write_txn_unit(
                            public_write,
                            partition,
                            Vec::new(),
                            &filesystem_state,
                            function_bindings,
                        );
                        units.push(TransactionWriteUnit::Public(unit));
                    }
                    WriteMode::Untracked => {
                        let canonical_changes = build_untracked_canonical_changes(
                            &partition.intended_post_state,
                            function_bindings.provider().clone(),
                        )
                        .ok()?;
                        units.push(TransactionWriteUnit::Public(build_public_write_txn_unit(
                            public_write,
                            partition,
                            canonical_changes,
                            &filesystem_state,
                            function_bindings,
                        )));
                    }
                }
            }
        }
    } else if let Some(direct_execution) = prepared.direct_write() {
        if direct_execution.read_only_query {
            return None;
        }
        units.push(TransactionWriteUnit::Direct(PlannedDirectWriteUnit {
            execution: direct_execution.clone(),
            result_contract: prepared.result_contract,
            function_bindings: function_bindings.clone(),
        }));
    }

    if units.is_empty() {
        None
    } else {
        let mut plan = PlannedWritePlan { units };
        plan.coalesce_filesystem_commit_units();
        Some(plan)
    }
}

pub(crate) fn build_transaction_write_delta(
    prepared: &PreparedWriteStatement,
    function_bindings: &PreparedWriteFunctionBindings,
) -> Result<Option<TransactionWriteDelta>, LixError> {
    build_planned_write_plan(prepared, function_bindings)
        .map(TransactionWriteDelta::from_materialization_plan)
        .transpose()
}

#[allow(dead_code)]
pub(crate) fn build_direct_mutation_transaction_write_delta(
    mutations: Vec<MutationRow>,
    origin_key: Option<String>,
) -> Result<TransactionWriteDelta, LixError> {
    let function_bindings = PreparedWriteFunctionBindings::new(
        false,
        SharedFunctionProvider::new(Box::new(SystemFunctionProvider)),
    );
    let live_table_requirements = mutations
        .iter()
        .map(|row| SchemaLiveTableRequirement {
            schema_key: row.schema_key.clone(),
            schema_definition: None,
        })
        .collect::<Vec<_>>();
    let direct = PreparedDirectWriteArtifact {
        prepared_batch: PreparedBatch { steps: Vec::new() },
        live_table_requirements,
        mutations,
        has_update_validations: false,
        should_refresh_file_cache: false,
        read_only_query: false,
        filesystem_state: Default::default(),
        effects: PlanEffects::default(),
        origin_key,
    };
    TransactionWriteDelta::from_materialization_plan(PlannedWritePlan {
        units: vec![TransactionWriteUnit::Direct(PlannedDirectWriteUnit {
            execution: direct,
            result_contract: ResultContract::DmlNoReturning,
            function_bindings,
        })],
    })
}

fn schema_registrations_for_planned_write_plan(plan: &PlannedWritePlan) -> SchemaRegistrationSet {
    let mut registrations = SchemaRegistrationSet::default();
    for unit in &plan.units {
        match unit {
            TransactionWriteUnit::Public(public) if public.is_commit_member_write() => {
                for requirement in coalesce_live_table_requirements(
                    &public.execution.schema_live_table_requirements,
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
            TransactionWriteUnit::Public(public) => {
                for row in &public.execution.intended_post_state {
                    registrations.insert(row.schema_key.clone());
                }
            }
            TransactionWriteUnit::Direct(internal) => {
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

pub(crate) fn planned_write_plan_is_independent_filesystem(plan: &PlannedWritePlan) -> bool {
    !plan.units.is_empty()
        && plan.units.iter().all(|unit| match unit {
            TransactionWriteUnit::Public(public) => commit_plan_is_coalescible_filesystem(public),
            TransactionWriteUnit::Direct(_) => false,
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
        let TransactionWriteUnit::Public(left_commit) = left_unit else {
            return false;
        };
        right.units.iter().all(|right_unit| {
            let TransactionWriteUnit::Public(right_commit) = right_unit else {
                return false;
            };
            filesystem_commit_plans_are_buffer_compatible(left_commit, right_commit)
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

fn collect_semantic_overlay_from_planned_write_plan(
    plan: &PlannedWritePlan,
    overlay: &mut PendingSemanticOverlay,
) -> Result<bool, LixError> {
    if plan.units.is_empty() {
        return Ok(false);
    }

    for unit in &plan.units {
        let unit_supported = match unit {
            TransactionWriteUnit::Public(public) if public.is_commit_member_write() => {
                !filesystem_transaction_state_has_binary_payloads(&public.filesystem_state)
                    && public
                        .public_writes
                        .iter()
                        .try_fold(true, |supported, public_write| {
                            if !supported {
                                return Ok(false);
                            }
                            collect_semantic_overlay_from_public_write(public_write, overlay)
                        })?
            }
            TransactionWriteUnit::Public(public) => {
                let planned_rows = public
                    .execution
                    .intended_post_state
                    .iter()
                    .collect::<Vec<_>>();
                public.filesystem_state.files.is_empty()
                    && collect_semantic_overlay_from_planned_rows(
                        &planned_rows,
                        true,
                        &public.canonical_changes,
                        overlay,
                    )?
            }
            TransactionWriteUnit::Direct(internal) => {
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
            TransactionWriteUnit::Public(public) => {
                collect_filesystem_overlay_from_commit_plan(public, overlay, &mut saw_entry)
            }
            TransactionWriteUnit::Direct(_) => false,
        };
        if !unit_supported {
            return false;
        }
    }

    saw_entry
}

fn collect_filesystem_overlay_from_commit_plan(
    commit: &PublicWriteTxnUnit,
    overlay: &mut PendingFilesystemOverlay,
    saw_entry: &mut bool,
) -> bool {
    if !commit.is_commit_member_write() {
        return false;
    }
    if !is_catalog_filesystem_file_surface(&commit.public_write.contract.target) {
        return false;
    }

    for public_write in &commit.public_writes {
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

    let state = commit.filesystem_state.clone();
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
            Some(crate::Value::Text(value)) => Some(value.clone()),
            Some(crate::Value::Null) | None => None,
            _ => return false,
        };
        let version_id = match row.version_id.as_deref() {
            Some(value) => value.to_string(),
            None => return false,
        };
        let plugin_key = match row.values.get("plugin_key") {
            Some(crate::Value::Text(value)) => Some(value.clone()),
            Some(crate::Value::Null) | None => None,
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
                untracked: false,
                schema_key: row.schema_key.clone(),
                entity_id: row.entity_id.clone(),
                file_id: file_id.clone(),
                version_id: version_id.clone(),
                plugin_key: plugin_key.clone(),
                schema_version: schema_version.clone(),
            },
            PendingSemanticRow {
                untracked: false,
                entity_id: row.entity_id.clone(),
                schema_key: row.schema_key.clone(),
                schema_version,
                file_id,
                version_id,
                plugin_key,
                change_id: None,
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
    public_write: &PreparedPublicWrite,
    overlay: &mut PendingSemanticOverlay,
) -> Result<bool, LixError> {
    let Some(resolved) = public_write.contract.resolved_write_plan.as_ref() else {
        return Ok(false);
    };
    let skip_file_descriptor_rows =
        is_catalog_filesystem_file_surface(&public_write.contract.target);
    let mut saw_row = false;
    for partition in &resolved.partitions {
        let untracked = matches!(partition.execution_mode, WriteMode::Untracked);
        let planned_rows = partition
            .intended_post_state
            .iter()
            .filter(|row| !(skip_file_descriptor_rows && row.schema_key == "lix_file_descriptor"))
            .collect::<Vec<_>>();
        saw_row |=
            collect_semantic_overlay_from_planned_rows(&planned_rows, untracked, &[], overlay)?;
    }
    Ok(saw_row)
}

fn collect_semantic_overlay_from_planned_rows(
    rows: &[&PlannedStateRow],
    default_untracked: bool,
    canonical_changes: &[CanonicalChangeWrite],
    overlay: &mut PendingSemanticOverlay,
) -> Result<bool, LixError> {
    if rows.is_empty() {
        return Ok(false);
    }

    if !canonical_changes.is_empty() && rows.len() != canonical_changes.len() {
        return Ok(false);
    }

    for (index, row) in rows.iter().enumerate() {
        let change = canonical_changes.get(index);
        if !collect_semantic_overlay_row(
            row,
            default_untracked,
            change.map(|change| change.id.as_str()),
            overlay,
        )? {
            return Ok(false);
        }
    }

    Ok(true)
}

fn collect_semantic_overlay_row(
    row: &PlannedStateRow,
    untracked: bool,
    change_id: Option<&str>,
    overlay: &mut PendingSemanticOverlay,
) -> Result<bool, LixError> {
    let file_id = match row.values.get("file_id") {
        Some(crate::Value::Text(value)) => Some(value.clone()),
        Some(crate::Value::Null) | None => None,
        _ => return Ok(false),
    };
    let version_id = match row.version_id.as_deref() {
        Some(value) => value.to_string(),
        None => return Ok(false),
    };
    let plugin_key = match row.values.get("plugin_key") {
        Some(crate::Value::Text(value)) => Some(value.clone()),
        Some(crate::Value::Null) | None => None,
        _ => return Ok(false),
    };
    let schema_version = match row.values.get("schema_version") {
        Some(crate::Value::Text(value)) => value.clone(),
        _ => return Ok(false),
    };
    let snapshot_content = if row.tombstone {
        None
    } else {
        planned_json_text(row, "snapshot_content")?
    };
    overlay.rows.insert(
        PendingSemanticRowIdentity {
            untracked,
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: file_id.clone(),
            version_id: version_id.clone(),
            plugin_key: plugin_key.clone(),
            schema_version: schema_version.clone(),
        },
        PendingSemanticRow {
            untracked,
            entity_id: row.entity_id.clone(),
            schema_key: row.schema_key.clone(),
            schema_version,
            file_id,
            version_id,
            plugin_key,
            change_id: change_id.map(ToOwned::to_owned),
            snapshot_content,
            metadata: row.values.get("metadata").and_then(|value| match value {
                crate::Value::Text(text) => Some(text.clone()),
                _ => None,
            }),
            tombstone: row.tombstone,
        },
    );

    Ok(true)
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
        overlay.rows.insert(
            PendingSemanticRowIdentity {
                untracked: row.untracked,
                schema_key: row.schema_key.clone(),
                entity_id: row.entity_id.clone(),
                file_id: row.file_id.clone(),
                version_id: row.version_id.clone(),
                plugin_key: row.plugin_key.clone(),
                schema_version: row.schema_version.clone(),
            },
            PendingSemanticRow {
                untracked: row.untracked,
                entity_id: row.entity_id.clone(),
                schema_key: row.schema_key.clone(),
                schema_version: row.schema_version.clone(),
                file_id: row.file_id.clone(),
                version_id: row.version_id.clone(),
                plugin_key: row.plugin_key.clone(),
                change_id: None,
                snapshot_content,
                metadata: row.metadata.clone(),
                tombstone: false,
            },
        );
    }

    Ok(true)
}

fn try_merge_filesystem_commit_plans(
    current: &mut PublicWriteTxnUnit,
    next: &PublicWriteTxnUnit,
) -> bool {
    if !filesystem_commit_plans_are_buffer_compatible(current, next) {
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

fn filesystem_commit_plans_are_buffer_compatible(
    current: &PublicWriteTxnUnit,
    next: &PublicWriteTxnUnit,
) -> bool {
    if !commit_plan_is_coalescible_filesystem(current)
        || !commit_plan_is_coalescible_filesystem(next)
    {
        return false;
    }
    current.public_write.contract.target.descriptor.public_name
        == next.public_write.contract.target.descriptor.public_name
        && current
            .execution
            .create_preconditions
            .as_ref()
            .map(|preconditions| &preconditions.write_lane)
            == next
                .execution
                .create_preconditions
                .as_ref()
                .map(|preconditions| &preconditions.write_lane)
        && create_commit_expected_head_compatible(
            current
                .execution
                .create_preconditions
                .as_ref()
                .map(|preconditions| &preconditions.expected_head),
            next.execution
                .create_preconditions
                .as_ref()
                .map(|preconditions| &preconditions.expected_head),
        )
        && commit_plan_entity_targets_disjoint(current, next)
        && current.origin_key == next.origin_key
}

fn commit_plan_is_coalescible_filesystem(plan: &PublicWriteTxnUnit) -> bool {
    plan.is_commit_member_write()
        && is_catalog_filesystem_file_surface(&plan.public_write.contract.target)
}

fn is_catalog_filesystem_file_surface(target: &ResolvedRelation) -> bool {
    builtin_catalog_compiler_facade()
        .write_surface_semantics(target)
        .ok()
        .flatten()
        .is_some_and(|semantics| {
            semantics.target_kind == CatalogWriteTargetKind::Filesystem
                && semantics.filesystem_kind == Some(FilesystemRelationKind::File)
        })
}

fn create_commit_expected_head_compatible(
    left: Option<&ExpectedHead>,
    right: Option<&ExpectedHead>,
) -> bool {
    matches!(
        (left, right),
        (
            Some(ExpectedHead::CurrentHead),
            Some(ExpectedHead::CurrentHead)
        )
    )
}

fn commit_plan_entity_targets_disjoint(
    left: &PublicWriteTxnUnit,
    right: &PublicWriteTxnUnit,
) -> bool {
    let left_targets = commit_plan_entity_targets(left);
    let right_targets = commit_plan_entity_targets(right);
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

fn commit_plan_entity_targets(plan: &PublicWriteTxnUnit) -> BTreeSet<(String, String, String)> {
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

fn build_untracked_canonical_changes(
    rows: &[PlannedStateRow],
    mut functions: impl LixFunctionProvider,
) -> Result<Vec<CanonicalChangeWrite>, LixError> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let created_at = functions.timestamp();
    rows.iter()
        .map(|row| {
            let schema_key = row.schema_key.clone();
            Ok(CanonicalChangeWrite {
                id: functions.uuid_v7(),
                entity_id: row.entity_id.clone().try_into().map_err(|_| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "public untracked planning produced invalid entity_id '{}'",
                            row.entity_id
                        ),
                    )
                })?,
                schema_key: schema_key.clone().try_into().map_err(|_| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "public untracked planning produced invalid schema_key '{}'",
                            schema_key
                        ),
                    )
                })?,
                schema_version: required_planned_text(row, "schema_version")?
                    .to_string()
                    .try_into()
                    .map_err(|_| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!(
                                "public untracked planning produced invalid schema_version for '{}'",
                                schema_key
                            ),
                        )
                    })?,
                file_id: optional_planned_text(row, "file_id")
                    .map(str::to_string)
                    .map(TryInto::try_into)
                    .transpose()
                    .map_err(|_| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!(
                                "public untracked planning produced invalid file_id for '{}'",
                                schema_key
                            ),
                        )
                    })?,
                plugin_key: optional_planned_text(row, "plugin_key")
                    .map(str::to_string)
                    .map(TryInto::try_into)
                    .transpose()
                    .map_err(|_| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!(
                                "public untracked planning produced invalid plugin_key for '{}'",
                                schema_key
                            ),
                        )
                    })?,
                snapshot_content: (!row.tombstone)
                    .then(|| planned_json_text(row, "snapshot_content"))
                    .transpose()?
                    .flatten()
                    .map(crate::canonical::CanonicalJson::from_text)
                    .transpose()
                    .map_err(|error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!(
                                "public untracked planning produced invalid snapshot_content for '{}': {}",
                                schema_key, error.description
                            ),
                        )
                    })?,
                metadata: optional_planned_text(row, "metadata")
                    .map(crate::canonical::CanonicalJson::from_text)
                    .transpose()
                    .map_err(|error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!(
                                "public untracked planning produced invalid metadata for '{}': {}",
                                schema_key, error.description
                            ),
                        )
                    })?,
                created_at: created_at.clone(),
            })
        })
        .collect()
}

fn required_planned_text<'a>(row: &'a PlannedStateRow, key: &str) -> Result<&'a str, LixError> {
    optional_planned_text(row, key).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "public untracked planning requires text field '{}' for schema '{}'",
                key, row.schema_key
            ),
        )
    })
}

fn optional_planned_text<'a>(row: &'a PlannedStateRow, key: &str) -> Option<&'a str> {
    match row.values.get(key) {
        Some(crate::Value::Text(value)) => Some(value.as_str()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{collect_semantic_overlay_from_planned_rows, PendingSemanticOverlay};
    use crate::canonical::{CanonicalChangeWrite, CanonicalJson};
    use crate::sql::PlannedStateRow;
    use crate::Value;
    use std::collections::BTreeMap;

    fn untracked_planned_row() -> PlannedStateRow {
        PlannedStateRow {
            entity_id: "pending-untracked".to_string(),
            schema_key: "lix_key_value".to_string(),
            version_id: Some("main".to_string()),
            values: BTreeMap::from([
                ("file_id".to_string(), Value::Null),
                ("plugin_key".to_string(), Value::Null),
                ("schema_version".to_string(), Value::Text("1".to_string())),
                (
                    "snapshot_content".to_string(),
                    Value::Text("{\"key\":\"pending-untracked\",\"value\":\"after\"}".to_string()),
                ),
            ]),
            origin_key: None,
            tombstone: false,
        }
    }

    fn untracked_canonical_change() -> CanonicalChangeWrite {
        CanonicalChangeWrite {
            id: "change-untracked-1".to_string(),
            entity_id: "pending-untracked".try_into().expect("valid entity id"),
            schema_key: "lix_key_value".try_into().expect("valid schema key"),
            schema_version: "1".try_into().expect("valid schema version"),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(
                CanonicalJson::from_text("{\"key\":\"pending-untracked\",\"value\":\"after\"}")
                    .expect("valid canonical json"),
            ),
            metadata: None,
            created_at: "2026-04-14T00:00:00Z".to_string(),
        }
    }

    fn tracked_canonical_change() -> CanonicalChangeWrite {
        CanonicalChangeWrite {
            id: "change-tracked-1".to_string(),
            ..untracked_canonical_change()
        }
    }

    #[test]
    fn pending_untracked_overlay_rows_preserve_planned_change_ids() {
        let rows = vec![untracked_planned_row()];
        let planned_rows = rows.iter().collect::<Vec<_>>();
        let canonical_changes = vec![untracked_canonical_change()];
        let mut overlay = PendingSemanticOverlay::default();

        let collected = collect_semantic_overlay_from_planned_rows(
            &planned_rows,
            true,
            &canonical_changes,
            &mut overlay,
        )
        .expect("overlay collection should succeed");

        assert!(collected);
        let pending = overlay
            .visible_rows(true, "lix_key_value")
            .next()
            .expect("expected pending untracked row");
        assert_eq!(pending.change_id.as_deref(), Some("change-untracked-1"));
    }

    #[test]
    fn pending_overlay_uses_canonical_storage_and_change_id_when_changes_are_available() {
        let rows = vec![untracked_planned_row()];
        let planned_rows = rows.iter().collect::<Vec<_>>();
        let canonical_changes = vec![tracked_canonical_change()];
        let mut overlay = PendingSemanticOverlay::default();

        let collected = collect_semantic_overlay_from_planned_rows(
            &planned_rows,
            true,
            &canonical_changes,
            &mut overlay,
        )
        .expect("overlay collection should succeed");

        assert!(collected);
        let pending = overlay
            .visible_rows(true, "lix_key_value")
            .next()
            .expect("expected pending untracked row");
        assert_eq!(pending.change_id.as_deref(), Some("change-tracked-1"));
    }
}

fn planned_json_text(row: &PlannedStateRow, key: &str) -> Result<Option<String>, LixError> {
    match row.values.get(key) {
        Some(crate::Value::Text(value)) => Ok(Some(value.clone())),
        Some(crate::Value::Json(value)) => {
            serde_json::to_string(value).map(Some).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "failed to serialize planned JSON field '{}' for schema '{}': {}",
                        key, row.schema_key, error
                    ),
                )
            })
        }
        Some(crate::Value::Null) | None => Ok(None),
        Some(other) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "public untracked planning expected text/json field '{}' for schema '{}', got {:?}",
                key, row.schema_key, other
            ),
        )),
    }
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
                origin_key: left.origin_key.clone().or_else(|| right.origin_key.clone()),
                semantic_effects,
            })
        }
    }
}
