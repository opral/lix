use crate::sql2::catalog::{
    DefaultScopeSemantics, SurfaceBinding, SurfaceCapability, SurfaceFamily, SurfaceVariant,
};
use crate::sql2::core::contracts::ExecutionContext;
use crate::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VersionScope {
    ActiveVersion,
    ExplicitVersion,
    History,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EntityProjectionSpec {
    pub(crate) schema_key: String,
    pub(crate) visible_columns: Vec<String>,
    pub(crate) hide_version_columns_by_default: bool,
}

impl EntityProjectionSpec {
    pub(crate) fn from_surface_binding(binding: &SurfaceBinding) -> Option<Self> {
        if binding.descriptor.surface_family != SurfaceFamily::Entity {
            return None;
        }

        Some(Self {
            schema_key: binding.implicit_overrides.fixed_schema_key.clone()?,
            visible_columns: binding.exposed_columns.clone(),
            hide_version_columns_by_default: !binding.implicit_overrides.expose_version_id,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalStateScan {
    pub(crate) binding: SurfaceBinding,
    pub(crate) version_scope: VersionScope,
    pub(crate) expose_version_id: bool,
    pub(crate) include_tombstones: bool,
    pub(crate) entity_projection: Option<EntityProjectionSpec>,
}

impl CanonicalStateScan {
    pub(crate) fn from_surface_binding(binding: SurfaceBinding) -> Option<Self> {
        let version_scope = match binding.default_scope {
            DefaultScopeSemantics::ActiveVersion => VersionScope::ActiveVersion,
            DefaultScopeSemantics::ExplicitVersion => VersionScope::ExplicitVersion,
            DefaultScopeSemantics::History => VersionScope::History,
            DefaultScopeSemantics::GlobalAdmin | DefaultScopeSemantics::WorkingChanges => {
                return None
            }
        };

        match binding.descriptor.surface_family {
            SurfaceFamily::State | SurfaceFamily::Entity => Some(Self {
                include_tombstones: binding.descriptor.surface_variant == SurfaceVariant::History,
                expose_version_id: binding.implicit_overrides.expose_version_id,
                entity_projection: EntityProjectionSpec::from_surface_binding(&binding),
                binding,
                version_scope,
            }),
            SurfaceFamily::Filesystem | SurfaceFamily::Admin | SurfaceFamily::Change => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalChangeScan {
    pub(crate) binding: SurfaceBinding,
}

impl CanonicalChangeScan {
    pub(crate) fn from_surface_binding(binding: SurfaceBinding) -> Option<Self> {
        if binding.descriptor.surface_family != SurfaceFamily::Change {
            return None;
        }
        Some(Self { binding })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FilesystemKind {
    File,
    Directory,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalFilesystemScan {
    pub(crate) binding: SurfaceBinding,
    pub(crate) kind: FilesystemKind,
    pub(crate) version_scope: VersionScope,
}

impl CanonicalFilesystemScan {
    pub(crate) fn from_surface_binding(binding: SurfaceBinding) -> Option<Self> {
        if binding.descriptor.surface_family != SurfaceFamily::Filesystem {
            return None;
        }

        let version_scope = match binding.descriptor.public_name.as_str() {
            "lix_file" | "lix_directory" => VersionScope::ActiveVersion,
            "lix_file_by_version" | "lix_directory_by_version" => VersionScope::ExplicitVersion,
            "lix_file_history" | "lix_file_history_by_version" | "lix_directory_history" => {
                VersionScope::History
            }
            _ => return None,
        };

        let kind = match binding.descriptor.public_name.as_str() {
            "lix_file"
            | "lix_file_by_version"
            | "lix_file_history"
            | "lix_file_history_by_version" => FilesystemKind::File,
            "lix_directory" | "lix_directory_by_version" | "lix_directory_history" => {
                FilesystemKind::Directory
            }
            _ => return None,
        };

        Some(Self {
            binding,
            kind,
            version_scope,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalWorkingChangesScan {
    pub(crate) binding: SurfaceBinding,
}

impl CanonicalWorkingChangesScan {
    pub(crate) fn from_surface_binding(binding: SurfaceBinding) -> Option<Self> {
        if binding.descriptor.public_name != "lix_working_changes" {
            return None;
        }
        Some(Self { binding })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CanonicalAdminKind {
    ActiveVersion,
    ActiveAccount,
    StoredSchema,
    Version,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalAdminScan {
    pub(crate) binding: SurfaceBinding,
    pub(crate) kind: CanonicalAdminKind,
}

impl CanonicalAdminScan {
    pub(crate) fn from_surface_binding(binding: SurfaceBinding) -> Option<Self> {
        if binding.descriptor.surface_family != SurfaceFamily::Admin {
            return None;
        }

        let kind = match binding.descriptor.public_name.as_str() {
            "lix_active_version" => CanonicalAdminKind::ActiveVersion,
            "lix_active_account" => CanonicalAdminKind::ActiveAccount,
            "lix_stored_schema" => CanonicalAdminKind::StoredSchema,
            "lix_version" => CanonicalAdminKind::Version,
            _ => return None,
        };

        Some(Self { binding, kind })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PredicateSpec {
    pub(crate) sql: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectionExpr {
    pub(crate) output_name: String,
    pub(crate) source_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SortKey {
    pub(crate) column_name: String,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReadPlan {
    Scan(CanonicalStateScan),
    FilesystemScan(CanonicalFilesystemScan),
    AdminScan(CanonicalAdminScan),
    ChangeScan(CanonicalChangeScan),
    WorkingChangesScan(CanonicalWorkingChangesScan),
    Filter {
        input: Box<ReadPlan>,
        predicate: PredicateSpec,
    },
    Project {
        input: Box<ReadPlan>,
        expressions: Vec<ProjectionExpr>,
    },
    Sort {
        input: Box<ReadPlan>,
        ordering: Vec<SortKey>,
    },
    Limit {
        input: Box<ReadPlan>,
        limit: Option<u64>,
        offset: u64,
    },
}

impl ReadPlan {
    pub(crate) fn scan(scan: CanonicalStateScan) -> Self {
        Self::Scan(scan)
    }

    pub(crate) fn admin_scan(scan: CanonicalAdminScan) -> Self {
        Self::AdminScan(scan)
    }

    pub(crate) fn filesystem_scan(scan: CanonicalFilesystemScan) -> Self {
        Self::FilesystemScan(scan)
    }

    pub(crate) fn change_scan(scan: CanonicalChangeScan) -> Self {
        Self::ChangeScan(scan)
    }

    pub(crate) fn working_changes_scan(scan: CanonicalWorkingChangesScan) -> Self {
        Self::WorkingChangesScan(scan)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadContract {
    CommittedAtStart,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadCommand {
    pub(crate) root: ReadPlan,
    pub(crate) contract: ReadContract,
    pub(crate) requested_commit_mapping: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteOperationKind {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteMode {
    Tracked,
    Untracked,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum MutationPayload {
    FullSnapshot(BTreeMap<String, Value>),
    Patch(BTreeMap<String, Value>),
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InsertOnConflictUpdate {
    pub(crate) conflict_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct WriteSelector {
    pub(crate) residual_predicates: Vec<String>,
    pub(crate) exact_filters: BTreeMap<String, Value>,
    pub(crate) exact_only: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WriteCommand {
    pub(crate) operation_kind: WriteOperationKind,
    pub(crate) target: SurfaceBinding,
    pub(crate) selector: WriteSelector,
    pub(crate) payload: MutationPayload,
    pub(crate) on_conflict_update: Option<InsertOnConflictUpdate>,
    pub(crate) mode: WriteMode,
    pub(crate) execution_context: ExecutionContext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StateSourceKind {
    AuthoritativeCommitted,
    UntrackedOverlay,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ScopeProof {
    ActiveVersion,
    SingleVersion(String),
    GlobalAdmin,
    FiniteVersionSet(BTreeSet<String>),
    Unbounded,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SchemaProof {
    Exact(BTreeSet<String>),
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TargetSetProof {
    Exact(BTreeSet<String>),
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WriteLane {
    ActiveVersion,
    SingleVersion(String),
    GlobalAdmin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpectedTip {
    CommitId(String),
    CreateIfMissing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdempotencyKey(pub(crate) String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitPreconditions {
    pub(crate) write_lane: WriteLane,
    pub(crate) expected_tip: ExpectedTip,
    pub(crate) idempotency_key: IdempotencyKey,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedRowRef {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) version_id: Option<String>,
    pub(crate) source_change_id: Option<String>,
    pub(crate) source_commit_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PlannedStateRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) version_id: Option<String>,
    pub(crate) values: BTreeMap<String, Value>,
    pub(crate) tombstone: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RowLineage {
    pub(crate) entity_id: String,
    pub(crate) source_change_id: Option<String>,
    pub(crate) source_commit_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolvedWritePlan {
    pub(crate) authoritative_pre_state: Vec<ResolvedRowRef>,
    pub(crate) intended_post_state: Vec<PlannedStateRow>,
    pub(crate) tombstones: Vec<ResolvedRowRef>,
    pub(crate) lineage: Vec<RowLineage>,
    pub(crate) target_write_lane: Option<WriteLane>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PlannedWrite {
    pub(crate) command: WriteCommand,
    pub(crate) scope_proof: ScopeProof,
    pub(crate) schema_proof: SchemaProof,
    pub(crate) target_set_proof: Option<TargetSetProof>,
    pub(crate) state_source: StateSourceKind,
    pub(crate) resolved_write_plan: Option<ResolvedWritePlan>,
    pub(crate) commit_preconditions: Option<CommitPreconditions>,
    pub(crate) residual_execution_predicates: Vec<String>,
    pub(crate) backend_rejections: Vec<String>,
}

impl PlannedWrite {
    pub(crate) fn requires_single_write_lane(&self) -> bool {
        self.command.mode == WriteMode::Tracked
    }

    pub(crate) fn target_is_writable(&self) -> bool {
        self.command.target.capability == SurfaceCapability::ReadWrite
    }
}

#[cfg(test)]
mod tests {
    use super::{CanonicalStateScan, EntityProjectionSpec, VersionScope};
    use crate::sql2::catalog::{DynamicEntitySurfaceSpec, SurfaceRegistry};

    #[test]
    fn canonical_state_scan_tracks_explicit_version_visibility() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let scan = CanonicalStateScan::from_surface_binding(
            registry
                .bind_relation_name("lix_state_by_version")
                .expect("surface should bind"),
        )
        .expect("state surface should canonicalize");

        assert_eq!(scan.version_scope, VersionScope::ExplicitVersion);
        assert!(scan.expose_version_id);
        assert!(scan.entity_projection.is_none());
    }

    #[test]
    fn entity_surface_canonicalizes_with_projection_spec() {
        let mut registry = SurfaceRegistry::with_builtin_surfaces();
        registry.register_dynamic_entity_surfaces(DynamicEntitySurfaceSpec {
            schema_key: "lix_key_value".to_string(),
            visible_columns: vec!["key".to_string(), "value".to_string()],
            fixed_version_id: None,
            predicate_overrides: Vec::new(),
        });

        let binding = registry
            .bind_relation_name("lix_key_value")
            .expect("dynamic surface should bind");
        let projection =
            EntityProjectionSpec::from_surface_binding(&binding).expect("entity projection");
        let scan =
            CanonicalStateScan::from_surface_binding(binding).expect("entity surface should scan");

        assert_eq!(projection.schema_key, "lix_key_value");
        assert_eq!(scan.version_scope, VersionScope::ActiveVersion);
        assert!(scan.entity_projection.is_some());
    }
}
