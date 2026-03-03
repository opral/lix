use std::collections::BTreeSet;

use crate::engine::sql::contracts::effects::DetectedFileDomainChange;
use crate::engine::sql::contracts::requirements::PlanRequirements;
use crate::engine::{CollectedExecutionSideEffects, Engine};
use crate::{LixBackend, LixError, Value};
use sqlparser::ast::Statement;

#[derive(Debug, Clone, Copy)]
pub(crate) struct IntentCollectionPolicy {
    pub(crate) allow_plugin_cache: bool,
    pub(crate) detect_plugin_file_changes: bool,
    pub(crate) skip_side_effect_collection: bool,
}

pub(crate) struct ExecutionIntent {
    pub(crate) pending_file_writes: Vec<crate::filesystem::pending_file_writes::PendingFileWrite>,
    pub(crate) pending_file_delete_targets: BTreeSet<(String, String)>,
    pub(crate) detected_file_domain_changes_by_statement: Vec<Vec<DetectedFileDomainChange>>,
    pub(crate) detected_file_domain_changes: Vec<DetectedFileDomainChange>,
    pub(crate) untracked_filesystem_update_domain_changes: Vec<DetectedFileDomainChange>,
}

pub(crate) async fn collect_execution_intent_with_backend(
    engine: &Engine,
    backend: &dyn LixBackend,
    statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    requirements: &PlanRequirements,
    policy: IntentCollectionPolicy,
) -> Result<ExecutionIntent, LixError> {
    let CollectedExecutionSideEffects {
        pending_file_writes,
        pending_file_delete_targets,
        detected_file_domain_changes_by_statement,
        detected_file_domain_changes,
        untracked_filesystem_update_domain_changes,
    } = if policy.skip_side_effect_collection || requirements.read_only_query {
        CollectedExecutionSideEffects {
            pending_file_writes: Vec::new(),
            pending_file_delete_targets: BTreeSet::new(),
            detected_file_domain_changes_by_statement: Vec::new(),
            detected_file_domain_changes: Vec::new(),
            untracked_filesystem_update_domain_changes: Vec::new(),
        }
    } else {
        engine
            .collect_execution_side_effects_with_backend_from_statements(
                backend,
                statements,
                params,
                active_version_id,
                writer_key,
                policy.allow_plugin_cache,
                policy.detect_plugin_file_changes,
            )
            .await?
    };

    Ok(ExecutionIntent {
        pending_file_writes,
        pending_file_delete_targets,
        detected_file_domain_changes_by_statement,
        detected_file_domain_changes,
        untracked_filesystem_update_domain_changes,
    })
}

pub(crate) fn authoritative_pending_file_write_targets(
    writes: &[crate::filesystem::pending_file_writes::PendingFileWrite],
) -> BTreeSet<(String, String)> {
    writes
        .iter()
        .filter(|write| write.data_is_authoritative)
        .filter(|write| {
            crate::filesystem::pending_file_writes::unresolved_auto_file_path_from_id(
                &write.file_id,
            )
            .is_none()
        })
        .map(|write| (write.file_id.clone(), write.version_id.clone()))
        .collect()
}
