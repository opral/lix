use crate::sql::prepare::contracts::planner_error::PlannerError;
use crate::sql::PlanEffects;
use crate::sql::PlannedStatementSet;
use crate::streams::{
    state_commit_stream_changes_from_mutations, StateCommitStreamRuntimeMetadata,
};
use std::collections::BTreeSet;

pub(crate) fn derive_effects_from_state_resolution(
    preprocess: &PlannedStatementSet,
    writer_key: Option<&str>,
) -> Result<PlanEffects, PlannerError> {
    let state_commit_stream_changes = state_commit_stream_changes_from_mutations(
        &preprocess.mutations,
        StateCommitStreamRuntimeMetadata::from_runtime_writer_key(writer_key),
    );
    let file_cache_refresh_targets = direct_state_file_cache_refresh_targets(&preprocess.mutations);

    Ok(PlanEffects {
        state_commit_stream_changes,
        file_cache_refresh_targets,
        ..PlanEffects::default()
    })
}

fn direct_state_file_cache_refresh_targets(
    mutations: &[crate::sql::MutationRow],
) -> BTreeSet<(String, String)> {
    mutations
        .iter()
        .filter(|mutation| !mutation.untracked)
        .filter(|mutation| mutation.schema_key != "lix_file_descriptor")
        .filter(|mutation| mutation.schema_key != "lix_directory_descriptor")
        .filter_map(|mutation| {
            mutation
                .file_id
                .as_ref()
                .map(|file_id| (file_id.clone(), mutation.version_id.clone()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::derive_effects_from_state_resolution;
    use crate::backend::PreparedStatement;
    use crate::sql::{MutationOperation, MutationRow, PlannedStatementSet, UpdateValidationPlan};
    use serde_json::json;

    #[test]
    fn ignores_legacy_active_version_mutations_for_session_deltas() {
        let preprocess = PlannedStatementSet {
            prepared_statements: vec![PreparedStatement {
                sql: "UPDATE lix_active_version SET version_id = 'version-b'".to_string(),
                params: Vec::new(),
            }],
            live_table_requirements: Vec::new(),
            mutations: vec![MutationRow {
                operation: MutationOperation::Insert,
                entity_id: "main".to_string(),
                schema_key: "lix_active_version".to_string(),
                schema_version: "1".to_string(),
                file_id: None,
                version_id: "global".to_string(),
                plugin_key: None,
                snapshot_content: Some(json!({
                    "id": "main",
                    "version_id": "version-b"
                })),
                untracked: true,
            }],
            update_validations: vec![UpdateValidationPlan {
                delete: false,
                table: "lix_internal_live_untracked_v1_lix_active_version".to_string(),
                where_clause: None,
                snapshot_content: Some(json!({
                    "id": "main",
                    "version_id": "version-c"
                })),
                snapshot_patch: None,
            }],
        };

        let effects =
            derive_effects_from_state_resolution(&preprocess, None).expect("effects should derive");

        assert_eq!(effects.session_delta.next_active_version_id, None);
    }
}
