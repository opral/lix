use crate::engine2::changelog::CanonicalChange;
use crate::engine2::transaction::types::StagedStateRow;
use crate::LixError;

/// Input for generating durable changelog facts from staged transaction rows.
pub(crate) struct CommitChangeGenerationInput<'a> {
    pub(crate) state_rows: &'a [StagedStateRow],
}

/// Converts live-state-ready staged rows into canonical changelog changes.
///
/// This only generates change facts for rows that providers already staged.
/// It does not write live_state and it does not invent `lix_commit` rows yet.
pub(crate) fn generate_commit_changes(
    input: CommitChangeGenerationInput<'_>,
) -> Result<Vec<CanonicalChange>, LixError> {
    generate_commit_changes_from_rows(input.state_rows.iter().cloned())
}

pub(crate) fn generate_commit_changes_from_rows(
    state_rows: impl IntoIterator<Item = StagedStateRow>,
) -> Result<Vec<CanonicalChange>, LixError> {
    Ok(state_rows
        .into_iter()
        .map(canonical_change_from_staged_row)
        .collect())
}

fn canonical_change_from_staged_row(row: StagedStateRow) -> CanonicalChange {
    CanonicalChange {
        id: row.change_id,
        entity_id: row.entity_id,
        schema_key: row.schema_key,
        schema_version: row.schema_version,
        file_id: row.file_id,
        plugin_key: row.plugin_key,
        snapshot_content: row.snapshot_content,
        metadata: row.metadata,
        created_at: row.created_at,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_commit_changes_maps_state_row_to_canonical_change() {
        let row = state_row();
        let changes = generate_commit_changes(CommitChangeGenerationInput {
            state_rows: std::slice::from_ref(&row),
        })
        .expect("change generation should succeed");

        assert_eq!(
            changes,
            vec![CanonicalChange {
                id: "change-1".to_string(),
                entity_id: "entity-1".to_string(),
                schema_key: "test_schema".to_string(),
                schema_version: "1".to_string(),
                file_id: None,
                plugin_key: None,
                snapshot_content: Some("{\"value\":1}".to_string()),
                metadata: Some("{\"source\":\"test\"}".to_string()),
                created_at: "2026-01-01T00:00:00Z".to_string(),
            }]
        );
    }

    #[test]
    fn generate_commit_changes_from_rows_consumes_live_state_rows() {
        let changes = generate_commit_changes_from_rows(vec![state_row()])
            .expect("owned row conversion should succeed");

        assert_eq!(changes[0].id, "change-1");
        assert_eq!(changes[0].entity_id, "entity-1");
    }

    fn state_row() -> StagedStateRow {
        StagedStateRow {
            entity_id: "entity-1".to_string(),
            schema_key: "test_schema".to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some("{\"value\":1}".to_string()),
            metadata: Some("{\"source\":\"test\"}".to_string()),
            schema_version: "1".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:01Z".to_string(),
            global: false,
            change_id: "change-1".to_string(),
            commit_id: None,
            untracked: false,
            version_id: "version-a".to_string(),
        }
    }
}
