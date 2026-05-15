use std::collections::BTreeSet;

use crate::live_state::MaterializedLiveStateRow;
use crate::sql2::plan::version_scope::VersionScope;
use crate::GLOBAL_VERSION_ID;

pub(crate) struct VisibilityRequest {
    pub(crate) version_scope: VersionScope,
    pub(crate) include_tombstones: bool,
    pub(crate) limit: Option<usize>,
}

pub(crate) fn resolve_visible_rows(
    base_rows: Vec<MaterializedLiveStateRow>,
    staged_rows: Vec<MaterializedLiveStateRow>,
    request: &VisibilityRequest,
) -> Vec<MaterializedLiveStateRow> {
    let Some(requested_version_ids) = requested_version_ids(&request.version_scope) else {
        return Vec::new();
    };
    resolve_live_state_rows(
        base_rows,
        staged_rows,
        &requested_version_ids,
        request.include_tombstones,
        request.limit,
    )
}

pub(crate) fn resolve_live_state_rows(
    base_rows: Vec<MaterializedLiveStateRow>,
    staged_rows: Vec<MaterializedLiveStateRow>,
    requested_version_ids: &[String],
    include_tombstones: bool,
    limit: Option<usize>,
) -> Vec<MaterializedLiveStateRow> {
    crate::live_state::resolve_overlay_rows(
        base_rows,
        staged_rows,
        requested_version_ids,
        include_tombstones,
        limit,
    )
}

fn requested_version_ids(version_scope: &VersionScope) -> Option<Vec<String>> {
    match version_scope {
        VersionScope::Active { version_id } => Some(vec![version_id.clone()]),
        VersionScope::Explicit { version_ids } | VersionScope::ExplicitRequired { version_ids } => {
            Some(set_to_vec(version_ids))
        }
        VersionScope::Global => Some(vec![GLOBAL_VERSION_ID.to_string()]),
        VersionScope::Empty => None,
    }
}

fn set_to_vec(values: &BTreeSet<String>) -> Vec<String> {
    values.iter().cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_identity::EntityIdentity;

    #[test]
    fn empty_version_scope_staged_tombstone_hides_base_row_before_filtering() {
        let rows = resolve_live_state_rows(
            vec![row("a", false)],
            vec![row("a", true)],
            &[],
            false,
            None,
        );

        assert!(rows.is_empty());
    }

    #[test]
    fn staged_global_tombstone_hides_projected_base_global_row() {
        let mut base = row("a", false);
        base.version_id = "version-a".to_string();
        base.global = true;
        let rows = resolve_live_state_rows(
            vec![base],
            vec![row("a", true)],
            &["version-a".to_string()],
            false,
            None,
        );

        assert!(rows.is_empty());
    }

    #[test]
    fn base_version_tombstone_hides_staged_global_row() {
        let mut base = row("a", true);
        base.version_id = "version-a".to_string();
        base.global = false;

        let rows = resolve_live_state_rows(
            vec![base],
            vec![row("a", false)],
            &["version-a".to_string()],
            false,
            None,
        );

        assert!(rows.is_empty());
    }

    #[test]
    fn base_tracked_version_tombstone_hides_staged_untracked_global_row() {
        let mut base = row("a", true);
        base.version_id = "version-a".to_string();
        base.global = false;
        base.untracked = false;
        let mut staged = row("a", false);
        staged.untracked = true;

        let rows = resolve_live_state_rows(
            vec![base],
            vec![staged],
            &["version-a".to_string()],
            false,
            None,
        );

        assert!(rows.is_empty());
    }

    #[test]
    fn staged_version_row_overrides_base_version_tombstone() {
        let mut base = row("a", true);
        base.version_id = "version-a".to_string();
        base.global = false;
        let mut staged = row("a", false);
        staged.version_id = "version-a".to_string();
        staged.global = false;

        let rows = resolve_live_state_rows(
            vec![base],
            vec![staged],
            &["version-a".to_string()],
            false,
            None,
        );

        assert_eq!(rows.len(), 1);
        assert!(!rows[0].deleted);
    }

    fn row(entity_id: &str, deleted: bool) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_id: EntityIdentity::single(entity_id),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            snapshot_content: (!deleted)
                .then(|| format!("{{\"key\":\"{entity_id}\",\"value\":\"value\"}}")),
            metadata: None,
            deleted,
            created_at: "test-created-at".to_string(),
            updated_at: "test-updated-at".to_string(),
            global: true,
            change_id: None,
            commit_id: None,
            untracked: true,
            version_id: "global".to_string(),
        }
    }
}
