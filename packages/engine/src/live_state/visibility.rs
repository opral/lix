use std::collections::BTreeMap;

use crate::live_state::{LiveStateRowIdentity, MaterializedLiveStateRow};
use crate::GLOBAL_VERSION_ID;

/// Expands a version-scoped storage read so global candidates are available for
/// the visibility overlay.
pub(crate) fn expanded_version_ids(version_ids: &[String]) -> Vec<String> {
    if version_ids.is_empty() {
        return Vec::new();
    }

    let mut expanded = version_ids.to_vec();
    if version_ids
        .iter()
        .any(|version_id| version_id != GLOBAL_VERSION_ID)
        && !expanded
            .iter()
            .any(|version_id| version_id == GLOBAL_VERSION_ID)
    {
        expanded.push(GLOBAL_VERSION_ID.to_string());
    }
    expanded
}

/// Resolves raw tracked/untracked candidates into the rows visible for a scan.
///
/// Global rows are projected into each requested version scope, but keep
/// `global = true`. Version-scoped rows win over projected global rows for the
/// same identity. Tombstones participate in winning and are filtered only after
/// visibility is resolved. This projection is a read concern; constraint
/// validation remains exact storage-scope local unless a validator explicitly
/// opts into overlay semantics.
pub(crate) fn resolve_scan_rows(
    rows: Vec<MaterializedLiveStateRow>,
    requested_version_ids: &[String],
    include_tombstones: bool,
) -> Vec<MaterializedLiveStateRow> {
    let mut rows = project_global_rows_into_requested_versions(rows, requested_version_ids);
    if !include_tombstones {
        rows.retain(|row| row.snapshot_content.is_some());
    }
    rows
}

/// Resolves a row loaded through a concrete storage version into the row visible
/// to the requested version scope.
pub(crate) fn project_loaded_row(
    mut row: MaterializedLiveStateRow,
    requested_version_id: &str,
    matched_version_id: &str,
) -> MaterializedLiveStateRow {
    if row.global && requested_version_id != GLOBAL_VERSION_ID {
        row.version_id = requested_version_id.to_string();
    } else if matched_version_id == GLOBAL_VERSION_ID && requested_version_id != GLOBAL_VERSION_ID {
        row.version_id = requested_version_id.to_string();
    }
    row
}

fn project_global_rows_into_requested_versions(
    rows: Vec<MaterializedLiveStateRow>,
    requested_version_ids: &[String],
) -> Vec<MaterializedLiveStateRow> {
    if requested_version_ids.is_empty() {
        return rows;
    }

    let mut rows_by_identity = BTreeMap::<LiveStateRowIdentity, MaterializedLiveStateRow>::new();
    for requested_version_id in requested_version_ids {
        for row in &rows {
            if row.version_id == GLOBAL_VERSION_ID {
                let mut projected = row.clone();
                projected.version_id = requested_version_id.clone();
                rows_by_identity.insert(LiveStateRowIdentity::from_row(&projected), projected);
            }
        }
        for row in rows
            .iter()
            .filter(|row| row.version_id == *requested_version_id)
        {
            rows_by_identity.insert(LiveStateRowIdentity::from_row(row), row.clone());
        }
    }

    rows_by_identity.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expands_requested_version_with_global_candidates() {
        assert_eq!(
            expanded_version_ids(&["version-a".to_string()]),
            vec!["version-a".to_string(), "global".to_string()]
        );
        assert_eq!(
            expanded_version_ids(&["global".to_string()]),
            vec!["global".to_string()]
        );
    }

    #[test]
    fn scan_projects_global_row_into_requested_version() {
        let rows = resolve_scan_rows(
            vec![row_at(
                "global",
                "global-value",
                true,
                Some("change-global"),
            )],
            &["version-a".to_string()],
            false,
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].version_id, "version-a");
        assert!(rows[0].global);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"global-value\"}")
        );
    }

    #[test]
    fn scan_prefers_requested_version_row_over_projected_global_row() {
        let rows = resolve_scan_rows(
            vec![
                row_at("global", "global-value", true, Some("change-global")),
                row_at("version-a", "version-value", false, Some("change-version")),
            ],
            &["version-a".to_string()],
            false,
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].version_id, "version-a");
        assert!(!rows[0].global);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"version-value\"}")
        );
    }

    #[test]
    fn version_tombstone_hides_global_row_after_visibility_resolution() {
        let rows = resolve_scan_rows(
            vec![
                row_at("global", "global-value", true, Some("change-global")),
                tombstone_at("version-a", false, Some("change-tombstone")),
            ],
            &["version-a".to_string()],
            false,
        );

        assert!(rows.is_empty());
    }

    #[test]
    fn tombstone_can_be_returned_when_requested() {
        let rows = resolve_scan_rows(
            vec![
                row_at("global", "global-value", true, Some("change-global")),
                tombstone_at("version-a", false, Some("change-tombstone")),
            ],
            &["version-a".to_string()],
            true,
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].version_id, "version-a");
        assert_eq!(rows[0].snapshot_content, None);
    }

    #[test]
    fn loaded_global_row_is_projected_into_requested_version() {
        let row = project_loaded_row(
            row_at("global", "global-value", true, Some("change-global")),
            "version-a",
            "global",
        );

        assert_eq!(row.version_id, "version-a");
        assert!(row.global);
    }

    fn row_at(
        version_id: &str,
        value: &str,
        global: bool,
        change_id: Option<&str>,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_id: crate::entity_identity::EntityIdentity::single("entity"),
            schema_key: "schema".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global,
            change_id: change_id.map(str::to_string),
            commit_id: Some("commit".to_string()),
            untracked: false,
            version_id: version_id.to_string(),
        }
    }

    fn tombstone_at(
        version_id: &str,
        global: bool,
        change_id: Option<&str>,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            snapshot_content: None,
            ..row_at(version_id, "ignored", global, change_id)
        }
    }
}
