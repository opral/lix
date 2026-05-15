use std::collections::BTreeMap;

use crate::live_state::{LiveStateRowIdentity, MaterializedLiveStateRow};
use crate::GLOBAL_VERSION_ID;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum OverlayTier {
    BaseGlobal,
    StagedGlobal,
    BaseVersion,
    StagedVersion,
}

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
        rows.retain(|row| !row.deleted);
    }
    rows
}

pub(crate) fn resolve_overlay_rows(
    base_rows: Vec<MaterializedLiveStateRow>,
    staged_rows: Vec<MaterializedLiveStateRow>,
    requested_version_ids: &[String],
    include_tombstones: bool,
    limit: Option<usize>,
) -> Vec<MaterializedLiveStateRow> {
    let mut rows = if requested_version_ids.is_empty() {
        let mut rows_by_identity =
            BTreeMap::<LiveStateRowIdentity, (OverlayTier, MaterializedLiveStateRow)>::new();
        for row in base_rows {
            insert_overlay_row(&mut rows_by_identity, OverlayTier::BaseVersion, row);
        }
        for row in staged_rows {
            insert_overlay_row(&mut rows_by_identity, OverlayTier::StagedVersion, row);
        }
        let mut rows = rows_by_identity
            .into_values()
            .map(|(_, row)| row)
            .collect::<Vec<_>>();
        if !include_tombstones {
            rows.retain(|row| !row.deleted);
        }
        rows
    } else {
        let base_rows = resolve_scan_rows(base_rows, requested_version_ids, true);
        let staged_rows = resolve_scan_rows(staged_rows, requested_version_ids, true);
        let mut rows_by_identity =
            BTreeMap::<LiveStateRowIdentity, (OverlayTier, MaterializedLiveStateRow)>::new();
        for row in base_rows {
            let tier = if row.global {
                OverlayTier::BaseGlobal
            } else {
                OverlayTier::BaseVersion
            };
            insert_overlay_row(&mut rows_by_identity, tier, row);
        }
        for row in staged_rows {
            let tier = if row.global {
                OverlayTier::StagedGlobal
            } else {
                OverlayTier::StagedVersion
            };
            insert_overlay_row(&mut rows_by_identity, tier, row);
        }
        let mut rows = rows_by_identity
            .into_values()
            .map(|(_, row)| row)
            .collect::<Vec<_>>();
        if !include_tombstones {
            rows.retain(|row| !row.deleted);
        }
        rows
    };
    if let Some(limit) = limit {
        rows.truncate(limit);
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
                insert_row_preferring_untracked(&mut rows_by_identity, projected);
            }
        }
        let mut version_rows_by_identity =
            BTreeMap::<LiveStateRowIdentity, MaterializedLiveStateRow>::new();
        for row in rows
            .iter()
            .filter(|row| row.version_id == *requested_version_id)
        {
            insert_row_preferring_untracked(&mut version_rows_by_identity, row.clone());
        }
        rows_by_identity.extend(version_rows_by_identity);
    }

    rows_by_identity.into_values().collect()
}

fn insert_overlay_row(
    rows_by_identity: &mut BTreeMap<LiveStateRowIdentity, (OverlayTier, MaterializedLiveStateRow)>,
    tier: OverlayTier,
    row: MaterializedLiveStateRow,
) {
    let identity = LiveStateRowIdentity::from_row(&row);
    match rows_by_identity.get(&identity) {
        Some((existing_tier, existing)) if *existing_tier > tier => {}
        Some((existing_tier, existing))
            if *existing_tier == tier && existing.untracked && !row.untracked => {}
        _ => {
            rows_by_identity.insert(identity, (tier, row));
        }
    }
}

fn insert_row_preferring_untracked(
    rows_by_identity: &mut BTreeMap<LiveStateRowIdentity, MaterializedLiveStateRow>,
    row: MaterializedLiveStateRow,
) {
    let identity = LiveStateRowIdentity::from_row(&row);
    match rows_by_identity.get(&identity) {
        Some(existing) if existing.untracked && !row.untracked => {}
        _ => {
            rows_by_identity.insert(identity, row);
        }
    }
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
    fn overlay_prefers_staged_untracked_over_staged_tracked_for_same_visible_identity() {
        let mut tracked = row_at("version-a", "tracked", false, Some("change-tracked"));
        tracked.untracked = false;
        let mut untracked = row_at("version-a", "untracked", false, None);
        untracked.untracked = true;

        let rows = resolve_overlay_rows(
            Vec::new(),
            vec![untracked.clone(), tracked.clone()],
            &["version-a".to_string()],
            false,
            None,
        );

        assert_eq!(rows.len(), 1);
        assert!(rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"untracked\"}")
        );

        let rows = resolve_overlay_rows(
            Vec::new(),
            vec![tracked, untracked],
            &["version-a".to_string()],
            false,
            None,
        );

        assert_eq!(rows.len(), 1);
        assert!(rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"untracked\"}")
        );
    }

    #[test]
    fn overlay_prefers_staged_tracked_over_base_untracked_for_same_visible_identity() {
        let mut base = row_at("version-a", "base-untracked", false, None);
        base.untracked = true;
        let mut staged = row_at("version-a", "staged-tracked", false, Some("change-staged"));
        staged.untracked = false;

        let rows = resolve_overlay_rows(
            vec![base],
            vec![staged],
            &["version-a".to_string()],
            false,
            None,
        );

        assert_eq!(rows.len(), 1);
        assert!(!rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"staged-tracked\"}")
        );
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
            deleted: false,
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
            deleted: true,
            ..row_at(version_id, "ignored", global, change_id)
        }
    }
}
