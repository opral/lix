use std::collections::BTreeMap;

use crate::live_state::{
    LiveStateReader, LiveStateRowIdentity, LiveStateScanRequest, MaterializedLiveStateRow,
};
use crate::LixError;
use crate::GLOBAL_VERSION_ID;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VisibilityRequest {
    pub(crate) version_scope: VisibilityVersionScope,
    pub(crate) include_tombstones: bool,
    pub(crate) limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum VisibilityVersionScope {
    VersionIds { version_ids: Vec<String> },
}

pub(crate) trait StagedLiveStateRows {
    fn staged_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError>;
}

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

pub(crate) fn resolve_visible_rows(
    base_rows: Vec<MaterializedLiveStateRow>,
    staged_rows: Vec<MaterializedLiveStateRow>,
    request: &VisibilityRequest,
) -> Vec<MaterializedLiveStateRow> {
    let requested_version_ids = requested_version_ids(&request.version_scope);
    resolve_live_state_rows(
        base_rows,
        staged_rows,
        &requested_version_ids,
        request.include_tombstones,
        request.limit,
    )
}

pub(crate) async fn overlay_scan_rows<S>(
    base: &dyn LiveStateReader,
    staged: &S,
    request: &LiveStateScanRequest,
) -> Result<Vec<MaterializedLiveStateRow>, LixError>
where
    S: StagedLiveStateRows + ?Sized,
{
    let mut candidate_request = request.clone();
    candidate_request.limit = None;
    candidate_request.filter.include_tombstones = true;
    candidate_request.filter.version_ids = expanded_version_ids(&request.filter.version_ids);
    let staged_rows = staged.staged_rows(&candidate_request)?;
    let rows = base.scan_rows(&candidate_request).await?;
    Ok(resolve_visible_rows(
        rows,
        staged_rows,
        &VisibilityRequest {
            version_scope: VisibilityVersionScope::VersionIds {
                version_ids: request.filter.version_ids.clone(),
            },
            include_tombstones: request.filter.include_tombstones,
            limit: request.limit,
        },
    ))
}

/// Resolves raw tracked/untracked candidates into the rows visible for a scan.
///
/// Global rows are projected into each requested version scope, but keep
/// `global = true`. Version-scoped rows win over projected global rows for the
/// same identity. Tombstones participate in winning and are filtered only after
/// visibility is resolved. This projection is a read concern; constraint
/// validation remains exact storage-scope local unless a validator explicitly
/// opts into overlay semantics.
fn resolve_scan_rows(
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

fn resolve_live_state_rows(
    base_rows: Vec<MaterializedLiveStateRow>,
    staged_rows: Vec<MaterializedLiveStateRow>,
    requested_version_ids: &[String],
    include_tombstones: bool,
    limit: Option<usize>,
) -> Vec<MaterializedLiveStateRow> {
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
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    rows
}

fn requested_version_ids(version_scope: &VisibilityVersionScope) -> Vec<String> {
    match version_scope {
        VisibilityVersionScope::VersionIds { version_ids } => version_ids.clone(),
    }
}

fn project_global_rows_into_requested_versions(
    rows: Vec<MaterializedLiveStateRow>,
    requested_version_ids: &[String],
) -> Vec<MaterializedLiveStateRow> {
    if requested_version_ids.is_empty() {
        return dedupe_rows(rows);
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

fn dedupe_rows(rows: Vec<MaterializedLiveStateRow>) -> Vec<MaterializedLiveStateRow> {
    let mut rows_by_identity = BTreeMap::<LiveStateRowIdentity, MaterializedLiveStateRow>::new();
    for row in rows {
        insert_row_preferring_untracked(&mut rows_by_identity, row);
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
        Some((existing_tier, _)) if *existing_tier > tier => {}
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
    use crate::entity_identity::EntityIdentity;
    use crate::live_state::LiveStateRowRequest;
    use async_trait::async_trait;

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
    fn committed_scan_projects_global_row_into_requested_version() {
        let rows = resolve_scan_rows(
            vec![row_at(
                "global",
                "entity",
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
    fn committed_scan_prefers_requested_version_row_over_projected_global_row() {
        let rows = resolve_scan_rows(
            vec![
                row_at(
                    "global",
                    "entity",
                    "global-value",
                    true,
                    Some("change-global"),
                ),
                row_at(
                    "version-a",
                    "entity",
                    "version-value",
                    false,
                    Some("change-version"),
                ),
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
    fn empty_version_filter_dedupes_duplicate_base_rows() {
        let mut tracked = row_at(
            "version-a",
            "entity",
            "tracked",
            false,
            Some("change-tracked"),
        );
        tracked.untracked = false;
        let mut untracked = row_at("version-a", "entity", "untracked", false, None);
        untracked.untracked = true;

        let rows = resolve_scan_rows(vec![tracked, untracked], &[], false);

        assert_eq!(rows.len(), 1);
        assert!(rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"untracked\"}")
        );
    }

    #[test]
    fn empty_version_filter_dedupes_duplicate_base_and_staged_overlay_identity() {
        let base = row_at("version-a", "entity", "base", false, Some("change-base"));
        let staged = row_at(
            "version-a",
            "entity",
            "staged",
            false,
            Some("change-staged"),
        );

        let rows = resolve_live_state_rows(vec![base], vec![staged], &[], false, None);

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"staged\"}")
        );
    }

    #[test]
    fn version_tombstone_hides_global_row_after_visibility_resolution() {
        let rows = resolve_scan_rows(
            vec![
                row_at(
                    "global",
                    "entity",
                    "global-value",
                    true,
                    Some("change-global"),
                ),
                tombstone_at("version-a", "entity", false, Some("change-tombstone")),
            ],
            &["version-a".to_string()],
            false,
        );

        assert!(rows.is_empty());
    }

    #[test]
    fn overlay_prefers_staged_untracked_over_staged_tracked_for_same_visible_identity() {
        let mut tracked = row_at(
            "version-a",
            "entity",
            "tracked",
            false,
            Some("change-tracked"),
        );
        tracked.untracked = false;
        let mut untracked = row_at("version-a", "entity", "untracked", false, None);
        untracked.untracked = true;

        let rows = resolve_live_state_rows(
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

        let rows = resolve_live_state_rows(
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
        let mut base = row_at("version-a", "entity", "base-untracked", false, None);
        base.untracked = true;
        let mut staged = row_at(
            "version-a",
            "entity",
            "staged-tracked",
            false,
            Some("change-staged"),
        );
        staged.untracked = false;

        let rows = resolve_live_state_rows(
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
    fn staged_global_tombstone_hides_projected_base_global_row() {
        let mut base = row_at("version-a", "entity", "base", true, Some("change-base"));
        base.global = true;

        let rows = resolve_live_state_rows(
            vec![base],
            vec![tombstone_at(
                "global",
                "entity",
                true,
                Some("change-staged"),
            )],
            &["version-a".to_string()],
            false,
            None,
        );

        assert!(rows.is_empty());
    }

    #[test]
    fn base_version_tombstone_hides_staged_global_row() {
        let base = tombstone_at("version-a", "entity", false, Some("change-base"));
        let staged = row_at("global", "entity", "staged", true, Some("change-staged"));

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
    fn base_tracked_version_tombstone_hides_staged_untracked_global_row() {
        let mut base = tombstone_at("version-a", "entity", false, Some("change-base"));
        base.untracked = false;
        let mut staged = row_at("global", "entity", "staged", true, None);
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
        let base = tombstone_at("version-a", "entity", false, Some("change-base"));
        let staged = row_at(
            "version-a",
            "entity",
            "staged",
            false,
            Some("change-staged"),
        );

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

    #[test]
    fn tombstone_can_be_returned_when_requested() {
        let rows = resolve_scan_rows(
            vec![
                row_at(
                    "global",
                    "entity",
                    "global-value",
                    true,
                    Some("change-global"),
                ),
                tombstone_at("version-a", "entity", false, Some("change-tombstone")),
            ],
            &["version-a".to_string()],
            true,
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].version_id, "version-a");
        assert_eq!(rows[0].snapshot_content, None);
    }

    #[test]
    fn resolve_visible_rows_maps_version_scope_and_applies_limit() {
        let request = VisibilityRequest {
            version_scope: VisibilityVersionScope::VersionIds {
                version_ids: vec!["version-a".to_string()],
            },
            include_tombstones: false,
            limit: Some(1),
        };
        let rows = resolve_visible_rows(
            vec![
                row_at("version-a", "a", "A", false, Some("change-a")),
                row_at("version-a", "b", "B", false, Some("change-b")),
            ],
            Vec::new(),
            &request,
        );

        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn overlay_scan_fetches_base_global_candidates_for_staged_only_version_scope() {
        let base = ExistingGlobalOnlyReader {
            rows: vec![row_at(
                "global",
                "entity",
                "global-value",
                true,
                Some("change-global"),
            )],
        };
        let staged = EmptyStagedRows;

        let rows = overlay_scan_rows(
            &base,
            &staged,
            &LiveStateScanRequest {
                filter: crate::live_state::LiveStateFilter {
                    version_ids: vec!["staged-version".to_string()],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .expect("overlay scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].version_id, "staged-version");
        assert!(rows[0].global);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"global-value\"}")
        );
    }

    fn row_at(
        version_id: &str,
        entity_id: &str,
        value: &str,
        global: bool,
        change_id: Option<&str>,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_id: EntityIdentity::single(entity_id),
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
        entity_id: &str,
        global: bool,
        change_id: Option<&str>,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            snapshot_content: None,
            deleted: true,
            ..row_at(version_id, entity_id, "ignored", global, change_id)
        }
    }

    struct EmptyStagedRows;

    impl StagedLiveStateRows for EmptyStagedRows {
        fn staged_rows(
            &self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(Vec::new())
        }
    }

    struct ExistingGlobalOnlyReader {
        rows: Vec<MaterializedLiveStateRow>,
    }

    #[async_trait]
    impl LiveStateReader for ExistingGlobalOnlyReader {
        async fn scan_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            if request
                .filter
                .version_ids
                .iter()
                .any(|version_id| version_id == GLOBAL_VERSION_ID)
            {
                Ok(self.rows.clone())
            } else {
                Ok(Vec::new())
            }
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(None)
        }
    }
}
