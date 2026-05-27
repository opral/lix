use std::collections::BTreeMap;

use crate::live_state::{
    LiveStateReader, LiveStateRowIdentity, LiveStateScanRequest, MaterializedLiveStateRow,
};
use crate::LixError;
use crate::GLOBAL_BRANCH_ID;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VisibilityRequest {
    pub(crate) branch_scope: VisibilityBranchScope,
    pub(crate) include_tombstones: bool,
    pub(crate) limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum VisibilityBranchScope {
    BranchIds { branch_ids: Vec<String> },
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
    BaseBranch,
    StagedBranch,
}

/// Expands a branch-scoped storage read so global candidates are available for
/// the visibility overlay.
pub(crate) fn expanded_branch_ids(branch_ids: &[String]) -> Vec<String> {
    if branch_ids.is_empty() {
        return Vec::new();
    }

    let mut expanded = branch_ids.to_vec();
    if branch_ids
        .iter()
        .any(|branch_id| branch_id != GLOBAL_BRANCH_ID)
        && !expanded
            .iter()
            .any(|branch_id| branch_id == GLOBAL_BRANCH_ID)
    {
        expanded.push(GLOBAL_BRANCH_ID.to_string());
    }
    expanded
}

pub(crate) fn resolve_visible_rows(
    base_rows: Vec<MaterializedLiveStateRow>,
    staged_rows: Vec<MaterializedLiveStateRow>,
    request: &VisibilityRequest,
) -> Vec<MaterializedLiveStateRow> {
    let requested_branch_ids = requested_branch_ids(&request.branch_scope);
    resolve_live_state_rows(
        base_rows,
        staged_rows,
        &requested_branch_ids,
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
    candidate_request.filter.branch_ids = expanded_branch_ids(&request.filter.branch_ids);
    let staged_rows = staged.staged_rows(&candidate_request)?;
    let rows = base.scan_rows(&candidate_request).await?;
    Ok(resolve_visible_rows(
        rows,
        staged_rows,
        &VisibilityRequest {
            branch_scope: VisibilityBranchScope::BranchIds {
                branch_ids: request.filter.branch_ids.clone(),
            },
            include_tombstones: request.filter.include_tombstones,
            limit: request.limit,
        },
    ))
}

/// Resolves raw tracked/untracked candidates into the rows visible for a scan.
///
/// Global rows are projected into each requested branch scope, but keep
/// `global = true`. Branch-scoped rows win over projected global rows for the
/// same identity. Tombstones participate in winning and are filtered only after
/// visibility is resolved. This projection is a read concern; constraint
/// validation remains exact storage-scope local unless a validator explicitly
/// opts into overlay semantics.
fn resolve_scan_rows(
    rows: Vec<MaterializedLiveStateRow>,
    requested_branch_ids: &[String],
    include_tombstones: bool,
) -> Vec<MaterializedLiveStateRow> {
    let mut rows = project_global_rows_into_requested_branches(rows, requested_branch_ids);
    if !include_tombstones {
        rows.retain(|row| !row.deleted);
    }
    rows
}

fn resolve_live_state_rows(
    base_rows: Vec<MaterializedLiveStateRow>,
    staged_rows: Vec<MaterializedLiveStateRow>,
    requested_branch_ids: &[String],
    include_tombstones: bool,
    limit: Option<usize>,
) -> Vec<MaterializedLiveStateRow> {
    let base_rows = resolve_scan_rows(base_rows, requested_branch_ids, true);
    let staged_rows = resolve_scan_rows(staged_rows, requested_branch_ids, true);
    let mut rows_by_identity =
        BTreeMap::<LiveStateRowIdentity, (OverlayTier, MaterializedLiveStateRow)>::new();

    for row in base_rows {
        let tier = if row.global {
            OverlayTier::BaseGlobal
        } else {
            OverlayTier::BaseBranch
        };
        insert_overlay_row(&mut rows_by_identity, tier, row);
    }
    for row in staged_rows {
        let tier = if row.global {
            OverlayTier::StagedGlobal
        } else {
            OverlayTier::StagedBranch
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

fn requested_branch_ids(branch_scope: &VisibilityBranchScope) -> Vec<String> {
    match branch_scope {
        VisibilityBranchScope::BranchIds { branch_ids } => branch_ids.clone(),
    }
}

fn project_global_rows_into_requested_branches(
    rows: Vec<MaterializedLiveStateRow>,
    requested_branch_ids: &[String],
) -> Vec<MaterializedLiveStateRow> {
    if requested_branch_ids.is_empty() {
        return dedupe_rows(rows);
    }

    let mut rows_by_identity = BTreeMap::<LiveStateRowIdentity, MaterializedLiveStateRow>::new();
    for requested_branch_id in requested_branch_ids {
        for row in &rows {
            if row.branch_id == GLOBAL_BRANCH_ID {
                let mut projected = row.clone();
                projected.branch_id = requested_branch_id.clone();
                insert_row_preferring_untracked(&mut rows_by_identity, projected);
            }
        }
        let mut branch_rows_by_identity =
            BTreeMap::<LiveStateRowIdentity, MaterializedLiveStateRow>::new();
        for row in rows
            .iter()
            .filter(|row| row.branch_id == *requested_branch_id)
        {
            insert_row_preferring_untracked(&mut branch_rows_by_identity, row.clone());
        }
        rows_by_identity.extend(branch_rows_by_identity);
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
    use crate::entity_pk::EntityPk;
    use crate::live_state::LiveStateRowRequest;
    use async_trait::async_trait;

    #[test]
    fn expands_requested_branch_with_global_candidates() {
        assert_eq!(
            expanded_branch_ids(&["branch-a".to_string()]),
            vec!["branch-a".to_string(), "global".to_string()]
        );
        assert_eq!(
            expanded_branch_ids(&["global".to_string()]),
            vec!["global".to_string()]
        );
    }

    #[test]
    fn committed_scan_projects_global_row_into_requested_branch() {
        let rows = resolve_scan_rows(
            vec![row_at(
                "global",
                "entity",
                "global-value",
                true,
                Some("change-global"),
            )],
            &["branch-a".to_string()],
            false,
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].branch_id, "branch-a");
        assert!(rows[0].global);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"global-value\"}")
        );
    }

    #[test]
    fn committed_scan_prefers_requested_branch_row_over_projected_global_row() {
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
                    "branch-a",
                    "entity",
                    "branch-value",
                    false,
                    Some("change-branch"),
                ),
            ],
            &["branch-a".to_string()],
            false,
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].branch_id, "branch-a");
        assert!(!rows[0].global);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"branch-value\"}")
        );
    }

    #[test]
    fn empty_branch_filter_dedupes_duplicate_base_rows() {
        let mut tracked = row_at(
            "branch-a",
            "entity",
            "tracked",
            false,
            Some("change-tracked"),
        );
        tracked.untracked = false;
        let mut untracked = row_at("branch-a", "entity", "untracked", false, None);
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
    fn empty_branch_filter_dedupes_duplicate_base_and_staged_overlay_identity() {
        let base = row_at("branch-a", "entity", "base", false, Some("change-base"));
        let staged = row_at("branch-a", "entity", "staged", false, Some("change-staged"));

        let rows = resolve_live_state_rows(vec![base], vec![staged], &[], false, None);

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"staged\"}")
        );
    }

    #[test]
    fn branch_tombstone_hides_global_row_after_visibility_resolution() {
        let rows = resolve_scan_rows(
            vec![
                row_at(
                    "global",
                    "entity",
                    "global-value",
                    true,
                    Some("change-global"),
                ),
                tombstone_at("branch-a", "entity", false, Some("change-tombstone")),
            ],
            &["branch-a".to_string()],
            false,
        );

        assert!(rows.is_empty());
    }

    #[test]
    fn overlay_prefers_staged_untracked_over_staged_tracked_for_same_visible_identity() {
        let mut tracked = row_at(
            "branch-a",
            "entity",
            "tracked",
            false,
            Some("change-tracked"),
        );
        tracked.untracked = false;
        let mut untracked = row_at("branch-a", "entity", "untracked", false, None);
        untracked.untracked = true;

        let rows = resolve_live_state_rows(
            Vec::new(),
            vec![untracked.clone(), tracked.clone()],
            &["branch-a".to_string()],
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
            &["branch-a".to_string()],
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
        let mut base = row_at("branch-a", "entity", "base-untracked", false, None);
        base.untracked = true;
        let mut staged = row_at(
            "branch-a",
            "entity",
            "staged-tracked",
            false,
            Some("change-staged"),
        );
        staged.untracked = false;

        let rows = resolve_live_state_rows(
            vec![base],
            vec![staged],
            &["branch-a".to_string()],
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
        let mut base = row_at("branch-a", "entity", "base", true, Some("change-base"));
        base.global = true;

        let rows = resolve_live_state_rows(
            vec![base],
            vec![tombstone_at(
                "global",
                "entity",
                true,
                Some("change-staged"),
            )],
            &["branch-a".to_string()],
            false,
            None,
        );

        assert!(rows.is_empty());
    }

    #[test]
    fn base_branch_tombstone_hides_staged_global_row() {
        let base = tombstone_at("branch-a", "entity", false, Some("change-base"));
        let staged = row_at("global", "entity", "staged", true, Some("change-staged"));

        let rows = resolve_live_state_rows(
            vec![base],
            vec![staged],
            &["branch-a".to_string()],
            false,
            None,
        );

        assert!(rows.is_empty());
    }

    #[test]
    fn base_tracked_branch_tombstone_hides_staged_untracked_global_row() {
        let mut base = tombstone_at("branch-a", "entity", false, Some("change-base"));
        base.untracked = false;
        let mut staged = row_at("global", "entity", "staged", true, None);
        staged.untracked = true;

        let rows = resolve_live_state_rows(
            vec![base],
            vec![staged],
            &["branch-a".to_string()],
            false,
            None,
        );

        assert!(rows.is_empty());
    }

    #[test]
    fn staged_branch_row_overrides_base_branch_tombstone() {
        let base = tombstone_at("branch-a", "entity", false, Some("change-base"));
        let staged = row_at("branch-a", "entity", "staged", false, Some("change-staged"));

        let rows = resolve_live_state_rows(
            vec![base],
            vec![staged],
            &["branch-a".to_string()],
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
                tombstone_at("branch-a", "entity", false, Some("change-tombstone")),
            ],
            &["branch-a".to_string()],
            true,
        );

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].branch_id, "branch-a");
        assert_eq!(rows[0].snapshot_content, None);
    }

    #[test]
    fn resolve_visible_rows_maps_branch_scope_and_applies_limit() {
        let request = VisibilityRequest {
            branch_scope: VisibilityBranchScope::BranchIds {
                branch_ids: vec!["branch-a".to_string()],
            },
            include_tombstones: false,
            limit: Some(1),
        };
        let rows = resolve_visible_rows(
            vec![
                row_at("branch-a", "a", "A", false, Some("change-a")),
                row_at("branch-a", "b", "B", false, Some("change-b")),
            ],
            Vec::new(),
            &request,
        );

        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn overlay_scan_fetches_base_global_candidates_for_staged_only_branch_scope() {
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
                    branch_ids: vec!["staged-branch".to_string()],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .expect("overlay scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].branch_id, "staged-branch");
        assert!(rows[0].global);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"global-value\"}")
        );
    }

    fn row_at(
        branch_id: &str,
        entity_pk: &str,
        value: &str,
        global: bool,
        change_id: Option<&str>,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: EntityPk::single(entity_pk),
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
            branch_id: branch_id.to_string(),
        }
    }

    fn tombstone_at(
        branch_id: &str,
        entity_pk: &str,
        global: bool,
        change_id: Option<&str>,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            snapshot_content: None,
            deleted: true,
            ..row_at(branch_id, entity_pk, "ignored", global, change_id)
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
                .branch_ids
                .iter()
                .any(|branch_id| branch_id == GLOBAL_BRANCH_ID)
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
