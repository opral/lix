use std::cmp::Ordering;
use std::collections::BTreeMap;

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::live_state::{
    LiveStateExactBatchRequest, LiveStateExactRowRequest, LiveStateReader, LiveStateRowIdentity,
    LiveStateScanRequest, MaterializedLiveStateRow,
};

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

    /// Loads exact staged storage identities in request order.
    ///
    /// This does not apply global fallback: overlay composition needs the
    /// branch and global candidates separately to preserve their precedence.
    fn load_exact_rows(
        &self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
        request
            .rows
            .iter()
            .map(|row| {
                Ok(self
                    .staged_rows(&request.row_scan_request(row))?
                    .into_iter()
                    .next())
            })
            .collect()
    }
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

/// Overlays staged tracked rows on the immutable tracked head.
///
/// This is deliberately separate from [`overlay_scan_rows`]: tracked schema
/// planning and validation must ignore unrelated untracked transaction rows
/// and remain based on an independently valid commit.
pub(crate) async fn overlay_scan_tracked_rows<S>(
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
    candidate_request.filter.untracked = Some(false);
    let staged_rows = staged.staged_rows(&candidate_request)?;
    let rows = base.scan_tracked_rows(&candidate_request).await?;
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

/// Overlays staged exact identities without converting correlated row keys to
/// independent scan filters.
pub(crate) async fn overlay_load_exact_rows<S>(
    base: &dyn LiveStateReader,
    staged: &S,
    request: &LiveStateExactBatchRequest,
) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError>
where
    S: StagedLiveStateRows + ?Sized,
{
    if request.rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut base_request = request.clone();
    base_request.include_tombstones = true;
    let base_rows = base.load_exact_rows(&base_request).await?;
    if base_rows.len() != request.rows.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "exact live-state base read expected {} result slots, got {}",
                request.rows.len(),
                base_rows.len()
            ),
        ));
    }

    let mut staged_requests = Vec::with_capacity(request.rows.len() * 2);
    let mut staged_indices = Vec::with_capacity(request.rows.len());
    for row in &request.rows {
        let global_index = staged_requests.len();
        staged_requests.push(LiveStateExactRowRequest {
            branch_id: GLOBAL_BRANCH_ID.to_string(),
            schema_key: row.schema_key.clone(),
            entity_pk: row.entity_pk.clone(),
            file_id: row.file_id.clone(),
        });
        let branch_index = if row.branch_id == GLOBAL_BRANCH_ID {
            None
        } else {
            let index = staged_requests.len();
            staged_requests.push(row.clone());
            Some(index)
        };
        staged_indices.push((global_index, branch_index));
    }
    let staged_request = LiveStateExactBatchRequest {
        rows: staged_requests,
        projection: request.projection.clone(),
        untracked: request.untracked,
        include_tombstones: true,
    };
    let staged_rows = staged.load_exact_rows(&staged_request)?;
    if staged_rows.len() != staged_request.rows.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "exact staged live-state read expected {} result slots, got {}",
                staged_request.rows.len(),
                staged_rows.len()
            ),
        ));
    }

    Ok(request
        .rows
        .iter()
        .zip(base_rows)
        .zip(staged_indices)
        .map(|((requested, base), (global_index, branch_index))| {
            let mut winner = base.map(|row| {
                let tier = if row.global {
                    OverlayTier::BaseGlobal
                } else {
                    OverlayTier::BaseBranch
                };
                (tier, row)
            });
            if let Some(mut row) = staged_rows[global_index].clone() {
                if requested.branch_id != GLOBAL_BRANCH_ID {
                    row.branch_id.clone_from(&requested.branch_id);
                }
                row.global = true;
                insert_exact_overlay_candidate(&mut winner, OverlayTier::StagedGlobal, row);
            }
            if let Some(index) = branch_index
                && let Some(row) = staged_rows[index].clone()
            {
                insert_exact_overlay_candidate(&mut winner, OverlayTier::StagedBranch, row);
            }
            let row = winner.map(|(_, row)| row)?;
            if row.deleted && !request.include_tombstones {
                None
            } else {
                Some(row)
            }
        })
        .collect())
}

fn insert_exact_overlay_candidate(
    winner: &mut Option<(OverlayTier, MaterializedLiveStateRow)>,
    tier: OverlayTier,
    row: MaterializedLiveStateRow,
) {
    if winner
        .as_ref()
        .is_none_or(|(existing_tier, _)| *existing_tier <= tier)
    {
        *winner = Some((tier, row));
    }
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
    if can_resolve_uncontested_single_branch_rows(&base_rows, &staged_rows, requested_branch_ids) {
        return resolve_uncontested_single_branch_rows(base_rows, include_tombstones, limit);
    }

    resolve_live_state_rows_via_overlay(
        base_rows,
        staged_rows,
        requested_branch_ids,
        include_tombstones,
        limit,
    )
}

fn resolve_live_state_rows_via_overlay(
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

fn can_resolve_uncontested_single_branch_rows(
    base_rows: &[MaterializedLiveStateRow],
    staged_rows: &[MaterializedLiveStateRow],
    requested_branch_ids: &[String],
) -> bool {
    let [requested_branch_id] = requested_branch_ids else {
        return false;
    };
    staged_rows.is_empty()
        && base_rows
            .iter()
            .all(|row| !row.global && row.branch_id == *requested_branch_id)
}

/// Resolves candidates which are already scoped to one nonglobal branch.
///
/// The general overlay path builds two `BTreeMap`s and clones every row while
/// projecting it. With no global or staged candidates, tier arbitration is a
/// no-op. A stable in-place sort followed by deduplication preserves the
/// general path's identity order and "last input row wins" behavior without
/// cloning row payloads. Reversing before the stable sort makes the last input
/// candidate the first equal-key candidate retained by `dedup_by`.
fn resolve_uncontested_single_branch_rows(
    mut rows: Vec<MaterializedLiveStateRow>,
    include_tombstones: bool,
    limit: Option<usize>,
) -> Vec<MaterializedLiveStateRow> {
    rows.reverse();
    rows.sort_by(compare_row_identity);
    rows.dedup_by(|later, earlier| same_row_identity(later, earlier));
    if !include_tombstones {
        rows.retain(|row| !row.deleted);
    }
    if let Some(limit) = limit {
        rows.truncate(limit);
    }
    rows
}

fn compare_row_identity(
    left: &MaterializedLiveStateRow,
    right: &MaterializedLiveStateRow,
) -> Ordering {
    left.branch_id
        .cmp(&right.branch_id)
        .then_with(|| left.schema_key.cmp(&right.schema_key))
        .then_with(|| left.entity_pk.cmp(&right.entity_pk))
        .then_with(|| left.file_id.cmp(&right.file_id))
}

fn same_row_identity(left: &MaterializedLiveStateRow, right: &MaterializedLiveStateRow) -> bool {
    compare_row_identity(left, right) == Ordering::Equal
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
                projected.branch_id.clone_from(requested_branch_id);
                rows_by_identity.insert(LiveStateRowIdentity::from_row(&projected), projected);
            }
        }
        let mut branch_rows_by_identity =
            BTreeMap::<LiveStateRowIdentity, MaterializedLiveStateRow>::new();
        for row in rows
            .iter()
            .filter(|row| row.branch_id == *requested_branch_id)
        {
            branch_rows_by_identity.insert(LiveStateRowIdentity::from_row(row), row.clone());
        }
        rows_by_identity.extend(branch_rows_by_identity);
    }

    rows_by_identity.into_values().collect()
}

fn dedupe_rows(rows: Vec<MaterializedLiveStateRow>) -> Vec<MaterializedLiveStateRow> {
    let mut rows_by_identity = BTreeMap::<LiveStateRowIdentity, MaterializedLiveStateRow>::new();
    for row in rows {
        rows_by_identity.insert(LiveStateRowIdentity::from_row(&row), row);
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
        _ => {
            rows_by_identity.insert(identity, (tier, row));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NullableKeyFilter;
    use crate::changelog::{ChangeId, CommitId};
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
    fn empty_branch_filter_uses_last_base_row_for_duplicate_identity() {
        let mut tracked = row_at(
            "branch-a",
            "entity",
            "tracked",
            false,
            Some("change-tracked"),
        );
        tracked.untracked = false;
        let mut untracked = row_at(
            "branch-a",
            "entity",
            "untracked",
            false,
            Some("change-untracked"),
        );
        untracked.untracked = true;
        untracked.commit_id = None;

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
    fn uncontested_single_branch_fast_path_matches_overlay_semantics() {
        let rows = vec![
            row_at("branch-a", "b", "B", false, Some("change-b")),
            row_at(
                "branch-a",
                "duplicate",
                "first",
                false,
                Some("change-first"),
            ),
            tombstone_at("branch-a", "deleted", false, Some("change-deleted")),
            row_at("branch-a", "a", "A", false, Some("change-a")),
            row_at("branch-a", "duplicate", "last", false, Some("change-last")),
        ];
        let requested = vec!["branch-a".to_string()];

        let expected =
            resolve_live_state_rows_via_overlay(rows.clone(), Vec::new(), &requested, false, None);
        let actual = resolve_live_state_rows(rows, Vec::new(), &requested, false, None);

        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 3);
        assert_eq!(
            actual[2].snapshot_content.as_deref(),
            Some("{\"value\":\"last\"}")
        );
    }

    #[test]
    fn uncontested_single_branch_fast_path_applies_tombstones_and_limit_after_deduplication() {
        let rows = vec![
            row_at("branch-a", "a", "first", false, Some("change-first")),
            tombstone_at("branch-a", "a", false, Some("change-delete")),
            row_at("branch-a", "b", "B", false, Some("change-b")),
            row_at("branch-a", "c", "C", false, Some("change-c")),
        ];

        let actual = resolve_uncontested_single_branch_rows(rows, false, Some(1));

        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].entity_pk, EntityPk::single("b"));
    }

    #[test]
    fn uncontested_single_branch_fast_path_keeps_distinct_file_identities() {
        let rows = vec![
            file_row_at("branch-a", "same", "file-a", false, "schema", "file-a"),
            file_row_at("branch-a", "same", "file-b", false, "schema", "file-b"),
        ];
        let requested = vec!["branch-a".to_string()];

        let expected =
            resolve_live_state_rows_via_overlay(rows.clone(), Vec::new(), &requested, true, None);
        let actual = resolve_live_state_rows(rows, Vec::new(), &requested, true, None);

        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 2);
    }

    #[test]
    fn uncontested_single_branch_fast_path_requires_exact_scope() {
        let branch_row = row_at("branch-a", "a", "A", false, Some("change-a"));
        let global_row = row_at("global", "a", "global", true, Some("change-global"));
        let requested = vec!["branch-a".to_string()];

        assert!(can_resolve_uncontested_single_branch_rows(
            std::slice::from_ref(&branch_row),
            &[],
            &requested,
        ));
        assert!(!can_resolve_uncontested_single_branch_rows(
            std::slice::from_ref(&global_row),
            &[],
            &requested,
        ));
        assert!(!can_resolve_uncontested_single_branch_rows(
            std::slice::from_ref(&branch_row),
            std::slice::from_ref(&branch_row),
            &requested,
        ));
        assert!(!can_resolve_uncontested_single_branch_rows(
            std::slice::from_ref(&branch_row),
            &[],
            &["branch-a".to_string(), "branch-b".to_string()],
        ));
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
    fn staged_duplicate_identity_uses_last_mutation_without_tracking_lane_preference() {
        let mut tracked = row_at(
            "branch-a",
            "entity",
            "tracked",
            false,
            Some("change-tracked"),
        );
        tracked.untracked = false;
        let mut untracked = row_at(
            "branch-a",
            "entity",
            "untracked",
            false,
            Some("change-untracked"),
        );
        untracked.untracked = true;
        untracked.commit_id = None;

        let rows = resolve_live_state_rows(
            Vec::new(),
            vec![untracked.clone(), tracked.clone()],
            &["branch-a".to_string()],
            false,
            None,
        );

        assert_eq!(rows.len(), 1);
        assert!(!rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"tracked\"}")
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
    fn staged_row_replaces_base_row_for_same_visible_identity() {
        let mut base = row_at(
            "branch-a",
            "entity",
            "base-untracked",
            false,
            Some("change-base-untracked"),
        );
        base.untracked = true;
        base.commit_id = None;
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
    fn base_branch_tombstone_hides_staged_global_row_regardless_of_tracking_state() {
        let base = tombstone_at("branch-a", "entity", false, Some("change-base"));
        let mut staged = row_at("global", "entity", "staged", true, Some("change-staged"));
        staged.untracked = true;
        staged.commit_id = None;

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

    #[tokio::test]
    async fn exact_overlay_preserves_branch_global_precedence_and_tombstones() {
        let base = FilteringReader {
            rows: vec![
                row_at(
                    "branch-a",
                    "base-branch",
                    "base-branch",
                    false,
                    Some("base-branch"),
                ),
                row_at(
                    "branch-a",
                    "base-global",
                    "base-global",
                    true,
                    Some("base-global"),
                ),
                row_at(
                    "branch-a",
                    "stage-branch",
                    "base-before-stage",
                    false,
                    Some("base-before-stage"),
                ),
                row_at(
                    "branch-a",
                    "stage-delete",
                    "base-before-delete",
                    true,
                    Some("base-before-delete"),
                ),
                tombstone_at("branch-a", "base-tombstone", false, Some("base-tombstone")),
                row_at(
                    "branch-a",
                    "global-delete",
                    "global-before-delete",
                    true,
                    Some("global-before-delete"),
                ),
            ],
        };
        let staged = FilteringStagedRows {
            rows: vec![
                row_at(
                    "global",
                    "base-branch",
                    "staged-global-loses",
                    true,
                    Some("staged-global-loses"),
                ),
                row_at(
                    "global",
                    "base-global",
                    "staged-global-wins",
                    true,
                    Some("staged-global-wins"),
                ),
                row_at(
                    "branch-a",
                    "stage-branch",
                    "staged-branch-wins",
                    false,
                    Some("staged-branch-wins"),
                ),
                tombstone_at(
                    "branch-a",
                    "stage-delete",
                    false,
                    Some("staged-branch-delete"),
                ),
                row_at(
                    "global",
                    "stage-global",
                    "staged-global-only",
                    true,
                    Some("staged-global-only"),
                ),
                row_at(
                    "global",
                    "base-tombstone",
                    "staged-global-hidden",
                    true,
                    Some("staged-global-hidden"),
                ),
                tombstone_at(
                    "global",
                    "global-delete",
                    true,
                    Some("staged-global-delete"),
                ),
            ],
        };
        let exact = |entity: &str| LiveStateExactRowRequest {
            schema_key: "schema".to_string(),
            branch_id: "branch-a".to_string(),
            entity_pk: EntityPk::single(entity),
            file_id: None,
        };
        let request = LiveStateExactBatchRequest {
            rows: [
                "base-branch",
                "base-global",
                "stage-branch",
                "stage-delete",
                "stage-global",
                "base-tombstone",
                "global-delete",
            ]
            .into_iter()
            .map(exact)
            .collect(),
            ..Default::default()
        };

        let rows = overlay_load_exact_rows(&base, &staged, &request)
            .await
            .expect("exact overlay should resolve");
        let value = |index: usize| {
            rows[index]
                .as_ref()
                .and_then(|row| row.snapshot_content.as_deref())
        };
        assert_eq!(value(0), Some("{\"value\":\"base-branch\"}"));
        assert_eq!(value(1), Some("{\"value\":\"staged-global-wins\"}"));
        assert_eq!(value(2), Some("{\"value\":\"staged-branch-wins\"}"));
        assert_eq!(rows[3], None, "staged branch tombstone should hide base");
        assert_eq!(value(4), Some("{\"value\":\"staged-global-only\"}"));
        assert_eq!(rows[5], None, "base branch tombstone beats staged global");
        assert_eq!(rows[6], None, "staged global tombstone hides base global");

        let tombstone = overlay_load_exact_rows(
            &base,
            &staged,
            &LiveStateExactBatchRequest {
                rows: vec![exact("stage-delete")],
                include_tombstones: true,
                ..Default::default()
            },
        )
        .await
        .expect("exact tombstone overlay should resolve")
        .pop()
        .flatten()
        .expect("requested tombstone should be returned");
        assert!(tombstone.deleted);
        assert!(!tombstone.global);
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
            change_id: change_id.map(ChangeId::for_test_label),
            commit_id: Some(CommitId::for_test_label("commit")),
            untracked: false,
            branch_id: branch_id.to_string(),
        }
    }

    fn file_row_at(
        branch_id: &str,
        entity_pk: &str,
        value: &str,
        global: bool,
        schema_key: &str,
        file_id: &str,
    ) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            schema_key: schema_key.to_string(),
            file_id: Some(file_id.to_string()),
            ..row_at(branch_id, entity_pk, value, global, Some("change"))
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

    fn matches_scan_request(
        row: &MaterializedLiveStateRow,
        request: &LiveStateScanRequest,
    ) -> bool {
        let filter = &request.filter;
        let branch_matches =
            filter.branch_ids.is_empty() || filter.branch_ids.contains(&row.branch_id);
        let schema_matches =
            filter.schema_keys.is_empty() || filter.schema_keys.contains(&row.schema_key);
        let entity_matches =
            filter.entity_pks.is_empty() || filter.entity_pks.contains(&row.entity_pk);
        let file_matches = filter.file_ids.is_empty()
            || filter.file_ids.iter().any(|file_id| match file_id {
                NullableKeyFilter::Any => true,
                NullableKeyFilter::Value(file_id) => row.file_id.as_ref() == Some(file_id),
                NullableKeyFilter::Null => row.file_id.is_none(),
            });
        let tombstone_matches = filter.include_tombstones || !row.deleted;
        branch_matches && schema_matches && entity_matches && file_matches && tombstone_matches
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

    struct FilteringStagedRows {
        rows: Vec<MaterializedLiveStateRow>,
    }

    impl StagedLiveStateRows for FilteringStagedRows {
        fn staged_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(self
                .rows
                .iter()
                .filter(|row| matches_scan_request(row, request))
                .cloned()
                .collect())
        }
    }

    struct ExistingGlobalOnlyReader {
        rows: Vec<MaterializedLiveStateRow>,
    }

    #[async_trait]
    impl LiveStateReader for ExistingGlobalOnlyReader {
        async fn load_exact_rows(
            &self,
            request: &LiveStateExactBatchRequest,
        ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
            crate::live_state::load_exact_rows_via_scan_for_test(self, request).await
        }

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

    struct FilteringReader {
        rows: Vec<MaterializedLiveStateRow>,
    }

    #[async_trait]
    impl LiveStateReader for FilteringReader {
        async fn load_exact_rows(
            &self,
            request: &LiveStateExactBatchRequest,
        ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
            crate::live_state::load_exact_rows_via_scan_for_test(self, request).await
        }

        async fn scan_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(self
                .rows
                .iter()
                .filter(|row| matches_scan_request(row, request))
                .cloned()
                .collect())
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(None)
        }
    }
}
