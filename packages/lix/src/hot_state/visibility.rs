use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
#[cfg(test)]
use crate::hot_state::MaterializedHotStateRow;
use crate::hot_state::{
    HotStateExactBatchRequest, HotStateExactRowRequest, HotStateReader, HotStateRowIdentityRef,
    HotStateScanRequest, MaterializedHotStateBatch, MaterializedHotStateBatchBuilder,
    MaterializedHotStateExactBatch, MaterializedHotStateRowRef,
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

pub(crate) trait StagedHotStateRows {
    /// Returns staged candidates in one shared columnar owner.
    fn staged_batch(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<MaterializedHotStateBatch, LixError>;

    /// Loads exact staged storage identities in request order.
    ///
    /// This does not apply global fallback: overlay composition needs the
    /// branch and global candidates separately to preserve their precedence.
    fn load_exact_batch(
        &self,
        request: &HotStateExactBatchRequest,
    ) -> Result<MaterializedHotStateExactBatch, LixError>;

    /// Whether this transaction has replaced the requested collection with a
    /// generation marker. Collection replacement suppresses committed members
    /// without expanding them into staged row tombstones.
    fn collection_replaced(
        &self,
        branch_id: &str,
        schema_key: &str,
        file_id: Option<&str>,
    ) -> Result<bool, LixError> {
        let _ = (branch_id, schema_key, file_id);
        Ok(false)
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

pub(crate) fn resolve_visible_batch(
    base_rows: MaterializedHotStateBatch,
    staged_rows: MaterializedHotStateBatch,
    request: &VisibilityRequest,
) -> MaterializedHotStateBatch {
    let requested_branch_ids = requested_branch_ids(&request.branch_scope);
    if staged_rows.is_empty()
        && request.limit.is_none_or(|limit| base_rows.len() <= limit)
        && base_rows.iter().all(|row| {
            (request.include_tombstones || !row.deleted())
                && (requested_branch_ids.is_empty()
                    || (!row.global()
                        && requested_branch_ids
                            .iter()
                            .any(|branch_id| branch_id == row.branch_id())))
        })
        && base_rows
            .iter()
            .map(materialized_row_identity)
            .is_sorted_by(|left, right| left < right)
    {
        return base_rows;
    }
    resolve_hot_state_batch(
        &base_rows,
        &staged_rows,
        &requested_branch_ids,
        request.include_tombstones,
        request.limit,
    )
}

fn materialized_row_identity(row: MaterializedHotStateRowRef<'_>) -> HotStateRowIdentityRef<'_> {
    HotStateRowIdentityRef {
        branch_id: row.branch_id(),
        schema_key: row.schema_key(),
        entity_pk: row.entity_pk(),
        file_id: row.file_id(),
    }
}

pub(crate) async fn overlay_scan_batch<S>(
    base: &dyn HotStateReader,
    staged: &S,
    request: &HotStateScanRequest,
) -> Result<MaterializedHotStateBatch, LixError>
where
    S: StagedHotStateRows + ?Sized,
{
    let mut visible_branch_ids = request.filter.branch_ids.clone();
    if let [schema_key] = request.filter.schema_keys.as_slice() {
        let mut retained = Vec::with_capacity(visible_branch_ids.len());
        for branch_id in visible_branch_ids {
            if branch_id == GLOBAL_BRANCH_ID
                || !staged.collection_replaced(&branch_id, schema_key, None)?
            {
                retained.push(branch_id);
            }
        }
        visible_branch_ids = retained;
    }
    if !request.filter.branch_ids.is_empty() && visible_branch_ids.is_empty() {
        return Ok(MaterializedHotStateBatch::default());
    }
    let mut candidate_request = request.clone();
    candidate_request.limit = None;
    candidate_request.filter.include_tombstones = true;
    candidate_request.filter.branch_ids = expanded_branch_ids(&visible_branch_ids);
    let staged_rows = staged.staged_batch(&candidate_request)?;
    let rows = base.scan_batch(&candidate_request).await?;
    Ok(resolve_visible_batch(
        rows,
        staged_rows,
        &VisibilityRequest {
            branch_scope: VisibilityBranchScope::BranchIds {
                branch_ids: visible_branch_ids,
            },
            include_tombstones: request.filter.include_tombstones,
            limit: request.limit,
        },
    ))
}

pub(crate) async fn overlay_scan_tracked_batch<S>(
    base: &dyn HotStateReader,
    staged: &S,
    request: &HotStateScanRequest,
) -> Result<MaterializedHotStateBatch, LixError>
where
    S: StagedHotStateRows + ?Sized,
{
    let mut candidate_request = request.clone();
    candidate_request.limit = None;
    candidate_request.filter.include_tombstones = true;
    candidate_request.filter.branch_ids = expanded_branch_ids(&request.filter.branch_ids);
    candidate_request.filter.untracked = Some(false);
    let staged_rows = staged.staged_batch(&candidate_request)?;
    let rows = base.scan_tracked_batch(&candidate_request).await?;
    Ok(resolve_visible_batch(
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
pub(crate) async fn overlay_load_exact_batch<S>(
    base: &dyn HotStateReader,
    staged: &S,
    request: &HotStateExactBatchRequest,
) -> Result<MaterializedHotStateExactBatch, LixError>
where
    S: StagedHotStateRows + ?Sized,
{
    if request.rows.is_empty() {
        return Ok(MaterializedHotStateExactBatch::default());
    }

    let mut base_request = request.clone();
    base_request.include_tombstones = true;
    let base_rows = base.load_exact_batch(&base_request).await?;
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
        staged_requests.push(HotStateExactRowRequest {
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
    let staged_request = HotStateExactBatchRequest {
        rows: staged_requests,
        projection: request.projection.clone(),
        untracked: request.untracked,
        include_tombstones: true,
    };
    let staged_rows = staged.load_exact_batch(&staged_request)?;
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

    let mut builder = MaterializedHotStateBatchBuilder::with_capacity(request.rows.len());
    let mut slots = Vec::with_capacity(request.rows.len());
    for (slot, (requested, (global_index, branch_index))) in
        request.rows.iter().zip(staged_indices).enumerate()
    {
        if requested.branch_id != GLOBAL_BRANCH_ID
            && staged.collection_replaced(
                &requested.branch_id,
                &requested.schema_key,
                requested.file_id.as_deref(),
            )?
        {
            slots.push(None);
            continue;
        }
        let mut winner = base_rows.row(slot).map(|row| {
            let tier = if row.global() {
                OverlayTier::BaseGlobal
            } else {
                OverlayTier::BaseBranch
            };
            (tier, row, None)
        });
        if let Some(row) = staged_rows.row(global_index) {
            let branch_override =
                (requested.branch_id != GLOBAL_BRANCH_ID).then_some(requested.branch_id.as_str());
            insert_exact_overlay_candidate(
                &mut winner,
                OverlayTier::StagedGlobal,
                row,
                branch_override,
            );
        }
        if let Some(index) = branch_index
            && let Some(row) = staged_rows.row(index)
        {
            insert_exact_overlay_candidate(&mut winner, OverlayTier::StagedBranch, row, None);
        }
        let Some((_, row, branch_override)) = winner else {
            slots.push(None);
            continue;
        };
        if row.deleted() && !request.include_tombstones {
            slots.push(None);
        } else {
            let ordinal = u32::try_from(builder.push_ref(row, branch_override)).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "exact live-state overlay exceeds u32 rows",
                )
            })?;
            slots.push(Some(ordinal));
        }
    }
    MaterializedHotStateExactBatch::new(builder.finish(), slots)
}

fn insert_exact_overlay_candidate<'a>(
    winner: &mut Option<(OverlayTier, MaterializedHotStateRowRef<'a>, Option<&'a str>)>,
    tier: OverlayTier,
    row: MaterializedHotStateRowRef<'a>,
    branch_override: Option<&'a str>,
) {
    if winner
        .as_ref()
        .is_none_or(|(existing_tier, _, _)| *existing_tier <= tier)
    {
        *winner = Some((tier, row, branch_override));
    }
}

#[derive(Clone, Copy)]
struct OverlayCandidate<'a> {
    row: MaterializedHotStateRowRef<'a>,
    branch_id: &'a str,
    tier: OverlayTier,
    sequence: usize,
}

impl<'a> OverlayCandidate<'a> {
    fn identity(self) -> HotStateRowIdentityRef<'a> {
        HotStateRowIdentityRef {
            branch_id: self.branch_id,
            ..materialized_row_identity(self.row)
        }
    }
}

/// Resolves visibility by sorting borrowed row ordinals.
///
/// The temporary vector carries one row view, effective branch view, and tier
/// per candidate. No identity field is cloned into a map. The winning rows are
/// then lowered directly into one dictionary-backed result batch.
fn resolve_hot_state_batch<'a>(
    base_rows: &'a MaterializedHotStateBatch,
    staged_rows: &'a MaterializedHotStateBatch,
    requested_branch_ids: &'a [String],
    include_tombstones: bool,
    limit: Option<usize>,
) -> MaterializedHotStateBatch {
    let capacity = projected_candidate_count(base_rows, requested_branch_ids)
        .checked_add(projected_candidate_count(staged_rows, requested_branch_ids))
        .expect("live-state candidate count overflow");
    let mut candidates = Vec::with_capacity(capacity);
    append_projected_candidates(
        &mut candidates,
        base_rows,
        requested_branch_ids,
        OverlayTier::BaseGlobal,
        OverlayTier::BaseBranch,
    );
    append_projected_candidates(
        &mut candidates,
        staged_rows,
        requested_branch_ids,
        OverlayTier::StagedGlobal,
        OverlayTier::StagedBranch,
    );
    debug_assert_eq!(candidates.len(), capacity);

    candidates.sort_unstable_by(|left, right| {
        left.identity()
            .cmp(&right.identity())
            .then_with(|| left.sequence.cmp(&right.sequence))
    });

    let output_capacity = limit.map_or(capacity, |limit| limit.min(capacity));
    let mut output = MaterializedHotStateBatchBuilder::with_capacity(output_capacity);
    let mut offset = 0;
    while offset < candidates.len() && output.len() < output_capacity {
        let mut end = offset + 1;
        let mut winner = candidates[offset];
        while end < candidates.len() && candidates[offset].identity() == candidates[end].identity()
        {
            let candidate = candidates[end];
            if winner.tier <= candidate.tier {
                winner = candidate;
            }
            end += 1;
        }
        if include_tombstones || !winner.row.deleted() {
            let branch_override =
                (winner.branch_id != winner.row.branch_id()).then_some(winner.branch_id);
            output.push_ref(winner.row, branch_override);
        }
        offset = end;
    }
    output.finish()
}

fn projected_candidate_count(
    rows: &MaterializedHotStateBatch,
    requested_branch_ids: &[String],
) -> usize {
    if requested_branch_ids.is_empty() {
        return rows.len();
    }
    requested_branch_ids
        .iter()
        .map(|requested_branch_id| {
            rows.iter()
                .filter(|row| {
                    row.branch_id() == GLOBAL_BRANCH_ID
                        || row.branch_id() == requested_branch_id.as_str()
                })
                .count()
        })
        .sum()
}

fn append_projected_candidates<'a>(
    candidates: &mut Vec<OverlayCandidate<'a>>,
    rows: &'a MaterializedHotStateBatch,
    requested_branch_ids: &'a [String],
    global_tier: OverlayTier,
    branch_tier: OverlayTier,
) {
    if requested_branch_ids.is_empty() {
        for row in rows.iter() {
            let sequence = candidates.len();
            candidates.push(OverlayCandidate {
                row,
                branch_id: row.branch_id(),
                tier: if row.global() {
                    global_tier
                } else {
                    branch_tier
                },
                sequence,
            });
        }
        return;
    }
    for requested_branch_id in requested_branch_ids {
        for row in rows.iter() {
            if row.branch_id() == GLOBAL_BRANCH_ID {
                let sequence = candidates.len();
                candidates.push(OverlayCandidate {
                    row,
                    branch_id: requested_branch_id,
                    tier: global_tier,
                    sequence,
                });
            } else if row.branch_id() == requested_branch_id {
                let sequence = candidates.len();
                candidates.push(OverlayCandidate {
                    row,
                    branch_id: row.branch_id(),
                    tier: if row.global() {
                        global_tier
                    } else {
                        branch_tier
                    },
                    sequence,
                });
            }
        }
    }
}

fn requested_branch_ids(branch_scope: &VisibilityBranchScope) -> Vec<String> {
    match branch_scope {
        VisibilityBranchScope::BranchIds { branch_ids } => branch_ids.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NullableKeyFilter;
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::entity_pk::EntityPk;

    use async_trait::async_trait;

    fn test_timestamp() -> LixTimestamp {
        LixTimestamp::expect_parse("test timestamp", "2026-01-01T00:00:00Z")
    }

    #[test]
    fn expands_requested_branch_with_global_candidates() {
        assert_eq!(
            expanded_branch_ids(&["01920000-0000-7000-8000-0000000000a1".to_string()]),
            vec![
                "01920000-0000-7000-8000-0000000000a1".to_string(),
                "ffffffff-ffff-7fff-bfff-ffffffffffff".to_string()
            ]
        );
        assert_eq!(
            expanded_branch_ids(&["ffffffff-ffff-7fff-bfff-ffffffffffff".to_string()]),
            vec!["ffffffff-ffff-7fff-bfff-ffffffffffff".to_string()]
        );
    }

    #[test]
    fn ten_thousand_row_visibility_keeps_identity_metadata_dictionary_encoded() {
        let branch_id = "01920000-0000-7000-8000-0000000000a1";
        let rows = (0..10_000)
            .map(|index| MaterializedHotStateRow {
                entity_pk: EntityPk::uuid_from_canonical(&format!(
                    "01920000-0000-7000-8000-{index:012x}"
                ))
                .expect("canonical test UUID"),
                schema_key: "shared_schema".to_owned(),
                file_id: Some("shared_file".to_owned()),
                snapshot_content: Some(crate::common::SharedStr::from_static("{}")),
                metadata: None,
                deleted: false,
                created_at: test_timestamp(),
                updated_at: test_timestamp(),
                global: false,
                change_id: None,
                commit_id: None,
                untracked: true,
                branch_id: branch_id.into(),
            })
            .collect();
        let source = MaterializedHotStateBatch::from_rows(rows);
        let source_entity_column = source.entity_column_ptr();
        let batch = resolve_visible_batch(
            source,
            MaterializedHotStateBatch::default(),
            &VisibilityRequest {
                branch_scope: VisibilityBranchScope::BranchIds {
                    branch_ids: vec![branch_id.to_owned()],
                },
                include_tombstones: false,
                limit: None,
            },
        );

        assert_eq!(batch.len(), 10_000);
        assert_eq!(batch.entity_column_ptr(), source_entity_column);
        assert_eq!(batch.dictionary_entry_count(), 3);
        assert_eq!(batch.row(0).schema_key(), batch.row(9_999).schema_key());
        assert_eq!(
            batch.row(0).schema_key().as_ptr(),
            batch.row(9_999).schema_key().as_ptr()
        );
    }

    #[test]
    fn committed_scan_projects_global_row_into_requested_branch() {
        let rows = resolve_hot_state_batch(
            &MaterializedHotStateBatch::from_rows(vec![row_at(
                "ffffffff-ffff-7fff-bfff-ffffffffffff",
                "entity",
                "global-value",
                true,
                Some("change-global"),
            )]),
            &MaterializedHotStateBatch::default(),
            &["01920000-0000-7000-8000-0000000000a1".to_string()],
            false,
            None,
        )
        .into_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].branch_id.as_ref(),
            "01920000-0000-7000-8000-0000000000a1"
        );
        assert!(rows[0].global);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"global-value\"}")
        );
    }

    #[test]
    fn committed_scan_prefers_requested_branch_row_over_projected_global_row() {
        let rows = resolve_hot_state_batch(
            &MaterializedHotStateBatch::from_rows(vec![
                row_at(
                    "ffffffff-ffff-7fff-bfff-ffffffffffff",
                    "entity",
                    "global-value",
                    true,
                    Some("change-global"),
                ),
                row_at(
                    "01920000-0000-7000-8000-0000000000a1",
                    "entity",
                    "branch-value",
                    false,
                    Some("change-branch"),
                ),
            ]),
            &MaterializedHotStateBatch::default(),
            &["01920000-0000-7000-8000-0000000000a1".to_string()],
            false,
            None,
        )
        .into_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].branch_id.as_ref(),
            "01920000-0000-7000-8000-0000000000a1"
        );
        assert!(!rows[0].global);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"branch-value\"}")
        );
    }

    #[test]
    fn empty_branch_filter_uses_last_base_row_for_duplicate_identity() {
        let mut tracked = row_at(
            "01920000-0000-7000-8000-0000000000a1",
            "entity",
            "tracked",
            false,
            Some("change-tracked"),
        );
        tracked.untracked = false;
        let mut untracked = row_at(
            "01920000-0000-7000-8000-0000000000a1",
            "entity",
            "untracked",
            false,
            Some("change-untracked"),
        );
        untracked.untracked = true;
        untracked.commit_id = None;

        let rows = resolve_hot_state_batch(
            &MaterializedHotStateBatch::from_rows(vec![tracked, untracked]),
            &MaterializedHotStateBatch::default(),
            &[],
            false,
            None,
        )
        .into_rows();

        assert_eq!(rows.len(), 1);
        assert!(rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"untracked\"}")
        );
    }

    #[test]
    fn empty_branch_filter_dedupes_duplicate_base_and_staged_overlay_identity() {
        let base = row_at(
            "01920000-0000-7000-8000-0000000000a1",
            "entity",
            "base",
            false,
            Some("change-base"),
        );
        let staged = row_at(
            "01920000-0000-7000-8000-0000000000a1",
            "entity",
            "staged",
            false,
            Some("change-staged"),
        );

        let rows = resolve_hot_state_batch(
            &MaterializedHotStateBatch::from_rows(vec![base]),
            &MaterializedHotStateBatch::from_rows(vec![staged]),
            &[],
            false,
            None,
        )
        .into_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"staged\"}")
        );
    }

    #[test]
    fn branch_tombstone_hides_global_row_after_visibility_resolution() {
        let rows = resolve_hot_state_batch(
            &MaterializedHotStateBatch::from_rows(vec![
                row_at(
                    "ffffffff-ffff-7fff-bfff-ffffffffffff",
                    "entity",
                    "global-value",
                    true,
                    Some("change-global"),
                ),
                tombstone_at(
                    "01920000-0000-7000-8000-0000000000a1",
                    "entity",
                    false,
                    Some("change-tombstone"),
                ),
            ]),
            &MaterializedHotStateBatch::default(),
            &["01920000-0000-7000-8000-0000000000a1".to_string()],
            false,
            None,
        )
        .into_rows();

        assert!(rows.is_empty());
    }

    #[test]
    fn staged_duplicate_identity_uses_last_mutation_without_tracking_lane_preference() {
        let mut tracked = row_at(
            "01920000-0000-7000-8000-0000000000a1",
            "entity",
            "tracked",
            false,
            Some("change-tracked"),
        );
        tracked.untracked = false;
        let mut untracked = row_at(
            "01920000-0000-7000-8000-0000000000a1",
            "entity",
            "untracked",
            false,
            Some("change-untracked"),
        );
        untracked.untracked = true;
        untracked.commit_id = None;

        let rows = resolve_hot_state_batch(
            &MaterializedHotStateBatch::default(),
            &MaterializedHotStateBatch::from_rows(vec![untracked.clone(), tracked.clone()]),
            &["01920000-0000-7000-8000-0000000000a1".to_string()],
            false,
            None,
        )
        .into_rows();

        assert_eq!(rows.len(), 1);
        assert!(!rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"tracked\"}")
        );

        let rows = resolve_hot_state_batch(
            &MaterializedHotStateBatch::default(),
            &MaterializedHotStateBatch::from_rows(vec![tracked, untracked]),
            &["01920000-0000-7000-8000-0000000000a1".to_string()],
            false,
            None,
        )
        .into_rows();

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
            "01920000-0000-7000-8000-0000000000a1",
            "entity",
            "base-untracked",
            false,
            Some("change-base-untracked"),
        );
        base.untracked = true;
        base.commit_id = None;
        let mut staged = row_at(
            "01920000-0000-7000-8000-0000000000a1",
            "entity",
            "staged-tracked",
            false,
            Some("change-staged"),
        );
        staged.untracked = false;

        let rows = resolve_hot_state_batch(
            &MaterializedHotStateBatch::from_rows(vec![base]),
            &MaterializedHotStateBatch::from_rows(vec![staged]),
            &["01920000-0000-7000-8000-0000000000a1".to_string()],
            false,
            None,
        )
        .into_rows();

        assert_eq!(rows.len(), 1);
        assert!(!rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"staged-tracked\"}")
        );
    }

    #[test]
    fn staged_global_tombstone_hides_projected_base_global_row() {
        let mut base = row_at(
            "01920000-0000-7000-8000-0000000000a1",
            "entity",
            "base",
            true,
            Some("change-base"),
        );
        base.global = true;

        let rows = resolve_hot_state_batch(
            &MaterializedHotStateBatch::from_rows(vec![base]),
            &MaterializedHotStateBatch::from_rows(vec![tombstone_at(
                "ffffffff-ffff-7fff-bfff-ffffffffffff",
                "entity",
                true,
                Some("change-staged"),
            )]),
            &["01920000-0000-7000-8000-0000000000a1".to_string()],
            false,
            None,
        )
        .into_rows();

        assert!(rows.is_empty());
    }

    #[test]
    fn base_branch_tombstone_hides_staged_global_row() {
        let base = tombstone_at(
            "01920000-0000-7000-8000-0000000000a1",
            "entity",
            false,
            Some("change-base"),
        );
        let staged = row_at(
            "ffffffff-ffff-7fff-bfff-ffffffffffff",
            "entity",
            "staged",
            true,
            Some("change-staged"),
        );

        let rows = resolve_hot_state_batch(
            &MaterializedHotStateBatch::from_rows(vec![base]),
            &MaterializedHotStateBatch::from_rows(vec![staged]),
            &["01920000-0000-7000-8000-0000000000a1".to_string()],
            false,
            None,
        )
        .into_rows();

        assert!(rows.is_empty());
    }

    #[test]
    fn base_branch_tombstone_hides_staged_global_row_regardless_of_tracking_state() {
        let base = tombstone_at(
            "01920000-0000-7000-8000-0000000000a1",
            "entity",
            false,
            Some("change-base"),
        );
        let mut staged = row_at(
            "ffffffff-ffff-7fff-bfff-ffffffffffff",
            "entity",
            "staged",
            true,
            Some("change-staged"),
        );
        staged.untracked = true;
        staged.commit_id = None;

        let rows = resolve_hot_state_batch(
            &MaterializedHotStateBatch::from_rows(vec![base]),
            &MaterializedHotStateBatch::from_rows(vec![staged]),
            &["01920000-0000-7000-8000-0000000000a1".to_string()],
            false,
            None,
        )
        .into_rows();

        assert!(rows.is_empty());
    }

    #[test]
    fn staged_branch_row_overrides_base_branch_tombstone() {
        let base = tombstone_at(
            "01920000-0000-7000-8000-0000000000a1",
            "entity",
            false,
            Some("change-base"),
        );
        let staged = row_at(
            "01920000-0000-7000-8000-0000000000a1",
            "entity",
            "staged",
            false,
            Some("change-staged"),
        );

        let rows = resolve_hot_state_batch(
            &MaterializedHotStateBatch::from_rows(vec![base]),
            &MaterializedHotStateBatch::from_rows(vec![staged]),
            &["01920000-0000-7000-8000-0000000000a1".to_string()],
            false,
            None,
        )
        .into_rows();

        assert_eq!(rows.len(), 1);
        assert!(!rows[0].deleted);
    }

    #[test]
    fn tombstone_can_be_returned_when_requested() {
        let rows = resolve_hot_state_batch(
            &MaterializedHotStateBatch::from_rows(vec![
                row_at(
                    "ffffffff-ffff-7fff-bfff-ffffffffffff",
                    "entity",
                    "global-value",
                    true,
                    Some("change-global"),
                ),
                tombstone_at(
                    "01920000-0000-7000-8000-0000000000a1",
                    "entity",
                    false,
                    Some("change-tombstone"),
                ),
            ]),
            &MaterializedHotStateBatch::default(),
            &["01920000-0000-7000-8000-0000000000a1".to_string()],
            true,
            None,
        )
        .into_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].branch_id.as_ref(),
            "01920000-0000-7000-8000-0000000000a1"
        );
        assert_eq!(rows[0].snapshot_content, None);
    }

    #[test]
    fn resolve_visible_batch_maps_branch_scope_and_applies_limit() {
        let request = VisibilityRequest {
            branch_scope: VisibilityBranchScope::BranchIds {
                branch_ids: vec!["01920000-0000-7000-8000-0000000000a1".to_string()],
            },
            include_tombstones: false,
            limit: Some(1),
        };
        let rows = resolve_visible_batch(
            MaterializedHotStateBatch::from_rows(vec![
                row_at(
                    "01920000-0000-7000-8000-0000000000a1",
                    "a",
                    "A",
                    false,
                    Some("change-a"),
                ),
                row_at(
                    "01920000-0000-7000-8000-0000000000a1",
                    "b",
                    "B",
                    false,
                    Some("change-b"),
                ),
            ]),
            MaterializedHotStateBatch::default(),
            &request,
        )
        .into_rows();

        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn overlay_scan_fetches_base_global_candidates_for_staged_only_branch_scope() {
        let base = ExistingGlobalOnlyReader {
            rows: vec![row_at(
                "ffffffff-ffff-7fff-bfff-ffffffffffff",
                "entity",
                "global-value",
                true,
                Some("change-global"),
            )],
        };
        let staged = EmptyStagedRows;

        let rows = overlay_scan_batch(
            &base,
            &staged,
            &HotStateScanRequest {
                filter: crate::hot_state::HotStateFilter {
                    branch_ids: vec!["staged-branch".to_string()],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .expect("overlay scan should succeed")
        .into_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].branch_id.as_ref(), "staged-branch");
        assert!(rows[0].global);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"value\":\"global-value\"}")
        );
    }

    #[tokio::test]
    async fn overlay_scan_replacement_hides_only_its_branch() {
        let replaced_branch = "01920000-0000-7000-8000-0000000000a1";
        let unaffected_branch = "01920000-0000-7000-8000-0000000000a2";
        let base = FilteringReader {
            rows: vec![
                row_at(
                    replaced_branch,
                    "replaced",
                    "replaced-value",
                    false,
                    Some("change-replaced"),
                ),
                row_at(
                    unaffected_branch,
                    "unaffected",
                    "unaffected-value",
                    false,
                    Some("change-unaffected"),
                ),
            ],
        };
        let staged = ReplacedBranchStagedRows {
            replaced_branch,
            schema_key: "schema",
        };

        let rows = overlay_scan_batch(
            &base,
            &staged,
            &HotStateScanRequest {
                filter: crate::hot_state::HotStateFilter {
                    branch_ids: vec![replaced_branch.to_string(), unaffected_branch.to_string()],
                    schema_keys: vec!["schema".to_string()],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .expect("multi-branch replacement overlay should resolve")
        .into_rows();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].branch_id.as_ref(), unaffected_branch);
        assert_eq!(rows[0].entity_pk, EntityPk::single("unaffected"));
    }

    #[tokio::test]
    async fn exact_overlay_preserves_branch_global_precedence_and_tombstones() {
        let base = FilteringReader {
            rows: vec![
                row_at(
                    "01920000-0000-7000-8000-0000000000a1",
                    "base-branch",
                    "base-branch",
                    false,
                    Some("base-branch"),
                ),
                row_at(
                    "01920000-0000-7000-8000-0000000000a1",
                    "base-global",
                    "base-global",
                    true,
                    Some("base-global"),
                ),
                row_at(
                    "01920000-0000-7000-8000-0000000000a1",
                    "stage-branch",
                    "base-before-stage",
                    false,
                    Some("base-before-stage"),
                ),
                row_at(
                    "01920000-0000-7000-8000-0000000000a1",
                    "stage-delete",
                    "base-before-delete",
                    true,
                    Some("base-before-delete"),
                ),
                tombstone_at(
                    "01920000-0000-7000-8000-0000000000a1",
                    "base-tombstone",
                    false,
                    Some("base-tombstone"),
                ),
                row_at(
                    "01920000-0000-7000-8000-0000000000a1",
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
                    "ffffffff-ffff-7fff-bfff-ffffffffffff",
                    "base-branch",
                    "staged-global-loses",
                    true,
                    Some("staged-global-loses"),
                ),
                row_at(
                    "ffffffff-ffff-7fff-bfff-ffffffffffff",
                    "base-global",
                    "staged-global-wins",
                    true,
                    Some("staged-global-wins"),
                ),
                row_at(
                    "01920000-0000-7000-8000-0000000000a1",
                    "stage-branch",
                    "staged-branch-wins",
                    false,
                    Some("staged-branch-wins"),
                ),
                tombstone_at(
                    "01920000-0000-7000-8000-0000000000a1",
                    "stage-delete",
                    false,
                    Some("staged-branch-delete"),
                ),
                row_at(
                    "ffffffff-ffff-7fff-bfff-ffffffffffff",
                    "stage-global",
                    "staged-global-only",
                    true,
                    Some("staged-global-only"),
                ),
                row_at(
                    "ffffffff-ffff-7fff-bfff-ffffffffffff",
                    "base-tombstone",
                    "staged-global-hidden",
                    true,
                    Some("staged-global-hidden"),
                ),
                tombstone_at(
                    "ffffffff-ffff-7fff-bfff-ffffffffffff",
                    "global-delete",
                    true,
                    Some("staged-global-delete"),
                ),
            ],
        };
        let exact = |entity: &str| HotStateExactRowRequest {
            schema_key: "schema".to_string(),
            branch_id: "01920000-0000-7000-8000-0000000000a1".to_string(),
            entity_pk: EntityPk::single(entity),
            file_id: None,
        };
        let request = HotStateExactBatchRequest {
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

        let rows = overlay_load_exact_batch(&base, &staged, &request)
            .await
            .expect("exact overlay should resolve")
            .into_rows();
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

        let tombstone = overlay_load_exact_batch(
            &base,
            &staged,
            &HotStateExactBatchRequest {
                rows: vec![exact("stage-delete")],
                include_tombstones: true,
                ..Default::default()
            },
        )
        .await
        .expect("exact tombstone overlay should resolve")
        .into_rows()
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
    ) -> MaterializedHotStateRow {
        MaterializedHotStateRow {
            entity_pk: EntityPk::single(entity_pk),
            schema_key: "schema".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}").into()),
            metadata: None,
            deleted: false,
            created_at: test_timestamp(),
            updated_at: test_timestamp(),
            global,
            change_id: change_id.map(ChangeId::for_test_label),
            commit_id: Some(CommitId::for_test_label("commit")),
            untracked: false,
            branch_id: branch_id.into(),
        }
    }

    fn tombstone_at(
        branch_id: &str,
        entity_pk: &str,
        global: bool,
        change_id: Option<&str>,
    ) -> MaterializedHotStateRow {
        MaterializedHotStateRow {
            snapshot_content: None,
            deleted: true,
            ..row_at(branch_id, entity_pk, "ignored", global, change_id)
        }
    }

    fn matches_scan_request(row: &MaterializedHotStateRow, request: &HotStateScanRequest) -> bool {
        let filter = &request.filter;
        let branch_matches = filter.branch_ids.is_empty()
            || filter
                .branch_ids
                .iter()
                .any(|branch_id| branch_id == row.branch_id.as_ref());
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

    impl StagedHotStateRows for EmptyStagedRows {
        fn staged_batch(
            &self,
            _request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            Ok(MaterializedHotStateBatch::default())
        }

        fn load_exact_batch(
            &self,
            request: &HotStateExactBatchRequest,
        ) -> Result<MaterializedHotStateExactBatch, LixError> {
            MaterializedHotStateExactBatch::new(
                MaterializedHotStateBatch::default(),
                vec![None; request.rows.len()],
            )
        }
    }

    struct ReplacedBranchStagedRows<'a> {
        replaced_branch: &'a str,
        schema_key: &'a str,
    }

    impl StagedHotStateRows for ReplacedBranchStagedRows<'_> {
        fn staged_batch(
            &self,
            _request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            Ok(MaterializedHotStateBatch::default())
        }

        fn load_exact_batch(
            &self,
            request: &HotStateExactBatchRequest,
        ) -> Result<MaterializedHotStateExactBatch, LixError> {
            MaterializedHotStateExactBatch::new(
                MaterializedHotStateBatch::default(),
                vec![None; request.rows.len()],
            )
        }

        fn collection_replaced(
            &self,
            branch_id: &str,
            schema_key: &str,
            file_id: Option<&str>,
        ) -> Result<bool, LixError> {
            Ok(branch_id == self.replaced_branch
                && schema_key == self.schema_key
                && file_id.is_none())
        }
    }

    struct FilteringStagedRows {
        rows: Vec<MaterializedHotStateRow>,
    }

    impl StagedHotStateRows for FilteringStagedRows {
        fn staged_batch(
            &self,
            request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            Ok(MaterializedHotStateBatch::from_rows(
                self.rows
                    .iter()
                    .filter(|row| matches_scan_request(row, request))
                    .cloned()
                    .collect(),
            ))
        }

        fn load_exact_batch(
            &self,
            request: &HotStateExactBatchRequest,
        ) -> Result<MaterializedHotStateExactBatch, LixError> {
            Ok(MaterializedHotStateExactBatch::from_rows(
                request
                    .rows
                    .iter()
                    .map(|request_row| {
                        self.rows
                            .iter()
                            .find(|row| {
                                matches_scan_request(row, &request.row_scan_request(request_row))
                            })
                            .cloned()
                    })
                    .collect(),
            ))
        }
    }

    struct ExistingGlobalOnlyReader {
        rows: Vec<MaterializedHotStateRow>,
    }

    #[async_trait]
    impl HotStateReader for ExistingGlobalOnlyReader {
        async fn load_exact_batch(
            &self,
            request: &HotStateExactBatchRequest,
        ) -> Result<MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            if request
                .filter
                .branch_ids
                .iter()
                .any(|branch_id| branch_id == GLOBAL_BRANCH_ID)
            {
                Ok(self.rows.clone().into())
            } else {
                Ok(Vec::new().into())
            }
        }
    }

    struct FilteringReader {
        rows: Vec<MaterializedHotStateRow>,
    }

    #[async_trait]
    impl HotStateReader for FilteringReader {
        async fn load_exact_batch(
            &self,
            request: &HotStateExactBatchRequest,
        ) -> Result<MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            Ok(MaterializedHotStateBatch::from_rows(
                self.rows
                    .iter()
                    .filter(|row| matches_scan_request(row, request))
                    .cloned()
                    .collect(),
            ))
        }
    }
}
