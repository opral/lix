use crate::canonical::{
    load_history, CanonicalHistoryContentMode, CanonicalHistoryRequest,
    CanonicalHistoryRootSelection, CanonicalRootCommit,
};
use crate::{LixBackend, LixError};

use super::roots::{
    resolve_history_root_facts_with_backend, HistoryRootFacts, HistoryRootTraversal,
    RootCommitResolutionRequest, RootCommitScope, RootLineageScope, RootVersionScope,
};
use super::{
    StateHistoryContentMode, StateHistoryLineageScope, StateHistoryOrder, StateHistoryRequest,
    StateHistoryRootScope, StateHistoryRow, StateHistoryVersionScope,
};

pub(crate) async fn load_state_history_rows(
    backend: &dyn LixBackend,
    request: &StateHistoryRequest,
) -> Result<Vec<StateHistoryRow>, LixError> {
    let root_facts =
        resolve_history_root_facts_with_backend(backend, root_commit_resolution_request(request))
            .await?;
    let mut rows = load_history(backend, &canonical_history_request(request, root_facts)).await?;

    match request.order {
        StateHistoryOrder::EntityFileSchemaDepthAsc => rows.sort_by(|left, right| {
            left.entity_id
                .cmp(&right.entity_id)
                .then_with(|| left.file_id.cmp(&right.file_id))
                .then_with(|| left.schema_key.cmp(&right.schema_key))
                .then_with(|| left.depth.cmp(&right.depth))
        }),
    }

    Ok(rows
        .into_iter()
        .map(|row| StateHistoryRow {
            entity_id: row.entity_id,
            schema_key: row.schema_key,
            file_id: row.file_id,
            plugin_key: row.plugin_key,
            snapshot_content: row.snapshot_content,
            metadata: row.metadata,
            schema_version: row.schema_version,
            change_id: row.change_id,
            commit_id: row.commit_id,
            commit_created_at: row.commit_created_at,
            root_commit_id: row.root_commit_id,
            depth: row.depth,
            version_id: row.version_id,
        })
        .collect())
}

fn root_commit_resolution_request<'a>(
    request: &'a StateHistoryRequest,
) -> RootCommitResolutionRequest<'a> {
    RootCommitResolutionRequest {
        lineage_scope: match request.lineage_scope {
            StateHistoryLineageScope::Standard => RootLineageScope::Standard,
            StateHistoryLineageScope::ActiveVersion => RootLineageScope::ActiveVersion,
        },
        lineage_version_id: request.lineage_version_id.as_deref(),
        root_scope: match &request.root_scope {
            StateHistoryRootScope::AllRoots => RootCommitScope::AllRoots,
            StateHistoryRootScope::RequestedRoots(root_commit_ids) => {
                RootCommitScope::RequestedRoots(root_commit_ids)
            }
        },
        version_scope: match &request.version_scope {
            StateHistoryVersionScope::Any => RootVersionScope::Any,
            StateHistoryVersionScope::RequestedVersions(version_ids) => {
                RootVersionScope::RequestedVersions(version_ids)
            }
        },
    }
}

fn canonical_history_request(
    request: &StateHistoryRequest,
    root_facts: HistoryRootFacts,
) -> CanonicalHistoryRequest {
    CanonicalHistoryRequest {
        root_selection: match root_facts.traversal {
            HistoryRootTraversal::AllRoots => CanonicalHistoryRootSelection::AllRoots,
            HistoryRootTraversal::RequestedRootCommitIds(root_commit_ids) => {
                CanonicalHistoryRootSelection::RequestedRootCommitIds(root_commit_ids)
            }
            HistoryRootTraversal::ResolvedRootCommits(root_commits) => {
                CanonicalHistoryRootSelection::ResolvedRootCommits(
                    root_commits
                        .into_iter()
                        .map(|root| CanonicalRootCommit {
                            commit_id: root.commit_id,
                            version_id: root.version_id,
                        })
                        .collect(),
                )
            }
        },
        root_version_refs: root_facts
            .root_version_refs
            .into_iter()
            .map(|root| CanonicalRootCommit {
                commit_id: root.commit_id,
                version_id: root.version_id,
            })
            .collect(),
        entity_ids: request.entity_ids.clone(),
        file_ids: request.file_ids.clone(),
        schema_keys: request.schema_keys.clone(),
        plugin_keys: request.plugin_keys.clone(),
        min_depth: request.min_depth,
        max_depth: request.max_depth,
        content_mode: match request.content_mode {
            StateHistoryContentMode::MetadataOnly => CanonicalHistoryContentMode::MetadataOnly,
            StateHistoryContentMode::IncludeSnapshotContent => {
                CanonicalHistoryContentMode::IncludeSnapshotContent
            }
        },
    }
}
