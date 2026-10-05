//! Shared native preparation for retained logical read recipes.
//!
//! This helper deliberately performs the same native preparation used during
//! candidate warming, but owns no candidate controls or write frontier. The
//! caller supplies the already scoped reader and recipe snapshot.

use crate::LixError;
use crate::filesystem::{
    FilesystemPathIndexReader, FilesystemPathIndexRequest, FilesystemPathIndexScope,
};
use crate::hot_state::{HotStateContext, LogicalReadInterest, ReadInterestSnapshot};
use crate::storage_adapter::StorageAdapterRead;
use crate::sync::partial_replica::PartialReplicaDescriptor;
use crate::tracked_state::NativeMetadataRef;
use std::sync::Arc;

enum NativeReadPreparationPurpose {
    Candidate {
        plugin_host: crate::plugin::runtime::PluginRuntimeHost,
    },
    Authority {
        blob_capture: Arc<crate::sync::read_fulfillment::BlobReadCapture>,
    },
}

fn consume_history_graph_node_budget(remaining: &mut usize) -> bool {
    if *remaining == 0 {
        return false;
    }
    *remaining -= 1;
    true
}

fn path_index_request_for_interest(
    scope: &FilesystemPathIndexScope,
    branch_ids: &[String],
    include_blob_refs: bool,
    cache_small_blob_data: bool,
) -> FilesystemPathIndexRequest {
    FilesystemPathIndexRequest::new(branch_ids.to_vec())
        .with_scope(scope.clone())
        .with_blob_refs(include_blob_refs || cache_small_blob_data)
        .with_cached_blob_data(cache_small_blob_data)
}

/// Prepare all native inputs selected by `interests` against one reader for
/// candidate warming. This preserves the existing plugin/catalog behavior.
pub(crate) async fn prepare_native_read_interests<R>(
    read: R,
    descriptor: &PartialReplicaDescriptor,
    interests: &ReadInterestSnapshot,
    active_account_id: &str,
    plugin_host: crate::plugin::runtime::PluginRuntimeHost,
    hot: HotStateContext,
) -> Result<(), LixError>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    prepare_native_read_interests_with_purpose(
        read,
        descriptor,
        interests,
        active_account_id,
        hot,
        NativeReadPreparationPurpose::Candidate { plugin_host },
    )
    .await
    .map(|_| ())
}

/// Prepare all native inputs selected by `interests` for authority-side
/// dependency discovery. No plugin host is consulted or plugin code executed.
pub(crate) async fn prepare_native_read_interests_authority<R>(
    read: R,
    descriptor: &PartialReplicaDescriptor,
    interests: &ReadInterestSnapshot,
    active_account_id: &str,
    hot: HotStateContext,
    blob_capture: Arc<crate::sync::read_fulfillment::BlobReadCapture>,
) -> Result<Vec<crate::sync::read_fulfillment::ReadInput>, LixError>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    prepare_native_read_interests_with_purpose(
        read,
        descriptor,
        interests,
        active_account_id,
        hot,
        NativeReadPreparationPurpose::Authority { blob_capture },
    )
    .await
}

async fn prepare_native_read_interests_with_purpose<R>(
    read: R,
    descriptor: &PartialReplicaDescriptor,
    interests: &ReadInterestSnapshot,
    active_account_id: &str,
    hot: HotStateContext,
    purpose: NativeReadPreparationPurpose,
) -> Result<Vec<crate::sync::read_fulfillment::ReadInput>, LixError>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let blob = crate::binary_cas::BinaryCasContext::new();
    blob.enable_referenced_manifest_demands();
    // Keep catalog and executable dependency preparation coupled to the same
    // reader as native input preparation. Neither path executes plugin render
    // code; the authority reader is captured for immutable dependency export.
    let candidate_catalog = crate::catalog::CatalogContext::new();
    let dependency_blobs: Arc<dyn crate::binary_cas::BlobDataReader> = match &purpose {
        NativeReadPreparationPurpose::Candidate { .. } => Arc::new(blob.reader(read.clone())),
        NativeReadPreparationPurpose::Authority { blob_capture } => {
            blob_capture.wrap(blob.reader(read.clone()))
        }
    };
    let mut mutation_identities = std::collections::BTreeMap::<
        String,
        std::collections::BTreeSet<crate::tracked_state::TrackedStateKey>,
    >::new();
    // The ordinary native recorder sees physical rows, but direct change
    // locators are derived from the change ID and therefore have no physical
    // row to observe. Retain the returned identities and close this logical
    // part of the graph once, after all recipes have been replayed.
    let mut returned_identities =
        std::collections::BTreeSet::<(String, crate::tracked_state::TrackedStateKey)>::new();
    let mut catalog_branches = std::collections::BTreeSet::<String>::new();
    // One authority request shares this traversal ceiling across every
    // History recipe; per-recipe bounds alone would multiply graph work.
    let native_diff_budget = matches!(&purpose, NativeReadPreparationPurpose::Authority { .. })
        .then(|| crate::tracked_state::NativeDiffIdentityBudget::new(16_384));
    let mut history_graph_nodes_remaining = crate::hot_state::MAX_HISTORY_RECIPE_GRAPH_NODES;
    if matches!(&purpose, NativeReadPreparationPurpose::Authority { .. })
        && interests
            .interests
            .iter()
            .any(|interest| matches!(interest.as_ref(), LogicalReadInterest::Diff { .. }))
    {
        let recipes = interests
            .interests
            .iter()
            .map(|interest| interest.as_ref().clone())
            .collect::<Vec<_>>();
        super::working_diff_recipe::validate_working_diff_recipes(
            &recipes,
            &descriptor.selected_branch.branch_id,
        )?;
        super::working_diff_recipe::prove_selected_branch_checkpoint_ancestry(
            read.clone(),
            descriptor,
            &mut history_graph_nodes_remaining,
        )
        .await?;
    }

    for interest in &interests.interests {
        match interest.as_ref() {
            LogicalReadInterest::Scan { request, domain } => {
                for branch in &request.filter.branch_ids {
                    super::partial_candidate_prepare::selected_branch(descriptor, branch)?;
                }
                // The shared foreground preparation below performs this scan
                // once and prepares the rows it actually resolves.
                if matches!(domain, crate::hot_state::InterestDomain::Untracked) {
                    hot.reader(read.clone()).scan_batch(request).await?;
                }
            }
            LogicalReadInterest::Exact {
                rows, untracked, ..
            } => {
                for row in rows {
                    super::partial_candidate_prepare::selected_branch(descriptor, &row.branch_id)?;
                    if *untracked != Some(true) {
                        mutation_identities
                            .entry(row.branch_id.clone())
                            .or_default()
                            .insert(crate::tracked_state::TrackedStateKey {
                                schema_key: row.schema_key.clone(),
                                file_id: row.file_id.clone(),
                                row_pk: row.row_pk.clone(),
                            });
                    }
                }
                // Exact absence and payloads are validated by the shared
                // preparation below; retain explicit keys for insertion paths.
            }
            LogicalReadInterest::CollectionGeneration {
                branch_id,
                schema_key,
                file_id,
            } => {
                super::partial_candidate_prepare::selected_branch(descriptor, branch_id)?;
                hot.reader(read.clone())
                    .collection_generation_active_token(
                        branch_id,
                        crate::collection_generation::CollectionScopeRef {
                            schema_key,
                            file_id: file_id.as_deref(),
                        },
                    )
                    .await?;
            }
            LogicalReadInterest::PackedIdentityMembership {
                branch_id,
                schema_key,
            } => {
                super::partial_candidate_prepare::selected_branch(descriptor, branch_id)?;
                hot.transaction_reader(
                    read.clone(),
                    Arc::new(crate::hot_state::BranchHeadControlCache::default()),
                )
                .prepare_packed_identity_membership(branch_id, schema_key)
                .await?;
            }
            LogicalReadInterest::FilesystemPaths {
                scope,
                branch_ids,
                include_blob_refs,
                cache_small_blob_data,
            } => {
                for branch in branch_ids {
                    super::partial_candidate_prepare::selected_branch(descriptor, branch)?;
                }
                let request = path_index_request_for_interest(
                    scope,
                    branch_ids,
                    *include_blob_refs,
                    *cache_small_blob_data,
                );
                match &purpose {
                    NativeReadPreparationPurpose::Candidate { .. } => {
                        hot.reader(read.clone())
                            .path_index(&request.with_cached_blob_data(*cache_small_blob_data))
                            .await?;
                    }
                    NativeReadPreparationPurpose::Authority { .. } => {
                        // The authority must not reuse its live path-index cache:
                        // the request is evaluated against the descriptor-scoped
                        // reader. Small cached payloads are loaded through the
                        // captured logical blob reader below so canonical blob
                        // inputs are exported with the native path dependencies.
                        let index_reader =
                            crate::filesystem::UncachedFilesystemPathIndexReader::new(Arc::new(
                                hot.reader(read.clone()),
                            ));
                        let index = index_reader.path_index(&request).await?;
                        if *cache_small_blob_data {
                            prepare_path_index_small_blob_inputs(
                                index.as_ref(),
                                dependency_blobs.as_ref(),
                            )
                            .await?;
                        }
                    }
                }
            }
            LogicalReadInterest::Diff {
                branch_id,
                relation,
                from,
                to,
                filter,
                retain_payloads,
                projected_columns,
                limit: _,
            } => {
                let from = super::partial_candidate_prepare::endpoint(
                    descriptor,
                    branch_id.as_deref(),
                    from,
                )?;
                let to = super::partial_candidate_prepare::endpoint(
                    descriptor,
                    branch_id.as_deref(),
                    to,
                )?;
                crate::sql2::prepare_native_diff_interest(
                    read.clone(),
                    relation,
                    &from,
                    &to,
                    &crate::tracked_state::TrackedStateDiffRequest {
                        filter: filter.clone(),
                        retain_payloads: *retain_payloads,
                    },
                    projected_columns,
                    native_diff_budget.clone(),
                )
                .await?;
            }
            LogicalReadInterest::History {
                branch_id,
                commit_ids,
                relation,
                filter,
                retain_payloads,
                projected_columns,
                limit,
            } => {
                let branch =
                    super::partial_candidate_prepare::selected_branch(descriptor, branch_id)?;
                if !matches!(&purpose, NativeReadPreparationPurpose::Authority { .. }) {
                    // Fixed history inputs are retained across refreshes; they
                    // are never rebound to the moving candidate head.
                    continue;
                }
                if branch_id != &descriptor.selected_branch.branch_id
                    || limit.is_some()
                    || commit_ids.is_empty()
                    || commit_ids.len() > crate::hot_state::MAX_HISTORY_RECIPE_COMMIT_IDS
                {
                    return Err(super::partial_candidate_prepare::unsupported(
                        "history recipe is outside its bounded leased branch scope",
                    ));
                }
                let mut selected = std::collections::BTreeSet::new();
                for id in commit_ids {
                    let parsed = crate::changelog::CommitId::parse_lix(id, "history commit")?;
                    if !parsed.has_canonical_text(id) || !selected.insert(parsed) {
                        return Err(super::partial_candidate_prepare::unsupported(
                            "history recipe contains a noncanonical or duplicate commit",
                        ));
                    }
                }
                let mut graph = crate::commit_graph::CommitGraphContext::new().reader(read.clone());
                let mut cursor = crate::changelog::CommitId::parse_lix(
                    &branch.head.commit_id,
                    "leased branch head",
                )?;
                let mut parents = std::collections::BTreeMap::new();
                let mut seen = std::collections::BTreeSet::new();
                let mut previous_generation = None;
                loop {
                    if !seen.insert(cursor) {
                        return Err(LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            "history recipe encountered a first-parent cycle",
                        ));
                    }
                    if !consume_history_graph_node_budget(&mut history_graph_nodes_remaining) {
                        return Err(LixError::new(
                            "LIX_HISTORY_RECIPE_FALLBACK",
                            "history recipes exceed the shared bounded ancestry work budget",
                        ));
                    }
                    let loaded = match graph.load_node(&cursor).await {
                        Err(error) if error.code == "LIX_SYNC_HISTORY_REQUIRED" => {
                            return Err(LixError::new(
                                "LIX_HISTORY_RECIPE_FALLBACK",
                                "history recipe ancestry requires unavailable history",
                            ));
                        }
                        result => result?,
                    };
                    let node = match loaded {
                        Some(node) => node,
                        None => {
                            return Err(LixError::new(
                                "LIX_HISTORY_RECIPE_FALLBACK",
                                "history recipe ancestry is not available on the leased branch",
                            ));
                        }
                    };
                    if node.commit_id != cursor {
                        return Err(LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            "history recipe node ID disagrees with its requested address",
                        ));
                    }
                    if previous_generation.is_some_and(|generation| node.generation >= generation) {
                        return Err(LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            "history recipe first-parent generation is not decreasing",
                        ));
                    }
                    previous_generation = Some(node.generation);
                    if selected.contains(&cursor) {
                        parents.insert(cursor, node.parent_commit_ids.first().copied());
                    }
                    if parents.len() == selected.len() {
                        break;
                    }
                    let Some(parent) = node.parent_commit_ids.first().copied() else {
                        break;
                    };
                    cursor = parent;
                }
                if parents.len() != selected.len() {
                    return Err(LixError::new(
                        "LIX_HISTORY_RECIPE_FALLBACK",
                        "history recipe is outside the bounded leased ancestry",
                    ));
                }
                let request = crate::tracked_state::TrackedStateDiffRequest {
                    filter: filter.clone(),
                    retain_payloads: *retain_payloads,
                };
                for id in commit_ids {
                    let to = crate::changelog::CommitId::parse_lix(id, "history commit")?;
                    let Some(from) = parents.get(&to) else {
                        return Err(super::partial_candidate_prepare::unsupported(
                            "selected history commit is outside its proved ancestry",
                        ));
                    };
                    if let Some(from) = from {
                        crate::sql2::prepare_native_diff_interest(
                            read.clone(),
                            relation,
                            &from.to_string(),
                            id,
                            &request,
                            projected_columns,
                            native_diff_budget.clone(),
                        )
                        .await?;
                    }
                }
            }
            LogicalReadInterest::FilesystemMetadata {
                directory,
                branch_ids,
                file_ids,
                directory_ids,
                root_directory,
                path_predicate,
            } => {
                for branch in branch_ids {
                    super::partial_candidate_prepare::selected_branch(descriptor, branch)?;
                }
                let capture = crate::hot_state::ReadInterestRegistry::new(4096, 4 * 1024 * 1024);
                let replay_hot = hot.with_read_interest_registry(capture.clone());
                let filesystem_path_index: Arc<dyn FilesystemPathIndexReader> = match &purpose {
                    NativeReadPreparationPurpose::Candidate { .. } => {
                        Arc::new(replay_hot.reader(read.clone()))
                    }
                    NativeReadPreparationPurpose::Authority { .. } => {
                        Arc::new(crate::filesystem::UncachedFilesystemPathIndexReader::new(
                            Arc::new(replay_hot.reader(read.clone())),
                        ))
                    }
                };
                crate::sql2::prepare_native_file_metadata_interest(
                    Arc::new(replay_hot.reader(read.clone())),
                    filesystem_path_index,
                    *directory,
                    branch_ids,
                    file_ids.as_deref(),
                    directory_ids.as_deref(),
                    *root_directory,
                    path_predicate,
                )
                .await?;
                let reader = hot.reader(read.clone());
                let executable_rows = reader
                    .prepare_captured_read_interests(&capture.snapshot()?, active_account_id)
                    .await?;
                returned_identities.extend(executable_rows.iter().cloned());
                catalog_branches.extend(executable_rows.iter().map(|(branch, _)| branch.clone()));
                candidate_catalog
                    .prepare_returned_row_catalogs(&reader, &executable_rows, None)
                    .await?;
                if matches!(&purpose, NativeReadPreparationPurpose::Authority { .. }) {
                    returned_identities.extend(
                        prepare_authority_returned_row_executables(
                            &hot,
                            read.clone(),
                            dependency_blobs.as_ref(),
                            &executable_rows,
                            active_account_id,
                        )
                        .await?,
                    );
                } else {
                    crate::plugin::runtime::prepare_returned_row_executables(
                        &reader,
                        dependency_blobs.as_ref(),
                        &executable_rows,
                    )
                    .await?;
                }
            }
            LogicalReadInterest::FileContent {
                request,
                file_ids,
                directory_ids,
                root_directory,
                indexed,
                path_predicate,
                byte_range,
            } => {
                for branch in &request.filter.branch_ids {
                    super::partial_candidate_prepare::selected_branch(descriptor, branch)?;
                }
                // Capture newly matching native identities independently of the
                // live registry and its persisted epoch.
                let capture = crate::hot_state::ReadInterestRegistry::new(4096, 4 * 1024 * 1024);
                let replay_hot = hot.with_read_interest_registry(capture.clone());
                let filesystem_path_index: Arc<dyn FilesystemPathIndexReader> = match &purpose {
                    NativeReadPreparationPurpose::Candidate { .. } => {
                        Arc::new(replay_hot.reader(read.clone()))
                    }
                    NativeReadPreparationPurpose::Authority { .. } => {
                        Arc::new(crate::filesystem::UncachedFilesystemPathIndexReader::new(
                            Arc::new(replay_hot.reader(read.clone())),
                        ))
                    }
                };
                match &purpose {
                    NativeReadPreparationPurpose::Candidate { plugin_host } => {
                        crate::sql2::prepare_native_file_content_interest(
                            Arc::new(replay_hot.reader(read.clone())),
                            filesystem_path_index,
                            dependency_blobs.clone(),
                            plugin_host.clone(),
                            request,
                            file_ids.as_deref(),
                            directory_ids.as_deref(),
                            *root_directory,
                            *indexed,
                            path_predicate,
                            *byte_range,
                        )
                        .await?;
                    }
                    NativeReadPreparationPurpose::Authority { .. } => {
                        crate::sql2::prepare_native_file_content_inputs(
                            Arc::new(replay_hot.reader(read.clone())),
                            filesystem_path_index,
                            dependency_blobs.clone(),
                            request,
                            file_ids.as_deref(),
                            directory_ids.as_deref(),
                            *root_directory,
                            *indexed,
                            path_predicate,
                            *byte_range,
                        )
                        .await?;
                    }
                }
                let reader = hot.reader(read.clone());
                let executable_rows = reader
                    .prepare_captured_read_interests(&capture.snapshot()?, active_account_id)
                    .await?;
                returned_identities.extend(executable_rows.iter().cloned());
                catalog_branches.extend(executable_rows.iter().map(|(branch, _)| branch.clone()));
                candidate_catalog
                    .prepare_returned_row_catalogs(&reader, &executable_rows, None)
                    .await?;
                if matches!(&purpose, NativeReadPreparationPurpose::Authority { .. }) {
                    returned_identities.extend(
                        prepare_authority_returned_row_executables(
                            &hot,
                            read.clone(),
                            dependency_blobs.as_ref(),
                            &executable_rows,
                            active_account_id,
                        )
                        .await?,
                    );
                } else {
                    crate::plugin::runtime::prepare_returned_row_executables(
                        &reader,
                        dependency_blobs.as_ref(),
                        &executable_rows,
                    )
                    .await?;
                }
            }
        }
    }
    for (branch_id, keys) in mutation_identities {
        let branch = super::partial_candidate_prepare::selected_branch(descriptor, &branch_id)?;
        let Some(root) = branch.head.row_pk_index_root_id else {
            return Err(super::partial_candidate_prepare::unsupported(
                "candidate has no native row-PK identity catalog for mutation preparation",
            ));
        };
        crate::tracked_state::prepare_row_pk_index_mutation_inputs(
            &read,
            &crate::tracked_state::TrackedStateRootId::new(root),
            &keys.into_iter().collect::<Vec<_>>(),
        )
        .await?;
    }
    // Foreground row reads promise the same bounded native edit inputs. Prepare
    // them against these unpublished controls before they become visible; the
    // operation-scoped context intentionally has no trusted live-epoch cache.
    let reader = hot.reader(read.clone());
    let executable_rows = reader
        .prepare_captured_read_interests_with_native_diff_budget(
            interests,
            active_account_id,
            native_diff_budget,
        )
        .await?;
    returned_identities.extend(executable_rows.iter().cloned());
    catalog_branches.extend(executable_rows.iter().map(|(branch, _)| branch.clone()));
    candidate_catalog
        .prepare_returned_row_catalogs(&reader, &executable_rows, None)
        .await?;
    if matches!(&purpose, NativeReadPreparationPurpose::Authority { .. }) {
        returned_identities.extend(
            prepare_authority_returned_row_executables(
                &hot,
                read.clone(),
                dependency_blobs.as_ref(),
                &executable_rows,
                active_account_id,
            )
            .await?,
        );
    } else {
        crate::plugin::runtime::prepare_returned_row_executables(
            &reader,
            dependency_blobs.as_ref(),
            &executable_rows,
        )
        .await?;
    }
    if matches!(&purpose, NativeReadPreparationPurpose::Authority { .. })
        && !catalog_branches.contains(&descriptor.selected_branch.branch_id)
    {
        // SQL planning deliberately suppresses foreground capture in
        // SessionSqlExecutionContext::compiled_sql_catalog. Preserve that
        // contract while still closing the selected branch's schema catalog
        // when this operation produced no tracked row branch to trigger the
        // normal returned-row catalog preparation.
        candidate_catalog
            .compiled_catalog_for_transaction_open(
                &reader,
                &crate::domain::Domain::schema_catalog(
                    descriptor.selected_branch.branch_id.clone(),
                    true,
                ),
                None,
            )
            .await?;
    }
    match purpose {
        NativeReadPreparationPurpose::Candidate { .. } => Ok(Vec::new()),
        NativeReadPreparationPurpose::Authority { .. } => {
            prepare_canonical_returned_row_inputs(&read, &hot, &returned_identities).await
        }
    }
}

/// The plugin executable preparation performs two exact reads which are not
/// part of its public return value: the branch's plugin registry row and each
/// selected file's owner row.  Capture those logical reads while preparing the
/// authority closure, then replay them through the same descriptor-scoped
/// reader so the recorder sees their complete native dependency graph.
async fn prepare_authority_returned_row_executables<R>(
    hot: &HotStateContext,
    read: R,
    blobs: &dyn crate::binary_cas::BlobDataReader,
    rows: &[(String, crate::tracked_state::TrackedStateKey)],
    active_account_id: &str,
) -> Result<Vec<(String, crate::tracked_state::TrackedStateKey)>, LixError>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let capture = crate::hot_state::ReadInterestRegistry::new(4096, 4 * 1024 * 1024);
    let replay_hot = hot.with_read_interest_registry(capture.clone());
    let replay_reader = replay_hot.reader(read.clone());
    crate::plugin::runtime::prepare_returned_row_executables(&replay_reader, blobs, rows).await?;
    let snapshot = capture.snapshot()?;
    replay_hot
        .reader(read)
        .prepare_captured_read_interests(&snapshot, active_account_id)
        .await
}

/// Complete the logical immutable dependencies of the rows selected by one
/// operation. Most of this graph is observed by the normal storage recorder.
/// Two cases need explicit handling here:
///
/// * direct change IDs have a canonical locator derived from their address and
///   consequently no locator row for the recorder to see;
/// * a standalone changelog row can satisfy the selected-row load without
///   touching the owning commit's mutation inventory.
///
/// The returned typed inputs are merged into the same discovery closure as
/// physical observations by the authority endpoint. The pass is operation
/// wide and deduplicates both row identities and commit owners.
async fn prepare_canonical_returned_row_inputs<R>(
    read: &R,
    hot: &HotStateContext,
    identities: &std::collections::BTreeSet<(String, crate::tracked_state::TrackedStateKey)>,
) -> Result<Vec<crate::sync::read_fulfillment::ReadInput>, LixError>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    if identities.is_empty() {
        return Ok(Vec::new());
    }
    let rows = identities
        .iter()
        .map(
            |(branch_id, key)| crate::hot_state::HotStateExactRowRequest {
                schema_key: key.schema_key.clone(),
                branch_id: branch_id.clone(),
                file_id: key.file_id.clone(),
                row_pk: key.row_pk.clone(),
            },
        )
        .collect::<Vec<_>>();
    let batch = hot
        .reader(read.clone())
        .load_exact_batch(&crate::hot_state::HotStateExactBatchRequest {
            rows,
            projection: crate::hot_state::HotStateProjection {
                columns: vec!["snapshot_content".to_owned()],
            },
            untracked: Some(false),
            include_tombstones: false,
        })
        .await?;

    let mut change_ids = std::collections::BTreeSet::new();
    let mut owner_commits = std::collections::BTreeSet::new();
    for index in 0..batch.len() {
        let Some(row) = batch.row(index) else {
            continue;
        };
        if let Some(change_id) = row.change_id() {
            change_ids.insert(change_id);
        }
        if let Some(commit_id) = row.commit_id() {
            owner_commits.insert(commit_id);
        }
    }

    let mut locators = std::collections::BTreeMap::new();
    for change_id in change_ids {
        if let Some(locator) =
            crate::tracked_state::load_canonical_change_locator(read, change_id).await?
        {
            owner_commits.insert(locator.commit_id);
            locators.insert(change_id, locator);
        }
    }

    // A single exact owner load per commit guarantees that a standalone
    // changelog hit cannot hide the mutation inventory needed by the local
    // immutable row and catalog readers. The storage adapter's physical-read
    // policy makes this bypass decoded process-global caches during authority
    // discovery; the recorder turns the inventory into a typed catalog input.
    for commit_id in owner_commits {
        let _ = crate::tracked_state::load_commit_state_manifest(read, commit_id).await?;
    }

    let identity_rows = identities.iter().collect::<Vec<_>>();
    let mut payload_requests = Vec::new();
    let mut locator_fallback_requests = Vec::new();
    let mut locator_fallback_ids = std::collections::BTreeSet::new();
    for index in 0..batch.len() {
        let Some(row) = batch.row(index) else {
            continue;
        };
        let Some(change_id) = row.change_id() else {
            continue;
        };
        let Some((branch_id, key)) = identity_rows.get(index).copied() else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "exact tracked row has no corresponding requested identity",
            ));
        };
        let locator = locators.get(&change_id);
        let source_commit_id = locator
            .map(|locator| locator.commit_id)
            .or_else(|| row.commit_id())
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked change '{change_id}' has neither an authoritative locator nor an exact-row owner"
                    ),
                )
            })?;
        if locator.is_none() {
            locator_fallback_ids.insert(change_id);
            locator_fallback_requests.push((
                source_commit_id,
                change_id,
                key.clone(),
                row.updated_at(),
            ));
        }
        payload_requests.push((
            branch_id.clone(),
            crate::tracked_state::AuthoritativeLiveChangeRequest {
                change_id,
                source_commit_id,
                key: key.clone(),
                updated_at: row.updated_at(),
            },
        ));
    }

    // Older physical layouts may have an exact selected row and its physical
    // owner while lacking the rebuildable CHANGE_LOCATOR projection. Recover
    // the canonical authored locator through exact-key, authenticated point
    // reads; selected-reference chains are followed by their recorded base
    // coordinates, never by scanning a commit or inventing an ordinal.
    let fallback_locators = crate::tracked_state::load_exact_tracked_row_change_locators(
        read,
        &locator_fallback_requests,
    )
    .await?;
    let mut fallback_owner_commits = std::collections::BTreeSet::new();
    for ((_, change_id, _, _), locator) in locator_fallback_requests.iter().zip(fallback_locators) {
        fallback_owner_commits.insert(locator.commit_id);
        locators.insert(*change_id, locator);
    }
    for commit_id in fallback_owner_commits {
        let _ = crate::tracked_state::load_commit_state_manifest(read, commit_id).await?;
    }
    for (_, request) in &mut payload_requests {
        if locator_fallback_ids.contains(&request.change_id) {
            request.source_commit_id = locators
                .get(&request.change_id)
                .expect("fallback locator was recovered")
                .commit_id;
        }
    }

    // CHANGE_SPACE is mutable, so raw physical observations are deliberately
    // not part of the generic immutable closure. Resolve every selected row
    // through the changelog's canonical authority policy: a valid standalone
    // payload is preferred, while an absent or stale projection can fall back
    // to the exact physical owner selected by the locator. In either case the
    // resolver enforces row identity and lifetime before producing wire data.
    let resolved_payloads = crate::tracked_state::load_authoritative_live_change_records(
        read,
        &payload_requests
            .iter()
            .map(
                |(_, request)| crate::tracked_state::AuthoritativeLiveChangeRequest {
                    change_id: request.change_id,
                    source_commit_id: request.source_commit_id,
                    key: request.key.clone(),
                    updated_at: request.updated_at,
                },
            )
            .collect::<Vec<_>>(),
    )
    .await?;
    let mut payload_inputs = Vec::with_capacity(resolved_payloads.len());
    for ((branch_id, request), record) in payload_requests.into_iter().zip(resolved_payloads) {
        let bytes = crate::changelog::encode_change_record(&record)?;
        payload_inputs.push(crate::sync::read_fulfillment::ReadInput {
            address: crate::sync::read_fulfillment::ReadInputAddress::ChangeRecord {
                change_id: request.change_id.to_string(),
                source_commit_id: request.source_commit_id.to_string(),
                branch_id,
                schema_key: request.key.schema_key,
                file_id: request.key.file_id,
                row_pk: request.key.row_pk,
                updated_at: request.updated_at.to_string(),
                payload_digest: *blake3::hash(&bytes).as_bytes(),
            },
            bytes,
        });
    }

    let mut inputs = locators
        .into_iter()
        .map(
            |(change_id, locator)| crate::sync::read_fulfillment::ReadInput {
                address: crate::sync::read_fulfillment::ReadInputAddress::Metadata(
                    NativeMetadataRef::ChangeLocator(change_id.to_string()),
                ),
                bytes: crate::tracked_state::encode_change_locator(locator),
            },
        )
        .collect::<Vec<_>>();
    inputs.extend(payload_inputs);
    Ok(inputs)
}

/// Reproduce the path-index eager-blob policy while routing the actual CAS
/// read through the authority capture. The index itself is deliberately built
/// without eager payload hydration because that implementation reads CAS
/// directly and would bypass canonical blob demand recording.
async fn prepare_path_index_small_blob_inputs(
    index: &crate::filesystem::FilesystemPathIndex,
    blobs: &dyn crate::binary_cas::BlobDataReader,
) -> Result<(), LixError> {
    const MAX_EAGER_BLOB_BYTES: usize = 32 * 1024;
    const MAX_EAGER_BLOB_CACHE_BYTES: usize = 16 * 1024 * 1024;

    #[derive(serde::Deserialize)]
    struct BlobRefSnapshot {
        blob_hash: String,
        size_bytes: u64,
    }

    let mut reserved_bytes = 0usize;
    let mut hashes = std::collections::BTreeSet::new();
    for entry in index.entries() {
        let Some(row) = entry.blob_ref_live_row() else {
            continue;
        };
        let Some(snapshot_content) = row.snapshot_content.as_deref() else {
            continue;
        };
        let snapshot: BlobRefSnapshot =
            serde_json::from_str(snapshot_content).map_err(|error| {
                LixError::unknown(format!(
                    "invalid lix_binary_blob_ref snapshot JSON: {error}"
                ))
            })?;
        let size_bytes = usize::try_from(snapshot.size_bytes)
            .map_err(|_| LixError::unknown("lix_binary_blob_ref size_bytes exceeds usize"))?;
        if size_bytes > MAX_EAGER_BLOB_BYTES {
            continue;
        }
        let Some(next_reserved_bytes) = reserved_bytes
            .checked_add(size_bytes)
            .filter(|total| *total <= MAX_EAGER_BLOB_CACHE_BYTES)
        else {
            continue;
        };
        reserved_bytes = next_reserved_bytes;
        hashes.insert(crate::binary_cas::BlobId::from_hex(&snapshot.blob_hash)?);
    }
    if !hashes.is_empty() {
        blobs
            .load_bytes_many(&hashes.into_iter().collect::<Vec<_>>())
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_graph_budget_is_shared_and_fail_closed() {
        let mut remaining = crate::hot_state::MAX_HISTORY_RECIPE_GRAPH_NODES;
        for _ in 0..crate::hot_state::MAX_HISTORY_RECIPE_GRAPH_NODES {
            assert!(consume_history_graph_node_budget(&mut remaining));
        }
        assert_eq!(remaining, 0);
        assert!(!consume_history_graph_node_budget(&mut remaining));
        assert_eq!(remaining, 0);
    }

    #[test]
    fn filesystem_path_interest_preparation_preserves_directory_scope() {
        let request = path_index_request_for_interest(
            &FilesystemPathIndexScope::DirectoriesOnly,
            &["branch".to_owned()],
            true,
            true,
        );

        assert_eq!(request.scope, FilesystemPathIndexScope::DirectoriesOnly);
        assert!(!request.include_blob_refs);
        assert!(!request.cache_small_blob_data);
        assert_eq!(
            request.hot_state_request().filter.schema_keys,
            vec!["lix_directory_descriptor".to_owned()]
        );
    }
}
