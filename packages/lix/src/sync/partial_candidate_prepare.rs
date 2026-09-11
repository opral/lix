//! Evaluate retained native recipes at unpublished candidate roots. No SQL is
//! replayed; no control, receipt, cursor, or derived cache is published here.
use super::partial_replica::PartialReplicaDescriptor;
use crate::LixError;
use crate::filesystem::{FilesystemPathIndexReader, FilesystemPathIndexRequest};
use crate::hot_state::{
    HotStateContext, HotStateReader, LogicalReadInterest, ReadInterestSnapshot,
};
use crate::storage_adapter::{StorageAdapterRead, StorageWriteSet};
use crate::storage_adapter::{
    StorageBeginScanOptions as BeginScanOptions, StorageCoreProjection as CoreProjection,
    StorageError, StorageGetManyRequest as GetManyRequest, StorageGetManyResult as GetManyResult,
    StorageKeyRange as KeyRange, StorageProjectedValue as ProjectedValue,
    StorageScanCursor as ScanCursor, StorageSpace,
};
use crate::storage_adapter::{StorageKey, StoragePrecondition, StorageReadEntry, StorageScanOrder};
use std::sync::Arc;

pub(crate) struct PreparedCandidateState {
    pub(crate) writes: Arc<StorageWriteSet>,
    pub(crate) source_control_guards: Vec<StoragePrecondition>,
}

// Candidate-only scan merge. Staged puts shadow the underlying key; no delete
// support is needed or permitted for the unpublished fresh-generation copy.
struct CandidateScanSource<'a> {
    base: ScanCursor<'a>,
    staged: std::collections::VecDeque<StorageReadEntry>,
    base_rows: std::collections::VecDeque<StorageReadEntry>,
    base_done: bool,
    descending: bool,
}
impl crate::storage_adapter::StorageScanSource for CandidateScanSource<'_> {
    fn next_page(
        &mut self,
        limit_rows: usize,
    ) -> std::pin::Pin<
        Box<
            dyn Future<Output = Result<crate::storage_adapter::StorageScanChunk, StorageError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async move {
            let mut result = Vec::with_capacity(limit_rows.min(64));
            while result.len() < limit_rows {
                if self.base_rows.is_empty() && !self.base_done {
                    let (rows, more) = self.base.next_page(64).await?.into_parts();
                    self.base_rows.extend(rows);
                    self.base_done = !more;
                }
                match (self.base_rows.front(), self.staged.front()) {
                    (None, None) => break,
                    (Some(_), None) => result.push(self.base_rows.pop_front().unwrap()),
                    (None, Some(_)) => result.push(self.staged.pop_front().unwrap()),
                    (Some(base), Some(staged)) => {
                        let order = base.key.0.cmp(&staged.key.0);
                        if order.is_eq() {
                            self.base_rows.pop_front();
                            result.push(self.staged.pop_front().unwrap());
                        } else if (order.is_lt() && !self.descending)
                            || (order.is_gt() && self.descending)
                        {
                            result.push(self.base_rows.pop_front().unwrap());
                        } else {
                            result.push(self.staged.pop_front().unwrap());
                        }
                    }
                }
            }
            let more = !self.base_done || !self.base_rows.is_empty() || !self.staged.is_empty();
            Ok(crate::storage_adapter::StorageScanChunk::new(result, more))
        })
    }
}
fn in_candidate_range(key: &[u8], range: &KeyRange) -> bool {
    use std::ops::Bound;
    let lower = match &range.lower {
        Bound::Unbounded => true,
        Bound::Included(value) => key >= value.0.as_ref(),
        Bound::Excluded(value) => key > value.0.as_ref(),
    };
    let upper = match &range.upper {
        Bound::Unbounded => true,
        Bound::Included(value) => key <= value.0.as_ref(),
        Bound::Excluded(value) => key < value.0.as_ref(),
    };
    lower && upper
}

#[derive(Clone)]
struct CandidateRead<R> {
    base: R,
    staged: Arc<StorageWriteSet>,
}
impl<R: StorageAdapterRead> StorageAdapterRead for CandidateRead<R> {
    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        let mut loaded = self.base.get_many(requests).await?;
        let mut index = 0;
        for request in requests {
            for key in request.keys {
                if let Some(bytes) = self.staged.staged_value(request.space, &key.0) {
                    loaded.values[index] = Some(match request.opts.projection {
                        CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
                        CoreProjection::FullValue => ProjectedValue::FullValue(bytes),
                    });
                }
                index += 1;
            }
        }
        Ok(loaded)
    }
    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        if space == crate::branch::BRANCH_HEAD_CONTROL_SPACE
            || space == crate::hot_state::ROOT_CURRENT_BASE_SPACE
        {
            return Err(StorageError::Io(
                "candidate control-plane scan is unsupported".into(),
            ));
        }
        let mut staged = self
            .staged
            .staged_values_in_space(space)
            .into_iter()
            .filter(|(key, _)| in_candidate_range(key, &range))
            .map(|(key, value)| StorageReadEntry {
                key: StorageKey(key),
                value: match opts.projection {
                    CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
                    CoreProjection::FullValue => ProjectedValue::FullValue(value),
                },
            })
            .collect::<Vec<_>>();
        if staged.is_empty() {
            return self.base.begin_scan(space, range, opts).await;
        }
        staged.sort_by(|left, right| left.key.0.cmp(&right.key.0));
        let descending = opts.order == StorageScanOrder::Descending;
        if descending {
            staged.reverse();
        }
        let base = self.base.begin_scan(space, range.clone(), opts).await?;
        ScanCursor::from_source(
            range,
            opts.order,
            CandidateScanSource {
                base,
                staged: staged.into(),
                base_rows: Default::default(),
                base_done: false,
                descending,
            },
        )
    }
}
fn unsupported(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_SCOPE_PREPARATION_REQUIRED", message)
}
fn selected_branch<'a>(
    descriptor: &'a PartialReplicaDescriptor,
    branch_id: &str,
) -> Result<&'a super::partial_replica::PartialReplicaBranch, LixError> {
    [&descriptor.selected_branch, &descriptor.global_branch]
        .into_iter()
        .find(|branch| branch.branch_id == branch_id)
        .ok_or_else(|| unsupported("candidate does not include a retained branch"))
}
fn endpoint(
    descriptor: &PartialReplicaDescriptor,
    branch_id: Option<&str>,
    endpoint: &crate::hot_state::DiffInterestEndpoint,
) -> Result<String, LixError> {
    use crate::hot_state::DiffInterestEndpoint::*;
    Ok(match endpoint {
        Fixed(id) => id.clone(),
        ActiveHead => selected_branch(
            descriptor,
            branch_id.ok_or_else(|| unsupported("dynamic diff has no branch"))?,
        )?
        .head
        .commit_id
        .clone(),
        WorkingCheckpoint => selected_branch(
            descriptor,
            branch_id.ok_or_else(|| unsupported("working diff has no branch"))?,
        )?
        .checkpoint
        .commit_id
        .clone(),
    })
}
/// Archived recipes retain their original concrete branch bindings. Skip a
/// whole recipe only if every out-of-scope branch has a durable switch receipt
/// represented in the state owner's archive. Unknown scopes still reach the
/// ordinary candidate validator and fail closed.
fn interest_belongs_to_candidate(
    interest: &LogicalReadInterest,
    selected: &str,
    global: &str,
    archived: &[String],
) -> Result<bool, LixError> {
    let admitted = |branch: &str| branch == selected || branch == global;
    let keep = |branches: Vec<&str>| {
        let outside = branches
            .into_iter()
            .filter(|branch| !admitted(branch))
            .collect::<Vec<_>>();
        outside.is_empty()
            || outside
                .iter()
                .any(|branch| !archived.iter().any(|saved| saved.as_str() == *branch))
    };
    Ok(match interest {
        LogicalReadInterest::Scan { request, .. }
        | LogicalReadInterest::FileContent { request, .. } => keep(
            request
                .filter
                .branch_ids
                .iter()
                .map(String::as_str)
                .collect(),
        ),
        LogicalReadInterest::FilesystemPaths { branch_ids, .. }
        | LogicalReadInterest::FilesystemMetadata { branch_ids, .. } => {
            keep(branch_ids.iter().map(String::as_str).collect())
        }
        LogicalReadInterest::Exact { rows, .. } => {
            keep(rows.iter().map(|row| row.branch_id.as_str()).collect())
        }
        LogicalReadInterest::CollectionGeneration { branch_id, .. }
        | LogicalReadInterest::PackedIdentityMembership { branch_id, .. }
        | LogicalReadInterest::Diff {
            branch_id: Some(branch_id),
            ..
        } => keep(vec![branch_id]),
        LogicalReadInterest::Diff {
            branch_id: None,
            from,
            to,
            ..
        } => {
            if !archived.is_empty()
                && !matches!(
                    (from, to),
                    (
                        crate::hot_state::DiffInterestEndpoint::Fixed(_),
                        crate::hot_state::DiffInterestEndpoint::Fixed(_)
                    )
                )
            {
                return Err(unsupported(
                    "unbound dynamic diff cannot be remapped across a branch transition",
                ));
            }
            true
        }
    })
}

/// Concrete trusted caller must scope `read` through the session-owned bridge;
/// no Arc reader or callback escapes this unit-returning native operation.
pub(crate) async fn prepare_candidate_native_interests<R>(
    read: R,
    state: &super::partial_state::PartialReplicaState,
    interests: &ReadInterestSnapshot,
    plugin_host: crate::plugin::runtime::PluginRuntimeHost,
    hot: HotStateContext,
    allow_missing_selected_control: bool,
) -> Result<PreparedCandidateState, LixError>
where
    R: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    let descriptor = state.descriptor();
    let mut active_interests = interests.clone();
    active_interests.interests.clear();
    for interest in &interests.interests {
        if interest_belongs_to_candidate(
            interest,
            &descriptor.selected_branch.branch_id,
            &descriptor.global_branch.branch_id,
            state.archived_branch_ids(),
        )? {
            active_interests.interests.push(interest.clone());
        }
    }
    // Keep the original revision and byte bound for the publisher's journal
    // fence; filtering is a private evaluation view, never journal mutation.
    let interests = &active_interests;
    descriptor.validate(
        &descriptor.lix_id,
        Some(&descriptor.selected_branch.branch_id),
    )?;
    if let Some(address) =
        super::partial_write_frontier::next_missing_candidate_write_frontier(&read, state).await?
    {
        return Err(address.annotate_missing(LixError::new(
            "LIX_PARTIAL_WRITE_FRONTIER_REQUIRED",
            "candidate baseline graph input is not resident",
        )));
    }
    let heads = [&descriptor.selected_branch, &descriptor.global_branch]
        .into_iter()
        .map(|branch| {
            crate::changelog::CommitId::parse_lix(&branch.head.commit_id, "candidate head")
        })
        .collect::<Result<Vec<_>, _>>()?;
    let headers = crate::tracked_state::load_commit_state_authority_ids(&read, &heads).await?;
    for (head, header) in heads.into_iter().zip(headers) {
        if header.is_none() {
            return Err(crate::tracked_state::NativeMetadataRef::CommitStateHeader(
                head.to_string(),
            )
            .annotate_missing(LixError::new(
                "LIX_PARTIAL_WRITE_FRONTIER_REQUIRED",
                "candidate baseline state header is not resident",
            )));
        }
    }
    let mut staged = StorageWriteSet::new();
    let mut source_control_guards = Vec::new();
    for (index, branch) in [&descriptor.selected_branch, &descriptor.global_branch]
        .into_iter()
        .enumerate()
    {
        if index == 1 && branch.branch_id == descriptor.selected_branch.branch_id {
            if branch != &descriptor.selected_branch {
                return Err(unsupported(
                    "candidate repeats a branch with conflicting coordinates",
                ));
            }
            continue;
        }
        let head = crate::changelog::CommitId::parse_lix(&branch.head.commit_id, "candidate head")?;
        let observation = if allow_missing_selected_control
            && branch.branch_id == descriptor.selected_branch.branch_id
        {
            crate::branch::observe_branch_control_coordinate(&read, &branch.branch_id).await?
        } else {
            crate::branch::BranchHeadControlContext::new()
                .reader(&read)
                .load_observed(std::slice::from_ref(&branch.branch_id))
                .await?
                .pop()
                .expect("one candidate control")
        };
        source_control_guards.push(crate::branch::branch_head_control_precondition(
            &branch.branch_id,
            observation.raw_token,
        )?);
        if let Some(source) = observation.control {
            crate::hot_state::TrackedHeadContext::new()
                .writer(&read, &mut staged)
                .stage_untracked_for_root_generation(
                    &branch.branch_id,
                    source.tracked_generation,
                    state.serving_generation(&branch.branch_id)?,
                    head,
                )
                .await?;
        } else if !(allow_missing_selected_control
            && branch.branch_id == descriptor.selected_branch.branch_id
            && branch.branch_id != crate::GLOBAL_BRANCH_ID)
        {
            return Err(unsupported("candidate source branch is absent"));
        }
        let control = super::partial_bootstrap::partial_branch_control(state, branch)?;
        source_control_guards.extend(crate::hot_state::root_generation_absence_preconditions(
            &branch.branch_id,
            control.tracked_generation,
            control
                .working_diff_checkpoint_commit_id
                .expect("partial control has checkpoint"),
        )?);
        source_control_guards.push(
            crate::hot_state::stage_root_working_diff_epoch(
                &read,
                &mut staged,
                &branch.branch_id,
                control.tracked_generation,
                control
                    .working_diff_checkpoint_commit_id
                    .expect("partial control has checkpoint"),
            )
            .await?,
        );
        crate::branch::stage_branch_head_control(&mut staged, &branch.branch_id, control)?;
        crate::hot_state::TrackedHeadContext::new()
            .writer(&read, &mut staged)
            .stage_root_current_base(
                &branch.branch_id,
                state.serving_generation(&branch.branch_id)?,
                head,
            );
    }
    let staged = Arc::new(staged);
    let read = CandidateRead {
        base: read,
        staged: Arc::clone(&staged),
    };
    let hot = hot.with_partial_scope_policy(
        &descriptor.selected_branch.branch_id,
        &descriptor.global_branch.branch_id,
    );
    let blob = crate::binary_cas::BinaryCasContext::new();
    blob.enable_referenced_manifest_demands();
    let mut mutation_identities = std::collections::BTreeMap::<
        String,
        std::collections::BTreeSet<crate::tracked_state::TrackedStateKey>,
    >::new();
    for interest in &interests.interests {
        match interest.as_ref() {
            LogicalReadInterest::Scan { request, domain } => {
                for branch in &request.filter.branch_ids {
                    selected_branch(descriptor, branch)?;
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
                    selected_branch(descriptor, &row.branch_id)?;
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
                selected_branch(descriptor, branch_id)?;
                hot.reader(read.clone())
                    .collection_generation(
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
                selected_branch(descriptor, branch_id)?;
                hot.transaction_reader(
                    read.clone(),
                    Arc::new(crate::hot_state::BranchHeadControlCache::default()),
                )
                .prepare_packed_identity_membership(branch_id, schema_key)
                .await?;
            }
            LogicalReadInterest::FilesystemPaths {
                branch_ids,
                include_blob_refs,
                cache_small_blob_data,
            } => {
                for branch in branch_ids {
                    selected_branch(descriptor, branch)?;
                }
                hot.reader(read.clone())
                    .path_index(
                        &FilesystemPathIndexRequest::new(branch_ids.clone())
                            .with_blob_refs(*include_blob_refs)
                            .with_cached_blob_data(*cache_small_blob_data),
                    )
                    .await?;
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
                let from = endpoint(descriptor, branch_id.as_deref(), from)?;
                let to = endpoint(descriptor, branch_id.as_deref(), to)?;
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
                )
                .await?;
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
                    selected_branch(descriptor, branch)?;
                }
                let capture = crate::hot_state::ReadInterestRegistry::new(4096, 4 * 1024 * 1024);
                let replay_hot = hot.with_read_interest_registry(capture.clone());
                crate::sql2::prepare_native_file_metadata_interest(
                    Arc::new(replay_hot.reader(read.clone())),
                    Arc::new(replay_hot.reader(read.clone())),
                    *directory,
                    branch_ids,
                    file_ids.as_deref(),
                    directory_ids.as_deref(),
                    *root_directory,
                    path_predicate,
                )
                .await?;
                hot.reader(read.clone())
                    .prepare_captured_read_interests(
                        &capture.snapshot()?,
                        state.active_account_id(),
                    )
                    .await?;
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
                    selected_branch(descriptor, branch)?;
                }
                // Capture newly matching native identities at this candidate,
                // independently of the live registry and its persisted epoch.
                let capture = crate::hot_state::ReadInterestRegistry::new(4096, 4 * 1024 * 1024);
                let replay_hot = hot.with_read_interest_registry(capture.clone());
                crate::sql2::prepare_native_file_content_interest(
                    Arc::new(replay_hot.reader(read.clone())),
                    Arc::new(replay_hot.reader(read.clone())),
                    Arc::new(blob.reader(read.clone())),
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
                hot.reader(read.clone())
                    .prepare_captured_read_interests(
                        &capture.snapshot()?,
                        state.active_account_id(),
                    )
                    .await?;
            }
        }
    }
    for (branch_id, keys) in mutation_identities {
        let branch = selected_branch(descriptor, &branch_id)?;
        let Some(root) = branch.head.row_pk_index_root_id else {
            return Err(unsupported(
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
    // them against these unpublished controls before they become visible; this
    // candidate context intentionally has no trusted live-epoch proof cache.
    hot.reader(read.clone())
        .prepare_captured_read_interests(interests, state.active_account_id())
        .await?;
    drop(hot);
    drop(read);
    Ok(PreparedCandidateState {
        writes: staged,
        source_control_guards,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::{StorageAdapter, StorageWriteOptions};
    use std::ops::Bound;
    #[test]
    fn only_explicit_archives_suspend_whole_recipes() {
        let recipe = |branches: &[&str]| LogicalReadInterest::FilesystemPaths {
            branch_ids: branches.iter().map(|branch| (*branch).to_owned()).collect(),
            include_blob_refs: false,
            cache_small_blob_data: false,
        };
        let archived = vec!["old".to_owned()];
        assert!(
            !interest_belongs_to_candidate(&recipe(&["old", "global"]), "new", "global", &archived)
                .unwrap()
        );
        assert!(
            interest_belongs_to_candidate(&recipe(&["old", "unknown"]), "new", "global", &archived)
                .unwrap(),
            "unknown branches must reach the ordinary rejecting validator"
        );
        assert!(
            interest_belongs_to_candidate(&recipe(&["old"]), "new", "global", &[]).unwrap(),
            "ordinary refresh cannot silently archive scopes"
        );
        assert!(
            interest_belongs_to_candidate(
                &recipe(&["old", "global"]),
                "old",
                "global",
                &["new".to_owned()]
            )
            .unwrap(),
            "switching back resumes the original recipe without remapping"
        );
    }

    #[tokio::test]
    async fn candidate_put_merge_preserves_bounds_pages_order_and_source() {
        let adapter = StorageAdapter::new(crate::Memory::new());
        let space = crate::hot_state::TRACKED_WORKING_DIFF_MARKER_SPACE;
        let mut base = StorageWriteSet::new();
        for key in [b"a", b"c", b"e"] {
            base.put(space, key.as_slice(), b"old".as_slice());
        }
        adapter
            .commit_write_set(base, StorageWriteOptions::default())
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut staged = StorageWriteSet::new();
        for key in [b"b", b"c", b"d"] {
            staged.put(space, key.as_slice(), b"new".as_slice());
        }
        let candidate = CandidateRead {
            base: &read,
            staged: Arc::new(staged),
        };
        for order in [StorageScanOrder::Ascending, StorageScanOrder::Descending] {
            let range = KeyRange {
                lower: Bound::Excluded(StorageKey(bytes::Bytes::from_static(b"a"))),
                upper: Bound::Excluded(StorageKey(bytes::Bytes::from_static(b"e"))),
            };
            if order == StorageScanOrder::Descending {
                // Canonical Memory does not support reverse scans. Candidate
                // overlays must preserve that capability error, not fabricate
                // an ordering unsupported by their pinned source.
                let options = BeginScanOptions {
                    order,
                    projection: CoreProjection::FullValue,
                };
                let base_error = match read.begin_scan(space, range.clone(), options.clone()).await
                {
                    Err(error) => error,
                    Ok(_) => panic!("Memory unexpectedly supports reverse scans"),
                };
                let overlay_error = match candidate.begin_scan(space, range.clone(), options).await
                {
                    Err(error) => error,
                    Ok(_) => panic!("candidate hid unsupported reverse scan"),
                };
                assert_eq!(overlay_error.to_string(), base_error.to_string());
                continue;
            }
            let mut cursor = candidate
                .begin_scan(
                    space,
                    range.clone(),
                    BeginScanOptions {
                        order,
                        projection: CoreProjection::FullValue,
                    },
                )
                .await
                .unwrap();
            let mut keys = Vec::new();
            loop {
                let (page, more) = cursor.next_page(1).await.unwrap().into_parts();
                assert!(page.len() <= 1);
                for entry in page {
                    assert!(
                        matches!(entry.value, ProjectedValue::FullValue(ref bytes) if bytes.as_ref() == b"new")
                    );
                    keys.push(entry.key.0);
                }
                if !more {
                    break;
                }
            }
            let expected: &[&[u8]] = if order == StorageScanOrder::Ascending {
                &[b"b", b"c", b"d"]
            } else {
                &[b"d", b"c", b"b"]
            };
            assert_eq!(
                keys.iter().map(|key| key.as_ref()).collect::<Vec<_>>(),
                expected
            );
            let mut original = read
                .begin_scan(space, range, BeginScanOptions::default())
                .await
                .unwrap();
            let (page, more) = original.next_page(8).await.unwrap().into_parts();
            assert!(!more);
            assert_eq!(page.len(), 1);
            assert!(
                matches!(page[0].value, ProjectedValue::FullValue(ref bytes) if bytes.as_ref() == b"old")
            );
        }
    }
}
