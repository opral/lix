//! Resumable topology-only proofs. Work limits schedule slices, not history age.
use super::{blocked, limited, record};
use crate::LixError;
use crate::changelog::{CommitId, CommitRecord};
use crate::storage_adapter::{Storage, StorageAdapter, StorageAdapterRead};
use crate::tracked_state::NativeMetadataRef;
use std::collections::{BTreeMap, BTreeSet};

const MARKER: &str = "partialAncestryProof";

pub(super) struct Walk {
    ancestor: CommitRecord,
    descendant: CommitId,
    pending: Vec<(CommitId, Option<u64>, Option<u64>)>,
    visited: BTreeSet<CommitId>,
    incorporated: bool,
    excluded_base: Option<CommitId>,
    source_proof: Option<Box<Walk>>,
    source_probe_complete: bool,
    legacy_unknown: bool,
    #[cfg(test)]
    pub(super) steps: usize,
}

impl Walk {
    pub(super) fn new(ancestor: CommitRecord, descendant: CommitId) -> Self {
        Self {
            ancestor,
            descendant,
            pending: vec![(descendant, None, None)],
            visited: BTreeSet::new(),
            incorporated: false,
            excluded_base: None,
            source_proof: None,
            source_probe_complete: false,
            legacy_unknown: false,
            #[cfg(test)]
            steps: 0,
        }
    }

    fn annotate_missing(
        &mut self,
        edge: (CommitId, Option<u64>, Option<u64>),
        mut error: LixError,
    ) -> Result<LixError, LixError> {
        self.pending.push(edge);
        if NativeMetadataRef::from_missing_error(&error)?.is_some() {
            let details = error
                .details
                .get_or_insert_with(|| Box::new(serde_json::json!({})));
            details[MARKER] = serde_json::json!({
                "ancestor": self.ancestor.commit_id.to_string(),
                "descendant": self.descendant.to_string(),
                "incorporated": self.incorporated,
                "excludedBase": self.excluded_base.map(|id| id.to_string()),
            });
        }
        Ok(error)
    }

    pub(super) async fn run(
        &mut self,
        read: &(impl StorageAdapterRead + ?Sized),
        cache: &mut BTreeMap<CommitId, CommitRecord>,
        work_slice: usize,
    ) -> Result<bool, LixError> {
        if work_slice == 0 {
            return Err(limited("ancestry work slice must be positive"));
        }
        let mut work = 0;
        while let Some(edge @ (current, child_generation, jump_generation)) = self.pending.pop() {
            #[cfg(test)]
            {
                self.steps += 1;
            }
            work += 1;
            if work == work_slice {
                tokio::task::yield_now().await;
                work = 0;
            }
            let node = if current == self.ancestor.commit_id {
                self.ancestor.clone()
            } else if let Some(node) = cache.get(&current) {
                node.clone()
            } else {
                match record(read, current, false).await {
                    Ok(node) => {
                        cache.insert(current, node.clone());
                        node
                    }
                    Err(error) => {
                        // Keep the edge and all completed work across the network
                        // fetch. Only a typed missing input may resume this proof.
                        return Err(self.annotate_missing(edge, error)?);
                    }
                }
            };
            if child_generation.is_some_and(|generation| node.generation >= generation) {
                return Err(blocked(
                    "causal parent generation does not precede its child",
                ));
            }
            if jump_generation.is_some_and(|generation| node.generation != generation) {
                return Err(blocked(
                    "causal jump target generation does not match its span",
                ));
            }
            if current == self.ancestor.commit_id {
                return Ok(true);
            }
            if Some(current) == self.excluded_base {
                continue;
            }
            if self.visited.contains(&current)
                || (!self.incorporated && node.generation <= self.ancestor.generation)
            {
                continue;
            }
            let complete_source = if self.incorporated {
                match crate::tracked_state::load_published_commit_state_topology(read, current)
                    .await
                {
                    Ok(Some(topology)) => {
                        let alias = crate::sync::commit::load_complete_state_alias_source(
                            read,
                            current,
                            topology.complete_state_source_commit_id(),
                        )
                        .await?;
                        match topology.incorporation() {
                            crate::tracked_state::CommitStateIncorporation::Complete(source) => {
                                Some(source)
                            }
                            crate::tracked_state::CommitStateIncorporation::None => alias,
                            crate::tracked_state::CommitStateIncorporation::LegacyUnknown => {
                                if alias.is_none() && !node.is_checkpoint {
                                    // Legacy aliases have one parent and no members.
                                    // SQL working continuations have a checkpoint parent.
                                    // Canonical graph/header facts can exclude both without
                                    // refining the immutable legacy header itself.
                                    let ordinary = match node.parent_commit_ids.as_slice() {
                                        [parent] if topology.mutation_member_count() > 0 => {
                                            let parent = if let Some(parent) = cache.get(parent) {
                                                parent.clone()
                                            } else {
                                                match record(read, *parent, false).await {
                                                    Ok(parent) => {
                                                        cache.insert(
                                                            parent.commit_id,
                                                            parent.clone(),
                                                        );
                                                        parent
                                                    }
                                                    Err(error) => {
                                                        return Err(
                                                            self.annotate_missing(edge, error)?
                                                        );
                                                    }
                                                }
                                            };
                                            !parent.is_checkpoint
                                        }
                                        [_] => false,
                                        _ => true,
                                    };
                                    self.legacy_unknown |= !ordinary;
                                } else {
                                    self.legacy_unknown |= alias.is_none();
                                }
                                alias
                            }
                        }
                    }
                    Ok(None) => {
                        let error = NativeMetadataRef::CommitStateHeader(current.to_string())
                            .annotate_missing(blocked(
                                "complete state-source header must be hydrated",
                            ));
                        return Err(self.annotate_missing(edge, error)?);
                    }
                    Err(error) => return Err(error),
                }
            } else {
                None
            };
            if let Some(source) = complete_source.filter(|_| !self.source_probe_complete) {
                // A whole-state source commonly names the top of a long local
                // linear suffix. Try its native causal jumps before loading a
                // header for every commit, retaining this subproof across misses.
                // One probe per walk avoids rewalking overlapping source histories.
                let proof = self
                    .source_proof
                    .get_or_insert_with(|| Box::new(Walk::new(self.ancestor.clone(), source)));
                match Box::pin(proof.run(read, cache, work_slice)).await {
                    Ok(true) => return Ok(true),
                    Ok(false) => {
                        self.source_proof = None;
                        self.source_probe_complete = true;
                    }
                    Err(error) => return Err(self.annotate_missing(edge, error)?),
                }
            }
            self.visited.insert(current);
            if !self.incorporated
                && node.parent_commit_ids.len() == 1
                && node.first_parent_jump_span > 1
            {
                let generation = node
                    .generation
                    .checked_sub(node.first_parent_jump_span)
                    .ok_or_else(|| blocked("causal jump span exceeds its generation"))?;
                if node.first_parent_jump_commit_id == current {
                    return Err(blocked("causal jump contains a cycle"));
                }
                // Only linear jumps prove that no secondary path was skipped.
                if generation >= self.ancestor.generation {
                    self.pending.push((
                        node.first_parent_jump_commit_id,
                        Some(node.generation),
                        Some(generation),
                    ));
                    continue;
                }
            }
            for parent in node.parent_commit_ids {
                self.pending.push((parent, Some(node.generation), None));
            }
            if let Some(source) = complete_source {
                // Checkpoint sources may have a higher causal generation than
                // their compacted checkpoint. Only complete-state fences prove
                // incorporation; selected source ranges cannot establish it.
                self.pending.push((source, None, None));
            }
        }
        if self.legacy_unknown {
            Err(LixError::new(
                "LIX_PARTIAL_MERGE_PROOF_UNAVAILABLE",
                "legacy checkpoint incorporation provenance is unavailable",
            )
            .with_hint("retain the replica storage; synchronization requires a provable incorporation path or recovery from the original source or backup, and fetching row payloads alone cannot restore erased legacy provenance"))
        } else {
            Ok(false)
        }
    }
}

/// Complete state incorporation includes causal ancestry and authenticated
/// whole-state checkpoint aliases. It does not imply causal DAG ancestry.
pub(crate) async fn incorporated(
    read: &(impl StorageAdapterRead + ?Sized),
    ancestor: &CommitRecord,
    descendant: CommitId,
    cache: &mut BTreeMap<CommitId, CommitRecord>,
    work_slice: usize,
) -> Result<bool, LixError> {
    if Walk::new(ancestor.clone(), descendant)
        .run(read, cache, work_slice)
        .await?
    {
        return Ok(true);
    }
    let mut walk = Walk::new(ancestor.clone(), descendant);
    walk.incorporated = true;
    walk.run(read, cache, work_slice).await
}

/// A strict predecessor of the incoming head cannot already incorporate it.
/// Prove that relation before using the confirmed base as an exclusion boundary;
/// never infer it from causal generation across checkpoint aliases.
pub(crate) async fn incorporated_since(
    read: &(impl StorageAdapterRead + ?Sized),
    ancestor: &CommitRecord,
    descendant: CommitId,
    confirmed_base: &CommitRecord,
    cache: &mut BTreeMap<CommitId, CommitRecord>,
    work_slice: usize,
) -> Result<bool, LixError> {
    if ancestor.commit_id == confirmed_base.commit_id
        || !incorporated(read, confirmed_base, ancestor.commit_id, cache, work_slice).await?
    {
        return incorporated(read, ancestor, descendant, cache, work_slice).await;
    }
    let mut walk = Walk::new(ancestor.clone(), descendant);
    walk.incorporated = true;
    walk.excluded_base = Some(confirmed_base.commit_id);
    walk.run(read, cache, work_slice).await
}

/// A cold proof retains its traversal across metadata fetches. The caller reruns
/// native analysis once after completion; it never replays the growing prefix
/// after every missing graph record. No storage read survives network I/O.
pub(crate) async fn hydrate<S, C>(
    storage: &StorageAdapter<S>,
    state: &crate::sync::partial_state::PartialReplicaState,
    transport: &crate::sync::http::HttpSyncTransport<C>,
    error: &LixError,
) -> Result<bool, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: crate::sync::http::RawHttpClient,
{
    let Some(marker) = error
        .details
        .as_ref()
        .and_then(|details| details.get(MARKER))
    else {
        return Ok(false);
    };
    let coordinate = |key| {
        let value = marker
            .get(key)
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| blocked("ancestry hydration coordinate is absent"))?;
        CommitId::parse_lix(value, "ancestry hydration")
    };
    let read = storage.begin_read(Default::default()).await?;
    let ancestor = record(&read, coordinate("ancestor")?, false).await?;
    drop(read);
    let mut walk = Walk::new(ancestor, coordinate("descendant")?);
    walk.incorporated = marker
        .get("incorporated")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    walk.excluded_base = marker
        .get("excludedBase")
        .and_then(serde_json::Value::as_str)
        .map(|id| CommitId::parse_lix(id, "ancestry excluded base"))
        .transpose()?;
    let mut cache = BTreeMap::new();
    loop {
        let read = storage.begin_read(Default::default()).await?;
        let result = walk.run(&read, &mut cache, 256).await;
        drop(read);
        match result {
            Ok(_) => return Ok(true),
            Err(error) => {
                let Some(
                    address @ (NativeMetadataRef::CommitGraphRecord(_)
                    | NativeMetadataRef::CommitStateHeader(_)),
                ) = NativeMetadataRef::from_missing_error(&error)?
                else {
                    return Err(error);
                };
                crate::sync::partial_runtime::hydrate_metadata_batch(
                    storage,
                    state,
                    transport,
                    vec![address],
                )
                .await?;
            }
        }
    }
}
