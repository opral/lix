#![allow(
    clippy::cast_possible_truncation,
    clippy::clone_on_copy,
    clippy::needless_borrows_for_generic_args,
    clippy::needless_pass_by_ref_mut,
    clippy::unused_self
)]

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use crate::LixError;
use crate::branch::{BRANCH_REF_SCHEMA_KEY, BranchRefReader};
use crate::changelog::{ChangeId, ChangeRecord, CommitId, CommitRecord};
use crate::commit_graph::{
    CommitGraphChange, CommitGraphChangeHistoryEntry, CommitGraphChangeHistoryRequest,
    CommitGraphHistory, CommitGraphNode, CommitGraphReader, ReachableCommitGraphNode,
};
use crate::common::ExactBatch;
use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::live_state::{
    LiveStateExactBatchRequest, LiveStateReader, LiveStateScanRequest, MaterializedLiveStateBatch,
    MaterializedLiveStateBatchBuilder, MaterializedLiveStateRow,
};
use crate::storage_adapter::StorageAdapterRead;

const COMMIT_SCHEMA_KEY: &str = "lix_commit";
/// Read model for resolving changelog commit facts at a head.
///
/// The commit graph owns semantic commit metadata. Physical tracked-state
/// manifests are required by state/history payload readers, but GC may retire
/// those manifests while retaining a changelog projection until the semantic
/// commit itself becomes unreachable. Metadata reads must therefore not make
/// the physical serving manifest a second membership authority.
///
/// The changelog commit plane is a compact serving projection. State/history
/// payload readers validate physical commit-state authority before decoding
/// tracked data; metadata topology does not require that physical projection.
#[derive(Clone)]
pub(crate) struct CommitGraphContext;

/// Derived commit surfaces must not enter the current-state reader. This
/// reader is the SQL adapter for the authenticated ForkTree commit topology:
/// it obtains branch heads and reachable topology from the operation's graph
/// capability, then exposes only terminal row snapshots to the existing entity
/// projection machinery.
pub(crate) struct CommitGraphLiveStateReader {
    schema_key: String,
    commit_graph: Arc<tokio::sync::Mutex<Box<dyn CommitGraphReader>>>,
    branch_ref: Arc<dyn BranchRefReader>,
    current_state: Option<Arc<dyn LiveStateReader>>,
    include_recovery_roots: bool,
    include_retained_nodes: bool,
}

impl CommitGraphLiveStateReader {
    pub(crate) fn new(
        schema_key: impl Into<String>,
        commit_graph: Arc<tokio::sync::Mutex<Box<dyn CommitGraphReader>>>,
        branch_ref: Arc<dyn BranchRefReader>,
        current_state: Option<Arc<dyn LiveStateReader>>,
        include_recovery_roots: bool,
        include_retained_nodes: bool,
    ) -> Self {
        Self {
            schema_key: schema_key.into(),
            commit_graph,
            branch_ref,
            current_state,
            include_recovery_roots,
            include_retained_nodes,
        }
    }

    async fn checkpoint_roots(
        &self,
    ) -> Result<(BTreeMap<String, BTreeSet<CommitId>>, bool), LixError> {
        let Some(current_state) = self.current_state.as_ref() else {
            return Ok((BTreeMap::new(), false));
        };
        let rows = current_state
            .scan_batch(&LiveStateScanRequest {
                filter: crate::live_state::LiveStateFilter {
                    rows: crate::live_state::LiveStateRowFilter::All,
                    schema_keys: vec![
                        "lix_checkpoint_recovery".to_owned(),
                        "lix_checkpoint_gc_state".to_owned(),
                    ],
                    branch_ids: vec![crate::GLOBAL_BRANCH_ID.to_owned()],
                    untracked: Some(true),
                    include_tombstones: true,
                    ..Default::default()
                },
                ..Default::default()
            })
            .await?
            .into_rows();
        let mut roots = BTreeMap::<String, BTreeSet<CommitId>>::new();
        let mut collectible_interval_count = 0;
        let mut checkpoint_sequence = None;
        let mut last_gc_sequence = None;
        let mut gc_state_seen = false;
        for row in rows {
            if row.schema_key == "lix_checkpoint_gc_state" {
                if row.file_id.is_some()
                    || row.deleted
                    || row.snapshot_content.is_none()
                    || row.entity_pk != EntityPk::single("repository")
                {
                    return Err(LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        "checkpoint GC state is missing an authenticated live value",
                    ));
                }
                let snapshot = row
                    .snapshot_content
                    .as_ref()
                    .expect("checkpoint GC state snapshot checked above");
                let object = serde_json::from_str::<serde_json::Value>(snapshot.as_str()).map_err(
                    |error| {
                        LixError::new(
                            LixError::CODE_STORAGE_ERROR,
                            format!("checkpoint GC state is malformed: {error}"),
                        )
                    },
                )?;
                if object.get("version").and_then(serde_json::Value::as_u64) != Some(1) {
                    return Err(LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        "checkpoint GC state version is unsupported",
                    ));
                }
                let debt = object
                    .get("collectible_interval_count")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_STORAGE_ERROR,
                            "checkpoint GC state is missing collectible debt",
                        )
                    })?;
                collectible_interval_count = debt;
                checkpoint_sequence = Some(
                    object
                        .get("checkpoint_sequence")
                        .and_then(serde_json::Value::as_u64)
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_STORAGE_ERROR,
                                "checkpoint GC state is missing its checkpoint sequence",
                            )
                        })?,
                );
                last_gc_sequence = Some(
                    object
                        .get("last_gc_sequence")
                        .and_then(serde_json::Value::as_u64)
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_STORAGE_ERROR,
                                "checkpoint GC state is missing its last GC sequence",
                            )
                        })?,
                );
                if std::mem::replace(&mut gc_state_seen, true) {
                    return Err(LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        "checkpoint GC state is duplicated",
                    ));
                }
                continue;
            }
            if row.schema_key != "lix_checkpoint_recovery"
                || row.file_id.is_some()
                || row.deleted
                || row.snapshot_content.is_none()
            {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "checkpoint recovery root is missing an authenticated live value",
                ));
            }
            let snapshot = row
                .snapshot_content
                .as_ref()
                .expect("checkpoint recovery snapshot checked above");
            let object =
                serde_json::from_str::<serde_json::Value>(snapshot.as_str()).map_err(|error| {
                    LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        format!("checkpoint recovery root is malformed: {error}"),
                    )
                })?;
            let branch_id = object
                .get("branch_id")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        "checkpoint recovery root is missing its branch identity",
                    )
                })?;
            if row.entity_pk != EntityPk::single(branch_id) {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "checkpoint recovery root identity does not match its state key",
                ));
            }
            let recovered_head = CommitId::parse_lix(
                object
                    .get("recovered_head_commit_id")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_STORAGE_ERROR,
                            "checkpoint recovery root is missing its recovered head",
                        )
                    })?,
                "checkpoint recovery recovered head",
            )?;
            let checkpoint_commit = CommitId::parse_lix(
                object
                    .get("checkpoint_commit_id")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_STORAGE_ERROR,
                            "checkpoint recovery root is missing its checkpoint commit",
                        )
                    })?,
                "checkpoint recovery checkpoint commit",
            )?;
            let branch_roots = roots.entry(branch_id.to_owned()).or_default();
            branch_roots.insert(recovered_head);
            branch_roots.insert(checkpoint_commit);
        }
        let retain_unreached_until_sweep = if collectible_interval_count == 0 {
            false
        } else {
            let checkpoint_sequence = checkpoint_sequence.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "checkpoint GC state is missing its authenticated sequence",
                )
            })?;
            let last_gc_sequence = last_gc_sequence.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "checkpoint GC state is missing its authenticated GC sequence",
                )
            })?;
            let age = checkpoint_sequence
                .checked_sub(last_gc_sequence)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        "checkpoint GC state sequence is inconsistent",
                    )
                })?;
            let age_limit = 64_u64.max(last_gc_sequence);
            age < age_limit
        };
        Ok((roots, retain_unreached_until_sweep))
    }

    async fn rows_for_request(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        if self.schema_key == BRANCH_REF_SCHEMA_KEY {
            return self.branch_ref_rows_for_request(request).await;
        }
        if !matches!(
            request.filter.rows,
            crate::live_state::LiveStateRowFilter::All
        ) {
            return Ok(Vec::new());
        }
        if !request.filter.schema_keys.is_empty()
            && !request
                .filter
                .schema_keys
                .iter()
                .any(|schema_key| schema_key == &self.schema_key)
        {
            return Ok(Vec::new());
        }

        // The base commit/edge surfaces are global derived views: their
        // logical contents must not depend on whichever branch happens to be
        // active in the SQL session.  EntityBase still seeds the generic
        // request with that active branch for ordinary schemas, so resolve
        // the authenticated branch-selector set here for these two ForkTree
        // owned schemas.  The by-branch surfaces deliberately keep their
        // caller-provided branch scope below.
        let global_commit_surface =
            matches!(self.schema_key.as_str(), "lix_commit" | "lix_commit_edge")
                && self.include_recovery_roots
                && self.include_retained_nodes;
        let (recovery_roots, retained_nodes_until_gc) = if self.include_recovery_roots {
            self.checkpoint_roots().await?
        } else {
            (BTreeMap::new(), false)
        };
        let requested_branch_ids = if global_commit_surface || request.filter.branch_ids.is_empty()
        {
            None
        } else {
            let mut unique = Vec::with_capacity(request.filter.branch_ids.len());
            let mut seen = BTreeSet::new();
            for branch_id in &request.filter.branch_ids {
                if seen.insert(branch_id.clone()) {
                    unique.push(branch_id.clone());
                }
            }
            Some(unique)
        };
        let authenticated_heads = match requested_branch_ids.as_deref() {
            Some(branch_ids) => self.branch_ref.load_head_metadata_batch(branch_ids).await?,
            None => self.branch_ref.scan_head_metadata().await?,
        };
        let authenticated_heads_by_branch = authenticated_heads
            .iter()
            .map(|(head, _metadata)| (head.branch_id.clone(), head.clone()))
            .collect::<BTreeMap<_, _>>();
        let global_root_ids = if global_commit_surface {
            let mut root_ids = BTreeSet::new();
            for (head, _metadata) in &authenticated_heads {
                root_ids.insert(head.commit_id);
            }
            for roots in recovery_roots.values() {
                root_ids.extend(roots.iter().copied());
            }
            root_ids
        } else {
            BTreeSet::new()
        };
        let branch_ids = if global_commit_surface {
            // Global commit entities are facts of the one authenticated
            // ForkTree topology, not one projection per branch selector. Walk
            // every authenticated branch/recovery root once and emit the
            // deduplicated result in the global scope below.
            vec![crate::GLOBAL_BRANCH_ID.to_owned()]
        } else if request.filter.branch_ids.is_empty() {
            authenticated_heads_by_branch
                .keys()
                .cloned()
                .collect::<Vec<_>>()
        } else {
            requested_branch_ids.expect("explicit branch filters select a requested batch")
        };
        let mut rows = Vec::new();
        for branch_id in branch_ids {
            let head = if global_commit_surface {
                None
            } else {
                Some(
                    authenticated_heads_by_branch
                        .get(&branch_id)
                        .cloned()
                        .ok_or_else(|| {
                            LixError::branch_not_found(
                                branch_id.clone(),
                                "scan ForkTree derived commit surface",
                                "branch head",
                            )
                        })?,
                )
            };
            let mut reachable_by_id = BTreeMap::new();
            if global_commit_surface && retained_nodes_until_gc {
                // Before the scheduled sweep, expose the complete
                // authenticated topology so readers do not observe a partial
                // retirement window. Once the cadence passes, the root walk
                // below hides expired, unreachable commits from the semantic
                // projection even if physical reclamation is still pending.
                let retained = {
                    let mut graph = self.commit_graph.lock().await;
                    graph.retained_nodes().await?
                };
                for commit in retained {
                    reachable_by_id
                        .entry(commit.commit_id)
                        .or_insert(ReachableCommitGraphNode { commit, depth: 0 });
                }
            } else if global_commit_surface {
                for root_id in &global_root_ids {
                    let reachable = {
                        let mut graph = self.commit_graph.lock().await;
                        graph.reachable_nodes(root_id).await?.to_vec()
                    };
                    for reachable in reachable {
                        reachable_by_id
                            .entry(reachable.commit.commit_id)
                            .or_insert(reachable);
                    }
                }
            } else if self.include_retained_nodes && retained_nodes_until_gc {
                let retained = {
                    let mut graph = self.commit_graph.lock().await;
                    graph.retained_nodes().await?
                };
                for commit in retained {
                    reachable_by_id
                        .entry(commit.commit_id)
                        .or_insert(ReachableCommitGraphNode { commit, depth: 0 });
                }
            } else {
                let head = head.expect("non-global derived surfaces require a selected head");
                let mut root_ids = BTreeSet::from([head.commit_id]);
                if let Some(roots) = recovery_roots.get(&branch_id) {
                    root_ids.extend(roots.iter().copied());
                }
                if self.schema_key == "lix_commit" && !self.include_retained_nodes {
                    for entity_pk in &request.filter.entity_pks {
                        let Ok(commit_text) = entity_pk.as_single_string_owned() else {
                            continue;
                        };
                        if let Ok(commit_id) = CommitId::parse_lix(&commit_text, "requested commit")
                        {
                            root_ids.insert(commit_id);
                        }
                    }
                }
                for root_id in root_ids {
                    let reachable = {
                        let mut graph = self.commit_graph.lock().await;
                        graph.reachable_nodes(&root_id).await?.to_vec()
                    };
                    for reachable in reachable {
                        reachable_by_id
                            .entry(reachable.commit.commit_id)
                            .or_insert(reachable);
                    }
                }
            }
            for reachable in reachable_by_id.into_values() {
                let commit = &reachable.commit;
                if self.schema_key == "lix_commit" {
                    let snapshot =
                        crate::changelog::commit_row_snapshot_json(&commit.commit_id.to_string())?;
                    let entity_pk = EntityPk::uuid_from_canonical(&commit.commit_id.to_string())
                        .map_err(|error| {
                            LixError::new(
                                LixError::CODE_STORAGE_ERROR,
                                format!("authenticated commit ID is not a UUID: {error}"),
                            )
                        })?;
                    rows.push(MaterializedLiveStateRow {
                        entity_pk,
                        schema_key: self.schema_key.clone(),
                        file_id: None,
                        snapshot_content: Some(snapshot.into()),
                        metadata: None,
                        deleted: false,
                        created_at: LixTimestamp::from_unix_millis_utc_lossy(0),
                        updated_at: LixTimestamp::from_unix_millis_utc_lossy(0),
                        global: branch_id == crate::GLOBAL_BRANCH_ID,
                        change_id: None,
                        commit_id: Some(commit.commit_id),
                        untracked: false,
                        branch_id: Arc::from(branch_id.as_str()),
                    });
                } else if self.schema_key == "lix_commit_edge" {
                    for (parent_order, parent_id) in commit.parent_commit_ids.iter().enumerate() {
                        let parent_order = i64::try_from(parent_order).map_err(|_| {
                            LixError::new(
                                LixError::CODE_STORAGE_ERROR,
                                "authenticated commit parent order exceeds SQL integer range",
                            )
                        })?;
                        let snapshot = serde_json::json!({
                            "parent_id": parent_id.to_string(),
                            "child_id": commit.commit_id.to_string(),
                            "parent_order": parent_order,
                        });
                        let entity_pk = EntityPk::from_json_values(
                            &[
                                serde_json::Value::String(commit.commit_id.to_string()),
                                serde_json::Value::Number(parent_order.into()),
                            ],
                            &[
                                crate::entity_pk::EntityPkComponentType::Uuid,
                                crate::entity_pk::EntityPkComponentType::Integer,
                            ],
                        )
                        .map_err(|error| {
                            LixError::new(
                                LixError::CODE_STORAGE_ERROR,
                                format!("authenticated commit edge identity is invalid: {error}"),
                            )
                        })?;
                        rows.push(MaterializedLiveStateRow {
                            entity_pk,
                            schema_key: self.schema_key.clone(),
                            file_id: None,
                            snapshot_content: Some(
                                serde_json::to_string(&snapshot)
                                    .map_err(|error| {
                                        LixError::new(
                                            LixError::CODE_STORAGE_ERROR,
                                            format!(
                                                "commit edge snapshot serialization failed: {error}"
                                            ),
                                        )
                                    })?
                                    .into(),
                            ),
                            metadata: None,
                            deleted: false,
                            created_at: LixTimestamp::from_unix_millis_utc_lossy(0),
                            updated_at: LixTimestamp::from_unix_millis_utc_lossy(0),
                            global: branch_id == crate::GLOBAL_BRANCH_ID,
                            change_id: None,
                            commit_id: Some(commit.commit_id),
                            untracked: false,
                            branch_id: Arc::from(branch_id.as_str()),
                        });
                    }
                } else {
                    return Err(LixError::new(
                        LixError::CODE_UNSUPPORTED_SQL,
                        format!(
                            "ForkTree derived reader does not serve '{}'",
                            self.schema_key
                        ),
                    ));
                }
            }
        }

        if !request.filter.entity_pks.is_empty() {
            rows.retain(|row| request.filter.entity_pks.contains(&row.entity_pk));
        }
        rows.retain(|_| {
            request
                .filter
                .file_ids
                .iter()
                .all(|file_id| file_id.matches(None))
        });
        rows.sort_by(|left, right| {
            left.branch_id
                .cmp(&right.branch_id)
                .then_with(|| left.entity_pk.cmp(&right.entity_pk))
        });
        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }

    async fn branch_ref_rows(
        &self,
        requested_branch_ids: Option<&[String]>,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let mut rows = Vec::new();
        let heads = match requested_branch_ids {
            Some(branch_ids) => self.branch_ref.load_head_metadata_batch(branch_ids).await?,
            None => self.branch_ref.scan_head_metadata().await?,
        };
        for (head, metadata) in heads {
            let entity_pk = EntityPk::uuid_from_canonical(&head.branch_id).map_err(|error| {
                LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    format!("authenticated branch ID is not a UUID: {error}"),
                )
            })?;
            let snapshot = serde_json::json!({
                "id": head.branch_id,
                "commit_id": head.commit_id.to_string(),
            });
            rows.push(MaterializedLiveStateRow {
                entity_pk,
                schema_key: BRANCH_REF_SCHEMA_KEY.to_string(),
                file_id: None,
                snapshot_content: Some(
                    serde_json::to_string(&snapshot)
                        .map_err(|error| {
                            LixError::new(
                                LixError::CODE_STORAGE_ERROR,
                                format!("branch-ref snapshot serialization failed: {error}"),
                            )
                        })?
                        .into(),
                ),
                metadata: None,
                deleted: false,
                created_at: LixTimestamp::from_unix_millis_utc_lossy(0),
                updated_at: metadata.updated_at,
                global: true,
                change_id: Some(metadata.change_id),
                commit_id: Some(head.commit_id),
                untracked: true,
                branch_id: Arc::from(crate::GLOBAL_BRANCH_ID),
            });
        }
        rows.sort_by(|left, right| left.entity_pk.cmp(&right.entity_pk));
        Ok(rows)
    }

    async fn branch_ref_rows_for_request(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        if !matches!(
            request.filter.rows,
            crate::live_state::LiveStateRowFilter::All
        ) {
            return Ok(Vec::new());
        }
        if request.filter.untracked == Some(false)
            || (!request.filter.branch_ids.is_empty()
                && !request
                    .filter
                    .branch_ids
                    .iter()
                    .any(|branch_id| branch_id == crate::GLOBAL_BRANCH_ID))
        {
            return Ok(Vec::new());
        }
        let requested_branch_ids = if request.filter.entity_pks.is_empty() {
            None
        } else {
            let mut unique = Vec::with_capacity(request.filter.entity_pks.len());
            let mut seen = BTreeSet::new();
            for entity_pk in &request.filter.entity_pks {
                let branch_id = entity_pk.as_single_string_owned().map_err(|error| {
                    LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        format!("requested branch-ref identity is not a UUID: {error}"),
                    )
                })?;
                if seen.insert(branch_id.clone()) {
                    unique.push(branch_id);
                }
            }
            Some(unique)
        };
        let mut rows = self
            .branch_ref_rows(requested_branch_ids.as_deref())
            .await?;
        rows.retain(|row| {
            request.filter.entity_pks.is_empty()
                || request.filter.entity_pks.contains(&row.entity_pk)
        });
        rows.retain(|_row| {
            request
                .filter
                .file_ids
                .iter()
                .all(|file_id| file_id.matches(None))
        });
        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }
}

#[async_trait::async_trait]
impl LiveStateReader for CommitGraphLiveStateReader {
    async fn scan_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        Ok(MaterializedLiveStateBatch::from_rows(
            self.rows_for_request(request).await?,
        ))
    }

    async fn load_exact_batch(
        &self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<crate::live_state::MaterializedLiveStateExactBatch, LixError> {
        if self.schema_key == BRANCH_REF_SCHEMA_KEY {
            let mut slots = Vec::with_capacity(request.rows.len());
            let mut builder = MaterializedLiveStateBatchBuilder::with_capacity(request.rows.len());
            if request.untracked == Some(false) {
                slots.resize(request.rows.len(), None);
                return crate::live_state::MaterializedLiveStateExactBatch::new(
                    builder.finish(),
                    slots,
                );
            }
            let mut requested_branch_ids = Vec::with_capacity(request.rows.len());
            let mut seen = BTreeSet::new();
            for requested in &request.rows {
                if requested.schema_key != BRANCH_REF_SCHEMA_KEY {
                    return Err(LixError::new(
                        LixError::CODE_UNSUPPORTED_SQL,
                        "ForkTree branch-ref exact reads require the branch-ref schema",
                    ));
                }
                if requested.file_id.is_some() {
                    return Err(LixError::new(
                        LixError::CODE_UNSUPPORTED_SQL,
                        "ForkTree branch-ref exact reads require a NULL file_id",
                    ));
                }
                if requested.branch_id != crate::GLOBAL_BRANCH_ID {
                    return Err(LixError::new(
                        LixError::CODE_UNSUPPORTED_SQL,
                        "ForkTree branch-ref exact reads require the global branch",
                    ));
                }
                let branch_id = requested
                    .entity_pk
                    .as_single_string_owned()
                    .map_err(|error| {
                        LixError::new(
                            LixError::CODE_STORAGE_ERROR,
                            format!("requested branch-ref identity is not a UUID: {error}"),
                        )
                    })?;
                if seen.insert(branch_id.clone()) {
                    requested_branch_ids.push(branch_id);
                }
            }
            let rows = self.branch_ref_rows(Some(&requested_branch_ids)).await?;
            for requested in &request.rows {
                let Some(row) = rows.iter().find(|row| row.entity_pk == requested.entity_pk) else {
                    slots.push(None);
                    continue;
                };
                let ordinal = u32::try_from(builder.len()).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "exact branch-ref result exceeds u32 rows",
                    )
                })?;
                builder.push_owned(row.clone());
                slots.push(Some(ordinal));
            }
            return crate::live_state::MaterializedLiveStateExactBatch::new(
                builder.finish(),
                slots,
            );
        }
        Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "ForkTree derived commit surfaces do not support current-state exact reads",
        ))
    }
}

impl CommitGraphContext {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Creates a graph reader over a caller-provided KV store.
    pub(crate) fn reader<S>(&self, store: S) -> CommitGraphStoreReader<S>
    where
        S: StorageAdapterRead,
    {
        CommitGraphStoreReader {
            topology: crate::forktree::CommitTopologyReader::new(store),
            node_cache: HashMap::new(),
            reachable_nodes_cache: HashMap::new(),
            member_changes_cache: HashMap::new(),
        }
    }
}

/// Commit-graph reader that resolves changelog entities at a commit head.
pub(crate) struct CommitGraphStoreReader<S>
where
    S: StorageAdapterRead,
{
    topology: crate::forktree::CommitTopologyReader<S>,
    node_cache: HashMap<CommitId, CommitGraphNode>,
    reachable_nodes_cache: HashMap<CommitId, Arc<[ReachableCommitGraphNode]>>,
    // A reader is bound to one pinned storage snapshot for the duration of a
    // SQL statement. File-history shaping asks the same reader for distinct
    // schema slices of that history, so retain immutable change records here.
    member_changes_cache: HashMap<Vec<String>, HashMap<CommitId, Vec<CommitGraphChange>>>,
}

enum LinearMergeBase {
    Resolved(CommitId),
    Disconnected,
    GeneralGraph,
}

impl<S> CommitGraphStoreReader<S>
where
    S: StorageAdapterRead,
{
    /// Loads one topology node without reading its member delta or payloads.
    pub(crate) async fn load_node(
        &mut self,
        commit_id: &CommitId,
    ) -> Result<Option<CommitGraphNode>, LixError> {
        Ok(self
            .load_nodes(std::slice::from_ref(commit_id))
            .await?
            .into_iter()
            .next()
            .and_then(|(_, value)| value))
    }

    pub(crate) async fn load_nodes<'a>(
        &mut self,
        commit_ids: &'a [CommitId],
    ) -> Result<ExactBatch<'a, CommitId, CommitGraphNode>, LixError> {
        let uncached_ids = commit_ids
            .iter()
            .filter(|commit_id| !self.node_cache.contains_key(commit_id))
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if !uncached_ids.is_empty() {
            let loaded = self.topology.load(&uncached_ids).await?;
            for topology in loaded.cache_seeded {
                let node = commit_graph_node_from_topology(topology);
                self.node_cache.insert(node.commit_id, node);
            }
            let batch = ExactBatch::try_new(
                "ForkTree commit graph",
                &uncached_ids,
                loaded
                    .requested
                    .into_iter()
                    .map(|topology| topology.map(commit_graph_node_from_topology))
                    .collect(),
            )?;
            for (commit_id, topology) in batch {
                if let Some(topology) = topology {
                    self.node_cache.insert(*commit_id, topology);
                }
            }
        }
        let nodes = commit_ids
            .iter()
            .map(|commit_id| self.node_cache.get(commit_id).cloned())
            .collect();
        ExactBatch::try_new("commit graph", commit_ids, nodes)
    }

    /// Walks from `head_commit_id` through parent commits and records nearest depth.
    pub(crate) async fn reachable_nodes(
        &mut self,
        head_commit_id: &CommitId,
    ) -> Result<Arc<[ReachableCommitGraphNode]>, LixError> {
        if let Some(nodes) = self.reachable_nodes_cache.get(head_commit_id) {
            return Ok(Arc::clone(nodes));
        }
        let nodes = Arc::from(walk_reachable_nodes(self, head_commit_id).await?);
        self.reachable_nodes_cache
            .insert(*head_commit_id, Arc::clone(&nodes));
        Ok(nodes)
    }

    pub(crate) async fn retained_nodes(&mut self) -> Result<Vec<CommitGraphNode>, LixError> {
        let mut commits = Vec::new();
        let mut start_after = None;
        loop {
            let page =
                crate::forktree::scan_commit_topologies(self.topology.read(), start_after, 1024)
                    .await?;
            if page.is_empty() {
                break;
            }
            let page_len = page.len();
            for topology in page {
                let node = commit_graph_node_from_topology(topology);
                self.node_cache.insert(node.commit_id, node.clone());
                commits.push(node);
            }
            if page_len < 1024 {
                break;
            }
            start_after = commits.last().map(|node| node.commit_id);
        }
        Ok(commits)
    }

    /// Returns the best common ancestors shared by two commit heads.
    ///
    /// This is the commit-DAG primitive. It can return more than one commit in
    /// criss-cross histories. Merge code should layer an explicit merge-base
    /// policy on top when it needs exactly one base for a three-way merge.
    pub(crate) async fn best_common_ancestors(
        &mut self,
        left_commit_id: &CommitId,
        right_commit_id: &CommitId,
    ) -> Result<Vec<CommitGraphNode>, LixError> {
        best_common_ancestors(self, left_commit_id, right_commit_id).await
    }

    /// Resolves the single commit base to use for a three-way merge.
    ///
    /// This is merge policy layered over `best_common_ancestors(...)`. Histories
    /// with no shared base or multiple equally good bases are rejected for now
    /// so merge code cannot accidentally hide unsupported graph semantics.
    pub(crate) async fn merge_base(
        &mut self,
        left_commit_id: &CommitId,
        right_commit_id: &CommitId,
    ) -> Result<CommitId, LixError> {
        let head_ids = [*left_commit_id, *right_commit_id];
        let heads = self.load_nodes(&head_ids).await?;
        let mut heads = heads.into_iter().map(|(_, node)| node);
        let left = heads
            .next()
            .flatten()
            .ok_or_else(|| missing_commit_graph_error(left_commit_id))?;
        let right = heads
            .next()
            .flatten()
            .ok_or_else(|| missing_commit_graph_error(right_commit_id))?;

        if left_commit_id == right_commit_id {
            return Ok(*left_commit_id);
        }
        if left.parent_commit_ids.as_slice() == [*right_commit_id] {
            validate_parent_generation(&left, &right)?;
            return Ok(*right_commit_id);
        }
        if right.parent_commit_ids.as_slice() == [*left_commit_id] {
            validate_parent_generation(&right, &left)?;
            return Ok(*left_commit_id);
        }
        if let ([left_parent], [right_parent]) = (
            left.parent_commit_ids.as_slice(),
            right.parent_commit_ids.as_slice(),
        ) && left_parent == right_parent
        {
            let parent_ids = [*left_parent];
            let parent = self
                .load_nodes(&parent_ids)
                .await?
                .into_iter()
                .next()
                .and_then(|(_, value)| value)
                .ok_or_else(|| missing_commit_graph_error(left_parent))?;
            validate_parent_generation(&left, &parent)?;
            validate_parent_generation(&right, &parent)?;
            return Ok(*left_parent);
        }

        match self.linear_merge_base(left, right).await? {
            LinearMergeBase::Resolved(base) => return Ok(base),
            LinearMergeBase::Disconnected => {
                return Err(no_common_history_error(left_commit_id, right_commit_id));
            }
            LinearMergeBase::GeneralGraph => {}
        }

        let ancestors = self
            .best_common_ancestors(left_commit_id, right_commit_id)
            .await?;
        match ancestors.as_slice() {
            [] => Err(no_common_history_error(left_commit_id, right_commit_id)),
            [base] => Ok(base.commit_id),
            _ => Err(LixError::ambiguous_merge_base(
                left_commit_id,
                right_commit_id,
                ancestors
                    .iter()
                    .map(|ancestor| ancestor.commit_id.to_string())
                    .collect(),
            )),
        }
    }

    /// Uses authoritative generation and parent facts to zip two linear
    /// frontiers without allocating the general DAG walk's ordered sets. Two
    /// same-generation parents are loaded together so remote and LSM-backed
    /// adapters receive one point-read batch per frontier step. Encountering a
    /// merge commit returns to the general algorithm with every observed node
    /// retained in this reader's immutable node cache.
    async fn linear_merge_base(
        &mut self,
        mut left: CommitGraphNode,
        mut right: CommitGraphNode,
    ) -> Result<LinearMergeBase, LixError> {
        loop {
            if left.commit_id == right.commit_id {
                return Ok(LinearMergeBase::Resolved(left.commit_id));
            }
            match left.generation.cmp(&right.generation) {
                Ordering::Greater => {
                    let [parent_id] = left.parent_commit_ids.as_slice() else {
                        return Ok(LinearMergeBase::GeneralGraph);
                    };
                    left = self.load_linear_parent(&left, *parent_id).await?;
                }
                Ordering::Less => {
                    let [parent_id] = right.parent_commit_ids.as_slice() else {
                        return Ok(LinearMergeBase::GeneralGraph);
                    };
                    right = self.load_linear_parent(&right, *parent_id).await?;
                }
                Ordering::Equal => match (
                    left.parent_commit_ids.as_slice(),
                    right.parent_commit_ids.as_slice(),
                ) {
                    ([], []) => return Ok(LinearMergeBase::Disconnected),
                    ([left_parent_id], [right_parent_id]) => {
                        let parent_ids = [*left_parent_id, *right_parent_id];
                        let parents = self.load_nodes(&parent_ids).await?;
                        let mut parents = parents.into_iter().map(|(_, parent)| parent);
                        let left_parent = parents
                            .next()
                            .flatten()
                            .ok_or_else(|| missing_commit_graph_error(left_parent_id))?;
                        let right_parent = parents
                            .next()
                            .flatten()
                            .ok_or_else(|| missing_commit_graph_error(right_parent_id))?;
                        validate_parent_generation(&left, &left_parent)?;
                        validate_parent_generation(&right, &right_parent)?;
                        left = left_parent;
                        right = right_parent;
                    }
                    _ => return Ok(LinearMergeBase::GeneralGraph),
                },
            }
        }
    }

    async fn load_linear_parent(
        &mut self,
        child: &CommitGraphNode,
        parent_id: CommitId,
    ) -> Result<CommitGraphNode, LixError> {
        let parent_ids = [parent_id];
        let parent = self
            .load_nodes(&parent_ids)
            .await?
            .into_iter()
            .next()
            .and_then(|(_, parent)| parent)
            .ok_or_else(|| missing_commit_graph_error(&parent_id))?;
        validate_parent_generation(child, &parent)?;
        Ok(parent)
    }

    /// Returns canonical changes reachable from `start_commit_id`.
    ///
    /// This is the primitive history API. It reports the commit/depth where a
    /// reachable commit's change-ref set first exposes each matching canonical
    /// change during graph traversal and leaves row shaping to callers such as
    /// SQL providers.
    pub(crate) async fn change_history_from_commit(
        &mut self,
        start_commit_id: &CommitId,
        request: &CommitGraphChangeHistoryRequest,
    ) -> Result<CommitGraphHistory, LixError> {
        let nodes = self.reachable_nodes(start_commit_id).await?;
        let member_schema_keys = request
            .schema_keys
            .iter()
            .filter(|schema_key| schema_key.as_str() != COMMIT_SCHEMA_KEY)
            .cloned()
            .collect::<Vec<_>>();
        let mut member_schema_keys = member_schema_keys;
        member_schema_keys.sort();
        member_schema_keys.dedup();
        let may_include_members = request.schema_keys.is_empty() || !member_schema_keys.is_empty();
        let may_include_commits = request.schema_keys.is_empty()
            || request
                .schema_keys
                .iter()
                .any(|schema_key| schema_key == COMMIT_SCHEMA_KEY);
        let mut entries = Vec::new();
        let mut seen_changes = BTreeSet::new();

        for reachable in nodes.iter() {
            if !depth_matches(reachable.depth, request) {
                continue;
            }

            let node = &reachable.commit;
            if may_include_commits {
                let records = self
                    .load_commit_records(std::slice::from_ref(&node.commit_id))
                    .await?;
                let record = records
                    .into_iter()
                    .next()
                    .flatten()
                    .ok_or_else(|| missing_commit_graph_error(&node.commit_id))?;
                let canonical_change = canonical_commit_change(&record);
                if seen_changes.insert(history_change_identity(&canonical_change))
                    && change_matches_history_request(&canonical_change, request)
                {
                    entries.push(CommitGraphChangeHistoryEntry {
                        change: canonical_change,
                        observed_commit_id: node.commit_id,
                        start_commit_id: *start_commit_id,
                        depth: reachable.depth,
                    });
                }
            }

            if !may_include_members {
                continue;
            }
            for change in self
                .load_member_changes(node.commit_id, &member_schema_keys)
                .await?
            {
                if !seen_changes.insert(history_change_identity(&change)) {
                    continue;
                }
                if change_matches_history_request(&change, request) {
                    entries.push(CommitGraphChangeHistoryEntry {
                        change,
                        observed_commit_id: node.commit_id,
                        start_commit_id: *start_commit_id,
                        depth: reachable.depth,
                    });
                }
            }
        }

        Ok(CommitGraphHistory {
            entries,
            reachable_nodes: nodes,
        })
    }

    /// Loads semantic commit records through the same retained authenticated
    /// view as topology and member reads.
    pub(crate) async fn load_commit_records(
        &mut self,
        commit_ids: &[CommitId],
    ) -> Result<Vec<Option<CommitRecord>>, LixError> {
        crate::forktree::load_commit_records(self.topology.read(), commit_ids).await
    }

    async fn load_member_changes(
        &mut self,
        commit_id: CommitId,
        schema_keys: &[String],
    ) -> Result<Vec<CommitGraphChange>, LixError> {
        if let Some(changes) = self
            .member_changes_cache
            .get(schema_keys)
            .and_then(|by_commit| by_commit.get(&commit_id))
        {
            return Ok(changes.clone());
        }
        let members = crate::forktree::load_commit_member_records(self.topology.read(), commit_id)
            .await?
            .ok_or_else(|| missing_commit_graph_error(&commit_id))?;
        let mut changes = members
            .into_iter()
            .filter(|change| schema_keys.is_empty() || schema_keys.contains(&change.schema_key))
            .map(commit_graph_change_from_change_record)
            .collect::<Vec<_>>();
        changes.sort_by_key(|change| change.id);
        self.member_changes_cache
            .entry(schema_keys.to_vec())
            .or_default()
            .insert(commit_id, changes.clone());
        Ok(changes)
    }
}

/// Storage-free graph walk over authenticated ForkTree Commit objects. The
/// graph algorithm remains local to the semantic reader; there is no legacy
/// walker owner or persisted chronology accelerator.
async fn walk_reachable_nodes<S>(
    reader: &mut CommitGraphStoreReader<S>,
    head_commit_id: &CommitId,
) -> Result<Vec<ReachableCommitGraphNode>, LixError>
where
    S: StorageAdapterRead,
{
    let mut visiting = BTreeSet::new();
    let mut nearest_depths = BTreeMap::new();
    let mut stack = vec![TraversalFrame {
        commit_id: *head_commit_id,
        depth: 0,
        expanded: false,
    }];
    while let Some(frame) = stack.pop() {
        if frame.expanded {
            visiting.remove(&frame.commit_id);
            continue;
        }
        if visiting.contains(&frame.commit_id) {
            return Err(LixError::unknown(format!(
                "commit_graph cycle detected at commit '{}'",
                frame.commit_id
            )));
        }
        if nearest_depths
            .get(&frame.commit_id)
            .is_some_and(|previous| *previous <= frame.depth)
        {
            continue;
        }
        let commit = reader
            .load_node(&frame.commit_id)
            .await?
            .ok_or_else(|| missing_commit_graph_error(&frame.commit_id))?;
        nearest_depths.insert(frame.commit_id, frame.depth);
        visiting.insert(frame.commit_id);
        stack.push(TraversalFrame {
            commit_id: frame.commit_id,
            depth: frame.depth,
            expanded: true,
        });
        for parent_commit_id in commit.parent_commit_ids.iter().rev() {
            stack.push(TraversalFrame {
                commit_id: *parent_commit_id,
                depth: frame.depth + 1,
                expanded: false,
            });
        }
    }
    let mut commits = Vec::with_capacity(nearest_depths.len());
    for (commit_id, depth) in nearest_depths {
        let commit = reader
            .load_node(&commit_id)
            .await?
            .ok_or_else(|| missing_commit_graph_error(&commit_id))?;
        commits.push(ReachableCommitGraphNode { commit, depth });
    }
    commits.sort_by(|left, right| {
        left.depth
            .cmp(&right.depth)
            .then_with(|| left.commit.commit_id.cmp(&right.commit.commit_id))
    });
    Ok(commits)
}

async fn best_common_ancestors<S>(
    reader: &mut CommitGraphStoreReader<S>,
    left_commit_id: &CommitId,
    right_commit_id: &CommitId,
) -> Result<Vec<CommitGraphNode>, LixError>
where
    S: StorageAdapterRead,
{
    const LEFT: u8 = 1;
    const RIGHT: u8 = 2;
    const BOTH: u8 = LEFT | RIGHT;
    const STALE: u8 = 4;

    let left = reader
        .load_node(left_commit_id)
        .await?
        .ok_or_else(|| missing_commit_graph_error(left_commit_id))?;
    let right = reader
        .load_node(right_commit_id)
        .await?
        .ok_or_else(|| missing_commit_graph_error(right_commit_id))?;
    let mut colors = BTreeMap::from([(*left_commit_id, LEFT), (*right_commit_id, RIGHT)]);
    if left_commit_id == right_commit_id {
        colors.insert(*left_commit_id, BOTH);
    }
    let mut queue = BTreeSet::from([
        (left.generation, *left_commit_id),
        (right.generation, *right_commit_id),
    ]);
    let mut non_stale_queued = BTreeSet::from([*left_commit_id, *right_commit_id]);
    let mut best = Vec::new();
    while !queue.is_empty() {
        if !best.is_empty() && non_stale_queued.is_empty() {
            break;
        }
        let (generation, commit_id) = queue.pop_last().expect("queue is not empty");
        non_stale_queued.remove(&commit_id);
        let commit = reader
            .load_node(&commit_id)
            .await?
            .ok_or_else(|| missing_commit_graph_error(&commit_id))?;
        if commit.generation != generation {
            return Err(LixError::unknown(format!(
                "commit '{commit_id}' generation changed during graph walk"
            )));
        }
        let mut color = colors[&commit_id];
        if color & STALE == 0 && color & BOTH == BOTH {
            best.push(commit_id);
            color |= STALE;
            colors.insert(commit_id, color);
        }
        for parent_commit_id in commit.parent_commit_ids.iter().copied() {
            let parent = reader
                .load_node(&parent_commit_id)
                .await?
                .ok_or_else(|| missing_commit_graph_error(&parent_commit_id))?;
            validate_parent_generation(&commit, &parent)?;
            let parent_color = colors.entry(parent_commit_id).or_default();
            *parent_color |= color;
            queue.insert((parent.generation, parent_commit_id));
            if *parent_color & STALE == 0 {
                non_stale_queued.insert(parent_commit_id);
            } else {
                non_stale_queued.remove(&parent_commit_id);
            }
        }
    }
    best.sort_unstable();
    best.dedup();
    let mut nodes = Vec::with_capacity(best.len());
    for commit_id in best {
        nodes.push(
            reader
                .load_node(&commit_id)
                .await?
                .ok_or_else(|| missing_commit_graph_error(&commit_id))?,
        );
    }
    Ok(nodes)
}

struct TraversalFrame {
    commit_id: CommitId,
    depth: u32,
    expanded: bool,
}

fn commit_graph_change_from_change_record(change: ChangeRecord) -> CommitGraphChange {
    CommitGraphChange {
        id: change.change_id,
        account_id: change.account_id,
        entity_pk: change.entity_pk,
        schema_key: change.schema_key,
        file_id: change.file_id,
        snapshot: change.snapshot,
        metadata: change.metadata,
        created_at: change.created_at,
        origin_key: change.origin_key,
    }
}

fn commit_graph_node_from_topology(topology: crate::forktree::CommitTopology) -> CommitGraphNode {
    CommitGraphNode {
        commit_id: topology.commit_id,
        generation: topology.generation,
        parent_commit_ids: topology.parent_commit_ids,
    }
}

fn missing_commit_graph_error(commit_id: &CommitId) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("commit_graph missing commit '{commit_id}'"),
    )
}

fn validate_parent_generation(
    child: &CommitGraphNode,
    parent: &CommitGraphNode,
) -> Result<(), LixError> {
    if parent.generation >= child.generation {
        return Err(LixError::unknown(format!(
            "commit '{}' parent '{}' does not have a lower generation",
            child.commit_id, parent.commit_id
        )));
    }
    Ok(())
}

fn no_common_history_error(left_commit_id: &CommitId, right_commit_id: &CommitId) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!(
            "commit_graph found no common history between '{left_commit_id}' and '{right_commit_id}'"
        ),
    )
}

#[async_trait::async_trait]
impl<S> CommitGraphReader for CommitGraphStoreReader<S>
where
    S: StorageAdapterRead,
{
    async fn load_node(
        &mut self,
        commit_id: &CommitId,
    ) -> Result<Option<CommitGraphNode>, LixError> {
        Self::load_node(self, commit_id).await
    }

    async fn reachable_nodes(
        &mut self,
        head_commit_id: &CommitId,
    ) -> Result<Arc<[ReachableCommitGraphNode]>, LixError> {
        Self::reachable_nodes(self, head_commit_id).await
    }

    async fn retained_nodes(&mut self) -> Result<Vec<CommitGraphNode>, LixError> {
        Self::retained_nodes(self).await
    }

    async fn load_commit_records(
        &mut self,
        commit_ids: &[CommitId],
    ) -> Result<Vec<Option<CommitRecord>>, LixError> {
        Self::load_commit_records(self, commit_ids).await
    }

    async fn change_history_from_commit(
        &mut self,
        start_commit_id: &CommitId,
        request: &CommitGraphChangeHistoryRequest,
    ) -> Result<CommitGraphHistory, LixError> {
        Self::change_history_from_commit(self, start_commit_id, request).await
    }
}

fn depth_matches(depth: u32, request: &CommitGraphChangeHistoryRequest) -> bool {
    request.min_depth.is_none_or(|min| depth >= min)
        && request.max_depth.is_none_or(|max| depth <= max)
}

fn change_matches_history_request(
    change: &CommitGraphChange,
    request: &CommitGraphChangeHistoryRequest,
) -> bool {
    (request.include_tombstones || change.snapshot.is_some())
        && (request.entity_pks.is_empty() || request.entity_pks.contains(&change.entity_pk))
        && (request.schema_keys.is_empty() || request.schema_keys.contains(&change.schema_key))
        && (request.file_ids.is_empty()
            || change
                .file_id
                .as_ref()
                .is_some_and(|file_id| request.file_ids.contains(file_id)))
}

fn history_change_identity(
    change: &CommitGraphChange,
) -> (ChangeId, String, Option<String>, EntityPk) {
    (
        change.id,
        change.schema_key.clone(),
        change.file_id.clone(),
        change.entity_pk.clone(),
    )
}

pub(crate) fn canonical_commit_change(record: &CommitRecord) -> CommitGraphChange {
    let snapshot_content =
        crate::changelog::commit_row_snapshot_json(&record.commit_id.to_string())
            .expect("lix_commit snapshot serialization should not fail");
    CommitGraphChange {
        id: record.change_id,
        account_id: record.account_id.clone(),
        entity_pk: EntityPk::uuid_from_canonical(&record.commit_id.to_string())
            .expect("commit IDs are canonical UUIDs"),
        schema_key: COMMIT_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot: crate::json_store::JsonSlot::from_json(&snapshot_content),
        metadata: crate::json_store::JsonSlot::None,
        created_at: record.created_at,
        origin_key: None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::branch::{BranchHead, BranchRefMetadata};
    use crate::common::LixTimestamp;
    use crate::live_state::{LiveStateExactRowRequest, LiveStateFilter, LiveStateRowFilter};

    struct CountingBatchBranchRefReader {
        rows: Vec<(BranchHead, BranchRefMetadata)>,
        scan_calls: AtomicUsize,
        load_calls: AtomicUsize,
        batch_calls: Mutex<Vec<Vec<String>>>,
    }

    impl CountingBatchBranchRefReader {
        fn new(branch_count: usize) -> Self {
            Self {
                rows: (0..branch_count)
                    .map(|index| {
                        (
                            BranchHead {
                                branch_id: format!("01930000-0000-7000-8000-{index:012x}"),
                                commit_id: CommitId::for_test_label(&format!("commit-{index}")),
                            },
                            BranchRefMetadata {
                                change_id: ChangeId::for_test_label(&format!("change-{index}")),
                                updated_at: LixTimestamp::from_unix_millis_utc_lossy(0),
                            },
                        )
                    })
                    .collect(),
                scan_calls: AtomicUsize::new(0),
                load_calls: AtomicUsize::new(0),
                batch_calls: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl BranchRefReader for CountingBatchBranchRefReader {
        async fn load_head(&self, _branch_id: &str) -> Result<Option<BranchHead>, LixError> {
            self.load_calls.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }

        async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
            Ok(self.rows.iter().map(|(head, _)| head.clone()).collect())
        }

        async fn scan_head_metadata(
            &self,
        ) -> Result<Vec<(BranchHead, BranchRefMetadata)>, LixError> {
            self.scan_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.rows.clone())
        }

        async fn load_head_metadata_batch(
            &self,
            branch_ids: &[String],
        ) -> Result<Vec<(BranchHead, BranchRefMetadata)>, LixError> {
            self.batch_calls
                .lock()
                .expect("batch call log is not poisoned")
                .push(branch_ids.to_vec());
            Ok(branch_ids
                .iter()
                .filter_map(|branch_id| {
                    self.rows
                        .iter()
                        .find(|(head, _)| &head.branch_id == branch_id)
                        .cloned()
                })
                .collect())
        }
    }

    struct EmptyCommitGraphReader;

    #[async_trait::async_trait]
    impl CommitGraphReader for EmptyCommitGraphReader {
        async fn load_node(
            &mut self,
            _commit_id: &CommitId,
        ) -> Result<Option<CommitGraphNode>, LixError> {
            Ok(None)
        }

        async fn reachable_nodes(
            &mut self,
            _head_commit_id: &CommitId,
        ) -> Result<Arc<[ReachableCommitGraphNode]>, LixError> {
            Ok(Vec::new().into())
        }

        async fn load_commit_records(
            &mut self,
            commit_ids: &[CommitId],
        ) -> Result<Vec<Option<CommitRecord>>, LixError> {
            Ok(vec![None; commit_ids.len()])
        }

        async fn change_history_from_commit(
            &mut self,
            _start_commit_id: &CommitId,
            _request: &CommitGraphChangeHistoryRequest,
        ) -> Result<CommitGraphHistory, LixError> {
            Ok(CommitGraphHistory {
                entries: Vec::new(),
                reachable_nodes: Vec::new().into(),
            })
        }
    }

    #[tokio::test]
    async fn derived_empty_branch_filter_batches_heads_without_point_reload() {
        for branch_count in [1, 32, 128] {
            let branch_ref = Arc::new(CountingBatchBranchRefReader::new(branch_count));
            let graph = Arc::new(tokio::sync::Mutex::new(
                Box::new(EmptyCommitGraphReader) as Box<dyn CommitGraphReader>
            ));
            let reader = CommitGraphLiveStateReader::new(
                "lix_commit_by_branch",
                graph,
                branch_ref.clone(),
                None,
                false,
                false,
            );
            let request = LiveStateScanRequest {
                filter: LiveStateFilter {
                    rows: LiveStateRowFilter::All,
                    schema_keys: vec!["lix_commit_by_branch".to_owned()],
                    branch_ids: Vec::new(),
                    ..Default::default()
                },
                ..Default::default()
            };

            let rows = reader
                .rows_for_request(&request)
                .await
                .expect("derived commit surface should scan authenticated heads");
            assert!(rows.is_empty());
            assert_eq!(branch_ref.scan_calls.load(Ordering::SeqCst), 1);
            assert_eq!(branch_ref.load_calls.load(Ordering::SeqCst), 0);
            assert!(
                branch_ref
                    .batch_calls
                    .lock()
                    .expect("batch call log is not poisoned")
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn derived_explicit_branch_filter_uses_one_requested_metadata_batch() {
        let branch_ref = Arc::new(CountingBatchBranchRefReader::new(128));
        let graph = Arc::new(tokio::sync::Mutex::new(
            Box::new(EmptyCommitGraphReader) as Box<dyn CommitGraphReader>
        ));
        let reader = CommitGraphLiveStateReader::new(
            "lix_commit_by_branch",
            graph,
            branch_ref.clone(),
            None,
            false,
            false,
        );
        let requested = vec![
            branch_ref.rows[5].0.branch_id.clone(),
            branch_ref.rows[2].0.branch_id.clone(),
            branch_ref.rows[5].0.branch_id.clone(),
        ];
        let request = LiveStateScanRequest {
            filter: LiveStateFilter {
                rows: LiveStateRowFilter::All,
                schema_keys: vec!["lix_commit_by_branch".to_owned()],
                branch_ids: requested,
                ..Default::default()
            },
            ..Default::default()
        };

        let rows = reader
            .rows_for_request(&request)
            .await
            .expect("explicit branch filter should use authenticated requested batch");
        assert!(rows.is_empty());
        assert_eq!(branch_ref.scan_calls.load(Ordering::SeqCst), 0);
        assert_eq!(branch_ref.load_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            *branch_ref
                .batch_calls
                .lock()
                .expect("batch call log is not poisoned"),
            vec![vec![
                branch_ref.rows[5].0.branch_id.clone(),
                branch_ref.rows[2].0.branch_id.clone(),
            ]]
        );
    }

    #[tokio::test]
    async fn derived_explicit_missing_branch_fails_closed_after_one_requested_batch() {
        let branch_ref = Arc::new(CountingBatchBranchRefReader::new(128));
        let graph = Arc::new(tokio::sync::Mutex::new(
            Box::new(EmptyCommitGraphReader) as Box<dyn CommitGraphReader>
        ));
        let reader = CommitGraphLiveStateReader::new(
            "lix_commit_by_branch",
            graph,
            branch_ref.clone(),
            None,
            false,
            false,
        );
        let requested = vec![
            branch_ref.rows[5].0.branch_id.clone(),
            "01930000-0000-7000-8000-ffffffffffff".to_owned(),
        ];
        let request = LiveStateScanRequest {
            filter: LiveStateFilter {
                rows: LiveStateRowFilter::All,
                schema_keys: vec!["lix_commit_by_branch".to_owned()],
                branch_ids: requested,
                ..Default::default()
            },
            ..Default::default()
        };

        let error = reader
            .rows_for_request(&request)
            .await
            .expect_err("missing requested branch must fail closed");
        assert_eq!(error.code, LixError::CODE_BRANCH_NOT_FOUND);
        assert_eq!(branch_ref.scan_calls.load(Ordering::SeqCst), 0);
        assert_eq!(branch_ref.load_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            *branch_ref
                .batch_calls
                .lock()
                .expect("batch call log is not poisoned"),
            vec![vec![
                branch_ref.rows[5].0.branch_id.clone(),
                "01930000-0000-7000-8000-ffffffffffff".to_owned(),
            ]]
        );
    }

    #[tokio::test]
    async fn global_commit_surface_reuses_scanned_heads_without_global_point_reload() {
        let branch_ref = Arc::new(CountingBatchBranchRefReader::new(128));
        let graph = Arc::new(tokio::sync::Mutex::new(
            Box::new(EmptyCommitGraphReader) as Box<dyn CommitGraphReader>
        ));
        let reader = CommitGraphLiveStateReader::new(
            "lix_commit",
            graph,
            branch_ref.clone(),
            None,
            true,
            true,
        );
        let request = LiveStateScanRequest {
            filter: LiveStateFilter {
                rows: LiveStateRowFilter::All,
                schema_keys: vec!["lix_commit".to_owned()],
                branch_ids: Vec::new(),
                ..Default::default()
            },
            ..Default::default()
        };

        let rows = reader
            .rows_for_request(&request)
            .await
            .expect("global derived surface should use the scanned authenticated roots");
        assert!(rows.is_empty());
        assert_eq!(branch_ref.scan_calls.load(Ordering::SeqCst), 1);
        assert_eq!(branch_ref.load_calls.load(Ordering::SeqCst), 0);
        assert!(
            branch_ref
                .batch_calls
                .lock()
                .expect("batch call log is not poisoned")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn branch_ref_entity_scope_uses_requested_metadata_batch() {
        let branch_ref = Arc::new(CountingBatchBranchRefReader::new(128));
        let graph = Arc::new(tokio::sync::Mutex::new(
            Box::new(EmptyCommitGraphReader) as Box<dyn CommitGraphReader>
        ));
        let reader = CommitGraphLiveStateReader::new(
            BRANCH_REF_SCHEMA_KEY,
            graph,
            branch_ref.clone(),
            None,
            false,
            false,
        );
        let branch_id = branch_ref.rows[5].0.branch_id.clone();
        let request = LiveStateScanRequest {
            filter: LiveStateFilter {
                rows: LiveStateRowFilter::All,
                schema_keys: vec![BRANCH_REF_SCHEMA_KEY.to_owned()],
                entity_pks: vec![EntityPk::uuid_from_canonical(&branch_id).expect("test UUID")],
                branch_ids: vec![crate::GLOBAL_BRANCH_ID.to_owned()],
                untracked: Some(true),
                ..Default::default()
            },
            ..Default::default()
        };

        let rows = reader
            .rows_for_request(&request)
            .await
            .expect("branch-ref entity scope should use requested batch");
        assert_eq!(rows.len(), 1);
        assert_eq!(branch_ref.scan_calls.load(Ordering::SeqCst), 0);
        assert_eq!(branch_ref.load_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            *branch_ref
                .batch_calls
                .lock()
                .expect("batch call log is not poisoned"),
            vec![vec![branch_id]]
        );
    }

    #[tokio::test]
    async fn branch_ref_exact_batch_uses_requested_metadata_batch() {
        let branch_ref = Arc::new(CountingBatchBranchRefReader::new(128));
        let graph = Arc::new(tokio::sync::Mutex::new(
            Box::new(EmptyCommitGraphReader) as Box<dyn CommitGraphReader>
        ));
        let reader = CommitGraphLiveStateReader::new(
            BRANCH_REF_SCHEMA_KEY,
            graph,
            branch_ref.clone(),
            None,
            false,
            false,
        );
        let branch_id = branch_ref.rows[5].0.branch_id.clone();
        let request = LiveStateExactBatchRequest {
            rows: vec![LiveStateExactRowRequest {
                schema_key: BRANCH_REF_SCHEMA_KEY.to_owned(),
                branch_id: crate::GLOBAL_BRANCH_ID.to_owned(),
                entity_pk: EntityPk::uuid_from_canonical(&branch_id).expect("test UUID"),
                file_id: None,
            }],
            untracked: Some(true),
            ..Default::default()
        };

        let result = reader
            .load_exact_batch(&request)
            .await
            .expect("branch-ref exact batch should use requested batch");
        assert_eq!(result.len(), 1);
        assert_eq!(branch_ref.scan_calls.load(Ordering::SeqCst), 0);
        assert_eq!(branch_ref.load_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            *branch_ref
                .batch_calls
                .lock()
                .expect("batch call log is not poisoned"),
            vec![vec![branch_id]]
        );
    }

    #[tokio::test]
    async fn branch_ref_exact_batch_rejects_non_branch_ref_schema() {
        let branch_ref = Arc::new(CountingBatchBranchRefReader::new(1));
        let graph = Arc::new(tokio::sync::Mutex::new(
            Box::new(EmptyCommitGraphReader) as Box<dyn CommitGraphReader>
        ));
        let reader = CommitGraphLiveStateReader::new(
            BRANCH_REF_SCHEMA_KEY,
            graph,
            branch_ref.clone(),
            None,
            false,
            false,
        );
        let branch_id = branch_ref.rows[0].0.branch_id.clone();
        let request = LiveStateExactBatchRequest {
            rows: vec![LiveStateExactRowRequest {
                schema_key: "not_lix_branch_ref".to_owned(),
                branch_id: crate::GLOBAL_BRANCH_ID.to_owned(),
                entity_pk: EntityPk::uuid_from_canonical(&branch_id).expect("test UUID"),
                file_id: None,
            }],
            untracked: Some(true),
            ..Default::default()
        };

        let error = reader
            .load_exact_batch(&request)
            .await
            .expect_err("branch-ref exact reads must reject another schema");
        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(
            branch_ref
                .batch_calls
                .lock()
                .expect("batch call log is not poisoned")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn branch_ref_exact_batch_rejects_non_null_file_id() {
        let branch_ref = Arc::new(CountingBatchBranchRefReader::new(1));
        let graph = Arc::new(tokio::sync::Mutex::new(
            Box::new(EmptyCommitGraphReader) as Box<dyn CommitGraphReader>
        ));
        let reader = CommitGraphLiveStateReader::new(
            BRANCH_REF_SCHEMA_KEY,
            graph,
            branch_ref.clone(),
            None,
            false,
            false,
        );
        let branch_id = branch_ref.rows[0].0.branch_id.clone();
        let request = LiveStateExactBatchRequest {
            rows: vec![LiveStateExactRowRequest {
                schema_key: BRANCH_REF_SCHEMA_KEY.to_owned(),
                branch_id: crate::GLOBAL_BRANCH_ID.to_owned(),
                entity_pk: EntityPk::uuid_from_canonical(&branch_id).expect("test UUID"),
                file_id: Some("unexpected-file".to_owned()),
            }],
            untracked: Some(true),
            ..Default::default()
        };

        let error = reader
            .load_exact_batch(&request)
            .await
            .expect_err("branch-ref exact reads must reject a file identity");
        assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert!(
            branch_ref
                .batch_calls
                .lock()
                .expect("batch call log is not poisoned")
                .is_empty()
        );
    }
}
