//! Typed serving contract for live relations derived from authoritative stores.
//!
//! Access-path selection is centralized here. Providers declare capabilities;
//! they never inspect a query and opportunistically switch from a scan to a
//! point lookup themselves. Adding a derived relation therefore requires an
//! explicit schema, retention, file-identity, and access-path declaration.

use async_trait::async_trait;
use tracing::Instrument;

use crate::branch::{BRANCH_REF_SCHEMA_KEY, BranchHeadControl, BranchHeadControlContext};
use crate::changelog::{ChangeId, CommitId};
use crate::commit_graph::{
    CommitGraphCommit, CommitGraphCommitRecord, CommitGraphContext, CommitGraphEdge, commit_edges,
};
use crate::entity_pk::{EntityPk, EntityPkComponent};
use crate::live_state::{LiveStateRowFilter, LiveStateScanRequest, MaterializedLiveStateRow};
use crate::storage_adapter::StorageAdapterRead;
use crate::{GLOBAL_BRANCH_ID, LixError, NullableKeyFilter};

const COMMIT_SCHEMA_KEY: &str = "lix_commit";
const COMMIT_EDGE_SCHEMA_KEY: &str = "lix_commit_edge";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DerivedFileIdentity {
    Null,
}

#[derive(Debug, Clone, Copy)]
struct DerivedProviderDescriptor {
    schema_key: &'static str,
    untracked: bool,
    file_identity: DerivedFileIdentity,
}

const COMMIT_DESCRIPTOR: DerivedProviderDescriptor = DerivedProviderDescriptor {
    schema_key: COMMIT_SCHEMA_KEY,
    untracked: false,
    file_identity: DerivedFileIdentity::Null,
};
const COMMIT_EDGE_DESCRIPTOR: DerivedProviderDescriptor = DerivedProviderDescriptor {
    schema_key: COMMIT_EDGE_SCHEMA_KEY,
    untracked: false,
    file_identity: DerivedFileIdentity::Null,
};
const BRANCH_REF_DESCRIPTOR: DerivedProviderDescriptor = DerivedProviderDescriptor {
    schema_key: BRANCH_REF_SCHEMA_KEY,
    untracked: true,
    file_identity: DerivedFileIdentity::Null,
};
#[derive(Debug, Clone, Copy)]
enum RegisteredDerivedProvider {
    Commit,
    CommitEdge,
    BranchRef,
}

impl RegisteredDerivedProvider {
    const fn descriptor(self) -> DerivedProviderDescriptor {
        match self {
            Self::Commit => COMMIT_DESCRIPTOR,
            Self::CommitEdge => COMMIT_EDGE_DESCRIPTOR,
            Self::BranchRef => BRANCH_REF_DESCRIPTOR,
        }
    }
}

const DERIVED_PROVIDERS: &[RegisteredDerivedProvider] = &[
    RegisteredDerivedProvider::Commit,
    RegisteredDerivedProvider::CommitEdge,
    RegisteredDerivedProvider::BranchRef,
];

struct DerivedReadContext<'a, S>
where
    S: StorageAdapterRead + ?Sized,
{
    store: &'a S,
    commit_graph: &'a CommitGraphContext,
    all_commits: Option<Vec<CommitGraphCommit>>,
}

impl<'a, S> DerivedReadContext<'a, S>
where
    S: StorageAdapterRead + ?Sized,
{
    fn new(store: &'a S, commit_graph: &'a CommitGraphContext) -> Self {
        Self {
            store,
            commit_graph,
            all_commits: None,
        }
    }

    async fn all_commits(&mut self) -> Result<&[CommitGraphCommit], LixError> {
        if self.all_commits.is_none() {
            self.all_commits = Some(self.commit_graph.reader(self.store).all_commits().await?);
        }
        Ok(self
            .all_commits
            .as_deref()
            .expect("derived commit scan cache was initialized"))
    }
}

struct DerivedScanScope<'a> {
    branch_ids: &'a [String],
    storage_branch_ids: &'a [String],
    retention: Option<bool>,
}

#[async_trait]
trait DerivedLiveStateProvider<S>: Sync
where
    S: StorageAdapterRead + ?Sized,
{
    fn descriptor(&self) -> DerivedProviderDescriptor;

    async fn scan_all(
        &self,
        reads: &mut DerivedReadContext<'_, S>,
        scope: &DerivedScanScope<'_>,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError>;
}

#[async_trait]
trait DerivedEntityPointProvider<S>: DerivedLiveStateProvider<S>
where
    S: StorageAdapterRead + ?Sized,
{
    async fn load_entity_points(
        &self,
        reads: &mut DerivedReadContext<'_, S>,
        entity_pks: &[EntityPk],
        scope: &DerivedScanScope<'_>,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError>;
}

struct CommitProvider;

#[async_trait]
impl<S> DerivedLiveStateProvider<S> for CommitProvider
where
    S: StorageAdapterRead + ?Sized,
{
    fn descriptor(&self) -> DerivedProviderDescriptor {
        COMMIT_DESCRIPTOR
    }

    async fn scan_all(
        &self,
        reads: &mut DerivedReadContext<'_, S>,
        scope: &DerivedScanScope<'_>,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let commits = reads.all_commits().await?;
        let mut rows = Vec::with_capacity(commits.len() * scope.branch_ids.len());
        for branch_id in scope.branch_ids {
            for commit in commits {
                rows.push(commit_row(
                    commit.commit_id,
                    commit.change.id,
                    commit.change.created_at,
                    branch_id,
                )?);
            }
        }
        Ok(rows)
    }
}

#[async_trait]
impl<S> DerivedEntityPointProvider<S> for CommitProvider
where
    S: StorageAdapterRead + ?Sized,
{
    async fn load_entity_points(
        &self,
        reads: &mut DerivedReadContext<'_, S>,
        entity_pks: &[EntityPk],
        scope: &DerivedScanScope<'_>,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let commit_ids = entity_pks
            .iter()
            .filter_map(commit_id_from_entity_pk)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if commit_ids.is_empty() {
            return Ok(Vec::new());
        }
        let records = reads
            .commit_graph
            .reader(reads.store)
            .load_commit_records(&commit_ids)
            .await?;
        let mut rows = Vec::with_capacity(records.len() * scope.branch_ids.len());
        for branch_id in scope.branch_ids {
            for (requested_commit_id, record) in commit_ids.iter().zip(&records) {
                let Some(record) = record else {
                    continue;
                };
                validate_commit_point_identity(*requested_commit_id, record)?;
                rows.push(commit_row(
                    record.commit_id,
                    record.change_id,
                    record.created_at,
                    branch_id,
                )?);
            }
        }
        Ok(rows)
    }
}

struct CommitEdgeProvider;

#[async_trait]
impl<S> DerivedLiveStateProvider<S> for CommitEdgeProvider
where
    S: StorageAdapterRead + ?Sized,
{
    fn descriptor(&self) -> DerivedProviderDescriptor {
        COMMIT_EDGE_DESCRIPTOR
    }

    async fn scan_all(
        &self,
        reads: &mut DerivedReadContext<'_, S>,
        scope: &DerivedScanScope<'_>,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let commits = reads.all_commits().await?;
        let edges = commit_edges(commits);
        let mut rows = Vec::with_capacity(edges.len() * scope.branch_ids.len());
        for branch_id in scope.branch_ids {
            for edge in &edges {
                rows.push(commit_edge_row(edge, branch_id)?);
            }
        }
        Ok(rows)
    }
}

struct BranchRefProvider;

#[async_trait]
impl<S> DerivedLiveStateProvider<S> for BranchRefProvider
where
    S: StorageAdapterRead + ?Sized,
{
    fn descriptor(&self) -> DerivedProviderDescriptor {
        BRANCH_REF_DESCRIPTOR
    }

    async fn scan_all(
        &self,
        reads: &mut DerivedReadContext<'_, S>,
        scope: &DerivedScanScope<'_>,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        if !scope
            .storage_branch_ids
            .iter()
            .any(|branch_id| branch_id == GLOBAL_BRANCH_ID)
        {
            return Ok(Vec::new());
        }
        BranchHeadControlContext::new()
            .reader(reads.store)
            .scan()
            .await?
            .into_iter()
            .map(|(branch_id, control)| branch_ref_row(&branch_id, control))
            .collect()
    }
}

#[async_trait]
impl<S> DerivedEntityPointProvider<S> for BranchRefProvider
where
    S: StorageAdapterRead + ?Sized,
{
    async fn load_entity_points(
        &self,
        reads: &mut DerivedReadContext<'_, S>,
        entity_pks: &[EntityPk],
        scope: &DerivedScanScope<'_>,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        if !scope
            .storage_branch_ids
            .iter()
            .any(|branch_id| branch_id == GLOBAL_BRANCH_ID)
        {
            return Ok(Vec::new());
        }
        let branch_ids = entity_pks
            .iter()
            .filter_map(uuid_string_from_entity_pk)
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if branch_ids.is_empty() {
            return Ok(Vec::new());
        }
        let controls = BranchHeadControlContext::new()
            .reader(reads.store)
            .load_many(&branch_ids)
            .await?;
        branch_ids
            .into_iter()
            .zip(controls)
            .filter_map(|(branch_id, control)| control.map(|control| (branch_id, control)))
            .map(|(branch_id, control)| branch_ref_row(&branch_id, control))
            .collect()
    }
}

pub(super) async fn scan_derived_rows<S>(
    store: &S,
    commit_graph: &CommitGraphContext,
    request: &LiveStateScanRequest,
    projection_branch_ids: &[String],
    storage_branch_ids: &[String],
    retention: Option<bool>,
) -> Result<Vec<MaterializedLiveStateRow>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    if matches!(request.filter.rows, LiveStateRowFilter::None) {
        return Ok(Vec::new());
    }
    if !request.filter.branch_ids.is_empty() && projection_branch_ids.is_empty() {
        return Ok(Vec::new());
    }
    let branch_ids = if projection_branch_ids.is_empty() {
        vec![GLOBAL_BRANCH_ID.to_string()]
    } else {
        projection_branch_ids.to_vec()
    };
    let scope = DerivedScanScope {
        branch_ids: &branch_ids,
        storage_branch_ids,
        retention,
    };
    let mut reads = DerivedReadContext::new(store, commit_graph);
    let mut rows = Vec::new();
    for provider in DERIVED_PROVIDERS {
        append_registered_provider_rows(*provider, &mut reads, request, &scope, &mut rows).await?;
    }
    Ok(rows)
}

async fn append_registered_provider_rows<S>(
    provider: RegisteredDerivedProvider,
    reads: &mut DerivedReadContext<'_, S>,
    request: &LiveStateScanRequest,
    scope: &DerivedScanScope<'_>,
    rows: &mut Vec<MaterializedLiveStateRow>,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    // Static dispatch keeps StorageAdapterRead's allocation-free `impl Future`
    // contract. The helper's trait bound makes each registered capability
    // require its implementation at compile time.
    match provider {
        RegisteredDerivedProvider::Commit => {
            append_point_provider_rows(&CommitProvider, reads, request, scope, rows).await
        }
        RegisteredDerivedProvider::CommitEdge => {
            append_full_scan_provider_rows(&CommitEdgeProvider, reads, request, scope, rows).await
        }
        RegisteredDerivedProvider::BranchRef => {
            append_point_provider_rows(&BranchRefProvider, reads, request, scope, rows).await
        }
    }
}

async fn append_point_provider_rows<S, P>(
    provider: &P,
    reads: &mut DerivedReadContext<'_, S>,
    request: &LiveStateScanRequest,
    scope: &DerivedScanScope<'_>,
    rows: &mut Vec<MaterializedLiveStateRow>,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + ?Sized,
    P: DerivedEntityPointProvider<S>,
{
    let descriptor = provider.descriptor();
    if !provider_filter_allows(request, scope, descriptor) {
        return Ok(());
    }

    let mut provider_rows = if request.filter.entity_pks.is_empty() {
        provider
            .scan_all(reads, scope)
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.derived.full_scan",
                schema_key = descriptor.schema_key,
            ))
            .await?
    } else {
        provider
            .load_entity_points(reads, &request.filter.entity_pks, scope)
            .instrument(tracing::debug_span!(
                target: "lix_perf",
                "lix.perf.derived.entity_points",
                schema_key = descriptor.schema_key,
            ))
            .await?
    };
    if !request.filter.entity_pks.is_empty() {
        provider_rows.retain(|row| request.filter.entity_pks.contains(&row.entity_pk));
    }
    rows.extend(provider_rows);
    Ok(())
}

async fn append_full_scan_provider_rows<S, P>(
    provider: &P,
    reads: &mut DerivedReadContext<'_, S>,
    request: &LiveStateScanRequest,
    scope: &DerivedScanScope<'_>,
    rows: &mut Vec<MaterializedLiveStateRow>,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + ?Sized,
    P: DerivedLiveStateProvider<S>,
{
    let descriptor = provider.descriptor();
    if !provider_filter_allows(request, scope, descriptor) {
        return Ok(());
    }
    let mut provider_rows = provider
        .scan_all(reads, scope)
        .instrument(tracing::debug_span!(
            target: "lix_perf",
            "lix.perf.derived.full_scan",
            schema_key = descriptor.schema_key,
        ))
        .await?;
    if !request.filter.entity_pks.is_empty() {
        provider_rows.retain(|row| request.filter.entity_pks.contains(&row.entity_pk));
    }
    rows.extend(provider_rows);
    Ok(())
}

fn provider_filter_allows(
    request: &LiveStateScanRequest,
    scope: &DerivedScanScope<'_>,
    descriptor: DerivedProviderDescriptor,
) -> bool {
    schema_filter_allows(&request.filter.schema_keys, descriptor.schema_key)
        && !scope
            .retention
            .is_some_and(|untracked| untracked != descriptor.untracked)
        && file_filter_allows(&request.filter.file_ids, descriptor.file_identity)
}

pub(super) fn request_may_include_derived(request: &LiveStateScanRequest) -> bool {
    request.filter.schema_keys.is_empty()
        || request
            .filter
            .schema_keys
            .iter()
            .any(|schema_key| is_derived_schema(schema_key))
}

pub(super) fn is_derived_only_request(request: &LiveStateScanRequest) -> bool {
    !request.filter.schema_keys.is_empty()
        && request
            .filter
            .schema_keys
            .iter()
            .all(|schema_key| is_derived_schema(schema_key))
}

pub(super) fn is_derived_schema(schema_key: &str) -> bool {
    DERIVED_PROVIDERS
        .iter()
        .any(|provider| provider.descriptor().schema_key == schema_key)
}

fn schema_filter_allows(schema_keys: &[String], schema_key: &str) -> bool {
    schema_keys.is_empty() || schema_keys.iter().any(|candidate| candidate == schema_key)
}

fn file_filter_allows(
    file_ids: &[NullableKeyFilter<String>],
    identity: DerivedFileIdentity,
) -> bool {
    match identity {
        DerivedFileIdentity::Null => {
            file_ids.is_empty()
                || file_ids.iter().any(|file_id| {
                    matches!(file_id, NullableKeyFilter::Any | NullableKeyFilter::Null)
                })
        }
    }
}

fn commit_id_from_entity_pk(entity_pk: &EntityPk) -> Option<CommitId> {
    let [EntityPkComponent::Uuid(bytes)] = entity_pk.components.as_slice() else {
        return None;
    };
    Some(CommitId::new(uuid::Uuid::from_bytes(*bytes)))
}

fn uuid_string_from_entity_pk(entity_pk: &EntityPk) -> Option<String> {
    let [EntityPkComponent::Uuid(bytes)] = entity_pk.components.as_slice() else {
        return None;
    };
    Some(uuid::Uuid::from_bytes(*bytes).as_hyphenated().to_string())
}

fn validate_commit_point_identity(
    requested_commit_id: CommitId,
    record: &CommitGraphCommitRecord,
) -> Result<(), LixError> {
    if record.commit_id == requested_commit_id {
        return Ok(());
    }
    Err(LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!(
            "commit point lookup for {requested_commit_id} decoded record {}",
            record.commit_id
        ),
    ))
}

fn commit_row(
    commit_id: CommitId,
    change_id: ChangeId,
    created_at: crate::common::LixTimestamp,
    branch_id: &str,
) -> Result<MaterializedLiveStateRow, LixError> {
    let snapshot_content =
        serde_json::to_string(&serde_json::json!({ "id": commit_id })).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("failed to encode derived lix_commit snapshot: {error}"),
            )
        })?;
    Ok(MaterializedLiveStateRow {
        entity_pk: EntityPk::uuid_from_bytes(*commit_id.as_uuid().as_bytes()),
        schema_key: COMMIT_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot_content: Some(snapshot_content.into()),
        metadata: None,
        deleted: false,
        created_at,
        updated_at: created_at,
        global: true,
        change_id: Some(change_id),
        commit_id: Some(commit_id),
        untracked: false,
        branch_id: branch_id.into(),
    })
}

fn commit_edge_row(
    edge: &CommitGraphEdge,
    branch_id: &str,
) -> Result<MaterializedLiveStateRow, LixError> {
    let snapshot_content = serde_json::to_string(&serde_json::json!({
        "parent_id": edge.parent_commit_id,
        "child_id": edge.child_commit_id,
        "parent_order": edge.parent_order,
    }))
    .map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to encode derived lix_commit_edge snapshot: {error}"),
        )
    })?;
    Ok(MaterializedLiveStateRow {
        entity_pk: EntityPk::from_components(smallvec::smallvec![
            EntityPkComponent::Uuid(*edge.child_commit_id.as_uuid().as_bytes()),
            EntityPkComponent::Integer(i64::from(edge.parent_order)),
        ])
        .expect("commit edge primary key has two components"),
        schema_key: COMMIT_EDGE_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot_content: Some(snapshot_content.into()),
        metadata: None,
        deleted: false,
        created_at: crate::common::LixTimestamp::from_unix_millis_utc_lossy(0),
        updated_at: crate::common::LixTimestamp::from_unix_millis_utc_lossy(0),
        global: true,
        change_id: None,
        commit_id: Some(edge.child_commit_id),
        untracked: false,
        branch_id: branch_id.into(),
    })
}

fn branch_ref_row(
    branch_id: &str,
    control: BranchHeadControl,
) -> Result<MaterializedLiveStateRow, LixError> {
    let snapshot_content = serde_json::to_string(&serde_json::json!({
        "id": branch_id,
        "commit_id": control.head_commit_id.to_string(),
    }))
    .map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("failed to encode direct branch-ref snapshot: {error}"),
        )
    })?;
    Ok(MaterializedLiveStateRow {
        entity_pk: EntityPk::uuid_from_canonical(branch_id).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("direct branch-ref id is not a canonical UUID: {error}"),
            )
        })?,
        schema_key: BRANCH_REF_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot_content: Some(snapshot_content.into()),
        metadata: None,
        deleted: false,
        created_at: control.created_at,
        updated_at: control.updated_at,
        global: true,
        change_id: Some(control.ref_change_id),
        commit_id: None,
        untracked: true,
        branch_id: GLOBAL_BRANCH_ID.into(),
    })
}
