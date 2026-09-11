//! Process-local logical read interests, not row coverage certificates.
//! Snapshots contain native read recipes, never SQL text or query results.
//! Operation guards must begin before the operation's storage snapshot and
//! remain held through registration and execution. Publication preparation
//! takes an unguarded snapshot; only the final local publication takes the
//! exclusive gate. Persistence and session wiring are separate prerequisites.
use super::{
    HotStateExactBatchRequest, HotStateProjection, HotStateReadDomain, HotStateScanRequest,
};
use crate::LixError;
use crate::row_pk::RowPk;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum InterestDomain {
    Combined,
    Tracked,
    Untracked,
}
impl From<HotStateReadDomain> for InterestDomain {
    fn from(domain: HotStateReadDomain) -> Self {
        match domain {
            HotStateReadDomain::Combined => Self::Combined,
            HotStateReadDomain::Tracked => Self::Tracked,
            HotStateReadDomain::Untracked => Self::Untracked,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct ExactReadIdentity {
    pub(crate) schema_key: String,
    pub(crate) branch_id: String,
    pub(crate) file_id: Option<String>,
    #[serde(with = "super::read_interests_codec::key")]
    pub(crate) row_pk: RowPk,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "commitId", rename_all = "snake_case")]
pub(crate) enum DiffInterestEndpoint {
    Fixed(String),
    ActiveHead,
    WorkingCheckpoint,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum FilePathInterest {
    All,
    Comparison {
        operation: FilePathInterestComparison,
        value: String,
    },
    In {
        values: Vec<String>,
    },
    LowercaseContains {
        value: String,
    },
    And {
        left: Box<Self>,
        right: Box<Self>,
    },
    Or {
        left: Box<Self>,
        right: Box<Self>,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FilePathInterestComparison {
    Equal,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum LogicalReadInterest {
    FileContent {
        #[serde(with = "super::read_interests_codec::NativeScan")]
        request: HotStateScanRequest,
        file_ids: Option<Vec<String>>,
        directory_ids: Option<Vec<String>>,
        root_directory: bool,
        indexed: bool,
        path_predicate: FilePathInterest,
        byte_range: Option<(u64, u64)>,
    },
    CollectionGeneration {
        branch_id: String,
        schema_key: String,
        file_id: Option<String>,
    },
    PackedIdentityMembership {
        branch_id: String,
        schema_key: String,
    },
    FilesystemPaths {
        branch_ids: Vec<String>,
        include_blob_refs: bool,
        cache_small_blob_data: bool,
    },
    Diff {
        branch_id: Option<String>,
        relation: String,
        from: DiffInterestEndpoint,
        to: DiffInterestEndpoint,
        #[serde(with = "super::read_interests_codec::NativeTrackedFilter")]
        filter: crate::tracked_state::TrackedStateFilter,
        retain_payloads: bool,
        projected_columns: Vec<String>,
        limit: Option<usize>,
    },
    Scan {
        #[serde(with = "super::read_interests_codec::NativeScan")]
        request: HotStateScanRequest,
        domain: InterestDomain,
    },
    Exact {
        rows: Vec<ExactReadIdentity>,
        projection: HotStateProjection,
        untracked: Option<bool>,
        include_tombstones: bool,
    },
}
impl LogicalReadInterest {
    pub(crate) fn scan(request: &HotStateScanRequest, domain: HotStateReadDomain) -> Self {
        Self::Scan {
            request: request.clone(),
            domain: domain.into(),
        }
    }
    pub(crate) fn exact(request: &HotStateExactBatchRequest) -> Self {
        Self::Exact {
            rows: request
                .rows
                .iter()
                .map(|row| ExactReadIdentity {
                    schema_key: row.schema_key.clone(),
                    branch_id: row.branch_id.clone(),
                    file_id: row.file_id.clone(),
                    row_pk: row.row_pk.clone(),
                })
                .collect(),
            projection: request.projection.clone(),
            untracked: request.untracked,
            include_tombstones: request.include_tombstones,
        }
    }
}
fn failure(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_READ_INTEREST_LIMIT", message)
}
#[derive(Default, Debug)]
struct RegistryState {
    revision: u64,
    restored: bool,
    durable_revision: u64,
    bytes: usize,
    interests: BTreeMap<Vec<u8>, Arc<LogicalReadInterest>>,
}
/// The serialized-byte bound controls retained recipe payload, not arbitrary
/// caller allocations or complete process RSS. No eviction can lose interests.
#[derive(Debug)]
pub(crate) struct ReadInterestRegistry {
    gate: Arc<RwLock<()>>,
    state: Mutex<RegistryState>,
    max_count: usize,
    max_bytes: usize,
    durability_required: bool,
}
#[derive(Clone)]
pub(crate) struct ReadInterestSnapshot {
    pub(crate) revision: u64,
    pub(crate) interests: Vec<Arc<LogicalReadInterest>>,
    pub(crate) serialized_bytes: usize,
}
pub(crate) struct ReadInterestOperation {
    registry: Arc<ReadInterestRegistry>,
    _guard: OwnedRwLockReadGuard<()>,
}
/// Keep alive through the atomic local control/coverage publication.
pub(crate) struct ReadInterestPublication {
    _guard: OwnedRwLockWriteGuard<()>,
}
impl ReadInterestRegistry {
    pub(crate) fn new(max_count: usize, max_bytes: usize) -> Arc<Self> {
        Self::construct(max_count, max_bytes, false)
    }
    pub(crate) fn new_durable(max_count: usize, max_bytes: usize) -> Arc<Self> {
        Self::construct(max_count, max_bytes, true)
    }
    fn construct(max_count: usize, max_bytes: usize, durability_required: bool) -> Arc<Self> {
        Arc::new(Self {
            gate: Arc::new(RwLock::new(())),
            state: Mutex::new(RegistryState::default()),
            max_count,
            max_bytes,
            durability_required,
        })
    }
    pub(crate) fn durability_is_clean(&self) -> Result<bool, LixError> {
        let state = self
            .state
            .lock()
            .map_err(|_| failure("read-interest registry is poisoned"))?;
        Ok(!self.durability_required
            || (state.restored && state.durable_revision == state.revision))
    }
    pub(crate) fn acknowledge_durable(&self, revision: u64) -> Result<(), LixError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| failure("read-interest registry is poisoned"))?;
        if !state.restored || revision > state.revision {
            return Err(failure(
                "durable interest acknowledgment does not match restored registry",
            ));
        }
        state.durable_revision = state.durable_revision.max(revision);
        Ok(())
    }
    /// Merge a fully validated durable inventory atomically. Concurrent newly
    /// registered interests remain dirty until their own union is committed.
    pub(crate) fn merge_persisted(
        &self,
        recipes: Vec<LogicalReadInterest>,
    ) -> Result<(), LixError> {
        let mut persisted = BTreeMap::new();
        let mut persisted_bytes = 0usize;
        for recipe in recipes {
            let mut encoded = BoundedEncoding {
                bytes: Vec::new(),
                limit: self.max_bytes,
            };
            serde_json::to_writer(&mut encoded, &recipe)
                .map_err(|_| failure("persisted interest exceeds byte budget"))?;
            persisted_bytes = persisted_bytes
                .checked_add(encoded.bytes.len())
                .ok_or_else(|| failure("interest byte count overflow"))?;
            if persisted.insert(encoded.bytes, Arc::new(recipe)).is_some() {
                return Err(failure("persisted interest inventory contains duplicates"));
            }
            if persisted.len() > self.max_count || persisted_bytes > self.max_bytes {
                return Err(failure("persisted interest inventory exceeds budget"));
            }
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| failure("read-interest registry is poisoned"))?;
        let mut union = state.interests.clone();
        union.extend(
            persisted
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        );
        let bytes = union
            .keys()
            .try_fold(0usize, |sum, key| sum.checked_add(key.len()))
            .ok_or_else(|| failure("interest union byte count overflow"))?;
        if union.len() > self.max_count || bytes > self.max_bytes {
            return Err(failure("restored and new interests together exceed budget"));
        }
        let revision = state
            .revision
            .checked_add((union.len() - state.interests.len()) as u64)
            .ok_or_else(|| failure("interest revision overflow"))?;
        let completely_persisted = union.keys().all(|key| persisted.contains_key(key));
        state.interests = union;
        state.bytes = bytes;
        state.revision = revision;
        state.restored = true;
        if completely_persisted {
            state.durable_revision = revision;
        }
        Ok(())
    }
    pub(crate) async fn begin_operation(self: &Arc<Self>) -> ReadInterestOperation {
        ReadInterestOperation {
            registry: Arc::clone(self),
            _guard: Arc::clone(&self.gate).read_owned().await,
        }
    }
    pub(crate) fn snapshot(&self) -> Result<ReadInterestSnapshot, LixError> {
        let state = self
            .state
            .lock()
            .map_err(|_| failure("read-interest registry is poisoned"))?;
        Ok(ReadInterestSnapshot {
            revision: state.revision,
            interests: state.interests.values().cloned().collect(),
            serialized_bytes: state.bytes,
        })
    }
    pub(crate) async fn begin_publication(
        self: &Arc<Self>,
        prepared_revision: u64,
    ) -> Result<ReadInterestPublication, LixError> {
        let guard = Arc::clone(&self.gate).write_owned().await;
        let revision = {
            let state = self
                .state
                .lock()
                .map_err(|_| failure("read-interest registry is poisoned"))?;
            if self.durability_required
                && (!state.restored || state.durable_revision != state.revision)
            {
                return Err(LixError::new(
                    "LIX_PARTIAL_INTERESTS_NOT_DURABLE",
                    "remote publication requires restored and durably retained logical interests",
                ));
            }
            state.revision
        };
        if revision != prepared_revision {
            return Err(LixError::new(
                "LIX_PARTIAL_READ_INTEREST_CHANGED",
                "candidate preparation omitted a newly registered logical read interest",
            ));
        }
        Ok(ReadInterestPublication { _guard: guard })
    }
}
struct BoundedEncoding {
    bytes: Vec<u8>,
    limit: usize,
}
impl std::io::Write for BoundedEncoding {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other(
                "logical read recipe exceeds byte budget",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl ReadInterestRegistry {
    /// Call before predicate lowering or physical loading, including reads
    /// returning zero rows. A duplicate leaves the revision unchanged.
    pub(crate) fn register(&self, interest: LogicalReadInterest) -> Result<(), LixError> {
        let mut encoding = BoundedEncoding {
            bytes: Vec::new(),
            limit: self.max_bytes,
        };
        serde_json::to_writer(&mut encoding, &interest)
            .map_err(|_| failure("logical read recipe exceeds byte budget or cannot be encoded"))?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| failure("read-interest registry is poisoned"))?;
        if state.interests.contains_key(&encoding.bytes) {
            return Ok(());
        }
        if state.interests.len() >= self.max_count
            || encoding.bytes.len() > self.max_bytes.saturating_sub(state.bytes)
        {
            return Err(failure(
                "retained logical read interests exceed configured count or byte budget",
            ));
        }
        let revision = state
            .revision
            .checked_add(1)
            .ok_or_else(|| failure("read-interest revision exhausted"))?;
        state.bytes += encoding.bytes.len();
        state.interests.insert(encoding.bytes, Arc::new(interest));
        state.revision = revision;
        Ok(())
    }
}

impl ReadInterestOperation {
    pub(crate) fn register(&self, interest: LogicalReadInterest) -> Result<(), LixError> {
        self.registry.register(interest)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{HotStateExactRowRequest, HotStateFilter};
    use super::*;
    fn negative_recipe() -> LogicalReadInterest {
        LogicalReadInterest::scan(
            &HotStateScanRequest {
                filter: HotStateFilter {
                    branch_ids: vec!["branch".into()],
                    schema_keys: vec!["files".into()],
                    row_pks: vec![RowPk::single("not-created-yet")],
                    ..Default::default()
                },
                limit: Some(1),
                ..Default::default()
            },
            HotStateReadDomain::Tracked,
        )
    }
    #[tokio::test]
    async fn scoped_native_reader_records_negative_request_before_access() {
        let lix = crate::open_lix().await.unwrap();
        let descriptor = lix.partial_replica_descriptor(None).await.unwrap();
        let registry = ReadInterestRegistry::new(8, 8192);
        let operation = Arc::new(registry.begin_operation().await);
        let scoped = crate::hot_state::HotStateContext::new(
            crate::tracked_state::TrackedStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(),
        )
        .with_read_interest_registry(registry.clone());
        let adapter = lix.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let request = HotStateScanRequest {
            filter: HotStateFilter {
                branch_ids: vec![descriptor.selected_branch.branch_id],
                schema_keys: vec!["lix_key_value".into()],
                row_pks: vec![RowPk::single("future-negative-interest")],
                untracked: Some(false),
                ..Default::default()
            },
            limit: Some(1),
            ..Default::default()
        };
        assert_eq!(
            scoped
                .reader(&read)
                .scan_batch(&request)
                .await
                .unwrap()
                .len(),
            0
        );
        assert!(
            registry
                .snapshot()
                .unwrap()
                .interests
                .iter()
                .any(|interest| interest.as_ref()
                    == &LogicalReadInterest::scan(&request, HotStateReadDomain::Tracked))
        );
        let tiny = ReadInterestRegistry::new(0, 0);
        let _tiny_operation = tiny.begin_operation().await;
        let failing = scoped.with_read_interest_registry(tiny);
        assert!(
            failing.reader(&read).scan_batch(&request).await.is_err(),
            "overflow cannot return a successful unregistered query"
        );
        drop(failing);
        drop(scoped);
        drop(operation);
    }

    #[tokio::test]
    async fn tracked_constraint_and_path_cache_hits_retain_original_native_recipes() {
        use crate::filesystem::{FilesystemPathIndexReader, FilesystemPathIndexRequest};
        use crate::hot_state::HotStateReader;
        let lix = crate::open_lix().await.unwrap();
        let branch = lix
            .partial_replica_descriptor(None)
            .await
            .unwrap()
            .selected_branch
            .branch_id;
        let registry = ReadInterestRegistry::new(256, 1024 * 1024);
        let context = crate::hot_state::HotStateContext::new(
            crate::tracked_state::TrackedStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(),
        )
        .with_read_interest_registry(registry.clone());
        let operation = registry.begin_operation().await;
        let adapter = lix.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let reader = context.reader(&read);
        let request = HotStateScanRequest {
            filter: HotStateFilter {
                branch_ids: vec![branch.clone()],
                schema_keys: vec!["lix_key_value".into()],
                row_pks: vec![RowPk::single("absent-constraint-interest")],
                ..Default::default()
            },
            limit: Some(1),
            ..Default::default()
        };
        assert_eq!(
            reader
                .scan_constraint_batch(&request, true)
                .await
                .unwrap()
                .len(),
            0
        );
        let paths = FilesystemPathIndexRequest::new(vec![branch.clone()]);
        let first = reader.path_index(&paths).await.unwrap();
        let second = reader.path_index(&paths).await.unwrap();
        assert!(
            Arc::ptr_eq(&first, &second),
            "fixture exercises path cache hit"
        );
        reader
            .prepare_packed_identity_membership(&branch, "lix_key_value")
            .await
            .unwrap();
        reader
            .collection_generation(
                &branch,
                crate::collection_generation::CollectionScopeRef {
                    schema_key: "lix_key_value",
                    file_id: None,
                },
            )
            .await
            .unwrap();
        let snapshot = registry.snapshot().unwrap();
        assert!(snapshot.interests.iter().any(|recipe| recipe.as_ref()
            == &LogicalReadInterest::scan(&request, HotStateReadDomain::Tracked)));
        assert_eq!(snapshot.interests.iter().filter(|recipe| matches!(recipe.as_ref(),
            LogicalReadInterest::FilesystemPaths { branch_ids, .. } if branch_ids == &vec![branch.clone()])).count(), 1);
        assert!(
            snapshot
                .interests
                .iter()
                .any(|recipe| matches!(recipe.as_ref(),
            LogicalReadInterest::CollectionGeneration { branch_id, schema_key, file_id: None }
            if branch_id == &branch && schema_key == "lix_key_value"))
        );
        assert!(
            snapshot
                .interests
                .iter()
                .any(|recipe| matches!(recipe.as_ref(),
            LogicalReadInterest::PackedIdentityMembership { branch_id, schema_key }
            if branch_id == &branch && schema_key == "lix_key_value"))
        );
        drop(operation);
        drop(registry.begin_publication(snapshot.revision).await.unwrap());
    }

    #[tokio::test]
    async fn negative_recipe_limit_and_correlated_exact_identities_survive_serialization() {
        let registry = ReadInterestRegistry::new(4, 8192);
        let operation = registry.begin_operation().await;
        let negative = negative_recipe();
        operation.register(negative.clone()).unwrap();
        operation.register(negative.clone()).unwrap();
        let exact = LogicalReadInterest::exact(&HotStateExactBatchRequest {
            rows: vec![
                HotStateExactRowRequest {
                    branch_id: "first".into(),
                    schema_key: "alpha".into(),
                    row_pk: RowPk::single("one"),
                    file_id: Some("file-one".into()),
                },
                HotStateExactRowRequest {
                    branch_id: "second".into(),
                    schema_key: "beta".into(),
                    row_pk: RowPk::single("two"),
                    file_id: None,
                },
            ],
            ..Default::default()
        });
        operation.register(exact.clone()).unwrap();
        let snapshot = registry.snapshot().unwrap();
        assert_eq!(snapshot.revision, 2);
        assert_eq!(snapshot.interests.len(), 2);
        for recipe in [negative, exact] {
            let bytes = serde_json::to_vec(&recipe).unwrap();
            assert_eq!(
                serde_json::from_slice::<LogicalReadInterest>(&bytes).unwrap(),
                recipe
            );
            assert!(
                snapshot
                    .interests
                    .iter()
                    .any(|registered| registered.as_ref() == &recipe)
            );
        }
        assert!(snapshot.serialized_bytes > 0);
    }
    #[tokio::test]
    async fn overflow_rejects_registration_without_losing_existing_interests() {
        let registry = ReadInterestRegistry::new(1, 8192);
        let operation = registry.begin_operation().await;
        operation.register(negative_recipe()).unwrap();
        assert!(
            operation
                .register(LogicalReadInterest::scan(
                    &HotStateScanRequest::default(),
                    HotStateReadDomain::Combined
                ))
                .is_err()
        );
        let retained = registry.snapshot().unwrap();
        assert_eq!(retained.revision, 1);
        assert_eq!(retained.interests.len(), 1);
        let tiny = ReadInterestRegistry::new(1, 1);
        assert!(
            tiny.begin_operation()
                .await
                .register(negative_recipe())
                .is_err()
        );
        assert_eq!(tiny.snapshot().unwrap().revision, 0);
    }
    #[tokio::test]
    async fn publication_waits_for_inflight_registration_and_rejects_stale_candidate() {
        let registry = ReadInterestRegistry::new(4, 8192);
        let operation = registry.begin_operation().await;
        let snapshot = registry.snapshot().unwrap();
        // A snapshot does not hold the publication gate across preparation or
        // network work: another ordinary operation can still start immediately.
        let concurrent = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            registry.begin_operation(),
        )
        .await
        .unwrap();
        drop(concurrent);
        let publisher = Arc::clone(&registry);
        let task =
            tokio::spawn(async move { publisher.begin_publication(snapshot.revision).await });
        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "publication must drain active statement guards"
        );
        operation.register(negative_recipe()).unwrap();
        drop(operation);
        let stale = task.await.unwrap();
        assert!(matches!(stale, Err(error) if error.code == "LIX_PARTIAL_READ_INTEREST_CHANGED"));
        let publication = registry
            .begin_publication(registry.snapshot().unwrap().revision)
            .await
            .unwrap();
        let reader = Arc::clone(&registry);
        let waiting = tokio::spawn(async move { reader.begin_operation().await });
        tokio::task::yield_now().await;
        assert!(
            !waiting.is_finished(),
            "new snapshots must wait until controls are published"
        );
        drop(publication);
        drop(waiting.await.unwrap());
    }
}
