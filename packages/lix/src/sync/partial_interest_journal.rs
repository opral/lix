//! Epoch-bound durable native read recipes. Not a coverage certificate.
//! The inventory is restored off the repository opening path. Warm unchanged
//! operations do no journal I/O; newly successful operations flush first.
use super::partial_state::{
    PARTIAL_REPLICA_STATE_SPACE, PartialReplicaState, load_partial_replica_state,
    partial_replica_state_key,
};
use crate::LixError;
use crate::hot_state::{LogicalReadInterest, ReadInterestRegistry};
use crate::storage_adapter::{
    PointReadPlan, Storage, StorageAdapter, StorageAdapterRead, StorageKey, StoragePrecondition,
    StorageProjectedValue, StorageSpace, StorageSpaceId, StorageWriteOptions, StorageWriteSet,
    ValueSemantics,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

pub(crate) const PARTIAL_READ_INTEREST_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_001b),
    "sync.partial_read_interests.v1",
    ValueSemantics::Mutable,
);
const MAX_RECIPES: usize = 4096;
const MAX_RECIPE_BYTES: usize = 4 * 1024 * 1024;
const MAX_DOCUMENT_BYTES: usize = MAX_RECIPE_BYTES + 256 * 1024;
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Journal {
    version: u32,
    epoch_id: String,
    recipes: Vec<LogicalReadInterest>,
}
fn invalid(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_INTEREST_JOURNAL_INVALID", message)
}
fn key(state: &PartialReplicaState) -> Result<StorageKey, LixError> {
    let bytes = crate::storage_codec::id_string::uuid_bytes_from_canonical(state.epoch_id())
        .ok_or_else(|| invalid("interest journal epoch is malformed"))?;
    Ok(StorageKey(Bytes::copy_from_slice(&bytes)))
}
fn canonical_recipes(
    recipes: Vec<LogicalReadInterest>,
) -> Result<Vec<LogicalReadInterest>, LixError> {
    if recipes.len() > MAX_RECIPES {
        return Err(invalid("interest journal exceeds recipe count bound"));
    }
    let mut ordered = BTreeMap::new();
    let mut total = 0usize;
    for recipe in recipes {
        let bytes =
            serde_json::to_vec(&recipe).map_err(|_| invalid("interest recipe encoding failed"))?;
        total = total
            .checked_add(bytes.len())
            .ok_or_else(|| invalid("interest byte count overflow"))?;
        if total > MAX_RECIPE_BYTES {
            return Err(invalid("interest journal exceeds recipe byte bound"));
        }
        if ordered.insert(bytes, recipe).is_some() {
            return Err(invalid("interest journal contains duplicate recipes"));
        }
    }
    Ok(ordered.into_values().collect())
}
fn encode(
    state: &PartialReplicaState,
    recipes: Vec<LogicalReadInterest>,
) -> Result<Bytes, LixError> {
    let recipes = canonical_recipes(recipes)?;
    let bytes = serde_json::to_vec(&Journal {
        version: 2,
        epoch_id: state.epoch_id().into(),
        recipes,
    })
    .map_err(|_| invalid("interest journal encoding failed"))?;
    if bytes.len() > MAX_DOCUMENT_BYTES {
        return Err(invalid("interest journal exceeds document bound"));
    }
    Ok(Bytes::from(bytes))
}
/// Fresh bootstrap must install this alongside its receipt. A missing journal
/// later is corruption; it is never interpreted as an empty retained set.
pub(super) fn stage_initial_read_interest_journal(
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
) -> Result<StoragePrecondition, LixError> {
    let key = key(state)?;
    writes.put(
        PARTIAL_READ_INTEREST_SPACE,
        key.clone(),
        crate::storage_adapter::StorageValue {
            bytes: encode(state, vec![])?,
        },
    );
    Ok(StoragePrecondition::KeyAbsent {
        space: PARTIAL_READ_INTEREST_SPACE,
        key,
    })
}
async fn load(
    read: &(impl StorageAdapterRead + ?Sized),
    expected: &PartialReplicaState,
) -> Result<(Vec<LogicalReadInterest>, Bytes, StoragePrecondition), LixError> {
    let (actual, receipt) = load_partial_replica_state(read)
        .await?
        .ok_or_else(|| invalid("interest journal has no partial admission"))?;
    if actual.repository_id() != expected.repository_id()
        || actual.remote_id() != expected.remote_id()
        || actual.active_account_id() != expected.active_account_id()
        || actual.epoch_id() != expected.epoch_id()
    {
        return Err(invalid("interest journal belongs to another admission"));
    }
    let value = PointReadPlan::new(PARTIAL_READ_INTEREST_SPACE, &[key(expected)?])
        .materialize(read, Default::default())
        .await?
        .value
        .pop()
        .flatten();
    let Some(StorageProjectedValue::FullValue(bytes)) = value else {
        return Err(invalid(
            "interest journal is missing or its value was omitted",
        ));
    };
    if bytes.len() > MAX_DOCUMENT_BYTES {
        return Err(invalid("interest journal exceeds document bound"));
    }
    let journal: Journal =
        serde_json::from_slice(&bytes).map_err(|_| invalid("interest journal is malformed"))?;
    if journal.version != 2 || journal.epoch_id != expected.epoch_id() {
        return Err(invalid("interest journal version or epoch mismatch"));
    }
    let recipes = canonical_recipes(journal.recipes)?;
    Ok((
        recipes,
        bytes,
        StoragePrecondition::KeyValueEquals {
            space: PARTIAL_REPLICA_STATE_SPACE,
            key: partial_replica_state_key(),
            expected: receipt,
        },
    ))
}
/// Candidate preparation must refresh durable interests even if this engine
/// already flushed its own scopes. Publish with both returned CAS guards.
/// This does not replace operation coordination across independent processes.
pub(super) async fn restore_candidate_read_interests(
    read: &(impl StorageAdapterRead + ?Sized),
    expected: &PartialReplicaState,
    registry: &Arc<ReadInterestRegistry>,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let (recipes, previous, epoch_guard) = load(read, expected).await?;
    registry.merge_persisted(recipes)?;
    Ok(vec![
        epoch_guard,
        StoragePrecondition::KeyValueEquals {
            space: PARTIAL_READ_INTEREST_SPACE,
            key: key(expected)?,
            expected: previous,
        },
    ])
}

/// Restore and flush against the same exact epoch. CAS retries merge concurrent
/// writers' recipes; neither side can overwrite another query's retained scope.
/// Do not call through session write admission: explicit transactions may own it.
pub(crate) async fn flush_partial_read_interests<S>(
    storage: &StorageAdapter<S>,
    expected: &PartialReplicaState,
    registry: &Arc<ReadInterestRegistry>,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if registry.durability_is_clean()? {
        return Ok(());
    }
    for _ in 0..16 {
        let read = storage.begin_read(Default::default()).await?;
        let (recipes, previous, epoch_guard) = load(&read, expected).await?;
        registry.merge_persisted(recipes)?;
        if registry.durability_is_clean()? {
            return Ok(());
        }
        let snapshot = registry.snapshot()?;
        let next = encode(
            expected,
            snapshot
                .interests
                .iter()
                .map(|recipe| recipe.as_ref().clone())
                .collect(),
        )?;
        let mut writes = storage.new_write_set();
        writes.put(
            PARTIAL_READ_INTEREST_SPACE,
            key(expected)?,
            crate::storage_adapter::StorageValue { bytes: next },
        );
        drop(read);
        let result = storage
            .commit_partial_replica_write_set(
                super::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    await_durable: true,
                    preconditions: vec![
                        epoch_guard,
                        StoragePrecondition::KeyValueEquals {
                            space: PARTIAL_READ_INTEREST_SPACE,
                            key: key(expected)?,
                            expected: previous,
                        },
                    ],
                    ..Default::default()
                },
            )
            .await;
        match result {
            Ok(_) => {
                registry.acknowledge_durable(snapshot.revision)?;
                return Ok(());
            }
            Err(crate::storage_adapter::StorageWriteSetError::Storage(
                crate::storage_adapter::StorageError::PreconditionFailed(_),
            )) => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Err(LixError::new(
        LixError::CODE_TRANSACTION_CONFLICT,
        "interest journal changed throughout bounded flush retries",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn recipe(key: &str) -> LogicalReadInterest {
        LogicalReadInterest::scan(
            &crate::hot_state::HotStateScanRequest {
                filter: crate::hot_state::HotStateFilter {
                    schema_keys: vec!["lix_key_value".into()],
                    row_pks: vec![crate::row_pk::RowPk::single(key)],
                    ..Default::default()
                },
                limit: Some(1),
                ..Default::default()
            },
            crate::hot_state::HotStateReadDomain::Tracked,
        )
    }
    async fn fixture() -> (StorageAdapter<crate::Memory>, PartialReplicaState) {
        let authority = crate::open_lix().await.unwrap();
        let state = PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            authority.active_account_id().into(),
            "00000000-0000-7000-8000-000000005101".into(),
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap();
        let storage = StorageAdapter::new(crate::Memory::new());
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        let guards = super::super::stage_partial_bootstrap(&read, &mut writes, &state).unwrap();
        crate::init::stage_partial_repository_protocol(&mut writes);
        drop(read);
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions: guards,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        (storage, state)
    }
    #[tokio::test]
    async fn durable_negative_interests_restore_and_warm_flush_writes_nothing() {
        let (storage, state) = fixture().await;
        let registry = ReadInterestRegistry::new_durable(MAX_RECIPES, MAX_RECIPE_BYTES);
        assert!(registry.begin_publication(0).await.is_err());
        let operation = registry.begin_operation().await;
        operation.register(recipe("negative-durable")).unwrap();
        drop(operation);
        assert!(
            registry
                .begin_publication(registry.snapshot().unwrap().revision)
                .await
                .is_err()
        );
        flush_partial_read_interests(&storage, &state, &registry)
            .await
            .unwrap();
        let revision = storage.load_mutation_revision().await.unwrap();
        flush_partial_read_interests(&storage, &state, &registry)
            .await
            .unwrap();
        assert_eq!(storage.load_mutation_revision().await.unwrap(), revision);
        let restored = ReadInterestRegistry::new_durable(MAX_RECIPES, MAX_RECIPE_BYTES);
        flush_partial_read_interests(&storage, &state, &restored)
            .await
            .unwrap();
        assert_eq!(
            storage.load_mutation_revision().await.unwrap(),
            revision,
            "restore is read-only"
        );
        let snapshot = restored.snapshot().unwrap();
        assert_eq!(snapshot.interests.len(), 1);
        assert_eq!(snapshot.interests[0].as_ref(), &recipe("negative-durable"));
        drop(restored.begin_publication(snapshot.revision).await.unwrap());
    }
    #[tokio::test]
    async fn independent_registry_flushes_union_scopes_and_corruption_blocks_publication() {
        let (storage, state) = fixture().await;
        let first = ReadInterestRegistry::new_durable(MAX_RECIPES, MAX_RECIPE_BYTES);
        let second = ReadInterestRegistry::new_durable(MAX_RECIPES, MAX_RECIPE_BYTES);
        for (registry, key) in [(&first, "first-negative"), (&second, "second-negative")] {
            let operation = registry.begin_operation().await;
            operation.register(recipe(key)).unwrap();
            drop(operation);
            flush_partial_read_interests(&storage, &state, registry)
                .await
                .unwrap();
        }
        let read = storage.begin_read(Default::default()).await.unwrap();
        assert_eq!(load(&read, &state).await.unwrap().0.len(), 2);
        drop(read);
        let mut writes = storage.new_write_set();
        writes.put(
            PARTIAL_READ_INTEREST_SPACE,
            key(&state).unwrap(),
            b"corrupt".as_slice(),
        );
        storage
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                writes,
                Default::default(),
            )
            .await
            .unwrap();
        let restored = ReadInterestRegistry::new_durable(MAX_RECIPES, MAX_RECIPE_BYTES);
        assert!(
            flush_partial_read_interests(&storage, &state, &restored)
                .await
                .is_err()
        );
        assert!(restored.begin_publication(0).await.is_err());
    }
    #[tokio::test]
    async fn lossy_version_one_journal_is_rejected_even_when_empty() {
        let (storage, state) = fixture().await;
        let mut writes = storage.new_write_set();
        writes.put(
            PARTIAL_READ_INTEREST_SPACE,
            key(&state).unwrap(),
            serde_json::to_vec(
                &serde_json::json!({"version":1,"epochId":state.epoch_id(),"recipes":[]}),
            )
            .unwrap(),
        );
        storage
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let error = load(&read, &state).await.err().unwrap();
        assert_eq!(error.code, "LIX_PARTIAL_INTEREST_JOURNAL_INVALID");
    }
}
