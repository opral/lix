//! The sole ForkTree bootstrap writer.
//!
//! Initialization has no selected view yet, so it is the one legitimate
//! construction boundary for the first authenticated graph.  It emits the
//! same object, selector, and state spaces consumed by normal ForkTree reads
//! in one backend write; it does not seed any superseded tracked-state,
//! branch-control, changelog, or CAS owner.

use bytes::Bytes;
use serde_json::json;

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::changelog::{
    ChangeId as ChangelogChangeId, ChangeRecord, CommitId as ChangelogCommitId,
};
use crate::row_pk::RowPk;
use crate::functions::FunctionProviderHandle;
use crate::json_store::JsonSlot;
use crate::plugin::{PLUGIN_REGISTRY_KEY, PluginRegistry};
use crate::schema::{
    registered_schema_row_pk, schema_key_from_definition, seed_schema_definitions,
};
use crate::storage_adapter::{
    PointReadPlan, Storage, StorageAdapter, StorageGetOptions, StorageWriteOptions, StorageWriteSet,
};

use super::encode_current_state_packs;
use super::model::{
    BranchSelectorV1, BranchSnapshotV1, CanonicalBranchId, ChangeCatalogEntry, ChangeCatalogOwner,
    ChangeObjectV1, CommitCatalogEntry, CommitChangePageV2, CommitId, CommitMemberV1,
    CommitObjectV1, GlobalSelectorV1, RepositoryRootV1, branch_selector_key, global_selector_key,
};
use super::object::{OBJECT_SPACE, ObjectId};
use super::state::{
    StateCell, StateKeyRef, StateValue, StateValueRef, encode_state_key, encode_state_value,
};
use super::tree::{
    ImmutableObjectSet, build_change_catalog, build_commit_catalog, build_state_tree,
};
use super::view::SELECTOR_SPACE;

const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";
const LIX_ID_KEY: &str = "lix_id";
const WORKSPACE_BRANCH_KEY: &str = "lix_workspace_branch_id";

struct SeedRow {
    key: Vec<u8>,
    change_id: ChangelogChangeId,
    local_change_id: Option<ChangelogChangeId>,
    schema_key: String,
    row_pk: RowPk,
    file_id: Option<String>,
    native_snapshot: serde_json::Value,
    snapshot: JsonSlot,
    metadata: JsonSlot,
}

pub(crate) async fn initialize_empty_repository<S>(
    storage: StorageAdapter<S>,
) -> Result<crate::init::InitReceipt, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let read = storage.begin_read(Default::default()).await?;
    let selector = PointReadPlan::new(
        SELECTOR_SPACE,
        &[crate::storage_adapter::StorageKey(Bytes::from(
            global_selector_key().to_vec(),
        ))],
    )
    .materialize(&read, StorageGetOptions::default())
    .await?;
    if selector.value.into_iter().next().flatten().is_some() {
        return Err(crate::init::already_initialized_error());
    }
    let mut functions = FunctionProviderHandle::system();
    let main_branch = functions.call_uuid_v7();
    let lix_id = functions.call_uuid_v7();
    let initial_commit_uuid = functions.call_uuid_v7();
    let initial_commit = ChangelogCommitId::new(initial_commit_uuid);
    let initial_commit_change = ChangelogChangeId::new(functions.call_uuid_v7());
    let model_commit = CommitId::from_bytes(*initial_commit_uuid.as_bytes());
    let timestamp = functions.call_timestamp();
    let global_branch = CanonicalBranchId::from_bytes(
        *uuid::Uuid::parse_str(GLOBAL_BRANCH_ID)
            .map_err(|error| LixError::new(LixError::CODE_INTERNAL_ERROR, error.to_string()))?
            .as_bytes(),
    );
    let main_branch_id = CanonicalBranchId::from_bytes(*main_branch.as_bytes());

    let mut rows = Vec::new();
    let mut add_row = |schema_key: &str,
                       file_id: Option<&str>,
                       row_pk: RowPk,
                       mut snapshot: serde_json::Value|
     -> Result<(), LixError> {
        let change_uuid = functions.call_uuid_v7();
        let change_id = ChangelogChangeId::new(change_uuid);
        let local = schema_key == crate::checkpoint::CHECKPOINT_MARKER_SCHEMA_KEY
            || (schema_key == KEY_VALUE_SCHEMA_KEY
                && row_pk.as_single_string().is_ok_and(|key| key == PLUGIN_REGISTRY_KEY));
        let schema = crate::native_row::seed_schema(schema_key)?;
        let serde_json::Value::Array(primary_key) = row_pk.as_json_array_value()? else {
            unreachable!("typed row primary key always encodes as an array")
        };
        let object = snapshot.as_object_mut().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("bootstrap row '{schema_key}' is not an object"),
            )
        })?;
        for (name, value) in schema.primary_key.iter().zip(primary_key) {
            object.insert(name.clone(), value);
        }
        let encoded_snapshot = JsonSlot::Inline(snapshot.to_string().into());
        let key = encode_state_key(StateKeyRef {
            schema_key,
            file_id,
            row_pk: &row_pk,
        });
        rows.push(SeedRow {
            key,
            change_id,
            local_change_id: local.then(|| ChangelogChangeId::new(functions.call_uuid_v7())),
            schema_key: schema_key.to_owned(),
            row_pk,
            file_id: file_id.map(str::to_owned),
            native_snapshot: snapshot,
            snapshot: encoded_snapshot,
            metadata: JsonSlot::None,
        });
        Ok(())
    };

    for schema in seed_schema_definitions() {
        let schema_key = schema_key_from_definition(schema)?.schema_key;
        add_row(
            "lix_registered_schema",
            None,
            registered_schema_row_pk(&schema_key)?,
            json!({ "value": schema }),
        )?;
    }
    add_row(
        KEY_VALUE_SCHEMA_KEY,
        None,
        RowPk::single(LIX_ID_KEY),
        json!({ "key": LIX_ID_KEY, "value": lix_id.to_string() }),
    )?;
    add_row(
        KEY_VALUE_SCHEMA_KEY,
        None,
        RowPk::single(WORKSPACE_BRANCH_KEY),
        json!({ "key": WORKSPACE_BRANCH_KEY, "value": main_branch.to_string() }),
    )?;
    // Registry absence is corruption, not an implicit empty authority. Seed
    // the canonical empty registry into the initial authenticated state so
    // the first install and every later lifecycle mutation replace one
    // explicit branch-local owner through normal transaction publication.
    add_row(
        KEY_VALUE_SCHEMA_KEY,
        None,
        RowPk::single(PLUGIN_REGISTRY_KEY),
        PluginRegistry::empty().to_snapshot()?,
    )?;
    add_row(
        "lix_branch_descriptor",
        None,
        RowPk::uuid_from_canonical(GLOBAL_BRANCH_ID)
            .map_err(|error| LixError::new(LixError::CODE_INTERNAL_ERROR, error.to_string()))?,
        json!({ "id": GLOBAL_BRANCH_ID, "name": "global", "hidden": true }),
    )?;
    add_row(
        "lix_branch_descriptor",
        None,
        RowPk::uuid_from_canonical(&main_branch.to_string())
            .map_err(|error| LixError::new(LixError::CODE_INTERNAL_ERROR, error.to_string()))?,
        json!({ "id": main_branch.to_string(), "name": "main", "hidden": false }),
    )?;
    for (id, name, kind) in [
        (crate::SYSTEM_ACCOUNT_ID, "System", "system"),
        (crate::ANONYMOUS_ACCOUNT_ID, "Anonymous", "anonymous"),
    ] {
        add_row(
            "lix_account",
            None,
            RowPk::uuid_from_canonical(id)
                .map_err(|error| LixError::new(LixError::CODE_INTERNAL_ERROR, error.to_string()))?,
            json!({ "id": id, "name": name, "kind": kind, "status": "active" }),
        )?;
    }
    add_row(
        "lix_checkpoint_marker",
        None,
        RowPk::uuid_from_canonical(&main_branch.to_string())
            .map_err(|error| LixError::new(LixError::CODE_INTERNAL_ERROR, error.to_string()))?,
        json!({ "branch_id": main_branch.to_string() }),
    )?;

    rows.sort_by(|left, right| left.key.cmp(&right.key));
    let local_rows = rows
        .iter()
        .filter(|row| row.local_change_id.is_some())
        .collect::<Vec<_>>();
    let mut semantic_members = Vec::with_capacity(rows.len() + local_rows.len());
    let mut changes = Vec::with_capacity(rows.len() + local_rows.len());
    for row in &rows {
            let public_change_id = row.change_id;
            let payload = crate::changelog::encode_forktree_change_payload(&ChangeRecord {
                format_version: 2,
                change_id: public_change_id,
                account_id: crate::SYSTEM_ACCOUNT_ID.to_owned(),
                schema_key: row.schema_key.clone(),
                row_pk: row.row_pk.clone(),
                file_id: row.file_id.clone(),
                snapshot: row.snapshot.clone(),
                metadata: row.metadata.clone(),
                created_at: timestamp,
                origin_key: None,
            })?;
            let change_id =
                super::model::ChangeId::from_bytes(*public_change_id.as_uuid().as_bytes());
        semantic_members.push(CommitMemberV1::introduced(
            change_id,
            payload,
            true,
            timestamp,
            Vec::new(),
        ));
        changes.push(change_id);
    }
    for row in &local_rows {
        let public_change_id = row.local_change_id.expect("local seed row has an identity");
        let payload = crate::changelog::encode_forktree_change_payload(&ChangeRecord {
            format_version: 2,
            change_id: public_change_id,
            account_id: crate::SYSTEM_ACCOUNT_ID.to_owned(),
            schema_key: row.schema_key.clone(),
            row_pk: row.row_pk.clone(),
            file_id: row.file_id.clone(),
            snapshot: row.snapshot.clone(),
            metadata: row.metadata.clone(),
            created_at: timestamp,
            origin_key: None,
        })?;
        let change_id = super::model::ChangeId::from_bytes(*public_change_id.as_uuid().as_bytes());
        semantic_members.push(CommitMemberV1::introduced(
            change_id,
            payload,
            false,
            timestamp,
            Vec::new(),
        ));
        changes.push(change_id);
    }

    let member_pages = CommitChangePageV2::encode_pages(model_commit, &semantic_members)
        .map_err(LixError::from)?;
    let values_for =
        |seed_rows: &[&SeedRow],
         locations: &[super::model::StatePageLocation],
         branch_id: &str|
         -> Result<Vec<(Vec<u8>, StateValue, super::model::StatePageLocation)>, LixError> {
            seed_rows.iter()
                .zip(locations)
                .map(|(row, location)| {
                    let change_id = if branch_id == crate::GLOBAL_BRANCH_ID {
                        row.change_id
                    } else {
                        row.local_change_id.expect("local seed row has an identity")
                    };
                    let cell = match &row.snapshot {
                        JsonSlot::Inline(_) => StateCell::NativeRow(crate::native_row::encode(
                            &crate::native_row::seed_schema(&row.schema_key)?,
                            &row.row_pk,
                            branch_id == crate::GLOBAL_BRANCH_ID,
                            row.file_id.as_deref(),
                            &row.native_snapshot,
                        )?),
                        JsonSlot::None => StateCell::Tombstone,
                        JsonSlot::ForkTreeObject(_) => {
                            return Err(LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "bootstrap current state contains an out-of-pack JSON reference",
                            ));
                        }
                    };
                    Ok((
                        row.key.clone(),
                        StateValue {
                            change_id,
                            commit_id: initial_commit,
                            created_at: timestamp,
                            updated_at: timestamp,
                            cell,
                            metadata: match &row.metadata {
                                JsonSlot::None => None,
                                JsonSlot::Inline(value) => Some(value.clone().into()),
                                JsonSlot::ForkTreeObject(_) => {
                                    return Err(LixError::new(
                                        LixError::CODE_INTERNAL_ERROR,
                                        "bootstrap metadata contains an out-of-pack JSON reference",
                                    ));
                                }
                            },
                            origin_key: None,
                            blob_manifest_object_ids: Vec::new(),
                        },
                        *location,
                    ))
                })
                .collect()
        };
    let global_packs = encode_current_state_packs(
        model_commit,
        true,
        values_for(
            &rows.iter().collect::<Vec<_>>(),
            &member_pages.member_locations[..rows.len()],
            crate::GLOBAL_BRANCH_ID,
        )?,
    )
    .map_err(LixError::from)?;
    let local_packs = encode_current_state_packs(
        model_commit,
        false,
        values_for(
            &local_rows,
            &member_pages.member_locations[rows.len()..],
            &main_branch.to_string(),
        )?,
    )
    .map_err(LixError::from)?;
    let global_entries = rows
        .iter()
        .map(|row| {
            let location = global_packs.locations.get(&row.key).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "bootstrap global pack omitted a row",
                )
            })?;
            Ok((
                row.key.clone(),
                encode_state_value(StateValueRef {
                    pack_object_id: location.pack_object_id,
                    pack_ordinal: location.pack_ordinal,
                })?,
            ))
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let local_entries = local_rows
        .iter()
        .map(|row| {
            let location = local_packs.locations.get(&row.key).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "bootstrap local pack omitted a row",
                )
            })?;
            Ok((
                row.key.clone(),
                encode_state_value(StateValueRef {
                    pack_object_id: location.pack_object_id,
                    pack_ordinal: location.pack_ordinal,
                })?,
            ))
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    let global_state = build_state_tree(&global_entries).map_err(LixError::from)?;
    let local_state = build_state_tree(&local_entries).map_err(LixError::from)?;

    let mut objects = ImmutableObjectSet::default();
    objects
        .extend(global_state.objects)
        .map_err(LixError::from)?;
    objects
        .extend(local_state.objects)
        .map_err(LixError::from)?;
    for (page_id, page_bytes) in &member_pages.objects {
        objects
            .insert(*page_id, page_bytes.clone())
            .map_err(LixError::from)?;
    }
    for (pack_id, pack_bytes) in global_packs.objects.into_iter().chain(local_packs.objects) {
        objects
            .insert(pack_id, pack_bytes)
            .map_err(LixError::from)?;
    }

    let mut commit = CommitObjectV1 {
        commit_id: model_commit,
        generation: 1,
        parent_commit_object_ids: Vec::new(),
        members: semantic_members,
        member_page_object_ids: member_pages.objects.iter().map(|(id, _)| *id).collect(),
        global_state_root: global_state.root.object_id,
        local_state_root: local_state.root.object_id,
        checkpoint_cursor: super::model::CheckpointCursorV1::root(),
        metadata: crate::changelog::encode_forktree_commit_payload(
            &crate::changelog::CommitRecord {
                format_version: 2,
                commit_id: initial_commit,
                generation: 1,
                account_id: crate::SYSTEM_ACCOUNT_ID.to_owned(),
                created_at: timestamp,
                parent_commit_ids: Vec::new(),
                change_id: initial_commit_change,
            },
        )?,
    };
    let _ = commit.prepare_member_pages().map_err(LixError::from)?;
    let (commit_object_id, commit_bytes) = commit.encode().map_err(LixError::from)?;
    objects
        .insert(commit_object_id, commit_bytes)
        .map_err(LixError::from)?;
    let global_ref = branch_ref_change(
        &mut functions,
        global_branch,
        commit_object_id,
        None,
        timestamp,
    )?;
    let main_ref = branch_ref_change(
        &mut functions,
        main_branch_id,
        commit_object_id,
        None,
        timestamp,
    )?;
    let global_ref_id = global_ref.0;
    let global_ref_object_id = global_ref.1;
    let main_ref_object_id = main_ref.1;
    let mut ref_changes = Vec::new();
    for (change_id, object_id, bytes) in [global_ref, main_ref] {
        objects.insert(object_id, bytes).map_err(LixError::from)?;
        ref_changes.push((change_id, object_id));
    }
    let commit_catalog =
        build_commit_catalog(&[(model_commit, CommitCatalogEntry { commit_object_id })])
            .map_err(LixError::from)?;
    let mut change_catalog_entries = changes
        .iter()
        .enumerate()
        .map(|(ordinal, change_id)| {
            (
                *change_id,
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::CommitMember {
                        commit_object_id,
                        ordinal: u32::try_from(ordinal).expect("bootstrap ordinal fits u32"),
                    },
                },
            )
        })
        .chain(ref_changes.iter().map(|(change_id, object_id)| {
            (
                *change_id,
                ChangeCatalogEntry {
                    owner: ChangeCatalogOwner::BranchRef {
                        ref_change_object_id: *object_id,
                        branch_id: if *change_id == global_ref_id {
                            global_branch
                        } else {
                            main_branch_id
                        },
                    },
                },
            )
        }))
        .collect::<Vec<_>>();
    change_catalog_entries.sort_by(|left, right| left.0.cmp(&right.0));
    let change_catalog = build_change_catalog(&change_catalog_entries).map_err(LixError::from)?;
    objects
        .extend(commit_catalog.objects)
        .map_err(LixError::from)?;
    objects
        .extend(change_catalog.objects)
        .map_err(LixError::from)?;
    let repository = RepositoryRootV1 {
        global_state_root: global_state.root.object_id,
        commit_catalog_root: commit_catalog.root.object_id,
        change_catalog_root: change_catalog.root.object_id,
    };
    let (repository_id, repository_bytes) = repository.encode().map_err(LixError::from)?;
    objects
        .insert(repository_id, repository_bytes)
        .map_err(LixError::from)?;
    let global_snapshot = BranchSnapshotV1 {
        branch_id: global_branch,
        local_state_root: local_state.root.object_id,
        semantic_head_commit_object_id: commit_object_id,
        latest_ref_change_object_id: Some(global_ref_object_id),
        historical_global_state_root: global_state.root.object_id,
    };
    let main_snapshot = BranchSnapshotV1 {
        branch_id: main_branch_id,
        local_state_root: local_state.root.object_id,
        semantic_head_commit_object_id: commit_object_id,
        latest_ref_change_object_id: Some(main_ref_object_id),
        historical_global_state_root: global_state.root.object_id,
    };
    let (global_snapshot_id, global_snapshot_bytes) =
        global_snapshot.encode().map_err(LixError::from)?;
    let (main_snapshot_id, main_snapshot_bytes) = main_snapshot.encode().map_err(LixError::from)?;
    objects
        .insert(global_snapshot_id, global_snapshot_bytes)
        .map_err(LixError::from)?;
    objects
        .insert(main_snapshot_id, main_snapshot_bytes)
        .map_err(LixError::from)?;

    let mut writes = StorageWriteSet::with_capacity(objects.iter().count() + 5, 3);
    for (id, bytes) in objects.iter() {
        writes.put(OBJECT_SPACE, id.as_bytes().to_vec(), bytes.to_vec());
    }
    let global_selector_storage_key = global_selector_key().to_vec();
    writes.put(
        SELECTOR_SPACE,
        global_selector_storage_key.clone(),
        GlobalSelectorV1 {
            repository_root: repository_id,
            epoch: 1,
            selector_generation: 1,
        }
        .encode()
        .map_err(LixError::from)?
        .to_vec(),
    );
    let global_branch_selector_storage_key = branch_selector_key(global_branch).to_vec();
    let main_branch_selector_storage_key = branch_selector_key(main_branch_id).to_vec();
    for (branch_id, snapshot_id) in [
        (global_branch, global_snapshot_id),
        (main_branch_id, main_snapshot_id),
    ] {
        writes.put(
            SELECTOR_SPACE,
            if branch_id == global_branch {
                global_branch_selector_storage_key.clone()
            } else {
                main_branch_selector_storage_key.clone()
            },
            BranchSelectorV1 {
                branch_id,
                branch_snapshot_object_id: snapshot_id,
                selector_generation: 1,
            }
            .encode()
            .map_err(LixError::from)?
            .to_vec(),
        );
    }
    crate::init::stage_repository_protocol(&mut writes);
    #[cfg(test)]
    tests::inject_selector_before_bootstrap_commit(
        &storage,
        global_selector_storage_key.clone(),
        main_branch_selector_storage_key.clone(),
    )
    .await?;
    storage
        .commit_write_set(
            writes,
            StorageWriteOptions {
                preconditions: vec![
                    crate::storage_adapter::StoragePrecondition::KeyAbsent {
                        space: crate::init::REPOSITORY_PROTOCOL_SPACE,
                        key: crate::storage_adapter::StorageKey(Bytes::from_static(
                            crate::init::REPOSITORY_PROTOCOL_KEY,
                        )),
                    },
                    crate::storage_adapter::StoragePrecondition::KeyAbsent {
                        space: SELECTOR_SPACE,
                        key: crate::storage_adapter::StorageKey(Bytes::from(
                            global_selector_storage_key,
                        )),
                    },
                    crate::storage_adapter::StoragePrecondition::KeyAbsent {
                        space: SELECTOR_SPACE,
                        key: crate::storage_adapter::StorageKey(Bytes::from(
                            global_branch_selector_storage_key,
                        )),
                    },
                    crate::storage_adapter::StoragePrecondition::KeyAbsent {
                        space: SELECTOR_SPACE,
                        key: crate::storage_adapter::StorageKey(Bytes::from(
                            main_branch_selector_storage_key,
                        )),
                    },
                ],
                ..StorageWriteOptions::default()
            },
        )
        .await?;
    Ok(crate::init::InitReceipt {
        lix_id: lix_id.to_string(),
        global_branch_id: GLOBAL_BRANCH_ID.to_owned(),
        main_branch_id: main_branch.to_string(),
        initial_commit_id: initial_commit.to_string(),
    })
}

fn branch_ref_change(
    functions: &mut FunctionProviderHandle,
    branch_id: CanonicalBranchId,
    commit_object_id: ObjectId,
    previous_ref: Option<ObjectId>,
    updated_at: crate::common::LixTimestamp,
) -> Result<(super::model::ChangeId, ObjectId, Bytes), LixError> {
    let change_uuid = functions.call_uuid_v7();
    let change_id = super::model::ChangeId::from_bytes(*change_uuid.as_bytes());
    let change = ChangeObjectV1::BranchRef {
        change_id,
        updated_at,
        branch_id,
        before_semantic_head_commit_object_id: None,
        after_semantic_head_commit_object_id: Some(commit_object_id),
        previous_ref_change_object_id: previous_ref,
        payload: Vec::new(),
        json_payload_object_ids: Vec::new(),
    };
    let (object_id, bytes) = change.encode().map_err(LixError::from)?;
    Ok((change_id, object_id, bytes))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU8, Ordering};

    use bytes::Bytes;

    use super::*;
    use crate::engine::Engine;
    use crate::storage_adapter::{
        Memory, PointReadPlan, StorageGetOptions, StorageKey, StorageWriteOptions,
    };

    const NO_SELECTOR_RACE: u8 = 0;
    const GLOBAL_SELECTOR_RACE: u8 = 1;
    const BRANCH_SELECTOR_RACE: u8 = 2;

    static SELECTOR_RACE: AtomicU8 = AtomicU8::new(NO_SELECTOR_RACE);
    static LAST_INJECTED_SELECTOR_KEY: Mutex<Option<Vec<u8>>> = Mutex::new(None);
    static RACE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    async fn race_test_lock() -> tokio::sync::MutexGuard<'static, ()> {
        RACE_TEST_LOCK.lock().await
    }

    fn arm_selector_race(kind: u8) {
        SELECTOR_RACE.store(kind, Ordering::SeqCst);
    }

    pub(super) async fn inject_selector_before_bootstrap_commit<S>(
        storage: &StorageAdapter<S>,
        global_selector_key: Vec<u8>,
        main_branch_selector_key: Vec<u8>,
    ) -> Result<(), LixError>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let kind = SELECTOR_RACE.swap(NO_SELECTOR_RACE, Ordering::SeqCst);
        let key = match kind {
            GLOBAL_SELECTOR_RACE => global_selector_key,
            BRANCH_SELECTOR_RACE => main_branch_selector_key,
            _ => return Ok(()),
        };
        *LAST_INJECTED_SELECTOR_KEY.lock().unwrap() = Some(key.clone());

        let mut writes = storage.new_write_set();
        writes.put(SELECTOR_SPACE, key, &b"concurrent-selector"[..]);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .map_err(LixError::from)?;
        Ok(())
    }

    async fn selector_value(storage: &Memory, key: Vec<u8>) -> Option<Bytes> {
        let adapter = StorageAdapter::new(storage.clone());
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let value = PointReadPlan::new(SELECTOR_SPACE, &[StorageKey(Bytes::from(key))])
            .materialize(&read, StorageGetOptions::default())
            .await
            .unwrap()
            .value
            .into_iter()
            .next()
            .flatten();
        match value {
            Some(crate::storage_adapter::StorageProjectedValue::FullValue(value)) => Some(value),
            _ => None,
        }
    }

    #[tokio::test]
    async fn initialize_accepts_first_repository_and_rejects_second() {
        let _race_test_lock = race_test_lock().await;
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("first initialization should succeed");

        let error = Engine::initialize(storage)
            .await
            .expect_err("second initialization should be rejected");
        assert_eq!(error.code, "LIX_ERROR_ALREADY_INITIALIZED");
    }

    #[tokio::test]
    async fn selector_insertion_race_is_atomic_for_global_and_branch_keys() {
        let _race_test_lock = race_test_lock().await;
        for (kind, expected_key_is_global) in
            [(GLOBAL_SELECTOR_RACE, true), (BRANCH_SELECTOR_RACE, false)]
        {
            let storage = Memory::new();
            arm_selector_race(kind);

            // The test-only hook inserts the exact key that bootstrap is about
            // to write. Selector KeyAbsent preconditions must reject the whole
            // write set, leaving the racing value intact.
            let error = Engine::initialize(storage.clone())
                .await
                .expect_err("concurrent selector insertion must fail bootstrap");
            assert_eq!(error.code, LixError::CODE_TRANSACTION_CONFLICT);

            let selector_key = LAST_INJECTED_SELECTOR_KEY
                .lock()
                .unwrap()
                .clone()
                .expect("race hook must record the inserted key");
            assert_eq!(
                selector_key == global_selector_key().to_vec(),
                expected_key_is_global
            );
            assert_eq!(
                selector_value(&storage, selector_key).await,
                Some(Bytes::from_static(b"concurrent-selector"))
            );

            let adapter = StorageAdapter::new(storage);
            let read = adapter
                .begin_read(Default::default())
                .await
                .expect("storage must remain readable after rejection");
            assert_eq!(
                crate::init::repository_protocol_status(&read)
                    .await
                    .unwrap(),
                crate::init::RepositoryProtocolStatus::Missing,
                "a rejected bootstrap must not publish the protocol marker"
            );
        }
    }
}
