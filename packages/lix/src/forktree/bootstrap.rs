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
use crate::entity_pk::EntityPk;
use crate::functions::FunctionProviderHandle;
use crate::json_store::JsonSlot;
use crate::schema::{
    registered_schema_entity_pk, schema_key_from_definition, seed_schema_definitions,
};
use crate::storage_adapter::{
    PointReadPlan, Storage, StorageAdapter, StorageGetOptions, StorageWriteOptions, StorageWriteSet,
};

use super::model::{
    BranchSelectorV1, BranchSnapshotV1, CanonicalBranchId, ChangeCatalogEntry, ChangeCatalogOwner,
    ChangeObjectV1, CommitCatalogEntry, CommitId, CommitMemberV1, CommitObjectV1, GlobalSelectorV1,
    RepositoryRootV1, branch_selector_key, global_selector_key,
};
use super::object::{OBJECT_SPACE, ObjectId};
use super::state::{
    StateCellRef, StateKeyRef, StateValueRef, UNTRACKED_ROW_SPACE, UntrackedValueRef,
    encode_state_key, encode_state_value, encode_untracked_key, encode_untracked_value,
};
use super::tree::{
    ImmutableObjectSet, build_change_catalog, build_commit_catalog, build_retention_tree,
    build_state_tree,
};
use super::view::SELECTOR_SPACE;

const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";
const LIX_ID_KEY: &str = "lix_id";
const WORKSPACE_BRANCH_KEY: &str = "lix_workspace_branch_id";

struct SeedRow {
    key: Vec<u8>,
    value: Vec<u8>,
    change_id: ChangelogChangeId,
    schema_key: String,
    entity_pk: EntityPk,
    file_id: Option<String>,
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
                       entity_pk: EntityPk,
                       snapshot: serde_json::Value|
     -> Result<(), LixError> {
        let change_uuid = functions.call_uuid_v7();
        let change_id = ChangelogChangeId::new(change_uuid);
        let snapshot_text = snapshot.to_string();
        let snapshot = JsonSlot::Inline(snapshot_text.clone().into());
        let key = encode_state_key(StateKeyRef {
            schema_key,
            file_id,
            entity_pk: &entity_pk,
        });
        let value = encode_state_value(StateValueRef {
            change_id,
            commit_id: initial_commit,
            created_at: timestamp,
            updated_at: timestamp,
            cell: StateCellRef::Value(&snapshot_text),
            metadata: None,
            origin_key: None,
            blob_manifest_object_ids: &[],
        })?;
        rows.push(SeedRow {
            key,
            value,
            change_id,
            schema_key: schema_key.to_owned(),
            entity_pk,
            file_id: file_id.map(str::to_owned),
            snapshot,
            metadata: JsonSlot::None,
        });
        Ok(())
    };

    for schema in seed_schema_definitions() {
        let schema_key = schema_key_from_definition(schema)?.schema_key;
        add_row(
            "lix_registered_schema",
            None,
            registered_schema_entity_pk(&schema_key)?,
            json!({ "value": schema }),
        )?;
    }
    add_row(
        KEY_VALUE_SCHEMA_KEY,
        None,
        EntityPk::single(LIX_ID_KEY),
        json!({ "key": LIX_ID_KEY, "value": lix_id.to_string() }),
    )?;
    add_row(
        KEY_VALUE_SCHEMA_KEY,
        None,
        EntityPk::single(WORKSPACE_BRANCH_KEY),
        json!({ "key": WORKSPACE_BRANCH_KEY, "value": main_branch.to_string() }),
    )?;
    add_row(
        "lix_branch_descriptor",
        None,
        EntityPk::uuid_from_canonical(GLOBAL_BRANCH_ID)
            .map_err(|error| LixError::new(LixError::CODE_INTERNAL_ERROR, error.to_string()))?,
        json!({ "id": GLOBAL_BRANCH_ID, "name": "global", "hidden": true }),
    )?;
    add_row(
        "lix_branch_descriptor",
        None,
        EntityPk::uuid_from_canonical(&main_branch.to_string())
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
            EntityPk::uuid_from_canonical(id)
                .map_err(|error| LixError::new(LixError::CODE_INTERNAL_ERROR, error.to_string()))?,
            json!({ "id": id, "name": name, "kind": kind, "status": "active" }),
        )?;
    }
    add_row(
        "lix_checkpoint_marker",
        None,
        EntityPk::uuid_from_canonical(&main_branch.to_string())
            .map_err(|error| LixError::new(LixError::CODE_INTERNAL_ERROR, error.to_string()))?,
        json!({ "branch_id": main_branch.to_string() }),
    )?;

    rows.sort_by(|left, right| left.key.cmp(&right.key));
    let global_entries = rows
        .iter()
        .map(|row| (row.key.clone(), row.value.clone()))
        .collect::<Vec<_>>();
    let global_state = build_state_tree(&global_entries).map_err(LixError::from)?;
    // Seed the first workspace branch with the same authenticated tracked
    // rows.  Global rows remain available to the global view, while the
    // branch-local copy gives ordinary branch-scoped catalog queries their
    // exact branch identity instead of forcing them through a global owner.
    let local_state = build_state_tree(&global_entries).map_err(LixError::from)?;
    let retention = build_retention_tree(&[]).map_err(LixError::from)?;

    let mut objects = ImmutableObjectSet::default();
    objects
        .extend(global_state.objects)
        .map_err(LixError::from)?;
    objects
        .extend(local_state.objects)
        .map_err(LixError::from)?;
    objects.extend(retention.objects).map_err(LixError::from)?;

    let mut semantic_members = Vec::with_capacity(rows.len());
    let mut changes = Vec::with_capacity(rows.len() + 2);
    for row in &rows {
        let payload = crate::changelog::encode_forktree_change_payload(&ChangeRecord {
            format_version: 2,
            change_id: row.change_id,
            account_id: crate::SYSTEM_ACCOUNT_ID.to_owned(),
            schema_key: row.schema_key.clone(),
            entity_pk: row.entity_pk.clone(),
            file_id: row.file_id.clone(),
            snapshot: row.snapshot.clone(),
            metadata: row.metadata.clone(),
            created_at: timestamp,
            origin_key: None,
        })?;
        let change = ChangeObjectV1::Semantic {
            change_id: super::model::ChangeId::from_bytes(*row.change_id.as_uuid().as_bytes()),
            payload,
        };
        let (object_id, bytes) = change.encode().map_err(LixError::from)?;
        objects.insert(object_id, bytes).map_err(LixError::from)?;
        semantic_members.push(CommitMemberV1::introduced(object_id));
        changes.push((row.change_id, object_id));
    }

    let commit = CommitObjectV1 {
        commit_id: model_commit,
        generation: 1,
        parent_commit_object_ids: Vec::new(),
        members: semantic_members,
        global_state_root: global_state.root.object_id,
        local_state_root: local_state.root.object_id,
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
    let (commit_object_id, commit_bytes) = commit.encode().map_err(LixError::from)?;
    objects
        .insert(commit_object_id, commit_bytes)
        .map_err(LixError::from)?;
    let global_ref = branch_ref_change(&mut functions, global_branch, commit_object_id, None)?;
    let main_ref = branch_ref_change(&mut functions, main_branch_id, commit_object_id, None)?;
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
        .map(|(ordinal, (change_id, object_id))| {
            (
                super::model::ChangeId::from_bytes(*change_id.as_uuid().as_bytes()),
                ChangeCatalogEntry {
                    change_object_id: *object_id,
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
                    change_object_id: *object_id,
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
        retention_policy_root: retention.root.object_id,
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
    writes.put(
        SELECTOR_SPACE,
        global_selector_key().to_vec(),
        GlobalSelectorV1 {
            repository_root: repository_id,
            epoch: 1,
            selector_generation: 1,
        }
        .encode()
        .map_err(LixError::from)?
        .to_vec(),
    );
    for (branch_id, snapshot_id) in [
        (global_branch, global_snapshot_id),
        (main_branch_id, main_snapshot_id),
    ] {
        writes.put(
            SELECTOR_SPACE,
            branch_selector_key(branch_id).to_vec(),
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
    let workspace_key = encode_untracked_key(
        global_branch,
        StateKeyRef {
            schema_key: KEY_VALUE_SCHEMA_KEY,
            file_id: None,
            entity_pk: &EntityPk::single(WORKSPACE_BRANCH_KEY),
        },
    );
    let workspace_branch_snapshot = json!({ "value": main_branch.to_string() }).to_string();
    let workspace_value = encode_untracked_value(UntrackedValueRef {
        created_at: timestamp,
        updated_at: timestamp,
        cell: StateCellRef::Value(&workspace_branch_snapshot),
        metadata: None,
        origin_key: None,
        blob_manifest_object_ids: &[],
    })?;
    writes.put(UNTRACKED_ROW_SPACE, workspace_key, workspace_value);
    crate::init::stage_repository_protocol(&mut writes);
    storage
        .commit_write_set(
            writes,
            StorageWriteOptions {
                preconditions: vec![crate::storage_adapter::StoragePrecondition::KeyAbsent {
                    space: crate::init::REPOSITORY_PROTOCOL_SPACE,
                    key: crate::storage_adapter::StorageKey(Bytes::from_static(
                        crate::init::REPOSITORY_PROTOCOL_KEY,
                    )),
                }],
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
) -> Result<(super::model::ChangeId, ObjectId, Bytes), LixError> {
    let change_uuid = functions.call_uuid_v7();
    let change_id = super::model::ChangeId::from_bytes(*change_uuid.as_bytes());
    let change = ChangeObjectV1::BranchRef {
        change_id,
        branch_id,
        before_semantic_head_commit_object_id: None,
        after_semantic_head_commit_object_id: Some(commit_object_id),
        previous_ref_change_object_id: previous_ref,
        payload: Vec::new(),
    };
    let (object_id, bytes) = change.encode().map_err(LixError::from)?;
    Ok((change_id, object_id, bytes))
}
