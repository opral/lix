#![allow(clippy::clone_on_copy, clippy::unnecessary_wraps)]

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::branch::{BRANCH_DESCRIPTOR_SCHEMA_KEY, BRANCH_REF_SCHEMA_KEY};
use crate::changelog::{
    ChangeId, ChangeRecord, ChangelogAppend, ChangelogContext, ChangelogWriter, CommitChangeRefSet,
    CommitId, CommitRecord,
};
use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::functions::FunctionProviderHandle;
use crate::json_store::{JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef};
use crate::live_state::{LiveStateIndexContext, LiveStateIndexDeltaRef};
use crate::schema::{
    registered_schema_entity_pk, schema_key_from_definition, seed_schema_definitions,
};
use crate::storage::SharedStorageRead;
use crate::storage::StorageBackend;
use crate::storage::{StorageContext, StorageWriteSet};
use crate::tracked_state::{TrackedStateContext, TrackedStateDeltaRef};
use serde_json::json;

const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";
const LIX_ID_KEY: &str = "lix_id";
const WORKSPACE_BRANCH_KEY: &str = "lix_workspace_branch_id";
const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";

/// Pure seed plan for initializing an engine repository.
///
/// Tracked bootstrap facts go to the changelog. Moving refs such as
/// `lix_branch_ref` are seeded as untracked local state so repository heads
/// can advance without becoming commit members.
pub(crate) struct InitSeedPlan {
    commit: InitSeedCommit,
    changes: Vec<InitSeedChange>,
    untracked_rows: Vec<InitSeedLiveRow>,
    pub(crate) receipt: InitReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InitSeedCommit {
    id: CommitId,
    change_id: ChangeId,
    parent_ids: Vec<CommitId>,
    author_account_ids: Vec<String>,
    created_at: LixTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InitSeedChange {
    id: ChangeId,
    entity_pk: EntityPk,
    schema_key: String,
    snapshot_content: String,
    created_at: LixTimestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InitSeedLiveRow {
    id: ChangeId,
    entity_pk: EntityPk,
    schema_key: String,
    snapshot_content: String,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    global: bool,
    branch_id: String,
}

/// Values generated while planning the initial repository seed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitReceipt {
    pub lix_id: String,
    pub global_branch_id: String,
    pub main_branch_id: String,
    pub initial_commit_id: String,
}

/// Builds the canonical bootstrap changes for a new engine repository.
///
/// The initial commit tracks durable content rows. Branch refs are moving
/// pointers and therefore live in untracked local state instead of the commit.
pub(crate) fn plan_init_seed(functions: FunctionProviderHandle) -> Result<InitSeedPlan, LixError> {
    let main_branch_id = functions.call_uuid_v7().to_string();
    let lix_id = functions.call_uuid_v7().to_string();
    let initial_commit_id = CommitId::from(functions.call_uuid_v7());
    let timestamp = functions.call_timestamp();

    let mut registered_schema_changes = Vec::new();
    for schema in seed_schema_definitions() {
        let key = schema_key_from_definition(schema)?;
        registered_schema_changes.push(canonical_change(
            functions.call_uuid_v7(),
            registered_schema_entity_pk(&key.schema_key)?,
            REGISTERED_SCHEMA_KEY,
            registered_schema_snapshot(schema)?,
            timestamp,
        ));
    }

    let global_branch_descriptor_change = canonical_change(
        functions.call_uuid_v7(),
        EntityPk::single(GLOBAL_BRANCH_ID),
        BRANCH_DESCRIPTOR_SCHEMA_KEY,
        branch_descriptor_snapshot(GLOBAL_BRANCH_ID, "global", true)?,
        timestamp,
    );
    let main_branch_descriptor_change = canonical_change(
        functions.call_uuid_v7(),
        EntityPk::single(&main_branch_id),
        BRANCH_DESCRIPTOR_SCHEMA_KEY,
        branch_descriptor_snapshot(&main_branch_id, "main", false)?,
        timestamp,
    );
    let kv_lix_id_change = canonical_change(
        functions.call_uuid_v7(),
        EntityPk::single(LIX_ID_KEY),
        KEY_VALUE_SCHEMA_KEY,
        key_value_snapshot(LIX_ID_KEY, &lix_id)?,
        timestamp,
    );

    let initial_commit = InitSeedCommit {
        id: initial_commit_id,
        change_id: ChangeId::from(functions.call_uuid_v7()),
        parent_ids: Vec::new(),
        author_account_ids: Vec::new(),
        created_at: timestamp,
    };
    let global_branch_ref_row = untracked_row(
        functions.call_uuid_v7(),
        EntityPk::single(GLOBAL_BRANCH_ID),
        BRANCH_REF_SCHEMA_KEY,
        branch_ref_snapshot(GLOBAL_BRANCH_ID, &initial_commit_id)?,
        timestamp,
    );
    let main_branch_ref_row = untracked_row(
        functions.call_uuid_v7(),
        EntityPk::single(&main_branch_id),
        BRANCH_REF_SCHEMA_KEY,
        branch_ref_snapshot(&main_branch_id, &initial_commit_id)?,
        timestamp,
    );
    let workspace_branch_row = untracked_row(
        functions.call_uuid_v7(),
        EntityPk::single(WORKSPACE_BRANCH_KEY),
        KEY_VALUE_SCHEMA_KEY,
        key_value_snapshot(WORKSPACE_BRANCH_KEY, &main_branch_id)?,
        timestamp,
    );

    Ok(InitSeedPlan {
        commit: initial_commit,
        changes: registered_schema_changes
            .into_iter()
            .chain([
                global_branch_descriptor_change,
                main_branch_descriptor_change,
                kv_lix_id_change,
            ])
            .collect(),
        untracked_rows: vec![
            global_branch_ref_row,
            main_branch_ref_row,
            workspace_branch_row,
        ],
        receipt: InitReceipt {
            lix_id,
            global_branch_id: GLOBAL_BRANCH_ID.to_string(),
            main_branch_id,
            initial_commit_id: initial_commit_id.to_string(),
        },
    })
}

/// Initializes an empty engine repository in one backend transaction.
///
/// The pure seed planner decides which bootstrap facts exist. This function is
/// only responsible for durably writing those facts to their owning stores:
/// changelog for tracked changes, and live_state for the serving state
/// plus untracked moving refs.
pub(crate) async fn initialize<B>(
    storage: StorageContext<B>,
    tracked_state: &TrackedStateContext,
    live_index: &LiveStateIndexContext,
) -> Result<InitReceipt, LixError>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
{
    let functions = FunctionProviderHandle::system();
    let plan = plan_init_seed(functions)?;
    let receipt = plan.receipt.clone();

    let mut read = SharedStorageRead::new(
        storage
            .begin_read(crate::storage::StorageReadOptions::default())
            .await?,
    );
    let mut writes = StorageWriteSet::new();
    let authored_changes = plan
        .changes
        .iter()
        .map(seed_change_to_change_record)
        .collect::<Vec<_>>();
    let untracked_changes = plan
        .untracked_rows
        .iter()
        .map(seed_untracked_change_to_change_record)
        .collect::<Vec<_>>();

    stage_init_json_payloads(&mut writes, &plan)?;
    stage_init_changelog_commit(
        &mut read,
        &mut writes,
        &plan,
        authored_changes
            .iter()
            .chain(&untracked_changes)
            .cloned()
            .collect(),
    )
    .await?;

    {
        let commit_row_change = seed_commit_row_change_record(&plan.commit)?;
        let mut deltas = authored_changes
            .iter()
            .map(|change| TrackedStateDeltaRef {
                schema_key: &change.schema_key,
                file_id: change.file_id.as_deref(),
                entity_pk: &change.entity_pk,
                change_id: change.change_id,
                commit_id: plan.commit.id,
                deleted: change.snapshot.is_none(),
                created_at: change.created_at,
                updated_at: change.created_at,
            })
            .collect::<Vec<_>>();
        deltas.push(TrackedStateDeltaRef {
            schema_key: &commit_row_change.schema_key,
            file_id: commit_row_change.file_id.as_deref(),
            entity_pk: &commit_row_change.entity_pk,
            change_id: commit_row_change.change_id,
            commit_id: plan.commit.id,
            deleted: commit_row_change.snapshot.is_none(),
            created_at: commit_row_change.created_at,
            updated_at: commit_row_change.created_at,
        });
        let tracked_report = tracked_state
            .writer(&read, &mut writes)
            .stage_commit_root(&receipt.initial_commit_id, None, deltas)
            .await?;

        let mut index_writer = live_index.writer(&read, &mut writes);
        index_writer
            .stage_branch_rows(
                GLOBAL_BRANCH_ID,
                authored_changes
                    .iter()
                    .map(|change| LiveStateIndexDeltaRef {
                        schema_key: &change.schema_key,
                        file_id: change.file_id.as_deref(),
                        entity_pk: &change.entity_pk,
                        change_id: change.change_id,
                        commit_id: Some(plan.commit.id),
                        deleted: change.snapshot.is_none(),
                        created_at: change.created_at,
                        updated_at: change.created_at,
                    })
                    .chain(
                        plan.untracked_rows
                            .iter()
                            .map(|row| LiveStateIndexDeltaRef {
                                schema_key: &row.schema_key,
                                file_id: None,
                                entity_pk: &row.entity_pk,
                                change_id: row.id,
                                commit_id: None,
                                deleted: false,
                                created_at: row.created_at,
                                updated_at: row.updated_at,
                            }),
                    ),
            )
            .await?;
        index_writer
            .stage_branch_root_from_existing(&receipt.main_branch_id, &tracked_report.root_id)?;
    }

    storage
        .commit_write_set(writes, crate::storage::StorageWriteOptions::default())
        .await?;
    Ok(receipt)
}

fn seed_change_to_change_record(change: &InitSeedChange) -> ChangeRecord {
    ChangeRecord {
        format_version: 1,
        change_id: change.id,
        entity_pk: change.entity_pk.clone(),
        schema_key: change.schema_key.clone(),
        file_id: None,
        snapshot: crate::json_store::JsonSlot::from_json(&change.snapshot_content),
        metadata: crate::json_store::JsonSlot::None,
        created_at: change.created_at,
        origin_key: None,
    }
}

fn seed_untracked_change_to_change_record(row: &InitSeedLiveRow) -> ChangeRecord {
    ChangeRecord {
        format_version: 2,
        change_id: row.id,
        entity_pk: row.entity_pk.clone(),
        schema_key: row.schema_key.clone(),
        file_id: None,
        snapshot: crate::json_store::JsonSlot::from_json(&row.snapshot_content),
        metadata: crate::json_store::JsonSlot::None,
        created_at: row.updated_at,
        origin_key: None,
    }
}

fn seed_commit_row_change_record(commit: &InitSeedCommit) -> Result<ChangeRecord, LixError> {
    let snapshot_content = commit_row_snapshot_content(&commit.id.to_string())?;
    Ok(ChangeRecord {
        format_version: 1,
        change_id: commit.change_id,
        entity_pk: EntityPk::single(commit.id),
        schema_key: "lix_commit".to_string(),
        file_id: None,
        snapshot: crate::json_store::JsonSlot::from_json(&snapshot_content),
        metadata: crate::json_store::JsonSlot::None,
        created_at: commit.created_at,
        origin_key: None,
    })
}

fn stage_init_json_payloads(
    writes: &mut StorageWriteSet,
    plan: &InitSeedPlan,
) -> Result<(), LixError> {
    // Only payloads above the inline threshold need store rows; inline
    // payloads live in their change records, and the commit-row snapshot
    // is derived from the key at read time.
    JsonStoreContext::new().writer().stage_batch(
        writes,
        JsonWritePlacementRef::OutOfBand,
        plan.changes
            .iter()
            .map(|change| change.snapshot_content.as_str())
            .chain(
                plan.untracked_rows
                    .iter()
                    .map(|row| row.snapshot_content.as_str()),
            )
            .filter(|snapshot| snapshot.len() > crate::json_store::JSON_INLINE_MAX_BYTES)
            .map(NormalizedJsonRef::new),
    )?;
    Ok(())
}

async fn stage_init_changelog_commit(
    read: &mut impl crate::storage::StorageRead,
    writes: &mut StorageWriteSet,
    plan: &InitSeedPlan,
    changes: Vec<ChangeRecord>,
) -> Result<(), LixError> {
    let commit = CommitRecord {
        format_version: 1,
        commit_id: plan.commit.id,
        parent_commit_ids: plan.commit.parent_ids.clone(),
        change_id: plan.commit.change_id,
        author_account_ids: plan.commit.author_account_ids.clone(),
        created_at: plan.commit.created_at,
    };
    let commit_change_refs = CommitChangeRefSet {
        commit_id: plan.commit.id,
        entries: plan.changes.iter().map(|change| change.id).collect(),
    };
    let mut writer = ChangelogContext::new().writer(read, writes);
    writer
        .stage_append(ChangelogAppend {
            commits: vec![commit],
            changes,
            commit_change_refs: vec![commit_change_refs],
        })
        .await
}

fn commit_row_snapshot_content(commit_id: &str) -> Result<String, LixError> {
    crate::changelog::commit_row_snapshot_json(commit_id)
}

fn untracked_row(
    id: uuid::Uuid,
    entity_pk: EntityPk,
    schema_key: &str,
    snapshot_content: String,
    timestamp: LixTimestamp,
) -> InitSeedLiveRow {
    InitSeedLiveRow {
        id: ChangeId::from(id),
        entity_pk,
        schema_key: schema_key.to_string(),
        snapshot_content,
        created_at: timestamp,
        updated_at: timestamp,
        global: true,
        branch_id: GLOBAL_BRANCH_ID.to_string(),
    }
}

fn canonical_change(
    id: uuid::Uuid,
    entity_pk: EntityPk,
    schema_key: &str,
    snapshot_content: String,
    created_at: LixTimestamp,
) -> InitSeedChange {
    InitSeedChange {
        id: ChangeId::from(id),
        entity_pk,
        schema_key: schema_key.to_string(),
        snapshot_content,
        created_at,
    }
}

fn branch_descriptor_snapshot(id: &str, name: &str, hidden: bool) -> Result<String, LixError> {
    encode_snapshot(json!({
        "id": id,
        "name": name,
        "hidden": hidden,
    }))
}

fn key_value_snapshot(key: &str, value: &str) -> Result<String, LixError> {
    encode_snapshot(json!({
        "key": key,
        "value": value,
    }))
}

fn registered_schema_snapshot(schema: &serde_json::Value) -> Result<String, LixError> {
    encode_snapshot(json!({
        "value": schema,
    }))
}

fn branch_ref_snapshot(id: &str, commit_id: &CommitId) -> Result<String, LixError> {
    encode_snapshot(json!({
        "id": id,
        "commit_id": commit_id.to_string(),
    }))
}

fn encode_snapshot(value: serde_json::Value) -> Result<String, LixError> {
    serde_json::to_string(&value).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("engine init seed snapshot serialization failed: {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use serde_json::Value as JsonValue;

    use super::*;
    use crate::changelog::ChangelogReader;
    use crate::functions::FunctionProvider;
    use crate::storage::InMemoryStorageBackend;
    use crate::storage::StorageContext;
    use crate::tracked_state::TrackedStateContext;

    #[test]
    fn plan_init_seed_returns_tracked_changes_and_untracked_workspace_state() {
        let plan = plan_init_seed(test_functions()).expect("init seed should plan");

        assert_eq!(plan.changes.len(), seed_schema_definitions().len() + 3);
        assert_eq!(plan.untracked_rows.len(), 3);
        assert_eq!(plan.receipt.global_branch_id, GLOBAL_BRANCH_ID);
        assert_eq!(plan.receipt.main_branch_id, test_uuid(1));
        assert_eq!(plan.receipt.lix_id, test_uuid(2));
        assert_eq!(plan.receipt.initial_commit_id, test_uuid(3));
    }

    #[test]
    fn plan_init_seed_commit_header_tracks_schema_registrations_descriptor_and_lix_id_changes() {
        let plan = plan_init_seed(test_functions()).expect("init seed should plan");

        assert_eq!(plan.commit.id, plan.receipt.initial_commit_id);
        assert_eq!(
            plan.commit.change_id.to_string(),
            test_uuid(seed_schema_definitions().len() + 7)
        );
        assert!(plan.commit.parent_ids.is_empty());
        assert!(plan.commit.author_account_ids.is_empty());
        assert_eq!(
            plan.commit.created_at.to_string(),
            "2026-01-01T00:00:00.001Z"
        );

        let change_ids = plan
            .changes
            .iter()
            .map(|change| change.id.to_string())
            .collect::<Vec<_>>();
        assert_eq!(change_ids.len(), seed_schema_definitions().len() + 3);
        let first_seed_change_id = test_uuid(4);
        assert!(change_ids.contains(&first_seed_change_id));
        assert!(!change_ids.contains(&plan.commit.change_id.to_string()));

        let registered_schema_change_ids = plan
            .changes
            .iter()
            .filter(|change| change.schema_key == REGISTERED_SCHEMA_KEY)
            .map(|change| change.id.to_string())
            .collect::<Vec<_>>();
        for change_id in registered_schema_change_ids {
            assert!(change_ids.contains(&change_id));
        }
    }

    #[test]
    fn plan_init_seed_registers_seed_schemas_as_initial_commit_rows() {
        let plan = plan_init_seed(test_functions()).expect("init seed should plan");
        let registered_schema_changes = plan
            .changes
            .iter()
            .filter(|change| change.schema_key == REGISTERED_SCHEMA_KEY)
            .collect::<Vec<_>>();

        assert_eq!(
            registered_schema_changes.len(),
            seed_schema_definitions().len()
        );
        assert!(registered_schema_changes.iter().any(|change| {
            snapshot(change)
                .pointer("/value/x-lix-key")
                .and_then(JsonValue::as_str)
                == Some(REGISTERED_SCHEMA_KEY)
        }));
        assert!(registered_schema_changes.iter().any(|change| {
            snapshot(change)
                .pointer("/value/x-lix-key")
                .and_then(JsonValue::as_str)
                == Some(KEY_VALUE_SCHEMA_KEY)
        }));
    }

    #[test]
    fn plan_init_seed_branch_refs_point_to_initial_commit() {
        let plan = plan_init_seed(test_functions()).expect("init seed should plan");
        let branch_refs = plan
            .untracked_rows
            .iter()
            .filter(|row| row.schema_key == BRANCH_REF_SCHEMA_KEY)
            .collect::<Vec<_>>();

        assert_eq!(branch_refs.len(), 2);
        assert!(
            plan.changes
                .iter()
                .all(|change| change.schema_key != BRANCH_REF_SCHEMA_KEY)
        );
        for row in branch_refs {
            assert_eq!(row.schema_key, BRANCH_REF_SCHEMA_KEY);
            assert_eq!(row.branch_id, GLOBAL_BRANCH_ID);
            let snapshot = untracked_snapshot(row);
            assert_eq!(
                snapshot.get("commit_id").and_then(JsonValue::as_str),
                Some(plan.receipt.initial_commit_id.as_str())
            );
        }
    }

    #[test]
    fn plan_init_seed_workspace_branch_points_to_main_branch() {
        let plan = plan_init_seed(test_functions()).expect("init seed should plan");
        let workspace_row = plan
            .untracked_rows
            .iter()
            .find(|row| {
                row.schema_key == KEY_VALUE_SCHEMA_KEY
                    && row.entity_pk == EntityPk::single(WORKSPACE_BRANCH_KEY)
            })
            .expect("workspace branch row should exist");

        assert_eq!(workspace_row.branch_id, GLOBAL_BRANCH_ID);
        assert!(workspace_row.global);
        let snapshot = untracked_snapshot(workspace_row);
        assert_eq!(
            snapshot.get("key").and_then(JsonValue::as_str),
            Some(WORKSPACE_BRANCH_KEY)
        );
        assert_eq!(
            snapshot.get("value").and_then(JsonValue::as_str),
            Some(plan.receipt.main_branch_id.as_str())
        );
    }

    #[tokio::test]
    async fn initialize_writes_initial_commit_through_changelog() {
        let backend = InMemoryStorageBackend::new();
        let storage = StorageContext::new(backend);
        let tracked_state = TrackedStateContext::new();
        let live_index = LiveStateIndexContext::new();

        let receipt = initialize(storage.clone(), &tracked_state, &live_index)
            .await
            .expect("engine should initialize");
        let mut reader = ChangelogContext::new().reader(
            storage
                .begin_read(crate::storage::StorageReadOptions::default())
                .await
                .expect("read should open"),
        );
        let commits = reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &[CommitId::for_test_label(&receipt.initial_commit_id)],
                projection: crate::changelog::CommitProjection::Full,
            })
            .await
            .expect("commit should load");
        let Some(crate::changelog::CommitLoadEntry::Full {
            record,
            change_ref_chunks,
        }) = commits.entries.into_iter().next().flatten()
        else {
            panic!("initial commit should exist");
        };

        assert_eq!(record.commit_id, receipt.initial_commit_id);
        let commit_change_id = record.change_id.clone();
        let change_refs = change_ref_chunks
            .iter()
            .flat_map(|chunk| chunk.entries.iter())
            .collect::<Vec<_>>();
        assert_eq!(change_refs.len(), seed_schema_definitions().len() + 3);
        assert!(
            !change_refs
                .iter()
                .any(|change_id| **change_id == record.change_id),
            "initial commit row is derived from changelog.commit, not stored in commit refs"
        );

        let sampled_change_id = *change_refs
            .first()
            .copied()
            .expect("initial commit should reference at least one change");
        let changes = reader
            .load_changes(crate::changelog::ChangeLoadRequest {
                change_ids: &[sampled_change_id],
            })
            .await
            .expect("change index should load");
        assert!(matches!(
            changes.entries.as_slice(),
            [Some(change)] if change.change_id == sampled_change_id
        ));
        let missing_derivable = reader
            .load_changes(crate::changelog::ChangeLoadRequest {
                change_ids: &[commit_change_id],
            })
            .await
            .expect("derivable change lookup should load");
        assert!(matches!(missing_derivable.entries.as_slice(), [None]));
        {
            let read = storage
                .begin_read(crate::storage::StorageReadOptions::default())
                .await
                .expect("read should open");
            let mut writes = storage.new_write_set();
            tracked_state
                .root_rebuilder(&read, &mut writes)
                .rebuild_commit_root_at(&receipt.initial_commit_id)
                .await
                .expect("initial commit root should rebuild from changelog refs");
            drop(read);
            storage
                .commit_write_set(writes, crate::storage::StorageWriteOptions::default())
                .await
                .expect("rebuilt initial commit root should commit");
        }
        let mut tracked_reader = tracked_state.reader(
            storage
                .begin_read(crate::storage::StorageReadOptions::default())
                .await
                .expect("read should open"),
        );
        let rows = tracked_reader
            .scan_rows_at_commit(
                &receipt.initial_commit_id,
                &crate::tracked_state::TrackedStateScanRequest {
                    filter: crate::tracked_state::TrackedStateFilter {
                        schema_keys: vec!["lix_commit".to_string()],
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .expect("tracked initial root should scan");
        assert!(
            rows.iter().any(|row| row.change_id == commit_change_id),
            "initial commit root should surface its lix_commit row"
        );
    }

    fn snapshot(change: &InitSeedChange) -> JsonValue {
        serde_json::from_str(&change.snapshot_content).expect("snapshot should be JSON")
    }

    fn untracked_snapshot(row: &InitSeedLiveRow) -> JsonValue {
        serde_json::from_str(&row.snapshot_content).expect("snapshot should be JSON")
    }

    #[expect(trivial_casts)]
    fn test_functions() -> FunctionProviderHandle {
        FunctionProviderHandle::shared(
            Box::new(TestFunctionProvider::default()) as Box<dyn FunctionProvider + Send>
        )
    }

    #[derive(Default)]
    struct TestFunctionProvider {
        uuid_count: usize,
        timestamp_count: usize,
    }

    impl FunctionProvider for TestFunctionProvider {
        fn uuid_v7(&mut self) -> uuid::Uuid {
            self.uuid_count += 1;
            test_uuid_value(self.uuid_count)
        }

        fn timestamp(&mut self) -> LixTimestamp {
            self.timestamp_count += 1;
            LixTimestamp::expect_parse(
                "timestamp",
                &format!("2026-01-01T00:00:00.{:03}Z", self.timestamp_count),
            )
        }
    }

    fn test_uuid(index: usize) -> String {
        test_uuid_value(index).to_string()
    }

    fn test_uuid_value(index: usize) -> uuid::Uuid {
        uuid::Uuid::from_u128(0x0192_0000_0000_7000_8000_0000_0000_0000 + index as u128)
    }
}
