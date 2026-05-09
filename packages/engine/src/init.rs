use crate::commit_store::{Change, CommitDraftRef, CommitStoreContext};
use crate::entity_identity::EntityIdentity;
use crate::functions::{
    FunctionProvider, FunctionProviderHandle, SharedFunctionProvider, SystemFunctionProvider,
};
use crate::json_store::{JsonRef, JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef};
use crate::schema::{
    registered_schema_entity_id, schema_key_from_definition, seed_schema_definitions,
};
use crate::storage::{StorageContext, StorageWriteSet};
use crate::tracked_state::{TrackedStateContext, TrackedStateDeltaRef};
use crate::untracked_state::{UntrackedStateContext, UntrackedStateRow};
use crate::version::{VERSION_DESCRIPTOR_SCHEMA_KEY, VERSION_REF_SCHEMA_KEY};
use crate::LixError;
use crate::GLOBAL_VERSION_ID;
use serde_json::json;
#[cfg(test)]
use std::sync::Arc;

const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";
const LIX_ID_KEY: &str = "lix_id";
const WORKSPACE_VERSION_KEY: &str = "lix_workspace_version_id";
const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";

/// Pure seed plan for initializing an engine repository.
///
/// Tracked bootstrap facts go to the commit store. Moving refs such as
/// `lix_version_ref` are seeded as untracked local state so repository heads
/// can advance without becoming commit members.
pub(crate) struct InitSeedPlan {
    commit: InitSeedCommit,
    changes: Vec<InitSeedChange>,
    untracked_rows: Vec<InitSeedLiveRow>,
    pub(crate) receipt: InitReceipt,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InitSeedCommit {
    id: String,
    change_id: String,
    parent_ids: Vec<String>,
    author_account_ids: Vec<String>,
    created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InitSeedChange {
    id: String,
    entity_id: EntityIdentity,
    schema_key: String,
    snapshot_content: String,
    created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InitSeedLiveRow {
    entity_id: EntityIdentity,
    schema_key: String,
    snapshot_content: String,
    created_at: String,
    updated_at: String,
    global: bool,
    version_id: String,
}

/// Values generated while planning the initial repository seed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitReceipt {
    pub lix_id: String,
    pub global_version_id: String,
    pub main_version_id: String,
    pub initial_commit_id: String,
}

/// Builds the canonical bootstrap changes for a new engine repository.
///
/// The initial commit tracks durable content rows. Version refs are moving
/// pointers and therefore live in untracked local state instead of the commit.
pub(crate) fn plan_init_seed(functions: FunctionProviderHandle) -> Result<InitSeedPlan, LixError> {
    let main_version_id = functions.call_uuid_v7();
    let lix_id = functions.call_uuid_v7();
    let initial_commit_id = functions.call_uuid_v7();
    let timestamp = functions.call_timestamp();

    let mut registered_schema_changes = Vec::new();
    for schema in seed_schema_definitions() {
        let key = schema_key_from_definition(schema)?;
        registered_schema_changes.push(canonical_change(
            functions.call_uuid_v7(),
            registered_schema_entity_id(&key.schema_key)?,
            REGISTERED_SCHEMA_KEY,
            registered_schema_snapshot(schema)?,
            &timestamp,
        ));
    }

    let global_version_descriptor_change = canonical_change(
        GLOBAL_VERSION_ID.to_string(),
        EntityIdentity::single(GLOBAL_VERSION_ID),
        VERSION_DESCRIPTOR_SCHEMA_KEY,
        version_descriptor_snapshot(GLOBAL_VERSION_ID, "global", true)?,
        &timestamp,
    );
    let main_version_descriptor_change = canonical_change(
        functions.call_uuid_v7(),
        EntityIdentity::single(&main_version_id),
        VERSION_DESCRIPTOR_SCHEMA_KEY,
        version_descriptor_snapshot(&main_version_id, "main", false)?,
        &timestamp,
    );
    let kv_lix_id_change = canonical_change(
        functions.call_uuid_v7(),
        EntityIdentity::single(LIX_ID_KEY),
        KEY_VALUE_SCHEMA_KEY,
        key_value_snapshot(LIX_ID_KEY, &lix_id)?,
        &timestamp,
    );

    let initial_commit = InitSeedCommit {
        id: initial_commit_id.clone(),
        change_id: functions.call_uuid_v7(),
        parent_ids: Vec::new(),
        author_account_ids: Vec::new(),
        created_at: timestamp.clone(),
    };
    let global_version_ref_row = untracked_row(
        EntityIdentity::single(GLOBAL_VERSION_ID),
        VERSION_REF_SCHEMA_KEY,
        version_ref_snapshot(GLOBAL_VERSION_ID, &initial_commit_id)?,
        &timestamp,
    );
    let main_version_ref_row = untracked_row(
        EntityIdentity::single(&main_version_id),
        VERSION_REF_SCHEMA_KEY,
        version_ref_snapshot(&main_version_id, &initial_commit_id)?,
        &timestamp,
    );
    let workspace_version_row = untracked_row(
        EntityIdentity::single(WORKSPACE_VERSION_KEY),
        KEY_VALUE_SCHEMA_KEY,
        key_value_snapshot(WORKSPACE_VERSION_KEY, &main_version_id)?,
        &timestamp,
    );

    Ok(InitSeedPlan {
        commit: initial_commit,
        changes: registered_schema_changes
            .into_iter()
            .chain([
                global_version_descriptor_change,
                main_version_descriptor_change,
                kv_lix_id_change,
            ])
            .collect(),
        untracked_rows: vec![
            global_version_ref_row,
            main_version_ref_row,
            workspace_version_row,
        ],
        receipt: InitReceipt {
            lix_id,
            global_version_id: GLOBAL_VERSION_ID.to_string(),
            main_version_id,
            initial_commit_id,
        },
    })
}

/// Initializes an empty engine repository in one backend transaction.
///
/// The pure seed planner decides which bootstrap facts exist. This function is
/// only responsible for durably writing those facts to their owning stores:
/// commit_store for tracked changes, and live_state for the serving projection
/// plus untracked moving refs.
pub(crate) async fn initialize(
    storage: StorageContext,
    commit_store: &CommitStoreContext,
    tracked_state: &TrackedStateContext,
    untracked_state: &UntrackedStateContext,
) -> Result<InitReceipt, LixError> {
    let functions = SharedFunctionProvider::new(
        Box::new(SystemFunctionProvider) as Box<dyn FunctionProvider + Send>
    );
    let plan = plan_init_seed(functions)?;
    let receipt = plan.receipt.clone();

    let mut transaction = storage.begin_write_transaction().await?;
    let mut writes = StorageWriteSet::new();

    let authored_changes = plan
        .changes
        .iter()
        .map(seed_change_to_commit_store_change)
        .collect::<Result<Vec<_>, _>>()?;
    JsonStoreContext::new().writer().stage_batch(
        &mut writes,
        JsonWritePlacementRef::CommitPack {
            commit_id: &plan.commit.id,
            pack_id: 0,
        },
        plan.changes.iter().map(|change| NormalizedJsonRef {
            normalized: change.snapshot_content.as_str(),
        }),
    )?;

    let staged_commit = {
        let commit = CommitDraftRef {
            id: &plan.commit.id,
            change_id: &plan.commit.change_id,
            parent_ids: &plan.commit.parent_ids,
            author_account_ids: &plan.commit.author_account_ids,
            created_at: &plan.commit.created_at,
        };
        let mut writer = commit_store.writer(transaction.as_mut(), &mut writes);
        writer
            .stage_commit_draft(
                commit,
                authored_changes.iter().map(Change::as_ref).collect(),
                Vec::new(),
            )
            .await?
    };

    let untracked_rows = plan
        .untracked_rows
        .iter()
        .map(untracked_state_row_from_seed)
        .collect::<Result<Vec<_>, _>>()?;

    {
        untracked_state
            .writer(&mut writes)
            .stage_rows(untracked_rows.iter().map(|row| row.as_ref()))?;
        let deltas = authored_changes
            .iter()
            .zip(&staged_commit.authored_locators)
            .map(|(change, locator)| TrackedStateDeltaRef {
                change: change.as_ref(),
                locator: locator.as_ref(),
                created_at: &change.created_at,
                updated_at: &change.created_at,
            })
            .collect::<Vec<_>>();
        let mut writer = tracked_state.writer(transaction.as_mut(), &mut writes);
        writer
            .stage_delta(&receipt.initial_commit_id, None, deltas)
            .await?;
    }

    writes.apply(&mut transaction.as_mut()).await?;
    transaction.commit().await?;
    Ok(receipt)
}

fn seed_change_to_commit_store_change(change: &InitSeedChange) -> Result<Change, LixError> {
    Ok(Change {
        id: change.id.clone(),
        entity_id: change.entity_id.clone(),
        schema_key: change.schema_key.clone(),
        file_id: None,
        snapshot_ref: Some(JsonRef::for_content(change.snapshot_content.as_bytes())),
        metadata_ref: None,
        created_at: change.created_at.clone(),
    })
}

fn untracked_state_row_from_seed(row: &InitSeedLiveRow) -> Result<UntrackedStateRow, LixError> {
    Ok(UntrackedStateRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: None,
        snapshot_content: Some(row.snapshot_content.clone()),
        metadata: None,
        created_at: row.created_at.clone(),
        updated_at: row.updated_at.clone(),
        global: row.global,
        version_id: row.version_id.clone(),
    })
}

fn untracked_row(
    entity_id: EntityIdentity,
    schema_key: &str,
    snapshot_content: String,
    timestamp: &str,
) -> InitSeedLiveRow {
    InitSeedLiveRow {
        entity_id,
        schema_key: schema_key.to_string(),
        snapshot_content,
        created_at: timestamp.to_string(),
        updated_at: timestamp.to_string(),
        global: true,
        version_id: GLOBAL_VERSION_ID.to_string(),
    }
}

fn canonical_change(
    id: String,
    entity_id: EntityIdentity,
    schema_key: &str,
    snapshot_content: String,
    created_at: &str,
) -> InitSeedChange {
    InitSeedChange {
        id,
        entity_id,
        schema_key: schema_key.to_string(),
        snapshot_content,
        created_at: created_at.to_string(),
    }
}

fn version_descriptor_snapshot(id: &str, name: &str, hidden: bool) -> Result<String, LixError> {
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

fn version_ref_snapshot(id: &str, commit_id: &str) -> Result<String, LixError> {
    encode_snapshot(json!({
        "id": id,
        "commit_id": commit_id,
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
    use crate::backend::{testing::UnitTestBackend, Backend};
    use crate::functions::{FunctionProvider, SharedFunctionProvider};
    use crate::storage::StorageContext;
    use crate::tracked_state::TrackedStateContext;
    use crate::untracked_state::UntrackedStateContext;

    #[test]
    fn plan_init_seed_returns_tracked_changes_and_untracked_workspace_state() {
        let plan = plan_init_seed(test_functions()).expect("init seed should plan");

        assert_eq!(plan.changes.len(), seed_schema_definitions().len() + 3);
        assert_eq!(plan.untracked_rows.len(), 3);
        assert_eq!(plan.receipt.global_version_id, GLOBAL_VERSION_ID);
        assert_eq!(plan.receipt.main_version_id, "test-uuid-1");
        assert_eq!(plan.receipt.lix_id, "test-uuid-2");
        assert_eq!(plan.receipt.initial_commit_id, "test-uuid-3");
    }

    #[test]
    fn plan_init_seed_commit_header_tracks_schema_registrations_descriptor_and_lix_id_changes() {
        let plan = plan_init_seed(test_functions()).expect("init seed should plan");

        assert_eq!(plan.commit.id, plan.receipt.initial_commit_id);
        assert_eq!(plan.commit.change_id, "test-uuid-21");
        assert!(plan.commit.parent_ids.is_empty());
        assert!(plan.commit.author_account_ids.is_empty());
        assert_eq!(plan.commit.created_at, "test-timestamp-1");

        let change_ids = plan
            .changes
            .iter()
            .map(|change| change.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(change_ids.len(), seed_schema_definitions().len() + 3);
        assert!(change_ids.contains(&"global"));
        assert!(!change_ids.contains(&plan.commit.change_id.as_str()));

        let registered_schema_change_ids = plan
            .changes
            .iter()
            .filter(|change| change.schema_key == REGISTERED_SCHEMA_KEY)
            .map(|change| change.id.as_str())
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
    fn plan_init_seed_version_refs_point_to_initial_commit() {
        let plan = plan_init_seed(test_functions()).expect("init seed should plan");
        let version_refs = plan
            .untracked_rows
            .iter()
            .filter(|row| row.schema_key == VERSION_REF_SCHEMA_KEY)
            .collect::<Vec<_>>();

        assert_eq!(version_refs.len(), 2);
        assert!(plan
            .changes
            .iter()
            .all(|change| change.schema_key != VERSION_REF_SCHEMA_KEY));
        for row in version_refs {
            assert_eq!(row.schema_key, VERSION_REF_SCHEMA_KEY);
            assert_eq!(row.version_id, GLOBAL_VERSION_ID);
            let snapshot = untracked_snapshot(row);
            assert_eq!(
                snapshot.get("commit_id").and_then(JsonValue::as_str),
                Some(plan.receipt.initial_commit_id.as_str())
            );
        }
    }

    #[test]
    fn plan_init_seed_workspace_version_points_to_main_version() {
        let plan = plan_init_seed(test_functions()).expect("init seed should plan");
        let workspace_row = plan
            .untracked_rows
            .iter()
            .find(|row| {
                row.schema_key == KEY_VALUE_SCHEMA_KEY
                    && row.entity_id
                        == crate::entity_identity::EntityIdentity::single(WORKSPACE_VERSION_KEY)
            })
            .expect("workspace version row should exist");

        assert_eq!(workspace_row.version_id, GLOBAL_VERSION_ID);
        assert!(workspace_row.global);
        let snapshot = untracked_snapshot(workspace_row);
        assert_eq!(
            snapshot.get("key").and_then(JsonValue::as_str),
            Some(WORKSPACE_VERSION_KEY)
        );
        assert_eq!(
            snapshot.get("value").and_then(JsonValue::as_str),
            Some(plan.receipt.main_version_id.as_str())
        );
    }

    #[tokio::test]
    async fn initialize_writes_initial_commit_through_commit_store() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(backend);
        let commit_store = CommitStoreContext::new();
        let tracked_state = TrackedStateContext::new();
        let untracked_state = UntrackedStateContext::new();

        let receipt = initialize(
            storage.clone(),
            &commit_store,
            &tracked_state,
            &untracked_state,
        )
        .await
        .expect("engine should initialize");
        let reader = commit_store.reader(storage.clone());
        let commit = reader
            .load_commit(&receipt.initial_commit_id)
            .await
            .expect("commit should load")
            .expect("initial commit should exist");

        assert_eq!(commit.id, receipt.initial_commit_id);
        assert_eq!(commit.change_pack_count, 1);
        assert_eq!(commit.membership_pack_count, 0);

        let change_pack = reader
            .load_change_pack(&commit.id, 0)
            .await
            .expect("change pack should load")
            .expect("initial change pack should exist");
        assert_eq!(change_pack.len(), seed_schema_definitions().len() + 3);
        assert!(change_pack
            .iter()
            .all(|change| change.id != commit.change_id));

        let entries = reader
            .load_change_index_entries(&[commit.change_id.clone(), "global".to_string()])
            .await
            .expect("change index should load");
        assert!(entries[0].is_some());
        assert!(entries[1].is_some());
    }

    fn snapshot(change: &InitSeedChange) -> JsonValue {
        serde_json::from_str(&change.snapshot_content).expect("snapshot should be JSON")
    }

    fn untracked_snapshot(row: &InitSeedLiveRow) -> JsonValue {
        serde_json::from_str(&row.snapshot_content).expect("snapshot should be JSON")
    }

    fn test_functions() -> FunctionProviderHandle {
        SharedFunctionProvider::new(
            Box::new(TestFunctionProvider::default()) as Box<dyn FunctionProvider + Send>
        )
    }

    #[derive(Default)]
    struct TestFunctionProvider {
        uuid_count: usize,
        timestamp_count: usize,
    }

    impl FunctionProvider for TestFunctionProvider {
        fn uuid_v7(&mut self) -> String {
            self.uuid_count += 1;
            format!("test-uuid-{}", self.uuid_count)
        }

        fn timestamp(&mut self) -> String {
            self.timestamp_count += 1;
            format!("test-timestamp-{}", self.timestamp_count)
        }
    }
}
