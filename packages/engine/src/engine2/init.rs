use serde_json::json;
use std::sync::Arc;

use crate::engine2::changelog::{CanonicalChange, ChangelogContext};
use crate::engine2::entity_identity::EntityIdentity;
use crate::engine2::functions::{
    FunctionProvider, FunctionProviderHandle, SharedFunctionProvider, SystemFunctionProvider,
};
use crate::engine2::live_state::{LiveStateContext, LiveStateRow};
use crate::engine2::untracked_state::UntrackedStateRow;
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackend, LixError, TransactionBeginMode};

const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";
const KEY_VALUE_SCHEMA_VERSION: &str = "1";
const LIX_ID_KEY: &str = "lix_id";
const WORKSPACE_VERSION_KEY: &str = "lix_workspace_version_id";
const VERSION_DESCRIPTOR_SCHEMA_KEY: &str = "lix_version_descriptor";
const VERSION_DESCRIPTOR_SCHEMA_VERSION: &str = "1";
const VERSION_REF_SCHEMA_KEY: &str = "lix_version_ref";
const VERSION_REF_SCHEMA_VERSION: &str = "1";
const COMMIT_SCHEMA_KEY: &str = "lix_commit";
const COMMIT_SCHEMA_VERSION: &str = "1";

/// Pure seed plan for initializing an engine2 repository.
///
/// Tracked bootstrap facts go to the changelog. Moving refs such as
/// `lix_version_ref` are seeded as untracked local state so repository heads can
/// advance without becoming commit members.
pub(crate) struct InitSeedPlan {
    pub(crate) changes: Vec<CanonicalChange>,
    pub(crate) untracked_rows: Vec<UntrackedStateRow>,
    pub(crate) receipt: InitReceipt,
}

/// Values generated while planning the initial repository seed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitReceipt {
    pub lix_id: String,
    pub global_version_id: String,
    pub main_version_id: String,
    pub initial_commit_id: String,
}

/// Builds the canonical bootstrap changes for a new engine2 repository.
///
/// The initial commit tracks durable content rows. Version refs are moving
/// pointers and therefore live in untracked local state instead of changelog.
pub(crate) fn plan_init_seed(functions: FunctionProviderHandle) -> Result<InitSeedPlan, LixError> {
    let main_version_id = functions.call_uuid_v7();
    let lix_id = functions.call_uuid_v7();
    let initial_commit_id = functions.call_uuid_v7();
    let initial_change_set_id = functions.call_uuid_v7();
    let timestamp = functions.call_timestamp();

    let global_version_descriptor_change = canonical_change(
        GLOBAL_VERSION_ID.to_string(),
        EntityIdentity::single(GLOBAL_VERSION_ID),
        VERSION_DESCRIPTOR_SCHEMA_KEY,
        VERSION_DESCRIPTOR_SCHEMA_VERSION,
        version_descriptor_snapshot(GLOBAL_VERSION_ID, "global", true)?,
        &timestamp,
    );
    let main_version_descriptor_change = canonical_change(
        functions.call_uuid_v7(),
        EntityIdentity::single(&main_version_id),
        VERSION_DESCRIPTOR_SCHEMA_KEY,
        VERSION_DESCRIPTOR_SCHEMA_VERSION,
        version_descriptor_snapshot(&main_version_id, "main", false)?,
        &timestamp,
    );
    let kv_lix_id_change = canonical_change(
        functions.call_uuid_v7(),
        EntityIdentity::single(LIX_ID_KEY),
        KEY_VALUE_SCHEMA_KEY,
        KEY_VALUE_SCHEMA_VERSION,
        key_value_snapshot(LIX_ID_KEY, &lix_id)?,
        &timestamp,
    );

    let initial_commit_change = canonical_change(
        functions.call_uuid_v7(),
        EntityIdentity::single(&initial_commit_id),
        COMMIT_SCHEMA_KEY,
        COMMIT_SCHEMA_VERSION,
        commit_snapshot(
            &initial_commit_id,
            &initial_change_set_id,
            &[
                global_version_descriptor_change.id.clone(),
                main_version_descriptor_change.id.clone(),
                kv_lix_id_change.id.clone(),
            ],
        )?,
        &timestamp,
    );
    let global_version_ref_row = untracked_row(
        EntityIdentity::single(GLOBAL_VERSION_ID),
        VERSION_REF_SCHEMA_KEY,
        VERSION_REF_SCHEMA_VERSION,
        version_ref_snapshot(GLOBAL_VERSION_ID, &initial_commit_id)?,
        &timestamp,
    );
    let main_version_ref_row = untracked_row(
        EntityIdentity::single(&main_version_id),
        VERSION_REF_SCHEMA_KEY,
        VERSION_REF_SCHEMA_VERSION,
        version_ref_snapshot(&main_version_id, &initial_commit_id)?,
        &timestamp,
    );
    let workspace_version_row = untracked_row(
        EntityIdentity::single(WORKSPACE_VERSION_KEY),
        KEY_VALUE_SCHEMA_KEY,
        KEY_VALUE_SCHEMA_VERSION,
        key_value_snapshot(WORKSPACE_VERSION_KEY, &main_version_id)?,
        &timestamp,
    );

    Ok(InitSeedPlan {
        changes: vec![
            global_version_descriptor_change,
            main_version_descriptor_change,
            kv_lix_id_change,
            initial_commit_change,
        ],
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

/// Initializes an empty engine2 repository in one backend transaction.
///
/// The pure seed planner decides which bootstrap facts exist. This function is
/// only responsible for durably writing those facts to their owning stores:
/// changelog for tracked changes, and live_state for the serving projection plus
/// untracked moving refs.
pub(crate) async fn initialize(
    backend: Arc<dyn LixBackend + Send + Sync>,
    changelog: &ChangelogContext,
    live_state: &LiveStateContext,
) -> Result<InitReceipt, LixError> {
    let functions = SharedFunctionProvider::new(
        Box::new(SystemFunctionProvider) as Box<dyn FunctionProvider + Send>
    );
    let plan = plan_init_seed(functions)?;
    let receipt = plan.receipt.clone();

    let mut transaction = backend
        .begin_transaction(TransactionBeginMode::Write)
        .await?;

    {
        let mut writer = changelog.writer(transaction.as_mut());
        writer.append_changes(&plan.changes).await?;
    }

    let mut live_rows = plan
        .changes
        .iter()
        .map(|change| live_state_row_from_initial_change(change, &receipt.initial_commit_id))
        .collect::<Vec<_>>();
    live_rows.extend(plan.untracked_rows.into_iter().map(LiveStateRow::from));

    {
        let mut writer = live_state.writer(transaction.as_mut());
        writer.write_rows(&live_rows).await?;
    }

    transaction.commit().await?;
    Ok(receipt)
}

fn live_state_row_from_initial_change(
    change: &CanonicalChange,
    initial_commit_id: &str,
) -> LiveStateRow {
    LiveStateRow {
        entity_id: change.entity_id.clone(),
        schema_key: change.schema_key.clone(),
        file_id: change.file_id.clone(),
        plugin_key: change.plugin_key.clone(),
        snapshot_content: change.snapshot_content.clone(),
        metadata: change.metadata.clone(),
        schema_version: change.schema_version.clone(),
        created_at: change.created_at.clone(),
        updated_at: change.created_at.clone(),
        global: true,
        change_id: Some(change.id.clone()),
        commit_id: Some(initial_commit_id.to_string()),
        untracked: false,
        version_id: GLOBAL_VERSION_ID.to_string(),
    }
}

fn untracked_row(
    entity_id: EntityIdentity,
    schema_key: &str,
    schema_version: &str,
    snapshot_content: String,
    timestamp: &str,
) -> UntrackedStateRow {
    UntrackedStateRow {
        entity_id,
        schema_key: schema_key.to_string(),
        file_id: None,
        plugin_key: None,
        snapshot_content: Some(snapshot_content),
        metadata: None,
        schema_version: schema_version.to_string(),
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
    schema_version: &str,
    snapshot_content: String,
    created_at: &str,
) -> CanonicalChange {
    CanonicalChange {
        id,
        entity_id,
        schema_key: schema_key.to_string(),
        schema_version: schema_version.to_string(),
        file_id: None,
        plugin_key: None,
        snapshot_content: Some(snapshot_content),
        metadata: None,
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

fn commit_snapshot(
    id: &str,
    change_set_id: &str,
    change_ids: &[String],
) -> Result<String, LixError> {
    encode_snapshot(json!({
        "id": id,
        "change_set_id": change_set_id,
        "change_ids": change_ids,
        "parent_commit_ids": [],
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
            format!("engine2 init seed snapshot serialization failed: {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use serde_json::Value as JsonValue;

    use super::*;
    use crate::engine2::functions::{FunctionProvider, SharedFunctionProvider};

    #[test]
    fn plan_init_seed_returns_tracked_changes_and_untracked_workspace_state() {
        let plan = plan_init_seed(test_functions()).expect("init seed should plan");

        assert_eq!(plan.changes.len(), 4);
        assert_eq!(plan.untracked_rows.len(), 3);
        assert_eq!(plan.receipt.global_version_id, GLOBAL_VERSION_ID);
        assert_eq!(plan.receipt.main_version_id, "test-uuid-1");
        assert_eq!(plan.receipt.lix_id, "test-uuid-2");
        assert_eq!(plan.receipt.initial_commit_id, "test-uuid-3");
    }

    #[test]
    fn plan_init_seed_commit_tracks_only_descriptor_and_lix_id_changes() {
        let plan = plan_init_seed(test_functions()).expect("init seed should plan");
        let commit_change = plan
            .changes
            .iter()
            .find(|change| change.schema_key == COMMIT_SCHEMA_KEY)
            .expect("initial commit change should exist");
        let commit_snapshot = snapshot(commit_change);

        assert_eq!(
            commit_snapshot.get("id").and_then(JsonValue::as_str),
            Some(plan.receipt.initial_commit_id.as_str())
        );
        assert_eq!(
            commit_snapshot
                .get("change_ids")
                .and_then(JsonValue::as_array)
                .expect("change_ids should be an array")
                .iter()
                .map(|value| value.as_str().expect("change id should be text"))
                .collect::<Vec<_>>(),
            vec!["global", "test-uuid-5", "test-uuid-6"]
        );
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
                        == crate::engine2::entity_identity::EntityIdentity::single(
                            WORKSPACE_VERSION_KEY,
                        )
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

    fn snapshot(change: &CanonicalChange) -> JsonValue {
        serde_json::from_str(
            change
                .snapshot_content
                .as_deref()
                .expect("change should have snapshot"),
        )
        .expect("snapshot should be JSON")
    }

    fn untracked_snapshot(row: &UntrackedStateRow) -> JsonValue {
        serde_json::from_str(
            row.snapshot_content
                .as_deref()
                .expect("row should have snapshot"),
        )
        .expect("snapshot should be JSON")
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
