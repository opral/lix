use bytes::Bytes;

use crate::LixError;
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageCoreProjection, StorageGetOptions, StorageKey,
    StorageProjectedValue, StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
};

const CATALOG_REVISION_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0007_0003), "catalog.schema_revision");
const CATALOG_REVISION_KEY: &[u8] = b"global";

/// Storage-snapshot identity for the visible registered-schema catalog.
///
/// The token is updated atomically with mutations that can change schema
/// visibility. It is intentionally opaque: equality is the only operation a
/// catalog cache needs.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CatalogRevision(Bytes);

#[cfg(test)]
impl CatalogRevision {
    pub(crate) fn for_test(value: &'static [u8]) -> Self {
        Self(Bytes::from_static(value))
    }
}

pub(crate) async fn load_catalog_revision(
    store: &(impl StorageAdapterRead + ?Sized),
) -> Result<Option<CatalogRevision>, LixError> {
    let result = PointReadPlan::new(
        CATALOG_REVISION_SPACE,
        &[StorageKey(Bytes::from_static(CATALOG_REVISION_KEY))],
    )
    .materialize(
        store,
        StorageGetOptions {
            projection: StorageCoreProjection::FullValue,
        },
    )
    .await?;
    Ok(result
        .value
        .into_iter()
        .next()
        .flatten()
        .and_then(|value| match value {
            StorageProjectedValue::FullValue(bytes) => Some(CatalogRevision(bytes)),
            StorageProjectedValue::KeyOnly => None,
        }))
}

pub(crate) fn stage_catalog_revision(writes: &mut StorageWriteSet) {
    writes.put(
        CATALOG_REVISION_SPACE,
        StorageKey(Bytes::from_static(CATALOG_REVISION_KEY)),
        StorageValue {
            bytes: Bytes::copy_from_slice(uuid::Uuid::now_v7().as_bytes()),
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value as JsonValue, json};

    use crate::changelog::CommitId;
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};
    use crate::{
        CreateBranchOptions, Engine, MergeBranchOptions, MergeBranchOutcome, SessionContext, Value,
    };

    #[tokio::test]
    async fn catalog_revision_round_trips_through_one_storage_snapshot() {
        let storage = StorageAdapter::new(Memory::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("initial read should open");
        assert_eq!(
            load_catalog_revision(&read)
                .await
                .expect("missing revision should load"),
            None
        );

        let mut writes = storage.new_write_set();
        stage_catalog_revision(&mut writes);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("revision should commit");

        assert_eq!(
            load_catalog_revision(&read)
                .await
                .expect("pinned read should remain valid"),
            None,
            "an existing read must retain its pre-commit snapshot"
        );
        let next_read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("next read should open");
        assert!(
            load_catalog_revision(&next_read)
                .await
                .expect("committed revision should load")
                .is_some()
        );
    }

    #[tokio::test]
    async fn schema_commit_advances_revision_while_ordinary_crud_does_not() {
        let storage = Memory::new();
        let receipt = Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let adapter = StorageAdapter::new(storage.clone());
        let initial_revision = current_revision(&adapter).await;

        let engine = Engine::new(storage.clone())
            .await
            .expect("engine should open");
        let session = engine
            .open_session(&receipt.main_branch_id)
            .await
            .expect("main session should open");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('revision-control', 'one')",
                &[],
            )
            .await
            .expect("ordinary CRUD should commit");
        assert_eq!(
            current_revision(&adapter).await,
            initial_revision,
            "ordinary state writes must keep the hot catalog generation"
        );

        let no_op = session
            .execute(
                "UPDATE lix_registered_schema SET value = value \
                 WHERE lixcol_entity_pk = lix_json('[\"missing-schema\"]')",
                &[],
            )
            .await
            .expect("zero-row schema update should succeed");
        assert_eq!(no_op.rows_affected(), 0);
        assert_eq!(current_revision(&adapter).await, initial_revision);

        let mut rolled_back = session
            .begin_transaction()
            .await
            .expect("rollback transaction should begin");
        rolled_back
            .execute(
                "INSERT INTO lix_registered_schema \
                 (value, lixcol_global, lixcol_untracked) VALUES ($1, false, true)",
                &[Value::Json(test_schema("rolled_back_schema", false))],
            )
            .await
            .expect("rolled-back schema should stage");
        rolled_back
            .rollback()
            .await
            .expect("schema transaction should roll back");
        assert_eq!(current_revision(&adapter).await, initial_revision);

        register_schema(&session, "untracked_revision_probe", true).await;
        let untracked_revision = current_revision(&adapter).await;
        assert_ne!(untracked_revision, initial_revision);
        let inserted = session
            .execute(
                "INSERT INTO untracked_revision_probe (id, lixcol_untracked) \
                 VALUES ('untracked-row', true)",
                &[],
            )
            .await
            .expect("untracked schema should be visible after commit");
        assert_eq!(inserted.rows_affected(), 1);
        assert_eq!(
            current_revision(&adapter).await,
            untracked_revision,
            "writes through a dynamic surface must not invalidate its catalog"
        );

        register_schema(&session, "tracked_revision_probe", false).await;
        let tracked_revision = current_revision(&adapter).await;
        assert_ne!(tracked_revision, untracked_revision);
        let amended = session
            .execute(
                "UPDATE lix_registered_schema SET value = $1 \
                 WHERE lixcol_entity_pk = lix_json('[\"tracked_revision_probe\"]')",
                &[Value::Json(test_schema("tracked_revision_probe", true))],
            )
            .await
            .expect("compatible tracked schema amendment should commit");
        assert_eq!(amended.rows_affected(), 1);
        let amended_revision = current_revision(&adapter).await;
        assert_ne!(amended_revision, tracked_revision);

        let delete_error = session
            .execute(
                "DELETE FROM lix_registered_schema \
                 WHERE lixcol_entity_pk = lix_json('[\"tracked_revision_probe\"]')",
                &[],
            )
            .await
            .expect_err("public registered-schema deletion remains unsupported");
        assert_eq!(delete_error.code, LixError::CODE_UNSUPPORTED_SQL);
        assert_eq!(current_revision(&adapter).await, amended_revision);
    }

    #[tokio::test]
    async fn concurrent_engine_commit_invalidates_next_open_but_not_open_transaction() {
        let storage = Memory::new();
        let receipt = Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let adapter = StorageAdapter::new(storage.clone());
        let engine_a = Engine::new(storage.clone())
            .await
            .expect("first engine should open");
        let engine_b = Engine::new(storage.clone())
            .await
            .expect("second engine should open");
        let session_a = engine_a
            .open_session(&receipt.main_branch_id)
            .await
            .expect("first session should open");
        let session_b = engine_b
            .open_session(&receipt.main_branch_id)
            .await
            .expect("second session should open");

        session_a
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('warm-engine-a', 'one')",
                &[],
            )
            .await
            .expect("first engine should warm its transaction-opening cache");
        let pinned_read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("pre-schema read should pin");
        let pinned_revision = load_catalog_revision(&pinned_read)
            .await
            .expect("pinned revision should load")
            .expect("pinned revision should exist");
        let mut open_transaction = session_a
            .begin_transaction()
            .await
            .expect("explicit transaction should capture the old catalog");

        register_schema(&session_b, "concurrent_revision_probe", false).await;
        let committed_revision = current_revision(&adapter).await;
        assert_ne!(committed_revision, pinned_revision);
        assert_eq!(
            load_catalog_revision(&pinned_read)
                .await
                .expect("pinned revision should remain readable"),
            Some(pinned_revision),
            "the token and schema facts share one pinned storage snapshot"
        );

        open_transaction
            .execute(
                "INSERT INTO concurrent_revision_probe (id) VALUES ('too-new')",
                &[],
            )
            .await
            .expect_err("an already-open transaction must retain its old SQL catalog");
        open_transaction
            .rollback()
            .await
            .expect("old transaction should roll back");

        let inserted = session_a
            .execute(
                "INSERT INTO concurrent_revision_probe (id) VALUES ('next-open')",
                &[],
            )
            .await
            .expect("the next transaction on the other engine must reload the catalog");
        assert_eq!(inserted.rows_affected(), 1);
        assert_eq!(current_revision(&adapter).await, committed_revision);
    }

    #[tokio::test]
    async fn branch_ref_rewind_and_restore_use_fresh_revisions_without_false_hits() {
        let storage = Memory::new();
        let receipt = Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let adapter = StorageAdapter::new(storage.clone());
        let initial_revision = current_revision(&adapter).await;
        let engine = Engine::new(storage.clone())
            .await
            .expect("engine should open");
        let session = engine
            .open_session(&receipt.main_branch_id)
            .await
            .expect("main session should open");
        let initial_head = engine
            .load_branch_head_commit_id(&receipt.main_branch_id)
            .await
            .expect("initial head should load")
            .expect("initial head should exist");

        register_schema(&session, "branch_rewind_probe", false).await;
        session
            .execute(
                "INSERT INTO branch_rewind_probe (id) VALUES ('before-rewind')",
                &[],
            )
            .await
            .expect("new surface should warm the registered catalog");
        let schema_head = engine
            .load_branch_head_commit_id(&receipt.main_branch_id)
            .await
            .expect("schema head should load")
            .expect("schema head should exist");
        let schema_revision = current_revision(&adapter).await;
        assert_ne!(schema_revision, initial_revision);

        move_branch_ref(&session, &receipt.main_branch_id, &initial_head).await;
        let rewind_revision = current_revision(&adapter).await;
        assert_ne!(rewind_revision, initial_revision);
        assert_ne!(rewind_revision, schema_revision);
        session
            .execute(
                "INSERT INTO branch_rewind_probe (id) VALUES ('must-not-bind')",
                &[],
            )
            .await
            .expect_err("rewinding the head must hide the newer schema");
        assert_eq!(
            current_revision(&adapter).await,
            rewind_revision,
            "failed SQL must not advance the token"
        );

        move_branch_ref(&session, &receipt.main_branch_id, &schema_head).await;
        let restored_revision = current_revision(&adapter).await;
        assert_ne!(restored_revision, rewind_revision);
        assert_ne!(restored_revision, schema_revision);
        let restored = session
            .execute(
                "INSERT INTO branch_rewind_probe (id) VALUES ('after-restore')",
                &[],
            )
            .await
            .expect("restoring the head must restore the newer schema");
        assert_eq!(restored.rows_affected(), 1);
    }

    #[tokio::test]
    async fn schema_merges_advance_revision_for_fast_forward_and_merge_commit_paths() {
        run_schema_merge_case(false, MergeBranchOutcome::FastForward).await;
        run_schema_merge_case(true, MergeBranchOutcome::MergeCommitted).await;
    }

    async fn run_schema_merge_case(diverge_target: bool, expected: MergeBranchOutcome) {
        let storage = Memory::new();
        let receipt = Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let adapter = StorageAdapter::new(storage.clone());
        let engine = Engine::new(storage).await.expect("engine should open");
        let main = engine
            .open_session(&receipt.main_branch_id)
            .await
            .expect("main session should open");
        let revision_before_branch = current_revision(&adapter).await;
        main.create_branch(CreateBranchOptions {
            id: Some("catalog-revision-draft".to_string()),
            name: "Catalog revision draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should be created");
        assert_ne!(current_revision(&adapter).await, revision_before_branch);
        let draft = engine
            .open_session("catalog-revision-draft")
            .await
            .expect("draft session should open");
        if diverge_target {
            main.execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('merge-target-change', 'one')",
                &[],
            )
            .await
            .expect("target should diverge");
        }

        let schema_key = if diverge_target {
            "merge_commit_revision_probe"
        } else {
            "fast_forward_revision_probe"
        };
        register_schema(&draft, schema_key, false).await;
        let revision_before_merge = current_revision(&adapter).await;
        let receipt = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "catalog-revision-draft".to_string(),
            })
            .await
            .expect("schema branch should merge");
        assert_eq!(receipt.outcome, expected);
        let revision_after_merge = current_revision(&adapter).await;
        assert_ne!(revision_after_merge, revision_before_merge);

        let insert_sql = format!("INSERT INTO {schema_key} (id) VALUES ('merged-row')");
        let inserted = main
            .execute(&insert_sql, &[])
            .await
            .expect("merged schema surface should be visible");
        assert_eq!(inserted.rows_affected(), 1);

        let no_op_revision = current_revision(&adapter).await;
        let no_op = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: "catalog-revision-draft".to_string(),
            })
            .await
            .expect("repeated merge should be a no-op");
        assert_eq!(no_op.outcome, MergeBranchOutcome::AlreadyUpToDate);
        assert_eq!(current_revision(&adapter).await, no_op_revision);
    }

    async fn current_revision(adapter: &StorageAdapter<Memory>) -> CatalogRevision {
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("revision read should open");
        load_catalog_revision(&read)
            .await
            .expect("catalog revision should load")
            .expect("initialized storage should have a catalog revision")
    }

    async fn register_schema(session: &SessionContext<Memory>, schema_key: &str, untracked: bool) {
        let sql = format!(
            "INSERT INTO lix_registered_schema \
             (value, lixcol_global, lixcol_untracked) VALUES ($1, false, {untracked})"
        );
        session
            .execute(&sql, &[Value::Json(test_schema(schema_key, false))])
            .await
            .expect("schema registration should commit");
    }

    fn test_schema(schema_key: &str, amended: bool) -> JsonValue {
        let mut schema = json!({
            "x-lix-key": schema_key,
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": { "id": { "type": "string" } },
            "required": ["id"],
            "additionalProperties": false
        });
        if amended {
            schema["description"] = json!("compatible additive amendment");
            schema["properties"]["title"] = json!({ "type": "string" });
        }
        schema
    }

    async fn move_branch_ref(session: &SessionContext<Memory>, branch_id: &str, commit_id: &str) {
        let branch_id = branch_id.to_string();
        let commit_id = CommitId::parse_lix(commit_id, "catalog revision test branch head")
            .expect("test commit id should parse");
        session
            .with_write_transaction(move |transaction| {
                Box::pin(async move { transaction.advance_branch_ref(&branch_id, commit_id).await })
            })
            .await
            .expect("branch ref should move");
    }
}
