use std::sync::Arc;

use lix::common::{LixTimestamp, MutationIdentity};
use lix::entity_pk::EntityPk;
use lix::hot_state::MaterializedHotStateRow;
use lix::plugin::runtime::{
    BoundCreateContext, PluginActorKey, reserve_create_row, validate_create_reservation,
};
use lix::{Lix, LixError, Memory, Value, open_lix};

#[tokio::test]
async fn create_reservations_survive_repository_reopen() {
    let storage = Memory::new();
    let path = "/durable-ids.txt";
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("repository opens");
    write_file(&lix, path, b"fixture\n".to_vec())
        .await
        .expect("fixture file writes");

    let file = lix
        .execute(
            "SELECT id FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_string())],
        )
        .await
        .expect("fixture identity reads");
    let file_id = file.rows()[0].get::<String>("id").expect("file id");
    let branch_id = lix.active_branch_id().await.expect("active branch id");
    let actor_key = PluginActorKey {
        branch_id: branch_id.clone(),
        file_id: file_id.clone(),
        path: path.to_string(),
        owner_change_id: "durable-owner".to_string(),
        plugin_key: "plugin_test".to_string(),
        plugin_generation: "durable-generation".to_string(),
    };
    let inserted_identity = MutationIdentity {
        namespace_seed: uuid::Uuid::parse_str("01920000-0000-7000-8000-000000000031")
            .expect("fixture UUIDv7")
            .into_bytes(),
        operation_proof: [0x41; 32],
    };
    let inserted = BoundCreateContext::bind(inserted_identity, &actor_key)
        .expect("mutation identity binds to the file authority");
    let reservation = reserve_create_row(None, inserted, &file_id, &branch_id, false)
        .expect("reservation materializes")
        .expect("a fresh identity writes one reservation");
    let reservation_key = reservation
        .entity_pk
        .as_ref()
        .expect("reservation key")
        .as_single_string()
        .expect("reservation key is text")
        .to_string();
    let mut transaction = lix
        .begin_transaction()
        .await
        .expect("reservation transaction opens");
    transaction
        .stage_test_row(reservation)
        .await
        .expect("engine-managed reservation stages");
    transaction
        .commit()
        .await
        .expect("reservation commits through the durable state path");
    lix.close().await.expect("first repository closes");

    let lix = open_lix()
        .with_storage(storage)
        .await
        .expect("repository reopens");
    let durable = load_reservation(&lix, &reservation_key, &file_id).await;
    validate_create_reservation(Some(&durable), inserted, &file_id, &branch_id, false)
        .expect("same-proof retry is accepted after reopen");

    let collision = BoundCreateContext::bind(
        MutationIdentity {
            namespace_seed: inserted_identity.namespace_seed,
            operation_proof: [0x42; 32],
        },
        &actor_key,
    )
    .expect("colliding seed still binds");
    let error = validate_create_reservation(Some(&durable), collision, &file_id, &branch_id, false)
        .expect_err("a reused namespace with another proof must remain rejected after reopen");
    assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);

    lix.execute(
        "DELETE FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_string())],
    )
    .await
    .expect("file tombstones with its durable reservations");
    let remaining = lix
        .execute(
            "SELECT COUNT(*) AS count FROM lix_key_value WHERE key = $1 AND lixcol_file_id = $2",
            &[Value::Text(reservation_key), Value::Text(file_id.clone())],
        )
        .await
        .expect("reservation deletion reads")
        .rows()[0]
        .get::<i64>("count")
        .expect("count is an integer");
    assert_eq!(
        remaining, 0,
        "file deletion must tombstone its reservations"
    );
    lix.close().await.expect("repository closes");
}

async fn write_file(lix: &Lix<Memory>, path: &str, content: Vec<u8>) -> Result<(), LixError> {
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET content = excluded.content",
        &[Value::Text(path.to_string()), Value::Blob(content.into())],
    )
    .await?;
    Ok(())
}

async fn load_reservation(
    lix: &Lix<Memory>,
    reservation_key: &str,
    file_id: &str,
) -> MaterializedHotStateRow {
    let result = lix
        .execute(
            "SELECT key, value, lixcol_file_id, lixcol_untracked \
             FROM lix_key_value WHERE key = $1 AND lixcol_file_id = $2",
            &[
                Value::Text(reservation_key.to_string()),
                Value::Text(file_id.to_string()),
            ],
        )
        .await
        .expect("durable reservation reads after reopen");
    assert_eq!(result.rows().len(), 1, "one durable reservation remains");
    let row = &result.rows()[0];
    let value = row
        .get::<serde_json::Value>("value")
        .expect("reservation value is JSON");
    MaterializedHotStateRow {
        entity_pk: EntityPk::single(reservation_key),
        schema_key: "lix_key_value".to_string(),
        file_id: Some(
            row.get::<String>("lixcol_file_id")
                .expect("reservation file id"),
        ),
        snapshot_content: Some(
            serde_json::to_string(&serde_json::json!({
                "key": reservation_key,
                "value": value,
            }))
            .expect("reservation snapshot serializes")
            .into(),
        ),
        metadata: None,
        deleted: false,
        created_at: LixTimestamp::from_unix_millis_utc_lossy(1),
        updated_at: LixTimestamp::from_unix_millis_utc_lossy(1),
        global: false,
        change_id: None,
        commit_id: None,
        untracked: row
            .get::<bool>("lixcol_untracked")
            .expect("reservation lane"),
        branch_id: Arc::from(lix.active_branch_id().await.expect("reservation branch id")),
    }
}
