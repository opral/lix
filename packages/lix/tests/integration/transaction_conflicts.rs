//! Explicit transactions conflict only when a concurrent commit changed state
//! that the transaction read for a decision or writes itself (#1900).

use lix::{Lix, LixError, Value, open_lix};

async fn insert_file(lix: &Lix, path: &str, content: &[u8]) -> String {
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2) RETURNING id",
        &[
            Value::Text(path.into()),
            Value::Blob(content.to_vec().into()),
        ],
    )
    .await
    .unwrap()
    .rows()[0]
        .get("id")
        .unwrap()
}

async fn file_content(lix: &Lix, id: &str) -> Vec<u8> {
    lix.execute(
        "SELECT content FROM lix_file WHERE id = $1",
        &[Value::Text(id.into())],
    )
    .await
    .unwrap()
    .rows()[0]
        .get("content")
        .unwrap()
}

async fn key_value(lix: &Lix, key: &str) -> Option<serde_json::Value> {
    lix.execute(
        "SELECT value FROM lix_key_value WHERE key = $1",
        &[Value::Text(key.into())],
    )
    .await
    .unwrap()
    .rows()
    .first()
    .map(|row| row.get("value").unwrap())
}

fn assert_conflict(error: &LixError) {
    assert_eq!(error.code, LixError::CODE_TRANSACTION_CONFLICT, "{error:?}");
    let details = error.details.as_ref().expect("conflicts carry details");
    assert_eq!(details["retryable"], true, "{details}");
}

fn overlap_schema_keys(error: &LixError) -> Vec<String> {
    error.details.as_ref().unwrap()["overlaps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|overlap| overlap["schemaKey"].as_str().unwrap().to_owned())
        .collect()
}

/// The #1900 reproduction: a file update commits after an unrelated
/// key-value insert on the same branch.
#[tokio::test]
async fn file_update_commits_after_unrelated_key_value_insert() {
    let lix = open_lix().await.unwrap();
    let file_id = insert_file(&lix, "/a.md", b"a").await;

    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "UPDATE lix_file SET content = $2 WHERE id = $1",
        &[
            Value::Text(file_id.clone()),
            Value::Blob(b"b".to_vec().into()),
        ],
    )
    .await
    .unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('unrelated', 1)",
        &[],
    )
    .await
    .unwrap();
    let receipt = tx.commit().await.unwrap();
    assert!(receipt.commit.is_some());

    assert_eq!(file_content(&lix, &file_id).await, b"b");
    assert_eq!(
        key_value(&lix, "unrelated").await,
        Some(serde_json::json!(1))
    );
}

#[tokio::test]
async fn key_value_update_commits_after_concurrent_file_write() {
    let lix = open_lix().await.unwrap();
    let file_id = insert_file(&lix, "/a.md", b"a").await;
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('counter', 1)",
        &[],
    )
    .await
    .unwrap();

    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "UPDATE lix_key_value SET value = 2 WHERE key = 'counter'",
        &[],
    )
    .await
    .unwrap();
    lix.execute(
        "UPDATE lix_file SET content = $2 WHERE id = $1",
        &[
            Value::Text(file_id.clone()),
            Value::Blob(b"b".to_vec().into()),
        ],
    )
    .await
    .unwrap();
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ('/new.md', CAST('new' AS BYTEA))",
        &[],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    assert_eq!(key_value(&lix, "counter").await, Some(serde_json::json!(2)));
    assert_eq!(file_content(&lix, &file_id).await, b"b");
}

#[tokio::test]
async fn updates_of_different_rows_commit_after_each_other() {
    let lix = open_lix().await.unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('left', 0), ('right', 0)",
        &[],
    )
    .await
    .unwrap();
    let other = lix.open_another_session().await.unwrap();

    let mut left = lix.begin_transaction().await.unwrap();
    let mut right = other.begin_transaction().await.unwrap();
    left.execute("SELECT value FROM lix_key_value WHERE key = 'left'", &[])
        .await
        .unwrap();
    left.execute("UPDATE lix_key_value SET value = 1 WHERE key = 'left'", &[])
        .await
        .unwrap();
    right
        .execute("SELECT value FROM lix_key_value WHERE key = 'right'", &[])
        .await
        .unwrap();
    right
        .execute(
            "UPDATE lix_key_value SET value = 1 WHERE key = 'right'",
            &[],
        )
        .await
        .unwrap();
    left.commit().await.unwrap();
    right.commit().await.unwrap();

    assert_eq!(key_value(&lix, "left").await, Some(serde_json::json!(1)));
    assert_eq!(key_value(&lix, "right").await, Some(serde_json::json!(1)));
    other.close().await.unwrap();
}

#[tokio::test]
async fn content_updates_of_different_files_commit_after_each_other() {
    let lix = open_lix().await.unwrap();
    let a = insert_file(&lix, "/a.txt", b"a0").await;
    let b = insert_file(&lix, "/b.txt", b"b0").await;
    let other = lix.open_another_session().await.unwrap();

    let mut first = lix.begin_transaction().await.unwrap();
    let mut second = other.begin_transaction().await.unwrap();
    first
        .execute(
            "UPDATE lix_file SET content = $2 WHERE id = $1",
            &[Value::Text(a.clone()), Value::Blob(b"a1".to_vec().into())],
        )
        .await
        .unwrap();
    second
        .execute(
            "UPDATE lix_file SET content = $2 WHERE id = $1",
            &[Value::Text(b.clone()), Value::Blob(b"b1".to_vec().into())],
        )
        .await
        .unwrap();
    first.commit().await.unwrap();
    second.commit().await.unwrap();

    assert_eq!(file_content(&lix, &a).await, b"a1");
    assert_eq!(file_content(&lix, &b).await, b"b1");
    other.close().await.unwrap();
}

#[tokio::test]
async fn updates_of_the_same_row_conflict() {
    let lix = open_lix().await.unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('shared', 0)",
        &[],
    )
    .await
    .unwrap();

    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "UPDATE lix_key_value SET value = 1 WHERE key = 'shared'",
        &[],
    )
    .await
    .unwrap();
    lix.execute(
        "UPDATE lix_key_value SET value = 2 WHERE key = 'shared'",
        &[],
    )
    .await
    .unwrap();
    let error = tx.commit().await.unwrap_err();
    assert_conflict(&error);
    assert!(
        overlap_schema_keys(&error).contains(&"lix_key_value".to_owned()),
        "{error:?}"
    );
    // No lost update: the concurrent value survives the rejected commit.
    assert_eq!(key_value(&lix, "shared").await, Some(serde_json::json!(2)));
}

#[tokio::test]
async fn content_updates_of_the_same_file_conflict() {
    let lix = open_lix().await.unwrap();
    let file_id = insert_file(&lix, "/shared.txt", b"0").await;

    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "UPDATE lix_file SET content = $2 WHERE id = $1",
        &[
            Value::Text(file_id.clone()),
            Value::Blob(b"tx".to_vec().into()),
        ],
    )
    .await
    .unwrap();
    lix.execute(
        "UPDATE lix_file SET content = $2 WHERE id = $1",
        &[
            Value::Text(file_id.clone()),
            Value::Blob(b"concurrent".to_vec().into()),
        ],
    )
    .await
    .unwrap();
    let error = tx.commit().await.unwrap_err();
    assert_conflict(&error);
    assert_eq!(file_content(&lix, &file_id).await, b"concurrent");
}

/// Write skew: each transaction reads the other's row to decide its own
/// write. Committing both would violate the invariant both checked.
#[tokio::test]
async fn read_then_write_conflicts_when_the_read_row_changed() {
    let lix = open_lix().await.unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('alice_on_call', true), ('bob_on_call', true)",
        &[],
    )
    .await
    .unwrap();
    let other = lix.open_another_session().await.unwrap();

    let mut alice = lix.begin_transaction().await.unwrap();
    let mut bob = other.begin_transaction().await.unwrap();
    let bob_on_call = alice
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'bob_on_call'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        bob_on_call.rows()[0]
            .get::<serde_json::Value>("value")
            .unwrap(),
        true
    );
    bob.execute(
        "SELECT value FROM lix_key_value WHERE key = 'alice_on_call'",
        &[],
    )
    .await
    .unwrap();
    alice
        .execute(
            "UPDATE lix_key_value SET value = false WHERE key = 'alice_on_call'",
            &[],
        )
        .await
        .unwrap();
    bob.execute(
        "UPDATE lix_key_value SET value = false WHERE key = 'bob_on_call'",
        &[],
    )
    .await
    .unwrap();

    alice.commit().await.unwrap();
    let error = bob.commit().await.unwrap_err();
    assert_conflict(&error);
    assert_eq!(error.details.as_ref().unwrap()["overlapKind"], "read");
    assert_eq!(
        key_value(&lix, "bob_on_call").await,
        Some(serde_json::json!(true))
    );
    other.close().await.unwrap();
}

#[tokio::test]
async fn update_matching_no_rows_conflicts_with_a_concurrent_matching_insert() {
    let lix = open_lix().await.unwrap();
    let mut tx = lix.begin_transaction().await.unwrap();
    let updated = tx
        .execute(
            "UPDATE lix_key_value SET value = 'claimed' WHERE key = 'slot'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(updated.rows_affected(), 0);
    tx.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('audit', 'no slot')",
        &[],
    )
    .await
    .unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('slot', 'free')",
        &[],
    )
    .await
    .unwrap();
    assert_conflict(&tx.commit().await.unwrap_err());
    assert_eq!(key_value(&lix, "audit").await, None);
}

#[tokio::test]
async fn blind_insert_conflicts_with_a_concurrent_insert_of_the_same_row_after_a_read() {
    let lix = open_lix().await.unwrap();
    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute("SELECT value FROM lix_key_value WHERE key = 'other'", &[])
        .await
        .unwrap();
    tx.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('claimed', 'tx')",
        &[],
    )
    .await
    .unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('claimed', 'concurrent')",
        &[],
    )
    .await
    .unwrap();
    let error = tx.commit().await.unwrap_err();
    assert_conflict(&error);
    assert_eq!(error.details.as_ref().unwrap()["overlapKind"], "write");
    assert_eq!(
        key_value(&lix, "claimed").await,
        Some(serde_json::json!("concurrent"))
    );
}

#[tokio::test]
async fn delete_commits_after_unrelated_concurrent_writes() {
    let lix = open_lix().await.unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('doomed', 1), ('kept', 1)",
        &[],
    )
    .await
    .unwrap();
    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute("DELETE FROM lix_key_value WHERE key = 'doomed'", &[])
        .await
        .unwrap();
    lix.execute("UPDATE lix_key_value SET value = 2 WHERE key = 'kept'", &[])
        .await
        .unwrap();
    insert_file(&lix, "/concurrent.txt", b"x").await;
    tx.commit().await.unwrap();
    assert_eq!(key_value(&lix, "doomed").await, None);
    assert_eq!(key_value(&lix, "kept").await, Some(serde_json::json!(2)));
}

#[tokio::test]
async fn reading_branch_state_keeps_the_conservative_branch_check() {
    let lix = open_lix().await.unwrap();
    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute("SELECT id, commit_id FROM lix_branch", &[])
        .await
        .unwrap();
    tx.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('after-branch-read', 1)",
        &[],
    )
    .await
    .unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('unrelated', 1)",
        &[],
    )
    .await
    .unwrap();
    let error = tx.commit().await.unwrap_err();
    assert_conflict(&error);
    assert_eq!(
        error.details.as_ref().unwrap()["reason"],
        "unvalidatedReadChanged"
    );
}

async fn key_values(lix: &Lix, prefix: &str) -> Vec<String> {
    lix.execute(
        "SELECT key FROM lix_key_value WHERE key LIKE $1 ORDER BY key",
        &[Value::Text(format!("{prefix}%"))],
    )
    .await
    .unwrap()
    .rows()
    .iter()
    .map(|row| row.get::<String>("key").unwrap())
    .collect()
}

fn assert_read_conflict(error: &LixError) {
    assert_conflict(error);
    assert_eq!(
        error.details.as_ref().unwrap()["reason"],
        "readSetChanged",
        "{error:?}"
    );
}

/// A row read only through a join still belongs to the read set.
#[tokio::test]
async fn join_read_conflicts_when_the_joined_row_changes() {
    let lix = open_lix().await.unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('order', 'open'), ('limit', 10)",
        &[],
    )
    .await
    .unwrap();
    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "SELECT o.value AS status, l.value AS lim FROM lix_key_value o \
         JOIN lix_key_value l ON l.key = 'limit' WHERE o.key = 'order'",
        &[],
    )
    .await
    .unwrap();
    tx.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('decision', 'approved')",
        &[],
    )
    .await
    .unwrap();
    lix.execute(
        "UPDATE lix_key_value SET value = 0 WHERE key = 'limit'",
        &[],
    )
    .await
    .unwrap();
    assert_read_conflict(&tx.commit().await.unwrap_err());
    assert_eq!(key_value(&lix, "decision").await, None);
}

/// An aggregate depends on every row it counted, including rows that did not
/// exist yet (phantoms).
#[tokio::test]
async fn aggregate_read_conflicts_with_a_concurrent_matching_insert() {
    let lix = open_lix().await.unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('seat-1', 'taken'), ('seat-2', 'taken')",
        &[],
    )
    .await
    .unwrap();
    let mut tx = lix.begin_transaction().await.unwrap();
    let count = tx
        .execute(
            "SELECT COUNT(*) AS n FROM lix_key_value WHERE key LIKE 'seat-%'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(count.rows()[0].get::<i64>("n").unwrap(), 2);
    tx.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('seat-3', 'taken')",
        &[],
    )
    .await
    .unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('seat-9', 'taken')",
        &[],
    )
    .await
    .unwrap();
    assert_read_conflict(&tx.commit().await.unwrap_err());
    assert_eq!(
        key_values(&lix, "seat-").await,
        ["seat-1", "seat-2", "seat-9"]
    );
}

/// `SELECT *` depends on the whole table, but not on other tables.
#[tokio::test]
async fn full_table_scan_conflicts_with_changes_to_that_table_only() {
    let lix = open_lix().await.unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('a', 1)",
        &[],
    )
    .await
    .unwrap();

    let mut unrelated = lix.begin_transaction().await.unwrap();
    unrelated
        .execute("SELECT * FROM lix_key_value", &[])
        .await
        .unwrap();
    unrelated
        .execute("UPDATE lix_key_value SET value = 2 WHERE key = 'a'", &[])
        .await
        .unwrap();
    insert_file(&lix, "/other-table.txt", b"x").await;
    unrelated.commit().await.unwrap();
    assert_eq!(key_value(&lix, "a").await, Some(serde_json::json!(2)));

    let mut related = lix.begin_transaction().await.unwrap();
    related
        .execute("SELECT * FROM lix_key_value", &[])
        .await
        .unwrap();
    related
        .execute("UPDATE lix_key_value SET value = 3 WHERE key = 'a'", &[])
        .await
        .unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('b', 1)",
        &[],
    )
    .await
    .unwrap();
    assert_read_conflict(&related.commit().await.unwrap_err());
    assert_eq!(key_value(&lix, "a").await, Some(serde_json::json!(2)));
}

/// UPDATE/DELETE predicates are protected against rows that start matching
/// them concurrently.
#[tokio::test]
async fn like_and_column_predicates_conflict_with_concurrent_phantoms() {
    let lix = open_lix().await.unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('tmp-1', 1), ('keep', 1)",
        &[],
    )
    .await
    .unwrap();

    let mut delete = lix.begin_transaction().await.unwrap();
    delete
        .execute("DELETE FROM lix_key_value WHERE key LIKE 'tmp-%'", &[])
        .await
        .unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('tmp-2', 1)",
        &[],
    )
    .await
    .unwrap();
    let error = delete.commit().await.unwrap_err();
    assert_conflict(&error);
    assert_eq!(key_values(&lix, "tmp-").await, ["tmp-1", "tmp-2"]);

    // A predicate on a non-key column scans the table, so any row that
    // starts matching it concurrently is a phantom.
    let mut update = lix.begin_transaction().await.unwrap();
    update
        .execute(
            "UPDATE lix_key_value SET value = 'handled' WHERE value = 'pending'",
            &[],
        )
        .await
        .unwrap();
    // The dependent write: "no job is pending any more".
    update
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('queue', 'drained')",
            &[],
        )
        .await
        .unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('job', 'pending')",
        &[],
    )
    .await
    .unwrap();
    assert_conflict(&update.commit().await.unwrap_err());
    assert_eq!(
        key_value(&lix, "job").await,
        Some(serde_json::json!("pending"))
    );
    assert_eq!(key_value(&lix, "queue").await, None);
}

#[tokio::test]
async fn concurrent_delete_of_a_read_row_conflicts() {
    let lix = open_lix().await.unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('grant', 'yes')",
        &[],
    )
    .await
    .unwrap();
    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute("SELECT value FROM lix_key_value WHERE key = 'grant'", &[])
        .await
        .unwrap();
    tx.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('granted-action', 1)",
        &[],
    )
    .await
    .unwrap();
    lix.execute("DELETE FROM lix_key_value WHERE key = 'grant'", &[])
        .await
        .unwrap();
    let error = tx.commit().await.unwrap_err();
    assert_read_conflict(&error);
    let overlap = &error.details.as_ref().unwrap()["overlaps"][0];
    assert_eq!(overlap["schemaKey"], "lix_key_value", "{error:?}");
    assert_eq!(overlap["rowPk"], serde_json::json!(["grant"]), "{error:?}");
    assert_eq!(key_value(&lix, "granted-action").await, None);
}

/// Path uniqueness is re-validated against the commit snapshot, so a
/// rebased transaction cannot publish a duplicate path.
#[tokio::test]
async fn rebased_transaction_cannot_publish_a_duplicate_path() {
    let lix = open_lix().await.unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('n', 0)",
        &[],
    )
    .await
    .unwrap();
    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute("UPDATE lix_key_value SET value = 1 WHERE key = 'n'", &[])
        .await
        .unwrap();
    tx.execute(
        "INSERT INTO lix_file (path, content) VALUES ('/dup.md', CAST('tx' AS BYTEA))",
        &[],
    )
    .await
    .unwrap();
    insert_file(&lix, "/dup.md", b"concurrent").await;
    assert!(tx.commit().await.is_err());
    let files = lix
        .execute("SELECT content FROM lix_file WHERE path = '/dup.md'", &[])
        .await
        .unwrap();
    assert_eq!(files.rows().len(), 1);
    assert_eq!(
        files.rows()[0].get::<Vec<u8>>("content").unwrap(),
        b"concurrent"
    );
    assert_eq!(key_value(&lix, "n").await, Some(serde_json::json!(0)));
}

/// `None` when the conversation does not exist, `Some(None)` when its target
/// is null, otherwise the debug rendering of the target reference.
async fn conversation_target(lix: &Lix, id: &str) -> Option<Option<String>> {
    lix.execute(
        "SELECT target FROM lix_conversation WHERE id = $1",
        &[Value::Text(id.into())],
    )
    .await
    .unwrap()
    .rows()
    .first()
    .map(|row| match row.get::<Value>("target").unwrap() {
        Value::Null => None,
        target => Some(format!("{target:?}")),
    })
}

const CONVERSATION: &str = "01950000-0000-7000-8000-00000000c001";

/// A delete's referential actions are planned against the commit snapshot:
/// a conversation that concurrently started targeting the deleted file is
/// detached instead of left dangling.
#[tokio::test]
async fn delete_applies_referential_actions_to_concurrently_added_references() {
    let lix = open_lix().await.unwrap();
    let file_id = insert_file(&lix, "/target.txt", b"t").await;
    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "DELETE FROM lix_file WHERE id = $1",
        &[Value::Text(file_id.clone())],
    )
    .await
    .unwrap();
    lix.execute(
        "INSERT INTO lix_conversation (id, target) VALUES ($1, lix_row_ref('lix_file', NULL, $2))",
        &[
            Value::Text(CONVERSATION.into()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .unwrap();
    match tx.commit().await {
        Ok(_) => {
            let exists = lix
                .execute(
                    "SELECT id FROM lix_file WHERE id = $1",
                    &[Value::Text(file_id.clone())],
                )
                .await
                .unwrap();
            assert!(exists.rows().is_empty());
            assert_eq!(conversation_target(&lix, CONVERSATION).await, Some(None));
        }
        Err(error) => {
            assert_conflict(&error);
            assert!(
                conversation_target(&lix, CONVERSATION)
                    .await
                    .unwrap()
                    .is_some()
            );
        }
    }
}

/// A reference written by the transaction is validated against the commit
/// snapshot, so it cannot point at a row deleted concurrently.
#[tokio::test]
async fn reference_to_a_concurrently_deleted_row_is_rejected() {
    let lix = open_lix().await.unwrap();
    let file_id = insert_file(&lix, "/target.txt", b"t").await;
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('n', 0)",
        &[],
    )
    .await
    .unwrap();
    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute("UPDATE lix_key_value SET value = 1 WHERE key = 'n'", &[])
        .await
        .unwrap();
    tx.execute(
        "INSERT INTO lix_conversation (id, target) VALUES ($1, lix_row_ref('lix_file', NULL, $2))",
        &[
            Value::Text(CONVERSATION.into()),
            Value::Text(file_id.clone()),
        ],
    )
    .await
    .unwrap();
    lix.execute(
        "DELETE FROM lix_file WHERE id = $1",
        &[Value::Text(file_id.clone())],
    )
    .await
    .unwrap();
    assert!(tx.commit().await.is_err());
    assert_eq!(conversation_target(&lix, CONVERSATION).await, None);
    assert_eq!(key_value(&lix, "n").await, Some(serde_json::json!(0)));
}

/// The Atelier case: re-targeting a conversation commits while another
/// session concurrently replies to it and stores unrelated state.
#[tokio::test]
async fn retargeting_a_conversation_commits_after_a_concurrent_reply() {
    let lix = open_lix().await.unwrap();
    let old_target = insert_file(&lix, "/old.md", b"old").await;
    let new_target = insert_file(&lix, "/new.md", b"new").await;
    lix.execute(
        "INSERT INTO lix_conversation (id, target) VALUES ($1, lix_row_ref('lix_file', NULL, $2))",
        &[
            Value::Text(CONVERSATION.into()),
            Value::Text(old_target.clone()),
        ],
    )
    .await
    .unwrap();
    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "UPDATE lix_file SET content = $2 WHERE id = $1",
        &[
            Value::Text(new_target.clone()),
            Value::Blob(b"saved".to_vec().into()),
        ],
    )
    .await
    .unwrap();
    tx.execute(
        "UPDATE lix_conversation SET target = lix_row_ref('lix_file', NULL, $2) WHERE id = $1",
        &[
            Value::Text(CONVERSATION.into()),
            Value::Text(new_target.clone()),
        ],
    )
    .await
    .unwrap();
    lix.execute(
        "INSERT INTO lix_comment (id, conversation_id, body) VALUES ($1, $2, CAST($3 AS JSONB))",
        &[
            Value::Text("01950000-0000-7000-8000-00000000c002".into()),
            Value::Text(CONVERSATION.into()),
            Value::Text(
                r#"{"_type":"zettel_doc","blocks":[{"_type":"zettel_block","_key":"p1","style":"normal","markDefs":[],"children":[{"_type":"zettel_span","_key":"s1","text":"reply","marks":[]}]}]}"#.into(),
            ),
        ],
    )
    .await
    .unwrap();
    insert_file(&lix, "/image.png", b"png").await;
    tx.commit().await.unwrap();
    assert_eq!(file_content(&lix, &new_target).await, b"saved");
    let comments = lix
        .execute(
            "SELECT id FROM lix_comment WHERE conversation_id = $1",
            &[Value::Text(CONVERSATION.into())],
        )
        .await
        .unwrap();
    assert_eq!(comments.rows().len(), 1);
    let retargeted = lix
        .execute(
            "SELECT CAST(target AS TEXT) = CAST(lix_row_ref('lix_file', NULL, $2) AS TEXT) AS retargeted \
             FROM lix_conversation WHERE id = $1",
            &[Value::Text(CONVERSATION.into()), Value::Text(new_target.clone())],
        )
        .await
        .unwrap();
    assert!(retargeted.rows()[0].get::<bool>("retargeted").unwrap());
}

#[tokio::test]
async fn concurrent_schema_registration_keeps_the_conservative_check() {
    let lix = open_lix().await.unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('n', 0)",
        &[],
    )
    .await
    .unwrap();
    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute("UPDATE lix_key_value SET value = 1 WHERE key = 'n'", &[])
        .await
        .unwrap();
    lix.execute(
        "INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)",
        &[Value::Text(
            serde_json::json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "concurrent_schema",
                "columns": [{"name": "id", "type": "text", "nullable": false}],
                "primary_key": ["id"]
            })
            .to_string(),
        )],
    )
    .await
    .unwrap();
    let error = tx.commit().await.unwrap_err();
    assert_conflict(&error);
    assert_eq!(
        error.details.as_ref().unwrap()["reason"],
        "unvalidatedReadChanged"
    );
}

#[tokio::test]
async fn reading_the_branch_head_keeps_the_conservative_check() {
    let lix = open_lix().await.unwrap();
    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute("SELECT lix_active_branch_commit_id() AS head", &[])
        .await
        .unwrap();
    tx.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('after-head', 1)",
        &[],
    )
    .await
    .unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('unrelated', 1)",
        &[],
    )
    .await
    .unwrap();
    let error = tx.commit().await.unwrap_err();
    assert_conflict(&error);
    assert_eq!(
        error.details.as_ref().unwrap()["reason"],
        "unvalidatedReadChanged"
    );
}

#[tokio::test]
async fn writing_a_branch_head_value_keeps_the_conservative_check() {
    let lix = open_lix().await.unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('head', 'x')",
        &[],
    )
    .await
    .unwrap();
    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "UPDATE lix_key_value SET value = lix_active_branch_commit_id() WHERE key = 'head'",
        &[],
    )
    .await
    .unwrap();
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('unrelated', 1)",
        &[],
    )
    .await
    .unwrap();
    assert_conflict(&tx.commit().await.unwrap_err());
}

/// A `path` predicate is resolved through the branch's whole path index, so
/// it is validated conservatively: a concurrent file insert conflicts. (An
/// `id` predicate is validated per file; see
/// `content_updates_of_different_files_commit_after_each_other`.)
#[tokio::test]
async fn file_update_by_path_conservatively_conflicts_with_a_concurrent_file_insert() {
    let lix = open_lix().await.unwrap();
    let file_id = insert_file(&lix, "/notes/a.md", b"a").await;
    let mut tx = lix.begin_transaction().await.unwrap();
    tx.execute(
        "UPDATE lix_file SET content = $1 WHERE path = '/notes/a.md'",
        &[Value::Blob(b"b".to_vec().into())],
    )
    .await
    .unwrap();
    insert_file(&lix, "/images/pasted.png", b"png").await;
    let error = tx.commit().await.unwrap_err();
    assert_conflict(&error);
    assert_eq!(file_content(&lix, &file_id).await, b"a");
}

/// A path predicate that matched nothing conflicts when a file appears at
/// that path concurrently.
#[tokio::test]
async fn file_update_by_path_conflicts_with_a_concurrent_file_at_that_path() {
    let lix = open_lix().await.unwrap();
    let mut tx = lix.begin_transaction().await.unwrap();
    let updated = tx
        .execute(
            "UPDATE lix_file SET content = $1 WHERE path = '/late.md'",
            &[Value::Blob(b"tx".to_vec().into())],
        )
        .await
        .unwrap();
    assert_eq!(updated.rows_affected(), 0);
    tx.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('missing', true)",
        &[],
    )
    .await
    .unwrap();
    insert_file(&lix, "/late.md", b"concurrent").await;
    assert_conflict(&tx.commit().await.unwrap_err());
    assert_eq!(key_value(&lix, "missing").await, None);
}
