//! `lix_conversation.resolved` on repositories and writers the column did not
//! exist for.
//!
//! `fixtures/conversations_before_resolved.lixsnap` was written by Lix
//! `fc455768b` (conversation titles, before `detached_target` and `resolved`).
//! It holds a file-targeted conversation, a standalone conversation, a global
//! commit discussion created after one checkpoint, and a local conversation
//! created after that checkpoint. See `fixtures/README.md` for provenance.

use futures_util::io::Cursor;
use lix::{Lix, Memory, Value, open_lix};

const SNAPSHOT: &[u8] = include_bytes!("fixtures/conversations_before_resolved.lixsnap");

const FILE_ID: &str = "01950000-0000-7000-8000-00000000f001";
const TARGETED: &str = "01950000-0000-7000-8000-00000000c001";
const STANDALONE: &str = "01950000-0000-7000-8000-00000000c002";
const GLOBAL: &str = "01950000-0000-7000-8000-00000000c003";
const AFTER_CHECKPOINT: &str = "01950000-0000-7000-8000-00000000c004";
const RESOLVER: &str = "01950000-0000-7000-8000-00000000a001";
const REOPENER: &str = "01950000-0000-7000-8000-00000000a002";
const NOTE: &str = "01950000-0000-7000-8000-00000000d101";
const BODY: &str = r#"{"_type":"zettel_doc","blocks":[]}"#;

/// Who last flipped a conversation to resolved and when, as documented in
/// `docs/conversations.md`.
const WHO_RESOLVED: &str = "SELECT ch.account_id, a.name, ch.created_at
     FROM lix_history('lix_conversation') h
     JOIN lix_change ch ON ch.id = h.to_lixcol_change_id
     LEFT JOIN lix_account a ON a.id = ch.account_id
     WHERE h.id = $1 AND h.to_resolved AND NOT COALESCE(h.from_resolved, false)
     ORDER BY h.lixcol_position
     LIMIT 1";

async fn open_fixture() -> Lix<Memory> {
    open_lix()
        .from_snapshot(Cursor::new(SNAPSHOT))
        .await
        .expect("a repository written before `resolved` should open without migration")
}

async fn rows(lix: &Lix<Memory>, sql: &str, params: &[Value]) -> Vec<Vec<Value>> {
    lix.execute(sql, params)
        .await
        .unwrap_or_else(|error| panic!("{sql} failed: {error:?}"))
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect()
}

fn text(value: &str) -> Value {
    Value::Text(value.into())
}

#[tokio::test]
async fn existing_conversations_read_as_unresolved() {
    let lix = open_fixture().await;
    assert_eq!(lix.open_report().migration, None);
    assert_eq!(
        rows(
            &lix,
            "SELECT id, title, detached_target, resolved, lixcol_global
             FROM lix_conversation ORDER BY id",
            &[],
        )
        .await,
        vec![
            vec![
                text(TARGETED),
                text("Launch plan"),
                Value::Null,
                Value::Boolean(false),
                Value::Boolean(false),
            ],
            vec![
                text(STANDALONE),
                Value::Null,
                Value::Null,
                Value::Boolean(false),
                Value::Boolean(false),
            ],
            vec![
                text(GLOBAL),
                text("Commit discussion"),
                Value::Null,
                Value::Boolean(false),
                Value::Boolean(true),
            ],
            vec![
                text(AFTER_CHECKPOINT),
                Value::Null,
                Value::Null,
                Value::Boolean(false),
                Value::Boolean(false),
            ],
        ],
    );
    assert_eq!(
        rows(
            &lix,
            "SELECT COUNT(*) FROM lix_conversation WHERE resolved = false",
            &[],
        )
        .await,
        vec![vec![Value::Integer(4)]],
    );
    // Historical endpoints written before the column existed read the default.
    assert_eq!(
        rows(
            &lix,
            "SELECT id, resolved FROM lix_as_of('lix_conversation', (
               SELECT commit_id FROM lix_log() WHERE is_checkpoint ORDER BY position LIMIT 1
             )) ORDER BY id",
            &[],
        )
        .await,
        vec![
            vec![text(TARGETED), Value::Boolean(false)],
            vec![text(STANDALONE), Value::Boolean(false)],
        ],
    );
}

#[tokio::test]
async fn existing_conversations_resolve_reopen_and_record_the_writer() {
    let root = open_fixture().await;
    root.execute(
        "INSERT INTO lix_account (id, kind, name, status, lixcol_global) VALUES
         ($1, 'human', 'Resolver', 'active', true),
         ($2, 'human', 'Reopener', 'active', true)",
        &[text(RESOLVER), text(REOPENER)],
    )
    .await
    .unwrap();
    let resolver = root
        .open_another_session()
        .with_account(RESOLVER)
        .await
        .unwrap();
    let reopener = root
        .open_another_session()
        .with_account(REOPENER)
        .await
        .unwrap();

    // Resolve with a closing note: one transaction, one commit.
    let mut transaction = resolver.begin_transaction().await.unwrap();
    transaction
        .execute(
            "UPDATE lix_conversation SET resolved = true WHERE id = $1",
            &[text(TARGETED)],
        )
        .await
        .unwrap();
    transaction
        .execute(
            "INSERT INTO lix_comment (id, conversation_id, body) VALUES ($1, $2, $3::jsonb)",
            &[text(NOTE), text(TARGETED), text(BODY)],
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    // Editing an untouched pre-upgrade row keeps the default.
    root.execute(
        "UPDATE lix_conversation SET title = 'Renamed' WHERE id = $1",
        &[text(STANDALONE)],
    )
    .await
    .expect("rows written before the column should stay writable");
    assert_eq!(
        rows(
            &root,
            "SELECT id, title, resolved FROM lix_conversation WHERE id IN ($1, $2) ORDER BY id",
            &[text(TARGETED), text(STANDALONE)],
        )
        .await,
        vec![
            vec![text(TARGETED), text("Launch plan"), Value::Boolean(true)],
            vec![text(STANDALONE), text("Renamed"), Value::Boolean(false)],
        ],
    );

    // The current state's change metadata names the resolver.
    let current = rows(
        &root,
        "SELECT ch.account_id, ch.created_at IS NOT NULL,
                c.lixcol_commit_id = note.lixcol_commit_id
         FROM lix_conversation c
         JOIN lix_change ch ON ch.id = c.lixcol_change_id
         JOIN lix_comment note ON note.conversation_id = c.id
         WHERE c.id = $1 AND note.id = $2",
        &[text(TARGETED), text(NOTE)],
    )
    .await;
    assert_eq!(
        current,
        vec![vec![
            text(RESOLVER),
            Value::Boolean(true),
            Value::Boolean(true)
        ]],
    );

    root.execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .unwrap();
    let who = rows(&root, WHO_RESOLVED, &[text(TARGETED)]).await;
    assert_eq!(who.len(), 1, "{who:?}");
    assert_eq!(who[0][0], text(RESOLVER));
    assert_eq!(who[0][1], text("Resolver"));
    assert!(matches!(&who[0][2], Value::Text(at) if !at.is_empty()));

    // Reopen is an ordinary write by another account.
    reopener
        .execute(
            "UPDATE lix_conversation SET resolved = false WHERE id = $1",
            &[text(TARGETED)],
        )
        .await
        .unwrap();
    assert_eq!(
        rows(
            &root,
            "SELECT c.resolved, ch.account_id
             FROM lix_conversation c JOIN lix_change ch ON ch.id = c.lixcol_change_id
             WHERE c.id = $1",
            &[text(TARGETED)],
        )
        .await,
        vec![vec![Value::Boolean(false), text(REOPENER)]],
    );
    // History still answers who resolved it last, after the checkpoint.
    assert_eq!(
        rows(&root, WHO_RESOLVED, &[text(TARGETED)]).await[0][0],
        text(RESOLVER),
    );

    // Values survive an export and cold reopen.
    let mut snapshot = Vec::new();
    root.export_snapshot()
        .write_to(&mut snapshot)
        .await
        .unwrap();
    let reopened = open_lix()
        .from_snapshot(Cursor::new(snapshot))
        .await
        .unwrap();
    assert_eq!(
        rows(
            &reopened,
            "SELECT id, resolved FROM lix_conversation WHERE id IN ($1, $2) ORDER BY id",
            &[text(TARGETED), text(STANDALONE)],
        )
        .await,
        vec![
            vec![text(TARGETED), Value::Boolean(false)],
            vec![text(STANDALONE), Value::Boolean(false)],
        ],
    );
}

#[tokio::test]
async fn existing_conversation_detaches_and_can_be_resolved() {
    let lix = open_fixture().await;
    lix.execute("DELETE FROM lix_file WHERE id = $1", &[text(FILE_ID)])
        .await
        .unwrap();
    assert_eq!(
        rows(
            &lix,
            "SELECT target IS NULL, detached_target IS NOT NULL, resolved
             FROM lix_conversation WHERE id = $1",
            &[text(TARGETED)],
        )
        .await,
        vec![vec![
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(false)
        ]],
    );
    lix.execute(
        "UPDATE lix_conversation SET resolved = true WHERE id = $1",
        &[text(TARGETED)],
    )
    .await
    .unwrap();
    assert_eq!(
        rows(
            &lix,
            "SELECT resolved, detached_target IS NOT NULL FROM lix_conversation WHERE id = $1",
            &[text(TARGETED)],
        )
        .await,
        vec![vec![Value::Boolean(true), Value::Boolean(true)]],
    );
}
