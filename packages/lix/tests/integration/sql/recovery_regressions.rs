//! Regression cases distilled from Git restore/revert/apply, Dolt revert, and
//! jj operation restore/revert behavior. These systems differ in their exact
//! object models. Lix deliberately appends a new immutable commit for recovery;
//! Git restore, for example, does not commit automatically. The adapted cases
//! verify preservation of unrelated work and atomic conflicts on selected rows.

use super::select_rows;
use crate::support::simulation_test::engine::SimSession;
use lix::{CreateBranchOptions, LixError, Value};

async fn head(session: &SimSession) -> String {
    let rows = select_rows(session, "SELECT lix_active_branch_commit_id() AS commit_id").await;
    match rows.as_slice() {
        [row] => match row.as_slice() {
            [Value::Text(id)] => id.clone(),
            other => panic!("expected commit_id text, got {other:?}"),
        },
        other => panic!("expected one commit row, got {other:?}"),
    }
}

async fn receipt(session: &SimSession, sql: &str, params: &[Value]) -> Option<String> {
    let rows = session
        .execute(sql, params)
        .await
        .expect("recovery command succeeds");
    assert_eq!(rows.rows().len(), 1);
    match rows.rows()[0].values() {
        [Value::Text(id)] => Some(id.clone()),
        [Value::Null] => None,
        other => panic!("expected nullable commit_id receipt, got {other:?}"),
    }
}

async fn create_branch(session: &SimSession, id: &str, from_commit_id: &str) {
    session
        .create_branch(CreateBranchOptions {
            id: Some(id.to_owned()),
            name: id.to_owned(),
            from_commit_id: Some(from_commit_id.to_owned()),
        })
        .await
        .expect("branch should be created");
}

// Git restore is path/source scoped, while jj operation restore is a new
// operation rather than a history rewind. Lix's row-scoped restore must have
// the same observable boundary and remain an ordinary undoable commit.
simulation_test!(
    restore_is_scoped_and_undoable_as_an_ordinary_commit,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine.open_session().await.expect("main session opens"),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('a','seed'),('b','seed')",
            &[],
        )
        .await
        .expect("seed commits");
        let seed = head(&main).await;
        let source_id = "01930000-0000-7000-8000-000000000071";
        let target_id = "01930000-0000-7000-8000-000000000072";
        create_branch(&main, source_id, &seed).await;
        create_branch(&main, target_id, &seed).await;

        let source = sim.wrap_session(
            engine
                .open_session_at(source_id)
                .await
                .expect("source session opens"),
            &engine,
        );
        source
            .execute(
                "UPDATE lix_key_value SET value = 'source' WHERE key = 'a'",
                &[],
            )
            .await
            .expect("source edit commits");
        let source_commit = head(&source).await;

        let target = sim.wrap_session(
            engine
                .open_session_at(target_id)
                .await
                .expect("target session opens"),
            &engine,
        );
        target
            .execute(
                "UPDATE lix_key_value SET value = 'target' WHERE key = 'a'",
                &[],
            )
            .await
            .expect("target edit commits");
        target
            .execute(
                "UPDATE lix_key_value SET value = 'later' WHERE key = 'b'",
                &[],
            )
            .await
            .expect("unrelated target edit commits");
        let before_restore = head(&target).await;

        let restored = receipt(
            &target,
            "SELECT commit_id FROM lix_restore($1, ARRAY[lix_row_ref('lix_key_value','a')])",
            &[Value::Text(source_commit)],
        )
        .await
        .expect("row-scoped restore publishes a commit");
        assert_ne!(restored, before_restore);
        assert_eq!(
            select_rows(
                &target,
                "SELECT key,value FROM lix_key_value WHERE key IN ('a','b') ORDER BY key",
            )
            .await,
            vec![
                vec![
                    Value::Text("a".into()),
                    Value::Jsonb(serde_json::json!("source").into()),
                ],
                vec![
                    Value::Text("b".into()),
                    Value::Jsonb(serde_json::json!("later").into()),
                ],
            ]
        );

        let undone = receipt(&target, "SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("restore is an ordinary undoable commit");
        assert_ne!(undone, restored);
        assert_eq!(head(&target).await, undone);
        assert_eq!(
            select_rows(
                &target,
                "SELECT key,value FROM lix_key_value WHERE key IN ('a','b') ORDER BY key",
            )
            .await,
            vec![
                vec![
                    Value::Text("a".into()),
                    Value::Jsonb(serde_json::json!("target").into()),
                ],
                vec![
                    Value::Text("b".into()),
                    Value::Jsonb(serde_json::json!("later").into()),
                ],
            ]
        );
    }
);

// Git revert and Dolt's historical revert both append a separate commit and
// preserve later disjoint work. The resulting Lix commit is itself ordinary
// undo/redo history, rather than a special checkpoint receipt.
simulation_test!(
    revert_preserves_later_disjoint_work_and_roundtrips_through_undo_redo,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session =
            sim.wrap_session(engine.open_session().await.expect("session opens"), &engine);
        session
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('a','base'),('b','base')",
                &[],
            )
            .await
            .expect("seed commits");
        session
            .execute(
                "UPDATE lix_key_value SET value = 'changed' WHERE key = 'a'",
                &[],
            )
            .await
            .expect("historical change commits");
        let changed = head(&session).await;
        session
            .execute(
                "UPDATE lix_key_value SET value = 'later' WHERE key = 'b'",
                &[],
            )
            .await
            .expect("later disjoint change commits");
        let before_revert = head(&session).await;

        let reverted = receipt(
            &session,
            "SELECT commit_id FROM lix_revert($1)",
            &[Value::Text(changed)],
        )
        .await
        .expect("historical revert publishes a commit");
        assert_ne!(reverted, before_revert);
        assert_eq!(
            select_rows(
                &session,
                "SELECT key,value FROM lix_key_value WHERE key IN ('a','b') ORDER BY key",
            )
            .await,
            vec![
                vec![
                    Value::Text("a".into()),
                    Value::Jsonb(serde_json::json!("base").into()),
                ],
                vec![
                    Value::Text("b".into()),
                    Value::Jsonb(serde_json::json!("later").into()),
                ],
            ]
        );

        let undone = receipt(&session, "SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("revert commit is undoable");
        assert_ne!(undone, reverted);
        assert_eq!(
            select_rows(
                &session,
                "SELECT key,value FROM lix_key_value WHERE key IN ('a','b') ORDER BY key",
            )
            .await,
            vec![
                vec![
                    Value::Text("a".into()),
                    Value::Jsonb(serde_json::json!("changed").into()),
                ],
                vec![
                    Value::Text("b".into()),
                    Value::Jsonb(serde_json::json!("later").into()),
                ],
            ]
        );
        let redone = receipt(&session, "SELECT commit_id FROM lix_redo()", &[])
            .await
            .expect("revert commit redo succeeds");
        assert_ne!(redone, undone);
        assert_eq!(
            select_rows(
                &session,
                "SELECT key,value FROM lix_key_value WHERE key IN ('a','b') ORDER BY key",
            )
            .await,
            vec![
                vec![
                    Value::Text("a".into()),
                    Value::Jsonb(serde_json::json!("base").into()),
                ],
                vec![
                    Value::Text("b".into()),
                    Value::Jsonb(serde_json::json!("later").into()),
                ],
            ]
        );
    }
);

// Git apply and Dolt revert reject conflicting current identities as one
// transaction. A scoped retry may then apply a disjoint row, and that apply
// remains an ordinary commit with its own undo receipt.
simulation_test!(
    apply_conflict_is_atomic_and_scoped_retry_preserves_unrelated_work,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine.open_session().await.expect("main session opens"),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('a','base'),('b','base')",
            &[],
        )
        .await
        .expect("seed commits");
        let before = head(&main).await;
        let source_id = "01930000-0000-7000-8000-000000000073";
        let target_id = "01930000-0000-7000-8000-000000000074";
        create_branch(&main, source_id, &before).await;
        create_branch(&main, target_id, &before).await;

        let source = sim.wrap_session(
            engine
                .open_session_at(source_id)
                .await
                .expect("source session opens"),
            &engine,
        );
        source
            .execute(
                "UPDATE lix_key_value SET value = 'source-a' WHERE key = 'a'",
                &[],
            )
            .await
            .expect("source a edit commits");
        source
            .execute(
                "UPDATE lix_key_value SET value = 'source-b' WHERE key = 'b'",
                &[],
            )
            .await
            .expect("source b edit commits");
        let after = head(&source).await;

        let target = sim.wrap_session(
            engine
                .open_session_at(target_id)
                .await
                .expect("target session opens"),
            &engine,
        );
        target
            .execute(
                "UPDATE lix_key_value SET value = 'conflict-a' WHERE key = 'a'",
                &[],
            )
            .await
            .expect("conflicting target edit commits");
        target
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('unrelated','keep')",
                &[],
            )
            .await
            .expect("unrelated target edit commits");
        let before_conflict = head(&target).await;

        let error = target
            .execute(
                "SELECT commit_id FROM lix_apply($1,$2)",
                &[Value::Text(before.clone()), Value::Text(after.clone())],
            )
            .await
            .expect_err("conflicting full apply must fail");
        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert_eq!(head(&target).await, before_conflict);
        assert_eq!(
            select_rows(
                &target,
                "SELECT key,value FROM lix_key_value WHERE key IN ('a','b','unrelated') ORDER BY key",
            )
            .await,
            vec![
                vec![
                    Value::Text("a".into()),
                    Value::Jsonb(serde_json::json!("conflict-a").into()),
                ],
                vec![
                    Value::Text("b".into()),
                    Value::Jsonb(serde_json::json!("base").into()),
                ],
                vec![
                    Value::Text("unrelated".into()),
                    Value::Jsonb(serde_json::json!("keep").into()),
                ],
            ]
        );

        let applied = receipt(
            &target,
            "SELECT commit_id FROM lix_apply($1,$2,ARRAY[lix_row_ref('lix_key_value','b')])",
            &[Value::Text(before), Value::Text(after)],
        )
        .await
        .expect("disjoint scoped apply succeeds");
        assert_eq!(
            select_rows(
                &target,
                "SELECT key,value FROM lix_key_value WHERE key IN ('a','b','unrelated') ORDER BY key",
            )
            .await,
            vec![
                vec![
                    Value::Text("a".into()),
                    Value::Jsonb(serde_json::json!("conflict-a").into()),
                ],
                vec![
                    Value::Text("b".into()),
                    Value::Jsonb(serde_json::json!("source-b").into()),
                ],
                vec![
                    Value::Text("unrelated".into()),
                    Value::Jsonb(serde_json::json!("keep").into()),
                ],
            ]
        );
        let undone = receipt(&target, "SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("apply commit is undoable");
        assert_ne!(undone, applied);
        assert_eq!(
            select_rows(
                &target,
                "SELECT key,value FROM lix_key_value WHERE key IN ('a','b','unrelated') ORDER BY key",
            )
            .await,
            vec![
                vec![
                    Value::Text("a".into()),
                    Value::Jsonb(serde_json::json!("conflict-a").into()),
                ],
                vec![
                    Value::Text("b".into()),
                    Value::Jsonb(serde_json::json!("base").into()),
                ],
                vec![
                    Value::Text("unrelated".into()),
                    Value::Jsonb(serde_json::json!("keep").into()),
                ],
            ]
        );
    }
);
