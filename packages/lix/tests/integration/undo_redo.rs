//! Public SQL recovery scenarios derived from jj's operation-stack tests and
//! Git/Dolt's inverse-vs-snapshot distinction. Checkpoint/row scope is Lix-specific.
use crate::support::simulation_test::engine::{SimSession as Lix, Simulation};
use lix::{CreateBranchOptions, LixError, MergeBranchOptions, MergeBranchOutcome, Value};

async fn open_sim(sim: &Simulation) -> Lix {
    let engine = sim.boot_engine().await;
    sim.wrap_session(engine.open_session().await.unwrap(), &engine)
}

async fn command(lix: &Lix, sql: &str, ids: &[&str]) -> Option<String> {
    let params = ids
        .iter()
        .map(|id| Value::Text((*id).into()))
        .collect::<Vec<_>>();
    let result = lix
        .execute(sql, &params)
        .await
        .unwrap_or_else(|error| panic!("{sql} {ids:?}: {error:?}"));
    assert_eq!(result.rows().len(), 1);
    match result.rows()[0].get::<Value>("commit_id").unwrap() {
        Value::Text(id) => Some(id),
        Value::Null => None,
        value => panic!("invalid receipt {value:?}"),
    }
}
async fn checkpoint(lix: &Lix) -> String {
    command(lix, "SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await
        .unwrap()
}
async fn baseline(lix: &Lix) -> String {
    lix.execute(
        "SELECT working_base_commit_id FROM lix_branch WHERE id = lix_active_branch_id()",
        &[],
    )
    .await
    .unwrap()
    .rows()[0]
        .get("working_base_commit_id")
        .unwrap()
}
async fn value(lix: &Lix, key: &str) -> Option<String> {
    lix.execute(
        "SELECT value FROM lix_key_value WHERE key = $1",
        &[Value::Text(key.into())],
    )
    .await
    .unwrap()
    .rows()
    .first()
    .and_then(|r| r.get::<Value>("value").ok())
    .and_then(|v| match v {
        Value::Jsonb(j) => Some(j.as_json_string().unwrap_or_else(|| j.as_str().to_owned())),
        Value::Text(s) => Some(s),
        _ => None,
    })
}
async fn fixture(sim: &Simulation) -> (Lix, String, String) {
    let lix = open_sim(sim).await;
    lix.execute(
        "INSERT INTO lix_key_value(key,value) VALUES ('x','0'),('y','0')",
        &[],
    )
    .await
    .unwrap();
    let b = checkpoint(&lix).await;
    lix.execute(
        "UPDATE lix_key_value SET value='1' WHERE key IN ('x','y')",
        &[],
    )
    .await
    .unwrap();
    let c = checkpoint(&lix).await;
    (lix, b, c)
}
const UNDO: &str = "SELECT commit_id FROM lix_undo($1)";
const REDO: &str = "SELECT commit_id FROM lix_redo($1)";
const UNDO_ROW: &str = "SELECT commit_id FROM lix_undo($1, ARRAY[lix_row_ref('lix_key_value',$2)])";
const REDO_ROW: &str = "SELECT commit_id FROM lix_redo($1, ARRAY[lix_row_ref('lix_key_value',$2)])";

simulation_test!(
    successful_explicit_undo_owns_transaction_mutation_slot,
    |sim| async move {
        let (lix, _, c) = fixture(&sim).await;
        let mut tx = lix.begin_transaction().await.unwrap();
        let undo = tx
            .execute(UNDO, &[Value::Text(c.clone())])
            .await
            .unwrap();
        let u = match undo.rows()[0].get::<Value>("commit_id").unwrap() {
            Value::Text(id) => id,
            value => panic!("expected undo receipt, got {value:?}"),
        };
        tx.execute(
            "SELECT value FROM lix_key_value WHERE key = 'x'",
            &[],
        )
        .await
        .unwrap();
        let error = tx
            .execute(
                "UPDATE lix_key_value SET value = 'later' WHERE key = 'x'",
                &[],
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE");
        tx.commit().await.unwrap();
        assert_eq!(value(&lix, "x").await.as_deref(), Some("0"));

        let mut redo_tx = lix.begin_transaction().await.unwrap();
        redo_tx
            .execute(REDO, &[Value::Text(u)])
            .await
            .unwrap();
        let error = redo_tx
            .execute(
                "UPDATE lix_key_value SET value = 'later' WHERE key = 'x'",
                &[],
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE");
        redo_tx.commit().await.unwrap();
        assert_eq!(value(&lix, "x").await.as_deref(), Some("1"));

        let mut no_op = lix.begin_transaction().await.unwrap();
        let result = no_op
            .execute(
                "SELECT commit_id FROM lix_undo($1, ARRAY[])",
                &[Value::Text(c)],
            )
            .await
            .unwrap();
        assert_eq!(
            result.rows()[0].get::<Value>("commit_id").unwrap(),
            Value::Null
        );
        no_op
            .execute(
                "UPDATE lix_key_value SET value = 'later' WHERE key = 'x'",
                &[],
            )
            .await
            .unwrap();
        no_op.commit().await.unwrap();
        assert_eq!(value(&lix, "x").await.as_deref(), Some("later"));
    }
);

simulation_test!(
    fast_forward_uses_selected_source_baseline_when_heads_are_shared,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(engine.open_session_at(sim.main_branch_id()).await.unwrap(), &engine);
        main.execute("INSERT INTO lix_key_value(key,value) VALUES ('shared-head','before')", &[]).await.unwrap();
        checkpoint(&main).await;
        main.execute("UPDATE lix_key_value SET value='after' WHERE key='shared-head'", &[]).await.unwrap();
        let c = checkpoint(&main).await;
        let author_id = "01930000-0000-7000-8000-000000000031";
        let selected_id = "01930000-0000-7000-8000-000000000032";
        main.create_branch(CreateBranchOptions { id: Some(author_id.into()), name: "undo author".into(), from_commit_id: Some(c.clone()) }).await.unwrap();
        let author = sim.wrap_session(engine.open_session_at(author_id).await.unwrap(), &engine);
        let receipt = command(&author, UNDO, &[&c]).await.unwrap();
        main.create_branch(CreateBranchOptions { id: Some(selected_id.into()), name: "selected merge source".into(), from_commit_id: Some(receipt.clone()) }).await.unwrap();
        let selected = sim.wrap_session(engine.open_session_at(selected_id).await.unwrap(), &engine);
        assert_eq!(baseline(&selected).await, receipt);
        assert_ne!(baseline(&author).await, receipt);
        let merge = main.merge_branch(MergeBranchOptions { source_branch_id: selected_id.into() }).await.unwrap();
        assert_eq!(merge.outcome, MergeBranchOutcome::FastForward);
        assert_eq!(baseline(&main).await, receipt);
        assert_eq!(value(&main, "shared-head").await.as_deref(), Some("before"));
    }
);

simulation_test!(
    fast_forward_after_source_undo_redo_preserves_source_baseline,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine.open_session_at(sim.main_branch_id()).await.unwrap(),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_key_value(key,value) VALUES ('ff-redo','before')",
            &[],
        )
        .await
        .unwrap();
        checkpoint(&main).await;
        main.execute(
            "UPDATE lix_key_value SET value='after' WHERE key='ff-redo'",
            &[],
        )
        .await
        .unwrap();
        let c = checkpoint(&main).await;
        let source_branch_id = "01930000-0000-7000-8000-000000000021";
        let target_branch_id = "01930000-0000-7000-8000-000000000022";
        for (id, name) in [
            (source_branch_id, "ff redo source"),
            (target_branch_id, "ff redo target"),
        ] {
            main.create_branch(CreateBranchOptions {
                id: Some(id.to_string()),
                name: name.to_string(),
                from_commit_id: Some(c.clone()),
            })
            .await
            .unwrap();
        }
        let source = sim.wrap_session(
            engine.open_session_at(source_branch_id).await.unwrap(),
            &engine,
        );
        let target = sim.wrap_session(
            engine.open_session_at(target_branch_id).await.unwrap(),
            &engine,
        );
        let undo_receipt = command(&source, UNDO, &[&c]).await.unwrap();
        command(&source, REDO, &[&undo_receipt]).await.unwrap();
        assert_eq!(baseline(&source).await, c);
        assert_eq!(value(&source, "ff-redo").await.as_deref(), Some("after"));
        let merge = target
            .merge_branch(MergeBranchOptions {
                source_branch_id: source_branch_id.to_string(),
            })
            .await
            .unwrap();
        assert_eq!(merge.outcome, MergeBranchOutcome::FastForward);
        assert_eq!(baseline(&target).await, c);
        assert_eq!(value(&target, "ff-redo").await.as_deref(), Some("after"));
        assert!(
            command(&target, "SELECT commit_id FROM lix_undo()", &[])
                .await
                .is_none()
        );
    }
);

simulation_test!(
    fast_forward_adopts_source_undo_baseline_and_foreign_marker_is_floor,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine.open_session_at(sim.main_branch_id()).await.unwrap(),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_key_value(key,value) VALUES ('ff-undo','before')",
            &[],
        )
        .await
        .unwrap();
        let b = checkpoint(&main).await;
        main.execute(
            "UPDATE lix_key_value SET value='after' WHERE key='ff-undo'",
            &[],
        )
        .await
        .unwrap();
        let c = checkpoint(&main).await;
        let source_branch_id = "01930000-0000-7000-8000-000000000011";
        let target_branch_id = "01930000-0000-7000-8000-000000000012";
        main.create_branch(CreateBranchOptions {
            id: Some(source_branch_id.to_string()),
            name: "ff undo source".to_string(),
            from_commit_id: Some(c.clone()),
        })
        .await
        .unwrap();
        main.create_branch(CreateBranchOptions {
            id: Some(target_branch_id.to_string()),
            name: "ff undo target".to_string(),
            from_commit_id: Some(c.clone()),
        })
        .await
        .unwrap();
        let source = sim.wrap_session(
            engine.open_session_at(source_branch_id).await.unwrap(),
            &engine,
        );
        let target = sim.wrap_session(
            engine.open_session_at(target_branch_id).await.unwrap(),
            &engine,
        );
        let source_receipt = command(&source, UNDO, &[&c]).await.unwrap();
        assert_eq!(baseline(&source).await, b);
        let merge = target
            .merge_branch(MergeBranchOptions {
                source_branch_id: source_branch_id.to_string(),
            })
            .await
            .unwrap();
        assert_eq!(merge.outcome, MergeBranchOutcome::FastForward);
        assert_eq!(baseline(&target).await, b);
        assert_eq!(value(&target, "ff-undo").await.as_deref(), Some("before"));
        let checkpoint_rows = target
            .execute(
                "SELECT is_checkpoint FROM lix_log($1) WHERE commit_id=$2",
                &[Value::Text(source_receipt), Value::Text(c.clone())],
            )
            .await
            .unwrap();
        assert_eq!(checkpoint_rows.rows().len(), 1);
        assert!(
            !checkpoint_rows.rows()[0]
                .get::<bool>("is_checkpoint")
                .unwrap()
        );
        // The source marker belongs to another branch. It must reset the
        // target's editor cursor to a floor rather than replaying source
        // history as a target-local undo action.
        assert!(
            command(&target, "SELECT commit_id FROM lix_undo()", &[])
                .await
                .is_none()
        );
    }
);

simulation_test!(
    whole_checkpoint_undo_redo_preserves_history_and_moves_baseline,
    |sim| async move {
        let (lix, b, c) = fixture(&sim).await;
        let u = command(&lix, UNDO, &[&c]).await.unwrap();
        assert_ne!(u, c);
        assert_eq!(baseline(&lix).await, b);
        assert_eq!(value(&lix, "x").await.as_deref(), Some("0"));
        assert_eq!(
            lix.execute(
                "SELECT id FROM lix_commit WHERE id=$1",
                &[Value::Text(c.clone())]
            )
            .await
            .unwrap()
            .rows()
            .len(),
            1
        );
        assert!(
            lix.execute("SELECT * FROM lix_diff('lix_key_value')", &[])
                .await
                .unwrap()
                .rows()
                .is_empty()
        );
        command(&lix, REDO, &[&u]).await.unwrap();
        assert_eq!(baseline(&lix).await, c);
        assert_eq!(value(&lix, "x").await.as_deref(), Some("1"));
    }
);

simulation_test!(
    disjoint_partial_cycles_complete_checkpoint_and_consumed_receipts_stay_consumed,
    |sim| async move {
        let (lix, b, c) = fixture(&sim).await;
        let u1 = command(&lix, UNDO_ROW, &[&c, "x"]).await.unwrap();
        assert_eq!(baseline(&lix).await, c);
        let u2 = command(&lix, UNDO_ROW, &[&c, "y"]).await.unwrap();
        assert_eq!(baseline(&lix).await, b);
        command(&lix, REDO_ROW, &[&u1, "x"]).await.unwrap();
        assert_eq!(baseline(&lix).await, b);
        let u3 = command(&lix, UNDO_ROW, &[&c, "x"]).await.unwrap();
        assert!(command(&lix, REDO_ROW, &[&u1, "x"]).await.is_none());
        assert_eq!(value(&lix, "x").await.as_deref(), Some("0"));
        command(&lix, REDO, &[&u3]).await.unwrap();
        assert_eq!(baseline(&lix).await, b);
        command(&lix, REDO, &[&u2]).await.unwrap();
        assert_eq!(baseline(&lix).await, c);
    }
);

simulation_test!(
    partial_redo_retains_remainder_for_editor_redo,
    |sim| async move {
        let (lix, b, c) = fixture(&sim).await;
        let u = command(&lix, UNDO, &[&c]).await.unwrap();
        command(&lix, REDO_ROW, &[&u, "x"]).await.unwrap();
        assert_eq!(baseline(&lix).await, b);
        command(&lix, "SELECT commit_id FROM lix_redo()", &[])
            .await
            .unwrap();
        assert_eq!(baseline(&lix).await, c);
        assert_eq!(value(&lix, "y").await.as_deref(), Some("1"));
        assert!(command(&lix, REDO, &[&u]).await.is_none());
    }
);

simulation_test!(
    empty_checkpoint_has_metadata_only_undo_and_redo,
    |sim| async move {
        let (lix, _, _) = fixture(&sim).await;
        let b = baseline(&lix).await;
        let c = checkpoint(&lix).await;
        assert!(
            command(&lix, "SELECT commit_id FROM lix_undo($1, ARRAY[])", &[&c])
                .await
                .is_none()
        );
        assert_eq!(baseline(&lix).await, c);
        let u = command(&lix, UNDO, &[&c]).await.unwrap();
        assert_eq!(baseline(&lix).await, b);
        assert!(
            command(&lix, "SELECT commit_id FROM lix_redo($1, ARRAY[])", &[&u])
                .await
                .is_none()
        );
        command(&lix, REDO, &[&u]).await.unwrap();
        assert_eq!(baseline(&lix).await, c);
    }
);

simulation_test!(
    later_conflicting_rows_abort_whole_undo_but_unrelated_scope_succeeds,
    |sim| async move {
        let (lix, _, c) = fixture(&sim).await;
        lix.execute("UPDATE lix_key_value SET value='later' WHERE key='x'", &[])
            .await
            .unwrap();
        assert!(lix.execute(UNDO, &[Value::Text(c.clone())]).await.is_err());
        assert_eq!(baseline(&lix).await, c);
        assert_eq!(value(&lix, "y").await.as_deref(), Some("1"));
        command(&lix, UNDO_ROW, &[&c, "y"]).await.unwrap();
        assert_eq!(value(&lix, "x").await.as_deref(), Some("later"));
        assert_eq!(baseline(&lix).await, c);
    }
);

simulation_test!(
    newer_checkpoint_blocks_old_checkpoint_redo_including_selected_rows,
    |sim| async move {
        let (lix, _, c) = fixture(&sim).await;
        let u = command(&lix, UNDO, &[&c]).await.unwrap();
        let d = checkpoint(&lix).await;
        assert!(
            command(&lix, "SELECT commit_id FROM lix_redo()", &[])
                .await
                .is_none()
        );
        let error = lix
            .execute(REDO_ROW, &[Value::Text(u), Value::Text("x".into())])
            .await
            .unwrap_err();
        assert_eq!(error.code, "LIX_STALE_CHECKPOINT_UNDO");
        assert_eq!(baseline(&lix).await, d);
    }
);

simulation_test!(
    receipt_roles_null_scope_and_filtered_query_are_explicit,
    |sim| async move {
        let (lix, _, c) = fixture(&sim).await;
        assert!(lix.execute(REDO, &[Value::Text(c.clone())]).await.is_err());
        assert!(
            lix.execute(
                "SELECT commit_id FROM lix_undo($1, NULL)",
                &[Value::Text(c.clone())]
            )
            .await
            .is_err()
        );
        let u=command(&lix,"SELECT commit_id FROM lix_undo($1, ARRAY(SELECT row_ref FROM lix_diff('lix_key_value', $2, $1) WHERE key='x'))",&[&c,&baseline(&lix).await]).await;
        // Same endpoint query produces no rows and must not alter metadata.
        assert!(u.is_none());
        assert_eq!(baseline(&lix).await, c);
        let u = command(&lix, UNDO_ROW, &[&c, "x"]).await.unwrap();
        assert!(lix.execute(UNDO, &[Value::Text(u)]).await.is_err());
    }
);

simulation_test!(
    jj_new_edits_skip_old_undo_stack_and_adjacent_redo_runs,
    |sim| async move {
        let lix = open_sim(&sim).await;
        checkpoint(&lix).await;
        for key in ["a", "b", "c", "d"] {
            lix.execute(
                "INSERT INTO lix_key_value(key,value) VALUES ($1,$1)",
                &[Value::Text(key.into())],
            )
            .await
            .unwrap();
        }
        for _ in 0..2 {
            command(&lix, "SELECT commit_id FROM lix_undo()", &[])
                .await
                .unwrap();
        }
        for key in ["e", "f"] {
            lix.execute(
                "INSERT INTO lix_key_value(key,value) VALUES ($1,$1)",
                &[Value::Text(key.into())],
            )
            .await
            .unwrap();
        }
        assert!(
            command(&lix, "SELECT commit_id FROM lix_redo()", &[])
                .await
                .is_none()
        );
        for key in ["f", "e", "b", "a"] {
            command(&lix, "SELECT commit_id FROM lix_undo()", &[])
                .await
                .unwrap();
            assert_eq!(value(&lix, key).await, None);
        }
        assert!(
            command(&lix, "SELECT commit_id FROM lix_undo()", &[])
                .await
                .is_none()
        );
        for _ in 0..4 {
            command(&lix, "SELECT commit_id FROM lix_redo()", &[])
                .await
                .unwrap();
        }
        for _ in 0..4 {
            command(&lix, "SELECT commit_id FROM lix_undo()", &[])
                .await
                .unwrap();
        }
        for _ in 0..4 {
            command(&lix, "SELECT commit_id FROM lix_redo()", &[])
                .await
                .unwrap();
        }
        assert_eq!(value(&lix, "f").await.as_deref(), Some("f"));
        assert_eq!(value(&lix, "c").await, None);
    }
);

simulation_test!(
    restore_and_revert_are_ordinary_undoable_actions,
    |sim| async move {
        let (lix, b, c) = fixture(&sim).await;
        let r = command(&lix, "SELECT commit_id FROM lix_restore($1)", &[&b])
            .await
            .unwrap();
        assert_eq!(baseline(&lix).await, c);
        let u = command(&lix, UNDO, &[&r]).await.unwrap();
        assert_eq!(value(&lix, "x").await.as_deref(), Some("1"));
        command(&lix, REDO, &[&u]).await.unwrap();
        assert_eq!(value(&lix, "x").await.as_deref(), Some("0"));
    }
);

simulation_test!(
    selected_ordinary_undo_keeps_remaining_effects_on_editor_stack,
    |sim| async move {
        let lix = open_sim(&sim).await;
        checkpoint(&lix).await;
        lix.execute(
            "INSERT INTO lix_key_value(key,value) VALUES ('x','1'),('y','1')",
            &[],
        )
        .await
        .unwrap();
        let action: String = lix
            .execute(
                "SELECT commit_id FROM lix_log() ORDER BY position LIMIT 1",
                &[],
            )
            .await
            .unwrap()
            .rows()[0]
            .get("commit_id")
            .unwrap();
        command(&lix, UNDO_ROW, &[&action, "x"]).await.unwrap();
        command(&lix, "SELECT commit_id FROM lix_undo()", &[])
            .await
            .unwrap();
        assert_eq!(value(&lix, "y").await, None);
        assert!(
            command(&lix, "SELECT commit_id FROM lix_undo()", &[])
                .await
                .is_none()
        );
    }
);

simulation_test!(
    explicit_out_of_order_redo_skips_consumed_receipts,
    |sim| async move {
        let lix = open_sim(&sim).await;
        checkpoint(&lix).await;
        for key in ["a", "b", "c"] {
            lix.execute(
                "INSERT INTO lix_key_value(key,value) VALUES ($1,$1)",
                &[Value::Text(key.into())],
            )
            .await
            .unwrap();
        }
        let uc = command(&lix, "SELECT commit_id FROM lix_undo()", &[])
            .await
            .unwrap();
        let ub = command(&lix, "SELECT commit_id FROM lix_undo()", &[])
            .await
            .unwrap();
        command(&lix, "SELECT commit_id FROM lix_undo()", &[])
            .await
            .unwrap();
        command(&lix, REDO, &[&ub]).await.unwrap();
        command(&lix, "SELECT commit_id FROM lix_redo()", &[])
            .await
            .unwrap();
        command(&lix, "SELECT commit_id FROM lix_redo()", &[])
            .await
            .unwrap();
        assert_eq!(value(&lix, "c").await.as_deref(), Some("c"));
        assert!(command(&lix, REDO, &[&uc]).await.is_none());
    }
);

simulation_test!(
    retired_checkpoint_stays_hidden_after_new_checkpoint_and_old_cycle_stays_stale,
    |sim| async move {
        let (lix, _, c) = fixture(&sim).await;
        let u = command(&lix, UNDO, &[&c]).await.unwrap();
        let d = checkpoint(&lix).await;
        let rows = lix
            .execute(
                "SELECT is_checkpoint FROM lix_log($1) WHERE commit_id=$2",
                &[Value::Text(u.clone()), Value::Text(c.clone())],
            )
            .await
            .unwrap();
        assert!(!rows.rows()[0].get::<bool>("is_checkpoint").unwrap());
        assert!(
            lix.execute(
                "SELECT commit_id FROM lix_log() WHERE is_checkpoint AND commit_id=$1",
                &[Value::Text(c)]
            )
            .await
            .unwrap()
            .rows()
            .is_empty()
        );
        command(&lix, UNDO, &[&d]).await.unwrap();
        let error = lix.execute(REDO, &[Value::Text(u)]).await.unwrap_err();
        assert_eq!(error.code, "LIX_STALE_CHECKPOINT_UNDO");
    }
);

simulation_test!(
    independent_checkpoint_undo_state_conflicts_atomically,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine.open_session_at(sim.main_branch_id()).await.unwrap(),
            &engine,
        );
        main.execute(
            "INSERT INTO lix_key_value(key,value) VALUES ('merge-undo-x','before')",
            &[],
        )
        .await
        .unwrap();
        checkpoint(&main).await;
        main.execute(
            "UPDATE lix_key_value SET value='after' WHERE key='merge-undo-x'",
            &[],
        )
        .await
        .unwrap();
        let checkpoint_id = checkpoint(&main).await;
        let source_branch_id = "01930000-0000-7000-8000-000000000001";
        main.create_branch(CreateBranchOptions {
            id: Some(source_branch_id.to_string()),
            name: "Independent undo source".to_string(),
            from_commit_id: Some(checkpoint_id.clone()),
        })
        .await
        .unwrap();
        let source = sim.wrap_session(
            engine.open_session_at(source_branch_id).await.unwrap(),
            &engine,
        );

        let target_receipt = command(&main, UNDO, &[&checkpoint_id]).await.unwrap();
        let source_receipt = command(&source, UNDO, &[&checkpoint_id]).await.unwrap();
        assert_ne!(
            target_receipt, source_receipt,
            "independent undo operations must have distinct receipts"
        );

        let target_head_before = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .unwrap()
            .unwrap();
        let target_baseline_before = baseline(&main).await;
        let target_value_before = value(&main, "merge-undo-x").await;
        let source_head_before = engine
            .load_branch_head_commit_id(source_branch_id)
            .await
            .unwrap()
            .unwrap();
        let source_baseline_before = baseline(&source).await;

        let error = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: source_branch_id.to_string(),
            })
            .await
            .unwrap_err();
        assert_eq!(error.code, LixError::CODE_MERGE_CONFLICT);
        assert!(
            error.message.contains("undo state"),
            "unexpected merge error: {error:?}"
        );
        assert_eq!(
            engine
                .load_branch_head_commit_id(sim.main_branch_id())
                .await
                .unwrap()
                .as_deref(),
            Some(target_head_before.as_str()),
            "rejected metadata merge must not move the target head"
        );
        assert_eq!(baseline(&main).await, target_baseline_before);
        assert_eq!(value(&main, "merge-undo-x").await, target_value_before);
        assert_eq!(
            engine
                .load_branch_head_commit_id(source_branch_id)
                .await
                .unwrap()
                .as_deref(),
            Some(source_head_before.as_str()),
            "rejected metadata merge must not move the source head"
        );
        assert_eq!(baseline(&source).await, source_baseline_before);
    }
);

simulation_test!(
    checkpoint_undo_keeps_unrelated_working_edits_and_packed_rows,
    |sim| async move {
        let lix = open_sim(&sim).await;
        let values = (0..600)
            .map(|i| format!("('stable_{i}', '{i}')"))
            .collect::<Vec<_>>()
            .join(",");
        lix.execute(
            &format!("INSERT INTO lix_key_value(key,value) VALUES {values}"),
            &[],
        )
        .await
        .unwrap();
        let b = checkpoint(&lix).await;
        lix.execute("INSERT INTO lix_key_value(key,value) VALUES ('x','1')", &[])
            .await
            .unwrap();
        let c = checkpoint(&lix).await;
        lix.execute(
            "INSERT INTO lix_key_value(key,value) VALUES ('later','2')",
            &[],
        )
        .await
        .unwrap();
        command(&lix, UNDO, &[&c]).await.unwrap();
        assert_eq!(baseline(&lix).await, b);
        assert_eq!(value(&lix, "later").await.as_deref(), Some("2"));
        let stable = lix
            .execute(
                "SELECT count(*) AS n FROM lix_key_value WHERE key LIKE 'stable_%'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(stable.rows()[0].get::<i64>("n").unwrap(), 600);
        let dirty = lix
            .execute(
                "SELECT key FROM lix_diff('lix_key_value') WHERE key='later'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(dirty.rows().len(), 1);
    }
);

simulation_test!(
    retiring_latest_checkpoint_allows_fresh_undo_of_previous_baseline,
    |sim| async move {
        let (lix, b, c) = fixture(&sim).await;
        let uc = command(&lix, UNDO, &[&c]).await.unwrap();
        let ub = command(&lix, UNDO, &[&b]).await.unwrap();
        assert_eq!(value(&lix, "x").await, None);
        command(&lix, REDO, &[&ub]).await.unwrap();
        assert_eq!(baseline(&lix).await, b);
        command(&lix, REDO, &[&uc]).await.unwrap();
        assert_eq!(baseline(&lix).await, c);
    }
);
