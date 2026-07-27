use lix_engine::{
    BranchDiff, BranchDiffOptions, ChangeProposalState, CreateBranchOptions,
    CreateChangeProposalOptions, Engine, LixError, MergeBranchOutcome, Value,
};
use serde_json::{Value as JsonValue, json};

use crate::support::simulation_test::engine::{SimSession, Simulation};

simulation_test!(
    change_proposal_pins_a_directional_review_diff,
    |sim| async move {
        let (engine, main, draft) = create_diverged_draft(&sim).await;
        let source_head_before = engine
            .load_branch_head_commit_id("draft-branch")
            .await
            .expect("source head should load")
            .expect("source branch should exist");
        let target_head_before = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("target head should load")
            .expect("target branch should exist");

        let proposal = draft
            .create_change_proposal(CreateChangeProposalOptions {
                id: Some("proposal-pinned".to_string()),
                source_branch_id: "draft-branch".to_string(),
                target_branch_id: sim.main_branch_id().to_string(),
            })
            .await
            .expect("proposal should be created");
        assert_eq!(proposal.state, ChangeProposalState::Open);
        assert_eq!(proposal.source_head_commit_id, source_head_before);
        assert_eq!(proposal.target_head_commit_id, target_head_before);

        let proposal_rows = main
            .execute(
                "SELECT id, state, source_branch_id, target_branch_id, \
                        source_head_commit_id, target_head_commit_id \
                 FROM lix_change_proposal WHERE id = ?",
                &[Value::Text(proposal.id.clone())],
            )
            .await
            .expect("proposal should be visible as a global SQL entity");
        assert_eq!(proposal_rows.len(), 1);
        assert_eq!(
            proposal_rows.rows()[0].values(),
            &[
                Value::Text(proposal.id.clone()),
                Value::Text("open".to_string()),
                Value::Text("draft-branch".to_string()),
                Value::Text(sim.main_branch_id().to_string()),
                Value::Text(source_head_before.clone()),
                Value::Text(target_head_before.clone()),
            ]
        );
        let dml_error = main
            .execute(
                "UPDATE lix_change_proposal SET state = 'accepted' WHERE id = 'proposal-pinned'",
                &[],
            )
            .await
            .expect_err("proposal SQL entity must remain lifecycle-controlled");
        assert_eq!(dml_error.code, LixError::CODE_READ_ONLY);

        let direct = draft
            .branch_diff(BranchDiffOptions {
                source_branch_id: "draft-branch".to_string(),
                target_branch_id: sim.main_branch_id().to_string(),
            })
            .await
            .expect("branch diff should succeed");
        assert_key_change(&direct, "source-only");
        assert_no_key_change(&direct, "target-only");

        let review = main
            .change_proposal_diff("proposal-pinned")
            .await
            .expect("proposal review should load");
        assert!(review.source_head_is_current);
        assert!(review.target_head_is_current);
        assert!(review.is_accept_ready);
        assert!(review.review.conflicts.is_empty());
        assert_key_change(&review.review, "source-only");
        assert_no_key_change(&review.review, "target-only");
        assert_eq!(review.review.source_head_commit_id, source_head_before);
        assert_eq!(review.review.target_head_commit_id, target_head_before);
    }
);

simulation_test!(
    change_proposal_sql_branch_diff_is_directional_and_pair_bounded,
    |sim| async move {
        let (engine, main, _draft) = create_diverged_draft(&sim).await;
        let source_head = engine
            .load_branch_head_commit_id("draft-branch")
            .await
            .expect("source head should load")
            .expect("source branch should exist");
        let target_head = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("target head should load")
            .expect("target branch should exist");
        let params = [
            Value::Text("draft-branch".to_string()),
            Value::Text(sim.main_branch_id().to_string()),
        ];
        let rows = main
            .execute(
                "SELECT entity_pk, schema_key, change_kind, \
                        source_head_commit_id, target_head_commit_id \
                 FROM lix_branch_diff \
                 WHERE source_branch_id = ? AND target_branch_id = ?",
                &params,
            )
            .await
            .expect("pair-bounded review SQL should succeed");

        assert!(rows.rows().iter().any(|row| {
            row.values()
                == &[
                    Value::Json(json!(["source-only"])),
                    Value::Text("lix_key_value".to_string()),
                    Value::Text("added".to_string()),
                    Value::Text(source_head.clone()),
                    Value::Text(target_head.clone()),
                ]
        }));
        assert!(!rows.rows().iter().any(|row| {
            row.values()
                .first()
                .is_some_and(|value| value == &Value::Json(json!(["target-only"])))
        }));

        let error = main
            .execute(
                "SELECT * FROM lix_branch_diff WHERE source_branch_id = 'draft-branch'",
                &[],
            )
            .await
            .expect_err("branch review SQL must require an exact target branch");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    }
);

simulation_test!(
    change_proposal_sql_merge_conflicts_match_review_analysis,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );
        main.create_branch(CreateBranchOptions {
            id: Some("conflict-draft".to_string()),
            name: "Conflict draft".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("draft branch should be created");
        let draft = main.wrap_session(
            engine
                .open_session("conflict-draft")
                .await
                .expect("draft session should open"),
            &engine,
        );
        draft
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('same-key', 'source')",
                &[],
            )
            .await
            .expect("source should change the row");
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('same-key', 'target')",
            &[],
        )
        .await
        .expect("target should change the row");

        let rows = main
            .execute(
                "SELECT conflict_kind, entity_pk, schema_key, \
                        target_change_kind, source_change_kind \
                 FROM lix_branch_merge_conflict \
                 WHERE source_branch_id = ? AND target_branch_id = ?",
                &[
                    Value::Text("conflict-draft".to_string()),
                    Value::Text(sim.main_branch_id().to_string()),
                ],
            )
            .await
            .expect("conflict SQL should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows.rows()[0].values(),
            &[
                Value::Text("same_entity_changed".to_string()),
                Value::Json(json!(["same-key"])),
                Value::Text("lix_key_value".to_string()),
                Value::Text("added".to_string()),
                Value::Text("added".to_string()),
            ]
        );
    }
);

simulation_test!(
    change_proposal_accept_merges_and_resolves_atomically,
    |sim| async move {
        let (engine, main, draft) = create_diverged_draft(&sim).await;
        let source_head_before = engine
            .load_branch_head_commit_id("draft-branch")
            .await
            .expect("source head should load")
            .expect("source branch should exist");
        let proposal = draft
            .create_change_proposal(CreateChangeProposalOptions {
                id: Some("proposal-accept".to_string()),
                source_branch_id: "draft-branch".to_string(),
                target_branch_id: sim.main_branch_id().to_string(),
            })
            .await
            .expect("proposal should be created");

        let accepted = main
            .accept_change_proposal(&proposal.id)
            .await
            .expect("target session should accept the proposal");
        assert_eq!(accepted.proposal.state, ChangeProposalState::Accepted);
        assert_eq!(accepted.merge.outcome, MergeBranchOutcome::MergeCommitted);
        assert_eq!(
            accepted.proposal.accepted_target_head_commit_id,
            Some(accepted.merge.target_head_after_commit_id.clone())
        );
        assert_eq!(
            engine
                .load_branch_head_commit_id("draft-branch")
                .await
                .expect("source head should load"),
            Some(source_head_before)
        );
        assert_eq!(
            engine
                .load_branch_head_commit_id(sim.main_branch_id())
                .await
                .expect("target head should load"),
            Some(accepted.merge.target_head_after_commit_id.clone())
        );
        assert_key_value(&main, "source-only", Some("\"source\"")).await;
        assert_key_value(&main, "target-only", Some("\"target\"")).await;

        let reopened_engine = sim
            .reboot_engine_from_current_snapshot()
            .await
            .expect("engine should reopen");
        let reopened_main = sim.wrap_session(
            reopened_engine
                .open_session(sim.main_branch_id())
                .await
                .expect("reopened target session should open"),
            &reopened_engine,
        );
        let persisted = reopened_main
            .get_change_proposal(&proposal.id)
            .await
            .expect("proposal lookup should succeed")
            .expect("accepted proposal should remain durable");
        assert_eq!(persisted.state, ChangeProposalState::Accepted);
        assert_eq!(
            persisted.accepted_target_head_commit_id,
            Some(accepted.merge.target_head_after_commit_id)
        );
    }
);

simulation_test!(
    change_proposal_reject_retains_source_and_releases_pair,
    |sim| async move {
        let (engine, main, draft) = create_diverged_draft(&sim).await;
        let source_head_before = engine
            .load_branch_head_commit_id("draft-branch")
            .await
            .expect("source head should load");
        let target_head_before = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("target head should load");
        let proposal = draft
            .create_change_proposal(CreateChangeProposalOptions {
                id: Some("proposal-reject".to_string()),
                source_branch_id: "draft-branch".to_string(),
                target_branch_id: sim.main_branch_id().to_string(),
            })
            .await
            .expect("proposal should be created");
        let created_tracking = proposal_tracking_metadata(&main, &proposal.id).await;

        let rejected = main
            .reject_change_proposal(&proposal.id)
            .await
            .expect("proposal should reject");
        assert_eq!(rejected.state, ChangeProposalState::Rejected);
        assert_eq!(rejected.accepted_target_head_commit_id, None);
        let rejected_tracking = proposal_tracking_metadata(&main, &proposal.id).await;
        assert_eq!(rejected_tracking.created_at, created_tracking.created_at);
        assert_ne!(rejected_tracking.change_id, created_tracking.change_id);
        assert!(!rejected_tracking.updated_at.is_empty());
        assert_eq!(
            engine
                .load_branch_head_commit_id("draft-branch")
                .await
                .expect("source head should load"),
            source_head_before
        );
        assert_eq!(
            engine
                .load_branch_head_commit_id(sim.main_branch_id())
                .await
                .expect("target head should load"),
            target_head_before
        );

        let replacement = main
            .create_change_proposal(CreateChangeProposalOptions {
                id: Some("proposal-replacement".to_string()),
                source_branch_id: "draft-branch".to_string(),
                target_branch_id: sim.main_branch_id().to_string(),
            })
            .await
            .expect("rejected proposal should release the ordered-pair slot");
        assert_eq!(replacement.state, ChangeProposalState::Open);
    }
);

simulation_test!(
    change_proposal_rejects_the_global_control_branch,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(
            engine
                .open_session(sim.main_branch_id())
                .await
                .expect("main session should open"),
            &engine,
        );

        for (source_branch_id, target_branch_id) in [
            ("global", sim.main_branch_id()),
            (sim.main_branch_id(), "global"),
        ] {
            let error = main
                .create_change_proposal(CreateChangeProposalOptions {
                    id: None,
                    source_branch_id: source_branch_id.to_string(),
                    target_branch_id: target_branch_id.to_string(),
                })
                .await
                .expect_err("global control branch must not be proposal input");
            assert_eq!(error.code, LixError::CODE_INVALID_MERGE);
        }
    }
);

simulation_test!(
    change_proposal_stale_accept_does_not_mutate_target,
    |sim| async move {
        let (engine, main, draft) = create_diverged_draft(&sim).await;
        let proposal = draft
            .create_change_proposal(CreateChangeProposalOptions {
                id: Some("proposal-stale".to_string()),
                source_branch_id: "draft-branch".to_string(),
                target_branch_id: sim.main_branch_id().to_string(),
            })
            .await
            .expect("proposal should be created");
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('target-after-review', 'later')",
            &[],
        )
        .await
        .expect("target should advance after review starts");
        let target_head_after_advance = engine
            .load_branch_head_commit_id(sim.main_branch_id())
            .await
            .expect("target head should load");

        let error = main
            .accept_change_proposal(&proposal.id)
            .await
            .expect_err("accepting a stale proposal should fail");
        assert_eq!(error.code, LixError::CODE_CHANGE_PROPOSAL_STALE);
        assert_eq!(
            engine
                .load_branch_head_commit_id(sim.main_branch_id())
                .await
                .expect("target head should load"),
            target_head_after_advance
        );
        assert_key_value(&main, "source-only", None).await;
        assert_key_value(&main, "target-after-review", Some("\"later\"")).await;
        assert_eq!(
            main.get_change_proposal(&proposal.id)
                .await
                .expect("proposal lookup should succeed")
                .expect("proposal should remain durable")
                .state,
            ChangeProposalState::Open
        );
    }
);

async fn create_diverged_draft(sim: &Simulation) -> (Engine, SimSession, SimSession) {
    let engine = sim.boot_engine().await;
    let main = sim.wrap_session(
        engine
            .open_session(sim.main_branch_id())
            .await
            .expect("main session should open"),
        &engine,
    );
    main.create_branch(CreateBranchOptions {
        id: Some("draft-branch".to_string()),
        name: "Draft".to_string(),
        from_commit_id: None,
    })
    .await
    .expect("draft branch should be created");
    let draft = main.wrap_session(
        engine
            .open_session("draft-branch")
            .await
            .expect("draft session should open"),
        &engine,
    );
    draft
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('source-only', 'source')",
            &[],
        )
        .await
        .expect("source write should succeed");
    main.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('target-only', 'target')",
        &[],
    )
    .await
    .expect("target write should succeed");
    (engine, main, draft)
}

fn assert_key_change(diff: &BranchDiff, key: &str) {
    assert!(
        diff.changes.iter().any(|change| {
            change.schema_key == "lix_key_value" && change.entity_pk == json!([key])
        }),
        "expected review diff to include key '{key}', got {diff:#?}"
    );
}

fn assert_no_key_change(diff: &BranchDiff, key: &str) {
    assert!(
        !diff.changes.iter().any(|change| {
            change.schema_key == "lix_key_value" && change.entity_pk == json!([key])
        }),
        "review diff unexpectedly included key '{key}', got {diff:#?}"
    );
}

async fn assert_key_value(session: &SimSession, key: &str, expected: Option<&str>) {
    let rows = session
        .execute(
            &format!("SELECT value FROM lix_key_value WHERE key = '{key}'"),
            &[],
        )
        .await
        .expect("key-value query should succeed");
    match expected {
        Some(value) => {
            let expected = serde_json::from_str::<JsonValue>(value)
                .expect("expected value should be valid JSON");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows.rows()[0].values(), &[Value::Json(expected)]);
        }
        None => assert_eq!(rows.len(), 0),
    }
}

struct ProposalTrackingMetadata {
    created_at: String,
    updated_at: String,
    change_id: String,
}

async fn proposal_tracking_metadata(
    session: &SimSession,
    proposal_id: &str,
) -> ProposalTrackingMetadata {
    let rows = session
        .execute(
            "SELECT lixcol_created_at, lixcol_updated_at, lixcol_change_id \
             FROM lix_change_proposal WHERE id = ?",
            &[Value::Text(proposal_id.to_string())],
        )
        .await
        .expect("proposal tracking metadata should be queryable");
    assert_eq!(rows.len(), 1);
    let values = rows.rows()[0].values();
    ProposalTrackingMetadata {
        created_at: expect_text(&values[0], "lixcol_created_at"),
        updated_at: expect_text(&values[1], "lixcol_updated_at"),
        change_id: expect_text(&values[2], "lixcol_change_id"),
    }
}

fn expect_text(value: &Value, column: &str) -> String {
    match value {
        Value::Text(value) => value.clone(),
        value => panic!("{column} should be text, got {value:?}"),
    }
}
