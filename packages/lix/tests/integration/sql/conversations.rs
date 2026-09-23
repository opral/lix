use lix::{CreateBranchOptions, MergeBranchOptions, MergeBranchPreviewOptions, Value};
use serde_json::json;

use super::assert_rows_eq;

const FILE_PARAGRAPH: &str = "01950000-0000-7000-8000-000000000101";
const FILE_CSV: &str = "01950000-0000-7000-8000-000000000102";
const FILE_TARGET: &str = "01950000-0000-7000-8000-000000000103";
const LOCAL_PARAGRAPH_CONVERSATION: &str = "01950000-0000-7000-8000-000000000201";
const LOCAL_CSV_CONVERSATION: &str = "01950000-0000-7000-8000-000000000202";
const LOCAL_FILE_CONVERSATION: &str = "01950000-0000-7000-8000-000000000203";
const STANDALONE_CONVERSATION: &str = "01950000-0000-7000-8000-000000000204";
const PARAGRAPH_COMMENT: &str = "01950000-0000-7000-8000-000000000301";
const CSV_COMMENT: &str = "01950000-0000-7000-8000-000000000302";
const FILE_COMMENT: &str = "01950000-0000-7000-8000-000000000303";
const STANDALONE_COMMENT: &str = "01950000-0000-7000-8000-000000000304";

const COPY_COMMENT: &str = "01950000-0000-7000-8000-000000000305";

const TARGET_CONVERSATION: &str = "01950000-0000-7000-8000-000000000402";
const TARGET_COMMENT: &str = "01950000-0000-7000-8000-000000000403";

const GLOBAL_CONVERSATION: &str = "01950000-0000-7000-8000-000000000501";
const GLOBAL_COMMENT: &str = "01950000-0000-7000-8000-000000000502";
const GENERATION_CONVERSATION: &str = "01950000-0000-7000-8000-000000000701";
const GENERATION_COMMENT: &str = "01950000-0000-7000-8000-000000000702";

const BODY: &str = r#"{"_type":"zettel_doc","blocks":[{"_type":"zettel_block","_key":"p1","style":"normal","markDefs":[],"children":[{"_type":"zettel_span","_key":"s1","text":"A stored reply","marks":[]}]}]}"#;

type SimSession = crate::support::simulation_test::engine::SimSession;

async fn register_local_target_schema(session: &SimSession, key: &str) {
    let schema = json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": key,
        "columns": [
            {"name":"id", "type":"text", "nullable":false},
            {"name":"label", "type":"text", "nullable":false}
        ],
        "primary_key": ["id"]
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("local conversation target schema should register");
}

async fn install_local_targets(session: &SimSession) {
    register_local_target_schema(session, "conversation_paragraph_target").await;
    register_local_target_schema(session, "conversation_csv_target").await;
    register_local_target_schema(session, "conversation_custom_target").await;
    session
        .execute(
            "INSERT INTO lix_file(id,path,content) VALUES
             ($1,'/conversation-paragraph.md',CAST('Paragraph one' AS BYTEA)),
             ($2,'/conversation-data.csv',CAST('name,value\\nAda,1' AS BYTEA)),
             ($3,'/conversation-target.txt',CAST('Target' AS BYTEA))",
            &[
                Value::Text(FILE_PARAGRAPH.into()),
                Value::Text(FILE_CSV.into()),
                Value::Text(FILE_TARGET.into()),
            ],
        )
        .await
        .expect("target files should insert");
    session
        .execute(
            "INSERT INTO conversation_paragraph_target(id,label,lixcol_file_id) VALUES ('paragraph-1','Paragraph one',$1)",
            &[Value::Text(FILE_PARAGRAPH.into())],
        )
        .await
        .expect("paragraph target should insert");
    session
        .execute(
            "INSERT INTO conversation_csv_target(id,label,lixcol_file_id) VALUES ('csv-row-1','Ada,1',$1)",
            &[Value::Text(FILE_CSV.into())],
        )
        .await
        .expect("CSV target should insert");
    session
        .execute(
            "INSERT INTO conversation_custom_target(id,label) VALUES ('custom-1','Custom target')",
            &[],
        )
        .await
        .expect("fileless custom target should insert");
}

async fn insert_local_conversations_and_comments(session: &SimSession) {
    session
        .execute(
            "INSERT INTO lix_conversation(id,target) VALUES
             ($1,lix_row_ref('conversation_paragraph_target',$5,'paragraph-1')),
             ($2,lix_row_ref('conversation_csv_target',$6,'csv-row-1')),
             ($3,lix_row_ref('lix_file',NULL,$7)),
             ($4,NULL)",
            &[
                Value::Text(LOCAL_PARAGRAPH_CONVERSATION.into()),
                Value::Text(LOCAL_CSV_CONVERSATION.into()),
                Value::Text(LOCAL_FILE_CONVERSATION.into()),
                Value::Text(STANDALONE_CONVERSATION.into()),
                Value::Text(FILE_PARAGRAPH.into()),
                Value::Text(FILE_CSV.into()),
                Value::Text(FILE_TARGET.into()),
            ],
        )
        .await
        .expect("built-in conversations should insert without registration");
    session
        .execute(
            "INSERT INTO lix_comment(id,conversation_id,body) VALUES
             ($1,$2,CAST($5 AS JSONB)),
             ($3,$4,CAST($5 AS JSONB)),
             ($6,$7,CAST($5 AS JSONB)),
             ($8,$9,CAST($5 AS JSONB))",
            &[
                Value::Text(PARAGRAPH_COMMENT.into()),
                Value::Text(LOCAL_PARAGRAPH_CONVERSATION.into()),
                Value::Text(CSV_COMMENT.into()),
                Value::Text(LOCAL_CSV_CONVERSATION.into()),
                Value::Text(BODY.into()),
                Value::Text(FILE_COMMENT.into()),
                Value::Text(LOCAL_FILE_CONVERSATION.into()),
                Value::Text(STANDALONE_COMMENT.into()),
                Value::Text(STANDALONE_CONVERSATION.into()),
            ],
        )
        .await
        .expect("JSONB comments should insert");
}

simulation_test!(
    conversation_builtins_are_available_without_registration,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);

        assert_rows_eq(
            session
                .execute(
                    "SELECT table_name FROM information_schema.tables
                 WHERE table_name IN ('lix_comment','lix_conversation') ORDER BY table_name",
                    &[],
                )
                .await
                .unwrap(),
            vec![
                vec![Value::Text("lix_comment".into())],
                vec![Value::Text("lix_conversation".into())],
            ],
        );

        let columns = session
            .execute(
                "SELECT table_name,column_name,data_type,is_nullable
             FROM information_schema.columns
             WHERE table_name IN ('lix_comment','lix_conversation')
               AND column_name IN ('id','target','title','conversation_id','body','author_id','order_key')
             ORDER BY table_name,ordinal_position",
                &[],
            )
            .await
            .unwrap();
        assert_rows_eq(
            columns,
            vec![
                vec![
                    Value::Text("lix_comment".into()),
                    Value::Text("id".into()),
                    Value::Text("UUID".into()),
                    Value::Text("NO".into()),
                ],
                vec![
                    Value::Text("lix_comment".into()),
                    Value::Text("conversation_id".into()),
                    Value::Text("UUID".into()),
                    Value::Text("NO".into()),
                ],
                vec![
                    Value::Text("lix_comment".into()),
                    Value::Text("body".into()),
                    Value::Text("JSONB".into()),
                    Value::Text("NO".into()),
                ],
                vec![
                    Value::Text("lix_conversation".into()),
                    Value::Text("id".into()),
                    Value::Text("UUID".into()),
                    Value::Text("NO".into()),
                ],
                vec![
                    Value::Text("lix_conversation".into()),
                    Value::Text("target".into()),
                    Value::Text("TEXT".into()),
                    Value::Text("YES".into()),
                ],
                vec![
                    Value::Text("lix_conversation".into()),
                    Value::Text("title".into()),
                    Value::Text("TEXT".into()),
                    Value::Text("YES".into()),
                ],
            ],
        );

        install_local_targets(&session).await;
        insert_local_conversations_and_comments(&session).await;
        session
            .execute(
                "UPDATE lix_conversation SET title='Launch plan' WHERE id=$1",
                &[Value::Text(LOCAL_PARAGRAPH_CONVERSATION.into())],
            )
            .await
            .expect("title should be writable");
        assert_rows_eq(
            session
                .execute(
                    "SELECT title FROM lix_conversation WHERE id IN ($1,$2) ORDER BY id",
                    &[
                        Value::Text(LOCAL_PARAGRAPH_CONVERSATION.into()),
                        Value::Text(LOCAL_CSV_CONVERSATION.into()),
                    ],
                )
                .await
                .unwrap(),
            vec![vec![Value::Text("Launch plan".into())], vec![Value::Null]],
        );
        assert_rows_eq(
            session
                .execute(
                    "SELECT id FROM lix_conversation
                 WHERE id IN ($1,$2,$3,$4) ORDER BY id",
                    &[
                        Value::Text(LOCAL_PARAGRAPH_CONVERSATION.into()),
                        Value::Text(LOCAL_CSV_CONVERSATION.into()),
                        Value::Text(LOCAL_FILE_CONVERSATION.into()),
                        Value::Text(STANDALONE_CONVERSATION.into()),
                    ],
                )
                .await
                .unwrap(),
            vec![
                vec![Value::Text(LOCAL_PARAGRAPH_CONVERSATION.into())],
                vec![Value::Text(LOCAL_CSV_CONVERSATION.into())],
                vec![Value::Text(LOCAL_FILE_CONVERSATION.into())],
                vec![Value::Text(STANDALONE_CONVERSATION.into())],
            ],
        );

        assert!(
            session
                .execute(
                    "INSERT INTO lix_comment(id,conversation_id,body) VALUES ($1,$2,NULL)",
                    &[
                        Value::Text("01950000-0000-7000-8000-000000000306".into()),
                        Value::Text(LOCAL_PARAGRAPH_CONVERSATION.into()),
                    ],
                )
                .await
                .is_err(),
            "body SQL NULL must violate the required JSONB column"
        );
        assert!(
        session
            .execute(
                "INSERT INTO lix_comment(id,conversation_id,body) VALUES ($1,NULL,CAST($2 AS JSONB))",
                &[
                    Value::Text("01950000-0000-7000-8000-000000000307".into()),
                    Value::Text(BODY.into()),
                ],
            )
            .await
            .is_err(),
        "conversation_id SQL NULL must violate the required foreign key column"
    );
    }
);

simulation_test!(
    conversation_local_targets_body_and_standalone_contract,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        install_local_targets(&session).await;
        insert_local_conversations_and_comments(&session).await;

        assert_rows_eq(
            session
                .execute(
                    "SELECT id FROM lix_conversation WHERE target IS NULL ORDER BY id",
                    &[],
                )
                .await
                .unwrap(),
            vec![vec![Value::Text(STANDALONE_CONVERSATION.into())]],
        );
        assert_rows_eq(
            session
                .execute(
                    "SELECT id,conversation_id,body FROM lix_comment
                 WHERE id IN ($1,$2,$3,$4) ORDER BY id",
                    &[
                        Value::Text(PARAGRAPH_COMMENT.into()),
                        Value::Text(CSV_COMMENT.into()),
                        Value::Text(FILE_COMMENT.into()),
                        Value::Text(STANDALONE_COMMENT.into()),
                    ],
                )
                .await
                .unwrap(),
            vec![
                vec![
                    Value::Text(PARAGRAPH_COMMENT.into()),
                    Value::Text(LOCAL_PARAGRAPH_CONVERSATION.into()),
                    Value::Jsonb(
                        serde_json::from_str::<serde_json::Value>(BODY)
                            .unwrap()
                            .into(),
                    ),
                ],
                vec![
                    Value::Text(CSV_COMMENT.into()),
                    Value::Text(LOCAL_CSV_CONVERSATION.into()),
                    Value::Jsonb(
                        serde_json::from_str::<serde_json::Value>(BODY)
                            .unwrap()
                            .into(),
                    ),
                ],
                vec![
                    Value::Text(FILE_COMMENT.into()),
                    Value::Text(LOCAL_FILE_CONVERSATION.into()),
                    Value::Jsonb(
                        serde_json::from_str::<serde_json::Value>(BODY)
                            .unwrap()
                            .into(),
                    ),
                ],
                vec![
                    Value::Text(STANDALONE_COMMENT.into()),
                    Value::Text(STANDALONE_CONVERSATION.into()),
                    Value::Jsonb(
                        serde_json::from_str::<serde_json::Value>(BODY)
                            .unwrap()
                            .into(),
                    ),
                ],
            ],
        );
    }
);

simulation_test!(
    conversation_global_commit_discussion_and_scope_copy,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let local = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        install_local_targets(&local).await;
        insert_local_conversations_and_comments(&local).await;

        let local_commit_id = local
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();
        let global = sim.wrap_session(
            engine.open_session_at(lix::GLOBAL_BRANCH_ID).await.unwrap(),
            &engine,
        );
        global
        .execute(
            "INSERT INTO lix_conversation(id,target) VALUES ($1,lix_row_ref('lix_commit',NULL,$2))",
            &[
                Value::Text(GLOBAL_CONVERSATION.into()),
                Value::Text(local_commit_id),
            ],
        )
        .await
        .expect("global commit conversation should insert");
        global
            .execute(
                "INSERT INTO lix_comment(id,conversation_id,body) VALUES ($1,$2,CAST($3 AS JSONB))",
                &[
                    Value::Text(GLOBAL_COMMENT.into()),
                    Value::Text(GLOBAL_CONVERSATION.into()),
                    Value::Text(BODY.into()),
                ],
            )
            .await
            .expect("global comment should insert");

        let copied_comment = local
            .execute(
                "INSERT INTO lix_comment(id,conversation_id,body,lixcol_global)
             SELECT $1,id,$2::JSONB,lixcol_global
             FROM lix_conversation WHERE id=$3
             RETURNING id,conversation_id,body,lixcol_global",
                &[
                    Value::Text(COPY_COMMENT.into()),
                    Value::Text(BODY.into()),
                    Value::Text(GLOBAL_CONVERSATION.into()),
                ],
            )
            .await
            .expect("local INSERT SELECT scope copy should succeed");
        assert_rows_eq(
            copied_comment,
            vec![vec![
                Value::Text(COPY_COMMENT.into()),
                Value::Text(GLOBAL_CONVERSATION.into()),
                Value::Jsonb(
                    serde_json::from_str::<serde_json::Value>(BODY)
                        .unwrap()
                        .into(),
                ),
                Value::Boolean(true),
            ]],
        );
        assert!(
        local
            .execute(
                "INSERT INTO lix_comment(id,conversation_id,body) VALUES ($1,$2,CAST($3 AS JSONB))",
                &[
                    Value::Text("01950000-0000-7000-8000-000000000308".into()),
                    Value::Text(GLOBAL_CONVERSATION.into()),
                    Value::Text(BODY.into()),
                ],
            )
            .await
            .is_err(),
        "a plain local reply must not attach to a global conversation"
    );
        let empty_copy = local
            .execute(
                "INSERT INTO lix_comment(id,conversation_id,body,lixcol_global)
             SELECT $1,id,$2::JSONB,lixcol_global
             FROM lix_conversation WHERE id=$3
             RETURNING id,conversation_id,body,lixcol_global",
                &[
                    Value::Text("01950000-0000-7000-8000-000000000309".into()),
                    Value::Text(BODY.into()),
                    Value::Text("01950000-0000-7000-8000-000000000599".into()),
                ],
            )
            .await
            .expect("an empty INSERT SELECT source should be a no-op");
        assert_rows_eq(empty_copy, vec![]);
        assert_rows_eq(
            local
                .execute(
                    "SELECT id FROM lix_conversation WHERE id=$1",
                    &[Value::Text(GLOBAL_CONVERSATION.into())],
                )
                .await
                .unwrap(),
            vec![vec![Value::Text(GLOBAL_CONVERSATION.into())]],
        );
    }
);

simulation_test!(
    conversation_scope_crossing_row_refs_are_rejected,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let local = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        install_local_targets(&local).await;

        let local_to_global = local
        .execute(
            "INSERT INTO lix_conversation(id,target) VALUES ($1,lix_row_ref('lix_commit',NULL,$2))",
            &[
                Value::Text("01950000-0000-7000-8000-000000000601".into()),
                Value::Text(sim.initial_global_commit_id().into()),
            ],
        )
        .await
        .expect_err("local conversations must not target global rows");
        assert_eq!(local_to_global.code, lix::LixError::CODE_FOREIGN_KEY);

        local
        .execute(
            "INSERT INTO lix_file(id,path,content) VALUES ($1,'/scope-local.txt',CAST('local' AS BYTEA))",
            &[Value::Text("01950000-0000-7000-8000-000000000602".into())],
        )
        .await
        .unwrap();
        let global = sim.wrap_session(
            engine.open_session_at(lix::GLOBAL_BRANCH_ID).await.unwrap(),
            &engine,
        );
        let global_to_local = global
        .execute(
            "INSERT INTO lix_conversation(id,target) VALUES ($1,lix_row_ref('lix_file',NULL,$2))",
            &[
                Value::Text("01950000-0000-7000-8000-000000000603".into()),
                Value::Text("01950000-0000-7000-8000-000000000602".into()),
            ],
        )
        .await
        .expect_err("global conversations must not target local rows");
        assert!(
            global_to_local.code == lix::LixError::CODE_FOREIGN_KEY
                || global_to_local.code == lix::LixError::CODE_TABLE_NOT_FOUND,
            "unexpected cross-scope error: {global_to_local:?}"
        );
    }
);

simulation_test!(
    conversation_target_conversation_comment_cascades_and_history,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        install_local_targets(&session).await;
        insert_local_conversations_and_comments(&session).await;

        session
            .execute(
                "DELETE FROM lix_file WHERE id=$1",
                &[Value::Text(FILE_TARGET.into())],
            )
            .await
            .expect("file target deletion should cascade");
        assert_rows_eq(
            session
                .execute(
                    "SELECT id FROM lix_conversation WHERE id=$1",
                    &[Value::Text(LOCAL_FILE_CONVERSATION.into())],
                )
                .await
                .unwrap(),
            vec![],
        );
        assert_rows_eq(
            session
                .execute(
                    "SELECT id FROM lix_comment WHERE id=$1",
                    &[Value::Text(FILE_COMMENT.into())],
                )
                .await
                .unwrap(),
            vec![],
        );

        for (target_key, target_id, conversation_id, comment_id) in [
            (
                "conversation_paragraph_target",
                "paragraph-1",
                LOCAL_PARAGRAPH_CONVERSATION,
                PARAGRAPH_COMMENT,
            ),
            (
                "conversation_csv_target",
                "csv-row-1",
                LOCAL_CSV_CONVERSATION,
                CSV_COMMENT,
            ),
        ] {
            session
                .execute(
                    &format!("DELETE FROM {target_key} WHERE id=$1"),
                    &[Value::Text(target_id.into())],
                )
                .await
                .expect("custom target deletion should cascade");
            assert_rows_eq(
                session
                    .execute(
                        "SELECT id FROM lix_conversation WHERE id=$1",
                        &[Value::Text(conversation_id.into())],
                    )
                    .await
                    .unwrap(),
                vec![],
            );
            assert_rows_eq(
                session
                    .execute(
                        "SELECT id FROM lix_comment WHERE id=$1",
                        &[Value::Text(comment_id.into())],
                    )
                    .await
                    .unwrap(),
                vec![],
            );
        }
        assert_rows_eq(
            session
                .execute(
                    "SELECT id FROM lix_conversation WHERE id=$1",
                    &[Value::Text(STANDALONE_CONVERSATION.into())],
                )
                .await
                .unwrap(),
            vec![vec![Value::Text(STANDALONE_CONVERSATION.into())]],
        );

        let history = session
            .execute(
                "SELECT id,diff_type FROM lix_history('lix_conversation') WHERE id=$1",
                &[Value::Text(LOCAL_FILE_CONVERSATION.into())],
            )
            .await
            .expect("conversation history should remain queryable after cascade deletion");
        assert!(
            history.rows().iter().any(|row| row.values()
                == [
                    Value::Text(LOCAL_FILE_CONVERSATION.into()),
                    Value::Text("removed".into())
                ]),
            "deleted conversation history should contain a removal: {:?}",
            history.rows()
        );
    }
);

simulation_test!(
    conversation_transaction_rolls_back_script_like_writes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        install_local_targets(&session).await;
        let mut transaction = session.begin_transaction().await.unwrap();
        transaction
        .execute(
            "INSERT INTO lix_conversation(id,target) VALUES ($1,lix_row_ref('conversation_custom_target',NULL,'custom-1'))",
            &[Value::Text(TARGET_CONVERSATION.into())],
        )
        .await
        .unwrap();
        transaction
            .execute(
                "INSERT INTO lix_comment(id,conversation_id,body) VALUES ($1,$2,CAST($3 AS JSONB))",
                &[
                    Value::Text(TARGET_COMMENT.into()),
                    Value::Text(TARGET_CONVERSATION.into()),
                    Value::Text(BODY.into()),
                ],
            )
            .await
            .unwrap();
        let same_transaction_copy = transaction
            .execute(
                "INSERT INTO lix_comment(id,conversation_id,body)
             SELECT $1,id,$2::JSONB FROM lix_conversation WHERE id=$3
             RETURNING id,conversation_id,body",
                &[
                    Value::Text("01950000-0000-7000-8000-000000000404".into()),
                    Value::Text(BODY.into()),
                    Value::Text(TARGET_CONVERSATION.into()),
                ],
            )
            .await
            .expect("same-transaction INSERT SELECT should read its pending conversation");
        assert_rows_eq(
            same_transaction_copy,
            vec![vec![
                Value::Text("01950000-0000-7000-8000-000000000404".into()),
                Value::Text(TARGET_CONVERSATION.into()),
                Value::Jsonb(
                    serde_json::from_str::<serde_json::Value>(BODY)
                        .unwrap()
                        .into(),
                ),
            ]],
        );
        transaction
            .execute(
                "INSERT INTO lix_comment(id,conversation_id,body) VALUES ($1,$2,CAST($3 AS JSONB))",
                &[
                    Value::Text("01950000-0000-7000-8000-000000000405".into()),
                    Value::Text("01950000-0000-7000-8000-000000000406".into()),
                    Value::Text(BODY.into()),
                ],
            )
            .await
            .expect("foreign-key validation is deferred until commit");
        let commit_error = transaction
            .commit()
            .await
            .expect_err("the script-like transaction must roll back on commit validation");
        assert_eq!(commit_error.code, lix::LixError::CODE_FOREIGN_KEY);
        assert_rows_eq(
            session
                .execute(
                    "SELECT id FROM lix_conversation WHERE id=$1",
                    &[Value::Text(TARGET_CONVERSATION.into())],
                )
                .await
                .unwrap(),
            vec![],
        );
        assert_rows_eq(
            session
                .execute(
                    "SELECT id FROM lix_comment WHERE id=$1",
                    &[Value::Text(TARGET_COMMENT.into())],
                )
                .await
                .unwrap(),
            vec![],
        );
    }
);

simulation_test!(conversation_branch_fork_delete_isolated, |sim| async move {
    let engine = sim.boot_engine().await;
    let main = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    install_local_targets(&main).await;
    main
        .execute(
            "INSERT INTO lix_conversation(id,target) VALUES ($1,lix_row_ref('conversation_custom_target',NULL,'custom-1'))",
            &[Value::Text(TARGET_CONVERSATION.into())],
        )
        .await
        .unwrap();
    main.execute(
        "INSERT INTO lix_comment(id,conversation_id,body) VALUES ($1,$2,CAST($3 AS JSONB))",
        &[
            Value::Text(TARGET_COMMENT.into()),
            Value::Text(TARGET_CONVERSATION.into()),
            Value::Text(BODY.into()),
        ],
    )
    .await
    .unwrap();
    let branch = main
        .create_branch(CreateBranchOptions {
            id: None,
            name: "conversation-delete-fork".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let fork = sim.wrap_session(engine.open_session_at(branch.id).await.unwrap(), &engine);
    fork.execute(
        "DELETE FROM conversation_custom_target WHERE id='custom-1'",
        &[],
    )
    .await
    .unwrap();
    assert_rows_eq(
        main.execute("SELECT id FROM lix_conversation", &[])
            .await
            .unwrap(),
        vec![vec![Value::Text(TARGET_CONVERSATION.into())]],
    );
    assert_rows_eq(
        fork.execute("SELECT id FROM lix_conversation", &[])
            .await
            .unwrap(),
        vec![],
    );
    assert_rows_eq(
        fork.execute("SELECT id FROM lix_comment", &[])
            .await
            .unwrap(),
        vec![],
    );
});

simulation_test!(
    conversation_merge_target_delete_and_concurrent_reply_destination_delete,
    |sim| async move {
        assert_conversation_merge_target_delete_and_reply(&sim, true).await;
    }
);

simulation_test!(
    conversation_merge_target_delete_and_concurrent_reply_source_delete,
    |sim| async move {
        assert_conversation_merge_target_delete_and_reply(&sim, false).await;
    }
);

simulation_test!(
    conversation_generation_delete_cascades_incoming_reply_destination_delete,
    |sim| async move {
        assert_conversation_generation_delete_merge(&sim, true).await;
    }
);

simulation_test!(
    conversation_generation_delete_cascades_incoming_reply_source_delete,
    |sim| async move {
        assert_conversation_generation_delete_merge(&sim, false).await;
    }
);

async fn assert_conversation_merge_target_delete_and_reply(
    sim: &crate::support::simulation_test::engine::Simulation,
    deletion_on_destination: bool,
) {
    let engine = sim.boot_engine().await;
    let main = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    install_local_targets(&main).await;
    main
        .execute(
            "INSERT INTO lix_conversation(id,target) VALUES ($1,lix_row_ref('conversation_custom_target',NULL,'custom-1'))",
            &[Value::Text(TARGET_CONVERSATION.into())],
        )
        .await
        .unwrap();
    main.execute(
        "INSERT INTO lix_comment(id,conversation_id,body) VALUES ($1,$2,CAST($3 AS JSONB))",
        &[
            Value::Text(TARGET_COMMENT.into()),
            Value::Text(TARGET_CONVERSATION.into()),
            Value::Text(BODY.into()),
        ],
    )
    .await
    .unwrap();
    let branch = main
        .create_branch(CreateBranchOptions {
            id: None,
            name: "conversation-delete-reply-merge".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let source = sim.wrap_session(
        engine.open_session_at(branch.id.clone()).await.unwrap(),
        &engine,
    );
    let deleted = if deletion_on_destination {
        &main
    } else {
        &source
    };
    let replying = if deletion_on_destination {
        &source
    } else {
        &main
    };
    deleted
        .execute(
            "DELETE FROM conversation_custom_target WHERE id='custom-1'",
            &[],
        )
        .await
        .unwrap();
    replying
        .execute(
            "INSERT INTO lix_comment(id,conversation_id,body) VALUES ($1,$2,CAST($3 AS JSONB))",
            &[
                Value::Text("01950000-0000-7000-8000-000000000406".into()),
                Value::Text(TARGET_CONVERSATION.into()),
                Value::Text(BODY.into()),
            ],
        )
        .await
        .unwrap();
    let preview = main
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: branch.id.clone(),
        })
        .await
        .unwrap();
    let receipt = main
        .merge_branch(MergeBranchOptions {
            source_branch_id: branch.id,
        })
        .await
        .unwrap();
    assert_eq!(preview.change_stats, receipt.change_stats);
    assert_rows_eq(
        main.execute("SELECT id FROM lix_conversation", &[])
            .await
            .unwrap(),
        vec![],
    );
    assert_rows_eq(
        main.execute("SELECT id FROM lix_comment", &[])
            .await
            .unwrap(),
        vec![],
    );
}

async fn assert_conversation_generation_delete_merge(
    sim: &crate::support::simulation_test::engine::Simulation,
    deletion_on_destination: bool,
) {
    let engine = sim.boot_engine().await;
    let main = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    // The base has target rows but no conversations. The collection delete
    // below therefore exercises the generation fast path while the reply
    // branch carries the first target reference into the merge.
    install_local_targets(&main).await;
    let branch = main
        .create_branch(CreateBranchOptions {
            id: None,
            name: "conversation-generation-delete-merge".into(),
            from_commit_id: None,
        })
        .await
        .unwrap();
    let source = sim.wrap_session(
        engine.open_session_at(branch.id.clone()).await.unwrap(),
        &engine,
    );
    let deleted = if deletion_on_destination {
        &main
    } else {
        &source
    };
    let replying = if deletion_on_destination {
        &source
    } else {
        &main
    };
    deleted
        .execute("DELETE FROM conversation_custom_target", &[])
        .await
        .unwrap();
    assert_rows_eq(
        deleted.execute(
            "SELECT COUNT(*) AS count FROM lix_change WHERE schema_key='lix_collection_generation'",
            &[],
        ).await.unwrap(),
        vec![vec![Value::Integer(1)]],
    );
    replying
        .execute(
            "INSERT INTO lix_conversation(id,target) VALUES ($1,lix_row_ref('conversation_custom_target',NULL,'custom-1'))",
            &[Value::Text(GENERATION_CONVERSATION.into())],
        )
        .await
        .unwrap();
    replying
        .execute(
            "INSERT INTO lix_comment(id,conversation_id,body) VALUES ($1,$2,CAST($3 AS JSONB))",
            &[
                Value::Text(GENERATION_COMMENT.into()),
                Value::Text(GENERATION_CONVERSATION.into()),
                Value::Text(BODY.into()),
            ],
        )
        .await
        .unwrap();
    let preview = main
        .merge_branch_preview(MergeBranchPreviewOptions {
            source_branch_id: branch.id.clone(),
        })
        .await
        .expect("generation delete merge preview should succeed");
    let receipt = main
        .merge_branch(MergeBranchOptions {
            source_branch_id: branch.id,
        })
        .await
        .expect("generation delete merge should succeed");
    assert_eq!(preview.change_stats, receipt.change_stats);
    assert_rows_eq(
        main.execute("SELECT id FROM conversation_custom_target", &[])
            .await
            .unwrap(),
        vec![],
    );
    assert_rows_eq(
        main.execute("SELECT id FROM lix_conversation", &[])
            .await
            .unwrap(),
        vec![],
    );
    assert_rows_eq(
        main.execute("SELECT id FROM lix_comment", &[])
            .await
            .unwrap(),
        vec![],
    );
}
