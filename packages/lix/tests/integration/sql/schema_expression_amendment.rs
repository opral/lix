use lix::Value;
use serde_json::json;

use super::assert_rows_eq;

simulation_test!(
    expression_default_amendments_materialize_existing_rows_once,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let mut schema = json!({
            "$schema": "https://lix.dev/schema-v1.json", "key": "expression_amendment",
            "columns": [
                {"name":"id", "type":"text", "nullable":false},
                {"name":"title", "type":"text", "nullable":false}
            ], "primary_key":["id"]
        });
        session.execute("INSERT INTO lix_registered_schema (value,lixcol_global,lixcol_untracked) VALUES ($1,false,false)", &[Value::Jsonb(schema.clone().into())]).await.unwrap();
        session
            .execute(
                "INSERT INTO expression_amendment (id,title) VALUES ('a','before'),('b','before')",
                &[],
            )
            .await
            .unwrap();
        let before_commit = session
            .execute(
                "SELECT commit_id FROM lix_branch WHERE id = lix_active_branch_id()",
                &[],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        schema["columns"].as_array_mut().unwrap().extend([
            json!({"name":"generated_id","type":"uuid","nullable":false,"default_expression":"uuidv7()"}),
            json!({"name":"generated_at","type":"timestamptz","nullable":false,"default_expression":"CURRENT_TIMESTAMP"}),
            json!({"name":"optional","type":"int8","nullable":true}),
        ]);
        session
            .execute(
                "UPDATE lix_registered_schema SET value=$1 WHERE schema_key='expression_amendment'",
                &[Value::Jsonb(schema.into())],
            )
            .await
            .unwrap();
        assert_rows_eq(
            session
                .execute(
                    "SELECT id,title FROM lix_as_of('expression_amendment',$1) ORDER BY id",
                    &[Value::Text(before_commit)],
                )
                .await
                .unwrap(),
            vec![
                vec![Value::Text("a".into()), Value::Text("before".into())],
                vec![Value::Text("b".into()), Value::Text("before".into())],
            ],
        );
        assert_rows_eq(session.execute("SELECT id FROM lix_history('expression_amendment') WHERE from_generated_id IS NULL AND to_generated_id IS NOT NULL ORDER BY id", &[]).await.unwrap(), vec![vec![Value::Text("a".into())], vec![Value::Text("b".into())]]);
        let sql = "SELECT id,generated_id,generated_at FROM expression_amendment ORDER BY id";
        let initial = session
            .execute(sql, &[])
            .await
            .expect("accepted default amendment keeps old rows readable");
        let expected = initial
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>();
        assert_eq!(expected.len(), 2);
        assert_ne!(
            expected[0][1], expected[1][1],
            "uuidv7 defaults are per row"
        );
        assert!(
            matches!(&expected[0][1], Value::Text(value) if uuid::Uuid::parse_str(value).is_ok())
        );
        assert!(matches!(&expected[0][2], Value::Timestamptz(_)));
        assert_eq!(
            expected[0][2], expected[1][2],
            "transaction timestamp is stable"
        );
        assert_rows_eq(session.execute(sql, &[]).await.unwrap(), expected.clone());
        session
            .execute("UPDATE expression_amendment SET title='after'", &[])
            .await
            .unwrap();
        assert_rows_eq(session.execute(sql, &[]).await.unwrap(), expected.clone());
        let rebooted = sim.reboot_engine_from_current_snapshot().await.unwrap();
        let reopened = sim.wrap_session(rebooted.open_session().await.unwrap(), &rebooted);
        assert_rows_eq(reopened.execute(sql, &[]).await.unwrap(), expected);
    }
);

simulation_test!(
    expression_default_amendments_include_pending_rows_and_rollback,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let schema = json!({"$schema":"https://lix.dev/schema-v1.json","key":"expression_rollback","columns":[{"name":"id","type":"text","nullable":false},{"name":"title","type":"text","nullable":true}],"primary_key":["id"]});
        session.execute("INSERT INTO lix_registered_schema (value,lixcol_global,lixcol_untracked) VALUES ($1,false,false)", &[Value::Jsonb(schema.clone().into())]).await.unwrap();
        session
            .execute("INSERT INTO expression_rollback (id) VALUES ('old')", &[])
            .await
            .unwrap();
        let mut amended = schema.clone();
        amended["columns"].as_array_mut().unwrap().push(json!({"name":"generated_id","type":"uuid","nullable":false,"default_expression":"uuidv7()"}));
        let mut transaction = session.begin_transaction().await.unwrap();
        transaction
            .execute(
                "INSERT INTO expression_rollback (id) VALUES ('pending')",
                &[],
            )
            .await
            .unwrap();
        let mut invalid = amended.clone();
        invalid["columns"][0]["type"] = json!("int8");
        transaction
            .execute(
                "UPDATE lix_registered_schema SET value=$1 WHERE schema_key='expression_rollback'",
                &[Value::Jsonb(invalid.into())],
            )
            .await
            .expect_err("incompatible amendment must leave prior staged rows/catalog intact");
        assert_rows_eq(
            transaction
                .execute("SELECT id FROM expression_rollback ORDER BY id", &[])
                .await
                .unwrap(),
            vec![
                vec![Value::Text("old".into())],
                vec![Value::Text("pending".into())],
            ],
        );
        transaction
            .execute(
                "UPDATE lix_registered_schema SET value=$1 WHERE schema_key='expression_rollback'",
                &[Value::Jsonb(amended.clone().into())],
            )
            .await
            .unwrap();
        // SQL binding intentionally retains the transaction-opening catalog;
        // newly appended columns become queryable after this transaction commits.
        assert_rows_eq(
            transaction
                .execute("SELECT id FROM expression_rollback ORDER BY id", &[])
                .await
                .expect("existing-column reads remain valid after a staged amendment"),
            vec![
                vec![Value::Text("old".into())],
                vec![Value::Text("pending".into())],
            ],
        );
        assert_rows_eq(
            transaction
                .execute("SELECT id FROM expression_rollback WHERE id='old'", &[])
                .await
                .expect("exact existing-column reads survive staged amendment"),
            vec![vec![Value::Text("old".into())]],
        );
        let unbound = transaction
            .execute("SELECT generated_id FROM expression_rollback", &[])
            .await
            .expect_err("new columns remain outside the opening SQL catalog");
        assert_eq!(unbound.code, "LIX_COLUMN_NOT_FOUND");
        assert_rows_eq(
            transaction
                .execute(
                    "UPDATE expression_rollback SET title='after' WHERE id='old' RETURNING id",
                    &[],
                )
                .await
                .expect("RETURNING uses opening output fields and current complete row validation"),
            vec![vec![Value::Text("old".into())]],
        );
        transaction.rollback().await.unwrap();
        assert_rows_eq(
            session
                .execute("SELECT id FROM expression_rollback", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("old".into())]],
        );
        assert_rows_eq(session.execute("SELECT value FROM lix_registered_schema WHERE schema_key='expression_rollback'", &[]).await.unwrap(), vec![vec![Value::Jsonb(schema.into())]]);
        let mut transaction = session.begin_transaction().await.unwrap();
        transaction
            .execute(
                "INSERT INTO expression_rollback (id) VALUES ('committed_pending')",
                &[],
            )
            .await
            .unwrap();
        transaction
            .execute(
                "UPDATE lix_registered_schema SET value=$1 WHERE schema_key='expression_rollback'",
                &[Value::Jsonb(amended.into())],
            )
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        let materialized = session
            .execute(
                "SELECT id,generated_id FROM expression_rollback ORDER BY id",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(materialized.len(), 2);
        assert!(
            materialized
                .rows()
                .iter()
                .all(|row| matches!(&row.values()[1], Value::Text(_)))
        );
        session.execute("INSERT INTO expression_rollback (id,generated_id) VALUES ('explicit','01930000-0000-7000-8000-000000000001')", &[]).await.unwrap();
        assert_rows_eq(
            session
                .execute(
                    "SELECT generated_id FROM expression_rollback WHERE id='explicit'",
                    &[],
                )
                .await
                .unwrap(),
            vec![vec![Value::Text(
                "01930000-0000-7000-8000-000000000001".into(),
            )]],
        );
    }
);

simulation_test!(
    expression_default_amendments_respect_schema_branch_scope,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let main = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        // Branch SQL surfaces intentionally exclude global-only schemas.
        let schema = json!({"$schema":"https://lix.dev/schema-v1.json","key":"expression_local","columns":[{"name":"id","type":"text","nullable":false}],"primary_key":["id"]});
        main.execute("INSERT INTO lix_registered_schema (value,lixcol_global,lixcol_untracked) VALUES ($1,false,false)", &[Value::Jsonb(schema.clone().into())]).await.unwrap();
        main.execute(
            "INSERT INTO expression_local (id) VALUES ('inherited')",
            &[],
        )
        .await
        .unwrap();
        let branch = main
            .create_branch(lix::CreateBranchOptions {
                name: "expression-scope".into(),
                id: None,
                from_commit_id: None,
            })
            .await
            .unwrap();
        let fork = sim.wrap_session(engine.open_session_at(&branch.id).await.unwrap(), &engine);
        let mut amended = schema.clone();
        amended["columns"].as_array_mut().unwrap().push(json!({"name":"generated_id","type":"uuid","nullable":false,"default_expression":"uuidv7()"}));
        main.execute(
            "UPDATE lix_registered_schema SET value=$1 WHERE schema_key='expression_local'",
            &[Value::Jsonb(amended.into())],
        )
        .await
        .unwrap();
        let rows = main
            .execute(
                "SELECT generated_id FROM expression_local WHERE id='inherited'",
                &[],
            )
            .await
            .unwrap();
        assert!(matches!(&rows.rows()[0].values()[0], Value::Text(_)));
        assert_rows_eq(
            fork.execute(
                "SELECT value FROM lix_registered_schema WHERE schema_key='expression_local'",
                &[],
            )
            .await
            .unwrap(),
            vec![vec![Value::Jsonb(schema.into())]],
        );
        assert_rows_eq(
            fork.execute("SELECT id FROM expression_local", &[])
                .await
                .unwrap(),
            vec![vec![Value::Text("inherited".into())]],
        );
    }
);

simulation_test!(
    expression_default_amendments_materialize_global_and_untracked_rows,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session_at(lix::GLOBAL_BRANCH_ID).await.unwrap(),
            &engine,
        );
        let mut schema = json!({"$schema":"https://lix.dev/schema-v1.json","key":"expression_global","columns":[{"name":"id","type":"text","nullable":false}],"primary_key":["id"]});
        session.execute("INSERT INTO lix_registered_schema (value,lixcol_global,lixcol_untracked) VALUES ($1,true,false)", &[Value::Jsonb(schema.clone().into())]).await.unwrap();
        session.execute("INSERT INTO expression_global (id,lixcol_global,lixcol_untracked) VALUES ('tracked',true,false),('untracked',true,true)", &[]).await.unwrap();
        schema["columns"].as_array_mut().unwrap().push(json!({"name":"generated_id","type":"uuid","nullable":false,"default_expression":"uuidv7()"}));
        session
            .execute(
                "UPDATE lix_registered_schema SET value=$1 WHERE schema_key='expression_global'",
                &[Value::Jsonb(schema.into())],
            )
            .await
            .unwrap();
        let sql = "SELECT id,generated_id,lixcol_untracked FROM expression_global ORDER BY id";
        let rows = session.execute(sql, &[]).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert!(
            rows.rows()
                .iter()
                .all(|row| matches!(&row.values()[1], Value::Text(_)))
        );
        let expected = rows
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>();
        let rebooted = sim.reboot_engine_from_current_snapshot().await.unwrap();
        let reopened = sim.wrap_session(
            rebooted
                .open_session_at(lix::GLOBAL_BRANCH_ID)
                .await
                .unwrap(),
            &rebooted,
        );
        assert_rows_eq(reopened.execute(sql, &[]).await.unwrap(), expected);
    }
);

simulation_test!(
    literal_default_amendments_preserve_exact_history_endpoints,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let mut schema = json!({"$schema":"https://lix.dev/schema-v1.json","key":"literal_amendment","columns":[{"name":"id","type":"text","nullable":false}],"primary_key":["id"]});
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES ($1)",
                &[Value::Jsonb(schema.clone().into())],
            )
            .await
            .unwrap();
        session
            .execute("INSERT INTO literal_amendment (id) VALUES ('row')", &[])
            .await
            .unwrap();
        let before = session
            .execute(
                "SELECT commit_id FROM lix_branch WHERE id=lix_active_branch_id()",
                &[],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        schema["columns"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name":"priority","type":"int8","nullable":false,"default_value":7}));
        session
            .execute(
                "UPDATE lix_registered_schema SET value=$1 WHERE schema_key='literal_amendment'",
                &[Value::Jsonb(schema.into())],
            )
            .await
            .unwrap();
        let after = session
            .execute(
                "SELECT commit_id FROM lix_branch WHERE id=lix_active_branch_id()",
                &[],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        assert_rows_eq(
            session
                .execute("SELECT priority FROM literal_amendment", &[])
                .await
                .unwrap(),
            vec![vec![Value::Integer(7)]],
        );
        for (commit, expected) in [(before, Value::Null), (after, Value::Integer(7))] {
            assert_rows_eq(
                session
                    .execute(
                        "SELECT priority FROM lix_as_of('literal_amendment',$1)",
                        &[Value::Text(commit)],
                    )
                    .await
                    .unwrap(),
                vec![vec![expected]],
            );
        }
        assert_rows_eq(session.execute("SELECT from_priority,to_priority FROM lix_history('literal_amendment') WHERE to_priority=7",&[]).await.unwrap(),vec![vec![Value::Null,Value::Integer(7)]]);
        assert_eq!(
            session
                .execute(
                    "UPDATE literal_amendment SET priority=priority+1 WHERE priority=7",
                    &[]
                )
                .await
                .unwrap()
                .rows_affected(),
            1
        );
        assert_rows_eq(
            session
                .execute("SELECT priority FROM literal_amendment", &[])
                .await
                .unwrap(),
            vec![vec![Value::Integer(8)]],
        );
    }
);
