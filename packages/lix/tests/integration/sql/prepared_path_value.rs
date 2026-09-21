use lix::Value;
use serde_json::json;

simulation_test!(
    transaction_replacement_preserves_sql_null,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('prepared-null-a', 'seed'), ('prepared-null-b', 'seed')",
            &[],
        )
        .await
        .unwrap();

        let mut transaction = session.begin_transaction().await.unwrap();
        let sql = "UPDATE lix_key_value SET value = CAST($1 AS JSONB) WHERE key = $2";
        transaction
            .execute(sql, &[Value::Null, Value::Text("prepared-null-a".into())])
            .await
            .unwrap();
        transaction
            .execute(sql, &[Value::Null, Value::Text("prepared-null-b".into())])
            .await
            .unwrap();
        transaction.commit().await.unwrap();

        let result = session
        .execute(
            "SELECT value, value IS NULL FROM lix_key_value WHERE key LIKE 'prepared-null-%' ORDER BY key",
            &[],
        )
        .await
        .unwrap();
        assert_eq!(
            result
                .rows()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![Value::Null, Value::Boolean(true)],
                vec![Value::Null, Value::Boolean(true)],
            ]
        );
    }
);

simulation_test!(
    exact_key_update_distinguishes_json_null_from_sql_null,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('specialized-null', 'seed')",
                &[],
            )
            .await
            .unwrap();

        session
            .execute(
                "UPDATE lix_key_value SET value = CAST($1 AS JSONB) WHERE key = $2",
                &[Value::Null, Value::Text("specialized-null".into())],
            )
            .await
            .unwrap();
        let sql_null = session
            .execute(
                "SELECT value, value IS NULL FROM lix_key_value WHERE key = 'specialized-null'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            sql_null.rows()[0].values(),
            &[Value::Null, Value::Boolean(true)]
        );

        session
            .execute(
                "UPDATE lix_key_value SET value = CAST($1 AS JSONB) WHERE key = $2",
                &[
                    Value::Text("null".into()),
                    Value::Text("specialized-null".into()),
                ],
            )
            .await
            .unwrap();
        let json_null = session
            .execute(
                "SELECT value, value IS NULL FROM lix_key_value WHERE key = 'specialized-null'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            json_null.rows()[0].values(),
            &[Value::Jsonb(json!(null).into()), Value::Boolean(false)]
        );
    }
);

simulation_test!(
    certified_path_replacement_rejects_sql_null_and_keeps_json_null,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let schema = json!({"$schema":"https://lix.dev/schema-v1.json","key":"certified_null_probe",
        "columns":[{"name":"path","type":"text","nullable":false},{"name":"value","type":"jsonb","nullable":false}],"primary_key":["path"]});
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES ($1)",
                &[Value::Jsonb(schema.into())],
            )
            .await
            .unwrap();
        session.execute("INSERT INTO certified_null_probe (path,value) VALUES ('/a','{}'::jsonb),('/b','{}'::jsonb)",&[]).await.unwrap();
        let mut transaction = session.begin_transaction().await.unwrap();
        let sql = "UPDATE certified_null_probe SET value=CAST($1 AS JSONB) WHERE path=$2";
        crate::sql2::take_certified_single_path_value_replacements();
        for path in ["/a", "/b", "/a"] {
            transaction
                .execute(sql, &[Value::Text("null".into()), Value::Text(path.into())])
                .await
                .unwrap();
        }
        assert!(
            crate::sql2::take_certified_single_path_value_replacements() > 0,
            "exercise the certified path"
        );
        for path in ["/a", "/b"] {
            assert!(
                transaction
                    .execute(sql, &[Value::Null, Value::Text(path.into())])
                    .await
                    .is_err()
            );
        }
        assert!(
            transaction
                .execute(
                    "UPDATE certified_null_probe SET value=CAST($1 AS JSONB) WHERE path LIKE '/%'",
                    &[Value::Null]
                )
                .await
                .is_err()
        );
        transaction.commit().await.unwrap();
        let result = session
            .execute(
                "SELECT value,value IS NULL FROM certified_null_probe ORDER BY path",
                &[],
            )
            .await
            .unwrap();
        for row in result.rows() {
            assert_eq!(
                row.values(),
                &[Value::Jsonb(json!(null).into()), Value::Boolean(false)]
            );
        }
    }
);

simulation_test!(
    certified_path_replacement_accepts_typed_jsonb_values,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let schema = json!({"$schema":"https://lix.dev/schema-v1.json","key":"certified_typed_jsonb_probe",
        "columns":[{"name":"path","type":"text","nullable":false},{"name":"value","type":"jsonb","nullable":false}],"primary_key":["path"]});
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES ($1)",
                &[Value::Jsonb(schema.into())],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO certified_typed_jsonb_probe (path,value) VALUES ('/single-null','{}'::jsonb),('/single-string','{}'::jsonb),('/single-number','{}'::jsonb),('/single-object','{}'::jsonb),('/scalar-integer','{}'::jsonb),('/scalar-boolean','{}'::jsonb),('/scalar-real','{}'::jsonb),('/scalar-blob','{}'::jsonb),('/batch-null','{}'::jsonb),('/batch-string','{}'::jsonb),('/batch-number','{}'::jsonb),('/batch-object','{}'::jsonb)",
                &[],
            )
            .await
            .unwrap();

        let sql = "UPDATE certified_typed_jsonb_probe SET value=CAST($1 AS JSONB) WHERE path=$2";
        let single_values = vec![
            ("/single-null", json!(null)),
            ("/single-string", json!("typed-string")),
            ("/single-number", json!(1.0)),
            ("/single-object", json!({"kind":"typed-object"})),
        ];
        crate::sql2::take_certified_single_path_value_replacements();
        let mut transaction = session.begin_transaction().await.unwrap();
        for (path, value) in &single_values {
            transaction
                .execute(
                    sql,
                    &[
                        Value::Jsonb(value.clone().into()),
                        Value::Text((*path).into()),
                    ],
                )
                .await
                .unwrap();
        }
        let scalar_single_values = vec![
            ("/scalar-integer", Value::Integer(7)),
            ("/scalar-boolean", Value::Boolean(true)),
            ("/scalar-real", Value::Real(1.5)),
            ("/scalar-blob", Value::Blob(b"\"binary\"".to_vec().into())),
        ];
        for (path, value) in &scalar_single_values {
            transaction
                .execute(sql, &[value.clone(), Value::Text((*path).into())])
                .await
                .unwrap();
        }
        transaction.commit().await.unwrap();
        assert!(
            crate::sql2::take_certified_single_path_value_replacements()
                >= single_values.len() + scalar_single_values.len(),
            "typed JSONB prepared values should use the certified path replacement"
        );

        let batch_values = vec![
            ("/batch-null", json!(null)),
            ("/batch-string", json!("typed-string")),
            ("/batch-number", json!(1.0)),
            ("/batch-object", json!({"kind":"typed-object"})),
        ];
        let expected_values = single_values
            .iter()
            .chain(&batch_values)
            .map(|(_, value)| value.clone())
            .collect::<Vec<_>>();
        let statements = batch_values
            .iter()
            .map(|(path, value)| lix::ExecuteBatchStatement {
                sql: sql.into(),
                params: vec![
                    Value::Jsonb(value.clone().into()),
                    Value::Text((*path).into()),
                ],
                label: None,
            })
            .collect::<Vec<_>>();
        session.execute_batch(&statements).await.unwrap();

        for ((path, _), expected) in single_values
            .into_iter()
            .chain(batch_values)
            .zip(expected_values)
        {
            let expected = session
                .execute("SELECT CAST($1 AS JSONB)", &[Value::Jsonb(expected.into())])
                .await
                .unwrap()
                .rows()[0]
                .values()[0]
                .clone();
            let result = session
                .execute(
                    "SELECT value FROM certified_typed_jsonb_probe WHERE path=$1",
                    &[Value::Text(path.into())],
                )
                .await
                .unwrap();
            assert_eq!(
                result.rows()[0].values(),
                &[expected],
                "typed JSONB value for {path} was not preserved"
            );
        }
        for (path, value) in scalar_single_values {
            let expected = session
                .execute("SELECT CAST($1 AS JSONB)", &[value])
                .await
                .unwrap()
                .rows()[0]
                .values()[0]
                .clone();
            let result = session
                .execute(
                    "SELECT value FROM certified_typed_jsonb_probe WHERE path=$1",
                    &[Value::Text(path.into())],
                )
                .await
                .unwrap();
            assert_eq!(result.rows()[0].values(), &[expected]);
        }
    }
);

simulation_test!(
    certified_path_execute_batch_rolls_back_sql_null_for_required_value,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let schema = json!({"$schema":"https://lix.dev/schema-v1.json","key":"certified_required_batch_probe",
        "columns":[{"name":"path","type":"text","nullable":false},{"name":"value","type":"jsonb","nullable":false}],"primary_key":["path"]});
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES ($1)",
                &[Value::Jsonb(schema.into())],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO certified_required_batch_probe (path,value) VALUES ('/first','\"seed\"'::jsonb),('/second','\"seed\"'::jsonb)",
                &[],
            )
            .await
            .unwrap();

        let sql = "UPDATE certified_required_batch_probe SET value=CAST($1 AS JSONB) WHERE path=$2";
        assert!(
            session
                .execute_batch(&[
                    lix::ExecuteBatchStatement {
                        sql: sql.into(),
                        params: vec![
                            Value::Jsonb(json!({"written":true}).into()),
                            Value::Text("/first".into()),
                        ],
                        label: Some("valid-first".into()),
                    },
                    lix::ExecuteBatchStatement {
                        sql: sql.into(),
                        params: vec![Value::Null, Value::Text("/second".into())],
                        label: Some("invalid-null".into()),
                    },
                ])
                .await
                .is_err(),
            "SQL NULL must violate the required certified value column"
        );

        let result = session
            .execute(
                "SELECT path,value FROM certified_required_batch_probe ORDER BY path",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            result
                .rows()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![
                    Value::Text("/first".into()),
                    Value::Jsonb(json!("seed").into()),
                ],
                vec![
                    Value::Text("/second".into()),
                    Value::Jsonb(json!("seed").into()),
                ],
            ]
        );
    }
);

simulation_test!(
    generic_nullable_path_execute_batch_preserves_mixed_null_provenance,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        let schema = json!({"$schema":"https://lix.dev/schema-v1.json","key":"generic_nullable_batch_probe",
        "columns":[{"name":"path","type":"text","nullable":false},{"name":"value","type":"jsonb","nullable":true}],"primary_key":["path"]});
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES ($1)",
                &[Value::Jsonb(schema.into())],
            )
            .await
            .unwrap();
        session
            .execute(
                "INSERT INTO generic_nullable_batch_probe (path,value) VALUES ('/sql-null','{}'::jsonb),('/json-null','{}'::jsonb)",
                &[],
            )
            .await
            .unwrap();

        let sql = "UPDATE generic_nullable_batch_probe SET value=CAST($1 AS JSONB) WHERE path=$2";
        session
            .execute_batch(&[
                lix::ExecuteBatchStatement {
                    sql: sql.into(),
                    params: vec![Value::Null, Value::Text("/sql-null".into())],
                    label: None,
                },
                lix::ExecuteBatchStatement {
                    sql: sql.into(),
                    params: vec![
                        Value::Jsonb(json!(null).into()),
                        Value::Text("/json-null".into()),
                    ],
                    label: None,
                },
            ])
            .await
            .unwrap();

        let result = session
            .execute(
                "SELECT path,value,value IS NULL FROM generic_nullable_batch_probe ORDER BY path",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            result
                .rows()
                .iter()
                .map(|row| row.values().to_vec())
                .collect::<Vec<_>>(),
            vec![
                vec![
                    Value::Text("/json-null".into()),
                    Value::Jsonb(json!(null).into()),
                    Value::Boolean(false),
                ],
                vec![
                    Value::Text("/sql-null".into()),
                    Value::Null,
                    Value::Boolean(true),
                ],
            ]
        );
    }
);
