use lix::Value;
use serde_json::json;

simulation_test!(typed_sql_values_survive_all_write_paths, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    let schema = json!({"$schema":"https://lix.dev/schema-v1.json","key":"typed_contract",
        "columns":[{"name":"id","type":"text","nullable":false},
            {"name":"stamp","type":"timestamptz","nullable":true},
            {"name":"n","type":"int8","nullable":true},
            {"name":"payload","type":"jsonb","nullable":true},
            {"name":"label","type":"text","nullable":true}],"primary_key":["id"]});
    session
        .execute(
            "INSERT INTO lix_registered_schema (schema_key,value) VALUES ('typed_contract', $1)",
            &[Value::Jsonb(schema.into())],
        )
        .await
        .unwrap();
    let micros = 1_735_787_045_123_456;
    let stamp = Value::Timestamptz(micros);
    let inserted = session.execute("INSERT INTO typed_contract (id,stamp,n,payload) VALUES ('a',$1,$2,$3) RETURNING stamp,n,payload", &[stamp.clone(),Value::Real(1.0),Value::Jsonb(json!(null).into())]).await.unwrap();
    assert_eq!(
        inserted.rows()[0].values(),
        &[
            stamp.clone(),
            Value::Integer(1),
            Value::Jsonb(json!(null).into())
        ]
    );
    let read_cast = session
        .execute("SELECT CAST($1 AS TEXT)", &[stamp.clone()])
        .await
        .unwrap();
    for filter in ["id = 'a'", "id LIKE 'a'"] {
        let result = session.execute(&format!("UPDATE typed_contract SET stamp = $1, label = CAST($1 AS TEXT), n = $2 WHERE {filter} RETURNING stamp,label,n,payload"), &[stamp.clone(),Value::Real(2.0)]).await.unwrap();
        assert_eq!(result.rows_affected(), 1);
        assert_eq!(
            result.rows()[0].values(),
            &[
                stamp.clone(),
                read_cast.rows()[0].values()[0].clone(),
                Value::Integer(2),
                Value::Jsonb(json!(null).into())
            ]
        );
    }
    for value in [
        stamp.clone(),
        Value::Text("2025-01-02T03:04:05.123456Z".into()),
        Value::Text("2025-01-02T04:04:05.123456+01:00".into()),
    ] {
        let selected = session
            .execute(
                "SELECT id FROM typed_contract WHERE stamp = $1",
                &[value.clone()],
            )
            .await
            .unwrap();
        assert_eq!(selected.rows().len(), 1);
        let updated = session
            .execute(
                "UPDATE typed_contract SET n = 3 WHERE stamp = $1 RETURNING id",
                &[value.clone()],
            )
            .await
            .unwrap();
        assert_eq!(updated.rows()[0].values(), &[Value::Text("a".into())]);
        let updated = session
            .execute(
                "UPDATE typed_contract SET n = 4 WHERE stamp IN ($1) RETURNING id",
                &[value],
            )
            .await
            .unwrap();
        assert_eq!(updated.rows()[0].values(), &[Value::Text("a".into())]);
    }
    let upsert=session.execute("INSERT INTO typed_contract (id,stamp) VALUES ('a',$1) ON CONFLICT (id) DO UPDATE SET stamp = excluded.stamp RETURNING stamp", &[stamp.clone()]).await.unwrap();
    assert_eq!(upsert.rows()[0].values(), &[stamp.clone()]);
    for filter in ["id = 'a'", "id LIKE 'a'"] {
        assert!(
            session
                .execute(
                    &format!("UPDATE typed_contract SET n = $1 WHERE {filter}"),
                    &[Value::Real(1.5)]
                )
                .await
                .is_err()
        );
        assert!(
            session
                .execute(
                    &format!("UPDATE typed_contract SET label = $1 WHERE {filter}"),
                    &[Value::Jsonb(json!("text").into())]
                )
                .await
                .is_err()
        );
        for literal in ["9007199254740993.0", "-9223372036854775808.0"] {
            let updated = session
                .execute(
                    &format!("UPDATE typed_contract SET n = {literal} WHERE {filter} RETURNING n"),
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(
                updated.rows()[0].values(),
                &[Value::Integer(
                    literal.trim_end_matches(".0").parse().unwrap()
                )]
            );
        }
        assert!(
            session
                .execute(
                    &format!("UPDATE typed_contract SET stamp = $1 WHERE {filter}"),
                    &[Value::Integer(micros)]
                )
                .await
                .is_err()
        );
    }
    let deleted = session
        .execute(
            "DELETE FROM typed_contract WHERE stamp = $1 RETURNING stamp",
            &[stamp.clone()],
        )
        .await
        .unwrap();
    assert_eq!(deleted.rows()[0].values(), &[stamp]);
});

simulation_test!(
    jsonb_casts_keep_their_sql_meaning_in_writes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('cast-probe', NULL)",
                &[],
            )
            .await
            .unwrap();
        for value in [
            json!(null),
            json!("null"),
            json!("text"),
            json!(true),
            json!(42),
            json!({"a":1}),
        ] {
            let params = [Value::Jsonb(value.into())];
            let read = session
                .execute("SELECT CAST($1 AS TEXT)", &params)
                .await
                .unwrap();
            let write=session.execute("UPDATE lix_key_value SET value = value WHERE key = 'cast-probe' RETURNING CAST($1 AS TEXT)",&params).await.unwrap();
            assert_eq!(write.rows()[0].values(), read.rows()[0].values());
            for target in ["BIGINT", "BOOLEAN", "DOUBLE PRECISION"] {
                let read = session
                    .execute(&format!("SELECT CAST($1 AS {target})"), &params)
                    .await;
                let write=session.execute(&format!("UPDATE lix_key_value SET value = value WHERE key = 'cast-probe' RETURNING CAST($1 AS {target})"),&params).await;
                match (read, write) {
                    (Ok(read), Ok(write)) => {
                        assert_eq!(write.rows()[0].values(), read.rows()[0].values())
                    }
                    (Err(_), Err(_)) => {}
                    other => panic!("read/write CAST AS {target} mismatch: {other:?}"),
                }
            }
        }
    }
);

simulation_test!(
    returning_casts_preserve_binary_and_integer_domains,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('returning-types', NULL)",
                &[],
            )
            .await
            .unwrap();
        for expr in [
            "CAST($1 AS BYTEA)",
            "CAST('hello' AS BYTEA)",
            "9223372036854775808",
            "CAST(1.5 AS BIGINT)",
        ] {
            let params = if expr.contains("$1") {
                vec![Value::Blob(vec![0, 255, 1].into())]
            } else {
                vec![]
            };
            let read = session.execute(&format!("SELECT {expr}"), &params).await;
            let write = session.execute(&format!("UPDATE lix_key_value SET value=value WHERE key='returning-types' RETURNING {expr}"), &params).await;
            match (read, write) {
                (Ok(read), Ok(write)) => {
                    assert_eq!(read.rows()[0].values(), write.rows()[0].values(), "{expr}")
                }
                (Err(_), Err(_)) => {}
                other => panic!("read/RETURNING mismatch for {expr}: {other:?}"),
            }
        }
    }
);

simulation_test!(
    binary_jsonb_casts_validate_utf8_consistently,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_key_value(key,value) VALUES ('binary-jsonb-cast',NULL)",
                &[],
            )
            .await
            .unwrap();
        for bytes in [b"\"hello\"".as_slice(), &[34, 255, 34]] {
            let params = [Value::Blob(bytes.to_vec().into())];
            for sql in [
                "SELECT CAST($1 AS JSONB)",
                "UPDATE lix_key_value SET value=value WHERE key='binary-jsonb-cast' RETURNING CAST($1 AS JSONB)",
                "UPDATE lix_key_value SET value=value WHERE key LIKE 'binary-jsonb-cast' RETURNING CAST($1 AS JSONB)",
            ] {
                let result = session.execute(sql, &params).await;
                if bytes == b"\"hello\"" {
                    assert_eq!(
                        result.unwrap().rows()[0].values(),
                        &[Value::Jsonb(json!("hello").into())],
                        "{sql}"
                    );
                } else {
                    assert!(result.is_err(), "invalid UTF-8 must be rejected: {sql}");
                }
            }
        }
    }
);

simulation_test!(
    native_real_predicates_share_arrow_equality,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('float-equality', '1'::jsonb)",
                &[],
            )
            .await
            .unwrap();
        for (left, right, matches) in [
            (0.0, -0.0, false),
            (-0.0, -0.0, true),
            (f64::NAN, f64::NAN, true),
            (f64::INFINITY, f64::INFINITY, true),
        ] {
            let params = [Value::Real(left), Value::Real(right)];
            for sql in [
                "SELECT key FROM lix_key_value WHERE key = 'float-equality' AND $1 = $2",
                "UPDATE lix_key_value SET value = value WHERE key = 'float-equality' AND $1 = $2 RETURNING key",
                "UPDATE lix_key_value SET value = value WHERE key LIKE 'float-equality' AND $1 = $2 RETURNING key",
            ] {
                let result = session.execute(sql, &params).await.unwrap();
                assert_eq!(
                    result.rows().len(),
                    usize::from(matches),
                    "{sql}: {left:?}, {right:?}"
                );
            }
        }
    }
);

simulation_test!(
    timestamptz_casts_keep_microsecond_contract,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('timestamp-cast-contract', NULL)",
                &[],
            )
            .await
            .unwrap();

        let micros = 1_735_787_045_123_456;
        let stamp = Value::Timestamptz(micros);
        let text_stamp = Value::Text("2025-01-02T03:04:05.123456Z".into());
        let expected_text_stamp = Value::Timestamptz(1_735_787_045_123_456);

        for (sql, params, expected) in [
            ("SELECT CAST(NULL AS TIMESTAMPTZ)", Vec::new(), Value::Null),
            (
                "SELECT CAST('2025-01-02T03:04:05.123456Z' AS TIMESTAMPTZ)",
                Vec::new(),
                expected_text_stamp.clone(),
            ),
            (
                "SELECT CAST($1 AS TIMESTAMPTZ)",
                vec![stamp.clone()],
                stamp.clone(),
            ),
            (
                "SELECT CAST($1 AS TIMESTAMPTZ)",
                vec![text_stamp],
                expected_text_stamp.clone(),
            ),
        ] {
            let result = session.execute(sql, &params).await.unwrap();
            assert_eq!(result.rows()[0].values(), &[expected], "{sql}");
        }

        // An exact-key UPDATE uses the direct row evaluator; LIKE selects the
        // generic DataFusion reference writer. Both RETURNING routes must expose
        // the same microsecond/UTC public value type.
        for predicate in [
            "key = 'timestamp-cast-contract'",
            "key LIKE 'timestamp-cast-contract'",
        ] {
            let result = session
            .execute(
                &format!(
                    "UPDATE lix_key_value SET value=value WHERE {predicate} RETURNING CAST(NULL AS TIMESTAMPTZ), CAST('2025-01-02T03:04:05.123456Z' AS TIMESTAMPTZ), CAST($1 AS TIMESTAMPTZ)"
                ),
                &[stamp.clone()],
            )
            .await
            .unwrap();
            assert_eq!(
                result.rows()[0].values(),
                &[Value::Null, expected_text_stamp.clone(), stamp.clone()],
                "{predicate}"
            );
        }
    }
);
