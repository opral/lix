use lix::{ResultColumnType, RowRef, Value};
use serde_json::json;

simulation_test!(row_ref_parts_is_jsonb_and_reads_canonical_identity, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    let id = "01991b1d-6d8b-7000-8000-0000000000f1";

    let result = session
        .execute(
            "SELECT lix_row_ref_parts(lix_row_ref('lix_file', NULL, $1)) AS parts, \
                    lix_row_ref_parts(lix_row_ref('lix_file', NULL, $1)) ->> 'relation' AS relation, \
                    lix_row_ref_parts(lix_row_ref('lix_file', NULL, $1)) -> 'primary_key' -> 0 ->> 'value' AS key",
            &[Value::Text(id.into())],
        )
        .await
        .unwrap();
    assert_eq!(result.column_types()[0], ResultColumnType::Jsonb);
    assert_eq!(result.rows()[0].values(), &[
        Value::Jsonb(json!({
            "relation": "lix_file",
            "file_id": null,
            "primary_key": [{"type": "uuid", "value": id}],
        }).into()),
        Value::Text("lix_file".into()),
        Value::Text(id.into()),
    ]);

    let conversation_id = "01991b1d-6d8b-7000-8000-0000000000f2";
    session.execute(
        "INSERT INTO lix_file (id, path, content) VALUES ($1, '/row-ref-parts.txt', CAST('body' AS BYTEA))",
        &[Value::Text(id.into())],
    ).await.unwrap();
    session.execute(
        "INSERT INTO lix_conversation (id, target) VALUES ($1, lix_row_ref('lix_file', NULL, $2))",
        &[Value::Text(conversation_id.into()), Value::Text(id.into())],
    ).await.unwrap();
    for detached in [false, true] {
        if detached {
            session.execute("DELETE FROM lix_file WHERE id = $1", &[Value::Text(id.into())]).await.unwrap();
        }
        let anchor = session.execute(
            "SELECT lix_row_ref_parts(target) ->> 'relation' \
             FROM lix_conversation WHERE id = $1",
            &[Value::Text(conversation_id.into())],
        ).await.unwrap();
        assert_eq!(anchor.rows()[0].values(), &[Value::Text("lix_file".into())]);
    }

    let null = session.execute("SELECT lix_row_ref_parts(NULL)", &[]).await.unwrap();
    assert_eq!(null.column_types(), &[ResultColumnType::Jsonb]);
    assert_eq!(null.rows()[0].values(), &[Value::Null]);

    let bad = session.execute("SELECT lix_row_ref_parts('invalid')", &[]).await.unwrap_err();
    assert!(bad.to_string().contains("canonical lix_row_ref"), "{bad}");
    let arity = session.execute("SELECT lix_row_ref_parts()", &[]).await.unwrap_err();
    assert!(arity.to_string().contains("requires exactly 1"), "{arity}");
});

simulation_test!(
    row_ref_constructor_and_default_diff_are_typed_and_canonical,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let id = "01991b1d-6d8b-7000-8000-0000000000f1";

        session
        .execute(
            "INSERT INTO lix_file (id, path, content) VALUES ($1, '/row-ref.txt', CAST('hello' AS BYTEA))",
            &[Value::Text(id.into())],
        )
        .await
        .expect("file should insert before the first explicit checkpoint");

        let direct = session
            .execute(
                "SELECT lix_row_ref('lix_file', NULL, $1) AS row_ref",
                &[Value::Text(id.into())],
            )
            .await
            .expect("UUID file identity should construct");
        assert_eq!(direct.column_types(), &[ResultColumnType::RowRef]);
        let [Value::RowRef(direct_ref)] = direct.rows()[0].values() else {
            panic!("constructor must return the opaque RowRef value kind");
        };
        assert_eq!(
            direct.rows()[0].get::<RowRef>("row_ref").unwrap(),
            direct_ref.clone()
        );

        let diff = session
            .execute(
                "SELECT row_ref, id, diff_type, from_path, to_path \
             FROM lix_diff('lix_file') WHERE id = $1",
                &[Value::Text(id.into())],
            )
            .await
            .expect("one-argument diff should default from checkpoint (or root) to head");
        assert_eq!(
            diff.columns(),
            ["row_ref", "id", "diff_type", "from_path", "to_path"]
        );
        assert_eq!(diff.column_types()[0], ResultColumnType::RowRef);
        assert_eq!(diff.rows().len(), 1);
        assert_eq!(
            diff.rows()[0].values()[0],
            Value::RowRef(direct_ref.clone())
        );
        assert_eq!(diff.rows()[0].values()[1], Value::Text(id.into()));
        assert_eq!(diff.rows()[0].values()[2], Value::Text("added".into()));
        assert!(
            diff.columns()
                .iter()
                .all(|column| !column.contains("row_pk")),
            "the public diff must not leak the JSON row-key representation"
        );

        let inserted = head(&engine, sim.main_branch_id()).await;
        session
            .execute(
                "DELETE FROM lix_file WHERE id = $1",
                &[Value::Text(id.into())],
            )
            .await
            .expect("file should delete");
        let deleted = head(&engine, sim.main_branch_id()).await;
        let removal = session
            .execute(
                "SELECT row_ref, id, diff_type, from_path, to_path \
             FROM lix_diff('lix_file', $1, $2) WHERE id = $3",
                &[
                    Value::Text(inserted),
                    Value::Text(deleted),
                    Value::Text(id.into()),
                ],
            )
            .await
            .expect("removed file identity should remain typed and addressable");
        assert_eq!(removal.rows().len(), 1);
        assert_eq!(
            removal.rows()[0].values()[0],
            Value::RowRef(direct_ref.clone())
        );
        assert_eq!(removal.rows()[0].values()[1], Value::Text(id.into()));
        assert_eq!(removal.rows()[0].values()[2], Value::Text("removed".into()));
        assert_eq!(
            removal.rows()[0].values()[3],
            Value::Text("/row-ref.txt".into())
        );
        assert_eq!(removal.rows()[0].values()[4], Value::Null);

        for (sql, expected) in [
            (
                "SELECT lix_row_ref('missing_relation', NULL, 'x')",
                "does not exist",
            ),
            (
                "SELECT lix_row_ref('lix_file', NULL, 'not-a-uuid')",
                "invalid primary key",
            ),
            (
                "SELECT lix_row_ref('lix_file', NULL, 'a', 'b')",
                "requires 1 primary-key values",
            ),
        ] {
            let error = session.execute(sql, &[]).await.expect_err(sql);
            assert!(
                error.to_string().contains(expected),
                "unexpected error: {error}"
            );
        }
    }
);

simulation_test!(
    parameterized_row_ref_resolves_newly_registered_schema,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("session should open"),
            &engine,
        );
        let schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "rr_parameter_probe",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES ($1)",
                &[Value::Jsonb(schema.into())],
            )
            .await
            .expect("schema registration should succeed");

        let parameterized = session
            .execute(
                "SELECT lix_row_ref($1, $2, $3) AS row_ref",
                &[
                    Value::Text("rr_parameter_probe".into()),
                    Value::Null,
                    Value::Text("probe-1".into()),
                ],
            )
            .await
            .expect("parameterized relation and primary key should construct");
        assert_eq!(parameterized.column_types(), &[ResultColumnType::RowRef]);

        let literal_relation = session
            .execute(
                "SELECT lix_row_ref('rr_parameter_probe', NULL, $1) AS row_ref",
                &[Value::Text("probe-1".into())],
            )
            .await
            .expect("literal relation and parameterized primary key should construct");
        assert_eq!(parameterized.rows(), literal_relation.rows());

        let expression_relation = session
            .execute(
                "SELECT lix_row_ref(CAST($1 AS TEXT), NULL, $2) AS row_ref",
                &[
                    Value::Text("rr_parameter_probe".into()),
                    Value::Text("probe-1".into()),
                ],
            )
            .await
            .expect("an expression relation and parameterized primary key should construct");
        assert_eq!(parameterized.rows(), expression_relation.rows());

        let column_relation = session
            .execute(
                "SELECT lix_row_ref(relation_name, NULL, primary_key) AS row_ref \
                 FROM (VALUES ($1, $2)) AS input(relation_name, primary_key)",
                &[
                    Value::Text("rr_parameter_probe".into()),
                    Value::Text("probe-1".into()),
                ],
            )
            .await
            .expect("column-sourced relation and primary key should construct");
        assert_eq!(parameterized.rows(), column_relation.rows());

        let error = session
            .execute(
                "SELECT lix_row_ref($1, NULL, $2)",
                &[
                    Value::Text("rr_parameter_probe_missing".into()),
                    Value::Text("probe-1".into()),
                ],
            )
            .await
            .expect_err("unknown parameterized relations should remain rejected");
        assert!(error.to_string().contains("does not exist"), "{error}");
    }
);

simulation_test!(
    row_ref_union_preserves_identity_and_rejects_plain_text,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('union-row-ref', 'value')",
                &[],
            )
            .await
            .expect("diff source row should insert");

        let mixed = session
            .execute(
                "SELECT row_ref FROM lix_diff('lix_key_value') \
                 UNION ALL SELECT 'plain text'",
                &[],
            )
            .await
            .expect_err("ROW_REF must not be coerced to text by set operations");
        assert_eq!(mixed.code, lix::LixError::CODE_TYPE_MISMATCH);

        let valid = session
            .execute(
                "SELECT row_ref FROM lix_diff('lix_key_value') \
                 UNION ALL SELECT lix_row_ref('lix_key_value', NULL, 'union-row-ref')",
                &[],
            )
            .await
            .expect("row-ref values from both UNION inputs should materialize");
        assert_eq!(valid.column_types(), &[ResultColumnType::RowRef]);
        assert_eq!(valid.rows().len(), 2);
        assert!(valid
            .rows()
            .iter()
            .all(|row| matches!(row.values(), [Value::RowRef(_)])));

        let nested = session
            .execute(
                "SELECT unnest(unnest(ARRAY[ARRAY[lix_row_ref('lix_key_value', NULL, 'union-row-ref')]])) \
                 AS row_ref",
                &[],
            )
            .await
            .expect("nested arrays should retain ROW_REF identity through each UNNEST depth");
        assert_eq!(nested.column_types(), &[ResultColumnType::RowRef]);
        assert_eq!(nested.rows().len(), 1);
        assert!(matches!(nested.rows()[0].values(), [Value::RowRef(_)]));

        let mixed_nested = session
            .execute(
                "SELECT unnest(unnest(ARRAY[ARRAY['{}'::jsonb]])) AS v \
                 UNION ALL \
                 SELECT unnest(unnest(ARRAY[ARRAY[lix_row_ref('lix_key_value', NULL, 'union-row-ref')]])) AS v",
                &[],
            )
            .await
            .expect_err("set operations must reject nested arrays with different Lix value kinds");
        assert_eq!(mixed_nested.code, lix::LixError::CODE_TYPE_MISMATCH);
    }
);

simulation_test!(
    file_scope_distinguishes_duplicate_keys_in_diff_checkpoint_and_restore,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let first_file = "01991b1d-6d8b-7000-8000-0000000000a1";
        let second_file = "01991b1d-6d8b-7000-8000-0000000000a2";
        session
            .execute(
                "INSERT INTO lix_file (id, path, content) VALUES \
                 ($1, '/row-ref-first', CAST('a' AS BYTEA)), \
                 ($2, '/row-ref-second', CAST('b' AS BYTEA))",
                &[Value::Text(first_file.into()), Value::Text(second_file.into())],
            )
            .await
            .expect("scope files should insert");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_file_id) VALUES \
                 ('shared-row-ref', 'first-before', $1), \
                 ('shared-row-ref', 'second-before', $2)",
                &[Value::Text(first_file.into()), Value::Text(second_file.into())],
            )
            .await
            .expect("duplicate file-scoped keys should insert");
        let baseline = head(&engine, sim.main_branch_id()).await;

        let first_ref = session
            .execute(
                "SELECT lix_row_ref('lix_key_value', $1, 'shared-row-ref')",
                &[Value::Text(first_file.into())],
            )
            .await
            .expect("first scoped row ref should construct")
            .rows()[0]
            .values()[0]
            .clone();
        let second_ref = session
            .execute(
                "SELECT lix_row_ref('lix_key_value', $1, 'shared-row-ref')",
                &[Value::Text(second_file.into())],
            )
            .await
            .expect("second scoped row ref should construct")
            .rows()[0]
            .values()[0]
            .clone();
        assert_ne!(first_ref, second_ref, "file scope must be part of row identity");

        for (file_id, value) in [(first_file, "first-after"), (second_file, "second-after")] {
            session.execute(
                "UPDATE lix_key_value SET value=$1 WHERE key='shared-row-ref' AND lixcol_file_id=$2",
                &[Value::Jsonb(json!(value).into()), Value::Text(file_id.into())],
            ).await.expect("both scoped rows should update");
        }
        let changed = head(&engine, sim.main_branch_id()).await;
        for (file_id, expected_ref, expected_value) in [
            (first_file, &first_ref, "first-after"),
            (second_file, &second_ref, "second-after"),
        ] {
            let filtered = session
                .execute(
                    "SELECT row_ref, key, diff_type, to_value \
                     FROM lix_diff('lix_key_value', $1, $2) \
                     WHERE row_ref = lix_row_ref('lix_key_value', $3, 'shared-row-ref')",
                    &[
                        Value::Text(baseline.clone()),
                        Value::Text(changed.clone()),
                        Value::Text(file_id.into()),
                    ],
                )
                .await
                .expect("exact scoped diff filter should execute");
            assert_eq!(filtered.rows().len(), 1);
            assert_eq!(&filtered.rows()[0].values()[0], expected_ref);
            assert_eq!(filtered.rows()[0].values()[1], Value::Text("shared-row-ref".into()));
            assert_eq!(filtered.rows()[0].values()[2], Value::Text("modified".into()));
            assert_eq!(filtered.rows()[0].values()[3], Value::Jsonb(json!(expected_value).into()));
        }

        let checkpoint = session
            .execute(
                "SELECT commit_id FROM lix_create_checkpoint(ARRAY[\
                   lix_row_ref('lix_key_value', $1, 'shared-row-ref')])",
                &[Value::Text(first_file.into())],
            )
            .await
            .expect("scoped checkpoint should select one duplicate key")
            .rows()[0]
            .get::<String>("commit_id")
            .expect("checkpoint should return a commit id");
        let remaining = session
            .execute(
                "SELECT row_ref FROM lix_diff('lix_key_value') \
                 WHERE row_ref = lix_row_ref('lix_key_value', $1, 'shared-row-ref')",
                &[Value::Text(second_file.into())],
            )
            .await
            .expect("unselected scoped row should remain in the diff");
        assert_eq!(remaining.rows().len(), 1);
        assert_eq!(remaining.rows()[0].values()[0], second_ref);
        let selected_crossed = session
            .execute(
                "SELECT row_ref FROM lix_diff('lix_key_value') \
                 WHERE row_ref = lix_row_ref('lix_key_value', $1, 'shared-row-ref')",
                &[Value::Text(first_file.into())],
            )
            .await
            .expect("selected scoped row should cross the checkpoint");
        assert!(selected_crossed.rows().is_empty());

        for (file_id, value) in [(first_file, "first-later"), (second_file, "second-later")] {
            session.execute(
                "UPDATE lix_key_value SET value=$1 WHERE key='shared-row-ref' AND lixcol_file_id=$2",
                &[Value::Jsonb(json!(value).into()), Value::Text(file_id.into())],
            ).await.expect("later scoped edits should update both rows");
        }
        session
            .execute(
                "SELECT commit_id FROM lix_restore($1, ARRAY[\
                   lix_row_ref('lix_key_value', $2, 'shared-row-ref')])",
                &[
                    Value::Text(checkpoint),
                    Value::Text(first_file.into()),
                ],
            )
            .await
            .expect("row-scoped restore should select only the first file");
        let current = session
            .execute(
                "SELECT value, lixcol_file_id FROM lix_key_value \
                 WHERE key = 'shared-row-ref' ORDER BY lixcol_file_id",
                &[],
            )
            .await
            .expect("both scoped rows should remain readable");
        assert_eq!(
            current.rows().iter().map(|row| row.values().to_vec()).collect::<Vec<_>>(),
            vec![
                vec![Value::Jsonb(json!("first-after").into()), Value::Text(first_file.into())],
                vec![Value::Jsonb(json!("second-later").into()), Value::Text(second_file.into())],
            ]
        );
    }
);

simulation_test!(
    composite_diff_exposes_typed_keys_once_for_every_diff_kind,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine.open_session().await.expect("session should open"),
            &engine,
        );
        let schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "row_ref_composite_member",
            "columns": [
                { "name": "parent_id", "type": "text", "nullable": false },
                { "name": "key", "type": "int8", "nullable": false },
                { "name": "value", "type": "text", "nullable": false }
            ],
            "primary_key": ["parent_id", "key"]
        });
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES ($1)",
                &[Value::Jsonb(schema.into())],
            )
            .await
            .expect("composite schema should register");
        let baseline = head(&engine, sim.main_branch_id()).await;

        session
        .execute(
            "INSERT INTO row_ref_composite_member (parent_id, key, value) VALUES ('parent', 7, 'one')",
            &[],
        )
        .await
        .expect("composite row should insert");
        let inserted = head(&engine, sim.main_branch_id()).await;
        assert_diff(
            &session,
            &baseline,
            &inserted,
            "added",
            Value::Null,
            Value::Text("one".into()),
        )
        .await;

        session
        .execute(
            "UPDATE row_ref_composite_member SET value = 'two' WHERE parent_id = 'parent' AND key = 7",
            &[],
        )
        .await
        .expect("composite row should update");
        let updated = head(&engine, sim.main_branch_id()).await;
        assert_diff(
            &session,
            &inserted,
            &updated,
            "modified",
            Value::Text("one".into()),
            Value::Text("two".into()),
        )
        .await;

        session
            .execute(
                "DELETE FROM row_ref_composite_member WHERE parent_id = 'parent' AND key = 7",
                &[],
            )
            .await
            .expect("composite row should delete");
        let deleted = head(&engine, sim.main_branch_id()).await;
        assert_diff(
            &session,
            &updated,
            &deleted,
            "removed",
            Value::Text("two".into()),
            Value::Null,
        )
        .await;
    }
);

async fn assert_diff(
    session: &crate::support::simulation_test::engine::SimSession,
    from: &str,
    to: &str,
    kind: &str,
    from_value: Value,
    to_value: Value,
) {
    let identity_only = session
        .execute(
            "SELECT row_ref, parent_id, key, diff_type \
             FROM lix_diff('row_ref_composite_member', $1, $2) \
             WHERE parent_id = 'parent' AND key = 7",
            &[Value::Text(from.into()), Value::Text(to.into())],
        )
        .await
        .expect("key-only composite diff should not require either side payload");
    assert_eq!(identity_only.rows().len(), 1);
    assert!(matches!(
        identity_only.rows()[0].values(),
        [Value::RowRef(_), Value::Text(parent), Value::Integer(7), Value::Text(actual_kind)]
            if parent == "parent" && actual_kind == kind
    ));

    let result = session
        .execute(
            "SELECT row_ref, parent_id, key, diff_type, from_value, to_value \
             FROM lix_diff('row_ref_composite_member', $1, $2) \
             WHERE parent_id = 'parent' AND key = 7",
            &[Value::Text(from.into()), Value::Text(to.into())],
        )
        .await
        .expect("composite diff should execute");
    assert_eq!(
        result.columns(),
        [
            "row_ref",
            "parent_id",
            "key",
            "diff_type",
            "from_value",
            "to_value"
        ]
    );
    assert_eq!(result.column_types()[0], ResultColumnType::RowRef);
    assert_eq!(result.rows().len(), 1);
    let values = result.rows()[0].values();
    assert!(matches!(values[0], Value::RowRef(_)));
    assert_eq!(values[1], Value::Text("parent".into()));
    assert_eq!(values[2], Value::Integer(7));
    assert_eq!(values[3], Value::Text(kind.into()));
    assert_eq!(values[4], from_value);
    assert_eq!(values[5], to_value);

    let direct = session
        .execute(
            "SELECT lix_row_ref('row_ref_composite_member', NULL, 'parent', 7)",
            &[],
        )
        .await
        .expect("typed composite identity should construct");
    assert_eq!(values[0], direct.rows()[0].values()[0]);
}

async fn head(engine: &lix::engine::Engine, branch_id: &str) -> String {
    engine
        .load_branch_head_commit_id(branch_id)
        .await
        .expect("head should load")
        .expect("head should exist")
        .to_string()
}
