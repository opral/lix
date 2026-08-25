use lix::{LixError, Value};

use super::assert_rows_eq;

simulation_test!(
    lix_surfaces_classifies_the_public_sql_contract,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let surfaces = session
        .execute(
            "SELECT surface_name, surface_class, relation_kind, can_read, can_insert, is_side_effecting \
             FROM information_schema.lix_surfaces \
             WHERE surface_name IN (\
               'lix_active_branch_id', 'lix_create_checkpoint', 'lix_diff',\
               'lix_file', 'lix_restore', 'lix_root_commit_id'\
             ) \
             ORDER BY surface_name",
            &[],
        )
        .await
        .expect("the unified Lix surface catalog should be readable");

        assert_rows_eq(
            surfaces,
            vec![
                vec![
                    Value::Text("lix_active_branch_id".to_string()),
                    Value::Text("SCALAR_FUNCTION".to_string()),
                    Value::Null,
                    Value::Boolean(true),
                    Value::Boolean(false),
                    Value::Boolean(false),
                ],
                vec![
                    Value::Text("lix_create_checkpoint".to_string()),
                    Value::Text("COMMAND_SINK".to_string()),
                    Value::Null,
                    Value::Boolean(false),
                    Value::Boolean(true),
                    Value::Boolean(true),
                ],
                vec![
                    Value::Text("lix_diff".to_string()),
                    Value::Text("TABLE_FUNCTION".to_string()),
                    Value::Null,
                    Value::Boolean(true),
                    Value::Boolean(false),
                    Value::Boolean(false),
                ],
                vec![
                    Value::Text("lix_file".to_string()),
                    Value::Text("RELATION".to_string()),
                    Value::Text("VIEW".to_string()),
                    Value::Boolean(true),
                    Value::Boolean(true),
                    Value::Boolean(false),
                ],
                vec![
                    Value::Text("lix_restore".to_string()),
                    Value::Text("COMMAND_SINK".to_string()),
                    Value::Null,
                    Value::Boolean(false),
                    Value::Boolean(true),
                    Value::Boolean(true),
                ],
                vec![
                    Value::Text("lix_root_commit_id".to_string()),
                    Value::Text("SCALAR_FUNCTION".to_string()),
                    Value::Null,
                    Value::Boolean(true),
                    Value::Boolean(false),
                    Value::Boolean(false),
                ],
            ],
        );

        let relations = session
            .execute(
                "SELECT table_name, table_type \
             FROM information_schema.tables \
             WHERE table_schema = 'public' \
               AND table_name IN ('lix_change', 'lix_file', 'lix_key_value') \
             ORDER BY table_name",
                &[],
            )
            .await
            .expect("standard relation classification should be readable");
        assert_rows_eq(
            relations,
            vec![
                vec![
                    Value::Text("lix_change".to_string()),
                    Value::Text("VIEW".to_string()),
                ],
                vec![
                    Value::Text("lix_file".to_string()),
                    Value::Text("VIEW".to_string()),
                ],
                vec![
                    Value::Text("lix_key_value".to_string()),
                    Value::Text("BASE TABLE".to_string()),
                ],
            ],
        );
    }
);

simulation_test!(
    diff_table_function_advertises_each_relations_typed_columns,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let columns = session
            .execute(
                "SELECT source_relation, result_column, data_type, is_nullable, lix_value_kind \
             FROM information_schema.table_functions \
             WHERE function_name = 'lix_diff' \
               AND source_relation IN ('lix_file', 'lix_key_value') \
               AND result_column IN (\
                 'row_pk', 'diff_type', 'from_path', 'to_path', \
                 'from_value', 'to_value', 'row_count'\
               ) \
             ORDER BY source_relation, ordinal_position",
                &[],
            )
            .await
            .expect("relation-specific diff result metadata should be readable");

        assert_rows_eq(
            columns,
            vec![
                vec![
                    Value::Text("lix_file".to_string()),
                    Value::Text("row_pk".to_string()),
                    Value::Text("TEXT".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Text("JSONB".to_string()),
                ],
                vec![
                    Value::Text("lix_file".to_string()),
                    Value::Text("diff_type".to_string()),
                    Value::Text("TEXT".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Null,
                ],
                vec![
                    Value::Text("lix_file".to_string()),
                    Value::Text("from_path".to_string()),
                    Value::Text("TEXT".to_string()),
                    Value::Text("YES".to_string()),
                    Value::Null,
                ],
                vec![
                    Value::Text("lix_file".to_string()),
                    Value::Text("to_path".to_string()),
                    Value::Text("TEXT".to_string()),
                    Value::Text("YES".to_string()),
                    Value::Null,
                ],
                vec![
                    Value::Text("lix_file".to_string()),
                    Value::Text("row_count".to_string()),
                    Value::Text("BIGINT".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Null,
                ],
                vec![
                    Value::Text("lix_key_value".to_string()),
                    Value::Text("row_pk".to_string()),
                    Value::Text("TEXT".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Text("JSONB".to_string()),
                ],
                vec![
                    Value::Text("lix_key_value".to_string()),
                    Value::Text("diff_type".to_string()),
                    Value::Text("TEXT".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Null,
                ],
                vec![
                    Value::Text("lix_key_value".to_string()),
                    Value::Text("from_value".to_string()),
                    Value::Text("TEXT".to_string()),
                    Value::Text("YES".to_string()),
                    Value::Text("JSONB".to_string()),
                ],
                vec![
                    Value::Text("lix_key_value".to_string()),
                    Value::Text("to_value".to_string()),
                    Value::Text("TEXT".to_string()),
                    Value::Text("YES".to_string()),
                    Value::Text("JSONB".to_string()),
                ],
                vec![
                    Value::Text("lix_key_value".to_string()),
                    Value::Text("row_count".to_string()),
                    Value::Text("BIGINT".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Null,
                ],
            ],
        );

        let retired = session
            .execute(
                "SELECT result_column FROM information_schema.table_functions \
             WHERE function_name = 'lix_diff' \
               AND result_column IN ('diff_id', 'before_change_id', 'after_change_id')",
                &[],
            )
            .await
            .expect("retired diff metadata should remain queryable");
        assert!(
            retired.rows().is_empty(),
            "retired diff columns must not be advertised"
        );
    }
);

simulation_test!(
    public_catalog_hides_internal_branch_surfaces,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
             VALUES (CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"no_explicit_branch_surface\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB), false, false)",
            &[],
        )
        .await
        .expect("registered schema insert should succeed");

        let tables = session
            .execute(
                "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'public' AND table_name LIKE '%\\_by\\_branch' ESCAPE '\\'",
                &[],
            )
            .await
            .expect("public table catalog should be readable");
        assert!(
            tables.rows().is_empty(),
            "no explicit-branch tables may remain"
        );

        let branch_component_tables = session
            .execute(
                "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'public' \
               AND table_name IN ('lix_branch_descriptor', 'lix_branch_ref')",
                &[],
            )
            .await
            .expect("public table catalog should be readable");
        assert!(
            branch_component_tables.rows().is_empty(),
            "internal branch component tables must not be advertised"
        );

        let branch_component_history = session
            .execute(
                "SELECT function_name FROM information_schema.table_functions \
             WHERE function_schema = 'public' \
               AND function_name IN (\
                 'lix_branch_descriptor_history', 'lix_branch_ref_history'\
               )",
                &[],
            )
            .await
            .expect("public table-function catalog should be readable");
        assert!(
            branch_component_history.rows().is_empty(),
            "internal branch history functions must not be advertised"
        );

        let columns = session
            .execute(
                "SELECT table_name FROM information_schema.columns \
             WHERE table_schema = 'public' AND column_name = 'lixcol_branch_id'",
                &[],
            )
            .await
            .expect("public column catalog should be readable");
        assert!(
            columns.rows().is_empty(),
            "no public data relation may expose lixcol_branch_id"
        );

        let error = session
            .execute("SELECT * FROM no_explicit_branch_surface_by_branch", &[])
            .await
            .expect_err("retired explicit-branch table names must fail closed");
        assert!(
            error
                .message
                .contains("no_explicit_branch_surface_by_branch"),
            "the unknown-table error should identify the retired surface: {error:?}"
        );
    }
);

simulation_test!(
    information_schema_never_exposes_raw_snapshot_content,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                   CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"snapshot_column_absence\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"title\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
                   false,\
                   false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");

        let table_columns = session
            .execute(
                "SELECT table_name \
                 FROM information_schema.columns \
                 WHERE column_name = 'lixcol_snapshot_content'",
                &[],
            )
            .await
            .expect("table column catalog should be readable");
        assert!(table_columns.rows().is_empty());

        let function_columns = session
            .execute(
                "SELECT function_name \
                 FROM information_schema.table_functions \
                 WHERE result_column = 'lixcol_snapshot_content'",
                &[],
            )
            .await
            .expect("table-function column catalog should be readable");
        assert!(function_columns.rows().is_empty());
    }
);

simulation_test!(
    information_schema_columns_select_star_is_safe_for_public_results,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        let result = session
            .execute(
                "SELECT * FROM information_schema.columns WHERE table_name = 'lix_file'",
                &[],
            )
            .await
            .expect("the complete lix_file column contract should be readable");
        assert!(!result.rows().is_empty());
        assert!(
            result
                .columns()
                .iter()
                .any(|column| column == "lix_value_kind")
        );
        assert!(
            result
                .columns()
                .iter()
                .any(|column| column == "lix_insert_policy")
        );

        let content_row = result
            .rows()
            .iter()
            .find(|row| {
                row.value("column_name")
                    .expect("column_name should be present")
                    == &Value::Text("content".to_string())
            })
            .expect("lix_file content column should be described");
        assert_eq!(
            content_row
                .value("character_octet_length")
                .expect("character_octet_length should be present"),
            &Value::Null,
            "unbounded binary content must not advertise an artificial i64::MAX length",
        );
        assert!(
            !result.rows().iter().any(|row| {
                matches!(
                    row.value("column_name"),
                    Ok(Value::Text(column_name)) if column_name == "data"
                )
            }),
            "the retired data column must not appear in the public schema",
        );
    }
);

simulation_test!(
    information_schema_exposes_executable_lix_column_contract,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                   CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_column_contract\",\"columns\":[{\"name\":\"id\",\"type\":\"uuid\",\"nullable\":false,\"default_expression\":\"uuidv7()\"},{\"name\":\"title\",\"type\":\"text\",\"nullable\":false},{\"name\":\"note\",\"type\":\"text\",\"nullable\":true},{\"name\":\"count\",\"type\":\"int8\",\"nullable\":false},{\"name\":\"ratio\",\"type\":\"float8\",\"nullable\":false},{\"name\":\"active\",\"type\":\"boolean\",\"nullable\":false},{\"name\":\"metadata\",\"type\":\"jsonb\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
                   false,\
                   false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");
        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                   CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_no_pk_contract\",\"columns\":[{\"name\":\"name\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"name\"]}' AS JSONB),\
                   false,\
                   false\
                 )",
                &[],
            )
            .await
            .expect("no-primary-key schema insert should succeed");
        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                   CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"columns\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"table_name\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
                   false,\
                   false\
                 )",
                &[],
            )
            .await
            .expect("schema colliding with an information-schema table name should register");

        let information_schema_self_contract = session
            .execute(
                "SELECT is_nullable, lix_insert_policy \
                 FROM information_schema.columns \
                 WHERE table_schema = 'information_schema' \
                   AND table_name = 'columns' \
                   AND column_name = 'table_name'",
                &[],
            )
            .await
            .expect("information schema should retain its own column contract");
        assert_rows_eq(
            information_schema_self_contract,
            vec![vec![
                Value::Text("NO".to_string()),
                Value::Text("READ_ONLY".to_string()),
            ]],
        );

        let read_only_row_contract = session
            .execute(
                "SELECT table_name, column_name, lix_insert_policy \
                 FROM information_schema.columns \
                 WHERE table_name = 'lix_commit' \
                   AND column_name = 'id' \
                 ORDER BY table_name",
                &[],
            )
            .await
            .expect("read-only generated schema surfaces should introspect");
        assert_rows_eq(
            read_only_row_contract,
            vec![vec![
                Value::Text("lix_commit".to_string()),
                Value::Text("id".to_string()),
                Value::Text("READ_ONLY".to_string()),
            ]],
        );

        let result = session
            .execute(
                "SELECT column_name, data_type, is_nullable, column_default, \
                        lix_value_kind, lix_insert_policy \
                 FROM information_schema.columns \
                 WHERE table_name = 'engine_column_contract' \
                   AND column_name IN ('active', 'count', 'id', 'metadata', 'note', 'ratio', 'title') \
                 ORDER BY column_name",
                &[],
            )
            .await
            .expect("information schema query should succeed");

        assert_rows_eq(
            result,
            vec![
                vec![
                    Value::Text("active".to_string()),
                    Value::Text("BOOLEAN".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Null,
                    Value::Null,
                    Value::Text("REQUIRED".to_string()),
                ],
                vec![
                    Value::Text("count".to_string()),
                    Value::Text("BIGINT".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Null,
                    Value::Null,
                    Value::Text("REQUIRED".to_string()),
                ],
                vec![
                    Value::Text("id".to_string()),
                    Value::Text("TEXT".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Text("uuidv7()".to_string()),
                    Value::Null,
                    Value::Text("DEFAULT".to_string()),
                ],
                vec![
                    Value::Text("metadata".to_string()),
                    Value::Text("TEXT".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Null,
                    Value::Text("JSONB".to_string()),
                    Value::Text("REQUIRED".to_string()),
                ],
                vec![
                    Value::Text("note".to_string()),
                    Value::Text("TEXT".to_string()),
                    Value::Text("YES".to_string()),
                    Value::Null,
                    Value::Null,
                    Value::Text("OPTIONAL".to_string()),
                ],
                vec![
                    Value::Text("ratio".to_string()),
                    Value::Text("DOUBLE PRECISION".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Null,
                    Value::Null,
                    Value::Text("REQUIRED".to_string()),
                ],
                vec![
                    Value::Text("title".to_string()),
                    Value::Text("TEXT".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Null,
                    Value::Null,
                    Value::Text("REQUIRED".to_string()),
                ],
            ],
        );

        let file_contract = session
            .execute(
                "SELECT column_name, data_type, is_nullable, column_default, lix_insert_policy \
                 FROM information_schema.columns \
                 WHERE table_name = 'lix_file' \
                   AND column_name IN ('content', 'id') \
                 ORDER BY column_name",
                &[],
            )
            .await
            .expect("file contract query should succeed");
        assert_rows_eq(
            file_contract,
            vec![
                vec![
                    Value::Text("content".to_string()),
                    Value::Text("BYTEA".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Text("CAST('' AS BYTEA)".to_string()),
                    Value::Text("DEFAULT".to_string()),
                ],
                vec![
                    Value::Text("id".to_string()),
                    Value::Text("TEXT".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Text("uuidv7()".to_string()),
                    Value::Text("DEFAULT".to_string()),
                ],
            ],
        );

        let by_branch_contract = session
            .execute(
                "SELECT table_name, is_nullable, lix_insert_policy \
                 FROM information_schema.columns \
                 WHERE table_name IN (\
                   'engine_column_contract_by_branch', \
                   'lix_directory_by_branch', \
                   'lix_file_by_branch'\
                 ) \
                   AND column_name = 'lixcol_branch_id' \
                 ORDER BY table_name",
                &[],
            )
            .await
            .expect("by-branch contract query should succeed");
        assert_rows_eq(by_branch_contract, vec![]);

        let identity_contract = session
            .execute(
                "SELECT table_name, column_name, is_nullable, column_default, lix_insert_policy \
                 FROM information_schema.columns \
                 WHERE (\
                   table_name = 'engine_column_contract' \
                   AND column_name IN (\
                     'lixcol_change_id', 'lixcol_commit_id', 'lixcol_created_at', \
                     'lixcol_row_pk', 'lixcol_global', 'lixcol_schema_key', \
                     'lixcol_untracked', 'lixcol_updated_at'\
                   )\
                 ) OR (\
                   table_name = 'engine_no_pk_contract' \
                   AND column_name = 'lixcol_row_pk'\
                 ) \
                 ORDER BY table_name, column_name",
                &[],
            )
            .await
            .expect("row system-column contract query should succeed");
        assert_rows_eq(
            identity_contract,
            vec![
                vec![
                    Value::Text("engine_column_contract".to_string()),
                    Value::Text("lixcol_change_id".to_string()),
                    Value::Text("YES".to_string()),
                    Value::Null,
                    Value::Text("READ_ONLY".to_string()),
                ],
                vec![
                    Value::Text("engine_column_contract".to_string()),
                    Value::Text("lixcol_commit_id".to_string()),
                    Value::Text("YES".to_string()),
                    Value::Null,
                    Value::Text("READ_ONLY".to_string()),
                ],
                vec![
                    Value::Text("engine_column_contract".to_string()),
                    Value::Text("lixcol_created_at".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Null,
                    Value::Text("READ_ONLY".to_string()),
                ],
                vec![
                    Value::Text("engine_column_contract".to_string()),
                    Value::Text("lixcol_global".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Text("FALSE".to_string()),
                    Value::Text("DEFAULT".to_string()),
                ],
                vec![
                    Value::Text("engine_column_contract".to_string()),
                    Value::Text("lixcol_row_pk".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Null,
                    Value::Text("CONDITIONAL".to_string()),
                ],
                vec![
                    Value::Text("engine_column_contract".to_string()),
                    Value::Text("lixcol_schema_key".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Null,
                    Value::Text("READ_ONLY".to_string()),
                ],
                vec![
                    Value::Text("engine_column_contract".to_string()),
                    Value::Text("lixcol_untracked".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Text("FALSE".to_string()),
                    Value::Text("DEFAULT".to_string()),
                ],
                vec![
                    Value::Text("engine_column_contract".to_string()),
                    Value::Text("lixcol_updated_at".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Null,
                    Value::Text("READ_ONLY".to_string()),
                ],
                vec![
                    Value::Text("engine_no_pk_contract".to_string()),
                    Value::Text("lixcol_row_pk".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Null,
                    Value::Text("CONDITIONAL".to_string()),
                ],
            ],
        );

        let filesystem_system_contract = session
            .execute(
                "SELECT table_name, column_name, is_nullable, column_default, lix_insert_policy \
                 FROM information_schema.columns \
                 WHERE table_name IN ('lix_file', 'lix_directory') \
                   AND column_name IN (\
                     'lixcol_created_at', 'lixcol_global', \
                     'lixcol_untracked', 'lixcol_updated_at'\
                   ) \
                 ORDER BY table_name, column_name",
                &[],
            )
            .await
            .expect("filesystem system-column contract query should succeed");
        let mut expected_filesystem_system_contract = Vec::new();
        for table_name in ["lix_directory", "lix_file"] {
            expected_filesystem_system_contract.extend([
                vec![
                    Value::Text(table_name.to_string()),
                    Value::Text("lixcol_created_at".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Null,
                    Value::Text("READ_ONLY".to_string()),
                ],
                vec![
                    Value::Text(table_name.to_string()),
                    Value::Text("lixcol_global".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Text("FALSE".to_string()),
                    Value::Text("DEFAULT".to_string()),
                ],
                vec![
                    Value::Text(table_name.to_string()),
                    Value::Text("lixcol_untracked".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Text("FALSE".to_string()),
                    Value::Text("DEFAULT".to_string()),
                ],
                vec![
                    Value::Text(table_name.to_string()),
                    Value::Text("lixcol_updated_at".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Null,
                    Value::Text("READ_ONLY".to_string()),
                ],
            ]);
        }
        assert_rows_eq(
            filesystem_system_contract,
            expected_filesystem_system_contract,
        );

        let history_contract = session
            .execute(
                "SELECT result_column, is_nullable \
                 FROM information_schema.table_functions \
                 WHERE function_name = 'lix_history' \
                   AND source_relation = 'engine_column_contract' \
                   AND result_column IN ('id', 'title') \
                 ORDER BY result_column",
                &[],
            )
            .await
            .expect("row history nullability contract query should succeed");
        assert_rows_eq(
            history_contract,
            vec![
                vec![Value::Text("id".to_string()), Value::Text("NO".to_string())],
                vec![
                    Value::Text("title".to_string()),
                    Value::Text("YES".to_string()),
                ],
            ],
        );
    }
);

simulation_test!(
    advertised_lix_types_work_in_select_insert_and_update,
    |sim| async move {
        #[derive(Clone, Debug)]
        struct CastContract {
            table_name: String,
            column_name: String,
            data_type: String,
            value_kind: Option<String>,
        }

        fn values_for_contract(contract: &CastContract) -> (Value, Value, Value, Value, Value) {
            match (
                contract.column_name.as_str(),
                contract.value_kind.as_deref(),
            ) {
                ("text_value", None) => (
                    Value::Integer(101),
                    Value::Text("101".to_string()),
                    Value::Text("101".to_string()),
                    Value::Integer(202),
                    Value::Text("202".to_string()),
                ),
                ("integer_value", None) => (
                    Value::Text("41".to_string()),
                    Value::Integer(41),
                    Value::Integer(41),
                    Value::Text("42".to_string()),
                    Value::Integer(42),
                ),
                ("number_value", None) => (
                    Value::Text("1.25".to_string()),
                    Value::Real(1.25),
                    Value::Real(1.25),
                    Value::Text("2.5".to_string()),
                    Value::Real(2.5),
                ),
                ("boolean_value", None) => (
                    Value::Text("true".to_string()),
                    Value::Boolean(true),
                    Value::Boolean(true),
                    Value::Text("false".to_string()),
                    Value::Boolean(false),
                ),
                ("json_value", Some("JSONB")) => (
                    Value::Text("{\"phase\":\"insert\"}".to_string()),
                    Value::Text("{\"phase\":\"insert\"}".to_string()),
                    Value::Jsonb(serde_json::json!({"phase": "insert"}).into()),
                    Value::Text("{\"phase\":\"update\"}".to_string()),
                    Value::Jsonb(serde_json::json!({"phase": "update"}).into()),
                ),
                ("content", None) => (
                    Value::Text("before".to_string()),
                    Value::Blob(b"before".to_vec().into()),
                    Value::Blob(b"before".to_vec().into()),
                    Value::Text("after".to_string()),
                    Value::Blob(b"after".to_vec().into()),
                ),
                _ => panic!("unexpected advertised cast contract: {contract:?}"),
            }
        }

        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                   CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_scalar_cast_contract\",\"columns\":[{\"name\":\"id\",\"type\":\"uuid\",\"nullable\":false,\"default_expression\":\"uuidv7()\"},{\"name\":\"text_value\",\"type\":\"text\",\"nullable\":false},{\"name\":\"integer_value\",\"type\":\"int8\",\"nullable\":false},{\"name\":\"number_value\",\"type\":\"float8\",\"nullable\":false},{\"name\":\"boolean_value\",\"type\":\"boolean\",\"nullable\":false},{\"name\":\"json_value\",\"type\":\"jsonb\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
                   false,\
                   false\
                 )",
                &[],
            )
            .await
            .expect("registered scalar cast schema should succeed");

        let contract_rows = session
            .execute(
                "SELECT table_name, column_name, data_type, lix_value_kind \
                 FROM information_schema.columns \
                 WHERE (\
                   table_name = 'engine_scalar_cast_contract' \
                   AND column_name IN (\
                     'text_value', 'integer_value', 'number_value', \
                     'boolean_value', 'json_value'\
                   )\
                 ) OR (table_name = 'lix_file' AND column_name = 'content') \
                 ORDER BY table_name, column_name",
                &[],
            )
            .await
            .expect("advertised cast contract query should succeed");
        let contracts = contract_rows
            .rows()
            .iter()
            .map(|row| {
                let [
                    Value::Text(table_name),
                    Value::Text(column_name),
                    Value::Text(data_type),
                    value_kind,
                ] = row.values()
                else {
                    panic!("unexpected information_schema cast row: {:?}", row.values());
                };
                let value_kind = match value_kind {
                    Value::Null => None,
                    Value::Text(value) => Some(value.clone()),
                    other => panic!("unexpected lix_value_kind: {other:?}"),
                };
                CastContract {
                    table_name: table_name.clone(),
                    column_name: column_name.clone(),
                    data_type: data_type.clone(),
                    value_kind,
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(contracts.len(), 6, "expected five row types plus BYTEA");

        for contract in &contracts {
            let expected_type = match contract.column_name.as_str() {
                "text_value" | "json_value" => "TEXT",
                "integer_value" => "BIGINT",
                "number_value" => "DOUBLE PRECISION",
                "boolean_value" => "BOOLEAN",
                "content" => "BYTEA",
                other => panic!("unexpected contract column {other}"),
            };
            assert_eq!(contract.data_type, expected_type);
            assert_eq!(
                contract.value_kind.as_deref(),
                (contract.column_name == "json_value").then_some("JSONB")
            );

            let (insert_param, select_expected, _, _, _) = values_for_contract(contract);
            let select_cast = session
                .execute(
                    &format!("SELECT CAST($1 AS {}) AS cast_value", contract.data_type),
                    &[insert_param],
                )
                .await
                .unwrap_or_else(|error| {
                    panic!("SELECT cast should follow {contract:?}: {error:?}")
                });
            assert_rows_eq(select_cast, vec![vec![select_expected]]);
        }

        let row_contracts = contracts
            .iter()
            .filter(|contract| contract.table_name == "engine_scalar_cast_contract")
            .collect::<Vec<_>>();
        let row_columns = row_contracts
            .iter()
            .map(|contract| contract.column_name.clone())
            .collect::<Vec<_>>();
        let insert_params = row_contracts
            .iter()
            .map(|contract| values_for_contract(contract).0)
            .collect::<Vec<_>>();
        let insert_casts = row_contracts
            .iter()
            .enumerate()
            .map(|(index, contract)| format!("CAST(${} AS {})", index + 1, contract.data_type))
            .collect::<Vec<_>>();
        session
            .execute(
                &format!(
                    "INSERT INTO engine_scalar_cast_contract ({}) VALUES ({})",
                    row_columns.join(", "),
                    insert_casts.join(", ")
                ),
                &insert_params,
            )
            .await
            .expect("all advertised row casts should work in a bound INSERT");

        let inserted = session
            .execute(
                &format!(
                    "SELECT {} FROM engine_scalar_cast_contract",
                    row_columns.join(", ")
                ),
                &[],
            )
            .await
            .expect("inserted scalar cast row should be readable");
        assert_rows_eq(
            inserted,
            vec![
                row_contracts
                    .iter()
                    .map(|contract| values_for_contract(contract).2)
                    .collect(),
            ],
        );

        let update_params = row_contracts
            .iter()
            .map(|contract| values_for_contract(contract).3)
            .collect::<Vec<_>>();
        let update_casts = row_contracts
            .iter()
            .enumerate()
            .map(|(index, contract)| {
                format!(
                    "{} = CAST(${} AS {})",
                    contract.column_name,
                    index + 1,
                    contract.data_type
                )
            })
            .collect::<Vec<_>>();
        session
            .execute(
                &format!(
                    "UPDATE engine_scalar_cast_contract SET {}",
                    update_casts.join(", ")
                ),
                &update_params,
            )
            .await
            .expect("all advertised row casts should work in a bound UPDATE");
        let updated = session
            .execute(
                &format!(
                    "SELECT {} FROM engine_scalar_cast_contract",
                    row_columns.join(", ")
                ),
                &[],
            )
            .await
            .expect("updated scalar cast row should be readable");
        assert_rows_eq(
            updated,
            vec![
                row_contracts
                    .iter()
                    .map(|contract| values_for_contract(contract).4)
                    .collect(),
            ],
        );

        let bytea_contract = contracts
            .iter()
            .find(|contract| contract.table_name == "lix_file")
            .expect("lix_file.content BYTEA contract should exist");
        let (file_insert_param, _, _, file_update_param, file_update_expected) =
            values_for_contract(bytea_contract);
        session
            .execute(
                &format!(
                    "INSERT INTO lix_file (path, content) \
                     VALUES ('/contract.bin', CAST($1 AS {}))",
                    bytea_contract.data_type
                ),
                &[file_insert_param],
            )
            .await
            .expect("advertised BYTEA should work in a bound INSERT");
        session
            .execute(
                &format!(
                    "UPDATE lix_file SET content = CAST($1 AS {}) \
                     WHERE path = '/contract.bin'",
                    bytea_contract.data_type
                ),
                &[file_update_param],
            )
            .await
            .expect("advertised BYTEA should work in a bound UPDATE");
        let file = session
            .execute(
                "SELECT content FROM lix_file WHERE path = '/contract.bin'",
                &[],
            )
            .await
            .expect("file read should succeed");
        assert_rows_eq(file, vec![vec![file_update_expected]]);

        for sql in [
            "SELECT CAST(1 AS INTEGER)",
            "SELECT CAST(CAST('2026-01-01' AS DATE) AS TEXT)",
            "SELECT CAST(CAST('12.50' AS DECIMAL(10, 2)) AS TEXT)",
            "SELECT CAST(CAST('2026-01-01T00:00:00' AS TIMESTAMP) AS TEXT)",
            "SELECT TRY_CAST('not-an-integer' AS INTEGER)",
        ] {
            session.execute(sql, &[]).await.unwrap_or_else(|error| {
                panic!("DataFusion read-expression cast should remain available: {error:?}")
            });
        }

        let binary_select_error = session
            .execute("SELECT CAST('legacy' AS BINARY)", &[])
            .await
            .expect_err("retired BINARY spelling must not be accepted by SELECT");
        assert_eq!(binary_select_error.code, LixError::CODE_DIALECT_UNSUPPORTED);

        for (unsupported, column_name, value_sql, table_name) in [
            (
                "VARCHAR",
                "text_value",
                "'legacy'",
                "engine_scalar_cast_contract",
            ),
            (
                "INT64",
                "integer_value",
                "'7'",
                "engine_scalar_cast_contract",
            ),
            (
                "FLOAT64",
                "number_value",
                "'1.5'",
                "engine_scalar_cast_contract",
            ),
            (
                "BOOL",
                "boolean_value",
                "'true'",
                "engine_scalar_cast_contract",
            ),
            ("BINARY", "content", "'legacy'", "lix_file"),
        ] {
            let write_error = session
                .execute(
                    &format!(
                        "UPDATE {table_name} SET {column_name} = \
                         CAST({value_sql} AS {unsupported})"
                    ),
                    &[],
                )
                .await
                .expect_err("unsupported cast spelling should not be accepted by bound UPDATE");
            assert_eq!(write_error.code, LixError::CODE_UNSUPPORTED_SQL);
        }

        let binary_insert_error = session
            .execute(
                "INSERT INTO lix_file (path, content) \
                 VALUES ('/legacy-binary.bin', CAST('legacy' AS BINARY))",
                &[],
            )
            .await
            .expect_err("BINARY must not be accepted by bound INSERT");
        assert_eq!(binary_insert_error.code, LixError::CODE_UNSUPPORTED_SQL);
    }
);

simulation_test!(
    defaulted_columns_distinguish_omission_from_explicit_null,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute("INSERT INTO lix_file (path) VALUES ('/generated.txt')", &[])
            .await
            .expect("omitted file id should generate");
        session
            .execute(
                "INSERT INTO lix_directory (path) VALUES ('/generated')",
                &[],
            )
            .await
            .expect("omitted directory id should generate");
        session
            .execute(
                "INSERT INTO lix_file (path) \
                 SELECT '/query-generated.txt' \
                 FROM information_schema.tables \
                 WHERE table_name = 'lix_file'",
                &[],
            )
            .await
            .expect("query-backed file insert should see information_schema and default id/data");
        session
            .execute(
                "INSERT INTO lix_directory (path) \
                 SELECT '/query-generated' \
                 FROM information_schema.tables \
                 WHERE table_name = 'lix_directory'",
                &[],
            )
            .await
            .expect("query-backed directory insert should see information_schema and default id");
        session
            .execute(
                "INSERT INTO lix_file (path) VALUES ('/upsert-generated.txt') \
                 ON CONFLICT (path) DO NOTHING",
                &[],
            )
            .await
            .expect("upsert with an omitted file id should generate");
        session
            .execute(
                "UPDATE lix_file SET content = CAST('old' AS BYTEA) WHERE path = '/generated.txt'",
                &[],
            )
            .await
            .expect("seed file contents should update");
        session
            .execute(
                "INSERT INTO lix_file (path) VALUES ('/generated.txt') \
                 ON CONFLICT (path) DO UPDATE SET content = excluded.content",
                &[],
            )
            .await
            .expect("excluded.content should materialize its advertised empty default");
        let defaulted_upsert = session
            .execute(
                "SELECT content FROM lix_file WHERE path = '/generated.txt'",
                &[],
            )
            .await
            .expect("defaulted upsert file should be readable");
        assert_rows_eq(defaulted_upsert, vec![vec![Value::Blob(Vec::new().into())]]);

        session
            .execute(
                "INSERT INTO lix_file (path) VALUES ('/excluded-file-id.txt')",
                &[],
            )
            .await
            .expect("file id default seed should insert");
        let file_before = session
            .execute(
                "SELECT id FROM lix_file WHERE path = '/excluded-file-id.txt'",
                &[],
            )
            .await
            .expect("seed file id should be readable");
        let [Value::Text(file_id)] = file_before.rows()[0].values() else {
            panic!("expected seed file id");
        };
        let file_id = file_id.clone();
        session
            .execute(
                "INSERT INTO lix_file (path) VALUES ('/excluded-file-id.txt') \
                 ON CONFLICT (path) DO UPDATE SET name = excluded.id",
                &[],
            )
            .await
            .expect("excluded.id should materialize the file UUID default");
        let file_after = session
            .execute(
                "SELECT name FROM lix_file WHERE id = $1",
                &[Value::Text(file_id.clone())],
            )
            .await
            .expect("renamed file should remain readable by durable id");
        let [Value::Text(file_name)] = file_after.rows()[0].values() else {
            panic!("expected materialized file id as name");
        };
        assert!(!file_name.is_empty());
        assert_ne!(file_name, &file_id);

        session
            .execute(
                "INSERT INTO lix_directory (path) VALUES ('/excluded-directory-id')",
                &[],
            )
            .await
            .expect("directory id default seed should insert");
        let directory_before = session
            .execute(
                "SELECT id FROM lix_directory WHERE path = '/excluded-directory-id'",
                &[],
            )
            .await
            .expect("seed directory id should be readable");
        let [Value::Text(directory_id)] = directory_before.rows()[0].values() else {
            panic!("expected seed directory id");
        };
        let directory_id = directory_id.clone();
        session
            .execute(
                "INSERT INTO lix_directory (path) VALUES ('/excluded-directory-id') \
                 ON CONFLICT (path) DO UPDATE SET name = excluded.id",
                &[],
            )
            .await
            .expect("excluded.id should materialize the directory UUID default");
        let directory_after = session
            .execute(
                "SELECT name FROM lix_directory WHERE id = $1",
                &[Value::Text(directory_id.clone())],
            )
            .await
            .expect("renamed directory should remain readable by durable id");
        let [Value::Text(directory_name)] = directory_after.rows()[0].values() else {
            panic!("expected materialized directory id as name");
        };
        assert!(!directory_name.is_empty());
        assert_ne!(directory_name, &directory_id);

        session
            .execute(
                "INSERT INTO lix_branch (id, name, hidden) \
                 VALUES ('6578636c-7564-8564-8d64-656661756c00', 'before', true)",
                &[],
            )
            .await
            .expect("branch default seed should insert");
        let active_head = session
            .execute("SELECT lix_active_branch_commit_id()", &[])
            .await
            .expect("active head default should resolve");
        let [Value::Text(active_head)] = active_head.rows()[0].values() else {
            panic!("expected active branch head");
        };
        let active_head = active_head.clone();
        session
            .execute(
                "INSERT INTO lix_branch (id, name) \
                 VALUES ('6578636c-7564-8564-8d64-656661756c00', 'after') \
                 ON CONFLICT (id) DO UPDATE \
                 SET name = excluded.name, \
                     hidden = excluded.hidden, \
                     commit_id = excluded.commit_id",
                &[],
            )
            .await
            .expect("excluded branch columns should materialize advertised defaults");
        assert_rows_eq(
            session
                .execute(
                    "SELECT name, hidden, commit_id FROM lix_branch \
                     WHERE id = '6578636c-7564-8564-8d64-656661756c00'",
                    &[],
                )
                .await
                .expect("defaulted branch should be readable"),
            vec![vec![
                Value::Text("after".to_string()),
                Value::Boolean(false),
                Value::Text(active_head),
            ]],
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                   CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_default_identity_contract\",\"columns\":[{\"name\":\"id\",\"type\":\"uuid\",\"nullable\":false,\"default_expression\":\"uuidv7()\"},{\"name\":\"name\",\"type\":\"text\",\"nullable\":false}],\"primary_key\":[\"id\"]}' AS JSONB),\
                   false,\
                   false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");
        session
            .execute(
                "INSERT INTO engine_default_identity_contract (name) VALUES ('generated')",
                &[],
            )
            .await
            .expect("omitted typed-row primary key should generate");

        let generated = session
            .execute(
                "SELECT id FROM engine_default_identity_contract WHERE name = 'generated'",
                &[],
            )
            .await
            .expect("generated typed row should be readable");
        let [Value::Text(id)] = generated.rows()[0].values() else {
            panic!("expected generated text identity");
        };
        assert!(!id.is_empty(), "generated identity should not be empty");

        let query_generated_file = session
            .execute(
                "SELECT id, content FROM lix_file WHERE path = '/query-generated.txt'",
                &[],
            )
            .await
            .expect("query-backed file should be readable");
        let [Value::Text(file_id), Value::Blob(data)] = query_generated_file.rows()[0].values()
        else {
            panic!("expected generated file id and binary data");
        };
        assert!(!file_id.is_empty(), "query-backed file id should generate");
        assert!(data.is_empty(), "omitted file data should default to empty");

        let query_generated_directory = session
            .execute(
                "SELECT id FROM lix_directory WHERE path = '/query-generated'",
                &[],
            )
            .await
            .expect("query-backed directory should be readable");
        let [Value::Text(directory_id)] = query_generated_directory.rows()[0].values() else {
            panic!("expected generated directory id");
        };
        assert!(
            !directory_id.is_empty(),
            "query-backed directory id should generate"
        );

        for (sql, expected_code) in [
            (
                "INSERT INTO lix_file (id, path) VALUES (NULL, '/null-id.txt')",
                LixError::CODE_TYPE_MISMATCH,
            ),
            (
                "INSERT INTO lix_file (id, path) VALUES (NULL, '/upsert-null-id.txt') \
                 ON CONFLICT (path) DO NOTHING",
                LixError::CODE_TYPE_MISMATCH,
            ),
            (
                "INSERT INTO lix_file (id, path) VALUES (CAST(NULL AS TEXT), '/generated.txt') \
                 ON CONFLICT (path) DO NOTHING",
                LixError::CODE_TYPE_MISMATCH,
            ),
            (
                "INSERT INTO lix_file (path, content) \
                 VALUES ('/generated.txt', CAST(NULL AS BYTEA)) \
                 ON CONFLICT (path) DO NOTHING",
                LixError::CODE_TYPE_MISMATCH,
            ),
            (
                "INSERT INTO lix_directory (id, path) VALUES (NULL, '/null-id')",
                LixError::CODE_TYPE_MISMATCH,
            ),
            (
                "INSERT INTO engine_default_identity_contract (id, name) VALUES (NULL, 'explicit-null')",
                LixError::CODE_SCHEMA_VALIDATION,
            ),
            (
                "INSERT INTO lix_directory (id, path) \
                 SELECT NULL, '/query-null-id' \
                 FROM information_schema.tables \
                 WHERE table_name = 'lix_directory'",
                LixError::CODE_TYPE_MISMATCH,
            ),
            (
                "INSERT INTO lix_file (path, content) \
                 SELECT '/query-null-content.txt', NULL \
                 FROM information_schema.tables \
                 WHERE table_name = 'lix_file'",
                LixError::CODE_TYPE_MISMATCH,
            ),
        ] {
            let error = session
                .execute(sql, &[])
                .await
                .expect_err("explicit NULL must not trigger a column default");
            assert_eq!(error.code, expected_code);
        }

        for sql in [
            "INSERT INTO lix_branch (id, name, hidden) \
             VALUES ('6e756c6c-2d68-8964-8465-6e2d62726100', 'Null hidden', NULL)",
            "INSERT INTO lix_branch (id, name, commit_id) \
             VALUES ('6e756c6c-2d63-8f6d-8d69-742d62726100', 'Null commit', NULL)",
            "INSERT INTO lix_file (path, lixcol_global) \
             VALUES ('/null-global-file.txt', NULL)",
            "INSERT INTO lix_file (path, lixcol_untracked) \
             VALUES ('/null-untracked-file.txt', NULL)",
            "INSERT INTO lix_directory (path, lixcol_global) \
             VALUES ('/null-global-directory', NULL)",
            "INSERT INTO lix_directory (path, lixcol_untracked) \
             VALUES ('/null-untracked-directory', NULL)",
        ] {
            let error = session
                .execute(sql, &[])
                .await
                .expect_err("explicit NULL must not trigger a boolean or branch default");
            assert!(
                error.code == LixError::CODE_TYPE_MISMATCH
                    || error.code == LixError::CODE_UNSUPPORTED_SQL,
                "unexpected explicit-NULL error for {sql}: {error:?}"
            );
        }
    }
);

simulation_test!(
    typed_row_upsert_materializes_omitted_defaults_in_excluded,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                   CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_excluded_typed_default\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"status\",\"type\":\"text\",\"nullable\":false,\"default_value\":\"fresh\"},{\"name\":\"mirror\",\"type\":\"text\",\"nullable\":true},{\"name\":\"identity_copy\",\"type\":\"jsonb\",\"nullable\":true}],\"primary_key\":[\"id\"]}' AS JSONB),\
                   false,\
                   false\
                 )",
                &[],
            )
            .await
            .expect("registered schema insert should succeed");
        session
            .execute(
                "INSERT INTO engine_excluded_typed_default (id, status, mirror) \
                 VALUES ('same', 'old', 'old')",
                &[],
            )
            .await
            .expect("seed insert should succeed");
        session
            .execute(
                "INSERT INTO engine_excluded_typed_default (id) VALUES ('same') \
                 ON CONFLICT (id) DO UPDATE \
                 SET mirror = excluded.status, \
                     identity_copy = excluded.lixcol_row_pk",
                &[],
            )
            .await
            .expect("typed row upsert should succeed");

        assert_rows_eq(
            session
                .execute(
                    "SELECT mirror, identity_copy \
                     FROM engine_excluded_typed_default WHERE id = 'same'",
                    &[],
                )
                .await
                .expect("updated row should be readable"),
            vec![vec![
                Value::Text("fresh".to_string()),
                Value::Jsonb(serde_json::json!(["same"]).into()),
            ]],
        );

        let mismatched_identity = session
            .execute(
                "INSERT INTO engine_excluded_typed_default \
                 (id, status, lixcol_row_pk) \
                 VALUES ('different', 'corrupted', CAST('[\"same\"]' AS JSONB)) \
                 ON CONFLICT (id) DO UPDATE SET status = excluded.status",
                &[],
            )
            .await
            .expect_err("opaque and public typed identities must agree before conflict routing");
        assert_eq!(mismatched_identity.code, LixError::CODE_SCHEMA_VALIDATION);
        assert_rows_eq(
            session
                .execute(
                    "SELECT status FROM engine_excluded_typed_default WHERE id = 'same'",
                    &[],
                )
                .await
                .expect("mismatched upsert must leave the existing row unchanged"),
            vec![vec![Value::Text("old".to_string())]],
        );

        for column_name in ["lixcol_global", "lixcol_untracked"] {
            let error = session
                .execute(
                    &format!(
                        "INSERT INTO engine_excluded_typed_default \
                         (id, status, {column_name}) VALUES ('null-{column_name}', 'x', NULL)"
                    ),
                    &[],
                )
                .await
                .expect_err("explicit NULL must not trigger a typed system-column default");
            assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH);
        }

        let inserted_returning = session
            .execute(
                "INSERT INTO engine_excluded_typed_default (id, status) \
                 VALUES ('returning-insert', 'x') \
                 RETURNING id, status AS inserted_status",
                &[],
            )
            .await
            .expect("row INSERT RETURNING should expose its final snapshot");
        assert_eq!(inserted_returning.rows_affected(), 1);
        assert_eq!(inserted_returning.columns(), ["id", "inserted_status"]);
        assert_rows_eq(
            inserted_returning,
            vec![vec![
                Value::Text("returning-insert".to_string()),
                Value::Text("x".to_string()),
            ]],
        );

        let updated_returning = session
            .execute(
                "UPDATE engine_excluded_typed_default SET status = 'changed' \
                 WHERE id = 'same' RETURNING id, status AS updated_status",
                &[],
            )
            .await
            .expect("row UPDATE RETURNING should expose the post-update snapshot");
        assert_eq!(updated_returning.rows_affected(), 1);
        assert_eq!(updated_returning.columns(), ["id", "updated_status"]);
        assert_rows_eq(
            updated_returning,
            vec![vec![
                Value::Text("same".to_string()),
                Value::Text("changed".to_string()),
            ]],
        );
        assert_rows_eq(
            session
                .execute(
                    "SELECT id FROM engine_excluded_typed_default \
                     WHERE id = 'returning-insert'",
                    &[],
                )
                .await
                .expect("INSERT RETURNING must write its row"),
            vec![vec![Value::Text("returning-insert".to_string())]],
        );
        assert_rows_eq(
            session
                .execute(
                    "SELECT status FROM engine_excluded_typed_default WHERE id = 'same'",
                    &[],
                )
                .await
                .expect("UPDATE RETURNING must write its row"),
            vec![vec![Value::Text("changed".to_string())]],
        );
    }
);

simulation_test!(nullable_columns_are_optional_on_insert, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session()
            .await
            .expect("main session should open"),
        &engine,
    );

    session
            .execute(
                "INSERT INTO lix_registered_schema (value) \
                 VALUES (CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_required_nullable_contract\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"payload\",\"type\":\"jsonb\",\"nullable\":true}],\"primary_key\":[\"id\"]}' AS JSONB))",
                &[],
            )
            .await
            .expect("required nullable schema should register");

    assert_rows_eq(
        session
            .execute(
                "SELECT is_nullable, lix_insert_policy \
                     FROM information_schema.columns \
                     WHERE table_name = 'engine_required_nullable_contract' \
                       AND column_name = 'payload'",
                &[],
            )
            .await
            .expect("required nullable column should introspect"),
        vec![vec![
            Value::Text("YES".to_string()),
            Value::Text("OPTIONAL".to_string()),
        ]],
    );
    session
        .execute(
            "INSERT INTO engine_required_nullable_contract (id) VALUES ('omitted')",
            &[],
        )
        .await
        .expect("nullable column may be omitted");

    session
        .execute(
            "INSERT INTO engine_required_nullable_contract (id, payload) \
                 VALUES ('explicit-null', CAST('null' AS JSONB))",
            &[],
        )
        .await
        .expect("required nullable column should accept explicit JSON null");
    assert_rows_eq(
        session
            .execute(
                "SELECT payload FROM engine_required_nullable_contract \
                     WHERE id = 'explicit-null'",
                &[],
            )
            .await
            .expect("typed JSON null should read as SQL NULL"),
        vec![vec![Value::Null]],
    );
    assert_rows_eq(
        session
            .execute(
                "DELETE FROM engine_required_nullable_contract \
                     WHERE id = 'explicit-null' \
                     RETURNING payload, CAST('null' AS JSONB)",
                &[],
            )
            .await
            .expect("DELETE RETURNING should match SELECT null semantics"),
        vec![vec![
            Value::Null,
            Value::Jsonb(serde_json::Value::Null.into()),
        ]],
    );
});

simulation_test!(
    typed_bigint_projection_is_lossless_or_explicit,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value) \
                 VALUES (CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_bigint_contract\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"count\",\"type\":\"int8\",\"nullable\":false},{\"name\":\"ratio\",\"type\":\"float8\",\"nullable\":true}],\"primary_key\":[\"id\"]}' AS JSONB))",
                &[],
            )
            .await
            .expect("integer schema should register");
        session
            .execute(
                "INSERT INTO engine_bigint_contract (id, count) \
                 VALUES ('integral-real', 1.0)",
                &[],
            )
            .await
            .expect("typed BIGINT should accept an exact integral real spelling");

        assert_rows_eq(
            session
                .execute(
                    "SELECT count FROM engine_bigint_contract \
                     WHERE id = 'integral-real'",
                    &[],
                )
                .await
                .expect("integral JSON real should project through BIGINT"),
            vec![vec![Value::Integer(1)]],
        );
        assert_rows_eq(
            session
                .execute("SELECT id FROM engine_bigint_contract WHERE count = 1", &[])
                .await
                .expect("integral JSON real should participate in BIGINT filter pushdown"),
            vec![vec![Value::Text("integral-real".to_string())]],
        );
        assert_rows_eq(
            session
                .execute(
                    "SELECT id FROM engine_bigint_contract WHERE count = 1.0",
                    &[],
                )
                .await
                .expect("real literal comparison should retain DataFusion coercion semantics"),
            vec![vec![Value::Text("integral-real".to_string())]],
        );
        assert_rows_eq(
            session
                .execute(
                    "SELECT count FROM lix_history('engine_bigint_contract') \
                       WHERE lixcol_row_pk = CAST('[\"integral-real\"]' AS JSONB)",
                    &[],
                )
                .await
                .expect("integral JSON real should project through typed history BIGINT"),
            vec![vec![Value::Integer(1)]],
        );

        let updated = session
            .execute(
                "UPDATE engine_bigint_contract SET ratio = 1 \
                 WHERE count = 1.0",
                &[],
            )
            .await
            .expect("BIGINT predicates should normalize an integral real literal");
        assert_eq!(updated.rows_affected(), 1);
        let updated = session
            .execute(
                "UPDATE engine_bigint_contract SET ratio = 2.5 \
                 WHERE 1 = ratio",
                &[],
            )
            .await
            .expect("DOUBLE predicates should normalize an integer literal symmetrically");
        assert_eq!(updated.rows_affected(), 1);
        let updated = session
            .execute(
                "UPDATE engine_bigint_contract SET ratio = 3 \
                 WHERE count IN (1.0)",
                &[],
            )
            .await
            .expect("bound IN predicates should use the same numeric normalization");
        assert_eq!(updated.rows_affected(), 1);
        assert_rows_eq(
            session
                .execute(
                    "SELECT ratio FROM engine_bigint_contract \
                     WHERE id = 'integral-real'",
                    &[],
                )
                .await
                .expect("an integer JSON spelling should project through DOUBLE PRECISION"),
            vec![vec![Value::Real(3.0)]],
        );

        for sql in [
            "INSERT INTO engine_bigint_contract (id, count) \
             VALUES ('below-min-insert', -9223372036854775809)",
            "UPDATE engine_bigint_contract SET count = -9223372036854775809 \
             WHERE id = 'integral-real'",
            "UPDATE engine_bigint_contract SET ratio = 9 \
             WHERE count = -9223372036854775809",
            "UPDATE engine_bigint_contract SET ratio = 9 \
             WHERE count = -9223372036854775809.0",
            "UPDATE engine_bigint_contract SET ratio = 9 \
             WHERE count IN (-9223372036854775809e0)",
            "INSERT INTO engine_bigint_contract (id, count) \
             VALUES ('above-max-insert', 9223372036854775808)",
            "INSERT INTO engine_bigint_contract (id, count) \
             VALUES ('below-min-real-insert', -9223372036854775809.0)",
            "UPDATE engine_bigint_contract SET count = -9223372036854775809e0 \
             WHERE id = 'integral-real'",
            "INSERT INTO engine_bigint_contract (id, count) \
             VALUES ('rounded-fraction-insert', 9007199254740992.5)",
            "INSERT INTO engine_bigint_contract (id, count) \
             VALUES ('underflow-insert', 1e-400)",
            "INSERT INTO engine_bigint_contract (id, count) \
             VALUES ('non-integral-insert', 1.5)",
            "UPDATE engine_bigint_contract SET count = 9007199254740992.5 \
             WHERE id = 'integral-real'",
            "UPDATE engine_bigint_contract SET ratio = 9 \
             WHERE count = 9007199254740992.5",
            "UPDATE engine_bigint_contract SET ratio = 9 \
             WHERE count IN (1e-400)",
        ] {
            let error = session
                .execute(sql, &[])
                .await
                .expect_err("inexact SQL numeric literals must never round into BIGINT");
            assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH, "{sql}");
            assert!(error.message.contains("count"), "{error:?}");
            assert!(error.message.contains("BIGINT"), "{error:?}");
        }
        assert_rows_eq(
            session
                .execute(
                    "SELECT count, ratio FROM engine_bigint_contract \
                     WHERE id = 'integral-real'",
                    &[],
                )
                .await
                .expect("rejected numeric writes and predicates must not mutate the row"),
            vec![vec![Value::Integer(1), Value::Real(3.0)]],
        );

        session
            .execute(
                "INSERT INTO engine_bigint_contract (id, count) VALUES \
                 ('max-real-spelling', 9223372036854775807.0), \
                 ('min-exponent-spelling', -9223372036854775808e0)",
                &[],
            )
            .await
            .expect("exact in-range real and exponent spellings should normalize without rounding");
        assert_rows_eq(
            session
                .execute(
                    "SELECT count FROM engine_bigint_contract \
                     WHERE id IN ('max-real-spelling', 'min-exponent-spelling') \
                     ORDER BY id",
                    &[],
                )
                .await
                .expect("exact BIGINT boundary spellings should remain lossless"),
            vec![
                vec![Value::Integer(i64::MAX)],
                vec![Value::Integer(i64::MIN)],
            ],
        );

        session
            .execute(
                "INSERT INTO engine_bigint_contract (id, count, ratio) \
                 VALUES ('delete-integral-real', 2.0, 1)",
                &[],
            )
            .await
            .expect("typed integral-real delete fixture should insert");
        let deleted = session
            .execute(
                "DELETE FROM engine_bigint_contract \
                 WHERE count = 2 RETURNING count, ratio",
                &[],
            )
            .await
            .expect("DELETE predicates and RETURNING should apply the typed numeric contract");
        assert_eq!(deleted.rows_affected(), 1);
        assert_rows_eq(deleted, vec![vec![Value::Integer(2), Value::Real(1.0)]]);
    }
);

simulation_test!(
    typed_update_evaluates_arithmetic_assignments_and_predicates,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value) \
                 VALUES (CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\"key\":\"engine_arithmetic_update\",\"columns\":[{\"name\":\"id\",\"type\":\"text\",\"nullable\":false},{\"name\":\"quantity\",\"type\":\"int8\",\"nullable\":true},{\"name\":\"order_key\",\"type\":\"int8\",\"nullable\":false},{\"name\":\"line_number\",\"type\":\"int8\",\"nullable\":true}],\"primary_key\":[\"id\"]}' AS JSONB))",
                &[],
            )
            .await
            .expect("arithmetic update schema should register");
        session
            .execute(
                "INSERT INTO engine_arithmetic_update \
                 (id, quantity, order_key, line_number) VALUES \
                 ('a', 10, 1, 13), ('b', 20, 2, 6), ('c', 30, 3, 1), \
                 ('d', NULL, 4, 12)",
                &[],
            )
            .await
            .expect("arithmetic update rows should insert");

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("arithmetic update transaction should begin");
        let updated = transaction
            .execute(
                "UPDATE engine_arithmetic_update \
                 SET quantity = quantity + 1, line_number = quantity \
                 WHERE (order_key * 7 + line_number) % 20 = 0 \
                 RETURNING id, quantity, line_number",
                &[],
            )
            .await
            .expect("arithmetic assignment and modulo predicate should execute");
        assert_eq!(updated.rows_affected(), 3);
        assert_rows_eq(
            updated,
            vec![
                vec![
                    Value::Text("a".to_string()),
                    Value::Integer(11),
                    Value::Integer(10),
                ],
                vec![
                    Value::Text("b".to_string()),
                    Value::Integer(21),
                    Value::Integer(20),
                ],
                vec![Value::Text("d".to_string()), Value::Null, Value::Null],
            ],
        );
        let overlay_updated = transaction
            .execute(
                "UPDATE engine_arithmetic_update \
                 SET quantity = quantity + 1 WHERE id = 'a' \
                 RETURNING quantity, line_number",
                &[],
            )
            .await
            .expect("arithmetic assignment should read a prior staged post-image");
        assert_eq!(overlay_updated.rows_affected(), 1);
        assert_rows_eq(
            overlay_updated,
            vec![vec![Value::Integer(12), Value::Integer(10)]],
        );
        let returning_expression = transaction
            .execute(
                "UPDATE engine_arithmetic_update \
                 SET line_number = line_number WHERE id = 'c' \
                 RETURNING quantity + 1",
                &[],
            )
            .await
            .expect("binary RETURNING alone should select the generic UPDATE executor");
        assert_eq!(returning_expression.rows_affected(), 1);
        assert_rows_eq(returning_expression, vec![vec![Value::Integer(31)]]);
        assert_rows_eq(
            transaction
                .execute(
                    "SELECT id, quantity, line_number \
                     FROM engine_arithmetic_update ORDER BY id",
                    &[],
                )
                .await
                .expect("updated quantities should remain readable"),
            vec![
                vec![
                    Value::Text("a".to_string()),
                    Value::Integer(12),
                    Value::Integer(10),
                ],
                vec![
                    Value::Text("b".to_string()),
                    Value::Integer(21),
                    Value::Integer(20),
                ],
                vec![
                    Value::Text("c".to_string()),
                    Value::Integer(30),
                    Value::Integer(1),
                ],
                vec![Value::Text("d".to_string()), Value::Null, Value::Null],
            ],
        );
        transaction
            .rollback()
            .await
            .expect("arithmetic update transaction should roll back");

        let error = session
            .execute(
                "UPDATE engine_arithmetic_update \
                 SET quantity = quantity / (order_key - order_key) \
                 WHERE id IN ('a', 'b')",
                &[],
            )
            .await
            .expect_err("division by zero should fail the whole UPDATE");
        assert!(
            error.message.to_ascii_lowercase().contains("divide")
                || error.message.to_ascii_lowercase().contains("division"),
            "{error:?}"
        );
        assert_rows_eq(
            session
                .execute(
                    "SELECT id, quantity, line_number \
                     FROM engine_arithmetic_update ORDER BY id",
                    &[],
                )
                .await
                .expect("failed and rolled-back updates must leave base rows unchanged"),
            vec![
                vec![
                    Value::Text("a".to_string()),
                    Value::Integer(10),
                    Value::Integer(13),
                ],
                vec![
                    Value::Text("b".to_string()),
                    Value::Integer(20),
                    Value::Integer(6),
                ],
                vec![
                    Value::Text("c".to_string()),
                    Value::Integer(30),
                    Value::Integer(1),
                ],
                vec![
                    Value::Text("d".to_string()),
                    Value::Null,
                    Value::Integer(12),
                ],
            ],
        );
    }
);
