use lix_engine::{LixError, Value};

use super::assert_rows_eq;

simulation_test!(
    information_schema_exposes_executable_lix_column_contract,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                   lix_json('{\"x-lix-key\":\"engine_column_contract\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\",\"x-lix-default\":\"lix_uuid_v7()\"},\"title\":{\"type\":\"string\"},\"note\":{\"type\":\"string\"},\"count\":{\"type\":\"integer\"},\"ratio\":{\"type\":\"number\"},\"active\":{\"type\":\"boolean\"},\"metadata\":{\"type\":\"object\"}},\"required\":[\"id\",\"title\",\"count\",\"ratio\",\"active\",\"metadata\"],\"additionalProperties\":false}'),\
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
                   lix_json('{\"x-lix-key\":\"engine_no_pk_contract\",\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},\"required\":[\"name\"],\"additionalProperties\":false}'),\
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
                   lix_json('{\"x-lix-key\":\"columns\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"table_name\":{\"type\":\"string\"}},\"required\":[\"id\",\"table_name\"],\"additionalProperties\":false}'),\
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

        let read_only_entity_contract = session
            .execute(
                "SELECT table_name, column_name, lix_insert_policy \
                 FROM information_schema.columns \
                 WHERE table_name IN ('lix_commit', 'lix_file_descriptor') \
                   AND column_name = 'id' \
                 ORDER BY table_name",
                &[],
            )
            .await
            .expect("read-only generated entity surfaces should introspect");
        assert_rows_eq(
            read_only_entity_contract,
            vec![
                vec![
                    Value::Text("lix_commit".to_string()),
                    Value::Text("id".to_string()),
                    Value::Text("READ_ONLY".to_string()),
                ],
                vec![
                    Value::Text("lix_file_descriptor".to_string()),
                    Value::Text("id".to_string()),
                    Value::Text("READ_ONLY".to_string()),
                ],
            ],
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
                    Value::Text("lix_uuid_v7()".to_string()),
                    Value::Null,
                    Value::Text("DEFAULT".to_string()),
                ],
                vec![
                    Value::Text("metadata".to_string()),
                    Value::Text("TEXT".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Null,
                    Value::Text("JSON".to_string()),
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
                   AND column_name IN ('data', 'id') \
                 ORDER BY column_name",
                &[],
            )
            .await
            .expect("file contract query should succeed");
        assert_rows_eq(
            file_contract,
            vec![
                vec![
                    Value::Text("data".to_string()),
                    Value::Text("BYTEA".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Text("X''".to_string()),
                    Value::Text("DEFAULT".to_string()),
                ],
                vec![
                    Value::Text("id".to_string()),
                    Value::Text("TEXT".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Text("lix_uuid_v7()".to_string()),
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
        assert_rows_eq(
            by_branch_contract,
            vec![
                vec![
                    Value::Text("engine_column_contract_by_branch".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Text("REQUIRED".to_string()),
                ],
                vec![
                    Value::Text("lix_directory_by_branch".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Text("REQUIRED".to_string()),
                ],
                vec![
                    Value::Text("lix_file_by_branch".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Text("REQUIRED".to_string()),
                ],
            ],
        );

        let identity_contract = session
            .execute(
                "SELECT table_name, column_name, is_nullable, column_default, lix_insert_policy \
                 FROM information_schema.columns \
                 WHERE (\
                   table_name = 'engine_column_contract' \
                   AND column_name IN (\
                     'lixcol_change_id', 'lixcol_commit_id', 'lixcol_created_at', \
                     'lixcol_entity_pk', 'lixcol_global', 'lixcol_schema_key', \
                     'lixcol_untracked', 'lixcol_updated_at'\
                   )\
                 ) OR (\
                   table_name = 'engine_no_pk_contract' \
                   AND column_name = 'lixcol_entity_pk'\
                 ) \
                 ORDER BY table_name, column_name",
                &[],
            )
            .await
            .expect("entity system-column contract query should succeed");
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
                    Value::Text("lixcol_entity_pk".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Null,
                    Value::Text("CONDITIONAL".to_string()),
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
                    Value::Text("lixcol_entity_pk".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Null,
                    Value::Text("REQUIRED".to_string()),
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

        let raw_state_contract = session
            .execute(
                "SELECT column_name, is_nullable, column_default, lix_insert_policy \
                 FROM information_schema.columns \
                 WHERE table_name = 'lix_state' \
                 ORDER BY column_name",
                &[],
            )
            .await
            .expect("raw state contract query should succeed");
        let expected_raw_state_contract = [
            ("change_id", "YES", None, "READ_ONLY"),
            ("commit_id", "YES", None, "READ_ONLY"),
            ("created_at", "NO", None, "READ_ONLY"),
            ("entity_pk", "NO", None, "REQUIRED"),
            ("file_id", "YES", None, "OPTIONAL"),
            ("global", "NO", Some("FALSE"), "DEFAULT"),
            ("metadata", "YES", None, "OPTIONAL"),
            ("schema_key", "NO", None, "REQUIRED"),
            ("snapshot_content", "YES", None, "OPTIONAL"),
            ("untracked", "NO", Some("FALSE"), "DEFAULT"),
            ("updated_at", "NO", None, "READ_ONLY"),
        ]
        .into_iter()
        .map(
            |(column_name, is_nullable, column_default, insert_policy)| {
                vec![
                    Value::Text(column_name.to_string()),
                    Value::Text(is_nullable.to_string()),
                    column_default.map_or(Value::Null, |value| Value::Text(value.to_string())),
                    Value::Text(insert_policy.to_string()),
                ]
            },
        )
        .collect();
        assert_rows_eq(raw_state_contract, expected_raw_state_contract);

        let raw_state_branch_contract = session
            .execute(
                "SELECT column_name, is_nullable, column_default, lix_insert_policy \
                 FROM information_schema.columns \
                 WHERE table_name = 'lix_state_by_branch' \
                   AND column_name IN ('branch_id', 'global') \
                 ORDER BY column_name",
                &[],
            )
            .await
            .expect("raw state branch contract query should succeed");
        assert_rows_eq(
            raw_state_branch_contract,
            vec![
                vec![
                    Value::Text("branch_id".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Null,
                    Value::Text("CONDITIONAL".to_string()),
                ],
                vec![
                    Value::Text("global".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Null,
                    Value::Text("CONDITIONAL".to_string()),
                ],
            ],
        );

        let history_contract = session
            .execute(
                "SELECT column_name, is_nullable, lix_insert_policy \
                 FROM information_schema.columns \
                 WHERE table_name = 'engine_column_contract_history' \
                   AND column_name IN ('id', 'title') \
                 ORDER BY column_name",
                &[],
            )
            .await
            .expect("entity history nullability contract query should succeed");
        assert_rows_eq(
            history_contract,
            vec![
                vec![
                    Value::Text("id".to_string()),
                    Value::Text("NO".to_string()),
                    Value::Text("READ_ONLY".to_string()),
                ],
                vec![
                    Value::Text("title".to_string()),
                    Value::Text("YES".to_string()),
                    Value::Text("READ_ONLY".to_string()),
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
                ("json_value", Some("JSON")) => (
                    Value::Text("{\"phase\":\"insert\"}".to_string()),
                    Value::Text("{\"phase\":\"insert\"}".to_string()),
                    Value::Json(serde_json::json!({"phase": "insert"})),
                    Value::Text("{\"phase\":\"update\"}".to_string()),
                    Value::Json(serde_json::json!({"phase": "update"})),
                ),
                ("data", None) => (
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
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                   lix_json('{\"x-lix-key\":\"engine_scalar_cast_contract\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\",\"x-lix-default\":\"lix_uuid_v7()\"},\"text_value\":{\"type\":\"string\"},\"integer_value\":{\"type\":\"integer\"},\"number_value\":{\"type\":\"number\"},\"boolean_value\":{\"type\":\"boolean\"},\"json_value\":{\"type\":\"object\"}},\"required\":[\"id\",\"text_value\",\"integer_value\",\"number_value\",\"boolean_value\",\"json_value\"],\"additionalProperties\":false}'),\
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
                 ) OR (table_name = 'lix_file' AND column_name = 'data') \
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
        assert_eq!(contracts.len(), 6, "expected five entity types plus BYTEA");

        for contract in &contracts {
            let expected_type = match contract.column_name.as_str() {
                "text_value" | "json_value" => "TEXT",
                "integer_value" => "BIGINT",
                "number_value" => "DOUBLE PRECISION",
                "boolean_value" => "BOOLEAN",
                "data" => "BYTEA",
                other => panic!("unexpected contract column {other}"),
            };
            assert_eq!(contract.data_type, expected_type);
            assert_eq!(
                contract.value_kind.as_deref(),
                (contract.column_name == "json_value").then_some("JSON")
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

        let entity_contracts = contracts
            .iter()
            .filter(|contract| contract.table_name == "engine_scalar_cast_contract")
            .collect::<Vec<_>>();
        let entity_columns = entity_contracts
            .iter()
            .map(|contract| contract.column_name.clone())
            .collect::<Vec<_>>();
        let insert_params = entity_contracts
            .iter()
            .map(|contract| values_for_contract(contract).0)
            .collect::<Vec<_>>();
        let insert_casts = entity_contracts
            .iter()
            .enumerate()
            .map(|(index, contract)| format!("CAST(${} AS {})", index + 1, contract.data_type))
            .collect::<Vec<_>>();
        session
            .execute(
                &format!(
                    "INSERT INTO engine_scalar_cast_contract ({}) VALUES ({})",
                    entity_columns.join(", "),
                    insert_casts.join(", ")
                ),
                &insert_params,
            )
            .await
            .expect("all advertised entity casts should work in a bound INSERT");

        let inserted = session
            .execute(
                &format!(
                    "SELECT {} FROM engine_scalar_cast_contract",
                    entity_columns.join(", ")
                ),
                &[],
            )
            .await
            .expect("inserted scalar cast row should be readable");
        assert_rows_eq(
            inserted,
            vec![
                entity_contracts
                    .iter()
                    .map(|contract| values_for_contract(contract).2)
                    .collect(),
            ],
        );

        let update_params = entity_contracts
            .iter()
            .map(|contract| values_for_contract(contract).3)
            .collect::<Vec<_>>();
        let update_casts = entity_contracts
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
            .expect("all advertised entity casts should work in a bound UPDATE");
        let updated = session
            .execute(
                &format!(
                    "SELECT {} FROM engine_scalar_cast_contract",
                    entity_columns.join(", ")
                ),
                &[],
            )
            .await
            .expect("updated scalar cast row should be readable");
        assert_rows_eq(
            updated,
            vec![
                entity_contracts
                    .iter()
                    .map(|contract| values_for_contract(contract).4)
                    .collect(),
            ],
        );

        let bytea_contract = contracts
            .iter()
            .find(|contract| contract.table_name == "lix_file")
            .expect("lix_file.data BYTEA contract should exist");
        let (file_insert_param, _, _, file_update_param, file_update_expected) =
            values_for_contract(bytea_contract);
        session
            .execute(
                &format!(
                    "INSERT INTO lix_file (path, data) \
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
                    "UPDATE lix_file SET data = CAST($1 AS {}) \
                     WHERE path = '/contract.bin'",
                    bytea_contract.data_type
                ),
                &[file_update_param],
            )
            .await
            .expect("advertised BYTEA should work in a bound UPDATE");
        let file = session
            .execute(
                "SELECT data FROM lix_file WHERE path = '/contract.bin'",
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
            ("BINARY", "data", "'legacy'", "lix_file"),
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
                "INSERT INTO lix_file (path, data) \
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
                .open_workspace_session()
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
                "INSERT INTO lix_directory (path) VALUES ('/generated/')",
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
                 SELECT '/query-generated/' \
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
                "UPDATE lix_file SET data = X'6F6C64' WHERE path = '/generated.txt'",
                &[],
            )
            .await
            .expect("seed file contents should update");
        session
            .execute(
                "INSERT INTO lix_file (path) VALUES ('/generated.txt') \
                 ON CONFLICT (path) DO UPDATE SET data = excluded.data",
                &[],
            )
            .await
            .expect("excluded.data should materialize its advertised empty default");
        let defaulted_upsert = session
            .execute(
                "SELECT data FROM lix_file WHERE path = '/generated.txt'",
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
                "INSERT INTO lix_directory (path) VALUES ('/excluded-directory-id/')",
                &[],
            )
            .await
            .expect("directory id default seed should insert");
        let directory_before = session
            .execute(
                "SELECT id FROM lix_directory WHERE path = '/excluded-directory-id/'",
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
                "INSERT INTO lix_directory (path) VALUES ('/excluded-directory-id/') \
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
                 VALUES ('excluded-default-branch', 'before', true)",
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
                 VALUES ('excluded-default-branch', 'after') \
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
                     WHERE id = 'excluded-default-branch'",
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
                "INSERT INTO lix_state (\
                   entity_pk, schema_key, snapshot_content, global, untracked\
                 ) \
                 VALUES (\
                   lix_json('[\"excluded-default-state\"]'), \
                   'lix_key_value', \
                   lix_json('{\"key\":\"excluded-default-state\",\"value\":\"before\"}'), \
                   true, \
                   true\
                 )",
                &[],
            )
            .await
            .expect("generic state default seed should insert");
        session
            .execute(
                "INSERT INTO lix_state (entity_pk, schema_key, snapshot_content) \
                 VALUES (\
                   lix_json('[\"excluded-default-state\"]'), \
                   'lix_key_value', \
                   lix_json('{\"key\":\"excluded-default-state\",\"value\":\"after\"}')\
                 ) \
                 ON CONFLICT (entity_pk, schema_key, file_id) DO UPDATE \
                 SET global = excluded.global, untracked = excluded.untracked",
                &[],
            )
            .await
            .expect("excluded state booleans should materialize advertised defaults");
        assert_rows_eq(
            session
                .execute(
                    "SELECT global, untracked FROM lix_state \
                     WHERE schema_key = 'lix_key_value' \
                       AND entity_pk = lix_json('[\"excluded-default-state\"]')",
                    &[],
                )
                .await
                .expect("defaulted state booleans should be readable"),
            vec![vec![Value::Boolean(false), Value::Boolean(false)]],
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                   lix_json('{\"x-lix-key\":\"engine_default_identity_contract\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\",\"x-lix-default\":\"lix_uuid_v7()\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
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
            .expect("omitted typed-entity primary key should generate");

        let generated = session
            .execute(
                "SELECT id FROM engine_default_identity_contract WHERE name = 'generated'",
                &[],
            )
            .await
            .expect("generated typed entity should be readable");
        let [Value::Text(id)] = generated.rows()[0].values() else {
            panic!("expected generated text identity");
        };
        assert!(!id.is_empty(), "generated identity should not be empty");

        let query_generated_file = session
            .execute(
                "SELECT id, data FROM lix_file WHERE path = '/query-generated.txt'",
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
                "SELECT id FROM lix_directory WHERE path = '/query-generated/'",
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
                "INSERT INTO lix_file (path, data) \
                 VALUES ('/generated.txt', CAST(NULL AS BYTEA)) \
                 ON CONFLICT (path) DO NOTHING",
                LixError::CODE_TYPE_MISMATCH,
            ),
            (
                "INSERT INTO lix_directory (id, path) VALUES (NULL, '/null-id/')",
                LixError::CODE_TYPE_MISMATCH,
            ),
            (
                "INSERT INTO engine_default_identity_contract (id, name) VALUES (NULL, 'explicit-null')",
                LixError::CODE_SCHEMA_VALIDATION,
            ),
            (
                "INSERT INTO lix_directory (id, path) \
                 SELECT NULL, '/query-null-id/' \
                 FROM information_schema.tables \
                 WHERE table_name = 'lix_directory'",
                LixError::CODE_TYPE_MISMATCH,
            ),
            (
                "INSERT INTO lix_file (path, data) \
                 SELECT '/query-null-data.txt', NULL \
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
             VALUES ('null-hidden-branch', 'Null hidden', NULL)",
            "INSERT INTO lix_branch (id, name, commit_id) \
             VALUES ('null-commit-branch', 'Null commit', NULL)",
            "INSERT INTO lix_file (path, lixcol_global) \
             VALUES ('/null-global-file.txt', NULL)",
            "INSERT INTO lix_file (path, lixcol_untracked) \
             VALUES ('/null-untracked-file.txt', NULL)",
            "INSERT INTO lix_directory (path, lixcol_global) \
             VALUES ('/null-global-directory/', NULL)",
            "INSERT INTO lix_directory (path, lixcol_untracked) \
             VALUES ('/null-untracked-directory/', NULL)",
            "INSERT INTO lix_state (entity_pk, schema_key, snapshot_content, global) \
             VALUES (\
               lix_json('[\"null-global-state\"]'), \
               'lix_key_value', \
               lix_json('{\"key\":\"null-global-state\",\"value\":null}'), \
               NULL\
             )",
            "INSERT INTO lix_state (entity_pk, schema_key, snapshot_content, untracked) \
             VALUES (\
               lix_json('[\"null-untracked-state\"]'), \
               'lix_key_value', \
               lix_json('{\"key\":\"null-untracked-state\",\"value\":null}'), \
               NULL\
             )",
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
    typed_entity_upsert_materializes_omitted_defaults_in_excluded,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                 VALUES (\
                   lix_json('{\"x-lix-key\":\"engine_excluded_typed_default\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\",\"x-lix-default\":\"lix_uuid_v7()\"},\"status\":{\"type\":\"string\",\"default\":\"fresh\"},\"mirror\":{\"type\":\"string\"},\"identity_copy\":{\"type\":\"array\"}},\"required\":[\"id\",\"status\"],\"additionalProperties\":false}'),\
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
                     identity_copy = excluded.lixcol_entity_pk",
                &[],
            )
            .await
            .expect("typed entity upsert should succeed");

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
                Value::Json(serde_json::json!(["same"])),
            ]],
        );

        session
            .execute(
                "INSERT INTO engine_excluded_typed_default (status) VALUES ('generated') \
                 ON CONFLICT (id) DO NOTHING",
                &[],
            )
            .await
            .expect("typed upsert should materialize a defaulted primary key");
        let generated = session
            .execute(
                "SELECT id FROM engine_excluded_typed_default WHERE status = 'generated'",
                &[],
            )
            .await
            .expect("generated upsert row should be readable");
        let [Value::Text(id)] = generated.rows()[0].values() else {
            panic!("expected generated typed-upsert identity");
        };
        assert!(!id.is_empty());

        let mismatched_identity = session
            .execute(
                "INSERT INTO engine_excluded_typed_default \
                 (id, status, lixcol_entity_pk) \
                 VALUES ('different', 'corrupted', lix_json('[\"same\"]')) \
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

        for sql in [
            "INSERT INTO engine_excluded_typed_default (id, status) \
             VALUES ('unsupported-returning-insert', 'x') RETURNING id",
            "UPDATE engine_excluded_typed_default SET status = 'changed' \
             WHERE id = 'same' RETURNING id",
        ] {
            let error = session
                .execute(sql, &[])
                .await
                .expect_err("unsupported entity RETURNING must not be silently ignored");
            assert_eq!(error.code, LixError::CODE_UNSUPPORTED_SQL, "{sql}");
            assert!(error.message.contains("RETURNING"), "{error:?}");
        }
        assert_rows_eq(
            session
                .execute(
                    "SELECT id FROM engine_excluded_typed_default \
                     WHERE id = 'unsupported-returning-insert'",
                    &[],
                )
                .await
                .expect("rejected INSERT RETURNING must not write"),
            vec![],
        );
        assert_rows_eq(
            session
                .execute(
                    "SELECT status FROM engine_excluded_typed_default WHERE id = 'same'",
                    &[],
                )
                .await
                .expect("rejected UPDATE RETURNING must not write"),
            vec![vec![Value::Text("old".to_string())]],
        );
    }
);

simulation_test!(
    lix_state_by_branch_scope_is_materialized_before_conflict_routing,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        for sql in [
            "INSERT INTO lix_state_by_branch \
             (entity_pk, schema_key, snapshot_content, branch_id) VALUES \
             (lix_json('[\"null-branch-insert\"]'), 'lix_key_value', lix_json('{\"key\":\"null-branch-insert\",\"value\":\"invalid\"}'), $1)",
            "INSERT INTO lix_state_by_branch \
             (entity_pk, schema_key, snapshot_content, branch_id) VALUES \
             (lix_json('[\"null-branch-upsert\"]'), 'lix_key_value', lix_json('{\"key\":\"null-branch-upsert\",\"value\":\"invalid\"}'), $1) \
             ON CONFLICT (entity_pk, schema_key, file_id, branch_id) DO NOTHING",
        ] {
            let error = session
                .execute(sql, &[Value::Null])
                .await
                .expect_err("NULL INSERT branch parameters must not become empty-scope no-ops");
            assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH, "{sql}");
            assert!(error.message.contains("branch-id"), "{error:?}");
        }

        session
            .execute(
                "INSERT INTO lix_state_by_branch \
                 (entity_pk, schema_key, file_id, snapshot_content, branch_id) VALUES \
                 (lix_json('[\"nothing-branch\"]'), 'lix_key_value', NULL, lix_json('{\"key\":\"nothing-branch\",\"value\":\"old\"}'), 'global'), \
                 (lix_json('[\"update-branch\"]'), 'lix_key_value', NULL, lix_json('{\"key\":\"update-branch\",\"value\":\"old\"}'), 'global')",
                &[],
            )
            .await
            .expect("branch-id spelling should seed global rows");
        session
            .execute(
                "INSERT INTO lix_state_by_branch \
                 (entity_pk, schema_key, file_id, snapshot_content, global) VALUES \
                 (lix_json('[\"nothing-global\"]'), 'lix_key_value', NULL, lix_json('{\"key\":\"nothing-global\",\"value\":\"old\"}'), true), \
                 (lix_json('[\"update-global\"]'), 'lix_key_value', NULL, lix_json('{\"key\":\"update-global\",\"value\":\"old\"}'), true)",
                &[],
            )
            .await
            .expect("global spelling should seed rows with a derived global branch id");

        let do_nothing = session
            .execute(
                "INSERT INTO lix_state_by_branch \
                 (entity_pk, schema_key, file_id, snapshot_content, global) VALUES \
                 (lix_json('[\"nothing-branch\"]'), 'lix_key_value', NULL, lix_json('{\"key\":\"nothing-branch\",\"value\":\"ignored\"}'), true) \
                 ON CONFLICT (entity_pk, schema_key, file_id, branch_id) DO NOTHING",
                &[],
            )
            .await
            .expect("global spelling should match branch-spelled conflict identity");
        assert_eq!(do_nothing.rows_affected(), 0);
        let do_nothing = session
            .execute(
                "INSERT INTO lix_state_by_branch \
                 (entity_pk, schema_key, file_id, snapshot_content, branch_id) VALUES \
                 (lix_json('[\"nothing-global\"]'), 'lix_key_value', NULL, lix_json('{\"key\":\"nothing-global\",\"value\":\"ignored\"}'), 'global') \
                 ON CONFLICT (entity_pk, schema_key, file_id, branch_id) DO NOTHING",
                &[],
            )
            .await
            .expect("branch spelling should match global-spelled conflict identity");
        assert_eq!(do_nothing.rows_affected(), 0);

        let do_update = session
            .execute(
                "INSERT INTO lix_state_by_branch \
                 (entity_pk, schema_key, file_id, snapshot_content, global) VALUES \
                 (lix_json('[\"update-branch\"]'), 'lix_key_value', NULL, lix_json('{\"key\":\"update-branch\",\"value\":\"new\"}'), true) \
                 ON CONFLICT (entity_pk, schema_key, file_id, branch_id) \
                 DO UPDATE SET snapshot_content = excluded.snapshot_content",
                &[],
            )
            .await
            .expect("global spelling should update branch-spelled conflict identity");
        assert_eq!(do_update.rows_affected(), 1);
        let do_update = session
            .execute(
                "INSERT INTO lix_state_by_branch \
                 (entity_pk, schema_key, file_id, snapshot_content, branch_id) VALUES \
                 (lix_json('[\"update-global\"]'), 'lix_key_value', NULL, lix_json('{\"key\":\"update-global\",\"value\":\"new\"}'), 'global') \
                 ON CONFLICT (entity_pk, schema_key, file_id, branch_id) \
                 DO UPDATE SET snapshot_content = excluded.snapshot_content",
                &[],
            )
            .await
            .expect("branch spelling should update global-spelled conflict identity");
        assert_eq!(do_update.rows_affected(), 1);

        for (id, expected_value) in [
            ("nothing-branch", "old"),
            ("nothing-global", "old"),
            ("update-branch", "new"),
            ("update-global", "new"),
        ] {
            assert_rows_eq(
                session
                    .execute(
                        &format!(
                            "SELECT snapshot_content, branch_id, global \
                             FROM lix_state_by_branch \
                             WHERE entity_pk = lix_json('[\"{id}\"]') \
                               AND schema_key = 'lix_key_value' \
                               AND branch_id = 'global'"
                        ),
                        &[],
                    )
                    .await
                    .expect("global conflict result should be readable"),
                vec![vec![
                    Value::Json(serde_json::json!({
                        "key": id,
                        "value": expected_value,
                    })),
                    Value::Text("global".to_string()),
                    Value::Boolean(true),
                ]],
            );
        }
    }
);

simulation_test!(
    required_nullable_columns_separate_read_and_insert_contracts,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value) \
                 VALUES (lix_json('{\"x-lix-key\":\"engine_required_nullable_contract\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"payload\":{\"type\":[\"object\",\"null\"]}},\"required\":[\"id\",\"payload\"],\"additionalProperties\":false}'))",
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
                Value::Text("REQUIRED".to_string()),
            ]],
        );
        assert_rows_eq(
            session
                .execute(
                    "SELECT is_nullable, lix_insert_policy \
                     FROM information_schema.columns \
                     WHERE table_name = 'lix_label_assignment' \
                       AND column_name = 'target_file_id'",
                    &[],
                )
                .await
                .expect("built-in required nullable column should introspect"),
            vec![vec![
                Value::Text("YES".to_string()),
                Value::Text("REQUIRED".to_string()),
            ]],
        );

        let omission_error = session
            .execute(
                "INSERT INTO engine_required_nullable_contract (id) VALUES ('omitted')",
                &[],
            )
            .await
            .expect_err("required nullable column must not be omittable");
        assert!(
            omission_error.message.contains("payload"),
            "{omission_error:?}"
        );

        session
            .execute(
                "INSERT INTO engine_required_nullable_contract (id, payload) \
                 VALUES ('explicit-null', lix_json('null'))",
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
                     RETURNING payload, lix_json('null')",
                    &[],
                )
                .await
                .expect("DELETE RETURNING should match SELECT null semantics"),
            vec![vec![Value::Null, Value::Json(serde_json::Value::Null)]],
        );
    }
);

simulation_test!(
    typed_bigint_projection_is_lossless_or_explicit,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_workspace_session()
                .await
                .expect("main session should open"),
            &engine,
        );

        session
            .execute(
                "INSERT INTO lix_registered_schema (value) \
                 VALUES (lix_json('{\"x-lix-key\":\"engine_bigint_contract\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"count\":{\"type\":\"integer\"},\"ratio\":{\"type\":\"number\"}},\"required\":[\"id\",\"count\"],\"additionalProperties\":false}'))",
                &[],
            )
            .await
            .expect("integer schema should register");
        session
            .execute(
                "INSERT INTO lix_state (entity_pk, schema_key, snapshot_content) \
                 VALUES (\
                   lix_json('[\"integral-real\"]'),\
                   'engine_bigint_contract',\
                   lix_json('{\"id\":\"integral-real\",\"count\":1.0}')\
                 )",
                &[],
            )
            .await
            .expect("raw state should accept a mathematically integral JSON real");

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
                    "SELECT count FROM engine_bigint_contract_history \
                     WHERE lixcol_as_of_commit_id = lix_active_branch_commit_id() \
                       AND lixcol_entity_pk = lix_json('[\"integral-real\"]')",
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
                "INSERT INTO lix_state (entity_pk, schema_key, snapshot_content) \
                 VALUES (\
                   lix_json('[\"delete-integral-real\"]'),\
                   'engine_bigint_contract',\
                   lix_json('{\"id\":\"delete-integral-real\",\"count\":2.0,\"ratio\":1}')\
                 )",
                &[],
            )
            .await
            .expect("raw integral-real delete fixture should insert");
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

        session
            .execute(
                "INSERT INTO lix_state (entity_pk, schema_key, snapshot_content) \
                 VALUES (\
                   lix_json('[\"out-of-range\"]'),\
                   'engine_bigint_contract',\
                   lix_json('{\"id\":\"out-of-range\",\"count\":9223372036854775808}')\
                 )",
                &[],
            )
            .await
            .expect("raw interoperability write should preserve a valid JSON-Schema integer");

        for sql in [
            "SELECT count FROM engine_bigint_contract WHERE id = 'out-of-range'",
            "SELECT id FROM engine_bigint_contract WHERE count = 1",
            "SELECT id FROM engine_bigint_contract WHERE count = 1.0",
            "SELECT count FROM engine_bigint_contract_history \
             WHERE lixcol_as_of_commit_id = lix_active_branch_commit_id() \
               AND lixcol_entity_pk = lix_json('[\"out-of-range\"]')",
        ] {
            let error = session
                .execute(sql, &[])
                .await
                .expect_err("typed BIGINT reads must reject values outside the i64 range");
            assert_eq!(error.code, LixError::CODE_TYPE_MISMATCH, "{sql}");
            assert!(
                error.message.contains("engine_bigint_contract"),
                "{error:?}"
            );
            assert!(error.message.contains("count"), "{error:?}");
            assert!(error.message.contains("BIGINT"), "{error:?}");
        }

        let update_error = session
            .execute(
                "UPDATE engine_bigint_contract SET ratio = 4 \
                 WHERE count = 1",
                &[],
            )
            .await
            .expect_err("bound numeric predicates must reject an out-of-BIGINT stored value");
        assert_eq!(update_error.code, LixError::CODE_TYPE_MISMATCH);
        assert!(update_error.message.contains("count"), "{update_error:?}");
        assert!(update_error.message.contains("BIGINT"), "{update_error:?}");
        assert_rows_eq(
            session
                .execute(
                    "SELECT ratio FROM engine_bigint_contract \
                     WHERE id = 'integral-real'",
                    &[],
                )
                .await
                .expect("failed bound UPDATE must not stage earlier candidate mutations"),
            vec![vec![Value::Real(3.0)]],
        );

        let delete_error = session
            .execute(
                "DELETE FROM engine_bigint_contract \
                 WHERE id = 'out-of-range' RETURNING count",
                &[],
            )
            .await
            .expect_err("DELETE RETURNING must reject an out-of-BIGINT stored value");
        assert_eq!(delete_error.code, LixError::CODE_TYPE_MISMATCH);
        assert!(delete_error.message.contains("count"), "{delete_error:?}");
        assert!(delete_error.message.contains("BIGINT"), "{delete_error:?}");
        assert_rows_eq(
            session
                .execute(
                    "SELECT entity_pk FROM lix_state \
                     WHERE schema_key = 'engine_bigint_contract' \
                       AND entity_pk = lix_json('[\"out-of-range\"]')",
                    &[],
                )
                .await
                .expect("failed DELETE RETURNING must leave raw state untouched"),
            vec![vec![Value::Json(serde_json::json!(["out-of-range"]))]],
        );

        let non_integral = session
            .execute(
                "INSERT INTO lix_state (entity_pk, schema_key, snapshot_content) \
                 VALUES (\
                   lix_json('[\"non-integral\"]'),\
                   'engine_bigint_contract',\
                   lix_json('{\"id\":\"non-integral\",\"count\":1.5}')\
                 )",
                &[],
            )
            .await
            .expect_err("schema validation should reject a non-integral raw value");
        assert_eq!(non_integral.code, LixError::CODE_SCHEMA_VALIDATION);
    }
);
