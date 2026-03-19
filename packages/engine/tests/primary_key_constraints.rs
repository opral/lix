mod support;

use serde_json::json;

async fn register_pk_schema(engine: &support::simulation_test::SimulationEngine, schema_key: &str) {
    engine
        .register_schema(&json!({
            "x-lix-key": schema_key,
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "name": { "type": "string" }
            },
            "required": ["id", "name"],
            "additionalProperties": false
        }))
        .await
        .unwrap();
}

async fn register_composite_pk_schema(
    engine: &support::simulation_test::SimulationEngine,
    schema_key: &str,
) {
    engine
        .register_schema(&json!({
            "x-lix-key": schema_key,
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id", "/locale"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "locale": { "type": "string" },
                "name": { "type": "string" }
            },
            "required": ["id", "locale", "name"],
            "additionalProperties": false
        }))
        .await
        .unwrap();
}

async fn insert_version(engine: &support::simulation_test::SimulationEngine, version_id: &str) {
    let sql = format!(
        "INSERT INTO lix_version (\
         id, name, hidden, commit_id\
         ) VALUES (\
         '{version_id}', '{version_id}', false, 'commit-{version_id}'\
         )"
    );
    engine.execute(&sql, &[]).await.unwrap();
}

simulation_test!(
    primary_key_conflicts_within_same_version_and_file,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_pk_schema(&engine, "pk_scope_same_file").await;
        insert_version(&engine, "version-a").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'row-1', 'pk_scope_same_file', 'alpha.md', 'version-a', 'lix', '1', '{\"id\":\"row-1\",\"name\":\"first\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let duplicate = engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'row-1', 'pk_scope_same_file', 'alpha.md', 'version-a', 'lix', '1', '{\"id\":\"row-1\",\"name\":\"second\"}'\
                 )",
                &[],
            )
            .await;

        assert!(
            duplicate.is_err(),
            "same primary key in same version and file should conflict"
        );
    }
);

simulation_test!(
    primary_key_insert_rejects_entity_id_mismatch,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_pk_schema(&engine, "pk_entity_id_insert").await;
        insert_version(&engine, "version-a").await;

        let result = engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-1', 'pk_entity_id_insert', 'alpha.md', 'version-a', 'lix', '1', '{\"id\":\"entity-2\",\"name\":\"Ada\"}'\
                 )",
                &[],
            )
            .await;

        let err = result.expect_err("expected entity_id consistency error");
        assert!(
            err.to_string().contains(
                "entity_id 'entity-1' is inconsistent for schema 'pk_entity_id_insert' (1)"
            ),
            "unexpected error: {err}"
        );
    }
);

simulation_test!(
    primary_key_insert_rejects_empty_primary_key_value,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_pk_schema(&engine, "pk_empty_primary_key").await;
        insert_version(&engine, "version-a").await;

        let result = engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 '', 'pk_empty_primary_key', 'alpha.md', 'version-a', 'lix', '1', '{\"id\":\"\",\"name\":\"Ada\"}'\
                 )",
                &[],
            )
            .await;

        let err = result.expect_err("expected empty primary-key rejection");
        assert!(
            err.to_string().contains("non-empty canonical identity"),
            "unexpected error: {err}"
        );
    }
);

simulation_test!(
    primary_key_update_rejects_entity_id_mismatch,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_pk_schema(&engine, "pk_entity_id_update").await;
        insert_version(&engine, "version-a").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-1', 'pk_entity_id_update', 'alpha.md', 'version-a', 'lix', '1', '{\"id\":\"entity-1\",\"name\":\"Ada\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let result = engine
            .execute(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"id\":\"entity-2\",\"name\":\"Ada\"}' \
                 WHERE schema_key = 'pk_entity_id_update' \
                   AND entity_id = 'entity-1' \
                   AND file_id = 'alpha.md' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await;

        let err = result.expect_err("expected entity_id consistency error");
        assert!(
            err.to_string().contains(
                "entity_id 'entity-1' is inconsistent for schema 'pk_entity_id_update' (1)"
            ),
            "unexpected error: {err}"
        );
    }
);

simulation_test!(
    primary_key_allows_same_value_in_different_files,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_pk_schema(&engine, "pk_scope_per_file").await;
        insert_version(&engine, "version-a").await;

        engine
        .execute(
            "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
             ) VALUES (\
             'row-1', 'pk_scope_per_file', 'alpha.md', 'version-a', 'lix', '1', '{\"id\":\"row-1\",\"name\":\"alpha\"}'\
             )",
            &[],
        )
        .await
        .unwrap();

        engine
        .execute(
            "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
             ) VALUES (\
             'row-1', 'pk_scope_per_file', 'beta.md', 'version-a', 'lix', '1', '{\"id\":\"row-1\",\"name\":\"beta\"}'\
             )",
            &[],
        )
        .await
        .expect("same primary key should be allowed in a different file");
    }
);

simulation_test!(
    composite_primary_key_entity_id_roundtrips,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_composite_pk_schema(&engine, "pk_scope_composite").await;
        insert_version(&engine, "version-a").await;

        let insert_result = engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'entity-1~en', 'pk_scope_composite', 'alpha.md', 'version-a', 'lix', '1', '{\"id\":\"entity-1\",\"locale\":\"en\",\"name\":\"Ada\"}'\
                 )",
                &[],
            )
            .await;
        assert!(insert_result.is_ok(), "{insert_result:?}");

        let update_result = engine
            .execute(
                "UPDATE lix_state_by_version \
                 SET snapshot_content = '{\"id\":\"entity-1\",\"locale\":\"en\",\"name\":\"Ada Lovelace\"}' \
                 WHERE schema_key = 'pk_scope_composite' \
                   AND entity_id = 'entity-1~en' \
                   AND file_id = 'alpha.md' \
                   AND version_id = 'version-a'",
                &[],
            )
            .await;
        assert!(update_result.is_ok(), "{update_result:?}");
    }
);

simulation_test!(
    primary_key_allows_same_value_in_different_versions,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_pk_schema(&engine, "pk_scope_per_version").await;
        insert_version(&engine, "version-a").await;
        insert_version(&engine, "version-b").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'row-1', 'pk_scope_per_version', 'alpha.md', 'version-a', 'lix', '1', '{\"id\":\"row-1\",\"name\":\"v1\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'row-1', 'pk_scope_per_version', 'alpha.md', 'version-b', 'lix', '1', '{\"id\":\"row-1\",\"name\":\"v2\"}'\
                 )",
                &[],
            )
            .await
            .expect("same primary key should be allowed in a different version");
    }
);
