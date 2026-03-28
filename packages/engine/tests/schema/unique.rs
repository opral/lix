use crate::support;

use serde_json::json;

async fn register_unique_schema(
    engine: &support::simulation_test::SimulationEngine,
    schema_key: &str,
) {
    engine
        .register_schema(&json!({
            "x-lix-key": schema_key,
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "x-lix-unique": [["/slug"]],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "slug": { "type": "string" },
                "title": { "type": "string" }
            },
            "required": ["id", "slug", "title"],
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
    unique_conflicts_within_same_version_and_file,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_unique_schema(&engine, "unique_scope_same_file").await;
        insert_version(&engine, "version-a").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'post-1', 'unique_scope_same_file', 'alpha.md', 'version-a', 'lix', '1', '{\"id\":\"post-1\",\"slug\":\"hello-world\",\"title\":\"first\"}'\
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
                 'post-2', 'unique_scope_same_file', 'alpha.md', 'version-a', 'lix', '1', '{\"id\":\"post-2\",\"slug\":\"hello-world\",\"title\":\"second\"}'\
                 )",
                &[],
            )
            .await;

        assert!(
            duplicate.is_err(),
            "same unique value in same version and file should conflict"
        );
    }
);

simulation_test!(
    unique_allows_same_value_in_different_files,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_unique_schema(&engine, "unique_scope_per_file").await;
        insert_version(&engine, "version-a").await;

        engine
        .execute(
            "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
             ) VALUES (\
             'post-1', 'unique_scope_per_file', 'alpha.md', 'version-a', 'lix', '1', '{\"id\":\"post-1\",\"slug\":\"hello-world\",\"title\":\"alpha\"}'\
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
             'post-2', 'unique_scope_per_file', 'beta.md', 'version-a', 'lix', '1', '{\"id\":\"post-2\",\"slug\":\"hello-world\",\"title\":\"beta\"}'\
             )",
            &[],
        )
        .await
        .expect("same unique value should be allowed in a different file");
    }
);

simulation_test!(
    unique_allows_same_value_in_different_versions,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_engine(None)
            .await
            .expect("boot_simulated_engine should succeed");
        engine.initialize().await.unwrap();

        register_unique_schema(&engine, "unique_scope_per_version").await;
        insert_version(&engine, "version-a").await;
        insert_version(&engine, "version-b").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'post-1', 'unique_scope_per_version', 'alpha.md', 'version-a', 'lix', '1', '{\"id\":\"post-1\",\"slug\":\"hello-world\",\"title\":\"v1\"}'\
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
                 'post-2', 'unique_scope_per_version', 'alpha.md', 'version-b', 'lix', '1', '{\"id\":\"post-2\",\"slug\":\"hello-world\",\"title\":\"v2\"}'\
                 )",
                &[],
            )
            .await
            .expect("same unique value should be allowed in a different version");
    }
);
