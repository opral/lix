use crate::support;

use lix_engine::Value;
use serde_json::json;

async fn register_unique_schema(engine: &support::simulation_test::SimulatedLix, schema_key: &str) {
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

async fn seed_file_descriptor(
    engine: &support::simulation_test::SimulatedLix,
    version_id: &str,
    file_id: &str,
) {
    let (name, extension) = file_id
        .rsplit_once('.')
        .map(|(name, extension)| (name, Some(extension)))
        .unwrap_or((file_id, None));
    let snapshot = json!({
        "id": file_id,
        "directory_id": null,
        "name": name,
        "extension": extension,
        "hidden": false
    })
    .to_string();

    engine
        .execute(
            "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
             ) VALUES (\
             $1, 'lix_file_descriptor', NULL, $2, NULL, '1', $3\
             )",
            &[
                Value::Text(file_id.to_string()),
                Value::Text(version_id.to_string()),
                Value::Text(snapshot),
            ],
        )
        .await
        .unwrap();
}

simulation_test!(
    unique_conflicts_within_same_version_and_file,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_unique_schema(&engine, "unique_scope_same_file").await;
        engine.create_named_version("version-a").await.unwrap();
        seed_file_descriptor(&engine, "version-a", "alpha.md").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'post-1', 'unique_scope_same_file', 'alpha.md', 'version-a', NULL, '1', '{\"id\":\"post-1\",\"slug\":\"hello-world\",\"title\":\"first\"}'\
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
                 'post-2', 'unique_scope_same_file', 'alpha.md', 'version-a', NULL, '1', '{\"id\":\"post-2\",\"slug\":\"hello-world\",\"title\":\"second\"}'\
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
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_unique_schema(&engine, "unique_scope_per_file").await;
        engine.create_named_version("version-a").await.unwrap();
        seed_file_descriptor(&engine, "version-a", "alpha.md").await;
        seed_file_descriptor(&engine, "version-a", "beta.md").await;

        engine
        .execute(
            "INSERT INTO lix_state_by_version (\
             entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
             ) VALUES (\
             'post-1', 'unique_scope_per_file', 'alpha.md', 'version-a', NULL, '1', '{\"id\":\"post-1\",\"slug\":\"hello-world\",\"title\":\"alpha\"}'\
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
             'post-2', 'unique_scope_per_file', 'beta.md', 'version-a', NULL, '1', '{\"id\":\"post-2\",\"slug\":\"hello-world\",\"title\":\"beta\"}'\
             )",
            &[],
        )
        .await
        .expect("same unique value should be allowed in a different file");
    }
);

async fn register_composite_unique_schema(
    engine: &support::simulation_test::SimulatedLix,
    schema_key: &str,
) {
    engine
        .register_schema(&json!({
            "x-lix-key": schema_key,
            "x-lix-version": "1",
            "x-lix-primary-key": ["/id"],
            "x-lix-unique": [["/locale", "/slug"]],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "locale": { "type": "string" },
                "slug": { "type": "string" }
            },
            "required": ["id", "locale", "slug"],
            "additionalProperties": false
        }))
        .await
        .unwrap();
}

simulation_test!(
    unique_violation_error_names_pointers_and_conflicting_value,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_unique_schema(&engine, "unique_msg_single").await;
        engine.create_named_version("version-a").await.unwrap();
        seed_file_descriptor(&engine, "version-a", "alpha.md").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'post-1', 'unique_msg_single', 'alpha.md', 'version-a', NULL, '1', '{\"id\":\"post-1\",\"slug\":\"hello-world\",\"title\":\"first\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let err = engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'post-2', 'unique_msg_single', 'alpha.md', 'version-a', NULL, '1', '{\"id\":\"post-2\",\"slug\":\"hello-world\",\"title\":\"second\"}'\
                 )",
                &[],
            )
            .await
            .expect_err("duplicate unique value should surface a violation");

        assert_eq!(
            err.code, "LIX_ERROR_UNIQUE",
            "unique violations should carry the categorized code"
        );
        let desc = &err.description;
        assert!(
            desc.contains("unique constraint violation on unique_msg_single./slug"),
            "expected schema + pointer in message: {desc}"
        );
        assert!(
            desc.contains("value \"hello-world\" already in use"),
            "expected conflicting value in message: {desc}"
        );
        assert!(
            desc.contains("conflicts with entity 'post-1'"),
            "expected conflicting entity in message: {desc}"
        );
        assert!(
            !desc.contains("version '"),
            "new message should not expose the internal version UUID: {desc}"
        );
        assert!(
            !desc.contains("'NULL'"),
            "new message should not print the literal 'NULL' file reference: {desc}"
        );
    }
);

simulation_test!(
    unique_violation_composite_renders_pointer_and_value_tuples,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_composite_unique_schema(&engine, "unique_msg_composite").await;
        engine.create_named_version("version-a").await.unwrap();
        seed_file_descriptor(&engine, "version-a", "alpha.md").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'post-1', 'unique_msg_composite', 'alpha.md', 'version-a', NULL, '1', '{\"id\":\"post-1\",\"locale\":\"en\",\"slug\":\"hello\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let err = engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'post-2', 'unique_msg_composite', 'alpha.md', 'version-a', NULL, '1', '{\"id\":\"post-2\",\"locale\":\"en\",\"slug\":\"hello\"}'\
                 )",
                &[],
            )
            .await
            .expect_err("duplicate composite unique tuple should surface a violation");

        let desc = &err.description;
        assert!(
            desc.contains("unique constraint violation on unique_msg_composite.(/locale, /slug)"),
            "expected composite pointer tuple in message: {desc}"
        );
        assert!(
            desc.contains("values (\"en\", \"hello\") already in use"),
            "expected composite value tuple in message: {desc}"
        );
        assert!(
            desc.contains("conflicts with entity 'post-1'"),
            "expected conflicting entity in message: {desc}"
        );
    }
);

simulation_test!(
    primary_key_violation_error_names_pointer_and_conflicting_value,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_unique_schema(&engine, "pk_msg_single").await;
        engine.create_named_version("version-a").await.unwrap();
        seed_file_descriptor(&engine, "version-a", "alpha.md").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'post-1', 'pk_msg_single', 'alpha.md', 'version-a', NULL, '1', '{\"id\":\"post-1\",\"slug\":\"first\",\"title\":\"first\"}'\
                 )",
                &[],
            )
            .await
            .unwrap();

        let err = engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'post-1', 'pk_msg_single', 'alpha.md', 'version-a', NULL, '1', '{\"id\":\"post-1\",\"slug\":\"second\",\"title\":\"second\"}'\
                 )",
                &[],
            )
            .await
            .expect_err("duplicate primary key should surface a violation");

        assert_eq!(
            err.code, "LIX_ERROR_UNIQUE",
            "primary-key violations should carry the categorized code"
        );
        let desc = &err.description;
        assert!(
            desc.contains("primary key violation on pk_msg_single./id"),
            "expected schema + pointer in message: {desc}"
        );
        assert!(
            desc.contains("value \"post-1\" already in use"),
            "expected conflicting value in message: {desc}"
        );
        assert!(
            desc.contains("conflicts with entity 'post-1'"),
            "expected conflicting entity in message: {desc}"
        );
        assert!(
            !desc.contains("version '"),
            "new message should not expose the internal version UUID: {desc}"
        );
        assert!(
            !desc.contains("'NULL'"),
            "new message should not print the literal 'NULL' file reference: {desc}"
        );
    }
);

simulation_test!(
    unique_allows_same_value_in_different_versions,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim
            .boot_simulated_lix(None)
            .await
            .expect("boot_simulated_lix should succeed");
        engine.initialize().await.unwrap();

        register_unique_schema(&engine, "unique_scope_per_version").await;
        engine.create_named_version("version-a").await.unwrap();
        engine.create_named_version("version-b").await.unwrap();
        seed_file_descriptor(&engine, "version-a", "alpha.md").await;
        seed_file_descriptor(&engine, "version-b", "alpha.md").await;

        engine
            .execute(
                "INSERT INTO lix_state_by_version (\
                 entity_id, schema_key, file_id, version_id, plugin_key, schema_version, snapshot_content\
                 ) VALUES (\
                 'post-1', 'unique_scope_per_version', 'alpha.md', 'version-a', NULL, '1', '{\"id\":\"post-1\",\"slug\":\"hello-world\",\"title\":\"v1\"}'\
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
                 'post-2', 'unique_scope_per_version', 'alpha.md', 'version-b', NULL, '1', '{\"id\":\"post-2\",\"slug\":\"hello-world\",\"title\":\"v2\"}'\
                 )",
                &[],
            )
            .await
            .expect("same unique value should be allowed in a different version");
    }
);
