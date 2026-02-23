mod support;

use lix_engine::Value;

async fn active_version_id(engine: &support::simulation_test::SimulationEngine) -> String {
    let rows = engine
        .execute(
            "SELECT version_id FROM lix_active_version ORDER BY id LIMIT 1",
            &[],
        )
        .await
        .unwrap();
    match &rows.rows[0][0] {
        Value::Text(value) => value.clone(),
        other => panic!("expected text, got {other:?}"),
    }
}

async fn insert_version(
    engine: &support::simulation_test::SimulationEngine,
    version_id: &str,
    parent_version_id: &str,
) {
    let sql = format!(
        "INSERT INTO lix_version (id, name, inherits_from_version_id, hidden, commit_id, working_commit_id) \
         VALUES ('{version_id}', '{version_id}', '{parent_version_id}', 0, 'commit-{version_id}', 'working-{version_id}')"
    );
    engine.execute(&sql, &[]).await.unwrap();
}

simulation_test!(
    debug_file_by_version_delete,
    simulations = [sqlite],
    |sim| async move {
        let engine = sim.boot_simulated_engine_deterministic().await.unwrap();
        engine.init().await.unwrap();

        let version_a = active_version_id(&engine).await;
        let version_b = "version-b".to_string();
        insert_version(&engine, &version_b, &version_a).await;

        let version_a_sql = version_a.replace('"', "\"\"");
        let version_b_sql = version_b.replace('"', "\"\"");

        engine.execute(
            &format!(
                "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                 VALUES ('file-shared', '/shared/config.json', 'ignored', '{version}')",
                version = version_a_sql
            ),
            &[],
        ).await.unwrap();

        engine.execute(
            &format!(
                "INSERT INTO lix_file_by_version (id, path, data, lixcol_version_id) \
                 VALUES ('file-shared', '/shared/config-b.json', 'ignored-b', '{version}')",
                version = version_b_sql
            ),
            &[],
        ).await.unwrap();

        engine.execute(
            &format!(
                "UPDATE lix_file_by_version \
                 SET path = '/shared/config-renamed.json', data = 'ignored-again' \
                 WHERE id = 'file-shared' AND lixcol_version_id = '{version}'",
                version = version_b_sql
            ),
            &[],
        ).await.unwrap();

        engine.execute(
            &format!(
                "DELETE FROM lix_file_by_version \
                 WHERE id = 'file-shared' AND lixcol_version_id = '{version}'",
                version = version_b_sql
            ),
            &[],
        ).await.unwrap();

        let result = engine.execute(
            &format!(
                "SELECT id, path, data FROM lix_file_by_version WHERE id='file-shared' AND lixcol_version_id='{version}'",
                version = version_b_sql
            ),
            &[],
        ).await.unwrap();
        eprintln!("after delete lix_file_by_version rows={:?}", result.rows);

        let internal_mat = engine.execute(
            &format!(
                "SELECT entity_id, version_id, is_tombstone, snapshot_content, change_id FROM lix_internal_state_materialized_v1_lix_file_descriptor \
                 WHERE entity_id='file-shared' AND version_id='{version}'",
                version = version_b_sql
            ),
            &[],
        ).await.unwrap();
        eprintln!("materialized rows={:?}", internal_mat.rows);

        let internal_untracked = engine.execute(
            &format!(
                "SELECT entity_id, version_id, snapshot_content FROM lix_internal_state_untracked \
                 WHERE entity_id='file-shared' AND version_id='{version}'",
                version = version_b_sql
            ),
            &[],
        ).await.unwrap();
        eprintln!("untracked rows={:?}", internal_untracked.rows);
    }
);
