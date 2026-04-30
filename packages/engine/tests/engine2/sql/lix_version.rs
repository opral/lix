use crate::simulation_test2;
use lix_engine::engine2::ExecuteResult;
use lix_engine::Value;

simulation_test2!(lix_version_lists_descriptors_with_refs, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(
        engine
            .open_session("global")
            .await
            .expect("global session should open"),
        &engine,
    );

    let result = session
        .execute(
            "SELECT id, name, hidden, commit_id FROM lix_version ORDER BY id",
            &[],
        )
        .await
        .expect("lix_version should read");
    let ExecuteResult::Rows(rows) = result else {
        panic!("SELECT should return rows");
    };
    assert_eq!(rows.len(), 2);

    let values = rows
        .rows()
        .iter()
        .map(|row| row.values().to_vec())
        .collect::<Vec<_>>();
    assert!(values.contains(&vec![
        Value::Text("global".to_string()),
        Value::Text("global".to_string()),
        Value::Boolean(true),
        Value::Text(sim.initial_commit_id().to_string()),
    ]));
    assert!(values.contains(&vec![
        Value::Text(sim.main_version_id().to_string()),
        Value::Text("main".to_string()),
        Value::Boolean(false),
        Value::Text(sim.initial_commit_id().to_string()),
    ]));
});
