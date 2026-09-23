use lix::Value;

use super::assert_rows_eq;

simulation_test!(sql_recursive_cte_walks_registered_parent_relations, |sim| async move {
    let engine = sim.boot_engine().await;
    let session = sim.wrap_session(engine.open_session().await.unwrap(), &engine);
    let schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "rr_recursive_tree",
        "columns": [
            {"name":"id","type":"text","nullable":false},
            {"name":"parent_id","type":"text","nullable":true}
        ],
        "primary_key": ["id"]
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema(value) VALUES ($1::jsonb)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("tree schema should register");
    session
        .execute(
            "INSERT INTO rr_recursive_tree(id, parent_id) VALUES \
             ('root', NULL), ('middle', 'root'), ('leaf', 'middle')",
            &[],
        )
        .await
        .expect("tree rows should insert");

    let result = session
        .execute(
            "WITH RECURSIVE ancestors(id, parent_id, depth) AS ( \
                 SELECT id, parent_id, 0 FROM rr_recursive_tree WHERE id = $1 \
                 UNION ALL \
                 SELECT parent.id, parent.parent_id, ancestors.depth + 1 \
                 FROM rr_recursive_tree AS parent \
                 JOIN ancestors ON parent.id = ancestors.parent_id \
             ) \
             SELECT id, depth FROM ancestors ORDER BY depth",
            &[Value::Text("leaf".to_string())],
        )
        .await
        .expect("DataFusion recursive CTE should traverse a registered relation");

    assert_rows_eq(
        result,
        vec![
            vec![Value::Text("leaf".to_string()), Value::Integer(0)],
            vec![Value::Text("middle".to_string()), Value::Integer(1)],
            vec![Value::Text("root".to_string()), Value::Integer(2)],
        ],
    );
});
