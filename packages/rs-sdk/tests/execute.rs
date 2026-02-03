use lix_rs_sdk::{open_lix, OpenLixConfig, Value};

#[tokio::test]
async fn select_works_with_default_in_memory_sqlite() {
    let lix = open_lix(OpenLixConfig::default())
        .await
        .expect("open_lix should succeed");

    let result = lix.execute("SELECT 1 + 1", &[]).await.unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Integer(2));
}
