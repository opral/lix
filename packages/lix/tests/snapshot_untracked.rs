//! A snapshot copies current untracked state as well as tracked history.
use futures_util::io::Cursor;
use lix::open_lix;

#[tokio::test]
async fn snapshot_preserves_untracked_rows_without_adding_history() {
    let source = open_lix().await.unwrap();
    source
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('tracked-example', 'first')",
            &[],
        )
        .await
        .unwrap();
    source
        .execute(
            "UPDATE lix_key_value SET value = 'second' WHERE key = 'tracked-example'",
            &[],
        )
        .await
        .unwrap();
    source.execute("INSERT INTO lix_key_value (key, value, lixcol_untracked) VALUES ('local-example', 'kept', true)", &[]).await.unwrap();
    source.execute("INSERT INTO lix_key_value (key, value, lixcol_global, lixcol_untracked) VALUES ('global-example', 'also-kept', true, true)", &[]).await.unwrap();
    let query = "SELECT key, value, lixcol_untracked FROM lix_key_value WHERE key IN ('tracked-example', 'local-example', 'global-example') ORDER BY key";
    let before = source.execute(query, &[]).await.unwrap();
    let history_before = source
        .execute("SELECT COUNT(*) AS count FROM lix_commit", &[])
        .await
        .unwrap();
    let mut snapshot = Vec::new();
    source
        .export_snapshot()
        .write_to(&mut snapshot)
        .await
        .unwrap();
    let restored = open_lix()
        .from_snapshot(Cursor::new(snapshot))
        .await
        .unwrap();
    let after = restored.execute(query, &[]).await.unwrap();
    assert_eq!(before.rows(), after.rows());
    assert_eq!(after.rows().len(), 3);
    let history_after = restored
        .execute("SELECT COUNT(*) AS count FROM lix_commit", &[])
        .await
        .unwrap();
    assert_eq!(history_before.rows(), history_after.rows());
    source.close().await.unwrap();
    restored.close().await.unwrap();
}
