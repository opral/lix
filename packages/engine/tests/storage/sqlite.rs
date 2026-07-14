use lix_engine::run_storage_conformance;
use lix_sqlite_storage::SQLiteFactory;

#[tokio::test]
async fn sqlite_passes_storage_conformance() {
    let factory = SQLiteFactory::new();

    run_storage_conformance(&factory).await.assert_no_failures();
}
