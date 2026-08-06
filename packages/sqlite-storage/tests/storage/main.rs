mod observe_mutation_revision;

use lix::storage::conformance::run_storage_conformance;
use lix_storage_sqlite::SQLiteFactory;

#[tokio::test]
async fn sqlite_passes_storage_conformance() {
    let factory = SQLiteFactory::new();

    run_storage_conformance(&factory).await.assert_no_failures();
}
