mod common;

use common::{ORIGINAL_RUST_SOURCE, UPDATED_RUST_SOURCE, file_id_for_path};
use lix_sdk::{FsWriteOptions, Value};

#[tokio::test]
async fn renders_sem_state_back_to_file_bytes() {
    let lix = common::open_lix_with_sem_plugin().await;

    lix.write_file(
        "/src/lib.rs",
        ORIGINAL_RUST_SOURCE.to_vec(),
        FsWriteOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        lix.read_file("/src/lib.rs").await.unwrap().as_deref(),
        Some(ORIGINAL_RUST_SOURCE)
    );

    lix.write_file(
        "/src/lib.rs",
        UPDATED_RUST_SOURCE.to_vec(),
        FsWriteOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        lix.read_file("/src/lib.rs").await.unwrap().as_deref(),
        Some(UPDATED_RUST_SOURCE)
    );

    let file_id = file_id_for_path(&lix, "/src/lib.rs").await;
    let rendered = lix
        .execute(
            "SELECT data FROM lix_file WHERE id = $1",
            &[Value::Text(file_id)],
        )
        .await
        .unwrap();
    assert_eq!(rendered.len(), 1);
    assert_eq!(
        rendered.rows()[0].values(),
        &[Value::Blob(UPDATED_RUST_SOURCE.to_vec())]
    );

    lix.close().await.unwrap();
}
