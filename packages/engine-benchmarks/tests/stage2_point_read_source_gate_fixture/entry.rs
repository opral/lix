// Source-gate fixture only: this need not compile.
async fn read_point(storage: &impl Storage, key: &[u8]) {
    let read = storage.begin_read(ReadOptions::default()).await?;
    load_selector_catalog_tree_object_authenticated(&read, key).await
}
