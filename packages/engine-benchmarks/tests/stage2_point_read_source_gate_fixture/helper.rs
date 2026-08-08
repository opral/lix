// Source-gate fixture only: every transitive helper accepts the same view.
async fn load_selector_catalog_tree_object_authenticated(
    read: &impl StorageRead,
    key: &[u8],
) {
    let selector = load_selector(read).await?;
    let catalog = load_catalog(read, selector).await?;
    let tree = load_tree(read, catalog, key).await?;
    load_object_authenticated(read, tree).await
}
