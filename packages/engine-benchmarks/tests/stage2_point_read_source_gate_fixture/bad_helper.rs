// Negative source-gate fixture only: replacement views are forbidden.
async fn load_object_authenticated(storage: &impl Storage) {
    let replacement = storage.begin_read(ReadOptions::default()).await?;
    authenticate(&replacement).await
}
