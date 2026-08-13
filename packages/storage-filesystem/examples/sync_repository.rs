use lix::open_lix;
use lix_storage_filesystem::LocalFilesystem;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "./repository".to_owned());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        let storage = LocalFilesystem::open(&root)?;
        let lix = open_lix().with_storage(storage.clone()).await?;
        let sync = storage.start_sync(&lix).await?;

        // Import changes already present on disk immediately. The returned
        // sync handle also keeps watching while it remains alive.
        sync.sync_disk_to_lix().await?;

        let files = lix
            .execute("SELECT path FROM lix_file ORDER BY path", &[])
            .await?;
        println!("{} repository files", files.rows().len());

        lix.close().await?;
        Ok::<_, lix::LixError>(())
    })?;
    Ok(())
}
