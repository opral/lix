use lix::open_lix;
use lix_storage_filesystem::FilesystemStorage;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "./repository".to_owned());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        let storage = FilesystemStorage::new(&root).open()?;
        let lix = open_lix().with_storage(storage.clone()).await?;
        storage.start_sync(&lix).await?;

        // Import changes already present on disk immediately. The storage
        // keeps watching until explicitly stopped or finally dropped.
        storage.sync_disk_to_lix().await?;

        let files = lix
            .execute("SELECT path FROM lix_file ORDER BY path", &[])
            .await?;
        println!("{} repository files", files.rows().len());

        storage.stop_sync().await?;
        lix.close().await?;
        Ok::<_, lix::LixError>(())
    })?;
    Ok(())
}
