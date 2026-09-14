//! Explicit offline maintenance. Serving processes must be stopped before use.
use super::*;

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthorityMigrationReport {
    pub repository_id: String,
    pub backup_prefix: String,
    pub source_storage_id: String,
    pub destination_storage_id: String,
    pub migration: lix_sdk::migration::RepositoryMigrationReport,
    pub admission_published: bool,
}

impl LixRuntimeManager {
    /// Copies physical objects before migration and records exact source versions.
    /// This does not open an engine or drop any source/receipt data.
    async fn backup_authority(&self, id: &str, storage_id: &str) -> Result<String> {
        let (objects, prefix) = self.catalog_store();
        let source_prefix = format!("{prefix}{storage_id}/");
        let backup_prefix = format!(
            "{prefix}.lix-migration-backups/{id}/{}/",
            uuid::Uuid::new_v4()
        );
        let source_path = ObjectPath::from(source_prefix.clone());
        let initial: Vec<_> = objects.list(Some(&source_path)).try_collect().await?;
        if initial.is_empty() {
            anyhow::bail!("authority physical storage is missing");
        }
        let mut manifest = Vec::new();
        for object in &initial {
            let relative = object
                .location
                .as_ref()
                .strip_prefix(&source_prefix)
                .context("unexpected backup source path")?;
            let target = ObjectPath::from(format!("{backup_prefix}{relative}"));
            objects.copy(&object.location, &target).await?;
            let source_digest = object_digest(&objects, &object.location).await?;
            if source_digest != object_digest(&objects, &target).await? {
                anyhow::bail!("authority backup content verification failed");
            }
            manifest.push(serde_json::json!({"key":relative,"bytes":object.size,"etag":object.e_tag,"version":object.version,"blake3":source_digest}));
        }
        let final_objects: Vec<_> = objects.list(Some(&source_path)).try_collect().await?;
        let signature = |items: &[object_store::ObjectMeta]| {
            items
                .iter()
                .map(|item| {
                    (
                        item.location.to_string(),
                        (
                            item.e_tag.clone(),
                            item.version.clone(),
                            item.size,
                            item.last_modified,
                        ),
                    )
                })
                .collect::<std::collections::BTreeMap<_, _>>()
        };
        if signature(&initial) != signature(&final_objects) {
            anyhow::bail!("authority changed during backup; stop all writers before retrying");
        }
        let catalog = ObjectPath::from(format!("{prefix}.lix-repositories/{id}.json"));
        objects
            .copy(
                &catalog,
                &ObjectPath::from(format!("{backup_prefix}repository-catalog.json")),
            )
            .await?;
        objects.put(&ObjectPath::from(format!("{backup_prefix}backup-manifest.json")), serde_json::to_vec(&serde_json::json!({"repositoryId":id,"storageId":storage_id,"objects":manifest}))?.into()).await?;
        Ok(backup_prefix)
    }

    pub(super) async fn migrate_authority_offline(
        &self,
        id: &str,
    ) -> Result<AuthorityMigrationReport> {
        let (objects, prefix) = self.catalog_store();
        let path = ObjectPath::from(format!("{prefix}.lix-repositories/{id}.json"));
        let response = objects
            .get(&path)
            .await
            .context("authority upgrade requires a live repository catalog entry")?;
        let version = object_store::UpdateVersion {
            e_tag: response.meta.e_tag.clone(),
            version: response.meta.version.clone(),
        };
        let record_bytes = response.bytes().await?;
        let mut record: RepositoryRecord = serde_json::from_slice(&record_bytes)?;
        if record.state != "live" {
            anyhow::bail!("authority upgrade requires a live repository catalog entry");
        }
        if !valid_lix_id(id) || !valid_lix_id(&record.storage_id) {
            anyhow::bail!("invalid repository or physical storage identifier");
        }
        let backup_prefix = self.backup_authority(id, &record.storage_id).await?;
        objects
            .put(
                &ObjectPath::from(format!("{backup_prefix}repository-catalog.json")),
                record_bytes.into(),
            )
            .await?;
        objects
            .put(
                &ObjectPath::from(format!("{backup_prefix}catalog-version.json")),
                serde_json::to_vec(
                    &serde_json::json!({"etag":version.e_tag,"version":version.version}),
                )?
                .into(),
            )
            .await?;
        let source_storage_id = record.storage_id.clone();
        let destination_storage_id = uuid::Uuid::new_v4().to_string();
        let stage_path = ObjectPath::from(format!(
            "{prefix}.lix-migration-stages/{id}/{destination_storage_id}.json"
        ));
        objects.put(&stage_path, serde_json::to_vec(&serde_json::json!({"repositoryId":id,"sourceStorageId":source_storage_id,"destinationStorageId":destination_storage_id,"backupPrefix":backup_prefix}))?.into()).await?;
        // The original physical store is never opened by the migration engine.
        // Its old writer epoch and source data remain available for recovery.
        let manifest = objects
            .get(&ObjectPath::from(format!(
                "{backup_prefix}backup-manifest.json"
            )))
            .await?
            .bytes()
            .await?;
        let manifest: serde_json::Value = serde_json::from_slice(&manifest)?;
        let entries = manifest["objects"]
            .as_array()
            .context("invalid backup manifest")?;
        for entry in entries {
            let key = entry["key"].as_str().context("invalid backup key")?;
            let target = ObjectPath::from(format!("{prefix}{destination_storage_id}/{key}"));
            objects
                .copy(&ObjectPath::from(format!("{backup_prefix}{key}")), &target)
                .await?;
            if object_digest(&objects, &target).await?
                != entry["blake3"].as_str().context("missing backup digest")?
            {
                anyhow::bail!(
                    "migration destination copy failed verification; original source retained"
                );
            }
        }
        let storage = self.open_storage(&destination_storage_id, SlateDBIoCounters::default())?;
        let before = lix_sdk::migration::inspect_repository(storage.clone()).await?;
        if before.role != lix_sdk::migration::RepositoryRole::Authority {
            anyhow::bail!(
                "catalogued repository is not an authority; explicit operator reconciliation required"
            );
        }
        let migration = lix_sdk::migration::migrate_repository(storage).await?;
        let verified = migration.semantic_preservation_verified
            && migration.after.current
            && migration.after.role == lix_sdk::migration::RepositoryRole::Authority;
        // Host resource IDs deliberately differ from portable snapshot IDs.
        // The original catalog mapping and its CAS preserve host identity;
        // the logical witness preserves the embedded repository identity.
        let mut report = AuthorityMigrationReport {
            repository_id: id.to_owned(),
            backup_prefix,
            source_storage_id,
            destination_storage_id,
            migration,
            admission_published: false,
        };
        let report_path = ObjectPath::from(format!(
            "{prefix}.lix-migration-reports/{id}/{}.json",
            report.destination_storage_id
        ));
        // Persist the witness before publishing admission, including on CAS failure.
        objects
            .put(&report_path, serde_json::to_vec(&report)?.into())
            .await?;
        if verified {
            // Detect writers that ignored the externally required stop barrier.
            // This is a guard, not a replacement for disabling serving/restarts.
            let source_path = ObjectPath::from(format!("{prefix}{}/", report.source_storage_id));
            let current: Vec<_> = objects.list(Some(&source_path)).try_collect().await?;
            if current.len() != entries.len() {
                anyhow::bail!(
                    "source changed after backup; original retained and admission withheld"
                );
            }
            for entry in entries {
                let key = entry["key"].as_str().context("invalid backup key")?;
                let source =
                    ObjectPath::from(format!("{prefix}{}/{key}", report.source_storage_id));
                if object_digest(&objects, &source).await?
                    != entry["blake3"].as_str().context("missing backup digest")?
                {
                    anyhow::bail!(
                        "source changed after backup; original retained and admission withheld"
                    );
                }
            }
            record.retired.push(report.source_storage_id.clone());
            record.storage_id = report.destination_storage_id.clone();
            record.admission = Some(AuthorityAdmission::current());
            objects
                .put_opts(
                    &path,
                    serde_json::to_vec(&record)?.into(),
                    object_store::PutOptions {
                        mode: object_store::PutMode::Update(version),
                        ..Default::default()
                    },
                )
                .await
                .context("catalog changed during migration; admission was not published")?;
        }
        report.admission_published = verified;
        objects
            .put(&report_path, serde_json::to_vec(&report)?.into())
            .await?;
        Ok(report)
    }

    pub async fn migrate_authority_fleet(self: Arc<Self>) -> Result<Vec<AuthorityMigrationReport>> {
        let manager = Arc::try_unwrap(self)
            .map_err(|_| anyhow::anyhow!("authority migration requires an offline manager"))?;
        if !manager.state.lock().await.entries.is_empty() {
            anyhow::bail!("close all repository runtimes before authority migration");
        }
        let inventory = manager.inventory_authorities().await?;
        if !inventory.malformed_catalog.is_empty() || !inventory.unreferenced_storage.is_empty() {
            anyhow::bail!(
                "reconcile malformed catalog or unreferenced physical storage before fleet migration"
            );
        }
        if inventory
            .entries
            .iter()
            .any(|entry| entry.state == "live" && !entry.storage_present)
        {
            anyhow::bail!(
                "live authority physical storage is missing; fleet migration is incomplete"
            );
        }
        let mut reports = Vec::new();
        for entry in inventory
            .entries
            .into_iter()
            .filter(|entry| entry.state == "live" && entry.requires_migration)
        {
            let report = manager
                .migrate_authority_offline(&entry.repository_id)
                .await?;
            let verified = report.admission_published;
            reports.push(report);
            if !verified {
                anyhow::bail!(
                    "semantic preservation not verified; fleet remains incomplete (report retained in object storage)"
                );
            }
        }
        Ok(reports)
    }
}

async fn object_digest(objects: &Arc<dyn ObjectStore>, path: &ObjectPath) -> Result<String> {
    let mut chunks = objects.get(path).await?.into_stream();
    let mut digest = Hasher::new();
    while let Some(chunk) = chunks.try_next().await? {
        digest.update(&chunk);
    }
    Ok(digest.finalize().to_hex().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    const ID: &str = "11111111-1111-4111-8111-111111111111";
    #[tokio::test]
    async fn backup_is_verified_and_retains_catalog_and_source() {
        let manager = LixRuntimeManager::new_in_memory(1);
        manager.write_record(ID, "live", None, true).await.unwrap();
        let (objects, _) = manager.catalog_store();
        let source = ObjectPath::from(format!("{ID}/data"));
        objects.put(&source, "original".into()).await.unwrap();
        let prefix = manager.backup_authority(ID, ID).await.unwrap();
        assert_eq!(
            objects.get(&source).await.unwrap().bytes().await.unwrap(),
            "original"
        );
        assert_eq!(
            objects
                .get(&ObjectPath::from(format!("{prefix}data")))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            "original"
        );
        let manifest = objects
            .get(&ObjectPath::from(format!("{prefix}backup-manifest.json")))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
        assert_eq!(
            manifest["objects"][0]["blake3"],
            blake3::hash(b"original").to_hex().to_string()
        );
        objects
            .head(&ObjectPath::from(format!(
                "{prefix}repository-catalog.json"
            )))
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn missing_physical_source_never_publishes_backup() {
        let manager = LixRuntimeManager::new_in_memory(1);
        assert!(
            manager
                .backup_authority(ID, ID)
                .await
                .unwrap_err()
                .to_string()
                .contains("physical storage is missing")
        );
    }
    #[tokio::test]
    async fn current_admission_never_hides_missing_physical_storage_in_fleet() {
        let manager = LixRuntimeManager::new_in_memory(1);
        manager.write_record(ID, "live", None, true).await.unwrap();
        assert!(
            manager
                .migrate_authority_fleet()
                .await
                .unwrap_err()
                .to_string()
                .contains("physical storage is missing")
        );
    }
}
