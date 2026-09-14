//! Offline metadata-only tombstones. Never opens, deletes, or cleans physical storage.
use super::inventory::{PhysicalManifestEntry, verify_physical_manifest};
use super::*;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RetainedSource {
    storage_id: String,
    objects: Vec<PhysicalManifestEntry>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TombstoneManifest {
    schema_version: u32,
    hosted_repository_id: String,
    control_inventory_path: String,
    control_inventory_blake3: String,
    control_deleted_at: String,
    catalog_blake3: String,
    physical_sources: Vec<RetainedSource>,
}

fn read_bounded(path: &Path) -> Result<Vec<u8>> {
    if fs::metadata(path)?.len() > 64 * 1024 * 1024 {
        anyhow::bail!("retained tombstone input exceeds 64 MiB");
    }
    Ok(fs::read(path)?)
}

fn validate_control(manifest: &TombstoneManifest, bytes: &[u8]) -> Result<()> {
    if manifest.schema_version != 1
        || !valid_lix_id(&manifest.hosted_repository_id)
        || manifest.control_deleted_at.trim().is_empty()
        || blake3::hash(bytes).to_hex().as_str() != manifest.control_inventory_blake3
    {
        anyhow::bail!("retained tombstone requires the exact reviewed control inventory");
    }
    let rows: Vec<serde_json::Value> = serde_json::from_slice(bytes)?;
    let mut ids = std::collections::BTreeSet::new();
    let mut deleted = false;
    for row in rows {
        let id = row["id"].as_str().context("control row missing identity")?;
        if !ids.insert(id.to_owned()) {
            anyhow::bail!("duplicate control identity");
        }
        if id == manifest.hosted_repository_id {
            deleted = row["deleted_at"].as_str() == Some(manifest.control_deleted_at.as_str());
        }
    }
    if !deleted {
        anyhow::bail!("control record is not the explicitly reviewed tombstone");
    }
    Ok(())
}

impl LixRuntimeManager {
    /// Requires all serving/lifecycle writers and automatic restarts to be stopped.
    pub async fn retain_tombstone_offline(
        self: Arc<Self>,
        manifest_path: &Path,
    ) -> Result<serde_json::Value> {
        let manager = Arc::try_unwrap(self)
            .map_err(|_| anyhow::anyhow!("retained tombstone requires an offline manager"))?;
        if !manager.state.lock().await.entries.is_empty() {
            anyhow::bail!("close all runtimes before retaining a tombstone");
        }
        let manifest: TombstoneManifest = serde_json::from_slice(&read_bounded(manifest_path)?)?;
        let parent = manifest_path.parent().unwrap_or_else(|| Path::new("."));
        let control_bytes = read_bounded(&parent.join(&manifest.control_inventory_path))?;
        validate_control(&manifest, &control_bytes)?;
        manager.retain_tombstone(&manifest).await
    }

    async fn verify_retained_sources(
        &self,
        manifest: &TombstoneManifest,
        record: &RepositoryRecord,
    ) -> Result<()> {
        let expected: std::collections::BTreeSet<_> = std::iter::once(record.storage_id.as_str())
            .chain(record.retired.iter().map(String::as_str))
            .collect();
        let supplied: std::collections::BTreeSet<_> = manifest
            .physical_sources
            .iter()
            .map(|source| source.storage_id.as_str())
            .collect();
        if expected != supplied || supplied.len() != manifest.physical_sources.len() {
            anyhow::bail!(
                "retained tombstone must account for active and every retired physical source"
            );
        }
        let (objects, prefix) = self.catalog_store();
        for source in &manifest.physical_sources {
            verify_physical_manifest(&objects, &prefix, &source.storage_id, &source.objects)
                .await?;
        }
        Ok(())
    }

    async fn retain_tombstone(&self, manifest: &TombstoneManifest) -> Result<serde_json::Value> {
        let manifest_digest = blake3::hash(&serde_json::to_vec(manifest)?)
            .to_hex()
            .to_string();
        let (objects, prefix) = self.catalog_store();
        let catalog_path = ObjectPath::from(format!(
            "{prefix}.lix-repositories/{}.json",
            manifest.hosted_repository_id
        ));
        let report_path = ObjectPath::from(format!(
            "{prefix}.lix-retained-tombstones/{}/{}.json",
            manifest.hosted_repository_id, manifest_digest
        ));
        let response = objects.get(&catalog_path).await?;
        let version = object_store::UpdateVersion {
            e_tag: response.meta.e_tag.clone(),
            version: response.meta.version.clone(),
        };
        let catalog = response.bytes().await?;
        let record: RepositoryRecord = serde_json::from_slice(&catalog)?;
        self.verify_retained_sources(manifest, &record).await?;
        let mut tombstone: serde_json::Value = serde_json::from_slice(&catalog)?;
        tombstone["state"] = "deleted".into();
        tombstone["fingerprint"] = serde_json::Value::Null;
        tombstone["admission"] = serde_json::Value::Null;
        let tombstone_bytes = serde_json::to_vec(&tombstone)?;
        let tombstone_digest = blake3::hash(&tombstone_bytes).to_hex().to_string();
        if record.state == "deleted" {
            let mut report: serde_json::Value =
                serde_json::from_slice(&objects.get(&report_path).await?.bytes().await?)?;
            if report["prepared"] != true
                || report["originalCatalog"].as_str().is_none_or(|original| {
                    blake3::hash(original.as_bytes()).to_hex().as_str() != manifest.catalog_blake3
                })
                || report["manifestDigest"] != manifest_digest
                || report["catalogBlake3"] != manifest.catalog_blake3
                || report["tombstoneBlake3"] != tombstone_digest
                || blake3::hash(&catalog).to_hex().as_str() != tombstone_digest
            {
                anyhow::bail!("existing tombstone is not the exact prepared retained operation");
            }
            report["published"] = true.into();
            objects
                .put(&report_path, serde_json::to_vec(&report)?.into())
                .await?;
            return Ok(report);
        }
        if record.state != "live"
            || blake3::hash(&catalog).to_hex().as_str() != manifest.catalog_blake3
        {
            anyhow::bail!("catalog differs from the reviewed live source; refusing tombstone");
        }
        let mut report = serde_json::json!({
            "schemaVersion":1, "manifestDigest":manifest_digest,"manifest":manifest,
            "catalogBlake3":manifest.catalog_blake3,"originalCatalog":std::str::from_utf8(&catalog)?,
            "catalogVersion":{"etag":version.e_tag,"version":version.version},
            "tombstoneBlake3":tombstone_digest,"prepared":true,"published":false,
            "physicalSourcesPreserved":true,
        });
        match objects
            .put_opts(
                &report_path,
                serde_json::to_vec(&report)?.into(),
                object_store::PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => {}
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => {
                let prior: serde_json::Value =
                    serde_json::from_slice(&objects.get(&report_path).await?.bytes().await?)?;
                if prior["manifestDigest"] != report["manifestDigest"]
                    || prior["originalCatalog"] != report["originalCatalog"]
                    || prior["tombstoneBlake3"] != report["tombstoneBlake3"]
                {
                    anyhow::bail!("conflicting prepared retained tombstone report");
                }
            }
            Err(error) => return Err(error.into()),
        }
        self.verify_retained_sources(manifest, &record).await?;
        objects
            .put_opts(
                &catalog_path,
                tombstone_bytes.into(),
                object_store::PutOptions {
                    mode: object_store::PutMode::Update(version),
                    ..Default::default()
                },
            )
            .await
            .context("catalog changed during retained tombstone; CAS refused publication")?;
        self.verify_retained_sources(manifest, &record).await?;
        report["published"] = true.into();
        objects
            .put(&report_path, serde_json::to_vec(&report)?.into())
            .await?;
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fixture() -> (Arc<LixRuntimeManager>, TombstoneManifest, tempfile::TempDir) {
        let manager = LixRuntimeManager::new_in_memory(4);
        let id = uuid::Uuid::new_v4().to_string();
        let source = uuid::Uuid::new_v4().to_string();
        let retired = uuid::Uuid::new_v4().to_string();
        let (objects, prefix) = manager.catalog_store();
        let catalog = serde_json::to_vec(&serde_json::json!({"state":"live","storage_id":source,"retired":[retired],"fingerprint":"create-proof","admission":null,"extraRetainedMetadata":{"important":true}})).unwrap();
        objects
            .put(
                &ObjectPath::from(format!("{prefix}.lix-repositories/{id}.json")),
                catalog.clone().into(),
            )
            .await
            .unwrap();
        let mut sources = vec![];
        for storage_id in [source, retired] {
            objects
                .put(
                    &ObjectPath::from(format!("{prefix}{storage_id}/original")),
                    b"original".to_vec().into(),
                )
                .await
                .unwrap();
            sources.push(RetainedSource {
                storage_id,
                objects: vec![PhysicalManifestEntry {
                    key: "original".into(),
                    bytes: 8,
                    blake3: blake3::hash(b"original").to_hex().to_string(),
                }],
            });
        }
        let deleted_at = "2026-09-09T23:31:49Z";
        let control =
            serde_json::to_vec(&serde_json::json!([{"id":id,"deleted_at":deleted_at}])).unwrap();
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("control.json"), &control).unwrap();
        let manifest = TombstoneManifest {
            schema_version: 1,
            hosted_repository_id: id,
            control_inventory_path: "control.json".into(),
            control_inventory_blake3: blake3::hash(&control).to_hex().to_string(),
            control_deleted_at: deleted_at.into(),
            catalog_blake3: blake3::hash(&catalog).to_hex().to_string(),
            physical_sources: sources,
        };
        fs::write(
            dir.path().join("manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        (manager, manifest, dir)
    }

    #[tokio::test]
    async fn retained_tombstone_keeps_active_retired_and_extra_metadata_and_retries() {
        let (manager, manifest, dir) = fixture().await;
        let (objects, prefix) = manager.catalog_store();
        let report = manager
            .retain_tombstone_offline(&dir.path().join("manifest.json"))
            .await
            .unwrap();
        assert_eq!(report["published"], true);
        let value: serde_json::Value = serde_json::from_slice(
            &objects
                .get(&ObjectPath::from(format!(
                    "{prefix}.lix-repositories/{}.json",
                    manifest.hosted_repository_id
                )))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(value["state"], "deleted");
        assert_eq!(value["extraRetainedMetadata"]["important"], true);
        let report_path = ObjectPath::from(format!(
            "{prefix}.lix-retained-tombstones/{}/{}.json",
            manifest.hosted_repository_id,
            report["manifestDigest"].as_str().unwrap()
        ));
        let mut pending = report.clone();
        pending["published"] = false.into();
        objects
            .put(&report_path, serde_json::to_vec(&pending).unwrap().into())
            .await
            .unwrap();
        let mut restarted = LixRuntimeManager::new_in_memory(4);
        Arc::get_mut(&mut restarted).unwrap().backend = StorageBackend::Memory {
            object_store: objects.clone(),
        };
        let retry = restarted
            .retain_tombstone_offline(&dir.path().join("manifest.json"))
            .await
            .unwrap();
        assert_eq!(retry["manifestDigest"], report["manifestDigest"]);
        assert_eq!(retry["published"], true);
        let durable: serde_json::Value = serde_json::from_slice(
            &objects
                .get(&report_path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(durable["published"], true);
    }

    #[tokio::test]
    async fn retained_tombstone_rejects_live_changed_or_duplicate_control() {
        let (_, manifest, _) = fixture().await;
        for rows in [
            serde_json::json!([{"id":manifest.hosted_repository_id,"deleted_at":null}]),
            serde_json::json!([{"id":manifest.hosted_repository_id,"deleted_at":manifest.control_deleted_at},{"id":manifest.hosted_repository_id,"deleted_at":manifest.control_deleted_at}]),
        ] {
            let bytes = serde_json::to_vec(&rows).unwrap();
            let mut changed: TombstoneManifest =
                serde_json::from_value(serde_json::to_value(&manifest).unwrap()).unwrap();
            changed.control_inventory_blake3 = blake3::hash(&bytes).to_hex().to_string();
            assert!(validate_control(&changed, &bytes).is_err());
        }
        assert!(validate_control(&manifest, b"[]").is_err());
    }

    #[tokio::test]
    async fn retained_tombstone_rejects_catalog_or_physical_changes() {
        for change_catalog in [false, true] {
            let (manager, manifest, dir) = fixture().await;
            let (objects, prefix) = manager.catalog_store();
            if change_catalog {
                let path = ObjectPath::from(format!(
                    "{prefix}.lix-repositories/{}.json",
                    manifest.hosted_repository_id
                ));
                let mut value: serde_json::Value = serde_json::from_slice(
                    &objects.get(&path).await.unwrap().bytes().await.unwrap(),
                )
                .unwrap();
                value["fingerprint"] = "changed".into();
                objects
                    .put(&path, serde_json::to_vec(&value).unwrap().into())
                    .await
                    .unwrap();
            } else {
                objects
                    .put(
                        &ObjectPath::from(format!(
                            "{prefix}{}/original",
                            manifest.physical_sources[0].storage_id
                        )),
                        b"changed!".to_vec().into(),
                    )
                    .await
                    .unwrap();
            }
            assert!(
                manager
                    .retain_tombstone_offline(&dir.path().join("manifest.json"))
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn retained_tombstone_rejects_omitted_retired_source() {
        let (manager, mut manifest, _) = fixture().await;
        manifest.physical_sources.pop();
        assert!(manager.retain_tombstone(&manifest).await.is_err());
    }
}
