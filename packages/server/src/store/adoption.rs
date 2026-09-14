//! Explicit offline adoption of proof-qualified, separately staged repositories.
use super::inventory::{PhysicalManifestEntry, verify_physical_manifest};
use super::*;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AdoptionManifest {
    schema_version: u32,
    hosted_repository_id: String,
    source_storage_id: String,
    staged_storage_id: String,
    source_objects: Vec<PhysicalManifestEntry>,
    current_proof_path: String,
    historical_proof_path: Option<String>,
}

fn read_json(path: &Path) -> Result<serde_json::Value> {
    if fs::metadata(path)?.len() > 64 * 1024 * 1024 {
        anyhow::bail!("adoption proof exceeds 64 MiB");
    }
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn validate_proofs(
    manifest: &AdoptionManifest,
    current: &serde_json::Value,
    historical: Option<&serde_json::Value>,
) -> Result<()> {
    if manifest.schema_version != 1
        || !valid_lix_id(&manifest.hosted_repository_id)
        || !valid_lix_id(&manifest.source_storage_id)
        || !valid_lix_id(&manifest.staged_storage_id)
        || manifest.source_storage_id == manifest.staged_storage_id
    {
        anyhow::bail!("invalid adoption manifest identities or version");
    }
    if current["semantic_preservation_verified"] != true
        || current["after"]["current"] != true
        || current["after"]["format"] != lix_sdk::CURRENT_STORAGE_FORMAT_VERSION
        || !matches!(
            current["after"]["role"].as_str(),
            Some("standalone" | "authority")
        )
        || current["after_content_digest"]
            .as_str()
            .is_none_or(|value| value.len() != 64)
    {
        anyhow::bail!("current migration proof is incomplete");
    }
    let source_objects = serde_json::to_value(&manifest.source_objects)?;
    if let Some(historical) = historical {
        if historical["fullSemanticPreservationVerified"] != true
            || historical["toVersion"] != current["before"]["format"]
            || historical["sourceObjects"] != source_objects
            || historical["verifiedDestinationObjects"] != current["sourceObjects"]
            || !matches!(historical["fromVersion"].as_u64(), Some(68 | 71))
            || historical["historicalRevision"] != "6e1efebb3953e8e4e5945fd9a9269a69803970c8"
            || historical["bridgePatchSha256"]
                != include_str!("../../../../tooling/historical-migration/bridge.patch.sha256")
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
        {
            anyhow::bail!(
                "historical bridge proof is incomplete or does not bind both physical hops"
            );
        }
    } else if current["sourceObjects"] != source_objects {
        anyhow::bail!("migration proof does not bind the original physical source");
    }
    Ok(())
}

impl LixRuntimeManager {
    /// All serving hosts must be stopped. This never opens or changes the source.
    pub async fn adopt_staged_repository_offline(
        self: Arc<Self>,
        manifest_path: &Path,
    ) -> Result<serde_json::Value> {
        let manager = Arc::try_unwrap(self)
            .map_err(|_| anyhow::anyhow!("adoption requires an offline manager"))?;
        if !manager.state.lock().await.entries.is_empty() {
            anyhow::bail!("close all repository runtimes before adoption");
        }
        let manifest: AdoptionManifest = serde_json::from_value(read_json(manifest_path)?)?;
        let parent = manifest_path.parent().unwrap_or_else(|| Path::new("."));
        let current = read_json(&parent.join(&manifest.current_proof_path))?;
        let historical = manifest
            .historical_proof_path
            .as_ref()
            .map(|path| read_json(&parent.join(path)))
            .transpose()?;
        validate_proofs(&manifest, &current, historical.as_ref())?;
        let manifest_digest = blake3::hash(&serde_json::to_vec(&serde_json::json!({
            "manifest":manifest,"currentProof":current,"historicalProof":historical
        }))?)
        .to_hex()
        .to_string();
        let (objects, prefix) = manager.catalog_store();
        let report_path = ObjectPath::from(format!(
            "{prefix}.lix-adoption-reports/{}/{}.json",
            manifest.hosted_repository_id, manifest.staged_storage_id
        ));
        if let Some(existing) = manager
            .repository_record(&manifest.hosted_repository_id)
            .await?
        {
            if existing.state != "live"
                || existing.storage_id != manifest.staged_storage_id
                || !existing.retired.contains(&manifest.source_storage_id)
                || existing.admission != Some(AuthorityAdmission::current())
            {
                anyhow::bail!(
                    "hosted catalog already exists with a conflicting or deleted identity"
                );
            }
            let mut report: serde_json::Value =
                serde_json::from_slice(&objects.get(&report_path).await?.bytes().await?)?;
            if report["manifestDigest"] != manifest_digest || report["prepared"] != true {
                anyhow::bail!("existing admission lacks the exact prepared adoption proof");
            }
            report["admissionPublished"] = true.into();
            objects
                .put(&report_path, serde_json::to_vec(&report)?.into())
                .await?;
            return Ok(report);
        }
        manager.reject_adoption_aliases(&manifest).await?;
        verify_physical_manifest(
            &objects,
            &prefix,
            &manifest.source_storage_id,
            &manifest.source_objects,
        )
        .await?;
        let stage = lix_sdk::storage::StorageSession::acquire(
            manager.open_storage(&manifest.staged_storage_id, SlateDBIoCounters::default())?,
        )
        .await?;
        let observed = lix_sdk::migration::migrate_repository(stage.clone()).await?;
        let prior = match objects.get(&report_path).await {
            Ok(value) => Some(serde_json::from_slice::<serde_json::Value>(
                &value.bytes().await?,
            )?),
            Err(object_store::Error::NotFound { .. }) => None,
            Err(error) => return Err(error.into()),
        };
        let activation;
        if let Some(prior) = prior.as_ref().filter(|prior| prior["prepared"] == true) {
            if prior["manifestDigest"] != manifest_digest
                || prior["authorityContentDigest"] != observed.before_content_digest
                || observed.after.role != lix_sdk::migration::RepositoryRole::Authority
            {
                anyhow::bail!("prepared adoption stage changed before publication");
            }
            activation = prior["activationProof"].clone();
        } else {
            if current["after_content_digest"] != observed.before_content_digest
                || !observed.semantic_preservation_verified
                || !observed.after.current
            {
                anyhow::bail!("staged repository differs from the verified migration destination");
            }
            lix_sdk::server_protocol::prepare_existing_repository(&stage).await?;
            if observed.after.role == lix_sdk::migration::RepositoryRole::Standalone {
                let witness =
                    lix_sdk::migration::prepare_authority_activation(stage.clone()).await?;
                let server = lix_sdk::open_lix()
                    .with_storage(stage.clone())
                    .serve()
                    .with_lix_id(&manifest.hosted_repository_id)
                    .await?;
                server.close().await?;
                drop(server);
                let proof =
                    lix_sdk::migration::verify_authority_activation(stage.clone(), witness).await?;
                if !proof.semantic_preservation_verified {
                    anyhow::bail!("authority activation proof failed");
                }
                activation = serde_json::to_value(proof)?;
            } else if observed.after.role == lix_sdk::migration::RepositoryRole::Authority {
                let server = lix_sdk::open_lix()
                    .with_storage(stage.clone())
                    .serve()
                    .with_lix_id(&manifest.hosted_repository_id)
                    .await?;
                server.close().await?;
                drop(server);
                let verified = lix_sdk::migration::migrate_repository(stage.clone()).await?;
                if !verified.semantic_preservation_verified
                    || verified.after.role != lix_sdk::migration::RepositoryRole::Authority
                    || verified.after_content_digest != observed.after_content_digest
                {
                    anyhow::bail!("existing authority changed during serving admission");
                }
                activation = serde_json::json!({"semantic_preservation_verified":true,
                    "before_content_digest":observed.before_content_digest,
                    "after_content_digest":verified.after_content_digest});
            } else {
                anyhow::bail!("adoption only accepts complete standalone or authority storage");
            }
        }
        drop(stage);
        let mut report = serde_json::json!({
            "schemaVersion":1,"manifestDigest":manifest_digest,"prepared":true,
            "admissionPublished":false,"hostedRepositoryId":manifest.hosted_repository_id,
            "sourceStorageId":manifest.source_storage_id,"stagedStorageId":manifest.staged_storage_id,
            "authorityContentDigest":activation["after_content_digest"],"activationProof":activation,
            "migrationProof":current,"historicalProof":historical,"sourceObjects":manifest.source_objects,
        });
        objects
            .put(&report_path, serde_json::to_vec(&report)?.into())
            .await?;
        manager.publish_adoption(&manifest).await?;
        report["admissionPublished"] = true.into();
        objects
            .put(&report_path, serde_json::to_vec(&report)?.into())
            .await?;
        Ok(report)
    }

    async fn reject_adoption_aliases(&self, manifest: &AdoptionManifest) -> Result<()> {
        let (objects, prefix) = self.catalog_store();
        let path = ObjectPath::from(format!("{prefix}.lix-repositories/"));
        let mut records = objects.list(Some(&path));
        while let Some(object) = records.try_next().await? {
            let record: RepositoryRecord =
                serde_json::from_slice(&objects.get(&object.location).await?.bytes().await?)?;
            if [
                manifest.source_storage_id.as_str(),
                manifest.staged_storage_id.as_str(),
            ]
            .iter()
            .any(|id| {
                record.storage_id == *id || record.retired.iter().any(|retired| retired == id)
            }) {
                anyhow::bail!("adoption source or stage is already owned by a catalog identity");
            }
        }
        Ok(())
    }

    async fn publish_adoption(&self, manifest: &AdoptionManifest) -> Result<()> {
        self.reject_adoption_aliases(manifest).await?;
        let (objects, prefix) = self.catalog_store();
        verify_physical_manifest(
            &objects,
            &prefix,
            &manifest.source_storage_id,
            &manifest.source_objects,
        )
        .await?;
        let record = RepositoryRecord {
            state: "live".to_owned(),
            fingerprint: None,
            storage_id: manifest.staged_storage_id.clone(),
            retired: vec![manifest.source_storage_id.clone()],
            admission: Some(AuthorityAdmission::current()),
        };
        objects
            .put_opts(
                &ObjectPath::from(format!(
                    "{prefix}.lix-repositories/{}.json",
                    manifest.hosted_repository_id
                )),
                serde_json::to_vec(&record)?.into(),
                object_store::PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .context("catalog appeared during adoption; existing identity was not overwritten")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn fixture() -> (Arc<LixRuntimeManager>, AdoptionManifest) {
        let manager = LixRuntimeManager::new_in_memory(4);
        let manifest = AdoptionManifest {
            schema_version: 1,
            hosted_repository_id: uuid::Uuid::new_v4().to_string(),
            source_storage_id: uuid::Uuid::new_v4().to_string(),
            staged_storage_id: uuid::Uuid::new_v4().to_string(),
            source_objects: vec![PhysicalManifestEntry {
                key: "original".into(),
                bytes: 3,
                blake3: blake3::hash(b"old").to_hex().to_string(),
            }],
            current_proof_path: "current.json".into(),
            historical_proof_path: None,
        };
        let (objects, prefix) = manager.catalog_store();
        objects
            .put(
                &ObjectPath::from(format!("{prefix}{}/original", manifest.source_storage_id)),
                b"old".to_vec().into(),
            )
            .await
            .unwrap();
        (manager, manifest)
    }

    #[tokio::test]
    async fn adoption_publication_retains_source_and_refuses_duplicate() {
        let (manager, manifest) = fixture().await;
        manager.publish_adoption(&manifest).await.unwrap();
        let record = manager
            .repository_record(&manifest.hosted_repository_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.storage_id, manifest.staged_storage_id);
        assert_eq!(record.retired, vec![manifest.source_storage_id.clone()]);
        assert!(manager.publish_adoption(&manifest).await.is_err());
        let (objects, prefix) = manager.catalog_store();
        verify_physical_manifest(
            &objects,
            &prefix,
            &manifest.source_storage_id,
            &manifest.source_objects,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn adoption_publication_refuses_changed_source() {
        let (manager, manifest) = fixture().await;
        let (objects, prefix) = manager.catalog_store();
        objects
            .put(
                &ObjectPath::from(format!("{prefix}{}/original", manifest.source_storage_id)),
                b"new".to_vec().into(),
            )
            .await
            .unwrap();
        assert!(manager.publish_adoption(&manifest).await.is_err());
        assert!(
            manager
                .repository_record(&manifest.hosted_repository_id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn adoption_catalog_create_never_overwrites_live_or_deleted_identity() {
        for state in ["live", "deleted"] {
            let (manager, manifest) = fixture().await;
            manager
                .write_record(&manifest.hosted_repository_id, state, None, true)
                .await
                .unwrap();
            assert!(manager.publish_adoption(&manifest).await.is_err());
            let record = manager
                .repository_record(&manifest.hosted_repository_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(record.state, state);
            assert_eq!(record.storage_id, manifest.hosted_repository_id);
        }
    }

    #[tokio::test]
    async fn adoption_refuses_physical_source_owned_by_another_catalog() {
        let (manager, manifest) = fixture().await;
        manager
            .write_record(&manifest.source_storage_id, "live", None, true)
            .await
            .unwrap();
        assert!(manager.publish_adoption(&manifest).await.is_err());
        assert!(
            manager
                .repository_record(&manifest.hosted_repository_id)
                .await
                .unwrap()
                .is_none()
        );
    }
    #[tokio::test]
    async fn adoption_activates_verified_stage_and_recovers_lost_publication_ack() {
        let (manager, manifest) = fixture().await;
        let stage = manager
            .open_storage(&manifest.staged_storage_id, SlateDBIoCounters::default())
            .unwrap();
        let lix = lix_sdk::open_lix()
            .with_storage(stage.clone())
            .await
            .unwrap();
        lix.execute(
            "INSERT INTO lix_key_value(key,value) VALUES ('adoption-proof','retained')",
            &[],
        )
        .await
        .unwrap();
        lix.close().await.unwrap();
        drop(lix);
        let proof = lix_sdk::migration::migrate_repository(stage.clone())
            .await
            .unwrap();
        assert!(proof.semantic_preservation_verified);
        drop(stage);
        let mut proof = serde_json::to_value(proof).unwrap();
        proof["sourceObjects"] = serde_json::to_value(&manifest.source_objects).unwrap();
        let directory = tempfile::tempdir().unwrap();
        fs::write(
            directory.path().join("current.json"),
            serde_json::to_vec(&proof).unwrap(),
        )
        .unwrap();
        let path = directory.path().join("manifest.json");
        fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        let (objects, _) = manager.catalog_store();
        let result = manager
            .adopt_staged_repository_offline(&path)
            .await
            .unwrap();
        assert_eq!(result["admissionPublished"], true);
        assert_eq!(
            result["activationProof"]["semantic_preservation_verified"],
            true
        );
        let mut restarted = LixRuntimeManager::new_in_memory(4);
        Arc::get_mut(&mut restarted).unwrap().backend = StorageBackend::Memory {
            object_store: objects.clone(),
        };
        let stage = restarted
            .open_storage(&manifest.staged_storage_id, SlateDBIoCounters::default())
            .unwrap();
        let lix = lix_sdk::open_lix().with_storage(stage).await.unwrap();
        let rows = lix
            .execute(
                "SELECT value FROM lix_key_value WHERE key = 'adoption-proof'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.rows().len(), 1);
        lix.close().await.unwrap();
        drop(lix);
        let report_path = ObjectPath::from(format!(
            ".lix-adoption-reports/{}/{}.json",
            manifest.hosted_repository_id, manifest.staged_storage_id
        ));
        let mut pending = result.clone();
        pending["admissionPublished"] = false.into();
        objects
            .put(&report_path, serde_json::to_vec(&pending).unwrap().into())
            .await
            .unwrap();
        let retried = restarted
            .adopt_staged_repository_offline(&path)
            .await
            .unwrap();
        assert_eq!(retried["manifestDigest"], result["manifestDigest"]);
        assert_eq!(retried["admissionPublished"], true);
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
        assert_eq!(durable["admissionPublished"], true);
    }
    #[tokio::test]
    async fn adoption_proof_chain_requires_exact_source_and_patched_tool() {
        let (_, manifest) = fixture().await;
        let objects = serde_json::to_value(&manifest.source_objects).unwrap();
        let current = serde_json::json!({
            "semantic_preservation_verified":true,
            "before":{"format":74},
            "after":{"current":true,"format":lix_sdk::CURRENT_STORAGE_FORMAT_VERSION,"role":"standalone"},
            "after_content_digest":"a".repeat(64), "sourceObjects":objects
        });
        assert!(validate_proofs(&manifest, &current, None).is_ok());
        let mut historical = serde_json::json!({
            "fullSemanticPreservationVerified":true,"toVersion":74,"fromVersion":68,
            "historicalRevision":"6e1efebb3953e8e4e5945fd9a9269a69803970c8",
            "sourceObjects":objects,"verifiedDestinationObjects":objects,
            "bridgePatchSha256":include_str!("../../../../tooling/historical-migration/bridge.patch.sha256").split_whitespace().next().unwrap()
        });
        assert!(validate_proofs(&manifest, &current, Some(&historical)).is_ok());
        historical["bridgePatchSha256"] = "unreviewed-bridge".into();
        assert!(validate_proofs(&manifest, &current, Some(&historical)).is_err());
        let mut changed = current;
        changed["sourceObjects"][0]["blake3"] = "changed".into();
        assert!(validate_proofs(&manifest, &changed, None).is_err());
    }
}
