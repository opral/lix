//! Automatic authority upgrades belong to the owned runtime opener. They use
//! Lix's retained epoch banks and storage fencing, never a live physical copy.
use super::*;

impl LixRuntimeManager {
    pub(super) async fn prepare_existing_authority(
        &self,
        id: &str,
        expected: &RepositoryRecord,
        storage: &SlateDB,
        opened: &watch::Sender<RuntimeOpenState>,
    ) -> Result<()> {
        let before = lix_sdk::migration::inspect_repository(storage.clone()).await?;
        if before.role != lix_sdk::migration::RepositoryRole::Authority {
            anyhow::bail!("catalogued storage is not an existing authority");
        }
        if !before.current {
            let from_version = before.format.context("authority format is missing")?;
            if from_version > lix_sdk::CURRENT_STORAGE_FORMAT_VERSION {
                anyhow::bail!("authority requires a newer storage format");
            }
            opened.send_replace(RuntimeOpenState::Migrating {
                from_version,
                to_version: lix_sdk::CURRENT_STORAGE_FORMAT_VERSION,
            });
            let migration =
                Box::pin(lix_sdk::migration::migrate_repository(storage.clone())).await?;
            if !migration.semantic_preservation_verified
                || !migration.after.current
                || migration.after.role != lix_sdk::migration::RepositoryRole::Authority
            {
                anyhow::bail!("authority migration did not verify preservation and identity");
            }
        }
        self.publish_open_admission(id, expected).await?;
        opened.send_replace(RuntimeOpenState::Opening);
        Ok(())
    }

    async fn publish_open_admission(&self, id: &str, expected: &RepositoryRecord) -> Result<()> {
        if expected.admission.as_ref() == Some(&AuthorityAdmission::current()) {
            return Ok(());
        }
        let (objects, prefix) = self.catalog_store();
        let path = ObjectPath::from(format!("{prefix}.lix-repositories/{id}.json"));
        let response = objects.get(&path).await?;
        let version = object_store::UpdateVersion {
            e_tag: response.meta.e_tag.clone(),
            version: response.meta.version.clone(),
        };
        let mut current: RepositoryRecord = serde_json::from_slice(&response.bytes().await?)?;
        // A runtime may never resurrect a deletion or publish admission for a
        // replaced physical store. Host identity remains the catalog mapping.
        if current.state != "live"
            || current.storage_id != expected.storage_id
            || current.fingerprint != expected.fingerprint
            || current.retired != expected.retired
        {
            anyhow::bail!("repository catalog changed while opening its authority");
        }
        if current.admission.as_ref() == Some(&AuthorityAdmission::current()) {
            return Ok(());
        }
        if current.admission != expected.admission {
            anyhow::bail!("repository admission changed while opening its authority");
        }
        current.admission = Some(AuthorityAdmission::current());
        objects
            .put_opts(
                &path,
                serde_json::to_vec(&current)?.into(),
                object_store::PutOptions {
                    mode: object_store::PutMode::Update(version),
                    ..Default::default()
                },
            )
            .await
            .context("repository catalog changed while publishing automatic admission")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use lix_sdk::storage::{
        GetManyRequest, Key, ProjectedValue, PutBatch, PutEntry, SpaceId, Storage, StorageRead,
        StorageSession, StorageSpace, StorageWrite, StoredValue,
    };

    const ID: &str = "01936f4e-7b6c-7c3d-8f9a-123456789abc";
    const EPOCH: StorageSpace = StorageSpace::mutable(SpaceId(0x0009_0001), "repository.epoch.v1");

    async fn point(
        storage: &StorageSession<SlateDB>,
        space: StorageSpace,
        key: &'static [u8],
    ) -> Bytes {
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut values = read
            .get_many(&[GetManyRequest {
                space,
                keys: &[Key(Bytes::from_static(key))],
                opts: Default::default(),
            }])
            .await
            .unwrap()
            .values;
        match values.pop().unwrap().unwrap() {
            ProjectedValue::FullValue(value) => value,
            _ => panic!("full fixture value required"),
        }
    }

    async fn stage_v80(storage: SlateDB) -> Bytes {
        let storage = StorageSession::acquire(storage).await.unwrap();
        let original = point(&storage, EPOCH, b"active").await;
        let mut parts: Vec<_> = std::str::from_utf8(&original)
            .unwrap()
            .split('|')
            .map(str::to_owned)
            .collect();
        assert_eq!(parts[1], "active");
        let prefix = match parts[2].as_str() {
            "a" => 0x4000_0000,
            "b" => 0x8000_0000,
            _ => panic!("fixture bank"),
        };
        parts[4] = "80".into();
        let pointer = Bytes::from(parts.join("|"));
        let mut write = storage.begin_write(Default::default()).await.unwrap();
        for (space, key, bytes) in [
            (EPOCH, b"active".as_slice(), pointer.clone()),
            (
                StorageSpace::mutable(SpaceId(prefix | 0x0004_0011), "repository.protocol.v1"),
                b"current".as_slice(),
                Bytes::from_static(b"tracked-default-branch.v80"),
            ),
        ] {
            write
                .put_many(
                    space,
                    PutBatch {
                        entries: vec![PutEntry {
                            key: Key(Bytes::copy_from_slice(key)),
                            value: StoredValue { bytes },
                        }],
                    },
                )
                .await
                .unwrap();
        }
        write.commit().await.unwrap();
        pointer
    }

    async fn seeded_authority(manager: &LixRuntimeManager) -> String {
        let physical = uuid::Uuid::new_v4().to_string();
        let storage = manager.open_storage(&physical, Default::default()).unwrap();
        let seed = lix_sdk::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        seed.execute(
            "INSERT INTO lix_key_value(key,value) VALUES ('auto-upgrade','preserved')",
            &[],
        )
        .await
        .unwrap();
        seed.close().await.unwrap();
        drop(seed);
        let authority = lix_sdk::open_lix()
            .with_storage(storage.clone())
            .serve()
            .with_lix_id(ID)
            .await
            .unwrap();
        authority.close().await.unwrap();
        drop(authority);
        drop(storage);
        physical
    }

    async fn catalog(
        manager: &LixRuntimeManager,
        physical: &str,
        admission: Option<AuthorityAdmission>,
    ) {
        let (objects, prefix) = manager.catalog_store();
        objects
            .put(
                &ObjectPath::from(format!("{prefix}.lix-repositories/{ID}.json")),
                serde_json::to_vec(&RepositoryRecord {
                    state: "live".into(),
                    storage_id: physical.into(),
                    fingerprint: Some("keep".into()),
                    retired: vec![],
                    admission,
                })
                .unwrap()
                .into(),
            )
            .await
            .unwrap();
    }

    async fn await_admission(manager: &Arc<LixRuntimeManager>) {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                match manager.authority_admission(ID, tokio::time::Instant::now() + Duration::from_secs(30)).await {
                    Ok(Some(admission)) => {
                        assert_eq!(admission, AuthorityAdmission::current());
                        break;
                    }
                    Err(error) if error.code == "LIX_REPOSITORY_MIGRATING" => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    other => panic!("unexpected admission: {other:?}"),
                }
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn automatic_open_migrates_v80_retains_source_and_catalog_identity() {
        let manager = LixRuntimeManager::new_in_memory(4);
        let physical = seeded_authority(&manager).await;
        let original =
            stage_v80(manager.open_storage(&physical, Default::default()).unwrap()).await;
        catalog(
            &manager,
            &physical,
            Some(AuthorityAdmission {
                storage_epoch: 80,
                protocol_epoch: lix_sdk::SYNC_PROTOCOL_VERSION - 1,
            }),
        )
        .await;
        let ((), ()) = tokio::join!(await_admission(&manager), await_admission(&manager));
        let record = manager.repository_record(ID).await.unwrap().unwrap();
        assert_eq!(record.storage_id, physical);
        assert_eq!(record.fingerprint.as_deref(), Some("keep"));
        assert_eq!(record.admission, Some(AuthorityAdmission::current()));
        manager.shutdown().await.unwrap();
        let storage =
            StorageSession::acquire(manager.open_storage(&physical, Default::default()).unwrap())
                .await
                .unwrap();
        assert_ne!(point(&storage, EPOCH, b"active").await, original);
        let source_marker = point(
            &storage,
            StorageSpace::mutable(SpaceId(0x4004_0011), "repository.protocol.v1"),
            b"current",
        )
        .await;
        assert_eq!(source_marker.as_ref(), b"tracked-default-branch.v80");
        let lix = lix_sdk::open_lix().with_storage(storage).await.unwrap();
        let result = lix
            .execute(
                "SELECT value FROM lix_key_value WHERE key='auto-upgrade'",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(
            result.rows()[0].get::<serde_json::Value>("value").unwrap(),
            serde_json::json!("preserved")
        );
        lix.close().await.unwrap();
    }

    #[tokio::test]
    async fn missing_catalog_admission_is_repaired_from_existing_current_authority() {
        let manager = LixRuntimeManager::new_in_memory(4);
        let physical = seeded_authority(&manager).await;
        catalog(&manager, &physical, None).await;
        await_admission(&manager).await;
        assert_eq!(
            manager
                .repository_record(ID)
                .await
                .unwrap()
                .unwrap()
                .admission,
            Some(AuthorityAdmission::current())
        );
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn transport_only_upgrade_is_catalog_only_even_without_physical_storage() {
        let manager = LixRuntimeManager::new_in_memory(4);
        catalog(
            &manager,
            ID,
            Some(AuthorityAdmission {
                storage_epoch: lix_sdk::CURRENT_STORAGE_FORMAT_VERSION,
                protocol_epoch: lix_sdk::SYNC_PROTOCOL_VERSION - 1,
            }),
        )
        .await;
        assert_eq!(
            manager.authority_admission(ID, tokio::time::Instant::now() + Duration::from_secs(30)).await.unwrap(),
            Some(AuthorityAdmission::current())
        );
        assert!(manager.state.lock().await.entries.is_empty());
        assert!(!manager.legacy_storage_present(ID).await.unwrap());
    }
    #[tokio::test]
    async fn admission_timeout_keeps_one_owned_opener_and_its_lifecycle_guard() {
        let mut manager = LixRuntimeManager::new_in_memory(4);
        let gate = TestOpenGate {
            started: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
            starts: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            fail_next: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };
        Arc::get_mut(&mut manager).unwrap().open_gate = Some(gate.clone());
        let physical = seeded_authority(&manager).await;
        catalog(&manager, &physical, None).await;
        use tower::ServiceExt as _;
        let app = crate::router(manager.clone(), None, Duration::from_secs(1), Default::default());
        let response = app.oneshot(axum::http::Request::builder()
            .uri(format!("/lix/v1/{ID}/admission"))
            .header("lix-sync-protocol-version", lix_sdk::SYNC_PROTOCOL_VERSION)
            .body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(response.into_body(), 16 * 1024).await.unwrap();
        let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(error["error"]["code"], "LIX_REPOSITORY_MIGRATING");
        let lifecycle = manager.lifecycle_lock(ID).await;
        assert!(
            lifecycle.try_write().is_err(),
            "opener retains its lifecycle guard after timeout"
        );
        let writer = tokio::spawn(async move {
            let _guard = lifecycle.write().await;
        });
        gate.release.notify_one();
        tokio::time::timeout(Duration::from_secs(10), writer)
            .await
            .unwrap()
            .unwrap();
        await_admission(&manager).await;
        assert_eq!(gate.starts.load(std::sync::atomic::Ordering::SeqCst), 1);
        manager.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn future_catalog_version_is_rejected_without_opening_storage() {
        let manager = LixRuntimeManager::new_in_memory(4);
        catalog(
            &manager,
            ID,
            Some(AuthorityAdmission {
                storage_epoch: lix_sdk::CURRENT_STORAGE_FORMAT_VERSION + 1,
                protocol_epoch: lix_sdk::SYNC_PROTOCOL_VERSION,
            }),
        )
        .await;
        assert_eq!(
            manager.authority_admission(ID, tokio::time::Instant::now() + Duration::from_secs(30)).await.unwrap_err().code,
            "LIX_PROTOCOL_VERSION_MISMATCH"
        );
        assert!(manager.state.lock().await.entries.is_empty());
        assert!(!manager.legacy_storage_present(ID).await.unwrap());
    }
    #[tokio::test]
    async fn admission_publication_cannot_replace_a_changed_catalog_mapping() {
        let manager = LixRuntimeManager::new_in_memory(4);
        catalog(&manager, ID, None).await;
        let expected = manager.repository_record(ID).await.unwrap().unwrap();
        let replacement = uuid::Uuid::new_v4().to_string();
        catalog(&manager, &replacement, None).await;
        assert!(manager.publish_open_admission(ID, &expected).await.is_err());
        let current = manager.repository_record(ID).await.unwrap().unwrap();
        assert_eq!(current.storage_id, replacement);
        assert_eq!(current.admission, None);
    }
}
