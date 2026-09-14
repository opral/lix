//! Exhaustive catalog/storage reconciliation. Listing never opens repository runtimes.
use super::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthorityInventory {
    pub target_protocol_epoch: u32,
    pub target_storage_epoch: u32,
    pub entries: Vec<AuthorityInventoryEntry>,
    pub unreferenced_storage: Vec<String>,
    /// Complete physical prefix inventory, including retained and staged sources.
    pub physical_storage: Vec<String>,
    /// Unpublished or retained migration destinations; never ordinary authorities.
    pub staged_storage: Vec<String>,
    /// Explicitly retained nonlive sources, verified against their complete manifest.
    pub quarantined_storage: Vec<String>,
    pub malformed_catalog: Vec<String>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthorityInventoryEntry {
    pub repository_id: String,
    pub state: String,
    pub storage_id: String,
    pub storage_present: bool,
    pub object_count: u64,
    pub object_bytes: u64,
    pub protocol_epoch: Option<u32>,
    pub storage_epoch: Option<u32>,
    pub requires_migration: bool,
    /// Catalog metadata is not an integrity check. Only the explicit verifier
    /// can produce a semantic preservation witness.
    pub integrity: &'static str,
}

impl LixRuntimeManager {
    pub async fn inventory_authorities(&self) -> Result<AuthorityInventory> {
        let (objects, prefix) = self.catalog_store();
        let base = (!prefix.is_empty()).then(|| ObjectPath::from(prefix.trim_end_matches('/')));
        let mut listed = objects.list(base.as_ref());
        let mut catalog = Vec::new();
        let mut stages = Vec::new();
        let mut quarantine = Vec::new();
        let mut physical: BTreeMap<String, (u64, u64)> = BTreeMap::new();
        while let Some(object) = listed.try_next().await? {
            let Some(relative) = object.location.as_ref().strip_prefix(&prefix) else {
                continue;
            };
            if let Some(name) = relative.strip_prefix(".lix-repositories/") {
                catalog.push((name.to_owned(), object.location));
            } else if let Some(name) = relative.strip_prefix(".lix-migration-stages/") {
                stages.push((name.to_owned(), object.location));
            } else if let Some(name) = relative.strip_prefix(".lix-quarantine/") {
                quarantine.push((name.to_owned(), object.location));
            } else if let Some((storage_id, _)) = relative.split_once('/') {
                if !storage_id.starts_with('.') {
                    let counts = physical.entry(storage_id.to_owned()).or_default();
                    counts.0 += 1;
                    counts.1 = counts.1.saturating_add(object.size);
                }
            }
        }
        let mut entries = Vec::new();
        let mut referenced = BTreeSet::new();
        let mut live_storage = BTreeSet::new();
        let mut repository_sources: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let mut malformed_catalog = Vec::new();
        for (name, path) in catalog {
            let Some(id) = name.strip_suffix(".json").filter(|id| valid_lix_id(id)) else {
                malformed_catalog.push(name);
                continue;
            };
            let record = objects.get(&path).await?.bytes().await?;
            let Ok(record) = serde_json::from_slice::<RepositoryRecord>(&record) else {
                malformed_catalog.push(name);
                continue;
            };
            if !matches!(record.state.as_str(), "live" | "deleted" | "creating")
                || !valid_lix_id(&record.storage_id)
                || record.retired.iter().any(|id| !valid_lix_id(id))
            {
                malformed_catalog.push(name);
                continue;
            }
            if record.state == "live" && !live_storage.insert(record.storage_id.clone()) {
                malformed_catalog.push(name);
                continue;
            }
            repository_sources.insert(
                id.to_owned(),
                std::iter::once(record.storage_id.clone())
                    .chain(record.retired.iter().cloned())
                    .collect(),
            );
            referenced.insert(record.storage_id.clone());
            referenced.extend(record.retired.iter().cloned());
            let (object_count, object_bytes) = physical
                .get(&record.storage_id)
                .copied()
                .unwrap_or_default();
            let current = record.admission.as_ref() == Some(&AuthorityAdmission::current());
            entries.push(AuthorityInventoryEntry {
                repository_id: id.to_owned(),
                requires_migration: record.state == "live" && !current,
                state: record.state,
                storage_id: record.storage_id,
                storage_present: object_count > 0,
                object_count,
                object_bytes,
                protocol_epoch: record
                    .admission
                    .as_ref()
                    .map(|admission| admission.protocol_epoch),
                storage_epoch: record.admission.map(|admission| admission.storage_epoch),
                integrity: "not-verified",
            });
        }
        let mut staged_storage = BTreeSet::new();
        for (name, path) in stages {
            let bytes = objects.get(&path).await?.bytes().await?;
            let stage = serde_json::from_slice::<serde_json::Value>(&bytes).unwrap_or_default();
            let repository = stage["repositoryId"].as_str().unwrap_or_default();
            let source = stage["sourceStorageId"].as_str().unwrap_or_default();
            let destination = stage["destinationStorageId"].as_str().unwrap_or_default();
            let backup = stage["backupPrefix"].as_str().unwrap_or_default();
            if !valid_lix_id(repository)
                || !valid_lix_id(source)
                || !valid_lix_id(destination)
                || destination == source
                || entries.iter().any(|entry| {
                    entry.repository_id != repository && entry.storage_id == destination
                })
                || name != format!("{repository}/{destination}.json")
                || !entries
                    .iter()
                    .any(|entry| entry.repository_id == repository)
                || !repository_sources
                    .get(repository)
                    .is_some_and(|sources| sources.contains(source))
                || !backup.starts_with(&format!("{prefix}.lix-migration-backups/{repository}/"))
            {
                malformed_catalog.push(format!("migration-stage/{name}"));
                continue;
            }
            // Known interrupted targets remain preserved, but do not prevent a
            // fresh retry. A migration revalidates its newly copied destination.
            referenced.insert(destination.to_owned());
            staged_storage.insert(destination.to_owned());
        }
        let mut quarantined_storage = Vec::new();
        for (name, path) in quarantine {
            let bytes = objects.get(&path).await?.bytes().await?;
            let record = serde_json::from_slice::<QuarantineManifest>(&bytes);
            let valid = match record {
                Ok(record)
                    if name == format!("{}.json", record.storage_id)
                        && !referenced.contains(&record.storage_id)
                        && !record.reason.trim().is_empty()
                        && valid_digest(&record.control_plane_digest) =>
                {
                    match verify_physical_manifest(
                        &objects,
                        &prefix,
                        &record.storage_id,
                        &record.objects,
                    )
                    .await
                    {
                        Ok(()) => {
                            quarantined_storage.push(record.storage_id.clone());
                            referenced.insert(record.storage_id);
                            true
                        }
                        Err(_) => false,
                    }
                }
                _ => false,
            };
            if !valid {
                malformed_catalog.push(format!("quarantine/{name}"));
            }
        }
        quarantined_storage.sort();
        entries.sort_by(|a, b| a.repository_id.cmp(&b.repository_id));
        malformed_catalog.sort();
        Ok(AuthorityInventory {
            target_protocol_epoch: lix_sdk::SYNC_PROTOCOL_VERSION,
            target_storage_epoch: lix_sdk::CURRENT_STORAGE_FORMAT_VERSION,
            entries,
            physical_storage: physical.keys().cloned().collect(),
            unreferenced_storage: physical
                .into_keys()
                .filter(|id| !referenced.contains(id))
                .collect(),
            malformed_catalog,
            staged_storage: staged_storage.into_iter().collect(),
            quarantined_storage,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn inventory_accounts_for_inactive_missing_and_orphaned_storage_without_opening() {
        let manager = LixRuntimeManager::new_in_memory(1);
        let live = "11111111-1111-4111-8111-111111111111";
        let deleted = "22222222-2222-4222-8222-222222222222";
        manager
            .write_record(live, "live", None, true)
            .await
            .unwrap();
        manager
            .write_record(deleted, "deleted", None, true)
            .await
            .unwrap();
        let (store, _) = manager.catalog_store();
        store
            .put(&ObjectPath::from("orphan/data"), "orphan".into())
            .await
            .unwrap();
        store
            .put(
                &ObjectPath::from(".lix-repositories/broken.json"),
                "broken".into(),
            )
            .await
            .unwrap();
        let inventory = manager.inventory_authorities().await.unwrap();
        assert_eq!(inventory.entries.len(), 2);
        assert!(!inventory.entries[0].storage_present);
        assert_eq!(inventory.entries[1].state, "deleted");
        assert_eq!(inventory.unreferenced_storage, ["orphan"]);
        assert_eq!(inventory.malformed_catalog, ["broken.json"]);
        assert!(
            inventory
                .entries
                .iter()
                .all(|entry| entry.integrity == "not-verified")
        );
        assert!(manager.state.lock().await.entries.is_empty());
    }
    #[tokio::test]
    async fn interrupted_migration_targets_are_retained_and_identified_for_retry() {
        let manager = LixRuntimeManager::new_in_memory(1);
        let id = "11111111-1111-4111-8111-111111111111";
        let destination = "22222222-2222-4222-8222-222222222222";
        manager.write_record(id, "live", None, true).await.unwrap();
        let (objects, _) = manager.catalog_store();
        objects
            .put(
                &ObjectPath::from(format!("{destination}/partial-copy")),
                "retained".into(),
            )
            .await
            .unwrap();
        objects.put(&ObjectPath::from(format!(".lix-migration-stages/{id}/{destination}.json")), serde_json::to_vec(&serde_json::json!({"repositoryId":id,"sourceStorageId":id,"destinationStorageId":destination,"backupPrefix":format!(".lix-migration-backups/{id}/attempt/")})).unwrap().into()).await.unwrap();
        let inventory = manager.inventory_authorities().await.unwrap();
        assert_eq!(inventory.staged_storage, [destination]);
        assert!(inventory.unreferenced_storage.is_empty());
        assert!(inventory.malformed_catalog.is_empty());
        objects
            .head(&ObjectPath::from(format!("{destination}/partial-copy")))
            .await
            .unwrap();
        assert_eq!(
            inventory.entries.len(),
            1,
            "a staged copy is never admitted as an authority"
        );
    }
    #[tokio::test]
    async fn live_catalog_aliases_fail_reconciliation() {
        let manager = LixRuntimeManager::new_in_memory(1);
        let first = "11111111-1111-4111-8111-111111111111";
        let second = "22222222-2222-4222-8222-222222222222";
        manager
            .write_record(first, "live", None, true)
            .await
            .unwrap();
        let (objects, _) = manager.catalog_store();
        objects
            .copy(
                &ObjectPath::from(format!(".lix-repositories/{first}.json")),
                &ObjectPath::from(format!(".lix-repositories/{second}.json")),
            )
            .await
            .unwrap();
        let inventory = manager.inventory_authorities().await.unwrap();
        assert_eq!(inventory.malformed_catalog.len(), 1);
    }
}

/// Exact immutable physical-source witness used by detached reconciliation tools.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PhysicalManifestEntry {
    pub key: String,
    pub bytes: u64,
    pub blake3: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct QuarantineManifest {
    pub storage_id: String,
    pub reason: String,
    pub control_plane_digest: String,
    pub objects: Vec<PhysicalManifestEntry>,
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(super) async fn verify_physical_manifest(
    objects: &Arc<dyn ObjectStore>,
    prefix: &str,
    storage_id: &str,
    entries: &[PhysicalManifestEntry],
) -> Result<()> {
    if !valid_lix_id(storage_id) || entries.is_empty() {
        anyhow::bail!("invalid or empty physical source manifest");
    }
    let source_prefix = format!("{prefix}{storage_id}/");
    let source_path = ObjectPath::from(source_prefix.clone());
    let listed: Vec<_> = objects.list(Some(&source_path)).try_collect().await?;
    let expected: BTreeMap<_, _> = entries
        .iter()
        .map(|entry| (entry.key.as_str(), entry))
        .collect();
    if expected.len() != entries.len() || listed.len() != entries.len() {
        anyhow::bail!("physical source object inventory changed");
    }
    for object in &listed {
        let key = object
            .location
            .as_ref()
            .strip_prefix(&source_prefix)
            .context("physical source escaped prefix")?;
        let entry = expected
            .get(key)
            .context("unexpected physical source object")?;
        if object.size != entry.bytes || !valid_digest(&entry.blake3) {
            anyhow::bail!("physical source object metadata changed");
        }
        let mut stream = objects.get(&object.location).await?.into_stream();
        let mut digest = Hasher::new();
        while let Some(bytes) = stream.try_next().await? {
            digest.update(&bytes);
        }
        if digest.finalize().to_hex().as_str() != entry.blake3 {
            anyhow::bail!("physical source content changed");
        }
    }
    let after: Vec<_> = objects.list(Some(&source_path)).try_collect().await?;
    let signature = |values: &[object_store::ObjectMeta]| {
        values
            .iter()
            .map(|object| {
                (
                    object.location.to_string(),
                    (
                        object.size,
                        object.e_tag.clone(),
                        object.version.clone(),
                        object.last_modified,
                    ),
                )
            })
            .collect::<BTreeMap<_, _>>()
    };
    if signature(&listed) != signature(&after) {
        anyhow::bail!("physical source changed during verification");
    }
    Ok(())
}

#[cfg(test)]
mod quarantine_tests {
    use super::*;
    const ID: &str = "11111111-1111-4111-8111-111111111111";
    #[tokio::test]
    async fn quarantined_source_is_retained_and_changed_content_blocks_inventory() {
        let manager = LixRuntimeManager::new_in_memory(1);
        let (objects, _) = manager.catalog_store();
        let path = ObjectPath::from(format!("{ID}/data"));
        objects.put(&path, "original".into()).await.unwrap();
        let manifest = QuarantineManifest {
            storage_id: ID.to_owned(),
            reason: "no live control mapping; preserve for recovery".into(),
            control_plane_digest: blake3::hash(b"control inventory").to_hex().to_string(),
            objects: vec![PhysicalManifestEntry {
                key: "data".into(),
                bytes: 8,
                blake3: blake3::hash(b"original").to_hex().to_string(),
            }],
        };
        objects
            .put(
                &ObjectPath::from(format!(".lix-quarantine/{ID}.json")),
                serde_json::to_vec(&manifest).unwrap().into(),
            )
            .await
            .unwrap();
        let inventory = manager.inventory_authorities().await.unwrap();
        assert_eq!(inventory.quarantined_storage, [ID]);
        assert_eq!(inventory.physical_storage, [ID]);
        assert!(inventory.unreferenced_storage.is_empty());
        assert!(inventory.entries.is_empty());
        assert_eq!(
            objects.get(&path).await.unwrap().bytes().await.unwrap(),
            "original"
        );
        objects.put(&path, "modified".into()).await.unwrap();
        let inventory = manager.inventory_authorities().await.unwrap();
        assert!(inventory.quarantined_storage.is_empty());
        assert_eq!(inventory.unreferenced_storage, [ID]);
        assert_eq!(
            inventory.malformed_catalog,
            [format!("quarantine/{ID}.json")]
        );
    }

    #[tokio::test]
    async fn quarantine_cannot_hide_a_catalogued_repository() {
        let manager = LixRuntimeManager::new_in_memory(1);
        manager.write_record(ID, "live", None, true).await.unwrap();
        let (objects, _) = manager.catalog_store();
        objects
            .put(&ObjectPath::from(format!("{ID}/data")), "original".into())
            .await
            .unwrap();
        let manifest = QuarantineManifest {
            storage_id: ID.to_owned(),
            reason: "invalid quarantine".into(),
            control_plane_digest: blake3::hash(b"control").to_hex().to_string(),
            objects: vec![PhysicalManifestEntry {
                key: "data".into(),
                bytes: 8,
                blake3: blake3::hash(b"original").to_hex().to_string(),
            }],
        };
        objects
            .put(
                &ObjectPath::from(format!(".lix-quarantine/{ID}.json")),
                serde_json::to_vec(&manifest).unwrap().into(),
            )
            .await
            .unwrap();
        let inventory = manager.inventory_authorities().await.unwrap();
        assert!(inventory.quarantined_storage.is_empty());
        assert_eq!(inventory.entries.len(), 1);
        assert_eq!(
            inventory.malformed_catalog,
            [format!("quarantine/{ID}.json")]
        );
    }
}
