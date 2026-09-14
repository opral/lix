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
    /// Unpublished or retained migration destinations; never ordinary authorities.
    pub staged_storage: Vec<String>,
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
        let mut physical: BTreeMap<String, (u64, u64)> = BTreeMap::new();
        while let Some(object) = listed.try_next().await? {
            let Some(relative) = object.location.as_ref().strip_prefix(&prefix) else {
                continue;
            };
            if let Some(name) = relative.strip_prefix(".lix-repositories/") {
                catalog.push((name.to_owned(), object.location));
            } else if let Some(name) = relative.strip_prefix(".lix-migration-stages/") {
                stages.push((name.to_owned(), object.location));
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
                || entries.iter().any(|entry| entry.repository_id != repository && entry.storage_id == destination)
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
        entries.sort_by(|a, b| a.repository_id.cmp(&b.repository_id));
        malformed_catalog.sort();
        Ok(AuthorityInventory {
            target_protocol_epoch: lix_sdk::SYNC_PROTOCOL_VERSION,
            target_storage_epoch: lix_sdk::CURRENT_STORAGE_FORMAT_VERSION,
            entries,
            unreferenced_storage: physical
                .into_keys()
                .filter(|id| !referenced.contains(id))
                .collect(),
            malformed_catalog,
            staged_storage: staged_storage.into_iter().collect(),
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
