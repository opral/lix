//! Checkpoint incorporation keeps proof metadata, not historical row payloads.
use super::*;

pub(super) struct ProofRetention {
    pub(super) graph_commits: BTreeSet<CommitId>,
    pub(super) commits: BTreeSet<CommitId>,
    pub(super) mutation_nodes: BTreeSet<[u8; 32]>,
    pub(super) scoped_nodes: BTreeSet<[u8; 32]>,
}

pub(super) async fn retain(
    store: &(impl StorageAdapterRead + Clone + Send + Sync),
    manifests: &BTreeMap<CommitId, crate::tracked_state::CommitStateManifest>,
    standard: &BTreeSet<CommitId>,
) -> Result<ProofRetention, LixError> {
    let mut pending = Vec::new();
    for manifest in manifests.values() {
        if let crate::tracked_state::CommitStateIncorporation::Complete(source) =
            manifest.incorporation
        {
            pending.push((source, false));
        }
        let native = manifest
            .snapshot_root
            .as_ref()
            .filter(|root| root.complete_state_fence)
            .and_then(|root| root.parent_roots.first())
            .map(|parent| parent.commit_id);
        if let Some(source) =
            crate::sync::load_complete_state_alias_source(store, manifest.commit_id, native).await?
        {
            pending.push((
                source,
                native.is_none()
                    || manifest.incorporation
                        == crate::tracked_state::CommitStateIncorporation::LegacyUnknown,
            ));
        }
    }
    let mut commits = BTreeSet::new();
    let mut visited = BTreeSet::new();
    let mut scoped_roots = Vec::new();
    let mut graph = CommitGraphContext::new().reader(store);
    while let Some((id, legacy)) = pending.pop() {
        if !visited.insert((id, legacy)) {
            continue;
        }
        if !standard.contains(&id) {
            commits.insert(id);
        }
        let node = graph.load_node(&id).await?;
        if node.is_none()
            && !legacy
            && !crate::tracked_state::commit_history_is_omitted(store, id).await?
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "checkpoint incorporation graph proof is absent",
            ));
        }
        let Some(topology) =
            crate::tracked_state::load_published_commit_state_topology(store, id).await?
        else {
            if legacy || crate::tracked_state::commit_history_is_deferred(store, id).await? {
                pending.extend(
                    node.into_iter()
                        .flat_map(|node| node.parent_commit_ids)
                        .map(|id| (id, legacy)),
                );
                if let Some(source) =
                    crate::sync::load_complete_state_alias_source(store, id, None).await?
                {
                    pending.push((source, true));
                }
                continue;
            }
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "checkpoint incorporation topology proof is absent",
            ));
        };
        // Legacy provenance may predate a sweep that already removed headers.
        // Preserve what remains; new explicit sources still require a header.
        let legacy = legacy
            || topology.incorporation()
                == crate::tracked_state::CommitStateIncorporation::LegacyUnknown;
        pending.extend(
            node.into_iter()
                .flat_map(|node| node.parent_commit_ids)
                .map(|id| (id, legacy)),
        );
        if let crate::tracked_state::CommitStateIncorporation::Complete(source) =
            topology.incorporation()
        {
            pending.push((source, false));
        }
        let native = topology.complete_state_source_commit_id();
        if let Some(source) =
            crate::sync::load_complete_state_alias_source(store, id, native).await?
        {
            // Materialized snapshots may never hydrate the detached source.
            // Retain resident proof metadata without demanding absent history.
            pending.push((source, legacy || native.is_none()));
        }
        if let Some(root) = topology.current_state_scoped_ranges() {
            scoped_roots.push(root.tree.clone());
        }
    }
    let mut mutation_nodes = BTreeSet::new();
    let roots = crate::tracked_state::load_commit_mutation_directory_roots(
        store,
        &commits.iter().copied().collect::<Vec<_>>(),
    )
    .await?;
    for root in roots.into_iter().flatten() {
        mutation_nodes
            .extend(crate::tracked_state::collect_mutation_directory_node_ids(store, &root).await?);
    }
    let scoped_nodes = if scoped_roots.is_empty() {
        BTreeSet::new()
    } else {
        crate::tracked_state::validate_scoped_range_trees(store, &scoped_roots)
            .await?
            .node_ids
    };
    Ok(ProofRetention {
        graph_commits: visited.into_iter().map(|(id, _)| id).collect(),
        commits,
        mutation_nodes,
        scoped_nodes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn complete_proof_missing_graph_requires_an_omission_not_only_a_header_marker() {
        let lix = crate::open_lix().await.unwrap();
        let head = lix
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();
        let head = CommitId::parse_lix(&head, "retained head").unwrap();
        let source = CommitId::for_test_label("intentionally-omitted-proof");
        let adapter = lix.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let mut manifest = crate::tracked_state::load_commit_state_manifest(&read, head)
            .await
            .unwrap()
            .unwrap();
        // Isolate source-edge retention from root payload ownership without
        // inventing or publishing any semantic commit record.
        manifest.incorporation = crate::tracked_state::CommitStateIncorporation::Complete(source);
        manifest.snapshot_root = None;
        let manifests = BTreeMap::from([(head, manifest)]);
        drop(read);
        for state in 0..3 {
            if state != 0 {
                let mut writes = adapter.new_write_set();
                if state == 1 {
                    crate::tracked_state::stage_commit_history_deferred_with_scope(
                        &mut writes,
                        source,
                        false,
                    );
                } else {
                    crate::tracked_state::stage_commit_history_omitted(
                        &mut writes,
                        source,
                        Some(false),
                    );
                }
                adapter
                    .commit_write_set(writes, Default::default())
                    .await
                    .unwrap();
            }
            let read = adapter.begin_read(Default::default()).await.unwrap();
            let result = retain(&&read, &manifests, &BTreeSet::new()).await;
            if state == 2 {
                result.unwrap();
            } else {
                assert_eq!(
                    result
                        .err()
                        .expect("missing graph must fail without an omission")
                        .message,
                    "checkpoint incorporation graph proof is absent"
                );
            }
        }
    }

    #[tokio::test]
    async fn complete_proof_deferred_header_does_not_excuse_unmarked_parent() {
        let lix = crate::open_lix().await.unwrap();
        let mut ids = Vec::new();
        for value in ["parent", "source"] {
            lix.execute("INSERT INTO lix_key_value(key,value) VALUES('proof-residency',$1) ON CONFLICT(key) DO UPDATE SET value=excluded.value", &[crate::Value::Text(value.into())]).await.unwrap();
            let head = lix
                .execute("SELECT lix_active_branch_commit_id() AS id", &[])
                .await
                .unwrap()
                .rows()[0]
                .get::<String>("id")
                .unwrap();
            ids.push(CommitId::parse_lix(&head, "proof source chain").unwrap());
        }
        let checkpoint = lix.execute("SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_key_value')))", &[]).await.unwrap().rows()[0].get::<String>("commit_id").unwrap();
        let checkpoint = CommitId::parse_lix(&checkpoint, "complete checkpoint").unwrap();
        let adapter = lix.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let manifest = crate::tracked_state::load_commit_state_manifest(&read, checkpoint)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            manifest.incorporation,
            crate::tracked_state::CommitStateIncorporation::Complete(ids[1])
        );
        let manifests = BTreeMap::from([(checkpoint, manifest)]);
        let mut writes = adapter.new_write_set();
        for id in &ids {
            let manifest = crate::tracked_state::load_commit_state_manifest(&read, *id)
                .await
                .unwrap()
                .unwrap();
            crate::tracked_state::stage_delete_commit_state_manifest_for_gc(
                &read,
                &mut writes,
                *id,
                &manifest,
            )
            .await
            .unwrap();
        }
        crate::tracked_state::stage_commit_history_deferred_with_scope(&mut writes, ids[1], false);
        drop(read);
        adapter
            .commit_write_set(writes, Default::default())
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let error = retain(&&read, &manifests, &BTreeSet::new())
            .await
            .err()
            .expect("source permission must not propagate to an unmarked parent");
        assert_eq!(
            error.message,
            "checkpoint incorporation topology proof is absent"
        );
        drop(read);
        let mut writes = adapter.new_write_set();
        crate::tracked_state::stage_commit_history_deferred_with_scope(&mut writes, ids[0], false);
        adapter
            .commit_write_set(writes, Default::default())
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        retain(&&read, &manifests, &BTreeSet::new()).await.unwrap();
    }

    async fn blob_chunks(read: &impl StorageAdapterRead) -> BTreeSet<StorageKey> {
        let mut cursor = read
            .begin_scan(
                crate::binary_cas::BINARY_CAS_CHUNK_SPACE,
                StoragePrefix {
                    bytes: Bytes::new(),
                }
                .to_range()
                .unwrap(),
                Default::default(),
            )
            .await
            .unwrap();
        cursor
            .collect_all()
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.key)
            .collect()
    }

    #[tokio::test]
    async fn checkpoint_proof_survives_payload_retirement_and_second_gc() {
        let lix = crate::open_lix().await.unwrap();
        let adapter = lix.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let original_chunks = blob_chunks(&read).await;
        drop(read);
        lix.execute(
            "INSERT INTO lix_file(path,content) VALUES('/retired-proof.bin',$1)",
            &[crate::Value::Blob(vec![71; 96 * 1024].into())],
        )
        .await
        .unwrap();
        lix.execute(
            "INSERT INTO lix_key_value(key,value) VALUES('retired-proof','old')",
            &[],
        )
        .await
        .unwrap();
        let old = lix
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("id")
            .unwrap();
        let old = CommitId::parse_lix(&old, "old proof").unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let old_chunks = blob_chunks(&read)
            .await
            .difference(&original_chunks)
            .cloned()
            .collect::<BTreeSet<_>>();
        assert!(!old_chunks.is_empty());
        let inventory = crate::tracked_state::scan_commit_delta_inventory(&read)
            .await
            .unwrap();
        let changes = inventory.commits[&old]
            .members
            .iter()
            .map(|member| member.change.change_id)
            .collect::<Vec<_>>();
        assert!(!changes.is_empty());
        drop(read);
        for (index, value) in ["new", "latest"].into_iter().enumerate() {
            lix.execute(
                &format!("UPDATE lix_key_value SET value='{value}' WHERE key='retired-proof'"),
                &[],
            )
            .await
            .unwrap();
            lix.execute(
                "UPDATE lix_file SET content=$1 WHERE path='/retired-proof.bin'",
                &[crate::Value::Blob(vec![81 + index as u8; 96 * 1024].into())],
            )
            .await
            .unwrap();
            lix.execute("SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_key_value') UNION ALL SELECT row_ref FROM lix_diff('lix_file')))", &[]).await.unwrap();
        }
        for pass in 0..2 {
            let read = adapter.begin_read(Default::default()).await.unwrap();
            let mut writes = adapter.new_write_set();
            let mut preconditions = Vec::new();
            let plan =
                stage_repository_gc_with_preconditions(&read, &mut writes, &mut preconditions)
                    .await
                    .unwrap();
            assert!(
                !plan.sweep.has_more,
                "small fixture GC did not finish on pass {pass}"
            );
            drop(read);
            adapter
                .commit_write_set(
                    writes,
                    crate::storage_adapter::StorageWriteOptions {
                        preconditions,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }
        let read = adapter.begin_read(Default::default()).await.unwrap();
        assert!(
            old_chunks.is_disjoint(&blob_chunks(&read).await),
            "retired source blob chunks remain live"
        );
        assert!(
            CommitGraphContext::new()
                .reader(&read)
                .load_node(&old)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            crate::tracked_state::load_published_commit_state_topology(&read, old)
                .await
                .unwrap()
                .is_some()
        );
        let keys = changes
            .iter()
            .map(|id| StorageKey(Bytes::copy_from_slice(id.as_uuid().as_bytes())))
            .collect::<Vec<_>>();
        let found = read
            .get_many(&[crate::storage_adapter::StorageGetManyRequest {
                space: crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE,
                keys: &keys,
                opts: Default::default(),
            }])
            .await
            .unwrap();
        assert!(
            found.values.iter().all(Option::is_none),
            "retired source row locators remain live"
        );
        assert!(
            !crate::tracked_state::scan_commit_delta_inventory(&read)
                .await
                .unwrap()
                .commits
                .contains_key(&old),
            "completed payload retirement must not be advertised as readable row history"
        );
    }
}
