// This proves only the native immutable-tree prerequisite for a partial replica
// with on-demand sync. It does not establish SQL coverage or sync admission.
mod partial_replica {
    use super::*;
    use crate::storage_adapter::{StorageKey, StorageValue};

    struct RecordingChunkRead<R> {
        read: R,
        requested: Arc<Mutex<BTreeSet<[u8; TRACKED_STATE_HASH_BYTES]>>>,
    }

    impl<R: StorageRead> StorageRead for RecordingChunkRead<R> {
        fn get_many(
            &self,
            requests: &[crate::storage::GetManyRequest<'_>],
        ) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send {
            {
                let mut requested = self.requested.lock().expect("recording lock");
                for request in requests {
                    assert_eq!(request.space, storage::TRACKED_STATE_TREE_CHUNK_SPACE);
                    for key in request.keys {
                        requested.insert(key.0.as_ref().try_into().expect("chunk digest"));
                    }
                }
            }
            self.read.get_many(requests)
        }

        fn begin_scan(
            &self,
            _space: crate::storage::StorageSpace,
            _range: KeyRange,
            _opts: BeginScanOptions,
        ) -> impl Future<Output = Result<ScanCursor<'_>, StorageError>> + Send {
            async { panic!("preparing a tree frontier must only use addressed chunk reads") }
        }
    }

    async fn copy_chunks(
        authority: &StorageAdapter,
        destination: &StorageAdapter,
        hashes: impl IntoIterator<Item = [u8; TRACKED_STATE_HASH_BYTES]>,
    ) -> (usize, usize) {
        let read = authority
            .begin_read(StorageReadOptions::default())
            .await
            .expect("authority read");
        let mut chunks = Vec::new();
        let mut bytes = 0;
        for hash in hashes {
            // Frontier staging also probes newly generated digests. Those are
            // absent at the authority and must not be fetched as base inputs.
            if let Some(content) = storage::read_chunk(&read, &hash).await.expect("read chunk") {
                bytes += content.len();
                chunks.push((
                    StorageKey(Bytes::copy_from_slice(&hash)),
                    StorageValue { bytes: content },
                ));
            }
        }
        let count = chunks.len();
        let mut writes = destination.new_write_set();
        writes.put_content_addressed_batch(storage::TRACKED_STATE_TREE_CHUNK_SPACE, chunks);
        destination
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("install immutable chunks");
        (count, bytes)
    }

    #[tokio::test]
    async fn prepared_existing_updates_preserve_absent_untouched_subtrees() {
        for row_count in [1_000, 100_000] {
            let authority_memory = Memory::new();
            let authority = StorageAdapter::new(authority_memory.clone());
            let builder = TrackedStateTree::new();
            let mut expected = (0..row_count)
                .map(|index| {
                    (
                        key("schema", None, &format!("row-{index:06}")),
                        value(&format!("change-{index}"), Some("{}")),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let base = apply_mutations_for_test(
                &builder,
                &authority,
                None,
                expected
                    .iter()
                    .map(|(key, value)| mutation(key, value))
                    .collect(),
                None,
            )
            .await
            .expect("authority tree");
            let target = key("schema", None, &format!("row-{:06}", row_count / 2));

            // Use production scoped preparation, then retain only its native
            // chunk read closure. Subsequent real mutations must work with
            // untouched authority chunks physically absent.
            let requested = Arc::new(Mutex::new(BTreeSet::new()));
            let recording = StorageAdapterReadScope::new(RecordingChunkRead {
                read: authority_memory
                    .begin_read(crate::storage::ReadOptions::default())
                    .await
                    .expect("preparation snapshot"),
                requested: Arc::clone(&requested),
            });
            TrackedStateTree::new()
                .prepare_existing_key_mutation_inputs(
                    &recording,
                    &base.root_id,
                    &[Bytes::from(encode_key(&target))],
                )
                .await
                .expect("prepare production mutation frontier");
            drop(recording);
            let requested = requested.lock().expect("recorded hashes").clone();

            let partial = StorageAdapter::new(Memory::new());
            let (loaded_nodes, loaded_bytes) =
                copy_chunks(&authority, &partial, requested.iter().copied()).await;
            let authority_read = authority
                .begin_read(StorageReadOptions::default())
                .await
                .expect("authority inventory read");
            let all_base_hashes = TrackedStateTree::new()
                .reachable_chunk_hashes_with_overlay(
                    &authority_read,
                    &storage::TrackedStateChunkOverlay::new(),
                    &base.root_id,
                )
                .await
                .expect("test oracle counts complete tree");
            eprintln!(
                "native prepared value-update closure rows={row_count} height={} loaded_nodes={loaded_nodes} total_nodes={} loaded_bytes={loaded_bytes}",
                base.tree_height,
                all_base_hashes.len(),
            );
            // Preparation includes the writer's left spine and conservative
            // resynchronization neighbors. In a two-level nine-node fixture,
            // that fixed overhead can exceed half the tree without loading it
            // all. Bound the measured closure by height in both fixtures and
            // retain a strong fractional bound once the tree is substantial.
            assert!(
                loaded_nodes < all_base_hashes.len(),
                "fixture must retain physically absent chunks"
            );
            assert!(
                loaded_nodes <= 6 * base.tree_height,
                "single-key preparation exceeded six chunks per tree level: rows={row_count} height={} loaded={loaded_nodes}",
                base.tree_height
            );
            if row_count >= 100_000 {
                assert!(
                    loaded_nodes * 10 < all_base_hashes.len(),
                    "large-tree preparation must retain more than ninety percent of authority chunks absent"
                );
            }
            let absent = *all_base_hashes
                .iter()
                .find(|hash| !requested.contains(*hash))
                .expect("untouched subtree chunk");
            let mut current = base.root_id.clone();
            let mut local_update_nanos = Vec::new();

            // There is no authority reader/transport in this execution path.
            // A fresh tree for every update also proves durability across cache
            // loss: warmed in-memory nodes cannot conceal absent storage data.
            for step in 0..12 {
                let next_value = value(&format!("prepared-{step}"), Some("{}"));
                let local_started = std::time::Instant::now();
                let updated = apply_mutations_for_test(
                    &TrackedStateTree::new(),
                    &partial,
                    Some(&current),
                    vec![mutation(&target, &next_value)],
                    None,
                )
                .await
                .expect("prepared native update must succeed with only local chunks");
                local_update_nanos.push(local_started.elapsed().as_nanos());
                assert_eq!(updated.row_count, row_count);
                assert_eq!(updated.tree_height, base.tree_height);
                assert!(updated.chunk_count <= base.tree_height);
                let local_read = partial
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("local read after update");
                assert_eq!(
                    TrackedStateTree::new()
                        .get(&local_read, &updated.root_id, &target)
                        .await
                        .expect("local read-your-write"),
                    Some(next_value.clone())
                );
                expected.insert(target.clone(), next_value);
                let canonical = TrackedStateTree::new()
                    .build_tree_from_entries(
                        expected
                            .iter()
                            .map(|(key, value)| EncodedLeafEntry {
                                key: encode_key(key).into(),
                                value: encode_value(value).into(),
                            })
                            .collect(),
                    )
                    .expect("independent canonical reference");
                assert_eq!(
                    updated.root_id, canonical.root_id,
                    "rows={row_count} step={step}"
                );
                current = updated.root_id;
            }
            let partial_read = partial
                .begin_read(StorageReadOptions::default())
                .await
                .expect("partial reopen");
            assert!(
                storage::read_chunk(&partial_read, &absent)
                    .await
                    .expect("absent check")
                    .is_none()
            );
            assert!(
                TrackedStateTree::new()
                    .scan(
                        &partial_read,
                        &current,
                        &TrackedStateTreeScanRequest::default()
                    )
                    .await
                    .is_err(),
                "unprepared complete scan must not succeed with an incomplete tree"
            );
            drop(partial_read);

            // Only the verifier now gains access to retained authority chunks.
            // New local roots must still expose every unchanged authority row.
            let (total_nodes, total_bytes) =
                copy_chunks(&authority, &partial, all_base_hashes.iter().copied()).await;
            let verified_read = partial
                .begin_read(StorageReadOptions::default())
                .await
                .expect("reconstituted verifier");
            let verified = TrackedStateTree::new()
                .scan(
                    &verified_read,
                    &current,
                    &TrackedStateTreeScanRequest::default(),
                )
                .await
                .expect("full verification scan");
            assert_eq!(verified, expected.into_iter().collect::<Vec<_>>());
            local_update_nanos.sort_unstable();
            eprintln!(
                "partial replica native frontier: rows={row_count} loaded_nodes={loaded_nodes} total_nodes={total_nodes} loaded_bytes={loaded_bytes} total_bytes={total_bytes} height={} updates=12 local_update_p50_us={} local_update_max_us={}",
                base.tree_height,
                local_update_nanos[local_update_nanos.len() / 2] / 1_000,
                local_update_nanos.last().expect("local update samples") / 1_000,
            );
        }
    }
}
