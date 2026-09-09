// Included inside repository::tests to reuse the real sparse-replica bootstrap.
mod upload_plan_profile {
    use super::*;
    use crate::storage::ProjectedValue;
    use std::sync::atomic::AtomicBool;
    use std::time::Instant;

    #[derive(Clone, Debug, Default, serde::Serialize)]
    struct ReadCounts {
        all_space_returned_bytes: u64,
        commit_header_keys: u64,
        change_value_keys: u64,
        change_value_bytes: u64,
        delta_segment_keys: u64,
        delta_segment_bytes: u64,
    }

    #[derive(Clone, Default)]
    struct ProfileStorage {
        inner: Memory,
        armed: Arc<AtomicBool>,
        counts: Arc<Mutex<ReadCounts>>,
    }

    struct ProfileRead {
        inner: MemoryRead,
        armed: Arc<AtomicBool>,
        counts: Arc<Mutex<ReadCounts>>,
    }

    impl Storage for ProfileStorage {
        type Read<'a>
            = ProfileRead
        where
            Self: 'a;
        type Write<'a>
            = MemoryWrite
        where
            Self: 'a;

        async fn acquire_session(
            &self,
        ) -> Result<crate::storage::StorageSessionToken, StorageError> {
            self.inner.acquire_session().await
        }

        async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
            Ok(ProfileRead {
                inner: self.inner.begin_read(options).await?,
                armed: Arc::clone(&self.armed),
                counts: Arc::clone(&self.counts),
            })
        }

        async fn begin_write(
            &self,
            options: WriteOptions,
        ) -> Result<Self::Write<'_>, StorageError> {
            self.inner.begin_write(options).await
        }

        async fn watch_for_changes(
            &self,
        ) -> Result<crate::storage::StorageChangeWatch, StorageError> {
            self.inner.watch_for_changes().await
        }
    }

    impl StorageRead for ProfileRead {
        fn snapshot_cache_key(&self) -> Option<u128> {
            self.inner.snapshot_cache_key()
        }

        async fn get_many(
            &self,
            requests: &[GetManyRequest<'_>],
        ) -> Result<GetManyResult, StorageError> {
            let result = self.inner.get_many(requests).await?;
            if self.armed.load(Ordering::Relaxed) {
                let mut counts = self.counts.lock().unwrap();
                let mut offset = 0;
                for request in requests {
                    let values = &result.values[offset..offset + request.keys.len()];
                    offset += request.keys.len();
                    let bytes = values
                        .iter()
                        .flatten()
                        .map(|value| match value {
                            ProjectedValue::FullValue(bytes) => bytes.len() as u64,
                            ProjectedValue::KeyOnly => 0,
                        })
                        .sum::<u64>();
                    counts.all_space_returned_bytes += bytes;
                    // Physical epoch banks reserve the top two space-ID bits.
                    if is_logical_space(request.space, COMMIT_SPACE) {
                        counts.commit_header_keys += request.keys.len() as u64;
                    } else if is_logical_space(request.space, crate::changelog::CHANGE_SPACE) {
                        counts.change_value_keys += request.keys.len() as u64;
                        counts.change_value_bytes += bytes;
                    } else if is_logical_space(
                        request.space,
                        crate::tracked_state::TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
                    ) {
                        counts.delta_segment_keys += request.keys.len() as u64;
                        counts.delta_segment_bytes += bytes;
                    }
                }
            }
            Ok(result)
        }

        async fn begin_scan(
            &self,
            space: StorageSpace,
            range: KeyRange,
            options: BeginScanOptions,
        ) -> Result<ScanCursor<'_>, StorageError> {
            self.inner.begin_scan(space, range, options).await
        }
    }

    async fn replica(authority: &Lix<Memory>) -> (Lix<ProfileStorage>, ProfileStorage) {
        let snapshot = authority.pull_sync_repository(None, 1).await.unwrap();
        let (branch_id, _) = default_head(&snapshot);
        let (history, rows, checkpoint_roots) = snapshot_parts(authority, &snapshot).await;
        let storage = ProfileStorage::default();
        Engine::initialize_with_main_branch_id(storage.clone(), Some(&branch_id))
            .await
            .unwrap();
        let mut replica = open_lix().with_storage(storage.clone()).await.unwrap();
        replica
            .set_sync_role(crate::sync::SyncRole::Replica)
            .unwrap();
        replica
            .try_install_initial_sync_snapshot(
                TEST_REMOTE,
                crate::ANONYMOUS_ACCOUNT_ID,
                &snapshot,
                &history.commits,
                &history.commit_headers,
                &rows,
                &checkpoint_roots,
            )
            .await
            .unwrap();
        install_publication_fence_responder_for_test(&mut replica);
        (replica, storage)
    }

    #[derive(Debug, Default, serde::Serialize)]
    struct DrainProfile {
        catch_up_micros: u128,
        retained_plan: bool,
        commit_payload_load_calls: u64,
        whole_process_vm_hwm_kib: Option<u64>,
        pages: usize,
        commits: usize,
        ref_updates: usize,
        planning_micros: u128,
        maximum_page_planning_micros: u128,
        emitted_members_json_bytes: usize,
        planning_reads: ReadCounts,
        maximum_ack_frontier: usize,
        final_ack_frontier: usize,
        final_receipt_json_bytes: usize,
    }

    async fn frontier(replica: &Lix<ProfileStorage>) -> (usize, usize) {
        let read = replica
            .storage_adapter()
            .begin_read(StorageReadOptions::default())
            .await
            .unwrap();
        let state = load_replica_state(&read).await.unwrap().0.unwrap();
        (
            state.authority_known_commit_ids.len(),
            serde_json::to_vec(&state).unwrap().len(),
        )
    }

    async fn drain(
        authority: &Lix<Memory>,
        replica: &Lix<ProfileStorage>,
        storage: &ProfileStorage,
        limit: usize,
        retained_plan: bool,
    ) -> DrainProfile {
        *storage.counts.lock().unwrap() = ReadCounts::default();
        let mut profile = DrainProfile::default();
        profile.retained_plan = retained_plan;
        let mut cache = None;
        let mut emitted = BTreeSet::new();
        let catch_up_started = Instant::now();
        loop {
            storage.armed.store(true, Ordering::Relaxed);
            let started = Instant::now();
            let (request, payload_loads) =
                crate::sync::upload_metrics::measure_commit_payload_loads(async {
                    if retained_plan {
                        replica
                            .build_sync_push_with_plan(TEST_REMOTE, limit, &mut cache)
                            .await
                    } else {
                        replica.build_sync_push(TEST_REMOTE, limit).await
                    }
                })
                .await;
            let elapsed = started.elapsed().as_micros();
            storage.armed.store(false, Ordering::Relaxed);
            profile.planning_micros += elapsed;
            profile.commit_payload_load_calls += payload_loads;
            profile.maximum_page_planning_micros =
                profile.maximum_page_planning_micros.max(elapsed);
            let Some(request) = request.unwrap() else {
                break;
            };
            assert!(request.commits.len() + request.ref_updates.len() <= limit);
            profile.pages += 1;
            profile.commits += request.commits.len();
            profile.ref_updates += request.ref_updates.len();
            for commit in &request.commits {
                assert!(
                    emitted.insert(commit.commit_id.clone()),
                    "accepted payload must not be resent"
                );
                profile.emitted_members_json_bytes +=
                    serde_json::to_vec(&commit.members).unwrap().len();
            }
            let receipt = authority.push_sync_repository(&request).await.unwrap();
            let cursor = replica
                .load_sync_repository_cursor(TEST_REMOTE)
                .await
                .unwrap()
                .unwrap();
            let response = authority
                .pull_sync_repository(Some(cursor), crate::sync::MAX_SYNC_REQUEST_ITEMS)
                .await
                .unwrap();
            replica
                .apply_sync_repository_pull(TEST_REMOTE, &response)
                .await
                .unwrap();
            assert!(
                replica
                    .load_sync_repository_cursor(TEST_REMOTE)
                    .await
                    .unwrap()
                    .unwrap()
                    >= receipt.cursor
            );
            if retained_plan {
                let plan = cache.as_mut().expect("successful page retains its plan");
                plan.acknowledge().unwrap();
                if plan.is_complete() {
                    cache = None;
                    replica.clear_converged_sync_frontier().await.unwrap();
                }
            }
            let (count, bytes) = frontier(replica).await;
            profile.maximum_ack_frontier = profile.maximum_ack_frontier.max(count);
            profile.final_ack_frontier = count;
            profile.final_receipt_json_bytes = bytes;
        }
        // The terminal planning call can retire the last converged frontier.
        let (count, bytes) = frontier(replica).await;
        profile.final_ack_frontier = count;
        profile.final_receipt_json_bytes = bytes;
        profile.catch_up_micros = catch_up_started.elapsed().as_micros();
        profile.planning_reads = storage.counts.lock().unwrap().clone();
        // Cumulative high-water mark includes fixture construction and all prior
        // tests in this process. This is deliberately not a planner peak metric.
        profile.whole_process_vm_hwm_kib = whole_process_vm_hwm_kib();
        profile
    }

    fn whole_process_vm_hwm_kib() -> Option<u64> {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        status.lines().find_map(|line| {
            line.strip_prefix("VmHWM:")?
                .split_whitespace()
                .next()?
                .parse()
                .ok()
        })
    }

    #[tokio::test]
    async fn upload_profile_counts_only_planning_and_preserves_page_boundaries() {
        let authority = open_lix().await.unwrap();
        let (replica, storage) = replica(&authority).await;
        for index in 0..33 {
            write_key_value(&replica, "profile", &format!("value-{index}")).await;
        }
        let profile = drain(&authority, &replica, &storage, 8, false).await;
        assert_eq!(profile.commits, 33);
        assert!(profile.pages >= 5);
        assert!(profile.planning_reads.commit_header_keys > 0);
        assert!(profile.planning_reads.all_space_returned_bytes > 0);
        assert_eq!(profile.commit_payload_load_calls, 33);
        assert!(profile.emitted_members_json_bytes > 0);
        assert!(
            profile.maximum_ack_frontier <= 1,
            "linear acknowledgments retain one frontier tip"
        );
        assert_eq!(read_key_value(&authority, "profile").await, "value-32");
    }

    #[tokio::test]
    async fn upload_profile_retained_wave_reads_headers_and_payloads_linearly() {
        let authority = open_lix().await.unwrap();
        let (replica, storage) = replica(&authority).await;
        for index in 0..65 {
            write_key_value(&replica, "profile", &format!("value-{index}")).await;
        }
        let profile = drain(&authority, &replica, &storage, 8, true).await;
        assert_eq!(profile.commits, 65);
        assert_eq!(profile.pages, 9);
        assert_eq!(profile.commit_payload_load_calls, 65);
        assert!(profile.planning_reads.commit_header_keys > 0);
        assert!(
            profile.planning_reads.commit_header_keys <= 8 * 65,
            "retaining a wave must avoid rewalking its graph for every page: {profile:?}"
        );
        assert_eq!(profile.final_ack_frontier, 0);
        assert_eq!(read_key_value(&authority, "profile").await, "value-64");
    }

    /// Run explicitly with --ignored --nocapture. Fixture writes and authority
    /// processing are excluded from planning time and point-read counters.
    /// Storage bytes are encoded backend reads, not a claimed decoder census.
    #[tokio::test]
    #[ignore = "explicit upload scaling profile: builds 512/2048/8192-commit queues"]
    async fn upload_plan_scaling_profile() {
        let retained_plan = std::env::var("LIX_UPLOAD_PROFILE_CACHED").as_deref() == Ok("1");
        let sizes = std::env::var("LIX_UPLOAD_PROFILE_SIZES")
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .map(|size| size.parse::<usize>().unwrap())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| vec![512, 2048, 8192]);
        for count in sizes {
            let authority = open_lix().await.unwrap();
            let (replica, storage) = replica(&authority).await;
            let payload = "x".repeat(1024);
            for index in 0..count {
                write_key_value(&replica, "profile", &format!("{index}:{payload}")).await;
            }
            let profile = drain(
                &authority,
                &replica,
                &storage,
                crate::sync::MAX_SYNC_REQUEST_ITEMS,
                retained_plan,
            )
            .await;
            assert_eq!(profile.commits, count);
            assert!(profile.maximum_ack_frontier <= 1);
            assert_eq!(
                read_key_value(&authority, "profile").await,
                format!("{}:{payload}", count - 1)
            );
            println!(
                "UPLOAD_PLAN_PROFILE {}",
                serde_json::json!({"queue_commits": count, "page_limit": crate::sync::MAX_SYNC_REQUEST_ITEMS, "profile": profile})
            );
        }
    }

    #[tokio::test]
    #[ignore = "explicit repeated checkpoint acknowledgment frontier profile"]
    async fn upload_checkpoint_frontier_profile() {
        let retained_plan = std::env::var("LIX_UPLOAD_PROFILE_CACHED").as_deref() == Ok("1");
        for scoped in [false, true] {
            let authority = open_lix().await.unwrap();
            let (replica, storage) = replica(&authority).await;
            let mut waves = Vec::new();
            for wave in 0..32 {
                for index in 0..4 {
                    write_key_value(&replica, "checkpoint-profile", &format!("{wave}:{index}"))
                        .await;
                }
                let sql = if scoped {
                    "SELECT commit_id FROM lix_create_checkpoint(ARRAY(SELECT row_ref FROM lix_diff('lix_key_value')))"
                } else {
                    "SELECT commit_id FROM lix_create_checkpoint()"
                };
                let checkpoint = replica.execute(sql, &[]).await.unwrap().rows()[0]
                    .get::<String>("commit_id")
                    .unwrap();
                let profile = drain(&authority, &replica, &storage, 2, retained_plan).await;
                let remote_checkpoint = authority
                    .execute("SELECT lix_latest_checkpoint_commit_id() AS id", &[])
                    .await
                    .unwrap()
                    .rows()[0]
                    .get::<String>("id")
                    .unwrap();
                assert_eq!(remote_checkpoint, checkpoint);
                assert_eq!(
                    read_key_value(&authority, "checkpoint-profile").await,
                    format!("{wave}:3")
                );
                waves.push(serde_json::json!({"wave": wave + 1, "profile": profile}));
            }
            println!(
                "UPLOAD_FRONTIER_PROFILE {}",
                serde_json::json!({"scoped": scoped, "waves": waves})
            );
        }
    }
}
