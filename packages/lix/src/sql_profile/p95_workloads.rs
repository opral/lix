//! Explicit profiling fixture for the seven highest-p95 LixRay query families.
//! Real engine + canonical Memory, with SQL phases, full semantic witnesses,
//! cold-session samples and repeated warm samples. Setup is excluded.
use crate::SqlReadProfile;
use crate::engine::Engine;
use crate::{Memory, Value};

fn millis(d: std::time::Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}
fn sample(p: SqlReadProfile) -> serde_json::Value {
    let (builds, descriptor_rows) = crate::filesystem::full_rebuild_stats();
    let (hits, misses) = crate::filesystem::path_index_cache_stats();
    let (nodes, diffs) = crate::sql2::take_mainline_work();
    let (retirement_batches, retirement_keys) = crate::sql2::take_checkpoint_retirement_work();
    let (metadata_batches, metadata_rows) = crate::sql2::take_mainline_metadata_work();
    #[cfg(feature = "storage-benches")]
    let wait_phases = serde_json::json!({
        "session_transaction_admission_ms": millis(p.session_transaction_admission_wait),
        "partial_publication_gate_ms": millis(p.partial_publication_gate_wait),
        "storage_backend_begin_read_ms": millis(p.storage_backend_begin_read),
        "storage_epoch_validation_ms": millis(p.storage_epoch_validation),
        "expired_read_retry_delay_ms": millis(p.expired_read_retry_delay),
        "partial_interest_journal_flush_ms": millis(p.partial_interest_journal_flush),
    });
    #[cfg(not(feature = "storage-benches"))]
    let wait_phases = serde_json::Value::Null;
    serde_json::json!({"total_ms":millis(p.total),"logical_ms":millis(p.logical_planning),"physical_ms":millis(p.physical_planning),"execution_ms":millis(p.arrow_execution),"materialization_ms":millis(p.public_result_materialization),"other_ms":millis(p.unattributed_overhead()),"scan_ms":millis(p.scan_elapsed),"scan_rows":p.scan_rows,"scan_batches":p.scan_batches,"scan_arrow_bytes":p.scan_arrow_bytes,"provider_rows_examined":p.provider_rows_examined,"diff_payload_rows_retained":p.diff_payload_rows_retained,"effective_payload_rows_captured":p.effective_payload_rows_captured,"derived_snapshot_content_rows":p.derived_snapshot_content_rows,"file_local_diff_rows_reused":p.file_local_diff_rows_reused,"diff_payload_joined_delta_validation_rows":p.diff_payload_joined_delta_validation_rows,"path_index_builds":builds,"path_index_descriptor_rows":descriptor_rows,"path_index_cache_hits":hits,"path_index_cache_misses":misses,"mainline_nodes":nodes,"mainline_diffs":diffs,"retirement_batches":retirement_batches,"retirement_keys":retirement_keys,"metadata_batches":metadata_batches,"metadata_rows":metadata_rows,"wait_phases":wait_phases})
}
async fn full_profile(
    session: &crate::session::SessionContext<Memory>,
    sql: &str,
    params: &[Value],
    observe: bool,
) -> (crate::ExecuteResult, SqlReadProfile) {
    crate::filesystem::reset_full_rebuild_stats();
    let _ = crate::sql2::take_mainline_work();
    let _ = crate::sql2::take_checkpoint_retirement_work();
    let _ = crate::sql2::take_mainline_metadata_work();
    let started = std::time::Instant::now();
    let (result, mut profile) = if observe {
        session
            .execute_for_observe_profiled(sql, params)
            .await
            .unwrap()
    } else {
        session.execute_profiled(sql, params).await.unwrap()
    };
    // execute_profiled consumes lazy public rows within its phase scope.
    // Oracle validation remains outside the timed operation.
    profile.total = started.elapsed();
    (result, profile)
}

fn setting(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(fallback)
}

#[tokio::test]
#[ignore = "explicit performance experiment; emits P95_WORKLOAD JSON"]
async fn seven_p95_workloads() {
    let rows = setting("LIX_P95_ROWS", 128);
    let history = setting("LIX_P95_HISTORY", 128);
    // Add ordinary restore commits after the final checkpoint, in its open
    // first-parent interval. Alternating between the last two distinct
    // checkpoints makes each restore a real non-checkpoint commit while
    // keeping the mainline checkpoint count fixed.
    let history_gap_commits = setting("LIX_P95_HISTORY_GAP_COMMITS", 0);
    // Checkpoints on this sibling branch enter the repository-wide inventory
    // but must not enter the active branch's first-parent log.
    let off_mainline_checkpoints = setting("LIX_P95_OFF_MAINLINE_CHECKPOINTS", 0);
    let repeats = setting("LIX_P95_REPEATS", 100);
    let dirty_files = setting("LIX_P95_DIRTY_FILES", 1);
    let root_current_base = setting("LIX_P95_ROOT_CURRENT_BASE", 0) != 0;
    let mode = std::env::var("LIX_P95_EXECUTION").unwrap_or_else(|_| "execute".into());
    assert!(matches!(mode.as_str(), "execute" | "production_kinds"));
    assert!(dirty_files >= 1 && dirty_files <= rows);
    assert!(rows >= 8 && history >= 1 && repeats >= 20);
    assert!(history_gap_commits == 0 || history >= 2);
    let storage = Memory::default();
    Engine::initialize(storage.clone()).await.unwrap();
    let seed_engine = Engine::new(storage.clone()).await.unwrap();
    let seed = seed_engine.open_session().await.unwrap();
    for i in 0..rows {
        seed.execute(
            "INSERT INTO lix_file (id,path,content) VALUES ($1,$2,$3)",
            &[
                Value::Text(format!("01940000-0000-7000-8000-{i:012x}")),
                Value::Text(format!("/directory-{}/file-{i}.txt", i / 8)),
                Value::Blob(vec![b'x'; 4096].into()),
            ],
        )
        .await
        .unwrap();
    }
    let mut checkpoint_ids = Vec::with_capacity(history);
    for i in 0..history {
        seed.execute("INSERT INTO lix_key_value (key,value) VALUES ('p95-history',$1) ON CONFLICT (key) DO UPDATE SET value=excluded.value", &[Value::Text(i.to_string())]).await.unwrap();
        let checkpoint = seed
            .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
            .await
            .unwrap();
        checkpoint_ids.push(
            checkpoint.rows()[0]
                .get::<String>("commit_id")
                .expect("checkpoint fixture returns its commit ID"),
        );
    }
    if history_gap_commits > 0 {
        let ordinary_count_before = seed
            .execute(
                "SELECT count(*) AS n FROM lix_log() WHERE is_checkpoint = false",
                &[],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<i64>("n")
            .unwrap();
        let previous_checkpoint = &checkpoint_ids[history - 2];
        let latest_checkpoint = &checkpoint_ids[history - 1];
        for i in 0..history_gap_commits {
            let target = if i % 2 == 0 {
                previous_checkpoint
            } else {
                latest_checkpoint
            };
            seed.execute(
                "SELECT commit_id FROM lix_restore($1)",
                &[Value::Text((*target).clone())],
            )
            .await
            .unwrap();
        }
        let ordinary_count_after = seed
            .execute(
                "SELECT count(*) AS n FROM lix_log() WHERE is_checkpoint = false",
                &[],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<i64>("n")
            .unwrap();
        assert_eq!(
            ordinary_count_after - ordinary_count_before,
            i64::try_from(history_gap_commits).unwrap(),
            "each alternating restore must add one visible non-checkpoint mainline node",
        );
    }
    if off_mainline_checkpoints > 0 {
        let branch = seed
            .create_branch(crate::session::CreateBranchOptions {
                id: None,
                name: "p95-off-mainline-history".into(),
                from_commit_id: Some(checkpoint_ids[history - 1].clone()),
            })
            .await
            .unwrap();
        let side = seed_engine.open_session_at(branch.id).await.unwrap();
        for i in 0..off_mainline_checkpoints {
            side.execute(
                "INSERT INTO lix_key_value (key,value) VALUES ('p95-off-mainline-history',$1) ON CONFLICT (key) DO UPDATE SET value=excluded.value",
                &[Value::Text(i.to_string())],
            )
            .await
            .unwrap();
            side.execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
                .await
                .unwrap();
        }
        let side_checkpoint_count = side
            .execute(
                "SELECT count(*) AS n FROM lix_log() WHERE is_checkpoint = true",
                &[],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<i64>("n")
            .unwrap();
        assert_eq!(
            side_checkpoint_count,
            i64::try_from(history + off_mainline_checkpoints).unwrap(),
            "side-branch fixture must publish all requested off-mainline checkpoints",
        );
        side.close().await.unwrap();
    }
    let root_current_base_branch = if root_current_base {
        let branch = seed
            .create_branch(crate::session::CreateBranchOptions {
                id: None,
                name: "p95-root-backed-current-base".into(),
                from_commit_id: None,
            })
            .await
            .unwrap();
        let read = seed_engine
            .storage()
            .begin_read(Default::default())
            .await
            .unwrap();
        let control = crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .load(&branch.id)
            .await
            .unwrap()
            .expect("normal branch publication writes its branch control");
        let current_base = crate::hot_state::TrackedHeadContext::new()
            .reader(&read)
            .root_current_base_commit(&branch.id, control.tracked_generation)
            .await
            .unwrap();
        assert!(
            current_base.is_some(),
            "fixture flag requires the normal current-base root publication path"
        );
        Some(branch.id)
    } else {
        None
    };
    let seed = if let Some(branch_id) = &root_current_base_branch {
        seed.close().await.unwrap();
        seed_engine
            .open_session_at(branch_id.clone())
            .await
            .unwrap()
    } else {
        seed
    };
    let target = Value::Text("01940000-0000-7000-8000-000000000000".into());
    for i in 0..dirty_files {
        seed.execute(
            "UPDATE lix_file SET content=$2 WHERE id=$1",
            &[
                Value::Text(format!("01940000-0000-7000-8000-{i:012x}")),
                Value::Blob(vec![b'y'; 4096].into()),
            ],
        )
        .await
        .unwrap();
    }
    if let Some(branch_id) = &root_current_base_branch {
        let read = seed_engine
            .storage()
            .begin_read(Default::default())
            .await
            .unwrap();
        let control = crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .load(branch_id)
            .await
            .unwrap()
            .expect("published branch still has a control after fixture mutations");
        let current_base = crate::hot_state::TrackedHeadContext::new()
            .reader(&read)
            .root_current_base_commit(branch_id, control.tracked_generation)
            .await
            .unwrap();
        assert!(
            current_base.is_some(),
            "fixture must retain root current-base coverage after mutations"
        );
    }
    let account = seed
        .execute("SELECT id FROM lix_account LIMIT 1", &[])
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("id")
        .unwrap();
    let queries = vec![
        (
            "checkpoint_summary",
            "SELECT created_at, count(*) over () AS total_count FROM lix_log() WHERE is_checkpoint ORDER BY position LIMIT 10",
            vec![],
        ),
        (
            "working_diff_count",
            "SELECT count(*) AS file_count FROM lix_diff('lix_file')",
            vec![],
        ),
        (
            "file_content_id",
            "SELECT content FROM lix_file WHERE id = $1",
            vec![target.clone()],
        ),
        (
            "file_row_id",
            "select id, path, content from lix_file where id = $1 limit $2",
            vec![target.clone(), Value::Integer(1)],
        ),
        (
            "directory_listing",
            "SELECT id, parent_id, path, name, lixcol_updated_at FROM lix_directory ORDER BY path",
            vec![],
        ),
        (
            "account_id",
            "SELECT name, kind, profile_uri FROM lix_account WHERE id = $1",
            vec![Value::Text(account)],
        ),
        (
            "file_paths_ids",
            "select id, path from lix_file where id in ($1)",
            vec![target],
        ),
    ];
    // Establish results independently of timing. Every cold and warm execution
    // is compared with this complete result, including blobs and total_count.
    let mut expected = Vec::new();
    for (_, sql, params) in &queries {
        expected.push(seed.execute(sql, params).await.unwrap());
    }
    let mainline_checkpoint_count = seed
        .execute(
            "SELECT count(*) AS n FROM lix_log() WHERE is_checkpoint = true",
            &[],
        )
        .await
        .unwrap()
        .rows()[0]
        .get::<i64>("n")
        .unwrap();
    assert_eq!(
        mainline_checkpoint_count,
        i64::try_from(history).unwrap(),
        "side-branch checkpoints and open-interval restores must not change mainline checkpoint count",
    );
    assert_eq!(
        expected[0].rows()[0].get::<i64>("total_count").unwrap(),
        i64::try_from(history).unwrap(),
        "the full window count must report all active-mainline checkpoints",
    );
    assert_eq!(
        expected[1].rows()[0].get::<i64>("file_count").unwrap(),
        dirty_files as i64
    );
    if root_current_base {
        let full_dirty_diff = seed
            .execute(
                "SELECT id, diff_type, from_content, to_content \
                 FROM lix_diff('lix_file') ORDER BY id",
                &[],
            )
            .await
            .expect("root-backed dirty diff should return its full payload rows");
        assert_eq!(
            full_dirty_diff.rows().len(),
            dirty_files,
            "known dirty writes should produce one complete working-diff row each",
        );
        for index in 0..dirty_files {
            let row = &full_dirty_diff.rows()[index];
            assert_eq!(
                row.get::<String>("id").unwrap(),
                format!("01940000-0000-7000-8000-{index:012x}"),
                "root-backed diff should retain each expected dirty file identity",
            );
            assert_eq!(row.get::<String>("diff_type").unwrap(), "modified");
            assert_eq!(
                row.get::<Vec<u8>>("from_content").unwrap(),
                vec![b'x'; 4096],
                "full payload should preserve the pre-write content",
            );
            assert_eq!(
                row.get::<Vec<u8>>("to_content").unwrap(),
                vec![b'y'; 4096],
                "full payload should preserve the post-write content",
            );
        }
        let dirty_diff_count = seed
            .execute(
                "SELECT count(*) AS file_count FROM lix_diff('lix_file')",
                &[],
            )
            .await
            .expect("root-backed dirty diff count should execute");
        assert_eq!(
            dirty_diff_count.rows()[0].get::<i64>("file_count").unwrap(),
            full_dirty_diff.rows().len() as i64,
            "identity-only count must match the independent full-payload row oracle",
        );
    }
    if let Some(branch_id) = &root_current_base_branch {
        let read = seed_engine
            .storage()
            .begin_read(Default::default())
            .await
            .unwrap();
        let control = crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .load(branch_id)
            .await
            .unwrap()
            .expect("published branch still has a control after fixture reads");
        let current_base = crate::hot_state::TrackedHeadContext::new()
            .reader(&read)
            .root_current_base_commit(branch_id, control.tracked_generation)
            .await
            .unwrap();
        assert!(
            current_base.is_some(),
            "fixture must retain root current-base coverage immediately before sampling"
        );
    }
    seed.close().await.unwrap();
    drop(seed);
    drop(seed_engine);
    for ((name, sql, params), oracle) in queries.into_iter().zip(expected) {
        let observe = mode == "production_kinds" && name != "account_id";
        let engine = Engine::new(storage.clone()).await.unwrap();
        let session = if let Some(branch_id) = &root_current_base_branch {
            engine.open_session_at(branch_id.clone()).await.unwrap()
        } else {
            engine.open_session().await.unwrap()
        };
        let (result, profile) = full_profile(&session, sql, &params, observe).await;
        assert_eq!(result, oracle, "cold result: {name}");
        let cold = sample(profile);
        // Two untimed warmups establish caches consistently for every query.
        for _ in 0..2 {
            assert_eq!(session.execute(sql, &params).await.unwrap(), oracle);
        }
        let mut warm = Vec::new();
        for _ in 0..repeats {
            let (result, profile) = full_profile(&session, sql, &params, observe).await;
            assert_eq!(result, oracle, "warm result: {name}");
            warm.push(sample(profile));
        }
        println!(
            "P95_WORKLOAD={}",
            serde_json::json!({"query":name,"execution_kind":if observe {"observe_sql"} else {"execute"},"backend":"canonical_memory","serving_layout":if root_current_base {"root_current_base"} else {"canonical_memory_default"},"files":rows,"checkpoints":history,"history_gap_noncheckpoint_commits":history_gap_commits,"off_mainline_checkpoints":off_mainline_checkpoints,"dirty_files":dirty_files,"repeats":repeats,"rows":oracle.len(),"cold":cold,"warm":warm,"verified":true,"timing_scope":"execute_through_lazy_public_rows_consumed","cold_scope":"fresh_engine_session_first_execution_same_memory_storage"})
        );
        if name == "file_content_id" {
            let mut changing_ids = Vec::new();
            for i in 0..repeats {
                let selected = (i * 37) % rows;
                let params = [Value::Text(format!(
                    "01940000-0000-7000-8000-{selected:012x}"
                ))];
                let (result, profile) = full_profile(&session, sql, &params, observe).await;
                assert_eq!(
                    result.rows()[0].get::<Vec<u8>>("content").unwrap(),
                    vec![if selected < dirty_files { b'y' } else { b'x' }; 4096]
                );
                changing_ids.push(sample(profile));
            }
            println!(
                "P95_WORKLOAD={}",
                serde_json::json!({"query":name,"variant":"changing_ids","execution_kind":if observe {"observe_sql"} else {"execute"},"backend":"canonical_memory","serving_layout":if root_current_base {"root_current_base"} else {"canonical_memory_default"},"files":rows,"checkpoints":history,"history_gap_noncheckpoint_commits":history_gap_commits,"off_mainline_checkpoints":off_mainline_checkpoints,"dirty_files":dirty_files,"repeats":repeats,"warm":changing_ids,"verified":true,"timing_scope":"execute_through_lazy_public_rows_consumed"})
            );
        }
        session.close().await.unwrap();
    }
}
