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
    serde_json::json!({"total_ms":millis(p.total),"logical_ms":millis(p.logical_planning),"physical_ms":millis(p.physical_planning),"execution_ms":millis(p.arrow_execution),"materialization_ms":millis(p.public_result_materialization),"other_ms":millis(p.unattributed_overhead()),"scan_ms":millis(p.scan_elapsed),"scan_rows":p.scan_rows,"scan_batches":p.scan_batches,"scan_arrow_bytes":p.scan_arrow_bytes,"provider_rows_examined":p.provider_rows_examined,"path_index_builds":builds,"path_index_descriptor_rows":descriptor_rows,"path_index_cache_hits":hits,"path_index_cache_misses":misses,"mainline_nodes":nodes,"mainline_diffs":diffs})
}
async fn full_profile(
    session: &crate::session::SessionContext<Memory>,
    sql: &str,
    params: &[Value],
) -> (crate::ExecuteResult, SqlReadProfile) {
    crate::filesystem::reset_full_rebuild_stats();
    let _ = crate::sql2::take_mainline_work();
    let started = std::time::Instant::now();
    let (result, mut profile) = session.execute_profiled(sql, params).await.unwrap();
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
    let repeats = setting("LIX_P95_REPEATS", 100);
    assert!(rows >= 8 && history >= 1 && repeats >= 20);
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
    for i in 0..history {
        seed.execute("INSERT INTO lix_key_value (key,value) VALUES ('p95-history',$1) ON CONFLICT (key) DO UPDATE SET value=excluded.value", &[Value::Text(i.to_string())]).await.unwrap();
        seed.execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
            .await
            .unwrap();
    }
    let target = Value::Text("01940000-0000-7000-8000-000000000000".into());
    seed.execute(
        "UPDATE lix_file SET content=$2 WHERE id=$1",
        &[target.clone(), Value::Blob(vec![b'y'; 4096].into())],
    )
    .await
    .unwrap();
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
    assert_eq!(expected[1].rows()[0].get::<i64>("file_count").unwrap(), 1);
    seed.close().await.unwrap();
    drop(seed);
    drop(seed_engine);
    for ((name, sql, params), oracle) in queries.into_iter().zip(expected) {
        let engine = Engine::new(storage.clone()).await.unwrap();
        let session = engine.open_session().await.unwrap();
        let (result, profile) = full_profile(&session, sql, &params).await;
        assert_eq!(result, oracle, "cold result: {name}");
        let cold = sample(profile);
        // Two untimed warmups establish caches consistently for every query.
        for _ in 0..2 {
            assert_eq!(session.execute(sql, &params).await.unwrap(), oracle);
        }
        let mut warm = Vec::new();
        for _ in 0..repeats {
            let (result, profile) = full_profile(&session, sql, &params).await;
            assert_eq!(result, oracle, "warm result: {name}");
            warm.push(sample(profile));
        }
        println!(
            "P95_WORKLOAD={}",
            serde_json::json!({"query":name,"backend":"canonical_memory","files":rows,"checkpoints":history,"repeats":repeats,"rows":oracle.len(),"cold":cold,"warm":warm,"verified":true,"timing_scope":"execute_through_lazy_public_rows_consumed","cold_scope":"fresh_engine_session_first_execution_same_memory_storage"})
        );
        if name == "file_content_id" {
            let mut changing_ids = Vec::new();
            for i in 0..repeats {
                let selected = (i * 37) % rows;
                let params = [Value::Text(format!(
                    "01940000-0000-7000-8000-{selected:012x}"
                ))];
                let (result, profile) = full_profile(&session, sql, &params).await;
                assert_eq!(
                    result.rows()[0].get::<Vec<u8>>("content").unwrap(),
                    vec![if selected == 0 { b'y' } else { b'x' }; 4096]
                );
                changing_ids.push(sample(profile));
            }
            println!(
                "P95_WORKLOAD={}",
                serde_json::json!({"query":name,"variant":"changing_ids","backend":"canonical_memory","files":rows,"checkpoints":history,"repeats":repeats,"warm":changing_ids,"verified":true,"timing_scope":"execute_through_lazy_public_rows_consumed"})
            );
        }
        session.close().await.unwrap();
    }
}
