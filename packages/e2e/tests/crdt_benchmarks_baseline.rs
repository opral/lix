//! Transactional slices comparable to dmonad/crdt-benchmarks.
//!
//! The heavier workloads here are ignored profiling runs. The upstream suite
//! measures synchronous in-memory CRDT update exchange; Lix measures durable
//! commit-to-convergence across same-base clients. These workloads use only the
//! existing `begin_transaction()` API and never create synthetic branches.

use std::fs;
use std::io::{Cursor, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use lix::storage::Storage;
use lix::{Lix, Value, open_lix};

const N: usize = 6_000;
const CONCURRENT_CLIENTS: usize = 100;
const SAMPLES: usize = 5;

#[tokio::test]
#[ignore = "dmonad/crdt-benchmarks B2.1 durable Lix baseline"]
async fn crdt_benchmarks_b2_1_markdown_concurrent_prefix_inserts() {
    let mut samples = Vec::with_capacity(SAMPLES);
    for _sample in 0..SAMPLES {
        let lix = open_lix().await.expect("B2.1 workspace should open");
        install_plugin(&lix, "plugin_markdown", &build_markdown_plugin_archive()).await;

        let path = "/b2-1.md";
        let base = format!("{}\n", "x".repeat(100));
        write_file(&lix, path, base.as_bytes()).await;
        let target = format!("{}{}", "a".repeat(N), base);
        let source_text = format!("{}{}", "b".repeat(N), base);
        let peer = lix
            .open_another_session()
            .await
            .expect("peer session should open");
        let mut target_transaction = lix.begin_transaction().await.expect("target transaction");
        let mut source_transaction = peer.begin_transaction().await.expect("peer transaction");
        for (transaction, bytes) in [
            (&mut target_transaction, target.as_bytes()),
            (&mut source_transaction, source_text.as_bytes()),
        ] {
            transaction
                .execute(
                    "UPDATE lix_file SET content = $1 WHERE path = $2",
                    &[
                        Value::Blob(bytes.to_vec().into()),
                        Value::Text(path.to_owned()),
                    ],
                )
                .await
                .expect("same-base Markdown edit should stage");
        }

        let started = Instant::now();
        target_transaction
            .commit()
            .await
            .expect("target should commit");
        source_transaction
            .commit()
            .await
            .expect("same-position Markdown inserts should resolve");
        samples.push(started.elapsed());

        let merged = read_file(&lix, path).await;
        let expected_ab = format!("{}{}{}", "a".repeat(N), "b".repeat(N), base);
        let expected_ba = format!("{}{}{}", "b".repeat(N), "a".repeat(N), base);
        assert!(merged == expected_ab.as_bytes() || merged == expected_ba.as_bytes());
        assert_eq!(read_file(&peer, path).await, merged);
        peer.close().await.expect("peer should close");
        lix.close().await.expect("B2.1 workspace should close");
    }

    report("B2.1", "markdown", &mut samples);
}

/// dmonad/crdt-benchmarks B3.1 durable Lix baseline.
///
/// Runs in CI rather than as a manual profiling workload: it is the only
/// coverage for a hundred-writer same-base wave, it completes in about a
/// second, and its convergence assertions no longer depend on how the
/// coordinator batched that wave. `LIX_CRDT_B3_CLIENTS` and `LIX_CRDT_SAMPLES`
/// scale it up for profiling runs.
#[tokio::test]
async fn crdt_benchmarks_b3_1_json_concurrent_map_sets() {
    let clients = std::env::var("LIX_CRDT_B3_CLIENTS")
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("client count should be numeric")
        })
        .unwrap_or(CONCURRENT_CLIENTS);
    let samples = std::env::var("LIX_CRDT_SAMPLES")
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("sample count should be numeric")
        })
        .unwrap_or(SAMPLES);
    assert!(clients >= 2, "B3.1 needs at least two clients");
    assert!(samples > 0, "B3.1 needs at least one sample");
    let mut all_latencies = Vec::with_capacity(samples * clients);
    let mut batch_latencies = Vec::with_capacity(samples);
    for sample in 0..samples {
        let lix = open_lix().await.expect("B3.1 workspace should open");
        install_plugin(&lix, "plugin_json", &build_json_plugin_archive()).await;
        let path = format!("/b3-1-{sample}.json");
        write_file(&lix, &path, br#"{"v":-1}"#).await;
        let file_id = file_id(&lix, &path).await;
        let mut peers = Vec::with_capacity(clients);
        let mut transactions = Vec::with_capacity(clients);
        for client in 0..clients {
            let peer = lix
                .open_another_session()
                .await
                .expect("peer session should open");
            let mut transaction = peer
                .begin_transaction()
                .await
                .expect("same-base transaction should open");
            transaction
                .execute(
                    "UPDATE json_object_member SET scalar_json = $1 \
                     WHERE parent_id = 'root' AND key = 'v' AND lixcol_file_id = $2",
                    &[
                        Value::Text(client.to_string()),
                        Value::Text(file_id.clone()),
                    ],
                )
                .await
                .expect("same-base JSON update should stage");
            peers.push(peer);
            transactions.push(transaction);
        }

        let batch_started = Instant::now();
        let commit_results = tokio::task::LocalSet::new()
            .run_until(async move {
                let mut commits = tokio::task::JoinSet::new();
                for transaction in transactions {
                    let started = Instant::now();
                    commits.spawn_local(async move {
                        let result = transaction.commit().await;
                        (started.elapsed(), result)
                    });
                }
                let mut results = Vec::new();
                while let Some(joined) = commits.join_next().await {
                    results.push(joined.expect("commit task should not panic"));
                }
                results
            })
            .await;
        batch_latencies.push(batch_started.elapsed());
        for (elapsed, result) in commit_results {
            result.expect("same-base JSON update should resolve");
            all_latencies.push(elapsed);
        }

        // The public contract is convergence, retained writes, and a linear
        // durable history. Resolver batching is internal instrumentation.
        let converged = read_file(&lix, &path).await;
        let merged: serde_json::Value =
            serde_json::from_slice(&converged).expect("merged JSON should parse");
        assert!(merged["v"].is_number());
        for peer in &peers {
            assert_eq!(read_file(peer, &path).await, converged);
        }
        assert_linear_history(&lix).await;
        for peer in peers {
            peer.close().await.expect("peer should close");
        }
        lix.close().await.expect("B3.1 workspace should close");
    }

    all_latencies.sort_unstable();
    let p95_index = (all_latencies.len() * 95).div_ceil(100).saturating_sub(1);
    let p95 = all_latencies[p95_index];
    eprintln!(
        "crdt_benchmarks_baseline workload=B3.1 format=json clients={clients} \
         samples={} p95_ms={:.3} commit_batch_ms={:?}",
        all_latencies.len(),
        p95.as_secs_f64() * 1_000.0,
        batch_latencies
            .iter()
            .map(|elapsed| elapsed.as_secs_f64() * 1_000.0)
            .collect::<Vec<_>>(),
    );
    assert!(
        p95 < Duration::from_millis(100),
        "same-base durable commit service p95 was {:.3} ms",
        p95.as_secs_f64() * 1_000.0
    );
}

#[tokio::test]
async fn ordinary_concurrent_execute_serializes_without_plugin_resolution() {
    let lix = open_lix().await.unwrap();
    let first = lix.open_another_session().await.unwrap();
    let second = lix.open_another_session().await.unwrap();
    let first_write = async {
        first
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('ordinary', $1) \
                 ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                &[Value::Text("first".to_owned())],
            )
            .await
    };
    let second_write = async {
        second
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('ordinary', $1) \
                 ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                &[Value::Text("second".to_owned())],
            )
            .await
    };
    let (first_result, second_result) = tokio::join!(first_write, second_write);
    first_result.unwrap();
    second_result.unwrap();
    let result = lix
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'ordinary'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(result.len(), 1);
    lix.close().await.unwrap();
}

#[test]
fn same_base_three_writer_cohort_converges_and_reuses_follower_session() {
    std::thread::Builder::new()
        .name("three-writer-cohort".to_owned())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let lix = open_lix().await.unwrap();
                    install_plugin(&lix, "plugin_json", &build_json_plugin_archive()).await;
                    let path = "/three-writer.json";
                    write_file(&lix, path, br#"{"v":-1}"#).await;
                    let file_id = file_id(&lix, path).await;
                    let mut peers = Vec::new();
                    let mut transactions = Vec::new();
                    for value in 0..3 {
                        let peer = lix.open_another_session().await.unwrap();
                        let mut transaction = peer.begin_transaction().await.unwrap();
                        transaction
                            .execute(
                                "UPDATE json_object_member SET scalar_json = $1 \
                 WHERE parent_id = 'root' AND key = 'v' AND lixcol_file_id = $2",
                                &[Value::Text(value.to_string()), Value::Text(file_id.clone())],
                            )
                            .await
                            .unwrap();
                        peers.push(peer);
                        transactions.push(transaction);
                    }

                    let results = tokio::task::LocalSet::new()
                        .run_until(async move {
                            let mut transactions = transactions.into_iter();
                            let leader_transaction = transactions.next().unwrap();
                            let leader = tokio::task::spawn_local(async move {
                                leader_transaction.commit().await
                            });
                            let mut commits = tokio::task::JoinSet::new();
                            for transaction in transactions {
                                commits.spawn_local(async move { transaction.commit().await });
                            }
                            tokio::task::yield_now().await;
                            leader.abort();
                            assert!(leader.await.unwrap_err().is_cancelled());
                            let mut results = Vec::new();
                            while let Some(result) = commits.join_next().await {
                                results.push(result.unwrap());
                            }
                            results
                        })
                        .await;
                    for result in results {
                        result.unwrap();
                    }
                    // A follower's private plugin observation must be evicted by the shared
                    // commit so its next edit cold-opens the converged durable document.
                    peers[1]
                        .execute(
                            "UPDATE json_object_member SET scalar_json = $1 \
             WHERE parent_id = 'root' AND key = 'v' AND lixcol_file_id = $2",
                            &[Value::Text("99".to_owned()), Value::Text(file_id)],
                        )
                        .await
                        .unwrap();
                    let converged = read_file(&lix, path).await;
                    assert_eq!(converged, br#"{"v":99}"#);
                    for peer in &peers {
                        assert_eq!(read_file(peer, path).await, converged);
                    }
                    assert_linear_history(&lix).await;
                    for peer in peers {
                        peer.close().await.unwrap();
                    }
                    lix.close().await.unwrap();
                });
        })
        .unwrap()
        .join()
        .unwrap();
}

/// Deterministic coverage for the multi-cohort path.
///
/// The racy same-base tests reach a split only by arrival timing, so they cannot
/// be relied on to exercise it. Committing the first writer to completion before
/// the others start forces the followers to open on a stale head and reconcile
/// against a commit that did not exist when they staged. Disjoint edits make a
/// lost update visible: every key must survive the split.
#[test]
fn serialized_leader_forces_stale_followers_to_reconcile_without_losing_writes() {
    std::thread::Builder::new()
        .name("stale-follower-reconcile".to_owned())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let lix = open_lix().await.unwrap();
                    install_plugin(&lix, "plugin_json", &build_json_plugin_archive()).await;
                    let path = "/stale-follower-reconcile.json";
                    write_file(&lix, path, br#"{"a":0,"b":0,"c":0}"#).await;
                    let file_id = file_id(&lix, path).await;
                    let mut peers = Vec::new();
                    let mut transactions = Vec::new();
                    for (key, value) in [("a", "1"), ("b", "2"), ("c", "3")] {
                        let peer = lix.open_another_session().await.unwrap();
                        let mut transaction = peer.begin_transaction().await.unwrap();
                        transaction
                            .execute(
                                "UPDATE json_object_member SET scalar_json = $1 \
                                 WHERE parent_id = 'root' AND key = $2 AND lixcol_file_id = $3",
                                &[
                                    Value::Text(value.to_owned()),
                                    Value::Text(key.to_owned()),
                                    Value::Text(file_id.clone()),
                                ],
                            )
                            .await
                            .unwrap();
                        peers.push(peer);
                        transactions.push(transaction);
                    }

                    let results = tokio::task::LocalSet::new()
                        .run_until(async move {
                            let mut transactions = transactions.into_iter();
                            let leader = transactions.next().unwrap();
                            // Publish the leader before the followers enqueue, so
                            // they cannot share its cohort.
                            let mut results = vec![leader.commit().await];
                            let mut commits = tokio::task::JoinSet::new();
                            for transaction in transactions {
                                commits.spawn_local(async move { transaction.commit().await });
                            }
                            while let Some(result) = commits.join_next().await {
                                results.push(result.unwrap());
                            }
                            results
                        })
                        .await;
                    for result in results {
                        result.expect("a stale follower must reconcile, not fail");
                    }

                    let converged = read_file(&lix, path).await;
                    let merged: serde_json::Value =
                        serde_json::from_slice(&converged).expect("merged JSON should parse");
                    for (key, value) in [("a", 1), ("b", 2), ("c", 3)] {
                        assert_eq!(
                            merged[key].as_i64(),
                            Some(value),
                            "disjoint write {key}={value} was lost reconciling against a \
                             commit published after it staged: {merged}"
                        );
                    }
                    for peer in &peers {
                        assert_eq!(read_file(peer, path).await, converged);
                    }
                    assert_linear_history(&lix).await;
                    for peer in peers {
                        peer.close().await.unwrap();
                    }
                    lix.close().await.unwrap();
                });
        })
        .unwrap()
        .join()
        .unwrap();
}

#[tokio::test]
async fn invalid_aggregate_member_does_not_poison_valid_transaction() {
    let lix = open_lix().await.unwrap();
    install_plugin(&lix, "plugin_json", &build_json_plugin_archive()).await;
    let first = lix.open_another_session().await.unwrap();
    let second = lix.open_another_session().await.unwrap();
    let mut first_transaction = first.begin_transaction().await.unwrap();
    let mut second_transaction = second.begin_transaction().await.unwrap();
    for (transaction, bytes) in [
        (&mut first_transaction, br#"{"winner":1}"#.as_slice()),
        (&mut second_transaction, br#"{"winner":2}"#.as_slice()),
    ] {
        transaction
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ('/unique-path.json', $1)",
                &[Value::Blob(bytes.to_vec().into())],
            )
            .await
            .unwrap();
    }

    let commits_before = commit_count(&lix).await;
    let results = tokio::task::LocalSet::new()
        .run_until(async move {
            let first = tokio::task::spawn_local(async move { first_transaction.commit().await });
            let second = tokio::task::spawn_local(async move { second_transaction.commit().await });
            vec![first.await.unwrap(), second.await.unwrap()]
        })
        .await;
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
    assert_eq!(commit_count(&lix).await - commits_before, 1);
    assert!(!read_file(&lix, "/unique-path.json").await.is_empty());
    first.close().await.unwrap();
    second.close().await.unwrap();
    lix.close().await.unwrap();
}

fn report(workload: &str, format: &str, samples: &mut [Duration]) {
    samples.sort_unstable();
    let p50 = samples[samples.len() / 2].as_secs_f64() * 1_000.0;
    let p95_index = (samples.len() * 95).div_ceil(100).saturating_sub(1);
    let p95 = samples[p95_index].as_secs_f64() * 1_000.0;
    let raw = samples
        .iter()
        .map(|sample| format!("{:.3}", sample.as_secs_f64() * 1_000.0))
        .collect::<Vec<_>>();
    eprintln!(
        "crdt_benchmarks_baseline workload={workload} format={format} n={N} \
         samples_ms={raw:?} p50_ms={p50:.3} p95_ms={p95:.3}"
    );
}

async fn install_plugin<StorageImpl>(lix: &Lix<StorageImpl>, key: &str, archive: &[u8])
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
        &[
            Value::Text(format!("/.lix/plugins/{key}.lixplugin")),
            Value::Blob(archive.to_vec().into()),
        ],
    )
    .await
    .expect("reference plugin should install");
}

async fn write_file<StorageImpl>(lix: &Lix<StorageImpl>, path: &str, bytes: &[u8])
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
         ON CONFLICT (path) DO UPDATE SET content = excluded.content",
        &[
            Value::Text(path.to_owned()),
            Value::Blob(bytes.to_vec().into()),
        ],
    )
    .await
    .expect("benchmark file should write");
}

async fn read_file<StorageImpl>(lix: &Lix<StorageImpl>, path: &str) -> Vec<u8>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT content FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_owned())],
    )
    .await
    .expect("benchmark file should read")
    .rows()[0]
        .get::<Vec<u8>>("content")
        .expect("benchmark file data should be bytes")
}

async fn file_id<StorageImpl>(lix: &Lix<StorageImpl>, path: &str) -> String
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "SELECT id FROM lix_file WHERE path = $1",
        &[Value::Text(path.to_owned())],
    )
    .await
    .expect("benchmark file id should query")
    .rows()[0]
        .get::<String>("id")
        .expect("benchmark file id should be text")
}

async fn commit_count<StorageImpl>(lix: &Lix<StorageImpl>) -> i64
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.execute("SELECT COUNT(*) AS count FROM lix_commit", &[])
        .await
        .expect("benchmark commit count should query")
        .rows()[0]
        .get::<i64>("count")
        .expect("benchmark commit count should be an integer")
}

/// Asserts the commit history never forked.
///
/// Two commits sharing a parent is what a genuine convergence defect would look
/// like: concurrent writers publishing divergent successors of the same base.
/// The number of commits a same-base wave publishes is timing-dependent and is
/// deliberately not asserted anywhere; this is the property that is not.
async fn assert_linear_history<StorageImpl>(lix: &Lix<StorageImpl>)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let forks = lix
        .execute(
            "SELECT parent_id, COUNT(*) AS children FROM lix_commit_edge \
             GROUP BY parent_id HAVING COUNT(*) > 1",
            &[],
        )
        .await
        .expect("commit edge fork query should run");
    let forks = forks
        .rows()
        .iter()
        .map(|row| {
            format!(
                "{} has {} children",
                row.get::<String>("parent_id").unwrap_or_default(),
                row.get::<i64>("children").unwrap_or_default(),
            )
        })
        .collect::<Vec<_>>();
    assert!(
        forks.is_empty(),
        "same-base writers must not fork the commit history: {forks:?}"
    );
}

fn build_markdown_plugin_archive() -> Vec<u8> {
    build_plugin_archive(
        Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_MARKDOWN_plugin_markdown")),
        include_str!("../../../plugins/markdown/manifest.json"),
        &[(
            "schema/markdown_node.json",
            include_str!("../../../plugins/markdown/schema/markdown_node.json"),
        )],
    )
}

fn build_json_plugin_archive() -> Vec<u8> {
    build_plugin_archive(
        Path::new(env!("CARGO_CDYLIB_FILE_PLUGIN_JSON_plugin_json")),
        include_str!("../../../plugins/json/manifest.json"),
        &[
            (
                "schema/json_root.json",
                include_str!("../../../plugins/json/schema/json_root.json"),
            ),
            (
                "schema/json_object_member.json",
                include_str!("../../../plugins/json/schema/json_object_member.json"),
            ),
            (
                "schema/json_array_item.json",
                include_str!("../../../plugins/json/schema/json_array_item.json"),
            ),
        ],
    )
}

fn build_plugin_archive(wasm_path: &Path, manifest: &str, schemas: &[(&str, &str)]) -> Vec<u8> {
    let wasm = fs::read(wasm_path).unwrap_or_else(|error| {
        panic!(
            "failed to read plugin component at {}: {error}",
            wasm_path.display()
        )
    });
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    writer.start_file("manifest.json", options).unwrap();
    writer.write_all(manifest.as_bytes()).unwrap();
    for (path, schema) in schemas {
        writer.start_file(path, options).unwrap();
        writer.write_all(schema.as_bytes()).unwrap();
    }
    writer.start_file("plugin.wasm", options).unwrap();
    writer.write_all(&wasm).unwrap();
    writer.finish().unwrap().into_inner()
}
