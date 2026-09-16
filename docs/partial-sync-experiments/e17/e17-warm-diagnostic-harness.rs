//! Public awaited SQL over real loopback HTTP and canonical authority handling.
//! Fixtures are exported once and restored for each run, so paired revisions
//! see identical commit IDs and physical trees. No caller-side hydration.
#![recursion_limit = "512"]
use futures_util::io::Cursor;
use http::{Request, Response};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use lix::server_protocol::{LixServerProtocol, ServerProtocolBody, ServerProtocolContext};
use lix::{Lix, Memory, ServerOptions, Value, open_lix};
use lix_storage_rocksdb::RocksDB;
use serde_json::json;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;

#[derive(Clone, Default, serde::Serialize)]
struct Counts {
    attempts: usize,
    bytes: usize,
}
#[derive(Default)]
struct Probe {
    counts: Mutex<BTreeMap<String, Counts>>,
    offline: AtomicBool,
}
impl Probe {
    fn snapshot(&self) -> BTreeMap<String, Counts> {
        self.counts.lock().unwrap().clone()
    }
}
fn diff(
    after: &BTreeMap<String, Counts>,
    before: &BTreeMap<String, Counts>,
) -> BTreeMap<String, Counts> {
    after
        .iter()
        .map(|(key, value)| {
            let old = before.get(key).cloned().unwrap_or_default();
            (
                key.clone(),
                Counts {
                    attempts: value.attempts - old.attempts,
                    bytes: value.bytes - old.bytes,
                },
            )
        })
        .collect()
}
fn native_requests(counts: &BTreeMap<String, Counts>) -> usize {
    counts
        .iter()
        .filter(|(k, _)| k.starts_with("native-"))
        .map(|(_, v)| v.attempts)
        .sum()
}
async fn handle(
    protocol: LixServerProtocol<RocksDB>,
    probe: Arc<Probe>,
    request: Request<Incoming>,
    rtt: u64,
) -> Result<Response<ServerProtocolBody>, Infallible> {
    let (parts, body) = request.into_parts();
    let endpoint = if parts.uri.path().ends_with("/descriptor")
        && parts.uri.query().is_some_and(|q| q.contains("after="))
    {
        "descriptor-watch".to_owned()
    } else {
        parts
            .uri
            .path()
            .rsplit('/')
            .next()
            .unwrap_or("root")
            .to_owned()
    };
    // Avoid serializing repository identifiers in the result artifact.
    let endpoint = if endpoint.is_empty() || uuid::Uuid::parse_str(&endpoint).is_ok() {
        "handshake".to_owned()
    } else {
        endpoint
    };
    probe
        .counts
        .lock()
        .unwrap()
        .entry(endpoint.clone())
        .or_default()
        .attempts += 1;
    if probe.offline.load(Ordering::Acquire) {
        return Ok(Response::builder()
            .status(503)
            .body(ServerProtocolBody::from(Vec::new()))
            .unwrap());
    }
    if rtt > 0 {
        tokio::time::sleep(Duration::from_millis(rtt / 2)).await;
    }
    let body = body.collect().await.unwrap().to_bytes();
    let response = protocol
        .handle(
            Request::from_parts(parts, ServerProtocolBody::full(body)),
            ServerProtocolContext::anonymous(),
        )
        .await;
    let (parts, body) = response.into_parts();
    let bytes = body.collect().await.unwrap().to_bytes();
    probe
        .counts
        .lock()
        .unwrap()
        .entry(endpoint)
        .or_default()
        .bytes += bytes.len();
    if rtt > 0 {
        tokio::time::sleep(Duration::from_millis(rtt - rtt / 2)).await;
    }
    Ok(Response::from_parts(parts, ServerProtocolBody::full(bytes)))
}
async fn serve(
    storage: RocksDB,
    rtt: u64,
    listen: Option<String>,
) -> (String, Arc<Probe>, tokio::task::JoinHandle<()>) {
    let protocol = open_lix()
        .with_storage(storage)
        .serve()
        .with_embedded_lix_id()
        .await
        .unwrap();
    let listener = TcpListener::bind(listen.as_deref().unwrap_or("127.0.0.1:0"))
        .await
        .unwrap();
    let url = format!(
        "http://{}/lix/{}",
        listener.local_addr().unwrap(),
        protocol.lix_id()
    );
    let probe = Arc::new(Probe::default());
    let shared = probe.clone();
    let task = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted=listener.accept()=>{let (socket,_)=accepted.unwrap();let protocol=protocol.clone();let probe=shared.clone();
                    connections.spawn(async move {let service=service_fn(move |request|handle(protocol.clone(),probe.clone(),request,rtt));
                        let _=http1::Builder::new().serve_connection(TokioIo::new(socket),service).await;});},
                _=connections.join_next(), if !connections.is_empty()=>{},
            }
        }
    });
    (url, probe, task)
}
#[derive(Clone, Copy)]
struct Case {
    name: &'static str,
    depth: usize,
    every: usize,
    width: usize,
    blob: usize,
}
async fn put(authority: &Lix<Memory>, path: &str, content: Vec<u8>) {
    let existing = authority
        .execute(
            "SELECT id FROM lix_file WHERE path=$1",
            &[Value::Text(path.into())],
        )
        .await
        .unwrap();
    let sql = if existing.rows().is_empty() {
        "INSERT INTO lix_file (path,content) VALUES ($1,$2)"
    } else {
        "UPDATE lix_file SET content=$2 WHERE path=$1"
    };
    authority
        .execute(
            sql,
            &[Value::Text(path.into()), Value::Blob(content.into())],
        )
        .await
        .unwrap();
}
fn selected_path(case: Case) -> String {
    let depth = case.name.strip_prefix("deep").and_then(|value| value.parse::<usize>().ok()).unwrap_or(0);
    format!("{}{}", (0..depth).map(|n| format!("/level-{n}")).collect::<String>(), "/selected.txt")
}
async fn fixture(case: Case, path: &PathBuf) {
    if path.exists() {
        return;
    }
    let authority = open_lix().await.unwrap();
    let target_path = selected_path(case);
    put(&authority, &target_path, b"initial".to_vec()).await;
    for chunk in (0..case.width).collect::<Vec<_>>().chunks(500) {
        let values = chunk
            .iter()
            .map(|i| if case.name == "dirs1600" {
                format!("('/unrelated-{i}/leaf.bin', $1)")
            } else {
                format!("('/unrelated-{i}.txt', $1)")
            })
            .collect::<Vec<_>>()
            .join(",");
        authority
            .execute(
                &format!("INSERT INTO lix_file (path,content) VALUES {values}"),
                &[Value::Blob(vec![42; 8].into())],
            )
            .await
            .unwrap();
    }
    if case.blob > 0 {
        put(
            &authority,
            "/unopened.bin",
            (0..case.blob)
                .map(|i| (i.wrapping_mul(131) ^ (i >> 9)) as u8)
                .collect(),
        )
        .await;
    }
    for index in 0..case.depth {
        put(
            &authority,
            if index % case.every == 0 {
                &target_path
            } else {
                "/other.txt"
            },
            format!("revision {index}").into_bytes(),
        )
        .await;
        authority
            .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
            .await
            .unwrap();
    }
    if let Some(depth) = case.name.strip_prefix("deep") {
        let directories = authority.execute("SELECT id FROM lix_directory WHERE path LIKE '/level-0%'", &[]).await.unwrap();
        assert_eq!(directories.rows().len(), depth.parse::<usize>().unwrap(), "deep control must create the declared ancestor depth");
    }
    if case.name == "dirs1600" {
        let directories = authority.execute("SELECT id FROM lix_directory WHERE path LIKE '/unrelated-%'", &[]).await.unwrap();
        assert_eq!(directories.rows().len(), case.width, "directory control must create one unrelated directory per file");
    }
    let mut bytes = Vec::new();
    authority
        .export_snapshot()
        .write_to(&mut bytes)
        .await
        .unwrap();
    std::fs::write(path, bytes).unwrap();
    authority.close().await.unwrap();
}
async fn run(case: Case, snapshot: &[u8], sample: usize, rtt: u64) {
    let authority_dir = tempfile::tempdir().unwrap();
    let replica_dir = tempfile::tempdir().unwrap();
    // Optional cross-version control: the baseline leaves a real partial
    // RocksDB replica after file selection; the candidate reopens it at the
    // same authority URL, without resetting or rewriting its stored receipt.
    let persisted = std::env::var("LIX_PROFILE_PERSISTED_REPLICAS")
        .ok()
        .map(|root| PathBuf::from(root).join(case.name));
    if let Some(root) = &persisted {
        std::fs::create_dir_all(root).unwrap();
    }
    let replica_path = persisted
        .as_ref()
        .map(|root| root.join("replica"))
        .unwrap_or_else(|| replica_dir.path().to_owned());
    let reopening = persisted.is_some() && replica_path.exists();
    let listen = persisted
        .as_ref()
        .and_then(|root| std::fs::read_to_string(root.join("authority-address")).ok());
    let prepare = std::env::var("LIX_PROFILE_PREPARE_REPLICA").as_deref() == Ok("1");
    assert!(
        !prepare || persisted.is_some(),
        "preparing requires a persistent replica directory"
    );
    assert!(
        !prepare || !reopening,
        "preparation requires a fresh replica directory"
    );
    let backing = RocksDB::open(authority_dir.path()).unwrap();
    let authority = open_lix()
        .with_storage(backing.clone())
        .from_snapshot(Cursor::new(snapshot.to_vec()))
        .await
        .unwrap();
    let file = authority
        .execute("SELECT id FROM lix_file WHERE path=$1", &[Value::Text(selected_path(case))])
        .await
        .unwrap()
        .rows()[0]
        .get::<String>("id")
        .unwrap();
    let history_limit = std::env::var("LIX_PROFILE_HISTORY_LIMIT")
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("history limit must be an integer")
        });
    let mut sql = "SELECT lixcol_to_commit_id, diff_type, coalesce(to_path,from_path) AS path FROM lix_history('lix_file') WHERE id=$1 AND lixcol_commit_is_checkpoint=true ORDER BY lixcol_position ASC".to_owned();
    if let Some(limit) = history_limit {
        assert!(limit > 0, "history limit must be positive");
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    // The absent-ID case exercises an empty history result over real checkpoints.
    let query_file = if case.name == "absent64" {
        uuid::Uuid::nil().to_string()
    } else {
        file.clone()
    };
    let params = [Value::Text(query_file)];
    let expected = authority.execute(&sql, &params).await.unwrap();
    if case.name == "absent64" {
        assert!(expected.rows().is_empty(), "absent file ID must produce no history");
    }
    let file_lookup = std::env::var("LIX_PROFILE_FILE_LOOKUP").unwrap_or("path".into());
    let (file_sql, file_params) = match file_lookup.as_str() {
        "path" => ("SELECT id,path FROM lix_file WHERE path=$1".to_owned(), vec![Value::Text(selected_path(case))]),
        "id" => ("SELECT id,path FROM lix_file WHERE id=$1".to_owned(), vec![Value::Text(file.clone())]),
        "missing_path" => ("SELECT id,path FROM lix_file WHERE path='/absent-selected.txt'".to_owned(), vec![]),
        "missing_id" => ("SELECT id,path FROM lix_file WHERE id=$1".to_owned(), vec![Value::Text(uuid::Uuid::nil().to_string())]),
        "prefix" => ("SELECT id,path FROM lix_file WHERE path LIKE $1 ORDER BY path,id".to_owned(), vec![Value::Text(format!("{}%", selected_path(case).trim_end_matches(".txt")))]),
        "invalid_id" => ("SELECT id,path FROM lix_file WHERE id=$1".to_owned(), vec![Value::Text("not-a-uuid".into())]),
        "id_batch16" | "id_batch256" => {
            let count = if file_lookup == "id_batch16" { 16 } else { 256 };
            let ids = authority.execute(
                &format!("SELECT id FROM lix_file WHERE path=$1 OR path LIKE '/unrelated-%' ORDER BY path LIMIT {count}"),
                &[Value::Text(selected_path(case))],
            ).await.unwrap();
            let params = ids.rows().iter().map(|row| Value::Text(row.get::<String>("id").unwrap())).collect::<Vec<_>>();
            assert!(!params.is_empty());
            let placeholders = (1..=params.len()).map(|index| format!("${index}")).collect::<Vec<_>>().join(",");
            (format!("SELECT id,path FROM lix_file WHERE id IN ({placeholders}) ORDER BY id"), params)
        }
        _ => panic!("unknown file lookup mode"),
    };
    let expected_file = authority.execute(&file_sql, &file_params).await.unwrap();
    authority.close().await.unwrap();
    let (url, probe, task) = serve(backing, rtt, listen).await;
    if let Some(root) = &persisted {
        let address = url
            .strip_prefix("http://")
            .unwrap()
            .split('/')
            .next()
            .unwrap();
        std::fs::write(root.join("authority-address"), address).unwrap();
    }
    let start = Instant::now();
    let replica = open_lix()
        .with_storage(RocksDB::open(&replica_path).unwrap())
        .with_server(ServerOptions::new(url))
        .await
        .unwrap();
    let open_us = start.elapsed().as_micros();
    let open_counts = probe.snapshot();
    assert_eq!(
        native_requests(&open_counts),
        0,
        "opening must not hydrate native inputs"
    );
    for endpoint in ["handshake", "descriptor"] {
        let attempts = open_counts.get(endpoint).map_or(0, |count| count.attempts);
        if reopening {
            assert!(attempts <= 1, "cached reopening must remain bounded");
        } else {
            assert_eq!(
                attempts, 1,
                "fresh opening requires its initial coordinates"
            );
        }
    }
    let foreground_bytes = open_counts
        .iter()
        .filter(|(k, _)| k.as_str() != "descriptor-watch")
        .map(|(_, v)| v.bytes)
        .sum::<usize>();
    assert!(
        foreground_bytes <= 8192,
        "opening bytes must remain bounded"
    );
    // Match the reported application sequence: open the selected file before history.
    let started = Instant::now();
    let before = probe.snapshot();
    let selected_file = replica.execute(&file_sql, &file_params).await.unwrap();
    let file_open_us = started.elapsed().as_micros();
    let file_open = diff(&probe.snapshot(), &before);
    assert_eq!(selected_file.rows(), expected_file.rows());
    probe.offline.store(true, Ordering::Release);
    let before_file_warm = probe.snapshot();
    let file_warm_start = Instant::now();
    let covered_file = replica.execute(&file_sql, &file_params).await.unwrap();
    let file_lookup_warm_us = file_warm_start.elapsed().as_micros();
    assert_eq!(covered_file.rows(), expected_file.rows());
    assert_eq!(native_requests(&diff(&probe.snapshot(), &before_file_warm)), 0);
    probe.offline.store(false, Ordering::Release);
    if prepare {
        replica.close().await.unwrap();
        task.abort();
        let _ = task.await;
        println!(
            "{}",
            json!({"schema":"lix.partial-history.prepare.v1", "case":case.name,
            "snapshot_digest":blake3::hash(snapshot).to_hex().to_string(), "opening":open_counts,
            "file_open":file_open, "prepared":true})
        );
        return;
    }
    let started = Instant::now();
    let before = probe.snapshot();
    let actual = replica.execute(&sql, &params).await.unwrap();
    let history_us = started.elapsed().as_micros();
    let history = diff(&probe.snapshot(), &before);
    assert_eq!(actual.rows(), expected.rows());
    probe.offline.store(true, Ordering::Release);
    let idle_ms: u64 = std::env::var("LIX_PROFILE_WARM_IDLE_MS").unwrap_or("0".into()).parse().unwrap();
    let repeats: usize = std::env::var("LIX_PROFILE_WARM_REPEATS").unwrap_or("20".into()).parse().unwrap();
    let mut warm_samples = Vec::new();
    for iteration in 0..repeats {
        if idle_ms > 0 { tokio::time::sleep(Duration::from_millis(idle_ms)).await; }
        let _ = attribution();
        let before = probe.snapshot();
        let started = Instant::now();
        let warm = replica.execute(&sql, &params).await.unwrap();
        let elapsed_us = started.elapsed().as_micros();
        let counters = attribution();
        assert_eq!(warm.rows(), expected.rows());
        let network = diff(&probe.snapshot(), &before);
        assert_eq!(native_requests(&network), 0, "covered query must be local offline");
        warm_samples.push(json!({"iteration":iteration,"us":elapsed_us,"counters":counters,"network":network}));
    }
    let warm_us = warm_samples[0]["us"].as_u64().unwrap();
    probe.offline.store(false, Ordering::Release);
    replica.close().await.unwrap();
    task.abort();
    let _ = task.await;
    println!(
        "{}",
        json!({"warm_samples":warm_samples,"warm_idle_ms":idle_ms,"trace_enabled":lix::storage_bench::root_replay_trace_enabled(),"schema":"lix.partial-history.e17-diagnostic","revision":std::env::var("LIX_PROFILE_REVISION").unwrap_or_default(),"case":case.name,"sample":sample,"rtt_ms":rtt,"history_limit":history_limit,"persistent_replica":persisted.is_some(),"reopening":reopening,"depth":case.depth,"directory_depth":case.name.strip_prefix("deep").and_then(|v|v.parse::<usize>().ok()).unwrap_or(0),"relevant_every":case.every,"width":case.width,"unopened_blob_bytes":case.blob,"snapshot_digest":blake3::hash(snapshot).to_hex().to_string(),"open_us":open_us,"opening":open_counts,"file_open_us":file_open_us,"file_open":file_open,"file_lookup":file_lookup,"file_lookup_warm_us":file_lookup_warm_us,"file_lookup_rows":selected_file.rows().len(),"history_us":history_us,"history":history,"history_native_requests":native_requests(&history),"warm_us":warm_us,"rows":actual.rows().len()})
    );
}
fn main() {
    let samples = std::env::var("LIX_PROFILE_SAMPLES")
        .unwrap_or("10".into())
        .parse::<usize>()
        .unwrap();
    let rtt = std::env::var("LIX_PROFILE_RTT_MS")
        .unwrap_or("0".into())
        .parse::<u64>()
        .unwrap();
    let selected = std::env::var("LIX_PROFILE_CASES").unwrap_or("dense64,sparse64".into());
    let dir = PathBuf::from(
        std::env::var("LIX_PROFILE_FIXTURES")
            .expect("set LIX_PROFILE_FIXTURES to a reusable synthetic-fixture directory"),
    );
    std::fs::create_dir_all(&dir).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        for case in [
            Case { name:"deep4", depth:4, every:1, width:0, blob:0 },
            Case { name:"deep16", depth:4, every:1, width:0, blob:0 },
            Case { name:"deep64", depth:4, every:1, width:0, blob:0 },
            Case {
                name: "dense4",
                depth: 4,
                every: 1,
                width: 0,
                blob: 0,
            },
            Case {
                name: "dense64",
                depth: 64,
                every: 1,
                width: 0,
                blob: 0,
            },
            Case {
                name: "sparse64",
                depth: 64,
                every: 8,
                width: 0,
                blob: 0,
            },
            Case {
                name: "dense256",
                depth: 256,
                every: 1,
                width: 0,
                blob: 0,
            },
            Case {
                name: "dirs1600",
                depth: 4,
                every: 1,
                width: 1600,
                blob: 0,
            },
            Case {
                name: "wide16000",
                depth: 4,
                every: 1,
                width: 16000,
                blob: 0,
            },
            Case {
                name: "wide_sparse64",
                depth: 64,
                every: 8,
                width: 16000,
                blob: 0,
            },
            Case {
                name: "absent64",
                depth: 64,
                every: 1,
                width: 0,
                blob: 0,
            },
            Case {
                name: "blob8m",
                depth: 4,
                every: 1,
                width: 0,
                blob: 8 * 1024 * 1024,
            },
        ] {
            if !selected.split(',').any(|name| name == case.name) {
                continue;
            }
            let path = dir.join(format!("{}.snapshot", case.name));
            fixture(case, &path).await;
            let snapshot = std::fs::read(path).unwrap();
            for sample in 0..samples {
                run(case, &snapshot, sample, rtt).await;
            }
        }
    });
}

fn attribution() -> serde_json::Value {
    use lix::storage_bench as b;
    let c = b::take_root_replay_cost_attribution();
    let r = b::take_root_replay_accounting();
    let plan = b::take_plan_load_attribution();
    fn cost(c: b::RootReplayCostBucket) -> serde_json::Value {
        json!({"ns":c.total_nanos,"bytes":c.total_bytes,"count":c.total_count,"replay_ns":c.replay_nanos})
    }
    json!({"read":cost(c.storage_read),"decode":cost(c.decode),"hash":cost(c.hash),"encode":cost(c.encode),
      "root_cache":b::take_root_base_batch_cache_accounting(),"scan_arms":b::take_tracked_scan_branch_accounting(),
      "replay":{"boundaries":r.boundaries,"loaded":r.plans_loaded,"staged":r.plans_staged,"probes":r.available_root_probes,"hits":r.available_root_hits,"load_ns":r.plan_load_nanos,"stage_ns":r.stage_nanos},
      "plan":{"plans":plan.plans,"members":plan.members_decoded,"bytes":plan.member_payload_bytes,
        "phases":plan.phases.iter().enumerate().map(|(i,v)|json!({"name":b::PLAN_LOAD_PHASE_NAMES[i],"wall_ns":v.wall_nanos,"io_ns":v.io_nanos,"calls":v.read_calls,"keys":v.read_keys,"bytes":v.read_bytes,"entries":v.entries})).collect::<Vec<_>>()}})
}
